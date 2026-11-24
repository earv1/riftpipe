//! riftpipe CLI. `text` runs an offline demo of the CRDT core. `share` and `join`
//! do a real one-shot CRDT exchange over iroh; `connect` syncs a folder with a
//! browser peer over WebRTC; `serve` static-hosts a folder with live change
//! events; `signal` and `webrtc-echo` back the browser stack.

use std::sync::Arc;

use riftpipe::crdt::text::EgWalkerText;
use riftpipe::net::negotiate::negotiate_link;
use riftpipe::net::secure::{authenticate, Ticket};
use riftpipe::net::transport::{accept_link, bind_accept, bind_connect, connect_link, local_addr};
use riftpipe::net::{anyerr, Counters};
use riftpipe::sync::folder::run_folder_reconnecting;
use riftpipe::sync::manifest::Manifest;
use riftpipe::sync::mirror::TextPeer;
use riftpipe::sync::pipe::{run_pipe_reconnecting, Role};
use riftpipe::sync::workspace::Workspace;

/// Parsed CLI options for `share`/`join`/`connect`.
struct Opts {
    pos: Vec<String>,
    pipe: bool,
    memory: bool,
    accept: bool,
    metrics: Option<String>,
    manifest: Option<String>,
    process: Option<String>,
    signal: Option<String>,
    /// Device-linking auth (mesh): sign our writes and reject unsigned/
    /// unauthorized ones. Identity persists in `<dir>/.device`.
    restricted: bool,
    /// Founder only: hex device ids to authorize (as Members) at startup.
    grant: Vec<String>,
    /// `--restricted` joiner only: display name on the broadcast join request.
    name: Option<String>,
    /// `connect --accept` only: forget `<dir>/.riftpipe-host` and mint a new
    /// identity + board secret (a new link), instead of resuming the old board.
    new_board: bool,
}

/// Parse `args[2..]` into positionals + flags. `--metrics`/`--manifest`/
/// `--process`/`--signal` each take a value.
fn parse(args: &[String]) -> Opts {
    let mut o = Opts {
        pos: Vec::new(),
        pipe: false,
        memory: false,
        accept: false,
        metrics: None,
        manifest: None,
        process: None,
        signal: None,
        restricted: false,
        grant: Vec::new(),
        name: None,
        new_board: false,
    };
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--pipe" => o.pipe = true,
            "--memory" => o.memory = true,
            "--accept" => o.accept = true,
            "--restricted" => o.restricted = true,
            "--new-board" => o.new_board = true,
            "--grant" => {
                if let Some(v) = args.get(i + 1).cloned() {
                    o.grant.push(v);
                }
                i += 1;
            }
            "--name" => {
                o.name = args.get(i + 1).cloned();
                i += 1;
            }
            "--metrics" => {
                o.metrics = args.get(i + 1).cloned();
                i += 1;
            }
            "--manifest" => {
                o.manifest = args.get(i + 1).cloned();
                i += 1;
            }
            "--process" => {
                o.process = args.get(i + 1).cloned();
                i += 1;
            }
            "--signal" => {
                o.signal = args.get(i + 1).cloned();
                i += 1;
            }
            s if s.starts_with("--") => {}
            s => o.pos.push(s.to_string()),
        }
        i += 1;
    }
    o
}

/// Load the manifest: `--manifest <path>` if given, else `<dir>/riftpipe.toml`,
/// else the default (all-rsync).
fn load_manifest(opts: &Opts, dir: &str) -> riftpipe::net::Result<Manifest> {
    let path = opts
        .manifest
        .clone()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::Path::new(dir).join("riftpipe.toml"));
    Manifest::load_or_default(&path).map_err(anyerr)
}

/// Find `--flag value` in args, returning the value that follows the flag.
fn flag_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

/// Where the in-memory `process` file goes: `--process <path>`, else `process`
/// in memory mode, else none (file mode doesn't need it).
fn process_path(opts: &Opts) -> Option<String> {
    opts.process
        .clone()
        .or_else(|| opts.memory.then(|| "process".to_string()))
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str).unwrap_or("help") {
        "text" => demo_text(),
        // Print (creating if absent) the device id for `<dir>/.device` — so a
        // founder can `--grant` it. The seed stays local; only the public id prints.
        "device-id" => {
            match args.get(2) {
                Some(dir) => {
                    let _ = std::fs::create_dir_all(dir);
                    let key = load_or_create_device(std::path::Path::new(dir));
                    let id: String = key.id().iter().map(|b| format!("{b:02x}")).collect();
                    println!("{id}");
                }
                None => eprintln!("usage: riftpipe device-id <dir>"),
            }
        }
        // Live admin commands on a running `connect … --restricted` peer: queue a
        // revoke/grant by appending to <dir>/.riftpipe-control, which the running
        // mesh loop polls once a second (`riftpipe::sync::control`).
        "revoke" | "grant" => {
            let verb = args[1].clone();
            match (args.get(2), args.get(3)) {
                (Some(dir), Some(id)) if hex_to_32(id).is_some() => {
                    match riftpipe::sync::control::append(std::path::Path::new(dir), &verb, id) {
                        Ok(()) => println!("queued `{verb} {id}` for the running restricted peer on {dir}"),
                        Err(e) => eprintln!("[riftpipe] {verb} failed: {e}"),
                    }
                }
                _ => eprintln!("usage: riftpipe {verb} <dir> <device-id>   # applied by the running `connect … --restricted` admin peer on <dir>"),
            }
        }
        // Read-only views of a running restricted peer's auth state, written to
        // <dir>/.riftpipe-auth.json by the mesh loop (`riftpipe::sync::control`).
        "requests" | "members" => {
            let verb = args[1].as_str();
            let Some(dir) = args.get(2) else {
                eprintln!("usage: riftpipe requests|members <dir> [--json]");
                std::process::exit(2);
            };
            let json = args.iter().any(|a| a == "--json");
            let Some(state) = riftpipe::sync::control::read_state(std::path::Path::new(dir)) else {
                eprintln!("[riftpipe] no auth state in {dir} — run `riftpipe connect … --restricted` on it first");
                std::process::exit(1);
            };
            if let Some(w) = riftpipe::sync::control::stale_warning(&state, riftpipe::sync::control::unix_now_secs()) {
                eprintln!("{w}");
            }
            if json {
                println!("{}", state.get(verb).cloned().unwrap_or(serde_json::json!([])));
            } else if verb == "requests" {
                let lines = riftpipe::sync::control::format_requests(&state);
                if lines.is_empty() {
                    eprintln!("no pending requests");
                }
                if riftpipe::sync::control::inbox_overflowed(&state) {
                    eprintln!(
                        "[riftpipe] warning: the request list overflowed this session — some requests were dropped; \
                         approve by device id, not safety code"
                    );
                }
                for line in lines {
                    println!("{line}");
                }
            } else {
                for line in riftpipe::sync::control::format_members(&state) {
                    println!("{line}");
                }
            }
        }
        // Resolve a device id or safety code against the running peer's auth
        // state, then queue it through the same control file `grant`/`revoke` use.
        "approve" => {
            if args.get(2).is_none() || args.len() <= 3 {
                eprintln!("usage: riftpipe approve <dir> <device-id|safety-code>");
                std::process::exit(2);
            }
            let dir = args[2].clone();
            let target = args[3..].concat();
            let state = riftpipe::sync::control::read_state(std::path::Path::new(&dir));
            if let Some(w) = state
                .as_ref()
                .and_then(|s| riftpipe::sync::control::stale_warning(s, riftpipe::sync::control::unix_now_secs()))
            {
                eprintln!("{w}");
            }
            match riftpipe::sync::control::resolve_approve_target(state.as_ref(), &target) {
                Ok((id, label)) => {
                    let hex: String = id.iter().map(|b| format!("{b:02x}")).collect();
                    match riftpipe::sync::control::append(std::path::Path::new(&dir), "approve", &hex) {
                        Ok(()) => {
                            let suffix = label.map(|l| format!(" {l}")).unwrap_or_default();
                            println!(
                                "queued `approve` for device {hex}{suffix} — the running restricted admin peer on {dir} grants it and wraps the read key"
                            );
                        }
                        Err(e) => {
                            eprintln!("[riftpipe] approve failed: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                Err(m) => {
                    eprintln!("[riftpipe] approve: {m}");
                    std::process::exit(1);
                }
            }
        }
        "share" => {
            let opts = parse(&args);
            match opts.pos.first().cloned() {
                Some(file) => {
                    if let Err(e) = share(&file, &opts).await {
                        eprintln!("[riftpipe] share failed: {e}");
                    }
                }
                None => eprintln!("usage: riftpipe share <file|dir> [--pipe] [--memory] [--metrics <path>] [--manifest <path>] [--process <path>]"),
            }
        }
        "join" => {
            let opts = parse(&args);
            match (opts.pos.first().cloned(), opts.pos.get(1).cloned()) {
                (Some(ticket), Some(file)) => {
                    if let Err(e) = join(&ticket, &file, &opts).await {
                        eprintln!("[riftpipe] join failed: {e}");
                    }
                }
                _ => eprintln!("usage: riftpipe join <ticket> <file|dir> [--pipe] [--memory] [--metrics <path>] [--manifest <path>] [--process <path>]"),
            }
        }
        "connect" => {
            let opts = parse(&args);
            let res = if opts.accept {
                match opts.pos.first() {
                    Some(dir) => connect_accept(dir, &opts).await,
                    None => {
                        eprintln!("usage: riftpipe connect --accept <dir> [--new-board] [--metrics <path>]");
                        return;
                    }
                }
            } else {
                match (opts.pos.first(), opts.pos.get(1)) {
                    (Some(target), Some(dir)) => connect_dial(target, dir, &opts).await,
                    _ => {
                        eprintln!("usage: riftpipe connect <ticket|browser-link|connection-id> <dir> [--signal ws://…] [--metrics <path>]");
                        eprintln!("       riftpipe connect --accept <dir> [--new-board] [--metrics <path>]");
                        return;
                    }
                }
            };
            if let Err(e) = res {
                eprintln!("[riftpipe] connect failed: {e}");
            }
        }
        "serve" => {
            let dir = args.get(2).cloned().unwrap_or_default();
            let port = flag_value(&args, "--port").and_then(|v| v.parse().ok()).unwrap_or(8080);
            if dir.is_empty() {
                eprintln!("usage: riftpipe serve <dir> [--port 8080]");
            } else {
                // The HTTP server is synchronous (tiny_http); run it off the async
                // runtime so a future sync task can share the process.
                let res = tokio::task::spawn_blocking(move || serve_dir(&dir, port)).await;
                if let Ok(Err(e)) = res {
                    eprintln!("[serve] failed: {e}");
                }
            }
        }
        "signal" => {
            let port = flag_value(&args, "--port").and_then(|v| v.parse().ok()).unwrap_or(9000);
            if let Err(e) = riftpipe::app::signal::serve(port).await {
                eprintln!("[signal] serve failed: {e}");
            }
        }
        // Cross-stack bridge probe: connect to a browser peer via the signaling
        // server, send a message, print the one received. Used by the e2e test.
        "webrtc-echo" => {
            use riftpipe::net::Link;
            let room = args.get(2).cloned().unwrap_or_default();
            let signal = flag_value(&args, "--signal").unwrap_or_else(|| "ws://127.0.0.1:9000".to_string());
            let send = flag_value(&args, "--send").unwrap_or_else(|| "hello-from-native".to_string());
            match riftpipe::net::webrtc::connect_via_signaling(&signal, &room).await {
                Ok(mut link) => {
                    let _ = link.send(send.into_bytes()).await;
                    match link.recv().await {
                        Ok(Some(b)) => println!("GOT:{}", String::from_utf8_lossy(&b)),
                        _ => eprintln!("webrtc-echo: no message"),
                    }
                    // Stay open briefly so our sent message reaches the peer before
                    // we close (otherwise the faster side races the slower's recv).
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Err(e) => eprintln!("webrtc-echo: connect failed: {e}"),
            }
        }
        _ => {
            eprintln!("riftpipe — collaborative pipe");
            eprintln!("usage:");
            eprintln!("  riftpipe text                # eg-walker convergence demo (offline)");
            eprintln!("  riftpipe share <file|dir> [--pipe] [--memory] [--metrics <path>] [--manifest <path>] [--process <path>]");
            eprintln!("  riftpipe join <ticket> <file|dir> [--pipe] [--memory] [--metrics <path>] [--manifest <path>] [--process <path>]");
            eprintln!("  riftpipe connect <ticket|browser-link|connection-id> <dir> [--signal ws://127.0.0.1:9000] [--metrics <path>]");
            eprintln!("                                      # tree-sync a dir with a peer. a mesh ticket (from `connect --accept`, or a");
            eprintln!("                                      # browser share link's hex ticket after '#') joins that folder's iroh-gossip");
            eprintln!("                                      # mesh natively — same protocol browsers speak, no signaling server. anything");
            eprintln!("                                      # else is a signaling connection-id — WebRTC via --signal (byte totals on exit)");
            eprintln!("                                      # --restricted [--name <text>]: joins a restricted board and asks for access");
            eprintln!("                                      # automatically — a signed request carrying <text> + a safety code to compare out-of-band");
            eprintln!("  riftpipe connect --accept <dir> [--new-board] [--restricted [--grant <device-id>]...]");
            eprintln!("                                      # host a gossip-mesh swarm for <dir>: prints a mesh ticket a browser OR CLI");
            eprintln!("                                      # peer joins with `connect <ticket> <dir>` — one protocol, N peers, no signaling.");
            eprintln!("                                      # <dir> is the same board across restarts (identity persists in <dir>/.riftpipe-host)");
            eprintln!("                                      # --new-board: forget <dir>/.riftpipe-host and mint a new board/link");
            eprintln!("                                      #   a --restricted host also resumes its trust log + epochs (<dir>/.riftpipe-sync/<board>/auth.state)");
            eprintln!("                                      # --restricted: only devices the founder grants can READ or write — content is sealed under a rotating epoch key");
            eprintln!("                                      # --grant <id> authorizes a device at startup (get one via `device-id <dir>`)");
            eprintln!("  riftpipe device-id <dir>            # print (creating if absent) this device's id for <dir> — share it to be --grant'ed");
            eprintln!("  riftpipe revoke <dir> <device-id>    # remove a device from a running restricted board (rotates the read key)");
            eprintln!("  riftpipe grant <dir> <device-id>     # authorize a device on a running restricted board");
            eprintln!("  riftpipe requests <dir> [--json]     # list a running restricted board's pending join requests");
            eprintln!("  riftpipe approve <dir> <device-id|safety-code>  # grant a pending join request, by id or safety code");
            eprintln!("  riftpipe members <dir> [--json]      # list a running restricted board's authorized devices");
            eprintln!("  riftpipe serve <dir> [--port 8080]  # static-host a folder + SSE change events at /events");
            eprintln!("                                      # (share/join a folder + serve = live-updating static site)");
            eprintln!("  riftpipe signal [--port 9000]       # WebRTC signaling relay for browser peers");
            eprintln!("  riftpipe webrtc-echo <connection-id> [--signal ws://…] [--send <msg>]  # cross-stack bridge probe");
            eprintln!("\nedit the file in your $EDITOR; changes converge across peers.");
            eprintln!("a DIR syncs the whole folder: each file gets the algorithm riftpipe.toml assigns");
            eprintln!("(text-crdt / rsync-file / wal-db; image planned). See DESIGN.md §17.");
            eprintln!("--pipe     speak the editor edit-stream protocol on stdin/stdout (for bridges)");
            eprintln!("--memory   hold resources in RAM (no disk mirror); see them in the --process file.");
            eprintln!("           a rule's `backing = \"memory\"|\"file\"` in riftpipe.toml overrides this per glob");
            eprintln!("--manifest path to riftpipe.toml (default: <dir>/riftpipe.toml)");
            eprintln!("--process  write size+hash of all in-memory resources to <path> (default: process)");
            eprintln!("--metrics  write a one-line status to <path> for tmux to display");
            eprintln!("try ./run-local.sh for a two-peer tmux demo.");
        }
    }
}

/// Serve a file for live collaboration: go online (so the ticket is dialable from
/// anywhere via relay), print a secret-bearing ticket (also written to
/// `<file>.ticket` for scripts), accept + authenticate one peer, then run.
async fn share(file: &str, opts: &Opts) -> riftpipe::net::Result<()> {
    use std::time::Duration;
    let endpoint = bind_accept().await?;
    // Best-effort: get a relay home so peers on other networks can reach us.
    // Falls back to direct addresses (LAN/loopback) if there's no internet.
    let _ = tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await;
    let addr = local_addr(&endpoint).await;

    let ticket = Ticket::new(addr);
    let secret = ticket.secret;
    let encoded = ticket.encode();
    let _ = std::fs::write(format!("{file}.ticket"), &encoded); // sidecar for scripts
    eprintln!("share this ticket with a peer:\n\n{encoded}\n");
    eprintln!("waiting for a peer to join...");

    // A directory -> folder mode: multiplex every resource over one link, each on
    // the algorithm the manifest assigns (DESIGN.md §17). Reconnects on drop.
    if std::path::Path::new(file).is_dir() {
        let counters = Arc::new(Counters::default());
        let ws = Workspace::new(file, load_manifest(opts, file)?, opts.memory).map_err(anyerr)?;
        let m = opts.metrics.clone().map(|p| (p, basename(file).to_string()));
        eprintln!("authenticated — live syncing folder {file} (end-to-end encrypted). ^C to stop.");
        return run_folder_reconnecting(
            endpoint, Role::Accept, secret, counters, ws, m, process_path(opts),
        )
        .await;
    }

    // --pipe gets reconnection (persistent doc, re-dials on drop); file-mirror is
    // single-shot.
    if opts.pipe {
        let counters = Arc::new(Counters::default());
        let m = opts.metrics.clone().map(|p| (p, basename(file).to_string()));
        return run_pipe_reconnecting(endpoint, Role::Accept, secret, counters, m).await;
    }
    let mut link = accept_link(&endpoint).await?;
    authenticate(&mut link, &secret).await?;
    let peer = link.remote_id();
    // `nl` (and its iroh keepalive on a WebRTC upgrade) lives for the whole loop.
    let mut nl = negotiate_link(link).await;
    if let Some(path) = opts.metrics.clone() {
        riftpipe::monitor::metrics::spawn(endpoint.clone(), peer, nl.counters.clone(), path, basename(file).into());
    }
    eprintln!("authenticated — live syncing {file} ({:?}, end-to-end encrypted). ^C to stop.", nl.transport);
    live_file_loop(file, &mut *nl.link).await
}

/// Join a shared file/dir via its ticket: dial, authenticate with the secret, run.
async fn join(ticket: &str, file: &str, opts: &Opts) -> riftpipe::net::Result<()> {
    let ticket = Ticket::decode(ticket)?;
    let endpoint = bind_connect().await?;

    // A directory -> folder mode (see `share`). The joiner discovers the
    // sharer's files as their frames arrive.
    if std::path::Path::new(file).is_dir() {
        let counters = Arc::new(Counters::default());
        let ws = Workspace::new(file, load_manifest(opts, file)?, opts.memory).map_err(anyerr)?;
        let m = opts.metrics.clone().map(|p| (p, basename(file).to_string()));
        return run_folder_reconnecting(
            endpoint, Role::Connect(ticket.addr), ticket.secret, counters, ws, m, process_path(opts),
        )
        .await;
    }

    if opts.pipe {
        let counters = Arc::new(Counters::default());
        let m = opts.metrics.clone().map(|p| (p, basename(file).to_string()));
        return run_pipe_reconnecting(endpoint, Role::Connect(ticket.addr), ticket.secret, counters, m).await;
    }
    let mut link = connect_link(&endpoint, ticket.addr).await?;
    authenticate(&mut link, &ticket.secret).await?;
    let peer = link.remote_id();
    // `nl` (and its iroh keepalive on a WebRTC upgrade) lives for the whole loop.
    let mut nl = negotiate_link(link).await;
    if let Some(path) = opts.metrics.clone() {
        riftpipe::monitor::metrics::spawn(endpoint.clone(), peer, nl.counters.clone(), path, basename(file).into());
    }
    eprintln!("authenticated — live syncing {file} ({:?}, end-to-end encrypted). ^C to stop.", nl.transport);
    live_file_loop(file, &mut *nl.link).await
}

/// `riftpipe connect <target> <dir>`: tree-sync `dir` with a peer. `<target>`
/// resolves in order:
///
/// 1. a browser share **link** (anything with `#`) → the fragment after the
///    LAST `#` is the ticket, resolved by the rules below;
/// 2. a mesh ticket — hex(postcard(EndpointAddr)), what `connect --accept` prints
///    and a browser peer's share link carries → join its iroh-gossip mesh (same
///    topic + wire frames browsers and CLI hosts speak; no signaling server);
/// 3. anything else → a signaling connection-id (WebRTC via `--signal`).
async fn connect_dial(target: &str, dir: &str, opts: &Opts) -> riftpipe::net::Result<()> {
    // A share link carries the ticket in its URL fragment (after the LAST '#').
    let target = target.rsplit('#').next().unwrap_or(target);
    if let Some((secret, addr)) = decode_mesh_ticket(target) {
        // Browser mesh path: join the board's sealed gossip swarm as a native
        // peer. The secret from the ticket derives the topic + frame cipher.
        // (No --metrics here yet: the metrics side-car reports a single peer's
        // connection_kind, which doesn't fit a swarm — phase 2.)
        let dir = riftpipe::sync::tree::prepare_dir(std::path::Path::new(dir))?;
        let (watcher, mut rx) = riftpipe::sync::tree::watch(&dir);
        let mut peer = riftpipe::sync::tree::TreePeer::new();
        let board = riftpipe_core::crypto::topic_from_secret(&secret);
        apply_auth(&mut peer, &dir, opts, AuthRole::Joiner, board)?;
        eprintln!("[riftpipe] joining sealed board (host {}); syncing {}", addr.id, dir.display());
        return riftpipe::sync::mesh::join_and_run(secret, addr, &dir, &mut peer, &mut rx, watcher.is_none()).await;
    }
    // Connection-id path (WebRTC via signaling). There's no iroh Endpoint here,
    // so the tmux metrics side-car (which needs one for `connection_kind`)
    // can't run — the session's byte totals are reported when it ends instead.
    let signal = opts
        .signal
        .clone()
        .unwrap_or_else(|| "ws://127.0.0.1:9000".to_string());
    let counters = Arc::new(Counters::default());
    let res = riftpipe::sync::tree::connect(&signal, target, dir, counters.clone()).await;
    eprintln!(
        "[riftpipe] session ended — ↑{}B ↓{}B",
        counters.sent.load(std::sync::atomic::Ordering::Relaxed),
        counters.recv.load(std::sync::atomic::Ordering::Relaxed),
    );
    res
}

/// Load this device's persisted signing identity from `<dir>/.device` (a hex
/// seed), or mint + store one. `.device` is a dot-file, so it stays machine-local
/// and never syncs (the `hidden()` rule). The seed is secret — guard the dir.
fn load_or_create_device(dir: &std::path::Path) -> riftpipe_core::identity::DeviceKey {
    use riftpipe_core::identity::DeviceKey;
    let path = dir.join(".device");
    if let Ok(hex) = std::fs::read_to_string(&path) {
        if let Some(seed) = hex_to_32(hex.trim()) {
            return DeviceKey::from_seed(&seed);
        }
    }
    let key = DeviceKey::generate();
    let hex: String = key.seed().iter().map(|b| format!("{b:02x}")).collect();
    let _ = std::fs::write(&path, hex);
    key
}

/// Parse 64 hex chars into a 32-byte array (a device id or a seed).
fn hex_to_32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Which side configures auth: the board's host (founder) or a joiner.
#[derive(Clone, Copy)]
enum AuthRole {
    /// `connect --accept`; `resuming` = an existing `<dir>/.riftpipe-host` is resumed.
    Host { resuming: bool },
    /// `connect <ticket> <dir>`.
    Joiner,
}

/// Configure device-linking auth on a mesh `TreePeer` bound to `board` when
/// `--restricted` is set: load/persist this device's identity, turn on signing +
/// enforcement, and restore the board's persisted trust log + epoch announces
/// (`<dir>/.riftpipe-sync/<board>/auth.state`). A host then checks it may resume
/// (`mesh::check_restricted_resume`), self-signs the genesis, and grants each
/// `--grant <hex>` device id as a Member unless the log already knows it. Prints
/// this device's id (share it to be granted). No-op otherwise.
fn apply_auth(
    peer: &mut riftpipe::sync::tree::TreePeer,
    dir: &std::path::Path,
    opts: &Opts,
    role: AuthRole,
    board: [u8; 32],
) -> riftpipe::net::Result<()> {
    use riftpipe::sync::tree::{AuthRestore, StartupGrant};
    if !opts.restricted {
        if opts.name.is_some() {
            eprintln!("[auth] --name only applies with --restricted (ignored)");
        }
        return Ok(());
    }
    let key = load_or_create_device(dir);
    let id_hex: String = key.id().iter().map(|b| format!("{b:02x}")).collect();
    let genesis = key.genesis();
    peer.set_identity(key, true);
    peer.set_board(board);
    let restore = peer.restore_auth(dir);
    match (&restore, role) {
        (AuthRestore::Restored { trust, epochs }, _) => {
            eprintln!("[auth] restored {trust} trust entries and {epochs} epoch announces for this board")
        }
        (AuthRestore::Rejected(why), AuthRole::Joiner) => {
            eprintln!("[auth] ignoring the persisted trust log/epochs ({why}) — relearning them from the board")
        }
        _ => {}
    }
    if let AuthRole::Host { resuming } = role {
        riftpipe::sync::mesh::check_restricted_resume(dir, resuming, &restore, peer.founder(), peer.device_id())?;
        peer.add_trust(genesis);
        for g in &opts.grant {
            let Some(gid) = hex_to_32(g) else {
                eprintln!("[auth] ignoring malformed --grant {g}");
                continue;
            };
            match peer.grant_at_startup(gid) {
                StartupGrant::Granted => eprintln!("[auth] granted device {g} (Member)"),
                StartupGrant::AlreadyAuthorized => {
                    eprintln!("[auth] --grant {g} skipped: already authorized on this board")
                }
                StartupGrant::Revoked => eprintln!(
                    "[auth] --grant {g} skipped: this device was revoked on this board — re-authorize it on the running board with: riftpipe grant {} {g}",
                    dir.display()
                ),
                StartupGrant::NoIdentity => {}
            }
        }
    }
    if let Some(n) = &opts.name {
        peer.set_request_name(n);
    }
    eprintln!("[auth] restricted mode — this device id: {id_hex}");
    if matches!(role, AuthRole::Joiner) {
        if let Some(req) = peer.my_request() {
            let name_json = serde_json::to_string(&req.name).unwrap_or_default();
            let safety = req.safety_code();
            let safety_nospace = safety.replace(' ', "");
            eprintln!(
                "[auth] join request name={name_json} safety={safety} — the board owner approves it with: riftpipe approve <their-dir> {safety_nospace}"
            );
        }
    }
    Ok(())
}

/// Encode a mesh ticket: `hex(postcard((secret, addr)))` — the exact encoding of
/// `web/src/iroh_link.rs::ticket_of` and the inverse of [`decode_mesh_ticket`],
/// so a browser or CLI peer joins our board from the printed string. The 32-byte
/// **secret** both names the topic (`H(secret)`) and keys the frame cipher, so
/// possession of the ticket is what grants access (see `SECURITY.md`).
fn encode_mesh_ticket(secret: &[u8; 32], addr: &iroh::EndpointAddr) -> String {
    postcard::to_allocvec(&(secret, addr))
        .unwrap_or_default()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Decode a mesh ticket into `(secret, addr)` — the inverse of
/// [`encode_mesh_ticket`], matching `web/src/iroh_link.rs`. `None` if `s` isn't
/// a valid such ticket (so the caller falls through to signaling).
fn decode_mesh_ticket(s: &str) -> Option<([u8; 32], iroh::EndpointAddr)> {
    if s.is_empty() || s.len() % 2 != 0 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let bytes: Vec<u8> = (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect::<Option<_>>()?;
    postcard::from_bytes(&bytes).ok()
}

/// `riftpipe connect --accept <dir>`: the hosting side of tree sync — it
/// **hosts a gossip-mesh swarm** for `dir` and prints a mesh ticket
/// (sidecar in `<dir>.ticket`). This is the exact protocol a browser host
/// speaks, so browsers and CLI peers join with the same `connect <ticket>` —
/// one mesh, no signaling, no native-only transport. Edits on any peer
/// converge. `_opts` (e.g. `--metrics`) is unused: the metrics side-car reports
/// a single link's `connection_kind`, which a swarm doesn't have. The same
/// `dir` is the same board across restarts (identity + board secret persist
/// in `<dir>/.riftpipe-host`); `--new-board` mints a new one. A `--restricted`
/// host also resumes its trust log + epochs from
/// `<dir>/.riftpipe-sync/<board>/auth.state`.
async fn connect_accept(dir: &str, opts: &Opts) -> riftpipe::net::Result<()> {
    use riftpipe::sync::{mesh, tree};
    use std::time::Duration;

    let dir = tree::prepare_dir(std::path::Path::new(dir))?;
    // Host role: load (or mint) a persisted identity + board secret, so a
    // restart resumes the SAME board (same topic, same EndpointId) instead of
    // a different one.
    let resuming = mesh::host_resumes(&dir, opts.new_board);
    let (sk, secret) = mesh::load_or_create_host(&dir, opts.new_board)?;
    let board = riftpipe_core::crypto::topic_from_secret(&secret);
    let mut peer = tree::TreePeer::new();
    apply_auth(&mut peer, &dir, opts, AuthRole::Host { resuming }, board)?; // founder: genesis + any --grant ids
    let (watcher, mut rx) = tree::watch(&dir);

    // Go online (best-effort relay home) so the ticket dials cross-network.
    let peers = mesh::load_peer_book(&dir, board);
    let conn =
        mesh::MeshConn::join_with(mesh::JoinOpts { secret, bootstrap: None, secret_key: Some(sk), peers }).await?;
    let _ = tokio::time::timeout(Duration::from_secs(5), conn.endpoint().online()).await;
    let addr = local_addr(conn.endpoint()).await;
    let ticket = encode_mesh_ticket(&secret, &addr);
    let _ = std::fs::write(format!("{}.ticket", dir.display()), &ticket); // sidecar for scripts
    eprintln!("share this ticket — a browser or CLI joins the same sealed board with `connect`:\n\n{ticket}\n");
    eprintln!(
        "[mesh] host id {} — identity + board secret persist in {}/{} (restart = same board; --new-board mints a new link)",
        conn.endpoint().id(),
        dir.display(),
        mesh::HOST_FILE
    );
    eprintln!("hosting {} on the gossip mesh (sealed); waiting for peers...", dir.display());

    mesh::run(conn, &dir, &mut peer, &mut rx, watcher.is_none()).await
}

/// `riftpipe serve`: static-host `dir` (with SPA index.html fallback) plus SSE
/// change events at `/events` — share/join a folder in another process and
/// `serve` it here for a live-updating static site. Blocks.
fn serve_dir(dir: &str, port: u16) -> Result<(), String> {
    use riftpipe::app::host::Host;
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let host = Host::new(dir);
    // Canonicalize so watcher event paths (which come back canonical, e.g.
    // /private/var on macOS) strip cleanly to relative paths.
    let root = std::fs::canonicalize(dir).map_err(|e| e.to_string())?;
    host.watch(root.clone(), move |p| {
        let rel = p.strip_prefix(&root).ok()?.to_string_lossy().replace('\\', "/");
        // Dot-paths are machine-local (same rule as sync) — don't announce them.
        if rel.is_empty() || rel.split('/').any(|c| c.starts_with('.')) {
            return None;
        }
        Some(serde_json::json!({ "type": "change", "path": rel }))
    });
    let server = tiny_http::Server::http(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    eprintln!("[serve] hosting {dir} on http://localhost:{port}  (change events at /events)");
    for request in server.incoming_requests() {
        let host = host.clone();
        // One thread per request: SSE connections block their thread for life.
        std::thread::spawn(move || {
            let path = request.url().split('?').next().unwrap_or("").to_string();
            let r = if path == "/events" {
                host.sse(request)
            } else {
                host.serve_static(request, &path)
            };
            if let Err(e) = r {
                eprintln!("[serve] request error: {e}");
            }
        });
    }
    Ok(())
}

/// Short label for the HUD (file name, not the whole path).
fn basename(p: &str) -> &str {
    std::path::Path::new(p)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(p)
}

/// The shared round loop (file-mirror mode): read the file, run a text-pipe
/// round, write back the merged result. Works over any `Link`.
async fn live_file_loop(file: &str, link: &mut dyn riftpipe::net::Link) -> riftpipe::net::Result<()> {
    use std::time::Duration;
    let mut peer = TextPeer::new(file);
    loop {
        let snapshot = std::fs::read_to_string(file).unwrap_or_default();
        if let Some(merged) = peer.round(link, &snapshot).await? {
            std::fs::write(file, &merged).map_err(riftpipe::net::anyerr)?;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// --------------------------------------------------------------------------
// Offline demos
// --------------------------------------------------------------------------

fn demo_text() {
    let mut a = EgWalkerText::new("alice");
    a.edit_to("hello world");
    let seed = a.encode_full();
    let mut b = EgWalkerText::new("bob");
    let _ = b.merge(&seed);

    let (ba, bb) = (a.version(), b.version());
    a.edit_to("hello brave world");
    b.edit_to("hello world!!!");
    let (da, db) = (a.encode_delta(&ba), b.encode_delta(&bb));
    let _ = a.merge(&db);
    let _ = b.merge(&da);

    println!("riftpipe :: eg-walker text convergence (diamond-types)\n");
    println!("  alice: {:?}", a.content());
    println!("  bob:   {:?}", b.content());
    println!(
        "  converged: {}",
        if a.content() == b.content() { "YES" } else { "NO" }
    );
}

