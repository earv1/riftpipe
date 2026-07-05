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
    };
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--pipe" => o.pipe = true,
            "--memory" => o.memory = true,
            "--accept" => o.accept = true,
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
                        eprintln!("usage: riftpipe connect --accept <dir> [--metrics <path>]");
                        return;
                    }
                }
            } else {
                match (opts.pos.first(), opts.pos.get(1)) {
                    (Some(target), Some(dir)) => connect_dial(target, dir, &opts).await,
                    _ => {
                        eprintln!("usage: riftpipe connect <ticket|browser-link|connection-id> <dir> [--signal ws://…] [--metrics <path>]");
                        eprintln!("       riftpipe connect --accept <dir> [--metrics <path>]");
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
            eprintln!("  riftpipe connect --accept <dir>");
            eprintln!("                                      # host a gossip-mesh swarm for <dir>: prints a mesh ticket a browser OR CLI");
            eprintln!("                                      # peer joins with `connect <ticket> <dir>` — one protocol, N peers, no signaling");
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
    if let Some(addr) = decode_mesh_ticket(target) {
        // Browser mesh path: join the folder's gossip swarm as a native peer.
        // (No --metrics here yet: the metrics side-car reports a single peer's
        // connection_kind, which doesn't fit a swarm — phase 2.)
        let dir = riftpipe::sync::tree::prepare_dir(std::path::Path::new(dir))?;
        let (watcher, mut rx) = riftpipe::sync::tree::watch(&dir);
        let mut peer = riftpipe::sync::tree::TreePeer::new();
        eprintln!("[riftpipe] joining browser mesh of {} (iroh-gossip); syncing {}", addr.id, dir.display());
        return riftpipe::sync::mesh::join_and_run(addr, &dir, &mut peer, &mut rx, watcher.is_none()).await;
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

/// Encode our address as a mesh ticket: hex(postcard(EndpointAddr)) — the exact
/// encoding of `web/src/iroh_link.rs::ticket_of` and the inverse of
/// [`decode_mesh_ticket`], so a browser or CLI peer joins our swarm from the
/// printed string (a bare address, no secret).
fn encode_mesh_ticket(addr: &iroh::EndpointAddr) -> String {
    postcard::to_allocvec(addr)
        .unwrap_or_default()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Decode a browser mesh ticket: hex(postcard(EndpointAddr)) — the exact
/// encoding of `web/src/iroh_link.rs::ticket_of` (a bare address, no secret).
/// `None` if `s` isn't that (so the caller falls through to signaling).
fn decode_mesh_ticket(s: &str) -> Option<iroh::EndpointAddr> {
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
/// a single link's `connection_kind`, which a swarm doesn't have.
async fn connect_accept(dir: &str, _opts: &Opts) -> riftpipe::net::Result<()> {
    use riftpipe::sync::{mesh, tree};
    use std::time::Duration;

    let dir = tree::prepare_dir(std::path::Path::new(dir))?;
    let (watcher, mut rx) = tree::watch(&dir);
    let mut peer = tree::TreePeer::new();

    // Host role: bind a fresh endpoint and start the swarm on OUR own topic
    // (`join(None, None)` keys the topic on our EndpointId — same as a browser
    // host). Go online (best-effort relay home) so the ticket dials cross-network.
    let conn = mesh::MeshConn::join(None, None).await?;
    let _ = tokio::time::timeout(Duration::from_secs(5), conn.endpoint().online()).await;
    let addr = local_addr(conn.endpoint()).await;
    let ticket = encode_mesh_ticket(&addr);
    let _ = std::fs::write(format!("{}.ticket", dir.display()), &ticket); // sidecar for scripts
    eprintln!("share this ticket — a browser or CLI joins the same mesh with `connect`:\n\n{ticket}\n");
    eprintln!("hosting {} on the gossip mesh; waiting for peers...", dir.display());

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

