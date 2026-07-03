//! riftpipe CLI. `text` runs an offline demo of the CRDT core. `share` and `join`
//! do a real one-shot CRDT exchange over iroh; `kanban`, `signal`, and
//! `webrtc-echo` back the browser stack.

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

/// Parsed CLI options for `share`/`join`.
struct Opts {
    pos: Vec<String>,
    pipe: bool,
    memory: bool,
    metrics: Option<String>,
    manifest: Option<String>,
    process: Option<String>,
}

/// Parse `args[2..]` into positionals + flags. `--metrics`/`--manifest`/
/// `--process` each take a value.
fn parse(args: &[String]) -> Opts {
    let mut o = Opts {
        pos: Vec::new(),
        pipe: false,
        memory: false,
        metrics: None,
        manifest: None,
        process: None,
    };
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--pipe" => o.pipe = true,
            "--memory" => o.memory = true,
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
        "kanban" => match args.get(2).map(String::as_str) {
            Some("serve") => {
                let dir = args.get(3).cloned().unwrap_or_else(|| "board".to_string());
                let port = flag_value(&args, "--port").and_then(|v| v.parse().ok()).unwrap_or(7777);
                let dist = flag_value(&args, "--dist")
                    .unwrap_or_else(|| "projects/kanban/dist".to_string());
                // The HTTP server is synchronous (tiny_http); run it off the async
                // runtime so a future folder-sync task can share the process.
                let res = tokio::task::spawn_blocking(move || {
                    riftpipe::app::kanban::serve(&dir, port, &dist).map_err(|e| e.to_string())
                })
                .await;
                if let Ok(Err(e)) = res {
                    eprintln!("[kanban] serve failed: {e}");
                }
            }
            Some("connect") => {
                let connid = args.get(3).cloned().unwrap_or_default();
                let dir = args.get(4).cloned().unwrap_or_else(|| "board".to_string());
                let signal = flag_value(&args, "--signal").unwrap_or_else(|| "ws://127.0.0.1:9000".to_string());
                if connid.is_empty() {
                    eprintln!("usage: riftpipe kanban connect <connection-id> <board-dir> [--signal ws://…]");
                } else if let Err(e) = riftpipe::app::kanban::connect_board(&signal, &connid, &dir).await {
                    eprintln!("[kanban] connect failed: {e}");
                }
            }
            _ => eprintln!("usage: riftpipe kanban serve <board-dir> [--port 7777] [--dist <spa-dir>]\n       riftpipe kanban connect <connection-id> <board-dir> [--signal ws://…]"),
        },
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
            eprintln!("  riftpipe kanban serve <board-dir> [--port 7777] [--dist <spa-dir>]  # serve the kanban UI + JSON file-API");
            eprintln!("\nedit the file in your $EDITOR; changes converge across peers.");
            eprintln!("a DIR syncs the whole folder: each file gets the algorithm riftpipe.toml assigns");
            eprintln!("(text-crdt / rsync-file; wal-db & image planned). See DESIGN.md §17.");
            eprintln!("--pipe     speak the editor edit-stream protocol on stdin/stdout (for bridges)");
            eprintln!("--memory   hold resources in RAM (no disk mirror); see them in the --process file");
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

