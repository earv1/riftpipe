//! riftpipe CLI. `simulate` and `text` run offline demos of the core. `share`
//! and `join` do a real one-shot CRDT exchange over iroh (the live editing loop
//! arrives with the demo app).

use std::sync::Arc;

use riftpipe::crdt::text::EgWalkerText;
use riftpipe::engine::identity::AgentId;
use riftpipe::engine::log::AppendLog;
use riftpipe::engine::op::{Action, Op, OpId};
use riftpipe::engine::rules::TwoPlayerTurns;
use riftpipe::engine::simulation::{Suite, Vector};
use riftpipe::net::secure::{authenticate, Ticket};
use riftpipe::net::transport::{accept_link, bind_accept, bind_connect, connect_link, local_addr};
use riftpipe::net::{anyerr, Counters, CountingLink};
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
        "simulate" => demo_simulation(),
        "text" => demo_text(),
        "td" => demo_td(),
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
        _ => {
            eprintln!("riftpipe — collaborative pipe");
            eprintln!("usage:");
            eprintln!("  riftpipe simulate            # ruled replay engine demo (offline)");
            eprintln!("  riftpipe text                # eg-walker convergence demo (offline)");
            eprintln!("  riftpipe td                  # tower-defense core preview (offline)");
            eprintln!("  riftpipe share <file|dir> [--pipe] [--memory] [--metrics <path>] [--manifest <path>] [--process <path>]");
            eprintln!("  riftpipe join <ticket> <file|dir> [--pipe] [--memory] [--metrics <path>] [--manifest <path>] [--process <path>]");
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
    let (mut counting, counters) = CountingLink::new(link);
    if let Some(path) = opts.metrics.clone() {
        riftpipe::monitor::metrics::spawn(endpoint.clone(), peer, counters, path, basename(file).into());
    }
    eprintln!("authenticated — live syncing {file} (end-to-end encrypted). ^C to stop.");
    live_file_loop(file, &mut counting).await
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
    let (mut counting, counters) = CountingLink::new(link);
    if let Some(path) = opts.metrics.clone() {
        riftpipe::monitor::metrics::spawn(endpoint.clone(), peer, counters, path, basename(file).into());
    }
    eprintln!("authenticated — live syncing {file} (end-to-end encrypted). ^C to stop.");
    live_file_loop(file, &mut counting).await
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

/// Solo scripted preview of the tower-defense sim (no networking) — just to see
/// the board render and confirm the deterministic core feels alive.
fn demo_td() {
    use riftpipe::engine::game::{Action, ActionKind, Player, World};

    let script = [
        Action { tick: 2, seq: 0, player: Player::P1, kind: ActionKind::PlaceTower { tile: 4 } },
        Action { tick: 2, seq: 1, player: Player::P2, kind: ActionKind::PlaceTower { tile: 6 } },
        Action { tick: 6, seq: 2, player: Player::P1, kind: ActionKind::PlaceTower { tile: 8 } },
        Action { tick: 12, seq: 3, player: Player::P2, kind: ActionKind::SendCreep },
        Action { tick: 24, seq: 4, player: Player::P1, kind: ActionKind::SendCreep },
        Action { tick: 24, seq: 5, player: Player::P1, kind: ActionKind::SendCreep },
    ];
    let mut sorted = script.to_vec();
    sorted.sort_by_key(|a| (a.tick, a.seq, a.player as usize));

    let mut w = World::default();
    let mut idx = 0;
    println!("riftpipe :: tower-defense core preview (solo, scripted)\n");
    for frame in 0..=6 {
        let target = frame * 12;
        while w.tick < target {
            while idx < sorted.len() && sorted[idx].tick == w.tick {
                w.apply(&sorted[idx]);
                idx += 1;
            }
            w.step();
        }
        print!("{}", riftpipe::engine::game::render(&w));
        println!();
    }
}

fn demo_text() {
    let mut a = EgWalkerText::new("alice");
    a.edit_to("hello world");
    let seed = a.encode_full();
    let mut b = EgWalkerText::new("bob");
    b.merge(&seed);

    let (ba, bb) = (a.version(), b.version());
    a.edit_to("hello brave world");
    b.edit_to("hello world!!!");
    let (da, db) = (a.encode_delta(&ba), b.encode_delta(&bb));
    a.merge(&db);
    b.merge(&da);

    println!("riftpipe :: eg-walker text convergence (diamond-types)\n");
    println!("  alice: {:?}", a.content());
    println!("  bob:   {:?}", b.content());
    println!(
        "  converged: {}",
        if a.content() == b.content() { "YES" } else { "NO" }
    );
}

fn mv(agent: AgentId, seq: u64, lamport: u64, text: &str) -> Op {
    Op {
        id: OpId { agent, seq },
        lamport,
        parents: vec![],
        action: Action::Append(format!("{text}\n")),
    }
}

fn demo_simulation() {
    let alice = AgentId::from_name("alice");
    let bob = AgentId::from_name("bob");
    let make_rule = || TwoPlayerTurns {
        first: alice,
        second: bob,
    };

    let suite = Suite {
        vectors: vec![
            Vector {
                name: "alternating-turns".into(),
                events: vec![
                    mv(alice, 0, 0, "a-move-1"),
                    mv(bob, 0, 1, "b-move-1"),
                    mv(alice, 1, 2, "a-move-2"),
                ],
                expect_state: "a-move-1\nb-move-1\na-move-2\n".into(),
                expect_rejected: vec![],
            },
            Vector {
                name: "out-of-turn-rejected".into(),
                events: vec![
                    mv(alice, 0, 0, "a"),
                    mv(bob, 0, 1, "b1"),
                    mv(bob, 1, 2, "b2"),
                ],
                expect_state: "a\nb1\n".into(),
                expect_rejected: vec![OpId { agent: bob, seq: 1 }],
            },
        ],
    };

    println!("riftpipe :: deterministic replay + guard + simulation\n");
    for r in suite.run::<AppendLog, _, _>(&make_rule) {
        println!("  [{}] {}", if r.passed { "PASS" } else { "FAIL" }, r.name);
    }
    let h = suite.transcript_hash::<AppendLog, _, _>(&make_rule);
    println!("\n  handshake transcript hash = {h:#018x}");
}
