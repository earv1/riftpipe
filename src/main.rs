//! autoshare CLI. `simulate` and `text` run offline demos of the core. `share`
//! and `join` do a real one-shot CRDT exchange over iroh (the live editing loop
//! arrives with the demo app).

use autoshare::engine::identity::AgentId;
use autoshare::engine::log::AppendLog;
use autoshare::net::CountingLink;
use autoshare::engine::op::{Action, Op, OpId};
use autoshare::sync::pipe::run_pipe;
use autoshare::engine::rules::TwoPlayerTurns;
use autoshare::net::secure::{authenticate, Ticket};
use autoshare::engine::simulation::{Suite, Vector};
use autoshare::crdt::text::EgWalkerText;
use autoshare::sync::mirror::TextPeer;
use autoshare::net::transport::{accept_link, bind_accept, bind_connect, connect_link, local_addr, IrohLink};
use iroh::{Endpoint, EndpointId};

/// Parse `args[2..]` into positionals + flags. `--metrics <path>` takes a value.
fn parse(args: &[String]) -> (Vec<String>, bool, Option<String>) {
    let (mut pos, mut pipe, mut metrics) = (Vec::new(), false, None);
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--pipe" => pipe = true,
            "--metrics" => {
                metrics = args.get(i + 1).cloned();
                i += 1;
            }
            s if s.starts_with("--") => {}
            s => pos.push(s.to_string()),
        }
        i += 1;
    }
    (pos, pipe, metrics)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str).unwrap_or("help") {
        "simulate" => demo_simulation(),
        "text" => demo_text(),
        "td" => demo_td(),
        "share" => {
            let (pos, pipe, metrics) = parse(&args);
            match pos.first() {
                Some(file) => {
                    if let Err(e) = share(file, pipe, metrics).await {
                        eprintln!("[autoshare] share failed: {e}");
                    }
                }
                None => eprintln!("usage: autoshare share <file> [--pipe] [--metrics <path>]"),
            }
        }
        "join" => {
            let (pos, pipe, metrics) = parse(&args);
            match (pos.first(), pos.get(1)) {
                (Some(ticket), Some(file)) => {
                    if let Err(e) = join(ticket, file, pipe, metrics).await {
                        eprintln!("[autoshare] join failed: {e}");
                    }
                }
                _ => eprintln!("usage: autoshare join <ticket> <file> [--pipe] [--metrics <path>]"),
            }
        }
        _ => {
            eprintln!("autoshare — collaborative pipe");
            eprintln!("usage:");
            eprintln!("  autoshare simulate            # ruled replay engine demo (offline)");
            eprintln!("  autoshare text                # eg-walker convergence demo (offline)");
            eprintln!("  autoshare td                  # tower-defense core preview (offline)");
            eprintln!("  autoshare share <file> [--pipe] [--metrics <path>]");
            eprintln!("  autoshare join <ticket> <file> [--pipe] [--metrics <path>]");
            eprintln!("\nedit the file in your $EDITOR; changes converge across peers.");
            eprintln!("--pipe    speak the editor edit-stream protocol on stdin/stdout (for bridges)");
            eprintln!("--metrics write a one-line status to <path> for tmux to display");
            eprintln!("try ./run-local.sh for a two-peer tmux demo.");
        }
    }
}

/// Serve a file for live collaboration: go online (so the ticket is dialable from
/// anywhere via relay), print a secret-bearing ticket (also written to
/// `<file>.ticket` for scripts), accept + authenticate one peer, then run.
async fn share(file: &str, pipe: bool, metrics: Option<String>) -> autoshare::net::Result<()> {
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

    let mut link = accept_link(&endpoint).await?;
    authenticate(&mut link, &secret).await?;
    let peer = link.remote_id();
    run_frontend(link, &endpoint, peer, file, pipe, metrics).await
}

/// Join a shared file via its ticket: dial, authenticate with the secret, run.
async fn join(ticket: &str, file: &str, pipe: bool, metrics: Option<String>) -> autoshare::net::Result<()> {
    let ticket = Ticket::decode(ticket)?;
    let endpoint = bind_connect().await?;
    let mut link = connect_link(&endpoint, ticket.addr).await?;
    authenticate(&mut link, &ticket.secret).await?;
    let peer = link.remote_id();
    run_frontend(link, &endpoint, peer, file, pipe, metrics).await
}

/// Pick the frontend. `--pipe` is event-driven (split the link into send/recv
/// halves, no lockstep); the default file-mirror loop polls the file.
async fn run_frontend(
    link: IrohLink,
    endpoint: &Endpoint,
    peer: EndpointId,
    file: &str,
    pipe: bool,
    metrics: Option<String>,
) -> autoshare::net::Result<()> {
    if pipe {
        let counters = std::sync::Arc::new(autoshare::net::Counters::default());
        let (sink, source) = link.into_halves(counters.clone());
        if let Some(path) = metrics {
            autoshare::monitor::metrics::spawn(endpoint.clone(), peer, counters, path, basename(file).into());
        }
        run_pipe(sink, source).await
    } else {
        let (mut counting, counters) = CountingLink::new(link);
        if let Some(path) = metrics {
            autoshare::monitor::metrics::spawn(endpoint.clone(), peer, counters, path, basename(file).into());
        }
        eprintln!("authenticated — live syncing {file} (end-to-end encrypted). ^C to stop.");
        live_file_loop(file, &mut counting).await
    }
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
async fn live_file_loop(file: &str, link: &mut dyn autoshare::net::Link) -> autoshare::net::Result<()> {
    use std::time::Duration;
    let mut peer = TextPeer::new(file);
    loop {
        let snapshot = std::fs::read_to_string(file).unwrap_or_default();
        if let Some(merged) = peer.round(link, &snapshot).await? {
            std::fs::write(file, &merged).map_err(autoshare::net::anyerr)?;
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
    use autoshare::engine::game::{Action, ActionKind, Player, World};

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
    println!("autoshare :: tower-defense core preview (solo, scripted)\n");
    for frame in 0..=6 {
        let target = frame * 12;
        while w.tick < target {
            while idx < sorted.len() && sorted[idx].tick == w.tick {
                w.apply(&sorted[idx]);
                idx += 1;
            }
            w.step();
        }
        print!("{}", autoshare::engine::game::render(&w));
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

    println!("autoshare :: eg-walker text convergence (diamond-types)\n");
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

    println!("autoshare :: deterministic replay + guard + simulation\n");
    for r in suite.run::<AppendLog, _, _>(&make_rule) {
        println!("  [{}] {}", if r.passed { "PASS" } else { "FAIL" }, r.name);
    }
    let h = suite.transcript_hash::<AppendLog, _, _>(&make_rule);
    println!("\n  handshake transcript hash = {h:#018x}");
}
