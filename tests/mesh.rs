//! REAL integration test for the gossip-mesh driver: two native peers on one
//! machine form the same iroh-gossip swarm a browser board uses (peer A in the
//! browser-host role — subscribed to its OWN topic, no bootstrap — peer B
//! joining via A's EndpointAddr, exactly like `riftpipe connect <browser-link>`),
//! and files converge in BOTH directions through `sync::mesh::run`.
//!
//! Mirrors `tests/networking.rs` structure: multi-thread runtime (iroh's actors
//! need real workers), `online()` + a hard BUDGET so a connectivity stall fails
//! loudly rather than hanging.

use std::path::PathBuf;
use std::time::Duration;

use riftpipe::sync::mesh::MeshConn;
use riftpipe::sync::tree::TreePeer;

const BUDGET: Duration = Duration::from_secs(60);

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("riftpipe-mesh-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    std::fs::canonicalize(&d).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_peers_converge_over_gossip_mesh() {
    let da = dir("a");
    let db = dir("b");

    // Peer A = the browser-host role: swarm keyed by its own id, no bootstrap.
    let host = MeshConn::join(None, None).await.expect("host join");
    // Acquire a relay home + direct addresses so the addr is dialable, like the
    // browser's wait_for_addr / networking.rs's online() preamble.
    let _ = tokio::time::timeout(Duration::from_secs(10), host.endpoint().online()).await;
    let host_addr = {
        let mut addr = host.endpoint().addr();
        for _ in 0..100 {
            if !addr.addrs.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            addr = host.endpoint().addr();
        }
        addr
    };
    assert!(!host_addr.addrs.is_empty(), "host never got a dialable address");

    // A has a file before B arrives (NeighborUp catch-up must ship it) …
    std::fs::create_dir_all(da.join("tickets/tk_1")).unwrap();
    std::fs::write(da.join("tickets/tk_1/card.md"), "# from A\n").unwrap();
    let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    tx_a.send(da.join("tickets/tk_1/card.md")).unwrap();

    // … and B has one of its own (its NeighborUp catch-up ships it back).
    std::fs::write(db.join("meta.toml"), "column = \"Todo\"\n").unwrap();
    let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    tx_b.send(db.join("meta.toml")).unwrap();

    let da_task = da.clone();
    let a_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        let _ = riftpipe::sync::mesh::run(host, &da_task, &mut peer, &mut rx_a, false).await;
    });
    let (db_task, addr) = (db.clone(), host_addr.clone());
    let b_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        let _ = riftpipe::sync::mesh::join_and_run(addr, &db_task, &mut peer, &mut rx_b, false).await;
    });

    // Converged when A's file is on B's disk and B's file on A's.
    let (want_b, want_a) = (db.join("tickets/tk_1/card.md"), da.join("meta.toml"));
    tokio::time::timeout(BUDGET, async {
        while !(want_b.is_file() && want_a.is_file()) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("mesh convergence timed out");

    assert_eq!(
        std::fs::read(&want_b).unwrap(),
        b"# from A\n",
        "A's text file arrives on B byte-identical via the CRDT path",
    );
    assert_eq!(
        std::fs::read(&want_a).unwrap(),
        b"column = \"Todo\"\n",
        "B's structural file arrives on A via LWW",
    );

    a_task.abort();
    b_task.abort();
    std::fs::remove_dir_all(&da).ok();
    std::fs::remove_dir_all(&db).ok();
}
