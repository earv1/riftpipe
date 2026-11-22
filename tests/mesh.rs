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

    // A shared board secret: it derives the topic (H(secret)) and the frame
    // cipher, so both peers must hold the same one — exactly what the share
    // ticket carries. Peer A = host role (no bootstrap); B joins with the same
    // secret + A's address.
    let secret = riftpipe_core::crypto::generate_secret();
    let host = MeshConn::join(secret, None).await.expect("host join");
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
        let _ = riftpipe::sync::mesh::join_and_run(secret, addr, &db_task, &mut peer, &mut rx_b, false).await;
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

/// Wait for a mesh host to acquire a dialable (relay) address.
async fn dialable_addr(conn: &MeshConn) -> iroh::EndpointAddr {
    let _ = tokio::time::timeout(Duration::from_secs(10), conn.endpoint().online()).await;
    let mut addr = conn.endpoint().addr();
    for _ in 0..100 {
        if !addr.addrs.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        addr = conn.endpoint().addr();
    }
    assert!(!addr.addrs.is_empty(), "host never got a dialable address");
    addr
}

/// Feed one file path to a peer's watcher channel so it's pushed on connect.
fn push_file(dir: &std::path::Path, rel: &str, bytes: &[u8]) -> (
    tokio::sync::mpsc::UnboundedSender<PathBuf>,
    tokio::sync::mpsc::UnboundedReceiver<PathBuf>,
) {
    std::fs::write(dir.join(rel), bytes).unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    tx.send(dir.join(rel)).unwrap();
    (tx, rx)
}

/// Device-linking auth END-TO-END over the real gossip mesh: a founder hosts a
/// **restricted** board and grants an authorized device — that device's signed
/// write is accepted and lands on the founder's disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restricted_mesh_accepts_an_authorized_writer() {
    use riftpipe_core::identity::{DeviceKey, Role};

    let df = dir("ra-founder");
    let db = dir("ra-member");
    let secret = riftpipe_core::crypto::generate_secret();

    let host = MeshConn::join(secret, None).await.expect("host join");
    let host_addr = dialable_addr(&host).await;

    // The member's device id (same seed reconstructs the same key on both ends).
    let member_id = DeviceKey::from_seed(&[42u8; 32]).id();

    // Member writes a file that — because it's authorized — must reach the founder.
    let (_tx_b, mut rx_b) = push_file(&db, "member.bin", b"authorized write");
    // The founder has no local file to push — an idle watcher channel.
    let (_tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();

    let df_task = df.clone();
    let a_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        let founder = DeviceKey::from_seed(&[41u8; 32]);
        peer.set_identity(DeviceKey::from_seed(&[41u8; 32]), true); // restricted
        peer.add_trust(founder.genesis()); // founder = Admin
        peer.add_trust(founder.grant(member_id, Role::Member, [1u8; 16])); // grant the member
        let _ = riftpipe::sync::mesh::run(host, &df_task, &mut peer, &mut rx_a, false).await;
    });
    let (db_task, addr) = (db.clone(), host_addr.clone());
    let b_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        peer.set_identity(DeviceKey::from_seed(&[42u8; 32]), true); // the granted member
        let _ = riftpipe::sync::mesh::join_and_run(secret, addr, &db_task, &mut peer, &mut rx_b, false).await;
    });

    let want = df.join("member.bin");
    tokio::time::timeout(BUDGET, async {
        while !want.is_file() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("an authorized member's signed write should reach the founder");
    assert_eq!(std::fs::read(&want).unwrap(), b"authorized write");

    a_task.abort();
    b_task.abort();
    std::fs::remove_dir_all(&df).ok();
    std::fs::remove_dir_all(&db).ok();
}

/// The security proof, END-TO-END over the real mesh: on a restricted board an
/// UNGRANTED device is connected (it learns the founder's epoch 1 from the
/// catch-up) but is never wrapped in — so it can't READ the founder's sealed
/// file, and its own writes never land on the founder (it holds no key to seal
/// them, and the founder would reject them as unauthorized anyway).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restricted_mesh_rejects_an_unauthorized_writer() {
    use riftpipe_core::identity::DeviceKey;

    let df = dir("rr-founder");
    let dc = dir("rr-outsider");
    let secret = riftpipe_core::crypto::generate_secret();

    let host = MeshConn::join(secret, None).await.expect("host join");
    let host_addr = dialable_addr(&host).await;

    // Founder writes a file that must NOT reach the outsider (read is gated).
    let (_tx_a, mut rx_a) = push_file(&df, "from_founder.bin", b"members only");
    // Outsider writes a file that must never reach the founder.
    let (_tx_c, mut rx_c) = push_file(&dc, "from_outsider.bin", b"should be dropped");

    let df_task = df.clone();
    let a_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        let founder = DeviceKey::from_seed(&[51u8; 32]);
        peer.set_identity(DeviceKey::from_seed(&[51u8; 32]), true); // restricted
        peer.add_trust(founder.genesis()); // founder = Admin; the outsider is NOT granted
        let _ = riftpipe::sync::mesh::run(host, &df_task, &mut peer, &mut rx_a, false).await;
    });
    let (dc_task, addr) = (dc.clone(), host_addr.clone());
    let (stop_c, stop_c_rx) = tokio::sync::oneshot::channel::<()>();
    let c_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        peer.set_identity(DeviceKey::from_seed(&[52u8; 32]), true); // signs, but ungranted
        tokio::select! {
            _ = riftpipe::sync::mesh::join_and_run(secret, addr, &dc_task, &mut peer, &mut rx_c, false) => {}
            _ = stop_c_rx => {}
        }
        peer
    });

    // Ample time to connect, catch up, and (wrongly) exchange content.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let _ = stop_c.send(());
    let c_peer = c_task.await.unwrap();

    assert_eq!(c_peer.epoch_known(), Some(1), "the outsider is connected: it learned the founder's epoch");
    assert_eq!(c_peer.epoch_held(), None, "an ungranted device is never wrapped into the epoch");
    assert!(
        !dc.join("from_founder.bin").exists(),
        "an ungranted device must not READ the restricted founder's content",
    );
    assert!(
        !df.join("from_outsider.bin").exists(),
        "an unauthorized device's write must never land on the restricted founder",
    );

    a_task.abort();
    std::fs::remove_dir_all(&df).ok();
    std::fs::remove_dir_all(&dc).ok();
}

fn hex(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

async fn wait_for(paths: &[PathBuf], what: &str) {
    tokio::time::timeout(BUDGET, async {
        while !paths.iter().all(|p| p.is_file()) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out: {what}"));
}

/// READ REVOCATION end-to-end over the real mesh: founder grants B and C, both are
/// wrapped into epoch 1 and read the founder's file. The founder revokes C on the
/// LIVE board (the same control file `riftpipe revoke` writes) → rotation to epoch 2
/// wrapped to {founder, B}. New content from the founder and from B reaches the other;
/// C — still connected, still holding epoch 1 — decrypts none of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restricted_mesh_revoked_device_loses_read_after_rotation() {
    use riftpipe_core::identity::{DeviceKey, Role};
    let (df, db, dc) = (dir("rv-founder"), dir("rv-member"), dir("rv-revoked"));
    let secret = riftpipe_core::crypto::generate_secret();
    let host = MeshConn::join(secret, None).await.expect("host join");
    let host_addr = dialable_addr(&host).await;
    let b_id = DeviceKey::from_seed(&[62u8; 32]).id();
    let c_id = DeviceKey::from_seed(&[63u8; 32]).id();

    let (tx_a, mut rx_a) = push_file(&df, "before.bin", b"epoch one");
    let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let (_tx_c, mut rx_c) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();

    let df_task = df.clone();
    let a_task = tokio::spawn(async move {
        let founder = DeviceKey::from_seed(&[61u8; 32]);
        let mut peer = TreePeer::new();
        peer.set_identity(DeviceKey::from_seed(&[61u8; 32]), true);
        peer.add_trust(founder.genesis());
        peer.add_trust(founder.grant(b_id, Role::Member, [1u8; 16]));
        peer.add_trust(founder.grant(c_id, Role::Member, [2u8; 16]));
        let _ = riftpipe::sync::mesh::run(host, &df_task, &mut peer, &mut rx_a, false).await;
    });
    let (db_task, addr_b) = (db.clone(), host_addr.clone());
    let b_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        peer.set_identity(DeviceKey::from_seed(&[62u8; 32]), true);
        let _ = riftpipe::sync::mesh::join_and_run(secret, addr_b, &db_task, &mut peer, &mut rx_b, false).await;
    });
    let (stop_c, stop_c_rx) = tokio::sync::oneshot::channel::<()>();
    let (dc_task, addr_c) = (dc.clone(), host_addr.clone());
    let c_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        peer.set_identity(DeviceKey::from_seed(&[63u8; 32]), true);
        tokio::select! {
            _ = riftpipe::sync::mesh::join_and_run(secret, addr_c, &dc_task, &mut peer, &mut rx_c, false) => {}
            _ = stop_c_rx => {}
        }
        peer
    });

    wait_for(&[db.join("before.bin"), dc.join("before.bin")], "B and C read epoch-1 content").await;

    riftpipe::sync::control::append(&df, "revoke", &hex(&c_id)).unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    std::fs::write(df.join("after.bin"), b"epoch two").unwrap();
    tx_a.send(df.join("after.bin")).unwrap();
    std::fs::write(db.join("after_b.bin"), b"member, epoch two").unwrap();
    tx_b.send(db.join("after_b.bin")).unwrap();
    wait_for(&[db.join("after.bin"), df.join("after_b.bin")], "remaining members keep syncing").await;

    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(!dc.join("after.bin").exists(), "revoked C must not decrypt the founder's post-rotation write");
    assert!(!dc.join("after_b.bin").exists(), "revoked C must not decrypt B's post-rotation write");

    let _ = stop_c.send(());
    let c_peer = c_task.await.unwrap();
    assert_eq!(c_peer.epoch_held(), Some(1), "C kept only the epoch it was wrapped into");
    assert_eq!(c_peer.epoch_known(), Some(2), "C saw the rotation but holds no key for it");

    a_task.abort();
    b_task.abort();
    for d in [&df, &db, &dc] {
        std::fs::remove_dir_all(d).ok();
    }
}
