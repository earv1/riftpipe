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

use riftpipe::sync::mesh::{load_or_create_host, load_peer_book, JoinOpts, MeshConn};
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

/// CLI request-to-join, END-TO-END over the real mesh: a keyless `--restricted`
/// joiner broadcasts a signed [`riftpipe_core::onboard::JoinRequest`], the founder's
/// inbox and `.riftpipe-auth.json` snapshot surface it, `riftpipe approve <dir>
/// <id>` (the control-file path exercised by the CLI's `approve` verb) grants it,
/// and the joiner reads the founder's content and gets wrapped into the epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restricted_mesh_request_then_approve_via_control_file() {
    use riftpipe_core::identity::DeviceKey;
    use riftpipe_core::onboard::JoinRequest;

    let (df, dj) = (dir("rq-founder"), dir("rq-joiner"));
    let secret = riftpipe_core::crypto::generate_secret();
    let host = MeshConn::join(secret, None).await.expect("host join");
    let host_addr = dialable_addr(&host).await;

    let (_tx_a, mut rx_a) = push_file(&df, "secret.bin", b"members only");
    let (tx_j, mut rx_j) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();

    let df_task = df.clone();
    let a_task = tokio::spawn(async move {
        let founder = DeviceKey::from_seed(&[71u8; 32]);
        let mut peer = TreePeer::new();
        peer.set_identity(DeviceKey::from_seed(&[71u8; 32]), true);
        peer.add_trust(founder.genesis()); // founder = Admin; no grants yet
        let _ = riftpipe::sync::mesh::run(host, &df_task, &mut peer, &mut rx_a, false).await;
    });
    let (dj_task, addr) = (dj.clone(), host_addr.clone());
    let j_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        peer.set_identity(DeviceKey::from_seed(&[72u8; 32]), true);
        peer.set_request_name("Bob");
        let _ = riftpipe::sync::mesh::join_and_run(secret, addr, &dj_task, &mut peer, &mut rx_j, false).await;
    });

    let j_id = DeviceKey::from_seed(&[72u8; 32]).id();
    let j_hex = hex(&j_id);
    let want_safety = JoinRequest::create(&DeviceKey::from_seed(&[72u8; 32]), "Bob").safety_code();

    // The founder's inbox (via its `.riftpipe-auth.json` snapshot) learns the request.
    let req = tokio::time::timeout(BUDGET, async {
        loop {
            if let Some(state) = riftpipe::sync::control::read_state(&df) {
                if let Some(arr) = state.get("requests").and_then(|v| v.as_array()) {
                    if let Some(r) = arr.iter().find(|r| r.get("device").and_then(|v| v.as_str()) == Some(j_hex.as_str())) {
                        break r.clone();
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("founder's auth state never showed the joiner's request");
    assert_eq!(req.get("name").and_then(|v| v.as_str()), Some("Bob"));
    assert_eq!(req.get("safety").and_then(|v| v.as_str()), Some(want_safety.as_str()));

    assert!(!dj.join("secret.bin").exists(), "an unapproved joiner must not read the founder's content");
    let state = riftpipe::sync::control::read_state(&df).unwrap();
    assert_eq!(state.get("pid").and_then(|v| v.as_u64()), Some(std::process::id() as u64), "stamped with the writer's pid");
    assert!(state.get("updatedAt").and_then(|v| v.as_u64()).is_some(), "stamped with updatedAt");

    // An approve for a device with no pending request is ignored (grant by id is
    // the route for that); it's queued first, so it's handled before the real one.
    let stranger = hex(&DeviceKey::from_seed(&[73u8; 32]).id());
    riftpipe::sync::control::append(&df, "approve", &stranger).unwrap();
    riftpipe::sync::control::append(&df, "approve", &j_hex).unwrap();
    wait_for(&[dj.join("secret.bin")], "the approved joiner reads the founder's pre-existing content").await;

    std::fs::write(dj.join("reply.bin"), b"from the joiner").unwrap();
    tx_j.send(dj.join("reply.bin")).unwrap();
    wait_for(&[df.join("reply.bin")], "the newly-approved joiner's write reaches the founder").await;

    // The founder's snapshot drops the request and lists the joiner as a member.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(state) = riftpipe::sync::control::read_state(&df) {
                let empty = state.get("requests").and_then(|v| v.as_array()).map(|a| a.is_empty()).unwrap_or(false);
                let is_member = state
                    .get("members")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter().any(|m| {
                            m.get("device").and_then(|v| v.as_str()) == Some(j_hex.as_str())
                                && m.get("role").and_then(|v| v.as_str()) == Some("member")
                        })
                    })
                    .unwrap_or(false);
                if empty && is_member {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("founder's auth state never settled to an empty inbox with the joiner as a member");
    let members = riftpipe::sync::control::read_state(&df).unwrap()["members"].clone();
    assert!(
        !members.as_array().unwrap().iter().any(|m| m["device"] == stranger.as_str()),
        "an approve with no pending request granted nothing: {members}"
    );

    // The snapshot belongs to the session: it's removed when the loop ends.
    a_task.abort();
    j_task.abort();
    let _ = a_task.await;
    let _ = j_task.await;
    for d in [&df, &dj] {
        assert!(riftpipe::sync::control::read_state(d).is_none(), "state file left behind in {}", d.display());
    }
    std::fs::remove_dir_all(&df).ok();
    std::fs::remove_dir_all(&dj).ok();
}
/// RESTART end-to-end over the real mesh: a native host's persisted identity +
/// board secret (`load_or_create_host`) make a restart the SAME board; durable
/// sync records (`TreePeer::seed`) resume its history instead of re-priming;
/// and the restarted host's `NeighborUp` Hello pulls back the joiner's gap
/// edits — made to existing AND brand-new files while the host was down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_host_restart_resyncs_gap_edits() {
    const RESTART_BUDGET: Duration = Duration::from_secs(90);

    let dh = dir("rs-host");
    let dj = dir("rs-joiner");
    std::fs::write(dh.join("a.md"), "# v1\n").unwrap();
    std::fs::write(dh.join("m.toml"), "col = 1\n").unwrap();

    let (sk, secret) = load_or_create_host(&dh, false).unwrap();
    let topic = riftpipe_core::crypto::topic_from_secret(&secret);

    let host = MeshConn::join_with(JoinOpts {
        secret,
        bootstrap: None,
        secret_key: Some(sk.clone()),
        peers: load_peer_book(&dh, topic),
    })
    .await
    .expect("host join");
    let addr = dialable_addr(&host).await;
    let ep1 = host.endpoint().clone();

    let (_tx_h, mut rx_h) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let (stop1, stop1_rx) = tokio::sync::oneshot::channel::<()>();
    let dh_task1 = dh.clone();
    let host_task1 = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        tokio::select! {
            _ = riftpipe::sync::mesh::run(host, &dh_task1, &mut peer, &mut rx_h, false) => {}
            _ = stop1_rx => {}
        }
    });

    let (tx_j, mut rx_j) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let (dj_task, addr_j) = (dj.clone(), addr.clone());
    let joiner_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        let _ = riftpipe::sync::mesh::join_and_run(secret, addr_j, &dj_task, &mut peer, &mut rx_j, false).await;
    });

    wait_for(&[dj.join("a.md"), dj.join("m.toml")], "joiner gets the host's tree").await;
    let peers_file = dh.join(".riftpipe-peers");
    tokio::time::timeout(BUDGET, async {
        while !peers_file.is_file() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("host never noted the joiner in its PeerBook");

    // Stop host 1 WITHOUT tearing down the joiner — a real restart, not a
    // clean shutdown the joiner also sees.
    let _ = stop1.send(());
    ep1.close().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Gap edits on the joiner while the host is down: one existing text file,
    // one existing LWW file, and one brand-new file.
    std::fs::write(dj.join("a.md"), "# v1\ngap line\n").unwrap();
    tx_j.send(dj.join("a.md")).unwrap();
    std::fs::write(dj.join("gap.md"), "# gap\n").unwrap();
    tx_j.send(dj.join("gap.md")).unwrap();
    std::fs::write(dj.join("m.toml"), "col = 2\n").unwrap();
    tx_j.send(dj.join("m.toml")).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Restart: same dir ⇒ same persisted identity ⇒ same board.
    let (sk2, secret2) = load_or_create_host(&dh, false).unwrap();
    assert_eq!(sk2.to_bytes(), sk.to_bytes(), "restart keeps the same identity");
    assert_eq!(secret2, secret, "restart keeps the same board secret");
    let host2 = MeshConn::join_with(JoinOpts {
        secret: secret2,
        bootstrap: None,
        secret_key: Some(sk2),
        peers: load_peer_book(&dh, topic),
    })
    .await
    .expect("host restart join");
    assert_eq!(host2.endpoint().id(), addr.id, "restart keeps the same EndpointId");

    let (_tx_h2, mut rx_h2) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let dh_task2 = dh.clone();
    let host_task2 = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        let _ = riftpipe::sync::mesh::run(host2, &dh_task2, &mut peer, &mut rx_h2, false).await;
    });

    let (gap, a_path, m_path) = (dh.join("gap.md"), dh.join("a.md"), dh.join("m.toml"));
    tokio::time::timeout(RESTART_BUDGET, async {
        loop {
            if gap.is_file()
                && std::fs::read(&a_path).ok() == Some(b"# v1\ngap line\n".to_vec())
                && std::fs::read(&m_path).ok() == Some(b"col = 2\n".to_vec())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("restarted host never pulled the joiner's gap edits");

    assert_eq!(std::fs::read(dj.join("a.md")).unwrap(), b"# v1\ngap line\n", "joiner not regressed");
    assert_eq!(std::fs::read(dj.join("m.toml")).unwrap(), b"col = 2\n", "joiner not regressed");

    host_task1.abort();
    host_task2.abort();
    joiner_task.abort();
    std::fs::remove_dir_all(&dh).ok();
    std::fs::remove_dir_all(&dj).ok();
}

/// A host that CRASHES (a killed `connect --accept` process: no QUIC close, so
/// the joiner still holds the dead connection) and restarts within the idle
/// timeout. The restarted host re-dials the joiner, whose gossip silently swaps
/// the connection in while hyparview's stale `pending_neighbor_requests` entry
/// suppresses its `Neighbor` reply — so without `SwapRejoin` the host never
/// gets a NeighborUp and the gap edits never arrive. The joiner must notice
/// the swapped-in connection and re-Join.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_host_crash_restart_resyncs_gap_edits() {
    const RESTART_BUDGET: Duration = Duration::from_secs(90);

    let dh = dir("crash-host");
    let dj = dir("crash-joiner");
    std::fs::write(dh.join("a.md"), "# v1\n").unwrap();
    // The CLI loads this same identity + board secret from `<dh>/.riftpipe-host`.
    let (sk, secret) = load_or_create_host(&dh, false).unwrap();
    let topic = riftpipe_core::crypto::topic_from_secret(&secret);

    let ticket_file = PathBuf::from(format!("{}.ticket", dh.display()));
    let _ = std::fs::remove_file(&ticket_file);
    let mut cli = std::process::Command::new(env!("CARGO_BIN_EXE_riftpipe"))
        .args(["connect", "--accept"])
        .arg(&dh)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn connect --accept");
    let ticket = tokio::time::timeout(BUDGET, async {
        loop {
            if let Ok(t) = std::fs::read_to_string(&ticket_file) {
                if !t.trim().is_empty() {
                    return t.trim().to_string();
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("CLI host never wrote its ticket");
    let bytes: Vec<u8> = (0..ticket.len() / 2)
        .map(|i| u8::from_str_radix(&ticket[i * 2..i * 2 + 2], 16).unwrap())
        .collect();
    let (t_secret, addr): ([u8; 32], iroh::EndpointAddr) = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(t_secret, secret, "the CLI hosts the persisted board");
    assert_eq!(addr.id, sk.public(), "the CLI uses the persisted identity");

    let (tx_j, mut rx_j) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let dj_task = dj.clone();
    let joiner_task = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        let _ = riftpipe::sync::mesh::join_and_run(secret, addr, &dj_task, &mut peer, &mut rx_j, false).await;
    });
    wait_for(&[dj.join("a.md")], "joiner gets the CLI host's tree").await;
    let peers_file = dh.join(".riftpipe-peers");
    tokio::time::timeout(BUDGET, async {
        while !peers_file.is_file() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("CLI host never noted the joiner in its PeerBook");

    // Crash: SIGKILL, no graceful close — the joiner keeps the dead connection.
    cli.kill().unwrap();
    let _ = cli.wait();
    let _ = std::fs::remove_file(&ticket_file);

    std::fs::write(dj.join("a.md"), "# v1\ngap line\n").unwrap();
    tx_j.send(dj.join("a.md")).unwrap();
    std::fs::write(dj.join("gap.md"), "# gap\n").unwrap();
    tx_j.send(dj.join("gap.md")).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Restart well inside the joiner's QUIC idle timeout, same identity.
    let host2 = MeshConn::join_with(JoinOpts {
        secret,
        bootstrap: None,
        secret_key: Some(sk),
        peers: load_peer_book(&dh, topic),
    })
    .await
    .expect("host restart join");
    let (_tx_h2, mut rx_h2) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let dh_task2 = dh.clone();
    let host_task2 = tokio::spawn(async move {
        let mut peer = TreePeer::new();
        let _ = riftpipe::sync::mesh::run(host2, &dh_task2, &mut peer, &mut rx_h2, false).await;
    });

    let (gap, a_path) = (dh.join("gap.md"), dh.join("a.md"));
    tokio::time::timeout(RESTART_BUDGET, async {
        while !(gap.is_file() && std::fs::read(&a_path).ok() == Some(b"# v1\ngap line\n".to_vec())) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("crash-restarted host never pulled the joiner's gap edits");

    host_task2.abort();
    joiner_task.abort();
    std::fs::remove_dir_all(&dh).ok();
    std::fs::remove_dir_all(&dj).ok();
}

/// `connect --accept --restricted` on a dir that already hosted a board stops
/// before going online and keeps the old identity file untouched: a dir hosted by
/// an older build or hosted open has no auth state, so a restricted host refuses
/// to resume it.
#[test]
fn restricted_cli_host_without_persisted_auth_refuses_to_resume() {
    let d = dir("restricted-restart");
    let (sk, _) = load_or_create_host(&d, false).unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_riftpipe"))
        .args(["connect", "--accept"])
        .arg(&d)
        .arg("--restricted")
        .output()
        .expect("run connect --accept --restricted");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("rerun with --new-board"), "refused with a hint, got: {stderr}");
    assert!(!stderr.contains("[mesh] host id"), "never went online: {stderr}");
    let (sk2, _) = load_or_create_host(&d, false).unwrap();
    assert_eq!(sk2.to_bytes(), sk.to_bytes(), "the persisted identity is untouched");
    std::fs::remove_dir_all(&d).ok();
}

/// A spawned `riftpipe` CLI process, killed (SIGKILL) and reaped when dropped — so
/// a failed assertion or timeout never leaves it running.
struct KillOnDrop(std::process::Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Run `riftpipe <args>` in the background, stderr to `log`.
fn spawn_cli(args: &[&std::ffi::OsStr], log: &std::path::Path) -> KillOnDrop {
    use std::process::{Command, Stdio};
    let child = Command::new(env!("CARGO_BIN_EXE_riftpipe"))
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::from(std::fs::File::create(log).unwrap()))
        .spawn()
        .unwrap();
    KillOnDrop(child)
}

/// A per-test CLI log path, outside the synced dirs.
fn cli_log(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("riftpipe-mesh-{}-{tag}.log", std::process::id()))
}

/// Poll `ok` every `every` until it holds; panic after [`BUDGET`].
fn wait_until(what: &str, every: Duration, mut ok: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while !ok() {
        assert!(start.elapsed() < BUDGET, "timed out waiting until {what}");
        std::thread::sleep(every);
    }
}

/// The device key a CLI peer persisted in `<dir>/.device` (a hex seed).
fn device_of(dir: &std::path::Path) -> riftpipe_core::identity::DeviceKey {
    let hex = std::fs::read_to_string(dir.join(".device")).unwrap();
    let mut seed = [0u8; 32];
    for (i, b) in seed.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex.trim()[i * 2..i * 2 + 2], 16).unwrap();
    }
    riftpipe_core::identity::DeviceKey::from_seed(&seed)
}

const POLL: Duration = Duration::from_millis(100);

/// A `--restricted` CLI host restarted after a live revoke resumes the SAME board
/// from its persisted trust log + epochs: the revoke sticks (its replayed `--grant`
/// is skipped), and nothing is re-minted. Run 1 is SIGKILLed — there's no save at
/// exit, so the state on disk is what was saved when the revoke happened.
#[test]
fn restricted_cli_host_restart_resumes_and_keeps_a_live_revoke() {
    use riftpipe_core::identity::DeviceKey;

    let d = dir("restricted-resume");
    let x = hex(&DeviceKey::from_seed(&[77u8; 32]).id());
    let spawn = |log: &PathBuf| {
        spawn_cli(&["connect".as_ref(), "--accept".as_ref(), d.as_os_str(), "--restricted".as_ref(), "--grant".as_ref(), x.as_ref()], log)
    };
    let has_member = |id: &str| {
        riftpipe::sync::control::read_state(&d).is_some_and(|state| {
            state["members"].as_array().is_some_and(|a| a.iter().any(|m| m["device"].as_str() == Some(id)))
        })
    };

    let (log1, log2) = (cli_log("rr-run1"), cli_log("rr-run2"));
    let run1 = spawn(&log1);
    wait_until("the --grant is applied", POLL, || has_member(&x));
    // x never sent a join request, so it isn't wrapped into epoch 1: the revoke
    // needs no rotation. The control file is tailed from its length when the run
    // loop opens it (just after the first state write), so re-append until seen.
    let mut last_append: Option<std::time::Instant> = None;
    wait_until("the live revoke is applied", POLL, || {
        if last_append.is_none_or(|t| t.elapsed() > Duration::from_secs(3)) {
            riftpipe::sync::control::append(&d, "revoke", &x).unwrap();
            last_append = Some(std::time::Instant::now());
        }
        riftpipe::sync::control::read_state(&d).is_some() && !has_member(&x)
    });

    let (_, secret) = load_or_create_host(&d, false).unwrap();
    let p = riftpipe::sync::auth_store::auth_path(&d, &riftpipe_core::crypto::topic_from_secret(&secret));
    assert!(p.is_file(), "the host persisted its trust log + epochs");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600, "auth.state is owner-only");
    }
    drop(run1);
    riftpipe::sync::control::remove_state(&d);

    let run2 = spawn(&log2);
    wait_until("the restarted host is running", POLL, || riftpipe::sync::control::read_state(&d).is_some());
    let out2 = std::fs::read_to_string(&log2).unwrap();
    assert!(out2.contains("[auth] restored"), "restored its trust log + epochs: {out2}");
    assert!(out2.contains(&format!("--grant {x} skipped")), "the replayed --grant is skipped: {out2}");
    assert!(!out2.contains("rerun with --new-board"), "resumed, not refused: {out2}");
    assert!(!out2.contains("rotated to epoch"), "no re-mint: {out2}");
    assert!(!has_member(&x), "the live revoke sticks across the restart");
    let state = riftpipe::sync::control::read_state(&d).unwrap();
    assert_eq!(state["epoch"], 1, "still epoch 1: {state}");

    drop(run2);
    std::fs::remove_dir_all(&d).ok();
    std::fs::remove_file(&log1).ok();
    std::fs::remove_file(&log2).ok();
}

/// A first `--restricted` host run killed while going online (its `.riftpipe-host`
/// already written, the mesh loop not started) has saved its genesis, so the next
/// start resumes instead of refusing with `rerun with --new-board`.
#[test]
fn restricted_cli_host_killed_before_its_mesh_loop_still_resumes() {
    let d = dir("restricted-firstrun");
    let args = ["connect".as_ref(), "--accept".as_ref(), d.as_os_str(), "--restricted".as_ref()];
    let (log1, log2) = (cli_log("fr-run1"), cli_log("fr-run2"));

    let run1 = spawn_cli(&args, &log1);
    // Printed right after the auth setup (and its save), before going online.
    wait_until("the auth setup ran", Duration::from_millis(10), || {
        std::fs::read_to_string(&log1).is_ok_and(|l| l.contains("[auth] restricted mode"))
    });
    drop(run1);
    let out1 = std::fs::read_to_string(&log1).unwrap();
    assert!(!out1.contains("rotated to epoch"), "killed before the mesh loop's startup: {out1}");

    let (_, secret) = load_or_create_host(&d, false).unwrap();
    let board = riftpipe_core::crypto::topic_from_secret(&secret);
    let p = riftpipe::sync::auth_store::auth_path(&d, &board);
    match riftpipe::sync::auth_store::load(&p, &board, &device_of(&d)) {
        riftpipe::sync::auth_store::Loaded::Found(s) => {
            assert_eq!((s.trust.len(), s.epochs.len()), (1, 0), "the genesis, saved before going online")
        }
        other => panic!("no auth state after the first run: {other:?}"),
    }

    let _run2 = spawn_cli(&args, &log2);
    wait_until("the restarted host is running", POLL, || riftpipe::sync::control::read_state(&d).is_some());
    let out2 = std::fs::read_to_string(&log2).unwrap();
    assert!(!out2.contains("rerun with --new-board"), "resumed, not refused: {out2}");
    assert!(out2.contains("[auth] restored 1 trust entries and 0 epoch announces"), "{out2}");
    assert!(out2.contains("rotated to epoch 1"), "epoch 1 is minted on the resumed run: {out2}");

    drop(_run2);
    std::fs::remove_dir_all(&d).ok();
    std::fs::remove_file(&log1).ok();
    std::fs::remove_file(&log2).ok();
}

/// The CLI joiner path: a granted `--restricted` joiner persists the board's trust
/// log + epochs, restores them on restart, and on a corrupt `auth.state` warns,
/// relearns from the board, and overwrites the bad file with its next save.
#[test]
fn restricted_cli_joiner_restores_and_replaces_a_corrupt_state() {
    use riftpipe::sync::auth_store::{auth_path, load, Loaded};
    use riftpipe_core::identity::DeviceKey;

    let (dh, dj) = (dir("rj-host"), dir("rj-joiner"));
    let seed = [88u8; 32];
    std::fs::write(dj.join(".device"), hex(&seed)).unwrap();
    let j_key = DeviceKey::from_seed(&seed);
    let j_id = hex(&j_key.id());
    std::fs::write(dh.join("secret.txt"), "members only").unwrap();

    let ticket_file = PathBuf::from(format!("{}.ticket", dh.display()));
    let _ = std::fs::remove_file(&ticket_file);
    let logs = [cli_log("rj-host"), cli_log("rj-j1"), cli_log("rj-j2"), cli_log("rj-j3")];
    let _host = spawn_cli(
        &["connect".as_ref(), "--accept".as_ref(), dh.as_os_str(), "--restricted".as_ref(), "--grant".as_ref(), j_id.as_ref()],
        &logs[0],
    );
    wait_until("the host printed its ticket", POLL, || ticket_file.is_file());
    let ticket = std::fs::read_to_string(&ticket_file).unwrap();
    let (_, secret) = load_or_create_host(&dh, false).unwrap();
    let board = riftpipe_core::crypto::topic_from_secret(&secret);
    let path = auth_path(&dj, &board);
    let joiner = |log: &PathBuf| {
        spawn_cli(&["connect".as_ref(), ticket.trim().as_ref(), dj.as_os_str(), "--restricted".as_ref()], log)
    };
    let can_read = || {
        riftpipe::sync::control::read_state(&dj).is_some_and(|s| s["canRead"] == true && s["epoch"].as_u64() >= Some(1))
    };

    let j1 = joiner(&logs[1]);
    wait_until("the granted joiner reads the board", POLL, || dj.join("secret.txt").is_file());
    assert!(matches!(load(&path, &board, &j_key), Loaded::Found(s) if !s.epochs.is_empty()), "the joiner persisted");
    drop(j1);
    riftpipe::sync::control::remove_state(&dj);

    let j2 = joiner(&logs[2]);
    wait_until("the restarted joiner can read", POLL, can_read);
    let out2 = std::fs::read_to_string(&logs[2]).unwrap();
    assert!(out2.contains("[auth] restored"), "restored on restart: {out2}");
    assert!(!out2.contains("and 0 epoch announces"), "its epochs came back too: {out2}");
    drop(j2);
    riftpipe::sync::control::remove_state(&dj);

    let mut bytes = std::fs::read(&path).unwrap();
    bytes[5] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
    assert!(matches!(load(&path, &board, &j_key), Loaded::Corrupt(_)));
    let j3 = joiner(&logs[3]);
    wait_until("the corrupt auth.state is replaced by a valid save", POLL, || {
        matches!(load(&path, &board, &j_key), Loaded::Found(s) if !s.epochs.is_empty())
    });
    wait_until("the joiner relearned the board", POLL, can_read);
    let out3 = std::fs::read_to_string(&logs[3]).unwrap();
    assert!(out3.contains("[auth] ignoring the persisted trust log/epochs"), "warned: {out3}");
    assert!(!out3.contains("[auth] restored"), "nothing restored from the bad file: {out3}");

    drop(j3);
    drop(_host);
    std::fs::remove_dir_all(&dh).ok();
    std::fs::remove_dir_all(&dj).ok();
    let _ = std::fs::remove_file(&ticket_file);
    for l in &logs {
        std::fs::remove_file(l).ok();
    }
}
