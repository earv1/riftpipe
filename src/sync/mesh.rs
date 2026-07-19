//! Native driver for the browser's **gossip-mesh** dialect: a shared tree is an
//! iroh-gossip topic keyed by the *host's* EndpointId, and every peer broadcasts
//! `riftpipe_core::sync::MeshMsg`s epidemically (see `web/src/gossip.rs`, which
//! speaks the identical wire layout). This is how `riftpipe connect` joins a
//! shared folder hosted by a browser: same topic, same postcard frames, same catch-up
//! semantics — the CLI is just another swarm member.
//!
//! Mirrors `web/src/gossip.rs`'s `GossipTreeSync` behavior exactly:
//! - received `Sync` → merge via the shared `Syncer`, persist, broadcast replies
//! - `NeighborUp` → broadcast our whole tree (`full_state`) + a `Presence`
//! - `NeighborDown` → drop the neighbor + re-broadcast `Presence`
//! - local file change → `.md` as text CRDT, else LWW with a ms timestamp
//!
//! Shape mirrors `sync/tree.rs`'s [`run`](super::tree::run): the caller owns
//! the [`TreePeer`] and the watcher channel; one `select!` loop, no spawned
//! sync tasks. The push-decision and persist logic are the shared `TreePeer`
//! methods, so link-sync and mesh-sync can't drift.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::StreamExt;
use iroh::address_lookup::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_gossip::proto::TopicId;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval, MissedTickBehavior};

use riftpipe_core::sync::{
    encode_mesh_frames, MeshAssembler, MeshMsg, PeerVersion, VersionMonitor, PROTOCOL_VERSION,
};

use crate::net::{anyerr, Result};
use crate::sync::tree::{scan_files, TreePeer};

/// The topic for a tree hosted by `host` — deterministic, so all peers
/// (browser and native) agree. Identical to `web/src/gossip.rs::topic_of`.
pub fn topic_of(host: EndpointId) -> TopicId {
    TopicId::from_bytes(*host.as_bytes())
}

/// A joined gossip mesh for one shared tree: our endpoint (fresh identity),
/// the spawned router, and the split topic handles.
pub struct MeshConn {
    endpoint: Endpoint,
    _router: Router,
    _gossip: Gossip,
    sender: GossipSender,
    receiver: GossipReceiver,
    my_id: EndpointId,
    /// The peer we bootstrapped off (`None` when we host the swarm) — kept so
    /// a TOTAL neighbor loss can re-join it: hyparview heals only from its
    /// passive view and never re-dials the original bootstrap on its own.
    bootstrap: Option<EndpointId>,
}

impl MeshConn {
    /// Bind a fresh endpoint and subscribe to `host`'s topic. `bootstrap` is
    /// the host's address (decoded from the browser's share-link ticket) for a
    /// joiner, or `None` when WE start the swarm (the browser-host role — used
    /// by the loopback test; the CLI is always a joiner in phase 1).
    pub async fn join(host: Option<EndpointId>, bootstrap: Option<EndpointAddr>) -> Result<MeshConn> {
        // Seed the bootstrap address so we can dial it without waiting on discovery.
        let lookup = MemoryLookup::new();
        let mut boot = Vec::new();
        if let Some(addr) = bootstrap {
            boot.push(addr.id);
            lookup.add_endpoint_info(addr);
        }
        let endpoint = Endpoint::builder(presets::N0)
            .address_lookup(lookup)
            .bind()
            .await
            .map_err(anyerr)?;
        let my_id = endpoint.id();
        // The library's 4 KiB frame limit stays at its default: big messages are
        // chunked (`core::sync::encode_mesh_frames`) so no peer — however old —
        // ever receives an over-limit frame.
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .spawn();
        let bootstrap = boot.first().copied();
        let topic = gossip
            .subscribe(topic_of(host.unwrap_or(my_id)), boot)
            .await
            .map_err(anyerr)?;
        let (sender, receiver) = topic.split();
        Ok(MeshConn { endpoint, _router: router, _gossip: gossip, sender, receiver, my_id, bootstrap })
    }

    /// The underlying endpoint (for `online()` / address sharing in tests).
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

/// Join the mesh behind `host_addr` (a browser share link's decoded ticket) and
/// sync `dir` with the swarm until the gossip stream ends. Mirrors
/// [`super::tree::run`]: one `select!` loop over watcher events (`rx`), gossip
/// events, and — when `poll` is set because the watcher failed — a slow rescan.
pub async fn join_and_run(
    host_addr: EndpointAddr,
    dir: &Path,
    peer: &mut TreePeer,
    rx: &mut UnboundedReceiver<PathBuf>,
    poll: bool,
) -> Result<()> {
    let host_id = host_addr.id;
    let conn = MeshConn::join(Some(host_id), Some(host_addr)).await?;
    eprintln!("[mesh] joined topic of host {} as {}", host_id, conn.my_id);
    run(conn, dir, peer, rx, poll).await
}

/// Drive an already-joined [`MeshConn`] (joiner OR host role) over `dir`.
pub async fn run(
    conn: MeshConn,
    dir: &Path,
    peer: &mut TreePeer,
    rx: &mut UnboundedReceiver<PathBuf>,
    poll: bool,
) -> Result<()> {
    // Keep the endpoint/router/gossip alive for the whole loop: a host must keep
    // its Router listening on GOSSIP_ALPN to accept joiners (a bare `..` would
    // drop them here). Underscore-named bindings live to end of scope.
    let MeshConn { endpoint: _endpoint, _router, _gossip, sender, mut receiver, my_id, bootstrap } =
        conn;
    let my_id_bytes = *my_id.as_bytes();
    // The gossiped routing map `id -> [neighbors]` (kept for parity with the
    // browser's debug map; phase 2 will surface it). Our own entry is sourced
    // from `receiver.neighbors()` — the library's authoritative set.
    let mut routing: BTreeMap<[u8; 32], Vec<[u8; 32]>> = BTreeMap::new();

    let mut rescan = interval(Duration::from_secs(2));
    rescan.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut rx_open = true;

    // Files that predate the session must be in the syncer BEFORE any
    // NeighborUp, or the full_state() catch-up broadcasts an empty tree
    // (the browser host primes OPFS the same way).
    peer.seed(dir);

    // A failed send must not kill the session (convergence is repaired by the
    // next catch-up), but it must be VISIBLE — a silently dropped broadcast
    // reads as "synced" when it isn't. Big messages go out as chunk frames.
    async fn broadcast(sender: &GossipSender, gm: &MeshMsg) {
        for b in encode_mesh_frames(gm) {
            let n = b.len();
            if let Err(e) = sender.broadcast(b.into()).await {
                eprintln!("[mesh] broadcast of {n} bytes failed: {e}");
            }
        }
    }

    /// Direct-neighbor-only send for the `NeighborUp` catch-up: the newcomer is
    /// (or is about to be) a direct neighbor, so flooding the whole swarm with
    /// our full tree on every join is pure waste — the epidemic layer spreads
    /// anything the rest actually lacks via the normal reply path.
    async fn broadcast_neighbors(sender: &GossipSender, gm: &MeshMsg) {
        for b in encode_mesh_frames(gm) {
            let n = b.len();
            if let Err(e) = sender.broadcast_neighbors(b.into()).await {
                eprintln!("[mesh] neighbor broadcast of {n} bytes failed: {e}");
            }
        }
    }

    // Reassembles incoming chunk frames into whole messages.
    let mut assembler = MeshAssembler::new();
    // Flags peers whose announced protocol version differs (or who never
    // announce — pre-versioning peers). One stable stderr line per peer, so
    // scripts (and the npm wrapper) can parse it.
    let mut versions = VersionMonitor::new();
    fn report_mismatch(p: &PeerVersion) {
        let hex: String = p.id.iter().map(|b| format!("{b:02x}")).collect();
        eprintln!(
            "[mesh] protocol-mismatch peer={hex} theirs={} ours={PROTOCOL_VERSION} — \
             sync with that peer may be incomplete",
            p.proto,
        );
    }
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
    // A quiet mesh may only ever see ONE presence from a pre-versioning peer —
    // this tick fires the grace-expiry path.
    let mut version_tick = interval(Duration::from_secs(5));
    version_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Local edits → broadcast (the watcher feeds paths into `rx`).
            ev = rx.recv(), if rx_open => {
                let Some(first) = ev else {
                    // Watcher gone; the poll arm is the safety net.
                    rx_open = false;
                    continue;
                };
                // Coalesce a burst of events into a de-duplicated batch.
                let mut batch = HashSet::new();
                batch.insert(first);
                while let Ok(p) = rx.try_recv() {
                    batch.insert(p);
                }
                for p in batch {
                    if let Some(msg) = peer.local_change(&p, dir) {
                        broadcast(&sender, &MeshMsg::Sync(msg)).await;
                    }
                }
            }
            // Fallback poll: rescan for files whose content drifted from `seen`.
            _ = rescan.tick(), if poll => {
                let mut files = Vec::new();
                scan_files(dir, &mut files);
                for p in files {
                    if let Some(msg) = peer.local_change(&p, dir) {
                        broadcast(&sender, &MeshMsg::Sync(msg)).await;
                    }
                }
            }
            // Grace expiry for announcement-less (pre-versioning) peers.
            _ = version_tick.tick() => {
                for p in versions.tick(now_ms()) {
                    report_mismatch(&p);
                }
            }
            // Mesh events: remote edits → disk, membership → catch-up + presence.
            ev = receiver.next() => {
                let ev = match ev {
                    None | Some(Err(_)) => {
                        eprintln!("[mesh] gossip stream ended");
                        return Ok(());
                    }
                    Some(Ok(ev)) => ev,
                };
                match ev {
                    Event::Received(m) => {
                        // Plain frames pass through; chunk frames buffer until whole.
                        let Some(gm) = assembler.feed(&m.content) else { continue };
                        match gm {
                            MeshMsg::Sync(msg) => {
                                // `apply_and_persist` prints SYNCED:{path} on persist.
                                for reply in peer.apply_and_persist(msg, dir) {
                                    broadcast(&sender, &MeshMsg::Sync(reply)).await;
                                }
                            }
                            MeshMsg::Presence { id, neighbors } => {
                                if id != my_id_bytes {
                                    if let Some(p) = versions.on_presence(id, now_ms()) {
                                        report_mismatch(&p);
                                    }
                                }
                                routing.insert(id, neighbors);
                            }
                            MeshMsg::Version { id, proto } => {
                                if id != my_id_bytes {
                                    if let Some(p) = versions.on_version(id, proto) {
                                        report_mismatch(&p);
                                    }
                                }
                            }
                            // feed() never yields a Chunk (it refuses nesting).
                            MeshMsg::Chunk { .. } => {}
                        }
                    }
                    Event::NeighborUp(id) => {
                        eprintln!("[mesh] neighbor up: {id}");
                        // Announce our protocol version BEFORE the Presence —
                        // that ordering is what lets peers deduce "Presence
                        // but no Version = pre-versioning peer".
                        broadcast(
                            &sender,
                            &MeshMsg::Version { id: my_id_bytes, proto: PROTOCOL_VERSION },
                        )
                        .await;
                        // Catch the new neighbor up with our whole tree —
                        // direct neighbors only; the swarm doesn't need it.
                        for msg in peer.full_state() {
                            broadcast_neighbors(&sender, &MeshMsg::Sync(msg)).await;
                        }
                        let list: Vec<[u8; 32]> =
                            receiver.neighbors().map(|n| *n.as_bytes()).collect();
                        routing.insert(my_id_bytes, list.clone());
                        broadcast(&sender, &MeshMsg::Presence { id: my_id_bytes, neighbors: list }).await;
                    }
                    Event::NeighborDown(id) => {
                        eprintln!("[mesh] neighbor down: {id}");
                        let list: Vec<[u8; 32]> =
                            receiver.neighbors().map(|n| *n.as_bytes()).collect();
                        // Total neighbor loss: hyparview refills only from its
                        // passive view — it never re-dials the original
                        // bootstrap. We still hold its address (MemoryLookup),
                        // so ask the swarm to re-join it.
                        if list.is_empty() {
                            if let Some(b) = bootstrap {
                                eprintln!("[mesh] all neighbors lost — re-joining bootstrap {b}");
                                if let Err(e) = sender.join_peers(vec![b]).await {
                                    eprintln!("[mesh] re-join failed: {e}");
                                }
                            }
                        }
                        routing.insert(my_id_bytes, list.clone());
                        broadcast(
                            &sender,
                            &MeshMsg::Version { id: my_id_bytes, proto: PROTOCOL_VERSION },
                        )
                        .await;
                        broadcast(&sender, &MeshMsg::Presence { id: my_id_bytes, neighbors: list }).await;
                    }
                    Event::Lagged => {}
                }
            }
        }
    }
}
