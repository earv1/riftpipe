//! Native driver for the browser's **gossip-mesh** dialect: a shared tree is an
//! iroh-gossip topic keyed by the *host's* EndpointId, and every peer broadcasts
//! `riftpipe_core::sync::MeshMsg`s epidemically (see `web/src/gossip.rs`, which
//! speaks the identical wire layout). This is how `riftpipe connect` joins a
//! board hosted by a browser: same topic, same postcard frames, same catch-up
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

use std::collections::{BTreeMap, BTreeSet, HashSet};
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

use riftpipe_core::sync::MeshMsg;

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
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .spawn();
        let topic = gossip
            .subscribe(topic_of(host.unwrap_or(my_id)), boot)
            .await
            .map_err(anyerr)?;
        let (sender, receiver) = topic.split();
        Ok(MeshConn { endpoint, _router: router, _gossip: gossip, sender, receiver, my_id })
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
    let MeshConn { sender, mut receiver, my_id, .. } = conn;
    let my_id_bytes = *my_id.as_bytes();
    let mut neighbors: BTreeSet<[u8; 32]> = BTreeSet::new();
    // The gossiped routing map `id -> [neighbors]` (kept for parity with the
    // browser's debug map; phase 2 will surface it).
    let mut routing: BTreeMap<[u8; 32], Vec<[u8; 32]>> = BTreeMap::new();

    let mut rescan = interval(Duration::from_secs(2));
    rescan.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut rx_open = true;

    // Broadcast one message; mesh sends are best-effort (a lagging swarm must
    // not kill the session — convergence is repaired by the next catch-up).
    async fn broadcast(sender: &GossipSender, gm: &MeshMsg) {
        if let Ok(b) = postcard::to_allocvec(gm) {
            let _ = sender.broadcast(b.into()).await;
        }
    }

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
                        let Ok(gm) = postcard::from_bytes::<MeshMsg>(&m.content) else { continue };
                        match gm {
                            MeshMsg::Sync(msg) => {
                                // `apply_and_persist` prints SYNCED:{path} on persist.
                                for reply in peer.apply_and_persist(msg, dir) {
                                    broadcast(&sender, &MeshMsg::Sync(reply)).await;
                                }
                            }
                            MeshMsg::Presence { id, neighbors } => {
                                routing.insert(id, neighbors);
                            }
                        }
                    }
                    Event::NeighborUp(id) => {
                        eprintln!("[mesh] neighbor up: {id}");
                        neighbors.insert(*id.as_bytes());
                        // Catch the new neighbor up with our whole tree.
                        for msg in peer.full_state() {
                            broadcast(&sender, &MeshMsg::Sync(msg)).await;
                        }
                        let list: Vec<[u8; 32]> = neighbors.iter().copied().collect();
                        routing.insert(my_id_bytes, list.clone());
                        broadcast(&sender, &MeshMsg::Presence { id: my_id_bytes, neighbors: list }).await;
                    }
                    Event::NeighborDown(id) => {
                        eprintln!("[mesh] neighbor down: {id}");
                        neighbors.remove(id.as_bytes());
                        let list: Vec<[u8; 32]> = neighbors.iter().copied().collect();
                        routing.insert(my_id_bytes, list.clone());
                        broadcast(&sender, &MeshMsg::Presence { id: my_id_bytes, neighbors: list }).await;
                    }
                    Event::Lagged => {}
                }
            }
        }
    }
}
