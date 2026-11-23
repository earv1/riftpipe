//! Native driver for the browser's **gossip-mesh** dialect: a shared tree is an
//! iroh-gossip topic keyed by the *host's* EndpointId, and every peer broadcasts
//! `riftpipe_core::sync::MeshMsg`s epidemically (see `web/src/gossip.rs`, which
//! speaks the identical wire layout). This is how `riftpipe connect` joins a
//! shared folder hosted by a browser: same topic, same postcard frames, same catch-up
//! semantics — the CLI is just another swarm member.
//!
//! Mirrors `web/src/gossip.rs`'s `GossipTreeSync` behavior exactly:
//! - received `Sync` → merge via the shared `Syncer`, persist, broadcast replies
//!   (on a restricted board content travels as epoch-`Sealed`; see `TreePeer`)
//! - `NeighborUp` → trust log + epoch announces, our whole tree (`full_state`),
//!   and a `Presence`
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

use riftpipe_core::crypto::{topic_from_secret, MeshCipher};
use riftpipe_core::sync::{
    encode_mesh_frames, MeshAssembler, MeshMsg, PeerVersion, VersionMonitor, PROTOCOL_VERSION,
};

use crate::net::{anyerr, Result};
use crate::sync::tree::{scan_files, TreePeer};

/// The gossip topic for a board secret — `H(secret)`, so it's underivable
/// without the share link. Identical to `web/src/gossip.rs::topic_of`.
pub fn topic_of(secret: &[u8; 32]) -> TopicId {
    TopicId::from_bytes(topic_from_secret(secret))
}

/// A joined gossip mesh for one shared tree: our endpoint (fresh identity),
/// the spawned router, the split topic handles, and the frame cipher.
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
    /// Seals/opens every gossip frame with the key derived from the board
    /// secret — the relay and non-holders see only ciphertext.
    cipher: MeshCipher,
    /// The board's topic bytes (`H(secret)`) — what passkey links are bound to.
    board: [u8; 32],
}

impl MeshConn {
    /// Bind a fresh endpoint and subscribe to the board's topic (`H(secret)`).
    /// `bootstrap` is the host's address (from the share-link ticket) for a
    /// joiner, or `None` when WE start the swarm (the host role). The `secret`
    /// both names the topic and keys the frame cipher, so only link-holders
    /// find and can read the board.
    pub async fn join(secret: [u8; 32], bootstrap: Option<EndpointAddr>) -> Result<MeshConn> {
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
            .subscribe(topic_of(&secret), boot)
            .await
            .map_err(anyerr)?;
        let (sender, receiver) = topic.split();
        let cipher = MeshCipher::from_secret(&secret);
        Ok(MeshConn {
            board: topic_from_secret(&secret),
            endpoint,
            _router: router,
            _gossip: gossip,
            sender,
            receiver,
            my_id,
            bootstrap,
            cipher,
        })
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
    secret: [u8; 32],
    host_addr: EndpointAddr,
    dir: &Path,
    peer: &mut TreePeer,
    rx: &mut UnboundedReceiver<PathBuf>,
    poll: bool,
) -> Result<()> {
    let host_id = host_addr.id;
    let conn = MeshConn::join(secret, Some(host_addr)).await?;
    eprintln!("[mesh] joined board (host {}) as {} — sealed", host_id, conn.my_id);
    run(conn, dir, peer, rx, poll).await
}

/// The last `.riftpipe-auth.json` write: the unstamped snapshot text, and when.
#[derive(Default)]
struct StateWrite {
    text: String,
    at: Option<std::time::Instant>,
}

/// Removes `<dir>/.riftpipe-auth.json` when the mesh loop ends, however it ends.
struct StateFileGuard(PathBuf);

impl Drop for StateFileGuard {
    fn drop(&mut self) {
        crate::sync::control::remove_state(&self.0);
    }
}

/// Resolves on Ctrl-C, or SIGTERM on unix (what `kill` and Node's `proc.kill()` send).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
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
    let MeshConn {
        endpoint: _endpoint,
        _router,
        _gossip,
        sender,
        mut receiver,
        my_id,
        bootstrap,
        cipher,
        board,
    } = conn;
    peer.set_board(board);
    let my_id_bytes = *my_id.as_bytes();
    // The gossiped routing map `id -> [neighbors]` (kept for parity with the
    // browser's debug map; phase 2 will surface it). Our own entry is sourced
    // from `receiver.neighbors()` — the library's authoritative set.
    let mut routing: BTreeMap<[u8; 32], Vec<[u8; 32]>> = BTreeMap::new();

    let mut rescan = interval(Duration::from_secs(2));
    rescan.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut rx_open = true;
    // Last-written `.riftpipe-auth.json` snapshot (unstamped text + when), so we
    // rewrite the file only when it changed or the heartbeat is due.
    let mut last_state = StateWrite::default();
    // The snapshot describes THIS session: remove it however the loop ends
    // (signal, gossip stream end, error), so `riftpipe requests|members|approve`
    // don't read a dead peer's state.
    let _state_guard = StateFileGuard(dir.to_path_buf());

    // Files that predate the session must be in the syncer BEFORE any
    // NeighborUp, or the full_state() catch-up broadcasts an empty tree
    // (the browser host primes OPFS the same way).
    peer.seed(dir);
    // A restricted founder mints epoch 1 here. The announces stay in the book
    // and reach peers through the `NeighborUp` catch-up.
    let _ = peer.startup();
    // Prime the auth state snapshot — or, when this session has no identity
    // (not `--restricted`), clear a stale one an earlier restricted session on
    // this dir left behind (e.g. after a crash).
    if peer.auth_state_json().is_none() {
        crate::sync::control::remove_state(dir);
    } else {
        sync_state(peer, dir, &mut last_state);
    }

    // A failed send must not kill the session (convergence is repaired by the
    // next catch-up), but it must be VISIBLE — a silently dropped broadcast
    // reads as "synced" when it isn't. Big messages go out as chunk frames,
    // and every frame is AEAD-sealed so only link-holders can read/inject.
    async fn broadcast(sender: &GossipSender, cipher: &MeshCipher, gm: &MeshMsg) {
        for b in encode_mesh_frames(gm) {
            let sealed = cipher.seal(&b);
            let n = sealed.len();
            if let Err(e) = sender.broadcast(sealed.into()).await {
                eprintln!("[mesh] broadcast of {n} bytes failed: {e}");
            }
        }
    }

    /// Direct-neighbor-only send for the `NeighborUp` catch-up: the newcomer is
    /// (or is about to be) a direct neighbor, so flooding the whole swarm with
    /// our full tree on every join is pure waste — the epidemic layer spreads
    /// anything the rest actually lacks via the normal reply path.
    async fn broadcast_neighbors(sender: &GossipSender, cipher: &MeshCipher, gm: &MeshMsg) {
        for b in encode_mesh_frames(gm) {
            let sealed = cipher.seal(&b);
            let n = sealed.len();
            if let Err(e) = sender.broadcast_neighbors(sealed.into()).await {
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

    // Live admin commands (`riftpipe revoke|grant <dir> <id>`), polled from the
    // control dotfile once a second.
    let mut control = crate::sync::control::Control::open(dir);
    let mut control_tick = interval(Duration::from_secs(1));
    control_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // A keyless restricted peer re-announces its join request: an Admin who
    // comes online later, or who isn't a direct neighbor, still sees it.
    const REQUEST_REANNOUNCE: Duration = Duration::from_secs(15);
    let mut request_tick =
        tokio::time::interval_at(tokio::time::Instant::now() + REQUEST_REANNOUNCE, REQUEST_REANNOUNCE);
    request_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    /// Rewrite `.riftpipe-auth.json` when the snapshot changed, or every
    /// `STATE_HEARTBEAT_SECS` so its `updatedAt` shows the peer is alive. Each
    /// write is stamped with `pid` + `updatedAt` (unix seconds).
    fn sync_state(peer: &TreePeer, dir: &Path, last: &mut StateWrite) {
        use crate::sync::control::{write_state, STATE_HEARTBEAT_SECS};
        let Some(mut json) = peer.auth_state_json() else { return };
        let text = json.to_string();
        let heartbeat_due =
            last.at.is_none_or(|t| t.elapsed() >= Duration::from_secs(STATE_HEARTBEAT_SECS));
        if text == last.text && !heartbeat_due {
            return;
        }
        json["pid"] = std::process::id().into();
        json["updatedAt"] = (now_ms() / 1000).into();
        match write_state(dir, &serde_json::to_string_pretty(&json).unwrap_or_default()) {
            Ok(()) => *last = StateWrite { text, at: Some(std::time::Instant::now()) },
            Err(e) => eprintln!("[auth] writing .riftpipe-auth.json failed: {e}"),
        }
    }

    // A clean stop on Ctrl-C / SIGTERM (not the default kill), so the state
    // file guard runs.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                eprintln!("[mesh] shutting down");
                return Ok(());
            }
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
                        // Signed when this peer has an identity, else a plain Sync;
                        // sealed on a restricted board (`None` until we hold a key).
                        if let Some(mm) = peer.sign_local(msg) {
                            broadcast(&sender, &cipher, &mm).await;
                        }
                    }
                }
            }
            // Fallback poll: rescan for files whose content drifted from `seen`.
            _ = rescan.tick(), if poll => {
                let mut files = Vec::new();
                scan_files(dir, &mut files);
                for p in files {
                    if let Some(msg) = peer.local_change(&p, dir) {
                        if let Some(mm) = peer.sign_local(msg) {
                            broadcast(&sender, &cipher, &mm).await;
                        }
                    }
                }
            }
            // Grace expiry for announcement-less (pre-versioning) peers.
            _ = version_tick.tick() => {
                for p in versions.tick(now_ms()) {
                    report_mismatch(&p);
                }
            }
            // A keyless restricted peer re-announces so an Admin who comes online
            // later, or isn't a direct neighbor, still sees the request.
            _ = request_tick.tick() => {
                if let Some(r) = peer.read_request() {
                    broadcast(&sender, &cipher, &r).await;
                }
            }
            // Live admin commands from `riftpipe revoke|grant|approve <dir> <id>`.
            _ = control_tick.tick() => {
                for cmd in control.poll() {
                    let out = match cmd {
                        crate::sync::control::Command::Revoke(id) => peer.revoke_device(id, dir),
                        crate::sync::control::Command::Grant(id) => peer.grant_device(id, dir),
                        crate::sync::control::Command::Approve(id) => peer.approve_request(id, dir),
                    };
                    for m in out {
                        broadcast(&sender, &cipher, &m).await;
                    }
                }
                sync_state(peer, dir, &mut last_state);
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
                        // Open the sealed frame (an outsider's or a corrupt frame
                        // won't decrypt — drop it). Then plain frames pass through
                        // and chunk frames buffer until whole.
                        let Some(plain) = cipher.open(&m.content) else { continue };
                        let Some(gm) = assembler.feed(&plain) else { continue };
                        match gm {
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
                            // Sync / SignedSync / Trust / Request / Epoch / Sealed —
                            // auth-aware apply (`authorize_and_apply` opens sealed
                            // content, enforces signing, verifies, persists, prints
                            // SYNCED:{path}, and runs epoch housekeeping). Replies
                            // come back already wrapped for the wire.
                            other => {
                                for reply in peer.authorize_and_apply(other, dir) {
                                    broadcast(&sender, &cipher, &reply).await;
                                }
                            }
                        }
                    }
                    Event::NeighborUp(id) => {
                        eprintln!("[mesh] neighbor up: {id}");
                        // Announce our protocol version BEFORE the Presence —
                        // that ordering is what lets peers deduce "Presence
                        // but no Version = pre-versioning peer".
                        broadcast(
                            &sender,
                            &cipher,
                            &MeshMsg::Version { id: my_id_bytes, proto: PROTOCOL_VERSION },
                        )
                        .await;
                        // Catch the new neighbor up: first the trust log and the
                        // epoch announces (so it learns the authorized set and
                        // any wrapped keys — gossip is ephemeral), then our whole
                        // tree. Direct neighbors only.
                        for m in peer.auth_catchup() {
                            broadcast_neighbors(&sender, &cipher, &m).await;
                        }
                        // A restricted peer without the current key asks the
                        // WHOLE swarm: the Admin who can wrap it may not be a
                        // direct neighbor.
                        if let Some(r) = peer.read_request() {
                            broadcast(&sender, &cipher, &r).await;
                        }
                        for msg in peer.full_state() {
                            if let Some(mm) = peer.sign_mesh(msg) {
                                broadcast_neighbors(&sender, &cipher, &mm).await;
                            }
                        }
                        let list: Vec<[u8; 32]> =
                            receiver.neighbors().map(|n| *n.as_bytes()).collect();
                        routing.insert(my_id_bytes, list.clone());
                        broadcast(&sender, &cipher, &MeshMsg::Presence { id: my_id_bytes, neighbors: list }).await;
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
                            &cipher,
                            &MeshMsg::Version { id: my_id_bytes, proto: PROTOCOL_VERSION },
                        )
                        .await;
                        broadcast(&sender, &cipher, &MeshMsg::Presence { id: my_id_bytes, neighbors: list }).await;
                    }
                    Event::Lagged => {}
                }
            }
        }
    }
}
