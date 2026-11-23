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
//! - `NeighborUp` → trust log + epoch announces, our whole tree (`full_state`), a
//!   `Hello` (so a one-sided reconnect also pulls what WE lack), and a `Presence`
//! - `NeighborDown` → drop the neighbor + re-broadcast `Presence`; total neighbor
//!   loss starts a backoff `Rejoin` (re-dials the bootstrap + persisted `PeerBook`)
//! - local file change → `.md` as text CRDT, else LWW with a ms timestamp
//! - every neighbor seen is noted in a per-board `PeerBook`, persisted to
//!   `<dir>/.riftpipe-peers` so a restart can re-dial them
//! - a NEW incoming connection from a current neighbor (it restarted under the
//!   same key) → re-Join it (`SwapRejoin`: hyparview stale-pending workaround)
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
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_gossip::proto::TopicId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{interval, MissedTickBehavior};

use riftpipe_core::crypto::{topic_from_secret, MeshCipher};
use riftpipe_core::reconnect::{link_state, rejoin_targets, LinkState, PeerBook, Rejoin, SwapRejoin};
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

/// `<dir>/.riftpipe-host`: line 1 = iroh secret key hex, line 2 = board secret hex.
/// Same key ⇒ same EndpointId, same secret ⇒ same topic: a restarted host is the SAME board.
pub const HOST_FILE: &str = ".riftpipe-host";
/// `<dir>/.riftpipe-peers`: postcard `PeerBook` (peers seen on this board).
pub const PEERS_FILE: &str = ".riftpipe-peers";

fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Load `<dir>/.riftpipe-host`'s persisted identity, or mint and save a fresh
/// one. `fresh` forgets any existing file first (`--new-board`: mint a new
/// link). Same key ⇒ same `EndpointId` ⇒ a restarted host is the SAME board.
pub fn load_or_create_host(dir: &Path, fresh: bool) -> Result<(SecretKey, [u8; 32])> {
    let path = dir.join(HOST_FILE);
    if fresh {
        let _ = std::fs::remove_file(&path);
    }
    if let Ok(text) = std::fs::read_to_string(&path) {
        let mut lines = text.lines();
        if let (Some(k), Some(s)) = (lines.next().and_then(parse_hex32), lines.next().and_then(parse_hex32)) {
            return Ok((SecretKey::from_bytes(&k), s));
        }
    }
    let sk = SecretKey::generate();
    let secret = riftpipe_core::crypto::generate_secret();
    std::fs::write(&path, format!("{}\n{}\n", hex32(&sk.to_bytes()), hex32(&secret))).map_err(anyerr)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok((sk, secret))
}

/// Refuse to resume a board `dir` already hosted when `restricted`, unless
/// `fresh` (`--new-board`). `TreePeer` doesn't persist the trust log or epochs,
/// so a restarted restricted founder would replay its `--grant`s under fresh
/// nonces — on peers that already hold `[grant, revoke]` the new grant lands
/// after the revoke and re-authorizes a device revoked live — and re-mint
/// epoch 1. Until trust + epochs persist, a restricted host starts a new board.
pub fn check_restricted_resume(dir: &Path, restricted: bool, fresh: bool) -> Result<()> {
    if restricted && !fresh && dir.join(HOST_FILE).exists() {
        return Err(anyerr(format!(
            "{} already hosts a board; a --restricted host can't resume it yet (its trust log and epochs \
             aren't persisted, so a restart could re-authorize a revoked device) — rerun with --new-board",
            dir.display()
        )));
    }
    Ok(())
}

/// This board's remembered peers (`<dir>/.riftpipe-peers`), or an empty book
/// if the file is absent, corrupt, or for a different topic.
pub fn load_peer_book(dir: &Path, topic: [u8; 32]) -> PeerBook {
    match std::fs::read(dir.join(PEERS_FILE)) {
        Ok(bytes) => PeerBook::decode(topic, &bytes),
        Err(_) => PeerBook::new(topic),
    }
}

/// Persist `book` (tmp + rename), ignoring errors — a missed save just means
/// fewer remembered peers to re-dial next time.
pub fn save_peer_book(dir: &Path, book: &PeerBook) {
    let path = dir.join(PEERS_FILE);
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    if std::fs::write(&tmp, book.encode()).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

/// The gossip protocol handler, wrapped to report each accepted connection's
/// remote id (a current neighbor dialing us anew has restarted — see
/// [`SwapRejoin`]). The connection itself goes to gossip unchanged.
#[derive(Debug, Clone)]
struct GossipAccept {
    gossip: Gossip,
    incoming: UnboundedSender<EndpointId>,
}

impl iroh::protocol::ProtocolHandler for GossipAccept {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> std::result::Result<(), iroh::protocol::AcceptError> {
        let id = conn.remote_id();
        self.gossip
            .handle_connection(conn)
            .await
            .map_err(iroh::protocol::AcceptError::from_err)?;
        let _ = self.incoming.send(id);
        Ok(())
    }
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
    /// Peers seen on earlier sessions of this board (host restart, or a
    /// joiner that reconnects): dialed alongside `bootstrap` at join time and
    /// on total neighbor loss.
    peers: PeerBook,
    /// Remote ids of accepted gossip connections (from [`GossipAccept`]).
    incoming: UnboundedReceiver<EndpointId>,
}

/// Inputs to [`MeshConn::join_with`].
pub struct JoinOpts {
    pub secret: [u8; 32],
    pub bootstrap: Option<EndpointAddr>,
    /// Persisted identity (host restart = same EndpointId); `None` = fresh key.
    pub secret_key: Option<SecretKey>,
    /// Peers seen on earlier sessions of this board: dialed now and on total loss.
    pub peers: PeerBook,
}

impl MeshConn {
    /// Bind an endpoint (fresh, or `opts.secret_key` for a persisted identity)
    /// and subscribe to the board's topic (`H(secret)`), dialing the bootstrap
    /// AND every remembered peer so a restarted host or joiner reconnects
    /// without waiting on discovery. The `secret` both names the topic and
    /// keys the frame cipher, so only link-holders find and can read the board.
    pub async fn join_with(opts: JoinOpts) -> Result<MeshConn> {
        let JoinOpts { secret, bootstrap, secret_key, peers } = opts;
        // Seed the bootstrap address so we can dial it without waiting on discovery.
        let lookup = MemoryLookup::new();
        let mut boot = Vec::new();
        if let Some(addr) = &bootstrap {
            boot.push(addr.id);
            lookup.add_endpoint_info(addr.clone());
        }
        let bootstrap_id = bootstrap.map(|a| a.id);
        let mut b = Endpoint::builder(presets::N0).address_lookup(lookup);
        if let Some(sk) = secret_key {
            b = b.secret_key(sk);
        }
        let endpoint = b.bind().await.map_err(anyerr)?;
        let my_id = endpoint.id();
        // Re-dial every peer we've seen on this board before (a restarted host
        // has no bootstrap of its own; a reconnecting joiner may need more
        // than just the host).
        for id in peers.ids() {
            if let Ok(pid) = EndpointId::from_bytes(id) {
                if pid != my_id && !boot.contains(&pid) {
                    boot.push(pid);
                }
            }
        }
        // The library's 4 KiB frame limit stays at its default: big messages are
        // chunked (`core::sync::encode_mesh_frames`) so no peer — however old —
        // ever receives an over-limit frame.
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let (incoming_tx, incoming) = tokio::sync::mpsc::unbounded_channel();
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, GossipAccept { gossip: gossip.clone(), incoming: incoming_tx })
            .spawn();
        let topic = gossip
            .subscribe(topic_of(&secret), boot)
            .await
            .map_err(anyerr)?;
        let (sender, receiver) = topic.split();
        let cipher = MeshCipher::from_secret(&secret);
        Ok(MeshConn {
            endpoint,
            _router: router,
            _gossip: gossip,
            sender,
            receiver,
            my_id,
            bootstrap: bootstrap_id,
            cipher,
            peers,
            incoming,
        })
    }

    /// Bind a fresh endpoint and subscribe to the board's topic (`H(secret)`).
    /// `bootstrap` is the host's address (from the share-link ticket) for a
    /// joiner, or `None` when WE start the swarm (the host role).
    pub async fn join(secret: [u8; 32], bootstrap: Option<EndpointAddr>) -> Result<MeshConn> {
        Self::join_with(JoinOpts {
            secret,
            bootstrap,
            secret_key: None,
            peers: PeerBook::new(topic_from_secret(&secret)),
        })
        .await
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
    let topic = topic_from_secret(&secret);
    let conn = MeshConn::join_with(JoinOpts {
        secret,
        bootstrap: Some(host_addr),
        secret_key: None,
        peers: load_peer_book(dir, topic),
    })
    .await?;
    eprintln!("[mesh] joined board (host {}) as {} — sealed", host_id, conn.my_id);
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
    let MeshConn {
        endpoint: _endpoint,
        _router,
        _gossip,
        sender,
        mut receiver,
        my_id,
        bootstrap,
        cipher,
        peers: mut book,
        mut incoming,
    } = conn;
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
    // A restricted founder mints epoch 1 here. The announces stay in the book
    // and reach peers through the `NeighborUp` catch-up.
    let _ = peer.startup();

    // Backoff re-dial of the bootstrap + remembered peers on total neighbor
    // loss, and the `[mesh] status=...` breadcrumb the npm wrapper/CLI prints.
    let mut rejoin = Rejoin::new();
    let mut ever = false;
    let mut last_status: Option<(LinkState, usize)> = None;
    let mut rejoin_tick = interval(Duration::from_secs(1));
    rejoin_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    fn report_status(last: &mut Option<(LinkState, usize)>, n: usize, ever: bool) {
        let s = link_state(n, ever, true);
        if *last != Some((s, n)) {
            eprintln!("[mesh] status={} peers={n}", s.as_str());
            *last = Some((s, n));
        }
    }
    rejoin.on_neighbors(0, now_ms());
    report_status(&mut last_status, 0, false);
    // Neighbors that restarted and re-dialed us: re-Join them (see `SwapRejoin`).
    let mut swap = SwapRejoin::new();

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
            // Live admin commands from `riftpipe revoke|grant <dir> <id>`.
            _ = control_tick.tick() => {
                for cmd in control.poll() {
                    let out = match cmd {
                        crate::sync::control::Command::Revoke(id) => peer.revoke_device(id, dir),
                        crate::sync::control::Command::Grant(id) => peer.grant_device(id, dir),
                    };
                    for m in out {
                        broadcast(&sender, &cipher, &m).await;
                    }
                }
            }
            // Re-Join restarted neighbors that re-dialed us (`SwapRejoin`). Total
            // neighbor loss: re-dial the bootstrap + remembered peers with
            // backoff (hyparview only refills from its passive view and never
            // re-dials on its own; `on_online` isn't wired here — there's no
            // native "online" event, so the tick alone drives it).
            _ = rejoin_tick.tick() => {
                let neighbors: Vec<[u8; 32]> = receiver.neighbors().map(|n| *n.as_bytes()).collect();
                let swapped: Vec<EndpointId> = swap.due(now_ms(), &neighbors)
                    .iter().filter_map(|b| EndpointId::from_bytes(b).ok()).collect();
                if !swapped.is_empty() {
                    for id in &swapped {
                        eprintln!("[mesh] neighbor {id} re-dialed us (restarted) — re-joining it");
                    }
                    if let Err(e) = sender.join_peers(swapped).await {
                        eprintln!("[mesh] re-join failed: {e}");
                    }
                }
                if rejoin.due(now_ms()) {
                    let targets: Vec<EndpointId> = rejoin_targets(bootstrap.map(|b| *b.as_bytes()), &book, my_id_bytes)
                        .iter().filter_map(|b| EndpointId::from_bytes(b).ok()).collect();
                    if !targets.is_empty() {
                        eprintln!("[mesh] no neighbors — re-joining {} peer(s)", targets.len());
                        if let Err(e) = sender.join_peers(targets).await {
                            eprintln!("[mesh] re-join failed: {e}");
                        }
                    }
                }
            }
            // An accepted gossip connection: if it's from a current neighbor, that
            // neighbor restarted and swapped its connection in — queue a re-Join.
            Some(id) = incoming.recv() => {
                let is_neighbor = receiver.neighbors().any(|n| n == id);
                swap.on_incoming(*id.as_bytes(), is_neighbor, now_ms());
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
                                    if book.note(id, my_id_bytes) {
                                        save_peer_book(dir, &book);
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
                        // A Hello makes a one-sided reconnect bidirectional: the
                        // peer replies with what WE lack (e.g. its gap edits
                        // while we were down). iroh-gossip emits NeighborUp only
                        // on the side that sees a new active peer.
                        if let Some(mm) = peer.sign_mesh(peer.hello()) {
                            broadcast_neighbors(&sender, &cipher, &mm).await;
                        }
                        for msg in peer.full_state() {
                            if let Some(mm) = peer.sign_mesh(msg) {
                                broadcast_neighbors(&sender, &cipher, &mm).await;
                            }
                        }
                        let list: Vec<[u8; 32]> =
                            receiver.neighbors().map(|n| *n.as_bytes()).collect();
                        routing.insert(my_id_bytes, list.clone());
                        broadcast(&sender, &cipher, &MeshMsg::Presence { id: my_id_bytes, neighbors: list.clone() }).await;
                        if book.note(*id.as_bytes(), my_id_bytes) {
                            save_peer_book(dir, &book);
                        }
                        ever = true;
                        rejoin.on_neighbors(list.len(), now_ms());
                        report_status(&mut last_status, list.len(), ever);
                    }
                    Event::NeighborDown(id) => {
                        eprintln!("[mesh] neighbor down: {id}");
                        let list: Vec<[u8; 32]> =
                            receiver.neighbors().map(|n| *n.as_bytes()).collect();
                        routing.insert(my_id_bytes, list.clone());
                        broadcast(
                            &sender,
                            &cipher,
                            &MeshMsg::Version { id: my_id_bytes, proto: PROTOCOL_VERSION },
                        )
                        .await;
                        broadcast(&sender, &cipher, &MeshMsg::Presence { id: my_id_bytes, neighbors: list.clone() }).await;
                        rejoin.on_neighbors(list.len(), now_ms());
                        report_status(&mut last_status, list.len(), ever);
                    }
                    Event::Lagged => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("riftpipe-mesh-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Same dir ⇒ same persisted identity across "restarts"; `fresh` (`--new-board`)
    /// mints a different key and secret; the file is owner-only on unix.
    #[test]
    fn host_identity_persists_and_new_board_mints_fresh() {
        let d = dir("host-identity");
        let (sk1, secret1) = load_or_create_host(&d, false).unwrap();
        let (sk2, secret2) = load_or_create_host(&d, false).unwrap();
        assert_eq!(sk1.to_bytes(), sk2.to_bytes(), "same dir, same restart identity");
        assert_eq!(secret1, secret2, "same dir, same board secret");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(d.join(HOST_FILE)).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "host identity file is owner-only");
        }

        let (sk3, secret3) = load_or_create_host(&d, true).unwrap();
        assert_ne!(sk3.to_bytes(), sk1.to_bytes(), "--new-board mints a fresh key");
        assert_ne!(secret3, secret1, "--new-board mints a fresh board secret");

        std::fs::remove_dir_all(&d).ok();
    }

    /// A restricted host refuses to resume a board its dir already hosted
    /// (`--grant` replay could re-authorize a live-revoked device); a first run,
    /// `--new-board`, or an open board are fine.
    #[test]
    fn restricted_host_refuses_to_resume_without_new_board() {
        let d = dir("restricted-resume");
        assert!(check_restricted_resume(&d, true, false).is_ok(), "first run: nothing to resume");
        load_or_create_host(&d, false).unwrap();
        let err = check_restricted_resume(&d, true, false).expect_err("restricted resume refused");
        assert!(err.to_string().contains("--new-board"), "the error says how to proceed: {err}");
        assert!(check_restricted_resume(&d, true, true).is_ok(), "--new-board starts over");
        assert!(check_restricted_resume(&d, false, false).is_ok(), "an open board resumes");
        std::fs::remove_dir_all(&d).ok();
    }

    /// `save_peer_book`/`load_peer_book` roundtrip, and a book saved for one
    /// topic is invisible to a different one (treated as absent).
    #[test]
    fn peer_book_file_roundtrips_and_ignores_other_topics() {
        let d = dir("peer-book");
        let topic_a = [1u8; 32];
        let topic_b = [2u8; 32];
        let empty = load_peer_book(&d, topic_a);
        assert!(empty.ids().is_empty(), "no file yet");

        let mut book = PeerBook::new(topic_a);
        book.note([9u8; 32], [0u8; 32]);
        save_peer_book(&d, &book);

        let loaded = load_peer_book(&d, topic_a);
        assert_eq!(loaded, book);

        let other = load_peer_book(&d, topic_b);
        assert!(other.ids().is_empty(), "a different topic's book is empty");

        std::fs::remove_dir_all(&d).ok();
    }
}
