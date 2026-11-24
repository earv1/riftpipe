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
//! - a `Hello`'s replies go to the whole swarm (the browser still sends them
//!   neighbors-only): iroh-gossip's neighbors scope misses a peer plumtree holds as
//!   lazy — see `tree::Outbound`
//! - `NeighborDown` → drop the neighbor + re-broadcast `Presence`; total neighbor
//!   loss starts a backoff `Rejoin` (re-dials the bootstrap + persisted `PeerBook`)
//! - local file change → `.md` as text CRDT, else LWW with a ms timestamp
//! - every neighbor seen is noted in a per-board `PeerBook`, persisted to
//!   `<dir>/.riftpipe-peers` so a restart can re-dial them
//! - a NEW incoming connection from a current neighbor (it restarted under the
//!   same key) → re-Join it (`SwapRejoin`: hyparview stale-pending workaround)
//!
//! - periodic anti-entropy: a jittered, backing-off `Hello` to direct neighbors
//!   (`core::antientropy`) repairs any dropped broadcast; gossip `Lagged`, a
//!   dropped partial chunked message, or a shed queued message force a round.
//!   Replies to a `Hello` go swarm-wide (see `SyncMsg::Hello`)
//! - one outbound queue (`core::outqueue`): whole messages in order, so a
//!   message's chunk frames go out back-to-back; a queued LWW update is replaced
//!   by a newer one for the same path
//!
//! Shape mirrors `sync/tree.rs`'s [`run`](super::tree::run): the caller owns
//! the [`TreePeer`] and the watcher channel; one `select!` loop plus one
//! outbound drain task. The push-decision and persist logic are the shared
//! `TreePeer` methods, so link-sync and mesh-sync can't drift.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

use riftpipe_core::antientropy::{AntiEntropy, AE_TICK_MS};
use riftpipe_core::crypto::{topic_from_secret, MeshCipher};
use riftpipe_core::outqueue::{coalesce_key, OutQueue, Scope};
use riftpipe_core::reconnect::{link_state, rejoin_targets, LinkState, PeerBook, Rejoin, SwapRejoin};
use riftpipe_core::sync::{MeshAssembler, MeshMsg, PeerVersion, VersionMonitor, PROTOCOL_VERSION};

use crate::net::{anyerr, Result};
use crate::sync::tree::{scan_files, AuthRestore, TreePeer};

/// The gossip topic for a board secret — `H(secret)`, so it's underivable
/// without the share link. Identical to `web/src/gossip.rs::topic_of`.
pub fn topic_of(secret: &[u8; 32]) -> TopicId {
    TopicId::from_bytes(topic_from_secret(secret))
}

/// `<dir>/.riftpipe-host`: line 1 = iroh secret key hex, line 2 = board secret hex, optional
/// line 3 = a restricted host's auth.state high-water mark (decimal item count; see
/// [`raise_auth_mark`]). Same key ⇒ same EndpointId, same secret ⇒ same topic: a restarted
/// host is the SAME board.
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

/// Parse `<dir>/.riftpipe-host` (line 1 = iroh secret key hex, line 2 = board
/// secret hex). `None` if absent or unparseable.
fn read_host(path: &Path) -> Option<(SecretKey, [u8; 32])> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut lines = text.lines();
    let k = lines.next().and_then(parse_hex32)?;
    let s = lines.next().and_then(parse_hex32)?;
    Some((SecretKey::from_bytes(&k), s))
}

/// Does starting a host on `dir` resume an existing board? `fresh` = `--new-board`.
/// An unparseable host file is not a resume: `load_or_create_host` mints a new
/// board over it.
pub fn host_resumes(dir: &Path, fresh: bool) -> bool {
    !fresh && read_host(&dir.join(HOST_FILE)).is_some()
}

/// Load `<dir>/.riftpipe-host`'s persisted identity, or mint and save a fresh
/// one. `fresh` forgets any existing file first (`--new-board`: mint a new
/// link). Same key ⇒ same `EndpointId` ⇒ a restarted host is the SAME board.
pub fn load_or_create_host(dir: &Path, fresh: bool) -> Result<(SecretKey, [u8; 32])> {
    let path = dir.join(HOST_FILE);
    if fresh {
        let _ = std::fs::remove_file(&path);
    }
    if let Some(host) = read_host(&path) {
        // A resumed file loosened by hand (or by an older build) goes back to owner-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if std::fs::metadata(&path).is_ok_and(|m| m.permissions().mode() & 0o077 != 0) {
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            }
        }
        return Ok(host);
    }
    let sk = SecretKey::generate();
    let secret = riftpipe_core::crypto::generate_secret();
    write_host(&path, &sk, &secret, 0).map_err(anyerr)?;
    Ok((sk, secret))
}

fn write_host(path: &Path, sk: &SecretKey, secret: &[u8; 32], auth_mark: u64) -> std::io::Result<()> {
    let mark = if auth_mark > 0 { format!("{auth_mark}\n") } else { String::new() };
    write_owner_only(path, format!("{}\n{}\n{mark}", hex32(&sk.to_bytes()), hex32(secret)).as_bytes())
}

/// The auth.state high-water mark recorded in `<dir>/.riftpipe-host` (0 if none).
pub fn read_auth_mark(dir: &Path) -> u64 {
    let text = std::fs::read_to_string(dir.join(HOST_FILE)).unwrap_or_default();
    text.lines().nth(2).and_then(|l| l.trim().parse().ok()).unwrap_or(0)
}

/// Raise the auth.state high-water mark in `<dir>/.riftpipe-host` to `count` (the
/// item count of the auth.state just written — it never shrinks). A resume then
/// refuses an auth.state holding fewer items: an older copy of the file (a backup
/// restore, or a rollback to undo a revoke). Call AFTER the auth.state write, so a
/// crash in between leaves the mark behind the file, never ahead of it.
pub fn raise_auth_mark(dir: &Path, count: u64) -> std::io::Result<()> {
    if count <= read_auth_mark(dir) {
        return Ok(());
    }
    let path = dir.join(HOST_FILE);
    let (sk, secret) = read_host(&path)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no parseable host file"))?;
    write_host(&path, &sk, &secret, count)
}

/// Write `contents` to `path` so it is NEVER readable by others, not even
/// briefly: a fresh tmp file created with mode 0600 (unix), then renamed over
/// `path` (which replaces any looser-permissioned file there). The parent dir is
/// fsynced after the rename (unix), so the new file survives a power loss.
pub(crate) fn write_owner_only(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let _ = std::fs::remove_file(&tmp);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&tmp)?;
    f.write_all(contents)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path)?;
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        let _ = std::fs::File::open(parent).and_then(|d| d.sync_all());
    }
    Ok(())
}

/// May a `--restricted` host start? `resuming` = `host_resumes(dir, --new-board)`. A resumed
/// restricted board needs its persisted trust log + epochs (`auth_store`), founded by this
/// `.device`: without them a restart would replay `--grant`s and re-mint epoch 1, which could
/// re-authorize a device revoked live. A rejected (corrupt/edited) file always refuses, and so
/// does one holding fewer items than `auth_mark` ([`read_auth_mark`]): an older copy.
pub fn check_restricted_resume(
    dir: &Path,
    resuming: bool,
    restore: &AuthRestore,
    founder: Option<[u8; 32]>,
    me: Option<[u8; 32]>,
    auth_mark: u64,
) -> Result<()> {
    match restore {
        AuthRestore::Rejected(why) => refuse(dir, &format!("its persisted trust log/epochs were rejected ({why})")),
        _ if !resuming => Ok(()),
        AuthRestore::Absent | AuthRestore::Skipped => {
            refuse(dir, "it has no persisted trust log/epochs for this board (hosted open, or by an older riftpipe)")
        }
        AuthRestore::Restored { .. } if founder.is_none() || founder != me => refuse(
            dir,
            &format!("its persisted trust log was founded by a device other than {}/.device", dir.display()),
        ),
        AuthRestore::Restored { trust, epochs } if ((trust + epochs) as u64) < auth_mark => refuse(
            dir,
            &format!(
                "its persisted trust log/epochs ({} items) are older than the last save recorded in {}/{HOST_FILE} \
                 ({auth_mark} items) — an older copy of auth.state was restored",
                trust + epochs,
                dir.display()
            ),
        ),
        AuthRestore::Restored { .. } => Ok(()),
    }
}

fn refuse(dir: &Path, why: &str) -> Result<()> {
    Err(anyerr(format!(
        "{} already hosts a board, but a --restricted host won't resume it: {why} — a restart without its \
         trust log could re-authorize a revoked device; rerun with --new-board to mint a new board/link",
        dir.display()
    )))
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
    /// The board's topic bytes (`H(secret)`): binds passkey links and scopes
    /// the tree's durable records.
    board: [u8; 32],
    /// See [`MeshConn::receive_blackout`].
    blackout: Arc<std::sync::atomic::AtomicBool>,
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
            board: topic_from_secret(&secret),
            blackout: Arc::default(),
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

    /// Test hook (simulated loss): while the flag is set, `run` discards every
    /// received gossip message unread — so a test can prove that a repair came
    /// from anti-entropy, not from the original broadcast.
    #[doc(hidden)]
    pub fn receive_blackout(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.blackout.clone()
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

/// The driver's one outbound queue (see `core::outqueue`) + its wakeup.
#[derive(Clone)]
struct Out {
    q: Arc<std::sync::Mutex<OutQueue>>,
    wake: Arc<tokio::sync::Notify>,
    /// The drain is sending a popped message. Set while holding `q`'s lock, so
    /// "queue empty and not sending" read under that lock means all sent.
    sending: Arc<std::sync::atomic::AtomicBool>,
}

/// How long a clean shutdown waits for queued messages (e.g. a revoke or grant
/// issued in the last tick) to go out before the drain is aborted.
const SHUTDOWN_FLUSH: Duration = Duration::from_secs(2);

impl Out {
    fn new() -> Self {
        Out {
            q: Arc::new(std::sync::Mutex::new(OutQueue::new())),
            wake: Arc::new(tokio::sync::Notify::new()),
            sending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    /// The next message to send, marking the drain busy while it sends it.
    fn take(&self) -> Option<(Scope, Vec<Vec<u8>>)> {
        let mut q = self.q.lock().unwrap();
        let next = q.pop();
        self.sending.store(next.is_some(), std::sync::atomic::Ordering::SeqCst);
        next
    }
    fn drained(&self) -> bool {
        let q = self.q.lock().unwrap();
        q.is_empty() && !self.sending.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Wait (up to `limit`) until the drain has sent everything queued.
    async fn flush(&self, limit: Duration) -> bool {
        tokio::time::timeout(limit, async {
            while !self.drained() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .is_ok()
    }
    fn push(&self, scope: Scope, key: Option<String>, m: &MeshMsg) {
        self.q.lock().unwrap().push(scope, key, m);
        self.wake.notify_one();
    }
    fn swarm(&self, m: &MeshMsg) {
        self.push(Scope::Swarm, None, m)
    }
    /// Direct neighbors only (plumtree-eager ones — iroh-gossip skips lazy peers):
    /// the `NeighborUp` catch-up and anti-entropy Hellos.
    fn neighbors(&self, m: &MeshMsg) {
        self.push(Scope::Neighbors, None, m)
    }
}

/// Aborts the drain task when `run` ends, however it ends.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Seal and send queued messages one at a time, frames back-to-back, off the
/// receive loop. Every frame is AEAD-sealed so only link-holders can
/// read/inject. A failed send must not kill the session (anti-entropy repairs
/// it), but it must be VISIBLE — a silently dropped broadcast reads as
/// "synced" when it isn't.
fn spawn_drain(sender: GossipSender, cipher: Arc<MeshCipher>, out: Out) -> AbortOnDrop {
    spawn_drain_with(out, move |scope, b| {
        let (sender, cipher) = (sender.clone(), cipher.clone());
        async move {
            let sealed = cipher.seal(&b);
            let n = sealed.len();
            let r = match scope {
                Scope::Swarm => sender.broadcast(sealed.into()).await,
                Scope::Neighbors => sender.broadcast_neighbors(sealed.into()).await,
            };
            if let Err(e) = r {
                eprintln!("[mesh] broadcast of {n} bytes failed: {e}");
            }
        }
    })
}

/// The drain loop over any frame sender (the gossip one, or a test's).
fn spawn_drain_with<F, Fut>(out: Out, send: F) -> AbortOnDrop
where
    F: Fn(Scope, Vec<u8>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    AbortOnDrop(tokio::spawn(async move {
        loop {
            let Some((scope, frames)) = out.take() else {
                out.wake.notified().await;
                continue;
            };
            for b in frames {
                send(scope, b).await;
            }
        }
    }))
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
        board,
        blackout,
    } = conn;
    let cipher = Arc::new(cipher);
    let out = Out::new();
    let _drain = spawn_drain(sender.clone(), cipher.clone(), out.clone());
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
    // Persist the (restored or freshly minted) trust log + epochs before anything goes out.
    peer.save_auth(dir);
    // Prime the auth state snapshot — or, when this session has no identity
    // (not `--restricted`), clear a stale one an earlier restricted session on
    // this dir left behind (e.g. after a crash).
    if peer.auth_state_json().is_none() {
        crate::sync::control::remove_state(dir);
    } else {
        sync_state(peer, dir, &mut last_state);
    }

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

    // Reassembles incoming chunk frames into whole messages.
    let mut assembler = MeshAssembler::new();

    // Anti-entropy: when to send our `Hello` digest to direct neighbors (see
    // `core::antientropy`). Losses we can see (gossip `Lagged`, an evicted
    // partial message, a shed queued message) force a round.
    let ae_t0 = std::time::Instant::now();
    let ae_now = move || ae_t0.elapsed().as_millis() as u64;
    let mut ae = AntiEntropy::new(ae_now(), rand::random::<u64>());
    let mut ae_tick = interval(Duration::from_millis(AE_TICK_MS));
    ae_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let (mut seen_evictions, mut seen_shed) = (0u64, 0u64);
    let mut last_lag_log: Option<std::time::Instant> = None;
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
                if !out.flush(SHUTDOWN_FLUSH).await {
                    eprintln!("[mesh] shutdown: queued messages not all sent within {SHUTDOWN_FLUSH:?}");
                }
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
                        // A queued, unsent LWW update for this path is
                        // superseded by this one (`coalesce_key`).
                        let key = coalesce_key(&msg);
                        // Signed when this peer has an identity, else a plain Sync;
                        // sealed on a restricted board (`None` until we hold a key).
                        if let Some(mm) = peer.sign_local(msg) {
                            out.push(Scope::Swarm, key, &mm);
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
                        let key = coalesce_key(&msg);
                        if let Some(mm) = peer.sign_local(msg) {
                            out.push(Scope::Swarm, key, &mm);
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
                    out.swarm(&r);
                }
            }
            // Live admin commands from `riftpipe revoke|grant|approve <dir> <id>`.
            _ = control_tick.tick() => {
                for cmd in control.poll() {
                    let msgs = match cmd {
                        crate::sync::control::Command::Revoke(id) => peer.revoke_device(id, dir),
                        crate::sync::control::Command::Grant(id) => peer.grant_device(id, dir),
                        crate::sync::control::Command::Approve(id) => peer.approve_request(id, dir),
                    };
                    for m in &msgs {
                        out.swarm(m);
                    }
                }
                // Retry a failed auth.state save (cheap no-op when nothing changed).
                peer.save_auth(dir);
                sync_state(peer, dir, &mut last_state);
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
            // Anti-entropy: note losses, then maybe send our Hello digest to
            // direct neighbors (they reply with what we lack, or pull back).
            _ = ae_tick.tick() => {
                let now = ae_now();
                let ev = assembler.evictions();
                if ev != seen_evictions {
                    eprintln!("[mesh] dropped {} partial message(s) — anti-entropy will repair", ev - seen_evictions);
                    seen_evictions = ev;
                    ae.on_loss(now);
                }
                let shed = out.q.lock().unwrap().shed();
                if shed != seen_shed {
                    eprintln!(
                        "[mesh] outbound queue over budget — shed {} message(s); anti-entropy will repair",
                        shed - seen_shed
                    );
                    seen_shed = shed;
                    ae.on_loss(now);
                }
                let neighbors: Vec<[u8; 32]> = receiver.neighbors().map(|n| *n.as_bytes()).collect();
                if ae.poll(now, peer.generation(), &neighbors) {
                    if let Some(mm) = peer.sign_mesh(peer.hello()) {
                        out.neighbors(&mm);
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
                        if blackout.load(std::sync::atomic::Ordering::Relaxed) {
                            continue;
                        }
                        let from = *m.delivered_from.as_bytes();
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
                                let auth_msg =
                                    matches!(other, MeshMsg::Trust(_) | MeshMsg::Request(_) | MeshMsg::Epoch(_));
                                let ob = peer.authorize_and_apply(other, dir);
                                // A trust/request/epoch change shows in
                                // `.riftpipe-auth.json` right away (no-op when the
                                // snapshot didn't change), not on the next tick.
                                if auth_msg {
                                    sync_state(peer, dir, &mut last_state);
                                }
                                if let Some(c) = ob.hello {
                                    ae.on_hello(ae_now(), from, c.consistent());
                                }
                                for (key, r) in ob.keyed_swarm() {
                                    out.push(Scope::Swarm, key, &r);
                                }
                            }
                        }
                    }
                    Event::NeighborUp(id) => {
                        eprintln!("[mesh] neighbor up: {id}");
                        // Announce our protocol version BEFORE the Presence —
                        // that ordering is what lets peers deduce "Presence
                        // but no Version = pre-versioning peer".
                        out.swarm(&MeshMsg::Version { id: my_id_bytes, proto: PROTOCOL_VERSION });
                        // Catch the new neighbor up: first the trust log and the
                        // epoch announces (so it learns the authorized set and
                        // any wrapped keys — gossip is ephemeral), then our whole
                        // tree. Direct neighbors only.
                        for m in peer.auth_catchup() {
                            out.neighbors(&m);
                        }
                        // A restricted peer without the current key asks the
                        // WHOLE swarm: the Admin who can wrap it may not be a
                        // direct neighbor.
                        if let Some(r) = peer.read_request() {
                            out.swarm(&r);
                        }
                        // A Hello makes a one-sided reconnect bidirectional: the
                        // peer replies with what WE lack (e.g. its gap edits
                        // while we were down). iroh-gossip emits NeighborUp only
                        // on the side that sees a new active peer.
                        if let Some(mm) = peer.sign_mesh(peer.hello()) {
                            out.neighbors(&mm);
                        }
                        ae.on_sent(ae_now());
                        for msg in peer.full_state() {
                            let key = coalesce_key(&msg);
                            if let Some(mm) = peer.sign_mesh(msg) {
                                out.push(Scope::Neighbors, key, &mm);
                            }
                        }
                        let list: Vec<[u8; 32]> =
                            receiver.neighbors().map(|n| *n.as_bytes()).collect();
                        routing.insert(my_id_bytes, list.clone());
                        out.swarm(&MeshMsg::Presence { id: my_id_bytes, neighbors: list.clone() });
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
                        out.swarm(&MeshMsg::Version { id: my_id_bytes, proto: PROTOCOL_VERSION });
                        out.swarm(&MeshMsg::Presence { id: my_id_bytes, neighbors: list.clone() });
                        rejoin.on_neighbors(list.len(), now_ms());
                        report_status(&mut last_status, list.len(), ever);
                    }
                    Event::Lagged => {
                        if last_lag_log.is_none_or(|t| t.elapsed() >= Duration::from_secs(10)) {
                            eprintln!("[mesh] receiver lagged — gossip dropped messages; anti-entropy will repair");
                            last_lag_log = Some(std::time::Instant::now());
                        }
                        ae.on_loss(ae_now());
                    }
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

    /// The host file is owner-only whichever way it comes to be: minted over a
    /// corrupt world-readable file (replaced, never chmod-ed after the fact),
    /// or resumed after being loosened. No tmp file is left behind.
    #[cfg(unix)]
    #[test]
    fn host_file_is_owner_only_when_minted_or_resumed() {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        let d = dir("host-perms");
        let host = d.join(HOST_FILE);
        std::fs::write(&host, "corrupt\n").unwrap();
        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o644)).unwrap();
        let (sk, _) = load_or_create_host(&d, false).unwrap();
        assert_eq!(mode(&host), 0o600, "minted owner-only");
        assert!(!d.join(format!("{HOST_FILE}.tmp")).exists(), "no tmp left behind");

        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o644)).unwrap();
        let (sk2, _) = load_or_create_host(&d, false).unwrap();
        assert_eq!(sk2.to_bytes(), sk.to_bytes(), "resumed, not re-minted");
        assert_eq!(mode(&host), 0o600, "a loosened file is tightened on resume");
        std::fs::remove_dir_all(&d).ok();
    }

    /// A `--restricted` host resumes only with a valid persisted trust log founded by
    /// this `.device`; a rejected file refuses even on a first run.
    #[test]
    fn restricted_resume_rule() {
        let d = dir("restricted-resume");
        let me = Some([1u8; 32]);
        let restored = AuthRestore::Restored { trust: 1, epochs: 1 };
        let rejected = AuthRestore::Rejected("x".into());
        assert!(check_restricted_resume(&d, false, &AuthRestore::Absent, None, me, 0).is_ok(), "first run");
        assert!(check_restricted_resume(&d, false, &AuthRestore::Skipped, None, me, 0).is_ok());
        let err = check_restricted_resume(&d, true, &AuthRestore::Absent, None, me, 0).expect_err("nothing persisted");
        assert!(err.to_string().contains("rerun with --new-board"), "the error says how to proceed: {err}");
        assert!(check_restricted_resume(&d, true, &rejected, None, me, 0).is_err());
        assert!(check_restricted_resume(&d, false, &rejected, None, me, 0).is_err(), "a rejected file always refuses");
        assert!(check_restricted_resume(&d, true, &restored, Some([1u8; 32]), me, 0).is_ok(), "our own board resumes");
        assert!(check_restricted_resume(&d, true, &restored, Some([2u8; 32]), me, 0).is_err(), "another founder");
        assert!(check_restricted_resume(&d, true, &restored, None, me, 0).is_err(), "no genesis");
        assert!(check_restricted_resume(&d, true, &restored, Some([1u8; 32]), me, 2).is_ok(), "at the mark");
        assert!(check_restricted_resume(&d, true, &restored, Some([1u8; 32]), me, 3).is_err(), "below the mark");
        std::fs::remove_dir_all(&d).ok();
    }

    /// The auth.state high-water mark lives on line 3 of the host file: it only
    /// rises, and writing it keeps the host identity intact.
    #[test]
    fn auth_mark_only_rises_and_keeps_the_host_identity() {
        let d = dir("auth-mark");
        assert_eq!(read_auth_mark(&d), 0, "no host file");
        assert!(raise_auth_mark(&d, 3).is_err(), "no host file to record it in");
        let (sk, secret) = load_or_create_host(&d, false).unwrap();
        assert_eq!(read_auth_mark(&d), 0);
        raise_auth_mark(&d, 5).unwrap();
        raise_auth_mark(&d, 2).unwrap();
        assert_eq!(read_auth_mark(&d), 5, "never lowered");
        assert!(host_resumes(&d, false));
        let (sk2, secret2) = load_or_create_host(&d, false).unwrap();
        assert_eq!((sk2.to_bytes(), secret2), (sk.to_bytes(), secret), "same board");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(d.join(HOST_FILE)).unwrap().permissions().mode() & 0o777, 0o600);
        }
        load_or_create_host(&d, true).unwrap();
        assert_eq!(read_auth_mark(&d), 0, "--new-board starts a new mark");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Restoring an older copy of auth.state (from before a live revoke) is refused
    /// on resume: it holds fewer items than the mark the host recorded.
    #[test]
    fn an_older_auth_state_copy_is_refused_on_resume() {
        use riftpipe_core::identity::{DeviceKey, Role};
        let d = dir("auth-rollback");
        let (_, secret) = load_or_create_host(&d, false).unwrap();
        let board = topic_from_secret(&secret);
        let f_key = DeviceKey::from_seed(&[1u8; 32]);
        let x = DeviceKey::from_seed(&[6u8; 32]).id();
        let mut host = TreePeer::new();
        host.set_identity(DeviceKey::from_seed(&[1u8; 32]), true);
        host.set_board(board);
        host.track_auth_mark();
        host.add_trust(f_key.genesis());
        host.add_trust(f_key.grant(x, Role::Member, [1u8; 16]));
        let _ = host.startup();
        host.save_auth(&d);
        let path = crate::sync::auth_store::auth_path(&d, &board);
        let old = std::fs::read(&path).unwrap();
        assert_eq!(read_auth_mark(&d), 3, "genesis + grant + epoch 1");
        assert!(!host.revoke_device(x, &d).is_empty());
        assert!(read_auth_mark(&d) > 3);

        let resume = |d: &Path| {
            let mut p = TreePeer::new();
            p.set_identity(DeviceKey::from_seed(&[1u8; 32]), true);
            p.set_board(board);
            let r = p.restore_auth(d);
            check_restricted_resume(d, true, &r, p.founder(), p.device_id(), read_auth_mark(d))
        };
        assert!(resume(&d).is_ok(), "the current file resumes");
        std::fs::write(&path, &old).unwrap();
        let err = resume(&d).expect_err("an older copy is refused");
        assert!(err.to_string().contains("older copy of auth.state"), "{err}");
        assert!(err.to_string().contains("rerun with --new-board"), "{err}");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Only a parseable host file, without `--new-board`, is a resume.
    #[test]
    fn host_resumes_only_a_parseable_host_file() {
        let d = dir("host-resumes");
        assert!(!host_resumes(&d, false), "empty dir");
        load_or_create_host(&d, false).unwrap();
        assert!(host_resumes(&d, false));
        assert!(!host_resumes(&d, true), "--new-board");
        std::fs::write(d.join(HOST_FILE), "corrupt\n").unwrap();
        assert!(!host_resumes(&d, false), "a corrupt host file is re-minted, not resumed");
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

    /// A clean shutdown's flush waits for everything queued — including a
    /// message pushed in the last tick — and gives up after its limit when the
    /// sender stalls.
    #[tokio::test]
    async fn shutdown_flush_sends_everything_queued() {
        let out = Out::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let _drain = spawn_drain_with(out.clone(), move |_, b| {
            let tx = tx.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                let _ = tx.send(b);
            }
        });
        for i in 0..5u8 {
            out.swarm(&MeshMsg::Version { id: [i; 32], proto: PROTOCOL_VERSION });
        }
        assert!(out.flush(Duration::from_secs(5)).await, "flushed within the limit");
        let mut got = 0;
        while rx.try_recv().is_ok() {
            got += 1;
        }
        assert_eq!(got, 5, "every queued message went out before flush returned");

        let stalled = Out::new();
        let _stuck = spawn_drain_with(stalled.clone(), |_, _| std::future::pending::<()>());
        stalled.swarm(&MeshMsg::Version { id: [9; 32], proto: PROTOCOL_VERSION });
        assert!(!stalled.flush(Duration::from_millis(100)).await, "a stalled sender can't hold shutdown");
    }
}
