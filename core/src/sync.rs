//! Platform-agnostic per-file tree sync: the wire messages + merge state, shared
//! by the browser (`riftpipe-web`) and native (`riftpipe`), so both ends speak ONE
//! protocol over whatever transport (`TreeSync` on iroh or WebRTC).
//!
//! Text files (`*.md`) are eg-walker CRDTs synced as
//! **events**: peers exchange version vectors on connect, then ship only the ops
//! the other lacks (`ops_since` / `encode_delta`) — not the whole history every
//! edit. First-connect and reconnect are the same operation ("send me everything
//! since version X"; a fresh peer is X = empty). Structural (non-text) files are
//! last-writer-wins; the same connect handshake carries their `(path, version)`
//! inventory, so a peer that lacks or is stale on one receives it immediately —
//! no re-touch needed. No I/O, no clock — callers persist the result and pass a
//! millisecond `now` for LWW versions.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::text::EgWalkerText;

/// THE text-vs-LWW classification: `.md` files merge as text CRDTs, everything
/// else as whole-file LWW. Single-sourced here because every peer must agree —
/// the same path riding both channels (one peer pushing text deltas, another
/// LWW) gives two conflicting merge semantics for one file. Native drivers,
/// the browser's priming, and the browser's live-push entry all call this;
/// apps never pick a channel themselves.
pub fn is_text_path(path: &str) -> bool {
    path.ends_with(".md")
}

/// The riftpipe wire-protocol version, announced to peers so a mismatch can
/// be SURFACED (a warning in the app / on stderr) instead of silently
/// degrading. Bump on any protocol change worth telling users about.
/// v0 is implicit — peers that predate versioning never announce (detected
/// via [`VersionMonitor::on_presence`]). v1: the strategy channel
/// (`StratAdvert`/`StratDelta`), mesh chunking, and version announcements.
/// v2: mesh security (`crypto`) — topic = `H(secret)` and AEAD-sealed frames;
/// a v2 board's topic differs from v1's, so v1/v2 peers land on different
/// topics and simply don't meet (changing the rendezvous is inherent to
/// blinding it), rather than meeting and mismatching.
pub const PROTOCOL_VERSION: u32 = 2;

/// Per-chunk payload budget for [`encode_mesh_frames`]. iroh-gossip's message
/// limit defaults to 4096 bytes and is enforced on SEND AND RECEIVE — so
/// rather than raising it (which would make every already-deployed peer error
/// on receiving a bigger frame, forcing reloads), we keep EVERY frame under
/// the default and split big messages into [`MeshMsg::Chunk`]s. A peer that
/// predates chunking skips the unknown variant cleanly — it just misses big
/// files, exactly as it always did. 3800 leaves room for the chunk envelope
/// (postcard varints + id) and the gossip header inside 4096.
pub const MESH_CHUNK_PAYLOAD: usize = 3800;
/// Reassembly cap: reject a `total` beyond this (~15 MiB) so a bogus header
/// can't balloon memory.
const MESH_MAX_CHUNKS: u32 = 4096;
/// Concurrent in-flight reassemblies per peer; oldest is evicted beyond this.
const MESH_ASSEMBLY_SLOTS: usize = 32;

/// A compact per-document state vector: `(agent, seq)` tips (usually one or two).
type Vv = Vec<(String, usize)>;
/// A document's origin (root op id) — the reliable "same document?" identity.
type Origin = Option<(String, usize)>;

/// WIRE COMPAT: postcard encodes by variant index, so variant order and field
/// shapes are load-bearing — deployed peers already speak this layout. New
/// variants may only be APPENDED (an old peer fails to decode an unknown index
/// and skips the message — every driver already does); existing variants must
/// never change shape or position. Guarded by `wire_compat` tests below.
#[derive(Serialize, Deserialize)]
pub enum SyncMsg {
    /// Connect handshake: "here are the version vectors of the text docs I have,
    /// and the LWW versions of my structural files — reply with anything I'm
    /// missing or stale on." Sent by both peers on connect (empty lists are valid
    /// and still solicit the other's files).
    Hello { versions: Vec<(String, Vv)>, lww_versions: Vec<(String, u64)> },
    /// A text delta: the ops the recipient lacks for `path`, the sender's version
    /// vector (so the recipient learns what the sender has), and the doc's origin
    /// (so a same-path but independently-created doc is detected, not interleaved).
    TextDelta { path: String, ops: Vec<u8>, vv: Vv, origin: Origin },
    /// "I couldn't apply your delta for `path` (missing an ancestor) — send me the
    /// full self-contained state." The recovery path when a delta can't bridge.
    Resync { path: String },
    /// A whole-file last-writer-wins update; newest version wins.
    Lww { path: String, version: u64, bytes: Vec<u8> },
    /// The generic **strategy channel** (folder mode's multi-algorithm sync —
    /// rsync, wal-db, lww-record, …): an advertisement of what the sender holds
    /// for `path`, as the resource's algorithm encodes it (`sv` is opaque —
    /// a version vector, block checksums, a WAL watermark). `reply: true` asks
    /// the peer to advertise back (sent on connect, local change, heartbeat);
    /// `reply: false` is that answer, so the exchange can't ping-pong forever.
    /// [`Syncer`] ignores these — the workspace/strategy layer drives them.
    StratAdvert { path: String, sv: Vec<u8>, reply: bool },
    /// A strategy-channel delta for `path`, opaque to the wire: whatever the
    /// resource's algorithm `merge`s (rsync token stream, WAL frames, …).
    StratDelta { path: String, bytes: Vec<u8> },
    /// "I speak protocol `proto`" — sent alongside the connect handshake so
    /// the driver can warn when the peer's version differs. [`Syncer`]
    /// ignores it; drivers intercept.
    Version { proto: u32 },
}

/// What travels over a gossip-mesh topic: tree [`SyncMsg`]s plus periodic
/// presence, so peers can build a routing map for debugging.
///
/// This is the single wire type for the mesh: both the native transport
/// (`src/sync/mesh.rs`) and the browser (`web/src/gossip.rs`) use it directly.
/// WIRE COMPAT: postcard encodes by variant index, so variant order and field
/// shapes are load-bearing — deployed browsers already speak this layout.
#[derive(Serialize, Deserialize)]
pub enum MeshMsg {
    /// A tree sync message, broadcast epidemically to the whole mesh.
    Sync(SyncMsg),
    /// "I am `id`, directly connected to `neighbors`." Aggregated into the map.
    Presence { id: [u8; 32], neighbors: Vec<[u8; 32]> },
    /// One piece of a message too big for a single gossip frame (the library
    /// caps frames at 4 KiB): `bytes` is `chunk_index`'th slice of an encoded
    /// `MeshMsg` split by [`encode_mesh_frames`]; [`MeshAssembler`] reassembles.
    /// `id` is a content hash, so a re-broadcast reuses the same reassembly
    /// slot. APPENDED variant: peers that predate it skip it cleanly.
    Chunk { id: u64, index: u32, total: u32, bytes: Vec<u8> },
    /// "Peer `id` speaks protocol `proto`" — broadcast right BEFORE every
    /// `Presence`, so a `Presence` from a peer that never announced marks a
    /// pre-versioning (v0) peer. Fed to [`VersionMonitor`]; drivers surface
    /// what it flags.
    Version { id: [u8; 32], proto: u32 },
}

/// FNV-1a 64-bit — a tiny, dependency-free content hash for chunk ids. Not
/// cryptographic; a collision only merges two identical-id reassemblies, and
/// the resulting decode failure is dropped like any corrupt frame.
fn fnv64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

/// Encode `msg` as ready-to-broadcast gossip frames: one plain frame when it
/// fits the per-frame budget, else a series of [`MeshMsg::Chunk`]s. Every
/// returned frame stays under iroh-gossip's 4 KiB default, so no peer — however
/// old — ever receives an over-limit frame.
pub fn encode_mesh_frames(msg: &MeshMsg) -> Vec<Vec<u8>> {
    let Ok(encoded) = postcard::to_allocvec(msg) else { return Vec::new() };
    if encoded.len() <= MESH_CHUNK_PAYLOAD {
        return vec![encoded];
    }
    let id = fnv64(&encoded);
    let total = encoded.len().div_ceil(MESH_CHUNK_PAYLOAD) as u32;
    encoded
        .chunks(MESH_CHUNK_PAYLOAD)
        .enumerate()
        .filter_map(|(index, part)| {
            postcard::to_allocvec(&MeshMsg::Chunk {
                id,
                index: index as u32,
                total,
                bytes: part.to_vec(),
            })
            .ok()
        })
        .collect()
}

/// One in-flight chunked message being reassembled.
struct Assembly {
    total: u32,
    parts: Vec<Option<Vec<u8>>>,
    received: u32,
}

/// Turns incoming gossip frames back into [`MeshMsg`]s: plain messages pass
/// straight through; [`MeshMsg::Chunk`]s are buffered per content id and the
/// whole message is returned when the last piece lands. Bounded: at most
/// [`MESH_ASSEMBLY_SLOTS`] concurrent reassemblies (oldest evicted — gossip is
/// best-effort, the catch-up paths repair a dropped message) and a `total` cap
/// so a bogus header can't balloon memory.
#[derive(Default)]
pub struct MeshAssembler {
    slots: HashMap<u64, Assembly>,
    /// Insertion order for eviction (ids, oldest first).
    order: Vec<u64>,
}

impl MeshAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one received frame; returns a complete message if this frame
    /// finished one (or WAS one). Undecodable frames return `None`.
    pub fn feed(&mut self, frame: &[u8]) -> Option<MeshMsg> {
        let msg = postcard::from_bytes::<MeshMsg>(frame).ok()?;
        let MeshMsg::Chunk { id, index, total, bytes } = msg else {
            return Some(msg);
        };
        if total == 0 || total > MESH_MAX_CHUNKS || index >= total {
            return None;
        }
        if !self.slots.contains_key(&id) {
            if self.order.len() >= MESH_ASSEMBLY_SLOTS {
                let evict = self.order.remove(0);
                self.slots.remove(&evict);
            }
            self.order.push(id);
            self.slots
                .insert(id, Assembly { total, parts: vec![None; total as usize], received: 0 });
        }
        let slot = self.slots.get_mut(&id)?;
        if slot.total != total {
            return None; // id collision with a different shape — drop
        }
        if slot.parts[index as usize].is_none() {
            slot.parts[index as usize] = Some(bytes);
            slot.received += 1;
        }
        if slot.received < slot.total {
            return None;
        }
        let assembly = self.slots.remove(&id)?;
        self.order.retain(|x| *x != id);
        let whole: Vec<u8> = assembly.parts.into_iter().flatten().flatten().collect();
        match postcard::from_bytes::<MeshMsg>(&whole).ok()? {
            // A chunked chunk is nonsense — refuse the recursion.
            MeshMsg::Chunk { .. } => None,
            inner => Some(inner),
        }
    }
}

/// One peer's announced protocol version (0 = never announced, i.e. a peer
/// that predates versioning).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerVersion {
    pub id: [u8; 32],
    pub proto: u32,
}

/// How long a peer may go announcement-less after its first `Presence` before
/// it is deemed pre-versioning (v0). Long enough that a reordered
/// Version/Presence pair can't false-flag; short enough to surface promptly.
pub const VERSION_GRACE_MS: u64 = 10_000;

/// Tracks peers' announced protocol versions and flags mismatches — shared by
/// the native and browser mesh drivers so the detection logic can't drift.
/// Pure state, no I/O or clock: callers pass a millisecond `now` and surface
/// what this returns (stderr line, browser event). Detection:
///   * a [`MeshMsg::Version`] announcing `proto != PROTOCOL_VERSION` flags
///     immediately;
///   * version-aware peers announce right BEFORE every `Presence`, so a peer
///     whose `Presence` arrives with no announcement on record is suspect —
///     flagged v0 on a SECOND bare presence (one gap tolerates gossip
///     reordering), or when [`VERSION_GRACE_MS`] passes without an
///     announcement (a quiet two-peer mesh may only ever see one presence) —
///     the driver calls [`tick`](Self::tick) periodically for that.
/// Each peer is flagged at most once.
#[derive(Default)]
pub struct VersionMonitor {
    /// Announced versions, per peer.
    versions: HashMap<[u8; 32], u32>,
    /// Peers seen via `Presence` only: (first-seen ms, presence count).
    presence_only: HashMap<[u8; 32], (u64, u8)>,
    /// Peers already reported — flag once.
    flagged: HashSet<[u8; 32]>,
}

impl VersionMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an announcement. Returns the peer the FIRST time it proves
    /// mismatched, for the driver to surface.
    pub fn on_version(&mut self, id: [u8; 32], proto: u32) -> Option<PeerVersion> {
        self.presence_only.remove(&id);
        self.versions.insert(id, proto);
        (proto != PROTOCOL_VERSION && self.flagged.insert(id)).then_some(PeerVersion { id, proto })
    }

    /// Record a `Presence` from `id` at `now` (ms). Returns the peer the first
    /// time it is deemed pre-versioning (v0).
    pub fn on_presence(&mut self, id: [u8; 32], now: u64) -> Option<PeerVersion> {
        if self.versions.contains_key(&id) {
            return None;
        }
        let (first, strikes) = self.presence_only.entry(id).or_insert((now, 0));
        *strikes = strikes.saturating_add(1);
        let overdue = now.saturating_sub(*first) >= VERSION_GRACE_MS;
        ((*strikes >= 2 || overdue) && self.flagged.insert(id))
            .then_some(PeerVersion { id, proto: 0 })
    }

    /// Flag every announcement-less peer whose grace expired by `now` (ms).
    /// Drivers call this on a periodic tick so a quiet mesh still surfaces a
    /// pre-versioning peer.
    pub fn tick(&mut self, now: u64) -> Vec<PeerVersion> {
        let overdue: Vec<[u8; 32]> = self
            .presence_only
            .iter()
            .filter(|(id, (first, _))| {
                now.saturating_sub(*first) >= VERSION_GRACE_MS && !self.flagged.contains(*id)
            })
            .map(|(id, _)| *id)
            .collect();
        overdue
            .into_iter()
            .filter(|id| self.flagged.insert(*id))
            .map(|id| PeerVersion { id, proto: 0 })
            .collect()
    }

    /// Every peer with a known-or-deduced version (announced, or flagged v0).
    /// Sorted by id for stable output.
    pub fn peers(&self) -> Vec<PeerVersion> {
        let mut v: Vec<PeerVersion> = self
            .versions
            .iter()
            .map(|(id, proto)| PeerVersion { id: *id, proto: *proto })
            .chain(
                self.flagged
                    .iter()
                    .filter(|id| !self.versions.contains_key(*id))
                    .map(|id| PeerVersion { id: *id, proto: 0 }),
            )
            .collect();
        v.sort_by_key(|p| p.id);
        v
    }

    /// The subset of [`peers`](Self::peers) whose version differs from ours.
    pub fn mismatched(&self) -> Vec<PeerVersion> {
        self.peers().into_iter().filter(|p| p.proto != PROTOCOL_VERSION).collect()
    }
}

/// While awaiting a resync, re-ask only after this many further failed merges:
/// the first failure is usually just the tail of the gapped-delta stream that
/// triggered the request; a second means the full state itself didn't land.
const RESYNC_FAILURES_PER_RETRY: u8 = 2;
/// Total full-resync requests per path (1 initial + 2 retries) before parking.
const MAX_RESYNC_REQUESTS: u8 = 3;

/// Progress of one path's full-resync recovery (see [`Syncer::resync`]).
struct ResyncState {
    /// Resync requests sent so far for this path.
    requests: u8,
    /// Failed merges since the most recent request.
    failures: u8,
}

/// Per-file sync state for one peer. Author under a **unique** `agent` (a shared
/// agent id would corrupt the CRDT).
pub struct Syncer {
    agent: String,
    docs: HashMap<String, EgWalkerText>,
    /// Our best knowledge of what the peer already has, per text path — so we send
    /// only deltas. Advanced optimistically on send and from the peer's reported vv.
    peer_vv: HashMap<String, Vv>,
    /// Paths mid-resync: we've asked the peer for a full resync and count failed
    /// merges since, so a stream of un-appliable deltas can't storm — and a full
    /// state that itself fails to merge (corrupt transport) gets a bounded number
    /// of re-requests instead of looping forever. Cleared on the first success.
    resync: HashMap<String, ResyncState>,
    /// Paths that exhausted the resync retry budget — parked (quiet) until some
    /// merge for them succeeds. Core does no I/O, so the breadcrumb is exposed
    /// via [`Syncer::parked_paths`] for an I/O-owning driver to report.
    parked: HashSet<String>,
    /// Per structural path: the LWW `(version, bytes)` — bytes cached so a new mesh
    /// neighbor can be caught up with them (`full_state`).
    lww: HashMap<String, (u64, Vec<u8>)>,
}

impl Syncer {
    pub fn new(agent: impl Into<String>) -> Self {
        Syncer {
            agent: agent.into(),
            docs: HashMap::new(),
            peer_vv: HashMap::new(),
            resync: HashMap::new(),
            parked: HashSet::new(),
            lww: HashMap::new(),
        }
    }

    fn doc(&mut self, path: &str) -> &mut EgWalkerText {
        let agent = &self.agent;
        self.docs
            .entry(path.to_string())
            .or_insert_with(|| EgWalkerText::new(agent))
    }

    /// Merge an incoming vv into our record of what the peer has (per-agent max, so
    /// an out-of-order or stale report never regresses what we know they hold).
    fn advance_peer_vv(&mut self, path: &str, incoming: &Vv) {
        let slot = self.peer_vv.entry(path.to_string()).or_default();
        for (agent, seq) in incoming {
            match slot.iter_mut().find(|(a, _)| a == agent) {
                Some(e) => e.1 = e.1.max(*seq),
                None => slot.push((agent.clone(), *seq)),
            }
        }
    }

    /// Messages to send on connect: advertise our text version vectors AND our
    /// LWW versions so the peer replies with what we're missing. Always send
    /// (even empty) so a fresh peer solicits the other's files.
    pub fn hello(&self) -> SyncMsg {
        let versions = self
            .docs
            .iter()
            .map(|(path, doc)| (path.clone(), doc.version_vector()))
            .collect();
        let lww_versions = self.lww.iter().map(|(path, (v, _))| (path.clone(), *v)).collect();
        SyncMsg::Hello { versions, lww_versions }
    }

    /// The cached LWW entries a peer with inventory `their` (path → version)
    /// needs: every entry it lacks or holds an older version of. LWW-safe by
    /// construction — the receiver's `apply(Lww)` still drops anything that
    /// doesn't beat its local version, so a newer local file is never regressed.
    fn lww_updates_for(&self, their: &HashMap<String, u64>) -> Vec<SyncMsg> {
        self.lww
            .iter()
            .filter(|(path, (version, _))| their.get(*path).is_none_or(|tv| *version > *tv))
            .map(|(path, (version, bytes))| SyncMsg::Lww {
                path: path.clone(),
                version: *version,
                bytes: bytes.clone(),
            })
            .collect()
    }

    /// Record a local text edit (snapshot diff-to-ops); returns a **delta** of the
    /// new ops to send, or `None` if the peer already has everything.
    pub fn local_text(&mut self, path: &str, content: &str) -> Option<SyncMsg> {
        let peer = self.peer_vv.get(path).cloned().unwrap_or_default();
        let doc = self.doc(path);
        doc.edit_to(content);
        let ops = doc.ops_since(&peer)?;
        let vv = doc.version_vector();
        let origin = doc.origin();
        self.advance_peer_vv(path, &vv); // optimistic: assume they'll get our ops
        Some(SyncMsg::TextDelta { path: path.to_string(), ops, vv, origin })
    }

    /// Full self-contained state of every text doc — broadcast to a newly-joined
    /// neighbor (gossip has no per-peer delta) so it catches up + merges.
    pub fn full_state(&self) -> Vec<SyncMsg> {
        let mut msgs: Vec<SyncMsg> = self
            .docs
            .iter()
            .filter_map(|(path, doc)| {
                doc.ops_since(&[]).map(|ops| SyncMsg::TextDelta {
                    path: path.clone(),
                    ops,
                    vv: doc.version_vector(),
                    origin: doc.origin(),
                })
            })
            .collect();
        msgs.extend(self.lww_updates_for(&HashMap::new()));
        msgs
    }

    /// Paths whose full-resync retry budget is exhausted — parked until a merge
    /// for them succeeds. Sorted for stable reporting. Core is no-I/O, so the
    /// driver (which owns stderr/metrics) turns a newly-parked path into a
    /// visible breadcrumb.
    pub fn parked_paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self.parked.iter().cloned().collect();
        v.sort();
        v
    }

    /// Record a local structural write; `now` is a millisecond clock.
    pub fn local_lww(&mut self, path: &str, bytes: Vec<u8>, now: u64) -> SyncMsg {
        let entry = self.lww.entry(path.to_string()).or_insert((0, Vec::new()));
        entry.0 = now.max(entry.0 + 1);
        entry.1 = bytes.clone();
        SyncMsg::Lww { path: path.to_string(), version: entry.0, bytes }
    }

    /// Apply a received message: returns `(bytes to persist, messages to send
    /// back)`. A `Hello` produces reply deltas; a `TextDelta`/`Lww` produces bytes.
    pub fn apply(&mut self, msg: SyncMsg) -> (Option<(String, Vec<u8>)>, Vec<SyncMsg>) {
        match msg {
            SyncMsg::Hello { versions, lww_versions } => {
                let their: HashMap<String, Vv> = versions.into_iter().collect();
                for (path, vv) in &their {
                    self.advance_peer_vv(path, vv);
                }
                // Reply with what the peer is missing from each of our docs.
                let mut replies = Vec::new();
                let paths: Vec<String> = self.docs.keys().cloned().collect();
                for path in paths {
                    let their_vv = their.get(&path).cloned().unwrap_or_default();
                    if let Some(ops) = self.docs.get(&path).unwrap().ops_since(&their_vv) {
                        let doc = self.docs.get(&path).unwrap();
                        let vv = doc.version_vector();
                        let origin = doc.origin();
                        self.advance_peer_vv(&path, &vv);
                        replies.push(SyncMsg::TextDelta { path, ops, vv, origin });
                    }
                }
                // LWW: ship any structural file the peer lacks or is stale on.
                // One round, no storms — a Hello never provokes another Hello,
                // and applying an Lww produces no replies.
                let their_lww: HashMap<String, u64> = lww_versions.into_iter().collect();
                replies.extend(self.lww_updates_for(&their_lww));
                (None, replies)
            }
            SyncMsg::TextDelta { path, ops, vv, origin } => {
                // Independently-created conflict on a file we already hold: same
                // path, DIFFERENT origin (root op). Don't naively merge — that
                // interleaves two unrelated texts. Resolve deterministically by
                // origin agent id (higher wins; both converge to it). Distinct paths
                // never hit this, so separate documents union; a shared origin (normal
                // concurrent editing) falls through to a clean CRDT merge.
                if let (Some(mine), Some(theirs)) =
                    (self.docs.get(&path).and_then(|d| d.origin()), origin.as_ref())
                {
                    if mine != *theirs {
                        if mine.0.as_str() < theirs.0.as_str() {
                            // We lose — adopt their version (their ops are a
                            // self-contained full on a fresh conflict).
                            let mut fresh = EgWalkerText::new(&self.agent);
                            if fresh.merge(&ops) {
                                let content = fresh.content().into_bytes();
                                self.docs.insert(path.clone(), fresh);
                                self.advance_peer_vv(&path, &vv);
                                return (Some((path, content)), Vec::new());
                            }
                            return (None, vec![SyncMsg::Resync { path }]);
                        }
                        // We win — keep ours, ignore their ops (they adopt ours).
                        self.advance_peer_vv(&path, &vv);
                        return (None, Vec::new());
                    }
                }
                if !self.doc(&path).merge(&ops) {
                    // Missing a causal ancestor (or corrupt bytes). Ask for a full
                    // self-contained resync; suppress repeats while it's pending so
                    // a run of un-appliable deltas can't cause a resync storm. If
                    // the full state itself keeps failing to merge, re-ask a bounded
                    // number of times, then PARK the path (visible via
                    // `parked_paths`) instead of looping forever.
                    if self.parked.contains(&path) {
                        return (None, Vec::new()); // gave up; stay quiet
                    }
                    let Some(st) = self.resync.get_mut(&path) else {
                        self.resync
                            .insert(path.clone(), ResyncState { requests: 1, failures: 0 });
                        return (None, vec![SyncMsg::Resync { path }]);
                    };
                    st.failures += 1;
                    if st.failures < RESYNC_FAILURES_PER_RETRY {
                        return (None, Vec::new()); // suppressed while awaiting
                    }
                    if st.requests >= MAX_RESYNC_REQUESTS {
                        // Retry budget exhausted — park and go quiet.
                        self.resync.remove(&path);
                        self.parked.insert(path);
                        return (None, Vec::new());
                    }
                    st.requests += 1;
                    st.failures = 0;
                    return (None, vec![SyncMsg::Resync { path }]);
                }
                self.resync.remove(&path);
                self.parked.remove(&path); // any successful merge un-parks
                let content = self.doc(&path).content().into_bytes();
                self.advance_peer_vv(&path, &vv);
                (Some((path, content)), Vec::new())
            }
            SyncMsg::Resync { path } => match self.docs.get(&path) {
                // Answer with our full state; the peer merges it to recover no
                // matter what it was missing.
                Some(doc) => {
                    let ops = doc.encode_full();
                    let vv = doc.version_vector();
                    let origin = doc.origin();
                    self.advance_peer_vv(&path, &vv);
                    (None, vec![SyncMsg::TextDelta { path, ops, vv, origin }])
                }
                None => (None, Vec::new()),
            },
            SyncMsg::Lww { path, version, bytes } => {
                let entry = self.lww.entry(path.clone()).or_insert((0, Vec::new()));
                if version > entry.0 {
                    entry.0 = version;
                    entry.1 = bytes.clone();
                    (Some((path, bytes)), Vec::new())
                } else {
                    (None, Vec::new())
                }
            }
            // The strategy channel belongs to the workspace/strategy layer
            // (folder mode), and version announcements to the driver; a
            // text/LWW syncer has nothing to do with either.
            SyncMsg::StratAdvert { .. } | SyncMsg::StratDelta { .. } | SyncMsg::Version { .. } => {
                (None, Vec::new())
            }
        }
    }
}

#[cfg(test)]
mod wire_compat {
    use super::*;

    /// Postcard encodes an enum as a varint of its variant index, so the FIRST
    /// byte of every encoded `SyncMsg` is its wire index (all six fit in one
    /// varint byte). Deployed peers already speak indexes 0–3 — this pins the
    /// full table so a reorder/insertion fails loudly, not on live browsers.
    #[test]
    fn variant_indexes_are_pinned() {
        let cases: Vec<(SyncMsg, u8)> = vec![
            (SyncMsg::Hello { versions: vec![], lww_versions: vec![] }, 0),
            (SyncMsg::TextDelta { path: "p".into(), ops: vec![], vv: vec![], origin: None }, 1),
            (SyncMsg::Resync { path: "p".into() }, 2),
            (SyncMsg::Lww { path: "p".into(), version: 1, bytes: vec![] }, 3),
            (SyncMsg::StratAdvert { path: "p".into(), sv: vec![], reply: true }, 4),
            (SyncMsg::StratDelta { path: "p".into(), bytes: vec![] }, 5),
            (SyncMsg::Version { proto: 1 }, 6),
        ];
        for (msg, index) in cases {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            assert_eq!(bytes[0], index, "variant index moved — deployed peers would misdecode");
        }
    }

    /// An old peer (built before the strategy channel) decodes the appended
    /// variants as an ERROR, not as garbage — and every driver skips messages
    /// that fail to decode, so mixed-version meshes stay safe.
    #[test]
    fn legacy_decoder_rejects_appended_variants_cleanly() {
        /// The wire enum exactly as deployed peers know it.
        #[derive(serde::Deserialize)]
        enum LegacySyncMsg {
            #[allow(dead_code)]
            Hello { versions: Vec<(String, Vv)>, lww_versions: Vec<(String, u64)> },
            #[allow(dead_code)]
            TextDelta { path: String, ops: Vec<u8>, vv: Vv, origin: Origin },
            #[allow(dead_code)]
            Resync { path: String },
            #[allow(dead_code)]
            Lww { path: String, version: u64, bytes: Vec<u8> },
        }

        let advert = SyncMsg::StratAdvert { path: "a.bin".into(), sv: vec![1, 2], reply: true };
        let delta = SyncMsg::StratDelta { path: "a.bin".into(), bytes: vec![3] };
        for msg in [advert, delta] {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            assert!(
                postcard::from_bytes::<LegacySyncMsg>(&bytes).is_err(),
                "legacy peers must reject (and thus skip) strategy-channel messages",
            );
        }
        // And a legacy-shaped message still round-trips through the new enum.
        let hello = postcard::to_allocvec(&SyncMsg::Hello { versions: vec![], lww_versions: vec![] }).unwrap();
        assert!(matches!(
            postcard::from_bytes::<SyncMsg>(&hello),
            Ok(SyncMsg::Hello { .. })
        ));
    }

    /// Pin `MeshMsg`'s variant indexes too (Sync=0, Presence=1, Chunk=2) and
    /// prove a pre-chunking peer skips `Chunk` frames cleanly — that's what
    /// lets deployed browser tabs coexist with chunking peers, no reload.
    #[test]
    fn mesh_msg_indexes_pinned_and_legacy_peers_skip_chunks() {
        let cases: Vec<(MeshMsg, u8)> = vec![
            (MeshMsg::Sync(SyncMsg::Resync { path: "p".into() }), 0),
            (MeshMsg::Presence { id: [0; 32], neighbors: vec![] }, 1),
            (MeshMsg::Chunk { id: 1, index: 0, total: 2, bytes: vec![9] }, 2),
            (MeshMsg::Version { id: [0; 32], proto: 1 }, 3),
        ];
        for (msg, index) in cases {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            assert_eq!(bytes[0], index, "MeshMsg variant index moved");
        }

        /// The mesh wire enum exactly as pre-chunking peers know it.
        #[derive(serde::Deserialize)]
        enum LegacyMeshMsg {
            #[allow(dead_code)]
            Sync(SyncMsg),
            #[allow(dead_code)]
            Presence { id: [u8; 32], neighbors: Vec<[u8; 32]> },
        }
        let chunk = MeshMsg::Chunk { id: 7, index: 0, total: 1, bytes: vec![1, 2] };
        let version = MeshMsg::Version { id: [1; 32], proto: 1 };
        for msg in [chunk, version] {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            assert!(
                postcard::from_bytes::<LegacyMeshMsg>(&bytes).is_err(),
                "legacy peers must reject (and thus skip) post-v0 frames",
            );
        }
    }

    /// The mismatch detector: an announced different version flags once;
    /// presence-without-announcement flags v0 on the second strike OR after
    /// the grace period; matching versions never flag; an announcement clears
    /// suspicion.
    #[test]
    fn version_monitor_flags_mismatches_and_v0_peers() {
        let mut m = VersionMonitor::new();
        let (a, b, c) = ([1u8; 32], [2u8; 32], [3u8; 32]);

        // Matching version: never flagged.
        assert!(m.on_version(a, PROTOCOL_VERSION).is_none());
        assert!(m.on_presence(a, 0).is_none() && m.on_presence(a, 0).is_none());

        // Different version: flagged exactly once.
        assert_eq!(m.on_version(b, 99), Some(PeerVersion { id: b, proto: 99 }));
        assert!(m.on_version(b, 99).is_none(), "flag once");

        // Presence-only peer: first strike tolerated (reordering), second flags v0.
        assert!(m.on_presence(c, 0).is_none());
        assert_eq!(m.on_presence(c, 1), Some(PeerVersion { id: c, proto: 0 }));
        assert!(m.on_presence(c, 2).is_none(), "flag once");

        assert_eq!(
            m.mismatched(),
            vec![PeerVersion { id: b, proto: 99 }, PeerVersion { id: c, proto: 0 }],
        );
        assert_eq!(m.peers().len(), 3);

        // A late announcement upgrades a suspect before the second strike.
        let mut m2 = VersionMonitor::new();
        assert!(m2.on_presence(a, 0).is_none());
        assert!(m2.on_version(a, PROTOCOL_VERSION).is_none());
        assert!(m2.on_presence(a, 1).is_none(), "announced peer never strikes");
        assert!(m2.mismatched().is_empty());
    }

    /// A quiet two-peer mesh sees only ONE presence from an old host — the
    /// grace tick must still flag it, and must not flag inside the grace.
    #[test]
    fn version_monitor_grace_tick_flags_quiet_v0_peer() {
        let mut m = VersionMonitor::new();
        let old = [7u8; 32];
        assert!(m.on_presence(old, 1_000).is_none(), "single presence: not yet");
        assert!(m.tick(1_000 + VERSION_GRACE_MS - 1).is_empty(), "inside grace");
        assert_eq!(
            m.tick(1_000 + VERSION_GRACE_MS),
            vec![PeerVersion { id: old, proto: 0 }],
            "grace expiry flags the quiet pre-versioning peer",
        );
        assert!(m.tick(1_000 + 2 * VERSION_GRACE_MS).is_empty(), "flag once");

        // An announcement inside the grace prevents the flag entirely.
        let mut m2 = VersionMonitor::new();
        let new = [8u8; 32];
        assert!(m2.on_presence(new, 1_000).is_none());
        assert!(m2.on_version(new, PROTOCOL_VERSION).is_none());
        assert!(m2.tick(1_000 + 2 * VERSION_GRACE_MS).is_empty(), "announced in time");
    }

    /// The text/LWW `Syncer` ignores the strategy channel: no persist, no reply.
    #[test]
    fn syncer_ignores_strategy_channel_messages() {
        let mut s = Syncer::new("a");
        s.local_text("note.md", "hi\n");
        let (persist, replies) =
            s.apply(SyncMsg::StratAdvert { path: "x.bin".into(), sv: vec![], reply: true });
        assert!(persist.is_none() && replies.is_empty());
        let (persist, replies) =
            s.apply(SyncMsg::StratDelta { path: "x.bin".into(), bytes: vec![1] });
        assert!(persist.is_none() && replies.is_empty());
    }
}

#[cfg(test)]
mod chunking {
    use super::*;

    fn big_sync_msg() -> MeshMsg {
        // ~20 KiB of low-compressibility payload rides an Lww so the encoded
        // frame decisively exceeds one chunk.
        let mut bytes = Vec::with_capacity(20_000);
        let mut x: u32 = 0x1234_5678;
        for _ in 0..20_000 {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            bytes.push((x >> 24) as u8);
        }
        MeshMsg::Sync(SyncMsg::Lww { path: "big.bin".into(), version: 9, bytes })
    }

    #[test]
    fn small_messages_stay_plain_frames() {
        let msg = MeshMsg::Sync(SyncMsg::Resync { path: "p".into() });
        let frames = encode_mesh_frames(&msg);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], postcard::to_allocvec(&msg).unwrap(), "byte-identical to plain");
        assert!(matches!(
            MeshAssembler::new().feed(&frames[0]),
            Some(MeshMsg::Sync(SyncMsg::Resync { .. }))
        ));
    }

    #[test]
    fn big_message_chunks_under_the_limit_and_reassembles() {
        let msg = big_sync_msg();
        let frames = encode_mesh_frames(&msg);
        assert!(frames.len() > 1, "a 20 KiB message must chunk");
        for f in &frames {
            assert!(f.len() <= 4096, "every frame under iroh-gossip's default: {}", f.len());
        }
        let mut asm = MeshAssembler::new();
        let mut out = None;
        for f in &frames {
            let got = asm.feed(f);
            assert!(out.is_none() || got.is_none(), "completes exactly once");
            out = out.or(got);
        }
        match out.expect("reassembled") {
            MeshMsg::Sync(SyncMsg::Lww { path, version, bytes }) => {
                assert_eq!((path.as_str(), version, bytes.len()), ("big.bin", 9, 20_000));
            }
            _ => panic!("wrong message out"),
        }
    }

    #[test]
    fn out_of_order_and_interleaved_senders_reassemble() {
        let a = big_sync_msg();
        let b = MeshMsg::Sync(SyncMsg::Lww {
            path: "other.bin".into(),
            version: 1,
            bytes: vec![0xAB; 10_000],
        });
        let (mut fa, fb) = (encode_mesh_frames(&a), encode_mesh_frames(&b));
        fa.reverse(); // out of order
        let mut asm = MeshAssembler::new();
        let mut done = 0;
        // Interleave the two chunk streams.
        let mut it_a = fa.iter();
        let mut it_b = fb.iter();
        loop {
            let (na, nb) = (it_a.next(), it_b.next());
            for f in [na, nb].into_iter().flatten() {
                if asm.feed(f).is_some() {
                    done += 1;
                }
            }
            if na.is_none() && nb.is_none() {
                break;
            }
        }
        assert_eq!(done, 2, "both interleaved messages reassemble");
    }

    #[test]
    fn duplicate_chunks_and_rebroadcasts_are_idempotent() {
        let frames = encode_mesh_frames(&big_sync_msg());
        let mut asm = MeshAssembler::new();
        // Feed the first chunk twice, then the rest — still completes once.
        assert!(asm.feed(&frames[0]).is_none());
        assert!(asm.feed(&frames[0]).is_none());
        let mut out = 0;
        for f in &frames[1..] {
            if asm.feed(f).is_some() {
                out += 1;
            }
        }
        assert_eq!(out, 1);
        // A full re-broadcast (same content hash id) reassembles again fine.
        let mut again = 0;
        for f in &frames {
            if asm.feed(f).is_some() {
                again += 1;
            }
        }
        assert_eq!(again, 1, "re-broadcast reuses the id and still completes");
    }

    #[test]
    fn bogus_headers_and_junk_are_dropped() {
        let mut asm = MeshAssembler::new();
        assert!(asm.feed(b"\xff\xff\xff").is_none(), "junk frame");
        let zero = MeshMsg::Chunk { id: 1, index: 0, total: 0, bytes: vec![] };
        assert!(asm.feed(&postcard::to_allocvec(&zero).unwrap()).is_none(), "total=0");
        let huge = MeshMsg::Chunk { id: 2, index: 0, total: u32::MAX, bytes: vec![1] };
        assert!(asm.feed(&postcard::to_allocvec(&huge).unwrap()).is_none(), "total cap");
        let oob = MeshMsg::Chunk { id: 3, index: 5, total: 2, bytes: vec![1] };
        assert!(asm.feed(&postcard::to_allocvec(&oob).unwrap()).is_none(), "index >= total");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a message into `dst`, persisting + forwarding replies until quiescent.
    /// Returns the last persisted bytes for `path`, if any.
    fn deliver(dst: &mut Syncer, src_to_dst: SyncMsg) -> Vec<SyncMsg> {
        let (_, replies) = dst.apply(src_to_dst);
        replies
    }

    #[test]
    fn first_edit_transfers_then_edits_are_small_deltas() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");

        // High-entropy content so the "full" encode doesn't just compress away
        // (the encoder compresses content, which would mask the delta size).
        let mut big = String::with_capacity(4000);
        let mut x: u32 = 0x9e3779b9;
        for _ in 0..4000 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            big.push(char::from(33 + (x % 94) as u8));
        }

        // First edit: peer has nothing, so the delta carries the whole doc.
        let m1 = a.local_text("note.md", &big).unwrap();
        let first_len = match &m1 {
            SyncMsg::TextDelta { ops, .. } => ops.len(),
            _ => panic!("expected delta"),
        };
        assert_eq!(b.apply(m1).0.unwrap().1, big.as_bytes());

        // A second small edit AFTER the peer is caught up must ship a tiny delta,
        // not the whole 4000-char document again.
        let m2 = a.local_text("note.md", &(big.clone() + "Y")).unwrap();
        let second_len = match &m2 {
            SyncMsg::TextDelta { ops, .. } => ops.len(),
            _ => panic!("expected delta"),
        };
        assert!(
            second_len < first_len / 10,
            "steady-state edit should be a small delta: {second_len} vs first {first_len}",
        );
        assert_eq!(b.apply(m2).0.unwrap().1, (big + "Y").into_bytes());
    }

    #[test]
    fn concurrent_edits_converge() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        // Shared base.
        let m = a.local_text("index.md", "# B\n\n- Todo\n").unwrap();
        b.apply(m);

        // Concurrent edits from the same base.
        let ma = a.local_text("index.md", "# B\n\n- Todo\n- A\n").unwrap();
        let mb = b.local_text("index.md", "# B\n\n- Todo\n- Bee\n").unwrap();
        let on_b = b.apply(ma).0.unwrap().1;
        let on_a = a.apply(mb).0.unwrap().1;
        assert_eq!(on_a, on_b, "concurrent edits converge");
    }

    #[test]
    fn hello_transfers_existing_tree_to_a_fresh_peer() {
        // A already has a tree; B connects fresh and must receive it via the
        // version-vector handshake (no edit needed).
        let mut a = Syncer::new("a");
        a.local_text("note.md", "# existing note\n");

        let mut b = Syncer::new("b");
        // B says hello (empty) → A replies with the deltas B lacks.
        let replies = deliver(&mut a, b.hello());
        assert_eq!(replies.len(), 1, "A offers its one doc");
        let (persisted, _) = b.apply(replies.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"# existing note\n");
    }

    #[test]
    fn reconnect_replays_only_the_gap() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        b.apply(a.local_text("note.md", "line 1\n").unwrap()); // synced

        // "Disconnect": A makes several edits B never sees.
        a.local_text("note.md", "line 1\nline 2\n");
        a.local_text("note.md", "line 1\nline 2\nline 3\n");
        let full = a.docs.get("note.md").unwrap().encode_full().len();

        // Reconnect: B advertises its (stale) version vector; A replies with only
        // the ops since then — smaller than the whole history.
        let replies = deliver(&mut a, b.hello());
        let gap = match &replies[0] {
            SyncMsg::TextDelta { ops, .. } => ops.len(),
            _ => panic!(),
        };
        assert!(gap < full, "reconnect ships only the gap ({gap}) not full ({full})");
        b.apply(replies.into_iter().next().unwrap());
        assert_eq!(
            b.docs.get("note.md").unwrap().content(),
            "line 1\nline 2\nline 3\n",
            "B caught up to A after reconnect",
        );
    }

    #[test]
    fn lww_newest_wins() {
        let mut b = Syncer::new("b");
        let mut a = Syncer::new("a");
        let newer = a.local_lww("state.bin", b"new".to_vec(), 1000);
        assert_eq!(b.apply(newer).0.unwrap().1, b"new");
        let stale = SyncMsg::Lww { path: "state.bin".into(), version: 1, bytes: b"old".to_vec() };
        assert!(b.apply(stale).0.is_none(), "stale LWW ignored");
    }

    #[test]
    fn hello_transfers_existing_lww_to_a_fresh_peer() {
        // A already holds a structural file; B joins fresh over the point-to-point
        // handshake and must receive it through hello/apply alone — no re-touch.
        let mut a = Syncer::new("a");
        a.local_lww("tickets/tk_1/meta.toml", b"column = \"Doing\"\n".to_vec(), 1000);

        let mut b = Syncer::new("b");
        let replies = deliver(&mut a, b.hello());
        assert_eq!(replies.len(), 1, "A offers its one LWW file");
        let (persisted, more) = b.apply(replies.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"column = \"Doing\"\n");
        assert!(more.is_empty(), "applying an Lww never produces replies (no storm)");
    }

    #[test]
    fn hello_updates_stale_lww_peer() {
        // B holds an old version of meta.toml; A's is newer. B's hello advertises
        // its version, so A ships only the fresher payload and B adopts it.
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        b.apply(a.local_lww("meta.toml", b"column = \"Todo\"\n".to_vec(), 1000));
        a.local_lww("meta.toml", b"column = \"Done\"\n".to_vec(), 2000); // B never sees this

        let replies = deliver(&mut a, b.hello());
        assert_eq!(replies.len(), 1, "A ships the newer LWW payload");
        let (persisted, _) = b.apply(replies.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"column = \"Done\"\n", "stale peer caught up");
    }

    #[test]
    fn hello_never_clobbers_newer_local_lww() {
        // B's local meta.toml is NEWER than A's. Neither direction of the
        // handshake may regress it — and A must end up adopting B's.
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        b.apply(a.local_lww("meta.toml", b"old\n".to_vec(), 1000)); // shared base
        b.local_lww("meta.toml", b"newer on B\n".to_vec(), 2000);

        // B hello → A: A's copy is not newer than B's advertised version, so A
        // sends nothing for it.
        let from_a = deliver(&mut a, b.hello());
        assert!(from_a.is_empty(), "A must not ship an LWW the peer already beats");

        // A hello → B: B ships its newer copy back; A adopts it. Even if an older
        // payload did arrive, apply(Lww) drops it — assert that too.
        let from_b = deliver(&mut b, a.hello());
        assert_eq!(from_b.len(), 1, "B offers its newer copy");
        let (persisted, _) = a.apply(from_b.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"newer on B\n", "A converges to B's newer file");
        let stale = SyncMsg::Lww { path: "meta.toml".into(), version: 1500, bytes: b"stale\n".to_vec() };
        assert!(b.apply(stale).0.is_none(), "an older payload never regresses B");
        assert_eq!(b.lww["meta.toml"].1, b"newer on B\n");
    }

    #[test]
    fn full_state_catches_up_text_and_lww() {
        // A fresh mesh neighbor is caught up with both the text docs AND the LWW
        // structural (LWW) files via full_state.
        let mut a = Syncer::new("a");
        a.local_text("notes/x/doc.md", "# hi\n");
        a.local_lww("notes/x/state.bin", b"state=doing\n".to_vec(), 1000);

        let mut b = Syncer::new("b");
        for m in a.full_state() {
            b.apply(m);
        }
        assert_eq!(b.docs["notes/x/doc.md"].content(), "# hi\n");
        assert_eq!(
            b.lww.get("notes/x/state.bin").map(|(_, by)| by.as_slice()),
            Some(&b"state=doing\n"[..]),
            "neighbor caught up the LWW meta too",
        );
    }

    #[test]
    fn gapped_delta_triggers_resync_and_recovers() {
        use crate::text::EgWalkerText;

        // Craft a delta that skips an ancestor (as a lost message would): edit 2's
        // ops without edit 1. `src` (a full syncer) will answer the resync.
        let mut probe = EgWalkerText::new("s");
        probe.edit_to("one\n");
        let v1 = probe.version();
        probe.edit_to("one\ntwo\n");
        let gapped = SyncMsg::TextDelta {
            path: "note.md".into(),
            ops: probe.encode_delta(&v1), // parent (edit 1) is missing
            vv: probe.version_vector(),
            origin: probe.origin(),
        };

        let mut src = Syncer::new("s");
        src.local_text("note.md", "one\n");
        src.local_text("note.md", "one\ntwo\n");

        // A fresh peer can't apply the gapped delta → asks for a resync.
        let mut dst = Syncer::new("d");
        let (persist, replies) = dst.apply(gapped);
        assert!(persist.is_none());
        assert!(
            matches!(replies.as_slice(), [SyncMsg::Resync { .. }]),
            "a gapped delta must trigger a resync request",
        );

        // A second un-appliable delta while awaiting the resync must NOT ask again.
        let (_, again) = dst.apply(SyncMsg::TextDelta {
            path: "note.md".into(),
            ops: probe.encode_delta(&v1),
            vv: probe.version_vector(),
            origin: probe.origin(),
        });
        assert!(again.is_empty(), "repeated gapped deltas are suppressed");

        // The source answers with full state → the peer recovers.
        let (_, full) = src.apply(replies.into_iter().next().unwrap());
        let (persist, _) = dst.apply(full.into_iter().next().unwrap());
        assert_eq!(persist.unwrap().1, b"one\ntwo\n", "peer recovered via full resync");
    }

    #[test]
    fn corrupt_full_state_parks_after_bounded_retries() {
        use crate::text::EgWalkerText;

        // A delta stream that never merges (a corrupt full state behaves the
        // same: merge returns false every time).
        let mut probe = EgWalkerText::new("s");
        probe.edit_to("one\n");
        let v1 = probe.version();
        probe.edit_to("one\ntwo\n");
        let bad = |probe: &EgWalkerText| SyncMsg::TextDelta {
            path: "note.md".into(),
            ops: probe.encode_delta(&v1),
            vv: probe.version_vector(),
            origin: probe.origin(),
        };

        let mut dst = Syncer::new("d");
        let (_, first) = dst.apply(bad(&probe));
        assert!(matches!(first.as_slice(), [SyncMsg::Resync { .. }]), "initial request");

        // Keep failing: the syncer re-asks a bounded number of times, then parks.
        let mut requests = 1;
        let mut fed = 0;
        while dst.parked_paths().is_empty() {
            let (_, r) = dst.apply(bad(&probe));
            if matches!(r.as_slice(), [SyncMsg::Resync { .. }]) {
                requests += 1;
            }
            fed += 1;
            assert!(fed < 32, "must park after the cap, not loop forever");
        }
        assert_eq!(requests, 3, "1 initial + 2 bounded retries, no more");
        assert_eq!(dst.parked_paths(), vec!["note.md".to_string()]);

        // Parked paths stay quiet — no further requests, still reported parked.
        let (_, after) = dst.apply(bad(&probe));
        assert!(after.is_empty(), "a parked path never re-requests");
        assert_eq!(dst.parked_paths(), vec!["note.md".to_string()]);
    }

    #[test]
    fn successful_resync_retry_never_parks() {
        use crate::text::EgWalkerText;

        // Same gapped-delta setup as `gapped_delta_triggers_resync_and_recovers`,
        // but the FIRST resync answer is "lost" and only the bounded retry's
        // answer arrives — the doc recovers and is never parked.
        let mut probe = EgWalkerText::new("s");
        probe.edit_to("one\n");
        let v1 = probe.version();
        probe.edit_to("one\ntwo\n");
        let gapped = |probe: &EgWalkerText| SyncMsg::TextDelta {
            path: "note.md".into(),
            ops: probe.encode_delta(&v1),
            vv: probe.version_vector(),
            origin: probe.origin(),
        };

        let mut src = Syncer::new("s");
        src.local_text("note.md", "one\n");
        src.local_text("note.md", "one\ntwo\n");

        let mut dst = Syncer::new("d");
        let (_, first) = dst.apply(gapped(&probe));
        assert!(matches!(first.as_slice(), [SyncMsg::Resync { .. }])); // lost
        let (_, sup) = dst.apply(gapped(&probe));
        assert!(sup.is_empty(), "first repeat is suppressed");
        let (_, retry) = dst.apply(gapped(&probe));
        assert!(matches!(retry.as_slice(), [SyncMsg::Resync { .. }]), "bounded re-request");

        // The retry's answer lands and merges → recovered, never parked.
        let (_, full) = src.apply(retry.into_iter().next().unwrap());
        let (persist, _) = dst.apply(full.into_iter().next().unwrap());
        assert_eq!(persist.unwrap().1, b"one\ntwo\n", "recovered via the retry");
        assert!(dst.parked_paths().is_empty(), "a successful retry never parks");
    }

    #[test]
    fn independent_trees_merge_union_docs_and_resolve_same_path() {
        // Two trees created independently (disjoint histories) — as two friends
        // who each already had a folder would.
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        a.local_text("index.md", "# A's index\n");
        a.local_text("notes/aaa/doc.md", "doc A\n");
        b.local_text("index.md", "# B's index\n");
        b.local_text("notes/bbb/doc.md", "doc B\n");

        // Handshake: each answers the other's hello with the docs it lacks.
        let (_, a_gives) = a.apply(b.hello());
        let (_, b_gives) = b.apply(a.hello());
        for m in b_gives {
            let _ = a.apply(m);
        }
        for m in a_gives {
            let _ = b.apply(m);
        }

        // Distinct docs union — both sides hold both.
        for s in [&a, &b] {
            assert!(s.docs.contains_key("notes/aaa/doc.md"), "has doc A");
            assert!(s.docs.contains_key("notes/bbb/doc.md"), "has doc B");
        }
        // The same-path index.md converges to the higher agent id ("b") on both —
        // deterministic, no interleaving of the two texts.
        assert_eq!(a.docs["index.md"].content(), "# B's index\n");
        assert_eq!(b.docs["index.md"].content(), "# B's index\n");
    }

    /// Full two-way state exchange between two peers (the handshake).
    fn sync(x: &mut Syncer, y: &mut Syncer) {
        let (xh, yh) = (x.hello(), y.hello());
        let (_, from_y) = y.apply(xh);
        let (_, from_x) = x.apply(yh);
        for m in from_y {
            let _ = x.apply(m);
        }
        for m in from_x {
            let _ = y.apply(m);
        }
    }

    #[test]
    fn three_independent_trees_converge() {
        // Three peers each independently create index.md. A same-path conflict
        // resolves by a *total order* on origin agent id, so all converge to the
        // single highest ("c") — even as an adopted version has to propagate onward
        // (A adopts B, then must still learn C). Union of distinct docs too.
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        let mut c = Syncer::new("c");
        a.local_text("index.md", "# A\n");
        a.local_text("notes/a/doc.md", "a\n");
        b.local_text("index.md", "# B\n");
        c.local_text("index.md", "# C\n");
        c.local_text("notes/c/doc.md", "c\n");

        // Two rounds over all pairs so an adoption propagates to everyone.
        for _ in 0..2 {
            sync(&mut a, &mut b);
            sync(&mut b, &mut c);
            sync(&mut a, &mut c);
        }

        for s in [&a, &b, &c] {
            assert_eq!(s.docs["index.md"].content(), "# C\n", "all converge to highest origin");
            assert!(s.docs.contains_key("notes/a/doc.md"), "doc a everywhere");
            assert!(s.docs.contains_key("notes/c/doc.md"), "doc c everywhere");
        }
    }
}
