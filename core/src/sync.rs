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
//! no re-touch needed. No I/O, no clock — callers persist the result, and their
//! [`DurablePath`] records, and pass a millisecond `now` for LWW versions.

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

/// The file name (no extension) a driver stores `path`'s durable record under:
/// 32 hex chars of blake3(path) — fixed length, filesystem-safe, no path structure.
pub fn record_name(path: &str) -> String {
    blake3::hash(path.as_bytes()).as_bytes()[..16].iter().map(|b| format!("{b:02x}")).collect()
}

/// One path's durable sync state — what a driver stores (in a never-synced dot-dir)
/// so a restarted peer resumes the SAME history: text keeps its eg-walker oplog (so
/// its origin is unchanged and concurrent edits still merge), LWW keeps its version
/// (so re-opening never re-stamps a stale file as newest). `disk` = blake3 of the
/// bytes this state materializes. Disk format (postcard): APPEND variants only —
/// pinned by `tests::durable_record_layout_is_pinned`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DurablePath {
    pub path: String,
    pub kind: DurableKind,
    pub disk: [u8; 32],
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DurableKind {
    Text { ops: Vec<u8> },
    Lww { version: u64 },
    /// APPENDED: the path is deleted — its tombstone, plus the deleted text doc's oplog (empty for LWW
    /// or a tombstone learned for a never-held path), so a stale copy arriving after a restart is
    /// still recognized and the delete is never undone by a restore.
    /// `past`: the earlier-round text tombstones the path was re-created after (see
    /// `Syncer::past`).
    Deleted { tomb: TreeTombstone, ops: Vec<u8>, past: Vec<PastTomb> },
    /// APPENDED: a text doc re-created after deletion(s) `superseded` (which it overrules); `past`
    /// as for `Deleted`.
    TextReborn { ops: Vec<u8>, superseded: Vec<u64>, past: Vec<PastTomb> },
    /// APPENDED: an LWW file re-created after deletion(s) `superseded`.
    LwwReborn { version: u64, superseded: Vec<u64> },
}

/// What [`Syncer::restore`] found.
pub enum Restored {
    /// The file matches the record: history restored, nothing new.
    Clean,
    /// The file changed while we were down: history restored + this local edit
    /// (already applied to our state; the driver re-saves the record).
    Changed(SyncMsg),
    /// No file, but a live record: the file was deleted while we were down — now tombstoned; this
    /// Delete (also in full_state) syncs it. The driver re-saves the record.
    Deleted(SyncMsg),
    /// A corrupt record, a kind/path mismatch, a dot-path, or the path already has state: nothing
    /// restored — treat the file as new (or drop the record).
    Unusable,
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
/// v3: device-linking auth wire types — `MeshMsg::Trust` (replicated trust log)
/// and `MeshMsg::SignedSync` (signed writes). Both APPENDED, so an open-mode
/// board stays wire-compatible; enforcement is opt-in per board.
/// v4: two APPENDED additions, one bump. (a) Epoch-sealed content for
/// restricted boards — `MeshMsg::Epoch` (admin-signed wrapped epoch keys) and
/// `MeshMsg::Sealed` (content under the current epoch key); open boards are
/// wire-identical. (b) Folder-mode deletes — `SyncMsg::StratDelete` (a v3
/// folder peer skips it and keeps deleted files). The bump surfaces both.
/// v5: passkey device-linking — two APPENDED `TrustEntry` variants (`Passkey`: an
/// Admin registers a WebAuthn credential; `PasskeyLink`: a device joins vouched for
/// by an assertion from it), carried in `MeshMsg::Trust`. A v4 peer fails to decode
/// those frames and skips them, so it never authorizes a passkey-linked device (and
/// drops its writes) — the bump surfaces that.
/// v6: tree/mesh deletes — two APPENDED SyncMsg variants, Delete (8, a tombstone) and Restore (9). A v5 peer fails to decode either and skips it, so it keeps deleted files and re-offers them on catch-up (v6 peers re-assert the delete) — the bump surfaces that.
pub const PROTOCOL_VERSION: u32 = 6;

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
/// Concurrent in-flight reassemblies per receiver, shared by all senders;
/// oldest is evicted beyond this.
const MESH_ASSEMBLY_SLOTS: usize = 64;

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
    /// and still solicit the other's files). On the gossip mesh a `Hello` goes to
    /// direct neighbors only, but its replies go swarm-wide: iroh-gossip delivers a
    /// neighbors-only message to plumtree-EAGER peers only, and the requester may
    /// already be lazy on the replier's side (pruned right after NeighborUp, or a
    /// stale entry after a same-key restart), so a neighbors-only reply can miss it.
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
    /// Folder-mode **tombstone** on the strategy channel: "I deleted `path`".
    /// `ids` name the deletion(s) this tombstone stands for (oldest first;
    /// carried unchanged by every peer that adopts the tombstone, merged when
    /// two peers delete independently), so a peer that has since RE-CREATED
    /// the file can recognize — and reject — an old deletion: it keeps the file
    /// if ANY id is one it overrode. Receivers ignore ids beyond a small cap.
    /// `seen` is the
    /// deleter's recent content fingerprints (blake3-32 of what each replica
    /// materializes, oldest first): a peer applies the delete only if its own
    /// current fingerprint is one the deleter had already seen; otherwise it
    /// holds a concurrent edit, which wins (it answers with a self-contained
    /// `StratDelta` and the deleter resurrects the file). [`Syncer`] ignores it.
    /// APPENDED (wire index 7): peers that predate it skip it cleanly.
    StratDelete { path: String, ids: Vec<u64>, seen: Vec<[u8; 32]> },
    /// Tree-dialect **tombstone** (text CRDT or LWW file): "I deleted `path`". `ids`: deterministic
    /// deletion ids it stands for (oldest first; merged across independent deletes) — a peer that
    /// re-created the path after any of them answers `Restore`. `seen`: blake3 fingerprints of the
    /// content the deleters removed. `origin`/`vv`: the deleted text doc's origin + frontier (empty for
    /// LWW). `version`: the deleted LWW version (0 for text). A receiver deletes only if it provably
    /// holds nothing the deleter hadn't seen; otherwise its concurrent edit wins and it answers with
    /// its full state. APPENDED (wire index 8, v6). Folder sessions ignore it.
    Delete { path: String, ids: Vec<u64>, seen: Vec<[u8; 32]>, origin: Origin, vv: Vv, version: u64 },
    /// "My copy of `path` re-creates it after deletion(s) `ids` — they are overruled." Sent with the
    /// full state by a peer refusing a stale `Delete`; a peer whose tombstone shares an id drops it.
    /// APPENDED (wire index 9, v6).
    Restore { path: String, ids: Vec<u64> },
}

/// How a peer's `Hello` compares to our state (anti-entropy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HelloCmp {
    pub we_ahead: bool,
    pub they_ahead: bool,
}
impl HelloCmp {
    pub fn consistent(&self) -> bool {
        !self.we_ahead && !self.they_ahead
    }
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
    /// A signed trust-log entry (device-linking auth) — broadcast so every peer
    /// converges on the authorized device set. Fed to
    /// [`identity::TrustLog`](crate::identity::TrustLog). APPENDED: peers that
    /// predate auth skip it cleanly. `TrustEntry`'s own variants are append-only too
    /// (pinned in `wire_compat`).
    Trust(crate::identity::TrustEntry),
    /// A **signed** tree write: the payload is a postcard-encoded [`SyncMsg`]
    /// signed by its author (device-linking auth). A driver enforcing signing
    /// verifies it against the trust log before applying; an open-mode peer that
    /// predates auth skips the unknown variant. APPENDED.
    SignedSync(crate::identity::SignedWrite),
    /// A signed **request to join** a locked board, broadcast by a would-be
    /// writer (the Google-Docs "request access" flow). An admin peer collects it
    /// and — on approval — replies with a `Trust` grant. Self-authenticating via
    /// its signature; peers that predate it skip the unknown variant. APPENDED.
    Request(crate::onboard::JoinRequest),
    /// An Admin-signed epoch announcement carrying wrapped epoch keys (restricted
    /// boards). APPENDED (v4): older peers skip it.
    Epoch(crate::epoch::EpochAnnounce),
    /// Content (`Sync`/`SignedSync`) sealed under an epoch key — only devices
    /// wrapped into that epoch can open it. APPENDED (v4).
    Sealed(crate::epoch::SealedMsg),
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
/// [`MESH_ASSEMBLY_SLOTS`] concurrent reassemblies — oldest evicted; the
/// eviction count lets the driver run an anti-entropy round — and a `total`
/// cap so a bogus header can't balloon memory.
#[derive(Default)]
pub struct MeshAssembler {
    slots: HashMap<u64, Assembly>,
    /// Insertion order for eviction (ids, oldest first).
    order: Vec<u64>,
    /// Count of dropped partial messages (oldest evicted beyond
    /// `MESH_ASSEMBLY_SLOTS`) — drivers trigger anti-entropy on change.
    evicted: u64,
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
                self.evicted += 1;
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

    /// A dropped partial message — drivers trigger anti-entropy on change.
    pub fn evictions(&self) -> u64 {
        self.evicted
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

/// Most fingerprints a tree tombstone keeps in `seen` (oldest dropped).
pub const TREE_SEEN_CAP: usize = 16;
/// Most deletion ids a tree tombstone stands for (oldest dropped).
pub const TREE_DELETION_IDS_CAP: usize = 8;
/// Most overruled deletion ids a re-created path remembers (oldest dropped).
pub const TREE_SUPERSEDED_CAP: usize = 64;
/// Most `(agent, seq)` tips a tree tombstone's `vv` keeps (oldest dropped).
pub const TREE_VV_CAP: usize = 256;
/// Most earlier-round text tombstones a path remembers (`Syncer::past`; oldest dropped).
pub const TREE_PAST_CAP: usize = 4;

/// blake3 of a file's bytes — what `Delete::seen` / `DurablePath::disk` hold.
pub fn fingerprint(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

/// Deterministic deletion id (two peers deleting the same content with the same history mint the
/// same id). Body = src/sync/workspace.rs::deletion_id with domain b"riftpipe/tree-delete/v1\0" and
/// `fp` in place of `seen.last()`.
pub fn tree_deletion_id(path: &str, fp: &[u8; 32], superseded: &[u64]) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(b"riftpipe/tree-delete/v1\0");
    h.update(path.as_bytes());
    h.update(&[0]);
    h.update(fp);
    let mut sup = superseded.to_vec();
    sup.sort_unstable();
    sup.dedup();
    for id in sup {
        h.update(&id.to_le_bytes());
    }
    u64::from_le_bytes(h.finalize().as_bytes()[..8].try_into().expect("8 bytes"))
}

/// A path a tree delete may name: non-empty, relative, no empty or dot-leading component (so no
/// `..`, no hidden/sync-state dirs like `.riftpipe-sync`).
fn deletable_path(p: &str) -> bool {
    !p.is_empty() && p.split('/').all(|c| !c.is_empty() && !c.starts_with('.'))
}

/// Push `item` unless present; drop oldest beyond `cap`. True if it was added.
fn push_absent<T: PartialEq>(list: &mut Vec<T>, item: T, cap: usize) -> bool {
    if list.contains(&item) {
        return false;
    }
    list.push(item);
    while list.len() > cap {
        list.remove(0);
    }
    true
}

/// Add tips of `from` not already in `into` (exact pair); drop oldest beyond TREE_VV_CAP. True if changed.
fn union_tips(into: &mut Vv, from: &[(String, usize)]) -> bool {
    let mut changed = false;
    for tip in from {
        changed |= push_absent(into, tip.clone(), TREE_VV_CAP);
    }
    changed
}

/// A tree tombstone: what a peer needs to judge delete-vs-concurrent-edit (see `SyncMsg::Delete`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TreeTombstone {
    pub ids: Vec<u64>,
    pub seen: Vec<[u8; 32]>,
    pub origin: Origin,
    pub vv: Vv,
    pub version: u64,
    /// Deletion ids the deleted file had itself overruled (local-only, never sent).
    pub prior: Vec<u64>,
}

/// Whether a deleter whose doc reached frontier `vv` had seen every op of `doc` (same doc, the
/// caller checks the origin): `doc.ops_since(vv)` is none, OR each tip `(agent, seq)` of `doc` has
/// some `(agent, s ≥ seq)` in `vv`. The per-agent half is what judges a doc that is BEHIND tips
/// `vv` names but `doc` lacks — `ops_since` drops unknown tips, so to it a lagging copy looks
/// ahead. Sound because one agent's ops form a causal chain: holding `(agent, s)` means holding
/// all of that agent's earlier ops and their history.
fn seen_by(doc: &EgWalkerText, vv: &Vv) -> bool {
    doc.ops_since(vv).is_none()
        || doc.version_vector().iter().all(|(a, seq)| vv.iter().any(|(b, s)| a == b && s >= seq))
}

/// `t` as the `Delete` for `path`.
fn delete_msg(path: &str, t: &TreeTombstone) -> SyncMsg {
    SyncMsg::Delete {
        path: path.to_string(),
        ids: t.ids.clone(),
        seen: t.seen.clone(),
        origin: t.origin.clone(),
        vv: t.vv.clone(),
        version: t.version,
    }
}

/// An earlier-round text tombstone a re-created path remembers (`Syncer::past`), with the deleted
/// doc's oplog (empty if this peer never held it) so an OLDER copy of it is recognized too.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PastTomb {
    pub tomb: TreeTombstone,
    pub ops: Vec<u8>,
}

/// A deleted path's state: its tombstone plus, for text, the deleted doc (so a stale copy of the
/// same doc is recognized as already-seen, and genuinely new ops revive it with full history).
struct Tomb {
    t: TreeTombstone,
    doc: Option<EgWalkerText>,
}

/// A file-level effect of [`Syncer::receive`] for the driver to apply on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Write { path: String, bytes: Vec<u8> },
    Remove { path: String },
}

/// What `receive` produced. Drivers apply `change` (write / delete the file), THEN rewrite `save`'s
/// durable record from `durable(save)` — or delete that record when `durable` is `None`.
#[derive(Default)]
pub struct Applied {
    pub change: Option<Change>,
    pub replies: Vec<SyncMsg>,
    pub save: Option<String>,
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
    /// Bumped whenever held state actually changes — drivers feed it to
    /// `AntiEntropy::poll`.
    generation: u64,
    /// Per re-created path: the deletion ids it overrules (a `Delete` naming any is answered with
    /// `Restore` + full state instead of being applied).
    superseded: HashMap<String, Vec<u64>>,
    /// Per deleted path: its tombstone (and, for text, the deleted doc).
    tombs: HashMap<String, Tomb>,
    /// Local re-creations so far — each re-created text doc is authored under
    /// `"{agent}~{rebirths}"` so its op ids can't collide with the deleted doc's.
    rebirths: u64,
    /// Per path: the TEXT tombstones it was re-created after, newest last (capped). The latest
    /// tombstone describes the latest doc only, so a peer that was away since an EARLIER round and
    /// still holds that round's (different) doc can't be judged by it; a copy one of these covers
    /// is answered with that round's own `Delete`, which the holder can judge. Kept across later
    /// deletes and re-creations.
    past: HashMap<String, Vec<PastTomb>>,
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
            generation: 0,
            superseded: HashMap::new(),
            tombs: HashMap::new(),
            rebirths: 0,
            past: HashMap::new(),
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
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

    /// How `msg` (expected to be a `Hello`) compares to our own state —
    /// `None` unless `msg` is a `Hello`. A tombstoned path they still hold counts as `we_ahead`
    /// (we answer with its `Delete`), plus `they_ahead` if their copy may hold ops or a version
    /// past the delete.
    pub fn compare_hello(&self, msg: &SyncMsg) -> Option<HelloCmp> {
        let SyncMsg::Hello { versions, lww_versions } = msg else { return None };
        let mut cmp = HelloCmp::default();
        let their: HashMap<String, Vv> = versions.iter().cloned().collect();

        // Text, their side.
        for (path, vv) in &their {
            if self.parked.contains(path) {
                continue;
            }
            if let Some(tomb) = self.tombs.get(path) {
                cmp.we_ahead = true;
                let known = tomb.doc.as_ref().is_some_and(|d| {
                    d.origin().is_some() && vv.iter().all(|(a, s)| d.knows(a, *s))
                });
                if !vv.is_empty() && !known {
                    cmp.they_ahead = true;
                }
                continue;
            }
            match self.docs.get(path) {
                None => {
                    if !vv.is_empty() {
                        cmp.they_ahead = true;
                    }
                }
                Some(doc) => {
                    if vv.iter().any(|(a, s)| !doc.knows(a, *s)) {
                        cmp.they_ahead = true;
                    }
                }
            }
        }
        // Text, our side.
        for (path, doc) in &self.docs {
            let their_vv = their.get(path).cloned().unwrap_or_default();
            if doc.has_ops_since(&their_vv) {
                cmp.we_ahead = true;
            }
        }
        // LWW, their side.
        let their_lww: HashMap<String, u64> = lww_versions.iter().cloned().collect();
        for (path, v) in &their_lww {
            if let Some(tomb) = self.tombs.get(path) {
                cmp.we_ahead = true;
                cmp.they_ahead |= *v > tomb.t.version;
                continue;
            }
            if self.lww.get(path).is_none_or(|(mine, _)| v > mine) {
                cmp.they_ahead = true;
            }
        }
        // LWW, our side.
        for (path, (mine, _)) in &self.lww {
            if their_lww.get(path).is_none_or(|t| mine > t) {
                cmp.we_ahead = true;
            }
        }
        Some(cmp)
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
    /// Re-creating a tombstoned path starts a NEW doc (a fresh agent, so op ids can't collide with
    /// the deleted doc's) that overrules the deletion(s).
    pub fn local_text(&mut self, path: &str, content: &str) -> Option<SyncMsg> {
        let reborn = self.untomb(path).is_some();
        if reborn {
            self.rebirths += 1;
            let agent = format!("{}~{}", self.agent, self.rebirths);
            self.docs.insert(path.to_string(), EgWalkerText::new(&agent));
            self.peer_vv.remove(path);
        }
        let peer = self.peer_vv.get(path).cloned().unwrap_or_default();
        let changed = {
            let doc = self.doc(path);
            let before = doc.version();
            doc.edit_to(content);
            doc.version() != before
        };
        if changed || reborn {
            self.generation += 1;
        }
        let doc = self.docs.get(path).expect("doc() just ensured it");
        let ops = doc.ops_since(&peer)?;
        let vv = doc.version_vector();
        let origin = doc.origin();
        self.advance_peer_vv(path, &vv); // optimistic: assume they'll get our ops
        Some(SyncMsg::TextDelta { path: path.to_string(), ops, vv, origin })
    }

    /// Full self-contained state of every text doc, LWW file and tombstone (a `Delete` per deleted
    /// path, last, sorted) — broadcast to a newly-joined neighbor (gossip has no per-peer delta) so
    /// it catches up + merges.
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
        let mut deleted: Vec<&String> = self.tombs.keys().collect();
        deleted.sort();
        msgs.extend(deleted.into_iter().map(|path| self.tomb_msg(path)));
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
    /// Re-creating a tombstoned path stamps a version past the deleted one.
    pub fn local_lww(&mut self, path: &str, bytes: Vec<u8>, now: u64) -> SyncMsg {
        let floor = self.untomb(path).map(|t| t.t.version.saturating_add(1)).unwrap_or(0);
        let entry = self.lww.entry(path.to_string()).or_insert((0, Vec::new()));
        entry.0 = now.max(entry.0 + 1).max(floor);
        entry.1 = bytes.clone();
        self.generation += 1;
        SyncMsg::Lww { path: path.to_string(), version: entry.0, bytes }
    }

    /// The durable record for `path` (None if we hold no state for it). Call after
    /// every local change or persisted merge of `path`.
    pub fn durable(&self, path: &str) -> Option<DurablePath> {
        let superseded = self.superseded.get(path).filter(|s| !s.is_empty()).cloned();
        let past = self.past.get(path).cloned().unwrap_or_default();
        if let Some(doc) = self.docs.get(path) {
            let content = doc.content();
            let kind = if superseded.is_some() || !past.is_empty() {
                let superseded = superseded.unwrap_or_default();
                DurableKind::TextReborn { ops: doc.encode_full(), superseded, past }
            } else {
                DurableKind::Text { ops: doc.encode_full() }
            };
            return Some(DurablePath {
                path: path.to_string(),
                kind,
                disk: *blake3::hash(content.as_bytes()).as_bytes(),
            });
        }
        if let Some((version, bytes)) = self.lww.get(path) {
            let kind = match superseded {
                Some(superseded) => DurableKind::LwwReborn { version: *version, superseded },
                None => DurableKind::Lww { version: *version },
            };
            return Some(DurablePath {
                path: path.to_string(),
                kind,
                disk: *blake3::hash(bytes).as_bytes(),
            });
        }
        if let Some(tomb) = self.tombs.get(path) {
            let ops = tomb.doc.as_ref().map(|d| d.encode_full()).unwrap_or_default();
            return Some(DurablePath {
                path: path.to_string(),
                kind: DurableKind::Deleted { tomb: tomb.t.clone(), ops, past },
                disk: [0; 32],
            });
        }
        None
    }

    /// Restore `rec` given the file's current bytes (`None` if it is gone — an offline delete).
    /// Call BEFORE any apply/local_* for that path (drivers restore before the session starts).
    /// New local ops are authored under THIS syncer's agent; the record's ops (and so the origin)
    /// are kept as history. `peer_vv` is untouched. A missing file for a live record tombstones it
    /// (the id matches what a live `local_delete` would have produced); a `Deleted` record is never
    /// resurrected by a restore — only a file reappearing next to it counts as a re-creation.
    pub fn restore(&mut self, rec: DurablePath, file: Option<&[u8]>, now: u64) -> Restored {
        if !deletable_path(&rec.path)
            || self.docs.contains_key(&rec.path)
            || self.lww.contains_key(&rec.path)
            || self.tombs.contains_key(&rec.path)
        {
            return Restored::Unusable;
        }
        match rec.kind {
            DurableKind::Text { ops } => self.restore_text(rec.path, ops, Vec::new(), Vec::new(), rec.disk, file),
            DurableKind::TextReborn { ops, superseded, past } => {
                self.restore_text(rec.path, ops, superseded, past, rec.disk, file)
            }
            DurableKind::Lww { version } => self.restore_lww(rec.path, version, Vec::new(), rec.disk, file, now),
            DurableKind::LwwReborn { version, superseded } => {
                self.restore_lww(rec.path, version, superseded, rec.disk, file, now)
            }
            DurableKind::Deleted { tomb, ops, past } => {
                if !past.is_empty() {
                    self.past.insert(rec.path.clone(), past);
                }
                let doc = if ops.is_empty() {
                    None
                } else {
                    let mut d = EgWalkerText::new(&self.agent);
                    (d.merge(&ops) && d.origin().is_some()).then_some(d)
                };
                self.bury(&rec.path, tomb, doc);
                let Some(file) = file else {
                    self.generation += 1;
                    return Restored::Clean;
                };
                let msg = if is_text_path(&rec.path) {
                    self.local_text(&rec.path, &String::from_utf8_lossy(file))
                } else {
                    Some(self.local_lww(&rec.path, file.to_vec(), now))
                };
                match msg {
                    Some(m) => Restored::Changed(m),
                    None => Restored::Clean,
                }
            }
        }
    }

    /// A restored `Text`/`TextReborn` record: rebuild the document from `ops`, then reconcile it
    /// with `file`. No file means the path was deleted while we were down: the document is
    /// installed and immediately tombstoned, as a live `local_delete` would.
    fn restore_text(
        &mut self,
        path: String,
        ops: Vec<u8>,
        superseded: Vec<u64>,
        past: Vec<PastTomb>,
        disk: [u8; 32],
        file: Option<&[u8]>,
    ) -> Restored {
        if !is_text_path(&path) {
            return Restored::Unusable;
        }
        let mut doc = EgWalkerText::new(&self.agent);
        if !doc.merge(&ops) || doc.origin().is_none() {
            return Restored::Unusable;
        }
        if !superseded.is_empty() {
            self.superseded.insert(path.clone(), superseded);
        }
        if !past.is_empty() {
            self.past.insert(path.clone(), past);
        }
        self.docs.insert(path.clone(), doc);
        let Some(file) = file else {
            return match self.local_delete(&path) {
                Some(m) => Restored::Deleted(m),
                None => Restored::Unusable,
            };
        };
        if *blake3::hash(file).as_bytes() == disk {
            self.generation += 1;
            return Restored::Clean;
        }
        match self.local_text(&path, &String::from_utf8_lossy(file)) {
            Some(m) => Restored::Changed(m),
            None => Restored::Clean,
        }
    }

    /// A restored `Lww`/`LwwReborn` record: reconcile the cached version with `file`. No file means
    /// the path was deleted while we were down: bury a tombstone directly (there's no live entry to
    /// route through `local_delete`), with the same id a live delete would have produced.
    fn restore_lww(
        &mut self,
        path: String,
        version: u64,
        superseded: Vec<u64>,
        disk: [u8; 32],
        file: Option<&[u8]>,
        now: u64,
    ) -> Restored {
        if is_text_path(&path) {
            return Restored::Unusable;
        }
        let Some(file) = file else {
            let ids = vec![tree_deletion_id(&path, &disk, &superseded)];
            let t = TreeTombstone { ids, seen: vec![disk], origin: None, vv: Vec::new(), version, prior: superseded };
            self.bury(&path, t, None);
            self.generation += 1;
            return Restored::Deleted(self.tomb_msg(&path));
        };
        if !superseded.is_empty() {
            self.superseded.insert(path.clone(), superseded);
        }
        if *blake3::hash(file).as_bytes() == disk {
            self.lww.insert(path, (version, file.to_vec()));
            self.generation += 1;
            return Restored::Clean;
        }
        self.lww.insert(path.clone(), (version, Vec::new()));
        Restored::Changed(self.local_lww(&path, file.to_vec(), now))
    }

    /// The live-state merge (no tombstones involved): returns `(bytes to persist, messages to send
    /// back)`. A `Hello` produces reply deltas; a `TextDelta`/`Lww` produces bytes.
    fn apply_live(&mut self, msg: SyncMsg) -> (Option<(String, Vec<u8>)>, Vec<SyncMsg>) {
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
                // A copy of a doc deleted in an earlier round (we re-created the path since):
                // answer with that round's Delete rather than take it in.
                let mine = self.docs.get(&path).and_then(|d| d.origin());
                if self.past.contains_key(&path) && origin.is_some() && mine != origin {
                    let mut fresh = EgWalkerText::new(&self.agent);
                    if fresh.merge(&ops) {
                        if let Some(old) = self.past_delete_covering(&path, &fresh) {
                            return (None, vec![old]);
                        }
                    }
                }
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
                        // Ours is itself an earlier round's deleted doc: never let it win.
                        let ours_deleted = self
                            .docs
                            .get(&path)
                            .is_some_and(|d| self.past_delete_covering(&path, d).is_some());
                        if ours_deleted || mine.0.as_str() < theirs.0.as_str() {
                            // We lose — adopt their version (their ops are a
                            // self-contained full on a fresh conflict).
                            let mut fresh = EgWalkerText::new(&self.agent);
                            if fresh.merge(&ops) {
                                let content = fresh.content().into_bytes();
                                self.docs.insert(path.clone(), fresh);
                                self.generation += 1;
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
                let before = self.docs.get(&path).map(|d| d.version());
                if !self.doc(&path).merge(&ops) {
                    // Missing a causal ancestor (or corrupt bytes). Ask for a full
                    // self-contained resync; suppress repeats while it's pending so
                    // a run of un-appliable deltas can't cause a resync storm. If
                    // the full state itself keeps failing to merge, re-ask a bounded
                    // number of times, then PARK the path (visible via
                    // `parked_paths`) instead of looping forever.
                    if self.merge_failed_should_request(&path) {
                        return (None, vec![SyncMsg::Resync { path }]);
                    }
                    return (None, Vec::new()); // suppressed, or parked: stay quiet
                }
                self.resync.remove(&path);
                self.parked.remove(&path); // any successful merge un-parks
                let after = self.docs.get(&path).map(|d| d.version());
                self.advance_peer_vv(&path, &vv);
                if after == before {
                    // A duplicate delta (common once anti-entropy makes several
                    // neighbours answer the same Hello): no new ops landed, so
                    // there's nothing to persist, log, or re-render.
                    return (None, Vec::new());
                }
                self.generation += 1;
                let content = self.doc(&path).content().into_bytes();
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
                    self.generation += 1;
                    (Some((path, bytes)), Vec::new())
                } else {
                    (None, Vec::new())
                }
            }
            // The strategy channel belongs to the workspace/strategy layer
            // (folder mode), and version announcements to the driver; a
            // text/LWW syncer has nothing to do with either. Tree deletes
            // (`Delete`/`Restore`) are handled by `receive` before this.
            SyncMsg::StratAdvert { .. }
            | SyncMsg::StratDelta { .. }
            | SyncMsg::StratDelete { .. }
            | SyncMsg::Version { .. }
            | SyncMsg::Delete { .. }
            | SyncMsg::Restore { .. } => {
                (None, Vec::new())
            }
        }
    }

    /// One failed merge for `path` under the bounded full-resync budget: true when the caller
    /// should (re-)ask for the full state now; false while suppressed or once parked.
    fn merge_failed_should_request(&mut self, path: &str) -> bool {
        if self.parked.contains(path) {
            return false; // gave up; stay quiet
        }
        let Some(st) = self.resync.get_mut(path) else {
            self.resync.insert(path.to_string(), ResyncState { requests: 1, failures: 0 });
            return true;
        };
        st.failures += 1;
        if st.failures < RESYNC_FAILURES_PER_RETRY {
            return false; // suppressed while awaiting
        }
        if st.requests >= MAX_RESYNC_REQUESTS {
            // Retry budget exhausted — park and go quiet.
            self.resync.remove(path);
            self.parked.insert(path.to_string());
            return false;
        }
        st.requests += 1;
        st.failures = 0;
        true
    }

    /// Apply a received message (tombstone-aware); see [`Applied`] for what the driver does with
    /// the result. A `Delete` removes our copy only if we provably hold nothing the deleter hadn't
    /// seen (else our edit wins: we answer with our full state); a tombstoned path answers stale
    /// copies, `Resync`s and `Hello`s naming it with its `Delete`, and revives on genuinely newer
    /// content. `generation` moves once per tombstone change (`save` set), never on a re-assert.
    pub fn receive(&mut self, msg: SyncMsg) -> Applied {
        let applied = match msg {
            SyncMsg::Delete { path, ids, seen, origin, vv, version } => {
                self.remote_delete(path, ids, seen, origin, vv, version)
            }
            SyncMsg::Restore { path, ids } => self.remote_restore(path, &ids),
            SyncMsg::TextDelta { path, ops, vv, origin } if self.tombs.contains_key(&path) => {
                self.text_for_tomb(path, ops, vv, origin)
            }
            SyncMsg::Lww { path, version, bytes } if self.tombs.contains_key(&path) => {
                self.lww_for_tomb(path, version, bytes)
            }
            SyncMsg::Resync { path } if self.tombs.contains_key(&path) => return self.reassert(&path),
            SyncMsg::Hello { versions, lww_versions } => {
                let mut named: Vec<String> = versions
                    .iter()
                    .map(|(p, _)| p)
                    .chain(lww_versions.iter().map(|(p, _)| p))
                    .filter(|p| self.tombs.contains_key(*p))
                    .cloned()
                    .collect();
                named.sort();
                named.dedup();
                let (_, mut replies) = self.apply_live(SyncMsg::Hello { versions, lww_versions });
                replies.extend(named.iter().map(|p| self.tomb_msg(p)));
                return Applied { replies, ..Default::default() };
            }
            other => {
                let (persist, replies) = self.apply_live(other);
                return match persist {
                    Some((path, bytes)) => Applied {
                        change: Some(Change::Write { path: path.clone(), bytes }),
                        replies,
                        save: Some(path),
                    },
                    None => Applied { replies, ..Default::default() },
                };
            }
        };
        if applied.save.is_some() {
            self.generation += 1;
        }
        applied
    }

    /// Shorthand for [`receive`](Self::receive) that DROPS removals and `save` — tests only; drivers
    /// use `receive`.
    pub fn apply(&mut self, msg: SyncMsg) -> (Option<(String, Vec<u8>)>, Vec<SyncMsg>) {
        let Applied { change, replies, .. } = self.receive(msg);
        let write = match change {
            Some(Change::Write { path, bytes }) => Some((path, bytes)),
            _ => None,
        };
        (write, replies)
    }

    /// Record a local delete (the file is gone): tombstone `path` and return the `Delete` to send.
    /// `None` for a dot/escaping path or one we hold nothing for. An already-tombstoned path
    /// returns its current `Delete` (idempotent).
    pub fn local_delete(&mut self, path: &str) -> Option<SyncMsg> {
        if !deletable_path(path) {
            return None;
        }
        if self.tombs.contains_key(path) {
            return Some(self.tomb_msg(path));
        }
        if !self.docs.contains_key(path) && !self.lww.contains_key(path) {
            return None;
        }
        let prior = self.superseded.remove(path).unwrap_or_default();
        let (fp, origin, vv, version, doc) = match self.drop_live(path) {
            (Some(d), _) => {
                (fingerprint(d.content().as_bytes()), d.origin(), d.version_vector(), 0, Some(d))
            }
            (None, Some((version, bytes))) => (fingerprint(&bytes), None, Vec::new(), version, None),
            (None, None) => unreachable!("held state checked above"),
        };
        let ids = vec![tree_deletion_id(path, &fp, &prior)];
        self.bury(path, TreeTombstone { ids, seen: vec![fp], origin, vv, version, prior }, doc);
        self.generation += 1;
        Some(self.tomb_msg(path))
    }

    pub fn is_tombstoned(&self, path: &str) -> bool {
        self.tombs.contains_key(path)
    }

    /// Every tombstoned path, sorted.
    pub fn tombstoned_paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self.tombs.keys().cloned().collect();
        v.sort();
        v
    }

    /// Drop `path`'s tombstone; its id chain (`prior`, then `ids`) becomes overruled by whatever
    /// re-creates the path.
    fn untomb(&mut self, path: &str) -> Option<Tomb> {
        let tomb = self.tombs.remove(path)?;
        let sup = self.superseded.entry(path.to_string()).or_default();
        for id in tomb.t.prior.iter().chain(&tomb.t.ids) {
            push_absent(sup, *id, TREE_SUPERSEDED_CAP);
        }
        if tomb.t.origin.is_some() {
            let ops = tomb.doc.as_ref().map(|d| d.encode_full()).unwrap_or_default();
            let past = PastTomb { tomb: tomb.t.clone(), ops };
            push_absent(self.past.entry(path.to_string()).or_default(), past, TREE_PAST_CAP);
        }
        self.resync.remove(path);
        self.parked.remove(path);
        Some(tomb)
    }

    /// Forget `path`'s live state; returns the dropped text doc and LWW entry.
    fn drop_live(&mut self, path: &str) -> (Option<EgWalkerText>, Option<(u64, Vec<u8>)>) {
        self.peer_vv.remove(path);
        self.resync.remove(path);
        self.parked.remove(path);
        (self.docs.remove(path), self.lww.remove(path))
    }

    fn bury(&mut self, path: &str, t: TreeTombstone, doc: Option<EgWalkerText>) {
        self.tombs.insert(path.to_string(), Tomb { t, doc });
    }

    /// `path`'s `Delete`; the caller ensures `path` is tombstoned.
    fn tomb_msg(&self, path: &str) -> SyncMsg {
        delete_msg(path, &self.tombs.get(path).expect("tomb_msg of a tombstoned path").t)
    }

    /// The `Delete` of an earlier-round text tombstone of `path` (newest first) that covers `doc`:
    /// same origin, and every op of `doc` was in that round's deleted doc (or `doc` is at that
    /// round's frontier). Its `vv` gains `doc`'s tips, so the holder's own stale check accepts
    /// it, exactly like a deleter re-asserting to a peer that is behind. Causal only (no
    /// fingerprint): a revived doc has ops past the deletion and a re-created one has a new
    /// origin, so neither is ever covered and this can't ping-pong with their `Restore`.
    fn past_delete_covering(&self, path: &str, doc: &EgWalkerText) -> Option<SyncMsg> {
        let origin = doc.origin()?;
        let tips = doc.version_vector();
        self.past.get(path)?.iter().rev().find_map(|p| {
            if p.tomb.origin.as_ref() != Some(&origin) {
                return None;
            }
            let covered = seen_by(doc, &p.tomb.vv) || {
                let mut deleted = EgWalkerText::new(&self.agent);
                !p.ops.is_empty() && deleted.merge(&p.ops) && tips.iter().all(|(a, s)| deleted.knows(a, *s))
            };
            covered.then(|| {
                let mut t = p.tomb.clone();
                union_tips(&mut t.vv, &tips);
                delete_msg(path, &t)
            })
        })
    }

    /// Answer with `path`'s `Delete` again (no state change).
    fn reassert(&self, path: &str) -> Applied {
        Applied { replies: vec![self.tomb_msg(path)], ..Default::default() }
    }

    /// `path`'s full self-contained live state, if we hold it.
    fn full_msg(&self, path: &str) -> Option<SyncMsg> {
        if let Some(doc) = self.docs.get(path) {
            return Some(SyncMsg::TextDelta {
                path: path.to_string(),
                ops: doc.encode_full(),
                vv: doc.version_vector(),
                origin: doc.origin(),
            });
        }
        let (version, bytes) = self.lww.get(path)?;
        Some(SyncMsg::Lww { path: path.to_string(), version: *version, bytes: bytes.clone() })
    }

    /// Bring a deleted text path back as `doc` (already untombed); reply with the ops the sender
    /// (at `their_vv`) lacks.
    fn revive_text(&mut self, path: String, doc: EgWalkerText, their_vv: &Vv) -> Applied {
        let reply = doc.ops_since(their_vv).map(|ops| SyncMsg::TextDelta {
            path: path.clone(),
            ops,
            vv: doc.version_vector(),
            origin: doc.origin(),
        });
        let content = doc.content().into_bytes();
        self.docs.insert(path.clone(), doc);
        self.advance_peer_vv(&path, their_vv);
        Applied {
            change: Some(Change::Write { path: path.clone(), bytes: content }),
            replies: reply.into_iter().collect(),
            save: Some(path),
        }
    }

    /// A content merge for a tombstoned path failed: re-assert the `Delete` under the same bounded
    /// budget as a live path's resync requests (so unmergeable content can't loop forever).
    fn tomb_merge_failed(&mut self, path: &str) -> Applied {
        if self.merge_failed_should_request(path) {
            self.reassert(path)
        } else {
            Applied::default()
        }
    }

    fn remote_delete(
        &mut self,
        path: String,
        mut ids: Vec<u64>,
        mut seen: Vec<[u8; 32]>,
        origin: Origin,
        mut vv: Vv,
        version: u64,
    ) -> Applied {
        if !deletable_path(&path) || ids.is_empty() {
            return Applied::default();
        }
        if ids.len() > TREE_DELETION_IDS_CAP {
            ids.drain(..ids.len() - TREE_DELETION_IDS_CAP);
        }
        if seen.len() > TREE_SEEN_CAP {
            seen.drain(..seen.len() - TREE_SEEN_CAP);
        }
        vv.truncate(TREE_VV_CAP);

        // Already tombstoned: merge what's new; a duplicate changes (and answers) nothing.
        if let Some(tomb) = self.tombs.get_mut(&path) {
            let t = &mut tomb.t;
            let mut changed = false;
            for id in ids {
                // An id this delete's own chain already overruled (`prior`) is an older round's
                // deletion — merging it would let a stale re-creator's `Restore` cancel this one.
                if !t.prior.contains(&id) {
                    changed |= push_absent(&mut t.ids, id, TREE_DELETION_IDS_CAP);
                }
            }
            for fp in seen {
                changed |= push_absent(&mut t.seen, fp, TREE_SEEN_CAP);
            }
            if t.origin.is_none() && origin.is_some() {
                t.origin = origin;
                changed = true;
                changed |= union_tips(&mut t.vv, &vv);
            } else if t.origin == origin {
                changed |= union_tips(&mut t.vv, &vv);
            }
            if version > t.version {
                t.version = version;
                changed = true;
            }
            return Applied { save: changed.then_some(path), ..Default::default() };
        }

        // We re-created the path after ALL of these deletions: they're overruled. (Any overlap is
        // not enough — a tombstone merged with an older, already-overruled deletion still carries
        // a newer one.)
        let sup = self.superseded.get(&path).cloned().unwrap_or_default();
        if ids.iter().all(|id| sup.contains(id)) {
            let mut replies = vec![SyncMsg::Restore { path: path.clone(), ids: sup }];
            replies.extend(self.full_msg(&path));
            return Applied { replies, ..Default::default() };
        }

        // Never held: remember the tombstone, leave the disk alone.
        if !self.docs.contains_key(&path) && !self.lww.contains_key(&path) {
            let prior = self.superseded.remove(&path).unwrap_or_default();
            self.bury(&path, TreeTombstone { ids, seen, origin, vv, version, prior }, None);
            return Applied { save: Some(path), ..Default::default() };
        }

        // Live: delete only if we provably hold nothing the deleter hadn't seen. An empty doc
        // (no ops, no origin) holds nothing at all.
        let stale = match self.docs.get(&path) {
            Some(d) => {
                d.origin().is_none()
                    || (d.origin() == origin && seen_by(d, &vv))
                    || seen.contains(&fingerprint(d.content().as_bytes()))
            }
            None => {
                let (mine, bytes) = &self.lww[&path];
                *mine <= version || seen.contains(&fingerprint(bytes))
            }
        };
        if !stale {
            // Our concurrent edit wins: answer with it.
            return Applied { replies: self.full_msg(&path).into_iter().collect(), ..Default::default() };
        }
        let prior = self.superseded.remove(&path).unwrap_or_default();
        let mut t = TreeTombstone { ids, seen, origin, vv, version, prior };
        let kept = match self.drop_live(&path) {
            (Some(d), _) => {
                push_absent(&mut t.seen, fingerprint(d.content().as_bytes()), TREE_SEEN_CAP);
                if d.origin().is_some() && d.origin() == t.origin {
                    union_tips(&mut t.vv, &d.version_vector());
                    Some(d)
                } else {
                    None
                }
            }
            (None, Some((mine, bytes))) => {
                push_absent(&mut t.seen, fingerprint(&bytes), TREE_SEEN_CAP);
                t.version = t.version.max(mine);
                None
            }
            (None, None) => None,
        };
        self.bury(&path, t, kept);
        Applied { change: Some(Change::Remove { path: path.clone() }), replies: Vec::new(), save: Some(path) }
    }

    /// Drop `path`'s tombstone only if the restorer overrules EVERY deletion it stands for — a
    /// tombstone merged with a later deletion survives a `Restore` of just the earlier one.
    fn remote_restore(&mut self, path: String, ids: &[u64]) -> Applied {
        let overruled = self.tombs.get(&path).is_some_and(|t| t.t.ids.iter().all(|id| ids.contains(id)));
        if !overruled {
            return Applied::default();
        }
        self.untomb(&path);
        Applied { save: Some(path), ..Default::default() }
    }

    /// Text content for a tombstoned path: re-assert the `Delete` if it holds nothing the delete
    /// hadn't seen, else bring the path back with it.
    fn text_for_tomb(&mut self, path: String, ops: Vec<u8>, vv: Vv, origin: Origin) -> Applied {
        let tomb = self.tombs.get_mut(&path).expect("caller checked the tombstone");
        if let Some(doc) = tomb.doc.as_mut().filter(|d| origin.is_some() && d.origin() == origin) {
            // The deleted doc itself: merge into it.
            let before = doc.version();
            if !doc.merge(&ops) {
                return self.tomb_merge_failed(&path);
            }
            if doc.version() == before {
                // Nothing new: learn the sender's tips (so a sender that is behind can prove it's
                // stale next round) and re-assert.
                let changed = union_tips(&mut tomb.t.vv, &vv);
                self.resync.remove(&path);
                self.parked.remove(&path);
                let mut a = self.reassert(&path);
                a.save = changed.then_some(path);
                return a;
            }
            // New ops for us — but maybe not past the delete: our kept doc can be behind `t.vv`
            // (we accepted a re-asserted `Delete`, or merged another deleter's tips), so a late or
            // reordered delta the deleter had already seen must not undo it.
            if seen_by(doc, &tomb.t.vv)
                || tomb.t.seen.contains(&fingerprint(doc.content().as_bytes()))
            {
                union_tips(&mut tomb.t.vv, &vv);
                self.resync.remove(&path);
                self.parked.remove(&path);
                let mut a = self.reassert(&path);
                a.save = Some(path); // the kept doc grew
                return a;
            }
            let doc = self.untomb(&path).and_then(|t| t.doc).expect("the tomb's doc just merged");
            return self.revive_text(path, doc, &vv);
        }
        let mut fresh = EgWalkerText::new(&self.agent);
        if !fresh.merge(&ops) {
            return self.tomb_merge_failed(&path);
        }
        self.resync.remove(&path);
        self.parked.remove(&path);
        let t = &self.tombs[&path].t;
        let causally_seen =
            t.origin.is_some() && fresh.origin() == t.origin && seen_by(&fresh, &t.vv);
        // An empty doc (no origin) holds nothing to bring back.
        if fresh.origin().is_none()
            || causally_seen
            || t.seen.contains(&fingerprint(fresh.content().as_bytes()))
        {
            return self.reassert(&path);
        }
        if let Some(old) = self.past_delete_covering(&path, &fresh) {
            // A doc deleted in an earlier round (its holder was away since): that round's Delete
            // lets the holder see it is stale; ours follows.
            return Applied { replies: vec![old, self.tomb_msg(&path)], ..Default::default() };
        }
        self.untomb(&path);
        self.revive_text(path, fresh, &vv)
    }

    /// LWW content for a tombstoned path: re-assert unless it's newer than the delete and unseen.
    fn lww_for_tomb(&mut self, path: String, version: u64, bytes: Vec<u8>) -> Applied {
        let t = &self.tombs[&path].t;
        if version <= t.version || t.seen.contains(&fingerprint(&bytes)) {
            return self.reassert(&path);
        }
        self.untomb(&path);
        self.lww.insert(path.clone(), (version, bytes.clone()));
        Applied { change: Some(Change::Write { path: path.clone(), bytes }), replies: Vec::new(), save: Some(path) }
    }
}

#[cfg(test)]
mod wire_compat {
    use super::*;

    /// Postcard encodes an enum as a varint of its variant index, so the FIRST
    /// byte of every encoded `SyncMsg` is its wire index (all ten fit in one
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
            (SyncMsg::StratDelete { path: "p".into(), ids: vec![1], seen: vec![] }, 7),
            (SyncMsg::Delete { path: "p".into(), ids: vec![1], seen: vec![], origin: None, vv: vec![], version: 0 }, 8),
            (SyncMsg::Restore { path: "p".into(), ids: vec![1] }, 9),
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
        let del = SyncMsg::StratDelete { path: "a.bin".into(), ids: vec![9], seen: vec![[7u8; 32]] };
        for msg in [advert, delta, del] {
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

    /// `StratDelete` round-trips its fields (path, ids, seen).
    #[test]
    fn strat_delete_round_trips() {
        let msg = SyncMsg::StratDelete {
            path: "d/x.bin".into(),
            ids: vec![u64::MAX, 3],
            seen: vec![[1; 32], [2; 32]],
        };
        let bytes = postcard::to_allocvec(&msg).unwrap();
        match postcard::from_bytes::<SyncMsg>(&bytes).unwrap() {
            SyncMsg::StratDelete { path, ids, seen } => {
                assert_eq!(path, "d/x.bin");
                assert_eq!(ids, vec![u64::MAX, 3]);
                assert_eq!(seen, vec![[1; 32], [2; 32]]);
            }
            _ => panic!("expected StratDelete"),
        }
    }

    /// A v5 peer (built before tree/mesh deletes) decodes `Delete`/`Restore` as an
    /// ERROR, not as garbage, so it skips them and keeps the deleted file instead of
    /// misreading the bytes as something else; `Hello` still decodes fine.
    #[test]
    fn v5_peers_skip_tree_deletes() {
        /// The wire enum exactly as a v5 peer knows it (indexes 0–7).
        #[derive(serde::Deserialize)]
        enum LegacyV5SyncMsg {
            #[allow(dead_code)]
            Hello { versions: Vec<(String, Vv)>, lww_versions: Vec<(String, u64)> },
            #[allow(dead_code)]
            TextDelta { path: String, ops: Vec<u8>, vv: Vv, origin: Origin },
            #[allow(dead_code)]
            Resync { path: String },
            #[allow(dead_code)]
            Lww { path: String, version: u64, bytes: Vec<u8> },
            #[allow(dead_code)]
            StratAdvert { path: String, sv: Vec<u8>, reply: bool },
            #[allow(dead_code)]
            StratDelta { path: String, bytes: Vec<u8> },
            #[allow(dead_code)]
            Version { proto: u32 },
            #[allow(dead_code)]
            StratDelete { path: String, ids: Vec<u64>, seen: Vec<[u8; 32]> },
        }

        let del = SyncMsg::Delete {
            path: "p".into(),
            ids: vec![1],
            seen: vec![],
            origin: None,
            vv: vec![],
            version: 0,
        };
        let restore = SyncMsg::Restore { path: "p".into(), ids: vec![1] };
        for msg in [del, restore] {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            assert!(
                postcard::from_bytes::<LegacyV5SyncMsg>(&bytes).is_err(),
                "a v5 peer must reject (and thus skip) tree/mesh deletes",
            );
        }
        let hello = postcard::to_allocvec(&SyncMsg::Hello { versions: vec![], lww_versions: vec![] }).unwrap();
        assert!(postcard::from_bytes::<LegacyV5SyncMsg>(&hello).is_ok());
    }

    /// `Delete` and `Restore` round-trip every field.
    #[test]
    fn tree_delete_round_trips() {
        let msg = SyncMsg::Delete {
            path: "d/x.bin".into(),
            ids: vec![u64::MAX, 3],
            seen: vec![[1; 32]],
            origin: Some(("a".into(), 0)),
            vv: vec![("a".into(), 3)],
            version: 7,
        };
        let bytes = postcard::to_allocvec(&msg).unwrap();
        match postcard::from_bytes::<SyncMsg>(&bytes).unwrap() {
            SyncMsg::Delete { path, ids, seen, origin, vv, version } => {
                assert_eq!(path, "d/x.bin");
                assert_eq!(ids, vec![u64::MAX, 3]);
                assert_eq!(seen, vec![[1; 32]]);
                assert_eq!(origin, Some(("a".into(), 0)));
                assert_eq!(vv, vec![("a".into(), 3)]);
                assert_eq!(version, 7);
            }
            _ => panic!("expected Delete"),
        }

        let restore = SyncMsg::Restore { path: "d/x.bin".into(), ids: vec![u64::MAX, 3] };
        let bytes = postcard::to_allocvec(&restore).unwrap();
        match postcard::from_bytes::<SyncMsg>(&bytes).unwrap() {
            SyncMsg::Restore { path, ids } => {
                assert_eq!(path, "d/x.bin");
                assert_eq!(ids, vec![u64::MAX, 3]);
            }
            _ => panic!("expected Restore"),
        }
    }

    /// Pin `MeshMsg`'s variant indexes too (Sync=0, Presence=1, Chunk=2) and
    /// prove a pre-chunking peer skips `Chunk` frames cleanly — that's what
    /// lets deployed browser tabs coexist with chunking peers, no reload.
    #[test]
    fn mesh_msg_indexes_pinned_and_legacy_peers_skip_chunks() {
        use crate::identity::{DeviceKey, SignedWrite};
        let k = DeviceKey::from_seed(&[5u8; 32]);
        let cases: Vec<(MeshMsg, u8)> = vec![
            (MeshMsg::Sync(SyncMsg::Resync { path: "p".into() }), 0),
            (MeshMsg::Presence { id: [0; 32], neighbors: vec![] }, 1),
            (MeshMsg::Chunk { id: 1, index: 0, total: 2, bytes: vec![9] }, 2),
            (MeshMsg::Version { id: [0; 32], proto: 1 }, 3),
            (MeshMsg::Trust(k.genesis()), 4),
            (MeshMsg::SignedSync(SignedWrite { author: [0; 32], sig: vec![], payload: vec![] }), 5),
            (MeshMsg::Request(crate::onboard::JoinRequest::create(&k, "x")), 6),
            (MeshMsg::Epoch(crate::epoch::EpochAnnounce::create(&k, 1, [0; 8], vec![])), 7),
            (MeshMsg::Sealed(crate::epoch::SealedMsg { seq: 1, id: [0; 8], body: vec![] }), 8),
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
        let trust = MeshMsg::Trust(k.genesis());
        let signed = MeshMsg::SignedSync(SignedWrite { author: [1; 32], sig: vec![1], payload: vec![2] });
        for msg in [chunk, version, trust, signed] {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            assert!(
                postcard::from_bytes::<LegacyMeshMsg>(&bytes).is_err(),
                "legacy peers must reject (and thus skip) post-v0 frames",
            );
        }
    }

    /// A v3 peer (device-linking auth, no epoch support) rejects the APPENDED
    /// `Epoch`/`Sealed` variants and skips them, rather than misdecoding.
    #[test]
    fn v3_peers_skip_epoch_and_sealed() {
        use crate::identity::{DeviceKey, SignedWrite};
        let k = DeviceKey::from_seed(&[6u8; 32]);

        /// The mesh wire enum exactly as v3 peers know it.
        #[derive(serde::Deserialize)]
        enum LegacyV3MeshMsg {
            #[allow(dead_code)]
            Sync(SyncMsg),
            #[allow(dead_code)]
            Presence { id: [u8; 32], neighbors: Vec<[u8; 32]> },
            #[allow(dead_code)]
            Chunk { id: u64, index: u32, total: u32, bytes: Vec<u8> },
            #[allow(dead_code)]
            Version { id: [u8; 32], proto: u32 },
            #[allow(dead_code)]
            Trust(crate::identity::TrustEntry),
            #[allow(dead_code)]
            SignedSync(SignedWrite),
            #[allow(dead_code)]
            Request(crate::onboard::JoinRequest),
        }

        let epoch = MeshMsg::Epoch(crate::epoch::EpochAnnounce::create(&k, 1, [0; 8], vec![]));
        let sealed = MeshMsg::Sealed(crate::epoch::SealedMsg { seq: 1, id: [0; 8], body: vec![1] });
        for msg in [epoch, sealed] {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            assert!(
                postcard::from_bytes::<LegacyV3MeshMsg>(&bytes).is_err(),
                "v3 peers must reject (and thus skip) epoch-sealed frames",
            );
        }
    }

    /// Protocol v5: two APPENDED `TrustEntry` variants (`Passkey`, `PasskeyLink`).
    /// Pin their postcard discriminants, and prove a v4 peer — which doesn't know
    /// about either — fails to decode (and thus skips) a `Trust` frame carrying one.
    #[test]
    fn trust_entry_indexes_pinned_and_v4_peers_skip_passkey_entries() {
        use crate::identity::{DeviceKey, Role, TrustEntry};
        use crate::passkey::{PasskeyAssertion, PasskeyCredential, ALG_ES256};
        let k = DeviceKey::from_seed(&[7u8; 32]);
        let cred =
            PasskeyCredential { id: vec![1; 16], alg: ALG_ES256, public_key: vec![4; 65], rp_id: "localhost".into() };
        let link = TrustEntry::PasskeyLink {
            device: [2; 32],
            cred_id: vec![1; 16],
            nonce: [3; 16],
            assertion: PasskeyAssertion {
                authenticator_data: vec![0; 37],
                client_data_json: b"{}".to_vec(),
                signature: vec![1],
            },
        };

        let cases: Vec<(TrustEntry, u8)> = vec![
            (k.genesis(), 0),
            (k.grant([1; 32], Role::Member, [0; 16]), 1),
            (k.revoke([1; 32], [0; 16]), 2),
            (k.register_passkey(cred.clone(), Role::Admin, [0; 16]), 3),
            (link.clone(), 4),
        ];
        for (e, index) in cases {
            assert_eq!(postcard::to_allocvec(&e).unwrap()[0], index, "TrustEntry variant index moved");
            let bytes = postcard::to_allocvec(&MeshMsg::Trust(e)).unwrap();
            assert_eq!(&bytes[..2], [4, index], "MeshMsg::Trust(..) wire prefix moved");
        }

        /// The `TrustEntry` enum exactly as v4 peers know it (no Passkey/PasskeyLink).
        #[derive(serde::Deserialize)]
        enum LegacyV4TrustEntry {
            #[allow(dead_code)]
            Genesis { founder: [u8; 32], sig: Vec<u8> },
            #[allow(dead_code)]
            Grant { device: [u8; 32], role: Role, by: [u8; 32], nonce: [u8; 16], sig: Vec<u8> },
            #[allow(dead_code)]
            Revoke { device: [u8; 32], by: [u8; 32], nonce: [u8; 16], sig: Vec<u8> },
        }

        /// The mesh wire enum exactly as v4 peers know it.
        #[derive(serde::Deserialize)]
        enum LegacyV4MeshMsg {
            #[allow(dead_code)]
            Sync(SyncMsg),
            #[allow(dead_code)]
            Presence { id: [u8; 32], neighbors: Vec<[u8; 32]> },
            #[allow(dead_code)]
            Chunk { id: u64, index: u32, total: u32, bytes: Vec<u8> },
            #[allow(dead_code)]
            Version { id: [u8; 32], proto: u32 },
            #[allow(dead_code)]
            Trust(LegacyV4TrustEntry),
        }

        let reg = k.register_passkey(cred, Role::Admin, [0; 16]);
        for e in [reg, link] {
            let bytes = postcard::to_allocvec(&MeshMsg::Trust(e)).unwrap();
            assert!(
                postcard::from_bytes::<LegacyV4MeshMsg>(&bytes).is_err(),
                "v4 peers must reject (and thus skip) passkey entries",
            );
        }

        // Sanity check: the legacy enum matches on an entry it does know.
        let bytes = postcard::to_allocvec(&MeshMsg::Trust(k.genesis())).unwrap();
        assert!(postcard::from_bytes::<LegacyV4MeshMsg>(&bytes).is_ok());
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
        let (persist, replies) =
            s.apply(SyncMsg::StratDelete { path: "x.bin".into(), ids: vec![1], seen: vec![] });
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

    #[test]
    fn eviction_is_counted() {
        fn payload(i: usize) -> MeshMsg {
            let mut bytes = vec![0u8; 10_000];
            let mut x: u32 = 0x1234_5678 ^ i as u32;
            for b in &mut bytes {
                x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                *b = (x >> 24) as u8;
            }
            MeshMsg::Sync(SyncMsg::Lww { path: format!("f{i}.bin"), version: 1, bytes })
        }

        let messages: Vec<Vec<Vec<u8>>> = (0..65).map(|i| encode_mesh_frames(&payload(i))).collect();
        for frames in &messages {
            assert!(frames.len() > 1, "a 10 KB payload must chunk");
        }

        let mut asm = MeshAssembler::new();
        for frames in &messages {
            asm.feed(&frames[0]);
        }
        assert_eq!(asm.evictions(), 1);

        // Message #1 kept its slot — its remaining frames complete it. Checked
        // FIRST: completing it removes its slot from tracking, so it can't be
        // knocked out by the eviction that re-touching #0 below triggers.
        let mut completed1 = false;
        for f in &messages[1][1..] {
            if asm.feed(f).is_some() {
                completed1 = true;
            }
        }
        assert!(completed1, "message #1 still had its slot and completes");

        // Message #0's slot was evicted — its remaining frames can't complete it.
        let mut completed0 = false;
        for f in &messages[0][1..] {
            if asm.feed(f).is_some() {
                completed0 = true;
            }
        }
        assert!(!completed0, "an evicted message cannot be completed by its remaining frames");
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
    fn duplicate_delta_persists_nothing() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        let m = a.local_text("note.md", "hello").unwrap();
        let bytes = postcard::to_allocvec(&m).unwrap();
        let dup: SyncMsg = postcard::from_bytes(&bytes).unwrap();

        let (persisted, _) = b.apply(m);
        assert!(persisted.is_some());
        let g = b.generation();

        let (persisted, replies) = b.apply(dup);
        assert_eq!(persisted, None);
        assert!(replies.is_empty());
        assert_eq!(b.generation(), g, "a duplicate delta does not bump generation");
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

    /// Apply each message to `s`; the last bytes it persisted for `path`, if any.
    fn content_after(s: &mut Syncer, msgs: Vec<SyncMsg>, path: &str) -> Option<Vec<u8>> {
        let mut out = None;
        for m in msgs {
            if let (Some((p, bytes)), _) = s.apply(m) {
                if p == path {
                    out = Some(bytes);
                }
            }
        }
        out
    }

    /// The origin carried by `path`'s `TextDelta` in `msgs`.
    fn text_origin(msgs: &[SyncMsg], path: &str) -> Origin {
        msgs.iter()
            .find_map(|m| match m {
                SyncMsg::TextDelta { path: p, origin, .. } if p == path => Some(origin.clone()),
                _ => None,
            })
            .expect("a TextDelta for the path")
    }

    /// The version carried by `path`'s `Lww` in `msgs`.
    fn lww_version(msgs: &[SyncMsg], path: &str) -> u64 {
        msgs.iter()
            .find_map(|m| match m {
                SyncMsg::Lww { path: p, version, .. } if p == path => Some(*version),
                _ => None,
            })
            .expect("an Lww for the path")
    }

    #[test]
    fn restored_text_keeps_its_origin_and_merges_cleanly() {
        let mut a = Syncer::new("a");
        let mut c = Syncer::new("c");
        let m = a.local_text("x.md", "hello\n").unwrap();
        assert_eq!(content_after(&mut c, vec![m], "x.md").as_deref(), Some(&b"hello\n"[..]));
        let rec = a.durable("x.md").unwrap();

        let mut b = Syncer::new("b");
        assert!(matches!(b.restore(rec, Some(b"hello\n"), 0), Restored::Clean));
        let origin = text_origin(&a.full_state(), "x.md");
        assert!(origin.is_some());
        assert_eq!(text_origin(&b.full_state(), "x.md"), origin);

        // A peer's concurrent edit on the shared history merges cleanly — not an
        // origin-conflict adoption (b's origin is unchanged).
        let edit = c.local_text("x.md", "hello world\n").unwrap();
        assert_eq!(
            content_after(&mut b, vec![edit], "x.md").as_deref(),
            Some(&b"hello world\n"[..])
        );
        assert_eq!(b.docs["x.md"].origin(), origin);
    }

    #[test]
    fn restore_turns_an_offline_file_change_into_a_local_edit() {
        let mut a = Syncer::new("a");
        let mut c = Syncer::new("c");
        let m = a.local_text("x.md", "hello\n").unwrap();
        content_after(&mut c, vec![m], "x.md");
        let rec = a.durable("x.md").unwrap();

        let mut b = Syncer::new("b");
        let Restored::Changed(msg) = b.restore(rec, Some(b"hello again\n"), 0) else {
            panic!("an offline change restores as Changed");
        };
        assert!(matches!(msg, SyncMsg::TextDelta { .. }));
        assert_eq!(
            content_after(&mut c, vec![msg], "x.md").as_deref(),
            Some(&b"hello again\n"[..])
        );
        assert_eq!(c.docs["x.md"].origin(), a.docs["x.md"].origin());
    }

    #[test]
    fn restored_lww_keeps_its_version() {
        let mut a = Syncer::new("a");
        a.local_lww("m.toml", b"x".to_vec(), 100);
        let rec = a.durable("m.toml").unwrap();
        let mut b = Syncer::new("b");
        assert!(matches!(b.restore(rec, Some(b"x"), 9_999), Restored::Clean));
        assert_eq!(lww_version(&b.full_state(), "m.toml"), 100);
    }

    #[test]
    fn restored_lww_bumps_on_an_offline_change() {
        let mut a = Syncer::new("a");
        a.local_lww("m.toml", b"x".to_vec(), 100);
        let rec = a.durable("m.toml").unwrap();
        let mut b = Syncer::new("b");
        let Restored::Changed(SyncMsg::Lww { path, version, bytes }) = b.restore(rec, Some(b"y"), 50)
        else {
            panic!("an offline LWW change restores as Changed(Lww)");
        };
        assert_eq!((path.as_str(), version, bytes.as_slice()), ("m.toml", 101, &b"y"[..]));
        assert_eq!(lww_version(&b.full_state(), "m.toml"), 101);
    }

    #[test]
    fn restore_refuses_mismatched_kinds_and_corrupt_records() {
        let mut a = Syncer::new("a");
        a.local_text("x.md", "hello\n");
        let rec = a.durable("x.md").unwrap();
        let mut b = Syncer::new("b");

        let text_for_toml = DurablePath {
            path: "m.toml".into(),
            kind: DurableKind::Text { ops: a.docs["x.md"].encode_full() },
            disk: [0; 32],
        };
        assert!(matches!(b.restore(text_for_toml, Some(b"x"), 0), Restored::Unusable));

        let corrupt = DurablePath {
            path: "y.md".into(),
            kind: DurableKind::Text { ops: vec![1, 2, 3] },
            disk: [0; 32],
        };
        assert!(matches!(b.restore(corrupt, Some(b"y"), 0), Restored::Unusable));
        assert!(b.durable("y.md").is_none(), "a corrupt record leaves no state");

        let hidden = DurablePath {
            path: ".hidden.md".into(),
            kind: DurableKind::Text { ops: a.docs["x.md"].encode_full() },
            disk: [0; 32],
        };
        assert!(matches!(b.restore(hidden, Some(b"x"), 0), Restored::Unusable));

        assert!(matches!(b.restore(rec.clone(), Some(b"hello\n"), 0), Restored::Clean));
        assert!(matches!(b.restore(rec, Some(b"hello\n"), 0), Restored::Unusable));
    }

    #[test]
    fn host_restart_then_hello_pulls_a_joiners_gap_edits() {
        let mut h = Syncer::new("h1");
        let mut j = Syncer::new("j");
        let t = h.local_text("card.md", "# one\n").unwrap();
        let l = h.local_lww("meta.toml", b"col=Todo".to_vec(), 1_000);
        content_after(&mut j, vec![t, l], "card.md");
        let rt = h.durable("card.md").unwrap();
        let rl = h.durable("meta.toml").unwrap();
        drop(h);

        // The joiner edits while the host is down.
        j.local_text("card.md", "# one\ngap\n");
        j.local_lww("meta.toml", b"col=Done".to_vec(), 2_000);

        let mut h2 = Syncer::new("h2");
        assert!(matches!(h2.restore(rt, Some(b"# one\n"), 3_000), Restored::Clean));
        assert!(matches!(h2.restore(rl, Some(b"col=Todo"), 3_000), Restored::Clean));

        let (_, replies) = j.apply(h2.hello());
        let mut persisted: HashMap<String, Vec<u8>> = HashMap::new();
        for m in replies {
            if let (Some((p, bytes)), _) = h2.apply(m) {
                persisted.insert(p, bytes);
            }
        }
        assert_eq!(persisted.get("card.md").map(Vec::as_slice), Some(&b"# one\ngap\n"[..]));
        assert_eq!(persisted.get("meta.toml").map(Vec::as_slice), Some(&b"col=Done"[..]));

        // The host's catch-up never regresses the joiner.
        for m in h2.full_state() {
            if let (Some((p, bytes)), _) = j.apply(m) {
                let current = match j.docs.get(&p) {
                    Some(doc) => doc.content().into_bytes(),
                    None => j.lww[&p].1.clone(),
                };
                assert_eq!(bytes, current, "{p} persisted something other than its content");
            }
        }
        assert_eq!(j.docs["card.md"].content(), "# one\ngap\n");
        assert_eq!(j.lww["meta.toml"].1, b"col=Done");
    }

    #[test]
    fn durable_record_layout_is_pinned() {
        let lww = DurablePath { path: "a".into(), kind: DurableKind::Lww { version: 1 }, disk: [0; 32] };
        let bytes = postcard::to_allocvec(&lww).unwrap();
        let mut expected = vec![1u8, 97, 1, 1];
        expected.extend_from_slice(&[0u8; 32]);
        assert_eq!(bytes, expected);

        let text = DurablePath { path: "a".into(), kind: DurableKind::Text { ops: vec![7] }, disk: [0; 32] };
        let bytes = postcard::to_allocvec(&text).unwrap();
        assert!(bytes.starts_with(&[1, 97, 0, 1, 7]), "got {bytes:?}");
        assert_eq!(postcard::from_bytes::<DurablePath>(&bytes).unwrap(), text);

        let tomb = TreeTombstone { ids: vec![], seen: vec![], origin: None, vv: vec![], version: 0, prior: vec![] };
        let past = vec![PastTomb { tomb: tomb.clone(), ops: vec![9] }];
        let deleted = DurablePath {
            path: "a".into(),
            kind: DurableKind::Deleted { tomb, ops: vec![], past },
            disk: [0; 32],
        };
        let bytes = postcard::to_allocvec(&deleted).unwrap();
        // path, variant 2, tomb (6 empty fields), ops [], past [{empty tomb, ops [9]}], disk.
        let mut expected = vec![1u8, 97, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 9];
        expected.extend_from_slice(&[0u8; 32]);
        assert_eq!(bytes, expected);
        assert_eq!(postcard::from_bytes::<DurablePath>(&bytes).unwrap(), deleted);

        let reborn = DurablePath {
            path: "a".into(),
            kind: DurableKind::TextReborn { ops: vec![7], superseded: vec![1], past: vec![] },
            disk: [0; 32],
        };
        let bytes = postcard::to_allocvec(&reborn).unwrap();
        // path, variant 3, ops [7], superseded [1], past [], disk.
        let mut expected = vec![1u8, 97, 3, 1, 7, 1, 1, 0];
        expected.extend_from_slice(&[0u8; 32]);
        assert_eq!(bytes, expected);
        assert_eq!(postcard::from_bytes::<DurablePath>(&bytes).unwrap(), reborn);

        let lww_reborn = DurablePath {
            path: "a".into(),
            kind: DurableKind::LwwReborn { version: 1, superseded: vec![1] },
            disk: [0; 32],
        };
        let bytes = postcard::to_allocvec(&lww_reborn).unwrap();
        assert_eq!(&bytes[..3], [1, 97, 4]);
        assert_eq!(postcard::from_bytes::<DurablePath>(&bytes).unwrap(), lww_reborn);
    }

    #[test]
    fn record_name_is_fixed_length_hex() {
        let n = record_name("tickets/tk_1/card.md");
        assert_eq!(n.len(), 32);
        assert!(n.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_eq!(n, record_name("tickets/tk_1/card.md"));
        assert_ne!(record_name("a"), record_name("b"));
    }

    #[test]
    fn durable_tombstone_never_resurrects() {
        let mut a = Syncer::new("a");
        let t = a.local_text("x.md", "hello\n").unwrap();
        let l = a.local_lww("m.toml", b"col=Todo".to_vec(), 1000);
        let mut c = Syncer::new("c");
        c.apply(t);
        c.apply(l);

        a.local_delete("x.md").unwrap();
        a.local_delete("m.toml").unwrap();

        let rx = a.durable("x.md").unwrap();
        let rm = a.durable("m.toml").unwrap();
        assert!(matches!(&rx.kind, DurableKind::Deleted { ops, .. } if !ops.is_empty()));
        assert!(matches!(&rm.kind, DurableKind::Deleted { ops, .. } if ops.is_empty()));

        let mut a2 = Syncer::new("a");
        assert!(matches!(a2.restore(rx, None, 0), Restored::Clean));
        assert!(matches!(a2.restore(rm, None, 0), Restored::Clean));
        assert!(a2.is_tombstoned("x.md") && a2.is_tombstoned("m.toml"));

        for m in c.full_state() {
            let applied = a2.receive(m);
            assert!(applied.change.is_none());
            assert!(matches!(applied.replies.as_slice(), [SyncMsg::Delete { .. }]));
        }
    }

    #[test]
    fn restore_of_a_missing_file_is_an_offline_delete() {
        let mut s1 = Syncer::new("s1");
        s1.local_text("x.md", "hi\n");
        s1.local_lww("m.toml", b"col=Todo".to_vec(), 100);
        let rx = s1.durable("x.md").unwrap();
        let rm = s1.durable("m.toml").unwrap();

        let live_x = s1.local_delete("x.md").unwrap();
        let live_m = s1.local_delete("m.toml").unwrap();

        let mut s2 = Syncer::new("s2");
        let Restored::Deleted(dx) = s2.restore(rx, None, 0) else { panic!("expected Deleted") };
        match (&dx, &live_x) {
            (SyncMsg::Delete { ids: a, version: 0, origin, .. }, SyncMsg::Delete { ids: b, .. }) => {
                assert_eq!(a, b);
                assert!(origin.is_some());
            }
            _ => panic!("expected matching Delete messages"),
        }
        assert!(matches!(s2.durable("x.md").unwrap().kind, DurableKind::Deleted { .. }));

        let Restored::Deleted(dm) = s2.restore(rm, None, 0) else { panic!("expected Deleted") };
        match (&dm, &live_m) {
            (SyncMsg::Delete { ids: a, version: 100, .. }, SyncMsg::Delete { ids: b, .. }) => {
                assert_eq!(a, b);
            }
            _ => panic!("expected matching Delete messages"),
        }
        assert!(matches!(s2.durable("m.toml").unwrap().kind, DurableKind::Deleted { .. }));
    }

    #[test]
    fn restore_of_a_tombstone_with_a_file_is_a_recreation() {
        let mut s1 = Syncer::new("s1");
        s1.local_text("x.md", "gone\n");
        let old = s1.local_delete("x.md").unwrap();
        let old_origin = msg_origin(&old);
        let old_ids = delete_ids(&old);
        let rec = s1.durable("x.md").unwrap();
        assert!(matches!(rec.kind, DurableKind::Deleted { .. }));

        let mut s2 = Syncer::new("s2");
        let Restored::Changed(m) = s2.restore(rec, Some(b"# back\n"), 0) else { panic!("expected Changed") };
        match &m {
            SyncMsg::TextDelta { origin, .. } => assert_ne!(*origin, old_origin),
            _ => panic!("expected a TextDelta"),
        }
        assert!(!s2.is_tombstoned("x.md"));
        match s2.durable("x.md").unwrap().kind {
            DurableKind::TextReborn { superseded, .. } => assert_eq!(superseded, old_ids),
            other => panic!("expected TextReborn, got {other:?}"),
        }

        let a = s2.receive(dup(&old));
        assert!(a.change.is_none());
        match a.replies.as_slice() {
            [SyncMsg::Restore { ids, .. }, SyncMsg::TextDelta { .. }] => assert_eq!(ids, &old_ids),
            _ => panic!("expected [Restore, TextDelta]"),
        }

        let mut s3 = Syncer::new("s3");
        assert!(matches!(s3.restore(s2.durable("x.md").unwrap(), Some(b"# back\n"), 0), Restored::Clean));
        let a3 = s3.receive(dup(&old));
        assert!(matches!(a3.replies.first(), Some(SyncMsg::Restore { .. })));

        // Same for LWW.
        let mut l1 = Syncer::new("l1");
        l1.local_lww("m.toml", b"col=Todo".to_vec(), 1000);
        let old_l = l1.local_delete("m.toml").unwrap();
        let old_l_version = match &old_l {
            SyncMsg::Delete { version, .. } => *version,
            _ => panic!("expected a Delete"),
        };
        let rec_l = l1.durable("m.toml").unwrap();
        let mut l2 = Syncer::new("l2");
        let Restored::Changed(ml) = l2.restore(rec_l, Some(b"col=Done"), 0) else { panic!("expected Changed") };
        match &ml {
            SyncMsg::Lww { version, .. } => assert!(*version > old_l_version),
            _ => panic!("expected an Lww"),
        }
        assert!(matches!(l2.durable("m.toml").unwrap().kind, DurableKind::LwwReborn { .. }));
    }

    #[test]
    fn compare_hello_detects_each_direction() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        sync(&mut a, &mut b);
        assert!(a.compare_hello(&b.hello()).unwrap().consistent());
        assert!(b.compare_hello(&a.hello()).unwrap().consistent());

        a.local_text("n.md", "x\ny");
        assert_eq!(
            b.compare_hello(&a.hello()),
            Some(HelloCmp { we_ahead: false, they_ahead: true })
        );
        assert_eq!(
            a.compare_hello(&b.hello()),
            Some(HelloCmp { we_ahead: true, they_ahead: false })
        );

        a.local_lww("m.toml", b"x".to_vec(), 100);
        assert_eq!(
            b.compare_hello(&a.hello()),
            Some(HelloCmp { we_ahead: false, they_ahead: true })
        );
        assert_eq!(
            a.compare_hello(&b.hello()),
            Some(HelloCmp { we_ahead: true, they_ahead: false })
        );

        sync(&mut a, &mut b);
        assert!(a.compare_hello(&b.hello()).unwrap().consistent());
        assert!(b.compare_hello(&a.hello()).unwrap().consistent());

        assert!(a.compare_hello(&SyncMsg::Resync { path: "x".into() }).is_none());
    }

    #[test]
    fn compare_hello_ignores_parked_paths() {
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
        assert!(matches!(first.as_slice(), [SyncMsg::Resync { .. }]));
        let mut fed = 0;
        while dst.parked_paths().is_empty() {
            dst.apply(bad(&probe));
            fed += 1;
            assert!(fed < 32, "must park after the cap, not loop forever");
        }
        assert_eq!(dst.parked_paths(), vec!["note.md".to_string()]);

        let mut src = Syncer::new("s");
        src.local_text("note.md", "one\n");
        src.local_text("note.md", "one\ntwo\n");

        let cmp = dst.compare_hello(&src.hello()).unwrap();
        assert!(!cmp.they_ahead, "a parked path is ignored by compare_hello");
    }

    #[test]
    fn independent_same_path_docs_are_inconsistent_both_ways() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        a.local_text("index.md", "# A\n");
        b.local_text("index.md", "# B\n");
        let ab = a.compare_hello(&b.hello()).unwrap();
        assert!(ab.we_ahead && ab.they_ahead);
        let ba = b.compare_hello(&a.hello()).unwrap();
        assert!(ba.we_ahead && ba.they_ahead);
    }

    #[test]
    fn generation_moves_only_on_real_change() {
        let mut a = Syncer::new("a");
        let g0 = a.generation();
        a.local_text("n.md", "hello");
        assert!(a.generation() > g0, "new content bumps generation");
        let g1 = a.generation();
        a.local_text("n.md", "hello");
        assert_eq!(a.generation(), g1, "same content does not bump");

        let mut b = Syncer::new("b");
        let gb0 = b.generation();
        b.local_lww("m.toml", b"x".to_vec(), 100);
        assert!(b.generation() > gb0, "local_lww always bumps");

        let lww = || SyncMsg::Lww { path: "m.toml".into(), version: 100, bytes: b"x".to_vec() };
        let mut c = Syncer::new("c");
        let gc0 = c.generation();
        c.apply(lww());
        assert!(c.generation() > gc0, "a newer Lww bumps on apply");
        let gc1 = c.generation();
        c.apply(lww());
        assert_eq!(c.generation(), gc1, "applying the same Lww twice bumps only once");
        let stale = SyncMsg::Lww { path: "m.toml".into(), version: 1, bytes: b"y".to_vec() };
        c.apply(stale);
        assert_eq!(c.generation(), gc1, "a stale Lww does not bump");

        let mut src = Syncer::new("s");
        let mut dst = Syncer::new("t");
        src.local_text("doc.md", "one").unwrap();
        let ops = src.docs["doc.md"].encode_full();
        let delta = |ops: &[u8]| SyncMsg::TextDelta {
            path: "doc.md".into(),
            ops: ops.to_vec(),
            vv: Vec::new(),
            origin: None,
        };
        let gd0 = dst.generation();
        dst.apply(delta(&ops));
        assert!(dst.generation() > gd0, "a TextDelta with new ops bumps");
        let gd1 = dst.generation();
        dst.apply(delta(&ops));
        assert_eq!(dst.generation(), gd1, "applying it a second time does not bump");

        let g_before_hello = dst.generation();
        let _ = dst.hello();
        assert_eq!(dst.generation(), g_before_hello, "hello() never bumps");
        dst.apply(src.hello());
        assert_eq!(dst.generation(), g_before_hello, "apply(Hello) never bumps");
    }

    // ---- tree deletes (tombstones) ----

    /// A wire round-trip copy (as a peer would receive it).
    fn dup(m: &SyncMsg) -> SyncMsg {
        postcard::from_bytes(&postcard::to_allocvec(m).unwrap()).unwrap()
    }

    fn text_of(s: &Syncer, path: &str) -> Option<String> {
        s.docs.get(path).map(|d| d.content())
    }

    /// Epidemic delivery: every message reaches every peer but its sender; every reply is
    /// flooded the same way. Returns the number of messages delivered.
    fn flood(peers: &mut [Syncer], from: usize, msgs: Vec<SyncMsg>) -> usize {
        let mut queue: std::collections::VecDeque<(usize, SyncMsg)> =
            msgs.into_iter().map(|m| (from, m)).collect();
        let mut n = 0;
        while let Some((src, m)) = queue.pop_front() {
            n += 1;
            assert!(n < 500, "message storm");
            for (i, peer) in peers.iter_mut().enumerate() {
                if i == src {
                    continue;
                }
                for r in peer.receive(dup(&m)).replies {
                    queue.push_back((i, r));
                }
            }
        }
        n
    }

    fn is_noop(a: &Applied) -> bool {
        a.change.is_none() && a.replies.is_empty() && a.save.is_none()
    }

    fn is_delete_of(m: &SyncMsg, p: &str) -> bool {
        matches!(m, SyncMsg::Delete { path, .. } if path == p)
    }

    fn delete_ids(m: &SyncMsg) -> Vec<u64> {
        match m {
            SyncMsg::Delete { ids, .. } => ids.clone(),
            _ => panic!("expected a Delete"),
        }
    }

    fn msg_origin(m: &SyncMsg) -> Origin {
        match m {
            SyncMsg::TextDelta { origin, .. } | SyncMsg::Delete { origin, .. } => origin.clone(),
            _ => panic!("expected a TextDelta or Delete"),
        }
    }

    fn three() -> Vec<Syncer> {
        vec![Syncer::new("p0"), Syncer::new("p1"), Syncer::new("p2")]
    }

    #[test]
    fn local_delete_tombstones_and_reasserts() {
        let mut a = Syncer::new("a");
        a.local_text("x.md", "hello\n");
        a.local_lww("m.toml", b"col=Todo".to_vec(), 1000);

        let d = a.local_delete("x.md").unwrap();
        match &d {
            SyncMsg::Delete { path, ids, seen, origin, vv, version } => {
                assert_eq!(path, "x.md");
                assert_eq!(seen, &vec![fingerprint(b"hello\n")]);
                assert!(origin.is_some());
                assert!(!vv.is_empty());
                assert_eq!(*version, 0);
                assert_eq!(ids.len(), 1);
            }
            _ => panic!("expected a Delete"),
        }
        assert!(!a.docs.contains_key("x.md"));
        let again = a.local_delete("x.md").unwrap();
        assert_eq!(postcard::to_allocvec(&again).unwrap(), postcard::to_allocvec(&d).unwrap());

        match a.local_delete("m.toml").unwrap() {
            SyncMsg::Delete { version, origin, vv, seen, .. } => {
                assert_eq!(version, 1000);
                assert!(origin.is_none() && vv.is_empty());
                assert_eq!(seen, vec![fingerprint(b"col=Todo")]);
            }
            _ => panic!("expected a Delete"),
        }
        assert!(!a.lww.contains_key("m.toml"));

        assert!(a.local_delete("nope.md").is_none(), "nothing held");
        for p in [".site", "../x", "a/.h/b.md"] {
            a.local_text(p, "x\n");
            assert!(a.local_delete(p).is_none(), "{p} is not deletable");
            assert!(a.docs.contains_key(p));
        }
        a.local_text("b.md", "b\n");
        a.local_delete("b.md").unwrap();
        assert_eq!(a.tombstoned_paths(), vec!["b.md", "m.toml", "x.md"]);
    }

    #[test]
    fn delete_propagates_to_three_peers() {
        let mut p = three();
        let t = p[0].local_text("card.md", "# card\n").unwrap();
        let l = p[0].local_lww("meta.toml", b"col=Todo".to_vec(), 1000);
        flood(&mut p, 0, vec![t, l]);
        for s in &p {
            assert_eq!(text_of(s, "card.md").as_deref(), Some("# card\n"));
            assert!(s.lww.contains_key("meta.toml"));
        }

        let d1 = p[0].local_delete("card.md").unwrap();
        let d2 = p[0].local_delete("meta.toml").unwrap();
        let a = p[1].receive(dup(&d1));
        assert_eq!(a.change, Some(Change::Remove { path: "card.md".into() }));
        assert_eq!(a.save.as_deref(), Some("card.md"));
        assert!(a.replies.is_empty());

        flood(&mut p, 0, vec![d1, d2]);
        for s in &p {
            assert!(s.is_tombstoned("card.md") && s.is_tombstoned("meta.toml"));
            assert!(!s.docs.contains_key("card.md") && !s.lww.contains_key("meta.toml"));
        }
    }

    #[test]
    fn concurrent_text_edit_beats_delete() {
        let mut p = three();
        let base = p[0].local_text("x.md", "base\n").unwrap();
        flood(&mut p, 0, vec![base]);

        let d = p[0].local_delete("x.md").unwrap();
        let e = p[1].local_text("x.md", "base\nedit by b\n").unwrap(); // held back
        flood(&mut p, 0, vec![d]);
        flood(&mut p, 1, vec![e]);
        for s in &p {
            assert_eq!(text_of(s, "x.md").as_deref(), Some("base\nedit by b\n"));
            assert!(!s.is_tombstoned("x.md"));
        }
    }

    #[test]
    fn concurrent_newer_lww_beats_delete() {
        let mut p = three();
        let l = p[0].local_lww("m.toml", b"col=Todo".to_vec(), 1000);
        flood(&mut p, 0, vec![l]);

        let d = p[0].local_delete("m.toml").unwrap();
        let w = p[1].local_lww("m.toml", b"col=Done".to_vec(), 2000); // held back
        flood(&mut p, 0, vec![d]);
        flood(&mut p, 1, vec![w]);
        for s in &p {
            assert_eq!(s.lww.get("m.toml"), Some(&(2000, b"col=Done".to_vec())));
            assert!(!s.is_tombstoned("m.toml"));
        }
    }

    #[test]
    fn stale_lww_loses_to_delete() {
        let mut p = three();
        let old = SyncMsg::Lww { path: "m.toml".into(), version: 500, bytes: b"old".to_vec() };
        p[1].receive(dup(&old));
        let cur = SyncMsg::Lww { path: "m.toml".into(), version: 1000, bytes: b"cur".to_vec() };
        p[0].receive(dup(&cur));
        p[2].receive(dup(&cur));

        let d = p[0].local_delete("m.toml").unwrap();
        flood(&mut p, 0, vec![d]);
        assert!(p.iter().all(|s| s.is_tombstoned("m.toml")));

        flood(&mut p, 1, vec![old]);
        for s in &p {
            assert!(s.is_tombstoned("m.toml"), "a stale copy brings nothing back");
            assert!(!s.lww.contains_key("m.toml"));
        }
    }

    #[test]
    fn hello_and_resync_answer_with_the_tombstone() {
        let mut a = Syncer::new("a");
        let mut c = Syncer::new("c");
        c.receive(a.local_text("x.md", "hi\n").unwrap());
        a.local_delete("x.md").unwrap();

        let r = a.receive(c.hello());
        assert!(r.replies.iter().any(|m| is_delete_of(m, "x.md")));
        for m in r.replies {
            c.receive(m);
        }
        assert!(c.is_tombstoned("x.md"));

        let r = a.receive(SyncMsg::Resync { path: "x.md".into() });
        assert!(matches!(r.replies.as_slice(), [SyncMsg::Delete { .. }]));
        assert!(r.save.is_none());
        assert!(a.full_state().iter().any(|m| is_delete_of(m, "x.md")));

        let mut j = Syncer::new("j");
        let r = a.receive(j.hello());
        assert!(!r.replies.iter().any(|m| matches!(m, SyncMsg::Delete { .. })));
        for m in a.full_state() {
            assert!(j.receive(m).change.is_none());
        }
        assert!(j.is_tombstoned("x.md"));
    }

    #[test]
    fn reseeded_identical_copy_is_deleted() {
        let mut a = Syncer::new("a");
        a.local_text("x.md", "same\n");
        a.local_lww("m.toml", b"col=Todo".to_vec(), 1000);
        a.local_delete("x.md").unwrap();
        a.local_delete("m.toml").unwrap();

        let mut c2 = Syncer::new("c2");
        c2.local_text("x.md", "same\n");
        c2.local_lww("m.toml", b"col=Todo".to_vec(), 5000);
        let state = c2.full_state();
        let mut p = vec![a, c2];
        flood(&mut p, 1, state);
        for s in &p {
            assert!(s.is_tombstoned("x.md") && s.is_tombstoned("m.toml"));
        }
        assert!(!p[0].docs.contains_key("x.md"));
    }

    #[test]
    fn recreation_survives_old_tombstone() {
        let mut p = vec![Syncer::new("a"), Syncer::new("b")];
        let t = p[0].local_text("x.md", "hi\n").unwrap();
        flood(&mut p, 0, vec![t]);
        let old = p[0].local_delete("x.md").unwrap();
        flood(&mut p, 0, vec![dup(&old)]);
        assert!(p.iter().all(|s| s.is_tombstoned("x.md")));

        let rec = p[0].local_text("x.md", "hi\n").unwrap();
        assert_ne!(msg_origin(&rec), msg_origin(&old), "a re-creation is a new doc");
        flood(&mut p, 0, vec![rec]);
        for s in &p {
            assert_eq!(text_of(s, "x.md").as_deref(), Some("hi\n"));
            assert!(!s.is_tombstoned("x.md"));
        }

        let r = p[0].receive(dup(&old));
        assert!(r.change.is_none());
        assert!(matches!(r.replies.first(), Some(SyncMsg::Restore { .. })));

        // A second delete, then a re-creation with different bytes.
        let d2 = p[0].local_delete("x.md").unwrap();
        assert_ne!(delete_ids(&d2), delete_ids(&old));
        flood(&mut p, 0, vec![d2]);
        assert!(p.iter().all(|s| s.is_tombstoned("x.md")));
        let rec2 = p[0].local_text("x.md", "different\n").unwrap();
        flood(&mut p, 0, vec![rec2]);
        for s in &p {
            assert_eq!(text_of(s, "x.md").as_deref(), Some("different\n"));
            assert!(!s.is_tombstoned("x.md"));
        }
    }

    #[test]
    fn behind_holder_converges() {
        let mut p = vec![Syncer::new("a"), Syncer::new("b")];
        let t = p[0].local_text("x.md", "v0\n").unwrap();
        flood(&mut p, 0, vec![t]);
        for i in 1..=20 {
            p[0].local_text("x.md", &format!("v0\n{}", "e\n".repeat(i))); // never delivered
        }
        let d = p[0].local_delete("x.md").unwrap();
        let n = flood(&mut p, 0, vec![d]);
        assert!(p.iter().all(|s| s.is_tombstoned("x.md")));
        assert!(n < 50, "converged in {n} messages");
    }

    #[test]
    fn independent_deletes_then_recreate() {
        let mut p = vec![Syncer::new("a"), Syncer::new("b")];
        let t = p[0].local_text("x.md", "hi\n").unwrap();
        flood(&mut p, 0, vec![t]);
        let da = p[0].local_delete("x.md").unwrap();
        let db = p[1].local_delete("x.md").unwrap();
        assert_eq!(delete_ids(&da), delete_ids(&db), "same content + history mint the same id");
        flood(&mut p, 0, vec![da]);
        flood(&mut p, 1, vec![db]);
        assert!(p.iter().all(|s| s.is_tombstoned("x.md")));

        let rec = p[0].local_text("x.md", "again\n").unwrap();
        flood(&mut p, 0, vec![rec]);
        for s in &p {
            assert_eq!(text_of(s, "x.md").as_deref(), Some("again\n"));
            assert!(!s.is_tombstoned("x.md"));
        }
    }

    #[test]
    fn unknown_path_delete_is_remembered() {
        let mut a = Syncer::new("a");
        a.local_text("p.md", "same\n");
        let d = a.local_delete("p.md").unwrap();

        let mut c = Syncer::new("c");
        let r = c.receive(dup(&d));
        assert!(r.change.is_none());
        assert_eq!(r.save.as_deref(), Some("p.md"));
        assert!(c.is_tombstoned("p.md"));

        let mut z = Syncer::new("z");
        let m = z.local_text("p.md", "same\n").unwrap();
        let r = c.receive(m);
        assert!(r.change.is_none());
        assert!(matches!(r.replies.as_slice(), [SyncMsg::Delete { .. }]));
        assert!(r.save.is_none());
        assert!(c.is_tombstoned("p.md"));
    }

    #[test]
    fn deletes_refuse_dot_and_escaping_paths() {
        let mut s = Syncer::new("s");
        s.local_lww(".site", b"x".to_vec(), 1);
        for p in [".site", "../x", "a//b", ""] {
            let r = s.receive(SyncMsg::Delete {
                path: p.into(),
                ids: vec![1],
                seen: vec![],
                origin: None,
                vv: vec![],
                version: u64::MAX,
            });
            assert!(is_noop(&r), "{p:?}");
            assert!(!s.is_tombstoned(p));
        }
        assert!(s.lww.contains_key(".site"));
    }

    #[test]
    fn compare_hello_sees_tombstones() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        let mut c = Syncer::new("c");
        a.local_text("x.md", "hi\n");
        a.local_lww("m.toml", b"v".to_vec(), 1000);
        sync(&mut a, &mut b);
        sync(&mut a, &mut c); // c: synced pre-delete
        let dx = a.local_delete("x.md").unwrap();
        let dm = a.local_delete("m.toml").unwrap();

        let ahead = Some(HelloCmp { we_ahead: true, they_ahead: false });
        assert_eq!(a.compare_hello(&b.hello()), ahead, "we hold deletes b lacks");
        assert_eq!(b.compare_hello(&a.hello()), ahead, "b holds copies a lacks");

        b.receive(dup(&dx));
        b.receive(dup(&dm));
        assert!(a.compare_hello(&b.hello()).unwrap().consistent());
        assert!(b.compare_hello(&a.hello()).unwrap().consistent());

        c.local_text("x.md", "hi\nmore\n");
        assert!(a.compare_hello(&c.hello()).unwrap().they_ahead, "a new op may overrule the delete");

        let mut d = Syncer::new("d");
        d.receive(SyncMsg::Lww { path: "m.toml".into(), version: 2000, bytes: b"w".to_vec() });
        assert!(a.compare_hello(&d.hello()).unwrap().they_ahead, "a newer version may overrule it");
    }

    #[test]
    fn generation_moves_only_on_tombstone_changes() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        b.receive(a.local_text("x.md", "hi\n").unwrap());
        let stale = b.full_state();

        let g = a.generation();
        let d = a.local_delete("x.md").unwrap();
        assert!(a.generation() > g, "a local delete bumps");
        let g = a.generation();
        a.local_delete("x.md").unwrap();
        assert_eq!(a.generation(), g, "re-asking for the Delete does not bump");

        let gb = b.generation();
        assert!(b.receive(dup(&d)).save.is_some());
        assert_eq!(b.generation(), gb + 1, "the first Delete bumps once");
        let gb = b.generation();
        assert!(is_noop(&b.receive(dup(&d))), "a duplicate Delete is a no-op");
        assert_eq!(b.generation(), gb);

        b.receive(SyncMsg::Resync { path: "x.md".into() });
        let hello = SyncMsg::Hello { versions: vec![("x.md".into(), vec![])], lww_versions: vec![] };
        assert!(b.receive(hello).replies.iter().any(|m| is_delete_of(m, "x.md")));
        for m in stale {
            let r = b.receive(m);
            assert!(r.save.is_none() && matches!(r.replies.as_slice(), [SyncMsg::Delete { .. }]));
        }
        assert_eq!(b.generation(), gb, "Resync / Hello / a stale copy only re-assert");

        let restore = SyncMsg::Restore { path: "x.md".into(), ids: delete_ids(&d) };
        assert!(b.receive(restore).save.is_some());
        assert!(b.generation() > gb, "a Restore dropping the tombstone bumps");
        assert!(!b.is_tombstoned("x.md"));

        let g = a.generation();
        a.local_text("x.md", "back\n").unwrap();
        assert!(a.generation() > g, "a re-creation bumps");
    }

    #[test]
    fn no_ping_pong_between_tombstone_and_stale_copy() {
        let mut p = vec![Syncer::new("a"), Syncer::new("b")];
        let t = p[0].local_text("x.md", "hi\n").unwrap();
        let l = p[0].local_lww("m.toml", b"v".to_vec(), 1000);
        flood(&mut p, 0, vec![t, l]);
        p[0].local_delete("x.md").unwrap(); // b never sees these
        p[0].local_delete("m.toml").unwrap();

        let copy = p[1].full_state();
        let n = flood(&mut p, 1, copy);
        assert!(n <= 4, "{n} messages");
        for s in &p {
            assert!(s.is_tombstoned("x.md") && s.is_tombstoned("m.toml"));
        }

        let deletes = p[1].full_state();
        assert!(deletes.iter().all(|m| matches!(m, SyncMsg::Delete { .. })));
        let sent = deletes.len();
        let n = flood(&mut p, 1, deletes);
        assert!(n <= 2, "{n} messages");
        assert_eq!(n, sent, "converged tombstones draw no reply");
    }

    #[test]
    fn unmergeable_content_for_a_tombstone_parks() {
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

        let mut a = Syncer::new("a");
        a.local_text("note.md", "x\n");
        a.local_delete("note.md").unwrap();
        let mut deletes = 0;
        let mut fed = 0;
        while a.parked_paths().is_empty() {
            let r = a.receive(bad(&probe));
            deletes += r.replies.iter().filter(|m| is_delete_of(m, "note.md")).count();
            fed += 1;
            assert!(fed < 32, "must park after the cap, not loop forever");
        }
        assert_eq!(deletes, MAX_RESYNC_REQUESTS as usize, "bounded re-asserts");
        assert_eq!(a.parked_paths(), vec!["note.md".to_string()]);
        assert!(is_noop(&a.receive(bad(&probe))), "a parked tombstone stays quiet");
        assert!(a.is_tombstoned("note.md"));

        let mut src = Syncer::new("s");
        src.local_text("note.md", "one\n");
        src.local_text("note.md", "one\ntwo\n");
        assert!(a.compare_hello(&src.hello()).unwrap().consistent(), "anti-entropy ignores it");
    }

    /// `flood` restricted to the `online` peers (the rest are offline: they get nothing).
    fn flood_among(peers: &mut [Syncer], online: &[usize], from: usize, msgs: Vec<SyncMsg>) -> usize {
        let mut queue: std::collections::VecDeque<(usize, SyncMsg)> =
            msgs.into_iter().map(|m| (from, m)).collect();
        let mut n = 0;
        while let Some((src, m)) = queue.pop_front() {
            n += 1;
            assert!(n < 500, "message storm");
            for &i in online {
                if i == src {
                    continue;
                }
                for r in peers[i].receive(dup(&m)).replies {
                    queue.push_back((i, r));
                }
            }
        }
        n
    }

    /// A delta the deleter had already seen, arriving AFTER the (small) `Delete` at a peer that
    /// was behind — a chunked delta overtaken by its delete — must not revive the file; a
    /// genuinely newer edit still does.
    #[test]
    fn late_delta_the_deleter_saw_never_revives() {
        let mut p = vec![Syncer::new("a"), Syncer::new("b")];
        let mut c = Syncer::new("c");
        let t = p[0].local_text("x.md", "v1\n").unwrap();
        c.receive(dup(&t));
        flood(&mut p, 0, vec![t]);
        let d2 = p[0].local_text("x.md", "v1\nv2\n").unwrap(); // overtaken by the delete
        c.receive(dup(&d2));
        let d = p[0].local_delete("x.md").unwrap();
        flood(&mut p, 0, vec![d]);
        assert!(p[1].is_tombstoned("x.md"), "b was behind, proved stale, deleted");

        let r = p[1].receive(dup(&d2));
        assert!(r.change.is_none(), "the deleter saw this delta: no revival");
        assert!(p[1].is_tombstoned("x.md"));
        assert!(matches!(r.replies.as_slice(), [SyncMsg::Delete { .. }]), "re-asserts the delete");
        assert!(r.save.is_some(), "the kept doc grew: its record changes");

        let e = c.local_text("x.md", "v1\nv2\nv3 by c\n").unwrap();
        let r = p[1].receive(dup(&e));
        assert!(matches!(r.change, Some(Change::Write { .. })), "a newer edit beats the delete");
        assert_eq!(text_of(&p[1], "x.md").as_deref(), Some("v1\nv2\nv3 by c\n"));
    }

    /// Delete (id1) → re-create → delete (id2), with one peer offline holding the id1 tombstone
    /// and another offline holding the re-created copy. When both come back, the old id1 must
    /// not let the re-creator's `Restore` cancel id2: everyone ends deleted.
    #[test]
    fn stale_peers_from_different_delete_rounds_never_undo_the_latest_delete() {
        let mut p: Vec<Syncer> = ["a", "b", "c", "d", "e"].into_iter().map(Syncer::new).collect();
        let all = [0, 1, 2, 3, 4];
        let t = p[0].local_text("x.md", "v1\n").unwrap();
        flood_among(&mut p, &all, 0, vec![t]);
        let d1 = p[0].local_delete("x.md").unwrap();
        flood_among(&mut p, &all, 0, vec![d1]);
        assert!(p.iter().all(|s| s.is_tombstoned("x.md")));

        // d goes offline holding the id1 tombstone; b re-creates.
        let rec = p[1].local_text("x.md", "v2\n").unwrap();
        flood_among(&mut p, &[0, 1, 2, 4], 1, vec![rec]);
        for i in [0, 1, 2, 4] {
            assert_eq!(text_of(&p[i], "x.md").as_deref(), Some("v2\n"));
        }

        // e goes offline holding the re-created copy; c deletes it (id2).
        let d2 = p[2].local_delete("x.md").unwrap();
        flood_among(&mut p, &[0, 1, 2], 2, vec![d2]);
        assert!([0, 1, 2].iter().all(|&i| p[i].is_tombstoned("x.md")));

        // d returns (catch-up both ways), then e does.
        let live = [0, 1, 2, 3];
        let s = p[3].full_state();
        flood_among(&mut p, &live, 3, s);
        let s = p[0].full_state();
        flood_among(&mut p, &live, 0, s);
        let s = p[4].full_state();
        flood_among(&mut p, &all, 4, s);
        let s = p[0].full_state();
        flood_among(&mut p, &all, 0, s);
        for (i, s) in p.iter().enumerate() {
            assert!(s.is_tombstoned("x.md"), "peer {i} brought the deleted file back");
            assert!(text_of(s, "x.md").is_none());
        }
    }

    /// A peer away since BEFORE the first of two delete rounds comes back holding an OLDER copy
    /// of the round-1 doc. The latest tombstone (round 2, a different doc) can't judge it, so the
    /// round-1 tombstone each peer remembers (`past`, persisted) answers it: everyone ends deleted.
    #[test]
    fn returning_copy_older_than_an_earlier_round_delete_is_deleted() {
        let mut p: Vec<Syncer> = ["a", "b", "c", "f"].into_iter().map(Syncer::new).collect();
        let (all, live) = ([0, 1, 2, 3], [0, 1, 2]);
        let t = p[0].local_text("x.md", "v1\n").unwrap();
        flood_among(&mut p, &all, 0, vec![t]);
        // f goes away holding v1; a edits on, deletes (round 1); b re-creates; c deletes (round 2).
        let e = p[0].local_text("x.md", "v1\nv2\n").unwrap();
        flood_among(&mut p, &live, 0, vec![e]);
        let d1 = p[0].local_delete("x.md").unwrap();
        flood_among(&mut p, &live, 0, vec![dup(&d1)]);
        let rec = p[1].local_text("x.md", "new\n").unwrap();
        flood_among(&mut p, &live, 1, vec![rec]);
        let d2 = p[2].local_delete("x.md").unwrap();
        flood_among(&mut p, &live, 2, vec![d2]);
        assert!(live.iter().all(|&i| p[i].is_tombstoned("x.md")));

        let old_copy = p[3].full_state();
        flood_among(&mut p, &all, 3, old_copy.iter().map(dup).collect());
        let s = p[0].full_state();
        flood_among(&mut p, &all, 0, s);
        for (i, s) in p.iter().enumerate() {
            assert!(s.is_tombstoned("x.md"), "peer {i} brought round-1 content back");
        }

        // The round-1 tombstone survives a restart.
        let rec = p[2].durable("x.md").unwrap();
        assert!(matches!(&rec.kind, DurableKind::Deleted { past, .. } if !past.is_empty()));
        let mut c2 = Syncer::new("c2");
        assert!(matches!(c2.restore(rec, None, 0), Restored::Clean));
        let r = c2.receive(dup(&old_copy[0]));
        assert!(r.change.is_none());
        assert!(
            r.replies.iter().any(|m| matches!(m, SyncMsg::Delete { ids, .. } if *ids == delete_ids(&d1))),
            "answers with the round-1 Delete"
        );
    }

    /// While the path is live again (re-created), a returning copy of the doc deleted earlier must
    /// not win the origin conflict against the re-creation, whatever the agent ids.
    #[test]
    fn live_recreation_beats_a_returning_copy_of_the_deleted_doc() {
        // "z" (the original author) outranks "b~1" (the re-creation) in the origin rule.
        let mut p: Vec<Syncer> = ["z", "b", "f"].into_iter().map(Syncer::new).collect();
        let (all, live) = ([0, 1, 2], [0, 1]);
        let t = p[0].local_text("x.md", "old\n").unwrap();
        flood_among(&mut p, &all, 0, vec![t]);
        let d1 = p[0].local_delete("x.md").unwrap();
        flood_among(&mut p, &live, 0, vec![d1]);
        let rec = p[1].local_text("x.md", "new\n").unwrap();
        flood_among(&mut p, &live, 1, vec![rec]);

        let old_copy = p[2].full_state();
        flood_among(&mut p, &all, 2, old_copy);
        let s = p[1].full_state();
        flood_among(&mut p, &all, 1, s);
        for (i, s) in p.iter().enumerate() {
            assert_eq!(text_of(s, "x.md").as_deref(), Some("new\n"), "peer {i}");
            assert!(!s.is_tombstoned("x.md"));
        }
    }

    /// Several deltas overtaken by their (small) `Delete`: a peer that is behind accepts the
    /// delete at once (its tips are dominated per agent by the deleter's frontier), and a late
    /// delta that is NOT the deleter's last one — so its tip isn't named in `t.vv` — still keeps
    /// the path deleted; a genuinely newer edit still revives it.
    #[test]
    fn late_deltas_short_of_the_deleters_frontier_never_revive() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        let mut c = Syncer::new("c");
        let t = a.local_text("x.md", "v1\n").unwrap();
        b.receive(dup(&t));
        c.receive(dup(&t));
        let d2 = a.local_text("x.md", "v1\nv2\n").unwrap(); // overtaken
        c.receive(dup(&d2));
        let d3 = a.local_text("x.md", "v1\nv2\nv3\n").unwrap(); // overtaken
        c.receive(dup(&d3));
        let d = a.local_delete("x.md").unwrap();

        let r = b.receive(dup(&d));
        assert_eq!(r.change, Some(Change::Remove { path: "x.md".into() }), "behind, but seen by the deleter");
        let r = b.receive(dup(&d2));
        assert!(r.change.is_none(), "d2 is short of the deleter's frontier: no revival");
        assert!(b.is_tombstoned("x.md"));
        assert!(matches!(r.replies.as_slice(), [SyncMsg::Delete { .. }]));
        let r = b.receive(dup(&d3));
        assert!(r.change.is_none() && b.is_tombstoned("x.md"), "nor d3");

        // The same after a re-assert round (the tombstone's vv also names the behind peer's tip,
        // which `ops_since` CAN map — and so maps d2 as ahead of it).
        let mut q = Syncer::new("q");
        q.receive(dup(&t));
        let mut p = vec![a, q];
        flood(&mut p, 0, vec![dup(&d)]);
        assert!(p[1].is_tombstoned("x.md"));
        assert!(p[1].receive(dup(&d2)).change.is_none(), "d2 after a re-assert round: no revival");
        assert!(p[1].is_tombstoned("x.md"));

        let e = c.local_text("x.md", "v1\nv2\nv3\nv4 by c\n").unwrap();
        let r = b.receive(dup(&e));
        assert!(matches!(r.change, Some(Change::Write { .. })), "a newer edit beats the delete");
        assert_eq!(text_of(&b, "x.md").as_deref(), Some("v1\nv2\nv3\nv4 by c\n"));
    }
}
