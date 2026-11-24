//! Tree sync — the native driver for the shared `riftpipe_core::sync` protocol
//! (the same one the browser speaks over its gossip mesh / WebRTC links). It
//! binds that pure protocol to ANY file tree + a split link: remote edits land
//! on disk, local edits (any editor touching the files) are watched and pushed.
//! Text files (`*.md`) merge as CRDTs, everything else as LWW, and dot-paths
//! never sync — all conflict resolution lives in `riftpipe_core::sync::Syncer`;
//! this module is only I/O. A file-backed app is the showcase consumer (its data
//! directory is just such a tree), not the owner.
//!
//! Transport-blind: takes the `net::{Sink, Source}` halves, so it runs over
//! whatever dialed the link — WebRTC via signaling for browser peers; `main.rs`
//! owns that dialing, [`run_over`] is the common entry. (The gossip-mesh path —
//! `connect --accept` and mesh tickets — drives the same `TreePeer` via
//! [`super::mesh`] instead of a single link.)
//!
//! Shape matches the sibling sessions (`folder::session`, `pipe::session`): the
//! caller owns the state ([`TreePeer`]) and the watcher channel; [`run`] is a
//! single `tokio::select!` loop — no spawned tasks, no shared-state locks.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval, MissedTickBehavior};

use riftpipe_core::epoch::{AccessMode, EpochBook, Ingested, OpenError};
use riftpipe_core::identity::{DeviceKey, Role, TrustEntry, TrustLog};
use riftpipe_core::onboard::JoinRequest;
use riftpipe_core::sync::{record_name, DurablePath, HelloCmp, MeshMsg, Restored, SyncMsg, Syncer};

use crate::net::{anyerr, Result, Sink, Source};
use crate::sync::control::{clean_name, is_canonical_name, NAME_MAX_BYTES};

/// Most verified join requests a peer keeps waiting (one per device); later ones
/// are ignored with a one-time warning, so a link-holder can't grow it unbounded.
pub const REQUEST_INBOX_CAP: usize = 64;

/// A verified join request waiting in the list, with its safety code computed
/// once on arrival (the state snapshot is rebuilt every second).
#[derive(Clone, Debug)]
pub struct PendingRequest {
    pub request: JoinRequest,
    pub safety: String,
}

fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn role_str(r: Role) -> &'static str {
    match r {
        Role::Admin => "admin",
        Role::Member => "member",
    }
}

fn escapes(path: &str) -> bool {
    Path::new(path).components().any(|c| matches!(c, Component::ParentDir | Component::RootDir))
}

/// Any path with a `.`-prefixed component is machine-local (top-level `.site`
/// — the per-machine site id — nested `tickets/.hidden`, editor droppings) and
/// must never cross the wire, in EITHER direction.
fn hidden(rel: &str) -> bool {
    rel.split('/').any(|c| c.starts_with('.'))
}

/// Never-synced dot-dir holding one durable record per path (`<record_name>.rec`):
/// directly inside it for link sync, under `<hex topic>/` for a mesh board (see
/// [`TreePeer::set_board`]).
pub(crate) const SYNC_DIR: &str = ".riftpipe-sync";
fn record_path(records: &Path, rel: &str) -> PathBuf {
    records.join(format!("{}.rec", record_name(rel)))
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
/// Every decodable record directly in `records`, by path.
fn load_records(records: &Path) -> HashMap<String, DurablePath> {
    let mut out = HashMap::new();
    let Ok(entries) = std::fs::read_dir(records) else { return out };
    for e in entries.flatten() {
        let p = e.path();
        if p.extension().and_then(|e| e.to_str()) != Some("rec") {
            continue;
        }
        let Ok(bytes) = std::fs::read(&p) else { continue };
        let Ok(rec) = postcard::from_bytes::<DurablePath>(&bytes) else { continue };
        out.insert(rec.path.clone(), rec);
    }
    out
}

fn rel_of(full: &Path, dir: &Path) -> Option<String> {
    let rel = full.strip_prefix(dir).ok()?.to_string_lossy().replace('\\', "/");
    if rel.is_empty() || hidden(&rel) {
        None
    } else {
        Some(rel)
    }
}

/// Create the dir if needed and canonicalize it so it matches the
/// watcher's event paths (on macOS /var is a symlink to /private/var, which
/// would otherwise break `strip_prefix`).
pub fn prepare_dir(dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).map_err(anyerr)?;
    std::fs::canonicalize(dir).map_err(anyerr)
}

/// One peer's tree-sync state, owned by the caller (like `Workspace` for
/// folder mode): the protocol `Syncer` plus a content-hash memo.
pub struct TreePeer {
    syncer: Syncer,
    /// blake3 of the last-known content per rel path — updated on every push
    /// and every remote persist. A push is skipped when the on-disk hash equals
    /// this (covers both the echo of a remote write and a no-op event), and it
    /// is what the fallback poll diffs against. Hashes, not bytes, so it never
    /// holds file copies.
    seen: HashMap<String, [u8; 32]>,
    /// Paths already reported as parked (resync retries exhausted in the core
    /// `Syncer`), so the stderr breadcrumb fires once per park — and again if a
    /// path recovers and later parks anew.
    parked_reported: HashSet<String>,
    /// Merged bytes whose disk write failed, by rel path, retried before every
    /// later apply. (A duplicate delivery used to rewrite the file; a merge that
    /// adds nothing now persists nothing, so this is what repairs a failed write.)
    failed_writes: HashMap<String, Vec<u8>>,
    // --- device-linking auth (protocol v3) ---
    /// This device's signing identity. `None` = open mode (writes unsigned).
    identity: Option<DeviceKey>,
    /// The board's authorized device set, built from replicated trust entries.
    trust: TrustLog,
    /// Trust entries we hold, kept to catch a new neighbor up (gossip is
    /// ephemeral, so a late joiner must be re-sent the log).
    trust_entries: Vec<TrustEntry>,
    /// Entry ids already stored (blake3 of the postcard), so replays don't grow
    /// `trust_entries`.
    trust_seen: HashSet<[u8; 32]>,
    /// The one access switch — see `riftpipe_core::epoch::AccessMode`. `Restricted`
    /// (`--restricted`) seals content under epoch keys and rejects unsigned or
    /// unauthorized writes; `LinkEditor` (the default) is the open board.
    mode: AccessMode,
    // --- epoch-sealed content (protocol v4, restricted boards) ---
    /// Epoch keys, announces, and wrap targets. `Some` iff an identity is set.
    epochs: Option<EpochBook>,
    /// Our signed join request — the binding from device id to wrap-target key that
    /// an Admin needs to wrap us into the current epoch, plus our display name
    /// (`--name`, default `"cli"`). `Some` iff an identity is set.
    my_request: Option<JoinRequest>,
    /// Epochs already reported as "dropped content (no key)", so the breadcrumb
    /// fires once per epoch.
    no_key_reported: HashSet<u64>,
    /// An open peer received sealed content once already (warned to rejoin restricted).
    warned_restricted: bool,
    /// A local edit was held back because we hold no epoch key yet (warned once).
    warned_keyless: bool,
    /// Epochs already reported as "not the current sealable epoch — writes held".
    stale_epoch_reported: HashSet<u64>,
    /// Verified requests from devices that aren't authorized, one per device —
    /// the `riftpipe requests` list. Cleared per device when it's granted.
    requests: Vec<PendingRequest>,
    /// The request list hit [`REQUEST_INBOX_CAP`] once already (warned).
    warned_inbox_full: bool,
    /// A new device's request was dropped because the list was full (sticky for
    /// the session). The dropped request is invisible here, so its safety code
    /// could be matched by one ground for an entry that IS listed: the state file
    /// reports it as `inboxOverflow` and `riftpipe approve` then refuses codes.
    inbox_overflow: bool,
    /// A join request with a non-canonical name was dropped once already (warned).
    warned_bad_name: bool,
    /// The mesh board (topic) this tree syncs, scoping its durable records — so
    /// a dir reused for another board never restores (and ships) the old
    /// board's history. `None` = link sync.
    board: Option<[u8; 32]>,
}

/// What an apply sends back out.
#[derive(Default)]
pub(crate) struct Outbound {
    /// To the whole swarm — every reply, including a `Hello`'s (see `SyncMsg::Hello`
    /// for why those can't go neighbors-only), a Resync answer, epoch housekeeping.
    pub(crate) swarm: Vec<MeshMsg>,
    /// `swarm[i]`'s outbound-queue coalescing key (`outqueue::coalesce_key` of the
    /// unwrapped reply), so identical `Lww` replies — several Hellos answered
    /// from the same current state — replace each other while queued. Empty =
    /// all unkeyed.
    swarm_keys: Vec<Option<String>>,
    /// How an applied Hello compared to our state (anti-entropy).
    pub(crate) hello: Option<HelloCmp>,
}

impl Outbound {
    fn swarm(swarm: Vec<MeshMsg>) -> Self {
        Outbound { swarm, swarm_keys: Vec::new(), hello: None }
    }
    /// Every swarm message with its coalescing key.
    pub(crate) fn keyed_swarm(self) -> impl Iterator<Item = (Option<String>, MeshMsg)> {
        let keys = self.swarm_keys.into_iter().chain(std::iter::repeat(None));
        keys.zip(self.swarm)
    }
}

impl TreePeer {
    pub fn new() -> Self {
        TreePeer {
            syncer: Syncer::new(format!("n{:08x}", rand::random::<u32>())),
            seen: HashMap::new(),
            parked_reported: HashSet::new(),
            failed_writes: HashMap::new(),
            identity: None,
            trust: TrustLog::new(),
            trust_entries: Vec::new(),
            trust_seen: HashSet::new(),
            mode: AccessMode::LinkEditor,
            epochs: None,
            my_request: None,
            no_key_reported: HashSet::new(),
            warned_restricted: false,
            warned_keyless: false,
            stale_epoch_reported: HashSet::new(),
            requests: Vec::new(),
            warned_inbox_full: false,
            inbox_overflow: false,
            warned_bad_name: false,
            board: None,
        }
    }

    /// Enable device-linking auth: sign our writes with `key`, and — when
    /// `restricted` — gate the board: content is sealed under epoch keys only
    /// wrapped devices can open, and writes must be signed by an authorized
    /// device. Open mode (the default) leaves both off.
    pub fn set_identity(&mut self, key: DeviceKey, restricted: bool) {
        self.mode = if restricted { AccessMode::Restricted } else { AccessMode::LinkEditor };
        self.epochs = Some(EpochBook::new(&key));
        self.my_request = Some(JoinRequest::create(&key, "cli"));
        self.identity = Some(key);
    }

    /// How this peer gates reads/writes.
    pub fn mode(&self) -> AccessMode {
        self.mode
    }

    /// Seq of the latest epoch this peer has seen announced (held or not).
    pub fn epoch_known(&self) -> Option<u64> {
        self.epochs.as_ref()?.latest_known_seq()
    }

    /// Seq of the latest epoch this peer holds the key for.
    pub fn epoch_held(&self) -> Option<u64> {
        self.epochs.as_ref()?.latest_held_seq()
    }

    /// Bind this tree to mesh board `board` (`H(secret)`): the trust log, so
    /// passkey links made for it verify ([`TrustLog::set_board`]), and the
    /// durable records (`<dir>/.riftpipe-sync/<hex board>/`). Call before
    /// [`seed`](Self::seed).
    pub fn set_board(&mut self, board: [u8; 32]) {
        self.trust.set_board(board);
        self.board = Some(board);
    }

    /// This device's id, if an identity is set.
    pub fn device_id(&self) -> Option<[u8; 32]> {
        self.identity.as_ref().map(|k| k.id())
    }

    /// Re-sign our join request with display name `name` (`--name`; cleaned of
    /// control characters and capped). No-op without an identity.
    pub fn set_request_name(&mut self, name: &str) {
        if let Some(key) = &self.identity {
            self.my_request = Some(JoinRequest::create(key, clean_name(name)));
        }
    }

    /// Our signed join request (`Some` iff an identity is set).
    pub fn my_request(&self) -> Option<&JoinRequest> {
        self.my_request.as_ref()
    }

    /// Verified requests waiting for approval (one per not-yet-authorized device).
    pub fn pending_requests(&self) -> &[PendingRequest] {
        &self.requests
    }

    /// The board's authorized devices and their roles, sorted by id.
    pub fn members(&self) -> Vec<([u8; 32], Role)> {
        self.trust.devices()
    }

    /// Is this device in the trust log?
    fn me_authorized(&self) -> bool {
        self.device_id().is_some_and(|id| self.trust.is_authorized(&id))
    }

    /// Ingest a trust-log entry (idempotent) and remember it for neighbor
    /// catch-up; a now-authorized device leaves the request list. Returns whether
    /// the log now holds it.
    pub fn add_trust(&mut self, entry: TrustEntry) -> bool {
        // An equivalent of an entry already applied (a passkey link re-sent with the
        // other valid ECDSA signature) is nothing new: don't store or re-send it.
        if self.trust.holds(&entry) {
            return true;
        }
        let id = *blake3::hash(&postcard::to_allocvec(&entry).unwrap_or_default()).as_bytes();
        if self.trust_seen.insert(id) {
            self.trust_entries.push(entry.clone());
        }
        let ok = self.trust.apply(entry);
        let trust = &self.trust;
        self.requests.retain(|r| !trust.is_authorized(&r.request.device_id));
        ok
    }

    /// The auth state to catch a new neighbor up (gossip is ephemeral, so a late
    /// joiner must be re-sent it): every trust entry, then every accepted epoch
    /// announce — trust first, so the announces' signers are known Admins.
    pub(crate) fn auth_catchup(&self) -> Vec<MeshMsg> {
        let trust = self.trust_entries.iter().cloned().map(MeshMsg::Trust);
        let epochs = self.epochs.iter().flat_map(|b| b.announces().iter().cloned().map(MeshMsg::Epoch));
        trust.chain(epochs).collect()
    }

    /// Our signed join request, while we can't read the board (restricted, and not
    /// holding the latest epoch key) — broadcast to the whole swarm so an Admin
    /// (maybe not a direct neighbor) can wrap us in once we're granted.
    pub(crate) fn read_request(&self) -> Option<MeshMsg> {
        if !self.mode.seals_content() || self.epochs.as_ref().is_some_and(|b| b.holds_latest()) {
            return None;
        }
        self.my_request.clone().map(MeshMsg::Request)
    }

    /// Session start (after `seed`): a restricted founder mints epoch 1. The
    /// announces stay in the book and reach peers via `auth_catchup`.
    pub(crate) fn startup(&mut self) -> Vec<MeshMsg> {
        self.maintenance()
    }

    /// Wrap an outgoing write for the mesh: a `SignedSync` if we have an
    /// identity, else a plain `Sync` (open mode) — then, on a restricted board,
    /// sealed under the current epoch. `None` when restricted and we hold no epoch
    /// key yet (the edit stays in the syncer and goes out with our full state once
    /// we're wrapped in).
    pub(crate) fn sign_mesh(&self, msg: SyncMsg) -> Option<MeshMsg> {
        let signed = match &self.identity {
            Some(key) => {
                let payload = postcard::to_allocvec(&msg).unwrap_or_default();
                MeshMsg::SignedSync(key.sign_write(payload))
            }
            None => MeshMsg::Sync(msg),
        };
        if self.mode.seals_content() {
            self.epochs.as_ref()?.seal(&signed, &self.trust)
        } else {
            Some(signed)
        }
    }

    /// [`sign_mesh`](Self::sign_mesh) for a LOCAL edit: when it can't go out (restricted,
    /// and no epoch key yet, or the epoch we hold isn't the latest one / still wraps
    /// a revoked device — see `EpochBook::can_seal`), say
    /// so once — the edit stays in the syncer and goes out with our full state when
    /// a (new) epoch key arrives.
    pub(crate) fn sign_local(&mut self, msg: SyncMsg) -> Option<MeshMsg> {
        let out = self.sign_mesh(msg);
        if out.is_some() || !self.mode.seals_content() {
            return out;
        }
        if self.epoch_held().is_none() {
            if !self.warned_keyless {
                self.warned_keyless = true;
                eprintln!(
                    "[epoch] no read key yet — local edits are held until an admin wraps this device in (is the board --restricted?)"
                );
            }
        } else if self.stale_epoch_reported.insert(self.epoch_held().unwrap_or(0)) {
            eprintln!("[epoch] local edits are held until this device holds the current epoch key (a device was removed, or our wrap for a new epoch is still coming)");
        }
        out
    }

    /// Accept one content message (`Sync` / `SignedSync`, already opened if it
    /// arrived sealed): enforce signing when the mode requires it, verify a signed
    /// write against the trust log, apply + persist, and return replies already
    /// wrapped for the wire.
    fn apply_content(&mut self, msg: MeshMsg, dir: &Path) -> Outbound {
        let require_signed = self.mode.requires_signed_writes();
        let inner = match msg {
            MeshMsg::Sync(s) => {
                if require_signed {
                    return Outbound::default(); // unsigned write rejected on a gated board
                }
                s
            }
            MeshMsg::SignedSync(sw) => {
                let ok = if require_signed {
                    // Gated: signature valid AND author authorized.
                    sw.verified(&self.trust).is_some()
                } else {
                    // Open: accept any authentically-signed write.
                    sw.author_verified().is_some()
                };
                if !ok {
                    return Outbound::default();
                }
                match postcard::from_bytes::<SyncMsg>(&sw.payload) {
                    Ok(m) => m,
                    Err(_) => return Outbound::default(),
                }
            }
            _ => return Outbound::default(),
        };
        let hello = self.syncer.compare_hello(&inner);
        let (swarm_keys, swarm) = self
            .apply_and_persist(inner, dir)
            .into_iter()
            .filter_map(|r| {
                let key = riftpipe_core::outqueue::coalesce_key(&r);
                self.sign_mesh(r).map(|m| (key, m))
            })
            .unzip();
        Outbound { swarm, swarm_keys, hello }
    }

    /// Auth-aware receive for every mesh message the driver doesn't handle itself
    /// (`Trust` / `Epoch` / `Request` / `Sealed` / `Sync` / `SignedSync`). On a
    /// restricted board, reads are epoch-gated: only `Sealed` content is accepted,
    /// and only if we hold its epoch key. Returns messages to broadcast, already
    /// wrapped for the wire. `Presence`/`Version`/`Chunk` are handled by the driver.
    pub(crate) fn authorize_and_apply(&mut self, msg: MeshMsg, dir: &Path) -> Outbound {
        match msg {
            MeshMsg::Trust(e) => {
                let (was_authorized, was_held) = (self.me_authorized(), self.writes_held());
                self.add_trust(e);
                let mut out = self.after_trust_change(dir, was_held);
                // Just approved but keyless: announce ourselves so an Admin wraps us in
                // (mirrors the browser's `accept(Trust)`).
                if !was_authorized && self.me_authorized() {
                    let role = self.device_id().and_then(|id| self.trust.role(&id)).map_or("member", role_str);
                    eprintln!("[auth] this device is authorized ({role})");
                    out.extend(self.read_request());
                }
                Outbound::swarm(out)
            }
            MeshMsg::Epoch(a) => {
                let was_held = self.writes_held();
                let Some(book) = self.epochs.as_mut() else { return Outbound::default() };
                let ing = book.ingest(a, &self.trust);
                Outbound::swarm(self.epoch_followups(ing, dir, was_held))
            }
            MeshMsg::Request(req) => {
                self.ingest_request(req, dir);
                Outbound::swarm(self.maintenance())
            }
            MeshMsg::Sealed(s) => {
                if !self.mode.seals_content() {
                    if !self.warned_restricted {
                        self.warned_restricted = true;
                        eprintln!("[epoch] this board is restricted — rejoin with --restricted to request access");
                    }
                    return Outbound::default();
                }
                let Some(book) = self.epochs.as_mut() else { return Outbound::default() };
                match book.open(s) {
                    Ok(inner) => self.apply_content(inner, dir),
                    Err(OpenError::NoKey { seq }) => {
                        if self.no_key_reported.insert(seq) {
                            eprintln!("[epoch] dropped content sealed under epoch {seq} (no key)");
                        }
                        Outbound::default()
                    }
                    Err(OpenError::Invalid) => Outbound::default(),
                }
            }
            // Restricted: content travels sealed only — an unsealed write is dropped.
            MeshMsg::Sync(_) | MeshMsg::SignedSync(_) if self.mode.seals_content() => Outbound::default(),
            MeshMsg::Sync(_) | MeshMsg::SignedSync(_) => self.apply_content(msg, dir),
            _ => Outbound::default(), // Presence/Version/Chunk not routed here
        }
    }

    /// A signed join request arrived: teach the epoch book the device's wrap key
    /// (an already-granted device gets wrapped in by `maintenance`), and — if the
    /// device isn't authorized yet — keep it in the request list (one entry per
    /// device, capped at [`REQUEST_INBOX_CAP`]) and report it.
    fn ingest_request(&mut self, req: JoinRequest, dir: &Path) {
        let Some(book) = self.epochs.as_mut() else { return };
        // Byte cap + canonical form first: an oversized name costs nothing, and
        // the displayed name is exactly the signed one (see `is_canonical_name`).
        if !is_canonical_name(&req.name) {
            if !self.warned_bad_name {
                self.warned_bad_name = true;
                eprintln!(
                    "[auth] ignoring a join request whose name isn't canonical (over {NAME_MAX_BYTES} bytes, \
                     untrimmed, or with control/format characters) — device {}",
                    hex32(&req.device_id)
                );
            }
            return;
        }
        if !book.learn_request(&req) {
            return;
        }
        if self.trust.is_authorized(&req.device_id) || Some(req.device_id) == self.device_id() {
            return;
        }
        let full = self.requests.len() >= REQUEST_INBOX_CAP;
        if let Some(i) = self.requests.iter().position(|r| r.request.device_id == req.device_id) {
            let old = &self.requests[i].request;
            // While full, an entry can't be re-signed either: that would let a
            // device already in a full list re-grind its safety code to match a
            // request that was dropped.
            if (old.name != req.name || old.enc_pub != req.enc_pub) && !full {
                let pending = PendingRequest { safety: req.safety_code(), request: req };
                self.report_request(&pending, dir);
                self.requests[i] = pending;
            }
            return;
        }
        if full {
            self.inbox_overflow = true;
            if !self.warned_inbox_full {
                self.warned_inbox_full = true;
                eprintln!(
                    "[auth] request inbox full ({REQUEST_INBOX_CAP}) — ignoring new requests; \
                     approve by device id from now on (safety codes can't be trusted to pick one)"
                );
            }
            return;
        }
        let pending = PendingRequest { safety: req.safety_code(), request: req };
        self.report_request(&pending, dir);
        self.requests.push(pending);
    }

    /// The `access-request` breadcrumb, plus how to approve it when we're an Admin.
    fn report_request(&self, pending: &PendingRequest, dir: &Path) {
        let (req, safety) = (&pending.request, &pending.safety);
        let name = serde_json::to_string(&clean_name(&req.name)).unwrap_or_default();
        eprintln!("[auth] access-request device={} safety={safety} name={name}", hex32(&req.device_id));
        let admin = self.device_id().is_some_and(|me| self.trust.role(&me) == Some(Role::Admin));
        if admin {
            eprintln!("[auth]   approve with: riftpipe approve {} {}", dir.display(), safety.replace(' ', ""));
        }
    }

    /// The trust log changed: announces deferred for an unknown signer may now
    /// apply, and an Admin may need to rotate (a revoke) or wrap (a grant).
    /// `was_held`: writes were held ([`writes_held`](Self::writes_held)) before the change.
    fn after_trust_change(&mut self, dir: &Path, was_held: bool) -> Vec<MeshMsg> {
        let Some(book) = self.epochs.as_mut() else { return Vec::new() };
        let ing = book.retry_pending(&self.trust);
        self.epoch_followups(ing, dir, was_held)
    }

    /// Are our writes held back right now (restricted, and `EpochBook::can_seal`
    /// says no: no key, the epoch we hold isn't the latest, or it still wraps a
    /// revoked device)?
    fn writes_held(&self) -> bool {
        self.mode.seals_content() && !self.epochs.as_ref().is_some_and(|b| b.can_seal(&self.trust))
    }

    /// Act on what accepting announce(s) changed: report an epoch we're not wrapped
    /// into, apply content that was parked for a newly-held key, broadcast our full
    /// state on any newly-held key (it carries edits held back while keyless or
    /// while our latest epoch still wrapped a revoked device), then run Admin
    /// maintenance.
    fn epoch_followups(&mut self, ing: Ingested, dir: &Path, was_held: bool) -> Vec<MeshMsg> {
        let mut out = Vec::new();
        if let Some(seq) = ing.unheld {
            eprintln!("[epoch] no key for epoch {seq} — this device is not wrapped in (revoked, or not yet approved)");
        }
        if let Some(seq) = ing.held {
            eprintln!("[epoch] holding epoch {seq} — new content is sealed under it");
        }
        for m in ing.released {
            out.extend(self.apply_content(m, dir).swarm);
        }
        if ing.gained_first_key {
            let n = self.epoch_held().unwrap_or(0);
            eprintln!("[epoch] read key acquired (epoch {n})");
        }
        // Writes held back go out once the hold lifts: a newly-held epoch (edits
        // made while keyless, or between a revoke and its rotation), or a trust
        // change without a new key (e.g. the revoked device was re-granted).
        if ing.held.is_some() || (was_held && !self.writes_held()) {
            out.extend(self.sealed_full_state());
        }
        out.extend(self.maintenance());
        out
    }

    /// Admin housekeeping on a restricted board (see `EpochBook::maintain`): mint
    /// epoch 1 as founder, rotate away from a revoked device, add-wrap newly
    /// authorized devices. After an add-wrap our full state goes out sealed, so the
    /// newcomer catches up.
    fn maintenance(&mut self) -> Vec<MeshMsg> {
        if !self.mode.seals_content() {
            return Vec::new();
        }
        let (Some(key), Some(book)) = (self.identity.as_ref(), self.epochs.as_mut()) else {
            return Vec::new();
        };
        let m = book.maintain(key, &self.trust);
        if let Some((seq, n)) = m.rotated {
            eprintln!("[epoch] rotated to epoch {seq} — {n} device(s) wrapped in");
        }
        if m.added > 0 {
            eprintln!("[epoch] wrapped the current epoch to {} more device(s)", m.added);
        }
        let mut out: Vec<MeshMsg> = m.announces.into_iter().map(MeshMsg::Epoch).collect();
        if m.added > 0 {
            out.extend(self.sealed_full_state());
        }
        out
    }

    /// Our whole tree, wrapped for the wire (sealed on a restricted board).
    fn sealed_full_state(&self) -> Vec<MeshMsg> {
        self.full_state().into_iter().filter_map(|s| self.sign_mesh(s)).collect()
    }

    /// Admin action on a live board: sign a `Revoke` for `id`, apply it, and run
    /// the follow-ups (on a restricted board: rotate to a new epoch the revoked
    /// device isn't wrapped into). Returns everything to broadcast.
    pub(crate) fn revoke_device(&mut self, id: [u8; 32], dir: &Path) -> Vec<MeshMsg> {
        let entry = {
            let Some(key) = self.identity.as_ref() else {
                eprintln!("[auth] revoke ignored: no device identity (not --restricted)");
                return Vec::new();
            };
            if self.trust.role(&key.id()) != Some(Role::Admin) {
                eprintln!("[auth] revoke ignored: this device is not an Admin");
                return Vec::new();
            }
            if self.trust.founder() == Some(id) {
                eprintln!("[auth] revoke ignored: the founder can't be revoked");
                return Vec::new();
            }
            key.revoke(id, rand::random::<[u8; 16]>())
        };
        let was_held = self.writes_held();
        self.add_trust(entry.clone());
        eprintln!("[auth] revoked device {}", hex32(&id));
        let mut out = vec![MeshMsg::Trust(entry)];
        out.extend(self.after_trust_change(dir, was_held));
        out
    }

    /// Admin action on a live board: grant `id` as a Member and run the follow-ups
    /// (on a restricted board: wrap it into the current epoch once its request is
    /// known). Returns everything to broadcast.
    pub(crate) fn grant_device(&mut self, id: [u8; 32], dir: &Path) -> Vec<MeshMsg> {
        let entry = {
            let Some(key) = self.identity.as_ref() else {
                eprintln!("[auth] grant ignored: no device identity (not --restricted)");
                return Vec::new();
            };
            if self.trust.role(&key.id()) != Some(Role::Admin) {
                eprintln!("[auth] grant ignored: this device is not an Admin");
                return Vec::new();
            }
            if self.trust.founder() == Some(id) {
                eprintln!("[auth] grant ignored: the founder is already the owner");
                return Vec::new();
            }
            key.grant(id, Role::Member, rand::random::<[u8; 16]>())
        };
        let was_held = self.writes_held();
        self.add_trust(entry.clone());
        eprintln!("[auth] granted device {} (Member)", hex32(&id));
        let mut out = vec![MeshMsg::Trust(entry)];
        out.extend(self.after_trust_change(dir, was_held));
        out
    }

    /// Admin action (`riftpipe approve`): grant a device that has a request waiting.
    /// Unlike [`grant_device`](Self::grant_device) it refuses an id with no pending
    /// request, so a stale or mistyped approve can't authorize an arbitrary device.
    /// The wrap follows via maintenance — the request already taught the book the
    /// device's key.
    pub fn approve_request(&mut self, id: [u8; 32], dir: &Path) -> Vec<MeshMsg> {
        if !self.device_id().is_some_and(|me| self.trust.role(&me) == Some(Role::Admin)) {
            eprintln!("[auth] approve ignored: this device is not an Admin — run `riftpipe approve` on an Admin's dir");
            return Vec::new();
        }
        let Some(req) = self.requests.iter().find(|r| r.request.device_id == id).map(|r| &r.request) else {
            eprintln!(
                "[auth] approve ignored: no pending request from {} — `riftpipe grant {} <device-id>` authorizes a device by id",
                hex32(&id),
                dir.display()
            );
            return Vec::new();
        };
        let name = serde_json::to_string(&clean_name(&req.name)).unwrap_or_default();
        eprintln!("[auth] approving request from {name} device={}", hex32(&id));
        self.grant_device(id, dir)
    }

    /// Machine-local snapshot of this peer's auth state for `.riftpipe-auth.json`
    /// (ids, names, roles, safety codes — never keys). `None` without an identity.
    pub fn auth_state_json(&self) -> Option<serde_json::Value> {
        let me = self.device_id()?;
        let book = self.epochs.as_ref()?;
        let founder = self.trust.founder();
        let members: Vec<serde_json::Value> = self
            .trust
            .devices()
            .into_iter()
            .map(|(device, role)| {
                serde_json::json!({
                    "device": hex32(&device),
                    "role": role_str(role),
                    "founder": founder == Some(device),
                    "me": device == me,
                })
            })
            .collect();
        let requests: Vec<serde_json::Value> = self
            .requests
            .iter()
            .map(|r| {
                serde_json::json!({
                    "device": hex32(&r.request.device_id),
                    "name": clean_name(&r.request.name),
                    "safety": r.safety,
                })
            })
            .collect();
        let my_request = self
            .my_request
            .as_ref()
            .filter(|_| self.mode.seals_content() && !self.me_authorized())
            .map(|r| serde_json::json!({ "name": r.name, "safety": r.safety_code() }));
        Some(serde_json::json!({
            "version": 1,
            "device": hex32(&me),
            "restricted": self.mode.seals_content(),
            "role": self.trust.role(&me).map(role_str),
            "founder": founder.map(|f| hex32(&f)),
            "epoch": self.epoch_held().unwrap_or(0),
            "canRead": !self.mode.seals_content() || book.holds_latest(),
            "members": members,
            "requests": requests,
            "inboxOverflow": self.inbox_overflow,
            "myRequest": my_request,
        }))
    }

    /// The push decision for one local file, shared by every driver (link +
    /// mesh): skip dot-paths, non-files, and content whose hash we've already
    /// pushed or persisted; otherwise record the hash and produce the outgoing
    /// message (`.md` → text CRDT, else LWW stamped with wall-clock ms).
    pub(crate) fn local_change(&mut self, path: &Path, dir: &Path) -> Option<SyncMsg> {
        let rel = rel_of(path, dir)?;
        if !path.is_file() {
            return None;
        }
        let bytes = std::fs::read(path).ok()?;
        let hash = *blake3::hash(&bytes).as_bytes();
        // Same content we last pushed or persisted (remote-write echo / no-op event).
        if self.seen.get(&rel) == Some(&hash) {
            return None;
        }
        let msg = if riftpipe_core::sync::is_text_path(&rel) {
            self.syncer.local_text(&rel, &String::from_utf8_lossy(&bytes))
        } else {
            Some(self.syncer.local_lww(&rel, bytes, now_ms()))
        };
        self.seen.insert(rel.clone(), hash);
        self.save_record(dir, &rel);
        msg
    }

    /// Where this tree's durable records live under `dir`.
    fn records_dir(&self, dir: &Path) -> PathBuf {
        match &self.board {
            Some(t) => dir.join(SYNC_DIR).join(t.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            None => dir.join(SYNC_DIR),
        }
    }

    /// Write `rel`'s durable sync record under [`records_dir`](Self::records_dir), replacing any
    /// existing one for that path (tmp file + rename, so a crash never leaves a
    /// half-written record). Call after every local change or persisted merge.
    fn save_record(&self, dir: &Path, rel: &str) {
        let Some(rec) = self.syncer.durable(rel) else { return };
        let records = self.records_dir(dir);
        let _ = std::fs::create_dir_all(&records);
        let path = record_path(&records, rel);
        let tmp = PathBuf::from(format!("{}.tmp", path.display()));
        if std::fs::write(&tmp, postcard::to_allocvec(&rec).unwrap_or_default()).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    /// Apply one remote message: merge, report any newly-parked paths, and
    /// persist the merged bytes under `dir` (refusing escaping paths AND
    /// dot-paths from the wire — `.site` and friends are machine-local and must
    /// not be overwritten). Prints the `SYNCED:{path}` breadcrumb on persist.
    /// Returns the replies to send back out.
    pub(crate) fn apply_and_persist(&mut self, msg: SyncMsg, dir: &Path) -> Vec<SyncMsg> {
        for (path, bytes) in std::mem::take(&mut self.failed_writes) {
            self.write_merged(dir, path, bytes);
        }
        let (persist, replies) = self.syncer.apply(msg);
        self.report_parked();
        if let Some((path, bytes)) = persist {
            if !escapes(&path) && !hidden(&path) {
                self.write_merged(dir, path, bytes);
            }
        }
        replies
    }

    /// Write merged bytes to disk, then their record. On failure the file keeps
    /// its old bytes, so neither `seen` (the poll would push the old bytes back
    /// as a local edit) nor the record (a restart would restore a state the
    /// disk doesn't hold) moves; the bytes wait in `failed_writes` for a retry.
    fn write_merged(&mut self, dir: &Path, path: String, bytes: Vec<u8>) {
        let full = dir.join(&path);
        if let Some(parent) = full.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::write(&full, &bytes) {
            Ok(()) => {
                self.seen.insert(path.clone(), *blake3::hash(&bytes).as_bytes());
                self.save_record(dir, &path);
                println!("SYNCED:{path}");
            }
            Err(e) => {
                eprintln!("[sync] writing {path} failed: {e} — retrying on the next message");
                self.failed_writes.insert(path, bytes);
            }
        }
    }

    /// The whole tree as self-contained messages — how a mesh driver catches a
    /// new neighbor up (the link driver's `hello()` handshake does it lazily).
    pub(crate) fn full_state(&self) -> Vec<SyncMsg> {
        self.syncer.full_state()
    }

    /// Version vectors + LWW inventory, no bodies — sent on `NeighborUp` so a
    /// one-sided reconnect becomes bidirectional (the peer replies with
    /// exactly what we lack).
    pub(crate) fn hello(&self) -> SyncMsg {
        self.syncer.hello()
    }

    /// The syncer's change counter — the mesh driver feeds it to `AntiEntropy::poll`.
    pub(crate) fn generation(&self) -> u64 {
        self.syncer.generation()
    }

    /// Load every file already on disk into the syncer — the native mirror of
    /// the browser's `opfs::prime_all`. Restores durable records first, so
    /// history and LWW versions survive a restart; files changed while down
    /// become local edits. Without this, files that predate the session are
    /// invisible: the watcher only reports NEW events, so `hello()` would
    /// advertise nothing and a mesh `full_state()` would be empty. The
    /// produced messages are discarded — the handshake (link) or the
    /// `NeighborUp` catch-up (mesh) does the actual transfer.
    pub(crate) fn seed(&mut self, dir: &Path) {
        let records = self.records_dir(dir);
        let mut recs = load_records(&records);
        let mut files = Vec::new();
        scan_files(dir, &mut files);
        let now = now_ms();
        for p in files {
            let Some(rel) = rel_of(&p, dir) else { continue };
            let Ok(bytes) = std::fs::read(&p) else { continue };
            let hash = *blake3::hash(&bytes).as_bytes();
            match recs.remove(&rel).map(|r| self.syncer.restore(r, Some(&bytes), now)) {
                Some(Restored::Clean) => {
                    self.seen.insert(rel, hash);
                }
                Some(Restored::Changed(_)) => {
                    self.seen.insert(rel.clone(), hash);
                    self.save_record(dir, &rel);
                }
                Some(Restored::Unusable) | None => {
                    let _ = self.local_change(&p, dir);
                }
            }
        }
        for rel in recs.into_keys() {
            let _ = std::fs::remove_file(record_path(&records, &rel));
        }
    }

    /// Core is no-I/O, so IT records parked paths and WE print them: eprintln
    /// for every newly-parked path, and forget recovered ones so a re-park is
    /// reported again.
    fn report_parked(&mut self) {
        let now: HashSet<String> = self.syncer.parked_paths().into_iter().collect();
        for p in &now {
            if !self.parked_reported.contains(p) {
                eprintln!(
                    "[tree] {p}: full resync failed repeatedly — parked (holding local copy; \
                     will recover when a mergeable state arrives)"
                );
            }
        }
        self.parked_reported = now;
    }
}

impl Default for TreePeer {
    fn default() -> Self {
        Self::new()
    }
}

/// Start a recursive watcher on `dir`, feeding event paths into the returned
/// channel. If the watcher can't start, warn and return `(None, rx)` — the
/// caller passes `poll = watcher.is_none()` to [`run`] so the session degrades
/// to the fallback poll instead of aborting (folder mode does the same).
pub fn watch(dir: &Path) -> (Option<RecommendedWatcher>, UnboundedReceiver<PathBuf>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let mut watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(ev) = res {
            for p in ev.paths {
                let _ = tx.send(p);
            }
        }
    }) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("[tree] watcher unavailable ({e}); falling back to poll");
            return (None, rx);
        }
    };
    if let Err(e) = watcher.watch(dir, RecursiveMode::Recursive) {
        eprintln!("[tree] watch failed ({e}); falling back to poll");
        return (None, rx);
    }
    (Some(watcher), rx)
}

/// Push one local file if its content changed since we last saw it. No-ops on
/// dot-paths, non-files, and unchanged content; records the hash on push.
async fn push_local(
    peer: &mut TreePeer,
    path: &Path,
    dir: &Path,
    sink: &mut dyn Sink,
) -> Result<()> {
    if let Some(msg) = peer.local_change(path, dir) {
        if let Ok(b) = postcard::to_allocvec(&msg) {
            sink.send(b).await?;
        }
    }
    Ok(())
}

/// Collect every regular file under `dir`, skipping any dot-component (manual
/// recursion — the tree is small and this avoids a walkdir dependency).
pub(crate) fn scan_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        if e.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let p = e.path();
        if p.is_dir() {
            scan_files(&p, out);
        } else if p.is_file() {
            out.push(p);
        }
    }
}

/// Sync the tree at `dir` both ways over an established link: one `select!`
/// loop over watcher events (`rx`), incoming messages, and — when `poll` is set
/// because the watcher failed — a slow rescan. Returns when the link closes
/// (`source.recv()` yields `Ok(None)` or an error is unrecoverable).
pub async fn run(
    peer: &mut TreePeer,
    rx: &mut UnboundedReceiver<PathBuf>,
    poll: bool,
    sink: &mut dyn Sink,
    source: &mut dyn Source,
    dir: &Path,
) -> Result<()> {
    let mut rescan = interval(Duration::from_secs(2));
    rescan.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut rx_open = true;

    // Files that predate the session must be in the syncer BEFORE the
    // handshake, or the hello advertises an empty tree.
    peer.seed(dir);

    // Announce our protocol version, then the handshake: advertise our
    // version vectors so the peer sends what we lack.
    if let Ok(b) =
        postcard::to_allocvec(&SyncMsg::Version { proto: riftpipe_core::sync::PROTOCOL_VERSION })
    {
        sink.send(b).await?;
    }
    if let Ok(b) = postcard::to_allocvec(&peer.syncer.hello()) {
        sink.send(b).await?;
    }

    loop {
        tokio::select! {
            // Native local edits → push (the watcher feeds paths into `rx`).
            ev = rx.recv(), if rx_open => {
                let Some(first) = ev else {
                    // Watcher gone (e.g. it failed after creation); the poll arm
                    // is the safety net — never abort the session over it.
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
                    push_local(peer, &p, dir, sink).await?;
                }
            }
            // Fallback poll: rescan for files whose content drifted from `seen`.
            _ = rescan.tick(), if poll => {
                let mut files = Vec::new();
                scan_files(dir, &mut files);
                for p in files {
                    push_local(peer, &p, dir, sink).await?;
                }
            }
            // Remote edits → disk (+ any handshake replies go back out).
            msg = source.recv() => {
                let bytes = match msg {
                    Err(_) | Ok(None) => return Ok(()),
                    Ok(Some(b)) => b,
                };
                let Ok(msg) = postcard::from_bytes::<SyncMsg>(&bytes) else { continue };
                if let SyncMsg::Version { proto } = msg {
                    if proto != riftpipe_core::sync::PROTOCOL_VERSION {
                        eprintln!(
                            "[tree] protocol-mismatch theirs={proto} ours={} — \
                             sync may be incomplete",
                            riftpipe_core::sync::PROTOCOL_VERSION,
                        );
                    }
                    continue;
                }
                for reply in peer.apply_and_persist(msg, dir) {
                    if let Ok(b) = postcard::to_allocvec(&reply) {
                        sink.send(b).await?;
                    }
                }
            }
        }
    }
}

/// The common session entry over any established link: prepare `dir`, start
/// the watcher (degrading to the poll fallback if it can't — never aborts),
/// and drive [`run`] until the link closes. Both the WebRTC-via-signaling path
/// and the native iroh ticket/accept paths end up here.
pub async fn run_over(sink: &mut dyn Sink, source: &mut dyn Source, dir: &str) -> Result<()> {
    let dir = prepare_dir(Path::new(dir))?;
    let (watcher, mut rx) = watch(&dir);
    let mut peer = TreePeer::new();
    run(&mut peer, &mut rx, watcher.is_none(), sink, source, &dir).await
}

/// Dial a peer's room over WebRTC (via the signaling server) and sync `dir`
/// both ways — the native end of browser↔native collaboration. Blocks until
/// the link drops. `counters` tracks the session's bytes for the caller to
/// report (a signaling session has no iroh `Endpoint`, so the tmux metrics
/// side-car can't run here — the caller prints totals instead).
pub async fn connect(
    signal: &str,
    room: &str,
    dir: &str,
    counters: std::sync::Arc<crate::net::Counters>,
) -> Result<()> {
    let link = crate::net::webrtc::connect_via_signaling(signal, room).await?;
    let (mut sink, mut source) = link.into_halves(counters);
    eprintln!("[riftpipe] connected to room '{room}' (WebRTC); syncing {dir}");
    run_over(&mut sink, &mut source, dir).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::mock_pair;
    use riftpipe_core::identity::DeviceKey;
    use tokio::time::{sleep, timeout};

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("riftpipe-tree-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::canonicalize(&d).unwrap()
    }

    /// The origin carried by `path`'s `TextDelta` in `msgs`.
    fn text_origin(msgs: &[SyncMsg], path: &str) -> Option<(String, usize)> {
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

    /// A restricted-mode peer (`--restricted`) with the device key from `seed`.
    fn restricted_peer(seed: u8) -> TreePeer {
        let mut p = TreePeer::new();
        p.set_identity(DeviceKey::from_seed(&[seed; 32]), true);
        p
    }

    /// What the mesh does when `joiner` meets `founder`: the keyless joiner's
    /// signed request reaches the founder (who add-wraps it if granted), then the
    /// founder's catch-up (trust log + epoch announces) reaches the joiner.
    fn join(founder: &mut TreePeer, df: &Path, joiner: &mut TreePeer, dj: &Path) {
        let req = joiner.read_request().expect("keyless restricted joiner announces itself");
        founder.authorize_and_apply(req, df);
        for m in founder.auth_catchup() {
            joiner.authorize_and_apply(m, dj);
        }
    }

    fn lww(path: &str, bytes: &[u8]) -> SyncMsg {
        SyncMsg::Lww { path: path.into(), version: 1000, bytes: bytes.to_vec() }
    }

    /// Founder (seed 1) of a restricted board with Members seed 2 and seed 3,
    /// both joined and wrapped into epoch 1.
    fn restricted_board(tag: &str) -> (TreePeer, PathBuf, TreePeer, PathBuf, TreePeer, PathBuf) {
        use riftpipe_core::identity::Role;
        let founder_key = DeviceKey::from_seed(&[1u8; 32]);
        let (df, dm, dc) = (dir(&format!("{tag}-f")), dir(&format!("{tag}-m")), dir(&format!("{tag}-c")));
        let mut founder = restricted_peer(1);
        founder.add_trust(founder_key.genesis());
        founder.add_trust(founder_key.grant(DeviceKey::from_seed(&[2u8; 32]).id(), Role::Member, [7u8; 16]));
        founder.add_trust(founder_key.grant(DeviceKey::from_seed(&[3u8; 32]).id(), Role::Member, [8u8; 16]));
        founder.startup();
        assert_eq!(founder.epoch_held(), Some(1), "the founder mints epoch 1 at startup");

        let (mut member, mut revokee) = (restricted_peer(2), restricted_peer(3));
        join(&mut founder, &df, &mut member, &dm);
        join(&mut founder, &df, &mut revokee, &dc);
        assert_eq!(member.epoch_held(), Some(1));
        assert_eq!(revokee.epoch_held(), Some(1));
        assert!(member.read_request().is_none(), "a wrapped peer stops asking");
        (founder, df, member, dm, revokee, dc)
    }

    /// Restricted-mode enforcement (`authorize_and_apply`, no network — what the
    /// mesh driver calls per incoming message): content is accepted only sealed
    /// under an epoch AND signed by an authorized device; unsealed or unsigned
    /// writes are dropped, and a revoked device's sealed write is rejected.
    #[test]
    fn restricted_mode_requires_sealed_authorized_writes() {
        let (mut founder, df, member, dm, revokee, dc) = restricted_board("auth");

        let sealed = member.sign_mesh(lww("state.bin", b"ok")).expect("a wrapped member seals");
        assert!(matches!(sealed, MeshMsg::Sealed(_)));
        founder.authorize_and_apply(sealed, &df);
        assert_eq!(std::fs::read(df.join("state.bin")).unwrap(), b"ok", "sealed authorized write persisted");

        let member_key = DeviceKey::from_seed(&[2u8; 32]);
        let unsealed =
            MeshMsg::SignedSync(member_key.sign_write(postcard::to_allocvec(&lww("unsealed.bin", b"x")).unwrap()));
        founder.authorize_and_apply(unsealed, &df);
        assert!(!df.join("unsealed.bin").exists(), "unsealed write dropped on a restricted board");

        founder.authorize_and_apply(MeshMsg::Sync(lww("unsigned.bin", b"y")), &df);
        assert!(!df.join("unsigned.bin").exists(), "unsigned write dropped on a restricted board");

        let out = founder.revoke_device(DeviceKey::from_seed(&[3u8; 32]).id(), &df);
        assert!(out.iter().any(|m| matches!(m, MeshMsg::Epoch(_))), "revoke rotates the epoch");

        let evil = revokee.sign_mesh(lww("evil.bin", b"z")).expect("still holds epoch 1");
        founder.authorize_and_apply(evil, &df);
        assert!(!df.join("evil.bin").exists(), "a revoked device's sealed write is rejected");

        for d in [&df, &dm, &dc] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// Read revocation: after the founder revokes a member, new content is sealed
    /// under an epoch the revoked member isn't wrapped into — the remaining member
    /// reads it, the revoked one doesn't.
    #[test]
    fn rotation_blinds_the_revoked_member() {
        let (mut founder, df, mut member, dm, mut revokee, dc) = restricted_board("rot");
        for m in founder.revoke_device(DeviceKey::from_seed(&[3u8; 32]).id(), &df) {
            let copy: MeshMsg = postcard::from_bytes(&postcard::to_allocvec(&m).unwrap()).unwrap();
            member.authorize_and_apply(copy, &dm);
            revokee.authorize_and_apply(m, &dc);
        }

        let after = founder.sign_mesh(lww("after.bin", b"epoch two")).unwrap();
        let copy: MeshMsg = postcard::from_bytes(&postcard::to_allocvec(&after).unwrap()).unwrap();
        member.authorize_and_apply(copy, &dm);
        revokee.authorize_and_apply(after, &dc);
        assert_eq!(std::fs::read(dm.join("after.bin")).unwrap(), b"epoch two");
        assert!(!dc.join("after.bin").exists(), "the revoked member can't open epoch 2");

        assert_eq!(member.epoch_held(), Some(2));
        assert_eq!(revokee.epoch_held(), Some(1));
        assert_eq!(revokee.epoch_known(), Some(2));

        for d in [&df, &dm, &dc] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// A member that has applied a revoke but not yet the rotation holds its writes
    /// (sealing under the old epoch would reach the removed device), then sends
    /// them sealed under the new epoch the moment it arrives.
    #[test]
    fn member_holds_writes_between_revoke_and_rotation() {
        let (mut founder, df, mut member, dm, _revokee, dc) = restricted_board("gap");
        // The swarm relays every announce: the member learns the revokee's wrap.
        for m in founder.auth_catchup() {
            member.authorize_and_apply(m, &dm);
        }
        std::fs::write(dm.join("gap.bin"), b"held").unwrap();
        let edit = member.local_change(&dm.join("gap.bin"), &dm).expect("a local edit");
        let out = founder.revoke_device(DeviceKey::from_seed(&[3u8; 32]).id(), &df);
        let (trust, epochs): (Vec<MeshMsg>, Vec<MeshMsg>) = out.into_iter().partition(|m| matches!(m, MeshMsg::Trust(_)));
        for m in trust {
            member.authorize_and_apply(m, &dm);
        }
        assert!(member.sign_local(edit).is_none(), "epoch 1 still wraps the revoked device");
        let mut released = Vec::new();
        for m in epochs {
            released.extend(member.authorize_and_apply(m, &dm).swarm);
        }
        assert!(
            released.iter().any(|m| matches!(m, MeshMsg::Sealed(s) if s.seq == 2)),
            "the held state goes out sealed under epoch 2"
        );
        for d in [&df, &dm, &dc] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// A hold that lifts without a new key (the revoked device is re-granted
    /// before the rotation reaches us) still sends the held writes.
    #[test]
    fn regrant_lifts_the_hold_and_resends() {
        let (mut founder, df, mut member, dm, _revokee, dc) = restricted_board("regrant");
        for m in founder.auth_catchup() {
            member.authorize_and_apply(m, &dm);
        }
        std::fs::write(dm.join("held.bin"), b"held").unwrap();
        let edit = member.local_change(&dm.join("held.bin"), &dm).expect("a local edit");
        let c_id = DeviceKey::from_seed(&[3u8; 32]).id();
        let revoke = founder.revoke_device(c_id, &df).into_iter().find(|m| matches!(m, MeshMsg::Trust(_))).unwrap();
        member.authorize_and_apply(revoke, &dm);
        assert!(member.sign_local(edit).is_none(), "held: epoch 1 wraps the revoked device");

        let regrant = founder.grant_device(c_id, &df).into_iter().find(|m| matches!(m, MeshMsg::Trust(_))).unwrap();
        let out = member.authorize_and_apply(regrant, &dm).swarm;
        assert!(
            out.iter().any(|m| matches!(m, MeshMsg::Sealed(s) if s.seq == 1)),
            "the hold lifted (no new key): held state goes out sealed"
        );
        for d in [&df, &dm, &dc] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// An open peer (no identity) can't read a restricted board's sealed content:
    /// it is dropped, nothing persists.
    #[test]
    fn keyless_restricted_peer_holds_local_edits_and_warns_once() {
        let mut keyless = restricted_peer(9);
        assert!(keyless.sign_local(lww("a.bin", b"1")).is_none(), "no key: nothing goes out");
        assert!(keyless.warned_keyless, "the held edit is reported");
        assert!(keyless.sign_local(lww("b.bin", b"2")).is_none());
    }

    /// The founder (seed 1) of a restricted board, holding epoch 1, no other members.
    fn founder_peer() -> TreePeer {
        let mut f = restricted_peer(1);
        f.add_trust(DeviceKey::from_seed(&[1u8; 32]).genesis());
        f.startup();
        f
    }

    fn request_from(seed: &[u8; 32], name: &str) -> MeshMsg {
        MeshMsg::Request(JoinRequest::create(&DeviceKey::from_seed(seed), name))
    }

    /// Request-to-join on the native peer: the joiner's signed request lands in the
    /// founder's list (once per device), and `approve_request` grants it AND wraps
    /// the current epoch to it, so the joiner can read.
    #[test]
    fn a_join_request_lands_in_the_inbox_and_approve_wraps_it_in() {
        let (df, dj) = (dir("inbox-f"), dir("inbox-j"));
        let mut founder = founder_peer();
        let mut joiner = restricted_peer(5);
        joiner.set_request_name("Bob");
        let jid = joiner.device_id().unwrap();

        founder.authorize_and_apply(joiner.read_request().expect("a keyless joiner asks"), &df);
        assert_eq!(founder.pending_requests().len(), 1);
        assert_eq!(founder.pending_requests()[0].request.name, "Bob");
        founder.authorize_and_apply(joiner.read_request().unwrap(), &df);
        assert_eq!(founder.pending_requests().len(), 1, "a repeated request keeps one entry");

        for m in founder.auth_catchup() {
            joiner.authorize_and_apply(m, &dj);
        }
        assert_eq!(joiner.epoch_held(), None, "not approved yet: no read key");

        let out = founder.approve_request(jid, &df);
        assert!(out.iter().any(|m| matches!(m, MeshMsg::Trust(_))), "approve grants");
        assert!(out.iter().any(|m| matches!(m, MeshMsg::Epoch(_))), "approve wraps the current epoch");
        assert!(founder.pending_requests().is_empty(), "a granted device leaves the list");
        assert!(founder.members().contains(&(jid, Role::Member)));

        for m in out {
            joiner.authorize_and_apply(m, &dj);
        }
        assert_eq!(joiner.epoch_held(), Some(1), "the approved joiner holds the read key");
        for d in [&df, &dj] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// Forged requests, our own request, and requests from already-authorized
    /// devices never enter the list; a grant by id clears a waiting entry.
    #[test]
    fn bad_or_already_authorized_requests_stay_out() {
        let df = dir("inbox-bad");
        let mut founder = founder_peer();

        let mut forged = JoinRequest::create(&DeviceKey::from_seed(&[5u8; 32]), "Mallory");
        forged.sig[0] ^= 1;
        founder.authorize_and_apply(MeshMsg::Request(forged), &df);

        let own = founder.my_request().unwrap().clone();
        founder.authorize_and_apply(MeshMsg::Request(own), &df);

        founder.grant_device(DeviceKey::from_seed(&[6u8; 32]).id(), &df);
        founder.authorize_and_apply(request_from(&[6u8; 32], "granted"), &df);
        assert!(founder.pending_requests().is_empty());

        founder.authorize_and_apply(request_from(&[7u8; 32], "later granted"), &df);
        assert_eq!(founder.pending_requests().len(), 1);
        founder.grant_device(DeviceKey::from_seed(&[7u8; 32]).id(), &df);
        assert!(founder.pending_requests().is_empty(), "grant by id clears the waiting entry");
        std::fs::remove_dir_all(&df).ok();
    }

    #[test]
    fn approve_without_a_pending_request_is_ignored() {
        let df = dir("approve-none");
        let mut founder = founder_peer();
        assert!(founder.approve_request([9u8; 32], &df).is_empty());
        assert_eq!(founder.members().len(), 1, "only the founder");
        std::fs::remove_dir_all(&df).ok();
    }

    /// A Member keeps the request list too (and shows it), but can't approve:
    /// approve refuses up front instead of announcing an approval that fails.
    #[test]
    fn approve_on_a_non_admin_peer_is_refused() {
        let (df, dm) = (dir("approve-member-f"), dir("approve-member-m"));
        let mut founder = founder_peer();
        let mut member = restricted_peer(5);
        let mid = member.device_id().unwrap();
        founder.authorize_and_apply(member.read_request().unwrap(), &df);
        for m in founder.auth_catchup().into_iter().chain(founder.approve_request(mid, &df)) {
            member.authorize_and_apply(m, &dm);
        }
        assert_eq!(member.members().len(), 2, "the member knows both devices");

        member.authorize_and_apply(request_from(&[8u8; 32], "Carol"), &dm);
        assert_eq!(member.pending_requests().len(), 1, "a Member sees the request");
        let carol = DeviceKey::from_seed(&[8u8; 32]).id();
        assert!(member.approve_request(carol, &dm).is_empty());
        assert_eq!(member.members().len(), 2, "nothing granted");
        assert_eq!(member.pending_requests().len(), 1, "the request stays for an Admin");
        for d in [&df, &dm] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    #[test]
    fn inbox_is_capped() {
        let df = dir("inbox-cap");
        let mut founder = founder_peer();
        for i in 0..REQUEST_INBOX_CAP + 5 {
            let mut s = [0u8; 32];
            s[0] = i as u8;
            s[1] = 0xAA;
            founder.authorize_and_apply(request_from(&s, "x"), &df);
        }
        assert_eq!(founder.pending_requests().len(), REQUEST_INBOX_CAP);
        assert!(founder.warned_inbox_full);
        std::fs::remove_dir_all(&df).ok();
    }

    /// The safety-code hijack: fill the list, let the victim's request drop, then
    /// re-sign a listed entry so its code matches the victim's. A full list refuses
    /// the re-sign, and once anything was dropped the state reports
    /// `inboxOverflow`, which makes approve-by-code refuse (approve by id still works).
    #[test]
    fn a_full_inbox_refuses_renames_and_disables_approve_by_code() {
        use crate::sync::control::resolve_approve_target;
        let df = dir("inbox-overflow");
        let mut founder = founder_peer();
        let seed = |i: usize| {
            let mut s = [0u8; 32];
            s[0] = i as u8;
            s[1] = 0xBB;
            s
        };
        for i in 0..REQUEST_INBOX_CAP {
            founder.authorize_and_apply(request_from(&seed(i), "junk"), &df);
        }
        assert_eq!(founder.pending_requests().len(), REQUEST_INBOX_CAP);
        assert_eq!(founder.auth_state_json().unwrap()["inboxOverflow"], false, "full, but nothing dropped yet");

        founder.authorize_and_apply(request_from(&seed(0), "renamed"), &df);
        assert_eq!(founder.pending_requests()[0].request.name, "junk", "a full list refuses re-signs");

        founder.authorize_and_apply(request_from(&[0xEEu8; 32], "victim"), &df);
        assert_eq!(founder.pending_requests().len(), REQUEST_INBOX_CAP, "the new request is dropped");
        let state = founder.auth_state_json().unwrap();
        assert_eq!(state["inboxOverflow"], true);

        let listed = &founder.pending_requests()[3];
        let code = listed.safety.clone();
        assert_eq!(code, listed.request.safety_code(), "the cached code is the real one");
        let listed = &listed.request;
        let err = resolve_approve_target(Some(&state), &code).unwrap_err();
        assert!(err.contains("overflowed"), "{err}");
        let by_id = resolve_approve_target(Some(&state), &hex32(&listed.device_id)).unwrap();
        assert_eq!(by_id.0, listed.device_id, "approve by device id still works");

        // Sticky: freeing a slot doesn't make codes trustworthy again.
        let first = founder.pending_requests()[0].request.device_id;
        founder.approve_request(first, &df);
        assert_eq!(founder.auth_state_json().unwrap()["inboxOverflow"], true);
        std::fs::remove_dir_all(&df).ok();
    }

    #[test]
    fn a_renamed_request_replaces_the_old_one() {
        let df = dir("inbox-rename");
        let mut founder = founder_peer();
        founder.authorize_and_apply(request_from(&[5u8; 32], "A"), &df);
        founder.authorize_and_apply(request_from(&[5u8; 32], "B"), &df);
        assert_eq!(founder.pending_requests().len(), 1);
        assert_eq!(founder.pending_requests()[0].request.name, "B");
        std::fs::remove_dir_all(&df).ok();
    }

    /// Granted by id before an Admin knew our wrap key: the Trust entry that
    /// authorizes us makes us re-announce our request, so the Admin can wrap us in.
    #[test]
    fn being_granted_prompts_a_keyless_device_to_re_announce() {
        let (df, dj) = (dir("regrant-f"), dir("regrant-j"));
        let mut founder = founder_peer();
        let mut joiner = restricted_peer(5);
        for m in founder.auth_catchup() {
            joiner.authorize_and_apply(m, &dj);
        }
        let jid = joiner.device_id().unwrap();
        let trust = founder.grant_device(jid, &df).into_iter().find(|m| matches!(m, MeshMsg::Trust(_))).unwrap();
        let out = joiner.authorize_and_apply(trust, &dj);
        assert!(out.swarm.iter().any(|m| matches!(m, MeshMsg::Request(_))), "the newly authorized device re-announces");
        for d in [&df, &dj] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    #[test]
    fn auth_state_json_reports_members_requests_and_my_request() {
        let df = dir("state-json");
        let mut founder = founder_peer();
        let mut joiner = restricted_peer(5);
        joiner.set_request_name("Bob");
        let req = joiner.my_request().unwrap().clone();
        founder.authorize_and_apply(MeshMsg::Request(req.clone()), &df);

        let f = founder.auth_state_json().unwrap();
        assert_eq!(f["role"], "admin");
        assert_eq!(f["epoch"], 1);
        assert_eq!(f["canRead"], true);
        assert_eq!(f["requests"][0]["safety"], req.safety_code());
        assert_eq!(f["requests"][0]["name"], "Bob");
        assert_eq!(f["members"][0]["founder"], true);
        assert_eq!(f["members"][0]["me"], true);
        assert!(f["myRequest"].is_null(), "an authorized device isn't asking");

        let j = joiner.auth_state_json().unwrap();
        assert!(j["role"].is_null());
        assert_eq!(j["myRequest"]["name"], "Bob");
        assert_eq!(j["myRequest"]["safety"], req.safety_code());
        assert_eq!(j["canRead"], false);

        assert!(TreePeer::new().auth_state_json().is_none());
        std::fs::remove_dir_all(&df).ok();
    }

    /// Pins `.riftpipe-auth.json`'s exact keys (top level, members, requests,
    /// myRequest), so no key material — `enc_pub`, a seed, a wrapped key — can
    /// slip into the machine-local snapshot unnoticed.
    #[test]
    fn auth_state_json_has_exactly_the_documented_keys() {
        fn keys(v: &serde_json::Value) -> Vec<&str> {
            let mut k: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
            k.sort_unstable();
            k
        }
        let df = dir("state-keys");
        let mut founder = founder_peer();
        let mut joiner = restricted_peer(5);
        joiner.set_request_name("Bob");
        founder.authorize_and_apply(joiner.read_request().unwrap(), &df);

        let f = founder.auth_state_json().unwrap();
        assert_eq!(
            keys(&f),
            [
                "canRead",
                "device",
                "epoch",
                "founder",
                "inboxOverflow",
                "members",
                "myRequest",
                "requests",
                "restricted",
                "role",
                "version"
            ]
        );
        assert_eq!(keys(&f["members"][0]), ["device", "founder", "me", "role"]);
        assert_eq!(keys(&f["requests"][0]), ["device", "name", "safety"]);
        let j = joiner.auth_state_json().unwrap();
        assert_eq!(keys(&j), keys(&f));
        assert_eq!(keys(&j["myRequest"]), ["name", "safety"]);
        std::fs::remove_dir_all(&df).ok();
    }

    #[test]
    fn request_name_is_cleaned() {
        let mut p = restricted_peer(5);
        p.set_request_name("  Bob\u{7} ");
        let req = p.my_request().unwrap();
        assert_eq!(req.name, "Bob?");
        assert!(req.verify(), "re-signed over the cleaned name");
    }

    /// The safety code hashes the signed name while Admins see the cleaned one,
    /// so only requests whose signed name is already canonical (and small) get in:
    /// no invisible suffix to grind a chosen code with, no megabyte names held.
    #[test]
    fn requests_with_non_canonical_names_stay_out() {
        let df = dir("inbox-canon");
        let mut founder = founder_peer();
        let huge = "x".repeat(1_000_000);
        for (i, name) in ["Bob ", "Bob\u{3000}", "Bob\u{200B}", "Bob\u{202E}", "\u{7}Bob", huge.as_str()]
            .into_iter()
            .enumerate()
        {
            founder.authorize_and_apply(request_from(&[20 + i as u8; 32], name), &df);
        }
        assert!(founder.pending_requests().is_empty());
        assert!(founder.warned_bad_name);

        founder.authorize_and_apply(request_from(&[30u8; 32], ""), &df);
        founder.authorize_and_apply(request_from(&[31u8; 32], "Bob"), &df);
        assert_eq!(founder.pending_requests().len(), 2, "browser (\"\") and cleaned CLI names get in");
        std::fs::remove_dir_all(&df).ok();
    }

    /// A Hello's replies (our whole tree, to a fresh joiner) go swarm-wide, like a
    /// Resync answer: a neighbors-only reply can miss a requester gossip holds as lazy.
    #[test]
    fn hello_replies_go_swarm_wide() {
        let d = dir("hello-scope");
        std::fs::write(d.join("a.md"), "# a\n").unwrap();
        let mut peer = TreePeer::new();
        peer.seed(&d);
        let out = peer.authorize_and_apply(MeshMsg::Sync(SyncMsg::Hello { versions: vec![], lww_versions: vec![] }), &d);
        assert!(
            out.swarm.iter().any(|m| matches!(m, MeshMsg::Sync(SyncMsg::TextDelta { path, .. }) if path == "a.md")),
            "the joiner's missing doc goes to the swarm"
        );
        let out = peer.authorize_and_apply(MeshMsg::Sync(SyncMsg::Resync { path: "a.md".into() }), &d);
        assert!(!out.swarm.is_empty(), "a Resync answer reaches whoever asked, anywhere in the swarm");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Anti-entropy input: an applied Hello reports which side is ahead, and the
    /// reply carries a write the other side never received.
    #[test]
    fn ae_hello_reports_divergence_and_pulls_the_missing_write() {
        let (da, db) = (dir("ae-open-a"), dir("ae-open-b"));
        let (mut a, mut b) = (TreePeer::new(), TreePeer::new());
        std::fs::write(da.join("x.toml"), "v1").unwrap();
        a.local_change(&da.join("x.toml"), &da).expect("a local write"); // never delivered

        let out = a.authorize_and_apply(b.sign_mesh(b.hello()).unwrap(), &da);
        assert_eq!(out.hello, Some(HelloCmp { we_ahead: true, they_ahead: false }));
        assert_eq!(out.swarm.len(), 1, "a answers b's Hello with the missed write");

        let at_b = b.authorize_and_apply(a.sign_mesh(a.hello()).unwrap(), &db);
        assert_eq!(at_b.hello, Some(HelloCmp { we_ahead: false, they_ahead: true }));

        let generation = b.generation();
        for m in out.swarm {
            b.authorize_and_apply(m, &db);
        }
        assert_eq!(std::fs::read(db.join("x.toml")).unwrap(), b"v1");
        assert!(b.generation() > generation, "the repaired write moves b's generation");
        let again = b.authorize_and_apply(a.sign_mesh(a.hello()).unwrap(), &db);
        assert!(again.hello.expect("a Hello is compared").consistent());

        for d in [&da, &db] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// A merged file whose disk write failed is written on a later message, even
    /// a duplicate that merges nothing (and so persists nothing by itself).
    #[test]
    fn failed_merge_write_is_retried_on_a_later_message() {
        let (da, db) = (dir("wfail-a"), dir("wfail-b"));
        let (mut a, mut b) = (TreePeer::new(), TreePeer::new());
        std::fs::write(da.join("x.toml"), "v1").unwrap();
        let msg = a.local_change(&da.join("x.toml"), &da).expect("a local write");
        let bytes = postcard::to_allocvec(&msg).unwrap();
        std::fs::create_dir_all(db.join("x.toml")).unwrap(); // a dir in the way: the write fails
        b.apply_and_persist(postcard::from_bytes(&bytes).unwrap(), &db);
        assert!(db.join("x.toml").is_dir(), "the write failed");
        std::fs::remove_dir_all(db.join("x.toml")).unwrap();
        b.apply_and_persist(postcard::from_bytes(&bytes).unwrap(), &db); // a duplicate: merges nothing
        assert_eq!(std::fs::read(db.join("x.toml")).unwrap(), b"v1", "the failed write was retried");
        for d in [&da, &db] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// A Hello's `Lww` reply carries its path's coalescing key, so the same reply
    /// to several neighbors' Hellos collapses in the outbound queue.
    #[test]
    fn hello_lww_replies_carry_their_coalescing_key() {
        let (da, db) = (dir("ae-key-a"), dir("ae-key-b"));
        let (mut a, b) = (TreePeer::new(), TreePeer::new());
        std::fs::write(da.join("x.toml"), "v1").unwrap();
        a.local_change(&da.join("x.toml"), &da).expect("a local write");
        std::fs::write(da.join("n.md"), "# n\n").unwrap();
        a.local_change(&da.join("n.md"), &da).expect("a local text write");
        let out = a.authorize_and_apply(b.sign_mesh(b.hello()).unwrap(), &da);
        let keys: Vec<Option<String>> = out.keyed_swarm().map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 2, "both files go back");
        assert!(keys.contains(&Some("lww:x.toml".into())), "the LWW reply is keyed: {keys:?}");
        assert!(keys.contains(&None), "a text delta stays unkeyed: {keys:?}");

        let mut q = riftpipe_core::outqueue::OutQueue::new();
        for _ in 0..3 {
            let out = a.authorize_and_apply(b.sign_mesh(b.hello()).unwrap(), &da);
            for (key, m) in out.keyed_swarm() {
                q.push(riftpipe_core::outqueue::Scope::Swarm, key, &m);
            }
        }
        assert_eq!(q.coalesced(), 2, "three identical LWW replies leave one queued");
        for d in [&da, &db] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// On a restricted board the Hello travels sealed and is still compared; the
    /// sealed reply repairs the missed write; an unsealed Hello is never compared.
    #[test]
    fn ae_hello_is_sealed_and_compared_on_a_restricted_board() {
        let (mut founder, df, mut member, dm, _revokee, dc) = restricted_board("ae");
        std::fs::write(dm.join("m.bin"), "v1").unwrap();
        member.local_change(&dm.join("m.bin"), &dm).expect("a local write");
        std::fs::write(dm.join("m.bin"), "v2").unwrap();
        member.local_change(&dm.join("m.bin"), &dm).expect("a local change"); // the founder misses both

        let member_hello = member.sign_mesh(member.hello()).expect("a wrapped member seals");
        assert!(matches!(member_hello, MeshMsg::Sealed(_)));
        let at_founder = founder.authorize_and_apply(member_hello, &df);
        assert!(at_founder.hello.expect("a sealed Hello is compared").they_ahead);

        let founder_hello = founder.sign_mesh(founder.hello()).expect("the founder seals");
        let at_member = member.authorize_and_apply(founder_hello, &dm);
        assert!(at_member.hello.expect("a sealed Hello is compared").we_ahead);
        assert!(at_member.swarm.iter().any(|m| matches!(m, MeshMsg::Sealed(_))), "the repair goes out sealed");

        for m in at_member.swarm {
            founder.authorize_and_apply(m, &df);
        }
        assert_eq!(std::fs::read(df.join("m.bin")).unwrap(), b"v2", "the missed write reaches the founder");

        let member_key = DeviceKey::from_seed(&[2u8; 32]);
        let unsealed = MeshMsg::SignedSync(member_key.sign_write(postcard::to_allocvec(&member.hello()).unwrap()));
        assert_eq!(founder.authorize_and_apply(unsealed, &df).hello, None, "an unsealed Hello is dropped unread");

        for d in [&df, &dm, &dc] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    #[test]
    fn open_peer_ignores_sealed_content() {
        use riftpipe_core::epoch::SealedMsg;
        let d = dir("open-sealed");
        let mut peer = TreePeer::new();
        let out = peer.authorize_and_apply(MeshMsg::Sealed(SealedMsg { seq: 1, id: [0; 8], body: vec![1] }), &d);
        assert!(out.swarm.is_empty());
        assert_eq!(std::fs::read_dir(&d).unwrap().count(), 0, "nothing persisted");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Open mode (the default — no identity, `LinkEditor`) is unchanged:
    /// plain unsigned writes still apply, so the shipped kanban flow keeps working.
    #[test]
    fn open_mode_still_accepts_unsigned_writes() {
        let d = dir("open");
        let mut peer = TreePeer::new(); // no identity
        let plain = MeshMsg::Sync(SyncMsg::Lww { path: "open.bin".into(), version: 1000, bytes: b"z".to_vec() });
        peer.authorize_and_apply(plain, &d);
        assert_eq!(std::fs::read(d.join("open.bin")).unwrap(), b"z", "open mode applies unsigned writes");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Drive two crossed TreePeers over a mock link: feed `feed` (paths
    /// relative to `da`) to A as watcher events, run both sessions until
    /// `settled()` holds, keep pumping briefly so in-flight messages land, then
    /// tear down by dropping A's send half — B's `source.recv()` sees the
    /// close and its `run()` returns (the session-end path under test).
    async fn converge(da: &Path, db: &Path, feed: &[&str], settled: impl Fn() -> bool) {
        let (la, lb) = mock_pair();
        let (mut sink_a, mut source_a) = la.into_halves();
        let (mut sink_b, mut source_b) = lb.into_halves();

        let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
        let (_tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
        for f in feed {
            tx_a.send(da.join(f)).unwrap();
        }

        let db_owned = db.to_path_buf();
        let b_task = tokio::spawn(async move {
            let mut pb = TreePeer::new();
            run(&mut pb, &mut rx_b, false, &mut sink_b, &mut source_b, &db_owned)
                .await
                .expect("B's session ends cleanly when A hangs up");
        });

        {
            let mut pa = TreePeer::new();
            let run_a = run(&mut pa, &mut rx_a, false, &mut sink_a, &mut source_a, da);
            tokio::pin!(run_a);
            loop {
                tokio::select! {
                    r = &mut run_a => panic!("A's session ended early: {r:?}"),
                    _ = sleep(Duration::from_millis(20)) => {
                        if settled() {
                            break;
                        }
                    }
                }
            }
            // Settled — keep both sides pumping a little longer so anything
            // still in flight (e.g. a wrongly-pushed dotfile) would land and
            // be caught by the assertions.
            tokio::select! {
                r = &mut run_a => panic!("A's session ended early: {r:?}"),
                _ = sleep(Duration::from_millis(300)) => {}
            }
        } // run_a dropped here, releasing the borrow on sink_a
        drop(sink_a); // close B's source → B's run() returns
        b_task.await.unwrap();
    }

    #[tokio::test]
    async fn two_peers_converge_text_and_lww() {
        let da = dir("conv-a");
        let db = dir("conv-b");
        std::fs::create_dir_all(da.join("tickets/tk_1")).unwrap();
        std::fs::write(da.join("tickets/tk_1/card.md"), "# card one\n\nbody\n").unwrap();
        std::fs::write(da.join("tickets/tk_1/meta.toml"), "column = \"Todo\"\nposition = 0\n").unwrap();

        let (cb, mb) = (db.join("tickets/tk_1/card.md"), db.join("tickets/tk_1/meta.toml"));
        timeout(
            Duration::from_secs(15),
            converge(
                &da,
                &db,
                &["tickets/tk_1/card.md", "tickets/tk_1/meta.toml"],
                || cb.is_file() && mb.is_file(),
            ),
        )
        .await
        .expect("tree sync timed out");

        assert_eq!(
            std::fs::read(db.join("tickets/tk_1/card.md")).unwrap(),
            b"# card one\n\nbody\n",
            "text file arrives byte-identical via the CRDT path",
        );
        assert_eq!(
            std::fs::read(db.join("tickets/tk_1/meta.toml")).unwrap(),
            b"column = \"Todo\"\nposition = 0\n",
            "structural file arrives via LWW",
        );
        std::fs::remove_dir_all(&da).ok();
        std::fs::remove_dir_all(&db).ok();
    }

    /// Files that exist BEFORE the session starts — no watcher events, no poll
    /// — must still transfer: `run` seeds the syncer from disk so the hello
    /// handshake advertises them. (The bug this pins: a native mesh host whose
    /// tree predates `connect --accept` broadcast an empty full_state.)
    #[tokio::test]
    async fn preexisting_files_sync_with_no_watcher_events() {
        let da = dir("seed-a");
        let db = dir("seed-b");
        std::fs::create_dir_all(da.join("tickets/tk_9")).unwrap();
        std::fs::write(da.join("tickets/tk_9/card.md"), "# was here first\n").unwrap();
        std::fs::write(da.join("tickets/tk_9/meta.toml"), "column = \"Done\"\n").unwrap();

        let (cb, mb) = (db.join("tickets/tk_9/card.md"), db.join("tickets/tk_9/meta.toml"));
        timeout(
            Duration::from_secs(15),
            converge(&da, &db, &[], || cb.is_file() && mb.is_file()), // NO fed events
        )
        .await
        .expect("pre-existing tree must sync via the seeded handshake");

        assert_eq!(std::fs::read(cb).unwrap(), b"# was here first\n");
        assert_eq!(std::fs::read(mb).unwrap(), b"column = \"Done\"\n");
        std::fs::remove_dir_all(&da).ok();
        std::fs::remove_dir_all(&db).ok();
    }

    #[tokio::test]
    async fn dot_paths_never_sync() {
        let da = dir("dot-a");
        let db = dir("dot-b");
        std::fs::create_dir_all(da.join("tickets")).unwrap();
        std::fs::write(da.join(".site"), "site-a\n").unwrap();
        std::fs::write(da.join("tickets/.hidden"), "shh\n").unwrap();
        // A normal sentinel file: once it lands on B, A's whole batch (which
        // included the dot-paths) has been processed.
        std::fs::write(da.join("board.md"), "# Board\n\n- Todo\n").unwrap();

        let sentinel = db.join("board.md");
        timeout(
            Duration::from_secs(15),
            converge(
                &da,
                &db,
                &[".site", "tickets/.hidden", "board.md"],
                || sentinel.is_file(),
            ),
        )
        .await
        .expect("tree sync timed out");

        assert!(!db.join(".site").exists(), "top-level .site must never sync");
        assert!(!db.join("tickets/.hidden").exists(), "nested dotfile must never sync");
        std::fs::remove_dir_all(&da).ok();
        std::fs::remove_dir_all(&db).ok();
    }

    /// A restarted peer's `seed` restores durable records before priming disk,
    /// so history and LWW versions survive the restart instead of being
    /// re-stamped as fresh (the bug step 2's `restore` fixes).
    #[test]
    fn restart_resumes_history_and_keeps_lww_versions() {
        let d = dir("restart-resume");
        std::fs::write(d.join("a.md"), "# one\n").unwrap();
        std::fs::write(d.join("m.toml"), "x = 1\n").unwrap();

        let mut p1 = TreePeer::new();
        p1.seed(&d);
        let rec_count = std::fs::read_dir(d.join(SYNC_DIR))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("rec"))
            .count();
        assert_eq!(rec_count, 2, "one durable record per path");

        let state1 = p1.full_state();
        let origin1 = text_origin(&state1, "a.md");
        let version1 = lww_version(&state1, "m.toml");

        let mut p2 = TreePeer::new();
        p2.seed(&d);
        let state2 = p2.full_state();
        assert_eq!(text_origin(&state2, "a.md"), origin1, "restart keeps the same CRDT origin");
        assert_eq!(lww_version(&state2, "m.toml"), version1, "restart keeps the same LWW version");

        assert!(
            p2.local_change(&d.join("a.md"), &d).is_none(),
            "unchanged file after seed is in `seen`, not re-pushed"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// A file edited while its peer was down becomes a proper local edit on
    /// top of the restored history — a clean merge, not an origin-conflict
    /// adoption (the restarted peer's origin is unchanged).
    #[test]
    fn restart_turns_offline_edits_into_local_changes() {
        let d = dir("restart-offline");
        std::fs::write(d.join("a.md"), "# one\n").unwrap();

        let mut p1 = TreePeer::new();
        p1.seed(&d);
        let state1 = p1.full_state();
        let origin1 = text_origin(&state1, "a.md");

        std::fs::write(d.join("a.md"), "# two\n").unwrap();
        let mut p2 = TreePeer::new();
        p2.seed(&d);
        let state2 = p2.full_state();
        let origin2 = text_origin(&state2, "a.md");

        let mut c = Syncer::new("c");
        for m in state1.into_iter().filter(|m| matches!(m, SyncMsg::TextDelta { path, .. } if path == "a.md")) {
            c.apply(m);
        }
        let mut persisted = None;
        for m in state2.into_iter().filter(|m| matches!(m, SyncMsg::TextDelta { path, .. } if path == "a.md")) {
            let (persist, _) = c.apply(m);
            if let Some((_, bytes)) = persist {
                persisted = Some(bytes);
            }
        }
        assert_eq!(persisted, Some(b"# two\n".to_vec()), "offline edit lands as a clean merge");
        assert_eq!(origin2, origin1, "no origin-conflict adoption");
        std::fs::remove_dir_all(&d).ok();
    }

    /// `apply_and_persist` writes a durable record alongside the file, not
    /// just on local edits — so a remote-only peer still resumes correctly.
    #[test]
    fn remote_merge_writes_a_record() {
        let d = dir("remote-merge");
        let mut s = Syncer::new("s");
        let m = s.local_text("r.md", "# r\n").unwrap();
        let mut peer = TreePeer::new();
        peer.apply_and_persist(m, &d);
        assert!(d.join("r.md").exists());
        assert!(record_path(&d.join(SYNC_DIR), "r.md").exists());
        std::fs::remove_dir_all(&d).ok();
    }

    /// A record for a path no longer on disk is dropped on the next seed,
    /// instead of lingering forever in `.riftpipe-sync`.
    #[test]
    fn records_of_deleted_files_are_dropped() {
        let d = dir("deleted-record");
        std::fs::write(d.join("a.md"), "# one\n").unwrap();
        let mut p1 = TreePeer::new();
        p1.seed(&d);
        assert!(record_path(&d.join(SYNC_DIR), "a.md").exists());

        std::fs::remove_file(d.join("a.md")).unwrap();
        let mut p2 = TreePeer::new();
        p2.seed(&d);
        assert!(!record_path(&d.join(SYNC_DIR), "a.md").exists(), "record for a deleted file is dropped");
        std::fs::remove_dir_all(&d).ok();
    }

    /// A dir reused for ANOTHER mesh board never restores the first board's
    /// history (which `full_state` would ship to the new board's peers, deleted
    /// text included): records are scoped per board, and the old board's stay
    /// for when it's opened again.
    #[test]
    fn records_are_scoped_per_board() {
        let d = dir("board-scoped-records");
        std::fs::write(d.join("a.md"), "# one\n").unwrap();
        let mut on_a = TreePeer::new();
        on_a.set_board([0xa; 32]);
        on_a.seed(&d);
        let origin_a = text_origin(&on_a.full_state(), "a.md");

        let mut on_b = TreePeer::new();
        on_b.set_board([0xb; 32]);
        on_b.seed(&d);
        assert_ne!(text_origin(&on_b.full_state(), "a.md"), origin_a, "board B starts fresh");

        let mut a_again = TreePeer::new();
        a_again.set_board([0xa; 32]);
        a_again.seed(&d);
        assert_eq!(text_origin(&a_again.full_state(), "a.md"), origin_a, "board A's history is intact");
        std::fs::remove_dir_all(&d).ok();
    }

    /// The NeighborUp Hello on a restricted board: it goes out sealed, a wrapped
    /// member answers it (sealed) with its gap edit, and the
    /// founder persists it. An unsealed Hello is dropped, and a device revoked
    /// since can't read the new-epoch Hello, so it answers nothing.
    #[test]
    fn restricted_hello_is_sealed_and_pulls_the_gap() {
        let (mut founder, df, mut member, dm, mut revokee, dc) = restricted_board("hello");
        std::fs::write(dm.join("gap.md"), "# made while apart\n").unwrap();
        assert!(member.local_change(&dm.join("gap.md"), &dm).is_some(), "the gap edit (never delivered)");

        let hello = founder.sign_mesh(founder.hello()).expect("a key holder can send its Hello");
        assert!(matches!(hello, MeshMsg::Sealed(_)), "the Hello is sealed on a restricted board");
        let out = member.authorize_and_apply(hello, &dm);
        assert!(!out.swarm.is_empty() && out.swarm.iter().all(|m| matches!(m, MeshMsg::Sealed(_))));
        for m in out.swarm {
            founder.authorize_and_apply(m, &df);
        }
        assert_eq!(std::fs::read(df.join("gap.md")).unwrap(), b"# made while apart\n");

        let plain = MeshMsg::Sync(SyncMsg::Hello { versions: vec![], lww_versions: vec![] });
        assert!(member.authorize_and_apply(plain, &dm).swarm.is_empty(), "an unsealed Hello is dropped");

        founder.revoke_device(DeviceKey::from_seed(&[3u8; 32]).id(), &df);
        let hello2 = founder.sign_mesh(founder.hello()).expect("sealed under the new epoch");
        assert!(revokee.authorize_and_apply(hello2, &dc).swarm.is_empty(), "a revoked device can't answer it");
        for d in [&df, &dm, &dc] {
            std::fs::remove_dir_all(d).ok();
        }
    }
}
