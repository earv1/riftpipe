//! Gossip-mesh transport for the browser: a shared file tree is an **iroh-gossip topic**, and
//! peers broadcast `SyncMsg`s epidemically — no fixed hub, the swarm self-organizes
//! and survives any peer leaving. The N-peer-safe merge in `riftpipe_core::sync`
//! handles convergence; this module is just the transport + membership.
//!
//! Topology: the topic is `H(board secret)` (32 bytes), so everyone opening the
//! same share link lands on the same topic — and a stranger who only knows the
//! host's public key cannot derive it (see `core::crypto` / `SECURITY.md`). A
//! joiner seeds the host's address (from the ticket) into a memory lookup and
//! bootstraps off it; after that, gossip membership spreads and the host is no
//! longer special. Every frame is AEAD-sealed with a key derived from the same
//! secret, so the relay and non-holders see only ciphertext.

use futures_util::StreamExt;
use iroh::address_lookup::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_gossip::proto::TopicId;

/// The gossip topic for a board secret — `H(secret)`, underivable without the
/// share link. Identical to the native `sync::mesh::topic_of`.
pub fn topic_of(secret: &[u8; 32]) -> TopicId {
    TopicId::from_bytes(riftpipe_core::crypto::topic_from_secret(secret))
}

/// A joined gossip mesh for one shared tree: broadcast bytes to everyone, receive a
/// stream of events (messages + membership changes).
pub struct Mesh {
    endpoint: Endpoint,
    _router: Router,
    _gossip: Gossip,
    sender: GossipSender,
    receiver: GossipReceiver,
    my_id: EndpointId,
    /// The peer we bootstrapped off (`None` when we host) — kept so a TOTAL
    /// neighbor loss can re-join it: hyparview heals only from its passive
    /// view and never re-dials the original bootstrap on its own.
    bootstrap: Option<EndpointId>,
    /// Seals/opens every gossip frame with the key derived from the board
    /// secret — the relay and non-holders see only ciphertext.
    cipher: riftpipe_core::crypto::MeshCipher,
}

impl Mesh {
    /// Join the board identified by `secret`. `bootstrap` is the host's address
    /// (from the ticket) for a joiner, or `None` for the host itself (it starts
    /// the swarm). The `secret` both names the topic (`H(secret)`) and keys the
    /// frame cipher, so only link-holders find and can read the board.
    pub async fn join(
        sk: SecretKey,
        secret: [u8; 32],
        bootstrap: Option<EndpointAddr>,
    ) -> Result<Mesh, String> {
        let my_id = sk.public();
        // Seed the bootstrap address so we can dial it without waiting on discovery.
        let lookup = MemoryLookup::new();
        let mut boot = Vec::new();
        if let Some(addr) = bootstrap {
            boot.push(addr.id);
            lookup.add_endpoint_info(addr);
        }
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(sk)
            .address_lookup(lookup)
            .bind()
            .await
            .map_err(|e| e.to_string())?;
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
            .map_err(|e| e.to_string())?;
        let (sender, receiver) = topic.split();
        let cipher = riftpipe_core::crypto::MeshCipher::from_secret(&secret);
        Ok(Mesh {
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

    pub fn my_id(&self) -> EndpointId {
        self.my_id
    }

    /// This peer's dialable address (share it as the bootstrap ticket).
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Wait until this peer has a dialable (relay) address to share.
    pub async fn wait_for_addr(&self) -> EndpointAddr {
        for _ in 0..400 {
            let a = self.endpoint.addr();
            if !a.addrs.is_empty() {
                return a;
            }
            gloo_timers::future::TimeoutFuture::new(50).await;
        }
        self.endpoint.addr()
    }

    /// Broadcast `bytes` to every peer in the mesh.
    pub async fn broadcast(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.sender
            .broadcast(bytes.into())
            .await
            .map_err(|e| e.to_string())
    }

    /// The next mesh event (a received message, or a neighbor up/down).
    pub async fn next(&mut self) -> Option<Event> {
        match self.receiver.next().await {
            Some(Ok(ev)) => Some(ev),
            _ => None,
        }
    }

    /// Split into a broadcast sender, an event receiver, the bootstrap peer
    /// (for a re-join after total neighbor loss), the frame cipher (shared —
    /// the recv task and the push path both seal/open with it), and a keep-alive
    /// handle (the endpoint/router/gossip must outlive both).
    fn into_parts(
        self,
    ) -> (
        GossipSender,
        GossipReceiver,
        Option<EndpointId>,
        std::rc::Rc<riftpipe_core::crypto::MeshCipher>,
        MeshKeepAlive,
    ) {
        (
            self.sender,
            self.receiver,
            self.bootstrap,
            std::rc::Rc::new(self.cipher),
            MeshKeepAlive { _endpoint: self.endpoint, _router: self._router, _gossip: self._gossip },
        )
    }
}

/// Keeps the mesh's endpoint/router/gossip alive for the sync's lifetime.
pub struct MeshKeepAlive {
    _endpoint: Endpoint,
    _router: Router,
    _gossip: Gossip,
}

impl MeshKeepAlive {
    /// Close the endpoint (and await it) so a reconnect can rebind the same key
    /// cleanly — a plain drop is async and would race the relay re-registration.
    async fn close(self) {
        self._endpoint.close().await;
    }
}

/// What travels over the gossip topic — the shared wire type, single-sourced in
/// core so native and browser peers speak the same layout.
use riftpipe_core::sync::MeshMsg;

use riftpipe_core::epoch::{AccessMode, EpochAnnounce, EpochBook, Ingested};
use riftpipe_core::identity::{DeviceKey, Role, TrustEntry, TrustLog};
use riftpipe_core::onboard::JoinRequest;
use riftpipe_core::sync::SyncMsg;

/// localStorage key prefix: `riftpipe:trust:v2:<hex topic>` = `hex(postcard(Vec<TrustEntry>))`.
/// One entry per board, so opening another board never overwrites (or unlocks)
/// this one, and a stale genesis from another board can't lock you out.
const TRUST_STORE_PREFIX: &str = "riftpipe:trust:v2:";
/// localStorage: the pre-v2, board-unscoped `hex(postcard(Vec<TrustEntry>))`.
/// Migrated once, and only into this browser's own hosted board (see
/// [`load_trust_entries`]).
const LEGACY_TRUST_STORE_KEY: &str = "riftpipe:trust";
/// localStorage key prefix: `riftpipe:epochs:<hex topic>` = `hex(postcard(Vec<EpochAnnounce>))`
/// — verified announces only (wraps are ciphertext to their recipients); raw
/// epoch keys are never stored, they're re-opened with the device seed on load.
const EPOCH_STORE_PREFIX: &str = "riftpipe:epochs:";

/// What handling a message (or an auth action) produced, for the driver to act
/// on: content to apply to the syncer, messages to broadcast to the swarm, and
/// whether to broadcast our full state too (we just gained a read key, or an
/// admin just wrapped a newcomer in).
#[derive(Default)]
struct Accepted {
    apply: Vec<SyncMsg>,
    send: Vec<MeshMsg>,
    send_full_state: bool,
}

impl Accepted {
    fn merge(&mut self, o: Accepted) {
        self.apply.extend(o.apply);
        self.send.extend(o.send);
        self.send_full_state |= o.send_full_state;
    }
}

/// Device-linking auth for a browser mesh session. The model is **lock =
/// authorship**: a board is *locked* (claimed) exactly when it has an author (a
/// genesis in the trust log). The first opener locks it by adding their genesis,
/// which broadcasts live. Before that (no author) the board is open — plain
/// unsigned writes, anyone with the link reads and edits. A claimed board is
/// **Restricted**: every peer signs its writes and rejects unsigned or
/// unauthorized ones, and content is sealed under the current epoch key, wrapped
/// only to approved devices — so an un-approved device can't read, and a removed
/// device is rotated out of new content. The device identity is always present
/// (persisted); the trust log and the epoch announces persist too, scoped to the
/// board, so a claimed board stays claimed (and readable) across reloads. See
/// `docs/security/`.
pub struct Auth {
    identity: DeviceKey,
    /// The board's gossip topic (`H(secret)`) — scopes the persisted trust log
    /// and epochs to this board.
    topic: [u8; 32],
    trust: TrustLog,
    entries: Vec<TrustEntry>,
    seen: std::collections::HashSet<[u8; 32]>,
    /// Epoch keys this device can open, and (as owner) whom to wrap next.
    epochs: EpochBook,
    /// Access requests received from would-be writers (owner side), deduped by
    /// requesting device id — the "someone asked" inbox the UI renders.
    requests: Vec<JoinRequest>,
    /// Our own outstanding access request (viewer side), re-announced to new
    /// neighbors so a late-arriving owner still sees it. Cleared once granted.
    my_request: Option<JoinRequest>,
}

impl Auth {
    /// Build from this device's identity, restoring the trust log and epoch
    /// announces persisted for `topic` (the board's gossip topic), so a claimed
    /// board stays claimed — and readable — across reloads.
    pub fn load(identity: DeviceKey, topic: [u8; 32]) -> Self {
        let epochs = EpochBook::new(&identity);
        let mut a = Auth {
            identity,
            topic,
            trust: TrustLog::new(),
            entries: Vec::new(),
            seen: Default::default(),
            epochs,
            requests: Vec::new(),
            my_request: None,
        };
        for e in load_trust_entries(&topic) {
            a.add_trust(e);
        }
        for an in load_epochs(&topic) {
            a.epochs.ingest(an, &a.trust);
        }
        // Anything this produces is in the book and goes out with the catch-up.
        let _ = a.maintain();
        a
    }

    /// Ingest a trust entry (idempotent), remember it for neighbor catch-up, and
    /// persist. The wire/load is the source; we persist our own converged view.
    pub fn add_trust(&mut self, e: TrustEntry) {
        let id = *blake3::hash(&postcard::to_allocvec(&e).unwrap_or_default()).as_bytes();
        let fresh = self.seen.insert(id);
        if fresh {
            self.entries.push(e.clone());
        }
        self.trust.apply(e);
        if fresh {
            persist_trust_entries(&self.topic, &self.entries);
        }
        // A grant may have resolved pending asks: drop any request whose device
        // is now authorized, and clear our own once we're in.
        let trust = &self.trust;
        self.requests.retain(|r| !trust.is_authorized(&r.device_id));
        if self.trust.is_authorized(&self.my_id()) {
            self.my_request = None;
        }
    }

    /// A board is locked once it has an author (genesis).
    fn locked(&self) -> bool {
        self.trust.founder().is_some()
    }

    /// How this board gates reads and writes — the **only** place the browser
    /// decides: claimed = `Restricted`, unclaimed = `LinkEditor`. A general-access
    /// setting (e.g. `LinkViewer`) would be chosen here; see `core::epoch::AccessMode`.
    fn mode(&self) -> AccessMode {
        if self.locked() {
            AccessMode::Restricted
        } else {
            AccessMode::LinkEditor
        }
    }

    /// Can we read new content? Always on an open board; on a restricted one only
    /// while we hold the latest epoch key.
    fn can_read(&self) -> bool {
        !self.mode().seals_content() || self.epochs.holds_latest()
    }

    fn my_id(&self) -> [u8; 32] {
        self.identity.id()
    }
    fn authorized(&self) -> bool {
        self.trust.is_authorized(&self.my_id())
    }
    fn is_author(&self) -> bool {
        self.trust.founder() == Some(self.my_id())
    }
    fn is_admin(&self) -> bool {
        self.trust.role(&self.my_id()) == Some(Role::Admin)
    }

    fn fresh_nonce() -> [u8; 16] {
        let mut nonce = [0u8; 16];
        nonce.copy_from_slice(&riftpipe_core::crypto::generate_secret()[..16]);
        nonce
    }

    /// Become the board's author (lock it) if it has none yet, minting epoch 1.
    /// Returns the genesis + epoch announce to broadcast.
    fn lock_as_author(&mut self) -> Option<Accepted> {
        if self.locked() {
            return None;
        }
        let g = self.identity.genesis();
        self.add_trust(g.clone());
        let mut acc = self.maintain();
        acc.send.insert(0, MeshMsg::Trust(g));
        Some(acc)
    }

    /// Grant `device` Member access (we must be an Admin) and wrap it into the
    /// current epoch if we know its encryption key. Returns what to broadcast.
    fn make_grant(&mut self, device: [u8; 32]) -> Option<Accepted> {
        if !self.is_admin() {
            return None;
        }
        let e = self.identity.grant(device, Role::Member, Self::fresh_nonce());
        self.add_trust(e.clone());
        let mut acc = self.maintain();
        acc.send.insert(0, MeshMsg::Trust(e));
        Some(acc)
    }

    /// Remove `device` (we must be an Admin; never the founder, never ourselves):
    /// a signed revoke, then a rotation to a new epoch it isn't wrapped into, so it
    /// can't read anything new. Returns what to broadcast.
    fn make_revoke(&mut self, device: [u8; 32]) -> Option<Accepted> {
        if !self.is_admin() || self.trust.founder() == Some(device) || device == self.my_id() {
            return None;
        }
        let e = self.identity.revoke(device, Self::fresh_nonce());
        self.add_trust(e.clone());
        let mut acc = self.maintain();
        acc.send.insert(0, MeshMsg::Trust(e));
        Some(acc)
    }

    /// Wrap an outgoing write: a `SignedSync` when the mode requires signed writes,
    /// else a plain `Sync` — then, on a restricted board, sealed under the latest
    /// epoch we hold. `None` when restricted and we hold no epoch key (the edit
    /// stays in the syncer and goes out with our full state once we're wrapped in).
    fn sign(&self, msg: SyncMsg) -> Option<MeshMsg> {
        let mode = self.mode();
        let signed = if mode.requires_signed_writes() {
            let payload = postcard::to_allocvec(&msg).unwrap_or_default();
            MeshMsg::SignedSync(self.identity.sign_write(payload))
        } else {
            MeshMsg::Sync(msg)
        };
        if mode.seals_content() {
            self.epochs.seal(&signed)
        } else {
            Some(signed)
        }
    }

    /// Ask to become a writer on a locked board: build a signed join request
    /// (idempotent — reuses ours), remember it for re-announce, and return it to
    /// broadcast. `None` when there's nothing to ask (open board, or we're
    /// already authorized).
    fn request_access(&mut self) -> Option<MeshMsg> {
        if !self.locked() || self.authorized() {
            return None;
        }
        let req = JoinRequest::create(&self.identity, String::new());
        self.my_request = Some(req.clone());
        Some(MeshMsg::Request(req))
    }

    /// Our signed join request while we're approved but can't read (restricted,
    /// not holding the latest epoch key) — broadcast to the whole swarm so an
    /// Admin (maybe not a direct neighbor) learns our encryption key and wraps us in.
    fn read_request(&self) -> Option<MeshMsg> {
        (self.mode().seals_content() && self.authorized() && !self.epochs.holds_latest())
            .then(|| MeshMsg::Request(JoinRequest::create(&self.identity, String::new())))
    }

    /// Record an incoming access request: an authentic one teaches the epoch book
    /// the device's encryption key (owner side: whom to wrap); it joins the inbox
    /// if the device isn't already authorized. Deduped by device.
    fn ingest_request(&mut self, req: JoinRequest) {
        if !req.verify() {
            return;
        }
        self.epochs.learn_request(&req);
        if self.trust.is_authorized(&req.device_id) {
            return;
        }
        if !self.requests.iter().any(|r| r.device_id == req.device_id) {
            self.requests.push(req);
        }
    }

    /// Pending asks awaiting our approval (only meaningful when we can grant).
    fn pending(&self) -> &[JoinRequest] {
        &self.requests
    }

    /// Authorized devices with their roles, sorted by id.
    fn members(&self) -> Vec<([u8; 32], Role)> {
        self.trust.devices()
    }

    /// The messages to catch a neighbor up (gossip is ephemeral): every trust
    /// entry, then every accepted epoch announce (trust first, so their signers
    /// are known Admins), plus our own outstanding ask so a late-arriving owner
    /// sees it.
    fn catchup(&self) -> Vec<MeshMsg> {
        let mut out: Vec<MeshMsg> = self.entries.iter().cloned().map(MeshMsg::Trust).collect();
        out.extend(self.epochs.announces().iter().cloned().map(MeshMsg::Epoch));
        if !self.authorized() {
            if let Some(req) = &self.my_request {
                out.push(MeshMsg::Request(req.clone()));
            }
        }
        out
    }

    /// The content inside a `Sync` / `SignedSync` (already opened if it arrived
    /// sealed), if it passes the write gate: unsigned only when the mode doesn't
    /// require signed writes; signed verified against the trust log when gated,
    /// else just author-verified.
    fn content_of(&self, gm: MeshMsg) -> Option<SyncMsg> {
        let gated = self.mode().requires_signed_writes();
        match gm {
            MeshMsg::Sync(s) => (!gated).then_some(s),
            MeshMsg::SignedSync(sw) => {
                let ok = if gated {
                    sw.verified(&self.trust).is_some() // authorized writer
                } else {
                    sw.author_verified().is_some() // open: any authentic signature
                };
                ok.then(|| postcard::from_bytes::<SyncMsg>(&sw.payload).ok()).flatten()
            }
            _ => None,
        }
    }

    /// Admin housekeeping on a restricted board (see `EpochBook::maintain`): the
    /// founder mints epoch 1, an epoch still wrapping a revoked device is rotated
    /// away, newly authorized devices are wrapped in (then our full state goes out
    /// so they catch up). Empty on an open board or for non-admins.
    fn maintain(&mut self) -> Accepted {
        let mut acc = Accepted::default();
        if !self.mode().seals_content() {
            return acc;
        }
        let m = self.epochs.maintain(&self.identity, &self.trust);
        if let Some((seq, n)) = m.rotated {
            web_sys::console::info_1(&format!("[epoch] rotated to epoch {seq} — {n} device(s) wrapped in").into());
        }
        if !m.announces.is_empty() {
            persist_epochs(&self.topic, self.epochs.announces());
        }
        acc.send = m.announces.into_iter().map(MeshMsg::Epoch).collect();
        acc.send_full_state = m.added > 0;
        acc
    }

    /// Act on what accepting announce(s) changed: persist, apply content parked
    /// for a newly-held key, broadcast our full state on our first key (it carries
    /// edits made while keyless), then run Admin maintenance.
    fn followups(&mut self, ing: Ingested) -> Accepted {
        if ing.changed {
            persist_epochs(&self.topic, self.epochs.announces());
        }
        if let Some(seq) = ing.unheld {
            web_sys::console::info_1(
                &format!("[epoch] no key for epoch {seq} — this device is not wrapped in (revoked, or not yet approved)").into(),
            );
        }
        let mut acc = Accepted {
            apply: ing.released.into_iter().filter_map(|m| self.content_of(m)).collect(),
            send: Vec::new(),
            send_full_state: ing.gained_first_key,
        };
        acc.merge(self.maintain());
        acc
    }

    /// Auth-aware receive for every data message (`Trust` / `Request` / `Epoch` /
    /// `Sealed` / `Sync` / `SignedSync`): ingest auth state, or gate content and
    /// return it to apply. On a restricted board reads are epoch-gated: only
    /// `Sealed` content is accepted, and only if we hold its epoch key.
    fn accept(&mut self, gm: MeshMsg) -> Accepted {
        match gm {
            MeshMsg::Trust(e) => {
                let was_authorized = self.authorized();
                self.add_trust(e);
                let ing = self.epochs.retry_pending(&self.trust);
                let mut acc = self.followups(ing);
                // Just approved but keyless: announce ourselves so an Admin wraps us in.
                if !was_authorized {
                    acc.send.extend(self.read_request());
                }
                acc
            }
            MeshMsg::Request(req) => {
                self.ingest_request(req);
                self.maintain()
            }
            MeshMsg::Epoch(a) => {
                let ing = self.epochs.ingest(a, &self.trust);
                self.followups(ing)
            }
            MeshMsg::Sealed(s) => {
                let mut acc = Accepted::default();
                if self.mode().seals_content() {
                    if let Ok(inner) = self.epochs.open(s) {
                        acc.apply.extend(self.content_of(inner));
                    }
                }
                acc
            }
            // Restricted: content travels sealed only — an unsealed write is dropped.
            gm @ (MeshMsg::Sync(_) | MeshMsg::SignedSync(_)) => {
                let mut acc = Accepted::default();
                if !self.mode().seals_content() {
                    acc.apply.extend(self.content_of(gm));
                }
                acc
            }
            _ => Accepted::default(),
        }
    }
}

fn local_storage() -> Option<web_sys::Storage> {
    web_sys::window().and_then(|w| w.local_storage().ok().flatten())
}

/// Store `value` as `hex(postcard(value))` under `key`.
fn store_hex_postcard<T: serde::Serialize>(key: &str, value: &T) {
    if let Some(store) = local_storage() {
        let bytes = postcard::to_allocvec(value).unwrap_or_default();
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let _ = store.set_item(key, &hex);
    }
}

/// Read a `hex(postcard(T))` value from `key`; `None` if absent or undecodable.
fn load_hex_postcard<T: serde::de::DeserializeOwned>(key: &str) -> Option<T> {
    let hex = local_storage()?.get_item(key).ok()??;
    let bytes: Vec<u8> = (0..hex.len() / 2)
        .filter_map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok())
        .collect();
    postcard::from_bytes(&bytes).ok()
}

fn trust_store_key(topic: &[u8; 32]) -> String {
    format!("{TRUST_STORE_PREFIX}{}", hex32(topic))
}

fn epoch_store_key(topic: &[u8; 32]) -> String {
    format!("{EPOCH_STORE_PREFIX}{}", hex32(topic))
}

/// Persist the converged trust log for `topic` so a claimed board — and its
/// author + grants — survives a reload.
fn persist_trust_entries(topic: &[u8; 32], entries: &[TrustEntry]) {
    store_hex_postcard(&trust_store_key(topic), &entries);
}

/// The trust log persisted for `topic`. Else, if `topic` is this browser's own
/// hosted board (`H(persisted board secret)`), a one-time migration of the
/// legacy board-unscoped log (the legacy key is removed) — it was written by
/// the board this browser hosts, so it must not land on any other board. Else empty.
fn load_trust_entries(topic: &[u8; 32]) -> Vec<TrustEntry> {
    if let Some(entries) = load_hex_postcard::<Vec<TrustEntry>>(&trust_store_key(topic)) {
        return entries;
    }
    let own_board = crate::tree_sync::persisted_board_secret()
        .is_some_and(|s| &riftpipe_core::crypto::topic_from_secret(&s) == topic);
    if own_board {
        if let Some(entries) = load_hex_postcard::<Vec<TrustEntry>>(LEGACY_TRUST_STORE_KEY) {
            persist_trust_entries(topic, &entries);
            if let Some(store) = local_storage() {
                let _ = store.remove_item(LEGACY_TRUST_STORE_KEY);
            }
            return entries;
        }
    }
    Vec::new()
}

/// Persist the accepted epoch announces for `topic` (never raw keys).
fn persist_epochs(topic: &[u8; 32], announces: &[EpochAnnounce]) {
    store_hex_postcard(&epoch_store_key(topic), &announces);
}

/// The epoch announces persisted for `topic`.
fn load_epochs(topic: &[u8; 32]) -> Vec<EpochAnnounce> {
    load_hex_postcard(&epoch_store_key(topic)).unwrap_or_default()
}

/// File-tree sync over the gossip mesh. Local pushes broadcast to everyone; received
/// messages merge via the N-peer-safe `Syncer`. A new neighbor is caught up by
/// re-broadcasting our full tree. Tracks direct neighbors + a gossiped routing map.
pub struct GossipTreeSync {
    syncer: std::rc::Rc<std::cell::RefCell<riftpipe_core::sync::Syncer>>,
    sender: GossipSender,
    cipher: std::rc::Rc<riftpipe_core::crypto::MeshCipher>,
    neighbors: std::rc::Rc<std::cell::RefCell<std::collections::BTreeSet<[u8; 32]>>>,
    routing: std::rc::Rc<std::cell::RefCell<std::collections::BTreeMap<[u8; 32], Vec<[u8; 32]>>>>,
    versions: std::rc::Rc<std::cell::RefCell<riftpipe_core::sync::VersionMonitor>>,
    /// Device-linking auth (shared with the recv task): signs pushes and gates
    /// incoming writes.
    auth: std::rc::Rc<std::cell::RefCell<Auth>>,
    _keep: MeshKeepAlive,
}

/// Surface a flagged peer: console.warn + a `riftpipe:protocol-mismatch`
/// CustomEvent on `window` (detail: `{peer, theirs, ours}` JSON string), so
/// ANY app on the library can show it — nothing app-specific here.
fn report_mismatch(p: &riftpipe_core::sync::PeerVersion) {
    let ours = riftpipe_core::sync::PROTOCOL_VERSION;
    let peer = hex32(&p.id);
    web_sys::console::warn_1(
        &format!(
            "[mesh] protocol-mismatch peer={peer} theirs={} ours={ours} — \
             sync with that peer may be incomplete",
            p.proto,
        )
        .into(),
    );
    if let Some(w) = web_sys::window() {
        let detail = format!(r#"{{"peer":"{peer}","theirs":{},"ours":{ours}}}"#, p.proto);
        let init = web_sys::CustomEventInit::new();
        init.set_detail(&wasm_bindgen::JsValue::from_str(&detail));
        if let Ok(ev) =
            web_sys::CustomEvent::new_with_event_init_dict("riftpipe:protocol-mismatch", &init)
        {
            let _ = w.dispatch_event(&ev);
        }
    }
}

impl GossipTreeSync {
    pub fn new(mesh: Mesh, on_merged: std::rc::Rc<dyn Fn(String, Vec<u8>)>, auth: Auth) -> GossipTreeSync {
        use std::cell::RefCell;
        use std::rc::Rc;
        let my_id = *mesh.my_id().as_bytes();
        let (sender, mut receiver, bootstrap, cipher, keep) = mesh.into_parts();
        let agent = format!("g{:08x}", (js_sys::Math::random() * 4_294_967_296.0) as u32);
        let syncer = Rc::new(RefCell::new(riftpipe_core::sync::Syncer::new(agent)));
        let neighbors = Rc::new(RefCell::new(std::collections::BTreeSet::new()));
        let routing = Rc::new(RefCell::new(std::collections::BTreeMap::new()));
        let versions = Rc::new(RefCell::new(riftpipe_core::sync::VersionMonitor::new()));
        let auth = Rc::new(RefCell::new(auth));

        let (sy, nb, rt, vs, snd, cph, au) = (
            syncer.clone(),
            neighbors.clone(),
            routing.clone(),
            versions.clone(),
            sender.clone(),
            cipher.clone(),
            auth.clone(),
        );
        wasm_bindgen_futures::spawn_local(async move {
            // A failed send must not kill the session, but it must be VISIBLE —
            // a silently dropped broadcast reads as "synced". Big messages go
            // out as chunk frames (each under the 4 KiB limit), and every frame
            // is AEAD-sealed so only link-holders can read/inject.
            async fn send(
                snd: &GossipSender,
                cph: &riftpipe_core::crypto::MeshCipher,
                gm: &MeshMsg,
                neighbors_only: bool,
            ) {
                for b in riftpipe_core::sync::encode_mesh_frames(gm) {
                    let sealed = cph.seal(&b);
                    let n = sealed.len();
                    let r = if neighbors_only {
                        snd.broadcast_neighbors(sealed.into()).await
                    } else {
                        snd.broadcast(sealed.into()).await
                    };
                    if let Err(e) = r {
                        web_sys::console::warn_1(
                            &format!("[mesh] broadcast of {n} bytes failed: {e}").into(),
                        );
                    }
                }
            }
            // Mirror the library's authoritative neighbor set into the shared
            // cell (for the `connectedPeers()` debug accessor + Presence).
            let snapshot = |receiver: &GossipReceiver| -> Vec<[u8; 32]> {
                receiver.neighbors().map(|n| *n.as_bytes()).collect()
            };
            // Reassembles incoming chunk frames into whole messages.
            let mut assembler = riftpipe_core::sync::MeshAssembler::new();
            while let Some(Ok(ev)) = receiver.next().await {
                match ev {
                    Event::Received(m) => {
                        // Open the sealed frame (an outsider's or a corrupt frame
                        // won't decrypt — drop it). Then plain frames pass through
                        // and chunk frames buffer until whole.
                        let Some(plain) = cph.open(&m.content) else { continue };
                        let Some(gm) = assembler.feed(&plain) else { continue };
                        match gm {
                            MeshMsg::Presence { id, neighbors } => {
                                if id != my_id {
                                    let now = js_sys::Date::now() as u64;
                                    if let Some(p) = vs.borrow_mut().on_presence(id, now) {
                                        report_mismatch(&p);
                                    }
                                }
                                rt.borrow_mut().insert(id, neighbors);
                            }
                            MeshMsg::Version { id, proto } => {
                                if id != my_id {
                                    if let Some(p) = vs.borrow_mut().on_version(id, proto) {
                                        report_mismatch(&p);
                                    }
                                }
                            }
                            // feed() never yields a Chunk (it refuses nesting).
                            MeshMsg::Chunk { .. } => {}
                            // Data messages (Trust / Request / Epoch / Sealed /
                            // Sync / SignedSync) — auth decides: auth state is
                            // ingested; a write is opened (if sealed), verified
                            // and, if accepted, applied. Whatever auth wants
                            // broadcast goes to the whole swarm; replies are
                            // signed (and sealed) on the way back out. No borrow
                            // is held across an `.await`.
                            other => {
                                let acc = au.borrow_mut().accept(other);
                                for m in &acc.send {
                                    send(&snd, &cph, m, false).await;
                                }
                                for msg in acc.apply {
                                    let (persist, replies) = sy.borrow_mut().apply(msg);
                                    if let Some((path, bytes)) = persist {
                                        on_merged(path, bytes);
                                    }
                                    for r in replies {
                                        let mm = au.borrow().sign(r);
                                        if let Some(mm) = mm {
                                            send(&snd, &cph, &mm, false).await;
                                        }
                                    }
                                }
                                if acc.send_full_state {
                                    let full = sy.borrow().full_state();
                                    for msg in full {
                                        let mm = au.borrow().sign(msg);
                                        if let Some(mm) = mm {
                                            send(&snd, &cph, &mm, false).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Event::NeighborUp(_) => {
                        // Announce our protocol version BEFORE the Presence —
                        // that ordering is what lets peers deduce "Presence
                        // but no Version = pre-versioning peer".
                        let ours = riftpipe_core::sync::PROTOCOL_VERSION;
                        send(&snd, &cph, &MeshMsg::Version { id: my_id, proto: ours }, false).await;
                        // Catch the new neighbor up: the trust log first (so it
                        // learns the authorized set — gossip is ephemeral), then
                        // the epoch announces, then our whole (signed, sealed)
                        // tree. Direct neighbors only — except our read request
                        // (approved but keyless), which goes to the whole swarm
                        // since the admin may not be a direct neighbor.
                        let catchup = au.borrow().catchup();
                        for m in catchup {
                            send(&snd, &cph, &m, true).await;
                        }
                        let rr = au.borrow().read_request();
                        if let Some(r) = rr {
                            send(&snd, &cph, &r, false).await;
                        }
                        let full = sy.borrow().full_state();
                        for msg in full {
                            let mm = au.borrow().sign(msg);
                            if let Some(mm) = mm {
                                send(&snd, &cph, &mm, true).await;
                            }
                        }
                        let list = snapshot(&receiver);
                        *nb.borrow_mut() = list.iter().copied().collect();
                        rt.borrow_mut().insert(my_id, list.clone());
                        send(&snd, &cph, &MeshMsg::Presence { id: my_id, neighbors: list }, false).await;
                    }
                    Event::NeighborDown(_) => {
                        let ours = riftpipe_core::sync::PROTOCOL_VERSION;
                        send(&snd, &cph, &MeshMsg::Version { id: my_id, proto: ours }, false).await;
                        let list = snapshot(&receiver);
                        *nb.borrow_mut() = list.iter().copied().collect();
                        // Total neighbor loss: hyparview refills only from its
                        // passive view — it never re-dials the original
                        // bootstrap. We still hold its address (MemoryLookup),
                        // so ask the swarm to re-join it.
                        if list.is_empty() {
                            if let Some(b) = bootstrap {
                                if let Err(e) = snd.join_peers(vec![b]).await {
                                    web_sys::console::warn_1(
                                        &format!("[mesh] re-join failed: {e}").into(),
                                    );
                                }
                            }
                        }
                        rt.borrow_mut().insert(my_id, list.clone());
                        send(&snd, &cph, &MeshMsg::Presence { id: my_id, neighbors: list }, false).await;
                    }
                    Event::Lagged => {}
                }
            }
        });

        // Grace expiry for announcement-less (pre-versioning) peers: a quiet
        // mesh may only ever see ONE presence from an old peer. The loop holds
        // a Weak so it dies with the sync (no timer leak past teardown).
        let weak_vs = Rc::downgrade(&versions);
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                gloo_timers::future::TimeoutFuture::new(5_000).await;
                let Some(vs) = weak_vs.upgrade() else { break };
                for p in vs.borrow_mut().tick(js_sys::Date::now() as u64) {
                    report_mismatch(&p);
                }
            }
        });

        GossipTreeSync { syncer, sender, cipher, neighbors, routing, versions, auth, _keep: keep }
    }

    pub fn push_text(&self, path: &str, content: &str) {
        let msg = self.syncer.borrow_mut().local_text(path, content);
        if let Some(msg) = msg {
            let mm = self.auth.borrow().sign(msg);
            if let Some(mm) = mm {
                self.broadcast(mm);
            }
        }
    }

    pub fn push_lww(&self, path: &str, bytes: &[u8]) {
        let now = js_sys::Date::now() as u64;
        let msg = self.syncer.borrow_mut().local_lww(path, bytes.to_vec(), now);
        let mm = self.auth.borrow().sign(msg);
        if let Some(mm) = mm {
            self.broadcast(mm);
        }
    }

    /// Lock (claim) the board: become its author (genesis) if it has none, mint
    /// epoch 1, and broadcast both live so every peer starts enforcing. `false`
    /// if already locked.
    pub fn lock(&self) -> bool {
        // Bind first so the auth borrow is released before `dispatch` re-borrows.
        let acc = self.auth.borrow_mut().lock_as_author();
        match acc {
            Some(acc) => {
                self.dispatch(acc);
                true
            }
            None => false,
        }
    }

    /// Grant `device` write access (we must be the author/Admin), wrap it into
    /// the current epoch, and broadcast it live. `false` if we can't grant.
    pub fn grant_device(&self, device: [u8; 32]) -> bool {
        let acc = self.auth.borrow_mut().make_grant(device);
        match acc {
            Some(acc) => {
                self.dispatch(acc);
                true
            }
            None => false,
        }
    }

    /// Remove `device` (we must be the author/Admin; never the founder): a signed
    /// revoke plus a read-key rotation, broadcast live — it can't decrypt anything
    /// new. `false` if we can't revoke it.
    pub fn revoke_device(&self, device: [u8; 32]) -> bool {
        let acc = self.auth.borrow_mut().make_revoke(device);
        match acc {
            Some(acc) => {
                self.dispatch(acc);
                true
            }
            None => false,
        }
    }

    /// Ask to become a writer on a locked board — broadcast a signed join
    /// request so the owner sees it and can approve. `false` when there's
    /// nothing to ask (open board, or already authorized).
    pub fn request_access(&self) -> bool {
        match self.auth.borrow_mut().request_access() {
            Some(m) => {
                self.broadcast(m);
                true
            }
            None => false,
        }
    }

    /// Pending access requests awaiting our approval, as a JSON array of
    /// `{device, name, safety}` — what the owner's "someone asked" inbox renders.
    pub fn pending_requests(&self) -> String {
        let a = self.auth.borrow();
        let items: Vec<serde_json::Value> = a
            .pending()
            .iter()
            .map(|r| {
                serde_json::json!({
                    "device": hex32(&r.device_id),
                    "name": r.name,
                    "safety": r.safety_code(),
                })
            })
            .collect();
        serde_json::to_string(&items).unwrap_or_else(|_| "[]".into())
    }

    /// Authorized devices as a JSON array of `{device, role, founder, me}`
    /// (`role` is `"admin"` or `"member"`) — the owner's members list.
    pub fn members(&self) -> String {
        let a = self.auth.borrow();
        let founder = a.trust.founder();
        let me = a.my_id();
        let items: Vec<serde_json::Value> = a
            .members()
            .into_iter()
            .map(|(device, role)| {
                serde_json::json!({
                    "device": hex32(&device),
                    "role": match role { Role::Admin => "admin", Role::Member => "member" },
                    "founder": founder == Some(device),
                    "me": device == me,
                })
            })
            .collect();
        serde_json::to_string(&items).unwrap_or_else(|_| "[]".into())
    }

    /// JSON snapshot for the UI: `{locked, device, authorized, isAuthor, author,
    /// requested, canRead, epoch}` — `requested` is true once we've asked and are
    /// still waiting; `canRead` is false on a claimed board until this device is
    /// wrapped into the latest epoch (or after it was removed); `epoch` is the
    /// latest epoch seq we hold (0 = none).
    pub fn auth_state(&self) -> String {
        let a = self.auth.borrow();
        serde_json::json!({
            "locked": a.locked(),
            "device": hex32(&a.my_id()),
            "authorized": a.authorized(),
            "isAuthor": a.is_author(),
            "author": a.trust.founder().map(|f| hex32(&f)).unwrap_or_default(),
            "requested": a.my_request.is_some(),
            "canRead": a.can_read(),
            "epoch": a.epochs.latest_held_seq().unwrap_or(0),
        })
        .to_string()
    }

    /// Broadcast what an auth action produced: its messages, then (if asked) our
    /// full state, signed and sealed. Auth actions never yield content to apply.
    fn dispatch(&self, acc: Accepted) {
        let mut msgs = acc.send;
        if acc.send_full_state {
            let full = self.syncer.borrow().full_state();
            let a = self.auth.borrow();
            msgs.extend(full.into_iter().filter_map(|m| a.sign(m)));
        }
        self.broadcast_all(msgs);
    }

    fn broadcast(&self, gm: MeshMsg) {
        self.broadcast_all(vec![gm]);
    }

    /// Encode and seal every frame of every message, sent in order from ONE task
    /// (so e.g. a trust entry precedes the epoch announce it authorizes).
    fn broadcast_all(&self, msgs: Vec<MeshMsg>) {
        let frames: Vec<Vec<u8>> = msgs.iter().flat_map(riftpipe_core::sync::encode_mesh_frames).collect();
        let sender = self.sender.clone();
        let cipher = self.cipher.clone();
        wasm_bindgen_futures::spawn_local(async move {
            for b in frames {
                let sealed = cipher.seal(&b);
                let n = sealed.len();
                if let Err(e) = sender.broadcast(sealed.into()).await {
                    web_sys::console::warn_1(
                        &format!("[mesh] broadcast of {n} bytes failed: {e}").into(),
                    );
                }
            }
        });
    }

    /// Direct neighbors in the swarm (this peer's connected peers), as hex ids.
    pub fn peers(&self) -> Vec<String> {
        self.neighbors.borrow().iter().map(hex32).collect()
    }

    /// Protocol-version status as JSON: our version, every peer's known (or
    /// deduced-v0) version, and the mismatched subset.
    pub fn protocol_status(&self) -> String {
        let vs = self.versions.borrow();
        let peers: std::collections::BTreeMap<String, u32> =
            vs.peers().into_iter().map(|p| (hex32(&p.id), p.proto)).collect();
        let mismatched: Vec<String> = vs.mismatched().iter().map(|p| hex32(&p.id)).collect();
        serde_json::json!({
            "ours": riftpipe_core::sync::PROTOCOL_VERSION,
            "peers": peers,
            "mismatched": mismatched,
        })
        .to_string()
    }

    /// The gossiped routing map: `id -> [neighbor ids]` across the whole mesh.
    pub fn routing_map(&self) -> std::collections::BTreeMap<String, Vec<String>> {
        self.routing
            .borrow()
            .iter()
            .map(|(k, v)| (hex32(k), v.iter().map(hex32).collect()))
            .collect()
    }

    /// Leave the mesh, closing the endpoint (awaited) for a clean rebind.
    async fn close(self) {
        self._keep.close().await;
    }
}

fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

thread_local! {
    /// The active gossip tree sync, if the app is on the mesh transport.
    static GOSSIP: std::cell::RefCell<Option<GossipTreeSync>> = const { std::cell::RefCell::new(None) };
}

/// Install the active gossip sync (replacing any prior one).
pub(crate) fn set_active(bs: GossipTreeSync) {
    GOSSIP.with(|c| *c.borrow_mut() = Some(bs));
}

/// Tear down any active gossip sync, closing the endpoint (awaited) so a reconnect
/// under the same persisted identity rebinds cleanly.
pub(crate) async fn clear_active() {
    if let Some(bs) = GOSSIP.with(|c| c.borrow_mut().take()) {
        bs.close().await;
    }
}

/// Route a text push to the mesh if active; `true` if it was handled.
pub(crate) fn try_push_text(path: &str, content: &str) -> bool {
    GOSSIP.with(|c| match c.borrow().as_ref() {
        Some(bs) => {
            bs.push_text(path, content);
            true
        }
        None => false,
    })
}

/// Route a structural push to the mesh if active; `true` if it was handled.
pub(crate) fn try_push_lww(path: &str, bytes: &[u8]) -> bool {
    GOSSIP.with(|c| match c.borrow().as_ref() {
        Some(bs) => {
            bs.push_lww(path, bytes);
            true
        }
        None => false,
    })
}

/// Lock the active board (become its author) — the login/auth surface.
pub(crate) fn active_lock() -> bool {
    GOSSIP.with(|c| c.borrow().as_ref().map(|bs| bs.lock()).unwrap_or(false))
}

/// Grant a device (hex id) write access on the active board.
pub(crate) fn active_grant(device: [u8; 32]) -> bool {
    GOSSIP.with(|c| c.borrow().as_ref().map(|bs| bs.grant_device(device)).unwrap_or(false))
}

/// Remove a device (hex id) from the active board: revoke + read-key rotation.
pub(crate) fn active_revoke(device: [u8; 32]) -> bool {
    GOSSIP.with(|c| c.borrow().as_ref().map(|bs| bs.revoke_device(device)).unwrap_or(false))
}

/// Authorized devices on the active board (JSON array).
pub(crate) fn active_members() -> String {
    GOSSIP.with(|c| c.borrow().as_ref().map(|bs| bs.members()).unwrap_or_else(|| "[]".into()))
}

/// Ask for write access on the active (locked) board.
pub(crate) fn active_request_access() -> bool {
    GOSSIP.with(|c| c.borrow().as_ref().map(|bs| bs.request_access()).unwrap_or(false))
}

/// Pending access requests on the active board (JSON array).
pub(crate) fn active_pending_requests() -> String {
    GOSSIP.with(|c| c.borrow().as_ref().map(|bs| bs.pending_requests()).unwrap_or_else(|| "[]".into()))
}

/// Auth state of the active board (JSON), or a safe default when not connected.
pub(crate) fn active_auth_state() -> String {
    GOSSIP.with(|c| {
        c.borrow().as_ref().map(|bs| bs.auth_state()).unwrap_or_else(|| {
            r#"{"locked":false,"device":"","authorized":false,"isAuthor":false,"author":"","requested":false,"canRead":true,"epoch":0}"#.into()
        })
    })
}

/// Debug: this peer's direct neighbors in the mesh (hex ids), as a JSON array.
#[wasm_bindgen::prelude::wasm_bindgen(js_name = connectedPeers)]
pub fn connected_peers() -> String {
    GOSSIP.with(|c| {
        let peers = c.borrow().as_ref().map(|bs| bs.peers()).unwrap_or_default();
        serde_json::to_string(&peers).unwrap_or_else(|_| "[]".into())
    })
}

/// Protocol-version status for the active mesh, as JSON:
/// `{"ours":N,"peers":{"<hexid>":M,...},"mismatched":["<hexid>",...]}`.
/// (Apps can also just listen for the `riftpipe:protocol-mismatch`
/// CustomEvent on `window` instead of polling this.)
#[wasm_bindgen::prelude::wasm_bindgen(js_name = protocolStatus)]
pub fn protocol_status() -> String {
    GOSSIP.with(|c| {
        c.borrow()
            .as_ref()
            .map(|bs| bs.protocol_status())
            .unwrap_or_else(|| {
                format!(
                    r#"{{"ours":{},"peers":{{}},"mismatched":[]}}"#,
                    riftpipe_core::sync::PROTOCOL_VERSION,
                )
            })
    })
}

/// Debug: the gossiped routing map `id -> [neighbors]` across the mesh, as JSON.
#[wasm_bindgen::prelude::wasm_bindgen(js_name = routingMap)]
pub fn routing_map() -> String {
    GOSSIP.with(|c| {
        let map = c.borrow().as_ref().map(|bs| bs.routing_map()).unwrap_or_default();
        serde_json::to_string(&map).unwrap_or_else(|_| "{}".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    async fn sleep_ms(ms: u32) {
        gloo_timers::future::TimeoutFuture::new(ms).await;
    }

    /// `MeshMsg` isn't `Clone`: copy one through its wire encoding.
    fn dup(m: &MeshMsg) -> MeshMsg {
        postcard::from_bytes(&postcard::to_allocvec(m).unwrap()).unwrap()
    }

    /// Deliver every message `acc` wants broadcast to `to`, in order.
    fn deliver(acc: &Accepted, to: &mut Auth) {
        for m in &acc.send {
            to.accept(dup(m));
        }
    }

    /// Offline: the claim is persisted per board — claiming (persisting) another
    /// board doesn't overwrite it, so a reloaded owner's board stays locked
    /// (sealed) even with nobody else online. The legacy board-unscoped log only
    /// migrates into this browser's own hosted board.
    #[wasm_bindgen_test]
    fn claim_persists_per_board() {
        let topic_a = riftpipe_core::crypto::generate_secret();
        let topic_b = riftpipe_core::crypto::generate_secret();
        let mut a = Auth::load(DeviceKey::from_seed(&[31; 32]), topic_a);
        a.lock_as_author().expect("A claims board A");
        let mut b = Auth::load(DeviceKey::from_seed(&[32; 32]), topic_b);
        assert!(!b.locked(), "board A's claim doesn't leak onto board B");
        b.lock_as_author().expect("B claims board B");

        let a2 = Auth::load(DeviceKey::from_seed(&[31; 32]), topic_a);
        assert!(a2.locked() && a2.is_author(), "board A is still claimed after board B persisted");
        assert!(a2.can_read() && a2.epochs.latest_held_seq() == Some(1), "A re-opens its epoch key");

        // Legacy log: left alone on a foreign board, migrated into our own.
        let store = local_storage().expect("localStorage");
        let legacy = vec![DeviceKey::from_seed(&[33; 32]).genesis()];
        store_hex_postcard(LEGACY_TRUST_STORE_KEY, &legacy);
        let foreign = Auth::load(DeviceKey::from_seed(&[34; 32]), riftpipe_core::crypto::generate_secret());
        assert!(!foreign.locked(), "legacy log must not migrate into a board we don't host");
        assert!(store.get_item(LEGACY_TRUST_STORE_KEY).unwrap().is_some(), "legacy log kept");

        let own_secret = riftpipe_core::crypto::generate_secret();
        let prior_secret = store.get_item("riftpipe:board_secret").unwrap();
        crate::tree_sync::persist_board_secret(&own_secret);
        let own = Auth::load(DeviceKey::from_seed(&[34; 32]), riftpipe_core::crypto::topic_from_secret(&own_secret));
        assert!(own.locked(), "legacy log migrates into our own hosted board");
        assert!(store.get_item(LEGACY_TRUST_STORE_KEY).unwrap().is_none(), "legacy key removed after migration");
        match prior_secret {
            Some(s) => store.set_item("riftpipe:board_secret", &s).unwrap(),
            None => store.remove_item("riftpipe:board_secret").unwrap(),
        }
    }

    /// Offline: a claimed board seals content under an epoch key that only
    /// approved devices hold, and removing a device rotates it out — it can't
    /// open anything sealed afterwards while the remaining member still can.
    #[wasm_bindgen_test]
    fn claimed_board_seals_and_revoke_rotates() {
        let topic = riftpipe_core::crypto::generate_secret();
        let mut a = Auth::load(DeviceKey::from_seed(&[11; 32]), topic);
        let acc = a.lock_as_author().expect("unclaimed board locks");
        assert_eq!(a.epochs.latest_held_seq(), Some(1), "claiming mints epoch 1");

        let mut b = Auth::load(DeviceKey::from_seed(&[12; 32]), topic);
        let mut c = Auth::load(DeviceKey::from_seed(&[13; 32]), topic);
        let (b_id, c_id) = (b.my_id(), c.my_id());
        deliver(&acc, &mut b);
        deliver(&acc, &mut c);
        assert!(b.locked() && !b.can_read(), "an un-approved device can't read a claimed board");

        a.accept(b.request_access().expect("b asks"));
        a.accept(c.request_access().expect("c asks"));
        let gb = a.make_grant(b_id).expect("owner grants b");
        deliver(&gb, &mut b);
        deliver(&gb, &mut c);
        let gc = a.make_grant(c_id).expect("owner grants c");
        deliver(&gc, &mut b);
        deliver(&gc, &mut c);
        assert!(b.can_read() && c.can_read(), "approved devices are wrapped into the epoch");

        let lww = |path: &str, bytes: &[u8]| SyncMsg::Lww { path: path.into(), version: 1, bytes: bytes.to_vec() };
        let s = a.sign(lww("x", b"1")).expect("owner seals");
        assert!(matches!(s, MeshMsg::Sealed(_)), "claimed-board content is sealed");
        assert_eq!(b.accept(dup(&s)).apply.len(), 1, "a member opens sealed content");

        let r = a.make_revoke(c_id).expect("owner removes c");
        deliver(&r, &mut b);
        deliver(&r, &mut c);
        assert!(!c.can_read(), "the removed device lost read");
        assert!(b.can_read(), "the remaining member still reads");
        assert_eq!(a.epochs.latest_held_seq(), Some(2), "removal rotated the epoch");

        let s2 = a.sign(lww("y", b"2")).expect("owner seals under the new epoch");
        assert_eq!(b.accept(dup(&s2)).apply.len(), 1, "remaining member opens new content");
        assert!(c.accept(dup(&s2)).apply.is_empty(), "removed device can't open new content");
    }

    /// Two browser peers join the same gossip topic (one bootstraps off the other)
    /// and a broadcast reaches the other — proves the mesh transport works in-browser
    /// over n0's relay. Needs network.
    #[wasm_bindgen_test]
    async fn gossip_roundtrip_two_peers() {
        // A shared board secret (would ride the share link) derives the topic +
        // frame cipher for both peers.
        let secret = riftpipe_core::crypto::generate_secret();
        let host_sk = SecretKey::generate();
        let mut host = Mesh::join(host_sk, secret, None).await.expect("host join");
        for _ in 0..200 {
            if !host.addr().addrs.is_empty() {
                break;
            }
            sleep_ms(50).await;
        }

        let join_sk = SecretKey::generate();
        let mut joiner = Mesh::join(join_sk, secret, Some(host.addr()))
            .await
            .expect("joiner join");

        // Joiner broadcasts once it sees a neighbor; host must receive it.
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(ev) = joiner.next().await {
                if let Event::NeighborUp(_) = ev {
                    let _ = joiner.broadcast(b"ping-over-gossip".to_vec()).await;
                }
            }
        });

        let mut got = None;
        for _ in 0..400 {
            if let Some(Event::Received(m)) = host.next().await {
                got = Some(m.content.to_vec());
                break;
            }
        }
        assert_eq!(
            got.as_deref(),
            Some(&b"ping-over-gossip"[..]),
            "host received the gossip broadcast",
        );
    }

    /// A file pushed on one peer syncs to another over the gossip mesh — proves
    /// tree sync (not just raw bytes) works over gossip.
    #[wasm_bindgen_test]
    async fn tree_syncs_over_gossip_mesh() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let secret = riftpipe_core::crypto::generate_secret();
        let host_sk = SecretKey::generate();
        let host_mesh = Mesh::join(host_sk, secret, None).await.expect("host");
        for _ in 0..200 {
            if !host_mesh.addr().addrs.is_empty() {
                break;
            }
            sleep_ms(50).await;
        }
        let host_addr = host_mesh.addr();
        let host_bs = GossipTreeSync::new(host_mesh, Rc::new(|_, _| {}), crate::gossip::Auth::load(riftpipe_core::identity::DeviceKey::from_seed(&[0u8; 32]), riftpipe_core::crypto::topic_from_secret(&secret)));

        let join_sk = SecretKey::generate();
        let joiner_mesh = Mesh::join(join_sk, secret, Some(host_addr)).await.expect("joiner");
        let got: Rc<RefCell<Vec<(String, Vec<u8>)>>> = Rc::new(RefCell::new(Vec::new()));
        let g = got.clone();
        let _joiner_bs =
            GossipTreeSync::new(joiner_mesh, Rc::new(move |p, b| g.borrow_mut().push((p, b))), crate::gossip::Auth::load(riftpipe_core::identity::DeviceKey::from_seed(&[0u8; 32]), riftpipe_core::crypto::topic_from_secret(&secret)));

        // Let the swarm form, then the host pushes a file.
        sleep_ms(2500).await;
        host_bs.push_text("notes/x/doc.md", "# gossip doc\n");

        for _ in 0..300 {
            if got.borrow().iter().any(|(p, _)| p == "notes/x/doc.md") {
                break;
            }
            sleep_ms(50).await;
        }
        assert!(
            got.borrow()
                .iter()
                .any(|(p, b)| p == "notes/x/doc.md" && b == b"# gossip doc\n"),
            "joiner received the file over the gossip mesh: {:?}",
            got.borrow(),
        );
    }
}
