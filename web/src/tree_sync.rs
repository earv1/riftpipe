//! Per-file tree sync over an established WebRTC link — the layer that turns
//! "runs in a browser" into "*collaborates* in a browser". The protocol + merge
//! state live in [`riftpipe_core::sync`] (shared with the native peer); this is
//! the browser binding: a WebRTC link, OPFS writes via `on_merged`, and the
//! `connectAndSync` entry the app calls.
//!
//! Decoupled from OPFS via `on_merged` (one page shares one OPFS, so a
//! storage-coupled test would be a confound).

use std::cell::RefCell;
use std::rc::Rc;

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::StreamExt;
use riftpipe_core::sync::{SyncMsg, Syncer};
use wasm_bindgen::prelude::*;

use crate::WebrtcLink;

thread_local! {
    /// The active tree connection, if any (single-threaded wasm).
    static SYNC: RefCell<Option<TreeSync>> = RefCell::new(None);
}

/// Connect to the peer sharing `room` (the connection id) via the signaling server,
/// then sync the file tree over the link: a peer's merged file lands in OPFS and
/// `on_change` fires so the UI refetches. Call once; the app's local edits are
/// pushed automatically thereafter (via [`push_text`] / [`push_lww`]).
#[wasm_bindgen(js_name = connectAndSync)]
pub async fn connect_and_sync(
    ws_url: String,
    room: String,
    on_change: js_sys::Function,
) -> Result<(), JsValue> {
    let link = crate::connect_via_signaling(&ws_url, &room).await?;
    // No topic on this transport: the tree is this page's board (its link or
    // its own hosted board), the same one pre-connect reads used.
    let board = enter_board(page_board_topic()).await;
    SYNC.with(|c| *c.borrow_mut() = Some(TreeSync::new(link, opfs_on_merged(on_change, board))));
    wasm_bindgen_futures::spawn_local(crate::opfs::prime_all(board));
    Ok(())
}

/// A merge handler that lands the file in board `board`'s OPFS tree (the board
/// the link syncs — even if the page has switched since), fires a per-path
/// `riftpipe:file-changed` event (the file API's `watch`), and nudges the UI to
/// refetch via the legacy `on_change` callback.
fn opfs_on_merged(on_change: js_sys::Function, board: [u8; 32]) -> Rc<dyn Fn(String, Vec<u8>)> {
    let on_change = Rc::new(on_change);
    Rc::new(move |path: String, bytes: Vec<u8>| {
        let cb = on_change.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = crate::opfs::write_path_in(&board, &path, &bytes).await;
            // Record the merge for history/debugging — but NOT for the event
            // files themselves, or logging a merged log would recurse forever.
            if !path.starts_with("events/") {
                crate::events::log("mesh.merge", serde_json::json!({ "path": path }));
            }
            dispatch_file_changed(&path);
            let _ = cb.call0(&JsValue::NULL);
        });
    })
}

/// Fire a `riftpipe:file-changed` CustomEvent on `window` (detail: the path), so
/// an app can `watch` the synced tree per path — nothing app-specific here.
fn dispatch_file_changed(path: &str) {
    if let Some(w) = web_sys::window() {
        let init = web_sys::CustomEventInit::new();
        init.set_detail(&JsValue::from_str(path));
        if let Ok(ev) = web_sys::CustomEvent::new_with_event_init_dict("riftpipe:file-changed", &init) {
            let _ = w.dispatch_event(&ev);
        }
    }
}

thread_local! {
    /// Keeps the iroh endpoint alive for the page's lifetime.
    static IROH_EP: RefCell<Option<iroh::Endpoint>> = const { RefCell::new(None) };
}

/// Connect + sync a file tree over **iroh** — no signaling server, no host you run
/// (traffic rides n0's relays). With an empty `ticket`, this peer becomes the
/// host: it binds, returns its ticket (put it in the share link), and accepts a
/// peer. With a ticket, this peer joins that host. Returns the host ticket (host)
/// or null (joiner). The app's pushes sync automatically thereafter.
#[wasm_bindgen(js_name = irohConnect)]
pub async fn iroh_connect(ticket: String, on_change: js_sys::Function) -> Result<JsValue, JsValue> {
    use crate::gossip::{GossipTreeSync, Mesh};
    use crate::iroh_link::{parse_ticket, ticket_of};
    let sk = load_or_create_secret_key();
    let my_id = sk.public();

    // Tear down any prior mesh session (closes the old endpoint, awaited) before
    // rebinding under the same persisted identity.
    crate::gossip::clear_active().await;

    // Parse an incoming link into (board secret, host address). We host when
    // there's no link, OR the link is our OWN (its address is us — a reloaded
    // host recognizing its share link). The board SECRET — not our endpoint id —
    // now defines the topic + frame cipher.
    let parsed = if ticket.is_empty() { None } else { parse_ticket(&ticket).ok() };
    let is_host = match &parsed {
        Some((_, addr)) => addr.id == my_id,
        None => true,
    };

    // Our own link is authoritative for the secret if present (survives a
    // localStorage wipe that kept the URL); else persist-or-mint one. A joiner's
    // link carries the secret and the host to dial.
    let secret = match &parsed {
        Some((s, _)) => *s,
        None => load_or_create_board_secret(),
    };
    if is_host {
        persist_board_secret(&secret);
    }
    // Scope the file tree to this board BEFORE joining (a failed join still
    // works solo on the right tree).
    let topic = enter_board(riftpipe_core::crypto::topic_from_secret(&secret)).await;
    let on_merged = opfs_on_merged(on_change, topic);

    let (mesh, my_ticket) = if is_host {
        let mesh = Mesh::join(sk, secret, None).await.map_err(|e| JsValue::from_str(&e))?;
        let addr = mesh.wait_for_addr().await;
        (mesh, Some(ticket_of(&secret, &addr)))
    } else {
        let addr = parsed.unwrap().1;
        let mesh = Mesh::join(sk, secret, Some(addr)).await.map_err(|e| JsValue::from_str(&e))?;
        (mesh, None)
    };

    // Device-linking auth: always carry this device's identity + the trust log
    // and epoch announces persisted for this board (its topic). The board is
    // "open" until someone locks it (adds a genesis); `lockBoard()` does that
    // live. See `web::gossip::Auth`.
    let mut auth = crate::gossip::Auth::load(load_or_create_device(), topic);
    if crate::opfs::board_claimed(&topic).await {
        auth.set_claimed_hint();
    }
    crate::gossip::set_active(GossipTreeSync::new(mesh, on_merged, auth));
    wasm_bindgen_futures::spawn_local(crate::opfs::prime_all(topic));

    Ok(my_ticket.map(|t| JsValue::from_str(&t)).unwrap_or(JsValue::NULL))
}

/// The board this page is for: its link's (`location.hash`) secret, else this
/// browser's own hosted board (minting one, as connecting would) — the same
/// choice [`iroh_connect`] makes, so the file API reads the right tree even
/// before the page connects.
pub(crate) fn page_board_topic() -> [u8; 32] {
    let hash = web_sys::window().and_then(|w| w.location().hash().ok()).unwrap_or_default();
    let ticket = hash.trim_start_matches('#');
    let secret = match crate::iroh_link::parse_ticket(ticket) {
        Ok((s, _)) if !ticket.is_empty() => s,
        _ => load_or_create_board_secret(),
    };
    riftpipe_core::crypto::topic_from_secret(&secret)
}

/// Scope the file tree to board `topic`, upgrade a pre-scoping OPFS root once,
/// and nudge the UI to re-read (the page may have read another board's tree).
async fn enter_board(topic: [u8; 32]) -> [u8; 32] {
    crate::opfs::set_active_board(topic);
    if crate::opfs::has_legacy_root().await {
        match legacy_root_decision(&topic) {
            LegacyRoot::Adopt(board) => crate::opfs::adopt_legacy_root(&board).await,
            LegacyRoot::Quarantine => crate::opfs::quarantine_legacy_root().await,
        };
    }
    dispatch_file_changed("");
    topic
}

/// What happens to a pre-scoping OPFS root (one tree shared by every board this
/// browser opened).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegacyRoot {
    /// Move it into this board's tree.
    Adopt([u8; 32]),
    /// Move it into `.legacy-root/`: kept, never synced, adopted, or wiped.
    Quarantine,
}

/// localStorage: the legacy-root decision, made once (`board:<hex>` or
/// `quarantine`) so a retry of a partial move, or a later page with a different
/// hosted board / link, never sends the tree elsewhere.
const LEGACY_ROOT_DECISION_KEY: &str = "riftpipe:legacy-root";

/// The (sticky) legacy-root decision; decided on first use with
/// [`decide_legacy_root`] and stored.
fn legacy_root_decision(opening: &[u8; 32]) -> LegacyRoot {
    let storage = web_sys::window().and_then(|w| w.local_storage().ok().flatten());
    let stored = storage.as_ref().and_then(|s| s.get_item(LEGACY_ROOT_DECISION_KEY).ok().flatten());
    if let Some(d) = stored.as_deref().and_then(parse_legacy_root) {
        return d;
    }
    let d = decide_legacy_root(
        persisted_board_secret().map(|s| riftpipe_core::crypto::topic_from_secret(&s)),
        crate::gossip::legacy_trust_founder(),
        load_or_create_device().id(),
        *opening,
    );
    if let Some(s) = &storage {
        let v = match d {
            LegacyRoot::Adopt(b) => format!("board:{}", to_hex(&b)),
            LegacyRoot::Quarantine => "quarantine".to_string(),
        };
        let _ = s.set_item(LEGACY_ROOT_DECISION_KEY, &v);
    }
    d
}

fn parse_legacy_root(v: &str) -> Option<LegacyRoot> {
    if v == "quarantine" {
        return Some(LegacyRoot::Quarantine);
    }
    v.strip_prefix("board:").and_then(hex32).map(LegacyRoot::Adopt)
}

/// Where a pre-scoping root tree may safely go. It is the union of everything
/// this browser opened, so it may hold claimed content. Adopt it only if no
/// other device's claim was ever seen here — no legacy trust log, or one this
/// device founded (that log then migrates into the same hosted board, so the
/// adopted files stay claimed):
/// - into this browser's own hosted board, if it has one;
/// - else, with no legacy log at all (all of it was open-board content), into
///   the board being opened;
/// - otherwise quarantine.
fn decide_legacy_root(
    hosted: Option<[u8; 32]>,
    legacy_founder: Option<Option<[u8; 32]>>,
    me: [u8; 32],
    opening: [u8; 32],
) -> LegacyRoot {
    let no_foreign_claim = match legacy_founder {
        None => true,
        Some(founder) => founder == Some(me),
    };
    match (hosted, legacy_founder) {
        (Some(h), _) if no_foreign_claim => LegacyRoot::Adopt(h),
        (None, None) => LegacyRoot::Adopt(opening),
        _ => LegacyRoot::Quarantine,
    }
}

/// This device's persisted signing identity (localStorage `riftpipe:device`),
/// or a freshly minted + stored one. The seed is secret; only the id is shared.
pub(crate) fn load_or_create_device() -> riftpipe_core::identity::DeviceKey {
    use riftpipe_core::identity::DeviceKey;
    const STORE_KEY: &str = "riftpipe:device";
    let storage = web_sys::window().and_then(|w| w.local_storage().ok().flatten());
    if let Some(store) = &storage {
        if let Ok(Some(hex)) = store.get_item(STORE_KEY) {
            if let Some(seed) = hex32(&hex) {
                return DeviceKey::from_seed(&seed);
            }
        }
    }
    let key = DeviceKey::generate();
    if let Some(store) = &storage {
        let _ = store.set_item(STORE_KEY, &to_hex(&key.seed()));
    }
    key
}

/// This browser's device id (hex), creating + persisting the identity if needed.
/// Share it so a board's author can authorize (grant) this device.
#[wasm_bindgen(js_name = deviceId)]
pub fn device_id() -> String {
    load_or_create_device().id().iter().map(|b| format!("{b:02x}")).collect()
}

/// **Lock this board**: become its author (the first opener locks it). Broadcasts
/// the genesis live so every peer starts enforcing. Returns `true` if it locked,
/// `false` if the board already has an author.
#[wasm_bindgen(js_name = lockBoard)]
pub fn lock_board() -> bool {
    let ok = crate::gossip::active_lock();
    if ok {
        let device = load_or_create_device().id().iter().map(|b| format!("{b:02x}")).collect::<String>();
        crate::events::log("auth.claim", serde_json::json!({ "device": device }));
    }
    ok
}

/// **Grant** a device (its hex id) write access on this board. Only the author
/// (Admin) can; broadcasts the grant live. `false` if we can't grant or the id
/// is malformed.
#[wasm_bindgen(js_name = grantDevice)]
pub fn grant_device(hex_id: String) -> bool {
    match hex32(&hex_id) {
        Some(id) => {
            let ok = crate::gossip::active_grant(id);
            if ok {
                crate::events::log("auth.grant", serde_json::json!({ "device": hex_id }));
            }
            ok
        }
        None => false,
    }
}

/// **Remove** a device (hex id): signed revoke + read-key rotation — it can't decrypt
/// anything new. Owner/Admin only; the founder can't be removed. `false` if we
/// can't remove it or the id is malformed.
#[wasm_bindgen(js_name = revokeDevice)]
pub fn revoke_device(hex_id: String) -> bool {
    match hex32(&hex_id) {
        Some(id) => {
            let ok = crate::gossip::active_revoke(id);
            if ok {
                crate::events::log("auth.revoke", serde_json::json!({ "device": hex_id }));
            }
            ok
        }
        None => false,
    }
}

/// Authorized devices as JSON `[{device, role, founder, me}]` (`role` is
/// `"admin"` or `"member"`) — the owner's members list.
#[wasm_bindgen(js_name = boardMembers)]
pub fn board_members() -> String {
    crate::gossip::active_members()
}

/// **Ask for access**: request write access on a locked board. Broadcasts a
/// signed join request so the author sees it in their approval inbox. `true` if
/// a request went out, `false` if the board is open or we're already authorized.
#[wasm_bindgen(js_name = requestAccess)]
pub fn request_access() -> bool {
    let ok = crate::gossip::active_request_access();
    if ok {
        let device = load_or_create_device().id().iter().map(|b| format!("{b:02x}")).collect::<String>();
        crate::events::log("auth.request", serde_json::json!({ "device": device }));
    }
    ok
}

/// Pending access requests awaiting this device's approval, as a JSON array of
/// `{device, name, safety}` — the author's "someone asked" inbox.
#[wasm_bindgen(js_name = pendingRequests)]
pub fn pending_requests() -> String {
    crate::gossip::active_pending_requests()
}

/// Auth state of the active board as JSON:
/// `{locked, device, authorized, isAuthor, author, requested, canRead, epoch,
/// isAdmin, passkeys, passkeyHere}` — what the login UI renders. `canRead` is
/// false on a claimed board until this device is wrapped into the latest epoch
/// (or once it was removed); `epoch` is the latest epoch seq this device holds
/// (0 = none); `passkeys` is how many registered passkeys can link a device
/// from this page's relying party (hostname; their registrant still an
/// Admin); `passkeyHere` is `passkeys > 0`.
#[wasm_bindgen(js_name = authState)]
pub fn auth_state() -> String {
    crate::gossip::active_auth_state()
}

/// Load the browser's persisted iroh identity from localStorage, or generate +
/// store one. A stable identity means a host's shareable ticket survives reloads.
pub(crate) fn load_or_create_secret_key() -> iroh::SecretKey {
    const STORE_KEY: &str = "riftpipe:iroh_sk";
    let storage = web_sys::window().and_then(|w| w.local_storage().ok().flatten());
    if let Some(store) = &storage {
        if let Ok(Some(hex)) = store.get_item(STORE_KEY) {
            if let Some(bytes) = hex32(&hex) {
                return iroh::SecretKey::from_bytes(&bytes);
            }
        }
    }
    let sk = iroh::SecretKey::generate();
    if let Some(store) = &storage {
        let _ = store.set_item(STORE_KEY, &to_hex(&sk.to_bytes()));
    }
    sk
}

/// localStorage key for the persisted board secret.
const BOARD_SECRET_KEY: &str = "riftpipe:board_secret";

/// Load the host's persisted board secret from localStorage, or mint + store
/// one. Persisting it keeps the board's topic (`H(secret)`) — and so the share
/// link — stable across reloads, exactly as [`load_or_create_secret_key`] keeps
/// the endpoint identity stable.
pub(crate) fn load_or_create_board_secret() -> [u8; 32] {
    if let Some(secret) = persisted_board_secret() {
        return secret;
    }
    let secret = riftpipe_core::crypto::generate_secret();
    persist_board_secret(&secret);
    secret
}

/// The board secret this browser hosts (persisted), if any — never mints one.
pub(crate) fn persisted_board_secret() -> Option<[u8; 32]> {
    let store = web_sys::window().and_then(|w| w.local_storage().ok().flatten())?;
    let hex = store.get_item(BOARD_SECRET_KEY).ok()??;
    hex32(&hex)
}

/// Persist `secret` as the board secret (so a reloaded host keeps the same
/// topic + link).
pub(crate) fn persist_board_secret(secret: &[u8; 32]) {
    if let Some(store) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
        let _ = store.set_item(BOARD_SECRET_KEY, &to_hex(secret));
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Push one file's new content to peers — mesh if active, else the
/// point-to-point link (no-op if neither is connected). THE app-facing entry:
/// the text-vs-LWW channel is chosen by the library's single classification
/// rule (`core::sync::is_text_path`), the same one the native CLI and
/// `prime_all` use — an app never picks a channel, so it can never diverge
/// from what a UI-less CLI peer does with the same path.
pub fn push(path: &str, bytes: &[u8]) {
    if riftpipe_core::sync::is_text_path(path) {
        push_text(path, &String::from_utf8_lossy(bytes));
    } else {
        push_lww(path, bytes);
    }
}

/// Text-channel push (mesh if active, else link). Crate-internal: callers go
/// through [`push`] so classification stays single-sourced.
pub(crate) fn push_text(path: &str, content: &str) {
    if crate::gossip::try_push_text(path, content) {
        return;
    }
    SYNC.with(|c| {
        if let Some(s) = c.borrow().as_ref() {
            s.push_text(path, content);
        }
    });
}

/// LWW-channel push (mesh if active, else link). Crate-internal: callers go
/// through [`push`] so classification stays single-sourced.
pub(crate) fn push_lww(path: &str, bytes: &[u8]) {
    if crate::gossip::try_push_lww(path, bytes) {
        return;
    }
    SYNC.with(|c| {
        if let Some(s) = c.borrow().as_ref() {
            s.push_lww(path, bytes);
        }
    });
}

/// A unique per-peer agent id (a shared id would corrupt the CRDT).
fn make_agent() -> String {
    format!("p{:08x}", (js_sys::Math::random() * 4_294_967_296.0) as u32)
}

/// Syncs a file tree over *either* transport. Local pushes are serialized to
/// an outbound channel that a transport-specific send loop drains; a recv loop
/// applies remote messages and fires `on_merged(path, bytes)` (the app writes OPFS
/// + refreshes; a test captures bytes). Same protocol regardless of transport.
pub struct TreeSync {
    outbound: UnboundedSender<Vec<u8>>,
    syncer: Rc<RefCell<Syncer>>,
}

impl TreeSync {
    /// Over a WebRTC link.
    pub fn new(link: WebrtcLink, on_merged: Rc<dyn Fn(String, Vec<u8>)>) -> TreeSync {
        let syncer = Rc::new(RefCell::new(Syncer::new(make_agent())));
        let (outbound, mut rx) = unbounded::<Vec<u8>>();
        let sender = link.sender();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bytes) = rx.next().await {
                let _ = sender.send(&bytes);
            }
        });
        let sy = syncer.clone();
        let ob = outbound.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let mut link = link;
            while let Some(bytes) = link.recv().await {
                Self::apply_inbound(&sy, &bytes, &on_merged, &ob);
            }
        });
        Self::greet(&syncer, &outbound);
        TreeSync { outbound, syncer }
    }

    /// Over a relay-brokered iroh link (no signaling server, no host).
    pub fn over_iroh(
        link: crate::iroh_link::IrohLink,
        on_merged: Rc<dyn Fn(String, Vec<u8>)>,
    ) -> TreeSync {
        let syncer = Rc::new(RefCell::new(Syncer::new(make_agent())));
        let (outbound, mut rx) = unbounded::<Vec<u8>>();
        let (mut sink, mut source) = link.into_halves();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bytes) = rx.next().await {
                let _ = sink.send(&bytes).await;
            }
        });
        let sy = syncer.clone();
        let ob = outbound.clone();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bytes) = source.recv().await {
                Self::apply_inbound(&sy, &bytes, &on_merged, &ob);
            }
        });
        Self::greet(&syncer, &outbound);
        TreeSync { outbound, syncer }
    }

    /// Send the connect handshake — advertises our version vectors so the peer
    /// replies with anything we lack. On iroh this first frame also opens the
    /// bi-stream (it replaces the old empty-frame kick).
    fn greet(syncer: &Rc<RefCell<Syncer>>, outbound: &UnboundedSender<Vec<u8>>) {
        if let Ok(bytes) = postcard::to_allocvec(&syncer.borrow().hello()) {
            let _ = outbound.unbounded_send(bytes);
        }
    }

    fn apply_inbound(
        syncer: &Rc<RefCell<Syncer>>,
        bytes: &[u8],
        on_merged: &Rc<dyn Fn(String, Vec<u8>)>,
        outbound: &UnboundedSender<Vec<u8>>,
    ) {
        let Ok(msg) = postcard::from_bytes::<SyncMsg>(bytes) else {
            return;
        };
        let (persist, replies) = syncer.borrow_mut().apply(msg);
        if let Some((path, merged)) = persist {
            on_merged(path, merged);
        }
        for reply in replies {
            if let Ok(bytes) = postcard::to_allocvec(&reply) {
                let _ = outbound.unbounded_send(bytes);
            }
        }
    }

    pub fn push_text(&self, path: &str, content: &str) {
        if let Some(msg) = self.syncer.borrow_mut().local_text(path, content) {
            self.send(&msg);
        }
    }

    pub fn push_lww(&self, path: &str, bytes: &[u8]) {
        let now = js_sys::Date::now() as u64;
        let msg = self.syncer.borrow_mut().local_lww(path, bytes.to_vec(), now);
        self.send(&msg);
    }

    fn send(&self, msg: &SyncMsg) {
        if let Ok(bytes) = postcard::to_allocvec(msg) {
            let _ = self.outbound.unbounded_send(bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    /// The legacy-root decision: adopt only where no other device's claim was
    /// ever seen; otherwise quarantine.
    #[wasm_bindgen_test]
    fn legacy_root_goes_only_where_it_is_safe() {
        let (me, other, hosted, opening) = ([1; 32], [2; 32], [3; 32], [4; 32]);
        assert_eq!(decide_legacy_root(Some(hosted), None, me, opening), LegacyRoot::Adopt(hosted));
        assert_eq!(decide_legacy_root(Some(hosted), Some(Some(me)), me, opening), LegacyRoot::Adopt(hosted));
        assert_eq!(decide_legacy_root(Some(hosted), Some(Some(other)), me, opening), LegacyRoot::Quarantine,
            "a foreign founder's claim was seen: never into our (unclaimed) hosted board");
        assert_eq!(decide_legacy_root(Some(hosted), Some(None), me, opening), LegacyRoot::Quarantine);
        assert_eq!(decide_legacy_root(None, None, me, opening), LegacyRoot::Adopt(opening), "all open content");
        assert_eq!(decide_legacy_root(None, Some(Some(me)), me, opening), LegacyRoot::Quarantine,
            "joining another board: our claimed content must not go there");
        assert_eq!(decide_legacy_root(None, Some(Some(other)), me, opening), LegacyRoot::Quarantine);
    }

    /// Decided once: the stored answer wins over whatever the state is later
    /// (e.g. a hosted secret minted after the first visit).
    #[wasm_bindgen_test]
    fn legacy_root_decision_is_sticky() {
        let store = web_sys::window().unwrap().local_storage().unwrap().unwrap();
        let prior = store.get_item(LEGACY_ROOT_DECISION_KEY).unwrap();
        store.set_item(LEGACY_ROOT_DECISION_KEY, "quarantine").unwrap();
        assert_eq!(legacy_root_decision(&[9; 32]), LegacyRoot::Quarantine);
        let b = [7u8; 32];
        store.set_item(LEGACY_ROOT_DECISION_KEY, &format!("board:{}", to_hex(&b))).unwrap();
        assert_eq!(legacy_root_decision(&[9; 32]), LegacyRoot::Adopt(b));
        match prior {
            Some(p) => store.set_item(LEGACY_ROOT_DECISION_KEY, &p).unwrap(),
            None => store.remove_item(LEGACY_ROOT_DECISION_KEY).unwrap(),
        }
    }
}
