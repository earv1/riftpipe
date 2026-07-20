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
    SYNC.with(|c| *c.borrow_mut() = Some(TreeSync::new(link, opfs_on_merged(on_change))));
    wasm_bindgen_futures::spawn_local(crate::opfs::prime_all());
    Ok(())
}

/// A merge handler that lands the file in OPFS and nudges the UI to refetch.
fn opfs_on_merged(on_change: js_sys::Function) -> Rc<dyn Fn(String, Vec<u8>)> {
    let on_change = Rc::new(on_change);
    Rc::new(move |path: String, bytes: Vec<u8>| {
        let cb = on_change.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = crate::opfs::write_path(&path, &bytes).await;
            let _ = cb.call0(&JsValue::NULL);
        });
    })
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
    let on_merged = opfs_on_merged(on_change);
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

    let (mesh, my_ticket) = if is_host {
        // Our own link is authoritative for the secret if present (survives a
        // localStorage wipe that kept the URL); else persist-or-mint one.
        let secret = match &parsed {
            Some((s, _)) => *s,
            None => load_or_create_board_secret(),
        };
        persist_board_secret(&secret);
        let mesh = Mesh::join(sk, secret, None).await.map_err(|e| JsValue::from_str(&e))?;
        let addr = mesh.wait_for_addr().await;
        (mesh, Some(ticket_of(&secret, &addr)))
    } else {
        // Joiner: the link carries everything — the secret and the host to dial.
        let (secret, addr) = parsed.unwrap();
        let mesh = Mesh::join(sk, secret, Some(addr)).await.map_err(|e| JsValue::from_str(&e))?;
        (mesh, None)
    };

    // Device-linking auth (open by default; `configureAuth` opts into
    // restricted). A restricted founder self-signs genesis + grants the
    // configured device ids; a restricted joiner just signs its writes and
    // learns the trust log from the founder's catch-up.
    let auth = build_auth(is_host);
    crate::gossip::set_active(GossipTreeSync::new(mesh, on_merged, auth));
    wasm_bindgen_futures::spawn_local(crate::opfs::prime_all());

    Ok(my_ticket.map(|t| JsValue::from_str(&t)).unwrap_or(JsValue::NULL))
}

const AUTH_STORE_KEY: &str = "riftpipe:auth";

/// Read the persisted auth config: `Some((true, grant_ids))` for restricted
/// mode, else `None` (open). Stored as `"1|<hexid>,<hexid>,…"` so it survives a
/// reload and the app's first connect picks it up (no racy in-tab reconnect).
fn read_auth_config() -> Option<(bool, Vec<[u8; 32]>)> {
    let store = web_sys::window().and_then(|w| w.local_storage().ok().flatten())?;
    let v = store.get_item(AUTH_STORE_KEY).ok().flatten()?;
    let rest = v.strip_prefix("1|")?;
    let grants = rest.split(',').filter(|s| !s.is_empty()).filter_map(hex32).collect();
    Some((true, grants))
}

/// Build the [`Auth`](crate::gossip::Auth) for this session from the persisted
/// config. Open mode unless `configureAuth(true, …)` was called first.
fn build_auth(is_host: bool) -> crate::gossip::Auth {
    use riftpipe_core::identity::Role;
    match read_auth_config() {
        Some((true, grants)) => {
            let key = load_or_create_device();
            // Build the founder's trust entries while we still hold the key,
            // then move it into the Auth (DeviceKey isn't Clone).
            let mut entries = Vec::new();
            if is_host {
                entries.push(key.genesis());
                for gid in grants {
                    entries.push(key.grant(gid, Role::Member, rand_nonce()));
                }
            }
            let mut auth = crate::gossip::Auth::restricted(key);
            for e in entries {
                auth.add_trust(e);
            }
            auth
        }
        _ => crate::gossip::Auth::open(),
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

/// 16 random bytes for a trust-entry nonce (reuses core's CSPRNG).
fn rand_nonce() -> [u8; 16] {
    let mut n = [0u8; 16];
    n.copy_from_slice(&riftpipe_core::crypto::generate_secret()[..16]);
    n
}

/// This browser's device id (hex), creating + persisting the identity if needed.
/// Share it so a founder can authorize this device (`configureAuth` grants).
#[wasm_bindgen(js_name = deviceId)]
pub fn device_id() -> String {
    load_or_create_device().id().iter().map(|b| format!("{b:02x}")).collect()
}

/// Persist device-linking **restricted** mode for this board: sign our writes
/// and (as a founder) authorize `grants` — a JS array of hex device ids. Takes
/// effect on the next connect / reload (stored in localStorage so it survives a
/// fresh load rather than a racy in-tab reconnect). `restricted=false` clears it
/// back to open mode.
#[wasm_bindgen(js_name = configureAuth)]
pub fn configure_auth(restricted: bool, grants: JsValue) {
    let Some(store) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) else {
        return;
    };
    if !restricted {
        let _ = store.remove_item(AUTH_STORE_KEY);
        return;
    }
    let ids: Vec<String> = js_sys::Array::from(&grants)
        .iter()
        .filter_map(|v| v.as_string())
        .filter(|s| hex32(s).is_some())
        .collect();
    let _ = store.set_item(AUTH_STORE_KEY, &format!("1|{}", ids.join(",")));
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
    let storage = web_sys::window().and_then(|w| w.local_storage().ok().flatten());
    if let Some(store) = &storage {
        if let Ok(Some(hex)) = store.get_item(BOARD_SECRET_KEY) {
            if let Some(bytes) = hex32(&hex) {
                return bytes;
            }
        }
    }
    let secret = riftpipe_core::crypto::generate_secret();
    persist_board_secret(&secret);
    secret
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
