//! Browser (wasm) side of riftpipe: the **WebRTC data-plane `Link`** built on
//! `web-sys`, the wasm counterpart of the native `webrtc-rs` link in
//! `riftpipe::net::webrtc`. Same shape: a non-trickle offer/answer brokered over
//! a signaling channel (the iroh-over-WebSocket link, in the real app), then a
//! `RtcDataChannel` carrying riftpipe's framed messages.
//!
//! This crate targets `wasm32-unknown-unknown` and is **verified headlessly** via
//! `wasm-pack test --headless --chrome` — the tests below run in a real browser
//! engine (no GUI), exercising the actual browser WebRTC API the production app
//! will use. It is excluded from the native workspace.

use std::cell::RefCell;
use std::rc::Rc;

use futures_channel::oneshot;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    MessageEvent, RtcDataChannel, RtcDataChannelEvent, RtcIceGatheringState, RtcPeerConnection,
    RtcSdpType, RtcSessionDescriptionInit,
};

/// The kanban "server" running in the browser (the JSON API over OPFS).
pub mod iroh_link;
pub mod kanban;
/// Per-file sync of a board over an established WebRTC link.
pub mod board_sync;

/// One end of a WebRTC data channel, wrapped so the rest of riftpipe can treat it
/// as a byte pipe. Mirrors the native `WebrtcLink`: `send` writes to the channel,
/// inbound messages are pushed by `on_message` into a queue `recv` drains.
pub struct WebrtcLink {
    dc: RtcDataChannel,
    inbound: futures_channel::mpsc::UnboundedReceiver<Vec<u8>>,
    // Kept alive so the peer connection isn't dropped under us.
    _pc: RtcPeerConnection,
}

impl WebrtcLink {
    /// Send a framed message over the data channel.
    pub fn send(&self, msg: &[u8]) -> Result<(), JsValue> {
        self.dc.send_with_u8_array(msg)
    }

    /// Await the next inbound message (`None` once the channel is gone).
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        use futures_util::StreamExt;
        self.inbound.next().await
    }

    /// A cheap clone of the send side, so a sync loop can keep sending while the
    /// link's `recv` is owned by a spawned receive task.
    pub fn sender(&self) -> WebrtcSender {
        WebrtcSender { dc: self.dc.clone() }
    }
}

/// The send half of a [`WebrtcLink`] (just the data channel).
pub struct WebrtcSender {
    dc: RtcDataChannel,
}

impl WebrtcSender {
    pub fn send(&self, bytes: &[u8]) -> Result<(), JsValue> {
        self.dc.send_with_u8_array(bytes)
    }
}

/// Block until ICE gathering completes, so `local_description` carries every
/// candidate (non-trickle — one complete SDP each way).
pub async fn wait_ice_complete(pc: &RtcPeerConnection) {
    if pc.ice_gathering_state() == RtcIceGatheringState::Complete {
        return;
    }
    let (tx, rx) = oneshot::channel::<()>();
    let tx = Rc::new(RefCell::new(Some(tx)));
    let pc2 = pc.clone();
    let cb = Closure::<dyn FnMut()>::new(move || {
        if pc2.ice_gathering_state() == RtcIceGatheringState::Complete {
            if let Some(tx) = tx.borrow_mut().take() {
                let _ = tx.send(());
            }
        }
    });
    pc.set_onicegatheringstatechange(Some(cb.as_ref().unchecked_ref()));
    cb.forget();
    let _ = rx.await;
}

/// Attach an `on_message` handler that pushes inbound bytes into a queue, and
/// return the queue's receiver. Handles both string and binary payloads.
pub fn pipe_inbound(dc: &RtcDataChannel) -> futures_channel::mpsc::UnboundedReceiver<Vec<u8>> {
    let (tx, rx) = futures_channel::mpsc::unbounded::<Vec<u8>>();
    let on_msg = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
        let data = e.data();
        if let Ok(buf) = data.clone().dyn_into::<js_sys::ArrayBuffer>() {
            let bytes = js_sys::Uint8Array::new(&buf).to_vec();
            let _ = tx.unbounded_send(bytes);
        } else if let Some(s) = data.as_string() {
            let _ = tx.unbounded_send(s.into_bytes());
        }
    });
    dc.set_onmessage(Some(on_msg.as_ref().unchecked_ref()));
    on_msg.forget();
    rx
}

pub fn desc(kind: RtcSdpType, sdp: &str) -> RtcSessionDescriptionInit {
    let d = RtcSessionDescriptionInit::new(kind);
    d.set_sdp(sdp);
    d
}

/// Signaling messages, byte-compatible in spirit with the native `Signal`
/// (`riftpipe::net::webrtc::Signal`). Carried over whatever channel the app has
/// (the iroh link). Kept as a plain enum here; serialization lives with the app.
pub enum Signal {
    Offer(String),
    Answer(String),
}

/// JS-facing handle to a riftpipe document — the eg-walker CRDT from
/// [`riftpipe_core`], the *same* engine the native CLI runs. An app creates one
/// per shared doc, feeds it local snapshots (`edit_to`), ships `delta()` bytes to
/// peers over a [`WebrtcLink`], and `merge`s what arrives. All exchanged bytes are
/// opaque CRDT deltas; convergence is guaranteed regardless of order/duplication.
///
/// This is the "library an app can use": `import { RiftDoc } from 'riftpipe-web'`.
#[wasm_bindgen]
pub struct RiftDoc {
    inner: riftpipe_core::text::EgWalkerText,
    last_sent: Vec<usize>,
}

#[wasm_bindgen]
impl RiftDoc {
    /// Create a document for this agent (a stable per-peer id).
    #[wasm_bindgen(constructor)]
    pub fn new(agent: &str) -> RiftDoc {
        let inner = riftpipe_core::text::EgWalkerText::new(agent);
        let last_sent = inner.version();
        RiftDoc { inner, last_sent }
    }

    /// Fold a whole new text snapshot into the CRDT (snapshot diff-to-ops). The
    /// pipe philosophy: hand it the new bytes, it derives the ops.
    #[wasm_bindgen(js_name = editTo)]
    pub fn edit_to(&mut self, text: &str) {
        self.inner.edit_to(text);
    }

    /// Current materialized text.
    pub fn content(&self) -> String {
        self.inner.content()
    }

    /// Encode the ops added since the last `delta()` (or since creation) — the
    /// bytes to send a peer. Advances the "last sent" mark.
    pub fn delta(&mut self) -> Vec<u8> {
        let bytes = self.inner.encode_delta(&self.last_sent);
        self.last_sent = self.inner.version();
        bytes
    }

    /// Encode the entire history — to seed a brand-new peer.
    pub fn snapshot(&self) -> Vec<u8> {
        self.inner.encode_full()
    }

    /// Merge a peer's `delta()`/`snapshot()` bytes. Idempotent + order-independent.
    /// Returns `false` if the bytes were a delta whose ancestors we don't hold
    /// (ask the peer for a full snapshot to recover).
    pub fn merge(&mut self, bytes: &[u8]) -> bool {
        self.inner.merge(bytes)
    }

    /// Persist the whole document to OPFS under `name` — the browser's private,
    /// **serverless** filesystem. Survives reloads with no backend.
    pub async fn persist(&self, name: String) -> Result<(), JsValue> {
        opfs_write(&name, &self.inner.encode_full()).await
    }

    /// Load a document for `agent` from OPFS `name`, or a fresh empty one if it
    /// has never been persisted.
    pub async fn load(agent: String, name: String) -> Result<RiftDoc, JsValue> {
        let mut doc = RiftDoc::new(&agent);
        if let Some(bytes) = opfs_read(&name).await? {
            let _ = doc.inner.merge(&bytes);
            doc.last_sent = doc.inner.version();
        }
        Ok(doc)
    }
}

// ---------------------------------------------------------------------------
// OPFS — local, serverless persistence (Origin Private File System)
// ---------------------------------------------------------------------------

use crate::kanban::opfs_root;

/// Write `bytes` to OPFS file `name` (created if absent).
pub async fn opfs_write(name: &str, bytes: &[u8]) -> Result<(), JsValue> {
    use web_sys::{FileSystemFileHandle, FileSystemGetFileOptions, FileSystemWritableFileStream};
    let dir = opfs_root().await?;
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(true);
    let handle = wasm_bindgen_futures::JsFuture::from(dir.get_file_handle_with_options(name, &opts))
        .await?
        .unchecked_into::<FileSystemFileHandle>();
    let writable = wasm_bindgen_futures::JsFuture::from(handle.create_writable())
        .await?
        .unchecked_into::<FileSystemWritableFileStream>();
    wasm_bindgen_futures::JsFuture::from(writable.write_with_u8_array(bytes)?).await?;
    wasm_bindgen_futures::JsFuture::from(writable.close()).await?;
    Ok(())
}

/// Read OPFS file `name`, or `None` if it doesn't exist.
pub async fn opfs_read(name: &str) -> Result<Option<Vec<u8>>, JsValue> {
    use web_sys::{File, FileSystemFileHandle};
    let dir = opfs_root().await?;
    let handle = match wasm_bindgen_futures::JsFuture::from(dir.get_file_handle(name)).await {
        Ok(h) => h.unchecked_into::<FileSystemFileHandle>(),
        Err(_) => return Ok(None), // not found
    };
    let file = wasm_bindgen_futures::JsFuture::from(handle.get_file())
        .await?
        .unchecked_into::<File>();
    let buf = wasm_bindgen_futures::JsFuture::from(file.array_buffer()).await?;
    Ok(Some(js_sys::Uint8Array::new(&buf).to_vec()))
}

// ---------------------------------------------------------------------------
// Signaling — connect to a peer via a WebSocket signaling server + room id
// ---------------------------------------------------------------------------

/// The connection id from the page URL (`#<id>` or `#conn=<id>`) — the room two
/// peers share to find each other. Sharing a link == sharing the connection.
#[wasm_bindgen(js_name = connectionId)]
pub fn connection_id() -> Option<String> {
    let hash = web_sys::window()?.location().hash().ok()?;
    let id = hash.trim_start_matches('#').trim_start_matches("conn=").to_string();
    if id.is_empty() { None } else { Some(id) }
}

/// A WebSocket to the signaling server: text frames in/out, the inbound ones
/// pumped into a queue `recv` drains.
struct Signaling {
    ws: web_sys::WebSocket,
    inbound: futures_channel::mpsc::UnboundedReceiver<String>,
}

impl Signaling {
    async fn connect(url: &str) -> Result<Signaling, JsValue> {
        let ws = web_sys::WebSocket::new(url)?;
        let (open_tx, open_rx) = oneshot::channel::<bool>();
        let open_tx = Rc::new(RefCell::new(Some(open_tx)));
        let on_open = {
            let t = open_tx.clone();
            Closure::<dyn FnMut()>::new(move || {
                if let Some(tx) = t.borrow_mut().take() {
                    let _ = tx.send(true);
                }
            })
        };
        let on_err = {
            let t = open_tx.clone();
            Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
                if let Some(tx) = t.borrow_mut().take() {
                    let _ = tx.send(false);
                }
            })
        };
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        ws.set_onerror(Some(on_err.as_ref().unchecked_ref()));
        on_open.forget();
        on_err.forget();

        let (in_tx, in_rx) = futures_channel::mpsc::unbounded::<String>();
        let on_msg = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
            if let Some(s) = e.data().as_string() {
                let _ = in_tx.unbounded_send(s);
            }
        });
        ws.set_onmessage(Some(on_msg.as_ref().unchecked_ref()));
        on_msg.forget();

        match open_rx.await {
            Ok(true) => Ok(Signaling { ws, inbound: in_rx }),
            _ => Err(JsValue::from_str("signaling websocket failed to open")),
        }
    }

    fn send(&self, s: &str) -> Result<(), JsValue> {
        self.ws.send_with_str(s)
    }

    async fn recv(&mut self) -> Option<String> {
        use futures_util::StreamExt;
        self.inbound.next().await
    }
}

fn json_field(msg: &str, expect_type: &str, field: &str) -> Option<String> {
    let v = js_sys::JSON::parse(msg).ok()?;
    let ty = js_sys::Reflect::get(&v, &"type".into()).ok()?.as_string()?;
    if ty != expect_type {
        return None;
    }
    js_sys::Reflect::get(&v, &field.into()).ok()?.as_string()
}

fn sdp_msg(sdp: &str) -> String {
    let o = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&o, &"type".into(), &"sdp".into());
    let _ = js_sys::Reflect::set(&o, &"sdp".into(), &sdp.into());
    js_sys::JSON::stringify(&o).ok().and_then(|s| s.as_string()).unwrap_or_default()
}

/// Wait for a data channel to reach the `open` state.
async fn wait_open(dc: &RtcDataChannel) {
    if dc.ready_state() == web_sys::RtcDataChannelState::Open {
        return;
    }
    let (tx, rx) = oneshot::channel::<()>();
    let tx = Rc::new(RefCell::new(Some(tx)));
    let t = tx.clone();
    let cb = Closure::<dyn FnMut()>::new(move || {
        if let Some(tx) = t.borrow_mut().take() {
            let _ = tx.send(());
        }
    });
    dc.set_onopen(Some(cb.as_ref().unchecked_ref()));
    cb.forget();
    let _ = rx.await;
}

/// Establish a [`WebrtcLink`] with the peer in our room, exchanging SDP over the
/// signaling channel (non-trickle). `we_offer` comes from the server's role msg.
/// SPIKE: bind an iroh endpoint in the browser (relay-only via n0) and return its
/// node id. Proves iroh compiles + runs in wasm before wiring it as a `Link`.
#[wasm_bindgen(js_name = irohNodeId)]
pub async fn iroh_node_id() -> Result<String, JsValue> {
    let ep = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .bind()
        .await
        .map_err(|e| JsValue::from_str(&format!("iroh bind: {e}")))?;
    Ok(ep.id().to_string())
}

struct IceCfg {
    stun_url: String,
    turn_url: String,
    user: String,
    pass: String,
    relay: bool,
}

thread_local! {
    static ICE_CFG: RefCell<Option<IceCfg>> = const { RefCell::new(None) };
}

/// Configure ICE for subsequent connections: a STUN server (for cross-network
/// hole-punching), an optional TURN server (relay fallback), and optional
/// relay-only policy. Any field may be empty. Call before `connectAndSync`.
#[wasm_bindgen(js_name = configureIce)]
pub fn configure_ice(
    stun_url: String,
    turn_url: String,
    username: String,
    credential: String,
    force_relay: bool,
) {
    ICE_CFG.with(|c| {
        *c.borrow_mut() = Some(IceCfg {
            stun_url,
            turn_url,
            user: username,
            pass: credential,
            relay: force_relay,
        });
    });
}

/// Build a peer connection, applying any configured STUN/TURN + relay policy.
fn new_pc() -> Result<RtcPeerConnection, JsValue> {
    ICE_CFG.with(|c| match c.borrow().as_ref() {
        Some(cfg) => {
            let config = web_sys::RtcConfiguration::new();
            let servers = js_sys::Array::new();
            if !cfg.stun_url.is_empty() {
                let s = web_sys::RtcIceServer::new();
                s.set_urls(&JsValue::from_str(&cfg.stun_url));
                servers.push(&s);
            }
            if !cfg.turn_url.is_empty() {
                let s = web_sys::RtcIceServer::new();
                s.set_urls(&JsValue::from_str(&cfg.turn_url));
                s.set_username(&cfg.user);
                s.set_credential(&cfg.pass);
                servers.push(&s);
            }
            if servers.length() > 0 {
                config.set_ice_servers(&servers);
            }
            if cfg.relay {
                config.set_ice_transport_policy(web_sys::RtcIceTransportPolicy::Relay);
            }
            RtcPeerConnection::new_with_configuration(&config)
        }
        None => RtcPeerConnection::new(),
    })
}

async fn establish_over_signaling(we_offer: bool, sig: &mut Signaling) -> Result<WebrtcLink, JsValue> {
    let pc = new_pc()?;
    if we_offer {
        let dc = pc.create_data_channel("riftpipe");
        let inbound = pipe_inbound(&dc);
        let offer = JsFuture::from(pc.create_offer()).await?.unchecked_into::<RtcSessionDescriptionInit>();
        JsFuture::from(pc.set_local_description(&offer)).await?;
        wait_ice_complete(&pc).await;
        let sdp = pc.local_description().ok_or_else(|| JsValue::from_str("no local sdp"))?.sdp();
        sig.send(&sdp_msg(&sdp))?;
        let answer = recv_sdp(sig).await?;
        JsFuture::from(pc.set_remote_description(&desc(RtcSdpType::Answer, &answer))).await?;
        wait_open(&dc).await;
        Ok(WebrtcLink { dc, inbound, _pc: pc })
    } else {
        // Attach the inbound pump INSIDE `ondatachannel`, before any await — a
        // channel can open and the offerer can send during the await gap, and
        // RtcDataChannel does not buffer messages for a late `onmessage` listener.
        type Inbound = futures_channel::mpsc::UnboundedReceiver<Vec<u8>>;
        let (dc_tx, dc_rx) = oneshot::channel::<(RtcDataChannel, Inbound)>();
        let dc_tx = Rc::new(RefCell::new(Some(dc_tx)));
        let on_dc = {
            let dc_tx = dc_tx.clone();
            Closure::<dyn FnMut(RtcDataChannelEvent)>::new(move |ev: RtcDataChannelEvent| {
                let dc = ev.channel();
                let inbound = pipe_inbound(&dc);
                if let Some(tx) = dc_tx.borrow_mut().take() {
                    let _ = tx.send((dc, inbound));
                }
            })
        };
        pc.set_ondatachannel(Some(on_dc.as_ref().unchecked_ref()));
        on_dc.forget();

        let offer = recv_sdp(sig).await?;
        JsFuture::from(pc.set_remote_description(&desc(RtcSdpType::Offer, &offer))).await?;
        let answer = JsFuture::from(pc.create_answer()).await?.unchecked_into::<RtcSessionDescriptionInit>();
        JsFuture::from(pc.set_local_description(&answer)).await?;
        wait_ice_complete(&pc).await;
        let sdp = pc.local_description().ok_or_else(|| JsValue::from_str("no local sdp"))?.sdp();
        sig.send(&sdp_msg(&sdp))?;

        let (dc, inbound) = dc_rx.await.map_err(|_| JsValue::from_str("no data channel"))?;
        wait_open(&dc).await;
        Ok(WebrtcLink { dc, inbound, _pc: pc })
    }
}

async fn recv_sdp(sig: &mut Signaling) -> Result<String, JsValue> {
    loop {
        match sig.recv().await {
            Some(m) => {
                if let Some(sdp) = json_field(&m, "sdp", "sdp") {
                    return Ok(sdp);
                }
                if json_field(&m, "peer-left", "type").is_some() || m.contains("peer-left") {
                    return Err(JsValue::from_str("peer left during signaling"));
                }
            }
            None => return Err(JsValue::from_str("signaling closed")),
        }
    }
}

/// Connect to the peer sharing `room` via the signaling server at `ws_url`, and
/// return the established WebRTC link. The signaling server pairs the room and
/// relays the offer/answer; data then flows **direct**, peer-to-peer.
pub async fn connect_via_signaling(ws_url: &str, room: &str) -> Result<WebrtcLink, JsValue> {
    // `/` before the query (browsers normalize this, but be explicit/consistent).
    let url = format!("{}/?room={room}", ws_url.trim_end_matches('/'));
    let mut sig = Signaling::connect(&url).await?;
    let role = loop {
        match sig.recv().await {
            Some(m) => {
                if let Some(r) = json_field(&m, "role", "role") {
                    break r;
                }
                if m.contains("room full") {
                    return Err(JsValue::from_str("room is full"));
                }
            }
            None => return Err(JsValue::from_str("signaling closed before role assignment")),
        }
    };
    establish_over_signaling(role == "offerer", &mut sig).await
}

/// Connect via signaling, send `payload`, await one message, return it as text.
/// Used by the browser↔native cross-stack e2e test.
#[wasm_bindgen(js_name = webrtcEcho)]
pub async fn webrtc_echo(ws_url: String, room: String, payload: String) -> Result<String, JsValue> {
    let mut link = connect_via_signaling(&ws_url, &room).await?;
    link.send(payload.as_bytes())?;
    let got = link.recv().await.ok_or_else(|| JsValue::from_str("no message received"))?;
    Ok(String::from_utf8_lossy(&got).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use riftpipe_core::text::EgWalkerText;
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::*;
    use web_sys::RtcDataChannelEvent;

    wasm_bindgen_test_configure!(run_in_browser);

    // Iroh browser↔browser sync is validated end-to-end in `run-iroh.sh` (two real
    // browser contexts over n0's relay) — a network test that belongs in the e2e
    // harness, not this fast loopback unit suite. The protocol/merge logic is
    // covered by `riftpipe_core::sync` unit tests.

    /// A persisted iroh identity is stable across "reloads" (a second call reads the
    /// stored key), so a host's shareable ticket survives a page reload. No network.
    #[wasm_bindgen_test]
    fn persisted_iroh_identity_is_stable() {
        let store = web_sys::window().unwrap().local_storage().unwrap().unwrap();
        store.remove_item("riftpipe:iroh_sk").ok();
        let id1 = crate::board_sync::load_or_create_secret_key().public();
        let id2 = crate::board_sync::load_or_create_secret_key().public();
        assert_eq!(id1, id2, "second call returns the persisted identity");
    }

    /// Establish a connected pair of `WebrtcLink`s in the (headless) browser via a
    /// full non-trickle offer/answer between two `RtcPeerConnection`s. Signaling is
    /// wired directly here (in the real app it crosses the iroh link).
    async fn establish_pair() -> (WebrtcLink, WebrtcLink) {
        let pc_a = RtcPeerConnection::new().expect("pc a");
        let pc_b = RtcPeerConnection::new().expect("pc b");

        // Offerer creates the channel; watch for it to open.
        let dc_a = pc_a.create_data_channel("riftpipe");
        let a_inbound = pipe_inbound(&dc_a);
        let (a_open_tx, a_open_rx) = oneshot::channel::<()>();
        let a_open_tx = Rc::new(RefCell::new(Some(a_open_tx)));
        {
            let t = a_open_tx.clone();
            let on_open = Closure::<dyn FnMut()>::new(move || {
                if let Some(tx) = t.borrow_mut().take() {
                    let _ = tx.send(());
                }
            });
            dc_a.set_onopen(Some(on_open.as_ref().unchecked_ref()));
            on_open.forget();
        }

        // Answerer captures its channel (arrives via ondatachannel).
        let (b_dc_tx, b_dc_rx) = oneshot::channel::<RtcDataChannel>();
        let b_dc_tx = Rc::new(RefCell::new(Some(b_dc_tx)));
        {
            let b_dc_tx = b_dc_tx.clone();
            let on_dc =
                Closure::<dyn FnMut(RtcDataChannelEvent)>::new(move |ev: RtcDataChannelEvent| {
                    if let Some(tx) = b_dc_tx.borrow_mut().take() {
                        let _ = tx.send(ev.channel());
                    }
                });
            pc_b.set_ondatachannel(Some(on_dc.as_ref().unchecked_ref()));
            on_dc.forget();
        }

        // Offer (A) → answer (B) → A applies answer. Non-trickle: gather, ship SDP.
        let offer = JsFuture::from(pc_a.create_offer()).await.expect("create offer");
        let offer: RtcSessionDescriptionInit = offer.unchecked_into();
        JsFuture::from(pc_a.set_local_description(&offer)).await.expect("set local A");
        wait_ice_complete(&pc_a).await;
        let sdp_a = pc_a.local_description().expect("local desc A").sdp();

        JsFuture::from(pc_b.set_remote_description(&desc(RtcSdpType::Offer, &sdp_a)))
            .await
            .expect("set remote B");
        let answer = JsFuture::from(pc_b.create_answer()).await.expect("create answer");
        let answer: RtcSessionDescriptionInit = answer.unchecked_into();
        JsFuture::from(pc_b.set_local_description(&answer)).await.expect("set local B");
        wait_ice_complete(&pc_b).await;
        let sdp_b = pc_b.local_description().expect("local desc B").sdp();

        JsFuture::from(pc_a.set_remote_description(&desc(RtcSdpType::Answer, &sdp_b)))
            .await
            .expect("set remote A");

        a_open_rx.await.expect("A channel opens");
        let dc_b = b_dc_rx.await.expect("B channel arrives");
        let b_inbound = pipe_inbound(&dc_b);

        (
            WebrtcLink { dc: dc_a, inbound: a_inbound, _pc: pc_a },
            WebrtcLink { dc: dc_b, inbound: b_inbound, _pc: pc_b },
        )
    }

    /// The `WebrtcLink` carries opaque bytes both ways (the transport works).
    #[wasm_bindgen_test]
    async fn webrtc_link_carries_bytes() {
        let (mut link_a, mut link_b) = establish_pair().await;
        link_a.send(b"hello over wasm webrtc").expect("send A->B");
        assert_eq!(link_b.recv().await.expect("B recv"), b"hello over wasm webrtc");
        link_b.send(b"reply").expect("send B->A");
        assert_eq!(link_a.recv().await.expect("A recv"), b"reply");
    }

    /// **The milestone:** two browser peers run the *real* eg-walker CRDT
    /// (`riftpipe-core`) and **converge a document** over the wasm WebRTC link —
    /// the whole point. Concurrent edits from a shared base, deltas exchanged over
    /// the data channel, both replicas end identical and lose nothing.
    #[wasm_bindgen_test]
    async fn two_browser_peers_converge_a_document() {
        let (mut link_a, mut link_b) = establish_pair().await;

        // Seed both replicas with a shared base via a full snapshot.
        let mut a = EgWalkerText::new("alice");
        a.edit_to("shared base.\n");
        let seed = a.encode_full();
        let mut b = EgWalkerText::new("bob");
        b.merge(&seed);
        assert_eq!(b.content(), "shared base.\n");

        let base = a.version(); // both replicas are here

        // Concurrent edits, each in its own frame: A prepends, B appends.
        a.edit_to("ALICE wuz here\nshared base.\n");
        b.edit_to("shared base.\nbob waz here\n");

        // Exchange exactly the new ops over the WebRTC data channel.
        link_a.send(&a.encode_delta(&base)).expect("A sends delta");
        link_b.send(&b.encode_delta(&base)).expect("B sends delta");
        let from_b = link_a.recv().await.expect("A receives B's delta");
        let from_a = link_b.recv().await.expect("B receives A's delta");
        a.merge(&from_b);
        b.merge(&from_a);

        // Converged: identical content on both ends, both edits preserved.
        assert_eq!(a.content(), b.content(), "replicas converge over webrtc");
        let merged = a.content();
        assert!(merged.contains("ALICE wuz here"), "A's edit survived: {merged:?}");
        assert!(merged.contains("bob waz here"), "B's edit survived: {merged:?}");
    }

    /// **Serverless persistence:** a document round-trips through OPFS — authored,
    /// saved to the browser's private filesystem, then hydrated into a fresh
    /// `RiftDoc` as if the page had reloaded. No backend touched.
    #[wasm_bindgen_test]
    async fn persists_and_reloads_via_opfs() {
        let mut doc = RiftDoc::new("alice");
        doc.edit_to("first line\nsecond line\n");
        doc.persist("riftpipe-test-board.bin".to_string())
            .await
            .expect("persist to OPFS");

        // Fresh page load: a new doc that hydrates from OPFS.
        let reloaded = RiftDoc::load("alice".to_string(), "riftpipe-test-board.bin".to_string())
            .await
            .expect("load from OPFS");
        assert_eq!(reloaded.content(), "first line\nsecond line\n");

        // A never-persisted name yields an empty doc, not an error.
        let fresh = RiftDoc::load("bob".to_string(), "riftpipe-no-such-file.bin".to_string())
            .await
            .expect("missing file is ok");
        assert_eq!(fresh.content(), "");
    }

    /// **End-to-end signaling:** two browser peers connect through the *real* Rust
    /// signaling server (`riftpipe signal`, started by `test-headless.sh` on 9011),
    /// pair via a shared room/connection-id, establish WebRTC, and converge a
    /// document — the full no-local-server connection path. Requires the server;
    /// the harness starts it.
    #[wasm_bindgen_test]
    async fn two_peers_connect_via_signaling_server_and_converge() {
        let url = "ws://127.0.0.1:9011/";
        let room = "wasm-signaling-it";

        // Both peers join the same room concurrently; the server pairs them.
        let (ra, rb) = futures_util::future::join(
            connect_via_signaling(url, room),
            connect_via_signaling(url, room),
        )
        .await;
        let mut link_a = ra.expect("peer A connects via signaling");
        let mut link_b = rb.expect("peer B connects via signaling");

        // Converge a document over the established (direct) WebRTC channel.
        let mut a = EgWalkerText::new("alice");
        a.edit_to("base.\n");
        let seed = a.encode_full();
        let mut b = EgWalkerText::new("bob");
        b.merge(&seed);
        let base = a.version();

        a.edit_to("alice line\nbase.\n");
        b.edit_to("base.\nbob line\n");
        link_a.send(&a.encode_delta(&base)).expect("A sends");
        link_b.send(&b.encode_delta(&base)).expect("B sends");
        a.merge(&link_a.recv().await.expect("A recv"));
        b.merge(&link_b.recv().await.expect("B recv"));

        assert_eq!(a.content(), b.content(), "converged via signaling-brokered webrtc");
        assert!(a.content().contains("alice line") && a.content().contains("bob line"));
    }

    /// **The kanban server, in the browser:** drive the same JSON API the SolidJS
    /// UI uses — create/read/patch/comment — entirely through `kanbanHandle` over
    /// OPFS, no localhost process. Each call reloads from OPFS, so a card created
    /// in one call being visible in the next proves serverless persistence.
    #[wasm_bindgen_test]
    async fn kanban_handler_runs_in_browser_over_opfs() {
        use crate::kanban::handle;

        let created = unpack(
            handle("POST".into(), "/api/cards".into(), r#"{"title":"made in browser","column":"Doing"}"#.into()).await,
        );
        assert_eq!(created.0, 200);
        let id = json_str(&created.1, "id");
        assert!(id.starts_with("tk_"));

        // A separate call (fresh OPFS read) sees the new card.
        let board = unpack(handle("GET".into(), "/api/board".into(), String::new()).await);
        assert!(board.1.contains("made in browser"), "board: {}", board.1);

        // Patch + comment, then detail reflects both — all serverless.
        handle("PATCH".into(), format!("/api/cards/{id}"), r#"{"done":true,"description":"no server!"}"#.into()).await;
        handle("POST".into(), format!("/api/cards/{id}/comments"), r#"{"author":"Claude","text":"hello"}"#.into()).await;
        let detail = unpack(handle("GET".into(), format!("/api/cards/{id}/detail"), String::new()).await);
        assert!(detail.1.contains("no server!"), "detail: {}", detail.1);
        assert!(detail.1.contains("hello"), "detail: {}", detail.1);
    }

    fn unpack(v: JsValue) -> (u16, String) {
        let status = js_sys::Reflect::get(&v, &"status".into()).unwrap().as_f64().unwrap() as u16;
        let body = js_sys::Reflect::get(&v, &"body".into()).unwrap().as_string().unwrap();
        (status, body)
    }

    fn json_str(json: &str, key: &str) -> String {
        let v = js_sys::JSON::parse(json).unwrap();
        js_sys::Reflect::get(&v, &key.into()).unwrap().as_string().unwrap()
    }

    /// Yield to the event loop for `ms` so spawned recv tasks can run.
    async fn sleep(ms: i32) {
        let p = js_sys::Promise::new(&mut |resolve, _| {
            web_sys::window()
                .unwrap()
                .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, ms)
                .unwrap();
        });
        let _ = JsFuture::from(p).await;
    }

    /// **Collaboration in the browser:** two `BoardSync`s, connected through the
    /// real signaling server + WebRTC, converge per-file board state — a text file
    /// (CRDT, concurrent edits both survive) and a structural file (LWW). This is
    /// the layer that makes the browser kanban actually *sync*, not just run.
    #[wasm_bindgen_test]
    async fn two_board_syncs_collaborate_over_the_link() {
        use crate::board_sync::BoardSync;
        use std::cell::RefCell;
        use std::collections::HashMap;
        use std::rc::Rc;

        let url = "ws://127.0.0.1:9011/";
        let room = "boardsync-it";
        let (la, lb) = futures_util::future::join(
            connect_via_signaling(url, room),
            connect_via_signaling(url, room),
        )
        .await;
        let (la, lb) = (la.expect("A connects"), lb.expect("B connects"));

        let got_a = Rc::new(RefCell::new(HashMap::<String, Vec<u8>>::new()));
        let got_b = Rc::new(RefCell::new(HashMap::<String, Vec<u8>>::new()));
        let (ga, gb) = (got_a.clone(), got_b.clone());
        let sync_a = BoardSync::new(la, Rc::new(move |p, b| { ga.borrow_mut().insert(p, b); }));
        let sync_b = BoardSync::new(lb, Rc::new(move |p, b| { gb.borrow_mut().insert(p, b); }));

        // A creates a card's prose; B receives it (CRDT) — and a structural move (LWW).
        sync_a.push_text("tickets/x/card.md", "# Hello\n\nfrom A\n");
        sync_a.push_lww("tickets/x/meta.toml", b"column = \"Doing\"\nposition = 0\ndone = false\n");

        let key = "tickets/x/card.md".to_string();
        for _ in 0..100 {
            if got_b.borrow().contains_key(&key) { break; }
            sleep(20).await;
        }
        assert_eq!(
            got_b.borrow().get(&key).map(|b| String::from_utf8_lossy(b).into_owned()),
            Some("# Hello\n\nfrom A\n".to_string()),
            "B received A's card prose over the link",
        );
        assert!(
            got_b.borrow().get("tickets/x/meta.toml").map(|b| String::from_utf8_lossy(b).contains("Doing")).unwrap_or(false),
            "B received A's structural move (LWW)",
        );

        // Establish a SHARED board.md (A creates → B receives) so later edits share
        // an origin and merge as a CRDT. (Two INDEPENDENTLY-created board.md are a
        // separate, origin-resolved case — see core::sync tests.)
        sync_a.push_text("board.md", "# Board\n\n- Todo\n");
        for _ in 0..100 {
            if got_b.borrow().contains_key("board.md") { break; }
            sleep(20).await;
        }
        // Concurrent edits on the now-shared doc converge on both peers, keeping both.
        sync_a.push_text("board.md", "# Board\n\n- Todo\n- A\n");
        sync_b.push_text("board.md", "# Board\n\n- Todo\n- Bee\n");
        let mut a_board = None;
        let mut b_board = None;
        for _ in 0..200 {
            a_board = got_a.borrow().get("board.md").map(|b| String::from_utf8_lossy(b).into_owned());
            b_board = got_b.borrow().get("board.md").map(|b| String::from_utf8_lossy(b).into_owned());
            let both = |s: &Option<String>| matches!(s, Some(x) if x.contains("- A") && x.contains("- Bee"));
            if both(&a_board) && both(&b_board) && a_board == b_board { break; }
            sleep(20).await;
        }
        assert_eq!(a_board, b_board, "concurrent board.md edits converge on both peers");
        assert!(
            matches!(&a_board, Some(x) if x.contains("- A") && x.contains("- Bee")),
            "both concurrent edits survive: {a_board:?}",
        );
    }
}
