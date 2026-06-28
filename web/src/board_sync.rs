//! Per-file board sync over an established WebRTC link — the layer that turns
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
    /// The active board connection, if any (single-threaded wasm).
    static SYNC: RefCell<Option<BoardSync>> = RefCell::new(None);
}

/// Connect to the peer sharing `room` (the connection id) via the signaling server,
/// then sync the board over the link: a peer's merged file lands in OPFS and
/// `on_change` fires so the UI refetches. Call once; the kanban handler pushes
/// local edits automatically thereafter.
#[wasm_bindgen(js_name = connectAndSync)]
pub async fn connect_and_sync(
    ws_url: String,
    room: String,
    on_change: js_sys::Function,
) -> Result<(), JsValue> {
    let link = crate::connect_via_signaling(&ws_url, &room).await?;
    SYNC.with(|c| *c.borrow_mut() = Some(BoardSync::new(link, opfs_on_merged(on_change))));
    Ok(())
}

/// A merge handler that lands the file in OPFS and nudges the UI to refetch.
fn opfs_on_merged(on_change: js_sys::Function) -> Rc<dyn Fn(String, Vec<u8>)> {
    let on_change = Rc::new(on_change);
    Rc::new(move |path: String, bytes: Vec<u8>| {
        let cb = on_change.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = crate::kanban::write_path(&path, &bytes).await;
            let _ = cb.call0(&JsValue::NULL);
        });
    })
}

thread_local! {
    /// Keeps the iroh endpoint alive for the page's lifetime.
    static IROH_EP: RefCell<Option<iroh::Endpoint>> = const { RefCell::new(None) };
}

/// Connect + sync a board over **iroh** — no signaling server, no host you run
/// (traffic rides n0's relays). With an empty `ticket`, this peer becomes the
/// host: it binds, returns its ticket (put it in the share link), and accepts a
/// peer. With a ticket, this peer joins that host. Returns the host ticket (host)
/// or null (joiner). The kanban handler's pushes sync automatically thereafter.
#[wasm_bindgen(js_name = irohConnect)]
pub async fn iroh_connect(ticket: String, on_change: js_sys::Function) -> Result<JsValue, JsValue> {
    use crate::iroh_link::{addr_of, bind_accept, bind_connect, ticket_of, wait_for_addr, IrohLink};
    let on_merged = opfs_on_merged(on_change);

    if ticket.is_empty() {
        // Host: bind, publish ticket, accept one peer in the background.
        let ep = bind_accept().await.map_err(|e| JsValue::from_str(&e))?;
        let my_ticket = ticket_of(&wait_for_addr(&ep).await);
        let accept_ep = ep.clone();
        IROH_EP.with(|c| *c.borrow_mut() = Some(ep));
        wasm_bindgen_futures::spawn_local(async move {
            if let Ok(link) = IrohLink::accept(&accept_ep).await {
                SYNC.with(|c| *c.borrow_mut() = Some(BoardSync::over_iroh(link, on_merged)));
            }
        });
        Ok(JsValue::from_str(&my_ticket))
    } else {
        // Joiner: dial the host's ticket through the relay.
        let ep = bind_connect().await.map_err(|e| JsValue::from_str(&e))?;
        let addr = addr_of(&ticket).map_err(|e| JsValue::from_str(&e))?;
        let link = IrohLink::connect(&ep, addr).await.map_err(|e| JsValue::from_str(&e))?;
        IROH_EP.with(|c| *c.borrow_mut() = Some(ep));
        SYNC.with(|c| *c.borrow_mut() = Some(BoardSync::over_iroh(link, on_merged)));
        Ok(JsValue::NULL)
    }
}

/// Push a text file's new content to the connected peer (no-op if not connected).
pub fn push_text(path: &str, content: &str) {
    SYNC.with(|c| {
        if let Some(s) = c.borrow().as_ref() {
            s.push_text(path, content);
        }
    });
}

/// Push a structural file (LWW) to the connected peer (no-op if not connected).
pub fn push_lww(path: &str, bytes: &[u8]) {
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

/// Syncs a board's files over *either* transport. Local pushes are serialized to
/// an outbound channel that a transport-specific send loop drains; a recv loop
/// applies remote messages and fires `on_merged(path, bytes)` (the app writes OPFS
/// + refreshes; a test captures bytes). Same protocol regardless of transport.
pub struct BoardSync {
    outbound: UnboundedSender<Vec<u8>>,
    syncer: Rc<RefCell<Syncer>>,
}

impl BoardSync {
    /// Over a WebRTC link.
    pub fn new(link: WebrtcLink, on_merged: Rc<dyn Fn(String, Vec<u8>)>) -> BoardSync {
        let syncer = Rc::new(RefCell::new(Syncer::new(make_agent())));
        let (outbound, mut rx) = unbounded::<Vec<u8>>();
        let sender = link.sender();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bytes) = rx.next().await {
                let _ = sender.send(&bytes);
            }
        });
        let sy = syncer.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let mut link = link;
            while let Some(bytes) = link.recv().await {
                Self::apply_inbound(&sy, &bytes, &on_merged);
            }
        });
        BoardSync { outbound, syncer }
    }

    /// Over a relay-brokered iroh link (no signaling server, no host).
    pub fn over_iroh(
        link: crate::iroh_link::IrohLink,
        on_merged: Rc<dyn Fn(String, Vec<u8>)>,
    ) -> BoardSync {
        let syncer = Rc::new(RefCell::new(Syncer::new(make_agent())));
        let (outbound, mut rx) = unbounded::<Vec<u8>>();
        let (mut sink, mut source) = link.into_halves();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bytes) = rx.next().await {
                let _ = sink.send(&bytes).await;
            }
        });
        let sy = syncer.clone();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bytes) = source.recv().await {
                Self::apply_inbound(&sy, &bytes, &on_merged);
            }
        });
        // Kick the bi-stream open: iroh's accept_bi only returns once the dialer
        // sends data, so an empty frame establishes the link before any edit. The
        // peer's recv loop ignores it (an empty frame fails to decode).
        let _ = outbound.unbounded_send(Vec::new());
        BoardSync { outbound, syncer }
    }

    fn apply_inbound(
        syncer: &Rc<RefCell<Syncer>>,
        bytes: &[u8],
        on_merged: &Rc<dyn Fn(String, Vec<u8>)>,
    ) {
        if let Ok(msg) = postcard::from_bytes::<SyncMsg>(bytes) {
            if let Some((path, merged)) = syncer.borrow_mut().apply(msg) {
                on_merged(path, merged);
            }
        }
    }

    pub fn push_text(&self, path: &str, content: &str) {
        let msg = self.syncer.borrow_mut().local_text(path, content);
        self.send(&msg);
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
