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
    let on_change = Rc::new(on_change);
    let on_merged: Rc<dyn Fn(String, Vec<u8>)> = Rc::new(move |path: String, bytes: Vec<u8>| {
        let cb = on_change.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = crate::kanban::write_path(&path, &bytes).await; // land the merge in OPFS
            let _ = cb.call0(&JsValue::NULL); // nudge the UI to refetch
        });
    });
    SYNC.with(|c| *c.borrow_mut() = Some(BoardSync::new(link, on_merged)));
    Ok(())
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
