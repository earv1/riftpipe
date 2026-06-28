//! Per-file sync of a board over an established WebRTC link — the layer that turns
//! "runs in a browser" into "*collaborates* in a browser".
//!
//! Each text file (`card.md`, `comments/*.md`, `board.md`) is a CRDT
//! (`riftpipe_core` eg-walker); structural files (`meta.toml`) are last-writer-
//! wins — matching the native folder-sync model and the on-disk format, so concurrent
//! edits and card moves converge instead of clobbering. It's deliberately decoupled
//! from OPFS via an `on_merged` callback: a single browser page shares one OPFS, so
//! testing sync against storage would be a confound — the callback lets the app
//! write OPFS while a test captures bytes in memory.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use riftpipe_core::text::EgWalkerText;
use serde::{Deserialize, Serialize};

use crate::{WebrtcLink, WebrtcSender};

#[derive(Serialize, Deserialize)]
enum SyncMsg {
    /// A text file's full CRDT state. Idempotent merge — snapshot-is-the-interface
    /// at the document level; v1 favours correctness over minimal deltas.
    Text { path: String, state: Vec<u8> },
    /// A whole-file last-writer-wins update (structural/meta); newest version wins.
    Lww { path: String, version: u64, bytes: Vec<u8> },
}

struct TextDoc {
    doc: EgWalkerText,
}

impl TextDoc {
    fn new(agent: &str) -> Self {
        TextDoc { doc: EgWalkerText::new(agent) }
    }
    fn edit(&mut self, content: &str) -> Vec<u8> {
        self.doc.edit_to(content); // snapshot diff-to-ops
        self.doc.encode_full()
    }
    fn merge(&mut self, bytes: &[u8]) -> String {
        self.doc.merge(bytes);
        self.doc.content()
    }
}

#[derive(Default)]
struct SyncState {
    docs: HashMap<String, TextDoc>,
    lww: HashMap<String, u64>,
}

/// Syncs a board's files over a WebRTC link. `on_merged(path, bytes)` fires for
/// every remote update (the app writes OPFS + refreshes; a test captures bytes).
pub struct BoardSync {
    sender: WebrtcSender,
    state: Rc<RefCell<SyncState>>,
    /// This peer's CRDT agent id — unique per peer (a shared agent id would corrupt
    /// diamond-types). All of this peer's text docs author under it.
    agent: String,
}

impl BoardSync {
    pub fn new(link: WebrtcLink, on_merged: Rc<dyn Fn(String, Vec<u8>)>) -> BoardSync {
        let agent = format!("p{:08x}", (js_sys::Math::random() * 4_294_967_296.0) as u32);
        let sender = link.sender();
        let state = Rc::new(RefCell::new(SyncState::default()));
        let st = state.clone();
        let agent_recv = agent.clone();
        // Pump remote updates: merge into the per-path CRDT / LWW slot, notify.
        wasm_bindgen_futures::spawn_local(async move {
            let mut link = link;
            while let Some(bytes) = link.recv().await {
                let Ok(msg) = postcard::from_bytes::<SyncMsg>(&bytes) else {
                    continue;
                };
                match msg {
                    SyncMsg::Text { path, state: full } => {
                        let content = st
                            .borrow_mut()
                            .docs
                            .entry(path.clone())
                            .or_insert_with(|| TextDoc::new(&agent_recv))
                            .merge(&full);
                        on_merged(path, content.into_bytes());
                    }
                    SyncMsg::Lww { path, version, bytes } => {
                        let accept = {
                            let mut s = st.borrow_mut();
                            let v = s.lww.entry(path.clone()).or_insert(0);
                            if version > *v {
                                *v = version;
                                true
                            } else {
                                false
                            }
                        };
                        if accept {
                            on_merged(path, bytes);
                        }
                    }
                }
            }
        });
        BoardSync { sender, state, agent }
    }

    /// Push a text file's new content (snapshot diff-to-ops under the hood).
    pub fn push_text(&self, path: &str, content: &str) {
        let state = self
            .state
            .borrow_mut()
            .docs
            .entry(path.to_string())
            .or_insert_with(|| TextDoc::new(&self.agent))
            .edit(content);
        self.send(&SyncMsg::Text { path: path.to_string(), state });
    }

    /// Push a structural file (last-writer-wins); version is a millisecond clock.
    pub fn push_lww(&self, path: &str, bytes: &[u8]) {
        let version = {
            let mut s = self.state.borrow_mut();
            let v = s.lww.entry(path.to_string()).or_insert(0);
            *v = (js_sys::Date::now() as u64).max(*v + 1);
            *v
        };
        self.send(&SyncMsg::Lww { path: path.to_string(), version, bytes: bytes.to_vec() });
    }

    fn send(&self, msg: &SyncMsg) {
        if let Ok(bytes) = postcard::to_allocvec(msg) {
            let _ = self.sender.send(&bytes);
        }
    }
}
