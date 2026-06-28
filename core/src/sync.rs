//! Platform-agnostic per-file board sync: the wire message + the merge state,
//! shared by the browser (`riftpipe-web`, over OPFS + WebRTC) and native
//! (`riftpipe`, over std::fs + WebRTC), so both ends speak ONE protocol and a
//! native peer can collaborate on a browser's board.
//!
//! Text files (`card.md`, comments, `board.md`) are eg-walker CRDTs; structural
//! files (`meta.toml`) are last-writer-wins. No I/O, no clock — callers supply
//! the bytes, persist the result, and pass a millisecond `now` for LWW versions.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::text::EgWalkerText;

#[derive(Serialize, Deserialize)]
pub enum SyncMsg {
    /// A text file's full CRDT state (idempotent merge).
    Text { path: String, state: Vec<u8> },
    /// A whole-file last-writer-wins update; newest version wins.
    Lww { path: String, version: u64, bytes: Vec<u8> },
}

/// Per-file sync state for one peer. Author under a **unique** `agent` (a shared
/// agent id would corrupt the CRDT).
pub struct Syncer {
    agent: String,
    docs: HashMap<String, EgWalkerText>,
    lww: HashMap<String, u64>,
}

impl Syncer {
    pub fn new(agent: impl Into<String>) -> Self {
        Syncer { agent: agent.into(), docs: HashMap::new(), lww: HashMap::new() }
    }

    fn doc(&mut self, path: &str) -> &mut EgWalkerText {
        let agent = &self.agent;
        self.docs.entry(path.to_string()).or_insert_with(|| EgWalkerText::new(agent))
    }

    /// Record a local text edit (snapshot diff-to-ops); returns the message to send.
    pub fn local_text(&mut self, path: &str, content: &str) -> SyncMsg {
        let doc = self.doc(path);
        doc.edit_to(content);
        SyncMsg::Text { path: path.to_string(), state: doc.encode_full() }
    }

    /// Record a local structural write; `now` is a millisecond clock. Returns the message.
    pub fn local_lww(&mut self, path: &str, bytes: Vec<u8>, now: u64) -> SyncMsg {
        let v = self.lww.entry(path.to_string()).or_insert(0);
        *v = now.max(*v + 1);
        SyncMsg::Lww { path: path.to_string(), version: *v, bytes }
    }

    /// Apply a remote message; returns `(path, bytes)` to persist, or `None` if a
    /// stale LWW update was ignored.
    pub fn apply(&mut self, msg: SyncMsg) -> Option<(String, Vec<u8>)> {
        match msg {
            SyncMsg::Text { path, state } => {
                let doc = self.doc(&path);
                doc.merge(&state);
                Some((path, doc.content().into_bytes()))
            }
            SyncMsg::Lww { path, version, bytes } => {
                let v = self.lww.entry(path.clone()).or_insert(0);
                if version > *v {
                    *v = version;
                    Some((path, bytes))
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_syncers_converge_text_and_lww() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");

        // A edits a text file; B applies it.
        let m = a.local_text("card.md", "# Hi\n\nfrom A\n");
        assert_eq!(b.apply(m).unwrap(), ("card.md".into(), b"# Hi\n\nfrom A\n".to_vec()));

        // Concurrent edits to the same file converge on both ends.
        let ma = a.local_text("board.md", "# B\n\n- Todo\n");
        let mb = b.local_text("board.md", "# B\n\n- Done\n");
        let on_b = b.apply(ma).unwrap().1;
        let on_a = a.apply(mb).unwrap().1;
        assert_eq!(on_a, on_b, "concurrent edits converge");

        // LWW: newer wins, older ignored.
        let newer = a.local_lww("meta.toml", b"new".to_vec(), 1000);
        assert_eq!(b.apply(newer).unwrap().1, b"new");
        let stale = SyncMsg::Lww { path: "meta.toml".into(), version: 1, bytes: b"old".to_vec() };
        assert!(b.apply(stale).is_none(), "stale LWW ignored");
    }
}
