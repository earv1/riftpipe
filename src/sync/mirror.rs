//! Collaborative text pipe (DESIGN.md §3, §4) — the live eg-walker demo. The
//! shared document is an `EgWalkerText`; each peer's "view" is a file (or stdin
//! snapshot). Every round a peer: (1) diffs its local snapshot into the CRDT
//! BEFORE merging remote ops — this keeps the diff base aligned (§3 moving-base
//! hazard avoided), (2) ships the ops it has added since last round, (3) merges
//! the peer's ops, (4) writes the merged text back to the view.
//!
//! Concurrent edits to the same document genuinely exercise eg-walker's
//! sequence merge — unlike the lockstep action-log game, this is the hero use.

use crate::net::{Link, Result};
use crate::crdt::text::EgWalkerText;

pub struct TextPeer {
    doc: EgWalkerText,
    /// Version of everything we've already shipped to the peer.
    last_sent: Vec<usize>,
    /// The last content we read from / wrote to the view, so we can tell a real
    /// local edit from an echo of what we just wrote.
    last_known: String,
}

impl TextPeer {
    pub fn new(name: &str) -> Self {
        let doc = EgWalkerText::new(name);
        let last_sent = doc.version();
        TextPeer {
            doc,
            last_sent,
            last_known: String::new(),
        }
    }

    /// Fold a fresh local snapshot (file contents) into the CRDT as edits.
    fn observe_local(&mut self, snapshot: &str) {
        if snapshot != self.last_known {
            self.doc.edit_to(snapshot); // §3 diff-to-ops
            self.last_known = snapshot.to_string();
        }
    }

    /// The merged text to write back to the view, if it changed.
    fn view(&mut self) -> Option<String> {
        let content = self.doc.content();
        if content != self.last_known {
            self.last_known = content.clone();
            Some(content)
        } else {
            None
        }
    }

    pub fn content(&self) -> String {
        self.doc.content()
    }

    /// One lockstep round. Returns the merged content if it changed (write it
    /// back to the view). Order matters: diff local edits in BEFORE merging
    /// remote ops, so the diff base never moves under us.
    pub async fn round(
        &mut self,
        link: &mut dyn Link,
        local_snapshot: &str,
    ) -> Result<Option<String>> {
        self.observe_local(local_snapshot);
        // Ship only what we've added since last round.
        let delta = self.doc.encode_delta(&self.last_sent);
        link.send(delta).await?;
        if let Some(bytes) = link.recv().await? {
            self.doc.merge(&bytes);
        }
        // Everything we now hold has been exchanged.
        self.last_sent = self.doc.version();
        Ok(self.view())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::mock_pair;

    /// Run one synchronized round for both peers, writing merged results back to
    /// their local "files" (the strings `va` / `vb`).
    async fn round2(
        a: &mut TextPeer,
        la: &mut crate::net::MockLink,
        va: &mut String,
        b: &mut TextPeer,
        lb: &mut crate::net::MockLink,
        vb: &mut String,
    ) {
        let (ra, rb) = tokio::join!(a.round(la, va), b.round(lb, vb));
        if let Some(m) = ra.unwrap() {
            *va = m;
        }
        if let Some(m) = rb.unwrap() {
            *vb = m;
        }
    }

    #[tokio::test]
    async fn concurrent_edits_to_shared_text_converge() {
        let (mut la, mut lb) = mock_pair();
        let mut a = TextPeer::new("a");
        let mut b = TextPeer::new("b");
        // Both type concurrently into the empty doc.
        let mut va = String::from("cat");
        let mut vb = String::from("dog");
        round2(&mut a, &mut la, &mut va, &mut b, &mut lb, &mut vb).await;

        // Converged, and neither edit was lost.
        assert_eq!(va, vb, "views diverged: {va:?} vs {vb:?}");
        assert!(va.contains("cat") && va.contains("dog"), "lost an edit: {va:?}");

        // A appends to the merged text; B is idle. Should still converge.
        va.push('!');
        round2(&mut a, &mut la, &mut va, &mut b, &mut lb, &mut vb).await;
        // one more round to let B's write-back settle to the same content
        round2(&mut a, &mut la, &mut va, &mut b, &mut lb, &mut vb).await;
        assert_eq!(va, vb);
        assert!(va.ends_with('!'));
        assert_eq!(a.content(), b.content());
    }
}
