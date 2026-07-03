//! Text CRDT strategy — the **adapter** onto diamond-types' [`EgWalkerText`]
//! (DESIGN.md §17). This keeps the existing eg-walker text path exactly as it
//! is and just dresses it in the [`SyncStrategy`] trait so a folder can mix text
//! resources with rsync/wal/image ones.
//!
//! The mapping is 1:1 with what `sync::pipe`/`sync::mirror` already do:
//!   observe      -> diff a new local snapshot into the CRDT (§3 diff-to-ops)
//!   push_delta   -> encode ops since the watermark, advance it
//!   state_vector -> the portable `(agent, seq)` version vector
//!   delta_since  -> the ops a peer is missing, given their vector
//!   merge        -> fold remote ops, hand back the new materialized text

use crate::crdt::text::EgWalkerText;
use crate::sync::strategy::{Kind, SyncStrategy};

pub struct TextCrdtSyncer {
    doc: EgWalkerText,
    /// Version of everything already shipped to the peer.
    last_sent: Vec<usize>,
    /// Last content we read from / wrote to the backing, so a real local edit is
    /// told apart from an echo of what we just materialized.
    last_known: String,
}

impl TextCrdtSyncer {
    pub fn new(name: &str) -> Self {
        let doc = EgWalkerText::new(name);
        let last_sent = doc.version();
        Self {
            doc,
            last_sent,
            last_known: String::new(),
        }
    }
}

impl SyncStrategy for TextCrdtSyncer {
    fn kind(&self) -> Kind {
        Kind::TextCrdt
    }

    fn observe(&mut self, current: &[u8]) -> bool {
        let text = String::from_utf8_lossy(current);
        if text.as_ref() == self.last_known {
            return false;
        }
        let before = self.doc.version();
        self.doc.edit_to(&text); // §3 diff-to-ops
        self.last_known = text.into_owned();
        self.doc.version() != before
    }

    fn push_delta(&mut self) -> Option<Vec<u8>> {
        let delta = self.doc.encode_delta(&self.last_sent);
        self.last_sent = self.doc.version();
        Some(delta)
    }

    fn state_vector(&self) -> Vec<u8> {
        serde_json::to_vec(&self.doc.version_vector()).unwrap_or_default()
    }

    fn delta_since(&self, theirs: &[u8]) -> Option<Vec<u8>> {
        let theirs: Vec<(String, usize)> = serde_json::from_slice(theirs).ok()?;
        self.doc.ops_since(&theirs)
    }

    fn merge(&mut self, delta: &[u8]) -> Option<Vec<u8>> {
        let before = self.doc.content();
        let _ = self.doc.merge(delta);
        // Advancing the watermark past merged ops keeps us from echoing them back.
        self.last_sent = self.doc.version();
        let after = self.doc.content();
        if after != before {
            self.last_known = after.clone();
            Some(after.into_bytes())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two text syncers converge through the adapter, losing no concurrent edit —
    /// the same guarantee the underlying CRDT gives, now via the trait.
    #[test]
    fn concurrent_text_converges_through_the_adapter() {
        let mut a = TextCrdtSyncer::new("a");
        let mut b = TextCrdtSyncer::new("b");

        // Concurrent local edits.
        assert!(a.observe(b"cat"));
        assert!(b.observe(b"dog"));
        let da = a.push_delta().unwrap();
        let db = b.push_delta().unwrap();

        // Exchange pushes.
        a.merge(&db);
        b.merge(&da);

        let ca = String::from_utf8(a.state_after()).unwrap();
        let cb = String::from_utf8(b.state_after()).unwrap();
        assert_eq!(ca, cb, "diverged: {ca:?} vs {cb:?}");
        assert!(ca.contains("cat") && ca.contains("dog"), "lost an edit: {ca:?}");
    }

    /// The pull path recovers a peer that missed a push.
    #[test]
    fn reconcile_recovers_a_missed_push() {
        let mut a = TextCrdtSyncer::new("a");
        let mut b = TextCrdtSyncer::new("b");
        a.observe(b"one ");
        let _d1 = a.push_delta(); // delivered nowhere ("lost")
        a.observe(b"one two");
        let _d2 = a.push_delta();

        // B advertises, A answers with exactly what B lacks.
        let missing = a.delta_since(&b.state_vector()).expect("A has ops B lacks");
        let merged = b.merge(&missing).expect("B materializes new content");
        assert_eq!(String::from_utf8(merged).unwrap(), "one two");

        // Already-in-sync: A has nothing more for B.
        assert!(a.delta_since(&b.state_vector()).is_none());
    }

    // test-only helper: read the current materialized content
    impl TextCrdtSyncer {
        fn state_after(&self) -> Vec<u8> {
            self.doc.content().into_bytes()
        }
    }
}
