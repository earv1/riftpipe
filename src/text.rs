//! The real eg-walker text document (DESIGN.md §2, §3), backed by
//! **diamond-types** — the optimized event-graph-walker text CRDT.
//!
//! Architectural note (resolves the §5 "one engine" tension): a high-performance
//! text CRDT does its *own* deterministic event-graph replay internally, so plain
//! text does NOT go through the generic guard engine (`engine.rs`). Two paths:
//!   - **text** (this file): diamond-types owns ordering/merge; input is the §3
//!     snapshot diff-to-ops; no guard (rule-set "always accept").
//!   - **ruled** (`engine.rs` + `log.rs`): the generic guard engine over coarse
//!     ops, for turn-based games / permissions where a guard must intercept.
//! Both are "deterministic event-graph replay" — one specialized, one generic.

use std::ops::Range;

use diamond_types::list::encoding::{ENCODE_FULL, ENCODE_PATCH};
use diamond_types::list::remote_ids::RemoteId;
use diamond_types::list::OpLog;
use diamond_types::AgentId;
use similar::{capture_diff_slices, Algorithm, DiffOp};

/// A single-writer-local view onto a shared text CRDT. Local edits enter as
/// whole-snapshot updates (`edit_to`); convergence with peers happens by
/// exchanging encoded oplog deltas (`encode_delta` / `merge`).
pub struct EgWalkerText {
    oplog: OpLog,
    agent: AgentId,
}

impl EgWalkerText {
    pub fn new(agent_name: &str) -> Self {
        let mut oplog = OpLog::new();
        let agent = oplog.get_or_create_agent_id(agent_name);
        Self { oplog, agent }
    }

    /// Current materialized text (checkout at the oplog tip).
    pub fn content(&self) -> String {
        self.oplog
            .checkout(self.oplog.local_version_ref())
            .content()
            .to_string()
    }

    /// DESIGN.md §3 — diff-to-ops. Recover the edits that turn the current
    /// content into `new_text` (Myers diff over chars) and feed them to the CRDT
    /// as anchored insert/delete ops. Positions are unicode-scalar indices, which
    /// is the coordinate frame diamond-types uses.
    ///
    /// NOTE: this diffs against *current local content*. The full §3 design
    /// diffs against the producer's **base** (a moving target under concurrency);
    /// that bookkeeping lands when this is wired to a live producer/watcher.
    pub fn edit_to(&mut self, new_text: &str) {
        let current = self.content();
        if current == new_text {
            return;
        }
        let old: Vec<char> = current.chars().collect();
        let new: Vec<char> = new_text.chars().collect();

        // Apply the edit script left-to-right; `pos` tracks the position in the
        // evolving document (don't advance on delete, do on insert).
        let mut pos = 0usize;
        for op in capture_diff_slices(Algorithm::Myers, &old, &new) {
            match op {
                DiffOp::Equal { len, .. } => pos += len,
                DiffOp::Delete { old_len, .. } => {
                    self.delete(pos..pos + old_len);
                }
                DiffOp::Insert {
                    new_index, new_len, ..
                } => {
                    self.insert(pos, &new[new_index..new_index + new_len]);
                    pos += new_len;
                }
                DiffOp::Replace {
                    old_len,
                    new_index,
                    new_len,
                    ..
                } => {
                    self.delete(pos..pos + old_len);
                    self.insert(pos, &new[new_index..new_index + new_len]);
                    pos += new_len;
                }
            }
        }
    }

    fn insert(&mut self, pos: usize, chars: &[char]) {
        let s: String = chars.iter().collect();
        self.oplog.add_insert(self.agent, pos, &s);
    }

    fn delete(&mut self, range: Range<usize>) {
        self.oplog.add_delete_without_content(self.agent, range);
    }

    /// Granular insert at a char offset (clamped to the document length). Used by
    /// the pipe protocol / editor bridges.
    pub fn insert_at(&mut self, pos: usize, text: &str) {
        if text.is_empty() {
            return;
        }
        let len = self.content().chars().count();
        let pos = pos.min(len);
        self.oplog.add_insert(self.agent, pos, text);
    }

    /// Granular delete of `len` chars at a char offset (clamped to the document).
    pub fn delete_at(&mut self, pos: usize, len: usize) {
        let total = self.content().chars().count();
        let pos = pos.min(total);
        let end = (pos + len).min(total);
        if end > pos {
            self.oplog.add_delete_without_content(self.agent, pos..end);
        }
    }

    /// The local version (causal frontier) — capture before concurrent edits to
    /// later encode just the delta since this point.
    pub fn version(&self) -> Vec<usize> {
        self.oplog.local_version_ref().to_vec()
    }

    /// Encode operations since `since` for the wire (DESIGN.md §5). Uses
    /// ENCODE_PATCH so the delta carries ONLY the new ops + their inserted text —
    /// not the whole document (ENCODE_FULL stores the start-branch content, which
    /// would bloat every delta with the full file). The receiver must already
    /// hold `since`'s ancestors (a shared seed / prior deltas).
    pub fn encode_delta(&self, since: &[usize]) -> Vec<u8> {
        self.oplog.encode_from(ENCODE_PATCH, since)
    }

    /// Encode the entire history — used to seed a brand-new peer.
    pub fn encode_full(&self) -> Vec<u8> {
        self.oplog.encode(ENCODE_FULL)
    }

    /// The version frontier as portable `(agent, seq)` pairs — the compact
    /// "state vector" peers exchange to reconcile after a drop/reconnect
    /// (DESIGN.md §16). Usually just one or two entries (the tips).
    pub fn version_vector(&self) -> Vec<(String, usize)> {
        self.oplog
            .remote_version()
            .iter()
            .map(|r| (r.agent.to_string(), r.seq))
            .collect()
    }

    /// Encode exactly the ops a peer is missing, given THEIR version vector. We
    /// map their frontier into our frame best-effort (tips we don't have are ops
    /// *they* have and we lack — irrelevant to what we send) and encode
    /// everything after it. `None` when they're already caught up (so we send
    /// nothing); the full history when their vector is empty (a fresh peer). This
    /// is what makes reconciliation recover from any missed delta.
    pub fn ops_since(&self, theirs: &[(String, usize)]) -> Option<Vec<u8>> {
        let mut frontier: Vec<usize> = Vec::new();
        for (agent, seq) in theirs {
            let id = RemoteId {
                agent: agent.as_str().into(),
                seq: *seq,
            };
            if let Ok(t) = self.oplog.try_remote_to_local_time(&id) {
                frontier.push(t);
            }
        }
        // An encode always carries header bytes even with zero ops, so check for
        // actual ops rather than byte-emptiness.
        if self.oplog.iter_range_since(&frontier).next().is_none() {
            return None;
        }
        Some(self.oplog.encode_from(ENCODE_PATCH, &frontier))
    }

    /// Merge encoded ops from a peer. diamond-types dedupes by global op id, so
    /// this is idempotent and order-independent — the CRDT guarantee.
    pub fn merge(&mut self, bytes: &[u8]) {
        self.oplog
            .decode_and_add(bytes)
            .expect("decode peer ops");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_edits_converge_and_lose_nothing() {
        // Seed both replicas with a shared starting point.
        let mut a = EgWalkerText::new("alice");
        a.edit_to("hello world");
        let seed = a.encode_full();

        let mut b = EgWalkerText::new("bob");
        b.merge(&seed);
        assert_eq!(b.content(), "hello world");

        // Concurrent edits from the same base, in each replica's local frame.
        let base_a = a.version();
        let base_b = b.version();
        a.edit_to("hello brave world"); // alice inserts "brave "
        b.edit_to("hello world!!!"); // bob appends "!!!"

        // Exchange deltas both ways.
        let da = a.encode_delta(&base_a);
        let db = b.encode_delta(&base_b);
        a.merge(&db);
        b.merge(&da);

        // Strong eventual consistency: identical content, both edits survive.
        assert_eq!(a.content(), b.content());
        let merged = a.content();
        assert!(merged.contains("brave"), "lost alice's edit: {merged:?}");
        assert!(merged.contains("!!!"), "lost bob's edit: {merged:?}");
    }

    #[test]
    fn delta_carries_only_the_change_not_the_whole_doc() {
        // High-entropy content so it doesn't just compress away (the encoder has
        // compress_content: true).
        let mut s = String::with_capacity(2000);
        let mut x: u32 = 0x9e3779b9;
        for _ in 0..2000 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            s.push(char::from(33 + (x % 94) as u8)); // printable ASCII
        }
        let mut d = EgWalkerText::new("a");
        d.edit_to(&s);
        let v = d.version();
        let full = d.encode_full();
        d.insert_at(2000, "Y"); // one tiny edit
        let delta = d.encode_delta(&v);

        assert!(
            full.len() > 1000,
            "sanity: a full encode of 2000 random chars should be large ({} bytes)",
            full.len()
        );
        // The delta must be a tiny fraction of the document — it carries only the
        // change, not the whole file (the ENCODE_FULL-vs-ENCODE_PATCH bug).
        assert!(
            delta.len() < full.len() / 10,
            "delta should carry only the change: {} bytes vs full doc {} bytes",
            delta.len(),
            full.len()
        );
    }

    #[test]
    fn mid_document_edit_via_diff() {
        let mut d = EgWalkerText::new("a");
        d.edit_to("the quick fox");
        d.edit_to("the quick brown fox"); // mid-document insert recovered by diff
        assert_eq!(d.content(), "the quick brown fox");
        d.edit_to("the brown fox"); // mid-document delete
        assert_eq!(d.content(), "the brown fox");
    }
}
