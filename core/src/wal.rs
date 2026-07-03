//! `wal-db` core: append-only **frames** + a deterministic **linearizer**
//! (`docs/planned/wal-db.md`). The log-native multi-writer primitive — where text
//! uses a CRDT, a WAL keeps whole frames intact and agrees on their *order*.
//!
//! `linearize()` folds the causal DAG of frames into one total order: causality
//! first (a frame after its `deps`), concurrency ties broken by `(writer, seq)`.
//! It's order-independent + idempotent, so two replicas holding the same frame set
//! produce the identical sequence — convergence for N writers, like the CRDT.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// A frame's identity: which writer produced it, and its seq in that writer's log.
pub type FrameId = (String, u64);

/// One append-only entry. `deps` is what the writer had seen when it appended (its
/// frontier), giving the causal DAG; `payload` is opaque app bytes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Frame {
    pub writer: String,
    pub seq: u64,
    pub deps: Vec<FrameId>,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn id(&self) -> FrameId {
        (self.writer.clone(), self.seq)
    }
}

/// The union of frames from every writer — a causal DAG that linearizes
/// deterministically.
#[derive(Default)]
pub struct Replica {
    frames: BTreeMap<FrameId, Frame>,
}

impl Replica {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge a frame in (idempotent — re-adding is a no-op). Returns `true` iff
    /// the frame was new, so callers can tell "changed" from "already had it".
    pub fn add(&mut self, frame: Frame) -> bool {
        let mut inserted = false;
        self.frames.entry(frame.id()).or_insert_with(|| {
            inserted = true;
            frame
        });
        inserted
    }

    /// Append a new local frame for `writer`, depending on the current frontier.
    pub fn append(&mut self, writer: &str, payload: Vec<u8>) -> Frame {
        let seq = self
            .frames
            .keys()
            .filter(|(w, _)| w == writer)
            .map(|(_, s)| s + 1)
            .max()
            .unwrap_or(0);
        let frame = Frame { writer: writer.to_string(), seq, deps: self.frontier(), payload };
        self.add(frame.clone());
        frame
    }

    /// The frontier — frame ids nothing else depends on (the tips).
    fn frontier(&self) -> Vec<FrameId> {
        let mut is_dep: BTreeSet<FrameId> = BTreeSet::new();
        for f in self.frames.values() {
            for d in &f.deps {
                is_dep.insert(d.clone());
            }
        }
        self.frames.keys().filter(|id| !is_dep.contains(id)).cloned().collect()
    }

    /// Frame ids this replica is missing relative to `have` (a per-writer high-water
    /// map) — for catching a neighbor up over the mesh.
    pub fn missing_for(&self, have: &BTreeMap<String, u64>) -> Vec<&Frame> {
        self.frames
            .values()
            .filter(|f| have.get(&f.writer).map(|&hw| f.seq > hw).unwrap_or(true))
            .collect()
    }

    /// Per-writer high-water seq — the compact "what I have" to exchange.
    pub fn watermarks(&self) -> BTreeMap<String, u64> {
        let mut wm = BTreeMap::new();
        for (w, s) in self.frames.keys() {
            let e = wm.entry(w.clone()).or_insert(0);
            *e = (*e).max(*s);
        }
        wm
    }

    /// Deterministic total order: causal, ties broken by `(writer, seq)`. Kahn's
    /// algorithm with a `(writer, seq)`-ordered ready set → order-independent.
    pub fn linearize(&self) -> Vec<&Frame> {
        let mut indegree: BTreeMap<FrameId, usize> = self.frames.keys().map(|id| (id.clone(), 0)).collect();
        let mut dependents: BTreeMap<FrameId, Vec<FrameId>> = BTreeMap::new();
        for (id, f) in &self.frames {
            for d in &f.deps {
                if self.frames.contains_key(d) {
                    *indegree.get_mut(id).unwrap() += 1;
                    dependents.entry(d.clone()).or_default().push(id.clone());
                }
            }
        }
        let mut ready: BTreeSet<FrameId> =
            indegree.iter().filter(|(_, d)| **d == 0).map(|(id, _)| id.clone()).collect();
        let mut out = Vec::with_capacity(self.frames.len());
        while let Some(id) = ready.iter().next().cloned() {
            ready.remove(&id);
            out.push(&self.frames[&id]);
            if let Some(deps) = dependents.get(&id) {
                for dep in deps {
                    let e = indegree.get_mut(dep).unwrap();
                    *e -= 1;
                    if *e == 0 {
                        ready.insert(dep.clone());
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(r: &Replica) -> Vec<FrameId> {
        r.linearize().iter().map(|f| f.id()).collect()
    }

    #[test]
    fn two_writers_converge_on_one_order() {
        // A and B append concurrently, then exchange; a later frame builds on both.
        let mut a = Replica::new();
        let mut b = Replica::new();
        let a1 = a.append("a", b"a1".to_vec());
        let b1 = b.append("b", b"b1".to_vec());
        a.add(b1.clone());
        b.add(a1.clone());
        let a2 = a.append("a", b"a2".to_vec()); // deps = {a1, b1}
        b.add(a2.clone());

        assert_eq!(order(&a), order(&b), "replicas converge on the identical order");
        // Causality holds: a1 and b1 precede a2.
        let ord = order(&a);
        let pos = |id: FrameId| ord.iter().position(|x| *x == id).unwrap();
        assert!(pos(("a".into(), 0)) < pos(("a".into(), 1)), "a1 before a2");
        assert!(pos(("b".into(), 0)) < pos(("a".into(), 1)), "b1 before a2");
    }

    #[test]
    fn linearization_is_order_independent() {
        // Build the same frame set by adding in two different orders → same result.
        let mut src = Replica::new();
        let f1 = src.append("a", b"1".to_vec());
        let f2 = src.append("b", b"2".to_vec());
        let f3 = src.append("a", b"3".to_vec());

        let mut x = Replica::new();
        for f in [&f1, &f2, &f3] {
            x.add(f.clone());
        }
        let mut y = Replica::new();
        for f in [&f3, &f1, &f2] {
            y.add(f.clone());
        }
        assert_eq!(order(&x), order(&y), "arrival order doesn't change the linearization");
    }

    #[test]
    fn add_reports_new_vs_already_held() {
        let mut src = Replica::new();
        let f = src.append("a", b"x".to_vec());

        let mut r = Replica::new();
        assert!(r.add(f.clone()), "first add is new");
        assert!(!r.add(f), "re-adding the same frame is a no-op");
    }

    #[test]
    fn watermarks_and_missing_drive_catch_up() {
        let mut full = Replica::new();
        full.append("a", b"a1".to_vec());
        full.append("a", b"a2".to_vec());
        full.append("b", b"b1".to_vec());

        // A peer that only has a's first frame asks for the rest.
        let mut have = BTreeMap::new();
        have.insert("a".to_string(), 0u64); // has a seq 0
        let missing: Vec<FrameId> = full.missing_for(&have).iter().map(|f| f.id()).collect();
        assert!(missing.contains(&("a".into(), 1)), "missing a2");
        assert!(missing.contains(&("b".into(), 0)), "missing b1");
        assert!(!missing.contains(&("a".into(), 0)), "already has a1");
    }
}
