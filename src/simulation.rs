//! Simulation / conformance harness (DESIGN.md §8, §9). The same replay engine,
//! run with no commit and no network. A conformance suite is the *behavioral
//! spec* of a rule-set: golden vectors of `ordered ops -> expected state`. Two
//! impls are interoperable iff they agree on every vector. The handshake
//! ALWAYS runs this (§8), comparing transcript hashes.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::document::Document;
use crate::engine::Engine;
use crate::op::{Op, OpId};
use crate::rules::Rule;

/// One golden vector.
pub struct Vector {
    pub name: String,
    pub events: Vec<Op>,
    pub expect_state: String,
    pub expect_rejected: Vec<OpId>,
}

pub struct VectorResult {
    pub name: String,
    pub passed: bool,
    pub got_state: String,
    pub got_rejected: Vec<OpId>,
}

pub struct Suite {
    pub vectors: Vec<Vector>,
}

impl Suite {
    /// Run every vector against a fresh engine built by `make_rule`.
    pub fn run<D, R, F>(&self, make_rule: F) -> Vec<VectorResult>
    where
        D: Document,
        R: Rule,
        F: Fn() -> R,
    {
        self.vectors
            .iter()
            .map(|v| {
                let mut eng: Engine<D, R> = Engine::new(make_rule());
                for op in &v.events {
                    eng.ingest(op.clone());
                }
                let got_state = eng.materialize();
                let got_rejected: Vec<OpId> =
                    eng.rejected.iter().map(|(id, _)| *id).collect();
                let passed =
                    got_state == v.expect_state && got_rejected == v.expect_rejected;
                VectorResult {
                    name: v.name.clone(),
                    passed,
                    got_state,
                    got_rejected,
                }
            })
            .collect()
    }

    /// Transcript hash exchanged during the handshake (DESIGN.md §8). Folds each
    /// vector's resulting state; a mismatch means the peers' rule-sets diverge.
    pub fn transcript_hash<D, R, F>(&self, make_rule: F) -> u64
    where
        D: Document,
        R: Rule,
        F: Fn() -> R,
    {
        let mut h = DefaultHasher::new();
        for r in self.run::<D, R, _>(&make_rule) {
            r.name.hash(&mut h);
            r.got_state.hash(&mut h);
        }
        h.finish()
    }
}
