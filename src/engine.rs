//! The deterministic replay engine (DESIGN.md §5, §9). Holds the event graph and
//! replays it in a deterministic total order through the guard, materializing a
//! fresh document each time. One engine, three uses: live, conformance, dry-run.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use crate::document::Document;
use crate::op::{Op, OpId};
use crate::rules::{Rule, Verdict};

pub struct Engine<D: Document, R: Rule> {
    rule: R,
    events: Vec<Op>,
    /// Ops the guard rejected on the last replay, with the reason.
    pub rejected: Vec<(OpId, String)>,
    _doc: PhantomData<D>,
}

impl<D: Document, R: Rule> Engine<D, R> {
    pub fn new(rule: R) -> Self {
        Engine {
            rule,
            events: Vec::new(),
            rejected: Vec::new(),
            _doc: PhantomData,
        }
    }

    /// Add an event to the graph. Order of ingestion does not matter — replay
    /// imposes the deterministic total order.
    pub fn ingest(&mut self, op: Op) {
        self.events.push(op);
    }

    /// Deterministic replay → materialized document. Also refreshes `rejected`.
    pub fn materialize(&mut self) -> String {
        let mut ordered: Vec<&Op> = self.events.iter().collect();
        ordered.sort_by(|a, b| a.total_order_key().cmp(&b.total_order_key()));

        let mut doc = D::default();
        self.rejected.clear();
        for op in ordered {
            let state_before = doc.materialize();
            match self.rule.validate(op, &state_before) {
                Verdict::Accept => doc.apply(op),
                Verdict::Reject(why) => self.rejected.push((op.id, why)),
            }
        }
        doc.materialize()
    }

    /// Behavioral signature for conformance / handshake comparison
    /// (DESIGN.md §8): folds the materialized state and the rejection set.
    pub fn state_hash(&mut self) -> u64 {
        let state = self.materialize();
        let mut h = DefaultHasher::new();
        state.hash(&mut h);
        for (id, _) in &self.rejected {
            id.agent.0.hash(&mut h);
            id.seq.hash(&mut h);
        }
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AgentId;
    use crate::log::AppendLog;
    use crate::op::Action;
    use crate::rules::TwoPlayerTurns;

    fn mv(agent: AgentId, seq: u64, lamport: u64, text: &str) -> Op {
        Op {
            id: OpId { agent, seq },
            lamport,
            parents: vec![],
            action: Action::Append(format!("{text}\n")),
        }
    }

    #[test]
    fn out_of_turn_is_rejected_deterministically() {
        let a = AgentId::from_name("alice");
        let b = AgentId::from_name("bob");
        let mut eng: Engine<AppendLog, _> = Engine::new(TwoPlayerTurns {
            first: a,
            second: b,
        });
        // Ingest out of order on purpose; replay must still be deterministic.
        eng.ingest(mv(b, 1, 2, "b2")); // bob twice in a row -> 2nd rejected
        eng.ingest(mv(a, 0, 0, "a1"));
        eng.ingest(mv(b, 0, 1, "b1"));

        assert_eq!(eng.materialize(), "a1\nb1\n");
        assert_eq!(eng.rejected.len(), 1);
        assert_eq!(eng.rejected[0].0, OpId { agent: b, seq: 1 });
    }
}
