//! The pluggable deterministic guard (DESIGN.md §6). `validate` is a *pure
//! function* of the op and the materialized state immediately before it in the
//! deterministic replay order. Because order is deterministic and the predicate
//! is pure, every honest peer computes the same verdict → convergence holds even
//! when some ops are rejected.

use crate::engine::identity::{AgentId, RuleFingerprint};
use crate::engine::op::Op;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Accept,
    Reject(String),
}

pub trait Rule {
    fn validate(&self, op: &Op, state_before: &str) -> Verdict;
    fn fingerprint(&self) -> RuleFingerprint;
}

/// Rule-set "text": always accept. Recovers plain collaborative text.
pub struct AllowAll;

impl Rule for AllowAll {
    fn validate(&self, _op: &Op, _state_before: &str) -> Verdict {
        Verdict::Accept
    }
    fn fingerprint(&self) -> RuleFingerprint {
        RuleFingerprint(0x0A11_0A11)
    }
}

/// Example I-confluent rule (DESIGN.md §6): strict turn-taking between two
/// agents — a single turn "token" passed by alternation. The turn is inferred
/// purely from `state_before` (one accepted move == one line), so the guard
/// stays a pure function of replay state. Out-of-turn ops are rejected; accepted
/// moves are stable (never revoked), because the rule is I-confluent.
pub struct TwoPlayerTurns {
    pub first: AgentId,
    pub second: AgentId,
}

impl Rule for TwoPlayerTurns {
    fn validate(&self, op: &Op, state_before: &str) -> Verdict {
        let moves_so_far = state_before.lines().count();
        let whose_turn = if moves_so_far % 2 == 0 {
            self.first
        } else {
            self.second
        };
        if op.id.agent == whose_turn {
            Verdict::Accept
        } else if op.id.agent == self.first || op.id.agent == self.second {
            Verdict::Reject("out of turn".into())
        } else {
            Verdict::Reject("not a participant".into())
        }
    }
    fn fingerprint(&self) -> RuleFingerprint {
        RuleFingerprint(0x7000_0002)
    }
}
