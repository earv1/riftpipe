//! The event-graph op model (DESIGN.md §5). Ops carry their causal parents (the
//! DAG) and a Lamport timestamp; the engine replays them in a deterministic
//! total order so every honest peer materializes the same state.

use crate::identity::AgentId;

/// Unique identity of an op: author + per-author sequence number.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct OpId {
    pub agent: AgentId,
    pub seq: u64,
}

/// What an op does. PLACEHOLDER: a grow-only `Append`. The eg-walker text
/// document (diamond-types) replaces this with anchored Insert/Delete carrying
/// a char and a neighbour id (DESIGN.md §2, §3).
#[derive(Clone, Debug)]
pub enum Action {
    Append(String),
}

/// An event in the graph.
#[derive(Clone, Debug)]
pub struct Op {
    pub id: OpId,
    pub lamport: u64,
    /// Causal parents — the version this op was made against (DESIGN.md §6).
    pub parents: Vec<OpId>,
    pub action: Action,
}

impl Op {
    /// The deterministic total order every honest peer applies identically:
    /// Lamport timestamp, then agent id, then seq (DESIGN.md §5/§6).
    pub fn total_order_key(&self) -> (u64, AgentId, u64) {
        (self.lamport, self.id.agent, self.id.seq)
    }
}
