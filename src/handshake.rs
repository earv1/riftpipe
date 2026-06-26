//! The handshake (DESIGN.md §8). Doc identity is checked first; then a
//! simulation transcript is ALWAYS compared — mandatory, not a fallback — so
//! incompatible or heterogeneous rule-sets are caught before any data syncs.

use crate::identity::{AgentId, DocId, RuleFingerprint};

/// First handshake message.
#[derive(Clone, Copy, Debug)]
pub struct Hello {
    pub doc: DocId,
    pub agent: AgentId,
    pub rule: RuleFingerprint,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Compatible,
    DocMismatch,
    /// Rule-sets behave differently. A real impl names the diverging vector.
    SimDiverged,
}

/// Evaluate a handshake. The simulation transcript is compared even when
/// fingerprints match (defense in depth — DESIGN.md §8).
pub fn evaluate(
    local: &Hello,
    remote: &Hello,
    local_transcript: u64,
    remote_transcript: u64,
) -> Outcome {
    if local.doc != remote.doc {
        return Outcome::DocMismatch;
    }
    if local_transcript != remote_transcript {
        return Outcome::SimDiverged;
    }
    Outcome::Compatible
}
