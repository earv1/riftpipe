//! The pluggable document seam (DESIGN.md §5, Mosh-SSP style). The transport and
//! engine don't know what kind of document they carry; they only need
//! apply/materialize/diff. The v0 placeholder is `log::AppendLog`; the eg-walker
//! text document (diamond-types) will implement this same trait.

use crate::op::{Action, Op};

pub trait Document: Default {
    /// Fold one (already-accepted) op into the document state.
    fn apply(&mut self, op: &Op);

    /// Render the current state.
    fn materialize(&self) -> String;

    /// Diff-to-ops: recover edit actions from a snapshot relative to `base`
    /// (DESIGN.md §3 — diff against the producer's base, NOT live state).
    fn diff(base: &str, new_snapshot: &str) -> Vec<Action>;
}
