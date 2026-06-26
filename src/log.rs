//! PLACEHOLDER document: a grow-only log (DESIGN.md §2). Entries are concatenated
//! in the deterministic order the engine applies them. It is a real, trivially
//! convergent CRDT — enough to exercise the engine, handshake, and simulation
//! end to end until the eg-walker text document (diamond-types) replaces it
//! behind the same `Document` trait.

use crate::document::Document;
use crate::op::{Action, Op};

#[derive(Default)]
pub struct AppendLog {
    text: String,
}

impl Document for AppendLog {
    fn apply(&mut self, op: &Op) {
        match &op.action {
            Action::Append(s) => self.text.push_str(s),
        }
    }

    fn materialize(&self) -> String {
        self.text.clone()
    }

    fn diff(base: &str, new_snapshot: &str) -> Vec<Action> {
        // Trivial append-only diff. eg-walker's diff handles mid-document edits
        // via Myers + anchored insert/delete (DESIGN.md §3).
        match new_snapshot.strip_prefix(base) {
            Some("") => vec![],
            Some(rest) => vec![Action::Append(rest.to_string())],
            // Non-append edit: placeholder re-emits the whole snapshot.
            None => vec![Action::Append(new_snapshot.to_string())],
        }
    }
}
