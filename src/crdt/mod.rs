//! The meat: the eg-walker text document (diamond-types) with diff-to-ops input,
//! delta encode/merge, and version-vector reconciliation primitives.

pub mod text;

pub use text::EgWalkerText;
