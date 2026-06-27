//! `riftpipe-core` — the wasm-compatible heart of riftpipe: the eg-walker text
//! CRDT ([`text::EgWalkerText`]) with snapshot diff-to-ops input, delta
//! encode/merge, and version-vector reconciliation primitives. No transport, no
//! async, no OS deps — so it builds for native and `wasm32` alike, and both the
//! CLI (`riftpipe`) and the browser crate (`riftpipe-web`) share one document model.

pub mod kanban;
pub mod text;

pub use text::EgWalkerText;
