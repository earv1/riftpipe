//! riftpipe — a Unix-like terminal construct for live, peer-to-peer
//! collaborative text (see DESIGN.md).
//!
//! Module map:
//!   net/      networking: the Link abstraction + mock, iroh transport, secure
//!             pairing (ticket + auth)
//!   crdt/     the meat: the eg-walker text document (diamond-types)
//!   sync/     reconciliation: the editor --pipe protocol (+ version-vector
//!             reconciliation) and the file-mirror loop, plus the pluggable
//!             multi-algorithm seam (syncer/algo/backing — DESIGN.md §17:
//!             text CRDT + rsync, with file/memory backings)
//!   monitor/  observability helpers: metrics + the in-memory `process` file
//!   engine/   the programmable-rules state machine + tower-defense demo

pub mod crdt;
pub mod engine;
pub mod monitor;
pub mod net;
pub mod sync;
