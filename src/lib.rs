//! riftpipe — a Unix-like terminal construct for live, peer-to-peer
//! collaborative text (see DESIGN.md).
//!
//! Module map:
//!   net/      networking: the Link abstraction + mock, iroh transport, WebRTC,
//!             capability negotiation, secure pairing (ticket + auth)
//!   crdt/     the meat: the eg-walker text document (re-export of
//!             riftpipe_core::text, diamond-types)
//!   sync/     reconciliation: the editor --pipe protocol (+ version-vector
//!             reconciliation) and the file-mirror loop, plus the pluggable
//!             multi-algorithm seam (strategy/algo/backing — DESIGN.md §17:
//!             text CRDT + rsync, with file/memory backings)
//!   monitor/  observability helpers: metrics + the in-memory `process` file
//!   app/      runnable servers on top of the plumbing: the kanban board
//!             server (serve/connect) and the WebRTC signaling relay

pub mod app;
pub mod crdt;
pub mod monitor;
pub mod net;
pub mod sync;
