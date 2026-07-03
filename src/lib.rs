//! riftpipe — a Unix-like terminal construct for live, peer-to-peer
//! collaborative text (see DESIGN.md).
//!
//! Module map:
//!   net/      networking: the Link abstraction + mock, iroh transport, WebRTC,
//!             capability negotiation, secure pairing (ticket + auth)
//!   crdt/     the meat: the eg-walker text document (re-export of
//!             riftpipe_core::text, diamond-types)
//!   sync/     reconciliation: the editor --pipe protocol (+ version-vector
//!             reconciliation), the file-mirror loop, the tree-sync driver
//!             (any file tree over the shared core protocol), plus the
//!             pluggable multi-algorithm seam (strategy/algo/backing —
//!             DESIGN.md §17: text CRDT + rsync, with file/memory backings)
//!   monitor/  observability helpers: metrics + the in-memory `process` file
//!   app/      runnable servers on top of the plumbing: generic HTTP hosting
//!             (static + SSE change events) and the WebRTC signaling relay

pub mod app;
pub mod crdt;
pub mod monitor;
pub mod net;
pub mod sync;
