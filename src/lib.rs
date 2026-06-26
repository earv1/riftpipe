//! autoshare — collaborative-pipe core.
//!
//! This crate is the *decided* core from DESIGN.md: identity (§7), the
//! event-graph op model (§5), the pluggable document seam (§5), the
//! deterministic replay engine + guard (§5/§6), the simulation/conformance
//! harness (§9), and the mandatory-simulation handshake (§8).
//!
//! The two external seams — `transport` (iroh 1.0) and the eg-walker text
//! document (diamond-types) — are stubbed so the core builds and runs offline.

pub mod document;
pub mod engine;
pub mod game; // co-op/PvP tower-defense demo (deterministic core)
pub mod handshake;
pub mod identity;
pub mod log; // placeholder grow-only document; eg-walker text replaces it
pub mod metrics; // one-line status written to a file for tmux to display
pub mod net; // transport-agnostic Link + sync driver + mock transports
pub mod op;
pub mod pipe; // editor-stream protocol (--pipe): the Unix boundary
pub mod play; // networked tower-defense client (lockstep over a Link)
pub mod rules;
pub mod secure; // ticket capability + challenge-response auth ("connect anywhere")
pub mod simulation;
pub mod text; // real eg-walker text document (diamond-types)
pub mod textpipe; // live collaborative text pipe (the eg-walker demo)
pub mod transport;
