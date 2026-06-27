//! The programmable-rules state machine and its tower-defense demo: the
//! deterministic replay engine (replay), the rule/guard trait (rules), the
//! event-graph op model (op), placeholder/log document (log, document), the
//! conformance/simulation harness (simulation), identities + handshake, and the
//! game (game) with its lockstep networked client (play).

pub mod document;
pub mod game;
pub mod handshake;
pub mod identity;
pub mod log;
pub mod op;
pub mod play;
pub mod replay;
pub mod rules;
pub mod simulation;

pub use replay::Engine;
