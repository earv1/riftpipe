//! Networking: transport-agnostic Link + sync driver + mock (link), the real
//! iroh transport (transport), secure pairing — ticket + auth (secure), and
//! connect-time transport negotiation (negotiate).

pub mod link;
pub mod negotiate;
pub mod secure;
pub mod transport;
pub mod webrtc;

pub use link::*; // Link, Counters, CountingLink, mock_pair, MockNet, sync_full, anyerr, Result, ...
