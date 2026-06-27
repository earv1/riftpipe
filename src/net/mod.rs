//! Networking: transport-agnostic Link + sync driver + mock (link), the real
//! iroh transport (transport), and secure pairing — ticket + auth (secure).

pub mod link;
pub mod secure;
pub mod transport;

pub use link::*; // Link, Counters, CountingLink, mock_pair, MockNet, sync_full, anyerr, Result, ...
