//! Networking: transport-agnostic Link + mock (link), the real iroh transport
//! (transport), WebRTC data-plane + upgrade (webrtc), secure pairing — ticket +
//! auth (secure), and connect-time transport negotiation (negotiate). The sync
//! drivers that run over a Link live in `sync/`, not here.

pub mod link;
pub mod negotiate;
pub mod secure;
pub mod transport;
pub mod webrtc;

pub use link::*; // Link, Counters, CountingLink, mock_pair, MockNet, anyerr, Result, ...
