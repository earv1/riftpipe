//! App-level runnables — things you *run*, as opposed to the library plumbing
//! they sit on (net/sync/monitor).
//!
//!   host     generic HTTP hosting over a live directory: static files (SPA
//!            fallback) + SSE change events (`riftpipe serve`); app servers
//!            consume it as a library
//!   signal   the content-blind WebSocket signaling relay (`riftpipe signal`)
//!            that brokers WebRTC offer/answer between peers

pub mod host;
pub mod signal;
