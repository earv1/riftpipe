//! App-level runnables — things you *run*, as opposed to the library plumbing
//! they sit on (net/sync/monitor).
//!
//!   kanban   the board server: HTTP/SSE file-API + bundled SPA host
//!            (`kanban serve`) and the WebRTC board-sync loop (`kanban connect`)
//!   signal   the content-blind WebSocket signaling relay (`riftpipe signal`)
//!            that brokers WebRTC offer/answer between peers

pub mod kanban;
pub mod signal;
