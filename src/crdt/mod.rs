//! The eg-walker text document — now lives in the shared, wasm-compatible
//! [`riftpipe_core`] crate so the browser build reuses the exact same CRDT + delta
//! encoding. Re-exported here so the rest of the native crate keeps using
//! `crate::crdt::text::EgWalkerText` unchanged.

pub use riftpipe_core::text;

pub use text::EgWalkerText;
