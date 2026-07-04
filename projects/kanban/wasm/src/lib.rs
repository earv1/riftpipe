//! `kanban-wasm` — everything kanban-specific in the browser stack, kept out of
//! the generic riftpipe crates (`agent.md`: generic core, kanban as showcase).
//!
//! - [`format`]: the on-disk board file format (pure parse/serialize, no I/O).
//! - [`handler`]: the in-browser "server" — the JSON API the SolidJS UI calls,
//!   handled by wasm over OPFS via [`riftpipe_web::opfs`], with local edits
//!   pushed to peers via [`riftpipe_web::tree_sync`].
//!
//! This crate is the app's single wasm bundle: it links `riftpipe-web` as an
//! rlib, so wasm-bindgen surfaces BOTH crates' exports (`kanbanHandle` here;
//! `connectAndSync`, `irohConnect`, `RiftDoc`, … from riftpipe-web) from this
//! one cdylib. Build with `wasm-pack build --target web`.

pub mod format;
pub mod handler;
