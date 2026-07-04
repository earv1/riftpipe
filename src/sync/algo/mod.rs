//! Concrete sync algorithms behind the [`SyncStrategy`](super::strategy::SyncStrategy)
//! adapter. One module per algorithm; the manifest (DESIGN.md §17) decides which
//! one a given resource gets.
//!
//!   text_crdt   eg-walker text CRDT (diamond-types)         — IMPLEMENTED
//!   rsync       rolling-checksum block diff for files       — IMPLEMENTED
//!   wal         append-only write-ahead-log db replication  — IMPLEMENTED
//!               (adapter over the riftpipe_core::wal primitive)
//!   lww_record  per-key LWW for `key = value` files         — IMPLEMENTED
//!               (absorbs the model of the former `sqlite.rs` per-cell LWW
//!               engine; that orphaned module was deleted — see lww_record docs)
//!   image       codec-aware image merge                     — PLANNED (stub)

pub mod image;
pub mod lww_record;
pub mod rsync;
pub mod text_crdt;
pub mod wal;
