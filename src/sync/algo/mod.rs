//! Concrete sync algorithms behind the [`SyncStrategy`](super::strategy::SyncStrategy)
//! adapter. One module per algorithm; the manifest (DESIGN.md §17) decides which
//! one a given resource gets.
//!
//!   text_crdt  eg-walker text CRDT (diamond-types)        — IMPLEMENTED
//!   rsync      rolling-checksum block diff for files       — IMPLEMENTED
//!   wal        append-only write-ahead-log db replication  — IMPLEMENTED
//!              (adapter over the riftpipe_core::wal primitive)
//!   image      codec-aware image merge                     — PLANNED (stub)
//!   sqlite     per-cell LWW table sync engine              — implemented +
//!              tested, but NOT wired into `Kind`/the manifest yet

pub mod image;
pub mod rsync;
pub mod sqlite;
pub mod text_crdt;
pub mod wal;
