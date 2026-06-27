//! Concrete sync algorithms behind the [`Syncer`](super::syncer::Syncer)
//! adapter. One module per algorithm; the manifest (DESIGN.md §17) decides which
//! one a given resource gets.
//!
//!   text_crdt  eg-walker text CRDT (diamond-types)        — IMPLEMENTED
//!   rsync      rolling-checksum block diff for files       — PLANNED (stub)
//!   wal        append-only write-ahead-log db replication  — PLANNED (stub)
//!   image      codec-aware image merge                     — PLANNED (stub)

pub mod image;
pub mod rsync;
pub mod text_crdt;
pub mod wal;
