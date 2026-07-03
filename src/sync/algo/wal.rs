//! Write-ahead-log database sync (DESIGN.md §17.3) — PLANNED (stub).
//!
//! Intended algorithm: treat the resource as an **append-only** log of records.
//! Each replica numbers its appended frames; `state_vector` advertises the
//! highest frame seq held per writer (a per-writer offset map), `delta_since`
//! ships the frames a peer is missing, `merge` appends them. Append-only +
//! per-writer offsets means convergence is "union of all frames in writer
//! order" — no rewrite, no whole-file diff. Compaction/checkpointing is a
//! separate concern layered on top.
//!
//! Not yet implemented: the sync methods `todo!()` so a misconfigured manifest
//! fails loudly rather than silently corrupting data.

use crate::sync::strategy::{Kind, SyncStrategy};

pub struct WalDb;

impl WalDb {
    pub fn new(_name: &str) -> Self {
        WalDb
    }
}

impl SyncStrategy for WalDb {
    fn kind(&self) -> Kind {
        Kind::WalDb
    }
    fn observe(&mut self, _current: &[u8]) -> bool {
        todo!("WAL: tail newly-appended frames (DESIGN.md §17.3)")
    }
    fn push_delta(&mut self) -> Option<Vec<u8>> {
        todo!("WAL: ship frames appended since the last push")
    }
    fn state_vector(&self) -> Vec<u8> {
        todo!("WAL: per-writer highest-seq offset map")
    }
    fn delta_since(&self, _theirs: &[u8]) -> Option<Vec<u8>> {
        todo!("WAL: frames the peer is missing")
    }
    fn merge(&mut self, _delta: &[u8]) -> Option<Vec<u8>> {
        todo!("WAL: append received frames in writer order")
    }
}
