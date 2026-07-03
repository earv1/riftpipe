//! Image sync (DESIGN.md §17.4) — PLANNED (stub).
//!
//! Intended algorithm: the eg-walker text CRDT is wrong for pixels — concurrent
//! edits to an image want **region/tile** granularity, not character ops. The
//! plan is a codec-aware merge: decode to a tile grid, treat each tile as an
//! independently-versioned cell (an LWW or per-tile op-log), `state_vector`
//! advertises per-tile versions, `delta_since` ships changed tiles, `merge`
//! composites them. Layer/alpha handling and a real codec (PNG/QOI) come with
//! the implementation.
//!
//! Not yet implemented: the sync methods `todo!()` so a misconfigured manifest
//! fails loudly rather than silently corrupting data.

use crate::sync::strategy::{Kind, SyncStrategy};

pub struct ImageSyncer;

impl ImageSyncer {
    pub fn new(_name: &str) -> Self {
        ImageSyncer
    }
}

impl SyncStrategy for ImageSyncer {
    fn kind(&self) -> Kind {
        Kind::Image
    }
    fn observe(&mut self, _current: &[u8]) -> bool {
        todo!("image: decode to tiles, mark changed cells (DESIGN.md §17.4)")
    }
    fn push_delta(&mut self) -> Option<Vec<u8>> {
        todo!("image: ship changed tiles")
    }
    fn state_vector(&self) -> Vec<u8> {
        todo!("image: per-tile version grid")
    }
    fn delta_since(&self, _theirs: &[u8]) -> Option<Vec<u8>> {
        todo!("image: tiles the peer is missing")
    }
    fn merge(&mut self, _delta: &[u8]) -> Option<Vec<u8>> {
        todo!("image: composite received tiles")
    }
}
