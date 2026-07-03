//! The **adapter seam** for multi-algorithm sync (DESIGN.md §17).
//!
//! riftpipe started as a single text CRDT. The folder model needs *different*
//! algorithms for different resources: an eg-walker CRDT for prose, an
//! rsync-style block diff for opaque files, an append-only WAL for a database,
//! a codec-aware merge for images. The session loop and wire framing must not
//! care which — so every algorithm hides behind one trait.
//!
//! ## Why this shape (Strategy, not just Adapter)
//! The organizing pattern is **Strategy**: a family of interchangeable
//! algorithms behind one interface, selected at runtime (by the manifest).
//! *Adapter* is the inner role each strategy plays when it wraps an existing
//! library (`TextCrdtSyncer` adapts diamond-types). The trait is kept to the
//! **minimal, reconcile-centric** contract the network layer truly needs —
//! advertise what you have, answer "what am I missing", merge an opaque delta —
//! rather than a snapshot-in/out shape that would distort non-snapshot
//! algorithms (a WAL tails records; rsync negotiates via block checksums).
//!
//! All payloads are opaque bytes: the framing layer never interprets them, so a
//! new algorithm is a new `impl SyncStrategy`, nothing else. Object-safe on purpose,
//! so heterogeneous resources can share one link in a
//! `HashMap<ResourceId, Box<dyn SyncStrategy>>`.

use serde::{Deserialize, Serialize};

/// One pluggable sync algorithm bound to one resource (a file, a db, a board).
///
/// Two ways state flows in/out, both driven by the session loop:
///   * **push** — a local change is observed ([`observe`](SyncStrategy::observe)); if
///     the algorithm can ship eagerly it returns a delta from
///     [`push_delta`](SyncStrategy::push_delta). (CRDTs push; rsync can't push without
///     the peer's checksums, so it returns `None` and waits for the pull path.)
///   * **pull / reconcile** — a peer advertises its
///     [`state_vector`](SyncStrategy::state_vector); we answer with
///     [`delta_since`](SyncStrategy::delta_since); the peer folds it in via
///     [`merge`](SyncStrategy::merge). This runs on connect, after a settle, and on a
///     heartbeat — it is also what recovers a missed push.
pub trait SyncStrategy: Send {
    /// Which algorithm this is (for diagnostics / the metrics HUD).
    fn kind(&self) -> Kind;

    /// Fold the resource's current local bytes into the algorithm's state.
    /// Returns `true` if this changed our state (so the loop knows to try a
    /// push and/or re-advertise).
    fn observe(&mut self, current: &[u8]) -> bool;

    /// A delta to **push** eagerly after a local change, advancing the "already
    /// sent" watermark. `None` when this algorithm only syncs via the pull path
    /// (e.g. rsync needs the peer's block checksums first).
    fn push_delta(&mut self) -> Option<Vec<u8>>;

    /// Compact advertisement of what we hold, for the pull/reconcile path
    /// (text: a version vector; rsync: block checksums). The peer replies with
    /// [`delta_since`](SyncStrategy::delta_since).
    fn state_vector(&self) -> Vec<u8>;

    /// Given a peer's [`state_vector`](SyncStrategy::state_vector), the delta that
    /// brings **them** up to **us** (text: the ops they lack; rsync: a token
    /// stream). `None` when they are already caught up (so we send nothing).
    fn delta_since(&self, theirs: &[u8]) -> Option<Vec<u8>>;

    /// Merge a delta from a peer. Returns the new materialized bytes **iff** they
    /// changed — the caller writes them to the backing (disk / memory).
    fn merge(&mut self, delta: &[u8]) -> Option<Vec<u8>>;
}

/// The set of sync algorithms a workspace can assign to a resource. Serialized
/// as kebab-case so the manifest reads naturally (`algo = "text-crdt"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    /// eg-walker text CRDT (diamond-types). The hero path; implemented.
    TextCrdt,
    /// rsync-style rolling-checksum block diff for opaque/binary files.
    RsyncFile,
    /// append-only write-ahead-log replication for databases; deterministic
    /// frame linearization via `riftpipe_core::wal`. Implemented.
    WalDb,
    /// codec-aware image merge (tiles / layers). Planned.
    Image,
}

impl Kind {
    /// Construct the live algorithm for this kind, bound to `name` (the resource
    /// path — used as the CRDT agent name / log id).
    pub fn build(self, name: &str) -> Box<dyn SyncStrategy> {
        use super::algo;
        match self {
            Kind::TextCrdt => Box::new(algo::text_crdt::TextCrdtSyncer::new(name)),
            Kind::RsyncFile => Box::new(algo::rsync::RsyncSyncer::new(name)),
            Kind::WalDb => Box::new(algo::wal::WalDbSyncer::new(name)),
            Kind::Image => Box::new(algo::image::ImageSyncer::new(name)),
        }
    }

    /// Whether this algorithm is actually implemented yet (vs. a planned stub).
    pub fn is_implemented(self) -> bool {
        matches!(self, Kind::TextCrdt | Kind::RsyncFile | Kind::WalDb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_round_trips_as_kebab_case() {
        assert_eq!(
            serde_json::to_string(&Kind::TextCrdt).unwrap(),
            r#""text-crdt""#
        );
        assert_eq!(
            serde_json::from_str::<Kind>(r#""wal-db""#).unwrap(),
            Kind::WalDb
        );
    }

    #[test]
    fn implemented_kinds_are_flagged() {
        assert!(Kind::TextCrdt.is_implemented());
        assert!(Kind::RsyncFile.is_implemented());
        assert!(Kind::WalDb.is_implemented());
        assert!(!Kind::Image.is_implemented());
    }

    #[test]
    fn factory_builds_the_requested_kind() {
        // Only build/inspect kind() — stub algorithms panic on real sync calls.
        assert_eq!(Kind::TextCrdt.build("x").kind(), Kind::TextCrdt);
        assert_eq!(Kind::RsyncFile.build("x").kind(), Kind::RsyncFile);
        assert_eq!(Kind::WalDb.build("x").kind(), Kind::WalDb);
        assert_eq!(Kind::Image.build("x").kind(), Kind::Image);
    }
}
