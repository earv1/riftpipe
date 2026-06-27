//! Sync: the algorithm-agnostic seam plus the concrete paths.
//!
//!   syncer    the `Syncer` adapter trait + `Kind` (the Strategy seam, §17)
//!   algo      concrete algorithms behind it (text CRDT, rsync, wal*, image*)
//!   backing   where a resource's bytes live: file vs. in-memory (§17.5)
//!   manifest  riftpipe.toml: glob -> algorithm (§17)
//!   workspace a folder of resources, each bound to a Syncer + backing (§17)
//!   folder    multiplexed reconnecting session: many resources over one link
//!   pipe      the editor edit-stream protocol over --pipe (text, live)
//!   mirror    the file-mirror loop (text, single-shot)
//!
//! (* planned stubs.) `pipe`/`mirror` are the original text-only paths;
//! `syncer`/`algo`/`backing`/`manifest`/`workspace`/`folder` are folder-wide,
//! per-resource, multi-algorithm sync (DESIGN.md §17).

pub mod algo;
pub mod backing;
pub mod folder;
pub mod manifest;
pub mod mirror;
pub mod pipe;
pub mod syncer;
pub mod workspace;
