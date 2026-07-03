//! Sync: the algorithm-agnostic seam plus the concrete paths.
//!
//!   strategy  the `SyncStrategy` adapter trait + `Kind` (the Strategy seam, §17)
//!   algo      concrete algorithms behind it (text CRDT, rsync, wal*, image*)
//!   backing   where a resource's bytes live: file vs. in-memory (§17.5)
//!   manifest  riftpipe.toml: glob -> algorithm (§17)
//!   workspace a folder of resources, each bound to a SyncStrategy + backing (§17)
//!   folder    multiplexed reconnecting session: many resources over one link
//!   tree      the tree-sync driver: riftpipe_core::sync (the browser protocol)
//!             bound to any file tree + a split link — native↔browser sync
//!   pipe      the editor edit-stream protocol over --pipe (text, live)
//!   mirror    the file-mirror loop (text, single-shot)
//!
//! (* planned stubs.) `pipe`/`mirror` are the original text-only paths;
//! `strategy`/`algo`/`backing`/`manifest`/`workspace`/`folder` are folder-wide,
//! per-resource, multi-algorithm sync (DESIGN.md §17). `tree` rides the shared
//! core protocol instead, for wire-compatibility with browser peers.

pub mod algo;
pub mod backing;
pub mod folder;
pub mod manifest;
pub mod mirror;
pub mod pipe;
pub mod strategy;
pub mod tree;
pub mod workspace;

use crate::crdt::text::EgWalkerText;
use crate::net::{Link, Result};

/// The shared sync driver used by BOTH mock and real transports: broadcast our
/// full CRDT state, then merge `peer_msgs` incoming states. For a pair use
/// `peer_msgs = 1`; for an N-client bus use `peer_msgs = N - 1`. Convergence is
/// order-independent (the CRDT guarantee), so the driver needs no coordination.
pub async fn sync_full(
    doc: &mut EgWalkerText,
    link: &mut dyn Link,
    peer_msgs: usize,
) -> Result<()> {
    link.send(doc.encode_full()).await?;
    for _ in 0..peer_msgs {
        match link.recv().await? {
            Some(bytes) => {
                let _ = doc.merge(&bytes);
            }
            None => break,
        }
    }
    link.done().await?;
    Ok(())
}
