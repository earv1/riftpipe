//! Write-ahead-log database sync (DESIGN.md §17.3) — IMPLEMENTED.
//!
//! The **adapter** onto [`riftpipe_core::wal`] (`docs/planned/wal-db.md`): the
//! resource is an **append-only** log of whole [`Frame`]s. Where text merges
//! character-by-character, a WAL keeps each frame intact and the replicas agree
//! on the frames' *order* — `core::wal::Replica::linearize()` folds the causal
//! DAG into one deterministic total order, so any two replicas holding the same
//! frame set materialize the identical log.
//!
//! ## On-disk encoding (this adapter owns it)
//! `wal-db.md` leaves the byte format to the integration, so the adapter
//! defines it: the file is a sequence of **`u32`-LE length-prefixed postcard
//! [`Frame`]s**. The local process appends by writing `len ++ postcard(Frame)`;
//! a truncated or garbled tail (a torn append) is tolerated — decoding stops at
//! the first bad record and the intact prefix still syncs. The merged,
//! materialized log written back by [`merge`](SyncStrategy::merge) is the full
//! frame set re-encoded the same way, in linearized order.
//!
//! ## The trait mapping
//!   observe      -> decode the local log, ingest its frames (idempotent);
//!                   true iff a frame we didn't hold appeared
//!   push_delta   -> frames appended since the last push (per-writer
//!                   watermarks), advancing the watermark — a WAL *can* push
//!   state_vector -> the replica's per-writer high-water map (`watermarks()`)
//!   delta_since  -> `missing_for(theirs)`: the frames the peer lacks
//!   merge        -> ingest remote frames; if any were new, hand back the
//!                   re-linearized log for the caller to write to the backing

use std::collections::BTreeMap;

use riftpipe_core::wal::{Frame, Replica};

use crate::sync::strategy::{Kind, SyncStrategy};

/// Decode a length-prefixed frame log. Stops (without erroring) at a truncated
/// or undecodable tail, so a torn local append never poisons the whole file.
fn decode_log(bytes: &[u8]) -> Vec<Frame> {
    let mut out = Vec::new();
    let mut rest = bytes;
    while rest.len() >= 4 {
        let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        let Some(body) = rest.get(4..4 + len) else {
            break; // torn tail: length says more than the file holds
        };
        match postcard::from_bytes::<Frame>(body) {
            Ok(f) => out.push(f),
            Err(_) => break,
        }
        rest = &rest[4 + len..];
    }
    out
}

/// Encode frames as the on-disk log: `u32`-LE length + postcard, per frame.
fn encode_log(frames: &[&Frame]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in frames {
        let body = postcard::to_allocvec(f).unwrap_or_default();
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&body);
    }
    out
}

pub struct WalDbSyncer {
    replica: Replica,
    /// Per-writer watermarks of everything already shipped via `push_delta`
    /// (the WAL analogue of the text CRDT's `last_sent` version).
    last_pushed: BTreeMap<String, u64>,
    /// Last bytes read from / written to the backing, so re-observing our own
    /// materialized write-back short-circuits without re-decoding.
    last_known: Vec<u8>,
}

impl WalDbSyncer {
    pub fn new(_name: &str) -> Self {
        // The writer id lives *in* each frame (the local appender stamps it),
        // so unlike the text CRDT the adapter needs no agent name of its own.
        Self {
            replica: Replica::new(),
            last_pushed: BTreeMap::new(),
            last_known: Vec::new(),
        }
    }
}

impl SyncStrategy for WalDbSyncer {
    fn kind(&self) -> Kind {
        Kind::WalDb
    }

    fn observe(&mut self, current: &[u8]) -> bool {
        if current == self.last_known.as_slice() {
            return false;
        }
        self.last_known = current.to_vec();
        // Ingest every decodable frame; `add` is idempotent, so an empty or
        // brand-new file (no frames) and an echo of our own write-back both
        // land here as "nothing new".
        let mut changed = false;
        for frame in decode_log(current) {
            changed |= self.replica.add(frame);
        }
        changed
    }

    fn push_delta(&mut self) -> Option<Vec<u8>> {
        // An append-only log pushes naturally: everything above the watermarks
        // we last shipped. (Contrast rsync, which returns None here.)
        let missing = self.replica.missing_for(&self.last_pushed);
        if missing.is_empty() {
            return None;
        }
        let delta = postcard::to_allocvec(&missing).ok()?;
        self.last_pushed = self.replica.watermarks();
        Some(delta)
    }

    fn state_vector(&self) -> Vec<u8> {
        postcard::to_allocvec(&self.replica.watermarks()).unwrap_or_default()
    }

    fn delta_since(&self, theirs: &[u8]) -> Option<Vec<u8>> {
        let theirs: BTreeMap<String, u64> = postcard::from_bytes(theirs).ok()?;
        let missing = self.replica.missing_for(&theirs);
        if missing.is_empty() {
            return None; // they're caught up — send nothing
        }
        postcard::to_allocvec(&missing).ok()
    }

    fn merge(&mut self, delta: &[u8]) -> Option<Vec<u8>> {
        let frames: Vec<Frame> = postcard::from_bytes(delta).ok()?;
        let mut changed = false;
        for f in frames {
            changed |= self.replica.add(f);
        }
        if !changed {
            return None; // held every frame already — nothing to rewrite
        }
        // Advancing the push watermark past merged frames keeps us from
        // echoing them straight back (same move as the text CRDT's merge).
        self.last_pushed = self.replica.watermarks();
        let bytes = encode_log(&self.replica.linearize());
        self.last_known = bytes.clone();
        Some(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simulate the local process appending a frame to its on-disk log: `wal`
    /// tracks the writer's own causal state, `log` is the file's bytes.
    fn append_local(wal: &mut Replica, log: &mut Vec<u8>, writer: &str, payload: &[u8]) {
        let frame = wal.append(writer, payload.to_vec());
        log.extend_from_slice(&encode_log(&[&frame]));
    }

    /// Decode a materialized log back to `(writer, seq)` ids, in order.
    fn ids(log: &[u8]) -> Vec<(String, u64)> {
        decode_log(log).iter().map(|f| f.id()).collect()
    }

    /// Two adapters with concurrent local appends converge — both materialize
    /// the identical linearized log after a bidirectional reconcile.
    #[test]
    fn concurrent_appends_converge_on_one_linearized_log() {
        let mut a = WalDbSyncer::new("a");
        let mut b = WalDbSyncer::new("b");

        // Each side's local process appends concurrently.
        let (mut wa, mut la) = (Replica::new(), Vec::new());
        let (mut wb, mut lb) = (Replica::new(), Vec::new());
        append_local(&mut wa, &mut la, "a", b"a1");
        append_local(&mut wa, &mut la, "a", b"a2");
        append_local(&mut wb, &mut lb, "b", b"b1");
        assert!(a.observe(&la), "new local frames change A's state");
        assert!(b.observe(&lb), "new local frames change B's state");

        // Reconcile both directions (the heartbeat SYNC path).
        let for_b = a.delta_since(&b.state_vector()).expect("B lacks A's frames");
        let mb = b.merge(&for_b).expect("B materializes the merged log");
        let for_a = b.delta_since(&a.state_vector()).expect("A lacks B's frames");
        let ma = a.merge(&for_a).expect("A materializes the merged log");

        assert_eq!(ids(&ma), ids(&mb), "replicas diverged");
        assert_eq!(ma, mb, "materialized bytes differ");
        assert_eq!(ids(&ma).len(), 3, "a frame went missing");
        // Causality: a2 depended on a1, so it comes after.
        let ord = ids(&ma);
        let pos = |w: &str, s: u64| ord.iter().position(|x| *x == (w.to_string(), s)).unwrap();
        assert!(pos("a", 0) < pos("a", 1), "a1 must precede a2");
    }

    /// A fresh joiner (empty file → empty replica) is caught up in one pull.
    #[test]
    fn fresh_joiner_gets_the_whole_log() {
        let mut a = WalDbSyncer::new("a");
        let (mut wa, mut la) = (Replica::new(), Vec::new());
        append_local(&mut wa, &mut la, "a", b"one");
        append_local(&mut wa, &mut la, "a", b"two");
        a.observe(&la);

        let mut joiner = WalDbSyncer::new("j");
        assert!(!joiner.observe(b""), "an empty file is not a change");

        let delta = a.delta_since(&joiner.state_vector()).expect("joiner lacks everything");
        let log = joiner.merge(&delta).expect("joiner materializes the log");
        assert_eq!(ids(&log), vec![("a".to_string(), 0), ("a".to_string(), 1)]);

        // Echo of the write-back is not a local change.
        assert!(!joiner.observe(&log));
    }

    /// Once caught up, reconcile sends nothing in either direction, and a
    /// replayed delta is a no-op merge.
    #[test]
    fn no_op_reconcile_sends_nothing() {
        let mut a = WalDbSyncer::new("a");
        let mut b = WalDbSyncer::new("b");
        let (mut wa, mut la) = (Replica::new(), Vec::new());
        append_local(&mut wa, &mut la, "a", b"x");
        a.observe(&la);

        let delta = a.delta_since(&b.state_vector()).unwrap();
        assert!(b.merge(&delta).is_some());

        assert!(a.delta_since(&b.state_vector()).is_none(), "B is caught up");
        assert!(b.delta_since(&a.state_vector()).is_none(), "A is caught up");
        assert!(b.merge(&delta).is_none(), "replayed delta changes nothing");
    }

    /// The push path ships exactly the frames appended since the last push,
    /// then goes quiet until something new appears.
    #[test]
    fn push_delta_advances_the_watermark() {
        let mut a = WalDbSyncer::new("a");
        let mut b = WalDbSyncer::new("b");
        let (mut wa, mut la) = (Replica::new(), Vec::new());

        assert!(a.push_delta().is_none(), "nothing to push yet");
        append_local(&mut wa, &mut la, "a", b"first");
        a.observe(&la);
        let push = a.push_delta().expect("new frame to push");
        assert!(a.push_delta().is_none(), "watermark advanced — no re-push");

        let log = b.merge(&push).expect("push lands on B");
        assert_eq!(ids(&log), vec![("a".to_string(), 0)]);
        assert!(b.push_delta().is_none(), "merged frames aren't echoed back");
    }

    /// A torn tail (partial local append) doesn't poison the intact prefix.
    #[test]
    fn torn_tail_is_tolerated() {
        let mut wa = Replica::new();
        let mut la = Vec::new();
        append_local(&mut wa, &mut la, "a", b"good");
        la.extend_from_slice(&99u32.to_le_bytes()); // length with no body
        la.extend_from_slice(b"junk");

        let mut a = WalDbSyncer::new("a");
        assert!(a.observe(&la), "the intact frame is ingested");
        let wm = a.replica.watermarks();
        assert_eq!(wm.get("a"), Some(&0));
    }
}
