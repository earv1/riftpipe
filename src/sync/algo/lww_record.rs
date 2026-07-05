//! Per-key LWW merge for `key = value` structural files (roadmap #19) — IMPLEMENTED.
//!
//! For flat, section-less files like a TOML `meta.toml`, whole-file LWW
//! (rsync) loses one side of any concurrent edit. Here the unit of conflict is
//! a **record**: each well-formed `key = value` line is an independent
//! last-writer-wins register, so concurrent edits to *different* keys of the
//! same file both survive; only same-key conflicts pick a winner.
//!
//! ## Versioning (mirrors rsync.rs's conventions)
//! Each key carries a `(version, value-hash)` stamp. A local change to a key
//! bumps its version; higher version wins, ties break on the larger blake3
//! value hash — deterministic across peers. (A site id can't tie-break here:
//! `Kind::build` hands every replica the same resource name.) Deleting a key
//! locally writes a **tombstone** (a version bump with no value), so deletes
//! propagate instead of resurrecting.
//!
//! ## Parsing + canonicalization (documented contract)
//! No TOML crate: a record is any line whose first `=` splits it into a
//! non-empty trimmed key and a trimmed value. Everything else — comments,
//! blank lines, malformed lines — is ignored (never a record, never an error).
//! When a merge changes state, the file is rematerialized in **canonical
//! form**: one `key = value` line per live record, keys sorted; comments and
//! layout are not preserved. Duplicate keys: last occurrence wins at parse.
//!
//! ## Lineage
//! This absorbs the *model* of the former `algo/sqlite.rs` per-cell LWW engine
//! (each cell an independent `(lamport, site)` LWW register); that module and
//! its rusqlite dependency were **deleted** — its SQLite-bound machinery
//! (connections, `_rp_*` tables) didn't fit the byte-snapshot `SyncStrategy`
//! seam, so the idea was reimplemented here over parsed records.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::sync::strategy::{Kind, SyncStrategy};

/// One key's LWW register. `value: None` is a tombstone (the key was deleted
/// locally at `version`); tombstones sync like values but never materialize.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Rec {
    value: Option<String>,
    version: u64,
}

impl Rec {
    /// The deterministic LWW stamp: higher version wins, ties break on the
    /// larger value hash (same convention as rsync.rs's `wins`).
    fn stamp(&self) -> (u64, [u8; 16]) {
        (self.version, value_hash(&self.value))
    }
}

/// blake3-16 over a tag byte + the value bytes, so a tombstone never collides
/// with a real value.
fn value_hash(value: &Option<String>) -> [u8; 16] {
    let mut h = blake3::Hasher::new();
    match value {
        None => {
            h.update(&[0]);
        }
        Some(v) => {
            h.update(&[1]);
            h.update(v.as_bytes());
        }
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&h.finalize().as_bytes()[..16]);
    out
}

/// Parse the well-formed `key = value` lines (see module docs). Malformed
/// lines are skipped; duplicate keys keep the last occurrence.
fn parse_records(bytes: &[u8]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim();
            if !key.is_empty() && !key.starts_with('#') {
                out.insert(key.to_string(), v.trim().to_string());
            }
        }
    }
    out
}

/// Advertisement on the wire (`state_vector`): every key's LWW stamp.
#[derive(Serialize, Deserialize, Default)]
struct Advert {
    stamps: Vec<(String, u64, [u8; 16])>,
}

/// A patch on the wire (`delta_since` / `push_delta` / `merge`): the winning
/// records (values or tombstones) the peer is missing.
#[derive(Serialize, Deserialize)]
struct Patch {
    recs: Vec<(String, Rec)>,
}

pub struct LwwRecordSyncer {
    records: BTreeMap<String, Rec>,
    /// Keys changed locally since the last `push_delta` (the push watermark,
    /// like text_crdt/wal keep).
    dirty: Vec<String>,
}

impl LwwRecordSyncer {
    pub fn new(_name: &str) -> Self {
        Self {
            records: BTreeMap::new(),
            dirty: Vec::new(),
        }
    }

    /// The canonical file bytes: live records only, keys sorted, `key = value`.
    fn materialize(&self) -> Vec<u8> {
        let mut out = String::new();
        for (k, r) in &self.records {
            if let Some(v) = &r.value {
                out.push_str(k);
                out.push_str(" = ");
                out.push_str(v);
                out.push('\n');
            }
        }
        out.into_bytes()
    }
}

impl SyncStrategy for LwwRecordSyncer {
    fn kind(&self) -> Kind {
        Kind::LwwRecord
    }

    fn observe(&mut self, current: &[u8]) -> bool {
        let parsed = parse_records(current);
        let mut changed = false;
        // New or edited keys: bump their version so the local write wins LWW.
        for (k, v) in &parsed {
            let stale = match self.records.get(k) {
                Some(r) => r.value.as_deref() != Some(v.as_str()),
                None => true,
            };
            if stale {
                let version = self.records.get(k).map_or(0, |r| r.version) + 1;
                self.records
                    .insert(k.clone(), Rec { value: Some(v.clone()), version });
                self.dirty.push(k.clone());
                changed = true;
            }
        }
        // Keys that vanished locally become tombstones (deletes propagate).
        let gone: Vec<String> = self
            .records
            .iter()
            .filter(|(k, r)| r.value.is_some() && !parsed.contains_key(*k))
            .map(|(k, _)| k.clone())
            .collect();
        for k in gone {
            let version = self.records[&k].version + 1;
            self.records.insert(k.clone(), Rec { value: None, version });
            self.dirty.push(k);
            changed = true;
        }
        changed
    }

    fn push_delta(&mut self) -> Option<Vec<u8>> {
        if self.dirty.is_empty() {
            return None;
        }
        self.dirty.sort();
        self.dirty.dedup();
        let recs = self
            .dirty
            .drain(..)
            .filter_map(|k| self.records.get(&k).map(|r| (k.clone(), r.clone())))
            .collect();
        postcard::to_allocvec(&Patch { recs }).ok()
    }

    fn state_vector(&self) -> Vec<u8> {
        let stamps = self
            .records
            .iter()
            .map(|(k, r)| {
                let (v, h) = r.stamp();
                (k.clone(), v, h)
            })
            .collect();
        postcard::to_allocvec(&Advert { stamps }).unwrap_or_default()
    }

    fn delta_since(&self, theirs: &[u8]) -> Option<Vec<u8>> {
        let theirs: Advert = postcard::from_bytes(theirs).ok()?;
        let their_stamps: BTreeMap<&str, (u64, [u8; 16])> = theirs
            .stamps
            .iter()
            .map(|(k, v, h)| (k.as_str(), (*v, *h)))
            .collect();
        let recs: Vec<(String, Rec)> = self
            .records
            .iter()
            .filter(|(k, r)| match their_stamps.get(k.as_str()) {
                Some(t) => r.stamp() > *t, // ours strictly wins LWW
                None => true,              // they don't have the key at all
            })
            .map(|(k, r)| (k.clone(), r.clone()))
            .collect();
        if recs.is_empty() {
            return None;
        }
        postcard::to_allocvec(&Patch { recs }).ok()
    }

    fn merge(&mut self, delta: &[u8]) -> Option<Vec<u8>> {
        let patch: Patch = postcard::from_bytes(delta).ok()?;
        let before = self.materialize();
        for (k, incoming) in patch.recs {
            let wins = match self.records.get(&k) {
                Some(cur) => incoming.stamp() > cur.stamp(),
                None => true,
            };
            if wins {
                self.records.insert(k, incoming);
            }
        }
        let after = self.materialize();
        (after != before).then_some(after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sync both ways until stable (mirrors the heartbeat reconcile loop).
    fn reconcile(a: &mut LwwRecordSyncer, b: &mut LwwRecordSyncer) {
        for _ in 0..2 {
            if let Some(d) = a.delta_since(&b.state_vector()) {
                b.merge(&d);
            }
            if let Some(d) = b.delta_since(&a.state_vector()) {
                a.merge(&d);
            }
        }
    }

    /// The headline property: concurrent edits to *different* keys of the same
    /// file both survive on both sides (whole-file LWW would lose one).
    #[test]
    fn different_keys_both_survive() {
        let mut a = LwwRecordSyncer::new("meta.toml");
        let mut b = LwwRecordSyncer::new("meta.toml");
        let base = b"title = Board\nlanes = todo,doing,done\n";
        a.observe(base);
        b.observe(base);
        reconcile(&mut a, &mut b); // level the stamps before diverging

        a.observe(b"title = Renamed Board\nlanes = todo,doing,done\n");
        b.observe(b"title = Board\nlanes = todo,doing,done,archived\n");
        reconcile(&mut a, &mut b);

        let expect = b"lanes = todo,doing,done,archived\ntitle = Renamed Board\n";
        assert_eq!(a.materialize(), expect);
        assert_eq!(b.materialize(), expect);
    }

    /// Same key edited on both sides converges deterministically: version
    /// first, then the larger value hash breaks the tie.
    #[test]
    fn same_key_conflict_resolves_deterministically() {
        let mut a = LwwRecordSyncer::new("m");
        let mut b = LwwRecordSyncer::new("m");
        a.observe(b"title = Alpha\n"); // version 1 on both: a tie
        b.observe(b"title = Beta\n");
        reconcile(&mut a, &mut b);
        assert_eq!(a.materialize(), b.materialize(), "diverged");

        let winner = if value_hash(&Some("Alpha".into())) > value_hash(&Some("Beta".into())) {
            "title = Alpha\n"
        } else {
            "title = Beta\n"
        };
        assert_eq!(a.materialize(), winner.as_bytes());

        // A strictly later edit (higher version) wins regardless of hash.
        b.observe(b"title = Gamma\n");
        reconcile(&mut a, &mut b);
        assert_eq!(a.materialize(), b"title = Gamma\n");
        assert_eq!(b.materialize(), b"title = Gamma\n");
    }

    /// A fresh joiner (empty state) receives every key via the pull path.
    #[test]
    fn fresh_joiner_gets_all_keys() {
        let mut a = LwwRecordSyncer::new("m");
        let mut b = LwwRecordSyncer::new("m");
        a.observe(b"one = 1\ntwo = 2\nthree = 3\n");
        reconcile(&mut a, &mut b);
        assert_eq!(b.materialize(), b"one = 1\nthree = 3\ntwo = 2\n");
    }

    /// Once converged, neither side ships anything.
    #[test]
    fn nothing_to_send_when_in_sync() {
        let mut a = LwwRecordSyncer::new("m");
        let mut b = LwwRecordSyncer::new("m");
        a.observe(b"k = v\n");
        reconcile(&mut a, &mut b);
        assert!(a.delta_since(&b.state_vector()).is_none());
        assert!(b.delta_since(&a.state_vector()).is_none());
    }

    /// Comments, blank and malformed lines are skipped, never a crash; only
    /// well-formed records survive canonicalization (see module docs).
    #[test]
    fn malformed_lines_do_not_crash() {
        let mut a = LwwRecordSyncer::new("m");
        assert!(a.observe(
            b"# a comment\n\nno equals sign here\n = value with empty key\nok = yes\n# k = commented out\ndup = 1\ndup = 2\n"
        ));
        assert_eq!(a.materialize(), b"dup = 2\nok = yes\n");
        // Re-observing the canonical form is a no-op (idempotent).
        let canon = a.materialize();
        assert!(!a.observe(&canon));
        // Invalid UTF-8 is lossily decoded, not a panic.
        a.observe(&[0xff, 0xfe, b'k', b'=', b'v', b'\n']);
    }

    /// A deleted key tombstones and propagates instead of resurrecting.
    #[test]
    fn deletes_propagate_via_tombstones() {
        let mut a = LwwRecordSyncer::new("m");
        let mut b = LwwRecordSyncer::new("m");
        a.observe(b"keep = 1\ndrop = 2\n");
        reconcile(&mut a, &mut b);
        a.observe(b"keep = 1\n"); // `drop` deleted locally on a
        reconcile(&mut a, &mut b);
        assert_eq!(a.materialize(), b"keep = 1\n");
        assert_eq!(b.materialize(), b"keep = 1\n");
    }

    /// The eager push path ships exactly the changed-since-last-push keys.
    #[test]
    fn push_delta_ships_dirty_keys_once() {
        let mut a = LwwRecordSyncer::new("m");
        let mut b = LwwRecordSyncer::new("m");
        a.observe(b"x = 1\n");
        let d = a.push_delta().expect("changed keys to push");
        assert!(a.push_delta().is_none(), "watermark advanced");
        assert_eq!(b.merge(&d).as_deref(), Some(b"x = 1\n".as_slice()));
    }
}
