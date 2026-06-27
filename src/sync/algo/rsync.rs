//! rsync-style file sync (DESIGN.md §17.2) — IMPLEMENTED.
//!
//! The classic rsync algorithm, over opaque byte buffers (a file *or* an
//! in-memory backing):
//!   1. The side that may be out of date advertises **block signatures** of its
//!      content: a cheap rolling **weak** checksum + a strong **blake3** hash per
//!      fixed-size block ([`signatures`], carried by [`Syncer::state_vector`]).
//!   2. The other side rolls a window byte-by-byte over *its* content, and where
//!      the weak (then strong) checksum matches a known block, emits a `Copy`
//!      referencing that block; the gaps become `Literal` bytes ([`diff`], via
//!      [`Syncer::delta_since`]).
//!   3. The first side reconstructs from its own blocks + the literals
//!      ([`reconstruct`], via [`Syncer::merge`]).
//!
//! ## rsync is replication, not merge — so we add an order
//! Unlike the text CRDT, rsync has no notion of merging concurrent edits: it
//! makes one buffer equal another. Run naively in a *bidirectional* reconcile
//! loop, two divergent peers would just swap contents forever. So each replica
//! carries a `(version, content-hash)` stamp and we apply **last-writer-wins**:
//! a local change bumps `version`; ties break on the larger hash. That makes the
//! register a deterministic LWW value — it converges. (DESIGN.md §17.2 spells
//! out the v1 caveat: a `Copy` references the *advertiser's* blocks, so if its
//! content changed between advertising and applying, reconstruction is rejected
//! by a hash check and the next heartbeat SYNC retries.)

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::sync::syncer::{Kind, Syncer};

/// Fixed block size. Small enough to dedup fine-grained edits, large enough to
/// keep signature overhead sane.
pub const BLOCK: usize = 1024;

/// A rolling weak checksum (rsync's adler-style sum), so the window can advance
/// one byte at a time without rehashing the whole block.
struct Rolling {
    a: u32,
    b: u32,
    len: u32,
}

impl Rolling {
    fn new(block: &[u8]) -> Self {
        let len = block.len() as u32;
        let (mut a, mut b) = (0u32, 0u32);
        for (i, &x) in block.iter().enumerate() {
            a = a.wrapping_add(x as u32);
            b = b.wrapping_add((len - i as u32).wrapping_mul(x as u32));
        }
        Rolling {
            a: a & 0xffff,
            b: b & 0xffff,
            len,
        }
    }

    fn digest(&self) -> u32 {
        (self.a & 0xffff) | ((self.b & 0xffff) << 16)
    }

    /// Slide the window: drop `old` at the front, take `new` at the back.
    fn roll(&mut self, old: u8, new: u8) {
        self.a = self.a.wrapping_sub(old as u32).wrapping_add(new as u32) & 0xffff;
        self.b = self
            .b
            .wrapping_sub(self.len.wrapping_mul(old as u32))
            .wrapping_add(self.a)
            & 0xffff;
    }
}

fn strong_hash(block: &[u8]) -> [u8; 16] {
    let h = blake3::hash(block);
    let mut out = [0u8; 16];
    out.copy_from_slice(&h.as_bytes()[..16]);
    out
}

#[derive(Serialize, Deserialize, Clone)]
struct BlockSig {
    weak: u32,
    strong: [u8; 16],
}

/// Block signatures of a buffer — only **full** blocks (the partial tail is
/// never a `Copy` target; it falls out as literals, and reconstruction rebuilds
/// the whole target anyway).
#[derive(Serialize, Deserialize, Clone)]
pub struct Signatures {
    block: usize,
    sigs: Vec<BlockSig>,
}

pub fn signatures(data: &[u8]) -> Signatures {
    let mut sigs = Vec::new();
    let full = data.len() / BLOCK;
    for k in 0..full {
        let b = &data[k * BLOCK..(k + 1) * BLOCK];
        sigs.push(BlockSig {
            weak: Rolling::new(b).digest(),
            strong: strong_hash(b),
        });
    }
    Signatures { block: BLOCK, sigs }
}

#[derive(Serialize, Deserialize, Clone)]
enum Token {
    /// Reuse full block `index` from the basis (the advertiser's content).
    Copy(usize),
    /// Insert these literal bytes verbatim.
    Literal(Vec<u8>),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Delta {
    tokens: Vec<Token>,
}

/// Express `target` in terms of `basis` blocks (the rsync sender side).
pub fn diff(basis: &Signatures, target: &[u8]) -> Delta {
    let block = basis.block;
    let mut tokens = Vec::new();

    // weak checksum -> candidate (block index, strong hash). Many blocks can
    // share a weak checksum; the strong hash disambiguates.
    let mut by_weak: HashMap<u32, Vec<(usize, [u8; 16])>> = HashMap::new();
    for (i, s) in basis.sigs.iter().enumerate() {
        by_weak.entry(s.weak).or_default().push((i, s.strong));
    }

    if target.len() < block || basis.sigs.is_empty() {
        if !target.is_empty() {
            tokens.push(Token::Literal(target.to_vec()));
        }
        return Delta { tokens };
    }

    let mut i = 0usize; // window start
    let mut lit_start = 0usize; // start of the pending literal run
    let mut roll = Rolling::new(&target[0..block]);

    while i + block <= target.len() {
        let mut matched = None;
        if let Some(cands) = by_weak.get(&roll.digest()) {
            let strong = strong_hash(&target[i..i + block]);
            for (idx, s) in cands {
                if *s == strong {
                    matched = Some(*idx);
                    break;
                }
            }
        }
        if let Some(idx) = matched {
            if lit_start < i {
                tokens.push(Token::Literal(target[lit_start..i].to_vec()));
            }
            tokens.push(Token::Copy(idx));
            i += block;
            lit_start = i;
            if i + block <= target.len() {
                roll = Rolling::new(&target[i..i + block]);
            }
        } else {
            if i + block < target.len() {
                roll.roll(target[i], target[i + block]);
            }
            i += 1;
        }
    }
    if lit_start < target.len() {
        tokens.push(Token::Literal(target[lit_start..].to_vec()));
    }
    Delta { tokens }
}

/// Rebuild the target from `basis` (the advertiser's content) + the delta.
pub fn reconstruct(basis: &[u8], delta: &Delta, block: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for t in &delta.tokens {
        match t {
            Token::Copy(i) => {
                let (start, end) = (i * block, i * block + block);
                if end <= basis.len() {
                    out.extend_from_slice(&basis[start..end]);
                }
                // else: stale index (basis moved) — leave it; merge's hash check
                // rejects the result and the next SYNC retries.
            }
            Token::Literal(b) => out.extend_from_slice(b),
        }
    }
    out
}

// --- LWW ordering ---------------------------------------------------------

/// Does `(va, ha)` win over `(vb, hb)`? Higher version wins; ties break on the
/// larger content hash (deterministic across peers).
fn wins(va: u64, ha: &[u8; 32], vb: u64, hb: &[u8; 32]) -> bool {
    (va, ha) > (vb, hb)
}

fn content_hash(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Advertisement on the wire (`state_vector`): our stamp + our block signatures.
#[derive(Serialize, Deserialize)]
struct Advert {
    version: u64,
    hash: [u8; 32],
    sigs: Signatures,
}

/// A patch on the wire (`delta_since` / `merge`): our stamp + the rsync tokens.
#[derive(Serialize, Deserialize)]
struct Patch {
    version: u64,
    hash: [u8; 32],
    delta: Delta,
}

// --- the Syncer adapter ---------------------------------------------------

pub struct RsyncSyncer {
    current: Vec<u8>,
    /// Logical clock: bumped on every local change, so the latest writer wins.
    version: u64,
}

impl RsyncSyncer {
    pub fn new(_name: &str) -> Self {
        Self {
            current: Vec::new(),
            version: 0,
        }
    }
}

impl Syncer for RsyncSyncer {
    fn kind(&self) -> Kind {
        Kind::RsyncFile
    }

    fn observe(&mut self, current: &[u8]) -> bool {
        if current == self.current.as_slice() {
            return false;
        }
        self.current = current.to_vec();
        self.version += 1; // a local write makes us the latest writer
        true
    }

    fn push_delta(&mut self) -> Option<Vec<u8>> {
        None // rsync is pull-only: it needs the peer's block signatures first
    }

    fn state_vector(&self) -> Vec<u8> {
        let advert = Advert {
            version: self.version,
            hash: content_hash(&self.current),
            sigs: signatures(&self.current),
        };
        postcard::to_allocvec(&advert).unwrap_or_default()
    }

    fn delta_since(&self, theirs: &[u8]) -> Option<Vec<u8>> {
        let theirs: Advert = postcard::from_bytes(theirs).ok()?;
        let ours = content_hash(&self.current);
        if ours == theirs.hash {
            return None; // identical content
        }
        if !wins(self.version, &ours, theirs.version, &theirs.hash) {
            return None; // they're newer (or win the tie) — don't clobber them
        }
        let patch = Patch {
            version: self.version,
            hash: ours,
            delta: diff(&theirs.sigs, &self.current),
        };
        postcard::to_allocvec(&patch).ok()
    }

    fn merge(&mut self, delta: &[u8]) -> Option<Vec<u8>> {
        let patch: Patch = postcard::from_bytes(delta).ok()?;
        let ours = content_hash(&self.current);
        if patch.hash == ours {
            return None; // already identical
        }
        if !wins(patch.version, &patch.hash, self.version, &ours) {
            return None; // we're newer — keep ours
        }
        let rebuilt = reconstruct(&self.current, &patch.delta, BLOCK);
        if content_hash(&rebuilt) != patch.hash {
            return None; // stale basis — reconstruction didn't match; retry on next SYNC
        }
        self.current = rebuilt.clone();
        self.version = patch.version;
        Some(rebuilt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rolling one byte must match a fresh checksum of the same window.
    #[test]
    fn rolling_checksum_matches_fresh() {
        let data = b"the quick brown fox jumps over the lazy dog!!";
        let w = 8;
        let mut roll = Rolling::new(&data[0..w]);
        for i in 0..data.len() - w {
            assert_eq!(
                roll.digest(),
                Rolling::new(&data[i..i + w]).digest(),
                "rolling diverged at offset {i}"
            );
            if i + w < data.len() {
                roll.roll(data[i], data[i + w]);
            }
        }
    }

    fn round_trip(basis: &[u8], target: &[u8]) -> Vec<u8> {
        let sigs = signatures(basis);
        let delta = diff(&sigs, target);
        reconstruct(basis, &delta, BLOCK)
    }

    #[test]
    fn reconstruct_round_trips_for_all_shapes() {
        // build a multi-block basis (>1 full block of structured bytes)
        let basis: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();

        // identical
        assert_eq!(round_trip(&basis, &basis), basis);

        // append
        let mut t = basis.clone();
        t.extend_from_slice(b"appended tail bytes");
        assert_eq!(round_trip(&basis, &t), t);

        // prepend
        let mut t = b"PREPENDED".to_vec();
        t.extend_from_slice(&basis);
        assert_eq!(round_trip(&basis, &t), t);

        // middle insert
        let mut t = basis.clone();
        t.splice(1500..1500, b"XXXXXMIDDLEXXXXX".iter().copied());
        assert_eq!(round_trip(&basis, &t), t);

        // middle delete
        let mut t = basis.clone();
        t.drain(1200..1400);
        assert_eq!(round_trip(&basis, &t), t);

        // totally different
        let t: Vec<u8> = (0..2048u32).map(|i| (i % 97 + 1) as u8).collect();
        assert_eq!(round_trip(&basis, &t), t);

        // empties / small
        assert_eq!(round_trip(&basis, b""), b"");
        assert_eq!(round_trip(b"", &basis), basis);
        assert_eq!(round_trip(&basis, b"tiny"), b"tiny");
    }

    #[test]
    fn delta_reuses_blocks_instead_of_resending_everything() {
        let basis: Vec<u8> = (0..6000u32).map(|i| (i % 251) as u8).collect();
        let mut target = basis.clone();
        target[3000] ^= 0xff; // one byte changes, in one block
        let delta = diff(&signatures(&basis), &target);
        let encoded = postcard::to_allocvec(&delta).unwrap();
        assert!(
            encoded.len() < target.len() / 2,
            "delta should reuse unchanged blocks: {} bytes vs target {} bytes",
            encoded.len(),
            target.len()
        );
        assert_eq!(reconstruct(&basis, &delta, BLOCK), target);
    }

    /// Two replicas reconcile to the same content (LWW winner), both directions.
    #[test]
    fn two_replicas_converge_via_lww() {
        let mut a = RsyncSyncer::new("a");
        let mut b = RsyncSyncer::new("b");

        // Each makes its first edit (both go version 0 -> 1), so this is a *tie*
        // resolved deterministically by the larger content hash.
        let ca0 = b"alpha content here".to_vec();
        let cb0 = b"beta content over there, different".to_vec();
        assert!(a.observe(&ca0));
        assert!(b.observe(&cb0));

        // Reconcile both directions until stable (mirrors the heartbeat loop).
        for _ in 0..3 {
            if let Some(p) = a.delta_since(&b.state_vector()) {
                b.merge(&p);
            }
            if let Some(p) = b.delta_since(&a.state_vector()) {
                a.merge(&p);
            }
        }

        let ca = a.merge_peek();
        let cb = b.merge_peek();
        assert_eq!(ca, cb, "replicas diverged");
        let expected = if content_hash(&ca0) > content_hash(&cb0) { ca0 } else { cb0 };
        assert_eq!(cb, expected, "LWW (hash-tiebreak) winner mismatch");
    }

    /// A strictly-later writer (higher version) wins regardless of hash.
    #[test]
    fn later_writer_wins_on_version() {
        let mut a = RsyncSyncer::new("a");
        let mut b = RsyncSyncer::new("b");
        a.observe(b"first");
        // bring b in sync with a
        if let Some(p) = a.delta_since(&b.state_vector()) {
            b.merge(&p);
        }
        // now b writes again -> version 2 > a's version 1
        b.observe(b"second, newer, wins");
        for _ in 0..3 {
            if let Some(p) = a.delta_since(&b.state_vector()) {
                b.merge(&p);
            }
            if let Some(p) = b.delta_since(&a.state_vector()) {
                a.merge(&p);
            }
        }
        assert_eq!(a.merge_peek(), b"second, newer, wins");
        assert_eq!(a.merge_peek(), b.merge_peek());
    }

    #[test]
    fn identical_content_produces_no_delta() {
        let mut a = RsyncSyncer::new("a");
        let mut b = RsyncSyncer::new("b");
        a.observe(b"same");
        // get B to the same content+version via one reconcile
        if let Some(p) = a.delta_since(&b.state_vector()) {
            b.merge(&p);
        }
        // now neither side has anything to send
        assert!(a.delta_since(&b.state_vector()).is_none());
        assert!(b.delta_since(&a.state_vector()).is_none());
    }

    impl RsyncSyncer {
        fn merge_peek(&self) -> Vec<u8> {
            self.current.clone()
        }
    }
}
