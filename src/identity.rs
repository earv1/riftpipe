//! The three identities (DESIGN.md §7). Kept distinct on purpose — each prevents
//! a different failure mode.
//!
//! NOTE: these are placeholder representations (u64 hashes) so the core builds
//! offline. Real versions: AgentId = ed25519 public key, DocId = 256-bit secret,
//! RuleFingerprint = blake3 of the conformance spec.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Who authored an op. Must be unique per writer — two writers sharing an
/// AgentId collide on `(agent, seq)` and corrupt the tie-break. TODO: ed25519.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct AgentId(pub u64);

impl AgentId {
    /// Placeholder constructor. Real impl derives this from a persisted keypair.
    pub fn from_name(name: &str) -> Self {
        let mut h = DefaultHasher::new();
        name.hash(&mut h);
        AgentId(h.finish())
    }
}

/// Which shared object this is. Being secret, it doubles as the access
/// capability (wormhole model). TODO: 256-bit secret.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct DocId(pub u64);

/// What semantics govern the document — a hash of the conformance spec, NOT the
/// binary, so cross-language impls that pass the same suite share an identity
/// (DESIGN.md §7/§8). TODO: blake3 of the suite.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct RuleFingerprint(pub u64);

/// What a creator hands out to invite a peer (DESIGN.md §7).
#[derive(Clone, Copy, Debug)]
pub struct Ticket {
    pub doc: DocId,
    pub rule: RuleFingerprint,
    // TODO: creator NodeId / relay hint (iroh 1.0).
}
