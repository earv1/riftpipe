//! Device identity + the signed trust log — the foundation of **device-linking
//! auth** (`docs/planned/auth.md`). No identity provider, no capability-token
//! library: an identity is a self-generated Ed25519 keypair, and a board's
//! authorized set is an append-only log of **signed** entries rooted at a
//! self-signed **genesis** (the founder's first device).
//!
//! An identity is a *set* of device keys. A new device is added by an existing
//! **Admin** signing a [`TrustEntry::Grant`]; each device holds its own private
//! key (never copied between devices), so a lost device is revoked on its own
//! (`Revoke`) without touching the others. Two roles: **Admin** (may grant and
//! revoke) and **Member** (may read/write but not manage membership) — so
//! adding another *person* as a Member can't let them lock everyone out. A
//! recovery key is just an Admin device you registered offline.
//!
//! Pure-Rust, no I/O, no clock — callers persist the keypair seed and replicate
//! the log. Entries arrive over the mesh in any order, so [`TrustLog::apply`]
//! buffers ones it can't yet validate (a grant whose granter isn't known yet)
//! and re-drains to a fixpoint as their prerequisites land.
//!
//! *Not resolved in v1 (documented in the roadmap):* concurrent admin conflicts
//! (A revokes B while B revokes A) merge by set-union, not a canonical order —
//! deterministic conflict resolution is a follow-on.

use std::collections::{HashMap, HashSet};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

/// A device's public key — its stable identity on a board.
pub type DeviceId = [u8; 32];
/// An Ed25519 signature (64 bytes). On the wire in [`TrustEntry`] it's a
/// length-checked `Vec<u8>` (serde doesn't derive for `[u8; 64]`).
type Sig = [u8; 64];

/// A device's signing identity. Persist [`seed`](Self::seed) (32 secret bytes)
/// locally; it never leaves the device.
pub struct DeviceKey {
    signing: SigningKey,
}

impl DeviceKey {
    /// Generate a fresh device identity.
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        getrandom::getrandom(&mut seed).expect("OS RNG unavailable");
        Self { signing: SigningKey::from_bytes(&seed) }
    }

    /// Reconstruct from a persisted 32-byte seed.
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        Self { signing: SigningKey::from_bytes(seed) }
    }

    /// The secret seed to persist. Guard it like a password.
    pub fn seed(&self) -> [u8; 32] {
        self.signing.to_bytes()
    }

    /// This device's public id.
    pub fn id(&self) -> DeviceId {
        self.signing.verifying_key().to_bytes()
    }

    /// Sign an arbitrary payload (e.g. a mesh write) with this device's key.
    pub fn sign(&self, msg: &[u8]) -> Sig {
        self.signing.sign(msg).to_bytes()
    }

    /// Self-sign the board **genesis** — declares this device the founding Admin
    /// and the root of trust. Exactly one genesis per board.
    pub fn genesis(&self) -> TrustEntry {
        let founder = self.id();
        let sig = self.sign(&genesis_msg(&founder)).to_vec();
        TrustEntry::Genesis { founder, sig }
    }

    /// Grant `device` a role (an Admin action — validated on apply). `nonce`
    /// makes re-grants distinct; callers pass a fresh random one.
    pub fn grant(&self, device: DeviceId, role: Role, nonce: [u8; 16]) -> TrustEntry {
        let by = self.id();
        let sig = self.sign(&grant_msg(&device, role, &by, &nonce)).to_vec();
        TrustEntry::Grant { device, role, by, nonce, sig }
    }

    /// Revoke `device` (an Admin action, or a device removing itself).
    pub fn revoke(&self, device: DeviceId, nonce: [u8; 16]) -> TrustEntry {
        let by = self.id();
        let sig = self.sign(&revoke_msg(&device, &by, &nonce)).to_vec();
        TrustEntry::Revoke { device, by, nonce, sig }
    }
}

/// Verify `sig` by `id` over `msg`. `false` on a bad key, wrong-length, or bad
/// signature. Accepts `&[u8; 64]` (coerces) or a wire `Vec<u8>` slice.
pub fn verify(id: &DeviceId, msg: &[u8], sig: &[u8]) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(id) else { return false };
    let Ok(sig): Result<[u8; 64], _> = sig.try_into() else { return false };
    vk.verify_strict(msg, &Signature::from_bytes(&sig)).is_ok()
}

/// A device's authority on a board.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    /// May grant and revoke devices (and read/write).
    Admin,
    /// May read/write, but not manage membership.
    Member,
}

/// One signed record in the append-only trust log. Serialized (postcard) and
/// replicated like any file; every peer verifies signatures on apply.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum TrustEntry {
    /// The board root: the founding device, self-signed, permanently Admin.
    Genesis { founder: DeviceId, sig: Vec<u8> },
    /// `by` (must be a current Admin) grants `device` the given role.
    Grant { device: DeviceId, role: Role, by: DeviceId, nonce: [u8; 16], sig: Vec<u8> },
    /// `by` (a current Admin, or `device` itself) removes `device`.
    Revoke { device: DeviceId, by: DeviceId, nonce: [u8; 16], sig: Vec<u8> },
}

// Domain-separated to-be-signed payloads (postcard, so field order is fixed).
fn genesis_msg(founder: &DeviceId) -> Vec<u8> {
    postcard::to_allocvec(&("riftpipe-genesis-v1", founder)).expect("encode")
}
fn grant_msg(device: &DeviceId, role: Role, by: &DeviceId, nonce: &[u8; 16]) -> Vec<u8> {
    postcard::to_allocvec(&("riftpipe-grant-v1", device, role, by, nonce)).expect("encode")
}
fn revoke_msg(device: &DeviceId, by: &DeviceId, nonce: &[u8; 16]) -> Vec<u8> {
    postcard::to_allocvec(&("riftpipe-revoke-v1", device, by, nonce)).expect("encode")
}

fn sig_of(e: &TrustEntry) -> &[u8] {
    match e {
        TrustEntry::Genesis { sig, .. }
        | TrustEntry::Grant { sig, .. }
        | TrustEntry::Revoke { sig, .. } => sig,
    }
}

/// A dedup id for an entry — the hash of its (unique) signature.
fn entry_id(e: &TrustEntry) -> [u8; 32] {
    *blake3::hash(sig_of(e)).as_bytes()
}

enum Outcome {
    /// Valid and applied to the authorized set.
    Applied,
    /// Signature is valid but a prerequisite is missing (e.g. the granter isn't
    /// known yet) — buffer and retry when more entries arrive.
    Deferred,
    /// Invalid (bad signature, unauthorized granter, protected target) — drop.
    Rejected,
}

/// The authorized device set for one board, built from [`TrustEntry`]s.
#[derive(Default)]
pub struct TrustLog {
    founder: Option<DeviceId>,
    roles: HashMap<DeviceId, Role>,
    /// Entries whose signature checked out but that couldn't apply yet.
    pending: Vec<TrustEntry>,
    /// Dedup — entry ids already applied (idempotent replay).
    applied: HashSet<[u8; 32]>,
}

impl TrustLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Is `id` currently authorized (any role)?
    pub fn is_authorized(&self, id: &DeviceId) -> bool {
        self.roles.contains_key(id)
    }

    /// `id`'s current role, if authorized.
    pub fn role(&self, id: &DeviceId) -> Option<Role> {
        self.roles.get(id).copied()
    }

    /// The founding device, once a genesis has been applied.
    pub fn founder(&self) -> Option<DeviceId> {
        self.founder
    }

    /// All authorized devices with their roles, sorted by id for stable output.
    pub fn devices(&self) -> Vec<(DeviceId, Role)> {
        let mut v: Vec<_> = self.roles.iter().map(|(id, r)| (*id, *r)).collect();
        v.sort_by_key(|(id, _)| *id);
        v
    }

    /// Ingest one entry. Returns `true` if it is (now) applied. Invalid entries
    /// return `false`; validly-signed-but-not-yet-applicable ones are buffered
    /// and may apply later (also `false` for now). Idempotent on replays.
    pub fn apply(&mut self, entry: TrustEntry) -> bool {
        let id = entry_id(&entry);
        if self.applied.contains(&id) {
            return true;
        }
        match self.try_apply(&entry) {
            Outcome::Applied => {
                self.applied.insert(id);
                self.drain_pending();
                true
            }
            Outcome::Deferred => {
                self.pending.push(entry);
                false
            }
            Outcome::Rejected => false,
        }
    }

    /// Retry buffered entries until a full pass applies nothing new — so an
    /// out-of-order grant lands as soon as its granter becomes known.
    fn drain_pending(&mut self) {
        loop {
            let mut progressed = false;
            for e in std::mem::take(&mut self.pending) {
                let id = entry_id(&e);
                if self.applied.contains(&id) {
                    continue;
                }
                match self.try_apply(&e) {
                    Outcome::Applied => {
                        self.applied.insert(id);
                        progressed = true;
                    }
                    Outcome::Deferred => self.pending.push(e),
                    Outcome::Rejected => {}
                }
            }
            if !progressed {
                break;
            }
        }
    }

    /// Validate + apply a single entry against the current set (no buffering).
    fn try_apply(&mut self, entry: &TrustEntry) -> Outcome {
        match entry {
            TrustEntry::Genesis { founder, sig } => {
                if !verify(founder, &genesis_msg(founder), sig) {
                    return Outcome::Rejected;
                }
                // Exactly one genesis; a second (even a different founder) is ignored.
                if self.founder.is_some() {
                    return Outcome::Rejected;
                }
                self.founder = Some(*founder);
                self.roles.insert(*founder, Role::Admin);
                Outcome::Applied
            }
            TrustEntry::Grant { device, role, by, nonce, sig } => {
                if !verify(by, &grant_msg(device, *role, by, nonce), sig) {
                    return Outcome::Rejected;
                }
                // The founder is permanently Admin — a grant targeting it is a no-op.
                if self.founder == Some(*device) {
                    return Outcome::Applied;
                }
                match self.role(by) {
                    Some(Role::Admin) => {
                        self.roles.insert(*device, *role);
                        Outcome::Applied
                    }
                    // Granter not (yet) a known Admin: its own grant may still
                    // arrive — defer. (A Member granter will simply never apply.)
                    _ => Outcome::Deferred,
                }
            }
            TrustEntry::Revoke { device, by, nonce, sig } => {
                if !verify(by, &revoke_msg(device, by, nonce), sig) {
                    return Outcome::Rejected;
                }
                // The founder can't be revoked (prevents locking out the root).
                if self.founder == Some(*device) {
                    return Outcome::Rejected;
                }
                let by_is_admin = self.role(by) == Some(Role::Admin);
                let self_leave = by == device;
                if by_is_admin || self_leave {
                    self.roles.remove(device);
                    Outcome::Applied
                } else {
                    // `by` might be an admin we don't know yet, or a Member (never
                    // authorized to revoke). Defer; a Member's revoke never applies.
                    Outcome::Deferred
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonce(n: u8) -> [u8; 16] {
        [n; 16]
    }

    #[test]
    fn genesis_authorizes_founder_as_admin() {
        let founder = DeviceKey::generate();
        let mut log = TrustLog::new();
        assert!(log.apply(founder.genesis()));
        assert_eq!(log.role(&founder.id()), Some(Role::Admin));
        assert_eq!(log.founder(), Some(founder.id()));
    }

    #[test]
    fn admin_grant_authorizes_new_device() {
        let founder = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        assert!(log.apply(founder.grant(phone.id(), Role::Member, nonce(1))));
        assert_eq!(log.role(&phone.id()), Some(Role::Member));
    }

    #[test]
    fn a_member_cannot_grant_or_revoke() {
        let founder = DeviceKey::generate();
        let member = DeviceKey::generate();
        let outsider = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(member.id(), Role::Member, nonce(1)));

        // A Member granting a third device: never authorized.
        assert!(!log.apply(member.grant(outsider.id(), Role::Member, nonce(2))));
        assert!(!log.is_authorized(&outsider.id()));

        // A Member revoking another device: never applies.
        assert!(!log.apply(member.revoke(founder.id(), nonce(3))));
        assert!(log.is_authorized(&founder.id()));
    }

    #[test]
    fn admin_grant_lets_the_new_admin_link_further() {
        // Founder → laptop (Admin); laptop links phone — the transitive
        // "link my own devices" case.
        let founder = DeviceKey::generate();
        let laptop = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(laptop.id(), Role::Admin, nonce(1)));
        assert!(log.apply(laptop.grant(phone.id(), Role::Member, nonce(2))));
        assert_eq!(log.role(&phone.id()), Some(Role::Member));
    }

    #[test]
    fn unauthorized_grant_is_rejected() {
        let founder = DeviceKey::generate();
        let attacker = DeviceKey::generate();
        let victim = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        // A random key grants itself power: not applied (its grant never validates).
        assert!(!log.apply(attacker.grant(victim.id(), Role::Admin, nonce(1))));
        assert!(!log.is_authorized(&victim.id()));
    }

    #[test]
    fn out_of_order_entries_resolve_to_fixpoint() {
        // Deliver grants BEFORE the genesis + granter they depend on; the buffer
        // must re-drain and authorize everyone once prerequisites land.
        let founder = DeviceKey::generate();
        let laptop = DeviceKey::generate();
        let phone = DeviceKey::generate();

        let g_phone = laptop.grant(phone.id(), Role::Member, nonce(2)); // needs laptop
        let g_laptop = founder.grant(laptop.id(), Role::Admin, nonce(1)); // needs founder
        let genesis = founder.genesis();

        let mut log = TrustLog::new();
        assert!(!log.apply(g_phone)); // deferred (laptop unknown)
        assert!(!log.apply(g_laptop)); // deferred (founder unknown)
        assert!(log.apply(genesis)); // unlocks the cascade

        assert_eq!(log.role(&laptop.id()), Some(Role::Admin));
        assert_eq!(log.role(&phone.id()), Some(Role::Member), "buffered grant applied on drain");
    }

    #[test]
    fn revoke_removes_a_device_and_self_leave_works() {
        let founder = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let tablet = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(phone.id(), Role::Member, nonce(1)));
        log.apply(founder.grant(tablet.id(), Role::Member, nonce(2)));

        // Admin revokes a lost phone.
        assert!(log.apply(founder.revoke(phone.id(), nonce(3))));
        assert!(!log.is_authorized(&phone.id()));

        // A device removes itself.
        assert!(log.apply(tablet.revoke(tablet.id(), nonce(4))));
        assert!(!log.is_authorized(&tablet.id()));
    }

    #[test]
    fn the_founder_cannot_be_revoked() {
        let founder = DeviceKey::generate();
        let admin2 = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(admin2.id(), Role::Admin, nonce(1)));
        // Even another Admin can't lock out the root.
        assert!(!log.apply(admin2.revoke(founder.id(), nonce(2))));
        assert!(log.is_authorized(&founder.id()));
    }

    #[test]
    fn tampered_entry_is_rejected() {
        let founder = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        let mut g = founder.grant(phone.id(), Role::Member, nonce(1));
        if let TrustEntry::Grant { sig, .. } = &mut g {
            sig[0] ^= 0x01; // flip a signature bit
        }
        assert!(!log.apply(g));
        assert!(!log.is_authorized(&phone.id()));
    }

    #[test]
    fn recovery_key_is_just_an_admin_device() {
        // At setup the founder registers an offline recovery key as an Admin.
        // Later, with all normal devices lost, the recovery key links a new one.
        let founder = DeviceKey::generate();
        let recovery = DeviceKey::generate();
        let new_device = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(recovery.id(), Role::Admin, nonce(1)));

        // "lost all devices" — the recovery key re-provisions.
        assert!(log.apply(recovery.grant(new_device.id(), Role::Admin, nonce(2))));
        assert_eq!(log.role(&new_device.id()), Some(Role::Admin));
    }

    #[test]
    fn apply_is_idempotent_and_a_second_genesis_is_ignored() {
        let founder = DeviceKey::generate();
        let other = DeviceKey::generate();
        let mut log = TrustLog::new();
        let g = founder.genesis();
        assert!(log.apply(g.clone()));
        assert!(log.apply(g), "replaying the same entry is a no-op that still reports applied");
        // A competing genesis from a different device is ignored.
        assert!(!log.apply(other.genesis()));
        assert_eq!(log.founder(), Some(founder.id()));
        assert!(!log.is_authorized(&other.id()));
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let key = DeviceKey::generate();
        let sig = key.sign(b"a mesh write");
        assert!(verify(&key.id(), b"a mesh write", &sig));
        assert!(!verify(&key.id(), b"tampered", &sig));
        let other = DeviceKey::generate();
        assert!(!verify(&other.id(), b"a mesh write", &sig), "wrong signer rejected");
    }

    #[test]
    fn persisted_seed_reconstructs_the_same_identity() {
        let key = DeviceKey::generate();
        let restored = DeviceKey::from_seed(&key.seed());
        assert_eq!(key.id(), restored.id());
        let sig = restored.sign(b"x");
        assert!(verify(&key.id(), b"x", &sig));
    }
}
