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
//!
//! A registered passkey can vouch for further devices (`Passkey` + `PasskeyLink`, `core::passkey`).

use std::collections::{HashMap, HashSet};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::passkey::{PasskeyAssertion, PasskeyCredential};

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

    /// Register `cred` as a board passkey conferring `role` (an Admin action —
    /// validated on apply): a device later presenting a fresh assertion from it
    /// joins with no approve click.
    pub fn register_passkey(&self, cred: PasskeyCredential, role: Role, nonce: [u8; 16]) -> TrustEntry {
        let by = self.id();
        let sig = self.sign(&passkey_msg(&cred, role, &by, &nonce)).to_vec();
        TrustEntry::Passkey { cred, role, by, nonce, sig }
    }
}

/// Verify `sig` by `id` over `msg`. `false` on a bad key, wrong-length, or bad
/// signature. Accepts `&[u8; 64]` (coerces) or a wire `Vec<u8>` slice.
pub fn verify(id: &DeviceId, msg: &[u8], sig: &[u8]) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(id) else { return false };
    let Ok(sig): Result<[u8; 64], _> = sig.try_into() else { return false };
    vk.verify_strict(msg, &Signature::from_bytes(&sig)).is_ok()
}

/// A write authenticated by its author — the wire wrapper that turns the trust
/// log's authority into enforcement. The `payload` is opaque (e.g. a postcard
/// `MeshMsg::Sync`); `author` + `sig` let a receiver prove who wrote it and drop
/// writes from keys the board hasn't authorized. Authorship is unforgeable:
/// claiming someone else's `author` fails [`verify`] (the sig won't match their
/// key), and claiming your own fails the trust-log check.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SignedWrite {
    pub author: DeviceId,
    pub sig: Vec<u8>,
    pub payload: Vec<u8>,
}

impl DeviceKey {
    /// Sign `payload` for authenticated broadcast.
    pub fn sign_write(&self, payload: Vec<u8>) -> SignedWrite {
        let sig = self.sign(&payload).to_vec();
        SignedWrite { author: self.id(), sig, payload }
    }
}

impl SignedWrite {
    /// The payload iff the signature is valid AND `log` authorizes the author —
    /// the check for **member mode** (writes require a trust-log signature).
    /// `None` (dropped) otherwise.
    pub fn verified<'a>(&'a self, log: &TrustLog) -> Option<&'a [u8]> {
        (log.is_authorized(&self.author) && verify(&self.author, &self.payload, &self.sig))
            .then_some(self.payload.as_slice())
    }

    /// Just check authorship (valid signature), ignoring authorization — for
    /// **Anyone-with-link Editor** mode, where writes are gated by epoch-key
    /// possession (sealing) rather than membership. Returns the proven author.
    pub fn author_verified(&self) -> Option<DeviceId> {
        verify(&self.author, &self.payload, &self.sig).then_some(self.author)
    }
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
    /// `by` (a current Admin) registers a passkey: any device that later presents a
    /// fresh assertion from it, bound to its own id ([`TrustEntry::PasskeyLink`]),
    /// joins as `role` — linking your own devices with no approve click.
    /// APPENDED (protocol v5).
    Passkey { cred: PasskeyCredential, role: Role, by: DeviceId, nonce: [u8; 16], sig: Vec<u8> },
    /// `device` joins vouched for by the registered passkey `cred_id`: `assertion`'s
    /// challenge is [`passkey_link_challenge`]`(founder, device, nonce)`. Verified
    /// offline against the registered public key; the passkey is the authority (no
    /// device signature). APPENDED (protocol v5).
    PasskeyLink { device: DeviceId, cred_id: Vec<u8>, nonce: [u8; 16], assertion: PasskeyAssertion },
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

fn passkey_msg(cred: &PasskeyCredential, role: Role, by: &DeviceId, nonce: &[u8; 16]) -> Vec<u8> {
    postcard::to_allocvec(&("riftpipe-passkey-v1", cred, role, by, nonce)).expect("encode")
}
/// The WebAuthn challenge a passkey signs to link `device` into the board founded by
/// `founder`. The founder scopes it to one board; `nonce` makes each link distinct.
pub fn passkey_link_challenge(founder: &DeviceId, device: &DeviceId, nonce: &[u8; 16]) -> [u8; 32] {
    let msg = postcard::to_allocvec(&("riftpipe-passkey-link-v1", founder, device, nonce)).expect("encode");
    *blake3::hash(&msg).as_bytes()
}

fn sig_of(e: &TrustEntry) -> &[u8] {
    match e {
        TrustEntry::Genesis { sig, .. }
        | TrustEntry::Grant { sig, .. }
        | TrustEntry::Revoke { sig, .. }
        | TrustEntry::Passkey { sig, .. } => sig,
        TrustEntry::PasskeyLink { assertion, .. } => &assertion.signature,
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


/// A passkey registered by an Admin — kept out of the public API; exposed via
/// [`TrustLog::passkeys`].
#[derive(Clone, Debug)]
struct RegisteredPasskey {
    cred: PasskeyCredential,
    role: Role,
    by: DeviceId,
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
    /// Devices removed by an applied Revoke (cleared by a later re-grant).
    revoked: HashSet<DeviceId>,
    /// Passkeys registered on this board, keyed by credential id.
    passkeys: HashMap<Vec<u8>, RegisteredPasskey>,
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

    /// Was `id` removed by an applied Revoke (and not re-granted since)? Distinct from
    /// "not authorized": a device whose grant simply hasn't arrived yet is NOT revoked.
    pub fn is_revoked(&self, id: &DeviceId) -> bool {
        self.revoked.contains(id)
    }

    /// Passkeys registered on this board with the role each confers, sorted by credential id.
    pub fn passkeys(&self) -> Vec<(PasskeyCredential, Role)> {
        let mut v: Vec<_> = self.passkeys.values().map(|r| (r.cred.clone(), r.role)).collect();
        v.sort_by(|a, b| a.0.id.cmp(&b.0.id));
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
                        self.revoked.remove(device);
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
                    self.revoked.insert(*device);
                    Outcome::Applied
                } else {
                    // `by` might be an admin we don't know yet, or a Member (never
                    // authorized to revoke). Defer; a Member's revoke never applies.
                    Outcome::Deferred
                }
            }
            TrustEntry::Passkey { cred, role, by, nonce, sig } => {
                if !verify(by, &passkey_msg(cred, *role, by, nonce), sig) || !cred.is_well_formed() {
                    return Outcome::Rejected;
                }
                match self.role(by) {
                    Some(Role::Admin) => {
                        // First registration of a credential id wins; a re-registration is a no-op.
                        self.passkeys
                            .entry(cred.id.clone())
                            .or_insert(RegisteredPasskey { cred: cred.clone(), role: *role, by: *by });
                        Outcome::Applied
                    }
                    _ => Outcome::Deferred,
                }
            }
            TrustEntry::PasskeyLink { device, cred_id, nonce, assertion } => {
                // Needs the genesis (binding scope) and the registration — may arrive later.
                let (Some(founder), Some(reg)) = (self.founder, self.passkeys.get(cred_id)) else {
                    return Outcome::Deferred;
                };
                let ok = reg.cred.verify(assertion, &passkey_link_challenge(&founder, device, nonce));
                let (by, role) = (reg.by, reg.role);
                if !ok {
                    return Outcome::Rejected;
                }
                // A passkey speaks only while its registrant is still an Admin.
                if self.role(&by) != Some(Role::Admin) {
                    return Outcome::Deferred;
                }
                if self.founder == Some(*device) {
                    return Outcome::Applied;
                }
                self.roles.insert(*device, role);
                self.revoked.remove(device);
                Outcome::Applied
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

    #[test]
    fn signed_write_is_accepted_only_from_an_authorized_author() {
        let founder = DeviceKey::generate();
        let member = DeviceKey::generate();
        let outsider = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(member.id(), Role::Member, nonce(1)));

        // Authorized member: accepted, payload returned intact.
        let w = member.sign_write(b"move card A".to_vec());
        assert_eq!(w.verified(&log), Some(&b"move card A"[..]));

        // Unauthorized key (not in the log): rejected even though its own
        // signature is valid.
        let w = outsider.sign_write(b"sneaky write".to_vec());
        assert!(w.author_verified().is_some(), "its signature is genuine");
        assert!(w.verified(&log).is_none(), "but it isn't authorized");
    }

    #[test]
    fn tampered_or_spoofed_writes_are_rejected() {
        let founder = DeviceKey::generate();
        let attacker = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());

        // Tampered payload: signature no longer matches.
        let mut w = founder.sign_write(b"legit".to_vec());
        w.payload = b"tampered".to_vec();
        assert!(w.verified(&log).is_none());
        assert!(w.author_verified().is_none());

        // Spoofed author: the attacker signs but claims to be the founder.
        let mut w = attacker.sign_write(b"forge as founder".to_vec());
        w.author = founder.id();
        assert!(w.author_verified().is_none(), "sig doesn't match the claimed author's key");
        assert!(w.verified(&log).is_none());
    }

    #[test]
    fn is_revoked_tracks_revoke_and_regrant() {
        let founder = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(phone.id(), Role::Member, nonce(1)));
        assert!(!log.is_revoked(&phone.id()));

        log.apply(founder.revoke(phone.id(), nonce(2)));
        assert!(log.is_revoked(&phone.id()));
        assert!(!log.is_authorized(&phone.id()));

        log.apply(founder.grant(phone.id(), Role::Member, nonce(3)));
        assert!(!log.is_revoked(&phone.id()));
        assert!(log.is_authorized(&phone.id()));
    }

    #[test]
    fn ungranted_is_not_revoked_and_member_revoke_does_not_mark() {
        let founder = DeviceKey::generate();
        let random = DeviceKey::generate();
        let member = DeviceKey::generate();
        let target = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(member.id(), Role::Member, nonce(1)));
        log.apply(founder.grant(target.id(), Role::Member, nonce(2)));

        // Never granted at all.
        assert!(!log.is_revoked(&random.id()));

        // A Member's revoke of another device is deferred and never applies.
        assert!(!log.apply(member.revoke(target.id(), nonce(3))));
        assert!(!log.is_revoked(&target.id()));
        assert!(log.is_authorized(&target.id()));
    }

    fn soft_passkey(seed: u8, rp: &str) -> (p256::ecdsa::SigningKey, PasskeyCredential) {
        let sk = p256::ecdsa::SigningKey::from_slice(&[seed; 32]).unwrap();
        let point = p256::ecdsa::VerifyingKey::from(&sk).to_encoded_point(false);
        let cred = PasskeyCredential {
            id: vec![seed; 16],
            alg: crate::passkey::ALG_ES256,
            public_key: point.as_bytes().to_vec(),
            rp_id: rp.into(),
        };
        (sk, cred)
    }
    fn soft_assert(sk: &p256::ecdsa::SigningKey, rp: &str, challenge: &[u8]) -> PasskeyAssertion {
        use p256::ecdsa::signature::Signer as _;
        let mut ad = crate::passkey::rp_id_hash(rp).to_vec();
        ad.extend_from_slice(&[0x05, 0, 0, 0, 0]);
        let cdj = format!(
            r#"{{"type":"webauthn.get","challenge":"{}","origin":"http://{rp}","crossOrigin":false}}"#,
            crate::passkey::b64url(challenge)
        )
        .into_bytes();
        let sig: p256::ecdsa::Signature = sk.sign(&crate::passkey::assertion_message(&ad, &cdj));
        PasskeyAssertion { authenticator_data: ad, client_data_json: cdj, signature: sig.to_der().as_bytes().to_vec() }
    }
    fn link(
        sk: &p256::ecdsa::SigningKey,
        cred: &PasskeyCredential,
        founder: &DeviceId,
        device: DeviceId,
        n: [u8; 16],
    ) -> TrustEntry {
        let ch = passkey_link_challenge(founder, &device, &n);
        TrustEntry::PasskeyLink { device, cred_id: cred.id.clone(), nonce: n, assertion: soft_assert(sk, &cred.rp_id, &ch) }
    }

    #[test]
    fn passkey_link_authorizes_a_device_with_the_registered_role() {
        let founder = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let third = DeviceKey::generate();
        let mut log = TrustLog::new();
        assert!(log.apply(founder.genesis()));
        let (sk, cred) = soft_passkey(1, "localhost");
        assert!(log.apply(founder.register_passkey(cred.clone(), Role::Admin, nonce(1))));
        assert!(log.apply(link(&sk, &cred, &founder.id(), phone.id(), nonce(2))));
        assert_eq!(log.role(&phone.id()), Some(Role::Admin));
        assert_eq!(log.passkeys().len(), 1);
        // The phone can then grant a third device.
        assert!(log.apply(phone.grant(third.id(), Role::Member, nonce(3))));
        assert_eq!(log.role(&third.id()), Some(Role::Member));
    }

    #[test]
    fn passkey_link_rejects_wrong_binding_key_or_rp() {
        let founder = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let attacker = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        let (sk, cred) = soft_passkey(1, "localhost");
        log.apply(founder.register_passkey(cred.clone(), Role::Admin, nonce(1)));

        // Bound to the phone's assertion but claiming the attacker's device.
        let mut wrong_device = link(&sk, &cred, &founder.id(), phone.id(), nonce(2));
        if let TrustEntry::PasskeyLink { device, .. } = &mut wrong_device {
            *device = attacker.id();
        }
        assert!(!log.apply(wrong_device));
        assert!(!log.is_authorized(&attacker.id()));

        // Signed by a different passkey, but with the registered cred id.
        let (other_sk, _) = soft_passkey(2, "localhost");
        assert!(!log.apply(link(&other_sk, &cred, &founder.id(), phone.id(), nonce(3))));
        assert!(!log.is_authorized(&phone.id()));

        // An assertion scoped to a different rp id.
        let ch = passkey_link_challenge(&founder.id(), &phone.id(), &nonce(4));
        let bad_rp = TrustEntry::PasskeyLink {
            device: phone.id(),
            cred_id: cred.id.clone(),
            nonce: nonce(4),
            assertion: soft_assert(&sk, "evil.example", &ch),
        };
        assert!(!log.apply(bad_rp));
        assert!(!log.is_authorized(&phone.id()));
    }

    #[test]
    fn passkey_registration_requires_an_admin() {
        let founder = DeviceKey::generate();
        let member = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(member.id(), Role::Member, nonce(1)));

        let (sk, cred) = soft_passkey(1, "localhost");
        assert!(!log.apply(member.register_passkey(cred.clone(), Role::Admin, nonce(2))), "a Member can't register");

        assert!(!log.apply(link(&sk, &cred, &founder.id(), phone.id(), nonce(3))));
        assert!(!log.is_authorized(&phone.id()));
    }

    #[test]
    fn passkey_entries_resolve_out_of_order() {
        let founder = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let (sk, cred) = soft_passkey(1, "localhost");

        let reg = founder.register_passkey(cred.clone(), Role::Admin, nonce(1));
        let ln = link(&sk, &cred, &founder.id(), phone.id(), nonce(2));
        let genesis = founder.genesis();

        let mut log = TrustLog::new();
        assert!(!log.apply(ln));
        assert!(!log.apply(reg));
        assert!(log.apply(genesis));
        assert_eq!(log.role(&phone.id()), Some(Role::Admin));
    }

    #[test]
    fn passkey_link_is_scoped_to_one_board() {
        let founder_a = DeviceKey::generate();
        let founder_b = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let (sk, cred) = soft_passkey(1, "localhost");

        let mut log_a = TrustLog::new();
        log_a.apply(founder_a.genesis());
        log_a.apply(founder_a.register_passkey(cred.clone(), Role::Admin, nonce(1)));

        let mut log_b = TrustLog::new();
        log_b.apply(founder_b.genesis());
        log_b.apply(founder_b.register_passkey(cred.clone(), Role::Admin, nonce(1)));

        let ln = link(&sk, &cred, &founder_a.id(), phone.id(), nonce(2));
        assert!(!log_b.apply(ln));
        assert!(!log_b.is_authorized(&phone.id()));
    }

    #[test]
    fn a_revoked_registrants_passkey_stops_linking() {
        let founder = DeviceKey::generate();
        let admin2 = DeviceKey::generate();
        let phone = DeviceKey::generate();
        let (sk, cred) = soft_passkey(1, "localhost");

        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        log.apply(founder.grant(admin2.id(), Role::Admin, nonce(1)));
        log.apply(admin2.register_passkey(cred.clone(), Role::Admin, nonce(2)));
        log.apply(founder.revoke(admin2.id(), nonce(3)));

        assert!(!log.apply(link(&sk, &cred, &founder.id(), phone.id(), nonce(4))));
        assert!(!log.is_authorized(&phone.id()));
    }
}
