//! Epoch-sealed content for restricted boards (see the plan's settled design,
//! points 1–8): on a restricted board, `Sync`/`SignedSync` content is sealed a
//! second time, inside the outer board-secret frame, under the current
//! **epoch key**. The epoch is identified inside the inner envelope
//! ([`SealedMsg`]), not in the outer frame header, so the rendezvous topic
//! never rotates — only who can read does. Wrapped epoch keys travel in an
//! Admin-signed [`EpochAnnounce`], and a device is wrapped in only once an
//! Admin has a verified binding from its device id to its encryption key (its
//! own signed `JoinRequest`). Rotation happens when the latest epoch a
//! device holds still wraps a now-revoked device; older epochs stay openable
//! for the rest of the session so in-flight content never breaks. The whole
//! scheme is gated by one switch, [`AccessMode`], so a future general-access
//! "Viewer" mode is just another variant. [`EpochBook`] is the per-device state
//! machine both drivers (native `TreePeer`, browser `Auth`) run.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::crypto::MeshCipher;
use crate::identity::{verify, DeviceId, DeviceKey, Role, TrustLog};
use crate::keywrap::{generate_epoch, open_epoch, wrap_epoch, ClientEncKey, EncPublic, EpochKey, WrappedEpoch};
use crate::onboard::JoinRequest;
use crate::sync::MeshMsg;

/// 8-byte public fingerprint of an epoch key (identifies it without revealing it).
pub type EpochId = [u8; 8];
const EPOCH_ID_CONTEXT: &str = "riftpipe epoch id v1";

/// How a board gates reads/writes — the ONE switch drivers consult (the
/// general-access dial). Only `LinkEditor` and `Restricted` are reachable today.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AccessMode {
    /// Anyone with the link reads and writes (open board; the default).
    LinkEditor,
    /// Anyone with the link reads; only trust-log members write. Reserved: needs an
    /// admin-signed access setting before it is reachable.
    LinkViewer,
    /// Only devices wrapped into the current epoch read; only members write.
    Restricted,
}
impl AccessMode {
    pub fn seals_content(self) -> bool {
        self == AccessMode::Restricted
    }
    pub fn requires_signed_writes(self) -> bool {
        self != AccessMode::LinkEditor
    }
}

pub fn epoch_id(key: &EpochKey) -> EpochId {
    let d = blake3::derive_key(EPOCH_ID_CONTEXT, key);
    let mut id = [0u8; 8];
    id.copy_from_slice(&d[..8]);
    id
}

/// One device's wrapped copy of an epoch key.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EpochWrap {
    pub device: DeviceId,
    pub wrapped: WrappedEpoch,
}

/// An Admin-signed epoch announcement: a new epoch (rotation) or extra wraps for
/// an existing one (add-wrap: same seq+id, only the new wraps).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EpochAnnounce {
    pub seq: u64,
    pub id: EpochId,
    pub wraps: Vec<EpochWrap>,
    pub by: DeviceId,
    pub sig: Vec<u8>,
}
fn announce_msg(seq: u64, id: &EpochId, wraps: &[EpochWrap], by: &DeviceId) -> Vec<u8> {
    postcard::to_allocvec(&("riftpipe-epoch-v1", seq, id, wraps, by)).expect("encode")
}
impl EpochAnnounce {
    pub fn create(admin: &DeviceKey, seq: u64, id: EpochId, wraps: Vec<EpochWrap>) -> Self {
        let by = admin.id();
        let sig = admin.sign(&announce_msg(seq, &id, &wraps, &by)).to_vec();
        EpochAnnounce { seq, id, wraps, by, sig }
    }
    /// Authentic signature by `by` (authorization is checked against the trust log
    /// by the receiver, not here).
    pub fn verify_sig(&self) -> bool {
        verify(&self.by, &announce_msg(self.seq, &self.id, &self.wraps, &self.by), &self.sig)
    }
}

/// Content sealed under epoch `(seq, id)`; `body` = `MeshCipher::from_epoch(key)
/// .seal_aad(postcard(MeshMsg::Sync|SignedSync), sealed_aad(seq, id))`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SealedMsg {
    pub seq: u64,
    pub id: EpochId,
    pub body: Vec<u8>,
}

pub(crate) fn sealed_aad(seq: u64, id: &EpochId) -> Vec<u8> {
    postcard::to_allocvec(&("riftpipe-sealed-v1", seq, id)).expect("encode")
}

/// Sealed messages held while their epoch key hasn't arrived yet.
const PARKED_CAP: usize = 128;
/// Announces with a valid signature whose signer isn't (yet) an Admin.
const PENDING_CAP: usize = 256;
/// An epoch's identity: `(seq, id)`. Ordered, so "latest" is the highest slot and
/// two concurrent rotations to the same `seq` resolve the same way everywhere.
type Slot = (u64, EpochId);

/// What ingesting announce(s) changed — drivers act on it.
#[derive(Default)]
pub struct Ingested {
    /// Parked content for a newly-held epoch that now opens: apply it.
    pub released: Vec<MeshMsg>,
    /// We went from holding NO epoch key to holding one: broadcast our full state.
    pub gained_first_key: bool,
    /// Seq of a newly-learned epoch we are NOT wrapped into (log it).
    pub unheld: Option<u64>,
    /// The accepted-announce set changed (persist it).
    pub changed: bool,
}
impl Ingested {
    pub fn absorb(&mut self, o: Ingested) {
        self.released.extend(o.released);
        self.gained_first_key |= o.gained_first_key;
        self.unheld = self.unheld.max(o.unheld);
        self.changed |= o.changed;
    }
}

/// Why a [`SealedMsg`] didn't open.
#[derive(Debug, PartialEq, Eq)]
pub enum OpenError {
    /// We don't hold epoch `seq` (yet, or ever): the message was parked.
    NoKey { seq: u64 },
    /// Wrong key/header, tampered bytes, or an inner message that isn't content.
    Invalid,
}

/// Admin housekeeping output.
#[derive(Default)]
pub struct Maintenance {
    pub announces: Vec<EpochAnnounce>,
    /// Some((seq, recipients)) when a new epoch was minted.
    pub rotated: Option<(u64, usize)>,
    /// Devices newly wrapped into the latest epoch (add-wrap) — caller broadcasts full state.
    pub added: usize,
}

/// One device's view of a restricted board's epochs: which it can open, which
/// exist, who is wrapped into each, and (for admins) whom to wrap next. Raw keys
/// live only here, in memory — persist [`announces`](Self::announces) instead.
pub struct EpochBook {
    me: DeviceId,
    enc: ClientEncKey,
    /// Epochs we can open.
    keys: BTreeMap<Slot, EpochKey>,
    /// Every accepted epoch, held or not.
    known: BTreeSet<Slot>,
    /// Who is wrapped into each epoch (merged across add-wraps).
    wrapped: BTreeMap<Slot, BTreeSet<DeviceId>>,
    /// Accepted announces, for catch-up and persistence.
    announces: Vec<EpochAnnounce>,
    /// blake3(sig) of accepted announces.
    seen: HashSet<[u8; 32]>,
    /// Valid signature, signer not (yet) an Admin — retried on trust changes.
    pending: Vec<EpochAnnounce>,
    /// Wrap targets: device id → X25519 public key, from verified requests and wraps.
    directory: BTreeMap<DeviceId, EncPublic>,
    parked: VecDeque<SealedMsg>,
}

impl EpochBook {
    pub fn new(key: &DeviceKey) -> Self {
        let enc = ClientEncKey::from_seed(&key.seed());
        let mut directory = BTreeMap::new();
        directory.insert(key.id(), enc.public());
        EpochBook {
            me: key.id(),
            enc,
            keys: BTreeMap::new(),
            known: BTreeSet::new(),
            wrapped: BTreeMap::new(),
            announces: Vec::new(),
            seen: HashSet::new(),
            pending: Vec::new(),
            directory,
            parked: VecDeque::new(),
        }
    }

    pub fn latest_known(&self) -> Option<Slot> {
        self.known.iter().next_back().copied()
    }
    pub fn latest_held(&self) -> Option<Slot> {
        self.keys.keys().next_back().copied()
    }
    pub fn latest_known_seq(&self) -> Option<u64> {
        self.latest_known().map(|s| s.0)
    }
    pub fn latest_held_seq(&self) -> Option<u64> {
        self.latest_held().map(|s| s.0)
    }
    /// Do we hold the key of the latest epoch we know of (i.e. can we read new content)?
    pub fn holds_latest(&self) -> bool {
        self.latest_held().is_some() && self.latest_held() == self.latest_known()
    }
    pub fn announces(&self) -> &[EpochAnnounce] {
        &self.announces
    }

    /// Record a device's encryption key from its self-signed request (the only
    /// trusted id → key binding). `false` if the signature doesn't verify.
    pub fn learn_request(&mut self, req: &JoinRequest) -> bool {
        if !req.verify() {
            return false;
        }
        self.directory.insert(req.device_id, req.enc_pub);
        true
    }

    /// Ingest one announce. Accepted only if signed by a current Admin in `trust`;
    /// a valid signature from a not-(yet)-Admin is held in `pending` and retried by
    /// [`retry_pending`](Self::retry_pending). A key is kept only if its fingerprint
    /// matches the announced id.
    pub fn ingest(&mut self, a: EpochAnnounce, trust: &TrustLog) -> Ingested {
        let mut out = Ingested::default();
        let h = *blake3::hash(&a.sig).as_bytes();
        if self.seen.contains(&h) || !a.verify_sig() {
            return out;
        }
        if trust.role(&a.by) != Some(Role::Admin) {
            if !self.pending.iter().any(|p| p.sig == a.sig) {
                if self.pending.len() >= PENDING_CAP {
                    self.pending.remove(0);
                }
                self.pending.push(a);
            }
            return out;
        }
        self.seen.insert(h);
        let slot = (a.seq, a.id);
        let newly_known = self.known.insert(slot);
        let wrapped = self.wrapped.entry(slot).or_default();
        for w in &a.wraps {
            wrapped.insert(w.device);
            self.directory.entry(w.device).or_insert(w.wrapped.recipient);
        }
        out.changed = true;
        if !self.keys.contains_key(&slot) {
            let mine: Vec<WrappedEpoch> =
                a.wraps.iter().filter(|w| w.device == self.me).map(|w| w.wrapped.clone()).collect();
            match open_epoch(&mine, &self.enc) {
                Some(k) if epoch_id(&k) == a.id => {
                    let first = self.keys.is_empty();
                    self.keys.insert(slot, k);
                    out.gained_first_key = first;
                    out.released = self.release(slot);
                }
                _ => {
                    if newly_known {
                        out.unheld = Some(a.seq);
                    }
                }
            }
        }
        self.announces.push(a);
        out
    }

    /// Open every parked message for `slot` (now held); the rest stay parked.
    fn release(&mut self, slot: Slot) -> Vec<MeshMsg> {
        let (ready, rest): (Vec<_>, Vec<_>) = self.parked.drain(..).partition(|s| (s.seq, s.id) == slot);
        self.parked = rest.into();
        ready.into_iter().filter_map(|s| self.open(s).ok()).collect()
    }

    /// Re-ingest pending announces after the trust log changed (one pass).
    pub fn retry_pending(&mut self, trust: &TrustLog) -> Ingested {
        let mut out = Ingested::default();
        for a in std::mem::take(&mut self.pending) {
            out.absorb(self.ingest(a, trust));
        }
        out
    }

    /// Seal content under the highest epoch we hold. `None` if we hold none.
    pub fn seal(&self, inner: &MeshMsg) -> Option<MeshMsg> {
        let (&(seq, id), key) = self.keys.iter().next_back()?;
        let pt = postcard::to_allocvec(inner).ok()?;
        let body = MeshCipher::from_epoch(key).seal_aad(&pt, &sealed_aad(seq, &id));
        Some(MeshMsg::Sealed(SealedMsg { seq, id, body }))
    }

    /// Open sealed content. Only `Sync`/`SignedSync` inners are returned; anything
    /// else is `Invalid`. A message for an epoch we don't hold is parked (bounded)
    /// and replayed via [`Ingested::released`] if its key arrives.
    pub fn open(&mut self, s: SealedMsg) -> Result<MeshMsg, OpenError> {
        let slot = (s.seq, s.id);
        let Some(key) = self.keys.get(&slot) else {
            let seq = s.seq;
            if self.parked.len() >= PARKED_CAP {
                self.parked.pop_front();
            }
            self.parked.push_back(s);
            return Err(OpenError::NoKey { seq });
        };
        let pt = MeshCipher::from_epoch(key).open_aad(&s.body, &sealed_aad(s.seq, &s.id)).ok_or(OpenError::Invalid)?;
        match postcard::from_bytes::<MeshMsg>(&pt) {
            Ok(m @ (MeshMsg::Sync(_) | MeshMsg::SignedSync(_))) => Ok(m),
            _ => Err(OpenError::Invalid),
        }
    }

    /// Mint a new epoch wrapped to every authorized device we have a key for, and
    /// accept it locally. `None` unless `admin` is an Admin in `trust`.
    pub fn rotate(&mut self, admin: &DeviceKey, trust: &TrustLog) -> Option<EpochAnnounce> {
        if trust.role(&admin.id()) != Some(Role::Admin) {
            return None;
        }
        let seq = self.latest_known().map_or(0, |s| s.0) + 1;
        let key = generate_epoch();
        let id = epoch_id(&key);
        let wraps = trust
            .devices()
            .into_iter()
            .filter_map(|(d, _)| self.directory.get(&d).map(|enc| (d, *enc)))
            .map(|(device, enc)| EpochWrap { device, wrapped: wrap_epoch(&key, &[enc]).remove(0) })
            .collect();
        let a = EpochAnnounce::create(admin, seq, id, wraps);
        self.ingest(a.clone(), trust);
        Some(a)
    }

    /// Admin housekeeping, run after anything that may change who should read:
    /// the founder bootstraps epoch 1; an epoch still wrapping a revoked device is
    /// rotated away; authorized devices missing from the latest epoch are add-wrapped.
    pub fn maintain(&mut self, admin: &DeviceKey, trust: &TrustLog) -> Maintenance {
        let mut m = Maintenance::default();
        if trust.role(&admin.id()) != Some(Role::Admin) {
            return m;
        }
        let Some(latest) = self.latest_known() else {
            if trust.founder() == Some(admin.id()) {
                if let Some(a) = self.rotate(admin, trust) {
                    m.rotated = Some((a.seq, a.wraps.len()));
                    m.announces.push(a);
                }
            }
            return m;
        };
        // We can't wrap (or judge) an epoch we don't hold.
        let Some(key) = self.keys.get(&latest).copied() else { return m };
        let wrapped = self.wrapped.get(&latest).cloned().unwrap_or_default();
        if wrapped.iter().any(|d| trust.is_revoked(d)) {
            if let Some(a) = self.rotate(admin, trust) {
                m.rotated = Some((a.seq, a.wraps.len()));
                m.announces.push(a);
            }
            return m;
        }
        let wraps: Vec<EpochWrap> = trust
            .devices()
            .into_iter()
            .filter(|(d, _)| !wrapped.contains(d))
            .filter_map(|(d, _)| self.directory.get(&d).map(|enc| (d, *enc)))
            .map(|(device, enc)| EpochWrap { device, wrapped: wrap_epoch(&key, &[enc]).remove(0) })
            .collect();
        if !wraps.is_empty() {
            m.added = wraps.len();
            let a = EpochAnnounce::create(admin, latest.0, latest.1, wraps);
            self.ingest(a.clone(), trust);
            m.announces.push(a);
        }
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_id_is_deterministic_and_not_a_key_prefix() {
        let k1 = generate_epoch();
        let k2 = generate_epoch();
        assert_eq!(epoch_id(&k1), epoch_id(&k1), "deterministic");
        assert_ne!(epoch_id(&k1), epoch_id(&k2), "key-dependent");
        assert_ne!(&epoch_id(&k1)[..], &k1[..8], "not just a key prefix");
    }

    #[test]
    fn announce_signature_verifies_and_catches_tampering() {
        let admin = DeviceKey::from_seed(&[1; 32]);
        let wrap = EpochWrap {
            device: [9; 32],
            wrapped: WrappedEpoch { recipient: [2; 32], eph_pub: [3; 32], nonce: [0; 24], ct: vec![1, 2, 3] },
        };
        let a = EpochAnnounce::create(&admin, 1, [7; 8], vec![wrap]);
        assert!(a.verify_sig());

        let mut bad_seq = a.clone();
        bad_seq.seq = 2;
        assert!(!bad_seq.verify_sig());

        let mut bad_wrap = a.clone();
        bad_wrap.wraps[0].device[0] ^= 0x01;
        assert!(!bad_wrap.verify_sig());

        let mut bad_by = a.clone();
        bad_by.by = [99; 32];
        assert!(!bad_by.verify_sig());
    }

    #[test]
    fn access_mode_truth_table() {
        assert!(!AccessMode::LinkEditor.seals_content());
        assert!(!AccessMode::LinkEditor.requires_signed_writes());

        assert!(!AccessMode::LinkViewer.seals_content());
        assert!(AccessMode::LinkViewer.requires_signed_writes());

        assert!(AccessMode::Restricted.seals_content());
        assert!(AccessMode::Restricted.requires_signed_writes());
    }

    // ---- EpochBook ----

    use crate::identity::TrustEntry;
    use crate::sync::SyncMsg;

    fn key(n: u8) -> DeviceKey {
        DeviceKey::from_seed(&[n; 32])
    }

    fn trust_of(entries: Vec<TrustEntry>) -> TrustLog {
        let mut t = TrustLog::new();
        for e in entries {
            assert!(t.apply(e));
        }
        t
    }

    fn feed(book: &mut EpochBook, announces: &[EpochAnnounce], trust: &TrustLog) -> Ingested {
        let mut out = Ingested::default();
        for a in announces {
            out.absorb(book.ingest(a.clone(), trust));
        }
        out
    }

    fn content(path: &str) -> MeshMsg {
        MeshMsg::Sync(SyncMsg::Resync { path: path.into() })
    }

    fn sealed(m: MeshMsg) -> SealedMsg {
        match m {
            MeshMsg::Sealed(s) => s,
            _ => panic!("expected a Sealed message"),
        }
    }

    fn is_resync(r: &Result<MeshMsg, OpenError>, want: &str) -> bool {
        matches!(r, Ok(MeshMsg::Sync(SyncMsg::Resync { path })) if path == want)
    }

    /// Founder F with Members B and C, all wrapped into epoch 1 (one shared trust log).
    fn three_wrapped() -> (DeviceKey, DeviceKey, DeviceKey, TrustLog, EpochBook, EpochBook, EpochBook) {
        let (f, b, c) = (key(1), key(2), key(3));
        let trust = trust_of(vec![
            f.genesis(),
            f.grant(b.id(), Role::Member, [1; 16]),
            f.grant(c.id(), Role::Member, [2; 16]),
        ]);
        let mut fb = EpochBook::new(&f);
        assert_eq!(fb.maintain(&f, &trust).rotated, Some((1, 1)));
        assert!(fb.learn_request(&JoinRequest::create(&b, "b")));
        assert!(fb.learn_request(&JoinRequest::create(&c, "c")));
        assert_eq!(fb.maintain(&f, &trust).added, 2);
        let (mut bb, mut cb) = (EpochBook::new(&b), EpochBook::new(&c));
        feed(&mut bb, fb.announces(), &trust);
        feed(&mut cb, fb.announces(), &trust);
        assert_eq!(bb.latest_held_seq(), Some(1));
        assert_eq!(cb.latest_held_seq(), Some(1));
        (f, b, c, trust, fb, bb, cb)
    }

    #[test]
    fn founder_bootstraps_epoch_one() {
        let (f, m) = (key(1), key(2));
        let trust = trust_of(vec![f.genesis(), f.grant(m.id(), Role::Member, [1; 16])]);
        let mut fb = EpochBook::new(&f);
        let out = fb.maintain(&f, &trust);
        assert_eq!(out.rotated, Some((1, 1)));
        assert_eq!(out.announces.len(), 1);
        assert!(fb.holds_latest());

        let mut mb = EpochBook::new(&m);
        let out = mb.maintain(&m, &trust);
        assert!(out.announces.is_empty() && out.rotated.is_none() && out.added == 0, "a Member never mints");
        assert_eq!(mb.latest_known(), None);
    }

    #[test]
    fn granted_member_is_wrapped_after_its_request() {
        let (f, m) = (key(1), key(2));
        let trust = trust_of(vec![f.genesis(), f.grant(m.id(), Role::Member, [1; 16])]);
        let mut fb = EpochBook::new(&f);
        fb.maintain(&f, &trust);

        let mut mb = EpochBook::new(&m);
        let ing = feed(&mut mb, fb.announces(), &trust);
        assert!(!ing.gained_first_key);
        assert_eq!(ing.unheld, Some(1), "learned epoch 1 but is not wrapped in");
        assert!(!mb.holds_latest());

        assert!(fb.learn_request(&JoinRequest::create(&m, "m")));
        let out = fb.maintain(&f, &trust);
        assert_eq!(out.added, 1);
        assert!(out.rotated.is_none(), "an add-wrap never rotates");

        let ing = feed(&mut mb, &out.announces, &trust);
        assert!(ing.gained_first_key);
        assert!(mb.holds_latest());

        let s = sealed(fb.seal(&content("p")).expect("founder holds a key"));
        assert!(is_resync(&mb.open(s), "p"));
    }

    #[test]
    fn ungranted_requester_is_not_wrapped() {
        let (f, x) = (key(1), key(9));
        let trust = trust_of(vec![f.genesis()]);
        let mut fb = EpochBook::new(&f);
        fb.maintain(&f, &trust);
        assert!(fb.learn_request(&JoinRequest::create(&x, "x")));
        let out = fb.maintain(&f, &trust);
        assert_eq!(out.added, 0);
        assert!(out.announces.is_empty());
    }

    #[test]
    fn non_admin_announce_is_deferred_then_accepted_after_grant() {
        let (f, m) = (key(1), key(2));
        let trust = trust_of(vec![f.genesis()]);
        let mut fb = EpochBook::new(&f);
        fb.maintain(&f, &trust);

        let mut m_trust = TrustLog::new();
        let mut mb = EpochBook::new(&m);
        feed(&mut mb, fb.announces(), &m_trust);
        assert_eq!(mb.latest_known(), None, "signer not known as Admin yet");

        assert!(m_trust.apply(f.genesis()));
        let ing = mb.retry_pending(&m_trust);
        assert!(ing.changed);
        assert_eq!(mb.latest_known_seq(), Some(1));
        assert!(mb.retry_pending(&m_trust).released.is_empty() && mb.pending.is_empty());
    }

    #[test]
    fn member_signed_or_tampered_announce_never_applies() {
        let (f, m, o) = (key(1), key(2), key(3));
        let trust = trust_of(vec![f.genesis(), f.grant(m.id(), Role::Member, [1; 16])]);
        let mut ob = EpochBook::new(&o);

        let k = generate_epoch();
        let by_member = EpochAnnounce::create(&m, 1, epoch_id(&k), vec![]);
        assert!(!ob.ingest(by_member.clone(), &trust).changed);
        ob.ingest(by_member, &trust);
        assert_eq!(ob.pending.len(), 1, "valid sig, not an Admin: deferred once");
        assert!(!ob.retry_pending(&trust).changed);
        assert_eq!(ob.latest_known(), None);

        let mut tampered = EpochAnnounce::create(&f, 1, epoch_id(&k), vec![]);
        tampered.sig[0] ^= 0x01;
        assert!(!ob.ingest(tampered, &trust).changed);
        assert_eq!(ob.pending.len(), 1, "a bad signature is dropped, not deferred");
        assert_eq!(ob.latest_known(), None);
    }

    #[test]
    fn revoke_rotates_and_blinds_the_revoked() {
        let (f, _b, c, mut trust, mut fb, mut bb, mut cb) = three_wrapped();
        assert!(trust.apply(f.revoke(c.id(), [3; 16])));
        let out = fb.maintain(&f, &trust);
        assert_eq!(out.rotated, Some((2, 2)), "rotated to {{F, B}}");

        feed(&mut bb, &out.announces, &trust);
        assert_eq!(bb.latest_held_seq(), Some(2));
        let ing = feed(&mut cb, &out.announces, &trust);
        assert_eq!(ing.unheld, Some(2));
        assert_eq!(cb.latest_held_seq(), Some(1));
        assert_eq!(cb.latest_known_seq(), Some(2));

        let s = sealed(fb.seal(&content("new")).unwrap());
        assert_eq!(s.seq, 2);
        assert!(is_resync(&bb.open(s.clone()), "new"));
        assert_eq!(cb.open(s).err(), Some(OpenError::NoKey { seq: 2 }));

        assert!(fb.maintain(&f, &trust).announces.is_empty(), "rotation is one-shot");
    }

    #[test]
    fn wrapped_but_ungranted_device_does_not_trigger_rotation() {
        let (f, x) = (key(1), key(9));
        let granted = trust_of(vec![f.genesis(), f.grant(x.id(), Role::Member, [1; 16])]);
        let mut fb = EpochBook::new(&f);
        fb.maintain(&f, &granted);
        fb.learn_request(&JoinRequest::create(&x, "x"));
        assert_eq!(fb.maintain(&f, &granted).added, 1);

        // A trust view that never saw X's grant: X is wrapped in but not revoked.
        let without_grant = trust_of(vec![f.genesis()]);
        assert!(!without_grant.is_revoked(&x.id()));
        let out = fb.maintain(&f, &without_grant);
        assert_eq!(out.rotated, None);
        assert!(out.announces.is_empty());
    }

    #[test]
    fn old_epoch_content_still_opens_after_rotation() {
        let (f, _b, c, mut trust, mut fb, bb, _cb) = three_wrapped();
        let from_b = sealed(bb.seal(&content("old")).unwrap());
        assert_eq!(from_b.seq, 1);
        trust.apply(f.revoke(c.id(), [3; 16]));
        assert!(fb.maintain(&f, &trust).rotated.is_some());
        assert_eq!(fb.latest_held_seq(), Some(2));
        assert!(is_resync(&fb.open(from_b), "old"));
    }

    #[test]
    fn parked_content_is_released_when_the_key_arrives() {
        let (f, _b, c, mut trust, mut fb, mut bb, _cb) = three_wrapped();
        trust.apply(f.revoke(c.id(), [3; 16]));
        let out = fb.maintain(&f, &trust);
        let early = sealed(fb.seal(&content("early")).unwrap());

        assert_eq!(bb.open(early).err(), Some(OpenError::NoKey { seq: 2 }));
        let ing = feed(&mut bb, &out.announces, &trust);
        assert_eq!(ing.released.len(), 1);
        assert!(matches!(&ing.released[0], MeshMsg::Sync(SyncMsg::Resync { path }) if path == "early"));
        assert!(!ing.gained_first_key, "B already held epoch 1");
    }

    #[test]
    fn sealed_rejects_wrong_aad_tamper_and_non_content() {
        let (f, _b, c, mut trust, mut fb, _bb, _cb) = three_wrapped();
        trust.apply(f.revoke(c.id(), [3; 16]));
        fb.maintain(&f, &trust);
        let slots: Vec<Slot> = fb.keys.keys().copied().collect();
        assert_eq!(slots.len(), 2);
        let (old, new) = (slots[0], slots[1]);

        let mut flipped = sealed(fb.seal(&content("x")).unwrap());
        let last = flipped.body.len() - 1;
        flipped.body[last] ^= 0x01;
        assert_eq!(fb.open(flipped).err(), Some(OpenError::Invalid));

        let mut moved = sealed(fb.seal(&content("x")).unwrap());
        assert_eq!((moved.seq, moved.id), new);
        (moved.seq, moved.id) = old;
        assert_eq!(fb.open(moved).err(), Some(OpenError::Invalid), "header swapped onto another held epoch");

        // Right key, wrong associated data: the header is bound into the tag.
        let pt = postcard::to_allocvec(&content("x")).unwrap();
        let body = MeshCipher::from_epoch(&fb.keys[&new]).seal_aad(&pt, &sealed_aad(new.0 + 7, &new.1));
        assert_eq!(fb.open(SealedMsg { seq: new.0, id: new.1, body }).err(), Some(OpenError::Invalid));

        let not_content = fb.seal(&MeshMsg::Trust(f.genesis())).unwrap();
        assert_eq!(fb.open(sealed(not_content)).err(), Some(OpenError::Invalid), "only content is sealed");
    }

    #[test]
    fn lying_fingerprint_is_not_stored() {
        let (f, m) = (key(1), key(2));
        let trust = trust_of(vec![f.genesis(), f.grant(m.id(), Role::Member, [1; 16])]);
        let real = generate_epoch();
        let m_enc = ClientEncKey::from_seed(&m.seed()).public();
        let wraps = vec![EpochWrap { device: m.id(), wrapped: wrap_epoch(&real, &[m_enc]).remove(0) }];
        let lie = EpochAnnounce::create(&f, 1, [0xAA; 8], wraps);
        assert_ne!(epoch_id(&real), [0xAA; 8]);

        let mut mb = EpochBook::new(&m);
        let ing = mb.ingest(lie, &trust);
        assert!(!ing.gained_first_key);
        assert_eq!(mb.latest_held(), None);
    }
}
