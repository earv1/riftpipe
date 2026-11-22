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
//! "Viewer" mode is just another variant.

use serde::{Deserialize, Serialize};

use crate::identity::{verify, DeviceId, DeviceKey};
use crate::keywrap::{EpochKey, WrappedEpoch};

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

#[allow(dead_code)] // used by EpochBook (next step)
pub(crate) fn sealed_aad(seq: u64, id: &EpochId) -> Vec<u8> {
    postcard::to_allocvec(&("riftpipe-sealed-v1", seq, id)).expect("encode")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keywrap::generate_epoch;

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
}
