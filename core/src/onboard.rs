//! Onboarding — the lib backend for adding a person or a device to a board
//! (`docs/security/user-flows.md`). Both the **request-to-join** flow
//! (Google-Docs "request access", asynchronous) and **device-linking** (your own
//! device, co-present) reduce to the same two data items:
//!
//!   * a signed [`JoinRequest`] — the newcomer proving it holds its keys and
//!     publishing what an admin needs: its signing id (write authority), its
//!     encryption pubkey (a wrap target for read), and a self-asserted name;
//!   * an [`approve`] result — a signed **grant** (trust-log entry) plus a
//!     **wrapped epoch key**, so the newcomer becomes both an authorized writer
//!     and a reader in one admin action.
//!
//! The two flows differ only in *assurance*, which is UX, not data: device
//! linking confirms the request out-of-band (a QR / [`safety_code`]) because the
//! devices are co-present; request-to-join approves a recognized name. A UI
//! calls these functions for all the crypto; it only renders and confirms.

use crate::identity::{verify, DeviceId, DeviceKey, Role, TrustEntry};
use crate::keywrap::{wrap_epoch, ClientEncKey, EncPublic, EpochKey, WrappedEpoch};
use serde::{Deserialize, Serialize};

/// A signed request to be added to a board — used for both request-to-join and
/// device-linking. Encodes to a compact string ([`encode`](Self::encode)) for a
/// QR code or a share channel.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JoinRequest {
    /// Ed25519 signing pubkey — the requester's write identity.
    pub device_id: DeviceId,
    /// X25519 encryption pubkey — where an admin wraps the epoch key (read).
    pub enc_pub: EncPublic,
    /// Self-asserted display name / note, so an admin knows who's asking.
    pub name: String,
    /// Signature by `device_id` over `(device_id, enc_pub, name)`.
    pub sig: Vec<u8>,
}

/// The bytes a [`JoinRequest`] signs (everything but the signature).
fn request_msg(device_id: &DeviceId, enc_pub: &EncPublic, name: &str) -> Vec<u8> {
    postcard::to_allocvec(&("riftpipe-join-v1", device_id, enc_pub, name)).expect("encode")
}

impl JoinRequest {
    /// Build a signed request from a device's `key` and a display `name`. The
    /// encryption pubkey is derived from the same seed as the signing key.
    pub fn create(key: &DeviceKey, name: impl Into<String>) -> Self {
        let name = name.into();
        let device_id = key.id();
        let enc_pub = ClientEncKey::from_seed(&key.seed()).public();
        let sig = key.sign(&request_msg(&device_id, &enc_pub, &name)).to_vec();
        JoinRequest { device_id, enc_pub, name, sig }
    }

    /// Whether the request is authentically signed by `device_id`.
    pub fn verify(&self) -> bool {
        verify(&self.device_id, &request_msg(&self.device_id, &self.enc_pub, &self.name), &self.sig)
    }

    /// A short human-comparable **safety code** (like Matrix SAS) — read it aloud
    /// or scan it to confirm, out-of-band, that this request is really the device
    /// in front of you before approving.
    pub fn safety_code(&self) -> String {
        let h = blake3::hash(&request_msg(&self.device_id, &self.enc_pub, &self.name));
        // 8 hex chars grouped for readability, e.g. "a1b2 c3d4".
        let hex: String = h.as_bytes()[..4].iter().map(|b| format!("{b:02x}")).collect();
        format!("{} {}", &hex[..4], &hex[4..])
    }

    /// Encode to a compact hex string for a QR code or a share channel.
    pub fn encode(&self) -> String {
        postcard::to_allocvec(self).map(|b| b.iter().map(|x| format!("{x:02x}")).collect()).unwrap_or_default()
    }

    /// Decode a request produced by [`encode`](Self::encode). `None` on garbage.
    pub fn decode(s: &str) -> Option<Self> {
        if s.len() % 2 != 0 {
            return None;
        }
        let bytes: Vec<u8> =
            (0..s.len() / 2).map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()).collect::<Option<_>>()?;
        postcard::from_bytes(&bytes).ok()
    }
}

/// Approve a verified [`JoinRequest`] as `admin`: produce a signed **grant**
/// (authorizes the requester as `role` — apply it to the trust log) and a
/// **wrapped epoch key** for the requester (so it can read). `None` if the
/// request isn't authentically signed. The caller must itself be an Admin for
/// the grant to take effect — that's enforced when peers apply it.
pub fn approve(
    admin: &DeviceKey,
    req: &JoinRequest,
    role: Role,
    epoch: &EpochKey,
    nonce: [u8; 16],
) -> Option<(TrustEntry, WrappedEpoch)> {
    if !req.verify() {
        return None;
    }
    let grant = admin.grant(req.device_id, role, nonce);
    let wrapped = wrap_epoch(epoch, &[req.enc_pub]).into_iter().next()?;
    Some((grant, wrapped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::TrustLog;
    use crate::keywrap::{generate_epoch, open_epoch};

    #[test]
    fn a_request_verifies_and_tampering_is_caught() {
        let key = DeviceKey::from_seed(&[1u8; 32]);
        let req = JoinRequest::create(&key, "Alice");
        assert!(req.verify());

        // Tamper with the name → signature no longer matches.
        let mut bad = req.clone();
        bad.name = "Mallory".into();
        assert!(!bad.verify());

        // Tamper with the enc pubkey (would redirect the read wrap) → caught.
        let mut bad = req.clone();
        bad.enc_pub[0] ^= 0x01;
        assert!(!bad.verify());
    }

    #[test]
    fn approve_produces_a_working_grant_and_read_wrap() {
        // End-to-end: founder approves a newcomer's request → the newcomer
        // becomes an authorized writer (grant) AND can read (unwraps the epoch).
        let founder = DeviceKey::from_seed(&[1u8; 32]);
        let newcomer = DeviceKey::from_seed(&[2u8; 32]);
        let epoch = generate_epoch();

        let req = JoinRequest::create(&newcomer, "Alice");
        let (grant, wrap) = approve(&founder, &req, Role::Member, &epoch, [5u8; 16]).expect("approved");

        // Write authority: applying the grant (under the founder's genesis)
        // authorizes the newcomer as a Member.
        let mut log = TrustLog::new();
        log.apply(founder.genesis());
        assert!(log.apply(grant));
        assert_eq!(log.role(&newcomer.id()), Some(Role::Member));

        // Read access: the newcomer unwraps the epoch key with its enc key.
        let newcomer_enc = ClientEncKey::from_seed(&newcomer.seed());
        assert_eq!(open_epoch(&[wrap], &newcomer_enc), Some(epoch), "newcomer can read");
    }

    #[test]
    fn approve_rejects_an_unsigned_request() {
        let founder = DeviceKey::from_seed(&[1u8; 32]);
        let mut req = JoinRequest::create(&DeviceKey::from_seed(&[2u8; 32]), "Alice");
        req.sig[0] ^= 0x01; // break the signature
        assert!(approve(&founder, &req, Role::Member, &generate_epoch(), [0u8; 16]).is_none());
    }

    #[test]
    fn encode_decode_roundtrips_and_safety_code_is_stable() {
        let key = DeviceKey::from_seed(&[3u8; 32]);
        let req = JoinRequest::create(&key, "Bob's phone");
        let wire = req.encode();
        let back = JoinRequest::decode(&wire).expect("decodes");
        assert!(back.verify());
        assert_eq!(back.device_id, req.device_id);
        assert_eq!(req.safety_code(), back.safety_code(), "safety code is deterministic");
        assert!(JoinRequest::decode("zz").is_none(), "garbage rejected");

        // Different requests have different safety codes.
        let other = JoinRequest::create(&DeviceKey::from_seed(&[4u8; 32]), "Bob's phone");
        assert_ne!(req.safety_code(), other.safety_code());
    }
}
