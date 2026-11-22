//! Read revocation — per-client encryption keys + **epoch-key wrapping**
//! (`docs/security/architecture.md`). This is the "key per client" primitive:
//! instead of one shared secret everyone keeps forever, board content is sealed
//! under a rotatable **epoch key**, and that epoch key is *wrapped* (encrypted)
//! to each authorized client's public key — one small blob per client. Revoking
//! a client is a **rotation**: mint a new epoch key, wrap it to the remaining
//! clients only, and the removed client — never wrapped in — can't derive it, so
//! everything from the new epoch on is opaque to them.
//!
//! Each wrap is a sealed box: an ephemeral X25519 key does ECDH with the
//! recipient's static X25519 key, the shared secret is domain-separated into an
//! AEAD key (blake3), and the epoch key is sealed with XChaCha20-Poly1305. Pure
//! Rust (`x25519-dalek` + `chacha20poly1305`), so it compiles to wasm and native
//! alike; no `ring`, no `openssl`.
//!
//! Naive **O(clients)** wrapping — right for riftpipe's scale (a handful of
//! devices/people per board). TreeKEM-style O(log n) rekey is deliberately out
//! of scope until boards are large.
//!
//! This module is the crypto primitive. `core::epoch` (`EpochBook`) builds the
//! mesh integration on it: where wrap targets come from (signed join requests),
//! Admin-signed epoch announcements, sealing content under the current epoch,
//! and rotate-on-revoke.

use chacha20poly1305::aead::Aead;
use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};
use x25519_dalek::{PublicKey, StaticSecret};

/// The symmetric content key for one epoch — what actually seals board frames.
pub type EpochKey = [u8; 32];
/// An X25519 public key (a wrap target).
pub type EncPublic = [u8; 32];

const NONCE_LEN: usize = 24;
const WRAP_CONTEXT: &str = "riftpipe epoch wrap v1";
const ENC_KEY_CONTEXT: &str = "riftpipe client enc x25519 v1";

/// A client's X25519 **encryption** keypair — where wrapped epoch keys are
/// addressed. Derived deterministically from a 32-byte seed (e.g. the device's
/// signing seed), domain-separated so it's independent of the signing key. The
/// secret never leaves the client; only [`public`](Self::public) is shared.
pub struct ClientEncKey {
    secret: StaticSecret,
}

impl ClientEncKey {
    /// Derive the encryption key from a seed (typically the device seed).
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        let k = blake3::derive_key(ENC_KEY_CONTEXT, seed);
        ClientEncKey { secret: StaticSecret::from(k) }
    }

    /// This client's public wrap-target key. Publish it (with the device id) so
    /// admins can wrap epoch keys to this client.
    pub fn public(&self) -> EncPublic {
        PublicKey::from(&self.secret).to_bytes()
    }
}

/// One wrapped copy of an epoch key, addressed to a single recipient. A board
/// holds one of these per authorized client (they're plain data — sync like any
/// file); a client picks out the blob whose `recipient` is its own key.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WrappedEpoch {
    /// Recipient's X25519 public key (so a client finds its own blob).
    pub recipient: EncPublic,
    /// Ephemeral X25519 public key for this wrap's ECDH.
    pub eph_pub: [u8; 32],
    /// AEAD nonce.
    pub nonce: [u8; NONCE_LEN],
    /// Sealed epoch key.
    pub ct: Vec<u8>,
}

/// Fresh random epoch key — a new content key for a new epoch (board creation or
/// a rotation on revoke).
pub fn generate_epoch() -> EpochKey {
    let mut k = [0u8; 32];
    getrandom::getrandom(&mut k).expect("OS RNG unavailable");
    k
}

/// Derive the per-recipient AEAD key from an ECDH shared secret.
fn wrap_key(shared: &[u8; 32]) -> Key {
    *Key::from_slice(&blake3::derive_key(WRAP_CONTEXT, shared))
}

/// Wrap `epoch` to each recipient public key — a sealed box per client. The set
/// is exactly who may read this epoch; on a rotation, pass the remaining
/// authorized clients (the revoked one is simply left out).
pub fn wrap_epoch(epoch: &EpochKey, recipients: &[EncPublic]) -> Vec<WrappedEpoch> {
    recipients
        .iter()
        .map(|recipient| {
            // Fresh ephemeral key per recipient (forward-secrecy of the wrap).
            let mut eph_bytes = [0u8; 32];
            getrandom::getrandom(&mut eph_bytes).expect("OS RNG unavailable");
            let eph = StaticSecret::from(eph_bytes);
            let shared = eph.diffie_hellman(&PublicKey::from(*recipient));
            let cipher = XChaCha20Poly1305::new(&wrap_key(shared.as_bytes()));
            let mut nonce = [0u8; NONCE_LEN];
            getrandom::getrandom(&mut nonce).expect("OS RNG unavailable");
            let ct = cipher.encrypt(XNonce::from_slice(&nonce), epoch.as_slice()).expect("seal");
            WrappedEpoch {
                recipient: *recipient,
                eph_pub: PublicKey::from(&eph).to_bytes(),
                nonce,
                ct,
            }
        })
        .collect()
}

/// Recover the epoch key from the blob addressed to `me`, if present and valid.
/// `None` when no blob targets us (e.g. we were revoked) or one won't open.
pub fn open_epoch(wrapped: &[WrappedEpoch], me: &ClientEncKey) -> Option<EpochKey> {
    let my_pub = me.public();
    for w in wrapped.iter().filter(|w| w.recipient == my_pub) {
        let shared = me.secret.diffie_hellman(&PublicKey::from(w.eph_pub));
        let cipher = XChaCha20Poly1305::new(&wrap_key(shared.as_bytes()));
        if let Ok(pt) = cipher.decrypt(XNonce::from_slice(&w.nonce), w.ct.as_slice()) {
            if let Ok(epoch) = <[u8; 32]>::try_from(pt.as_slice()) {
                return Some(epoch);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(seed: u8) -> ClientEncKey {
        ClientEncKey::from_seed(&[seed; 32])
    }

    #[test]
    fn wrap_then_open_roundtrips_for_recipients() {
        let (a, b) = (client(1), client(2));
        let epoch = generate_epoch();
        let wrapped = wrap_epoch(&epoch, &[a.public(), b.public()]);
        assert_eq!(open_epoch(&wrapped, &a), Some(epoch), "A recovers the epoch");
        assert_eq!(open_epoch(&wrapped, &b), Some(epoch), "B recovers the epoch");
    }

    #[test]
    fn a_non_recipient_cannot_open() {
        let (a, outsider) = (client(1), client(9));
        let epoch = generate_epoch();
        let wrapped = wrap_epoch(&epoch, &[a.public()]); // only A is wrapped in
        assert!(open_epoch(&wrapped, &outsider).is_none(), "no blob for a non-recipient");
    }

    #[test]
    fn rotation_excludes_a_removed_client() {
        // epoch1 → {A,B,C}; then revoke C by rotating to a new epoch → {A,B}.
        let (a, b, c) = (client(1), client(2), client(3));
        let epoch1 = generate_epoch();
        let w1 = wrap_epoch(&epoch1, &[a.public(), b.public(), c.public()]);
        assert_eq!(open_epoch(&w1, &c), Some(epoch1), "C could read epoch 1");

        let epoch2 = generate_epoch();
        assert_ne!(epoch1, epoch2, "a rotation mints a fresh key");
        let w2 = wrap_epoch(&epoch2, &[a.public(), b.public()]); // C left out
        assert_eq!(open_epoch(&w2, &a), Some(epoch2), "A follows to epoch 2");
        assert_eq!(open_epoch(&w2, &b), Some(epoch2), "B follows to epoch 2");
        assert!(open_epoch(&w2, &c).is_none(), "the revoked client is blind to epoch 2");
    }

    #[test]
    fn each_wrap_uses_a_fresh_ephemeral_and_nonce() {
        let a = client(1);
        let epoch = generate_epoch();
        let w1 = wrap_epoch(&epoch, &[a.public()]);
        let w2 = wrap_epoch(&epoch, &[a.public()]);
        assert_ne!(w1[0].eph_pub, w2[0].eph_pub, "fresh ephemeral per wrap");
        assert_ne!(w1[0].nonce, w2[0].nonce, "fresh nonce per wrap");
        // Both still open to the same epoch.
        assert_eq!(open_epoch(&w1, &a), open_epoch(&w2, &a));
    }

    #[test]
    fn tampered_wrap_is_rejected() {
        let a = client(1);
        let mut wrapped = wrap_epoch(&generate_epoch(), &[a.public()]);
        let last = wrapped[0].ct.len() - 1;
        wrapped[0].ct[last] ^= 0x01; // flip a ciphertext bit
        assert!(open_epoch(&wrapped, &a).is_none(), "the auth tag catches tampering");
    }

    #[test]
    fn enc_key_is_deterministic_and_independent_per_seed() {
        assert_eq!(client(7).public(), client(7).public(), "deterministic from seed");
        assert_ne!(client(7).public(), client(8).public(), "distinct per seed");
    }
}
