//! Mesh crypto — the "first cut" of the capability model in `SECURITY.md`:
//! a board is identified by a random **secret**, and everything is derived from
//! it so possession of the secret (which rides the share link's URL fragment)
//! is what grants access.
//!
//!   * **Topic = `H(secret)`** ([`topic_from_secret`]) — the gossip rendezvous
//!     id is a one-way function of the secret, so a stranger (or a DHT) that
//!     only learns a host's public key cannot derive the topic. (Contrast the
//!     old scheme, where the topic *was* the host's public EndpointId.)
//!   * **Sealed frames** ([`MeshCipher`]) — every gossip frame is AEAD-sealed
//!     with a key derived from the same secret, so the relay and any non-holder
//!     see only ciphertext and cannot inject accepted messages.
//!
//! Domain-separated derivations (`blake3::derive_key` with distinct contexts)
//! keep the topic id and the AEAD key independent — neither reveals the other,
//! and neither reveals the secret.
//!
//! Deliberately NOT here yet (see `SECURITY.md` follow-ons): per-message
//! **signatures** (one shared symmetric key means any *holder* can still forge
//! authorship — sealing stops outsiders, not insiders) and a **read/write
//! split**. This layer closes the outsider threats; signing closes the rest.
//!
//! Shared by native (`sync::mesh`) and browser (`web::gossip`) so both derive
//! the identical topic + key from a link and interoperate.

use chacha20poly1305::aead::Aead;
use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305, XNonce};

/// Length of a board secret, in bytes.
pub const SECRET_LEN: usize = 32;
/// XChaCha20-Poly1305 nonce length. Big enough (192 bits) that a random nonce
/// per message never realistically collides — essential for a mesh where every
/// peer encrypts under the same key with no shared counter.
const NONCE_LEN: usize = 24;

/// blake3 domain-separation contexts. Changing either string changes the
/// derived value, so they double as a crypto-scheme version tag.
const TOPIC_CONTEXT: &str = "riftpipe board topic v1";
const AEAD_CONTEXT: &str = "riftpipe mesh aead v1";

/// A fresh random board secret. The caller persists it (a host, so the topic is
/// stable across restarts) or reads it from a share link (a joiner).
pub fn generate_secret() -> [u8; SECRET_LEN] {
    let mut s = [0u8; SECRET_LEN];
    getrandom::getrandom(&mut s).expect("OS RNG unavailable");
    s
}

/// The gossip topic id for a board — `blake3::derive_key(TOPIC_CONTEXT, secret)`.
/// Underivable without the secret, so the discovery layer stays blind.
pub fn topic_from_secret(secret: &[u8; SECRET_LEN]) -> [u8; 32] {
    blake3::derive_key(TOPIC_CONTEXT, secret)
}

/// AEAD-seals mesh frames under a key derived from the board secret. Cheaply
/// constructed per session; holds only the cipher (the key material is consumed
/// into it).
pub struct MeshCipher {
    cipher: XChaCha20Poly1305,
}

impl MeshCipher {
    /// Derive the sealing key from `secret` and build the cipher.
    pub fn from_secret(secret: &[u8; SECRET_LEN]) -> Self {
        let key = blake3::derive_key(AEAD_CONTEXT, secret);
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
        MeshCipher { cipher }
    }

    /// Seal `plaintext` into a self-contained frame `nonce(24) ‖ ciphertext‖tag`
    /// with a fresh random nonce. The nonce travels in the clear (it must — the
    /// receiver needs it); confidentiality rests on the key, integrity on the tag.
    pub fn seal(&self, plaintext: &[u8]) -> Vec<u8> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::getrandom(&mut nonce).expect("OS RNG unavailable");
        // AEAD encryption fails only on absurd (multi-GiB) inputs; a mesh frame
        // is at most a few KiB, so this cannot fail in practice.
        let ct = self
            .cipher
            .encrypt(XNonce::from_slice(&nonce), plaintext)
            .expect("AEAD seal");
        let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        out
    }

    /// Open a frame produced by [`seal`](Self::seal). `None` if it's too short,
    /// sealed under a different secret, or tampered with — the caller drops such
    /// frames (an outsider's traffic, or corruption), exactly like an
    /// undecodable message.
    pub fn open(&self, framed: &[u8]) -> Option<Vec<u8>> {
        if framed.len() < NONCE_LEN {
            return None;
        }
        let (nonce, ct) = framed.split_at(NONCE_LEN);
        self.cipher.decrypt(XNonce::from_slice(nonce), ct).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_is_deterministic_and_secret_dependent() {
        let s1 = [7u8; SECRET_LEN];
        let s2 = [8u8; SECRET_LEN];
        assert_eq!(topic_from_secret(&s1), topic_from_secret(&s1), "deterministic");
        assert_ne!(topic_from_secret(&s1), topic_from_secret(&s2), "secret-dependent");
        // The topic must not equal the secret (it's a one-way derivation).
        assert_ne!(topic_from_secret(&s1), s1);
    }

    #[test]
    fn topic_and_aead_key_are_independent() {
        // Same secret, different domain-separation contexts -> unrelated outputs.
        let s = [3u8; SECRET_LEN];
        let topic = topic_from_secret(&s);
        let aead = blake3::derive_key(AEAD_CONTEXT, &s);
        assert_ne!(topic, aead, "domain separation: topic id != aead key");
    }

    #[test]
    fn seal_open_roundtrips() {
        let cipher = MeshCipher::from_secret(&[42u8; SECRET_LEN]);
        for msg in [&b""[..], b"hi", &[0u8; 4000][..]] {
            let sealed = cipher.seal(msg);
            assert!(sealed.len() > msg.len(), "envelope adds nonce + tag");
            assert_eq!(cipher.open(&sealed).as_deref(), Some(msg));
        }
    }

    #[test]
    fn fresh_nonce_per_seal() {
        // Two seals of identical plaintext differ (random nonce) yet both open.
        let cipher = MeshCipher::from_secret(&[1u8; SECRET_LEN]);
        let (a, b) = (cipher.seal(b"same"), cipher.seal(b"same"));
        assert_ne!(a, b, "nonce reuse would make identical plaintexts identical");
        assert_eq!(cipher.open(&a).as_deref(), Some(&b"same"[..]));
        assert_eq!(cipher.open(&b).as_deref(), Some(&b"same"[..]));
    }

    #[test]
    fn wrong_secret_cannot_open() {
        let sealed = MeshCipher::from_secret(&[1u8; SECRET_LEN]).seal(b"secret board data");
        let outsider = MeshCipher::from_secret(&[2u8; SECRET_LEN]);
        assert!(outsider.open(&sealed).is_none(), "a different secret must not decrypt");
    }

    #[test]
    fn tampering_is_rejected() {
        let cipher = MeshCipher::from_secret(&[9u8; SECRET_LEN]);
        let mut sealed = cipher.seal(b"authentic");
        let last = sealed.len() - 1;
        sealed[last] ^= 0x01; // flip a ciphertext/tag bit
        assert!(cipher.open(&sealed).is_none(), "the auth tag must catch tampering");
        assert!(cipher.open(&sealed[..NONCE_LEN]).is_none(), "truncation is rejected");
        assert!(cipher.open(b"short").is_none(), "under-length frame is rejected");
    }
}
