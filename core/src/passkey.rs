//! Passkey (WebAuthn) assertion verification — the lib backend of the passkey
//! front door (`docs/planned/auth.md`). The browser ceremony
//! (`navigator.credentials.get`) is thin interactive glue; the load-bearing,
//! testable part is verifying the assertion, and that's pure math done here.
//!
//! A passkey signs, per the W3C WebAuthn spec, exactly:
//! `authenticatorData ‖ SHA-256(clientDataJSON)`. We support **ES256**
//! (COSE −7, P-256/ECDSA with SHA-256 — the dominant passkey algorithm) and
//! **EdDSA** (−8, Ed25519). Both verify pure-Rust (`p256` / `ed25519-dalek`),
//! so this compiles to wasm and native alike — no `ring`, no `openssl`, and
//! `webauthn-rs` (which is OpenSSL-bound) is deliberately avoided.
//!
//! ES256 signatures are ASN.1 **DER**, not raw `r‖s` — handled below. The
//! passkey's role is to *vouch* for a device signing key: put the device id in
//! the WebAuthn challenge, and the caller checks that binding via
//! [`challenge_of`] after this returns true.

use ed25519_dalek::{Signature as EdSignature, VerifyingKey as EdVerifyingKey};
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature as P256Signature, VerifyingKey as P256VerifyingKey};
use sha2::{Digest, Sha256};

/// The message a WebAuthn assertion signs.
fn signed_message(authenticator_data: &[u8], client_data_json: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(authenticator_data.len() + 32);
    msg.extend_from_slice(authenticator_data);
    msg.extend_from_slice(&Sha256::digest(client_data_json));
    msg
}

/// Verify an **ES256** (P-256/ECDSA-SHA256) passkey assertion. `public_key_sec1`
/// is the credential's public key as an uncompressed SEC1 point (`0x04‖x‖y`, 65
/// bytes — the caller extracts it from the COSE key at registration);
/// `signature_der` is the assertion signature (ASN.1 DER). `true` iff genuine.
pub fn verify_es256(
    public_key_sec1: &[u8],
    authenticator_data: &[u8],
    client_data_json: &[u8],
    signature_der: &[u8],
) -> bool {
    let Ok(vk) = P256VerifyingKey::from_sec1_bytes(public_key_sec1) else { return false };
    let Ok(sig) = P256Signature::from_der(signature_der) else { return false };
    vk.verify(&signed_message(authenticator_data, client_data_json), &sig).is_ok()
}

/// Verify an **EdDSA** (Ed25519) passkey assertion. `public_key` is the 32-byte
/// Ed25519 credential key; `signature` is 64 bytes.
pub fn verify_eddsa(
    public_key: &[u8],
    authenticator_data: &[u8],
    client_data_json: &[u8],
    signature: &[u8],
) -> bool {
    let Ok(pk): Result<[u8; 32], _> = public_key.try_into() else { return false };
    let Ok(vk) = EdVerifyingKey::from_bytes(&pk) else { return false };
    let Ok(sig): Result<[u8; 64], _> = signature.try_into() else { return false };
    vk.verify_strict(&signed_message(authenticator_data, client_data_json), &EdSignature::from_bytes(&sig))
        .is_ok()
}

/// Extract the base64url `challenge` field from a `clientDataJSON` — what the
/// passkey vouched for. The caller confirms it equals the expected value (e.g.
/// the base64url of the device id) so the assertion is bound to *this* device.
/// Minimal parse (no serde_json dependency): finds `"challenge":"…"`.
pub fn challenge_of(client_data_json: &[u8]) -> Option<String> {
    let s = core::str::from_utf8(client_data_json).ok()?;
    let key = "\"challenge\":\"";
    let start = s.find(key)? + key.len();
    let end = start + s[start..].find('"')?;
    Some(s[start..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use p256::ecdsa::signature::Signer;

    /// A realistic authenticatorData prefix: 32-byte rpIdHash + flags + counter.
    fn auth_data() -> Vec<u8> {
        let mut d = vec![0u8; 37];
        d[32] = 0x05; // UP + UV flags
        d
    }

    fn client_data(challenge_b64url: &str) -> Vec<u8> {
        format!(
            r#"{{"type":"webauthn.get","challenge":"{challenge_b64url}","origin":"https://example.com"}}"#
        )
        .into_bytes()
    }

    #[test]
    fn es256_assertion_verifies_and_tampering_is_caught() {
        // Stand in for a platform authenticator holding a P-256 credential.
        let signing = p256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
        let vk = P256VerifyingKey::from(&signing);
        let pub_sec1 = vk.to_encoded_point(false); // uncompressed 0x04‖x‖y

        let ad = auth_data();
        let cd = client_data("ZGV2aWNlLWlk"); // base64url("device-id")
        let sig: P256Signature = signing.sign(&signed_message(&ad, &cd));
        let sig_der = sig.to_der();

        assert!(verify_es256(pub_sec1.as_bytes(), &ad, &cd, sig_der.as_bytes()), "genuine assertion verifies");

        // Tampered authenticatorData (e.g. flipped flags) → rejected.
        let mut bad_ad = ad.clone();
        bad_ad[32] ^= 0x01;
        assert!(!verify_es256(pub_sec1.as_bytes(), &bad_ad, &cd, sig_der.as_bytes()));

        // Tampered clientData (e.g. a swapped challenge) → rejected.
        let bad_cd = client_data("b3RoZXI"); // different challenge
        assert!(!verify_es256(pub_sec1.as_bytes(), &ad, &bad_cd, sig_der.as_bytes()));

        // Wrong credential key → rejected.
        let other = P256VerifyingKey::from(&p256::ecdsa::SigningKey::from_slice(&[8u8; 32]).unwrap());
        assert!(!verify_es256(other.to_encoded_point(false).as_bytes(), &ad, &cd, sig_der.as_bytes()));
    }

    #[test]
    fn eddsa_assertion_verifies() {
        let signing = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let ad = auth_data();
        let cd = client_data("ZGV2aWNlLWlk");
        let sig = signing.sign(&signed_message(&ad, &cd)).to_bytes();
        assert!(verify_eddsa(&signing.verifying_key().to_bytes(), &ad, &cd, &sig));
        // A tampered signature is rejected.
        let mut bad = sig;
        bad[0] ^= 0x01;
        assert!(!verify_eddsa(&signing.verifying_key().to_bytes(), &ad, &cd, &bad));
    }

    #[test]
    fn challenge_is_extracted_for_the_device_binding_check() {
        let cd = client_data("ZGV2aWNlLWlk");
        assert_eq!(challenge_of(&cd).as_deref(), Some("ZGV2aWNlLWlk"));
        assert!(challenge_of(b"not json").is_none());
    }
}
