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
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// COSE algorithm id: ES256 (P-256 / ECDSA-SHA256).
pub const ALG_ES256: i32 = -7;
/// COSE algorithm id: EdDSA (Ed25519).
pub const ALG_EDDSA: i32 = -8;
/// authenticatorData flag: User Present.
const FLAG_UP: u8 = 0x01;
/// DER prefix of a P-256 SubjectPublicKeyInfo (uncompressed point follows, 65 bytes).
const SPKI_P256_PREFIX: [u8; 26] = [
    0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce,
    0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
];
/// DER prefix of an Ed25519 SubjectPublicKeyInfo (32-byte key follows).
const SPKI_ED25519_PREFIX: [u8; 12] = [0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00];

/// A registered passkey — public data only, safe to replicate in the trust log.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PasskeyCredential {
    /// WebAuthn credential id (`rawId`), passed back in `allowCredentials`.
    pub id: Vec<u8>,
    /// COSE algorithm: [`ALG_ES256`] or [`ALG_EDDSA`].
    pub alg: i32,
    /// ES256: uncompressed SEC1 point (65 bytes). EdDSA: the 32-byte key.
    pub public_key: Vec<u8>,
    /// Relying-party id the credential is scoped to (the page hostname,
    /// e.g. `earv1.github.io` or `localhost`).
    pub rp_id: String,
}

/// One `navigator.credentials.get` response, as raw bytes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PasskeyAssertion {
    pub authenticator_data: Vec<u8>,
    pub client_data_json: Vec<u8>,
    pub signature: Vec<u8>,
}

impl PasskeyCredential {
    /// Build a credential from a registration response: the credential id, the
    /// COSE algorithm, and the public key as SPKI DER (`response.getPublicKey()`).
    /// `None` if `alg` is unsupported, the SPKI prefix doesn't match, or the key
    /// that follows it doesn't parse.
    pub fn from_spki(id: Vec<u8>, alg: i32, spki: &[u8], rp_id: impl Into<String>) -> Option<Self> {
        let public_key = match alg {
            ALG_ES256 => {
                let rest = spki.strip_prefix(&SPKI_P256_PREFIX[..])?;
                if rest.len() != 65 {
                    return None;
                }
                rest.to_vec()
            }
            ALG_EDDSA => {
                let rest = spki.strip_prefix(&SPKI_ED25519_PREFIX[..])?;
                if rest.len() != 32 {
                    return None;
                }
                rest.to_vec()
            }
            _ => return None,
        };
        let cred = PasskeyCredential { id, alg, public_key, rp_id: rp_id.into() };
        cred.is_well_formed().then_some(cred)
    }

    /// `true` if the shape is self-consistent: a non-empty, bounded id, a
    /// non-empty rp id, and a public key of the right length that actually
    /// parses for its declared algorithm.
    pub fn is_well_formed(&self) -> bool {
        if self.id.is_empty() || self.id.len() > 1023 || self.rp_id.is_empty() {
            return false;
        }
        match self.alg {
            ALG_ES256 => self.public_key.len() == 65 && P256VerifyingKey::from_sec1_bytes(&self.public_key).is_ok(),
            ALG_EDDSA => <[u8; 32]>::try_from(self.public_key.as_slice())
                .ok()
                .is_some_and(|k| EdVerifyingKey::from_bytes(&k).is_ok()),
            _ => false,
        }
    }

    /// Verify a WebAuthn assertion against this credential: the `rpIdHash`, the
    /// user-present flag, `clientDataJSON.type == "webauthn.get"`, the
    /// challenge, and finally the signature.
    ///
    /// The sign counter is deliberately ignored (synced passkeys report 0);
    /// there's no origin check because `rpIdHash` already scopes to the host
    /// and the browser enforces that the RP ID is within the origin. User
    /// *verification* (the UV flag, `0x04`) is deliberately not required: the
    /// ceremony asks for it as `"preferred"`, so a security key without a PIN or
    /// biometric still works with just a touch (user presence).
    pub fn verify(&self, a: &PasskeyAssertion, expected_challenge: &[u8]) -> bool {
        let ad = &a.authenticator_data;
        if ad.len() < 37 || ad[..32] != rp_id_hash(&self.rp_id)[..] || ad[32] & FLAG_UP == 0 {
            return false;
        }
        if client_data_type(&a.client_data_json).as_deref() != Some("webauthn.get") {
            return false;
        }
        if challenge_of(&a.client_data_json) != Some(b64url(expected_challenge)) {
            return false;
        }
        match self.alg {
            ALG_ES256 => verify_es256(&self.public_key, ad, &a.client_data_json, &a.signature),
            ALG_EDDSA => verify_eddsa(&self.public_key, ad, &a.client_data_json, &a.signature),
            _ => false,
        }
    }
}

/// The bytes a WebAuthn assertion signs: `authenticatorData ‖ SHA-256(clientDataJSON)`.
pub fn assertion_message(authenticator_data: &[u8], client_data_json: &[u8]) -> Vec<u8> {
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
    vk.verify(&assertion_message(authenticator_data, client_data_json), &sig).is_ok()
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
    vk.verify_strict(&assertion_message(authenticator_data, client_data_json), &EdSignature::from_bytes(&sig))
        .is_ok()
}

/// SHA-256 of the relying-party id, as stored in `authenticatorData[..32]`.
pub fn rp_id_hash(rp_id: &str) -> [u8; 32] {
    Sha256::digest(rp_id.as_bytes()).into()
}

/// RFC 4648 §5 base64url (`A–Z a–z 0–9 - _`), no padding.
pub fn b64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(b2 & 0x3f) as usize] as char);
        }
    }
    out
}

/// Find a top-level JSON string field `"<field>":"<value>"` in `clientDataJSON`.
/// Minimal parse (no serde_json dependency).
fn json_str_field(cdj: &[u8], field: &str) -> Option<String> {
    let s = core::str::from_utf8(cdj).ok()?;
    let key = format!("\"{field}\":\"");
    let start = s.find(&key)? + key.len();
    let end = start + s[start..].find('"')?;
    Some(s[start..end].to_string())
}

/// Extract the base64url `challenge` field from a `clientDataJSON` — what the
/// passkey vouched for. The caller confirms it equals the expected value (e.g.
/// the base64url of the device id) so the assertion is bound to *this* device.
pub fn challenge_of(client_data_json: &[u8]) -> Option<String> {
    json_str_field(client_data_json, "challenge")
}

/// Extract the `type` field from a `clientDataJSON` — `"webauthn.get"` for an
/// assertion, `"webauthn.create"` for a registration.
pub fn client_data_type(cdj: &[u8]) -> Option<String> {
    json_str_field(cdj, "type")
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
        let sig: P256Signature = signing.sign(&assertion_message(&ad, &cd));
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
        let sig = signing.sign(&assertion_message(&ad, &cd)).to_bytes();
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

    #[test]
    fn b64url_is_url_safe_and_unpadded() {
        assert_eq!(b64url(b"device-id"), "ZGV2aWNlLWlk");
        assert_eq!(b64url(&[0xfb, 0xff]), "-_8");
        assert_eq!(b64url(b""), "");
        assert_eq!(b64url(&[0]), "AA");
    }

    #[test]
    fn from_spki_extracts_es256_and_eddsa_keys() {
        let sk = p256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
        let point = P256VerifyingKey::from(&sk).to_encoded_point(false);
        let mut spki = SPKI_P256_PREFIX.to_vec();
        spki.extend_from_slice(point.as_bytes());

        let cred = PasskeyCredential::from_spki(vec![1], ALG_ES256, &spki, "localhost").expect("es256 spki parses");
        assert_eq!(cred.public_key, point.as_bytes());

        assert!(PasskeyCredential::from_spki(vec![1], -257, &spki, "localhost").is_none());
        assert!(PasskeyCredential::from_spki(vec![1], ALG_EDDSA, &spki, "localhost").is_none());

        let mut bad_prefix = spki.clone();
        bad_prefix[5] ^= 0xff;
        assert!(PasskeyCredential::from_spki(vec![1], ALG_ES256, &bad_prefix, "localhost").is_none());

        let ed_sk = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let mut ed_spki = SPKI_ED25519_PREFIX.to_vec();
        ed_spki.extend_from_slice(&ed_sk.verifying_key().to_bytes());
        assert!(PasskeyCredential::from_spki(vec![1], ALG_EDDSA, &ed_spki, "localhost").is_some());

        // Trailing bytes after the key: not an exact SPKI, rejected.
        for (alg, good) in [(ALG_ES256, &spki), (ALG_EDDSA, &ed_spki)] {
            let mut trailing = good.clone();
            trailing.push(0);
            assert!(PasskeyCredential::from_spki(vec![1], alg, &trailing, "localhost").is_none(), "alg {alg}");
        }
    }

    #[test]
    fn credential_verify_checks_rp_presence_type_and_challenge() {
        let sk = p256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
        let vk = P256VerifyingKey::from(&sk);
        let cred = PasskeyCredential {
            id: vec![1],
            alg: ALG_ES256,
            public_key: vk.to_encoded_point(false).as_bytes().to_vec(),
            rp_id: "localhost".into(),
        };
        let mut ad = rp_id_hash("localhost").to_vec();
        ad.extend_from_slice(&[0x05, 0, 0, 0, 0]);
        let cdj = format!(
            r#"{{"type":"webauthn.get","challenge":"{}","origin":"http://localhost:8140","crossOrigin":false}}"#,
            b64url(&[3u8; 32])
        )
        .into_bytes();
        let sig: P256Signature = sk.sign(&assertion_message(&ad, &cdj));
        let a = PasskeyAssertion {
            authenticator_data: ad.clone(),
            client_data_json: cdj.clone(),
            signature: sig.to_der().as_bytes().to_vec(),
        };

        assert!(cred.verify(&a, &[3u8; 32]));

        assert!(!cred.verify(&a, &[4u8; 32]), "wrong challenge");

        let mut evil_ad = rp_id_hash("evil.example").to_vec();
        evil_ad.extend_from_slice(&[0x05, 0, 0, 0, 0]);
        let evil_a = PasskeyAssertion { authenticator_data: evil_ad, ..a.clone() };
        assert!(!cred.verify(&evil_a, &[3u8; 32]), "wrong rpIdHash");

        let mut no_up_ad = ad.clone();
        no_up_ad[32] = 0x04;
        let no_up_a = PasskeyAssertion { authenticator_data: no_up_ad, ..a.clone() };
        assert!(!cred.verify(&no_up_a, &[3u8; 32]), "UP cleared");

        // Presence without verification (a touch-only security key) is accepted.
        let mut up_only_ad = ad.clone();
        up_only_ad[32] = 0x01;
        let up_only_sig: P256Signature = sk.sign(&assertion_message(&up_only_ad, &cdj));
        let up_only_a = PasskeyAssertion {
            authenticator_data: up_only_ad,
            signature: up_only_sig.to_der().as_bytes().to_vec(),
            ..a.clone()
        };
        assert!(cred.verify(&up_only_a, &[3u8; 32]), "UV isn't required");

        let create_cdj = String::from_utf8(cdj).unwrap().replace("webauthn.get", "webauthn.create").into_bytes();
        let create_a = PasskeyAssertion { client_data_json: create_cdj, ..a.clone() };
        assert!(!cred.verify(&create_a, &[3u8; 32]), "wrong type");

        let other_vk = P256VerifyingKey::from(&p256::ecdsa::SigningKey::from_slice(&[8u8; 32]).unwrap());
        let other_cred =
            PasskeyCredential { public_key: other_vk.to_encoded_point(false).as_bytes().to_vec(), ..cred.clone() };
        assert!(!other_cred.verify(&a, &[3u8; 32]), "different key");
    }

    #[test]
    fn credential_verify_eddsa() {
        let sk = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let cred = PasskeyCredential {
            id: vec![1],
            alg: ALG_EDDSA,
            public_key: sk.verifying_key().to_bytes().to_vec(),
            rp_id: "localhost".into(),
        };
        let mut ad = rp_id_hash("localhost").to_vec();
        ad.extend_from_slice(&[0x05, 0, 0, 0, 0]);
        let cdj = format!(
            r#"{{"type":"webauthn.get","challenge":"{}","origin":"http://localhost:8140","crossOrigin":false}}"#,
            b64url(&[3u8; 32])
        )
        .into_bytes();
        let sig = sk.sign(&assertion_message(&ad, &cdj)).to_bytes();
        let a = PasskeyAssertion { authenticator_data: ad, client_data_json: cdj, signature: sig.to_vec() };
        assert!(cred.verify(&a, &[3u8; 32]));
    }
}
