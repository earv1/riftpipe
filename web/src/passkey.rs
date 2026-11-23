//! The browser WebAuthn ceremony for passkey device-linking — thin glue over
//! `navigator.credentials`; all verification lives in `riftpipe_core::passkey`
//! plus the trust log, offline, on every peer. The RP ID is the page hostname
//! (Pages: `<user>.github.io`, shared by every project site on that host;
//! tests: `localhost`). WebAuthn rejects IP-address origins such as
//! `127.0.0.1`. There's no server.

use riftpipe_core::passkey::{PasskeyAssertion, PasskeyCredential};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(inline_js = r#"
export function rpSupported() {
  const h = location.hostname;
  const ip = /^[0-9.]+$/.test(h) || h.includes(":");
  return !!(window.isSecureContext && window.PublicKeyCredential && navigator.credentials) && h !== "" && !ip;
}
export function rpCreate(rpId, userId, userName, challenge) {
  return navigator.credentials.create({ publicKey: {
    rp: { id: rpId, name: "riftpipe" },
    user: { id: userId.slice(), name: userName, displayName: userName },
    challenge: challenge.slice(),
    pubKeyCredParams: [{ type: "public-key", alg: -7 }, { type: "public-key", alg: -8 }],
    authenticatorSelection: { residentKey: "preferred", userVerification: "preferred" },
    attestation: "none",
    timeout: 120000,
  }}).then((c) => {
    const pk = c.response.getPublicKey ? c.response.getPublicKey() : null;
    if (!pk) throw new Error("this browser can't export the passkey public key");
    return { id: new Uint8Array(c.rawId), alg: c.response.getPublicKeyAlgorithm(), spki: new Uint8Array(pk) };
  });
}
export function rpGet(rpId, credIds, challenge) {
  return navigator.credentials.get({ publicKey: {
    rpId,
    challenge: challenge.slice(),
    allowCredentials: credIds.map((id) => ({ type: "public-key", id })),
    userVerification: "preferred",
    timeout: 120000,
  }}).then((c) => ({
    id: new Uint8Array(c.rawId),
    authenticatorData: new Uint8Array(c.response.authenticatorData),
    clientDataJSON: new Uint8Array(c.response.clientDataJSON),
    signature: new Uint8Array(c.response.signature),
  }));
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = rpSupported)]
    fn rp_supported() -> bool;
    #[wasm_bindgen(js_name = rpCreate, catch)]
    fn rp_create(rp_id: &str, user_id: &[u8], user_name: &str, challenge: &[u8]) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(js_name = rpGet, catch)]
    fn rp_get(rp_id: &str, cred_ids: &js_sys::Array, challenge: &[u8]) -> Result<js_sys::Promise, JsValue>;
}
// The `.slice()` calls copy the wasm-memory views synchronously, before anything is awaited.

/// `hex(postcard(PasskeyCredential))`, public data only. This browser's passkey,
/// reused for later claims.
const PASSKEY_STORE_KEY: &str = "riftpipe:passkey";

fn hostname() -> String {
    web_sys::window().and_then(|w| w.location().hostname().ok()).unwrap_or_default()
}

fn bytes_of(obj: &JsValue, field: &str) -> Option<Vec<u8>> {
    js_sys::Reflect::get(obj, &JsValue::from_str(field)).ok()?.dyn_into::<js_sys::Uint8Array>().ok().map(|a| a.to_vec())
}

async fn ceremony(p: Result<js_sys::Promise, JsValue>) -> Option<JsValue> {
    match p {
        Ok(p) => match JsFuture::from(p).await {
            Ok(v) => Some(v),
            Err(e) => {
                web_sys::console::warn_2(&"[passkey] ceremony failed:".into(), &e);
                None
            }
        },
        Err(e) => {
            web_sys::console::warn_2(&"[passkey] ceremony failed:".into(), &e);
            None
        }
    }
}

/// Is passkey device-linking usable on this page? Needs a secure context with
/// WebAuthn and a domain origin; use `localhost`, not `127.0.0.1`.
#[wasm_bindgen(js_name = passkeySupported)]
pub fn passkey_supported() -> bool {
    rp_supported()
}

/// This browser's passkey for `rp_id`: the one already created and stored, or
/// a freshly created one (one platform prompt).
async fn passkey_for_claim(rp_id: &str) -> Option<PasskeyCredential> {
    if let Some(c) = crate::gossip::load_hex_postcard::<PasskeyCredential>(PASSKEY_STORE_KEY) {
        if c.rp_id == rp_id {
            return Some(c);
        }
    }
    let s = riftpipe_core::crypto::generate_secret();
    let name = format!(
        "riftpipe · {}",
        crate::tree_sync::load_or_create_device().id()[..4].iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
    let obj = ceremony(rp_create(rp_id, &s[..16], &name, &s[16..])).await?;
    let alg = js_sys::Reflect::get(&obj, &"alg".into()).ok()?.as_f64()? as i32;
    let cred = PasskeyCredential::from_spki(bytes_of(&obj, "id")?, alg, &bytes_of(&obj, "spki")?, rp_id);
    let Some(cred) = cred else {
        web_sys::console::warn_1(
            &format!("[passkey] unsupported credential algorithm {alg} — need ES256 (-7) or EdDSA (-8)").into(),
        );
        return None;
    };
    crate::gossip::store_hex_postcard(PASSKEY_STORE_KEY, &cred);
    Some(cred)
}

/// Claim with passkey: registers this browser's passkey (creating one, one platform
/// prompt, if none yet) on the board's trust log, claiming the board first if it's
/// unclaimed. `true` once registered; `false` if unsupported, cancelled, or the
/// board belongs to someone else.
#[wasm_bindgen(js_name = claimWithPasskey)]
pub async fn claim_with_passkey() -> Result<bool, JsValue> {
    if !rp_supported() {
        return Ok(false);
    }
    let rp_id = hostname();
    let Some(cred) = passkey_for_claim(&rp_id).await else {
        return Ok(false);
    };
    let ok = crate::gossip::active_claim_with_passkey(cred);
    if ok {
        crate::events::log("auth.passkey.register", serde_json::json!({ "rpId": rp_id }));
    }
    Ok(ok)
}

/// Link with passkey: join a claimed board as one of its owner's devices by
/// asserting the board's registered passkey. It reaches this device through
/// platform sync (iCloud Keychain, Google Password Manager, 1Password…), a
/// security key, or a phone. Every peer verifies it offline; no approve click.
/// `true` once authorized; reading starts once an online Admin wraps this device in.
#[wasm_bindgen(js_name = linkWithPasskey)]
pub async fn link_with_passkey() -> Result<bool, JsValue> {
    if !rp_supported() {
        return Ok(false);
    }
    let rp_id = hostname();
    let Some((challenge, nonce, creds)) = crate::gossip::active_passkey_link_ask(&rp_id) else {
        return Ok(false);
    };
    let ids = js_sys::Array::new();
    for c in &creds {
        ids.push(&js_sys::Uint8Array::from(&c.id[..]));
    }
    let Some(obj) = ceremony(rp_get(&rp_id, &ids, &challenge)).await else {
        return Ok(false);
    };
    let (Some(id), Some(authenticator_data), Some(client_data_json), Some(signature)) = (
        bytes_of(&obj, "id"),
        bytes_of(&obj, "authenticatorData"),
        bytes_of(&obj, "clientDataJSON"),
        bytes_of(&obj, "signature"),
    ) else {
        return Ok(false);
    };
    let assertion = PasskeyAssertion { authenticator_data, client_data_json, signature };
    let ok = crate::gossip::active_link_by_passkey(id.clone(), nonce, assertion);
    if ok {
        if let Some(cred) = creds.into_iter().find(|c| c.id == id) {
            crate::gossip::store_hex_postcard(PASSKEY_STORE_KEY, &cred);
        }
        let device = crate::tree_sync::load_or_create_device().id().iter().map(|b| format!("{b:02x}")).collect::<String>();
        crate::events::log("auth.passkey.link", serde_json::json!({ "device": device }));
    }
    Ok(ok)
}
