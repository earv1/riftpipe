//! Generic append-only **event log** — the library's history + debugging
//! backbone. Every device appends to its OWN `events/<device-id>.jsonl` in the
//! synced tree, so logs merge across peers with zero conflict (one writer per
//! file); their union, time-sorted, is a full cross-peer history.
//!
//! App-agnostic: the library records what *it* uniquely knows — remote merges
//! (which file arrived) and auth transitions (claim/grant/request) — while apps
//! append their own semantic events (`logEvent("card.move", …)`). Each layer
//! logs what only it can see; the merged log tells the whole story. Every event
//! also fires a `riftpipe:event` CustomEvent on `window` for live debugging.

use wasm_bindgen::prelude::*;

/// This device's stable id (hex) — the same signing identity that authors writes.
fn device_hex() -> String {
    crate::tree_sync::load_or_create_device().id().iter().map(|b| format!("{b:02x}")).collect()
}

/// This device's event-log path, `events/<device-id>.jsonl`.
fn log_path() -> String {
    format!("events/{}.jsonl", device_hex())
}

/// Append one `{ts, device, kind, …data}` event to this device's log, persist to
/// OPFS, and sync it to peers. Never fails loudly — logging is additive history.
/// (Read-modify-write on the per-device file; concurrent appends from sibling
/// tabs can rarely race, same as the retired server's log — acceptable here.)
pub async fn append(kind: &str, data: serde_json::Value) {
    let ts = js_sys::Date::new_0().to_iso_string().as_string().unwrap_or_default();
    let mut obj = serde_json::Map::new();
    obj.insert("ts".into(), serde_json::json!(ts));
    obj.insert("device".into(), serde_json::json!(device_hex()));
    obj.insert("kind".into(), serde_json::json!(kind));
    if let serde_json::Value::Object(m) = data {
        for (k, v) in m {
            obj.insert(k, v);
        }
    }
    let line = serde_json::Value::Object(obj).to_string();

    let path = log_path();
    let mut log = crate::opfs::read_path(&path).await.unwrap_or_default();
    log.push_str(&line);
    log.push('\n');
    if crate::opfs::write_path(&path, log.as_bytes()).await.is_ok() {
        crate::tree_sync::push(&path, log.as_bytes());
        dispatch_event(&line);
    }
}

/// Fire-and-forget append usable from sync contexts (spawns the async write).
pub fn log(kind: &str, data: serde_json::Value) {
    let kind = kind.to_string();
    wasm_bindgen_futures::spawn_local(async move { append(&kind, data).await });
}

/// Fire a `riftpipe:event` CustomEvent (detail: the JSON line) so a debug view
/// can tail the log live — nothing app-specific here.
fn dispatch_event(line: &str) {
    if let Some(w) = web_sys::window() {
        let init = web_sys::CustomEventInit::new();
        init.set_detail(&JsValue::from_str(line));
        if let Ok(ev) = web_sys::CustomEvent::new_with_event_init_dict("riftpipe:event", &init) {
            let _ = w.dispatch_event(&ev);
        }
    }
}

/// App-facing: append a semantic event. `data_json` is a JSON object string
/// (e.g. `{"id":"tk_1","to":"Done"}`); its fields join ts/device/kind on the line.
#[wasm_bindgen(js_name = logEvent)]
pub async fn log_event(kind: String, data_json: String) {
    let data = serde_json::from_str(&data_json).unwrap_or(serde_json::Value::Null);
    append(&kind, data).await;
}

/// The merged, time-sorted history across all peers, as a JSON array string —
/// every `events/*.jsonl`, oldest first.
#[wasm_bindgen(js_name = readEvents)]
pub async fn read_events() -> String {
    let mut events: Vec<serde_json::Value> = Vec::new();
    for fname in crate::opfs::list_path("events").await {
        if !fname.ends_with(".jsonl") {
            continue;
        }
        let text = crate::opfs::read_path(&format!("events/{fname}")).await.unwrap_or_default();
        for line in text.lines() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                events.push(v);
            }
        }
    }
    events.sort_by(|a, b| {
        let ka = a.get("ts").and_then(|v| v.as_str()).unwrap_or("");
        let kb = b.get("ts").and_then(|v| v.as_str()).unwrap_or("");
        ka.cmp(kb)
    });
    serde_json::to_string(&events).unwrap_or_else(|_| "[]".into())
}
