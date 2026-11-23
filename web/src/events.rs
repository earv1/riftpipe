//! Generic append-only **event log** — the library's history + debugging
//! backbone. Every device appends to its OWN log in the synced tree, so logs
//! merge across peers with zero conflict (one writer per file); their union,
//! time-sorted, is a full cross-peer history.
//!
//! Layout per device: the active `events/<device-id>.jsonl` takes new lines;
//! once it would pass [`SEGMENT_MAX_BYTES`] it is frozen into an immutable
//! segment `events/<device-id>.NNNNNN.jsonl` and the active file restarts. So an
//! append re-sends at most one small file, never the whole history.
//!
//! Appends within a tab go through one writer queue, so concurrent `log` /
//! `logEvent` calls never lose lines to each other's read-modify-write. Sibling
//! tabs of one profile can still race (each has its own queue).
//!
//! App-agnostic: the library records what *it* uniquely knows — remote merges
//! (which file arrived) and auth transitions (claim/grant/request) — while apps
//! append their own semantic events (`logEvent("card.move", …)`). Each layer
//! logs what only it can see; the merged log tells the whole story. Every event
//! also fires a `riftpipe:event` CustomEvent on `window` for live debugging.

use std::cell::RefCell;
use std::collections::HashMap;

use futures_util::StreamExt;
use wasm_bindgen::prelude::*;

/// This device's stable id (hex) — the same signing identity that authors writes.
fn device_hex() -> String {
    crate::tree_sync::load_or_create_device().id().iter().map(|b| format!("{b:02x}")).collect()
}

/// A device's active event-log path, `events/<device-id>.jsonl`.
fn log_path(device: &str) -> String {
    format!("events/{device}.jsonl")
}

/// The active log rotates into an immutable segment past this size, so an append
/// re-sends at most ~16 KiB (≈4 mesh chunks), never the whole history.
pub const SEGMENT_MAX_BYTES: usize = 16 * 1024;

/// What one append writes: optionally a full segment to archive first, then the
/// new active log contents.
pub(crate) struct Planned {
    pub archive: Option<String>,
    pub active: String,
}

/// Plan appending `line` to the `active` log under a `max`-byte cap. A missing
/// trailing newline is repaired; a non-empty log that would pass `max` is
/// archived whole and the active log restarts with just `line`. An empty active
/// log never rotates, even if `line` alone exceeds `max`.
pub(crate) fn plan_append(active: &str, line: &str, max: usize) -> Planned {
    let mut cur = active.to_string();
    if !cur.is_empty() && !cur.ends_with('\n') {
        cur.push('\n');
    }
    if !cur.is_empty() && cur.len() + line.len() + 1 > max {
        return Planned { archive: Some(cur), active: format!("{line}\n") };
    }
    Planned { archive: None, active: format!("{cur}{line}\n") }
}

/// The next segment number for `device`: one past the highest `<device>.<n>.jsonl`
/// in `names` (entries of `events/`), or 1 if it has none. Unparseable names are
/// ignored.
pub(crate) fn next_segment_seq(names: &[String], device: &str) -> u32 {
    let prefix = format!("{device}.");
    names
        .iter()
        .filter_map(|n| n.strip_prefix(&prefix)?.strip_suffix(".jsonl")?.parse::<u32>().ok())
        .max()
        .map_or(1, |m| m + 1)
}

/// Path of `device`'s immutable segment number `seq`, `events/<device>.NNNNNN.jsonl`.
pub(crate) fn segment_path(device: &str, seq: u32) -> String {
    format!("events/{device}.{seq:06}.jsonl")
}

const RESERVED: [&str; 3] = ["ts", "device", "kind"];

/// One JSON event line: `{ts, device, kind, …data}`. Data fields named like a
/// reserved field are dropped — an event's author/time/kind can't be spoofed.
pub(crate) fn event_line(ts: &str, device: &str, kind: &str, data: serde_json::Value) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert("ts".into(), serde_json::json!(ts));
    obj.insert("device".into(), serde_json::json!(device));
    obj.insert("kind".into(), serde_json::json!(kind));
    if let serde_json::Value::Object(m) = data {
        for (k, v) in m {
            if RESERVED.contains(&k.as_str()) {
                continue;
            }
            obj.insert(k, v);
        }
    }
    serde_json::Value::Object(obj).to_string()
}

/// `mesh.merge` is logged at most once per path within this window, so a burst
/// of merges for the same file (e.g. racing writers) doesn't flood the log.
pub const MERGE_LOG_COALESCE_MS: f64 = 10_000.0;

thread_local! {
    /// Last `mesh.merge` log time (ms) per path, for [`should_log_merge`].
    static LAST_MERGE: RefCell<HashMap<String, f64>> = RefCell::new(HashMap::new());
}

/// Whether a `mesh.merge` for `path` at `now_ms` should be logged: never for
/// `events/` paths (would recurse), never again within `window_ms` of the last
/// log for that path. Prunes stale entries once `last` grows past 1024 paths.
pub(crate) fn should_log_merge(
    last: &mut HashMap<String, f64>,
    path: &str,
    now_ms: f64,
    window_ms: f64,
) -> bool {
    if path.starts_with("events/") {
        return false;
    }
    if let Some(t) = last.get(path) {
        if now_ms - t < window_ms {
            return false;
        }
    }
    if last.len() >= 1024 {
        last.retain(|_, t| now_ms - *t < window_ms);
    }
    last.insert(path.to_string(), now_ms);
    true
}

/// Log a `mesh.merge` event for `path`, coalesced per [`MERGE_LOG_COALESCE_MS`].
pub fn log_merge(path: &str) {
    let now = js_sys::Date::now();
    if LAST_MERGE.with(|m| should_log_merge(&mut m.borrow_mut(), path, now, MERGE_LOG_COALESCE_MS)) {
        log("mesh.merge", serde_json::json!({ "path": path }));
    }
}

/// One queued append; `done` fires once its line is written (or dropped).
struct Job {
    kind: String,
    data: serde_json::Value,
    done: Option<futures_channel::oneshot::Sender<()>>,
}

thread_local! {
    /// This tab's single event writer; jobs run strictly one after another.
    static QUEUE: RefCell<Option<futures_channel::mpsc::UnboundedSender<Job>>> = const { RefCell::new(None) };
}

/// Hand `job` to this tab's writer, starting the writer loop on first use.
fn enqueue(job: Job) {
    QUEUE.with(|q| {
        let mut q = q.borrow_mut();
        let tx = q.get_or_insert_with(|| {
            let (tx, mut rx) = futures_channel::mpsc::unbounded::<Job>();
            wasm_bindgen_futures::spawn_local(async move {
                while let Some(j) = rx.next().await {
                    write_one(&j.kind, j.data).await;
                    if let Some(d) = j.done {
                        let _ = d.send(());
                    }
                }
            });
            tx
        });
        let _ = tx.unbounded_send(job);
    });
}

/// Write one `{ts, device, kind, …data}` event: archive the active log into a
/// segment first if it's full, then persist the active log to OPFS and sync both
/// to peers. Never fails loudly — logging is additive history.
async fn write_one(kind: &str, data: serde_json::Value) {
    let ts = js_sys::Date::new_0().to_iso_string().as_string().unwrap_or_default();
    let device = device_hex();
    let line = event_line(&ts, &device, kind, data);
    let path = log_path(&device);

    let current = crate::opfs::read_path(&path).await.unwrap_or_default();
    let planned = plan_append(&current, &line, SEGMENT_MAX_BYTES);
    if let Some(archive) = planned.archive {
        let seq = next_segment_seq(&crate::opfs::list_path("events").await, &device);
        let seg = segment_path(&device, seq);
        if crate::opfs::write_path(&seg, archive.as_bytes()).await.is_err() {
            return;
        }
        crate::tree_sync::push(&seg, archive.as_bytes());
    }
    if crate::opfs::write_path(&path, planned.active.as_bytes()).await.is_ok() {
        crate::tree_sync::push(&path, planned.active.as_bytes());
        dispatch_event(&line);
    }
}

/// Append one event to this device's log via the tab's writer queue; resolves
/// once it has been written.
pub async fn append(kind: &str, data: serde_json::Value) {
    let (tx, rx) = futures_channel::oneshot::channel();
    enqueue(Job { kind: kind.into(), data, done: Some(tx) });
    let _ = rx.await;
}

/// Fire-and-forget append usable from sync contexts (queues the write).
pub fn log(kind: &str, data: serde_json::Value) {
    enqueue(Job { kind: kind.into(), data, done: None });
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

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn event_line_keeps_reserved_fields() {
        let line = event_line(
            "T",
            "me",
            "card.move",
            serde_json::json!({"ts":"x","device":"evil","kind":"y","id":"tk_1"}),
        );
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["ts"], "T");
        assert_eq!(v["device"], "me");
        assert_eq!(v["kind"], "card.move");
        assert_eq!(v["id"], "tk_1");
    }

    #[wasm_bindgen_test]
    fn plan_append_appends_under_cap() {
        let p = plan_append("a\n", "b", 100);
        assert_eq!(p.archive, None);
        assert_eq!(p.active, "a\nb\n");
    }

    #[wasm_bindgen_test]
    fn plan_append_repairs_missing_newline() {
        let p = plan_append("a", "b", 100);
        assert_eq!(p.archive, None);
        assert_eq!(p.active, "a\nb\n");
    }

    #[wasm_bindgen_test]
    fn plan_append_rotates_over_cap() {
        let active = format!("{}\n", "x".repeat(90));
        let line = "y".repeat(20);
        let p = plan_append(&active, &line, 100);
        assert_eq!(p.archive, Some(active));
        assert_eq!(p.active, format!("{line}\n"));
    }

    #[wasm_bindgen_test]
    fn plan_append_never_rotates_empty() {
        let line = "z".repeat(200);
        let p = plan_append("", &line, 100);
        assert_eq!(p.archive, None);
        assert_eq!(p.active, format!("{line}\n"));
    }

    #[wasm_bindgen_test]
    fn next_segment_seq_picks_max_plus_one() {
        let names: Vec<String> =
            ["dev.jsonl", "dev.000001.jsonl", "dev.000003.jsonl", "other.000009.jsonl", "dev.x.jsonl"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        assert_eq!(next_segment_seq(&names, "dev"), 4);
        assert_eq!(next_segment_seq(&[], "dev"), 1);
        assert_eq!(segment_path("dev", 4), "events/dev.000004.jsonl");
    }

    #[wasm_bindgen_test]
    fn should_log_merge_coalesces() {
        let mut last = HashMap::new();
        assert!(!should_log_merge(&mut last, "events/a.jsonl", 0.0, 10_000.0));
        assert!(should_log_merge(&mut last, "x.md", 0.0, 10_000.0));
        assert!(!should_log_merge(&mut last, "x.md", 5000.0, 10_000.0));
        assert!(should_log_merge(&mut last, "x.md", 10_001.0, 10_000.0));
        assert!(should_log_merge(&mut last, "y.md", 5000.0, 10_000.0));
    }
}
