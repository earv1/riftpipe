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
//! Event files are whole-file LWW over best-effort gossip. If a segment's one
//! push is dropped but the restarted (short) active file lands, a live peer
//! would lose those lines until it reconnects — so for
//! [`SEGMENT_REPUSH_WINDOW_MS`] after a rotation, pushes re-send the recent
//! segments too (at most once per [`SEGMENT_REPUSH_MIN_GAP_MS`] each), and one
//! re-push is scheduled that long after each rotation.
//!
//! Pushes are **batched**: an append writes OPFS (and fires `riftpipe:event`)
//! at once, but the active file goes to the mesh on a trailing
//! [`EVENT_PUSH_DEBOUNCE_MS`] timer (at most [`EVENT_PUSH_MAX_WAIT_MS`] after
//! the oldest unpushed line). `mesh.merge` lines don't trigger a push of their
//! own; they ride along with the next one, or go out after
//! [`MERGE_PUSH_DELAY_MS`]. Without this every remote merge made every peer
//! re-broadcast its log — N² multi-chunk traffic per board write, crowding out
//! the board files themselves on best-effort gossip.
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

/// How long after a rotation appends keep re-pushing that segment. Time-based,
/// not count-based: drops cluster in bursts, and the append that actually lands
/// is usually the first one after the burst, however many appends that was.
pub const SEGMENT_REPUSH_WINDOW_MS: f64 = 30_000.0;
/// Minimum gap between re-pushes of one segment, bounding the extra traffic to
/// ≤ 6 segment sends per rotation, and none once the window closes.
pub const SEGMENT_REPUSH_MIN_GAP_MS: f64 = 5_000.0;

/// A segment rotated in this tab recently, for [`due_repushes`].
pub(crate) struct RecentSegment {
    pub board: [u8; 32],
    pub path: String,
    pub rotated_ms: f64,
    pub pushed_ms: f64,
}

/// The recent segments to re-push alongside an append at `now_ms`: drops those
/// past `window_ms` since rotation, returns those not pushed within `gap_ms`
/// (and marks them pushed now), oldest first.
pub(crate) fn due_repushes(recent: &mut Vec<RecentSegment>, now_ms: f64, window_ms: f64, gap_ms: f64) -> Vec<String> {
    recent.retain(|r| now_ms - r.rotated_ms < window_ms);
    let mut due = Vec::new();
    for r in recent.iter_mut() {
        if now_ms - r.pushed_ms >= gap_ms {
            r.pushed_ms = now_ms;
            due.push(r.path.clone());
        }
    }
    due
}

thread_local! {
    /// Segments this tab rotated within the re-push window.
    static RECENT_SEGMENTS: RefCell<Vec<RecentSegment>> = const { RefCell::new(Vec::new()) };
}

/// Trailing debounce for pushing the active log after an app/auth event.
pub const EVENT_PUSH_DEBOUNCE_MS: f64 = 500.0;
/// A steady stream of appends still pushes at least this often.
pub const EVENT_PUSH_MAX_WAIT_MS: f64 = 2_000.0;
/// A `mesh.merge`-only change is pushed this long after it (or sooner, with the
/// next app/auth event).
pub const MERGE_PUSH_DELAY_MS: f64 = 15_000.0;

/// When the active log is next due on the mesh: `first_ms` is the oldest
/// unpushed append, `due_ms` the push time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct FlushPlan {
    pub first_ms: f64,
    pub due_ms: f64,
}

/// Plan the next push after an append at `now_ms`. `trailing` (app/auth
/// events) debounces by `delay_ms` but never past `max_wait_ms` after the
/// oldest unpushed append; otherwise (`mesh.merge`) the push is due `delay_ms`
/// out and an already-planned earlier push is never postponed.
pub(crate) fn plan_flush(cur: Option<FlushPlan>, now_ms: f64, delay_ms: f64, trailing: bool, max_wait_ms: f64) -> FlushPlan {
    let first_ms = cur.map_or(now_ms, |c| c.first_ms);
    let due_ms = if trailing {
        (now_ms + delay_ms).min(first_ms + max_wait_ms)
    } else {
        cur.map_or(now_ms + delay_ms, |c| c.due_ms.min(now_ms + delay_ms))
    };
    FlushPlan { first_ms, due_ms }
}

/// The active log as last written, awaiting (or past) its push.
struct ActiveLog {
    board: [u8; 32],
    path: String,
    content: String,
    dirty: bool,
}

thread_local! {
    static FLUSH: RefCell<Option<FlushPlan>> = const { RefCell::new(None) };
    static ACTIVE: RefCell<Option<ActiveLog>> = const { RefCell::new(None) };
}

/// Plan a push after an append and arm a timer for it.
fn schedule_flush(now: f64, kind: &str) {
    let (delay, trailing) =
        if kind == "mesh.merge" { (MERGE_PUSH_DELAY_MS, false) } else { (EVENT_PUSH_DEBOUNCE_MS, true) };
    let plan = FLUSH.with(|f| {
        let p = plan_flush(*f.borrow(), now, delay, trailing, EVENT_PUSH_MAX_WAIT_MS);
        *f.borrow_mut() = Some(p);
        p
    });
    let wait = (plan.due_ms - now).max(0.0) as u32;
    wasm_bindgen_futures::spawn_local(async move {
        gloo_timers::future::TimeoutFuture::new(wait).await;
        let now = js_sys::Date::now();
        let due = FLUSH.with(|f| {
            let mut f = f.borrow_mut();
            match *f {
                Some(p) if now + 1.0 >= p.due_ms => {
                    *f = None;
                    true
                }
                _ => false,
            }
        });
        if due {
            push_pending(false).await;
        }
    });
}

/// Push what's pending: recent segments due a re-push, then the active log if
/// it changed since its last push (or, with `force`, regardless). Dropped if
/// the page has switched boards since — another board's log must never reach
/// this board's mesh.
async fn push_pending(force: bool) {
    let board = crate::opfs::active_board();
    let now = js_sys::Date::now();
    let segs = RECENT_SEGMENTS.with(|r| {
        let mut r = r.borrow_mut();
        r.retain(|s| s.board == board);
        due_repushes(&mut r, now, SEGMENT_REPUSH_WINDOW_MS, SEGMENT_REPUSH_MIN_GAP_MS)
    });
    for seg in &segs {
        if let Some(bytes) = crate::opfs::read_path(seg).await {
            crate::tree_sync::push(seg, bytes.as_bytes());
        }
    }
    let force = force || !segs.is_empty();
    let active = ACTIVE.with(|a| {
        let mut a = a.borrow_mut();
        match a.as_mut() {
            Some(l) if l.board != board => {
                *a = None;
                None
            }
            Some(l) if l.dirty || force => {
                l.dirty = false;
                Some((l.path.clone(), l.content.clone()))
            }
            _ => None,
        }
    });
    if let Some((path, content)) = active {
        crate::tree_sync::push(&path, content.as_bytes());
    }
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

    let board = crate::opfs::active_board();
    let current = crate::opfs::read_path(&path).await.unwrap_or_default();
    let planned = plan_append(&current, &line, SEGMENT_MAX_BYTES);
    let now = js_sys::Date::now();
    if let Some(archive) = planned.archive {
        let seq = next_segment_seq(&crate::opfs::list_path("events").await, &device);
        let seg = segment_path(&device, seq);
        if crate::opfs::write_path(&seg, archive.as_bytes()).await.is_err() {
            return;
        }
        // A segment goes out at once (the restarted active file must not land
        // first and shrink a peer's copy), then once more after the re-push gap.
        crate::tree_sync::push(&seg, archive.as_bytes());
        RECENT_SEGMENTS.with(|r| r.borrow_mut().push(RecentSegment { board, path: seg, rotated_ms: now, pushed_ms: now }));
        wasm_bindgen_futures::spawn_local(async {
            gloo_timers::future::TimeoutFuture::new(SEGMENT_REPUSH_MIN_GAP_MS as u32 + 50).await;
            push_pending(true).await;
        });
    }
    if crate::opfs::write_path(&path, planned.active.as_bytes()).await.is_ok() {
        ACTIVE.with(|a| *a.borrow_mut() = Some(ActiveLog { board, path, content: planned.active, dirty: true }));
        schedule_flush(now, kind);
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

/// The JSON-object lines of one log file. Anyone with the link can write
/// `events/*` on an open board, so non-JSON lines and non-object values
/// (`null`, `7`, `[…]`) are skipped; apps still validate field types.
pub(crate) fn parse_event_lines(text: &str) -> Vec<serde_json::Value> {
    text.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|v| v.is_object())
        .collect()
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
        events.extend(parse_event_lines(&text));
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
    fn due_repushes_throttles_within_window_and_expires() {
        let seg = |p: &str, t: f64| RecentSegment { board: [0; 32], path: p.into(), rotated_ms: t, pushed_ms: t };
        let mut recent = vec![seg("s1", 0.0), seg("s2", 1_000.0)];
        // Inside the gap of both: nothing due.
        assert!(due_repushes(&mut recent, 4_000.0, 30_000.0, 5_000.0).is_empty());
        // s1 is past its gap, s2 not yet.
        assert_eq!(due_repushes(&mut recent, 5_500.0, 30_000.0, 5_000.0), vec!["s1"]);
        // Both due now (s1 re-pushed at 5.5 s; burst of rotations all covered).
        assert_eq!(due_repushes(&mut recent, 11_000.0, 30_000.0, 5_000.0), vec!["s1", "s2"]);
        // s1 leaves the window at 30 s; s2 stays until 31 s.
        assert_eq!(due_repushes(&mut recent, 30_500.0, 30_000.0, 5_000.0), vec!["s2"]);
        assert_eq!(recent.len(), 1);
        assert!(due_repushes(&mut recent, 31_000.0, 30_000.0, 5_000.0).is_empty());
        assert!(recent.is_empty());
    }

    #[wasm_bindgen_test]
    fn parse_event_lines_keeps_only_objects() {
        let text = "null\n7\n[1]\n\"s\"\nnot json\n{}\n{\"ts\":\"T\",\"device\":\"d\",\"kind\":\"k\"}\n";
        let v = parse_event_lines(text);
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], serde_json::json!({}));
        assert_eq!(v[1]["kind"], "k");
    }

    #[wasm_bindgen_test]
    fn plan_flush_debounces_caps_and_defers_merges() {
        let (d, w) = (500.0, 2_000.0);
        // App event: due one debounce out; more appends extend it…
        let p = plan_flush(None, 0.0, d, true, w);
        assert_eq!(p, FlushPlan { first_ms: 0.0, due_ms: 500.0 });
        let p = plan_flush(Some(p), 400.0, d, true, w);
        assert_eq!(p.due_ms, 900.0);
        // …but never past max-wait after the oldest unpushed append.
        let p = plan_flush(Some(p), 1_800.0, d, true, w);
        assert_eq!(p, FlushPlan { first_ms: 0.0, due_ms: 2_000.0 });
        // A merge alone waits the long delay, and never postpones a plan.
        let m = plan_flush(None, 0.0, 15_000.0, false, w);
        assert_eq!(m.due_ms, 15_000.0);
        assert_eq!(plan_flush(Some(m), 5_000.0, 15_000.0, false, w).due_ms, 15_000.0);
        assert_eq!(plan_flush(Some(p), 1_900.0, 15_000.0, false, w).due_ms, 2_000.0);
        // An app event after a merge pushes promptly, capped by the merge's age.
        assert_eq!(plan_flush(Some(m), 1_000.0, d, true, w).due_ms, 1_500.0);
        assert_eq!(plan_flush(Some(m), 9_000.0, d, true, w).due_ms, 2_000.0);
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
