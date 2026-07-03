//! The kanban serve layer — a small JSON file-API + static host over a board
//! directory, ported from the Deno reference server (`projects/kanban/server`).
//! It does **not** re-implement sync: it just reads/writes plain files, and
//! riftpipe's folder mode carries them to peers (DESIGN §17, app-and-frontends.md).
//!
//! On-disk model (human-editable, git-friendly):
//!   <dir>/board.md              "# Title", then "- Column" per column (in order)
//!   <dir>/tickets/<id>/card.md  "# Card Title\n\n<markdown description>"
//!   <dir>/tickets/<id>/meta.toml  column (str), position (int), done (bool)
//!   <dir>/tickets/<id>/comments/<ts>__<author>.md
//!   <id> = "tk_" + 8 lowercase hex.
//!
//! HTTP (what the bundled SolidJS UI calls): GET /api/board, GET /api/cards/:id,
//! GET /api/cards/:id/detail, POST /api/cards, PATCH /api/cards/:id,
//! POST /api/cards/:id/comments, GET /api/events (SSE), and the static SPA.

use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use riftpipe_core::kanban as kb;
use riftpipe_core::kanban::{Card, Comment};
use serde::Serialize;
use serde_json::{json, Value};
use tiny_http::{Header, Method, Request, Response, Server};

/// Shared, cheaply-clonable server state handed to each request thread.
#[derive(Clone)]
struct State {
    dir: PathBuf,
    dist: PathBuf,
    /// SSE subscribers — each gets change frames; dead ones are pruned on send.
    clients: Arc<Mutex<Vec<Sender<Vec<u8>>>>>,
    /// Serializes mutating requests so concurrent create/patch can't interleave
    /// reads+writes of the same files (e.g. duplicate `position`).
    writes: Arc<Mutex<()>>,
}

/// Serve the board `dir` on `127.0.0.1:port`, hosting the SPA from `dist`. Blocks.
pub fn serve(dir: &str, port: u16, dist: &str) -> Result<(), Box<dyn std::error::Error>> {
    let state = State {
        dir: PathBuf::from(dir),
        dist: PathBuf::from(dist),
        clients: Arc::new(Mutex::new(Vec::new())),
        writes: Arc::new(Mutex::new(())),
    };
    std::fs::create_dir_all(&state.dir)?;

    spawn_watcher(state.clone());
    spawn_keepalive(state.clients.clone());

    let server = Server::http(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    eprintln!("[kanban] serving {dir} on http://localhost:{port}  (UI from {dist})");
    for request in server.incoming_requests() {
        let state = state.clone();
        thread::spawn(move || {
            if let Err(e) = route(request, &state) {
                eprintln!("[kanban] request error: {e}");
            }
        });
    }
    Ok(())
}

/// Join a browser peer's board over WebRTC (the same signaling room / shared link)
/// and sync `dir` both ways — the native end of browser↔native board
/// collaboration. Dials the link, then hands the halves to the shared board-sync
/// driver ([`crate::sync::board`]). Blocks until the link drops.
pub async fn connect_board(signal: &str, room: &str, dir: &str) -> crate::net::Result<()> {
    use crate::sync::board;

    let link = crate::net::webrtc::connect_via_signaling(signal, room).await?;
    let (mut sink, mut source) = link.into_halves(Arc::new(crate::net::Counters::default()));
    let dir = board::prepare_dir(Path::new(dir))?;
    eprintln!("[kanban] connected to room '{room}'; syncing board at {}", dir.display());
    // If the watcher can't start, degrade to the poll fallback — never abort.
    let (watcher, mut rx) = board::watch(&dir);
    let mut peer = board::BoardPeer::new();
    board::run(&mut peer, &mut rx, watcher.is_none(), &mut sink, &mut source, &dir).await
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

fn route(mut request: Request, state: &State) -> io::Result<()> {
    let url = request.url().to_string();
    let path = url.split('?').next().unwrap_or("").to_string();
    let segs: Vec<&str> = path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    let method = request.method().clone();

    // Serialize mutating requests (GET/SSE stay concurrent).
    let _writes = (method != Method::Get).then(|| state.writes.lock().unwrap_or_else(|e| e.into_inner()));

    match (&method, segs.as_slice()) {
        (Method::Get, ["api", "board"]) => {
            respond_json(request, &read_board(&state.dir))
        }
        (Method::Get, ["api", "events"]) => sse(request, state),
        (Method::Get, ["api", "cards", id, "detail"]) => match read_detail(&state.dir, id) {
            Some(d) => respond_json(request, &d),
            None => respond(request, 404, "Not Found"),
        },
        (Method::Get, ["api", "cards", id]) => {
            if !card_dir(&state.dir, id).is_dir() {
                return respond(request, 404, "Not Found");
            }
            let cols = read_board_meta(&state.dir).1;
            respond_json(request, &read_card(&state.dir, id, &cols))
        }
        (Method::Post, ["api", "cards"]) => {
            let body = read_json(&mut request);
            let column = body.get("column").and_then(Value::as_str).unwrap_or("");
            let title = body.get("title").and_then(Value::as_str).unwrap_or("");
            let card = create_card(&state.dir, column, title);
            respond_json(request, &card)
        }
        (Method::Patch, ["api", "cards", id]) => {
            let body = read_json(&mut request);
            match patch_card(&state.dir, id, &body) {
                Some(card) => respond_json(request, &card),
                None => respond(request, 404, "Not Found"),
            }
        }
        (Method::Post, ["api", "cards", id, "comments"]) => {
            if !card_dir(&state.dir, id).is_dir() {
                return respond(request, 404, "Not Found");
            }
            let body = read_json(&mut request);
            let text = body.get("text").and_then(Value::as_str).unwrap_or("").trim().to_string();
            if text.is_empty() {
                return respond(request, 400, "Comment text is required");
            }
            let author = body.get("author").and_then(Value::as_str).unwrap_or("anon");
            let comment = add_comment(&state.dir, id, author, &text);
            respond_json(request, &comment)
        }
        (Method::Get, _) if !path.starts_with("/api/") => serve_static(request, state, &path),
        _ => respond(request, 404, "Not Found"),
    }
}

// ---------------------------------------------------------------------------
// Board / card reading
// ---------------------------------------------------------------------------

fn tickets_dir(dir: &Path) -> PathBuf {
    dir.join("tickets")
}
fn card_dir(dir: &Path, id: &str) -> PathBuf {
    tickets_dir(dir).join(id)
}

fn read_text(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// board.md → (title, ordered columns). Robust to a missing/blank file.
fn read_board_meta(dir: &Path) -> (String, Vec<String>) {
    kb::parse_board_md(&read_text(&dir.join("board.md")))
}

/// Read one card, falling back to defaults for any missing/invalid piece.
fn read_card(dir: &Path, id: &str, columns: &[String]) -> Card {
    let default_column = columns.first().map(String::as_str).unwrap_or("Todo");
    kb::card_from_files(
        id,
        &read_text(&card_dir(dir, id).join("card.md")),
        &read_text(&card_dir(dir, id).join("meta.toml")),
        default_column,
    )
}

fn read_board(dir: &Path) -> Value {
    let (title, columns) = read_board_meta(dir);
    let mut cards = Vec::new();
    if let Ok(entries) = std::fs::read_dir(tickets_dir(dir)) {
        for e in entries.flatten() {
            if e.path().is_dir() {
                if let Some(name) = e.file_name().to_str() {
                    let c = read_card(dir, name, &columns);
                    cards.push(json!({
                        "id": c.id, "title": c.title, "column": c.column,
                        "position": c.position, "done": c.done,
                    }));
                }
            }
        }
    }
    json!({ "title": title, "columns": columns, "cards": cards })
}

fn read_detail(dir: &Path, id: &str) -> Option<Value> {
    if !card_dir(dir, id).is_dir() {
        return None;
    }
    let cols = read_board_meta(dir).1;
    let card = read_card(dir, id, &cols);
    let (_t, description) = kb::split_card_md(&read_text(&card_dir(dir, id).join("card.md")));
    let comments = read_comments(dir, id);
    Some(json!({
        "id": card.id, "title": card.title, "column": card.column,
        "position": card.position, "done": card.done,
        "description": description, "comments": comments,
    }))
}

// ---------------------------------------------------------------------------
// Comments
// ---------------------------------------------------------------------------

fn comments_dir(dir: &Path, id: &str) -> PathBuf {
    card_dir(dir, id).join("comments")
}

fn read_comments(dir: &Path, id: &str) -> Vec<Comment> {
    let mut comments = Vec::new();
    if let Ok(entries) = std::fs::read_dir(comments_dir(dir, id)) {
        for e in entries.flatten() {
            let fname = e.file_name();
            let Some(name) = fname.to_str().and_then(|n| n.strip_suffix(".md")) else {
                continue;
            };
            let Some((ts, author)) = kb::parse_comment_name(name) else {
                continue;
            };
            let text = read_text(&comments_dir(dir, id).join(format!("{name}.md"))).trim_end().to_string();
            comments.push(Comment { id: name.to_string(), author, ts, text });
        }
    }
    comments.sort_by(|a, b| a.ts.cmp(&b.ts));
    comments
}

fn add_comment(dir: &Path, id: &str, author: &str, text: &str) -> Comment {
    let slug = kb::sanitize_author(author);
    let ts = iso_now().replace(':', "-");
    let name = format!("{ts}__{slug}");
    let _ = std::fs::create_dir_all(comments_dir(dir, id));
    let _ = std::fs::write(comments_dir(dir, id).join(format!("{name}.md")), text);
    Comment { id: name, author: slug, ts, text: text.to_string() }
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

fn write_meta(dir: &Path, id: &str, column: &str, position: i64, done: bool) {
    let _ = std::fs::write(card_dir(dir, id).join("meta.toml"), kb::meta_toml(column, position, done));
}

fn write_card_md(dir: &Path, id: &str, title: &str, description: &str) {
    let _ = std::fs::write(card_dir(dir, id).join("card.md"), kb::card_md(title, description));
}

fn new_id() -> String {
    format!("tk_{:08x}", rand::random::<u32>())
}

fn create_card(dir: &Path, column: &str, title: &str) -> Card {
    let (_t, columns) = read_board_meta(dir);
    let col = if !column.is_empty() {
        column.to_string()
    } else {
        columns.first().cloned().unwrap_or_else(|| "Todo".to_string())
    };
    // Next position = max in that column + 1.
    let mut max_pos = -1i64;
    if let Ok(entries) = std::fs::read_dir(tickets_dir(dir)) {
        for e in entries.flatten() {
            if e.path().is_dir() {
                if let Some(name) = e.file_name().to_str() {
                    let c = read_card(dir, name, &columns);
                    if c.column == col {
                        max_pos = max_pos.max(c.position);
                    }
                }
            }
        }
    }
    let id = new_id();
    let title = if title.is_empty() { id.clone() } else { title.to_string() };
    let card = Card { id: id.clone(), title: title.clone(), column: col.clone(), position: max_pos + 1, done: false };
    let _ = std::fs::create_dir_all(comments_dir(dir, &id));
    write_card_md(dir, &id, &title, "");
    write_meta(dir, &id, &col, card.position, false);
    card
}

fn patch_card(dir: &Path, id: &str, patch: &Value) -> Option<Card> {
    if !card_dir(dir, id).is_dir() {
        return None;
    }
    let cols = read_board_meta(dir).1;
    let mut current = read_card(dir, id, &cols);

    if let Some(c) = patch.get("column").and_then(Value::as_str) {
        current.column = c.to_string();
    }
    if let Some(p) = patch.get("position").and_then(Value::as_i64) {
        current.position = p;
    }
    if let Some(d) = patch.get("done").and_then(Value::as_bool) {
        current.done = d;
    }
    write_meta(dir, id, &current.column, current.position, current.done);

    if patch.get("title").is_some() || patch.get("description").is_some() {
        let existing = kb::split_card_md(&read_text(&card_dir(dir, id).join("card.md")));
        let title = patch.get("title").and_then(Value::as_str).map(str::to_string).unwrap_or(existing.0);
        let description = patch.get("description").and_then(Value::as_str).map(str::to_string).unwrap_or(existing.1);
        let title = if title.is_empty() { id.to_string() } else { title };
        write_card_md(dir, id, &title, &description);
        current.title = title;
    }
    Some(current)
}

// ---------------------------------------------------------------------------
// Change notifications (SSE) — a notify watcher broadcasts to subscribers
// ---------------------------------------------------------------------------

/// Map a changed path to an SSE message, or None to ignore (events log, .site).
fn message_for_path(p: &Path) -> Option<Value> {
    let s = p.to_string_lossy().replace('\\', "/");
    if s.contains("/events/") || s.ends_with("/.site") {
        return None;
    }
    if let Some(idx) = s.find("/tickets/") {
        let rest = &s[idx + "/tickets/".len()..];
        let id = rest.split('/').next().unwrap_or("");
        if !id.is_empty() {
            return Some(json!({ "type": "ticket", "id": id }));
        }
    }
    if s.ends_with("board.md") {
        return Some(json!({ "type": "board" }));
    }
    None
}

fn broadcast(clients: &Arc<Mutex<Vec<Sender<Vec<u8>>>>>, frame: Vec<u8>) {
    let mut guard = clients.lock().unwrap();
    guard.retain(|tx| tx.send(frame.clone()).is_ok());
}

fn spawn_watcher(state: State) {
    use notify::{RecursiveMode, Watcher};
    thread::spawn(move || {
        let clients = state.clients.clone();
        let mut watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if let Ok(ev) = res {
                let mut seen = std::collections::HashSet::new();
                for p in ev.paths {
                    if let Some(msg) = message_for_path(&p) {
                        let key = msg.to_string();
                        if seen.insert(key) {
                            let frame = format!("data: {msg}\n\n").into_bytes();
                            broadcast(&clients, frame);
                        }
                    }
                }
            }
        }) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("[kanban] watcher init failed: {e}");
                return;
            }
        };
        if let Err(e) = watcher.watch(&state.dir, RecursiveMode::Recursive) {
            eprintln!("[kanban] watch failed: {e}");
            return;
        }
        // Keep the watcher alive for the process lifetime.
        loop {
            thread::sleep(Duration::from_secs(3600));
        }
    });
}

/// Periodic SSE comment so dead connections are detected/pruned and proxies
/// don't time the stream out.
fn spawn_keepalive(clients: Arc<Mutex<Vec<Sender<Vec<u8>>>>>) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(15));
        broadcast(&clients, b":\n\n".to_vec());
    });
}

/// A `Read` that yields SSE frames from a channel, blocking until the next one.
struct SseReader {
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
    pos: usize,
}

impl Read for SseReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.buf.len() {
            match self.rx.recv() {
                Ok(frame) => {
                    self.buf = frame;
                    self.pos = 0;
                }
                Err(_) => return Ok(0), // server gone → EOF
            }
        }
        let n = (self.buf.len() - self.pos).min(out.len());
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

fn sse(request: Request, state: &State) -> io::Result<()> {
    let (tx, rx) = channel::<Vec<u8>>();
    // Send an initial comment so the stream opens immediately.
    let _ = tx.send(b":ok\n\n".to_vec());
    state.clients.lock().unwrap().push(tx);
    let reader = SseReader { rx, buf: Vec::new(), pos: 0 };
    let response = Response::new(
        tiny_http::StatusCode(200),
        vec![
            header("Content-Type", "text/event-stream"),
            header("Cache-Control", "no-cache"),
            header("Connection", "keep-alive"),
        ],
        reader,
        None, // chunked — the stream never ends
        None,
    );
    request.respond(response)
}

// ---------------------------------------------------------------------------
// Static files (the built SPA), with index.html fallback for client routing
// ---------------------------------------------------------------------------

fn serve_static(request: Request, state: &State, path: &str) -> io::Result<()> {
    let rel = path.trim_start_matches('/');
    let candidate = if rel.is_empty() { state.dist.join("index.html") } else { state.dist.join(rel) };
    // Resolve `..`/symlinks and confirm the result is STILL inside dist — a lexical
    // `starts_with` is fooled by `..` (`dist/../../etc/passwd` lexically starts with
    // `dist`), so canonicalize both and compare. A miss falls back to the SPA shell.
    let safe = std::fs::canonicalize(&candidate).ok().filter(|c| {
        c.is_file() && std::fs::canonicalize(&state.dist).map(|d| c.starts_with(d)).unwrap_or(false)
    });
    let target = match safe {
        Some(c) => c,
        None => state.dist.join("index.html"), // SPA fallback
    };
    match std::fs::read(&target) {
        Ok(bytes) => {
            let ct = content_type(&target);
            let response = Response::from_data(bytes).with_header(header("Content-Type", ct));
            request.respond(response)
        }
        Err(_) => respond(request, 404, "Not Found"),
    }
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") | Some("mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("png") => "image/png",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("valid header")
}

/// Cap a request body at 1 MiB so a client can't stream unbounded data into RAM.
const MAX_BODY: u64 = 1 << 20;

fn read_json(request: &mut Request) -> Value {
    let mut body = String::new();
    let _ = request.as_reader().take(MAX_BODY).read_to_string(&mut body);
    serde_json::from_str(&body).unwrap_or(Value::Null)
}

fn respond_json<T: Serialize>(request: Request, value: &T) -> io::Result<()> {
    let body = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    request.respond(Response::from_string(body).with_header(header("Content-Type", "application/json")))
}

fn respond(request: Request, status: u16, msg: &str) -> io::Result<()> {
    request.respond(Response::from_string(msg).with_status_code(status))
}

/// UTC ISO-8601 timestamp with millis, e.g. `2026-06-27T18:37:19.123Z`. Millis
/// keep same-author comments from colliding on the same-second filename.
fn iso_now() -> String {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let millis = dur.subsec_millis();
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // civil-from-days (Howard Hinnant's algorithm).
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{millis:03}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Board/card/comment parsing lives in (and is tested by) riftpipe_core::kanban.
    // Here we only cover the native-only bits: id generation and the clock.
    #[test]
    fn ids_and_iso_are_well_formed() {
        let id = new_id();
        assert!(id.starts_with("tk_") && id.len() == 11);
        let ts = iso_now();
        assert_eq!(ts.len(), 24); // YYYY-MM-DDTHH:MM:SS.mmmZ
        assert!(ts.ends_with('Z') && ts.contains('T') && ts.contains('.'));
    }
}
