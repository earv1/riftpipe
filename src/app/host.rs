//! Generic HTTP hosting over a live directory — the app-agnostic serving layer
//! any file-backed app can build on (the kanban server does; `riftpipe serve`
//! is the plain CLI face). Two capabilities, no app routes:
//!
//!   * static file serving of a directory, with an `index.html` SPA fallback
//!     for client-side routing and a content-type map,
//!   * SSE change events: a notify watcher on a directory broadcasts JSON
//!     change frames to every subscribed client (plus a keepalive comment so
//!     dead connections are detected/pruned and proxies don't time out).
//!
//! A [`Host`] owns the subscriber registry; an app's request loop calls
//! [`Host::serve_static`] / [`Host::sse`] per request and decides what a
//! changed path *means* by passing its own path→JSON mapper to [`Host::watch`].
//! The `respond`/`respond_json`/`read_json` helpers are shared plumbing for
//! whatever routes the app adds on top.

use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tiny_http::{Header, Request, Response};

/// SSE subscribers — each gets every broadcast frame; dead ones are pruned on
/// send.
type Clients = Arc<Mutex<Vec<Sender<Vec<u8>>>>>;

/// Shared, cheaply-clonable hosting state: the static root plus the SSE
/// subscriber registry. Construction spawns the keepalive ticker.
#[derive(Clone)]
pub struct Host {
    dist: PathBuf,
    clients: Clients,
}

impl Host {
    /// A host serving static files from `dist`. Spawns the SSE keepalive.
    pub fn new(dist: impl Into<PathBuf>) -> Self {
        let host = Host { dist: dist.into(), clients: Arc::new(Mutex::new(Vec::new())) };
        spawn_keepalive(host.clients.clone());
        host
    }

    /// Watch `dir` recursively; each changed path goes through `message` (None
    /// = ignore) and the resulting JSON is broadcast to all SSE subscribers as
    /// a `data:` frame — de-duplicated within one filesystem event. Failure to
    /// start the watcher is logged, never fatal: the app still serves, clients
    /// just get no change events.
    pub fn watch(
        &self,
        dir: impl Into<PathBuf>,
        message: impl Fn(&Path) -> Option<Value> + Send + 'static,
    ) {
        use notify::{RecursiveMode, Watcher};
        let dir = dir.into();
        let clients = self.clients.clone();
        thread::spawn(move || {
            let clients2 = clients.clone();
            let mut watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                if let Ok(ev) = res {
                    let mut seen = std::collections::HashSet::new();
                    for p in ev.paths {
                        if let Some(msg) = message(&p) {
                            let key = msg.to_string();
                            if seen.insert(key) {
                                let frame = format!("data: {msg}\n\n").into_bytes();
                                broadcast(&clients2, frame);
                            }
                        }
                    }
                }
            }) {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("[host] watcher init failed: {e}");
                    return;
                }
            };
            if let Err(e) = watcher.watch(&dir, RecursiveMode::Recursive) {
                eprintln!("[host] watch failed: {e}");
                return;
            }
            // Keep the watcher alive for the process lifetime.
            loop {
                thread::sleep(Duration::from_secs(3600));
            }
        });
    }

    /// Serve `path` from the static root, with an `index.html` fallback for
    /// client-side routing (any miss — including a traversal attempt — gets
    /// the SPA shell).
    pub fn serve_static(&self, request: Request, path: &str) -> io::Result<()> {
        let rel = path.trim_start_matches('/');
        let candidate = if rel.is_empty() { self.dist.join("index.html") } else { self.dist.join(rel) };
        // Resolve `..`/symlinks and confirm the result is STILL inside dist — a lexical
        // `starts_with` is fooled by `..` (`dist/../../etc/passwd` lexically starts with
        // `dist`), so canonicalize both and compare. A miss falls back to the SPA shell.
        let safe = std::fs::canonicalize(&candidate).ok().filter(|c| {
            c.is_file() && std::fs::canonicalize(&self.dist).map(|d| c.starts_with(d)).unwrap_or(false)
        });
        let target = match safe {
            Some(c) => c,
            None => self.dist.join("index.html"), // SPA fallback
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

    /// Subscribe this request to the change-event stream: responds with an
    /// endless `text/event-stream` fed by [`Host::watch`] broadcasts. Blocks
    /// the calling thread for the connection's lifetime.
    pub fn sse(&self, request: Request) -> io::Result<()> {
        let (tx, rx) = channel::<Vec<u8>>();
        // Send an initial comment so the stream opens immediately.
        let _ = tx.send(b":ok\n\n".to_vec());
        self.clients.lock().unwrap().push(tx);
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
}

fn broadcast(clients: &Clients, frame: Vec<u8>) {
    let mut guard = clients.lock().unwrap();
    guard.retain(|tx| tx.send(frame.clone()).is_ok());
}

/// Periodic SSE comment so dead connections are detected/pruned and proxies
/// don't time the stream out.
fn spawn_keepalive(clients: Clients) {
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

/// tiny_http writes every response through a fixed 1 KiB `BufWriter` and only
/// flushes it after the body is fully copied — which never happens on an
/// endless stream, so a bare frame (and the response headers before it!) would
/// sit in that buffer indefinitely and the client would see nothing. Pad every
/// frame past the buffer size with an SSE comment (clients ignore `:` lines);
/// a single write ≥ the buffer capacity bypasses it, pushing the frame — and
/// anything buffered before it — out immediately.
fn pad_to_flush(mut frame: Vec<u8>) -> Vec<u8> {
    const FLUSH: usize = 1200; // > tiny_http's 1024-byte BufWriter
    if frame.len() < FLUSH {
        frame.push(b':');
        frame.resize(FLUSH - 2, b' ');
        frame.extend_from_slice(b"\n\n");
    }
    frame
}

impl Read for SseReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.buf.len() {
            match self.rx.recv() {
                Ok(frame) => {
                    self.buf = pad_to_flush(frame);
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
// HTTP helpers — shared plumbing for whatever routes an app adds on top
// ---------------------------------------------------------------------------

pub fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("valid header")
}

/// Cap a request body at 1 MiB so a client can't stream unbounded data into RAM.
const MAX_BODY: u64 = 1 << 20;

/// Read the request body as JSON (`Null` on absent/invalid), capped at 1 MiB.
pub fn read_json(request: &mut Request) -> Value {
    let mut body = String::new();
    let _ = request.as_reader().take(MAX_BODY).read_to_string(&mut body);
    serde_json::from_str(&body).unwrap_or(Value::Null)
}

pub fn respond_json<T: Serialize>(request: Request, value: &T) -> io::Result<()> {
    let body = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    request.respond(Response::from_string(body).with_header(header("Content-Type", "application/json")))
}

pub fn respond(request: Request, status: u16, msg: &str) -> io::Result<()> {
    request.respond(Response::from_string(msg).with_status_code(status))
}
