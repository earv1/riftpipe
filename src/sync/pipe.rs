//! Pipe mode — the editor-stream protocol (DESIGN.md §15). This is the Unix
//! boundary: riftpipe becomes a CRDT-sync daemon that reads local edits on
//! stdin and writes remote edits on stdout, both as line-delimited JSON. Any
//! frontend (a neovim Lua bridge, a script, a test) drives it the same way; the
//! core never knows what an editor is.
//!
//! Protocol (char-offset coordinates into the whole document):
//!   {"op":"snapshot","text":"..."}   full state (init / resync)
//!   {"op":"insert","pos":N,"text":"..."}
//!   {"op":"delete","pos":N,"len":M}

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iroh::{Endpoint, EndpointAddr};
use serde::{Deserialize, Serialize};
use similar::{capture_diff_slices, Algorithm, DiffOp};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval, sleep_until, Instant, MissedTickBehavior};

use crate::crdt::text::EgWalkerText;
use crate::net::negotiate::{exchange_caps, Caps, Transport};
use crate::net::secure::authenticate;
use crate::net::transport::{accept_link, connect_link, IrohLink, IrohSink, IrohSource};
use crate::net::webrtc::{upgrade_to_webrtc, WebrtcSink, WebrtcSource};
use crate::net::{anyerr, Counters, Result};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum EditOp {
    Snapshot { text: String },
    Insert { pos: usize, text: String },
    Delete { pos: usize, len: usize },
}

/// Translate the difference between two document states into edit ops that, when
/// applied left-to-right to `before`, reproduce `after`. Used to tell a frontend
/// what the *remote* peer changed.
pub fn diff_to_ops(before: &str, after: &str) -> Vec<EditOp> {
    if before == after {
        return vec![];
    }
    let old: Vec<char> = before.chars().collect();
    let new: Vec<char> = after.chars().collect();
    let mut ops = Vec::new();
    let mut pos = 0usize;
    for d in capture_diff_slices(Algorithm::Myers, &old, &new) {
        match d {
            DiffOp::Equal { len, .. } => pos += len,
            DiffOp::Delete { old_len, .. } => ops.push(EditOp::Delete { pos, len: old_len }),
            DiffOp::Insert { new_index, new_len, .. } => {
                ops.push(EditOp::Insert {
                    pos,
                    text: new[new_index..new_index + new_len].iter().collect(),
                });
                pos += new_len;
            }
            DiffOp::Replace {
                old_len,
                new_index,
                new_len,
                ..
            } => {
                ops.push(EditOp::Delete { pos, len: old_len });
                ops.push(EditOp::Insert {
                    pos,
                    text: new[new_index..new_index + new_len].iter().collect(),
                });
                pos += new_len;
            }
        }
    }
    ops
}

pub struct PipePeer {
    doc: EgWalkerText,
    last_sent: Vec<usize>,
}

impl PipePeer {
    pub fn new(name: &str) -> Self {
        let doc = EgWalkerText::new(name);
        let last_sent = doc.version();
        PipePeer { doc, last_sent }
    }

    pub fn apply_local(&mut self, op: &EditOp) {
        match op {
            EditOp::Snapshot { text } => self.doc.edit_to(text),
            EditOp::Insert { pos, text } => self.doc.insert_at(*pos, text),
            EditOp::Delete { pos, len } => self.doc.delete_at(*pos, *len),
        }
    }

    pub fn content(&self) -> String {
        self.doc.content()
    }

    pub fn version(&self) -> Vec<usize> {
        self.doc.version()
    }

    /// Encode the ops added since we last sent, and advance the sent watermark.
    /// (Setting the watermark to the full version also means merged remote ops
    /// are never echoed back.)
    pub fn encode_since_sent(&mut self) -> Vec<u8> {
        let delta = self.doc.encode_delta(&self.last_sent);
        self.last_sent = self.doc.version();
        delta
    }

    /// Merge a remote delta and return the edit ops the frontend should apply
    /// (diffed BEFORE/AFTER, so they reflect only the remote change).
    pub fn merge_remote(&mut self, bytes: &[u8]) -> Vec<EditOp> {
        let before = self.doc.content();
        self.doc.merge(bytes);
        self.last_sent = self.doc.version();
        let after = self.doc.content();
        diff_to_ops(&before, &after)
    }

    /// Our version vector as `(agent, seq)` pairs (for reconciliation/tests).
    pub fn version_vector_pairs(&self) -> Vec<(String, usize)> {
        self.doc.version_vector()
    }

    /// Our version vector (for reconciliation), JSON-encoded for the wire.
    pub fn version_json(&self) -> Vec<u8> {
        serde_json::to_vec(&self.doc.version_vector()).unwrap_or_default()
    }

    /// Ops the peer is missing, given their version vector (None if caught up).
    pub fn ops_since(&self, theirs: &[(String, usize)]) -> Option<Vec<u8>> {
        self.doc.ops_since(theirs)
    }
}

// Wire message tags (prefix byte before the payload). The link otherwise carries
// opaque bytes; this multiplexes ops vs. reconciliation.
const TAG_DELTA: u8 = 0; // CRDT ops (push or sync response) -> merge
const TAG_SYNC: u8 = 1; // sender's version vector -> reply with the missing ops

fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(payload.len() + 1);
    v.push(tag);
    v.extend_from_slice(payload);
    v
}

// The send/recv halves a session runs over. Abstracted so the session loop is
// testable with mocks and reusable across reconnects.
#[async_trait]
pub trait PipeSink: Send {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()>;
    async fn finish(&mut self);
}
#[async_trait]
pub trait PipeSource: Send {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>>;
}

#[async_trait]
impl PipeSink for IrohSink {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        IrohSink::send(self, msg).await
    }
    async fn finish(&mut self) {
        IrohSink::finish(self).await
    }
}
#[async_trait]
impl PipeSource for IrohSource {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        IrohSource::recv(self).await
    }
}

#[async_trait]
impl PipeSink for WebrtcSink {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        WebrtcSink::send(self, msg).await
    }
    async fn finish(&mut self) {
        WebrtcSink::finish(self).await
    }
}
#[async_trait]
impl PipeSource for WebrtcSource {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        WebrtcSource::recv(self).await
    }
}

/// Negotiate the data transport over the (authenticated) iroh `link`, optionally
/// upgrade to WebRTC, and return the session's send/recv halves
/// (`docs/planned/transport-negotiation.md`). Shared by `--pipe` and folder mode.
///
/// On a WebRTC upgrade the iroh link is returned as the `keepalive` (control /
/// fallback — and it keeps the QUIC connection alive so metrics' `connection_kind`
/// still resolves); on iroh transport it's consumed into the halves. An upgrade
/// failure transparently falls back to iroh. The returned `Transport` is what the
/// data plane actually ended up using.
pub async fn negotiate_session_halves(
    mut link: IrohLink,
    counters: Arc<Counters>,
) -> (Box<dyn PipeSink>, Box<dyn PipeSource>, Option<IrohLink>, Transport) {
    let outcome = exchange_caps(&mut link, &Caps::native()).await;
    if let Ok(o) = &outcome {
        if o.transport == Transport::WebrtcDirect {
            match upgrade_to_webrtc(&mut link, o.we_offer).await {
                Ok(w) => {
                    let (sink, source) = w.into_halves(counters);
                    return (Box::new(sink), Box::new(source), Some(link), Transport::WebrtcDirect);
                }
                Err(e) => eprintln!("[riftpipe] webrtc upgrade failed ({e}); staying on iroh"),
            }
        }
    }
    // Either iroh was chosen, caps failed, or the WebRTC upgrade fell back. We're
    // on the iroh link now; report iroh-direct rather than the unrealized webrtc.
    let transport = match outcome {
        Ok(o) if o.transport != Transport::WebrtcDirect => o.transport,
        _ => Transport::IrohDirect,
    };
    let (sink, source) = link.into_halves(counters);
    (Box::new(sink), Box::new(source), None, transport)
}

/// Why a session ended.
#[derive(Debug, PartialEq, Eq)]
pub enum SessionOutcome {
    /// The link dropped — reconnect and resume (the doc state persists).
    LinkClosed,
    /// stdin closed (the frontend is gone) — quit.
    StdinClosed,
}

/// Spawn a task reading edit ops from stdin into a channel. Runs once for the
/// process lifetime, independent of sessions — so edits typed *during* a
/// reconnect gap queue up and are applied when the next session resumes.
pub fn stdin_ops() -> UnboundedReceiver<EditOp> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(op) = serde_json::from_str::<EditOp>(line) {
                if tx.send(op).is_err() {
                    break;
                }
            }
        }
    });
    rx
}

/// One sync session over a single link. `peer`/`rx`/`out` are owned by the
/// reconnect loop and **persist across sessions**, so a dropped+re-established
/// link resumes from the same document. Returns when the link drops
/// (`LinkClosed`) or stdin ends (`StdinClosed`). A link error is reported as
/// `LinkClosed`, never a hard error — so the loop reconnects.
pub async fn session(
    peer: &mut PipePeer,
    rx: &mut UnboundedReceiver<EditOp>,
    out: &mut (impl AsyncWrite + Unpin),
    sink: &mut dyn PipeSink,
    source: &mut dyn PipeSource,
) -> Result<SessionOutcome> {
    let mut heartbeat = interval(Duration::from_secs(5));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let idle = || Instant::now() + Duration::from_secs(3600);
    let mut settle_at = idle();

    // Reconcile on connect — recovers anything missed since the last session.
    if sink.send(frame(TAG_SYNC, &peer.version_json())).await.is_err() {
        return Ok(SessionOutcome::LinkClosed);
    }

    loop {
        tokio::select! {
            op = rx.recv() => {
                match op {
                    None => return Ok(SessionOutcome::StdinClosed), // frontend gone
                    Some(op) => {
                        let before = peer.version();
                        peer.apply_local(&op);
                        if peer.version() != before {
                            if sink.send(frame(TAG_DELTA, &peer.encode_since_sent())).await.is_err() {
                                return Ok(SessionOutcome::LinkClosed);
                            }
                            settle_at = Instant::now() + Duration::from_millis(500);
                        }
                    }
                }
            }
            msg = source.recv() => {
                match msg {
                    Err(_) | Ok(None) => return Ok(SessionOutcome::LinkClosed),
                    Ok(Some(bytes)) if !bytes.is_empty() => {
                        let (tag, payload) = (bytes[0], &bytes[1..]);
                        match tag {
                            TAG_DELTA => {
                                for op in peer.merge_remote(payload) {
                                    let mut line = serde_json::to_string(&op).map_err(anyerr)?;
                                    line.push('\n');
                                    if out.write_all(line.as_bytes()).await.is_err() {
                                        return Ok(SessionOutcome::StdinClosed);
                                    }
                                }
                                let _ = out.flush().await;
                                settle_at = Instant::now() + Duration::from_millis(500);
                            }
                            TAG_SYNC => {
                                if let Ok(theirs) =
                                    serde_json::from_slice::<Vec<(String, usize)>>(payload)
                                {
                                    if let Some(missing) = peer.ops_since(&theirs) {
                                        if sink.send(frame(TAG_DELTA, &missing)).await.is_err() {
                                            return Ok(SessionOutcome::LinkClosed);
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(Some(_)) => {} // empty frame
                }
            }
            _ = sleep_until(settle_at) => {
                if sink.send(frame(TAG_SYNC, &peer.version_json())).await.is_err() {
                    return Ok(SessionOutcome::LinkClosed);
                }
                settle_at = idle();
            }
            _ = heartbeat.tick() => {
                if sink.send(frame(TAG_SYNC, &peer.version_json())).await.is_err() {
                    return Ok(SessionOutcome::LinkClosed);
                }
            }
        }
    }
}

/// Whether this peer dials out or waits for connections.
pub enum Role {
    Accept,
    Connect(EndpointAddr),
}

/// Run `--pipe` with **reconnection**: keep one document + stdin/stdout pipe for
/// the process lifetime, and repeatedly (re)establish a link. On a drop, resume
/// the same document — the on-connect SYNC reconciles whatever was missed.
/// Returns only when stdin closes (the frontend quit).
pub async fn run_pipe_reconnecting(
    endpoint: Endpoint,
    role: Role,
    secret: [u8; 32],
    counters: Arc<Counters>,
    metrics: Option<(String, String)>, // (path, title)
) -> Result<()> {
    let mut peer = PipePeer::new("pipe");
    let mut rx = stdin_ops();
    let mut out = tokio::io::stdout();
    let mut metrics_started = false;
    let base = Duration::from_millis(200);
    let mut backoff = base;

    loop {
        let link = match &role {
            Role::Accept => accept_link(&endpoint).await,
            Role::Connect(addr) => connect_link(&endpoint, addr.clone()).await,
        };
        if let Ok(mut link) = link {
            if authenticate(&mut link, &secret).await.is_ok() {
                backoff = base; // reset on a good connection
                if !metrics_started {
                    if let Some((path, title)) = &metrics {
                        crate::monitor::metrics::spawn(
                            endpoint.clone(),
                            link.remote_id(),
                            counters.clone(),
                            path.clone(),
                            title.clone(),
                        );
                        metrics_started = true;
                    }
                }
                // Negotiate the transport + maybe upgrade to WebRTC. `_keep` holds
                // the iroh link alive (control/fallback) for the session's lifetime.
                let (mut sink, mut source, _keep, transport) =
                    negotiate_session_halves(link, counters.clone()).await;
                eprintln!("[riftpipe] connected — syncing ({transport:?})");
                match session(&mut peer, &mut rx, &mut out, &mut *sink, &mut *source).await? {
                    SessionOutcome::StdinClosed => {
                        sink.finish().await;
                        return Ok(());
                    }
                    SessionOutcome::LinkClosed => {
                        eprintln!("[riftpipe] disconnected — reconnecting…");
                    }
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Apply edit ops to a naive "editor buffer" (char-offset based), exactly as
    /// a frontend would.
    fn apply_ops(buf: &mut String, ops: &[EditOp]) {
        let char_to_byte = |s: &str, c: usize| -> usize {
            s.char_indices().nth(c).map(|(i, _)| i).unwrap_or(s.len())
        };
        for op in ops {
            match op {
                EditOp::Snapshot { text } => *buf = text.clone(),
                EditOp::Insert { pos, text } => {
                    let i = char_to_byte(buf, *pos);
                    buf.insert_str(i, text);
                }
                EditOp::Delete { pos, len } => {
                    let start = char_to_byte(buf, *pos);
                    let end = char_to_byte(buf, *pos + *len);
                    buf.replace_range(start..end, "");
                }
            }
        }
    }

    #[test]
    fn event_driven_exchange_keeps_editor_buffers_in_sync() {
        let mut a = PipePeer::new("a");
        let mut b = PipePeer::new("b");
        let mut buf_a = String::new();
        let mut buf_b = String::new();

        // Each "editor" applies its own local edit and pushes the resulting delta
        // (what `run_pipe`'s stdin arm does).
        let la = EditOp::Insert { pos: 0, text: "hello ".into() };
        apply_ops(&mut buf_a, std::slice::from_ref(&la));
        a.apply_local(&la);
        let delta_a = a.encode_since_sent();

        let lb = EditOp::Insert { pos: 0, text: "world".into() };
        apply_ops(&mut buf_b, std::slice::from_ref(&lb));
        b.apply_local(&lb);
        let delta_b = b.encode_since_sent();

        // Deltas arrive (the recv arm): merge and apply the emitted ops.
        apply_ops(&mut buf_b, &b.merge_remote(&delta_a));
        apply_ops(&mut buf_a, &a.merge_remote(&delta_b));

        assert_eq!(buf_a, a.content(), "editor A out of sync with its CRDT");
        assert_eq!(buf_b, b.content(), "editor B out of sync with its CRDT");
        assert_eq!(buf_a, buf_b, "editors diverged: {buf_a:?} vs {buf_b:?}");
        assert!(buf_a.contains("hello") && buf_a.contains("world"));
    }

    #[test]
    fn version_vector_resync_recovers_a_dropped_delta() {
        let mut a = PipePeer::new("a");
        let mut b = PipePeer::new("b");

        // A makes two edits; B receives only the FIRST (the second delta is
        // "lost", e.g. across a reconnect).
        a.apply_local(&EditOp::Insert { pos: 0, text: "one ".into() });
        let d1 = a.encode_since_sent();
        a.apply_local(&EditOp::Insert { pos: 4, text: "two".into() });
        let _dropped = a.encode_since_sent();
        b.merge_remote(&d1);
        assert_ne!(a.content(), b.content(), "B should be behind after the drop");

        // Reconcile: B advertises its version, A sends exactly what B is missing.
        let bv = b.version_vector_pairs();
        let missing = a.ops_since(&bv).expect("A should have ops B lacks");
        b.merge_remote(&missing);

        assert_eq!(a.content(), b.content(), "resync must re-converge");
        assert!(b.content().contains("one") && b.content().contains("two"));
    }

    #[test]
    fn resync_sends_nothing_when_already_in_sync() {
        let mut a = PipePeer::new("a");
        let mut b = PipePeer::new("b");
        a.apply_local(&EditOp::Insert { pos: 0, text: "hello".into() });
        let d = a.encode_since_sent();
        b.merge_remote(&d);
        assert_eq!(a.content(), b.content());
        // B is caught up -> A has nothing to send.
        assert!(a.ops_since(&b.version_vector_pairs()).is_none());
    }

    // --- reconnection (session) tests ---
    use std::collections::VecDeque;

    struct MockSink {
        sent: Vec<Vec<u8>>,
    }
    #[async_trait]
    impl PipeSink for MockSink {
        async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
            self.sent.push(msg);
            Ok(())
        }
        async fn finish(&mut self) {}
    }
    /// Yields queued frames, then `None` forever — simulating a link that
    /// delivers some data and then drops.
    struct DroppingSource {
        queue: VecDeque<Vec<u8>>,
    }
    #[async_trait]
    impl PipeSource for DroppingSource {
        async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
            Ok(self.queue.pop_front())
        }
    }
    /// Never yields — a live but quiet link.
    struct PendingSource;
    #[async_trait]
    impl PipeSource for PendingSource {
        async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn session_resumes_same_doc_across_a_reconnect() {
        // Peer A produces a delta to deliver to B.
        let mut a = PipePeer::new("a");
        a.apply_local(&EditOp::Insert { pos: 0, text: "hello".into() });
        let delta = a.encode_since_sent();

        let mut b = PipePeer::new("b");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<EditOp>();
        let mut out: Vec<u8> = Vec::new();

        // Session 1: deliver the delta, then the link drops.
        let mut sink = MockSink { sent: vec![] };
        let mut src = DroppingSource {
            queue: VecDeque::from(vec![frame(TAG_DELTA, &delta)]),
        };
        let outcome = session(&mut b, &mut rx, &mut out, &mut sink, &mut src)
            .await
            .unwrap();
        assert_eq!(outcome, SessionOutcome::LinkClosed);
        assert_eq!(b.content(), "hello", "delta applied");

        // Session 2 (reconnect) with a fresh, quiet link: state persists, and on
        // a real drop it would reconcile via the on-connect SYNC (recorded here).
        let mut sink2 = MockSink { sent: vec![] };
        let mut src2 = DroppingSource {
            queue: VecDeque::new(),
        };
        let outcome2 = session(&mut b, &mut rx, &mut out, &mut sink2, &mut src2)
            .await
            .unwrap();
        assert_eq!(outcome2, SessionOutcome::LinkClosed);
        assert_eq!(b.content(), "hello", "doc state persists across reconnect");
        assert!(!sink2.sent.is_empty(), "a reconnect sends an on-connect SYNC");
        drop(tx);
    }

    #[tokio::test]
    async fn session_quits_when_stdin_closes() {
        let mut b = PipePeer::new("b");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<EditOp>();
        drop(tx); // frontend gone
        let mut out: Vec<u8> = Vec::new();
        let mut sink = MockSink { sent: vec![] };
        let mut src = PendingSource; // link stays up, so only stdin can end it
        let outcome = session(&mut b, &mut rx, &mut out, &mut sink, &mut src)
            .await
            .unwrap();
        assert_eq!(outcome, SessionOutcome::StdinClosed);
    }

    #[test]
    fn protocol_round_trips_as_json() {
        let op = EditOp::Insert {
            pos: 5,
            text: "hi".into(),
        };
        let line = serde_json::to_string(&op).unwrap();
        assert_eq!(line, r#"{"op":"insert","pos":5,"text":"hi"}"#);
        assert_eq!(serde_json::from_str::<EditOp>(&line).unwrap(), op);
    }
}
