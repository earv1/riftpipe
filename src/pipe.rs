//! Pipe mode — the editor-stream protocol (DESIGN.md §15). This is the Unix
//! boundary: autoshare becomes a CRDT-sync daemon that reads local edits on
//! stdin and writes remote edits on stdout, both as line-delimited JSON. Any
//! frontend (a neovim Lua bridge, a script, a test) drives it the same way; the
//! core never knows what an editor is.
//!
//! Protocol (char-offset coordinates into the whole document):
//!   {"op":"snapshot","text":"..."}   full state (init / resync)
//!   {"op":"insert","pos":N,"text":"..."}
//!   {"op":"delete","pos":N,"len":M}

use std::time::Duration;

use serde::{Deserialize, Serialize};
use similar::{capture_diff_slices, Algorithm, DiffOp};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::{interval, sleep_until, Instant, MissedTickBehavior};

use crate::net::{anyerr, Result};
use crate::text::EgWalkerText;
use crate::transport::{IrohSink, IrohSource};

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

/// Event-driven stdio driver (`autoshare ... --pipe`). A single `select!` loop
/// reacts to whichever happens: a local edit op on stdin (→ push a delta) or a
/// remote delta on the link (→ emit edit ops to stdout). When idle, both arms
/// park and **no traffic flows** — no lockstep, no polling.
pub async fn run_pipe(mut sink: IrohSink, mut source: IrohSource) -> Result<()> {
    let mut peer = PipePeer::new("pipe");
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut out = tokio::io::stdout();

    // Reconciliation triggers (DESIGN.md §16): a periodic heartbeat, plus a
    // debounced "after edits settle" timer. Both send our version vector; the
    // peer replies with exactly the ops we're missing (empty if in sync).
    let mut heartbeat = interval(Duration::from_secs(5));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let idle = || Instant::now() + Duration::from_secs(3600); // "disabled" deadline
    let mut settle_at = idle();

    // Reconcile on connect (recovers state after a reconnect).
    sink.send(frame(TAG_SYNC, &peer.version_json())).await?;

    loop {
        tokio::select! {
            line = stdin.next_line() => {
                match line.map_err(anyerr)? {
                    Some(line) => {
                        let line = line.trim();
                        if line.is_empty() { continue; }
                        if let Ok(op) = serde_json::from_str::<EditOp>(line) {
                            let before = peer.version();
                            peer.apply_local(&op);
                            if peer.version() != before {
                                sink.send(frame(TAG_DELTA, &peer.encode_since_sent())).await?;
                                settle_at = Instant::now() + Duration::from_millis(500);
                            }
                        }
                    }
                    None => break, // stdin closed → frontend gone
                }
            }
            msg = source.recv() => {
                match msg? {
                    Some(bytes) if !bytes.is_empty() => {
                        let (tag, payload) = (bytes[0], &bytes[1..]);
                        match tag {
                            TAG_DELTA => {
                                for op in peer.merge_remote(payload) {
                                    let mut line = serde_json::to_string(&op).map_err(anyerr)?;
                                    line.push('\n');
                                    out.write_all(line.as_bytes()).await.map_err(anyerr)?;
                                }
                                out.flush().await.map_err(anyerr)?;
                                settle_at = Instant::now() + Duration::from_millis(500);
                            }
                            TAG_SYNC => {
                                if let Ok(theirs) =
                                    serde_json::from_slice::<Vec<(String, usize)>>(payload)
                                {
                                    if let Some(missing) = peer.ops_since(&theirs) {
                                        sink.send(frame(TAG_DELTA, &missing)).await?;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(_) => {} // empty frame
                    None => break, // peer closed
                }
            }
            _ = sleep_until(settle_at) => {
                sink.send(frame(TAG_SYNC, &peer.version_json())).await?;
                settle_at = idle();
            }
            _ = heartbeat.tick() => {
                sink.send(frame(TAG_SYNC, &peer.version_json())).await?;
            }
        }
    }
    sink.finish().await;
    Ok(())
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
