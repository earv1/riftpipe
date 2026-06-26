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

use serde::{Deserialize, Serialize};
use similar::{capture_diff_slices, Algorithm, DiffOp};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
}

/// Event-driven stdio driver (`autoshare ... --pipe`). A single `select!` loop
/// reacts to whichever happens: a local edit op on stdin (→ push a delta) or a
/// remote delta on the link (→ emit edit ops to stdout). When idle, both arms
/// park and **no traffic flows** — no lockstep, no polling.
pub async fn run_pipe(mut sink: IrohSink, mut source: IrohSource) -> Result<()> {
    let mut peer = PipePeer::new("pipe");
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut out = tokio::io::stdout();

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
                            // Send only when the op actually changed the doc.
                            if peer.version() != before {
                                sink.send(peer.encode_since_sent()).await?;
                            }
                        }
                    }
                    None => break, // stdin closed → frontend gone
                }
            }
            msg = source.recv() => {
                match msg? {
                    Some(bytes) => {
                        for op in peer.merge_remote(&bytes) {
                            let mut line = serde_json::to_string(&op).map_err(anyerr)?;
                            line.push('\n');
                            out.write_all(line.as_bytes()).await.map_err(anyerr)?;
                        }
                        out.flush().await.map_err(anyerr)?;
                    }
                    None => break, // peer closed
                }
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
