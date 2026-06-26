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

use crate::net::{anyerr, Link, Result};
use crate::text::EgWalkerText;

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

    fn apply_local(&mut self, op: &EditOp) {
        match op {
            EditOp::Snapshot { text } => self.doc.edit_to(text),
            EditOp::Insert { pos, text } => self.doc.insert_at(*pos, text),
            EditOp::Delete { pos, len } => self.doc.delete_at(*pos, *len),
        }
    }

    pub fn content(&self) -> String {
        self.doc.content()
    }

    /// One round: apply local edit ops, sync with the peer, and return the edit
    /// ops the *remote* peer caused (to be applied by the frontend). Local edits
    /// are diffed in BEFORE merging remote ops, so the returned ops reflect only
    /// the remote change (no echo of the frontend's own edits).
    pub async fn round(
        &mut self,
        link: &mut dyn Link,
        local: &[EditOp],
    ) -> Result<Vec<EditOp>> {
        for op in local {
            self.apply_local(op);
        }
        let before = self.doc.content();

        let delta = self.doc.encode_delta(&self.last_sent);
        link.send(delta).await?;
        if let Some(bytes) = link.recv().await? {
            self.doc.merge(&bytes);
        }
        self.last_sent = self.doc.version();

        let after = self.doc.content();
        Ok(diff_to_ops(&before, &after))
    }
}

/// stdio driver: read local edit ops from stdin, emit remote edit ops to stdout,
/// syncing over `link`. This is what `autoshare ... --pipe` runs.
pub async fn run_pipe(link: &mut dyn Link) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<EditOp>();

    // Read stdin lines into the channel without blocking the sync loop.
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

    let mut peer = PipePeer::new("pipe");
    let mut out = tokio::io::stdout();
    loop {
        let mut local = Vec::new();
        while let Ok(op) = rx.try_recv() {
            local.push(op);
        }
        for op in peer.round(link, &local).await? {
            let mut line = serde_json::to_string(&op).map_err(anyerr)?;
            line.push('\n');
            out.write_all(line.as_bytes()).await.map_err(anyerr)?;
        }
        out.flush().await.map_err(anyerr)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::mock_pair;

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

    #[tokio::test]
    async fn two_pipe_peers_keep_editor_buffers_in_sync() {
        let (mut la, mut lb) = mock_pair();
        let mut a = PipePeer::new("a");
        let mut b = PipePeer::new("b");

        // Each "editor" applies its own local edit, then drives a round.
        let mut buf_a = String::new();
        let mut buf_b = String::new();
        let local_a = vec![EditOp::Insert { pos: 0, text: "hello ".into() }];
        let local_b = vec![EditOp::Insert { pos: 0, text: "world".into() }];
        apply_ops(&mut buf_a, &local_a);
        apply_ops(&mut buf_b, &local_b);

        let (ra, rb) = tokio::join!(a.round(&mut la, &local_a), b.round(&mut lb, &local_b));
        apply_ops(&mut buf_a, &ra.unwrap());
        apply_ops(&mut buf_b, &rb.unwrap());

        // The remote ops emitted to each frontend keep its buffer == the CRDT,
        // and both frontends converge.
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
