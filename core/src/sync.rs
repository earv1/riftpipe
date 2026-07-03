//! Platform-agnostic per-file board sync: the wire messages + merge state, shared
//! by the browser (`riftpipe-web`) and native (`riftpipe`), so both ends speak ONE
//! protocol over whatever transport (`BoardSync` on iroh or WebRTC).
//!
//! Text files (`card.md`, comments, `board.md`) are eg-walker CRDTs synced as
//! **events**: peers exchange version vectors on connect, then ship only the ops
//! the other lacks (`ops_since` / `encode_delta`) — not the whole history every
//! edit. First-connect and reconnect are the same operation ("send me everything
//! since version X"; a fresh peer is X = empty). Structural files (`meta.toml`) are
//! last-writer-wins. No I/O, no clock — callers persist the result and pass a
//! millisecond `now` for LWW versions.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::text::EgWalkerText;

/// A compact per-document state vector: `(agent, seq)` tips (usually one or two).
type Vv = Vec<(String, usize)>;

#[derive(Serialize, Deserialize)]
pub enum SyncMsg {
    /// Connect handshake: "here are the version vectors of the text docs I have —
    /// reply with anything I'm missing." Sent by both peers on connect (an empty
    /// list is valid and still solicits the other's docs).
    Hello { versions: Vec<(String, Vv)> },
    /// A text delta: the ops the recipient lacks for `path`, plus the sender's
    /// version vector so the recipient learns what the sender now has.
    TextDelta { path: String, ops: Vec<u8>, vv: Vv },
    /// "I couldn't apply your delta for `path` (missing an ancestor) — send me the
    /// full self-contained state." The recovery path when a delta can't bridge.
    Resync { path: String },
    /// A whole-file last-writer-wins update; newest version wins.
    Lww { path: String, version: u64, bytes: Vec<u8> },
}

/// Per-file sync state for one peer. Author under a **unique** `agent` (a shared
/// agent id would corrupt the CRDT).
pub struct Syncer {
    agent: String,
    docs: HashMap<String, EgWalkerText>,
    /// Our best knowledge of what the peer already has, per text path — so we send
    /// only deltas. Advanced optimistically on send and from the peer's reported vv.
    peer_vv: HashMap<String, Vv>,
    /// Paths we've asked the peer to resync (full) and are still waiting on — so a
    /// stream of un-appliable deltas can't trigger a storm of resync requests.
    awaiting_resync: HashSet<String>,
    lww: HashMap<String, u64>,
}

impl Syncer {
    pub fn new(agent: impl Into<String>) -> Self {
        Syncer {
            agent: agent.into(),
            docs: HashMap::new(),
            peer_vv: HashMap::new(),
            awaiting_resync: HashSet::new(),
            lww: HashMap::new(),
        }
    }

    fn doc(&mut self, path: &str) -> &mut EgWalkerText {
        let agent = &self.agent;
        self.docs
            .entry(path.to_string())
            .or_insert_with(|| EgWalkerText::new(agent))
    }

    /// Merge an incoming vv into our record of what the peer has (per-agent max, so
    /// an out-of-order or stale report never regresses what we know they hold).
    fn advance_peer_vv(&mut self, path: &str, incoming: &Vv) {
        let slot = self.peer_vv.entry(path.to_string()).or_default();
        for (agent, seq) in incoming {
            match slot.iter_mut().find(|(a, _)| a == agent) {
                Some(e) => e.1 = e.1.max(*seq),
                None => slot.push((agent.clone(), *seq)),
            }
        }
    }

    /// Messages to send on connect: advertise our version vectors so the peer
    /// replies with what we're missing. Always send (even empty) so a fresh peer
    /// solicits the other's board.
    pub fn hello(&self) -> SyncMsg {
        let versions = self
            .docs
            .iter()
            .map(|(path, doc)| (path.clone(), doc.version_vector()))
            .collect();
        SyncMsg::Hello { versions }
    }

    /// Record a local text edit (snapshot diff-to-ops); returns a **delta** of the
    /// new ops to send, or `None` if the peer already has everything.
    pub fn local_text(&mut self, path: &str, content: &str) -> Option<SyncMsg> {
        let peer = self.peer_vv.get(path).cloned().unwrap_or_default();
        let doc = self.doc(path);
        doc.edit_to(content);
        let ops = doc.ops_since(&peer)?;
        let vv = doc.version_vector();
        self.advance_peer_vv(path, &vv); // optimistic: assume they'll get our ops
        Some(SyncMsg::TextDelta { path: path.to_string(), ops, vv })
    }

    /// Record a local structural write; `now` is a millisecond clock.
    pub fn local_lww(&mut self, path: &str, bytes: Vec<u8>, now: u64) -> SyncMsg {
        let v = self.lww.entry(path.to_string()).or_insert(0);
        *v = now.max(*v + 1);
        SyncMsg::Lww { path: path.to_string(), version: *v, bytes }
    }

    /// Apply a received message: returns `(bytes to persist, messages to send
    /// back)`. A `Hello` produces reply deltas; a `TextDelta`/`Lww` produces bytes.
    pub fn apply(&mut self, msg: SyncMsg) -> (Option<(String, Vec<u8>)>, Vec<SyncMsg>) {
        match msg {
            SyncMsg::Hello { versions } => {
                let their: HashMap<String, Vv> = versions.into_iter().collect();
                for (path, vv) in &their {
                    self.advance_peer_vv(path, vv);
                }
                // Reply with what the peer is missing from each of our docs.
                let mut replies = Vec::new();
                let paths: Vec<String> = self.docs.keys().cloned().collect();
                for path in paths {
                    let their_vv = their.get(&path).cloned().unwrap_or_default();
                    if let Some(ops) = self.docs.get(&path).unwrap().ops_since(&their_vv) {
                        let vv = self.docs.get(&path).unwrap().version_vector();
                        self.advance_peer_vv(&path, &vv);
                        replies.push(SyncMsg::TextDelta { path, ops, vv });
                    }
                }
                (None, replies)
            }
            SyncMsg::TextDelta { path, ops, vv } => {
                if !self.doc(&path).merge(&ops) {
                    // Missing a causal ancestor. Ask once for a full self-contained
                    // resync; suppress repeats until it arrives so a run of
                    // un-appliable deltas can't cause a resync storm.
                    if self.awaiting_resync.insert(path.clone()) {
                        return (None, vec![SyncMsg::Resync { path }]);
                    }
                    return (None, Vec::new());
                }
                self.awaiting_resync.remove(&path);
                let content = self.doc(&path).content().into_bytes();
                self.advance_peer_vv(&path, &vv);
                (Some((path, content)), Vec::new())
            }
            SyncMsg::Resync { path } => match self.docs.get(&path) {
                // Answer with our full state; the peer merges it to recover no
                // matter what it was missing.
                Some(doc) => {
                    let ops = doc.encode_full();
                    let vv = doc.version_vector();
                    self.advance_peer_vv(&path, &vv);
                    (None, vec![SyncMsg::TextDelta { path, ops, vv }])
                }
                None => (None, Vec::new()),
            },
            SyncMsg::Lww { path, version, bytes } => {
                let v = self.lww.entry(path.clone()).or_insert(0);
                if version > *v {
                    *v = version;
                    (Some((path, bytes)), Vec::new())
                } else {
                    (None, Vec::new())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a message into `dst`, persisting + forwarding replies until quiescent.
    /// Returns the last persisted bytes for `path`, if any.
    fn deliver(dst: &mut Syncer, src_to_dst: SyncMsg) -> Vec<SyncMsg> {
        let (_, replies) = dst.apply(src_to_dst);
        replies
    }

    #[test]
    fn first_edit_transfers_then_edits_are_small_deltas() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");

        // High-entropy content so the "full" encode doesn't just compress away
        // (the encoder compresses content, which would mask the delta size).
        let mut big = String::with_capacity(4000);
        let mut x: u32 = 0x9e3779b9;
        for _ in 0..4000 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            big.push(char::from(33 + (x % 94) as u8));
        }

        // First edit: peer has nothing, so the delta carries the whole doc.
        let m1 = a.local_text("card.md", &big).unwrap();
        let first_len = match &m1 {
            SyncMsg::TextDelta { ops, .. } => ops.len(),
            _ => panic!("expected delta"),
        };
        assert_eq!(b.apply(m1).0.unwrap().1, big.as_bytes());

        // A second small edit AFTER the peer is caught up must ship a tiny delta,
        // not the whole 4000-char document again.
        let m2 = a.local_text("card.md", &(big.clone() + "Y")).unwrap();
        let second_len = match &m2 {
            SyncMsg::TextDelta { ops, .. } => ops.len(),
            _ => panic!("expected delta"),
        };
        assert!(
            second_len < first_len / 10,
            "steady-state edit should be a small delta: {second_len} vs first {first_len}",
        );
        assert_eq!(b.apply(m2).0.unwrap().1, (big + "Y").into_bytes());
    }

    #[test]
    fn concurrent_edits_converge() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        // Shared base.
        let m = a.local_text("board.md", "# B\n\n- Todo\n").unwrap();
        b.apply(m);

        // Concurrent edits from the same base.
        let ma = a.local_text("board.md", "# B\n\n- Todo\n- A\n").unwrap();
        let mb = b.local_text("board.md", "# B\n\n- Todo\n- Bee\n").unwrap();
        let on_b = b.apply(ma).0.unwrap().1;
        let on_a = a.apply(mb).0.unwrap().1;
        assert_eq!(on_a, on_b, "concurrent edits converge");
    }

    #[test]
    fn hello_transfers_existing_board_to_a_fresh_peer() {
        // A already has a board; B connects fresh and must receive it via the
        // version-vector handshake (no edit needed).
        let mut a = Syncer::new("a");
        a.local_text("card.md", "# existing card\n");

        let mut b = Syncer::new("b");
        // B says hello (empty) → A replies with the deltas B lacks.
        let replies = deliver(&mut a, b.hello());
        assert_eq!(replies.len(), 1, "A offers its one doc");
        let (persisted, _) = b.apply(replies.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"# existing card\n");
    }

    #[test]
    fn reconnect_replays_only_the_gap() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        b.apply(a.local_text("card.md", "line 1\n").unwrap()); // synced

        // "Disconnect": A makes several edits B never sees.
        a.local_text("card.md", "line 1\nline 2\n");
        a.local_text("card.md", "line 1\nline 2\nline 3\n");
        let full = a.docs.get("card.md").unwrap().encode_full().len();

        // Reconnect: B advertises its (stale) version vector; A replies with only
        // the ops since then — smaller than the whole history.
        let replies = deliver(&mut a, b.hello());
        let gap = match &replies[0] {
            SyncMsg::TextDelta { ops, .. } => ops.len(),
            _ => panic!(),
        };
        assert!(gap < full, "reconnect ships only the gap ({gap}) not full ({full})");
        b.apply(replies.into_iter().next().unwrap());
        assert_eq!(
            b.docs.get("card.md").unwrap().content(),
            "line 1\nline 2\nline 3\n",
            "B caught up to A after reconnect",
        );
    }

    #[test]
    fn lww_newest_wins() {
        let mut b = Syncer::new("b");
        let mut a = Syncer::new("a");
        let newer = a.local_lww("meta.toml", b"new".to_vec(), 1000);
        assert_eq!(b.apply(newer).0.unwrap().1, b"new");
        let stale = SyncMsg::Lww { path: "meta.toml".into(), version: 1, bytes: b"old".to_vec() };
        assert!(b.apply(stale).0.is_none(), "stale LWW ignored");
    }

    #[test]
    fn gapped_delta_triggers_resync_and_recovers() {
        use crate::text::EgWalkerText;

        // Craft a delta that skips an ancestor (as a lost message would): edit 2's
        // ops without edit 1. `src` (a full syncer) will answer the resync.
        let mut probe = EgWalkerText::new("s");
        probe.edit_to("one\n");
        let v1 = probe.version();
        probe.edit_to("one\ntwo\n");
        let gapped = SyncMsg::TextDelta {
            path: "card.md".into(),
            ops: probe.encode_delta(&v1), // parent (edit 1) is missing
            vv: probe.version_vector(),
        };

        let mut src = Syncer::new("s");
        src.local_text("card.md", "one\n");
        src.local_text("card.md", "one\ntwo\n");

        // A fresh peer can't apply the gapped delta → asks for a resync.
        let mut dst = Syncer::new("d");
        let (persist, replies) = dst.apply(gapped);
        assert!(persist.is_none());
        assert!(
            matches!(replies.as_slice(), [SyncMsg::Resync { .. }]),
            "a gapped delta must trigger a resync request",
        );

        // A second un-appliable delta while awaiting the resync must NOT ask again.
        let (_, again) = dst.apply(SyncMsg::TextDelta {
            path: "card.md".into(),
            ops: probe.encode_delta(&v1),
            vv: probe.version_vector(),
        });
        assert!(again.is_empty(), "repeated gapped deltas are suppressed");

        // The source answers with full state → the peer recovers.
        let (_, full) = src.apply(replies.into_iter().next().unwrap());
        let (persist, _) = dst.apply(full.into_iter().next().unwrap());
        assert_eq!(persist.unwrap().1, b"one\ntwo\n", "peer recovered via full resync");
    }
}
