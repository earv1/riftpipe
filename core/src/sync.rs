//! Platform-agnostic per-file tree sync: the wire messages + merge state, shared
//! by the browser (`riftpipe-web`) and native (`riftpipe`), so both ends speak ONE
//! protocol over whatever transport (`TreeSync` on iroh or WebRTC).
//!
//! Text files (`*.md`) are eg-walker CRDTs synced as
//! **events**: peers exchange version vectors on connect, then ship only the ops
//! the other lacks (`ops_since` / `encode_delta`) — not the whole history every
//! edit. First-connect and reconnect are the same operation ("send me everything
//! since version X"; a fresh peer is X = empty). Structural (non-text) files are
//! last-writer-wins; the same connect handshake carries their `(path, version)`
//! inventory, so a peer that lacks or is stale on one receives it immediately —
//! no re-touch needed. No I/O, no clock — callers persist the result and pass a
//! millisecond `now` for LWW versions.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::text::EgWalkerText;

/// A compact per-document state vector: `(agent, seq)` tips (usually one or two).
type Vv = Vec<(String, usize)>;
/// A document's origin (root op id) — the reliable "same document?" identity.
type Origin = Option<(String, usize)>;

#[derive(Serialize, Deserialize)]
pub enum SyncMsg {
    /// Connect handshake: "here are the version vectors of the text docs I have,
    /// and the LWW versions of my structural files — reply with anything I'm
    /// missing or stale on." Sent by both peers on connect (empty lists are valid
    /// and still solicit the other's files).
    Hello { versions: Vec<(String, Vv)>, lww_versions: Vec<(String, u64)> },
    /// A text delta: the ops the recipient lacks for `path`, the sender's version
    /// vector (so the recipient learns what the sender has), and the doc's origin
    /// (so a same-path but independently-created doc is detected, not interleaved).
    TextDelta { path: String, ops: Vec<u8>, vv: Vv, origin: Origin },
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
    /// Per structural path: the LWW `(version, bytes)` — bytes cached so a new mesh
    /// neighbor can be caught up with them (`full_state`).
    lww: HashMap<String, (u64, Vec<u8>)>,
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

    /// Messages to send on connect: advertise our text version vectors AND our
    /// LWW versions so the peer replies with what we're missing. Always send
    /// (even empty) so a fresh peer solicits the other's files.
    pub fn hello(&self) -> SyncMsg {
        let versions = self
            .docs
            .iter()
            .map(|(path, doc)| (path.clone(), doc.version_vector()))
            .collect();
        let lww_versions = self.lww.iter().map(|(path, (v, _))| (path.clone(), *v)).collect();
        SyncMsg::Hello { versions, lww_versions }
    }

    /// The cached LWW entries a peer with inventory `their` (path → version)
    /// needs: every entry it lacks or holds an older version of. LWW-safe by
    /// construction — the receiver's `apply(Lww)` still drops anything that
    /// doesn't beat its local version, so a newer local file is never regressed.
    fn lww_updates_for(&self, their: &HashMap<String, u64>) -> Vec<SyncMsg> {
        self.lww
            .iter()
            .filter(|(path, (version, _))| their.get(*path).is_none_or(|tv| *version > *tv))
            .map(|(path, (version, bytes))| SyncMsg::Lww {
                path: path.clone(),
                version: *version,
                bytes: bytes.clone(),
            })
            .collect()
    }

    /// Record a local text edit (snapshot diff-to-ops); returns a **delta** of the
    /// new ops to send, or `None` if the peer already has everything.
    pub fn local_text(&mut self, path: &str, content: &str) -> Option<SyncMsg> {
        let peer = self.peer_vv.get(path).cloned().unwrap_or_default();
        let doc = self.doc(path);
        doc.edit_to(content);
        let ops = doc.ops_since(&peer)?;
        let vv = doc.version_vector();
        let origin = doc.origin();
        self.advance_peer_vv(path, &vv); // optimistic: assume they'll get our ops
        Some(SyncMsg::TextDelta { path: path.to_string(), ops, vv, origin })
    }

    /// Full self-contained state of every text doc — broadcast to a newly-joined
    /// neighbor (gossip has no per-peer delta) so it catches up + merges.
    pub fn full_state(&self) -> Vec<SyncMsg> {
        let mut msgs: Vec<SyncMsg> = self
            .docs
            .iter()
            .filter_map(|(path, doc)| {
                doc.ops_since(&[]).map(|ops| SyncMsg::TextDelta {
                    path: path.clone(),
                    ops,
                    vv: doc.version_vector(),
                    origin: doc.origin(),
                })
            })
            .collect();
        msgs.extend(self.lww_updates_for(&HashMap::new()));
        msgs
    }

    /// Record a local structural write; `now` is a millisecond clock.
    pub fn local_lww(&mut self, path: &str, bytes: Vec<u8>, now: u64) -> SyncMsg {
        let entry = self.lww.entry(path.to_string()).or_insert((0, Vec::new()));
        entry.0 = now.max(entry.0 + 1);
        entry.1 = bytes.clone();
        SyncMsg::Lww { path: path.to_string(), version: entry.0, bytes }
    }

    /// Apply a received message: returns `(bytes to persist, messages to send
    /// back)`. A `Hello` produces reply deltas; a `TextDelta`/`Lww` produces bytes.
    pub fn apply(&mut self, msg: SyncMsg) -> (Option<(String, Vec<u8>)>, Vec<SyncMsg>) {
        match msg {
            SyncMsg::Hello { versions, lww_versions } => {
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
                        let doc = self.docs.get(&path).unwrap();
                        let vv = doc.version_vector();
                        let origin = doc.origin();
                        self.advance_peer_vv(&path, &vv);
                        replies.push(SyncMsg::TextDelta { path, ops, vv, origin });
                    }
                }
                // LWW: ship any structural file the peer lacks or is stale on.
                // One round, no storms — a Hello never provokes another Hello,
                // and applying an Lww produces no replies.
                let their_lww: HashMap<String, u64> = lww_versions.into_iter().collect();
                replies.extend(self.lww_updates_for(&their_lww));
                (None, replies)
            }
            SyncMsg::TextDelta { path, ops, vv, origin } => {
                // Independently-created conflict on a file we already hold: same
                // path, DIFFERENT origin (root op). Don't naively merge — that
                // interleaves two unrelated texts. Resolve deterministically by
                // origin agent id (higher wins; both converge to it). Distinct paths
                // never hit this, so separate documents union; a shared origin (normal
                // concurrent editing) falls through to a clean CRDT merge.
                if let (Some(mine), Some(theirs)) =
                    (self.docs.get(&path).and_then(|d| d.origin()), origin.as_ref())
                {
                    if mine != *theirs {
                        if mine.0.as_str() < theirs.0.as_str() {
                            // We lose — adopt their version (their ops are a
                            // self-contained full on a fresh conflict).
                            let mut fresh = EgWalkerText::new(&self.agent);
                            if fresh.merge(&ops) {
                                let content = fresh.content().into_bytes();
                                self.docs.insert(path.clone(), fresh);
                                self.advance_peer_vv(&path, &vv);
                                return (Some((path, content)), Vec::new());
                            }
                            return (None, vec![SyncMsg::Resync { path }]);
                        }
                        // We win — keep ours, ignore their ops (they adopt ours).
                        self.advance_peer_vv(&path, &vv);
                        return (None, Vec::new());
                    }
                }
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
                    let origin = doc.origin();
                    self.advance_peer_vv(&path, &vv);
                    (None, vec![SyncMsg::TextDelta { path, ops, vv, origin }])
                }
                None => (None, Vec::new()),
            },
            SyncMsg::Lww { path, version, bytes } => {
                let entry = self.lww.entry(path.clone()).or_insert((0, Vec::new()));
                if version > entry.0 {
                    entry.0 = version;
                    entry.1 = bytes.clone();
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
        let m1 = a.local_text("note.md", &big).unwrap();
        let first_len = match &m1 {
            SyncMsg::TextDelta { ops, .. } => ops.len(),
            _ => panic!("expected delta"),
        };
        assert_eq!(b.apply(m1).0.unwrap().1, big.as_bytes());

        // A second small edit AFTER the peer is caught up must ship a tiny delta,
        // not the whole 4000-char document again.
        let m2 = a.local_text("note.md", &(big.clone() + "Y")).unwrap();
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
        let m = a.local_text("index.md", "# B\n\n- Todo\n").unwrap();
        b.apply(m);

        // Concurrent edits from the same base.
        let ma = a.local_text("index.md", "# B\n\n- Todo\n- A\n").unwrap();
        let mb = b.local_text("index.md", "# B\n\n- Todo\n- Bee\n").unwrap();
        let on_b = b.apply(ma).0.unwrap().1;
        let on_a = a.apply(mb).0.unwrap().1;
        assert_eq!(on_a, on_b, "concurrent edits converge");
    }

    #[test]
    fn hello_transfers_existing_tree_to_a_fresh_peer() {
        // A already has a tree; B connects fresh and must receive it via the
        // version-vector handshake (no edit needed).
        let mut a = Syncer::new("a");
        a.local_text("note.md", "# existing note\n");

        let mut b = Syncer::new("b");
        // B says hello (empty) → A replies with the deltas B lacks.
        let replies = deliver(&mut a, b.hello());
        assert_eq!(replies.len(), 1, "A offers its one doc");
        let (persisted, _) = b.apply(replies.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"# existing note\n");
    }

    #[test]
    fn reconnect_replays_only_the_gap() {
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        b.apply(a.local_text("note.md", "line 1\n").unwrap()); // synced

        // "Disconnect": A makes several edits B never sees.
        a.local_text("note.md", "line 1\nline 2\n");
        a.local_text("note.md", "line 1\nline 2\nline 3\n");
        let full = a.docs.get("note.md").unwrap().encode_full().len();

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
            b.docs.get("note.md").unwrap().content(),
            "line 1\nline 2\nline 3\n",
            "B caught up to A after reconnect",
        );
    }

    #[test]
    fn lww_newest_wins() {
        let mut b = Syncer::new("b");
        let mut a = Syncer::new("a");
        let newer = a.local_lww("state.bin", b"new".to_vec(), 1000);
        assert_eq!(b.apply(newer).0.unwrap().1, b"new");
        let stale = SyncMsg::Lww { path: "state.bin".into(), version: 1, bytes: b"old".to_vec() };
        assert!(b.apply(stale).0.is_none(), "stale LWW ignored");
    }

    #[test]
    fn hello_transfers_existing_lww_to_a_fresh_peer() {
        // A already holds a structural file; B joins fresh over the point-to-point
        // handshake and must receive it through hello/apply alone — no re-touch.
        let mut a = Syncer::new("a");
        a.local_lww("tickets/tk_1/meta.toml", b"column = \"Doing\"\n".to_vec(), 1000);

        let mut b = Syncer::new("b");
        let replies = deliver(&mut a, b.hello());
        assert_eq!(replies.len(), 1, "A offers its one LWW file");
        let (persisted, more) = b.apply(replies.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"column = \"Doing\"\n");
        assert!(more.is_empty(), "applying an Lww never produces replies (no storm)");
    }

    #[test]
    fn hello_updates_stale_lww_peer() {
        // B holds an old version of meta.toml; A's is newer. B's hello advertises
        // its version, so A ships only the fresher payload and B adopts it.
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        b.apply(a.local_lww("meta.toml", b"column = \"Todo\"\n".to_vec(), 1000));
        a.local_lww("meta.toml", b"column = \"Done\"\n".to_vec(), 2000); // B never sees this

        let replies = deliver(&mut a, b.hello());
        assert_eq!(replies.len(), 1, "A ships the newer LWW payload");
        let (persisted, _) = b.apply(replies.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"column = \"Done\"\n", "stale peer caught up");
    }

    #[test]
    fn hello_never_clobbers_newer_local_lww() {
        // B's local meta.toml is NEWER than A's. Neither direction of the
        // handshake may regress it — and A must end up adopting B's.
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        b.apply(a.local_lww("meta.toml", b"old\n".to_vec(), 1000)); // shared base
        b.local_lww("meta.toml", b"newer on B\n".to_vec(), 2000);

        // B hello → A: A's copy is not newer than B's advertised version, so A
        // sends nothing for it.
        let from_a = deliver(&mut a, b.hello());
        assert!(from_a.is_empty(), "A must not ship an LWW the peer already beats");

        // A hello → B: B ships its newer copy back; A adopts it. Even if an older
        // payload did arrive, apply(Lww) drops it — assert that too.
        let from_b = deliver(&mut b, a.hello());
        assert_eq!(from_b.len(), 1, "B offers its newer copy");
        let (persisted, _) = a.apply(from_b.into_iter().next().unwrap());
        assert_eq!(persisted.unwrap().1, b"newer on B\n", "A converges to B's newer file");
        let stale = SyncMsg::Lww { path: "meta.toml".into(), version: 1500, bytes: b"stale\n".to_vec() };
        assert!(b.apply(stale).0.is_none(), "an older payload never regresses B");
        assert_eq!(b.lww["meta.toml"].1, b"newer on B\n");
    }

    #[test]
    fn full_state_catches_up_text_and_lww() {
        // A fresh mesh neighbor is caught up with both the text docs AND the LWW
        // structural (LWW) files via full_state.
        let mut a = Syncer::new("a");
        a.local_text("notes/x/doc.md", "# hi\n");
        a.local_lww("notes/x/state.bin", b"state=doing\n".to_vec(), 1000);

        let mut b = Syncer::new("b");
        for m in a.full_state() {
            b.apply(m);
        }
        assert_eq!(b.docs["notes/x/doc.md"].content(), "# hi\n");
        assert_eq!(
            b.lww.get("notes/x/state.bin").map(|(_, by)| by.as_slice()),
            Some(&b"state=doing\n"[..]),
            "neighbor caught up the LWW meta too",
        );
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
            path: "note.md".into(),
            ops: probe.encode_delta(&v1), // parent (edit 1) is missing
            vv: probe.version_vector(),
            origin: probe.origin(),
        };

        let mut src = Syncer::new("s");
        src.local_text("note.md", "one\n");
        src.local_text("note.md", "one\ntwo\n");

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
            path: "note.md".into(),
            ops: probe.encode_delta(&v1),
            vv: probe.version_vector(),
            origin: probe.origin(),
        });
        assert!(again.is_empty(), "repeated gapped deltas are suppressed");

        // The source answers with full state → the peer recovers.
        let (_, full) = src.apply(replies.into_iter().next().unwrap());
        let (persist, _) = dst.apply(full.into_iter().next().unwrap());
        assert_eq!(persist.unwrap().1, b"one\ntwo\n", "peer recovered via full resync");
    }

    #[test]
    fn independent_trees_merge_union_docs_and_resolve_same_path() {
        // Two trees created independently (disjoint histories) — as two friends
        // who each already had a folder would.
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        a.local_text("index.md", "# A's index\n");
        a.local_text("notes/aaa/doc.md", "doc A\n");
        b.local_text("index.md", "# B's index\n");
        b.local_text("notes/bbb/doc.md", "doc B\n");

        // Handshake: each answers the other's hello with the docs it lacks.
        let (_, a_gives) = a.apply(b.hello());
        let (_, b_gives) = b.apply(a.hello());
        for m in b_gives {
            let _ = a.apply(m);
        }
        for m in a_gives {
            let _ = b.apply(m);
        }

        // Distinct docs union — both sides hold both.
        for s in [&a, &b] {
            assert!(s.docs.contains_key("notes/aaa/doc.md"), "has doc A");
            assert!(s.docs.contains_key("notes/bbb/doc.md"), "has doc B");
        }
        // The same-path index.md converges to the higher agent id ("b") on both —
        // deterministic, no interleaving of the two texts.
        assert_eq!(a.docs["index.md"].content(), "# B's index\n");
        assert_eq!(b.docs["index.md"].content(), "# B's index\n");
    }

    /// Full two-way state exchange between two peers (the handshake).
    fn sync(x: &mut Syncer, y: &mut Syncer) {
        let (xh, yh) = (x.hello(), y.hello());
        let (_, from_y) = y.apply(xh);
        let (_, from_x) = x.apply(yh);
        for m in from_y {
            let _ = x.apply(m);
        }
        for m in from_x {
            let _ = y.apply(m);
        }
    }

    #[test]
    fn three_independent_trees_converge() {
        // Three peers each independently create index.md. A same-path conflict
        // resolves by a *total order* on origin agent id, so all converge to the
        // single highest ("c") — even as an adopted version has to propagate onward
        // (A adopts B, then must still learn C). Union of distinct docs too.
        let mut a = Syncer::new("a");
        let mut b = Syncer::new("b");
        let mut c = Syncer::new("c");
        a.local_text("index.md", "# A\n");
        a.local_text("notes/a/doc.md", "a\n");
        b.local_text("index.md", "# B\n");
        c.local_text("index.md", "# C\n");
        c.local_text("notes/c/doc.md", "c\n");

        // Two rounds over all pairs so an adoption propagates to everyone.
        for _ in 0..2 {
            sync(&mut a, &mut b);
            sync(&mut b, &mut c);
            sync(&mut a, &mut c);
        }

        for s in [&a, &b, &c] {
            assert_eq!(s.docs["index.md"].content(), "# C\n", "all converge to highest origin");
            assert!(s.docs.contains_key("notes/a/doc.md"), "doc a everywhere");
            assert!(s.docs.contains_key("notes/c/doc.md"), "doc c everywhere");
        }
    }
}
