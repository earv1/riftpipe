//! The mesh drivers' single outbound queue: whole messages, FIFO, so one message's
//! chunk frames go out back-to-back (no cross-message interleaving at receivers);
//! a queued, not-yet-sent LWW update for a path is replaced by a newer one, and a
//! queued re-assert of the same tombstone is replaced by a later one (it only grows);
//! over a byte budget the oldest sheddable message is dropped (anti-entropy
//! repairs it) — auth messages (Trust/Epoch/Request) and the tiny protocol
//! `Version` announce are never shed.
use crate::sync::{encode_mesh_frames, MeshMsg, SyncMsg};
use std::collections::VecDeque;

pub const OUT_QUEUE_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Swarm,
    Neighbors,
}

/// The coalescing key for a not-yet-wrapped sync message: `Some("lww:<path>")` for `Lww` (a newer
/// version supersedes it for every receiver); `Some("del:<path>:<first id>")` for a `Delete` (a
/// later re-assert of the same tombstone — same first id; `tomb_msg` only ever grows ids, `seen`,
/// `vv` and `version` — supersedes a queued older one); `None` for everything else, including
/// `Restore` (never moved or dropped by coalescing, so it can't be reordered after a `Delete` it
/// overrules) and `TextDelta`.
pub fn coalesce_key(msg: &SyncMsg) -> Option<String> {
    match msg {
        SyncMsg::Lww { path, .. } => Some(format!("lww:{path}")),
        SyncMsg::Delete { path, ids, .. } => ids.first().map(|id| format!("del:{path}:{id:016x}")),
        _ => None,
    }
}

struct Item {
    scope: Scope,
    key: Option<String>,
    sheddable: bool,
    frames: Vec<Vec<u8>>,
    bytes: usize,
}

pub struct OutQueue {
    items: VecDeque<Item>,
    bytes: usize,
    max_bytes: usize,
    shed: u64,
    coalesced: u64,
}

impl Default for OutQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl OutQueue {
    pub fn new() -> Self {
        OutQueue { items: VecDeque::new(), bytes: 0, max_bytes: OUT_QUEUE_MAX_BYTES, shed: 0, coalesced: 0 }
    }

    pub fn with_max_bytes(n: usize) -> Self {
        OutQueue { items: VecDeque::new(), bytes: 0, max_bytes: n, shed: 0, coalesced: 0 }
    }

    pub fn push(&mut self, scope: Scope, key: Option<String>, msg: &MeshMsg) {
        let frames = encode_mesh_frames(msg);
        if frames.is_empty() {
            return;
        }
        // Not `Version`: shedding it while the `Presence` pushed right after it
        // survives makes peers report a pre-versioning peer (`theirs=0`).
        let sheddable = matches!(
            msg,
            MeshMsg::Sync(_) | MeshMsg::SignedSync(_) | MeshMsg::Sealed(_) | MeshMsg::Presence { .. }
        );
        if let Some(k) = &key {
            let mut removed_bytes = 0usize;
            let mut removed_count = 0u64;
            self.items.retain(|item| {
                let matches = item.key.as_deref() == Some(k.as_str())
                    && (scope == Scope::Swarm || item.scope == Scope::Neighbors);
                if matches {
                    removed_bytes += item.bytes;
                    removed_count += 1;
                }
                !matches
            });
            self.bytes -= removed_bytes;
            self.coalesced += removed_count;
        }
        let bytes: usize = frames.iter().map(Vec::len).sum();
        self.bytes += bytes;
        self.items.push_back(Item { scope, key, sheddable, frames, bytes });

        while self.bytes > self.max_bytes {
            let len = self.items.len();
            let victim = self.items.iter().enumerate().take(len.saturating_sub(1)).find(|(_, it)| it.sheddable).map(|(i, _)| i);
            let Some(i) = victim else { break };
            let removed = self.items.remove(i).expect("index from iteration over self.items");
            self.bytes -= removed.bytes;
            self.shed += 1;
        }
    }

    pub fn pop(&mut self) -> Option<(Scope, Vec<Vec<u8>>)> {
        let item = self.items.pop_front()?;
        self.bytes -= item.bytes;
        Some((item.scope, item.frames))
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn shed(&self) -> u64 {
        self.shed
    }

    pub fn coalesced(&self) -> u64 {
        self.coalesced
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::DeviceKey;

    fn lww(path: &str, version: u64) -> MeshMsg {
        MeshMsg::Sync(SyncMsg::Lww { path: path.into(), version, bytes: vec![0; 10] })
    }

    fn unkeyed(path: &str) -> MeshMsg {
        MeshMsg::Sync(SyncMsg::Resync { path: path.into() })
    }

    #[test]
    fn fifo_order() {
        let mut q = OutQueue::new();
        q.push(Scope::Swarm, None, &unkeyed("a"));
        q.push(Scope::Swarm, None, &unkeyed("b"));
        q.push(Scope::Swarm, None, &unkeyed("c"));
        for p in ["a", "b", "c"] {
            let (scope, frames) = q.pop().unwrap();
            assert_eq!(scope, Scope::Swarm);
            assert_eq!(frames, encode_mesh_frames(&unkeyed(p)));
        }
        assert!(q.pop().is_none());
    }

    #[test]
    fn newer_lww_replaces_queued_one() {
        let mut q = OutQueue::new();
        q.push(Scope::Swarm, Some("lww:a".into()), &lww("a", 1));
        q.push(Scope::Swarm, None, &unkeyed("x"));
        q.push(Scope::Swarm, Some("lww:a".into()), &lww("a", 2));

        let (_, f1) = q.pop().unwrap();
        assert_eq!(f1, encode_mesh_frames(&unkeyed("x")));
        let (_, f2) = q.pop().unwrap();
        assert_eq!(f2, encode_mesh_frames(&lww("a", 2)));
        assert!(q.pop().is_none());
        assert_eq!(q.coalesced(), 1);
    }

    #[test]
    fn scope_rules() {
        let mut q = OutQueue::new();
        // Swarm then Neighbors: both stay (a Neighbors-scoped update doesn't
        // reach every Swarm receiver, so it can't supersede a swarm-wide one).
        q.push(Scope::Swarm, Some("lww:a".into()), &lww("a", 1));
        q.push(Scope::Neighbors, Some("lww:a".into()), &lww("a", 2));
        assert_eq!(q.len(), 2);

        let mut q = OutQueue::new();
        // Neighbors then Swarm: the swarm-wide update supersedes the
        // neighbors-only one (it reaches at least as far).
        q.push(Scope::Neighbors, Some("lww:a".into()), &lww("a", 1));
        q.push(Scope::Swarm, Some("lww:a".into()), &lww("a", 2));
        assert_eq!(q.len(), 1);
        let (_, frames) = q.pop().unwrap();
        assert_eq!(frames, encode_mesh_frames(&lww("a", 2)));
    }

    #[test]
    fn chunked_message_pops_whole() {
        let big = MeshMsg::Sync(SyncMsg::Lww { path: "big.bin".into(), version: 1, bytes: vec![7; 20_000] });
        let mut q = OutQueue::new();
        q.push(Scope::Swarm, Some("lww:big.bin".into()), &big);
        let (_, frames) = q.pop().unwrap();
        assert!(frames.len() > 1, "a 20 KB Lww must chunk");
        assert!(q.pop().is_none());
    }

    #[test]
    fn sheds_oldest_content_never_auth() {
        let mut q = OutQueue::with_max_bytes(10_000);
        q.push(Scope::Swarm, None, &MeshMsg::Trust(DeviceKey::from_seed(&[1; 32]).genesis()));
        for i in 0..4 {
            q.push(
                Scope::Swarm,
                None,
                &MeshMsg::Sync(SyncMsg::Lww { path: format!("f{i}"), version: 1, bytes: vec![0; 3_000] }),
            );
        }
        assert!(q.shed() >= 1);
        let (first_scope, first_frames) = q.pop().unwrap();
        assert_eq!(first_scope, Scope::Swarm);
        assert_eq!(
            first_frames,
            encode_mesh_frames(&MeshMsg::Trust(DeviceKey::from_seed(&[1; 32]).genesis())),
            "the Trust message is still first"
        );
        // The last-pushed item is still present.
        let remaining: Vec<_> = std::iter::from_fn(|| q.pop()).collect();
        assert!(
            remaining
                .iter()
                .any(|(_, frames)| *frames
                    == encode_mesh_frames(&MeshMsg::Sync(SyncMsg::Lww {
                        path: "f3".into(),
                        version: 1,
                        bytes: vec![0; 3_000]
                    }))),
            "the last-pushed item must survive shedding"
        );
    }

    fn delete(path: &str, ids: &[u64]) -> SyncMsg {
        SyncMsg::Delete { path: path.into(), ids: ids.to_vec(), seen: vec![], origin: None, vv: vec![], version: 1 }
    }

    /// Push `msg` keyed by [`coalesce_key`], as the drivers do.
    fn push_keyed(q: &mut OutQueue, msg: SyncMsg) {
        let key = coalesce_key(&msg);
        q.push(Scope::Swarm, key, &MeshMsg::Sync(msg));
    }

    #[test]
    fn newer_delete_replaces_queued_one_but_never_crosses_kinds() {
        let text = SyncMsg::TextDelta { path: "a".into(), ops: vec![1, 2, 3], vv: vec![], origin: None };
        let mut q = OutQueue::new();
        push_keyed(&mut q, SyncMsg::Lww { path: "a".into(), version: 1, bytes: vec![0; 10] });
        push_keyed(&mut q, delete("a", &[7]));
        push_keyed(&mut q, SyncMsg::TextDelta { path: "a".into(), ops: vec![1, 2, 3], vv: vec![], origin: None });
        push_keyed(&mut q, delete("a", &[7, 9]));

        let popped: Vec<_> = std::iter::from_fn(|| q.pop().map(|(_, f)| f)).collect();
        assert_eq!(
            popped,
            vec![
                encode_mesh_frames(&lww("a", 1)),
                encode_mesh_frames(&MeshMsg::Sync(text)),
                encode_mesh_frames(&MeshMsg::Sync(delete("a", &[7, 9]))),
            ],
            "the re-assert replaced the older Delete; the Lww and TextDelta stay, in order"
        );
        assert_eq!(q.coalesced(), 1);
    }

    #[test]
    fn restore_is_never_coalesced_or_reordered() {
        let mut q = OutQueue::new();
        push_keyed(&mut q, delete("p", &[1]));
        push_keyed(&mut q, SyncMsg::Restore { path: "p".into(), ids: vec![1] });
        push_keyed(&mut q, delete("p", &[2]));

        let popped: Vec<_> = std::iter::from_fn(|| q.pop().map(|(_, f)| f)).collect();
        assert_eq!(
            popped,
            vec![
                encode_mesh_frames(&MeshMsg::Sync(delete("p", &[1]))),
                encode_mesh_frames(&MeshMsg::Sync(SyncMsg::Restore { path: "p".into(), ids: vec![1] })),
                encode_mesh_frames(&MeshMsg::Sync(delete("p", &[2]))),
            ],
        );
        assert_eq!(q.coalesced(), 0, "a fresh deletion (new first id) never coalesces with the old one");
    }

    #[test]
    fn deletes_are_sheddable() {
        let mut q = OutQueue::with_max_bytes(3_000);
        let big_delete = |i: u64| SyncMsg::Delete {
            path: format!("f{i}"),
            ids: vec![i],
            seen: vec![[0; 32]; 16],
            origin: None,
            vv: (0..40).map(|n| (format!("agent-{n:04}"), n)).collect(),
            version: 1,
        };
        for i in 0..6 {
            push_keyed(&mut q, big_delete(i));
        }
        assert!(q.shed() >= 1, "over budget, a queued Delete is shed like any content");
        assert!(q.bytes() <= 3_000);
    }

    #[test]
    fn version_is_never_shed() {
        let mut q = OutQueue::with_max_bytes(5_000);
        let version = MeshMsg::Version { id: [7; 32], proto: 5 };
        q.push(Scope::Swarm, None, &version);
        q.push(Scope::Swarm, None, &MeshMsg::Presence { id: [7; 32], neighbors: vec![] });
        for i in 0..4 {
            q.push(
                Scope::Swarm,
                None,
                &MeshMsg::Sync(SyncMsg::Lww { path: format!("f{i}"), version: 1, bytes: vec![0; 3_000] }),
            );
        }
        assert!(q.shed() >= 1, "over budget, something went");
        assert_eq!(q.pop().unwrap().1, encode_mesh_frames(&version), "the Version announce survives, first");
    }

    #[test]
    fn only_auth_over_budget_is_kept() {
        let mut q = OutQueue::with_max_bytes(10);
        q.push(Scope::Swarm, None, &MeshMsg::Trust(DeviceKey::from_seed(&[1; 32]).genesis()));
        q.push(Scope::Swarm, None, &MeshMsg::Trust(DeviceKey::from_seed(&[2; 32]).genesis()));
        assert!(q.pop().is_some());
        assert!(q.pop().is_some());
        assert!(q.pop().is_none());
        assert_eq!(q.shed(), 0);
    }
}
