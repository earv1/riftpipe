//! Deterministic lossy-mesh simulation: five `Syncer`s on a fixed gossip graph,
//! every delivery dropped independently with probability `p_drop`, driven by the
//! anti-entropy scheduler exactly as the mesh drivers drive it (`poll` every
//! `AE_TICK_MS`, neighbors-only `Hello`, `compare_hello` → `on_hello`, every reply
//! swarm-wide). Proves that periodic `Hello`s repair whatever the lossy broadcasts
//! dropped, and that a converged idle mesh goes silent — and converges also when, as in
//! iroh-gossip's plumtree, a neighbors-only message never crosses a LAZY edge
//! (pruned cycle edges, or one side's stale view after a restart), and when every
//! send goes through a bandwidth-limited `OutQueue` that coalesces and sheds
//! (shed → `on_loss`, as the drivers do). Deletes ride the same machinery: a
//! tombstone counts as converged state, and a lost `Delete` is repaired by
//! anti-entropy. Public API only.

use std::collections::BTreeMap;

use riftpipe_core::antientropy::{AntiEntropy, AE_TICK_MS};
use riftpipe_core::outqueue::{coalesce_key, OutQueue, Scope};
use riftpipe_core::sync::{DurableKind, HelloCmp, MeshAssembler, MeshMsg, SyncMsg, Syncer};
use riftpipe_core::EgWalkerText;

const PEERS: usize = 5;
/// Each peer's direct gossip neighbors (symmetric).
const GRAPH: [&[usize]; PEERS] = [&[1, 4, 2], &[0, 2], &[1, 3, 0], &[2, 4], &[3, 0]];

fn id(i: usize) -> [u8; 32] {
    [i as u8; 32]
}

/// xorshift64* — the same generator as `AntiEntropy`'s jitter.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// True with probability `p`.
    fn chance(&mut self, p: f64) -> bool {
        ((self.next_u64() >> 11) as f64 / (1u64 << 53) as f64) < p
    }
}

/// The lossy network: messages sent this tick are delivered next tick, each
/// (sender, receiver) copy dropped independently. `Syncer` iterates `HashMap`s
/// (per-process random order), so for a run to depend only on its seed, every
/// batch is put in a canonical order before it consumes the drop rng: replies
/// are sent sorted by bytes, `Hello`s are canonicalized, and in-flight messages
/// are delivered sorted.
struct Net {
    p_drop: f64,
    /// Directed `(from, to)` graph edges a neighbors-only message never crosses:
    /// `to` is lazy in `from`'s plumtree view (iroh-gossip sends Scope::Neighbors
    /// to eager peers only). Swarm messages still reach everyone.
    lazy: &'static [(usize, usize)],
    rng: Rng,
    /// `(to, from, encoded SyncMsg)`.
    inflight: Vec<(usize, usize, Vec<u8>)>,
    /// Messages sent (one per `send`, whatever the fan-out).
    sent: u64,
}

/// The path a per-path message is about.
fn msg_path(m: &SyncMsg) -> Option<&str> {
    match m {
        SyncMsg::TextDelta { path, .. }
        | SyncMsg::Resync { path }
        | SyncMsg::Lww { path, .. }
        | SyncMsg::Delete { path, .. }
        | SyncMsg::Restore { path, .. } => Some(path),
        _ => None,
    }
}

/// A batch in canonical order: sorted by encoded bytes, then each `Restore`
/// moved to just before the first same-path message (the byte sort would put
/// it, variant index 9, after the full state it precedes). A batch without a
/// `Restore` keeps the plain byte order.
fn canonical_batch(msgs: &[SyncMsg]) -> Vec<(Vec<u8>, &SyncMsg)> {
    let mut sorted: Vec<(Vec<u8>, &SyncMsg)> =
        msgs.iter().map(|m| (postcard::to_allocvec(m).unwrap(), m)).collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let restores: Vec<usize> = (0..sorted.len())
        .filter(|&i| matches!(sorted[i].1, SyncMsg::Restore { .. }))
        .collect();
    for i in restores.into_iter().rev() {
        let path = msg_path(sorted[i].1);
        let item = sorted.remove(i);
        let at = sorted.iter().position(|(_, m)| msg_path(m) == path).unwrap_or(sorted.len());
        sorted.insert(at, item);
    }
    sorted
}

impl Net {
    /// Send a batch from one peer, in canonical (byte) order.
    fn send_all(&mut self, from: usize, scope: Scope, msgs: &[SyncMsg]) {
        for (bytes, _) in canonical_batch(msgs) {
            self.send(from, scope, bytes);
        }
    }

    fn send(&mut self, from: usize, scope: Scope, bytes: Vec<u8>) {
        self.sent += 1;
        let targets: Vec<usize> = match scope {
            Scope::Swarm => (0..PEERS).filter(|&p| p != from).collect(),
            Scope::Neighbors => GRAPH[from]
                .iter()
                .copied()
                .filter(|&to| !self.lazy.contains(&(from, to)))
                .collect(),
        };
        for to in targets {
            if !self.rng.chance(self.p_drop) {
                self.inflight.push((to, from, bytes.clone()));
            }
        }
    }
}

/// Each peer's outbound queue, drained at a fixed byte rate — the drivers' one
/// `OutQueue` + drain task, over a link of limited bandwidth.
struct Queues {
    qs: Vec<OutQueue>,
    bytes_per_tick: usize,
    seen_shed: Vec<u64>,
}

struct Peer {
    syncer: Syncer,
    ae: AntiEntropy,
    /// Virtual ms of every anti-entropy `Hello` this peer sent.
    hellos: Vec<u64>,
    /// `state(&syncer)` memoized by `syncer.generation()` (it only moves on change).
    state_cache: std::cell::RefCell<Option<(u64, BTreeMap<String, [u8; 32]>)>>,
}

impl Peer {
    fn state(&self) -> BTreeMap<String, [u8; 32]> {
        let generation = self.syncer.generation();
        let mut cache = self.state_cache.borrow_mut();
        match &*cache {
            Some((g, st)) if *g == generation => st.clone(),
            _ => {
                let st = state(&self.syncer);
                *cache = Some((generation, st.clone()));
                st
            }
        }
    }
}

struct Sim {
    peers: Vec<Peer>,
    net: Net,
    /// Drive the anti-entropy scheduler (false = the control: no periodic Hellos).
    anti_entropy: bool,
    tick: u64,
    /// Peer 0's growing LWW event log (`events/p0.jsonl`).
    p0_log: String,
    /// A peer that makes no local edits (see `PLUMTREE_LAZY_ASYMMETRIC`).
    read_only: Option<usize>,
    /// `Some`: sends queue and drain at a byte rate; `None`: sent immediately.
    queues: Option<Queues>,
}

/// `s.hello()` with its (HashMap-ordered) inventories sorted — same meaning,
/// stable bytes.
fn canonical_hello(s: &Syncer) -> SyncMsg {
    let SyncMsg::Hello {
        mut versions,
        mut lww_versions,
    } = s.hello()
    else {
        unreachable!()
    };
    versions.sort();
    lww_versions.sort();
    SyncMsg::Hello {
        versions,
        lww_versions,
    }
}

/// The materialized text of `path` at `s`, rebuilt from its durable record.
fn text_of(s: &Syncer, path: &str) -> String {
    let Some(rec) = s.durable(path) else {
        return String::new();
    };
    let (DurableKind::Text { ops } | DurableKind::TextReborn { ops, .. }) = rec.kind else {
        panic!("{path} is not a live text doc")
    };
    let mut doc = EgWalkerText::new("viewer");
    assert!(doc.merge(&ops));
    doc.content()
}

/// Every path `s` advertises or holds a tombstone for, with the blake3 of the
/// bytes it materializes (`[0; 32]` for a tombstone).
fn state(s: &Syncer) -> BTreeMap<String, [u8; 32]> {
    let SyncMsg::Hello {
        versions,
        lww_versions,
    } = s.hello()
    else {
        unreachable!()
    };
    versions
        .into_iter()
        .map(|(p, _)| p)
        .chain(lww_versions.into_iter().map(|(p, _)| p))
        .chain(s.tombstoned_paths())
        .map(|p| {
            let disk = s.durable(&p).unwrap().disk;
            (p, disk)
        })
        .collect()
}

impl Sim {
    fn new(seed: u64, p_drop: f64, anti_entropy: bool) -> Self {
        Self::with_lazy(seed, p_drop, anti_entropy, &[])
    }

    fn with_lazy(seed: u64, p_drop: f64, anti_entropy: bool, lazy: &'static [(usize, usize)]) -> Self {
        let peers = (0..PEERS)
            .map(|i| Peer {
                syncer: Syncer::new(format!("p{i}")),
                ae: AntiEntropy::new(0, seed * 31 + i as u64),
                hellos: Vec::new(),
                state_cache: Default::default(),
            })
            .collect();
        Sim {
            peers,
            net: Net {
                p_drop,
                lazy,
                rng: Rng::new(seed),
                inflight: Vec::new(),
                sent: 0,
            },
            anti_entropy,
            tick: 0,
            p0_log: String::new(),
            read_only: None,
            queues: None,
        }
    }

    fn now(&self) -> u64 {
        self.tick * AE_TICK_MS
    }

    /// Peer 0 creates `board.md`, delivered losslessly to everyone (shared origin).
    fn seed_board(&mut self) {
        let msg = self.peers[0]
            .syncer
            .local_text("board.md", "# board\n")
            .unwrap();
        let bytes = postcard::to_allocvec(&msg).unwrap();
        for p in &mut self.peers[1..] {
            let (persist, replies) = p.syncer.apply(postcard::from_bytes(&bytes).unwrap());
            assert!(persist.is_some() && replies.is_empty());
        }
    }

    /// Workload W(n), tick `t < n`: peer 0 appends to a growing LWW event log every
    /// tick; peers 1..=4 take turns appending to the shared board and their own card.
    fn workload(&mut self, t: u64) -> Vec<(usize, SyncMsg)> {
        let mut out = Vec::new();
        self.p0_log
            .push_str(&format!("line {t} {}\n", "x".repeat(200)));
        let bytes = self.p0_log.clone().into_bytes();
        out.push((
            0,
            self.peers[0]
                .syncer
                .local_lww("events/p0.jsonl", bytes, t * 250 + 1),
        ));
        for p in 1..PEERS {
            if self.read_only == Some(p) {
                continue;
            }
            // Plan deviation: `t % 4 == p` never fires for p = 4, so peer p takes
            // the slot `t % 4 == p - 1`.
            if t % 4 != p as u64 - 1 {
                continue;
            }
            let s = &mut self.peers[p].syncer;
            let board = format!("{}p{p}-{t}\n", text_of(s, "board.md"));
            out.extend(s.local_text("board.md", &board).map(|m| (p, m)));
            let card_path = format!("card-{p}.md");
            let card = format!("{}p{p}-{t}\n", text_of(s, &card_path));
            out.extend(s.local_text(&card_path, &card).map(|m| (p, m)));
        }
        out
    }

    /// Queue every send (keyed like the drivers key them) with `max_bytes` per
    /// queue, draining `bytes_per_tick` per peer per tick.
    fn with_queues(mut self, max_bytes: usize, bytes_per_tick: usize) -> Self {
        self.queues = Some(Queues {
            qs: (0..PEERS).map(|_| OutQueue::with_max_bytes(max_bytes)).collect(),
            bytes_per_tick,
            seen_shed: vec![0; PEERS],
        });
        self
    }

    /// Send (or queue) a batch from one peer, in canonical (byte) order.
    fn send(&mut self, from: usize, scope: Scope, msgs: &[SyncMsg]) {
        let Some(qs) = &mut self.queues else {
            self.net.send_all(from, scope, msgs);
            return;
        };
        for (bytes, m) in canonical_batch(msgs) {
            let wrapped = MeshMsg::Sync(postcard::from_bytes(&bytes).unwrap());
            qs.qs[from].push(scope, coalesce_key(m), &wrapped);
        }
        let shed = qs.qs[from].shed();
        if shed != qs.seen_shed[from] {
            qs.seen_shed[from] = shed;
            let now = self.tick * AE_TICK_MS;
            self.peers[from].ae.on_loss(now);
        }
    }

    /// Each queue sends up to its byte budget: whole messages, frames together.
    fn drain(&mut self) {
        let Some(qs) = &mut self.queues else { return };
        for from in 0..PEERS {
            let mut budget = qs.bytes_per_tick as i64;
            while budget > 0 {
                let Some((scope, frames)) = qs.qs[from].pop() else { break };
                budget -= frames.iter().map(|f| f.len() as i64).sum::<i64>();
                let mut asm = MeshAssembler::new();
                let whole = frames.iter().find_map(|f| asm.feed(f));
                let Some(MeshMsg::Sync(m)) = whole else { panic!("a queued message reassembles") };
                self.net.send(from, scope, postcard::to_allocvec(&m).unwrap());
            }
        }
    }

    fn shed_total(&self) -> u64 {
        self.queues.as_ref().map_or(0, |q| q.qs.iter().map(|q| q.shed()).sum())
    }

    /// One tick of virtual time: deliver last tick's messages, run local ops, poll AE.
    fn step(&mut self, local: impl FnOnce(&mut Self) -> Vec<(usize, SyncMsg)>) {
        let now = self.now();
        let mut inflight = std::mem::take(&mut self.net.inflight);
        inflight.sort();
        for (to, from, bytes) in inflight {
            let msg: SyncMsg = postcard::from_bytes(&bytes).unwrap();
            let peer = &mut self.peers[to];
            if let Some(c) = peer.syncer.compare_hello(&msg) {
                peer.ae.on_hello(now, id(from), c.consistent());
            }
            let a = peer.syncer.receive(msg);
            self.send(to, Scope::Swarm, &a.replies);
        }
        for (from, msg) in local(self) {
            self.send(from, Scope::Swarm, &[msg]);
        }
        if self.anti_entropy {
            for i in 0..PEERS {
                let nbrs: Vec<[u8; 32]> = GRAPH[i].iter().map(|&n| id(n)).collect();
                let peer = &mut self.peers[i];
                if peer.ae.poll(now, peer.syncer.generation(), &nbrs) {
                    peer.hellos.push(now);
                    let hello = canonical_hello(&peer.syncer);
                    self.send(i, Scope::Neighbors, &[hello]);
                }
            }
        }
        self.drain();
        self.tick += 1;
    }

    fn run_workload(&mut self, n: u64) {
        self.seed_board();
        for t in 0..n {
            self.step(|sim| sim.workload(t));
        }
    }

    fn converged(&self) -> bool {
        let first = self.peers[0].state();
        !first.is_empty() && self.peers[1..].iter().all(|p| p.state() == first)
    }

    /// Idle ticks until converged; `Some(ticks taken)` or `None` after `k` ticks.
    fn settle(&mut self, k: u64) -> Option<u64> {
        for extra in 0..=k {
            if self.converged() {
                return Some(extra);
            }
            if extra < k {
                self.step(|_| Vec::new());
            }
        }
        None
    }
}

/// The digest's direction flags on a converged mesh are all clear.
fn assert_pairwise_consistent(sim: &Sim) {
    for (i, p) in sim.peers.iter().enumerate() {
        for &n in GRAPH[i] {
            let cmp = p.syncer.compare_hello(&sim.peers[n].syncer.hello());
            assert_eq!(
                cmp,
                Some(HelloCmp::default()),
                "peer {i} vs {n} not consistent"
            );
        }
    }
}

#[test]
fn lossy_burst_converges_within_k_rounds() {
    let mut max_ticks = 0;
    for seed in 1..=20 {
        let mut sim = Sim::new(seed, 0.30, true);
        sim.run_workload(120);
        let ticks = sim
            .settle(240)
            .unwrap_or_else(|| panic!("seed {seed}: not converged after 240 ticks"));
        assert_pairwise_consistent(&sim);
        max_ticks = max_ticks.max(ticks);
    }
    println!(
        "30% loss, W(120): converged within {max_ticks} ticks ({} ms) after the burst",
        max_ticks * AE_TICK_MS
    );
}

/// Through bandwidth-limited queues small enough to shed (each shed forces an
/// anti-entropy round), the burst still converges — and the queues really shed.
#[test]
fn shedding_queues_still_converge() {
    let (mut max_ticks, mut shed) = (0, 0);
    for seed in 1..=10 {
        let mut sim = Sim::new(seed, 0.30, true).with_queues(16 * 1024, 4 * 1024);
        sim.run_workload(120);
        let ticks = sim
            .settle(480)
            .unwrap_or_else(|| panic!("seed {seed}: not converged after 480 ticks"));
        assert_pairwise_consistent(&sim);
        max_ticks = max_ticks.max(ticks);
        shed += sim.shed_total();
    }
    println!(
        "queued (16 KiB, 4 KiB/tick), 30% loss, W(120): converged within {max_ticks} ticks ({} ms); {shed} messages shed",
        max_ticks * AE_TICK_MS
    );
    assert!(shed > 0, "the queue budget never forced a shed — the test proves nothing about shedding");
}

/// Hellos while every peer keeps editing: the 30 s burst stays at the Trickle
/// cadence — a max-defer round every 10 s, backing off 2 s → 8 s within it, so
/// ~4 Hellos per 10 s — with no Hello ping-pong between editing neighbors on
/// top (11 per peer at most; 14 when mismatches reset during activity).
#[test]
fn hellos_during_sustained_activity_are_bounded() {
    let mut worst = 0;
    for seed in 1..=10 {
        let mut sim = Sim::new(seed, 0.30, true);
        sim.run_workload(120);
        for p in &sim.peers {
            worst = worst.max(p.hellos.len());
        }
    }
    println!("during a 30 s burst with 30% loss: at most {worst} Hellos per peer");
    assert!(worst <= 12, "{worst} Hellos from one peer in 30 s of activity");
}

#[test]
fn half_loss_converges() {
    let mut max_ticks = 0;
    for seed in 1..=10 {
        let mut sim = Sim::new(seed, 0.5, true);
        sim.run_workload(40);
        let ticks = sim
            .settle(480)
            .unwrap_or_else(|| panic!("seed {seed}: not converged after 480 ticks"));
        assert_pairwise_consistent(&sim);
        max_ticks = max_ticks.max(ticks);
    }
    println!(
        "50% loss, W(40): converged within {max_ticks} ticks ({} ms) after the burst",
        max_ticks * AE_TICK_MS
    );
}

/// Plumtree's steady state: the cycle edges 0–4 and 0–2 pruned to lazy both
/// ways (the eager edges form the spanning tree 0–1–2–3–4). Hellos only cross
/// eager edges; the swarm-wide replies still reach the requester.
const PLUMTREE_LAZY: &[(usize, usize)] = &[(0, 4), (4, 0), (0, 2), (2, 0)];
/// 0–4 lazy both ways, and a stale one-sided view: 3 holds 4 as lazy while 4
/// still holds 3 as eager (as after 4 restarted under the same key, or a prune
/// racing 4's Hello). 4's Hellos reach only 3, and only a swarm-wide reply gets
/// back to 4 — with neighbors-only replies this case never converges. Nobody's
/// Hello reaches 4 either, so 4's OWN writes would ride only their original
/// broadcast; in iroh that one-sided view heals by `IHave` → `Graft` on 3's
/// next swarm message, which the sim doesn't model — so 4 makes no edits here.
const PLUMTREE_LAZY_ASYMMETRIC: &[(usize, usize)] = &[(0, 4), (4, 0), (3, 4)];

#[test]
fn lazy_edges_still_converge() {
    for (name, lazy, read_only) in [
        ("spanning tree", PLUMTREE_LAZY, None),
        ("one-sided stale", PLUMTREE_LAZY_ASYMMETRIC, Some(4)),
    ] {
        let mut max_ticks = 0;
        for seed in 1..=20 {
            let mut sim = Sim::with_lazy(seed, 0.30, true, lazy);
            sim.read_only = read_only;
            sim.run_workload(120);
            let ticks = sim
                .settle(240)
                .unwrap_or_else(|| panic!("{name}, seed {seed}: not converged after 240 ticks"));
            assert_pairwise_consistent(&sim);
            max_ticks = max_ticks.max(ticks);
        }
        println!(
            "lazy edges ({name}), 30% loss, W(120): converged within {max_ticks} ticks ({} ms)",
            max_ticks * AE_TICK_MS
        );
    }
}

/// A peer never hears a lazy neighbor's Hello, so never confirms it: it keeps
/// probing that neighbor at the backed-off interval, which stays cheap.
#[test]
fn lazy_edges_idle_cost_is_bounded() {
    let mut sim = Sim::with_lazy(1, 0.0, true, PLUMTREE_LAZY);
    sim.run_workload(40);
    sim.settle(480).expect("lossless mesh converges");
    for _ in 0..600_000 / AE_TICK_MS {
        sim.step(|_| Vec::new());
    }
    assert!(sim.converged());
    let window_start = sim.now() - 300_000;
    for (i, p) in sim.peers.iter().enumerate() {
        let late = p.hellos.iter().filter(|&&t| t >= window_start).count();
        println!("lazy edges, peer {i}: {late} Hello(s) in the final 300 s");
        assert!(late <= 5, "peer {i} sent {late} Hellos in the final 300 s of idle");
    }
}

#[test]
fn converged_idle_mesh_is_silent() {
    let mut sim = Sim::new(1, 0.0, true);
    sim.run_workload(40);
    sim.settle(480).expect("lossless mesh converges");
    let idle_ticks = 600_000 / AE_TICK_MS;
    for _ in 0..idle_ticks {
        sim.step(|_| Vec::new());
    }
    assert!(sim.converged());
    let window_start = sim.now() - 300_000;
    for (i, p) in sim.peers.iter().enumerate() {
        let late = p.hellos.iter().filter(|&&t| t >= window_start).count();
        println!(
            "peer {i}: {late} Hello(s) in the final 300 s (stats {:?})",
            p.ae.stats()
        );
        assert_eq!(
            late, 0,
            "peer {i} sent {late} Hellos in the final 300 s of a converged, fully-eager idle mesh"
        );
    }
}

/// The delete step after a settled W(120): two plain deletes (a text card and
/// peer 0's LWW event log) and a delete racing a concurrent edit of the same
/// card (the edit must win), all in one tick.
fn delete_step(sim: &mut Sim) {
    sim.step(|sim| {
        let edited = format!("{}edit by p4\n", text_of(&sim.peers[4].syncer, "card-1.md"));
        vec![
            (2, sim.peers[2].syncer.local_delete("card-2.md").expect("p2 holds card-2.md")),
            (0, sim.peers[0].syncer.local_delete("events/p0.jsonl").expect("p0 holds its log")),
            (1, sim.peers[1].syncer.local_delete("card-1.md").expect("p1 holds card-1.md")),
            (4, sim.peers[4].syncer.local_text("card-1.md", &edited).expect("a real edit")),
        ]
    });
}

fn assert_deletes_landed(sim: &Sim, what: &str) {
    for (i, p) in sim.peers.iter().enumerate() {
        assert!(p.syncer.is_tombstoned("card-2.md"), "{what}: peer {i} still holds card-2.md");
        assert!(p.syncer.is_tombstoned("events/p0.jsonl"), "{what}: peer {i} still holds events/p0.jsonl");
        assert!(!p.syncer.is_tombstoned("card-1.md"), "{what}: peer {i} lost card-1.md to the delete");
        assert!(
            text_of(&p.syncer, "card-1.md").contains("edit by p4"),
            "{what}: peer {i}'s card-1.md lacks the concurrent edit"
        );
    }
    assert_pairwise_consistent(sim);
}

#[test]
fn lossy_deletes_converge_and_go_silent() {
    let mut max_ticks = 0;
    for seed in 1..=10 {
        let mut sim = Sim::new(seed, 0.30, true).with_queues(16 * 1024, 4 * 1024);
        sim.run_workload(120);
        sim.settle(480).unwrap_or_else(|| panic!("seed {seed}: the workload didn't converge"));
        delete_step(&mut sim);
        let ticks = sim
            .settle(480)
            .unwrap_or_else(|| panic!("seed {seed}: deletes not converged after 480 ticks"));
        assert_deletes_landed(&sim, &format!("seed {seed}"));
        max_ticks = max_ticks.max(ticks);
    }
    println!(
        "queued, 30% loss: deletes (+ an edit racing one) converged within {max_ticks} ticks ({} ms)",
        max_ticks * AE_TICK_MS
    );

    // No ping-pong: once converged, tombstones and the restored card go silent.
    let mut sim = Sim::new(1, 0.0, true);
    sim.run_workload(120);
    sim.settle(480).expect("lossless mesh converges");
    delete_step(&mut sim);
    let ticks = sim.settle(480).expect("lossless deletes converge");
    assert_deletes_landed(&sim, "lossless");
    println!("lossless: deletes converged within {ticks} ticks ({} ms)", ticks * AE_TICK_MS);
    let idle_ticks = 600_000 / AE_TICK_MS;
    let half = idle_ticks / 2;
    for _ in 0..half {
        sim.step(|_| Vec::new());
    }
    let (window_start, sent_before) = (sim.now(), sim.net.sent);
    for _ in half..idle_ticks {
        sim.step(|_| Vec::new());
    }
    assert!(sim.converged());
    for (i, p) in sim.peers.iter().enumerate() {
        let late = p.hellos.iter().filter(|&&t| t >= window_start).count();
        assert_eq!(late, 0, "peer {i} sent {late} Hellos in the final 300 s after deletes");
    }
    assert_eq!(sim.net.sent, sent_before, "messages kept flowing in the final 300 s after deletes");
}

/// Delete → re-create → delete again in quick succession under loss, so stale copies and
/// tombstones from different rounds are in flight together: every peer must end deleted
/// (an older, overruled deletion id must never let a `Restore` cancel the latest delete).
#[test]
fn lossy_delete_recreate_delete_ends_deleted() {
    let mut max_ticks = 0;
    for seed in 1..=24 {
        let mut sim = Sim::new(seed, 0.30, true).with_queues(16 * 1024, 4 * 1024);
        sim.run_workload(120);
        sim.settle(480).unwrap_or_else(|| panic!("seed {seed}: the workload didn't converge"));
        sim.step(|sim| vec![(2, sim.peers[2].syncer.local_delete("card-2.md").expect("p2 holds card-2.md"))]);
        sim.step(|_| Vec::new());
        sim.step(|sim| {
            let m = sim.peers[3].syncer.local_text("card-2.md", "# card 2, again\n");
            vec![(3, m.expect("a re-creation (or an edit) of card-2.md"))]
        });
        sim.step(|_| Vec::new());
        sim.step(|sim| vec![(3, sim.peers[3].syncer.local_delete("card-2.md").expect("p3 holds card-2.md"))]);
        let ticks = sim
            .settle(480)
            .unwrap_or_else(|| panic!("seed {seed}: delete/re-create/delete not converged"));
        for (i, p) in sim.peers.iter().enumerate() {
            assert!(p.syncer.is_tombstoned("card-2.md"), "seed {seed}: peer {i} brought card-2.md back");
        }
        assert_pairwise_consistent(&sim);
        max_ticks = max_ticks.max(ticks);
    }
    println!("queued, 30% loss: delete/re-create/delete converged within {max_ticks} ticks");
}

/// Edits right before their delete, under loss: p3 edits card-3 on two consecutive ticks and
/// deletes it on the next, so its deltas can arrive after the (small) `Delete` — every peer
/// must still end deleted (a late delta the deleter had seen never revives the card).
#[test]
fn lossy_edits_overtaken_by_their_delete_end_deleted() {
    let mut max_ticks = 0;
    for seed in 1..=24 {
        let mut sim = Sim::new(seed, 0.30, true).with_queues(16 * 1024, 4 * 1024);
        sim.run_workload(120);
        sim.settle(480).unwrap_or_else(|| panic!("seed {seed}: the workload didn't converge"));
        for n in 0..2 {
            sim.step(|sim| {
                let s = &mut sim.peers[3].syncer;
                let card = format!("{}late edit {n}\n", text_of(s, "card-3.md"));
                vec![(3, s.local_text("card-3.md", &card).expect("a real edit"))]
            });
        }
        sim.step(|sim| vec![(3, sim.peers[3].syncer.local_delete("card-3.md").expect("p3 holds card-3.md"))]);
        let ticks = sim
            .settle(480)
            .unwrap_or_else(|| panic!("seed {seed}: edits + delete not converged"));
        for (i, p) in sim.peers.iter().enumerate() {
            assert!(p.syncer.is_tombstoned("card-3.md"), "seed {seed}: peer {i} revived card-3.md");
        }
        assert_pairwise_consistent(&sim);
        max_ticks = max_ticks.max(ticks);
    }
    println!("queued, 30% loss: edits overtaken by their delete converged within {max_ticks} ticks");
}

#[test]
fn control_without_anti_entropy_tail_loss_persists() {
    let unconverged: Vec<u64> = (1..=20)
        .filter(|&seed| {
            let mut sim = Sim::new(seed, 0.30, false);
            sim.run_workload(120);
            sim.settle(240).is_none()
        })
        .collect();
    println!(
        "without anti-entropy: {} of 20 seeds never converge: {unconverged:?}",
        unconverged.len()
    );
    assert!(
        !unconverged.is_empty(),
        "the simulation can't detect lost tails"
    );
}

#[test]
fn hello_digest_size_budget() {
    let mut s = Syncer::new("g0123abcd");
    for i in 0..125u32 {
        s.local_text(
            &format!("tickets/tk_{i:08x}/card.md"),
            &format!("# ticket {i}\n\nsome body text\n"),
        );
        s.local_lww(
            &format!("tickets/tk_{i:08x}/meta.toml"),
            format!("status = \"open\"\n# {i}\n").into_bytes(),
            1_700_000_000_000 + i as u64,
        );
    }
    let size = postcard::to_allocvec(&s.hello()).unwrap().len();
    println!("Hello digest for 125 cards + 125 LWW files: {size} bytes");
    assert!(size < 16 * 1024, "Hello is {size} bytes");
}
