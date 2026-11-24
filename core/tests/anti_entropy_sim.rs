//! Deterministic lossy-mesh simulation: five `Syncer`s on a fixed gossip graph,
//! every delivery dropped independently with probability `p_drop`, driven by the
//! anti-entropy scheduler exactly as the mesh drivers drive it (`poll` every
//! `AE_TICK_MS`, neighbors-only `Hello`, `compare_hello` → `on_hello`, every reply
//! swarm-wide). Proves that periodic `Hello`s repair whatever the lossy broadcasts
//! dropped, and that a converged idle mesh goes nearly silent — also when, as in
//! iroh-gossip's plumtree, a neighbors-only message never crosses a LAZY edge
//! (pruned cycle edges, or one side's stale view after a restart). Public API only.

use std::collections::BTreeMap;

use riftpipe_core::antientropy::{AntiEntropy, AE_TICK_MS};
use riftpipe_core::outqueue::Scope;
use riftpipe_core::sync::{DurableKind, HelloCmp, SyncMsg, Syncer};
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
}

impl Net {
    /// Send a batch from one peer, in canonical (byte) order.
    fn send_all(&mut self, from: usize, scope: Scope, msgs: &[SyncMsg]) {
        let mut encoded: Vec<Vec<u8>> = msgs
            .iter()
            .map(|m| postcard::to_allocvec(m).unwrap())
            .collect();
        encoded.sort();
        for bytes in encoded {
            self.send(from, scope, bytes);
        }
    }

    fn send(&mut self, from: usize, scope: Scope, bytes: Vec<u8>) {
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
    let DurableKind::Text { ops } = rec.kind else {
        panic!("{path} is not a text doc")
    };
    let mut doc = EgWalkerText::new("viewer");
    assert!(doc.merge(&ops));
    doc.content()
}

/// Every path `s` advertises, with the blake3 of the bytes it materializes.
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
            },
            anti_entropy,
            tick: 0,
            p0_log: String::new(),
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
            let (_, replies) = peer.syncer.apply(msg);
            self.net.send_all(to, Scope::Swarm, &replies);
        }
        for (from, msg) in local(self) {
            self.net.send_all(from, Scope::Swarm, &[msg]);
        }
        if self.anti_entropy {
            for i in 0..PEERS {
                let nbrs: Vec<[u8; 32]> = GRAPH[i].iter().map(|&n| id(n)).collect();
                let peer = &mut self.peers[i];
                if peer.ae.poll(now, peer.syncer.generation(), &nbrs) {
                    peer.hellos.push(now);
                    let hello = canonical_hello(&peer.syncer);
                    self.net.send_all(i, Scope::Neighbors, &[hello]);
                }
            }
        }
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
/// back to 4 — with neighbors-only replies this case never converges.
const PLUMTREE_LAZY_ASYMMETRIC: &[(usize, usize)] = &[(0, 4), (4, 0), (3, 4)];

#[test]
fn lazy_edges_still_converge() {
    for (name, lazy) in [("spanning tree", PLUMTREE_LAZY), ("one-sided stale", PLUMTREE_LAZY_ASYMMETRIC)] {
        let mut max_ticks = 0;
        for seed in 1..=20 {
            let mut sim = Sim::with_lazy(seed, 0.30, true, lazy);
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
fn converged_idle_mesh_is_nearly_silent() {
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
        assert!(
            late <= 5,
            "peer {i} sent {late} Hellos in the final 300 s of idle"
        );
    }
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
