//! Reconnection policy shared by the native and browser mesh drivers.

use serde::{Deserialize, Serialize};

pub const REJOIN_MIN_MS: u64 = 1_000;
pub const REJOIN_MAX_MS: u64 = 16_000;
pub const PEER_BOOK_CAP: usize = 16;

/// When to re-dial after losing every neighbor: first attempt REJOIN_MIN_MS after the
/// loss, then doubling to REJOIN_MAX_MS; reset on any neighbor; `on_online` = retry now.
#[derive(Debug, Clone)]
pub struct Rejoin { backoff: u64, next_at: Option<u64> }
impl Default for Rejoin { fn default() -> Self { Self::new() } }
impl Rejoin {
    pub fn new() -> Self { Rejoin { backoff: REJOIN_MIN_MS, next_at: None } }
    pub fn on_neighbors(&mut self, count: usize, now: u64) {
        if count > 0 { self.next_at = None; self.backoff = REJOIN_MIN_MS; }
        else if self.next_at.is_none() { self.next_at = Some(now + self.backoff); }
    }
    pub fn on_online(&mut self, now: u64) {
        self.backoff = REJOIN_MIN_MS;
        if self.next_at.is_some() { self.next_at = Some(now); }
    }
    /// `true` if an attempt is due now; schedules the next one (doubled backoff).
    pub fn due(&mut self, now: u64) -> bool {
        match self.next_at {
            Some(t) if now >= t => {
                self.backoff = (self.backoff * 2).min(REJOIN_MAX_MS);
                self.next_at = Some(now + self.backoff);
                true
            }
            _ => false,
        }
    }
    pub fn next_at(&self) -> Option<u64> { self.next_at }
}

/// Peers recently seen on ONE board (topic), most-recent first, capped — persisted so a
/// restarted peer can re-dial them (the host has no bootstrap of its own).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct PeerBook { topic: [u8; 32], ids: Vec<[u8; 32]> }
impl PeerBook {
    pub fn new(topic: [u8; 32]) -> Self { PeerBook { topic, ids: Vec::new() } }
    pub fn topic(&self) -> [u8; 32] { self.topic }
    pub fn ids(&self) -> &[[u8; 32]] { &self.ids }
    /// Record `id` as most recent; `false` if nothing changed (it's `me`, or already first).
    pub fn note(&mut self, id: [u8; 32], me: [u8; 32]) -> bool {
        if id == me || self.ids.first() == Some(&id) { return false; }
        self.ids.retain(|x| *x != id);
        self.ids.insert(0, id);
        self.ids.truncate(PEER_BOOK_CAP);
        true
    }
    pub fn encode(&self) -> Vec<u8> { postcard::to_allocvec(self).unwrap_or_default() }
    /// Decode; an empty book for `topic` if corrupt or for another board.
    pub fn decode(topic: [u8; 32], bytes: &[u8]) -> PeerBook {
        match postcard::from_bytes::<PeerBook>(bytes) { Ok(b) if b.topic == topic => b, _ => PeerBook::new(topic) }
    }
}

/// Who to re-dial: the bootstrap first, then the book; deduped, never ourselves.
pub fn rejoin_targets(bootstrap: Option<[u8; 32]>, book: &PeerBook, me: [u8; 32]) -> Vec<[u8; 32]> {
    let mut out = Vec::new();
    for id in bootstrap.into_iter().chain(book.ids().iter().copied()) {
        if id != me && !out.contains(&id) { out.push(id); }
    }
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkState { Waiting, Connected, Reconnecting, Offline }
impl LinkState {
    pub fn as_str(&self) -> &'static str {
        match self { LinkState::Waiting => "waiting", LinkState::Connected => "connected",
                     LinkState::Reconnecting => "reconnecting", LinkState::Offline => "offline" }
    }
}
/// offline beats everything; any neighbor = connected; lost all after having one =
/// reconnecting; never had one = waiting.
pub fn link_state(neighbors: usize, ever_connected: bool, online: bool) -> LinkState {
    if !online { LinkState::Offline } else if neighbors > 0 { LinkState::Connected }
    else if ever_connected { LinkState::Reconnecting } else { LinkState::Waiting }
}

/// How long after a swapped connection arrives we re-Join its peer (lets the gossip
/// actor install the new connection first, so the Join doesn't go out on the dead one).
pub const SWAP_REJOIN_DELAY_MS: u64 = 500;
/// At most one swap re-Join per peer per this window (a flapping peer can't storm).
pub const SWAP_REJOIN_MIN_MS: u64 = 5_000;

/// Re-Join a peer that opened a NEW connection to us while we still count it as a
/// neighbor — it restarted under the same key (a reloaded tab, a restarted CLI host).
///
/// Why: iroh-gossip's hyparview leaves the peer that sent the LAST `Neighbor` of a
/// handshake (normally the joiner) with a stale `pending_neighbor_requests` entry for
/// the other side. When that other side restarts and re-dials us, gossip swaps the
/// connection in silently (it stays an active neighbor: no NeighborDown/Up here), and
/// our reply to its `Join` (`send_neighbor`) is suppressed by the stale entry — so
/// the restarted peer never gets a NeighborUp, never sends its Hello, and both sides
/// sit "connected"/"waiting" forever. Our own `Join` makes the restarted peer add us
/// (NeighborUp there) and answer with a `Neighbor` that clears the stale entry.
///
/// Pure: callers feed the remote id of every accepted gossip connection
/// (`on_incoming`) and poll `due` from their 1 s tick.
#[derive(Debug, Clone, Default)]
pub struct SwapRejoin { pending: Vec<([u8; 32], u64)>, last: Vec<([u8; 32], u64)> }
impl SwapRejoin {
    pub fn new() -> Self { Self::default() }
    /// An incoming connection from `id`; queued only if it's already a neighbor (a
    /// first-time joiner isn't one yet and needs nothing).
    pub fn on_incoming(&mut self, id: [u8; 32], is_neighbor: bool, now: u64) {
        if is_neighbor && !self.pending.iter().any(|(x, _)| *x == id) {
            self.pending.push((id, now + SWAP_REJOIN_DELAY_MS));
        }
    }
    /// The peers to re-Join now: queued at least SWAP_REJOIN_DELAY_MS ago and still a
    /// neighbor (else dropped), at most once per SWAP_REJOIN_MIN_MS per peer.
    pub fn due(&mut self, now: u64, neighbors: &[[u8; 32]]) -> Vec<[u8; 32]> {
        let last = &mut self.last;
        last.retain(|(_, t)| now < t + SWAP_REJOIN_MIN_MS);
        let mut out = Vec::new();
        self.pending.retain(|&(id, at)| {
            if now < at { return true; }
            if !neighbors.contains(&id) { return false; }
            // Rate-limited: keep it, it fires once the window opens.
            if last.iter().any(|(x, _)| *x == id) { return true; }
            last.push((id, now));
            out.push(id);
            false
        });
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejoin_backs_off_and_resets() {
        let mut r = Rejoin::new();
        r.on_neighbors(0, 0);
        let mut fired = Vec::new();
        for now in 0..=50_000u64 {
            if r.due(now) { fired.push(now); }
        }
        assert_eq!(fired, vec![1000, 3000, 7000, 15000, 31000, 47000]);
        r.on_neighbors(1, 47_001);
        assert_eq!(r.next_at(), None);
        r.on_neighbors(0, 50_000);
        assert_eq!(r.next_at(), Some(51_000));
    }

    #[test]
    fn rejoin_online_retries_now() {
        let mut r = Rejoin::new();
        r.on_neighbors(0, 0);
        assert!(r.due(1000));
        assert!(r.due(3000));
        r.on_online(3500);
        assert!(r.due(3500));
        // Plan deviation: the plan's prose expected `next_at() == Some(4500)` here, but
        // the plan's own literal `due`/`on_online` code (used verbatim above) computes
        // Some(5500): `due` always doubles `backoff` (1000 -> 2000) before adding it to
        // `now`, including on the attempt that `on_online` just made immediate. Keeping
        // the literal code (it's what later steps build on) and fixing the expected
        // number to match its actual, correct-per-the-stated-policy behavior.
        assert_eq!(r.next_at(), Some(5500));

        let mut r2 = Rejoin::new();
        r2.on_neighbors(1, 0);
        r2.on_online(100);
        assert_eq!(r2.next_at(), None);
    }

    #[test]
    fn peer_book_mru_cap_and_topic() {
        let topic = [1u8; 32];
        let me = [2u8; 32];
        let mut book = PeerBook::new(topic);
        assert!(!book.note(me, me));
        let id0 = [3u8; 32];
        assert!(book.note(id0, me));
        assert!(!book.note(id0, me));

        for i in 0..20u8 {
            let mut id = [0u8; 32];
            id[0] = i;
            book.note(id, me);
        }
        assert_eq!(book.ids().len(), 16);
        let mut newest = [0u8; 32];
        newest[0] = 19;
        assert_eq!(book.ids().first(), Some(&newest));

        let older = book.ids()[5];
        assert!(book.note(older, me));
        assert_eq!(book.ids().first(), Some(&older));

        let other_topic = [9u8; 32];
        assert_eq!(PeerBook::decode(other_topic, &book.encode()), PeerBook::new(other_topic));
        assert_eq!(PeerBook::decode(topic, &[0xff, 0xff, 0xff]), PeerBook::new(topic));

        let roundtrip = PeerBook::decode(topic, &book.encode());
        assert_eq!(roundtrip, book);
    }

    #[test]
    fn rejoin_targets_dedup() {
        let me = [0u8; 32];
        let bootstrap = [1u8; 32];
        let mut book = PeerBook::new([9u8; 32]);
        book.note(bootstrap, me);
        book.note([2u8; 32], me);
        book.note(me, me);
        let targets = rejoin_targets(Some(bootstrap), &book, me);
        assert_eq!(targets, vec![bootstrap, [2u8; 32]]);
    }

    #[test]
    fn link_state_table() {
        assert_eq!(link_state(0, false, true), LinkState::Waiting);
        assert_eq!(link_state(1, false, true), LinkState::Connected);
        assert_eq!(link_state(0, true, true), LinkState::Reconnecting);
        assert_eq!(link_state(3, true, false), LinkState::Offline);
    }

    #[test]
    fn swap_rejoin_delays_rate_limits_and_ignores_strangers() {
        let (a, b) = ([1u8; 32], [2u8; 32]);
        let mut g = SwapRejoin::new();
        // Not a neighbor (a first-time joiner): never queued.
        g.on_incoming(b, false, 0);
        assert!(g.due(10_000, &[a, b]).is_empty());
        // A neighbor's new connection: due only after the delay.
        g.on_incoming(a, true, 0);
        assert!(g.due(SWAP_REJOIN_DELAY_MS - 1, &[a]).is_empty());
        assert_eq!(g.due(SWAP_REJOIN_DELAY_MS, &[a]), vec![a]);
        assert!(g.due(SWAP_REJOIN_DELAY_MS + 1, &[a]).is_empty(), "fires once");
        // A flapping peer: held while rate-limited, fired once when the window opens.
        g.on_incoming(a, true, 1_000);
        assert!(g.due(2_000, &[a]).is_empty(), "within SWAP_REJOIN_MIN_MS");
        g.on_incoming(a, true, 3_000);
        assert!(g.due(SWAP_REJOIN_DELAY_MS + SWAP_REJOIN_MIN_MS - 1, &[a]).is_empty());
        assert_eq!(g.due(SWAP_REJOIN_DELAY_MS + SWAP_REJOIN_MIN_MS, &[a]), vec![a]);
        assert!(g.due(9_000, &[a]).is_empty(), "the duplicate incoming was coalesced");
        // Gone by the time it's due (NeighborDown already took over): dropped.
        g.on_incoming(b, true, 20_000);
        assert!(g.due(30_000, &[a]).is_empty());
        assert!(g.due(40_000, &[a, b]).is_empty(), "the dropped entry doesn't resurface");
    }
}
