//! Anti-entropy scheduling for the gossip mesh (Trickle-style, RFC 6206 flavour):
//! when to send our `Hello` digest to direct neighbors. Pure — callers pass ms
//! `now`, the Syncer `generation`, the current neighbor ids, and feed back what
//! they hear. No I/O, no clock, deterministic given the seed.

pub const AE_IMIN_MS: u64 = 2_000;
pub const AE_IMAX_MS: u64 = 128_000;
pub const AE_QUIET_MS: u64 = 1_500;
pub const AE_MAX_DEFER_MS: u64 = 10_000;
pub const AE_MIN_GAP_MS: u64 = 1_000;
pub const AE_INCONSISTENT_GAP_MIN_MS: u64 = 8_000;
/// Minimum gap between loss-triggered resets (doubling up to `AE_IMAX_MS` while
/// losses keep coming): a peer under sustained loss must not answer every
/// `Lagged` with a Hello, each of which draws diffs from every neighbor ahead.
pub const AE_LOSS_GAP_MIN_MS: u64 = 8_000;
/// How often drivers call `poll`.
pub const AE_TICK_MS: u64 = 250;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AeStats {
    pub sent: u64,
    pub skipped: u64,
    pub resets: u64,
    pub losses: u64,
}

pub struct AntiEntropy {
    interval: u64,
    next_at: u64,
    last_sent: Option<u64>,
    generation: u64,
    changed: bool,
    /// A neighbor just became confirmed: send it one Hello so it can confirm us
    /// too (it only does on a Hello from us), else it keeps probing us.
    ack: bool,
    activity_first: Option<u64>,
    activity_last: u64,
    confirmed: std::collections::HashSet<[u8; 32]>,
    inc_gap: u64,
    last_inc_reset: Option<u64>,
    loss_gap: u64,
    last_loss_reset: Option<u64>,
    rng: u64,
    stats: AeStats,
}

impl AntiEntropy {
    pub fn new(now: u64, seed: u64) -> Self {
        let rng = if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed };
        let mut ae = AntiEntropy {
            interval: AE_IMIN_MS,
            next_at: now,
            last_sent: None,
            generation: 0,
            changed: true,
            ack: false,
            activity_first: None,
            activity_last: now,
            confirmed: std::collections::HashSet::new(),
            inc_gap: AE_INCONSISTENT_GAP_MIN_MS,
            last_inc_reset: None,
            loss_gap: AE_LOSS_GAP_MIN_MS,
            last_loss_reset: None,
            rng,
            stats: AeStats::default(),
        };
        ae.next_at = now + ae.rand_in(AE_IMIN_MS / 2, AE_IMIN_MS);
        ae
    }

    /// xorshift64*.
    fn next_u64(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn rand_in(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            lo
        } else {
            lo + self.next_u64() % (hi - lo)
        }
    }

    fn schedule_next(&mut self, now: u64) {
        let i = self.interval;
        self.next_at = now + self.rand_in(i / 2, i);
        self.interval = (i * 2).min(AE_IMAX_MS);
    }

    fn reset(&mut self, now: u64) {
        self.interval = AE_IMIN_MS;
        let at = now + self.rand_in(AE_IMIN_MS / 4, AE_IMIN_MS / 2);
        self.next_at = self.next_at.min(at);
        self.stats.resets += 1;
    }

    pub fn on_sent(&mut self, now: u64) {
        self.last_sent = Some(now);
        self.changed = false;
        self.ack = false;
        self.stats.sent += 1;
        self.schedule_next(now);
    }

    pub fn on_loss(&mut self, now: u64) {
        self.confirmed.clear();
        self.changed = true;
        self.stats.losses += 1;
        // `changed` alone makes the next scheduled round go out; the immediate
        // reset is rate-limited, backing off while losses keep coming and
        // starting over after a loss-free stretch.
        if self.last_loss_reset.is_none_or(|t| now >= t + self.loss_gap) {
            if let Some(t) = self.last_loss_reset {
                self.loss_gap = if now < t + 2 * self.loss_gap {
                    (self.loss_gap * 2).min(AE_IMAX_MS)
                } else {
                    AE_LOSS_GAP_MIN_MS
                };
            }
            self.last_loss_reset = Some(now);
            self.reset(now);
        }
    }

    pub fn on_hello(&mut self, now: u64, from: [u8; 32], consistent: bool) {
        if consistent {
            // Unconfirmed → confirmed: one Hello back. On the other side we are
            // then already confirmed, so the exchange ends there. (While our own
            // state is still changing, the round the activity timer schedules
            // serves as that Hello — pulling it earlier only adds traffic.)
            if self.confirmed.insert(from) && self.activity_first.is_none() {
                self.ack = true;
                let at = now + self.rand_in(AE_IMIN_MS / 4, AE_IMIN_MS / 2);
                self.next_at = self.next_at.min(at);
            }
            return;
        }
        self.confirmed.remove(&from);
        // While our own state is still changing, the quiet / max-defer timer in
        // `poll` already schedules the next round; resetting here too would make
        // two editing neighbors (whose Hellos always mismatch while edits are in
        // flight) answer each other's Hellos at the 1 s floor.
        if self.activity_first.is_some() {
            return;
        }
        if self.last_inc_reset.is_none_or(|t| now >= t + self.inc_gap) {
            if self.last_inc_reset.is_some() {
                self.inc_gap = (self.inc_gap * 2).min(AE_IMAX_MS);
            }
            self.last_inc_reset = Some(now);
            self.reset(now);
        }
    }

    pub fn poll(&mut self, now: u64, generation: u64, neighbors: &[[u8; 32]]) -> bool {
        self.confirmed.retain(|n| neighbors.contains(n));
        if generation != self.generation {
            self.generation = generation;
            self.changed = true;
            self.confirmed.clear();
            // A fresh change restarts the mismatch backoff at its minimum gap, but
            // the gap still runs from the last mismatch reset (not from now).
            self.inc_gap = AE_INCONSISTENT_GAP_MIN_MS;
            self.activity_first.get_or_insert(now);
            self.activity_last = now;
        }
        if let Some(first) = self.activity_first {
            if now >= self.activity_last + AE_QUIET_MS || now >= first + AE_MAX_DEFER_MS {
                self.activity_first = None;
                self.reset(now);
            }
        }
        if now < self.next_at {
            return false;
        }
        if self.last_sent.is_some_and(|t| now < t + AE_MIN_GAP_MS) {
            return false;
        }
        if neighbors.is_empty()
            || (!self.changed && !self.ack && neighbors.iter().all(|n| self.confirmed.contains(n)))
        {
            self.schedule_next(now);
            self.stats.skipped += 1;
            return false;
        }
        self.on_sent(now);
        true
    }

    pub fn stats(&self) -> AeStats {
        self.stats
    }

    pub fn interval_ms(&self) -> u64 {
        self.interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const N1: [u8; 32] = [1; 32];
    const N2: [u8; 32] = [2; 32];

    /// Poll every `AE_TICK_MS` from `start` to `end` (exclusive), recording the
    /// times `poll` returns true. `step` is called each tick with the current
    /// `now` BEFORE polling, so a test can drive generation/hello changes.
    fn drive(
        ae: &mut AntiEntropy,
        start: u64,
        end: u64,
        neighbors: &[[u8; 32]],
        mut generation: u64,
        mut step: impl FnMut(u64, &mut u64),
    ) -> Vec<u64> {
        let mut sends = Vec::new();
        let mut now = start;
        while now < end {
            step(now, &mut generation);
            if ae.poll(now, generation, neighbors) {
                sends.push(now);
            }
            now += AE_TICK_MS;
        }
        sends
    }

    fn assert_min_gap(sends: &[u64]) {
        for w in sends.windows(2) {
            assert!(
                w[1] - w[0] >= AE_MIN_GAP_MS,
                "sends too close together: {} then {}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn first_hello_goes_out_within_imin() {
        let mut ae = AntiEntropy::new(0, 7);
        let sends = drive(&mut ae, 0, 600_000, &[N1], 0, |_, _| {});
        assert_min_gap(&sends);
        assert!(!sends.is_empty());
        assert!(sends[0] <= AE_IMIN_MS, "first send at {}", sends[0]);
    }

    #[test]
    fn unconfirmed_neighbor_backs_off_to_imax() {
        let mut ae = AntiEntropy::new(0, 7);
        let sends = drive(&mut ae, 0, 600_000, &[N1], 0, |_, _| {});
        assert_min_gap(&sends);
        assert!(sends.len() <= 20, "too many sends: {}", sends.len());
        for w in sends.windows(2).rev().take(3) {
            let gap = w[1] - w[0];
            assert!(
                (AE_IMAX_MS / 2..=AE_IMAX_MS + AE_TICK_MS).contains(&gap),
                "gap {gap} not near AE_IMAX_MS"
            );
        }
    }

    #[test]
    fn confirmed_idle_is_silent() {
        let mut ae = AntiEntropy::new(0, 7);
        let first = drive(&mut ae, 0, AE_IMIN_MS + AE_TICK_MS, &[N1], 0, |_, _| {});
        assert_min_gap(&first);
        let t0 = *first.last().expect("a first send happened");
        ae.on_hello(t0, N1, true);
        let sends = drive(&mut ae, t0 + AE_TICK_MS, t0 + 600_000, &[N1], 0, |_, _| {});
        assert_min_gap(&sends);
        assert_eq!(sends.len(), 1, "one Hello back to the newly confirmed neighbor, then silence: {sends:?}");
        assert!(sends[0] <= t0 + AE_IMIN_MS / 2 + AE_MIN_GAP_MS + AE_TICK_MS);
        ae.on_hello(t0 + 600_000, N1, true);
        let later = drive(&mut ae, t0 + 600_000, t0 + 1_200_000, &[N1], 0, |_, _| {});
        assert!(later.is_empty(), "an already-confirmed neighbor's Hello needs no answer: {later:?}");
        assert!(ae.stats().skipped > 0);
    }

    /// One-sided confirmation: B's first Hello reaches A before A caught up (a
    /// mismatch), then A catches up (its generation moves) and sends a Hello B
    /// finds consistent. B — with nothing changed — would skip, leaving A
    /// unconfirmed about B and probing forever; B's one Hello back ends it.
    #[test]
    fn one_sided_confirmation_is_answered_then_silent() {
        let (na, nb) = ([0xA; 32], [0xB; 32]);
        let mut a = AntiEntropy::new(0, 3);
        let mut b = AntiEntropy::new(0, 4);
        let (mut sa, mut sb) = (Vec::new(), Vec::new());
        let (mut a_gen, mut caught_up) = (0u64, false);
        let mut now = 0;
        while now < 900_000 {
            if !caught_up && !sb.is_empty() && now >= sb[0] + 20_000 {
                a_gen += 1; // A applies the write B's Hello told it about
                caught_up = true;
            }
            if a.poll(now, a_gen, &[nb]) {
                sa.push(now);
                b.on_hello(now, na, caught_up);
            }
            if b.poll(now, 0, &[na]) {
                sb.push(now);
                a.on_hello(now, nb, caught_up);
            }
            now += AE_TICK_MS;
        }
        assert_min_gap(&sa);
        assert_min_gap(&sb);
        assert!(caught_up);
        let late = sa.iter().chain(&sb).filter(|&&t| t >= 60_000).count();
        assert_eq!(late, 0, "still probing long after convergence: A {sa:?} B {sb:?}");
    }

    #[test]
    fn quiet_after_burst_triggers_a_round() {
        let mut ae = AntiEntropy::new(0, 7);
        let first = drive(&mut ae, 0, AE_IMIN_MS + AE_TICK_MS, &[N1], 0, |_, _| {});
        let t0 = *first.last().unwrap();
        ae.on_hello(t0, N1, true);
        drive(&mut ae, t0 + AE_TICK_MS, 600_000, &[N1], 0, |_, _| {});

        let mut sends = Vec::new();
        let mut now = 600_000u64;
        let mut generation = 0u64;
        while now < 620_000 {
            if now < 605_000 {
                generation += 1;
            }
            if ae.poll(now, generation, &[N1]) {
                sends.push(now);
            }
            now += AE_TICK_MS;
        }
        assert_min_gap(&sends);
        assert!(
            sends.iter().any(|&t| (606_250..=608_250).contains(&t)),
            "no send in the expected quiet-after-burst window: {sends:?}"
        );
    }

    #[test]
    fn continuous_activity_still_rounds_every_max_defer() {
        let mut ae = AntiEntropy::new(0, 7);
        let first = drive(&mut ae, 0, AE_IMIN_MS + AE_TICK_MS, &[N1], 0, |_, _| {});
        let t0 = *first.last().unwrap();
        ae.on_hello(t0, N1, true);

        let mut sends = Vec::new();
        let mut now = t0;
        let mut generation = 0u64;
        while now < t0 + 30_000 {
            generation += 1;
            if ae.poll(now, generation, &[N1]) {
                sends.push(now);
            }
            now += AE_TICK_MS;
        }
        assert_min_gap(&sends);
        let first_send = sends.first().copied().expect("some send during continuous activity");
        assert!(
            first_send <= t0 + AE_MAX_DEFER_MS + AE_IMIN_MS / 2 + AE_MIN_GAP_MS + 2 * AE_TICK_MS,
            "first send too late: {first_send}"
        );
        assert!(
            (2..=30).contains(&sends.len()),
            "unexpected send count: {}",
            sends.len()
        );
    }

    /// Two neighbors both editing continuously, each hearing the other's Hello
    /// as a mismatch (edits in flight): the mismatches must not reset the
    /// schedule on every Hello — activity already schedules the next round.
    #[test]
    fn sustained_editing_does_not_ping_pong() {
        let (na, nb) = ([0xA; 32], [0xB; 32]);
        let mut a = AntiEntropy::new(0, 7);
        let mut b = AntiEntropy::new(0, 8);
        // Converge and confirm each other first.
        let mut now = 0;
        while now < 60_000 {
            if a.poll(now, 0, &[nb]) {
                b.on_hello(now, na, true);
            }
            if b.poll(now, 0, &[na]) {
                a.on_hello(now, nb, true);
            }
            now += AE_TICK_MS;
        }
        let t0 = now;
        let (mut sa, mut sb) = (Vec::new(), Vec::new());
        let mut generation = 0u64;
        while now < t0 + 30_000 {
            generation += 1;
            if a.poll(now, generation, &[nb]) {
                sa.push(now);
                b.on_hello(now, na, false);
            }
            if b.poll(now, generation, &[na]) {
                sb.push(now);
                a.on_hello(now, nb, false);
            }
            now += AE_TICK_MS;
        }
        assert_min_gap(&sa);
        assert_min_gap(&sb);
        println!("30 s of sustained editing: A sent {}, B sent {}", sa.len(), sb.len());
        assert!(sa.len() <= 6 && sb.len() <= 6, "ping-pong: A {sa:?} B {sb:?}");
        assert!(!sa.is_empty() && !sb.is_empty(), "rounds still happen during activity");
    }

    #[test]
    fn inconsistent_hellos_are_rate_limited() {
        let mut ae = AntiEntropy::new(0, 7);
        let first = drive(&mut ae, 0, AE_IMIN_MS + AE_TICK_MS, &[N1], 0, |_, _| {});
        let t1 = *first.last().unwrap();
        ae.on_hello(t1, N1, true);

        let mut sends = Vec::new();
        let mut now = t1;
        let mut last_hello = t1 - 1_000;
        while now < t1 + 600_000 {
            if now >= last_hello + 1_000 {
                ae.on_hello(now, N1, false);
                last_hello = now;
            }
            if ae.poll(now, 0, &[N1]) {
                sends.push(now);
            }
            now += AE_TICK_MS;
        }
        assert_min_gap(&sends);
        let first_send = sends.first().copied().expect("some send happens");
        assert!(
            first_send <= t1 + AE_IMIN_MS / 2 + AE_TICK_MS,
            "first send too late: {first_send}"
        );
        assert!(
            (8..=60).contains(&sends.len()),
            "unexpected send count: {}",
            sends.len()
        );
    }

    #[test]
    fn no_neighbors_never_sends() {
        let mut ae = AntiEntropy::new(0, 7);
        let sends = drive(&mut ae, 0, 600_000, &[], 0, |_, _| {});
        assert!(sends.is_empty());
    }

    #[test]
    fn loss_forces_a_round_even_when_confirmed() {
        let mut ae = AntiEntropy::new(0, 7);
        let first = drive(&mut ae, 0, AE_IMIN_MS + AE_TICK_MS, &[N1], 0, |_, _| {});
        let t0 = *first.last().unwrap();
        ae.on_hello(t0, N1, true);
        ae.on_loss(t0 + AE_TICK_MS);

        let sends = drive(&mut ae, t0 + 2 * AE_TICK_MS, t0 + 600_000, &[N1], 0, |_, _| {});
        assert_min_gap(&sends);
        let first_after = sends.first().copied().expect("a send after loss");
        assert!(
            first_after <= t0 + AE_TICK_MS + AE_IMIN_MS / 2 + AE_TICK_MS,
            "send too late after loss: {first_after}"
        );
    }

    /// A loss on every tick (a peer drowning in `Lagged`) still backs off.
    #[test]
    fn sustained_loss_is_rate_limited() {
        let mut ae = AntiEntropy::new(0, 7);
        let first = drive(&mut ae, 0, AE_IMIN_MS + AE_TICK_MS, &[N1], 0, |_, _| {});
        let t0 = *first.last().unwrap();
        ae.on_hello(t0, N1, true);
        let mut sends = Vec::new();
        let mut now = t0 + AE_TICK_MS;
        while now < t0 + 600_000 {
            ae.on_loss(now);
            if ae.poll(now, 0, &[N1]) {
                sends.push(now);
            }
            now += AE_TICK_MS;
        }
        assert_min_gap(&sends);
        let first_send = sends.first().copied().expect("a loss forces a round");
        assert!(first_send <= t0 + AE_TICK_MS + AE_IMIN_MS / 2 + AE_TICK_MS, "first send too late: {first_send}");
        println!("600 s of a loss every tick: {} sends", sends.len());
        assert!(sends.len() <= 60, "loss-driven sends not rate-limited: {}", sends.len());
    }

    #[test]
    fn new_neighbor_is_unconfirmed() {
        let mut ae = AntiEntropy::new(0, 7);
        let first = drive(&mut ae, 0, AE_IMIN_MS + AE_TICK_MS, &[N1], 0, |_, _| {});
        let t0 = *first.last().unwrap();
        ae.on_hello(t0, N1, true);

        let sends = drive(
            &mut ae,
            t0 + AE_TICK_MS,
            t0 + AE_IMAX_MS + AE_TICK_MS,
            &[N1, N2],
            0,
            |_, _| {},
        );
        assert_min_gap(&sends);
        assert!(!sends.is_empty(), "a new unconfirmed neighbor must trigger a send");
    }

    #[test]
    fn same_seed_same_schedule() {
        let mut a = AntiEntropy::new(0, 42);
        let mut b = AntiEntropy::new(0, 42);
        let sa = drive(&mut a, 0, 600_000, &[N1], 0, |_, _| {});
        let sb = drive(&mut b, 0, 600_000, &[N1], 0, |_, _| {});
        assert_eq!(sa, sb);
    }
}
