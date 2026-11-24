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
    activity_first: Option<u64>,
    activity_last: u64,
    confirmed: std::collections::HashSet<[u8; 32]>,
    inc_gap: u64,
    last_inc_reset: Option<u64>,
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
            activity_first: None,
            activity_last: now,
            confirmed: std::collections::HashSet::new(),
            inc_gap: AE_INCONSISTENT_GAP_MIN_MS,
            last_inc_reset: None,
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
        self.stats.sent += 1;
        self.schedule_next(now);
    }

    pub fn on_loss(&mut self, now: u64) {
        self.confirmed.clear();
        self.changed = true;
        self.stats.losses += 1;
        self.reset(now);
    }

    pub fn on_hello(&mut self, now: u64, from: [u8; 32], consistent: bool) {
        if consistent {
            self.confirmed.insert(from);
            return;
        }
        self.confirmed.remove(&from);
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
            self.inc_gap = AE_INCONSISTENT_GAP_MIN_MS;
            self.last_inc_reset = None;
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
        if neighbors.is_empty() || (!self.changed && neighbors.iter().all(|n| self.confirmed.contains(n))) {
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
        assert!(sends.is_empty(), "confirmed neighbor should stay silent: {sends:?}");
        assert!(ae.stats().skipped > 0);
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
