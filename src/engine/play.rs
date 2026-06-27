//! Networked game client (DESIGN.md §6, §9b). Wraps the deterministic game core
//! with a replicated **action log** (a grow-only set — trivially convergent) and
//! drives it with **deterministic lockstep**: every tick, peers exchange the
//! actions they issued, then advance the sim in step. Because the sim is a pure
//! function of the action log and the tick, both peers stay byte-identical.
//!
//! The client is a clean action-in / state-out unit (§9b): a human TUI or a piped
//! bot drive it the same way (`issue` / read `world`), and it runs over any
//! `Link` — the in-memory mock or real iroh QUIC.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::engine::game::{Action, ActionKind, Player, World, INPUT_DELAY};
use crate::net::{anyerr, Link, Result};

/// One tick's worth of input exchanged between peers (possibly empty).
#[derive(Serialize, Deserialize)]
struct TickMsg {
    tick: u64,
    actions: Vec<Action>,
}

pub struct GameClient {
    pub me: Player,
    pub world: World,
    log: Vec<Action>,
    seen: HashSet<(u8, u64)>, // (player discriminant, seq) for dedup
    next_seq: u64,
    pending_out: Vec<Action>,
}

impl GameClient {
    pub fn new(me: Player) -> Self {
        GameClient {
            me,
            world: World::default(),
            log: Vec::new(),
            seen: HashSet::new(),
            next_seq: 0,
            pending_out: Vec::new(),
        }
    }

    /// Issue a local action. It takes effect `INPUT_DELAY` ticks in the future so
    /// the peer applies it at the same tick we do (§game module docs).
    pub fn issue(&mut self, kind: ActionKind) {
        let a = Action {
            tick: self.world.tick + INPUT_DELAY,
            seq: self.next_seq,
            player: self.me,
            kind,
        };
        self.next_seq += 1;
        self.record(a);
        self.pending_out.push(a);
    }

    fn record(&mut self, a: Action) {
        if self.seen.insert((a.player as u8, a.seq)) {
            self.log.push(a);
        }
    }

    /// Merge a peer's action (grow-only set: dedup by id, order-independent).
    pub fn ingest(&mut self, a: Action) {
        self.record(a);
    }

    /// Apply all actions effective at the current tick (deterministic order),
    /// then advance the simulation one tick.
    pub fn advance(&mut self) {
        let t = self.world.tick;
        let mut due: Vec<Action> = self.log.iter().copied().filter(|a| a.tick == t).collect();
        due.sort_by_key(|a| (a.player as usize, a.seq));
        for a in due {
            self.world.apply(&a);
        }
        self.world.step();
    }

    /// One lockstep round over a link: send our pending actions, receive the
    /// peer's, then advance one tick. Both peers send before receiving, so this
    /// never deadlocks.
    pub async fn lockstep_round(&mut self, link: &mut dyn Link) -> Result<()> {
        let msg = TickMsg {
            tick: self.world.tick,
            actions: std::mem::take(&mut self.pending_out),
        };
        link.send(serde_json::to_vec(&msg).map_err(anyerr)?).await?;
        if let Some(bytes) = link.recv().await? {
            let peer: TickMsg = serde_json::from_slice(&bytes).map_err(anyerr)?;
            for a in peer.actions {
                self.ingest(a);
            }
        }
        self.advance();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::mock_pair;

    #[tokio::test]
    async fn two_clients_lockstep_converge() {
        let (mut l1, mut l2) = mock_pair();
        let mut c1 = GameClient::new(Player::P1);
        let mut c2 = GameClient::new(Player::P2);

        for t in 0..200u64 {
            // Scripted "bot" inputs from each side.
            if t == 5 {
                c1.issue(ActionKind::PlaceTower { tile: 4 });
                c2.issue(ActionKind::PlaceTower { tile: 6 });
            }
            if t == 20 {
                c1.issue(ActionKind::SendCreep);
            }
            if t == 40 {
                c2.issue(ActionKind::SendCreep);
            }

            tokio::join!(
                async { c1.lockstep_round(&mut l1).await.unwrap() },
                async { c2.lockstep_round(&mut l2).await.unwrap() },
            );
        }

        // Both peers simulated to byte-identical worlds purely from the shared
        // action log — real-time multiplayer convergence.
        assert_eq!(c1.world.state_hash(), c2.world.state_hash());
        assert_eq!(c1.world.tick, c2.world.tick);
        assert_eq!(c1.world.p1.towers.len(), 1);
        assert_eq!(c1.world.p2.towers.len(), 1);
    }
}
