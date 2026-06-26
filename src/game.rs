//! Co-op/PvP ASCII tower-defense demo — the deterministic game core (DESIGN.md
//! §6, §9b). The whole simulation is a *pure deterministic function* of (initial
//! seed + ordered action log + tick count), so every peer that has the same
//! actions computes byte-identical world state. That determinism is what lets a
//! CRDT-replicated action log drive a convergent real-time game.
//!
//! Real-time convergence uses **deterministic lockstep with input delay**:
//! actions take effect `INPUT_DELAY` ticks in the future, so a peer that issues
//! or receives an action still applies it at the *same tick* as everyone else —
//! no rollback needed (exact under the assumption the action arrives within the
//! delay window, which holds on loopback).
//!
//! Frontends (human TUI, piped bot) live above this via action-in/state-out
//! (§9b); the network layer replicates the action log via `net`/`transport`.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

pub const WIDTH: usize = 12; // tiles per lane
pub const SUBTILE: u32 = 100; // sub-tile units for smooth movement
pub const ENEMY_SPEED: u32 = 20; // sub-tiles per tick
pub const TOWER_COST: i32 = 50;
pub const TOWER_RANGE: i32 = 2; // tiles
pub const TOWER_DMG: i32 = 4;
pub const TOWER_COOLDOWN: u32 = 5; // ticks between shots
pub const CREEP_COST: i32 = 25;
pub const ENEMY_HP: i32 = 10;
pub const ENEMY_BOUNTY: i32 = 8;
pub const SPAWN_PERIOD: u32 = 40; // ticks between auto-spawns
pub const START_GOLD: i32 = 120;
pub const START_LIVES: i32 = 10;
pub const INPUT_DELAY: u64 = 4; // ticks; see module docs

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub enum Player {
    P1,
    P2,
}

impl Player {
    pub fn opponent(self) -> Player {
        match self {
            Player::P1 => Player::P2,
            Player::P2 => Player::P1,
        }
    }
}

/// A player action — the unit replicated across peers. `tick` is the *effective*
/// tick (issue tick + INPUT_DELAY); `(tick, seq, player)` totally orders actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    pub tick: u64,
    pub seq: u64,
    pub player: Player,
    pub kind: ActionKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionKind {
    /// Build a tower on your own lane at `tile`.
    PlaceTower { tile: usize },
    /// Spend gold to send a creep into the opponent's lane.
    SendCreep,
}

#[derive(Clone, Copy, Debug, Hash)]
pub struct Tower {
    pub tile: usize,
    pub cooldown: u32,
}

#[derive(Clone, Copy, Debug, Hash)]
pub struct Enemy {
    pub pos: u32, // sub-tile position along the lane
    pub hp: i32,
}

#[derive(Clone, Debug)]
pub struct Lane {
    pub towers: Vec<Tower>,
    pub enemies: Vec<Enemy>,
    pub lives: i32,
    pub gold: i32,
    pub spawn_timer: u32,
}

impl Lane {
    fn new() -> Self {
        Lane {
            towers: Vec::new(),
            enemies: Vec::new(),
            lives: START_LIVES,
            gold: START_GOLD,
            spawn_timer: SPAWN_PERIOD,
        }
    }
    fn tile_occupied(&self, tile: usize) -> bool {
        self.towers.iter().any(|t| t.tile == tile)
    }
}

#[derive(Clone, Debug)]
pub struct World {
    pub tick: u64,
    pub p1: Lane,
    pub p2: Lane,
}

impl Default for World {
    fn default() -> Self {
        World {
            tick: 0,
            p1: Lane::new(),
            p2: Lane::new(),
        }
    }
}

impl World {
    pub fn lane(&self, p: Player) -> &Lane {
        match p {
            Player::P1 => &self.p1,
            Player::P2 => &self.p2,
        }
    }
    fn lane_mut(&mut self, p: Player) -> &mut Lane {
        match p {
            Player::P1 => &mut self.p1,
            Player::P2 => &mut self.p2,
        }
    }

    pub fn alive(&self) -> bool {
        self.p1.lives > 0 && self.p2.lives > 0
    }

    pub fn winner(&self) -> Option<Player> {
        match (self.p1.lives > 0, self.p2.lives > 0) {
            (true, false) => Some(Player::P1),
            (false, true) => Some(Player::P2),
            _ => None,
        }
    }

    /// Apply an action. This is the **guard** (§6): illegal actions (wrong lane,
    /// occupied tile, not enough gold) are deterministically rejected — every
    /// peer rejects identically, so a cheating/buggy peer can't desync honest
    /// ones. Returns whether the action was accepted.
    pub fn apply(&mut self, a: &Action) -> bool {
        match a.kind {
            ActionKind::PlaceTower { tile } => {
                if tile >= WIDTH {
                    return false;
                }
                let lane = self.lane_mut(a.player);
                if lane.gold < TOWER_COST || lane.tile_occupied(tile) {
                    return false;
                }
                lane.gold -= TOWER_COST;
                lane.towers.push(Tower { tile, cooldown: 0 });
                true
            }
            ActionKind::SendCreep => {
                let lane = self.lane_mut(a.player);
                if lane.gold < CREEP_COST {
                    return false;
                }
                lane.gold -= CREEP_COST;
                // Creep goes into the OPPONENT's lane.
                self.lane_mut(a.player.opponent())
                    .enemies
                    .push(Enemy { pos: 0, hp: ENEMY_HP });
                true
            }
        }
    }

    /// Advance the simulation by one tick (towers fire, enemies move, spawns).
    pub fn step(&mut self) {
        step_lane(&mut self.p1);
        step_lane(&mut self.p2);
        self.tick += 1;
    }

    /// A deterministic state fingerprint — used for convergence checks and
    /// lockstep desync detection.
    pub fn state_hash(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.tick.hash(&mut h);
        for lane in [&self.p1, &self.p2] {
            lane.lives.hash(&mut h);
            lane.gold.hash(&mut h);
            lane.spawn_timer.hash(&mut h);
            for t in &lane.towers {
                t.tile.hash(&mut h);
                t.cooldown.hash(&mut h);
            }
            for e in &lane.enemies {
                e.pos.hash(&mut h);
                e.hp.hash(&mut h);
            }
        }
        h.finish()
    }
}

fn step_lane(lane: &mut Lane) {
    // 1) Towers fire at the most-advanced enemy in range.
    for ti in 0..lane.towers.len() {
        if lane.towers[ti].cooldown > 0 {
            lane.towers[ti].cooldown -= 1;
            continue;
        }
        let tower_tile = lane.towers[ti].tile as i32;
        // pick the enemy furthest along that is within range
        let mut target: Option<usize> = None;
        let mut best_pos = -1i32;
        for (ei, e) in lane.enemies.iter().enumerate() {
            if e.hp <= 0 {
                continue;
            }
            let etile = (e.pos / SUBTILE) as i32;
            if (etile - tower_tile).abs() <= TOWER_RANGE && e.pos as i32 > best_pos {
                best_pos = e.pos as i32;
                target = Some(ei);
            }
        }
        if let Some(ei) = target {
            lane.enemies[ei].hp -= TOWER_DMG;
            lane.towers[ti].cooldown = TOWER_COOLDOWN;
            if lane.enemies[ei].hp <= 0 {
                lane.gold += ENEMY_BOUNTY;
            }
        }
    }

    // 2) Move enemies; those reaching the base cost a life.
    let reach = (WIDTH as u32) * SUBTILE;
    let mut leaked = 0;
    for e in lane.enemies.iter_mut() {
        if e.hp <= 0 {
            continue;
        }
        e.pos += ENEMY_SPEED;
        if e.pos >= reach {
            leaked += 1;
            e.hp = 0; // mark for removal
        }
    }
    lane.lives -= leaked;
    lane.enemies.retain(|e| e.hp > 0);

    // 3) Auto-spawn waves.
    if lane.spawn_timer == 0 {
        lane.enemies.push(Enemy { pos: 0, hp: ENEMY_HP });
        lane.spawn_timer = SPAWN_PERIOD;
    } else {
        lane.spawn_timer -= 1;
    }
}

/// Render the world as an emoji board (two lanes + HUD).
pub fn render(world: &World) -> String {
    let mut out = String::new();
    out.push_str(&format!("  tick {}\n", world.tick));
    out.push_str(&render_lane("P1", &world.p1));
    out.push_str(&render_lane("P2", &world.p2));
    if let Some(w) = world.winner() {
        out.push_str(&format!(
            "  *** {} WINS ***\n",
            if w == Player::P1 { "P1" } else { "P2" }
        ));
    }
    out
}

fn render_lane(name: &str, lane: &Lane) -> String {
    let mut tiles: Vec<&str> = vec!["🟫"; WIDTH];
    for t in &lane.towers {
        if t.tile < WIDTH {
            tiles[t.tile] = "🗼";
        }
    }
    // enemies drawn on top of their tile
    for e in &lane.enemies {
        let tile = (e.pos / SUBTILE) as usize;
        if tile < WIDTH {
            tiles[tile] = "👾";
        }
    }
    let lane_str: String = tiles.concat();
    format!(
        "{name} {lane_str}🏰 ❤️{lives:>2} 💰{gold:>3}\n",
        lives = lane.lives,
        gold = lane.gold,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scripted_actions() -> Vec<Action> {
        vec![
            Action { tick: 2, seq: 0, player: Player::P1, kind: ActionKind::PlaceTower { tile: 3 } },
            Action { tick: 2, seq: 1, player: Player::P2, kind: ActionKind::PlaceTower { tile: 5 } },
            Action { tick: 10, seq: 2, player: Player::P1, kind: ActionKind::SendCreep },
            Action { tick: 20, seq: 3, player: Player::P1, kind: ActionKind::PlaceTower { tile: 6 } },
            Action { tick: 30, seq: 4, player: Player::P2, kind: ActionKind::SendCreep },
        ]
    }

    /// Run the sim from an ordered action log for `ticks` steps.
    fn run(actions: &[Action], ticks: u64) -> World {
        let mut w = World::default();
        // actions must be applied in deterministic order at their effective tick
        let mut sorted = actions.to_vec();
        sorted.sort_by_key(|a| (a.tick, a.seq, a.player as usize));
        let mut idx = 0;
        while w.tick < ticks {
            while idx < sorted.len() && sorted[idx].tick == w.tick {
                w.apply(&sorted[idx]);
                idx += 1;
            }
            w.step();
        }
        w
    }

    #[test]
    fn simulation_is_deterministic() {
        // Two independent runs of the same action log produce identical worlds —
        // the property that makes CRDT-replicated play converge.
        let a = run(&scripted_actions(), 200);
        let b = run(&scripted_actions(), 200);
        assert_eq!(a.state_hash(), b.state_hash());
        assert_eq!(render(&a), render(&b));
    }

    #[test]
    fn order_independent_action_log_converges() {
        // Same actions, shuffled input order -> sorting yields the same sim.
        let mut shuffled = scripted_actions();
        shuffled.reverse();
        let a = run(&scripted_actions(), 200);
        let b = run(&shuffled, 200);
        assert_eq!(a.state_hash(), b.state_hash());
    }

    #[test]
    fn guard_rejects_illegal_builds() {
        let mut w = World::default();
        // occupy tile 3
        assert!(w.apply(&Action { tick: 0, seq: 0, player: Player::P1, kind: ActionKind::PlaceTower { tile: 3 } }));
        // same tile again -> rejected
        assert!(!w.apply(&Action { tick: 0, seq: 1, player: Player::P1, kind: ActionKind::PlaceTower { tile: 3 } }));
        // out of bounds -> rejected
        assert!(!w.apply(&Action { tick: 0, seq: 2, player: Player::P1, kind: ActionKind::PlaceTower { tile: 99 } }));
    }
}
