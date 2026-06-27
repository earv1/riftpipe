# riftpipe — project status & resume notes

> Resume doc for picking the project back up (e.g. after reopening in a new
> directory / fresh session). Deep architecture + decision trail lives in
> **DESIGN.md**; this file is the "where are we, what's next, how to run it."

## What it is
A Unix-like terminal construct for **live, peer-to-peer collaborative text** —
"pipe a document to other computers and converge." eg-walker CRDT
(diamond-types) over **iroh** QUIC, end-to-end encrypted, no central server. The
flagship use is the **`--pipe` editor protocol + a neovim bridge** for live
char-level collaborative editing.

(Name: was `autoshare`, renamed to **riftpipe** — unique, free crate/org,
riftpipe.io/.sh available. "Open a rift, pipe through it.")

## Repo / branch state
- GitHub: **https://github.com/earv1/riftpipe** (public).
- Work branch: **`event-driven-pipe`**, with **PR #1** open against `main`.
- `main` has only the initial commit; essentially all real work is on the branch.
- **Guardrail:** a hook blocks committing/pushing to `main` (and any command
  containing the word "main") — the user pushes branches and merges PRs. Claude
  commits on feature branches only.
- Local dir renamed to `riftpipe/` (this file moved with it).

## How to build / run
```sh
cargo test                  # full suite (currently 21 passing, 0 warnings)
cargo run -- simulate       # offline: ruled replay engine demo
cargo run -- text           # offline: eg-walker convergence demo
cargo run -- td             # offline: tower-defense core preview (emoji board)
./run-local.sh              # two-peer tmux demo: nvim + bridge, live char sync, metrics panes
```
CLI: `riftpipe share <file> [--pipe] [--metrics <path>]` /
`riftpipe join <ticket> <file> [--pipe] [--metrics <path>]`.

nvim bridge (session-local, no install):
```sh
RIFTPIPE_BIN=./target/debug/riftpipe RIFTPIPE_ARGS="share /path/file --pipe" \
  nvim -c 'luafile nvim/riftpipe.lua' /path/file
```

## Module map (src/)
- `net/`   — link (Link trait + mock + counters), transport (iroh), secure (ticket + auth)
- `crdt/`  — text (eg-walker document, diamond-types): diff-to-ops, delta encode/merge, version vectors
- `sync/`  — pipe (the `--pipe` protocol: event-driven session, reconciliation, reconnection), mirror (file-mirror loop)
- `monitor/` — metrics (one-line status to a file for tmux; connection-kind detection)
- `engine/` — programmable-rules state machine + tower-defense demo (replay, rules, op, log, document, simulation, identity, handshake, game, play)

## What's done (the branch)
- Event-driven `--pipe` (split link halves + `select!` loop; **idle = silent**).
- Neovim bridge (`nvim/riftpipe.lua`): buffer↔pipe, cursor-preserving, echo-suppressed.
- Delta fix: `ENCODE_PATCH` (deltas carry only the change, not the whole doc).
- Desync handling: **version-vector reconciliation** (on connect / after-settle / 5s heartbeat).
- **Reconnection**: a dropped link no longer kills the session — persistent doc +
  stdin/stdout, re-dial with backoff, on-connect SYNC reconciles. QUIC liveness
  tuned (keep-alive 2s, idle 6s) for fast drop detection. (DESIGN.md §16)
- Module reorg, `.luarc.json` (silences nvim `vim` global errors), rename.

## TODO / next steps (rough priority)
1. **Merge PR #1** (`event-driven-pipe` → `main`).
2. **Phase 2 nvim bridge** — send granular `on_bytes` edits instead of whole-buffer
   snapshots (lighter, fixes the snapshot-vs-CRDT race; DESIGN.md §15/§16).
3. **File-mirror reconnection** — only `--pipe` reconnects today; the file-mirror
   path is still single-shot.
4. **CRDT-native tower defense demo** (DESIGN.md §13) — board as a shared text
   doc with per-region ownership (the guard). The lockstep `game.rs`/`play.rs`
   version exists but doesn't use eg-walker.
5. Verify **connect-anywhere** for real (two different networks / self-hosted
   iroh-relay) — only loopback has been tested. (DESIGN.md §14.1, n0 relay caveats)
6. Niceties: a README, `riftpipe.io`/`.sh` domain + landing, WASM-distributed
   rules (DESIGN.md §8), selective-sharing is explicitly deferred (DESIGN.md §14.3).

## Key gotchas / decisions to remember
- **Deltas must use ENCODE_PATCH**, not ENCODE_FULL (ENCODE_FULL embeds the whole
  doc → every "delta" shipped the full file).
- **`session` returns LinkClosed on any link error** (never a hard error) so the
  reconnect loop can re-dial; `StdinClosed` means the frontend quit → exit.
- **Bridge snapshot race** (DESIGN.md §16): typing in the window between merging a
  remote op and the bridge applying it can clobber — Phase 2 granular ops fixes it.
- **Lockstep is gone from text sync** (kept only in the game sim, where it's correct).
- iroh `presets::N0` relays are dev-only / metadata-exposing — fine for bootstrap,
  go-direct removes them from the data path; self-host a relay for production.
