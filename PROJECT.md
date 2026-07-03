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
- **PR #1 (`event-driven-pipe`) merged into `main`** (fast-forward). Now working
  **locally on `main`** — uncommitted folder-sync scaffolding lives in the tree.
- **Guardrail:** a hook blocks committing/pushing to `main` (and any command
  containing the word "main") — the user pushes branches and merges PRs. Claude
  does not commit on `main`; current work is left in the working tree.
- Local dir renamed to `riftpipe/` (this file moved with it).

## How to build / run
```sh
cargo test                  # full suite (currently 21 passing, 0 warnings)
cargo run -- text           # offline: eg-walker convergence demo
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
  - `syncer` — the `Syncer` adapter trait + `Kind` (the Strategy seam, DESIGN §17)
  - `algo/`  — concrete algorithms: text_crdt (real), rsync (real), wal/image (stubs)
  - `backing` — where bytes live: FileBacking vs MemoryBacking + MemoryRegistry (§17.5)
- `monitor/` — metrics (one-line status to a file for tmux; connection-kind detection), process (the in-memory `process` sidecar: size+hash for all RAM resources, §17.6)

## What's done
- **Folder sync — CLI-wired (DESIGN §17, local on `main`):** `share <dir>` /
  `join <ticket> <dir>` sync a whole tree; each file gets the algorithm
  `riftpipe.toml` assigns (glob → `Kind`). Multiplexed reconnecting session
  (`sync::folder`, resource-id framing, REQ/REP advertise, poll-based local
  change detection, peer-driven discovery). `--memory` (RAM backing) + `--process
  <path>` (size+hash of all in-memory resources). **Verified end-to-end over
  loopback**: text-crdt + rsync files sync into nested subdirs, live edits
  converge, memory/process file works. 46 tests green, 0 warnings.
- **The adapter seam:** `Syncer` trait (Strategy) + `Kind` factory; **rsync**
  (rolling weak + blake3 strong checksums, postcard wire, LWW `(version,hash)`
  convergence); text-crdt adapter; file/memory backings + `MemoryRegistry`.
  Example `riftpipe.toml` at repo root.
- Event-driven `--pipe` (split link halves + `select!` loop; **idle = silent**).
- Neovim bridge (`nvim/riftpipe.lua`): buffer↔pipe, cursor-preserving, echo-suppressed.
- Delta fix: `ENCODE_PATCH` (deltas carry only the change, not the whole doc).
- Desync handling: **version-vector reconciliation** (on connect / after-settle / 5s heartbeat).
- **Reconnection**: a dropped link no longer kills the session — persistent doc +
  stdin/stdout, re-dial with backoff, on-connect SYNC reconciles. QUIC liveness
  tuned (keep-alive 2s, idle 6s) for fast drop detection. (DESIGN.md §16)
- Module reorg, `.luarc.json` (silences nvim `vim` global errors), rename.

## Planned (design docs)
- **P2P kanban — files as the database** — see [`docs/planned/`](docs/planned/).
  Decision (June 2026): the board is a **directory tree** synced by the existing
  folder mode — `board.md` + `tickets/<id>/{card.md, meta.toml, comments/*.md,
  attachments/*}`. Each file gets the right `Syncer` via the manifest (card/
  comments → text-crdt; `meta.toml`/attachments → rsync). Ticket folders are
  **stable**; column (incl. `archived`) is a **field** in `meta.toml` (path = sync
  identity, so cards don't move between folders). riftpipe is the wrapper
  (`kanban serve`/`connect`) serving a bundled web UI; vim plugin + any markdown
  editor are interchangeable views. **No new sync engine, no native deps** —
  mostly wiring + UI. SQLite/cr-sqlite was considered and set aside (see
  `docs/planned/db-sync.md`).

## TODO / next steps (rough priority)
Folder sync (DESIGN.md §17) is CLI-wired with text-crdt + rsync. Next:
1. **Implement `wal-db`** (append-only frames) — sync *state* (the WAL) separately
   from a *view* (e.g. a DB's authoritative log vs. a rendered projection).
2. **Implement `image`** (tile merge) — eg-walker is the wrong granularity for
   pixels.
3. **Per-resource backing in the manifest** — choose memory vs file per glob
   (today `--memory` is all-or-nothing).
4. **Phase 2 nvim bridge** — granular `on_bytes` edits (DESIGN §15/§16).
5. Robustness: rsync v1 caveat (Copy references advertiser's blocks — stale-basis
   reconstruction is rejected by hash + retried; consider basis-pinning); folder
   deletes aren't synced yet (only creates/edits).
Older/parallel: file-mirror reconnection;
verify connect-anywhere on real networks (§14.1); domain + landing.

## Key gotchas / decisions to remember
- **Deltas must use ENCODE_PATCH**, not ENCODE_FULL (ENCODE_FULL embeds the whole
  doc → every "delta" shipped the full file).
- **`session` returns LinkClosed on any link error** (never a hard error) so the
  reconnect loop can re-dial; `StdinClosed` means the frontend quit → exit.
- **Bridge snapshot race** (DESIGN.md §16): typing in the window between merging a
  remote op and the bridge applying it can clobber — Phase 2 granular ops fixes it.
- **Lockstep is gone from text sync.**
- iroh `presets::N0` relays are dev-only / metadata-exposing — fine for bootstrap,
  go-direct removes them from the data path; self-host a relay for production.
