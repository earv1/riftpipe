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
- Current state: everything is on **`main`** (PR #2 merged 2026-11-22:
  protocol versioning v4, mesh sealing, signed writes + trust log, the browser
  claim/request-to-join flow, the generic file API, the synced event log,
  epoch-sealed content + read revocation, per-board OPFS, folder deletes —
  see `What's done`). New work goes on feature branches + PRs.
- **Guardrail:** a hook blocks committing/pushing to `main` (and any command
  containing the word "main") — the user pushes branches and merges PRs. Claude
  does not commit on `main`; current work is left in the working tree.
- Local dir renamed to `riftpipe/` (this file moved with it).

## How to build / run
```sh
cargo test                  # full workspace suite (riftpipe + core), 0 warnings
cargo run -- text           # offline: eg-walker convergence demo
./run-local.sh              # two-peer tmux demo: nvim + bridge, live char sync, metrics panes
```
CLI (all generic — no app verbs, see `agent.md`):
`riftpipe share <file|dir> [--pipe] [--metrics <path>]` ·
`riftpipe join <ticket> <file|dir> [--pipe]` ·
`riftpipe connect <connection-id> <dir> [--signal ws://…]` (tree sync with a
core-protocol peer, e.g. a browser board) ·
`riftpipe connect … <dir> --restricted [--name <text>]` (a restricted board;
a keyless joiner auto-asks for access) ·
`riftpipe requests|approve|members <dir>` (the running restricted peer's
pending requests / approve by safety code or id / member list) ·
`riftpipe serve <dir> [--port]` (generic static host + SSE change events) ·
`riftpipe signal [--port]`. The kanban app has **no server** — its board
domain is TypeScript in the page over riftpipe's wasm file API;
`riftpipe serve` hosts the static bundle.

nvim bridge (session-local, no install):
```sh
RIFTPIPE_BIN=./target/debug/riftpipe RIFTPIPE_ARGS="share /path/file --pipe" \
  nvim -c 'luafile nvim/riftpipe.lua' /path/file
```

## Module map
Crates: `core/` (riftpipe-core: pure, wasm-safe — text CRDT, tree sync
protocol, wal frames, crypto/identity/keywrap/onboard/passkey), `src/`
(native binary), `web/` (riftpipe-web: app-generic wasm/browser — WebRTC/iroh
links, gossip mesh, `opfs`, `tree_sync`, the `file` API, the `events` log;
workspace-excluded), `projects/kanban/wasm/` (kanban-wasm: a thin passthrough
that links riftpipe-web into the one app bundle — no kanban Rust; the board
domain is TypeScript in `projects/kanban/src/board.ts`). See
`docs/architecture.md` for the diagram.

src/:
- `net/`   — link (Link trait + mock + counters), transport (iroh), webrtc
  (data-plane + upgrade + browser signaling client), negotiate (capability
  ladder), secure (ticket + auth)
- `crdt/`  — re-export shim over `riftpipe_core::text` (eg-walker document,
  diamond-types): diff-to-ops, delta encode/merge, version vectors
- `sync/`  — sync_full (the shared full-state driver), pipe (the `--pipe`
  protocol: event-driven session, reconciliation, reconnection), mirror
  (file-mirror loop), folder (multiplexed reconnecting folder session), tree
  (the riftpipe_core::sync file-tree driver — wire-compatible with browser peers)
  - `strategy` — the `SyncStrategy` adapter trait + `Kind` (the Strategy seam, DESIGN §17)
  - `algo/`  — concrete algorithms: text_crdt, rsync, wal (real; wal's core
    primitive is `riftpipe_core::wal`), lww_record (per-key LWW), image (stub)
  - `backing` — where bytes live: FileBacking vs MemoryBacking + MemoryRegistry (§17.5)
  - `manifest`/`workspace` — riftpipe.toml glob→Kind rules; a folder of resources
- `monitor/` — metrics (one-line status to a file for tmux; connection-kind detection), process (the in-memory `process` sidecar: size+hash for all RAM resources, §17.6)
- `app/`   — generic runnables: host (static dir hosting + SSE change events —
  `riftpipe serve`, also consumable as a library) and signal (WebSocket
  signaling relay). No app servers anywhere: the kanban app's backend is its
  TypeScript domain over the wasm file API, in the page.

## What's done
- **Folder sync — CLI-wired (DESIGN §17, local on `main`):** `share <dir>` /
  `join <ticket> <dir>` sync a whole tree; each file gets the algorithm
  `riftpipe.toml` assigns (glob → `Kind`). Multiplexed reconnecting session
  (`sync::folder`, resource-id framing, REQ/REP advertise, poll-based local
  change detection, peer-driven discovery). `--memory` (RAM backing) + `--process
  <path>` (size+hash of all in-memory resources). **Verified end-to-end over
  loopback**: text-crdt + rsync files sync into nested subdirs, live edits
  converge, memory/process file works. 46 tests green, 0 warnings.
- **The adapter seam:** `SyncStrategy` trait + `Kind` factory; **rsync**
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
- **July 2026 restructure + review series** (this branch): `app/` split from
  plumbing; transport-blind drivers via `net::{Sink, Source}`; `SyncStrategy`
  rename (no more `Syncer` collision with `riftpipe_core::sync::Syncer`); board
  sync rewritten as the generic `sync::tree` select!-loop driver (dotfile leak
  + lock-across-await fixed, blake3 `seen` map, degrade-to-poll, first tests
  over mock halves); negotiation consolidated (`NegotiatedSession` /
  `negotiate_link`, one policy, the binary's divergent copy deleted); **kanban
  fully extracted from the binary** (generic `connect`/`serve` verbs,
  `app::host`) and then **all kanban servers removed** (Deno + the brief
  `kanban-server` crate) — the wasm payload is the backend;
  `docs/architecture.md` with validated mermaid diagrams; `agent.md` with the
  durable working rules.
- **Protocol + security series** (branch `protocol-versioning`, July 2026):
  protocol version announcements (`PROTOCOL_VERSION`, mismatch surfaced on
  stderr / `riftpipe:protocol-mismatch`); mesh sealing (secret-derived topic +
  AEAD frames); signed writes + trust-log enforcement (`--restricted`,
  `--grant`, `device-id`); `core::keywrap`/`onboard`/`passkey` lib backends;
  the browser on signed boards with a **claim / request-to-join / approve** UI
  (`MeshMsg::Request`). Then **"filify the API"**: the wasm bundle is a
  generic file library (`readFile`/`writeFile`/`listDir`,
  `riftpipe:file-changed`), the kanban domain moved to TypeScript
  (`board.ts`/`fs.ts`) and `kanban-wasm`'s `format`/`handler` Rust was
  deleted; plus a generic synced **event log** (`logEvent`/`readEvents`,
  `events/<device-id>.jsonl`, `riftpipe:event`), now rendered by the kanban
  Activity panel (board-wide) and per-card History (branch `history-view`):
  segment rotation at 16 KiB, a per-tab writer queue, coalesced
  `mesh.merge`/`card.edit` logging, and reserved `ts`/`device`/`kind` fields.
  Details: `docs/planned/roadmap.md` §Security and items 8/17.
- **Epoch-key mesh seal** (branch `epoch-seal`, July 2026): restricted/claimed
  boards now seal content under a rotating epoch key (`core::epoch`,
  `PROTOCOL_VERSION` 4) — read revocation by rotation, not just write-revoke.
  `riftpipe revoke|grant <dir> <device-id>` drives it live on a running CLI
  peer; the kanban owner gets a members list with "Remove". E2E:
  `run-epoch-revoke.sh` (native), `run-browser-auth.sh` /
  `e2e/epoch-revoke.mjs` (browser). Details: `docs/planned/roadmap.md`
  §Security.
- **CLI request/approve parity** (branch `cli-request-approve`): a keyless
  `connect … --restricted [--name]` joiner auto-broadcasts a signed join
  request (re-announced every 15 s); any restricted peer keeps a request list
  and writes `.riftpipe-auth.json`; `riftpipe requests|members <dir> [--json]`
  read it and `riftpipe approve <dir> <safety-code|device-id>` grants + wraps
  through `.riftpipe-control`. Interoperates with the browser both ways. E2E:
  `run-request-approve-native.sh`, `run-request-approve-browser.sh` /
  `e2e/request-approve.mjs`; npm `requests`/`approve`/`members`. Details:
  `docs/planned/roadmap.md` §Security ("CLI request/approve").
- **Passkey ceremony** (branch `passkey-ceremony`): claim/add/link with a
  passkey, verified offline (`core::passkey`, `TrustEntry::Passkey`/
  `PasskeyLink`, `web/src/passkey.rs`); e2e `passkey-link.mjs` in
  `run-browser-auth.sh`; `PROTOCOL_VERSION` 5. Details: `SECURITY.md`
  §"Passkey device linking".
- **Live reconnection** (branch `live-reconnect`): a reloaded/restarted host or
  joiner rejoins the same board and both sides exchange their gap edits —
  durable sync records (`.riftpipe-sync/`), one reused page `gossip::Node`,
  backoff rejoin + per-board peer book, Hello on NeighborUp, a persisted CLI
  host identity (`<dir>/.riftpipe-host`, `--new-board`), and a status pill
  (`meshStatus()` / `riftpipe:mesh-status`). Details: `docs/planned/roadmap.md`
  §Networking ("Live reconnection — done"); e2e `run-reconnect.sh`,
  `run-cli-host-restart.sh`.
- **Mesh anti-entropy** (branch `mesh-anti-entropy`): periodic neighbor
  `Hello`s repair any dropped gossip broadcast (Trickle-style backoff, silent
  when neighbors agree), Hello replies go swarm-wide (plumtree lazy peers miss
  neighbors-only ones), one ordered outbound queue per driver, `meshStats()`.
  No wire change. Details + follow-ups: `docs/planned/roadmap.md` item 21; e2e
  gate `projects/kanban/e2e/run-repeat.sh run-iroh-mesh-n.sh 10 9`.

## Planned (design docs)
- **P2P kanban — files as the database** — see [`docs/planned/`](docs/planned/).
  Decision (June 2026): the board is a **directory tree** synced by the existing
  folder mode — `board.md` + `tickets/<id>/{card.md, meta.toml, comments/*.md,
  attachments/*}`. Each file gets the right `SyncStrategy` via the manifest (card/
  comments → text-crdt; `meta.toml`/attachments → rsync). Ticket folders are
  **stable**; column (incl. `archived`) is a **field** in `meta.toml` (path = sync
  identity, so cards don't move between folders). The app composes with generic
  riftpipe verbs (`riftpipe connect`, `riftpipe serve`) — there is no server,
  the page's TypeScript (`board.ts`) reads/writes the files via the wasm file
  API; vim plugin + any markdown editor
  are interchangeable views. **No new sync engine, no native deps** — mostly wiring
  + UI. SQLite/cr-sqlite was considered and set aside (see
  `docs/planned/db-sync.md`).

## TODO / next steps (rough priority)
Folder sync (DESIGN.md §17) is CLI-wired with text-crdt + rsync. Next:
1. ~~Finish `wal-db`~~ — *done:* core primitive + the real `Kind::WalDb`
   folder-mode adapter (postcard-framed frames, push + reconcile, linearized
   materialization). Remaining design nuance lives in the roadmap.
2. **Implement `image`** (tile merge) — eg-walker is the wrong granularity for
   pixels.
3. **Per-resource backing in the manifest** — choose memory vs file per glob
   (today `--memory` is all-or-nothing).
4. **Phase 2 nvim bridge** — granular `on_bytes` edits (DESIGN §15/§16).
5. Robustness: rsync v1 caveat (Copy references advertiser's blocks — stale-basis
   reconstruction is rejected by hash + retried; consider basis-pinning).
   Folder deletes now sync (tombstones, edit-wins, `.riftpipe-folder-index`) —
   see `docs/planned/roadmap.md` §Smaller ("Folder deletes sync");
   browser/kanban (`sync::tree`/`sync::mesh`) deletes are the remaining
   follow-up there ("tree/mesh (browser) deletes").
6. **Architecture/hygiene backlog** — see `docs/planned/roadmap.md` §"Architecture
   / hygiene": all items done (kanban out of the riftpipe crates — and, since
   "filify", no kanban Rust at all; one wire protocol; `connect` over iroh +
   negotiation; `web/Cargo.lock` tracked; signaling demoted; real-relay test
   de-flaked).
7. ~~**Auth follow-through**~~ (roadmap §Security) — *done:* the mesh seal now
   runs on epoch keys, read revocation by rotation, and
   `riftpipe revoke|grant <dir> <id>` on the native CLI (see `What's done`).
   The WebAuthn browser ceremony shipped too (passkey device-linking).
   Remaining: passkey revocation; the QR / short-code link variant; the CLI
   has no passkey ceremony (it only verifies).
8. ~~**History view**~~ — *done:* `readEvents()` is rendered by the kanban
   board-wide Activity panel and per-card History (branch `history-view`),
   verified end-to-end over the real sealed mesh by
   `projects/kanban/e2e/run-history.sh`.
9. **Mesh anti-entropy follow-ups** — roadmap item 21 *Follow-ups* (compact
   digest, trust-log/epoch anti-entropy, dropping the NeighborUp `full_state`
   flood, removing the event log's segment re-push).
Older/parallel: file-mirror reconnection;
verify connect-anywhere on real networks (§14.1); verify reconnection on real
networks (see roadmap, "Live reconnection — done"); domain + landing.

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
