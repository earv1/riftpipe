# riftpipe

**Open a rift, pipe through it.** Live, peer-to-peer sync for text and files —
no server, no account, end-to-end encrypted. Two machines converge on the same
content; edit either side and both stay in sync.

riftpipe is a Unix-like terminal construct: it speaks tickets and pipes, not
logins and clouds. Under the hood it's an [eg-walker text
CRDT](https://github.com/josephg/diamond-types) and an rsync block-diff carried
over [iroh](https://www.iroh.computer/) QUIC (direct when it can hole-punch,
relayed only to bootstrap), each link authenticated and encrypted.

```sh
# machine A — share a folder; prints a ticket
riftpipe share ./project

# machine B — join with the ticket; the folder converges, live
riftpipe join <ticket> ./project
```

---

## What it can do

### 1. Collaborative editing in *your* editor (not a browser)
Edit the same document from two machines, char-by-char, with a real CRDT doing
the merge — concurrent edits never clobber each other. A Neovim bridge ships in
`nvim/riftpipe.lua`; any editor can drive it because the boundary is a tiny
line-delimited JSON protocol on stdin/stdout (`--pipe`). Think serverless,
self-hosted, end-to-end-encrypted collaborative editing for code and prose.

### 2. Keep a project folder in sync across your own machines
`share`/`join` a **directory** and riftpipe syncs the whole tree, picking the
right algorithm per file from `riftpipe.toml`:
- text & code (`*.md`, `*.txt`, …) merge with the **text CRDT** — edit both
  sides at once, nothing is lost;
- binaries/assets sync with **rsync** block-diff — only changed blocks go over
  the wire (last-writer-wins per file).

A laptop↔desktop live mirror with no Dropbox, no cloud, no daemon account.

### 3. Share a scratch directory with a teammate, peer-to-peer
Hand someone a ticket and you're both editing the same folder over an encrypted
direct link. Nothing touches a third party; when you both quit, it's gone. Great
for a quick shared workspace, a pairing session, or moving a tree between boxes
behind different NATs.

### 4. Sync that never touches disk (`--memory`)
Hold resources in RAM instead of mirroring to files. The bytes live only in
process memory; a single `--process` file reports each resource's **size + hash**
(no contents) so you can watch what's flowing without persisting it. Useful for
ephemeral data, secrets you don't want written to disk, or a pure in-RAM pipe
between two machines.

### 5. Survive flaky links
A dropped connection doesn't end the session: the document persists, riftpipe
re-dials with backoff, and an on-connect reconciliation (compact version-vector /
block-signature exchange) heals whatever was missed. Edits made during the gap
queue up and apply on reconnect.

### 6. Drive it from scripts (headless)
The `--pipe` protocol is just JSON lines — `{"op":"insert","pos":N,"text":"…"}`
in, remote edits out. Any program (a test, a bot, a build tool) can be a peer.

### 7. Run a P2P kanban — files *are* the database (no DB, no cloud)
A serverless, end-to-end-encrypted kanban board. The board **is a directory tree**
— `board.md` plus a folder per ticket (`card.md` prose, `meta.toml` structure,
`comments/*.md`) — synced by folder mode: prose merges with the text CRDT,
structure is last-writer-wins per file, so concurrent edits and card moves don't
clobber. One Rust binary serves the bundled web UI *and* syncs the files:
```sh
riftpipe kanban serve ./board --port 7777
```
No database, no account — the board stays human-editable and git-friendly.

### 8. …or run the whole app **in a browser**, no local server
The same core compiles to WebAssembly (`riftpipe-core` + the `web/` crate): the
eg-walker CRDT runs in the page, the board persists to the browser's private
filesystem (**OPFS**), and the JSON API the UI calls is handled by wasm — so
there's no localhost process, just a static bundle. Two browsers connect
**peer-to-peer over WebRTC by sharing a link**: the connection id lives in the URL
hash, a tiny content-blind signaling server (`riftpipe signal`) pairs the room,
and data then flows direct. *Verified headlessly in real Chrome* — CRDT
convergence, OPFS persistence, and link-based connection all pass; wiring the
board's per-file sync over that link is the active next step.

### 9. Embed the CRDT in your own web app
`riftpipe-core` ships as a wasm package (`wasm-pack build`): `import { RiftDoc }`
and you have an eg-walker document — `editTo` / `delta` / `merge` / `content`,
plus `persist` / `load` to OPFS — the *same* CRDT the native CLI runs. Snapshot
in, deltas out; converge with any peer over the link of your choice.

### Planned
- **`wal-db`** — append-only write-ahead-log replication for databases (sync
  *state* separately from a *view*, e.g. a game's authoritative state vs. its
  rendered board).
- **`image`** — codec-aware tile/region merge for editing pictures live.
- Granular editor edits, per-resource memory/file choice, folder deletes.

---

## Install & run

Requires a recent Rust toolchain.

```sh
cargo build --release          # binary at target/release/riftpipe
cargo test                     # full suite

# offline demos (no networking)
cargo run -- text              # eg-walker text convergence
cargo run -- simulate          # deterministic ruled-replay engine
cargo run -- td                # tower-defense core preview

# two-peer tmux demo: nvim + bridge, live char sync, metrics panes
./run-local.sh
```

### Single file (live collaborative edit)
```sh
riftpipe share notes.md        # prints a ticket (also written to notes.md.ticket)
riftpipe join <ticket> notes.md
# edit notes.md in your $EDITOR on either side; changes converge
```

### Neovim bridge (live, char-level)
```sh
RIFTPIPE_BIN=./target/release/riftpipe \
RIFTPIPE_ARGS="share /path/file --pipe" \
  nvim -c 'luafile nvim/riftpipe.lua' /path/file
```

### Folder
```sh
riftpipe share ./project                 # uses ./project/riftpipe.toml if present
riftpipe join <ticket> ./project
```

`riftpipe.toml` (at the folder root, or pass `--manifest`) maps globs to
algorithms — see the example at the repo root:
```toml
default = "rsync-file"
[[rule]]
glob = "**/*.md"
algo = "text-crdt"
```

### Kanban + browser
```sh
# native: serve the kanban UI + JSON file-API over a board directory
riftpipe kanban serve ./board --port 7777

# the connection broker for browser peers (self-hosted, content-blind)
riftpipe signal --port 9000

# build the browser (wasm) package and verify it headlessly in real Chrome
cd web && wasm-pack build --target web && ./test-headless.sh

# mint a shareable connection link (the id is the room two peers join)
./web/share-link.sh https://your-host    # -> https://your-host/#<id>
```
The shared layout lives in `riftpipe-core::kanban`, so a board is byte-for-byte
portable between the native server and the browser build.

### Useful flags
| flag | meaning |
|------|---------|
| `--pipe` | speak the editor edit-stream protocol on stdin/stdout (for bridges) |
| `--memory` | hold resources in RAM, no disk mirror |
| `--process <path>` | write size+hash of all in-memory resources to a file |
| `--manifest <path>` | manifest location (default `<dir>/riftpipe.toml`) |
| `--metrics <path>` | write a one-line status (connection, bytes, rate) for tmux |

---

## How it works (short version)

- **Transport** — every link is an abstraction (`Link`), so the wire underneath is
  swappable and negotiated at connect time (a capability ladder: WebRTC-direct →
  iroh-direct → iroh-relay):
  - **native** — iroh QUIC: dialable tickets, NAT hole-punching, direct when it
    can, relay only to bootstrap; authenticated from the ticket secret, e2e
    encrypted.
  - **browser** — WebRTC data channels, brokered by a tiny content-blind signaling
    server (`riftpipe signal`) keyed on a connection id shared in a link; data
    flows direct after the handshake.
- **The seam** — each resource is synced by a pluggable `Syncer` (a *Strategy*):
  advertise what you have, answer "what are you missing", merge an opaque delta.
  Today's strategies are `text-crdt` (diamond-types eg-walker) and `rsync-file`
  (rolling weak checksum + blake3 strong hash + token-stream diff, last-writer-
  wins so bidirectional reconcile converges).
- **Folders** — one link multiplexes many resources, each frame tagged with its
  path; unknown paths are discovered from the peer.

The full design, decisions, and trade-offs live in **[DESIGN.md](DESIGN.md)**;
status and resume notes in **[PROJECT.md](PROJECT.md)**.

## Security model

End-to-end encrypted; the secret rides in the ticket, so **anyone with the
ticket can join** — treat it like a password and share it over a trusted
channel. iroh's default relays are convenient for bootstrapping but see your
metadata; a direct connection removes them from the data path, and you can
self-host a relay for production.

## Status

Early/experimental (`v0.0.0`), developed against loopback and local networks.
Working: text CRDT, rsync, folder sync, in-memory mode, reconnection, the nvim
bridge, the native kanban server, and the browser stack — the wasm CRDT, OPFS
persistence, WebRTC + link-based signaling, the in-browser kanban handler, **and
per-file board sync over the link** (two browsers converge card prose as a CRDT
and structural moves as LWW). All verified headlessly in real Chrome, and the full
**two-real-browser loop is verified end-to-end with Playwright** (a card created
in one browser's UI appears in the other, no server in the data path). The SolidJS
app builds as a self-contained static bundle. The **browser↔native bridge** works
too — and **board sync between a native peer and a browser is bidirectional**: a
card made in the browser UI lands on the native peer's disk, and editing the
native `card.md` in any editor updates the browser (`riftpipe kanban connect`,
shared `riftpipe_core::sync`; verified end-to-end with Playwright). `wal-db`/`image`
are stubs. The one thing **not** yet proven is real two-machine, **cross-NAT**
connectivity — it needs two boxes on different networks and the env-configurable
STUN/TURN relay (a hardware/network requirement, not a code gap).
