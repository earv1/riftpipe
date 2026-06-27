# riftpipe kanban

A super-simple, file-backed kanban board. **SolidJS + Vite** frontend, a small
**Deno** server that reads/writes a board *directory* — and that's it. It has no
idea riftpipe exists: point [riftpipe](../../) at the board directory and the
board becomes live, peer-to-peer, end-to-end-encrypted, with zero changes here.

> This is a **separate project** from the riftpipe core (different stack, own
> tooling). They compose through the filesystem.

## The board is just files

```
board/
  board.md                       # columns (one per "- " line) + board title
  tickets/
    <id>/
      card.md                    # "# Title" + markdown description
      meta.toml                  # column, position, done   (structural fields)
      comments/<ts>-<author>.md  # one file per comment (planned)
      attachments/*              # arbitrary files (planned)
  events/<site>.jsonl            # append-only change log, one file per peer
  .site                          # this machine's site id (not synced)
```

### Change-event log (history)

Every mutation appends a line to `events/<site>.jsonl`. The trick: each replica
writes its **own** file (named by a per-machine site id in `.site`, a dotfile
riftpipe skips), so two peers never touch the same file — the log merges across
machines with **zero conflicts**. `GET /api/history` merges every
`events/*.jsonl` and sorts by time. Board files stay the source of truth; this is
a purely additive trail to build history/undo on later.

Why split prose (`card.md`) from structure (`meta.toml`): they sync differently
under riftpipe — prose merges (text CRDT), scalars are last-writer-wins (rsync).
See the planning & design docs in [`docs/`](docs/planned.md).

## Run it

```sh
# terminal 1 — the file API server (watches the board dir)
deno task api

# terminal 2 — the Vite dev server (HMR), proxies /api -> :8000
deno task dev
# open the URL Vite prints (http://localhost:5173)
```

Production-style (single process serving the built UI + API):
```sh
deno task build      # vite build -> dist/
deno task serve      # Deno serves dist/ + /api on :8000
```

Point at a different board dir with `KANBAN_DIR=/path/to/board`, and pick the
port with `KANBAN_PORT=8001`.

## Two-peer demo (one command)

```sh
./run-demo.sh
```
Builds the UI, spins up **two** kanban servers over two boards kept in sync by
riftpipe, and opens a browser window each:

- peer A → http://localhost:8000
- peer B → http://localhost:8001

Edit a card in one window and watch it appear in the other. Peer B starts empty
and fills in once the peers connect (a few seconds). Ctrl-C stops everything.

- `KANBAN_BROWSER=none ./run-demo.sh` — skip auto-opening; instead use VS Code's
  **Command Palette → "Simple Browser: Show"** with each URL (or the Ports panel
  preview) to keep both boards inside the editor.

## Make it collaborative (manually)

```sh
# machine A
riftpipe share ./board && KANBAN_DIR=./board deno task serve
# machine B
riftpipe join <ticket> ./board && KANBAN_DIR=./board KANBAN_PORT=8001 deno task serve
```
Both run the kanban over their local `board/`; riftpipe keeps the files converged.

## Status
Vertical slice: columns, add card, move (←/→), toggle done, live refresh, and a
per-peer change-event log (`/api/history`). Drag-and-drop, a card detail panel,
comments, attachments, and a history view are next.
