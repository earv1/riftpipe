# riftpipe kanban

A super-simple, file-backed kanban board. **SolidJS + Vite** frontend over a
tiny JSON file-API — and **there is no server**: `src/api.ts` calls
`kanbanHandle` from [`web/pkg`](../../web/), the Rust kanban handler compiled
to WebAssembly, running in the page. The board lives in the browser's private
filesystem (OPFS); peers sync directly over iroh / the gossip mesh. The app is
a static bundle plus a wasm payload.

Native machines participate through riftpipe's **generic verbs** (the binary
has no kanban code):

- `riftpipe connect <connection-id> ./board` — sync a browser peer's board
  into a real on-disk directory (edit `card.md` in vim, it converges);
- `riftpipe serve ./dist` — statically host the built app (or any dir) with
  live SSE change events.

(Earlier server implementations — a Deno reference server and a Rust
`kanban-server` — have been removed: the wasm payload *is* the backend.)

> This is a **separate project** from the riftpipe core (different stack, own
> tooling). They compose through the filesystem (or OPFS in the browser).

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
machines with **zero conflicts**. Board files stay the source of truth; this is
a purely additive trail to build history/undo on later. *(The retired servers
wrote this log; moving it into the wasm handler is tracked in
[`docs/planned.md`](docs/planned.md).)*

Why split prose (`card.md`) from structure (`meta.toml`): they sync differently
under riftpipe — prose merges (text CRDT), scalars are last-writer-wins (rsync).
See the planning & design docs in [`docs/`](docs/planned.md).

## Run it

```sh
# build the wasm payload once (from web/), then the UI dev server
(cd ../../web && wasm-pack build --target web)
deno task dev
# open the URL Vite prints (http://localhost:5173) — the API runs in-page
```

Production-style (a static bundle — host it anywhere):
```sh
deno task build                    # vite build -> dist/ (bundles the wasm)
riftpipe serve ./dist              # …or any static host / GitHub Pages
```

## Two-browser demo

Open the app, share the link in the header (it carries the iroh ticket), and
open it in a second browser/machine — both converge over the gossip mesh, no
server anywhere. Scripted versions live in [`e2e/`](e2e/)
(`run-iroh.sh`, `run-iroh-mesh.sh` for three browsers).

## Bring it to disk (vim, scripts, native tools)

```sh
riftpipe connect <connection-id> ./board   # the board materializes as files
$EDITOR board/tickets/<id>/card.md         # edit; it converges back to the browser
```
`riftpipe share ./board` / `join <ticket> ./board` similarly keep two on-disk
copies converged (folder mode) — the board is just files either way.

## Status
Vertical slice: columns, add card, move (←/→), toggle done, live refresh —
running fully in-browser (wasm + OPFS + gossip mesh), deployed on GitHub Pages.
Drag-and-drop, comments UI, attachments, and a history view are next
([`docs/planned.md`](docs/planned.md)).
