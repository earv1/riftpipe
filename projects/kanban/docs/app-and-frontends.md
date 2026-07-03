# Planned: the kanban app & frontends

**Status:** the **Serve** layer (HTTP file-API + static UI host) is **implemented
in Rust** — `riftpipe kanban serve <board-dir> [--port 7777] [--dist <spa-dir>]`
(`src/app/kanban.rs`, `tiny_http`). It ports the Deno reference server route-for-route
(`/api/board`, `/api/cards/:id[/detail]`, POST `/api/cards`, PATCH
`/api/cards/:id`, POST `/api/cards/:id/comments`, SSE `/api/events`), serves the
built SolidJS SPA, and reads/writes the same on-disk model — verified end-to-end
against the existing UI bundle. **Remaining:** fold the **Sync** step (folder mode
over the same dir) into the command so one process serves *and* syncs, and
**Bundle** the SPA via `include_str!` (today it's served from `--dist`).

## Commands

```
riftpipe kanban serve   <board-dir> [--port 7777]        # share + serve; prints a ticket
riftpipe kanban connect <ticket> <board-dir> [--port 7777]  # join + serve
```

One process does three things:
1. **Sync** — folder mode (the §17 path) over `<board-dir>`, with the kanban
   manifest (`card.md`/`comments/*.md` → text-crdt, `meta.toml`/`attachments`
   → rsync). Reconnects on drop. Prints/loads the ticket like `share`/`join`.
2. **Serve** — a tiny HTTP server on `127.0.0.1:<port>` (loopback only).
3. **Bundle** — a single-file web UI embedded in the binary (`include_str!`), so
   there's nothing to install.

The browser talks **only to localhost**; the **files are the source of truth**;
riftpipe carries file changes to the peer:
`browser ⇄ localhost ⇄ <board-dir> ⇄ riftpipe ⇄ peer …` — no shared server,
e2e-encrypted.

## HTTP surface

The server reads/writes the board directory (it does not re-implement sync — it
just edits files, and the folder loop picks them up):

- `GET  /`                        → the embedded kanban page
- `GET  /api/board`               → board state JSON: columns (from `board.md`) +
  cards (folded from each ticket's `meta.toml` + `card.md` title), ordered
- `GET  /api/ticket?id=`          → one ticket (description, meta, comments)
- `POST /api/ticket`              → create a ticket folder (`card.md` + `meta.toml`)
- `PATCH /api/ticket?id=`         → edit fields → write `card.md` / `meta.toml`
  (a **move** is just `column`/`position` in `meta.toml`)
- `POST /api/comment?id=`         → write a new `comments/<ts>-<author>.md`
- `POST /api/attachment?id=`      → write into `attachments/`
- `GET  /api/poll?since=`         → long-poll; returns changed ticket ids when
  files change on disk (so a peer's merged edit re-renders)

Edits are **small, targeted file writes**, which keeps the §16 snapshot-race
window tiny (one card's file, not a whole board), and the CRDT/LWW per file means
nothing is lost on overlap.

Server impl: a small dep (e.g. `tiny_http`, synchronous) rather than hand-rolling
HTTP — only a handful of routes.

## The bundled web UI

A single self-contained HTML/JS file (no framework — truly bundled and hackable):
- fetch `/api/board`, render columns and cards;
- drag/click to move/edit/check → `PATCH /api/ticket`;
- ticket detail panel: description + comments + attachments;
- `/api/poll` to live-update from the peer.

**Your "plugins" live here** — vanilla-JS modules you drop in (filters, swimlanes,
a detail panel). The HTTP API is the stable contract; no host app to fight.

The detail panel's title/description box is a **plain `<textarea>` that writes the
whole `card.md`** (with clobber-safety) — deliberately *not* a homegrown
collaborative editor. See the principle below: better prose editing comes from
bridging a *real* editor, never from growing a CRDT textarea in the browser.

## Editor integrations (not a homegrown editor)

> **Principle: we will NEVER build our own text editor.** Description/comment
> editing always ties into an *existing* editor — Neovim via riftpipe's `--pipe`
> bridge, VS Code, or "open in `$EDITOR`". The file-backed data model is what
> makes this work: any editor that opens the file is a frontend. "Better editing"
> therefore always means *better editor integration* (bridges/plugins), never a
> homegrown editor or an in-app CRDT textarea.
> See [`planned.md`](planned.md) for the full statement and the OUT-OF-SCOPE note.

Because the board is just files, integrations come in layers — all reuse, no new
protocol:

- **Zero-effort (any editor):** open `<board-dir>/tickets/<id>/card.md` in vim,
  VS Code, or whatever `$EDITOR` is — riftpipe's existing folder sync already
  keeps it converged. Editing a description or adding a comment file Just Works.
- **Live Neovim bridge:** point `nvim/riftpipe.lua` at a card's `card.md` with
  `--pipe` (`RIFTPIPE_ARGS="share .../card.md --pipe"`). The buffer syncs
  char-by-char through the existing text-crdt session — the editor *is* the
  collaborative surface, so the app never reimplements one. The web UI can offer
  "open this card in Neovim" as a one-click handoff.
- **Board view (plugin):** render `board.md` + folded ticket summaries; commands
  `:KanbanMove`, `:KanbanAdd`, `:KanbanComment` that write the right files
  (`meta.toml` / a new `comments/*.md`). A thin Lua layer over the file
  convention — no new protocol.

Because every frontend reads/writes the *same* files, the web UI, Neovim, VS Code,
and a plain `$EDITOR` are interchangeable — open whichever; they converge.

## Notes

- **Multi-board** falls out of "a parent dir of board dirs" later; v1 is one
  board dir.
- **`--memory`** keeps the tree in RAM (memory backings) and surfaces resources
  in the `process` file (size+hash) — ephemeral boards.
- **Security** = riftpipe's: the ticket is the secret; loopback-only HTTP keeps
  the UI off the network.
