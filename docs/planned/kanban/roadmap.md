# Planned: kanban roadmap (files as the database)

**Status:** planned. The board is a directory tree synced by riftpipe's existing
folder mode — so most of this is *wiring and UI*, not new sync code. No native
deps, no new database engine.

Decision recap: each ticket lives at a **stable** `tickets/<id>/`; its column
(incl. `archived`) is a **field** in `meta.toml`. (Folders-as-columns was set
aside because path = sync identity, so moving a folder would duplicate the card —
see [`README.md`](README.md). Revisit only if move/delete sync gets built.)

## Phase 0 — done (today)
Folder sync with the multi-algorithm seam: `Syncer` trait, **text-crdt** +
**rsync**, file/memory backings, the `process` file, manifest (glob → `Kind`),
multiplexed reconnecting session. DESIGN §17.

## Phase 1 — the board convention
Define + validate the directory layout ([`data-model.md`](data-model.md)):
`board.md`, `tickets/<id>/{card.md, meta.toml, comments/*.md, attachments/*}`, and
the kanban `riftpipe.toml`. Mostly a spec + a small helper crate-side:
- a fractional-index helper for `position`;
- a tiny board model: scan `tickets/`, fold each `meta.toml` + `card.md` title
  into columns/cards, ordered by `position`.
- Test: two board dirs converge through folder sync (a move = an LWW edit to one
  `meta.toml`; a new comment = a new file → never conflicts).

No new sync algorithm required — it's the existing text-crdt + rsync via globs.

## Phase 2 — the app wrapper
`riftpipe kanban serve` / `connect` ([`app-and-frontends.md`](app-and-frontends.md)):
folder sync over the board dir + a loopback HTTP server + an embedded single-file
web UI. Routes: `/api/board`, `/api/ticket`, `/api/comment`, `/api/attachment`,
`/api/poll`. End-to-end: two browsers, drag a card, both converge.

## Phase 3 — frontends
- **vim plugin** — board view (folded ticket summaries) + `:KanbanMove` /
  `:KanbanAdd` / `:KanbanComment` writing the right files; reuses the
  `nvim/riftpipe.lua` bridge pattern. (And plain "open `card.md` in any editor"
  already works.)
- markdown interop: a `board.md` summary view is human-readable already.

## Phase 4 — niceties
- **`lww-record` Syncer** — per-field LWW for `meta.toml` so concurrent edits to
  *different* fields of the same card both survive (whole-file rsync loses one).
- attachments UX, labels/assignee/due fields, board search/filter (UI plugins).
- `--memory` boards (RAM backings + the `process` file).

## Later / optional
- **Folder move/delete sync** (track resources by stable id, not path). Generally
  useful (DESIGN §17 TODO); would also unlock folders-as-columns if ever wanted.
- Real card deletion + tombstone GC (until then, archive = `column="archived"`).

## Notes
- **Snapshot race is minimized**: edits are small per-card file writes, not
  whole-board rewrites; the CRDT/LWW per file means overlap never loses data.
- **No native build, no external db** — pure reuse of what Phase 0 shipped.
