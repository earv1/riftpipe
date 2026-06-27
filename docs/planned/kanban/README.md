# Planned: P2P kanban (files as the database)

**Status:** planned (design only). A serverless, end-to-end-encrypted kanban board
two+ people edit live — the flagship use case for riftpipe.

## The big picture

```
 browser ─┐                                                   ┌─ browser
          │  localhost HTTP      riftpipe (iroh, e2e)          │  localhost HTTP
 vim ─────┼─ <board>/ dir ─────── folder sync ─────── <board>/ dir ─┼───── vim
          │  (card.md, meta.toml,                              │
 files ───┘   comments/, attachments/)                         └─── files
```

- **Source of truth = a directory tree.** The board is `board.md` + a folder per
  ticket (`card.md` prose, `meta.toml` structure, `comments/*.md`, `attachments/`).
  See [`data-model.md`](data-model.md).
- **No new database.** Each file type is bound to an existing `Syncer` (text-crdt
  for prose, rsync for structural/binary) via the manifest. The "db" is a
  *convention* over riftpipe's already-shipped folder sync — minimal new code.
- **riftpipe is the wrapper.** One process syncs the folder *and* serves a thin
  local web UI: `riftpipe kanban serve` / `connect`.
- **Frontends are interchangeable views** over the same files: the bundled web UI,
  a vim plugin, or just opening the markdown in any editor.

## Why files (not one markdown board file, not SQLite)

- **One big board file** breaks on the defining action — moving a card is a
  cross-region text edit, so concurrent moves duplicate/clobber. Splitting each
  card into its own files makes most edits touch *one small file*, and structural
  fields use last-writer-wins (rsync) so a move never corrupts.
- **SQLite** would make the `.db` the mergeable format only via cr-sqlite (a
  native extension) — and our own engine already has a CRDT+guard if we wanted
  that. Files need *zero* new sync code and stay inspectable/diffable.
  ([`../db-sync.md`](../db-sync.md) records the SQLite/cr-sqlite analysis we set
  aside.)

## Key properties

- **Comments are conflict-free by construction** — one file per comment; no two
  writers ever touch the same file.
- **Deletes → archive** (`column = "archived"`) because folder-delete sync isn't
  built yet (DESIGN §17 TODO).
- **Attachments just work** — drop a file in `attachments/`, rsync carries it.

## Docs

- [`data-model.md`](data-model.md) — the directory layout, per-file algorithm, ordering, conflict semantics.
- [`app-and-frontends.md`](app-and-frontends.md) — the `serve`/`connect` wrapper, HTTP surface, web UI, vim plugin.
- [`roadmap.md`](roadmap.md) — phased build (mostly wiring; little new sync code).
