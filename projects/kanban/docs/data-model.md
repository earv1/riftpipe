# Planned: kanban data model (files as the database)

**Status:** planned (design only). The board is a **directory tree** — no SQLite,
no new sync engine. Each file type is bound to a `Syncer` we already ship
(text-crdt / rsync) via the manifest, so the "database" is a *convention*, not
code. (For why not SQLite/cr-sqlite, see
[`db-sync.md`](../../../docs/planned/db-sync.md).)

## Layout

```
<board>/
  riftpipe.toml                  # the manifest (globs -> algorithm)
  board.md                       # columns: names + order, board title
  tickets/
    <ticket-id>/                 # one folder per ticket (card)
      card.md                    # title + markdown description (prose)
      meta.toml                  # structural fields: column, position, done, …
      comments/
        <ts>-<author>.md         # one file per comment
      attachments/
        <whatever>               # arbitrary attached files
  events/
    <site>.jsonl                 # append-only change log, one file per peer
  .site                          # this machine's site id (dotfile, NOT synced)
```

`<ticket-id>` is a client-generated stable id (e.g. `tk_8f3a`), so it never
collides on merge and a move/edit always refers to the same ticket.

## Why prose and structure are separate files

They want **opposite** merge behavior, and we already have both algorithms:

| File | Content | Algorithm | Why |
|---|---|---|---|
| `card.md` | title + description (prose) | **text-crdt** | concurrent edits *merge* char-level |
| `comments/*.md` | one comment each | **text-crdt** | + each is its own file → **conflict-free** |
| `meta.toml` | scalars: `column`, `position`, `done`, `labels` | **rsync** (LWW) | char-merging `column="doing"` vs `"todo"` = garbage; want last-writer-wins |
| `attachments/**` | binaries/opaque | **rsync** | efficient block transfer |
| `board.md` | column defs + order | **text-crdt** | rarely concurrent; lists merge |

Manifest:
```toml
default = "rsync-file"
[[rule]]
glob = "board.md"
algo = "text-crdt"
[[rule]]
glob = "**/card.md"
algo = "text-crdt"
[[rule]]
glob = "**/comments/*.md"
algo = "text-crdt"
[[rule]]
glob = "**/meta.toml"
algo = "rsync-file"
# attachments fall to the default (rsync-file)
```

## meta.toml

```toml
column   = "doing"     # which column this card is in (a "move" edits this)
position = "an"        # fractional index within the column (ordering)
done     = false
labels   = ["bug", "p1"]
assignee = "ann"
created  = "2026-06-27T10:00:00Z"
```

- **Move a card** → edit `column` (+ `position`) in *its* `meta.toml`. Different
  cards are different files → never conflict. The *same* card moved twice
  concurrently → rsync LWW picks one winner deterministically (no duplication —
  the failure mode a single board-text-file can't avoid).
- **Ordering** → `position` is a **fractional index**: a string that sorts
  lexicographically; insert between two cards = pick a key strictly between
  theirs (`"a"`,`"b"` → `"an"`); reorder touches only the moved card; ties break
  on ticket id. (Figma/Logoot/LSEQ style.)

## Card membership lives on the card, not the board

`board.md` only defines the **columns** (names + order); it does *not* list which
cards are in each column. A column's cards = "every ticket whose `meta.toml`
`column ==` this one, ordered by `position`." This keeps moves isolated to one
small file and keeps `board.md` quiet (low-conflict).

## Deletes → archive (deletes aren't synced yet)

riftpipe doesn't sync file/folder deletes yet (DESIGN §17 TODO). So "delete a
card" = set `column = "archived"` in `meta.toml` (a hidden lane), not `rm` the
folder. Sidesteps the gap and gives undo for free. Real deletion + tombstone GC
lands when folder-delete sync does.

## Conflict semantics users will see

- **Description / comments:** concurrent edits merge (text-crdt); nothing lost.
- **Different cards:** fully independent (different files) — no conflicts ever.
- **Same card, structural field:** last-writer-wins (rsync). Worst case, two
  people moving the *same* card at the *same* moment → one move wins; never a
  duplicate or corrupt value.
- **Comments:** never conflict (one file each).

## Change-event log (history) — IMPLEMENTED in the app

Every mutation also appends a line to `events/<site>.jsonl` (JSONL):

```json
{"ts":"2026-06-27T12:28:20.883Z","site":"d2c7cc76","kind":"card.create","id":"tk_…","column":"Todo","title":"…"}
{"ts":"…","site":"d2c7cc76","kind":"card.move","id":"tk_…","from":"Todo","to":"Doing"}
{"ts":"…","site":"d2c7cc76","kind":"card.check","id":"tk_…","done":true}
{"ts":"…","site":"d2c7cc76","kind":"card.edit","id":"tk_…","field":"title","value":"…"}
```

The board files stay the **source of truth**; the log is a **purely additive
audit trail** (history/undo later, not event-sourcing). It's conflict-free by the
same principle as comments: **one file per peer**, named by a machine-local
`site` id kept in the `.site` dotfile (which riftpipe's scan skips, so each
replica gets a distinct events file). `events/*.jsonl` syncs via text-crdt
(append-merge-friendly); "history" = every `events/*.jsonl` merged and sorted by
`ts`. Served at `GET /api/history`.

## Future nicety: an `lww-record` Syncer

`meta.toml` under whole-file rsync loses the *other* edit if two people change
*different* fields of the *same* card simultaneously (rare). A small future
`lww-record` Syncer would do **per-field** LWW (each key carries a lamport+site;
merge takes the max per key), so independent field edits both survive. Not needed
for v1 — noted so the upgrade path is clear.
