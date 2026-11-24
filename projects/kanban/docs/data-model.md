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
    <device-id>.jsonl            # append-only event log, one file per device
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

## Deletes

"Delete card" is a real delete of `tickets/<id>/` through the file API's
`deleteFile`, which syncs a tombstone to every peer (riftpipe tree/mesh deletes,
protocol v6). Order matters: `card.md` goes **first**, then every other file
(comments and anything else under the ticket), `meta.toml` **last**. A ticket without `card.md` never
renders, so no peer ever shows a half-deleted card while the rest of the delete
is in flight. A single comment can be deleted the same way
(`comments/<ts>__<author>.md`). A concurrent edit beats a delete: a peer that
changed a file of the card without having seen that content keeps its copy
and sends it back, so the edited file survives on every peer rather than the
edit being lost. If only `meta.toml` or a comment was edited, the card still
doesn't render (it has no `card.md`), so the board lists that ticket as
**leftovers** ("Deleted card … left files behind") with a **Remove leftovers**
button, which deletes the rest of `tickets/<id>/` everywhere (a no-op if
`card.md` has come back). This is never swept automatically: a card that is
still arriving (its `card.md` not synced yet) looks the same, and an automatic
cascade would also undo the concurrent edit that just won. Deleting a card or
a comment asks for confirmation first.
There is no archive lane or undo.

## Conflict semantics users will see

- **Description / comments:** concurrent edits merge (text-crdt); nothing lost.
- **Different cards:** fully independent (different files) — no conflicts ever.
- **Same card, structural field:** last-writer-wins (rsync). Worst case, two
  people moving the *same* card at the *same* moment → one move wins; never a
  duplicate or corrupt value.
- **Comments:** never conflict (one file each).

## Change-event log (history) — IMPLEMENTED in the app

Every mutation also appends a line to `events/<device-id>.jsonl` (JSONL):

```json
{"ts":"2026-06-27T12:28:20.883Z","device":"<64-hex>","kind":"card.create","id":"tk_…","column":"Todo","title":"…"}
{"ts":"…","device":"<64-hex>","kind":"card.move","id":"tk_…","from":"Todo","to":"Doing"}
{"ts":"…","device":"<64-hex>","kind":"card.check","id":"tk_…","done":true}
{"ts":"…","device":"<64-hex>","kind":"card.edit","id":"tk_…","field":"title","value":"…"}
```

The board files stay the **source of truth**; the log is a **purely additive
audit trail** (history/undo later, not event-sourcing). It's conflict-free by the
same principle as comments: **one file per device**, named by the device's
signing id. The log itself is a generic riftpipe-web primitive
(`logEvent`/`readEvents`, live `riftpipe:event`); `board.ts` logs the app's
semantic events (above, plus `comment.add`) and the library adds `mesh.merge`
and `auth.*` lines. `events/*.jsonl` syncs as LWW (one writer per file, so
whole-file newest-wins converges); "history" = `readEvents()`, every
`events/*.jsonl` merged and sorted by `ts`. No UI renders it yet.

## Future nicety: an `lww-record` Syncer

`meta.toml` under whole-file rsync loses the *other* edit if two people change
*different* fields of the *same* card simultaneously (rare). A small future
`lww-record` Syncer would do **per-field** LWW (each key carries a lamport+site;
merge takes the max per key), so independent field edits both survive. Not needed
for v1 — noted so the upgrade path is clear.
