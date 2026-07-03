# Kanban — planning, roadmap & TODO

The authoritative planning doc for the **riftpipe kanban** project (this is a
*separate* project from the riftpipe core — SolidJS UI over a file-backed
board, deployed as an **in-browser wasm app**; made collaborative by riftpipe).
It composes with riftpipe purely through the filesystem (OPFS in the browser).
There is **no server** — the wasm payload handles the API in-page — and the
riftpipe binary has **no kanban code** (see `agent.md`): native machines use
the generic verbs (`riftpipe connect` / `share` / `join` / `serve`). The
earlier Deno reference server and the short-lived Rust `kanban-server` have
both been removed.

Companion design docs (co-located here):

- [`data-model.md`](data-model.md) — the directory layout, per-file sync
  algorithm, ordering, conflict semantics.
- [`app-and-frontends.md`](app-and-frontends.md) — the serve/connect wrapper,
  HTTP surface, web UI, and **editor integrations**.
- [`cm6-editor.md`](cm6-editor.md) — **RETIRED** (an in-app CodeMirror editor;
  out of scope per the principle below, kept only as historical analysis).
- background: [`../../../docs/planned/db-sync.md`](../../../docs/planned/db-sync.md)
  (why files, not SQLite/cr-sqlite) and the core
  [`DESIGN.md`](../../../DESIGN.md) (the `Syncer` seam, folder sync §17,
  snapshot-race §16).

---

## Status — what's built

A working vertical slice over a board *directory* (no SQLite, no new sync engine):

- **Board** with columns, add-card, drag-to-column, toggle done.
- **Ticket detail drawer:** live (debounce-free) title + description editing with
  clobber-safety; comments (one file each under `comments/`, conflict-free).
- **Per-peer append-only change-event log** design (`events/<site>.jsonl`, one
  file per peer → merges with zero conflicts). *Currently writer-less — it
  lived in the removed servers; moving into the wasm handler is TODO below.*
- **Realtime:** in-page — mutations go straight to the Solid store; remote
  merges arrive via the sync layer's `on_merged` and reconcile (only the
  changed card re-renders).
- **Demos:** two/three-browser e2e flows over iroh + the gossip mesh
  (`e2e/run-iroh*.sh`).

The board is just files: `board.md` + `tickets/<id>/{card.md, meta.toml,
comments/*.md, attachments/*}` + `events/<site>.jsonl`. Prose (`card.md`,
comments) syncs via **text-crdt**; structural scalars (`meta.toml`) and binaries
(`attachments/`) via **rsync** (LWW). See [`data-model.md`](data-model.md).

---

## Principle: no homegrown editor

> **We will NEVER build our own text editor.** Text/description editing always
> ties into **existing** editors — Neovim via riftpipe's `--pipe` bridge, VS Code,
> "open in `$EDITOR`", etc. The file-backed data model is what makes this work:
> **any editor that opens the file is a frontend.** So "better editing" always
> means better *editor integration* (bridges/plugins), **never** a homegrown
> editor or an in-app CRDT textarea.

Concretely:

- The web UI's title/description box is a plain `<textarea>` that writes the whole
  `card.md` (with clobber-safety). It is deliberately *not* a collaborative
  editor — we do not grow a CRDT editor in the browser.
- Char-level collaborative prose editing comes from **bridging a real editor** to
  riftpipe's `--pipe` protocol (the [`nvim/riftpipe.lua`](../../../nvim/riftpipe.lua)
  bridge already does exactly this for an arbitrary file). The editor *is* the
  collaborative surface; the app just hands it the file.
- This is why [`cm6-editor.md`](cm6-editor.md) (an in-app CodeMirror 6 CRDT pane)
  is **retired**: it is the homegrown-editor approach this principle rejects.

---

## TODO / next

Roughly priority order. Most of this is *wiring and UI*, not new sync code.

- **Drag-to-reorder within a column** — set the moved card's `position`
  (fractional index in `meta.toml`); reorder touches only that one card. The
  cross-column drag already exists; this is the within-column case.
- **Markdown rendering** for descriptions and comments (render the stored
  markdown; editing still happens over the raw file — see the principle, this is
  *render*, not a homegrown editor).
- **Editor integrations** (the sanctioned path for "better editing"):
  - **Neovim bridge** — open a card's `card.md` through riftpipe's `--pipe` for
    live, char-level editing, reusing [`nvim/riftpipe.lua`](../../../nvim/riftpipe.lua)
    (`RIFTPIPE_ARGS="share .../tickets/<id>/card.md --pipe"`). The web UI can
    offer a one-click "open this card in Neovim" handoff.
  - **VS Code** — open the card file / workspace; folder sync keeps it converged.
  - **"open in `$EDITOR`"** — the zero-effort fallback that already Just Works
    because the board is files.
- **WebSocket-based presence** — "who's viewing/editing this card" (a separate
  channel; cursors/avatars are decorations, never part of the text/file model).
- **History view** over `GET /api/history` — render the merged `events/*.jsonl`
  timeline (the log is already produced; this is the UI for it).
- **Attachments UI** — drop/upload into `attachments/`; rsync carries the bytes.
- **Per-field `meta.toml` merge** — a future `lww-record` Syncer doing *per-key*
  LWW (each key carries a lamport+site; merge takes the max per key) so concurrent
  edits to *different* fields of the *same* card both survive. Whole-file rsync
  today loses one (rare). Lives in the riftpipe core, not the app. See
  [`data-model.md`](data-model.md#future-nicety-an-lww-record-syncer).
- **Servers removed — finish the follow-through.** `server/main.ts` (Deno),
  `server-rs/` (Rust), and `run-demo.sh` are gone; the wasm payload is the
  backend. Remaining: (a) the **change-event log** (`events/<site>.jsonl`) was
  written by the servers — move it into the wasm handler so history survives
  serverless; (b) Vite-via-Deno remains as *build tooling only* — swap to
  plain npm/vite (or keep, it's contained) when convenient.
- **Own the kanban code riftpipe still carries** — `riftpipe_core::kanban` (the
  format parser) and the kanban handler in `web/` belong to this project, not
  the riftpipe crates. Blocked on the wasm-crate split (roadmap §"Architecture
  / hygiene" #8): a generic riftpipe wasm crate + a kanban wasm crate here.

### Out of scope (per the principle)

- **Building our own live/collaborative editor** — an in-app CRDT textarea, a
  browser-side CodeMirror/ProseMirror CRDT pane, a "Word-like" WYSIWYG editor,
  etc. All retired in favor of **editor integrations**. The previous design for
  this is preserved (marked RETIRED) in [`cm6-editor.md`](cm6-editor.md).

---

## Later / optional (depends on core)

These need work in the riftpipe *core*, not just the app:

- **Folder move/delete sync** — track resources by stable id, not path
  ([`DESIGN.md`](../../../DESIGN.md) §17 TODO). Until it lands, "delete a card" =
  set `column = "archived"` in `meta.toml` (a hidden lane), which also gives undo
  for free. Real deletion + tombstone GC follows.
- **`--memory` boards** — keep the tree in RAM (memory backings) and surface
  resources in the `process` file (size + hash) for ephemeral boards.
- **Multi-board** — falls out of "a parent dir of board dirs" later; v1 is one
  board dir.

---

## Notes / invariants

- **Edits are small, targeted file writes** (one card's file, not a whole-board
  rewrite), so the snapshot-race window ([`DESIGN.md`](../../../DESIGN.md) §16)
  stays tiny and the per-file CRDT/LWW means overlap never loses data.
- **No native build, no external DB** — pure reuse of riftpipe's shipped folder
  sync (text-crdt + rsync via the manifest globs).
- **Stable ticket ids** (`tickets/<id>/`, e.g. `tk_8f3a`) so a move/edit always
  refers to the same ticket and never collides on merge. Column membership lives
  on the card (`meta.toml`), not in `board.md`.
</content>
</invoke>
