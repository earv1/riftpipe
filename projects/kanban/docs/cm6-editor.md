# RETIRED: the CM6 live-preview markdown editor

> **OUT OF SCOPE — retired by the "no homegrown editor" principle.** This document
> designs an in-app, browser-side collaborative editor (a CodeMirror 6 CRDT pane).
> We have since decided we will **NEVER build our own text editor**: prose editing
> ties into *existing* editors instead (Neovim via `--pipe`, VS Code, `$EDITOR`).
> See [`planned.md`](planned.md) for the principle and the OUT-OF-SCOPE note. This
> file is kept only as historical analysis — do **not** treat it as a roadmap
> item. The right replacement is the **editor integrations** section in
> [`app-and-frontends.md`](app-and-frontends.md).

**Status:** retired (design only — superseded). A CodeMirror 6 editing pane for a
ticket's `card.md` (and `comments/*.md`) that *looks* like editing a Word
document —
hidden syntax, inline formatting, a toolbar — while the document stays a **flat
markdown string** synced char-by-char by the existing **text-crdt** `Syncer`.

This is the web-UI counterpart to `nvim/riftpipe.lua`: same protocol, same
char-offset op model, different host. CM6 is a closer fit than nvim because **a
CM6 position already *is* a document char offset** — the row/col conversion the
lua bridge does (`char_to_rowcol`) just disappears.

## Why CM6 fits the flat-char CRDT

The whole bet (see [`data-model.md`](data-model.md) and the project
[`README.md`](../README.md)) is that
**markdown keeps formatting inside a flat char sequence** — `**bold**` and `# h`
are characters — so the eg-walker (`crdt/text`) document never needs a tree.
CM6 preserves that all the way up:

| Concern              | CM6 primitive                                  | Maps to |
| -------------------- | ---------------------------------------------- | ------- |
| Document model       | a single flat string (`state.doc`)             | the CRDT char sequence |
| A local edit         | `ChangeSet` — ranged `{from, to, insert}`      | insert/delete ops |
| Position             | integer char offset into the doc               | `pos` on the wire |
| Applying a remote op | `view.dispatch({changes})`                     | `apply_remote` in the bridge |
| Cursor after a remote op | CM6 **maps selections through the change** automatically | the cursor-preservation the lua bridge hand-rolls |
| WYSIWYG look         | a **decoration** layer (separate from the doc) | *render only — never touches synced bytes* |

The key separation: **sync operates on the markdown text; the Word-like
appearance is a pure view layer on top.** They don't interact. That's what makes
this safe — the decorations can be as fancy as we like without ever changing
what crosses the wire.

## Architecture — where the pane plugs in

The browser can't spawn a process, so the `--pipe` stdio that the nvim bridge
talks to is fronted by the localhost server (`app-and-frontends.md`). The pane
speaks the **exact same JSON op protocol** the lua bridge speaks, just over a
WebSocket instead of a job's stdin/stdout:

```
 CM6 pane  ──WS JSON ops──►  localhost server  ──stdin──►  riftpipe --pipe (card.md)
 (browser)  ◄──WS JSON ops── (per-file bridge)  ◄─stdout── text-crdt session ⇄ peer
```

- The server, on "open ticket `<id>` for live editing", attaches a **per-file
  pipe session** to `tickets/<id>/card.md` and shims it to a WebSocket. This is
  the `vim.fn.jobstart` + `chansend` role, moved server-side.
- Wire protocol is **unchanged** from the bridge:
  - **up** (local change): `{"op":"snapshot","text":"…"}` — riftpipe diffs it;
    only the delta hits the network. (We *can* send pre-diffed ops; see
    "Snapshot vs ops" — start with snapshot to keep the wire identical.)
  - **down** (remote edit): `{"op":"insert","pos":N,"text":"…"}`,
    `{"op":"delete","pos":N,"len":N}`, `{"op":"snapshot","text":"…"}`.

### Relationship to the HTTP file path

`app-and-frontends.md` already has `PATCH /api/ticket` (whole-file writes, folder
loop picks them up) and `/api/poll`. That path is fine for **structural** edits
(`meta.toml`, a card move) and for low-frequency prose edits. The CM6 live path
is for **prose being typed concurrently** — it gives char-level convergence
inside one `card.md` instead of debounced last-write snapshots, shrinking the
§16 snapshot-race window to nothing for the active field. Both write the same
file; a board can use the file path everywhere and "upgrade" the currently-open
description/comment to the live WS path.

## Local change → outgoing op

A CM6 `updateListener` fires on every doc change. Coalesce to once per
animation frame (the bridge's "once per event-loop tick"), then send a snapshot
— **unless** we're currently applying a remote op (echo-suppression):

```js
const remoteAnnotation = Annotation.define()   // tags our own remote-applied tx

const syncListener = EditorView.updateListener.of((u) => {
  if (!u.docChanged) return
  if (u.transactions.some(t => t.annotation(remoteAnnotation))) return // echo guard
  scheduleSnapshot()   // rAF-coalesced: ws.send({op:"snapshot", text: view.state.doc.toString()})
})
```

Sending a **snapshot** (not raw ChangeSets) keeps the wire byte-identical to the
nvim bridge and lets riftpipe own the diff — one diff implementation, already
tested. See the tradeoff below before optimizing this.

## Remote op → CM6

Apply with a single dispatch, tagged so the listener ignores it. CM6 remaps the
user's selection through the change for free — no cursor bookkeeping:

```js
function applyRemote(op) {
  if (op.op === "insert")
    view.dispatch({ changes: { from: op.pos, insert: op.text },
                    annotations: remoteAnnotation.of(true) })
  else if (op.op === "delete")
    view.dispatch({ changes: { from: op.pos, to: op.pos + op.len },
                    annotations: remoteAnnotation.of(true) })
  else if (op.op === "snapshot")
    view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: op.text },
                    annotations: remoteAnnotation.of(true) })
}
```

Note `pos`/`len` are **char offsets**, and `state.doc` is indexed in the same
units — the lua bridge's `char_to_rowcol`/`str_byteindex` dance is gone.

## The Word-like layer (decorations)

This is the part that makes it feel like a word processor, and it is **entirely
local rendering** — zero effect on sync:

- **Live preview / hidden syntax:** a `ViewPlugin` builds `Decoration`s that
  replace/hide markdown markers (`**`, `#`, `-`) with styled rendering, and
  **reveals the raw markers only on the line/span containing the cursor**
  (the Typora / Obsidian Live-Preview behavior — the honest "almost" from the
  design discussion). `@codemirror/lang-markdown` gives the syntax tree to drive
  this; the hard part is the reveal-near-cursor toggle, not the styling.
- **Toolbar:** Bold / Heading / List buttons are just **text edits** —
  "wrap selection in `**`", "prefix `# `", "prefix `- `". They go through the
  normal dispatch path and sync like any keystroke. The toolbar is not special.
- **Block widgets** (rendered tables, checkboxes, images) are decorations over
  the underlying markdown text; editing a table still edits the pipe characters
  underneath. (In-place table editing is where "almost Word" stops being almost
  — acceptable for v1.)

## Snapshot is the interface (not a v1 shortcut)

CM6 hands us the exact `ChangeSet`, so we *could* send `insert`/`delete` ops
directly and skip riftpipe's re-diff. **Don't — and not just for v1.** The
snapshot-in interface ("pipe me your new bytes, I converge") is riftpipe's whole
thesis, the narrow waist every producer shares:

- The moment this pane sends pre-computed ops, it speaks a protocol only *it*
  speaks. Then nvim needs its own, the file-watcher needs its own, and
  `cat card.md | riftpipe` can't play at all. The universality that makes it a
  *pipe* is gone.
- The CM6 pane's value as a demo is that it is the **same handful of lines as the
  nvim bridge** with no bespoke protocol. Being trivially thin *is* the point;
  being fast is not.

So the diff lives in riftpipe, once, and stays excellent there (cheap
prefix/suffix fast-path, allocation trim) — every producer benefits at the same
time. The granular `insert`/`delete` ops remain an **available lower layer** /
escape hatch, never a roadmap destination; nothing in the demo depends on them.
If per-keystroke snapshotting of a *large* doc ever measurably hurts, the fix is
to make the one diff path cheaper, not to fragment the interface.

## Gotchas

- **Auto-normalization churn (the big one).** Any feature that rewrites the
  source — reflowing, `*`↔`_` normalization, list renumbering, trailing-space
  trimming, "smart" quotes — generates phantom ops and fights a concurrent
  editor. **Disable all markdown auto-formatting.** Decorations render; they
  must not edit. This is the single most important rule for this pane.
- **IME / composition.** Don't snapshot mid-composition — gate `scheduleSnapshot`
  on `compositionend` (or check `view.composing`) so half-formed CJK/diacritic
  input doesn't flush partial bytes.
- **Undo/history.** Remote-applied transactions must be excluded from the local
  undo stack (`addToHistory: false` annotation) so Ctrl-Z doesn't revert a
  peer's edit. CM6 `history()` respects this.
- **rAF coalescing + echo guard ordering.** A remote op that lands between a
  local edit and its scheduled snapshot must not be clobbered — apply remote ops
  immediately, and let the next snapshot reflect the merged state (the CRDT
  converges regardless; this just avoids a redundant round-trip).
- **Large paste.** A big paste is one ChangeSet but a large snapshot; rAF
  coalescing already batches it. Fine as-is; just don't snapshot per-keystroke
  inside the paste.
- **Presence (later).** Remote cursors/selections are a *separate* channel
  (decorations again), not part of the text protocol — out of scope for v1, but
  the decoration layer is where they'd live.

## Build phases

1. **Bare CM6 pane** over `card.md` via the existing `PATCH`/`poll` file path —
   no live socket yet. Proves embedding in the SolidJS UI.
2. **Live WS bridge:** server-side per-file `--pipe` shim ⇄ WebSocket; pane
   speaks the snapshot/insert/delete/snapshot protocol. Char-level convergence
   on one open card, verified two-browser over loopback (mirror `run-local.sh`).
3. **Live-preview decorations:** hide syntax, reveal-near-cursor, basic toolbar.
4. **Polish:** IME gate, history exclusion, block widgets (tables/checkboxes),
   then presence as a follow-on.

Phase 2 is the only real new code (the WS↔pipe shim); 1, 3, 4 are UI. The CRDT,
the pipe protocol, and the text-crdt `Syncer` are all reused unchanged.
