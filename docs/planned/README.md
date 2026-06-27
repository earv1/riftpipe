# docs/planned — forward-looking design

This folder holds **design docs for things not yet built**. It's the staging area
for plans before they become code; once something ships, its authoritative design
moves into the top-level [`DESIGN.md`](../../DESIGN.md) and its status into
[`PROJECT.md`](../../PROJECT.md).

- `DESIGN.md` — what *is* built and why (source of truth for shipped design).
- `docs/planned/` — what we *intend* to build (proposals, schemas, roadmaps).

## Index

### kanban/ — a P2P kanban (files as the database) — CHOSEN
Flagship use case: a serverless, e2e-encrypted collaborative kanban. **Decision
(June 2026): the board is a directory tree** synced by riftpipe's existing folder
mode — each file type bound to a `Syncer` we already ship (text-crdt for prose,
rsync for structural/binary). The "database" is a *convention*, not new code.
Source of truth = files; the web UI, a vim plugin, and any markdown editor are
**interchangeable views**.

- [`kanban/README.md`](kanban/README.md) — overview & big picture
- [`kanban/data-model.md`](kanban/data-model.md) — directory layout, per-file algorithm, ordering, conflicts
- [`kanban/app-and-frontends.md`](kanban/app-and-frontends.md) — `kanban serve`/`connect` wrapper, web UI, vim plugin
- [`kanban/roadmap.md`](kanban/roadmap.md) — phased plan (mostly wiring + UI)

### [`db-sync.md`](db-sync.md) — SQLite substrate — CONSIDERED, NOT CHOSEN
The cr-sqlite / SQLite-session analysis we evaluated and set aside (native
dependency / new engine vs. zero new sync code). Kept for rationale and in case a
future feature genuinely needs a relational CRDT db over the wire.
