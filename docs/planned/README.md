# docs/planned — forward-looking design

This folder holds **design docs for things not yet built**. It's the staging area
for plans before they become code; once something ships, its authoritative design
moves into the top-level [`DESIGN.md`](../../DESIGN.md) and its status into
[`PROJECT.md`](../../PROJECT.md).

- `DESIGN.md` — what *is* built and why (source of truth for shipped design).
- `docs/planned/` — what we *intend* to build (proposals, schemas, roadmaps).

## Index

### kanban — a P2P kanban (files as the database) — planning lives with the project
Flagship use case: a serverless, e2e-encrypted collaborative kanban. **Decision
(June 2026): the board is a directory tree** synced by riftpipe's folder mode
(text-crdt for prose, rsync for structural/binary) — the "database" is a
*convention*, not new code. Source of truth = files; the web UI and any editor
are **interchangeable views** (we never build our own editor — we integrate).
The kanban's planning + design now live **with the project**:

- [`projects/kanban/docs/planned.md`](../../projects/kanban/docs/planned.md) — status, roadmap, TODO + the "no homegrown editor" principle
- [`projects/kanban/docs/data-model.md`](../../projects/kanban/docs/data-model.md) — directory layout, per-file algorithm, ordering, conflicts
- [`projects/kanban/docs/app-and-frontends.md`](../../projects/kanban/docs/app-and-frontends.md) — the wrapper, web UI, editor integrations

### [`db-integration.md`](db-integration.md) — using a real DB with riftpipe (brainstorm)
Forward-looking options for backing riftpipe's sync pattern with an actual
database: **Shape A** (the DB provides CRDT merge — Automerge/Yrs, or the now-
frozen cr-sqlite) vs **Shape B** (DB as plain storage, our op-log stays the merge
engine — redb/sled, GlueSQL for SQL). Recommendations + non-fits. Builds on
`db-sync.md`.

### [`db-sync.md`](db-sync.md) — SQLite substrate — CONSIDERED, NOT CHOSEN
The earlier cr-sqlite / SQLite-session analysis we evaluated and set aside.
Superseded by `db-integration.md`'s broader survey; kept for rationale.
