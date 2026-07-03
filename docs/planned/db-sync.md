# Considered (not chosen): SQLite-backed db sync (`crdt.sqlite` + `wal.sqlite`)

**Status:** alternatives considered — **NOT chosen.** Kept for the rationale /
future reference. The kanban went with a **files-as-database** model instead
(see [`kanban/`](kanban/README.md)): a directory tree synced by the existing
folder mode, with no native dependency and no new sync engine.

Why set aside: the SQLite paths buy you "the `.db` file itself is the
mergeable, any-tool-writable format," but at the cost of a **native extension**
(cr-sqlite) or a **new engine**, and our reuse-the-folder-sync approach needs
*zero* new sync code. We may revisit cr-sqlite if a future feature genuinely
needs a relational CRDT db over the wire.

---

The two SQLite strategies we evaluated (both behind the §17 multi-algorithm
seam):

## Why SQLite

A "database" resource wants structure, queries, and a format every tool already
understands. SQLite is that format. The open question was *how to sync a `.db`
peer-to-peer*; the answer is two strategies, picked per file:

| File convention | Strategy | Use when | Merge |
|---|---|---|---|
| `*.crdt.sqlite` | **cr-sqlite** (CRDT) | writers can edit the **same rows** | conflict-free (LWW columns + causal log) |
| `*.wal.sqlite` | **SQLite session changesets** | writers own **disjoint rows** / append-only | union (no resolution needed) |

Both sit behind the same [`SyncStrategy`](../../src/sync/strategy.rs) trait — only the
encode/merge internals differ. The manifest maps the globs to `Kind`s:

```toml
[[rule]]
glob = "**/*.crdt.sqlite"
algo = "crdt-sqlite"
[[rule]]
glob = "**/*.wal.sqlite"
algo = "wal-sqlite"
```

## Strategy A — `crdt.sqlite` (cr-sqlite)

[cr-sqlite](https://github.com/vlcn-io/cr-sqlite) (vlcn.io) is a loadable SQLite
extension adding **multi-writer CRDT** support. It's the only mature option that
keeps SQLite's `.db` as the real file *and* does leaderless concurrent merge
(Litestream/LiteFS/rqlite/dqlite all keep SQLite single-writer — replication/HA,
not merge).

How it works:
- `SELECT crsql_as_crr('cards')` upgrades a table to a *conflict-free replicated
  relation* — adds metadata tables + triggers; **your own schema is unchanged**
  (caveats: unique constraints & foreign keys — see Risks).
- `crsql_changes` is a virtual table you **read changesets from** and **apply
  changesets to**. Each change carries a site id + db version.

Mapping onto the `Syncer` seam:
- `state_vector()` → our per-site `db_version` map (a version vector over sites).
- `delta_since(theirs)` → `SELECT * FROM crsql_changes WHERE db_version > ?` per
  site the peer is behind on.
- `merge(delta)` → `INSERT INTO crsql_changes VALUES (…)` (cr-sqlite resolves
  conflicts on apply).
- backing → the `.db` file, or an in-memory SQLite db (memory mode + the
  `process` file still apply — size+hash of the db bytes).

cr-sqlite provides the **merge**; riftpipe provides the **transport, framing, and
app wrapper**. We don't reinvent LWW-maps or sequence CRDTs.

## Strategy B — `wal.sqlite` (logical changesets, not physical WAL)

For data where writers never touch the same row — each peer appends its own rows,
or rows are partitioned by owner — full CRDT machinery is overkill. We just need
to **ship each peer's row changes and union them**.

**Accuracy note:** this is *not* shipping SQLite's physical WAL file. SQLite's
WAL is page-level; two peers' WALs diverge at the b-tree and cannot be unioned
(that's why Litestream-style WAL shipping is single-writer only). Instead we use
SQLite's built-in **session extension**: `sqlite3session_attach` records logical
row changes, `sqlite3session_changeset` exports them, `sqlite3changeset_apply`
applies them on the peer with an omit/replace conflict handler. (rusqlite exposes
this behind its `session` feature.)

Conflict-free **by construction** when keys are disjoint / append-only; the
conflict handler is a backstop, not the primary mechanism. Lighter than cr-sqlite
(built into SQLite, no extension, no per-row metadata).

Mapping onto the `Syncer` seam:
- `state_vector()` → a per-writer high-water mark (e.g. a monotonic change seq
  per site id, or `MAX(rowid)` per owner partition).
- `delta_since(theirs)` → a session changeset of rows newer than the peer's mark.
- `merge(delta)` → `changeset_apply` with `OMIT`/`REPLACE` on conflict.
- backing → same as A.

Good fit for: event/audit tables, per-device logs, the game "state" stream
(each player appends their own actions — the "sync state separately from a view"
case), anything where ownership is partitioned.

## Prior art (the models this borrows from)

The proven shape for P2P append-only databases is an **operation-based CRDT over
a causal DAG**. riftpipe's own `engine/` (`Op { id, lamport, parents, action }`)
is already that DAG, which is why this all lines up.

- **cr-sqlite / `crsql_changes`** — operation/CRDT layer *inside* SQLite (what we
  adopt for Strategy A). [repo](https://github.com/vlcn-io/cr-sqlite) ·
  [intro](https://vlcn.io/docs/cr-sqlite/intro)
- **OrbitDB / `ipfs-log`** — operation-based append-only log CRDT (a G-Set),
  formalized as a **Merkle-CRDT** (Merkle-DAG + CRDT). The canonical "P2P
  append-only db." [ipfs-log](https://github.com/orbitdb-archive/ipfs-log) ·
  [Merkle-CRDTs paper](https://arxiv.org/pdf/2004.00107)
- **Hypercore + Autobase (+ Autobee/Hyperbee)** — signed append-only logs;
  multi-writer via a causal DAG linearized into an event-sourced view; a KV store
  built on top. [hypercore](https://github.com/holepunchto/hypercore) ·
  [autobase](https://github.com/holepunchto/autobase) ·
  [autobee](https://github.com/holepunchto/autobee)
- **Automerge** — JSON CRDT as a log of changes; its sync protocol exchanges
  "have/need" via hashes — essentially what our `state_vector`/`delta_since`
  already do.
- **Secure Scuttlebutt** — per-identity append-only signed feeds, gossip-
  replicated (the partitioned-writer model, like Strategy B).
- **Litestream / LiteFS / rqlite / dqlite** — SQLite replication/HA, but
  single-writer or consensus-based; *not* leaderless multi-writer merge. Useful
  contrast for why we picked cr-sqlite + session changesets.
  [comparison](https://onidel.com/blog/sqlite-replication-vps-2025)

**What we borrow:** the op-based-CRDT-over-causal-DAG *model*. **What we don't:**
the IPFS / Hypercore / consensus *stacks* — riftpipe already has transport (iroh)
and the sync framing; SQLite supplies the data model.

## Risks & open questions

- **Native build.** cr-sqlite is a loadable C/Rust extension — it ends "pure
  `cargo build`." Spike first: build/load it from `rusqlite`, confirm a clean
  static-link or bundled-extension story across macOS/Linux. Strategy B (session
  ext) needs only `rusqlite`'s `session` feature — much lighter; could ship first.
- **cr-sqlite caveats:** unique constraints and foreign keys need care under
  CRDT merge; design schemas accordingly (surrogate keys, avoid cross-row
  invariants the CRDT can't preserve).
- **Identity / site ids** must be stable per replica and survive reconnects (tie
  to the existing agent identity in `engine/identity.rs`).
- **Maintenance** of cr-sqlite is external; pin a version, vendor if needed.
- **Snapshot race goes away** for db resources: edits are logical changes
  appended/merged, not whole-file rewrites — a real advantage over the markdown
  path (DESIGN §16).
- **`Kind` additions:** `crdt-sqlite`, `wal-sqlite` (the existing `wal-db`/`image`
  stubs get reconciled with these).
