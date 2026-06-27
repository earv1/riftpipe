# Brainstorm: riftpipe + an *actual* database (future directions)

**Status:** brainstorm, **not a decision.** Forward-looking survey of how
riftpipe's custom P2P sync could ride on (or feed) a real database with good
Rust support. No commitment, no `Kind` is being added here. Builds on — and
deliberately does not repeat — [`db-sync.md`](db-sync.md), which evaluated
cr-sqlite + SQLite session changesets and **set them aside** in favor of the
files-as-database kanban. Read that first; this widens the lens past SQLite.

The user's framing: *"we already have a custom sync system, so if there's a DB
with good Rust support that already implements this pattern, that might be the
way."* The honest finding below is that **almost nothing implements our exact
pattern** (leaderless, P2P-over-iroh, no central server). Most "sync databases"
are hub-and-spoke. So the realistic question is narrower: which DB gives us a
*merge engine* or a *storage substrate* we can bolt onto the existing
[`Syncer`](../../src/sync/syncer.rs) seam, and which are just hype for our case.

**The headline, though, isn't "which database" — see §2.** Because riftpipe apps
only ever have *one local writer* per replica, DB concurrency/locking is a
non-problem, and the whole question collapses to *"treat each row as a document
keyed by id"* — the kanban pattern, generalized. The candidate survey then
matters only for *how rich the per-row merge needs to be*, not for choosing a
database engine.

---

## 0. The seam we have to map onto (recap)

DESIGN.md §17 defines a minimal, reconcile-centric trait. Every candidate below
is judged on how cleanly its change-stream maps to exactly these three methods:

- `state_vector() -> Vec<u8>` — "here's what I hold" (a version vector).
- `delta_since(theirs) -> Option<Vec<u8>>` — "here's what brings you up to me."
- `merge(delta) -> Option<Vec<u8>>` — "fold this in; tell me the new bytes."
- (+ `observe`/`push_delta` for the eager push path.)

Payloads are opaque bytes over iroh; framing never interprets them. A candidate
that natively speaks "version vector → delta → apply" drops straight in. One
that doesn't makes us *build* that protocol on top of it — at which point the DB
is just storage and we've written the CRDT ourselves anyway.

---

## 1. The spine: two integration shapes

Everything sorts into one of two shapes. **Which shape a candidate fits is the
single most important fact about it** — it decides whether we reuse the DB's
merge or only its disk format.

### Shape A — DB provides the merge; riftpipe is the transport

The DB natively does **conflict-free multi-writer merge** and exposes a
**change-stream** (changeset / update / op-log) plus a **version summary**. We
bridge that stream over iroh:

```
DB.changes_since(peer_version)  ─►  Syncer::delta_since
DB.apply(changeset)             ─►  Syncer::merge
DB.local_version_vector()       ─►  Syncer::state_vector
```

riftpipe supplies transport, framing, discovery, reconnect, the manifest, the
`process` file. The DB supplies correctness of merge. **This is the dream
case** and the whole point of the user's framing — we don't reinvent LWW-maps or
sequence CRDTs. cr-sqlite, Automerge, and Yrs are the only real Shape-A
candidates.

### Shape B — DB is *only* local storage; riftpipe's op-log/CRDT sits on top

The DB gives no merge — just durable, queryable local state. riftpipe keeps
owning convergence (its `engine/` op-DAG: `Op { id, lamport, parents, action }`,
already an operation-based CRDT over a causal DAG). The DB is where the
*materialized view* and/or the *op-log* live. Sync moves **our** ops; on apply
we run them against the DB.

```
riftpipe op-log  ──merge (ours)──►  materialize  ──►  DB tables (queryable)
```

Here the DB earns its keep with **queries, indexes, schema, durability** — not
merge. redb/sled/fjall/rocksdb, GlueSQL, and cozo are Shape-B substrates.
SurrealDB/SpacetimeDB/libSQL are mostly Shape-B-or-non-fit because their own
"sync" is server-authoritative and doesn't compose with leaderless P2P.

A subtle hybrid worth naming: a Shape-B store can *hold* the op-log frames
(the planned `wal-db` algorithm is exactly this — `state_vector` = per-writer
high-water mark, `delta_since` = missing frames, `merge` = union), giving us
durability + indexable history while riftpipe still owns merge.

---

## 2. The simplifying realization: one writer ⇒ rows are documents

The framing above quietly over-complicates the problem. Two facts collapse it:

**1. There is only ever one local writer.** Like the kanban (one server process
per board), each replica has a single local process writing its store. So the
entire category of *DB concurrency control* — row/table locks, transactions
across competing local writers, MVCC — is **irrelevant to us**: nothing contends,
so there are no locks to worry about. cr-sqlite's table-level CRDT machinery,
libSQL/Turso's server arbitration, SurrealDB's cluster protocol all solve
*multi-writer concurrency* — a problem we don't have.

**2. The only hard problem is cross-replica conflict — so resolve it per row.**
Treat **each row as an independent document keyed by its id.** Conflicts resolve
per-row; rows are independent units of sync. A row two replicas edited while apart
converges on its own, unrelated to its neighbours.

Together: **DB sync reduces to the kanban pattern, generalized.** A kanban ticket
*is* an id-keyed document (its folder of files); a DB row is the same thing. We
already sync id-keyed documents with per-resource algorithms and per-resource
conflict resolution — that machinery doesn't change just because the documents are
now called "rows."

### What this changes

- **No relational CRDT engine needed.** cr-sqlite (frozen, native, *table*-level)
  answered a question — "merge concurrent writers into one SQL DB" — we no longer
  ask. Off the build path entirely.
- **The sync unit is the row/document, not the table.** Shape A vs Shape B
  re-reads at *row granularity*:
  - *Shape A, per row* = **one Automerge/Yrs document per row** → rich,
    field-level conflict-free merge of a single row, pure-Rust, for free.
  - *Shape B, per row* = **id-keyed blobs** (JSON/postcard) + our existing
    per-row resolution: whole-document LWW now (rsync-style), the planned
    `lww-record` (per-field LWW with a lamport) next — identical to how
    `meta.toml` already works in the kanban.
- **Storage & query are orthogonal and later.** Hold the id-keyed documents in
  redb (or just files, as the kanban does); add a query layer (GlueSQL over the
  materialized rows) only if "queryable" becomes a real need. The query engine
  never participates in sync.

So the "database" is, first, *a collection of id-keyed documents that sync exactly
like kanban tickets.* SQL, indexes, joins are convenience built on top — never
part of convergence.

---

## 3. Candidate survey

Tag = which shape. Verdict is skeptical on purpose.

### cr-sqlite — *Shape A.* The best fit, with a real maintenance cloud.

[vlcn-io/cr-sqlite](https://github.com/vlcn-io/cr-sqlite) is still the only mature
thing that keeps a real SQLite `.db` *and* does leaderless concurrent merge
(`crsql_as_crr` upgrades a table to a CRDT; `crsql_changes` is the read/apply
changeset virtual table). The seam mapping is already worked out in `db-sync.md`
§A and is essentially perfect: per-site `db_version` map → `state_vector`,
`crsql_changes WHERE db_version > ?` → `delta_since`, `INSERT INTO crsql_changes`
→ `merge`. License MIT. **Build on db-sync.md, don't repeat it — here's what's
new since that doc:**

- **Maintenance risk is now concrete, not hypothetical.** Last tagged release is
  **v0.16.3, Jan 2024** ([releases](https://github.com/vlcn-io/cr-sqlite/releases))
  — ~2.5 years stale as of June 2026. Author Matt Wonlaw **joined Rocicorp**
  (Replicache/Zero) and the community is openly asking "why was cr-sqlite
  abandoned in favor of Replicache and Zero?"
  ([localfirst.fm #10](https://www.localfirst.fm/10)). db-sync.md flagged
  "maintenance is external; pin/vendor"; that risk has now largely
  *materialized*. Treat cr-sqlite as **frozen-but-working**, not living.
- **Implication:** still the cleanest Shape-A SQL story, but adopting it means
  committing to vendor + maintain a stalled C/Rust extension ourselves. The
  native-build spike db-sync.md asked for is now also an *ownership* decision.

**Verdict:** technically the closest match to riftpipe's pattern; adopt only if
we accept becoming its de-facto maintainer. Conflict-free multi-writer: **yes.**

### libSQL / Turso — *mostly non-fit for leaderless P2P (Shape B at best).*

[libSQL](https://github.com/tursodatabase/libsql) (C fork) and the newer Turso
(clean-room Rust rewrite, ex-Limbo) have excellent Rust support and embedded
replicas with offline writes ([offline sync
beta](https://turso.tech/blog/turso-offline-sync-public-beta)). But the model is
**hub-and-spoke, server-authoritative**: replicas push/pull against **Turso
Cloud**; there is no leaderless peer-to-peer path, and crucially the offline-sync
beta ships **conflict *detection* with resolution "not yet implemented"** and "no
durability guarantees." That is the opposite of conflict-free-by-construction.

**Verdict:** great local SQLite-in-Rust *engine* (could be a Shape-B store), but
its sync is a different architecture than riftpipe's and gives us no merge we'd
want to ride. Conflict-free multi-writer: **no** (server arbitrates, and even
that is unfinished). Don't bridge its sync; at most use the engine for storage.

### Automerge (automerge-rs) — *Shape A.* Document/JSON CRDT, near-perfect seam fit.

[Automerge 2.x](https://automerge.org/blog/automerge-2/) is a production-ready
JSON-like CRDT; the Rust core is "hundreds of times faster" than 1.x, pure Rust
crate (no C build for the core), MIT. Its [sync
protocol](https://automerge.org/automerge/automerge/sync/index.html) exchanges
**have/need by hash** over a reliable in-order stream — which is *exactly*
`state_vector` / `delta_since` / `merge`. db-sync.md already noted this alignment;
worth restating that Automerge is the lowest-friction Shape-A adoption because
it's a library, not an extension, and won't go stale the way cr-sqlite did.

**Caveat:** it's a *document* CRDT (JSON tree), not a relational/SQL DB. You get
conflict-free merge of structured documents and lists, not `SELECT … JOIN`. For
the kanban or game-state that may be plenty; for "an actual database with
queries" it isn't one.

**Verdict:** strongest *living* Shape-A option. Conflict-free multi-writer:
**yes.** Storage/query: weak (it's a doc, not a query engine).

### Yrs (y-crdt) — *Shape A.* Yjs port; sequence/text CRDT, very active.

[y-crdt/yrs](https://github.com/y-crdt/y-crdt) is the Rust port of Yjs,
**actively maintained** (v0.27.x, releases within days as of mid-2026), MIT,
binary-protocol-compatible with Yjs. Its `state_vector` + `encode_state_as_update`
API is a *literal* match for our two advertise/delta methods. Excellent for
collaborative text and shared structured types (maps/arrays/text), the same
problem space as our current diamond-types text path.

**Caveat:** like Automerge, it's a document/sequence CRDT, not a query DB; and it
overlaps heavily with what `TextCrdtSyncer` already does, so the marginal win is
"shared maps/arrays" not "a database."

**Verdict:** the most *alive* CRDT library here; ideal if a future resource wants
Yjs-style structured docs. Conflict-free: **yes.** A DB: **no.**

### redb / sled / fjall / rocksdb — *Shape B substrate for our op-log.*

These are storage engines, **no merge of their own** — they'd back riftpipe's
op-log or materialized view (the `wal-db` direction).

- **[redb](https://github.com/cberner/redb)** — pure-Rust, LMDB-inspired
  copy-on-write B+trees, **stable file format**, single active maintainer but
  steady. *Best default* for a Shape-B substrate: no native build, ACID, simple.
- **[fjall](https://fjall-rs.github.io/post/fjall-3/)** — pure-Rust LSM, v3 is
  "the most capable Rust storage engine," great write throughput — but the
  author has said **active feature development winds down going into 2026**.
  Good now, watch maintenance.
- **sled** — most-known, but still **alpha / rewrite incomplete**; not
  production-ready. Avoid for new work.
- **rocksdb** (via `rust-rocksdb`) — battle-tested but **C++ native build**,
  heavy dep; only if we need its scale. Cuts against riftpipe's "stays `cargo
  build`" ethos.

**Verdict:** redb is the clean Shape-B pick if we want durable indexed local
storage under our own op-log. None of these provide merge. Conflict-free
multi-writer: **no** (that stays riftpipe's job).

### GlueSQL — *Shape B.* SQL queries over a custom (riftpipe-backed) store.

[gluesql/gluesql](https://github.com/gluesql/gluesql) is a pure-Rust SQL engine
whose **storage is a trait you implement** (sled, memory, JSON, web storage, even
git already exist). We could implement a `Store` backed by a riftpipe-synced
op-log/redb, getting **SQL + queries on top of our own CRDT**. Actively
maintained through early 2026.

**Caveat:** GlueSQL gives the *query layer*, not merge — convergence is still
ours. The interesting combo is "riftpipe op-log (merge) → materialize → GlueSQL
(query)." Promising but bespoke: we write the Store adapter and the
materialization.

**Verdict:** the cleanest way to get **SQL over a riftpipe-synced store** without
a native dep. Conflict-free: **no** (storage/query only, by design). Worth a
prototype if "queryable" becomes a hard requirement.

### cozo — *Shape B.* Embeddable Datalog/graph over pluggable storage.

[cozodb/cozo](https://github.com/cozodb/cozo) is a transactional
relational-graph-vector DB with **Datalog** queries, embeddable like SQLite,
storage engines pluggable (in-memory, RocksDB, SQLite). Strong for recursive
queries / graph traversal / vectors.

**Caveat:** no native multi-writer CRDT merge; its persistence (RocksDB) reintro-
duces a C++ build. Compelling only if a future feature genuinely needs *graph/
Datalog* queries over synced data — otherwise it's a heavier GlueSQL with no
extra sync benefit.

**Verdict:** niche Shape-B; pick only for graph/Datalog needs. Conflict-free:
**no.**

### SurrealDB — *Shape B / non-fit for our sync.*

[surrealdb/surrealdb](https://github.com/surrealdb/surrealdb) runs embedded in
Rust (in-memory / file / its own KV engines) with **live queries** — genuinely
nice as a local engine. But its distribution story is its **own cluster/protocol**,
not leaderless CRDT merge we can bridge over iroh; "embedded" and "distributed"
are different deployment modes. Big dependency surface.

**Verdict:** usable as a local Shape-B engine with live queries, but its sync
doesn't compose with riftpipe's P2P model and it's a heavy dep. Conflict-free
leaderless: **no.**

### SpacetimeDB — *non-fit (model + license).*

[clockworklabs/SpacetimeDB](https://github.com/clockworklabs/SpacetimeDB) is a
"database *is* the server" model: clients call reducers, the DB is the central
authority. That's the antithesis of leaderless P2P. Also **BSL 1.1** (converts to
AGPL after years) — a licensing footgun for an MIT-spirited tool.

**Verdict:** wrong architecture (server-authoritative) **and** restrictive
license. Not a candidate for riftpipe's pattern. Conflict-free leaderless: **no.**

### The non-fits, and why (brief)

- **Hypercore/Autobase, OrbitDB** — the *right model* (append-only signed logs,
  Merkle-CRDT) but **JavaScript** ecosystems; no first-class Rust. We already
  *borrow their model* in `engine/` (db-sync.md "Prior art"); we don't want their
  JS stacks.
- **Litestream / LiteFS / rqlite / dqlite** — SQLite **replication/HA**:
  single-writer log shipping or Raft consensus, **not leaderless merge**. Useful
  contrast (db-sync.md already makes it), not a fit.
- **ElectricSQL** — has **pivoted to a read-path-only sync engine** (streams
  Postgres→client via Shapes; **writes go through your backend API**, no
  bidirectional conflict handling) ([PowerSync's
  comparison](https://powersync.com/blog/electricsql-electric-next-vs-powersync)).
  Postgres-centric, server-required, no client-side merge.
- **PowerSync** — full bidirectional, but **requires a dedicated sync service**
  reading Postgres/Mongo/MySQL replication logs ([powersync.com](https://powersync.com/));
  server-centric, not P2P.

All four assume a **server / Postgres**. riftpipe is serverless P2P, so they're
architecturally out regardless of Rust support.

---

## 4. Recommendation framework

### Decision criteria (in rough priority order)

1. **Merge semantics vs storage-only.** Does it give *conflict-free multi-writer
   merge* (Shape A) or just storage (Shape B)? Shape A is the only thing that
   *reduces* the code we own.
2. **Maps cleanly to `state_vector`/`delta_since`/`merge`?** A native
   version-vector → delta → apply API (Automerge, Yrs, cr-sqlite) is a drop-in;
   anything else means we build that protocol ourselves.
3. **Rust maturity & liveness.** Pure-Rust + *actively maintained* beats
   powerful-but-stalled. (cr-sqlite is the cautionary tale: best fit, frozen.)
4. **Native-build cost.** riftpipe's ethos is "stays `cargo build`." Pure-Rust
   (redb, Automerge, Yrs, GlueSQL, libSQL-Rust) > C/C++ extensions (cr-sqlite,
   rocksdb-backed cozo).
5. **Query needs.** Do we actually need SQL/Datalog, or is a document/KV enough?
   Don't pay for a query engine we won't query.
6. **License.** MIT/Apache preferred; BSL (SpacetimeDB) is a footgun.

### Quick scorecard

| Candidate | Shape | Merge? | Seam fit | Rust/liveness | Native build | Query |
|---|---|---|---|---|---|---|
| cr-sqlite | A | **yes** | excellent | C ext, **stalled** | yes (C) | SQL |
| Automerge | A | **yes** | excellent | pure, alive | no | none (doc) |
| Yrs | A | **yes** | excellent | pure, **very** alive | no | none (doc) |
| libSQL/Turso | B/non | no (server) | n/a | pure, alive | no | SQL |
| redb | B | no | (substrate) | pure, stable | no | KV |
| fjall | B | no | (substrate) | pure, slowing | no | KV |
| sled | B | no | (substrate) | pure, **alpha** | no | KV |
| rocksdb | B | no | (substrate) | C++ | **yes** | KV |
| GlueSQL | B | no | (over our log) | pure, alive | no | **SQL** |
| cozo | B | no | (over our log) | pure (+RocksDB) | maybe | **Datalog** |
| SurrealDB | B/non | no | n/a | pure, heavy | no | SQL-ish |
| SpacetimeDB | non | no (server) | n/a | pure, BSL | no | SQL |

### Directions worth prototyping (2–3)

1. **Rows-as-documents — generalize the kanban (do this first).** Model each row
   as an id-keyed document synced as a riftpipe resource, with **per-row** conflict
   resolution. Start with whole-document LWW (rsync-style, already shipped), then
   the planned `lww-record` for per-field LWW — the exact path `meta.toml` is on.
   Lowest risk, highest alignment: it's the kanban model we already run, pointed at
   "rows" instead of "tickets," with **no new merge engine and no native build.**
   Storage can literally be files first, redb later.

2. **Per-row Automerge/Yrs for rich field merge (Shape A, when LWW isn't enough).**
   When a single row needs *field-level* conflict-free merge (two replicas editing
   different columns of the same row, or concurrent list edits inside it), wrap
   **one Automerge or Yrs document per row** behind a `Syncer` and map its
   state-vector/update API to `state_vector`/`delta_since`/`merge`. Living,
   pure-Rust, no native build — the upgrade from LWW when a row's *internal*
   structure matters. (Prefer Yrs for Yjs interop, Automerge for history/branching.)

3. **(Stretch) Query layer over the synced rows.** redb to hold the id-keyed
   documents durably; GlueSQL (pure-Rust, pluggable `Store`) for SQL over the
   *materialized* rows — *only if* relational queries become a hard requirement.
   Storage/query is bolted on; merge stays per-row and ours.

**What to *not* do yet:** adopt cr-sqlite (best fit but stalled — revisit only if
a relational CRDT becomes load-bearing *and* we accept maintaining it); bridge
Turso/libSQL/Electric/PowerSync sync (wrong, server-centric architecture);
SpacetimeDB (model + license). These stay on the watch-list, not the build-list.

---

## Cross-references

- [`db-sync.md`](db-sync.md) — the earlier, narrower SQLite-only survey
  (cr-sqlite Strategy A, session-changeset Strategy B) that was set aside for the
  files-as-database kanban. This doc widens past SQLite and re-checks cr-sqlite's
  (now-confirmed) staleness.
- DESIGN.md §17 — the `Syncer` seam, backings, and the planned `wal-db`/`image`
  kinds these directions would extend.
- [`../../src/sync/syncer.rs`](../../src/sync/syncer.rs) — the trait every
  candidate must map onto.

## Sources (verified June 2026)

- cr-sqlite repo / releases (v0.16.3, Jan 2024) — https://github.com/vlcn-io/cr-sqlite · https://github.com/vlcn-io/cr-sqlite/releases
- Matt Wonlaw → Rocicorp; "abandoned for Replicache/Zero" discussion — https://www.localfirst.fm/10
- Turso offline sync beta (conflict resolution "not yet implemented," server-centric) — https://turso.tech/blog/turso-offline-sync-public-beta · https://github.com/tursodatabase/libsql
- Automerge 2.0 + sync protocol — https://automerge.org/blog/automerge-2/ · https://automerge.org/automerge/automerge/sync/index.html
- Yrs / y-crdt (v0.27.x, active) — https://github.com/y-crdt/y-crdt · https://docs.rs/yrs
- redb — https://github.com/cberner/redb · fjall 3.0 (dev slowing 2026) — https://fjall-rs.github.io/post/fjall-3/
- GlueSQL (pluggable storage) — https://github.com/gluesql/gluesql
- cozo (Datalog, RocksDB/SQLite storage) — https://github.com/cozodb/cozo
- SurrealDB (embedded + live queries) — https://github.com/surrealdb/surrealdb
- SpacetimeDB (BSL 1.1, server-authoritative) — https://github.com/clockworklabs/SpacetimeDB · https://spacetimedb.com/docs/intro/faq/
- ElectricSQL (read-path pivot) / PowerSync (server-required) — https://powersync.com/blog/electricsql-electric-next-vs-powersync · https://powersync.com/
