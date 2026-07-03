# `wal-db` — append-only WAL replication (design)

The log-native multi-writer case, for syncing **state** (a write-ahead log)
separately from a **view** (a rendered projection). Where text uses a CRDT, a WAL
uses an append-only log + a **deterministic linearizer** — the Autobase lesson
(`docs/planned/roadmap.md`, "deliberately NOT migrating to Pear"), applied in Rust.

## Why not the text CRDT

eg-walker is right when concurrent edits must *merge* character-by-character. A WAL
is different: each writer appends **whole frames** (rows, ops, transactions) that
should stay intact and be applied in a single agreed order. You don't want to
interleave two transactions' bytes; you want to order the two transactions. That's
linearization of logs, not text merge.

## Model

- **Frame** — one append-only entry: `{ writer, seq, deps, payload }`. `writer` is a
  stable id, `seq` is that writer's monotonic counter, `deps` are the frames this
  one causally follows (its knowledge at append time), `payload` is opaque bytes.
- **Wal** — one writer's own append-only sequence of frames (`seq` 0,1,2,…).
- **Replica** — the union of every writer's frames seen so far (a DAG via `deps`).
- **Linearize** — fold the DAG into one **total order**, deterministically:
  1. respect causality (a frame comes after all its `deps`);
  2. break concurrency ties by `(writer, seq)` — a fixed total order over writers.
  This is order-independent + idempotent: two replicas with the same frame *set*
  produce the identical sequence, regardless of arrival order. Convergence for N
  writers, same as the CRDT (a total order on writers → one linearization).
- **Apply** — the caller folds the linearized frames through a reducer to get state
  (rows → a table, ops → a document). Riftpipe ships the ordering; the reducer is
  the app's (the DB-rows-as-documents pattern generalized).

## Transport

Frames replicate over the same mesh as everything else: broadcast new frames, and a
new neighbor is caught up with the frames it's missing (by `(writer, seq)` gaps).
It rides `core::sync` alongside text/LWW — a WAL is just another resource kind.

## Status

- [ ] `core::wal` — `Frame`, `Wal`, `Replica`, `linearize()` (this doc's core).
- [ ] Missing-frame catch-up (gaps by writer/seq), like the text resync.
- [ ] Wire as a `core::sync` resource kind + a `.wal` example.
- [ ] The DB binding (rows-as-documents) on top.
