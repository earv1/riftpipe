# Roadmap — what we want to build next

Captured from the serverless-browser + iroh work. Ordered roughly by when it makes
sense to do it, not strict priority.

## Sync protocol

1. **Event/delta sync** *(in progress)* — stop shipping `encode_full()` on every
   edit. Exchange version vectors on connect, then send only `encode_delta` /
   `ops_since` — the ops the peer actually lacks. Reconnection becomes "send me
   everything since version X", where a fresh peer is just X = empty (one code
   path for first-connect and reconnect). The `EgWalkerText` primitives already
   exist (`version_vector`, `ops_since`, `encode_delta`); this is a `core::sync`
   protocol change. Matters most for long-lived documents (use case #1), where
   full-history-per-edit is O(history).

2. **Make delta sync bulletproof** — the event approach trades simplicity for a
   real invariant: never send a delta whose causal parents the peer lacks. Harden:
   - detect an un-appliable delta (missing dependency) and fall back to a
     full-state resync for that doc;
   - correct version-vector merge on both send (optimistic advance) and receive;
   - survive dropped / out-of-order frames (today we lean on the reliable ordered
     channel — QUIC / DataChannel — which is fine but undocumented as a dependency).

## Networking

3. **iroh DHT / pkarr discovery** — today the browser rides *n0's relays* even for
   the rendezvous, so n0 sees connection metadata. Pursue iroh's DHT/pkarr
   discovery so two browsers find each other with **no** hosted infra (the
   Holepunch HyperDHT lesson, applied within iroh). Closes the last "someone sees
   metadata" gap.

4. **Persisted host identity** — the host's iroh key is ephemeral, so a reload
   mints a new ticket and the shared `#ticket` link goes stale. Persist the secret
   key (localStorage); decide host-vs-join by comparing the URL ticket's id to our
   own. Makes share links durable across reloads.

5. **Multi-peer (N-writer)** — today it's 2-peer host/join. Accept multiple peers
   (host fans out, or a small mesh) so a board can have >2 collaborators.

## Data model / DB

6. **`wal-db` — append-only WAL replication** — the log-native multi-writer case.
   Study **Autobase's linearizer** (deterministic causal order of N logs + a
   reducer) as the reference; this is where log-linearization fits and CRDTs don't
   need to. Sync *state* (the WAL) separately from a *view*.

7. **DB rows as documents** — each row an id-keyed document with per-row conflict
   resolution; one local writer so locking is moot (the kanban pattern
   generalized). See `db-integration.md`.

## Smaller

8. **"New board" UX** — a button that mints a fresh ticket instead of relying on
   open-with-empty-hash.
9. **Consolidate `opfs_root` helpers** (lib.rs vs kanban.rs).

## Deliberately NOT doing

- **Migrating to Pear/Holepunch** — it's a JS/Bare stack with a log+linearizer data
  model; adopting it means abandoning Rust + iroh + eg-walker, and it doesn't beat
  what we have (iroh already gives browser P2P with nothing hosted). We keep
  eg-walker for text: linearizing flattens the causal DAG the text merge needs.
