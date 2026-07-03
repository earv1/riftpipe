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

2. **Make delta sync bulletproof** — *mostly done.*
   - ✅ Un-appliable delta (missing ancestor) → `merge` returns `false` instead of
     panicking; the receiver sends `Resync`, the sender answers with full
     self-contained state. Loop-capped (`awaiting_resync`) so a run of gapped
     deltas can't cause a resync storm. Tested in `core::sync` + `core::text`.
   - ✅ Version-vector merge is per-agent-max on both send and receive
     (`advance_peer_vv`), so a stale report can't regress what we know they hold.
   - *Remaining:* a `Resync` is only wired for **text**, not LWW files — a fresh
     joiner still doesn't get existing `meta.toml` state until it's re-touched
     (needs the transport to supply file bytes; see initial-state sync). And if a
     *full* resync itself keeps failing (transport corruption), the doc parks
     rather than looping — acceptable, but a bounded retry/telemetry hook is the
     next refinement.

## Networking

3. **iroh DHT / pkarr discovery** — today the browser rides *n0's relays* even for
   the rendezvous, so n0 sees connection metadata. Pursue iroh's DHT/pkarr
   discovery so two browsers find each other with **no** hosted infra (the
   Holepunch HyperDHT lesson, applied within iroh). Closes the last "someone sees
   metadata" gap.

4. **Persisted host identity** — *done.* The iroh secret key is persisted in
   localStorage, so a host keeps the same EndpointId — and the same shareable
   ticket — across reloads. `irohConnect` hosts when the URL ticket is empty *or*
   its id equals ours (a reloaded host recognizes its own link), and joins
   otherwise. Proven deterministically headless (`persisted_iroh_identity_is_stable`)
   + the two-browser `run-iroh` flow still passes. (Reconnecting an *already-joined*
   peer after the host reloads is separate — that's live reconnection, below.)

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

### Done

- **Merge two independent boards** — peers prime their stored board on connect
  (`kanban::prime_board`) and the sync layer unions distinct cards + resolves a
  same-path file by origin (`core::sync`). Verified `run-iroh-merge.sh`: two
  browsers each with their own card connect over iroh and both see both. Converges
  for N peers (`three_independent_boards_converge`). *Remaining nits:* primed
  `meta.toml` (LWW) uses wall-clock now() so a same-card meta conflict is racy
  (cards rarely collide); and pasting a share link into a **solo tab** reconnects
  best-effort via a `hashchange` listener. On reconnect we now tear down the prior
  session (`Endpoint::close()`), which is correct and needed for live reconnection —
  but rebinding the **same persisted key** immediately still races (relay
  re-registration), so in-tab reconnect *from a solo host* isn't reliable yet.
  Opening the link fresh (new tab / reload) works cleanly. The remaining fix
  (delay/await the same-key rebind, or reuse the endpoint instead of rebinding)
  folds into **live reconnection**.

## Smaller

8. **"New board" UX** — a button that mints a fresh ticket instead of relying on
   open-with-empty-hash.
9. **Consolidate `opfs_root` helpers** (lib.rs vs kanban.rs).

## Deliberately NOT doing

- **Migrating to Pear/Holepunch** — it's a JS/Bare stack with a log+linearizer data
  model; adopting it means abandoning Rust + iroh + eg-walker, and it doesn't beat
  what we have (iroh already gives browser P2P with nothing hosted). We keep
  eg-walker for text: linearizing flattens the causal DAG the text merge needs.
