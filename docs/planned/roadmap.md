# Roadmap — what we want to build next

Captured from the serverless-browser + iroh work. Ordered roughly by when it makes
sense to do it, not strict priority.

## Sync protocol

1. **Event/delta sync** — *done.* Version vectors exchanged on connect
   (`hello`), then only `ops_since` deltas ship — the ops the peer actually
   lacks; a fresh peer is just an empty vector, so first-connect and reconnect
   are one code path. Lives in `core::sync`; native + browser both ride it.

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

5. **Multi-peer (N-writer)** — *done, as a gossip mesh.* A board is an **iroh-gossip
   topic** (= the host's EndpointId); peers broadcast `SyncMsg`s epidemically and the
   swarm routes them — no fixed hub, it self-organizes and survives peers leaving.
   The N-peer-safe origin merge (already built) converges everyone. `web/src/gossip.rs`
   (`Mesh` + `GossipBoardSync`); the app's `irohConnect` joins the mesh. A `NeighborUp`
   catches a newcomer up with our full board. **Debug:** `riftpipe.connectedPeers()`
   (direct neighbors) and `riftpipe.routingMap()` (the gossiped `{id:[neighbors]}`
   topology). Verified `run-iroh-mesh.sh`: three browsers, all see all cards.
   *Remaining:* the topology is currently a star when everyone bootstraps off the host;
   hyparview will add cross-links over time, and gossiping more bootstrap peers would
   speed mesh healing. (LWW `full_state` catch-up landed — the Syncer caches
   structural bytes so `meta.toml` reaches new neighbors too.)

## Data model / DB

6. **`wal-db` — append-only WAL replication** — *core landed* (`core/src/wal.rs`:
   append-only frames + a deterministic Kahn linearizer, the Autobase idea).
   *Remaining:* wire the folder-mode adapter (`src/sync/algo/wal.rs` is still a
   `todo!()` stub behind `Kind::WalDb`) so a manifest glob can use it, and sync
   *state* (the WAL) separately from a rendered *view*.

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

## Architecture / hygiene (from the July 2026 review + restructure)

The restructure made the native binary fully generic (verbs: `share · join ·
connect · serve · signal`; kanban's server lives in
`projects/kanban/server-rs`, the tree-sync driver is `sync::tree`, generic
static+SSE hosting is `app::host`). What remains:

8. **Get kanban out of the riftpipe crates entirely** — `riftpipe_core::kanban`
   (the board file format) and the kanban handler inside `web/` still violate
   the generic-core principle (see `agent.md`). The fix is splitting `web/`
   into a generic wasm crate (links, gossip, OPFS, the sync driver) and a
   kanban wasm crate under `projects/kanban` that owns the format. Needs the
   wasm-pack + headless-Chrome cycle to verify — don't do it blind.
9. **One wire protocol** — folder mode (`sync::folder` framing + `SyncStrategy`
   seam) and tree/board sync (`core::sync` `SyncMsg`, browser-compatible) are
   two protocols for overlapping jobs. Consolidating means folder mode adopting
   the core protocol (or vice versa) without breaking browser peers.
10. **`riftpipe connect` transport generalization** — today it's hardcoded to
    WebRTC-via-signaling; the browser already speaks iroh (`irohConnect`), so
    accept a ticket for direct iroh dial + capability negotiation/fallback,
    and wire its byte counters to `--metrics`.
11. **Track `web/Cargo.lock` in git** — the wasm crate is workspace-excluded
    with an ignored lockfile, so native and browser dependency graphs can
    silently skew on wire-critical deps (iroh, postcard, serde).
12. **Retire the signaling deploy path** — Pages ships the iroh app ("nothing
    to host") while `deploy/` + the workflow's `VITE_SIGNAL_URL` vars still
    treat the WebSocket signaling server as first-class; demote it to an
    explicit fallback.
13. **De-flake `capability_negotiation_over_real_iroh`** — it rides the real
    n0 relay and fails intermittently ("peer sent no capabilities"); bounded
    retry in the test or a localhost relay fixture.

## Smaller

14. **"New board" UX** — a button that mints a fresh ticket instead of relying
    on open-with-empty-hash.
15. ~~Consolidate `opfs_root` helpers~~ — *done* (lib.rs uses kanban's).
16. **TypeScript-first glue (npm wrapper)** — frontend people shouldn't need
    Rust to build on riftpipe. The seams are already language-agnostic (JSON
    lines on stdio for `--pipe`, `SYNCED:` lines from `connect`, HTTP+SSE from
    `host`, the wasm API in the browser); package them the esbuild/biome way:
    an npm package that ships the prebuilt `riftpipe` binary and exposes a
    typed TS API (spawn + typed protocol wrappers). `src/<entrypoint>.rs`
    stays a ~350-line shell; all product glue can then live in TS.

## Deliberately NOT doing

- **Migrating to Pear/Holepunch** — it's a JS/Bare stack with a log+linearizer data
  model; adopting it means abandoning Rust + iroh + eg-walker, and it doesn't beat
  what we have (iroh already gives browser P2P with nothing hosted). We keep
  eg-walker for text: linearizing flattens the causal DAG the text merge needs.
