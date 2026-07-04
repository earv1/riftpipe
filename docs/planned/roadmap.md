# Roadmap — what we want to build next

Captured from the serverless-browser + iroh work. Ordered roughly by when it makes
sense to do it, not strict priority.

## Sync protocol

1. **Event/delta sync** — *done.* Version vectors exchanged on connect
   (`hello`), then only `ops_since` deltas ship — the ops the peer actually
   lacks; a fresh peer is just an empty vector, so first-connect and reconnect
   are one code path. Lives in `core::sync`; native + browser both ride it.

2. **Make delta sync bulletproof** — *done* (one refinement parked, below).
   - ✅ Un-appliable delta (missing ancestor) → `merge` returns `false` instead of
     panicking; the receiver sends `Resync`, the sender answers with full
     self-contained state. Loop-capped (`awaiting_resync`) so a run of gapped
     deltas can't cause a resync storm. Tested in `core::sync` + `core::text`.
   - ✅ Version-vector merge is per-agent-max on both send and receive
     (`advance_peer_vv`), so a stale report can't regress what we know they hold.
   - ✅ ~~A `Resync` is only wired for **text**, not LWW files~~ — LWW state now
     rides the connect handshake: `Hello` carries the LWW inventory (`(path,
     version)` per structural file) alongside the text version vectors, and
     `apply(Hello)` replies with the cached payloads the peer lacks or is stale
     on (the same bytes cache `full_state` uses — one shared `lww_updates_for`).
     A fresh point-to-point joiner gets existing `meta.toml` with no re-touch;
     a stale peer is updated; a newer local file is never regressed (the
     receiver's `apply(Lww)` version check still gates). One round, no storms —
     a Hello never provokes another Hello, and applying an `Lww` produces no
     replies. Tested in `core::sync` (`hello_transfers_existing_lww_to_a_fresh_peer`,
     `hello_updates_stale_lww_peer`, `hello_never_clobbers_newer_local_lww`).
   - *Remaining:* if a *full* resync itself keeps failing (transport corruption),
     the doc parks rather than looping — acceptable, but a bounded retry/telemetry
     hook is the next refinement.

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
   (`Mesh` + `GossipTreeSync`); the app's `irohConnect` joins the mesh. A `NeighborUp`
   catches a newcomer up with our full board. **Debug:** `riftpipe.connectedPeers()`
   (direct neighbors) and `riftpipe.routingMap()` (the gossiped `{id:[neighbors]}`
   topology). Verified `run-iroh-mesh.sh`: three browsers, all see all cards.
   *Remaining:* the topology is currently a star when everyone bootstraps off the host;
   hyparview will add cross-links over time, and gossiping more bootstrap peers would
   speed mesh healing. (LWW `full_state` catch-up landed — the Syncer caches
   structural bytes so `meta.toml` reaches new neighbors too.)

## Data model / DB

6. **`wal-db` — append-only WAL replication** — *done through folder mode.*
   Core (`core/src/wal.rs`: frames + deterministic Kahn linearizer) plus the
   real `Kind::WalDb` adapter (`src/sync/algo/wal.rs`): u32-LE postcard-framed
   frames on disk (torn-tail tolerant), push = frames-since-last-push,
   reconcile via watermarks, merge rematerializes the linearized log.
   *Remaining (design):* the state-vs-view split — sync the WAL, render a
   projection separately — and a `core::sync` resource kind for browser peers.

7. **DB rows as documents** — each row an id-keyed document with per-row conflict
   resolution; one local writer so locking is moot (the kanban pattern
   generalized). See `db-integration.md`.

### Done

- **Merge two independent boards** — peers prime their stored tree on connect
  (`opfs::prime_all`) and the sync layer unions distinct files + resolves a
  same-path file by origin (`core::sync`). Verified `run-iroh-merge.sh`: two
  browsers each with their own card connect over iroh and both see both. Converges
  for N peers (`three_independent_trees_converge`). *Remaining nits:* primed
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
connect · serve · signal`; the tree-sync driver is `sync::tree`, generic
static+SSE hosting is `app::host`), and the kanban servers were then removed
entirely — the wasm payload is the app's backend. What remains:

8. ~~Get kanban out of the riftpipe crates entirely~~ — *done.* The board file
   format and the OPFS kanban handler now live in the `kanban-wasm` crate
   (`projects/kanban/wasm/`, modules `format` + `handler`); `riftpipe-web` is
   app-generic (links, gossip, `opfs` helpers, `tree_sync` — `BoardSync` is now
   `TreeSync`, browser ALPN `riftpipe/tree/0`) and `riftpipe-core` is CRDT +
   sync protocol + wal only. The app builds ONE bundle from `kanban-wasm`
   (which links `riftpipe-web`, so both crates' wasm exports surface);
   `api.ts`/e2e/`pages.yml` point at `projects/kanban/wasm/pkg`, and
   `web/test-headless.sh` runs both crates' headless suites.
9. **One wire protocol** — folder mode (`sync::folder` framing + `SyncStrategy`
   seam) and tree/board sync (`core::sync` `SyncMsg`, browser-compatible) are
   two protocols for overlapping jobs. Consolidating means folder mode adopting
   the core protocol (or vice versa) without breaking browser peers.
10. ~~`riftpipe connect` transport generalization~~ — *done.* `connect
    <ticket>` dials native↔native over iroh (auth + negotiation + WebRTC
    upgrade); `connect --accept` mints the ticket and accepts — zero
    signaling infra; the connection-id/signaling path stays as the browser
    bridge (the browser's iroh listener is a different, handshake-less
    dialect — documented, not faked). `--metrics` wired on the iroh paths;
    byte totals on the signaling path. Real-loopback e2e test.
11. ~~Track `web/Cargo.lock` in git~~ — *done.*
12. ~~Retire the signaling deploy path~~ — *done as demotion:* labeled LEGACY
    fallback in `deploy/signal.Dockerfile`, `pages.yml` vars, and the deploy
    doc appendix; iroh is the documented default with nothing to host.
    (Actually deleting `deploy/` can happen once the `?transport=ws` bridge
    has no remaining users.)
13. ~~De-flake `capability_negotiation_over_real_iroh`~~ — *done:* the whole
    connect→auth→caps sequence retries with fresh endpoints (3 attempts);
    a failure now indicates a real problem, not relay weather.

## Smaller

14. ~~"New board" UX~~ — *done:* a confirm-guarded topbar button wipes the
    local OPFS board, drops the persisted iroh identity (new key ⇒ new topic
    ⇒ fresh share ticket), clears the hash, and reloads as the host of an
    empty board.
15. ~~Consolidate `opfs_root` helpers~~ — *done* (one `web/src/opfs.rs` module).
16. **TypeScript-first glue (npm wrapper)** — frontend people shouldn't need
    Rust to build on riftpipe. The seams are already language-agnostic (JSON
    lines on stdio for `--pipe`, `SYNCED:` lines from `connect`, HTTP+SSE from
    `host`, the wasm API in the browser); package them the esbuild/biome way:
    an npm package that ships the prebuilt `riftpipe` binary and exposes a
    typed TS API (spawn + typed protocol wrappers). `src/<entrypoint>.rs`
    stays a ~350-line shell; all product glue can then live in TS.

## Iteration 2 (added 2026-07-04)

17. **Events log in the wasm handler** — the per-peer change log
    (`events/<site>.jsonl` + `.site` id) lost its writer when the servers were
    removed. Restore it inside `kanban-wasm`'s handler: mint/persist a site id,
    append one JSON line per mutation. Dot-`.site` stays local (dotfile rule);
    `events/*.jsonl` sync as per-peer append-only files (zero conflicts — one
    writer each). Unblocks the history view.
18. **Per-resource backing in the manifest** — `riftpipe.toml` rules gain an
    optional `backing = "memory" | "file"` per glob (today `--memory` is
    all-or-nothing); `--memory` stays as the global default override.
19. **`lww-record` strategy (resolve the sqlite orphan)** — per-key LWW merge
    for `key = value` structural files (the kanban `meta.toml` case: concurrent
    edits to *different* fields both survive; whole-file LWW loses one). Build
    it as a real `Kind::LwwRecord` reusing `algo/sqlite.rs`'s per-cell LWW
    machinery — and either absorb or delete sqlite.rs so the orphan is gone.
20. **Bounded resync retry + telemetry** — *done.* `Syncer` re-requests a
    failing full resync up to 2 times, then parks the path — exposed via
    `parked_paths()` (core stays no-I/O); the tree driver eprintlns once per
    newly-parked path, and any later successful merge un-parks it.

## Deliberately NOT doing

- **Migrating to Pear/Holepunch** — it's a JS/Bare stack with a log+linearizer data
  model; adopting it means abandoning Rust + iroh + eg-walker, and it doesn't beat
  what we have (iroh already gives browser P2P with nothing hosted). We keep
  eg-walker for text: linearizing flattens the causal DAG the text merge needs.
