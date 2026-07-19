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
9. ~~One wire protocol~~ — *done.* Folder mode adopted the core protocol:
   `core::sync::SyncMsg` gained two APPENDED variants — `StratAdvert { path,
   sv, reply }` (unifying `SYNCREQ`/`SYNCREP`) and `StratDelta { path, bytes }`
   — carrying the `SyncStrategy` seam's opaque payloads, and `sync::folder` now
   speaks postcard-encoded `SyncMsg` like tree/mesh; the bespoke
   `[path_len][path][tag]` framing is deleted. Wire-safe for deployed browsers:
   variants were appended (old peers decode-fail and skip; every driver already
   does), guarded by `core::sync::wire_compat` tests that pin all variant
   indexes and prove a legacy 4-variant decoder rejects the new ones cleanly.
   The text/LWW `Syncer` ignores strategy messages and folder sessions ignore
   tree messages — one envelope, two dialects. Moving the strategy channel
   into core is also the prerequisite for item 6's "browser peers speak WAL".
   *Follow-up (not blocking):* folder-mode text resources still ride the
   strategy channel rather than `TextDelta`; deep-merging them onto the tree
   dialect would let a folder session and a browser peer share text docs.
10. ~~`riftpipe connect` transport generalization~~ — *done, all three
    transports.* `connect <ticket>` dials native↔native over iroh (auth +
    negotiation + WebRTC upgrade); `connect --accept` mints the ticket and
    accepts; **`connect <browser-share-link>` joins the board's iroh-gossip
    mesh natively** (`sync::mesh`, speaking `core::sync::MeshMsg` — the same
    wire type `web`'s GossipTreeSync uses; proven by the `board-cli-mesh`
    e2e: headless browser hosts, CLI joins the link, bidirectional sync,
    zero servers); the connection-id/signaling path remains as fallback.
    `--metrics` on the point-to-point iroh paths; byte totals on signaling.
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
16. ~~TypeScript-first glue (npm wrapper)~~ — *done (local package; publishing
    is release engineering for later).* `npm/`: zero-runtime-dependency
    TypeScript package typing the language-agnostic seams — `PipeSession`
    (the `--pipe` JSON-lines editor stream, `EditOp` typed as the wire's
    serde shape), `TreeSession` (`connect`/`connect --accept`, `SYNCED:`
    breadcrumbs as events, `ticket()` from the stderr banner), `Serve`
    (spawn + readiness probe + typed SSE `/events` client). Binary resolution:
    `$RIFTPIPE_BIN` → packaged `bin/` → enclosing cargo `target/{release,debug}`
    → `$PATH` (the esbuild model — `npm/` mirrors its layout). e2e tests spawn
    the real binary through the built `dist/` exports. Building the wrapper
    ALSO surfaced and fixed a real `serve` bug: SSE frames were padded to
    1200 bytes to defeat tiny_http's 1 KiB `BufWriter`, but the binding buffer
    is `chunked_transfer`'s 8192-byte chunk (which only emits on overflow, not
    exact fill) — events reached clients ~7 stale. Frames now pad to two
    chunks so each event's data line is forced out by its own padding.

- **Folder-mode dead-link detection is slow** (observed 2026-07-18 while
  verifying the wire unification): a sharer whose WebRTC-direct joiner dies
  stays in the dead session well past 45s — heartbeat sends don't error
  promptly on a dead data channel — so a NEW joiner can't connect until it
  notices. Reconnect + catch-up work once it does (and across full restarts).
  Fix candidates: a recv-idle timeout in `folder::session`, or surfacing
  data-channel close from the WebRTC transport as a link error.

- ~~Mesh messages over 4 KiB are silently lost~~ — *fixed, by chunking (no
  reload needed).* Rather than raising iroh-gossip's 4 KiB frame limit (it's
  enforced on receive too, so already-open tabs would error on bigger frames
  until reloaded), every frame stays under the default: `core::sync` gained an
  APPENDED `MeshMsg::Chunk` variant plus `encode_mesh_frames` /
  `MeshAssembler` (content-hash ids so re-broadcasts are idempotent; bounded
  slots + total cap so bogus headers can't balloon memory; nested chunks
  refused), and both drivers broadcast via frames and receive via the
  assembler. A pre-chunking peer skips the unknown variant cleanly — it just
  keeps missing big files, exactly as it always did, and never errors
  (`mesh_msg_indexes_pinned_and_legacy_peers_skip_chunks` pins this). No size
  cap anymore either (~15 MiB reassembly guard). Broadcast failures are
  logged (stderr / console.warn) instead of `let _ =`-swallowed. Verified: a
  60 KB `big.md` (≈16 chunk frames) crosses a CLI mesh byte-identical; the
  browser↔CLI `board-cli-mesh` e2e passes with chunking peers on both ends.
- ~~Mesh drivers: adopt what iroh-gossip already provides~~ — *done.* Both
  drivers source neighbors from `GossipReceiver::neighbors()` (the hand-kept
  sets are gone; the web cell is a mirror for the `connectedPeers()` debug
  accessor); the `NeighborUp` full_state catch-up uses `broadcast_neighbors()`
  (direct neighbors only — the join no longer floods the whole swarm with the
  tree); and on TOTAL neighbor loss a joiner `join_peers()`s its bootstrap
  again (hyparview only refills from its passive view; the bootstrap address
  is still in the MemoryLookup — this is the swarm half of live reconnection;
  exercising it end-to-end needs a persistent-identity host restart, still
  open below). `joined()`/`subscribe_and_join` deliberately NOT adopted: it
  consumes the first `NeighborUp`, which our catch-up logic depends on — the
  library's own docs point to exactly our track-the-stream pattern. Kept
  app-level by design: late-joiner catch-up (gossip is ephemeral), the
  Presence routing map (the library only knows local neighbors), author
  identity (`delivered_from` is the relay, not the author).

- **Protocol version announcements** — *done (2026-07-19).*
  `core::sync::PROTOCOL_VERSION` (v1; v0 = implicit pre-versioning) rides two
  appended variants: `SyncMsg::Version` (sent with the connect handshake on
  the tree link + folder sessions) and `MeshMsg::Version` (broadcast right
  BEFORE every `Presence`). The shared `core::sync::VersionMonitor` (no I/O,
  no clock — callers pass ms) flags a peer once when: it announces a
  different version; a second bare `Presence` arrives with no announcement;
  or the 10 s grace expires (drivers tick it — a quiet 2-peer mesh sees only
  one presence from an old host). Surfacing is library-level and
  app-agnostic: native prints one stable stderr line per peer
  (`protocol-mismatch peer=<hex> theirs=<n> ours=<n>`, parsed by the npm
  wrapper's `onProtocolMismatch`); the browser fires a
  `riftpipe:protocol-mismatch` CustomEvent on `window` + exposes
  `riftpipe.protocolStatus()`. Kanban only renders the event (a banner).
  Verified live: an old (pre-versioning) binary hosting a mesh is flagged
  `theirs=0 ours=1` by a new joiner while files still sync; two new peers
  stay quiet.
- **Text-vs-LWW classification single-sourced** — *done (2026-07-19; found by
  the kanban/library parity review).* The rule (`.md` → text CRDT, else LWW)
  now lives in `core::sync::is_text_path`, used by the native driver, the
  browser's `prime_all`, and a new unified `tree_sync::push(path, bytes)` —
  the app-facing entry; `push_text`/`push_lww` went crate-private. Fixes a
  real divergence: kanban pushed `events/<site>.jsonl` as text-CRDT while
  the CLI and `prime_all` classified it LWW — one path, two merge semantics,
  so a CLI relay could regress event lines. Now every peer classifies
  identically (events ride LWW: one writer per site file, so whole-file
  newest-wins converges). Rest of the parity review was clean: priming,
  tickets, identity, reconnect, merge all live in the library; the TS app
  only renders.

## Security / trust — see `SECURITY.md`

- ~~Mesh sealing (first cut): secret-derived topic + AEAD frames~~ — *done
  (2026-07-19, `PROTOCOL_VERSION` 2).* The share ticket now carries a 32-byte
  random **secret** (`hex(postcard((secret, addr)))`, native + browser
  identical). `core::crypto`: `topic = blake3::derive_key(…, secret)` (blinds
  the rendezvous — a stranger with the host's public key, or a future DHT,
  can't derive the topic) and every gossip frame is AEAD-sealed with a
  domain-separated `KDF(secret)` (XChaCha20-Poly1305, random 192-bit nonce, no
  cross-peer coordination). Both mesh drivers seal on send / open on receive,
  after chunking so it composes with the 4 KiB framing. The web host persists
  `riftpipe:board_secret` so the topic is stable across reloads; "New board"
  clears it (new secret ⇒ new topic ⇒ new link — the rotation-as-revocation
  shape). Verified: a wrong-secret ticket lands on a different topic and never
  syncs; a 12 KB file syncs sealed+chunked; the browser↔CLI `board-cli-mesh`
  e2e passes end-to-end sealed.
- **Signing (insider authenticity) + read/write split** — the next step, NOT
  yet built. Sealing stops outsiders, but one shared symmetric key means any
  *holder* can still forge `Presence`/`Version`, freeze a file (`Lww`
  `version=u64::MAX` — clamp received LWW to `now + ε` too), or replace a doc
  via the origin-conflict rule. A per-message Ed25519 signature makes
  authorship provable; a signing key distinct from the read/seal key enables
  view-only links. See `SECURITY.md` §"the insider gap".

## Iteration 2 (added 2026-07-04)

17. **Events log in the wasm handler** — *done.* The per-peer change log
    (`events/<site>.jsonl` + `.site` id) lost its writer when the servers were
    removed; restored inside `kanban-wasm`'s handler: mints/persists the site id
    (`.site`, 8 lowercase hex), appends one JSON line per mutation in the old
    server's exact shape (`ts`/`site`/`kind` + fields), pushes the log to peers
    as text-CRDT, and serves `GET /api/history` (merged, ts-sorted). Dot-`.site`
    stays local — the web `prime_all` now skips dot-entries like the native
    tree driver's `hidden()` rule. Unblocks the history view.
18. **Per-resource backing in the manifest** — `riftpipe.toml` rules gain an
    optional `backing = "memory" | "file"` per glob (today `--memory` is
    all-or-nothing); `--memory` stays as the global default override.
19. **`lww-record` strategy (resolve the sqlite orphan)** — *done.* Per-key LWW
    merge for `key = value` structural files (the kanban `meta.toml` case:
    concurrent edits to *different* fields both survive; whole-file LWW loses
    one). Shipped as `Kind::LwwRecord` / `algo/lww_record.rs`: `(version,
    value-hash)` stamps per key (rsync's LWW convention), tombstoned deletes,
    canonical sorted rematerialization. The orphaned `algo/sqlite.rs` was
    **deleted** (and rusqlite dropped): its per-cell LWW *model* is absorbed by
    lww_record, but its SQLite-bound machinery didn't fit the byte-snapshot
    `SyncStrategy` seam.
20. **Bounded resync retry + telemetry** — *done.* `Syncer` re-requests a
    failing full resync up to 2 times, then parks the path — exposed via
    `parked_paths()` (core stays no-I/O); the tree driver eprintlns once per
    newly-parked path, and any later successful merge un-parks it.

## Deliberately NOT doing

- **Migrating to Pear/Holepunch** — it's a JS/Bare stack with a log+linearizer data
  model; adopting it means abandoning Rust + iroh + eg-walker, and it doesn't beat
  what we have (iroh already gives browser P2P with nothing hosted). We keep
  eg-walker for text: linearizing flattens the causal DAG the text merge needs.
