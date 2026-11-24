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
   + the two-browser `run-iroh` flow still passes. Reconnecting an *already-joined*
   peer after the host reloads or restarts: see **Live reconnection — done**, below.

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

- **Live reconnection — done.** A peer that reloads, restarts, switches boards in
  its tab, or drops offline rejoins its board and both sides exchange what they
  missed, with no manual re-share.
  - *Reuse the endpoint (D1):* the page binds one `gossip::Node` per key; a board
    switch drops the old `GossipTreeSync` (stop channel → topic left) and
    re-subscribes on the same `Node`, so the key is never rebound in-tab.
  - *Backoff rejoin (D2):* `core::reconnect::Rejoin` (1 s doubling to 16 s, reset on
    any neighbor, `online` ⇒ retry now) re-dials the bootstrap plus a persisted
    **PeerBook** (≤16 recently seen ids per board: `<dir>/.riftpipe-peers`,
    localStorage `riftpipe:peers:<topic>`).
  - *Host finds its joiners (D3):* a restarted host dials its PeerBook at subscribe
    time, and every `NeighborUp` also sends a `SyncMsg::Hello`, so a one-sided
    reconnect still pulls the other side's gap edits.
  - *Swapped-connection re-Join (`core::reconnect::SwapRejoin`):* iroh-gossip's
    hyparview leaves the peer that sent the last `Neighbor` of a handshake
    (normally the joiner) with a stale `pending_neighbor_requests` entry. When the
    other side restarts under the same key within the QUIC idle timeout and
    re-dials, gossip silently swaps the connection in (no NeighborDown/Up), and the
    joiner's reply to the new `Join` is suppressed by that stale entry — the
    restarted host never gets a NeighborUp, the joiner never notices the loss, and
    nothing syncs. Both drivers wrap the gossip protocol handler to see each
    accepted connection's remote id; a connection from a *current neighbor* queues
    a re-Join of it (≥0.5 s later, at most once per peer per 5 s, one log line).
    Proven by `tests/mesh.rs::mesh_host_crash_restart_resyncs_gap_edits` (a
    SIGKILLed CLI host; fails without the fix) and `run-reconnect.sh` /
    `run-cli-host-restart.sh`. An upstream iroh-gossip fix would make it moot.
  - *Hello replies go swarm-wide (native):* iroh-gossip sends `broadcast_neighbors`
    (Scope::Neighbors) to plumtree's EAGER peers only; lazy peers get nothing, not
    even an `IHave`. In a mesh with a cycle, plumtree prunes a fresh neighbor to lazy
    within milliseconds of NeighborUp: a duplicate of any swarm frame from the
    catch-up also arrives through a third peer. A silent connection swap keeps a
    stale lazy entry too. So a neighbors-only reply to a `Hello` (the gap edits) can
    silently miss the asker; this made `run-cli-host-restart-restricted.sh` (three
    peers) fail about 1 run in 6. The native driver now sends a `Hello`'s replies
    swarm-wide, which reaches a lazy peer via `IHave` → `Graft`. The cost: each reply
    (only the asker's diff) floods the swarm once. The `Hello` and the NeighborUp
    `full_state` stay neighbors-only. The browser driver still replies
    neighbors-only. Known limit: the NeighborUp `Hello` itself is neighbors-only
    and can be dropped by the same pruning. With the fix, the e2e failed 1 run
    in 45, with no trace. A general "the request and its reply must reach each
    other" fix (anti-entropy) is planned separately. Draft upstream issue:
    `docs/planned/iroh-gossip-upstream.md`.
  - *Durable sync records (D4):* `Syncer::durable/restore` + one `DurablePath` per
    path in the never-synced `.riftpipe-sync/` (OPFS per board, natively
    `<dir>/.riftpipe-sync/<hex topic>/` — also per board, so a reused dir never
    ships another board's history), restored before the mesh starts — history,
    CRDT origins and LWW versions survive a restart; files edited while down
    become local edits on top. In the browser a merged file and its record go
    through one serialized queue, file first, so a reload can't pair a new record
    with the old file (which would author a revert of the merge).
  - *CLI host identity (D5):* `connect --accept <dir>` persists its iroh key + board
    secret in `<dir>/.riftpipe-host` (restart = same board); `--new-board` mints a
    new link. A `--restricted` host also persists its trust log + epoch
    announces (`auth.state`) and resumes; a startup `--grant` skips devices the
    log already knows; e2e `run-cli-host-restart-restricted.sh`.
  - *Status (D6):* `waiting | connected | reconnecting | offline` (`idle` with no
    session) as the `riftpipe:mesh-status` event + `meshStatus()` in the browser
    (kanban renders a pill), `[mesh] status=<s> peers=<n>` lines natively.
  - *Wire (D7):* unchanged, no `PROTOCOL_VERSION` bump; the `DurablePath` disk
    format is pinned by `durable_record_layout_is_pinned`.
  - *Tests:* `core::reconnect` (incl. `SwapRejoin`) + `core::sync` durable/restore
    unit tests (incl. `host_restart_then_hello_pulls_a_joiners_gap_edits`),
    `sync::tree` record tests,
    `tests/mesh.rs::mesh_host_restart_resyncs_gap_edits` and
    `mesh_host_crash_restart_resyncs_gap_edits`, wasm
    `reused_node_switches_topics_without_rebinding`, and the e2e suites
    `projects/kanban/e2e/run-reconnect.sh` (host reload + long gap, in-tab board
    switch, offline) and `run-cli-host-restart.sh` (CLI host restart, browser joiner).
  - *Known limits:* a crash/reload in the few ms between a merged file landing and
    its record landing can duplicate that one merged edit on the next start (fails
    toward duplication, never loss); no app-level heartbeat or `pagehide` →
    Disconnect, so a joiner notices a vanished host only via QUIC idle (~15–30 s)
    and the pill can read "connected" that long; the legacy WebRTC/signaling
    `connectAndSync` path has no records or reconnection;
    the npm wrapper doesn't parse `[mesh] status=` lines. Pre-existing and
    separate: out-of-order OPFS writes of rapid merges to one
    file, the fresh joiner's `ensureSeed` `board.md` origin race.
  - *Needs real-network verification:* the host re-dialling remembered peers by id
    only (n0 pkarr publish/resolve across real NATs; else recovery waits for the
    joiner's rejoin after QUIC idle), detection latency on real relays, relay-home
    stability across a CLI restart (a stale `MemoryLookup` entry leaves dialling
    to pkarr), real offline (Wi-Fi off, sleep/wake, throttled background tabs —
    Playwright's `setOffline` may not cut an established relay WebSocket), and
    peers on different relay regions / browser ↔ CLI across networks.

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

- **Peers of one board merge their stored trees** — peers prime their stored tree
  on connect (`opfs::prime_all`) and the sync layer unions distinct files +
  resolves a same-path file by origin (`core::sync`). Converges for N peers
  (`three_independent_trees_converge`). **Browser trees are scoped per board**
  (`.boards/<hex topic>/` in OPFS): opening another board's link no longer merges
  this browser's other board into it — that primed a *claimed* board's files into
  any board opened next, as plain Sync. Verified `run-board-isolation.sh` (was
  `run-iroh-merge.sh`): a claimed board's card never reaches another board.
  *Remaining nits:* ~~primed `meta.toml` (LWW) uses wall-clock now() so a same-card
  meta conflict is racy~~ — fixed by durable sync records (a reopened board
  restores each LWW file's version instead of re-stamping it); ~~in-tab reconnect
  from a solo host races the same-key rebind (relay re-registration)~~ — fixed by
  the reused page-lifetime `gossip::Node` (a pasted share link re-subscribes a
  topic on the same endpoint; the key is never rebound). See **Live
  reconnection — done**.

## Architecture / hygiene (from the July 2026 review + restructure)

The restructure made the native binary fully generic (verbs: `share · join ·
connect · serve · signal`; the tree-sync driver is `sync::tree`, generic
static+SSE hosting is `app::host`), and the kanban servers were then removed
entirely — the app's backend runs in the page (since "filify": TypeScript
over riftpipe's wasm file API). What remains:

8. ~~Get kanban out of the riftpipe crates entirely~~ — *done, then taken
   further ("filify the API").* `riftpipe-web` is app-generic (links, gossip,
   `opfs` helpers, `tree_sync` — `BoardSync` is now `TreeSync`, browser ALPN
   `riftpipe/tree/0`) and `riftpipe-core` is CRDT + sync protocol + wal only.
   The browser bundle is now a generic **file library**: `web/src/file.rs`
   exports `readFile`/`writeFile`/`listDir` over the synced OPFS tree
   (`writeFile` persists *and* pushes, channel picked by `is_text_path`), and
   a remote merge fires a per-path `riftpipe:file-changed` CustomEvent (the
   API's `watch`). The kanban board domain — parsing/serializing `board.md`,
   `meta.toml`, `card.md`, comments, card positions — is plain TypeScript
   (`projects/kanban/src/board.ts` over the `fs.ts` file wrapper); the old
   kanban-specific Rust (`format.rs` + `handler.rs`, the in-browser JSON
   "server") is **deleted**. `kanban-wasm` (`projects/kanban/wasm/`) is a thin
   passthrough that links `riftpipe-web` so its exports surface from the ONE
   app bundle; `api.ts`/e2e/`pages.yml` point at `projects/kanban/wasm/pkg`.
   On-disk layout is unchanged, so a board stays portable between the browser
   and a `riftpipe connect` working copy.
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

14. ~~"New board" UX~~ — *done:* a confirm-guarded topbar button drops the
    persisted iroh identity + board secret (new topic ⇒ fresh share ticket and
    a fresh, empty per-board OPFS tree), clears the hash, and reloads as the
    host of an empty board. The old board's files stay in its own tree.
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

- ~~Folder-mode dead-link detection is slow~~ (observed 2026-07-18 while
  verifying the wire unification) — *done.* Both fix candidates landed:
  `folder::session` now has a 15s recv-idle timeout
  (`folder::RECV_IDLE_TIMEOUT`) that treats silence as a dead link even when
  the transport never reports a close, and every heartbeat (5s) opens with a
  `Version` frame so an idle-but-alive, empty workspace still proves it's
  there. The WebRTC transport also now surfaces death directly: a data
  channel's `on_close` and the peer connection reaching
  Failed/Closed both latch recv to end in `None` (link EOF), and ICE
  timeouts are tightened (disconnected 3s / failed 7s / keepalive 1s, down
  from webrtc-rs's 5s/25s/2s defaults) so a vanished direct peer is detected
  faster at the transport layer too.
- **Folder deletes sync** — *done.* `riftpipe_core::sync::SyncMsg` gained an
  APPENDED `StratDelete { path, ids, seen }` variant (wire index 7;
  `PROTOCOL_VERSION` 3 → 4 — one bump shared with the epoch-key mesh seal —
  since a pre-bump peer silently ignores deletes and keeps the files). A
  file-backed resource that was materialized at least once and then goes missing from disk (watcher/poll scan, or a startup
  index comparison after being offline) becomes a tombstone and sends
  `StratDelete`. Delete-vs-concurrent-edit is judged by content via
  `SyncStrategy::fingerprint()` (blake3 of what a replica materializes —
  canonical bytes for `lww-record`, so formatting differences don't count as
  a conflict): a receiver deletes only if its own current fingerprint is one
  the deleter had already seen (`seen`, capped at 8, oldest first); otherwise
  the concurrent edit wins (add-wins, matching Syncthing/Dropbox) and it
  answers with a self-contained delta so the deleter resurrects the file. A
  `StratAdvert` for a tombstoned path re-asserts the delete (how an
  offline-at-delete-time peer learns on reconnect); a non-self-contained
  delta re-asserts too, at most once per heartbeat (loop guard). Re-creating
  a tombstoned path locally drops the tombstone and remembers its whole
  delete/re-create chain as `superseded` (ids + local-only `prior`), so an
  old copy of any of those deletions held by a peer is rejected even for a
  byte-identical re-creation (cost: a lagging peer's deliberate delete can be
  reverted once per chain step — fails toward keeping data). Deletion ids are deterministic
  (blake3 of path + last fingerprint + superseded ids) and independent
  deletes of one path merge their ids, so concurrent deletes followed by an
  identical restore can't lose the restored file. Tombstones + last-known live files
  persist in `<root>/.riftpipe-folder-index` (postcard, atomic write), so
  deletes made or received while offline survive a restart. The
  `sync::tree`/`sync::mesh` (browser/kanban) counterpart is the next bullet.
- **tree/mesh (browser) deletes** — *done* (`PROTOCOL_VERSION` 5 → 6). The
  core `Syncer` that `sync::tree`/`sync::mesh` and the browser/kanban ride
  gained two APPENDED `SyncMsg` variants: `Delete { path, ids, seen, origin,
  vv, version }` (index 8, a tombstone: deterministic deletion ids, blake3
  fingerprints of the deleted content, the deleted text doc's origin +
  frontier, the deleted LWW version) and `Restore { path, ids }` (index 9, "I
  re-created `path` after deletions `ids`"). A v5 peer can't decode them: it
  keeps the file and shows the version-mismatch banner.
  - *Stale-safe rule:* a live holder applies a `Delete` only if it provably
    holds nothing the deleter hadn't seen — text: same origin and no ops past
    the tombstone's `vv`, or its content fingerprint ∈ `seen`; LWW: its
    version ≤ the tombstone's, or fingerprint ∈ `seen`. Otherwise the edit
    wins and it answers with its full state. A tombstoned peer that receives
    content merges it (text with the kept doc's origin) and brings the file
    back only if it carries something the delete didn't see; else it
    re-asserts the `Delete`. Re-creating a tombstoned path locally moves the
    tombstone's id chain into `superseded` (a fresh text doc under agent
    `"{agent}~{n}"`, an LWW version past the tombstone's), and an old
    `Delete` of any of those ids draws `[Restore, full state]` instead.
  - *Catch-up:* `full_state()` ends with every tombstone's `Delete`, so the
    `NeighborUp` catch-up (neighbours only) and `Resync` carry deletes; a
    `Hello` naming a tombstoned path is answered with its `Delete`, swarm-wide
    like every reply. Anti-entropy (item 21) repairs a lost delete in both
    directions: the holder's `Hello` draws the `Delete`, and the tombstoned
    peer's `Hello` lacks the path, so the holder's full copy draws a re-assert.
    `compare_hello` is tombstone-aware; `generation` moves only on a real
    tombstone change; in the outbound queue a `Delete` coalesces per
    tombstone (`del:<path>:<first id>`) and a `Restore` is never coalesced;
    both are sheddable (a shed one is repaired by anti-entropy).
  - *Durable records:* `DurableKind` gained `Deleted { tomb, ops }` (the
    deleted text history is kept), `TextReborn` and `LwwReborn`. A record whose
    file is missing at start is an offline delete; a `Deleted` record never
    brings a file back (a file there was re-created while offline). Native
    records sit in `.riftpipe-sync/<board>/` next to `auth.state`.
  - *Detecting / applying:* native peers count a file as deleted after it is
    missing on two consecutive 1 s checks (an editor's atomic save isn't a
    delete); a remote delete removes the file (an unpushed local edit wins
    instead), prunes emptied dirs, prints `DELETED:<path>` (npm
    `TreeSession.onDeleted`), and a failed remove is retried. The browser
    file API gained `deleteFile(path)` (OPFS removal, dir pruning, dot-paths
    refused); a remote delete removes the OPFS file and fires
    `riftpipe:file-changed`. Kanban deletes for real: `tickets/<id>/` with
    `card.md` first and `meta.toml` last (a card without `card.md` never
    renders; files a concurrent edit kept are listed as leftovers with a
    "Remove leftovers" button), and comments can be deleted (both confirm).
    Repeated delete/re-create/delete rounds: a `Delete` is overruled only if
    every id is superseded, a `Restore` drops a tombstone only if it covers
    all its ids, and re-created paths remember up to 4 earlier text
    tombstones (persisted) so a copy from before an earlier round is answered
    with that round's `Delete` (DESIGN §17.7). A remote delete burst is one
    `mesh.delete` history event per peer.
  - *Proofs:* `core` sync tests;
    `core/tests/anti_entropy_sim.rs::lossy_deletes_converge_and_go_silent`
    (30 % loss through shedding queues: two deletes plus a delete racing an
    edit, which wins; then a lossless run goes fully silent — no Hellos, no
    messages — for the last 300 s) and `::lossy_delete_recreate_delete_ends_deleted`
    (24 seeds, a delete/re-create/delete burst under loss ends deleted);
    `tests/mesh.rs::mesh_deletes_reach_live_and_restarted_peers` and
    `::mesh_lost_delete_is_repaired_by_anti_entropy`;
    `e2e/run-delete-sync.sh` (browser + browser + CLI, an offline-at-delete
    late joiner with a stale OPFS copy, a CLI on-disk delete);
    `e2e/delete-claimed.mjs` (claimed board: an authorized delete lands, a
    removed device's doesn't); `run-restricted-mesh.sh` (CLI, restricted).
  - *Known limits:* no tombstone GC (tombstones, kept deleted histories and the
    `Delete`s in every `full_state` grow); a copy re-seeded without a record
    whose content differs from every deleted fingerprint comes back;
    `superseded` isn't shared with peers that never held the tombstone; the
    legacy signaling WebRTC `TreeSync` has live deletes only (no records); a
    v5 peer keeps a deleted file, and anti-entropy re-asserts the `Delete` to
    it at its backed-off cadence (≤ one exchange per 128 s per peer pair),
    bounded and harmless; moves/renames stay delete + create; a peer that never
    held an earlier round's doc recognizes only copies at exactly that round's
    frontier, and rounds beyond the last 4 are forgotten.

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
  tree); and on TOTAL neighbor loss a peer `join_peers()`s its bootstrap
  again — now with backoff, plus its remembered peers (hyparview only refills
  from its passive view; the bootstrap address is still in the MemoryLookup —
  this is the swarm half of live reconnection, proven end-to-end — see Live
  reconnection, above). `joined()`/`subscribe_and_join` deliberately NOT adopted: it
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
  the app-facing entry (now behind the `writeFile` file API); `push_text`/`push_lww` went crate-private. Fixes a
  real divergence: kanban pushed `events/<site>.jsonl` as text-CRDT while
  the CLI and `prime_all` classified it LWW — one path, two merge semantics,
  so a CLI relay could regress event lines. Now every peer classifies
  identically (events ride LWW: one writer per site file, so whole-file
  newest-wins converges). Rest of the parity review was clean: priming,
  tickets, identity, reconnect, merge all live in the library; the TS app
  only renders.

## Security / trust — see `SECURITY.md` + `docs/security/`

> Consolidated model: [`docs/security/architecture.md`](../security/architecture.md)
> + [`docs/security/user-flows.md`](../security/user-flows.md) (LikeC4-backed).
> Read/write capability split, per-client keys + epoch-key wrapping (read
> revocation), the general-access dial (Restricted / Anyone-with-link
> Viewer/Editor), and the create / open-link / request-access / link-device /
> revoke flows.


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
- **Write signing + trust-log enforcement** — *mechanism shipped
  (2026-07-20, `PROTOCOL_VERSION` 3).* `core::identity` (device keys + the
  signed `TrustLog`) is now wired into the mesh: `MeshMsg::Trust` replicates
  the authorized-device set and `MeshMsg::SignedSync` carries a per-message
  Ed25519-signed write. In restricted mode the native `TreePeer` rejects
  unsigned writes and signed writes from unauthorized devices; open mode (the
  default) is unchanged, so the shipped kanban flow and browser↔CLI sync keep
  working (both APPENDED variants; browsers skip them for now). Enforcement is
  unit-tested at `authorize_and_apply` (authorized accepted, unauthorized/
  unsigned rejected, open-mode unsigned still applies); the wire bump is
  verified by the mesh integration test + the `board-cli-mesh` e2e (open mode
  unbroken). **Usable from the CLI:** `connect --accept <dir> --restricted
  [--grant <device-id>]…` hosts a restricted board (founder = genesis Admin,
  grants authorize writers); joiners run `connect <ticket> <dir> --restricted`;
  `device-id <dir>` prints a device's id to `--grant`. Identity persists in
  `<dir>/.device` (a dot-file, never synced). **E2E-proven over the real mesh:**
  two integration tests (`restricted_mesh_accepts_an_authorized_writer`,
  `restricted_mesh_rejects_an_unauthorized_writer`) and a real-process shell e2e
  (`run-restricted-mesh.sh`) — an authorized device's write syncs; an
  unauthorized device's write is rejected while the founder's write still
  reaches it (proving connection, not a failed link).
  Sealing stops outsiders; this stops unauthorized *writers*. See `SECURITY.md`
  §"the insider gap".
- **Read revocation + onboarding + passkey — lib backends shipped (tested,
  wasm-clean).** Everything a UX needs is now a library primitive (the app just
  renders + confirms):
  - `core::keywrap` — per-client X25519 keys + epoch-key wrapping. Content
    seals under a rotatable epoch key wrapped to each authorized client; revoke
    = rotate + wrap to the remaining. `rotation_excludes_a_removed_client`
    proves read-revocation. (The "key per client".)
  - `core::onboard` — `JoinRequest` (signed; doubles for request-to-join and
    device-linking; `encode`/`safety_code` for QR/out-of-band) + `approve()`
    → a grant + a wrapped epoch key in one admin action. E2E test: approve →
    the newcomer both writes (authorized) and reads (unwraps the epoch).
  - `core::passkey` — WebAuthn assertion verifier (ES256 + EdDSA, pure-Rust,
    no `webauthn-rs`/OpenSSL) + `challenge_of` for the device-id binding.
  **Browser into signed boards — done (2026-07-20).** `web::gossip`'s
  `GossipTreeSync` now carries the same `Auth` (identity + trust log +
  require_signed) as the native `TreePeer`: signs pushes, verifies incoming
  writes, replicates the trust log, catches neighbors up. Device identity +
  the converged trust log persist in localStorage.
  **Claim + request-to-join — done (2026-07-20).** A board is open until its
  first opener **claims** it (`lockBoard()` → becomes the immutable author,
  trust-log genesis broadcast live); everyone else is view-only. A viewer
  **asks for access** (`requestAccess()` broadcasts a signed `JoinRequest` as
  the APPENDED `MeshMsg::Request` variant, re-announced to new neighbors so a
  late-joining owner still sees it); the author's inbox (`pendingRequests()`:
  name · device · safety code) approves with one click (`grantDevice()`), and
  the landed grant prunes the request. `deviceId()`/`authState()` round out
  the wasm surface (on `window.riftpipe`); the kanban auth bar renders the
  states (Claim board / Claimed · you can edit / view only + Ask for edit
  access). **Proven in real browsers** over the sealed mesh
  (`e2e/lock-grant.mjs`: unlocked sync → claim → view-only rejection → ask →
  inbox → approve → edit access; `shot-auth.mjs` screenshots it). Native
  peers keep the same request list (see "CLI request/approve").
  (Driving the mesh seal from epoch keys and approve delivering a wrapped
  epoch key are shipped — see "Epoch-key mesh seal" below; request/approve
  on the native CLI is "CLI request/approve" below; the WebAuthn ceremony is
  "Passkey device-linking" below.)
- **Epoch-key mesh seal — done (`PROTOCOL_VERSION` 4, shared with folder deletes).** `core::epoch::EpochBook`
  wires the content key into the mesh for real: on a **Restricted** board,
  `Sync`/`SignedSync` is sealed a second time under the current epoch key
  (`MeshMsg::Sealed`), identified by an 8-byte key fingerprint so two admins
  rotating at once still agree everywhere on "current." Wrapped keys travel in
  an Admin-signed `MeshMsg::Epoch`: any online Admin rotates on revoke and
  auto add-wraps a newly-granted device without one. Older epochs are kept
  for the session (content sealed under them still opens; out-of-order
  content is parked and replayed once its key arrives); a revoked device
  stays a gossip member (sees control-plane metadata, never the keys).
  **Behaviour change:** a claimed/`--restricted` board now means true
  **no-access** for an ungranted device, not "view only" — see `SECURITY.md`
  §"Restricted boards: epoch-sealed content". **Proven over the real mesh:**
  native (`restricted_mesh_revoked_device_loses_read_after_rotation`,
  `run-epoch-revoke.sh`) and browser (`epoch-revoke.mjs`,
  `run-browser-auth.sh`); live via `riftpipe revoke|grant <dir> <id>` and the
  kanban owner's members-list "Remove" button.
- **CLI request/approve — done.** `connect … --restricted [--name <text>]`
  auto-broadcasts a signed `JoinRequest` (name defaults to `cli`) and
  re-announces it every 15 s while keyless, so an Admin who comes online later
  or isn't a direct neighbor still sees it. Any restricted peer keeps a request
  list (verified, one per device, max 64, cleared on grant) and writes it, with
  the member list, to `.riftpipe-auth.json` (ids, names, roles, safety codes —
  never keys; never synced; stamped with `pid`/`updatedAt` as a 5 s heartbeat
  and removed when the peer stops — the CLI warns on a stale one).
  `riftpipe requests|members <dir> [--json]` read it; `riftpipe approve <dir> <device-id|safety-code>` goes through
  `.riftpipe-control` — the running peer requires a waiting request from that
  device, then grants it (Member) and wraps it into the current epoch. No wire
  change of its own (`MeshMsg::Request` was already pinned).
  **Proven:** `tests/mesh.rs::restricted_mesh_request_then_approve_via_control_file`,
  `run-request-approve-native.sh` (CLI↔CLI), and
  `run-request-approve-browser.sh` / `request-approve.mjs` (browser owner ←
  CLI joiner and CLI owner ← browser joiner). The npm wrapper exposes
  `requests`/`approve`/`members` and `TreeSession.onAccessRequest`.
- **Passkey device-linking — done (`PROTOCOL_VERSION` 5).** The owner claims
  with a passkey ("🔑 Claim with passkey", or "🔑 Add passkey" after a plain
  claim): an Admin-signed `TrustEntry::Passkey` registers the WebAuthn
  credential's public key on the trust log. Another of the owner's devices
  holding the same passkey (platform-synced, a security key, a phone) clicks
  "🔑 Link with my passkey": its assertion over
  `blake3(board, founder, device, nonce)` rides a `TrustEntry::PasskeyLink`, every
  peer verifies it offline (`core::passkey` — signature, `rpIdHash`, UP,
  type, challenge), and the device joins as an Admin with no approve click;
  an online Admin then wraps it into the epoch. The ceremony is generic
  (`web/src/passkey.rs`: `passkeySupported` / `claimWithPasskey` /
  `linkWithPasskey`; RP ID = page hostname, IP origins unsupported). **Proven
  in real browsers** with CDP virtual authenticators (`e2e/passkey-link.mjs`
  in `run-browser-auth.sh`). Not built: passkey revocation, attestation, the
  QR link variant, a CLI ceremony — see `SECURITY.md` §"Passkey device
  linking".
- **Auth & identity** — designed in [`docs/planned/auth.md`](auth.md). Built so
  far (above): device keys + signed trust log, write signing, onboarding, the
  claim/request-to-join flow, the `core::passkey` verifier and passkey
  device-linking; *not* built: Biscuit capabilities. Two planes:
  centralized *issuance* (an identity vouches for you, touched once) +
  decentralized *offline* verification (peers check a Biscuit capability
  against a root public key; data stays P2P). **Cemented identity: passkeys /
  WebAuthn** — no company, no hosting, no committed secret, verified in-core with
  pure-Rust `p256`/`ed25519-dalek`; **Google (OIDC + PKCE)** optional for
  real-account gating. Build order: signing → Biscuit capabilities → passkey
  front door → membership/roles → read-key rotation.

## Iteration 2 (added 2026-07-04)

17. **Event log** — *done, now a generic library primitive.* The per-peer
    change log lost its writer when the servers were removed; it briefly lived
    in `kanban-wasm`'s handler, was dropped with it in the "filify" change, and
    is back app-agnostic in `web/src/events.rs`: each device appends to its own
    `events/<device-id>.jsonl` in the synced tree (one writer per file ⇒ zero
    merge conflicts; rides LWW per `is_text_path`), keyed by the same device
    identity that signs writes. Wasm exports `logEvent(kind, dataJson)` /
    `readEvents()` (every `events/*.jsonl`, merged + ts-sorted), and each
    append fires a `riftpipe:event` CustomEvent for live tailing. The library
    records what only it sees — `mesh.merge` (guarded against `events/*` to
    avoid a merge-about-merge loop) and `auth.claim`/`auth.request`/
    `auth.grant`; the kanban app logs its semantic events from `board.ts`
    (`card.create`/`move`/`check`/`edit`, `comment.add`) via `events.ts`.
    Verified over the real mesh (`e2e/events.mjs`). It is now rendered: the
    kanban Activity panel and per-card History, in TypeScript.
    - Growth controls: the active log rotates into `<device>.NNNNNN.jsonl`
      segments at 16 KiB (appends re-push segments rotated in the last 30 s,
      ≤ once per 5 s each, so a dropped segment push can't wipe a live peer's
      lines); one writer queue per tab; pushes are batched (500 ms trailing,
      ≤ 2 s; `mesh.merge`-only changes ride along or go out after 15 s, so a
      remote merge no longer makes every peer re-broadcast its log — 9→~5
      event pushes per board write on 5 peers, far fewer under bursts);
      `mesh.merge` is logged at most once per path per 10 s; `card.edit` is coalesced app-side, per board, and flushed
      on board switch / tab hide. Malformed lines are skipped (`readEvents`
      keeps objects; the app keeps string `ts`/`device`/`kind`).
    - Event data can't overwrite `ts`/`device`/`kind`; grant/revoke log
      `target`.
    - Sealed like content on claimed boards (`e2e/history.mjs`).
    - *Remaining:* old segments are never deleted (now buildable on
      `deleteFile`);
      sibling tabs of one profile can still race.
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
21. **Mesh anti-entropy** — *done.* Gossip delivery is best-effort, and a
    dropped broadcast used to be repaired only by the next `NeighborUp`. Now
    each mesh driver (`src/sync/mesh.rs`, `web/src/gossip.rs`) periodically
    sends its `Hello` (version vectors + LWW inventory — the digest) to direct
    neighbors, scheduled by `core::antientropy::AntiEntropy` (Trickle-style,
    seeded jitter): 2 s doubling to 128 s; a state change (the Syncer
    `generation` moved) resets it once quiet for 1.5 s (or after 10 s of
    continuous activity); nothing is sent while nothing changed and every
    neighbor's last Hello matched ours (`Syncer::compare_hello`), and a
    neighbor that just became confirmed gets one Hello back so it can confirm
    us too; a mismatched Hello resets it at most every 8 s → 128 s, and not at
    all while our own state is still changing (two editing neighbors would
    otherwise ping-pong at the 1 s floor); ≥ 1 s between Hellos. The side
    that is ahead answers with exactly what the other lacks; the side that is
    behind answers with its own Hello. Every reply goes **swarm-wide**:
    iroh-gossip sends neighbors-only messages to plumtree-eager peers only, and
    the requester may already be lazy on the replier's side (pruned right after
    NeighborUp, or a stale one-sided entry after a same-key restart).
    - One ordered outbound queue per driver (`core::outqueue`) drained by one
      task: a message's chunk frames go out back-to-back (concurrent pushes used
      to interleave frames and evict half-built messages at receivers); a
      queued LWW update — a local push or a Hello reply — is replaced by a
      newer one for the same path; over 4 MiB the oldest content message is
      shed (auth messages and the `Version` announce never). A clean native
      shutdown flushes the queue (≤ 2 s).
    - `MeshAssembler` has 64 slots and counts evictions; an eviction, a shed
      message or gossip `Lagged` forces an anti-entropy round, at most every
      8 s → 128 s while losses keep coming.
    - A merge that adds no ops persists nothing (duplicate deltas are common
      once several neighbors answer one Hello); a failed disk write is logged
      and retried on the next message instead.
    - No wire change (`PROTOCOL_VERSION` stays 5); v5 peers without it still
      answer Hellos. Browser diagnostics: `meshStats()`.
    - Tree/mesh deletes (v6, the "tree/mesh (browser) deletes" bullet) ride it
      unchanged: `Hello` stays wire-identical, `compare_hello` is
      tombstone-aware (a tombstoned path they still hold counts as us ahead),
      a lost `Delete` is repaired by the holder's `Hello` drawing it, a
      `Delete` coalesces per tombstone and a `Restore` never does.
    - Tests: `core/tests/anti_entropy_sim.rs` (5 peers, 30 %/50 % loss, a
      120-op burst, lazy plumtree edges incl. a one-sided stale one, shedding
      bandwidth-limited queues, Hellos during activity, idle silence; a control
      without anti-entropy never converges);
      `tests/mesh.rs::mesh_lww_burst_converges_without_a_settle_write` (the
      receiver discards the whole burst, so only anti-entropy delivers it);
      `e2e/history.mjs` (120-append burst, no settle event; converges ~2 s
      after the burst); mesh-n pass rate via `e2e/run-repeat.sh`, 10 runs
      interleaved with a baseline at load ≤ 12: **10/10 vs 4/10** on the
      pre-history-view base (63ff42e), and **10/10 vs 10/10** after merging
      the batched event-log pushes (f849fce) — batching alone removed most of
      the contention mesh-n hit, so mesh-n no longer separates the two there.
      In every passing run `meshStats` showed 0 evictions, 0 `Lagged`, 0 shed.
    - *Follow-ups:* a compact digest (`SyncMsg::Digest`, a protocol bump) if a
      real board's Hello exceeds 16 KiB (250 paths measure ~9.5 KiB);
      anti-entropy for the trust log and epoch announces (still repaired only
      on NeighborUp, though never shed); dropping the NeighborUp `full_state`
      flood; coalescing consecutive TextDeltas per path in the queue; removing
      the event log's segment re-push (now redundant); per-peer exact version
      vectors (a frontier-tip Hello still draws FULL docs for paths that
      differ); older v5 peers don't originate anti-entropy; link-mode
      `tree::run` and WebRTC `TreeSync` (reliable streams) are untouched; no
      byte cap or expiry on `MeshAssembler`; a peer whose neighbors never
      confirm it (lazy edges, v5 peers) still sends one Hello per 64–128 s.

## Deliberately NOT doing

- **Migrating to Pear/Holepunch** — it's a JS/Bare stack with a log+linearizer data
  model; adopting it means abandoning Rust + iroh + eg-walker, and it doesn't beat
  what we have (iroh already gives browser P2P with nothing hosted). We keep
  eg-walker for text: linearizing flattens the causal DAG the text merge needs.
