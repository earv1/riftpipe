# riftpipe — Design Doc

**Status:** Core working — eg-walker text (diamond-types) + iroh transport, with
mock + real integration tests passing
**Last updated:** 2026-06-26

A Unix-like terminal construct for live, peer-to-peer collaborative sharing of
text — "pipe a document to other computers and converge." Think `tee`/`tail -f`,
but the buffer is a shared, multi-writer document that merges edits from every
machine with no central server. Beyond text, the same substrate is a
**programmable convergent state machine**: a shared task tracker, a permissioned
column store, etc., are all just different *rule-sets* plugged into one engine.

---

## 1. Vision

```sh
# machine A — mirror a shared doc into a local file, live
riftpipe join <ticket> > notes.txt

# machine B — edit the same doc; both sides converge
riftpipe share notes.txt        # watches + syncs on save
```

Two terminals on two machines hold the same document. Edits made anywhere merge
everywhere. No server in the path. The tool feels like plumbing, not an app.

---

## 2. Locked decisions

| Area | Decision | Why |
|---|---|---|
| **Language** | **Rust** | Best CRDT/eg-walker libraries, single static binary, pairs with iroh. |
| **Document type** | **Text sequence** (v0) | The shape we're chasing; log/KV shapes plug into the same seam later. |
| **Merge engine** | **Eg-walker** (Event Graph Walker) | OT-like memory efficiency *with* CRDT-like decentralization. Build on / draw from **diamond-types**. Ref: Gentle & Kleppmann 2024. |
| **Topology** | **No server / p2p** (held) | This constraint is what *forces* eg-walker. We deliberately keep it. |
| **Transport** | **iroh** (QUIC) | NAT hole-punching + relay fallback + roaming + tickets, in-language. |
| **Input model** | **Diff-to-ops** (snapshot reconciliation) | Recover edits by diffing snapshots against a per-producer **base**. No editor plugin needed. |
| **Output model** | **Snapshot stream**, behavior by destination | TTY → repaint; file → seek/truncate/rewrite; pipe → snapshots + `--patch`. |
| **Rules** | **Pluggable deterministic guard** above the engine | Lets us express turn-based games, edit permissions, etc. (§6). |
| **Rule compat** | **Separate fingerprint, checked in handshake** + **mandatory simulation** | Catch incompatible/heterogeneous clients before syncing (§7–8). |

---

## 3. Input: diff-to-ops against a moving base

We do **not** require explicit `insert/delete` events. A producer emits full-text
snapshots; we recover edits:

```
ops = diff(base, new_snapshot)     # base = the version the producer last saw
apply ops onto CURRENT doc state    # which already merged remote edits
base = new_snapshot
```

**Critical rule — diff against the base, not the live state.** A remote peer may
have edited since the producer's last snapshot; diffing against current state
would "delete" the remote's text and break convergence. Keep a per-producer base
and reconcile 3-way (same reason Git needs a merge base). **First thing to
prototype.** Cost is a non-issue (Myers `O(ND)`); the hazard is correctness.

---

## 4. Output: resolve the pipe/document tension by destination

Detect destination via `isatty`:

- **TTY** → repaint the materialized document (we own the screen). North star:
  Mosh-style predictive local echo (post-v0).
- **Regular file** → `seek(0)` + truncate + rewrite on each change. `join > file`
  gives a live-mirrored file. **Likely the killer demo.**
- **True pipe** (`| grep`) → successive full snapshots; `--patch` emits a diff
  stream consumable by `riftpipe apply`.

---

## 5. Architecture seam (from Mosh's SSP)

A *generic* state-sync transport parameterized over a document object that knows
how to `diff`/`apply`/`materialize`; plus a deterministic **guard** layer:

```
+-------------------+      +-------------------+      +----------------------+
| transport (iroh)  | <--> | engine            | <--> | document object      |
| - QUIC, no HOL    |      | - event graph     |      | - eg-walker text v0  |
| - NAT/relay/roam  |      | - deterministic   |      | - apply(op)          |
| - tickets         |      |   total-order     |      | - materialize()      |
| - opaque sync msg |      |   replay          |      | - diff(base,new)     |
+-------------------+      | - guard hook ↓    |      +----------------------+
                           +---------+---------+
                                     v
                           +-------------------+
                           | rule / guard      |
                           | validate(op,state)|
                           +-------------------+
```

Transport doesn't know it carries text. Engine replays the event-graph DAG in a
deterministic total order (Lamport + agent-ID tie-break), runs the guard per op,
and materializes the document.

### What goes over the wire
Operations / event-graph entries, not full state. New peers backfill via a
**state-vector handshake** ("I have agent A up to seq 100, send the rest").

### Lessons carried from Mosh
- **Coalesce your own local run** before broadcasting — but **never drop a peer's
  concurrent ops** (Mosh can discard intermediate states only because it's
  single-writer; we are multi-writer).
- State-diff sync is proven in the single-writer regime; our novel surface is
  making it work when the base is edited from both ends — that's what eg-walker
  buys.

---

## 6. Programmable rules (the guard layer)

A rule-set is a **pure deterministic guard** `validate(op, state_before) ->
Accept | Reject`, run **during deterministic total-order replay**. Because the
order is deterministic and the predicate is pure, every honest peer reaches the
identical accept/reject verdict → convergence holds even though some ops are now
rejected. This turns riftpipe into a programmable convergent state machine;
text is just the rule-set "always accept."

Examples: turn-based game (a single **turn token** in shared state; valid iff
`author == turn_holder`), columnar edit permissions (valid iff
`author ∈ writers[column]`).

### Honest-peer model
Both peers run the same rules and govern themselves. A deterministic guard means
honest peers **auto-reject** a cheater's illegal ops and stay consistent with
each other; the cheater only diverges itself. Out of scope: equivocation/forking
(needs signed, hash-chained events — Byzantine territory).

### Coordination — PARKED (out of scope)
General invariant enforcement runs into **invariant-confluence**: rules that
bound a scarce resource (uniqueness, balance ≥ 0, "at most N", "exactly one
winner") cannot be enforced coordination-free; they'd require escrow/tokens or
detect-and-compensate. **We deliberately don't build non-confluent rules.** The
rules we care about (turn token, static permissions) *are* I-confluent → stable,
never-revoked, self-governing. Revisit only if a scarcity rule is ever added.

### What rules do NOT give us
Convergence ≠ intention preservation. Everyone agrees on the same state; that
state may not match what anyone *meant*. We accept syntactic convergence only.

---

## 7. Identity & pairing

"Client id" is really **three** distinct identities; keeping them separate
prevents three distinct failure modes:

| ID | Answers | Prevents | Lifetime |
|---|---|---|---|
| **Agent ID** | who authored this op? | two writers colliding → corrupts `(agent, seq)` tie-break | per replica, persisted |
| **Doc ID** | which shared object? | syncing the wrong document together | per document |
| **Rule fingerprint** | what semantics govern it? | same doc, incompatible rule-sets → guard diverges | per rule-set version |

Scheme:
- **Agent ID** = **ed25519 keypair** generated on first run, persisted. (UUID
  would do for honest-peer v0, but a keypair is nearly free and is the one thing
  you can't retrofit — enables signed ops / equivocation detection later.)
- **Doc ID** = 256-bit secret minted by the creator. Being secret, it doubles as
  the **access capability** (wormhole model). Carried in an **iroh ticket**:
  `{ doc_id, rule_fingerprint, creator NodeId / relay hint }`.
- **Rule fingerprint** = hash of the **conformance spec / suite** (NOT the
  binary), so it tracks *semantics*: cross-language impls that pass the same
  suite legitimately share an identity.

---

## 8. Rule compatibility & handshake

**Decision: separate fingerprint, checked in handshake, AND a simulation is
always run as part of every handshake** (not just a fallback).

Handshake phases:
1. **Hello** — exchange `{ doc_id, agent_id, rule_fingerprint }`.
   - Doc mismatch → refuse (`DocMismatch`).
2. **Simulation (mandatory)** — both sides run the shared **conformance suite**
   (golden vectors: `ordered ops → expected final-state hash + accept/reject
   trace`) and exchange the transcript hash.
   - Agree → **Compatible**, proceed to sync.
   - Diverge → refuse and **name the diverging vector** (`SimDiverged{vector}`) —
     a real diagnostic, not a silent "no peers."
   - Run even when fingerprints match (defense in depth: catches
     nondeterminism/platform bugs between "identical" builds).

Why simulation, not just the hash: a binary hash only catches byte-identical
rule-sets — too strict (rejects compatible cross-language impls) and too weak
(says nothing about behavior). The suite is a **complete behavioral spec**
because the guard is a pure function of the ordered event graph: two impls are
interoperable iff they agree on every vector.

**Caveat:** finite vectors give *confidence, not proof* — untested inputs could
still diverge. Compatibility spectrum, weakest → strongest:

| Approach | Guarantee |
|---|---|
| Byte-identical same-lang build, hash only | exact (trivially), no cross-impl |
| Different native impls + conformance suite | high confidence, **not proof** |
| **One portable artifact both run (WASM)** | **guaranteed identical behavior** |

Long-term mechanism: distribute rule-sets as **WASM** so every client runs *the
same code*; `rule_fingerprint = H(wasm)` collapses to an exact check, and
pluggable rules become portable, sandboxed blobs shipped in the ticket. Keep the
conformance suite regardless — it's the spec/regression harness.

---

## 9. Simulation engine (one engine, three modes) — REMOVED

> **Removed from this repo** (commit `10c385e`, July 2026): the game/simulation
> engine belonged in a separate project. Kept here as the decision record; no
> `simulation` module exists in `src/` anymore.

Simulation is not new machinery — it's the deterministic replay engine run with
no commit and no network. Three uses of one loop:

1. **Live** — materialize the doc from the event graph (+ transport).
2. **Conformance** — replay golden vectors, compare state hashes (the handshake
   gate, §8).

---

## 9b. Programmatic peers / headless clients (locked requirement)

A peer must not require a human at a TTY. A **program** must be able to drive a
peer over a pipe — reading converged state from stdout, writing actions to stdin.
The control logic of a client is itself a pipe-connected process. This is the
original Unix-pipe vision applied to *control*, and it reuses the Mosh-SSP
action-in/state-out seam (§5).

- **Swappable frontend:** human TUI and external logic program are
  interchangeable on either side of the client core. Same machine protocol.
- **Protocol:** newline-delimited JSON. Inbound `{"action":...}`, outbound
  `{"state":...}` / `{"event":...}`.
- **Unlocks:** AI/bot players; *system logic as a peer* (e.g. the TD enemy/wave
  controller is its own headless peer); scripted automation and deterministic
  testing.
- **Still governed:** a programmatic peer is just another agent id, subject to
  the same deterministic guard (§6) — it cannot cheat. It connects through the
  same iroh/`Link` layer (§5), so it can live on another machine.
- **Build implication:** the demo's player-client is built behind the
  action-in/state-out seam from the start, so a piped bot is "attach to the pipe,"
  not a rewrite.

## 10. Open questions (still unlocked)

1. **Local-edit entry point** — (a) re-pipe a file / (b) `tail -f` append stream
   / (c) **watch a file on disk**. *Leaning (c)* — pairs with file-mirror output
   for "two machines edit the same file in `$EDITOR`, converge live."
2. **Multi-writer topology** — full mesh vs hub-and-spoke through the creator.
   *Leaning spoke for v0*, mesh later. (Transport already supports pairwise +
   broadcast; mock bus proves N-client convergence.)
3. **`--patch` stream format** — custom `@@`-style vs reuse an existing format.
4. ~~Adopt diamond-types directly vs roll our own eg-walker core.~~ **RESOLVED:**
   adopted **diamond-types** (`src/text.rs`).

## Progress (implemented)

- **`src/text.rs`** — real eg-walker text doc (diamond-types): snapshot
  diff-to-ops input (§3), encode/merge sync, proven concurrent convergence.
- **`src/net/`** — transport-agnostic `Link` seam + in-memory mock transports
  (`mock_pair`, `MockNet` broadcast bus). The shared `sync::sync_full` driver
  runs over mock and real links alike — this is what makes integration tests
  cheap.
- **`src/transport.rs`** — real **iroh 1.0** `Link` (QUIC bi-stream, length
  framing, graceful `done()` teardown — note: must `finish()` the stream or the
  last message is reset on drop).
- **`src/textpipe.rs`** — live collaborative text pipe (the eg-walker hero
  demo): file-as-view, diff-before-merge rounds (§3). `share`/`join` run it over
  iroh; verified end-to-end with two real processes editing & converging a file.
- **`src/secure.rs`** — "connect anywhere securely". **Encryption is free** from
  iroh (QUIC/TLS 1.3, endpoints authenticated by ed25519 key = no MITM); relays +
  discovery give NAT-traversing reach by identity. This module adds
  **authorization**: a 256-bit secret capability in a compact base32 ticket +
  BLAKE3 challenge-response (`authenticate`) so only secret-holders may join.
- **Tests:** `tests/mock_sync.rs` (2- and 3-client convergence, no sockets) and
  `tests/iroh_real.rs` (two real iroh endpoints over loopback).
- **Two-path architecture note (refines §5):** plain text rides diamond-types'
  *own* event-graph replay (no guard); the generic guard engine (`engine.rs`) is
  for *ruled* documents (turn-based / permissions). Both are deterministic
  event-graph replay — one specialized, one generic.

---

## 11. Stack summary

- **Merge:** eg-walker text core (diamond-types lineage)
- **Transport:** iroh (QUIC) + iroh-gossip
- **Diffing:** Myers (`similar` crate) for snapshot → ops
- **Identity/crypto:** ed25519 (agent keypair), blake3 (fingerprints/state hashes)
- **Rules:** deterministic guard; WASM-distributed long-term
- **Glue:** base bookkeeping (§3), output-mode dispatch (§4), handshake (§8)

---

## 12. References

- Gentle & Kleppmann, *Collaborative Text Editing with Eg-walker* (2024)
- diamond-types (Seph Gentle)
- Winstein & Balakrishnan, *Mosh* (USENIX ATC 2012) — SSP state-diff sync
- Bailis et al., *Coordination Avoidance in Database Systems* (VLDB 2015) —
  invariant confluence (parked, §6)
- Fraser, *Differential Synchronization* (2009) — considered, set aside
- iroh — https://iroh.computer

---

## 15. Editor bridges & the sync protocol (Unix decomposition — IMPLEMENTED)

**Status:** `--pipe` core is event-driven (no lockstep, idle = silent); the
neovim bridge (`nvim/riftpipe.lua`) is built and verified. Local→core uses
snapshots (phase 1; core diffs them); core→local uses granular ops applied via
`nvim_buf_set_text` (cursor-preserving). Phase 2 (granular local→core via
`on_bytes`) is the remaining optimization.


**Direction:** keep the sync core **editor-agnostic** and expose a **local
edit-stream protocol**; editors connect via small, *separate* **bridge tools**.
This is §9b (programmatic peers) applied to editing — the editor integration is
its own composable Unix tool, not baked into the core.

```
nvim  <--msgpack-RPC-->  riftpipe-nvim (bridge)  <--edit-stream-->  riftpipe sync  <--iroh-->  peer
```

### Boundary protocol (the key artifact)
Bidirectional, line-delimited edits between bridge and core (stdio or a unix
socket). Char-offset coordinates into the whole document (one agreed system; the
bridge converts editor line/col ↔ char offset):
- `{"op":"snapshot","text":"..."}` — full state (initial / resync)
- `{"op":"insert","pos":N,"text":"..."}`
- `{"op":"delete","pos":N,"len":M}`
- *(later)* `{"op":"cursor","pos":N}` — peer cursors

### Core "pipe mode"
`riftpipe sync --pipe`: read local edits from stdin → apply to the eg-walker
CRDT → sync to peer; write remote edits to stdout → bridge applies them. Makes the
core a CRDT-sync daemon any frontend can drive (the file-mirror and TUI become
just two built-in frontends; bridges are external ones).

### The nvim bridge (`riftpipe-nvim`)
Connect to a running nvim (`$NVIM` / `--listen` socket); `nvim_buf_attach` →
local edits → core; core's remote edits → `nvim_buf_set_text`. Separate binary;
other editors (vscode, emacs, helix) get their own bridge without touching core.

### Phasing
- **Phase 1 — snapshot bridge:** on each buffer change send the whole buffer as a
  `snapshot`; core diffs (existing `edit_to`) + syncs; remote merged snapshot →
  set buffer. Simplest, reuses what we have; coarse cursor/undo.
- **Phase 2 — granular ops:** map nvim line-change events to char-offset
  insert/delete, apply remote ops via `set_text` — preserves cursor/undo,
  char-level live sync.

### Known challenges
- **Coordinate mapping:** editor line/col ↔ CRDT char offset (UTF-8/grapheme).
- **Echo suppression:** applying a remote edit fires nvim's change event — must
  not loop it back as a local edit (guard flag / track expected changes).
- **Cursor preservation:** snapshot-replace resets the cursor; granular
  `set_text` avoids it.

---

## 14. Connection strategy, observability & layered state (planned — under discussion)

### 14.1 Prefer direct, warn on relay
- **Goal: reliability + independence from n0.** Attempt a **direct** connection
  first using the ticket's direct addresses (bypassing the relay); use it when it
  works. Fall back to the relay only if direct fails.
- **Surface the mode.** Show a **warning when the connection is relayed** (n0 sees
  metadata + your IP, possible rate limits — §security). Proceed quietly when
  direct; ideally print "relay → upgraded to direct" when holepunch succeeds.
- **Honest limit:** two peers both behind symmetric NAT / CGNAT *must* use the
  relay to coordinate (NAT theorem) — bypass works for LAN / friendly-NAT /
  public peers, not the worst-case pair. The warning is exactly for that case.
- *Open:* confirm the intended meaning of "warning" — warn on relay fallback, or
  warn whenever bypassing? (Leaning: warn on relay.)
- **Generalization → transport negotiation & multi-peer.** §14.1's direct/relay
  choice is the bottom of a transport *ladder* (`webrtc-direct → iroh-direct →
  iroh-relay`). The connect handshake (§8) now carries a **capability exchange**
  that picks the highest mutually-supported rung, with the iroh `Link` as the
  always-reachable floor — the seam the browser (iroh-signals/WebRTC-data) build
  and the N-peer fan-out plug into. Design + multi-peer thinking:
  [`docs/planned/transport-negotiation.md`](docs/planned/transport-negotiation.md).
  *Status:*
  - Capability negotiation (`net::negotiate`, `CAPS` over the link) — **implemented
    and wired into every handshake** (`--pipe`, folder, file-mirror).
  - WebRTC data-plane `Link` + iroh-brokered non-trickle signaling
    (`net::webrtc`, `webrtc-rs`) — **implemented**.
  - **Session runs over the negotiated `Link`** (`net::{Sink, Source}` halves for
    both iroh and WebRTC; `net::negotiate::negotiate_session_halves`), and
    `Caps::native` advertises `WebrtcDirect` — so **native↔native now upgrades to a
    WebRTC data channel end-to-end**, with the iroh link kept alive as
    control/fallback and a transparent fall-back to iroh on upgrade failure.
  - STUN/TURN are **env-configurable** (`RIFTPIPE_STUN` / `RIFTPIPE_TURN[_USER|_PASS]`);
    default is host candidates only (LAN/loopback/public-IP).
  - **Native tests:** `net::negotiate` + `net::webrtc` unit tests, and an
    end-to-end integration test (`tests/networking.rs`) over a real loopback iroh
    connection: auth → caps → WebRTC upgrade → data over the negotiated transport.
  - **Browser `web-sys` `Link`** (`web/` crate) — the wasm counterpart of
    `net::webrtc`, **implemented and verified headlessly**: `web/test-headless.sh`
    runs the establishment test in real (headless) Chrome via `wasm-bindgen-test`
    (it auto-fetches a chromedriver matching the local Chrome build, since
    `wasm-pack` otherwise force-uses a mismatched latest driver). The `web/` crate
    is `[workspace] exclude`d from the native build.
  - *Remaining:* wire the browser `Link` to an iroh-over-WebSocket signaling link
    in the wasm app (the kanban integration); multi-peer fan-out stays design-only.

### 14.2 Observability — metrics via tmux (REVISED: no in-app TUI)
**Pivot:** an in-app crossterm TUI compositor (drawing the doc + an overlay) was
built then **removed** — once the editor (nvim via `--pipe`, §15) is the document
UI, riftpipe drawing the document is cruft. Instead: **riftpipe renders
nothing; tmux is the compositor.** A decoupled side-car task (`src/metrics.rs`)
writes a one-line status to a file every ~0.5s — connection `direct`/`⚠RELAY`
(§14.1), bytes up/down, sync rate — and tmux shows it in a **thin pane per peer**
(`run-local.sh`). Instrumentation kept: `CountingLink` (byte counters),
`connection_kind` (live path). Removed: `compositor.rs`, `tui.rs`, `--tui`,
crossterm dep. This is the first concrete piece of *local, not shared* state and
stays fully outside the synced plane (§14.3).

### 14.3 Layered state: shared vs local (the "exclude things" worry)
**Concern:** we've conflated "the document" with "what's synced" and "what's
displayed." Separate three planes:
1. **Shared plane** — CRDT-replicated content synced to peers (possibly
   region-owned, §6/§13).
2. **Local plane** — per-client ephemeral state, **never synced**: stats HUD,
   status/help messages, local cursor/selection, private UI.
3. **Composite/render plane** — what the user sees = shared composited with local
   overlays.

"Exclude certain things" splits into two distinct mechanisms (disambiguate
before building):
- (a) **Local-only overlays** — never shared (stats, messages, cursors).
  Composited at render time. Easy; the stats HUD is exactly this.
- (b) **Selective sharing** — excluding/redacting parts of the *shared* doc from
  sync, or showing different peers different views (partial replication /
  per-peer filtering). Harder; touches the CRDT + permissions (§6).

**DECISION: (a) local-only layers. (b) selective sharing is deferred** — it's a
can of worms: it breaks convergence (per-peer filtered replicas = partial
replication), read-hiding needs per-region *encryption keys* (not just the guard;
write-perms are doable, read-redaction is not without crypto or a trusted
redacting server), and it tangles causality (ops referencing redacted data).

**Principle that makes (a) safe — exclusion by construction, not by policy:**
local state has **no route to the wire**. Model shared vs local as a *structural
distinction* (two containers/types), where only the **shared plane** has a
serialization path to the `Link`; the **local plane** has *no encode path at
all*. The sync loop can only see the shared plane, so local state cannot leak —
far safer than a per-item `synced: bool` flag. `render = composite(shared,
local_overlays)`.

**Accepted limitation:** everything shared is visible to all peers — **no hidden
shared state** (no fog-of-war, secret hands, per-peer secrets). Those need (b).
Anything with only public shared state is unaffected: all peers see everything;
ownership controls who may *write* each region (the guard), not who may *see* it.

**Output implication:** a flat file can't carry an overlay, so:
- **file-mirror mode** stays as-is (no overlays, scriptable, pipe-friendly).
- **TUI mode** adds a compositor: shared doc + local overlays (stats HUD,
  messages) drawn together, overlays never synced.

---

## 16. Desync handling — reconciliation (IMPLEMENTED, partial)

**Where desync comes from:**
- *In-stream loss/reorder:* impossible — QUIC streams are reliable + ordered, so
  while connected the CRDT always converges.
- *Reconnection:* a dropped+re-established connection misses the deltas sent while
  down.
- *Bridge snapshot race:* the nvim bridge sends whole-buffer snapshots; if you
  type in the window after riftpipe merged a remote op but before the bridge
  applied it, the snapshot diff can delete the remote edit → lasting divergence.

**Mechanism (version-vector reconciliation):** peers exchange a compact **version
vector** — the diamond-types frontier as portable `(agent, seq)` pairs
(`version_vector()`), usually one or two entries. The receiver computes exactly
the ops the sender is missing (`ops_since()` → map their frontier into our frame
best-effort, `iter_range_since` to test for any ops, `encode_from(ENCODE_PATCH)`
to encode them) — `None` when caught up (sends nothing), the full history for a
fresh peer. `decode_and_add` is idempotent, so a resync is always safe.

**Wire framing:** link messages are tagged — `DELTA` (ops → merge) vs `SYNC`
(version vector → reply with the missing ops).

**Triggers (the user chose both):**
- **on connect** — recovers state after a reconnect / seeds a late joiner,
- **after edits settle** — debounced ~500ms (activity-triggered),
- **heartbeat** — every 5s (periodic). The vector is tiny, so idle traffic is a
  few bytes every 5s (near-silent, not absolutely silent — the trade for
  continuous detection).

A `SYNC` exchange both *detects* and *heals* in one step: if the peer is missing
ops they arrive; if not, nothing is sent.

**Reconnection (IMPLEMENTED):** `--pipe` now survives a dropped link. The
document + stdin/stdout pipe persist for the process lifetime (`PipePeer` +
`stdin_ops()` channel); a `session` runs over one link and returns `LinkClosed`
on drop (never a hard error). `run_pipe_reconnecting` loops: (re)dial/accept →
auth → run session → on drop, back-off and reconnect — and the **on-connect
SYNC reconciles** whatever was missed. Edits typed during the gap queue in the
stdin channel and apply on resume. Fast drop detection comes from a short QUIC
liveness config (keep-alive 2s, idle timeout 6s — vs the ~30s default).

Verified end-to-end: a peer is killed and restarted; the survivor stays alive,
detects the drop, and re-syncs the returning peer.

---

## 17. Folders & pluggable sync algorithms (SCAFFOLDED — rsync implemented)

riftpipe began as one document with one CRDT. The next step is a **folder**: a
tree of heterogeneous resources where *different files want different sync
algorithms* — prose merges char-by-char, a binary blob wants efficient
replication, a database wants append-only log shipping, an image wants tiles.
Syncing a folder also cleanly solves the game problem: keep **state** (a
db/log resource) separate from a **view** (a text resource), each on the
algorithm that fits.

### 17.1 The seam — Strategy, not "the adapter pattern"

The organizing pattern is **Strategy**: a family of interchangeable algorithms
behind one interface, selected at runtime. *Adapter* is the inner role each
strategy plays when it wraps an existing library (the text strategy adapts
diamond-types). The `Kind` enum + factory is a small **Abstract Factory**.

The trait (`sync::strategy::SyncStrategy`) is kept to the **minimal, reconcile-centric**
contract the network layer truly needs — *advertise what you have*
(`state_vector`), *answer "what are you missing"* (`delta_since`), *merge an
opaque delta* (`merge`), plus an eager push path (`observe`/`push_delta`) for
algorithms that can. It deliberately avoids a snapshot-in/out shape, which would
distort non-snapshot algorithms (a WAL tails records; rsync negotiates via block
checksums). All payloads are opaque bytes, so a new algorithm is one `impl
SyncStrategy`, nothing else. It is object-safe so heterogeneous resources can
share one link in a `HashMap<ResourceId, Box<dyn SyncStrategy>>`.

`Kind`: `text-crdt` (done) · `rsync-file` (done) · `wal-db` (stub) · `image`
(stub). Stubs `todo!()` their sync methods so a misconfigured manifest fails
loudly, never silently corrupts.

### 17.2 rsync (IMPLEMENTED)

Classic rsync over opaque byte buffers (`sync::algo::rsync`):
- **signatures** — split content into fixed blocks (`BLOCK = 1024`); per full
  block a rolling **weak** checksum (adler-style, advances one byte at a time)
  + a strong **blake3** hash. Advertised as the `state_vector`.
- **diff** — the other side rolls a window over *its* content; on a weak (then
  strong) match it emits `Copy(block)`, gaps become `Literal(bytes)`. This is
  `delta_since`.
- **reconstruct** — rebuild from the advertiser's blocks + literals (`merge`).

Wire format is **postcard**, not JSON — JSON balloons `Vec<u8>` literals into
number-arrays (a one-block change serialized larger than the file). The
block-reuse test asserts the delta stays a fraction of the file.

**rsync is replication, not merge.** It makes one buffer equal another; run
bidirectionally on divergent content it would swap forever. So each replica
carries a `(version, content-hash)` stamp and we apply **last-writer-wins**: a
local change bumps `version`; ties break on the larger hash → a deterministic LWW
register that converges. **v1 caveat:** a `Copy` references the *advertiser's*
blocks, so if its content changed between advertising and applying, the rebuilt
hash won't match `patch.hash` — `merge` rejects it and the next heartbeat SYNC
retries. (Correct, occasionally one round slower.)

### 17.5 Backings — file vs. in-memory (coexist)

A `SyncStrategy` only sees `&[u8]`; *where those bytes live* is a separate seam
(`sync::backing::Backing`): `FileBacking` (mirror a path, today's behavior) or
`MemoryBacking` (hold bytes in RAM, never touch disk). The two coexist — chosen
per run, and later per-resource in the manifest. In-memory resources register
with a `MemoryRegistry` so they can be observed together.

### 17.6 The `process` file

A single sidecar reporting **all** in-memory resources at once — one line each:
`name⇥size⇥blake3-16`. **Size + hash only** (no payload bytes), and **decoupled**
from the sync loop (a ~1s side-car task, like the metrics file): `cat` it
whenever. Modeled on §14.2's "tmux is the compositor" — riftpipe writes a file,
something else reads it.

### 17.3 / 17.4 wal-db & image (PLANNED)

- **wal-db** — append-only log: `state_vector` = per-writer highest-seq offset
  map; `delta_since` = missing frames; `merge` = append in writer order.
  Convergence is "union of frames," no rewrite. Compaction layered on top.
- **image** — codec-aware tile merge: decode to a tile grid, each tile an
  independently-versioned cell; ship/composite changed tiles. Text CRDT ops are
  the wrong granularity for pixels.

### 17.7 Folder mode — wired (CLI)

`share <dir>` / `join <ticket> <dir>` now sync a whole folder:
- **Manifest** (`sync::manifest`) — `riftpipe.toml` at the dir root (or
  `--manifest`), glob → `Kind`. First match wins, else `default` (rsync).
- **Workspace** (`sync::workspace`) — scans the tree (skips dotfiles, the
  manifest, `.ticket`s), binds each file to a `SyncStrategy` + a backing; unimplemented
  kinds are skipped (a stub never panics a live session).
- **Multiplexed session** (`sync::folder`) — one link, many resources. Frame =
  `[path_len u16][path][tag][payload]`; tags `DELTA` / `SYNCREQ` / `SYNCREP`.
  Local changes are found by **polling** the backings (200ms); an unknown path in
  an inbound frame **auto-creates** the resource (peer-driven discovery). Same
  reconnect loop as `--pipe`.
- **Request/reply advertising** — a local change sends `SYNCREQ`; the peer
  answers `SYNCREP` with *its* signatures so a pull-only algorithm (rsync) can
  then push its `DELTA`. `REP` is never replied to, so it can't ping-pong; the
  heartbeat re-`REQ`s every 5s.
- **`--memory`** holds resources in RAM (seeded once from disk) instead of
  mirroring to disk; **`--process <path>`** writes the §17.6 size+hash file.

Verified end-to-end over loopback: a text file (text-crdt) and a binary blob
(rsync) sync into a nested subdir A→B with discovery; a live edit on one side
converges to the other; memory mode surfaces resources in the `process` file.

**Still to come:** implement `wal-db` and `image`; per-resource backing choice in
the manifest (memory vs file per glob); Phase 2 granular nvim edits (§15/§16).
