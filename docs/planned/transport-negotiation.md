# Planned: connection negotiation & multi-peer transport

**Status:** planned (design only — thinking, not building). Extends the §8
handshake and the §14.1 connection strategy; generalizes the 1:1 session to N
peers. The browser arc (iroh→WebRTC) is the forcing function, but the design is
transport- and count-agnostic.

## The one-line model

**iroh is the control plane; the data plane is negotiated.** Every peer can
always talk over the iroh `Link` (the floor). On connect, peers negotiate the
*best mutually-supported* data transport and upgrade to it if it beats what they
have — but the iroh `Link` is always there to fall back to. So no peer is ever
unreachable for lacking a fancier transport.

This folds three existing ideas into one decision:
- §5 `Link` seam — the session runs over *a* `Link`, whichever we negotiate.
- §14.1 prefer-direct/warn-on-relay — becomes one rung of a transport ladder.
- §8 handshake — gains a capability-negotiation phase.

## 1. The transport ladder

Per peer-pair, pick the highest rung both support and can establish:

```
  webrtc-direct      browser↔browser, browser↔native(+webrtc-rs); LAN/NAT-direct data
  iroh-direct        native↔native (QUIC hole-punch, §14.1); also browser? no (no UDP)
  iroh-relay         always available — the floor; works for any pair, data via relay
```

- `webrtc-direct` needs **both** sides to speak WebRTC (browser = built-in
  `web-sys`; native = `webrtc-rs`). iroh-QUIC and WebRTC **do not interop** — a
  WebRTC data path to a native peer requires that peer to carry `webrtc-rs`.
- `iroh-direct` is the native↔native win (already shipped behavior); a browser
  can't reach it (no UDP hole-punch in-browser).
- `iroh-relay` is the universal floor and the **signaling channel** that brokers
  the WebRTC upgrade (so we run **no signaling server of our own** — see §3).

### Reachability matrix (from the browser arc)

| Pair | Best path | Floor if no WebRTC |
|---|---|---|
| native ↔ native | iroh-direct (unchanged) | iroh-direct |
| native(+webrtc-rs, public IP) ↔ browser | webrtc-direct (trivial — host candidate, no holepunch) | iroh-relay |
| native(+webrtc-rs, NAT) ↔ browser | webrtc-direct (ICE holepunch) | iroh-relay |
| **iroh-only native** ↔ browser | — | **iroh-relay (works, relayed)** |
| browser ↔ browser | webrtc-direct (ICE) | iroh-relay |

## 2. The handshake, extended (capability negotiation)

Today (§8): **Hello** `{doc_id, agent_id, rule_fingerprint}` → **mandatory
simulation** (conformance suite) → **Compatible** → sync. We insert one phase
after compatibility, before sync:

**Capabilities.** Each side sends a descriptor:

```
Caps {
  proto_version: u16,
  transports:    [TransportId],   // ordered by *this peer's* preference
  webrtc:        Option<WebrtcInfo>,   // ICE/DTLS params hint; None ⇒ no webrtc
  role:          Browser | Native | Server,   // informational
  agent_id:      AgentId,         // also the deterministic tie-breaker
  // unknown fields ignored — forward-compatible
}
```

Negotiation = intersect the two `transports` lists, pick the highest rung both
list, deterministic tie-break by `agent_id` (lower = WebRTC **offerer**). Result
is the *target* transport; the iroh `Link` is the floor if the target fails to
establish. New wire tag **`CAPS`** alongside the existing `DELTA`/`SYNC` (§16).

**Why negotiate, not hardcode:** peers are heterogeneous (native/browser/server,
with/without `webrtc-rs`), each *edge* picks its own best path, and future
transports (WebTransport, raw QUIC-direct) slot in by appending to the ladder —
**no protocol break**. An old peer that offers fewer transports still meets at
the iroh-relay floor. This is the **backward-compat invariant** made explicit.

## 3. Connect → upgrade flow (per edge)

```
 0. iroh connect      direct-first per §14.1, relay fallback
 1. auth              BLAKE3 challenge-response over the iroh Link (§7, secure.rs)
 2. compat            §8 Hello + mandatory simulation
 3. capabilities      §2 — choose target transport
 4. upgrade (if webrtc chosen):
       exchange SDP offer/answer + ICE candidates **over the iroh Link**
       (iroh relay = our signaling rendezvous; no signaling server we run)
       → establish RTCDataChannel → wrap as a `Link`
 5. run sync          over the negotiated `Link` — snapshot/op protocol UNCHANGED
                      (the stack is `Link`-generic; see [[snapshot-is-the-interface]])
```

- **Upgrade failure never regresses:** WebRTC negotiation times out / ICE fails →
  stay on the iroh `Link`. The session already works there (it's today's path).
- **Mid-session downgrade:** a dead WebRTC channel falls back to the still-open
  iroh `Link` (kept as control/fallback) and re-attempts upgrade. (Open: keep
  iroh hot for the whole session, or re-dial on demand? — §8 open questions.)
- iroh's relay carries only the brief SDP/ICE handshake, then WebRTC data goes
  direct — so even relayed signaling means **no data through the relay** once up.

## 4. Multi-peer (keep in mind now; do not build)

Today: 1:1 (`share`/`join`, one `Link`, one session). The generalization:

### 4.1 A session is a *peer set*, not a link
One **negotiated `Link` per peer** (mixed: some WebRTC-direct, some relayed), and
a **broadcast layer** above them: a local change fans out to every peer `Link`.
The `Link` trait and snapshot/op protocol are unchanged; only the orchestration
above grows from one link to a set.

### 4.2 The CRDT already makes this safe (the big win)
eg-walker ops are commutative/idempotent and `decode_and_add` is idempotent
(§16). **Receiving the same op from multiple peers is a no-op** (dedup by
version). So duplicate / multi-path delivery — inherent to any mesh or gossip —
needs **no new conflict logic**; the existing convergence machinery covers it.
Version-vector reconciliation (§16) generalizes directly: a joiner runs the
on-connect `SYNC` against whoever it reaches and catches up; redundant ops fall
away. **Multi-peer is a fan-out + membership problem, not a merge problem.**

### 4.3 Topologies (pick by scale)
- **Full mesh** — every peer ↔ every peer; each change sent to all. N² links, no
  SPOF, simplest correctness. **Default for small groups** (a kanban board's
  handful of editors).
- **Star / hub** — a `Server` peer (§"run a server") as hub; others connect to it,
  it re-broadcasts. N links, simplest membership, but hub is SPOF + load + sees
  all (encrypted) traffic. Natural when a persistent anchor already exists.
- **Gossip / epidemic** — forward to a subset; eventually consistent; scales past
  mesh. Needs dedup (have it) + anti-entropy (`SYNC` *is* that). Future.
- **Hybrid** — server as always-on anchor + mesh among browsers; hub for
  discovery, mesh for data.

### 4.4 Membership & joining
- Join via ticket → connect to one known peer (or the hub).
- **Peer exchange (PEX):** the contacted peer shares its roster (`EndpointId`s);
  the joiner dials the rest → mesh forms. Or hub-only → star.
- Per new edge: negotiate transport (§2–3), then §16 on-connect `SYNC` to catch
  up. Roster is soft state, gossiped; each peer holds its own view of the set.

### 4.5 Churn & partial connectivity (the sharp edge)
- Peer leaves/drops → §16 reconnection per edge; roster heals.
- **Partial connectivity** — A↔B and A↔C up, but B↔C can't connect (e.g. two
  symmetric-NAT browsers). Pure mesh loses that edge → you need **forwarding**
  (A relays B↔C), which is exactly gossip. *So partial connectivity is the trigger
  to graduate mesh → gossip/forwarding.* Note now, defer. A hub sidesteps it
  entirely (everyone reaches the hub) — another argument for an optional anchor.

### 4.6 Identity & auth at N
- The shared secret capability (ticket secret) already admits **anyone holding
  it** — N peers all pass the same BLAKE3 challenge (§7). No change.
- Each peer keeps a distinct `agent_id` (eg-walker frame). Fine at N.
- Open: revocation / per-peer caps (deferred; ties to the §6 guard).

## 5. What it touches when built (not now)
- `net/` — `Caps` descriptor + negotiation (extends `secure.rs` handshake or a new
  `caps` module); a WebRTC `Link` impl (`web-sys` browser / `webrtc-rs` native);
  iroh-as-signaling glue (SDP/ICE over the iroh `Link`).
- `sync/` — lift the single-`Link` session to a peer set + broadcast/fan-out;
  membership/roster (PEX); generalize §16 `SYNC` to per-peer.
- Unchanged: the `Link` trait, the snapshot/op protocol, the CRDT, the auth.

## 6. Invariants to preserve
- **iroh `Link` is the floor** — every peer can always talk over it; WebRTC is an
  opportunistic upgrade, never a requirement (backward compat by construction).
- **snapshot-is-the-interface** — peers still emit snapshots/deltas; fan-out lives
  at the `Link`/broadcast layer, not in producers. See [[snapshot-is-the-interface]].
- **Convergence by idempotent ops** — multi-path/duplicate delivery is safe; no
  new conflict resolution at N peers.
- **Direct-preferred, relay-warned** (§14.1) — now the top rungs of the ladder.

## 7. Open questions
- Mesh vs star as the v-next default; partial-connectivity threshold to introduce
  forwarding/gossip.
- Roster/PEX format and gossip cadence — piggyback on the §16 `SYNC` heartbeat?
- Upgrade timing: negotiate WebRTC eagerly on connect, or lazily once relay cost
  is observed (§14.1 already wants to *detect* relay)?
- WebRTC offerer tie-break in a mesh (deterministic by `agent_id` — confirm).
- Group-size target: kanban is small; does anything need to scale to gossip?
- Presence (peer cursors) vs §14.3 layered state: cursors are *local* plane, yet
  you want to *show* peers' cursors — ephemeral-shared, fanned out but never
  persisted. Where does it sit? (Likely a separate, lossy presence channel.)
