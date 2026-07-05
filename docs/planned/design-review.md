# Design review — the serverless-browser-kanban branch

A re-review after unifying the data model. What's solid, what's still broken, and
what's honestly deferred.

## Resolved by the unification

- **One data model.** Native (`std::fs`) and browser (OPFS) now use the *same*
  file tree (`board.md` + `tickets/<id>/{card.md,meta.toml,comments/*}`), synced by
  the generic `riftpipe_core::sync` tree protocol (the format itself lives in the
  app, not the library). A board is portable between a native peer and a browser
  peer; no more `board.json` monolith. (Fixes critique #1, #2.)
- **Structural consistency = per-file LWW, prose = CRDT** — now matches the design
  intent (`data-model.md`) on both platforms, rather than the browser silently
  doing whole-board LWW. (Fixes critique #4 *to the documented design*; see the
  open question on concurrent same-card moves below.)

## The critical remaining gap — RESOLVED

> **Done.** `web/src/board_sync.rs` (`BoardSync`) now syncs the board over the
> established WebRTC link: text files (`card.md`, `comments/*.md`, `board.md`) as
> eg-walker CRDTs, structural files (`meta.toml`) as last-writer-wins. The kanban
> handler pushes each mutation; `connectAndSync` lands remote merges in OPFS and
> refreshes the UI; `api.ts`/`App.tsx` connect P2P when the URL carries a
> connection id. Verified headlessly (two `BoardSync`s converge a text file with
> concurrent edits + a structural move), and the SolidJS app builds as a
> self-contained static bundle. The browser kanban now **collaborates**, not just
> runs.

Original gap (kept for context): the browser handler wrote OPFS but didn't sync;
the building blocks (RiftDoc, the WebRTC link, the signaling server) existed but
weren't wired. They are now.

*Verified end-to-end:* `projects/kanban/e2e/` drives **two real isolated browser
contexts** with Playwright — both load the static bundle at the same connection-id
link, connect P2P over WebRTC via the signaling server, and a card created through
browser A's UI appears in browser B, with no server in the data path. Run with
`projects/kanban/e2e/run.sh`.

## Transport fragmentation (#3) — RESOLVED

> **Done & verified end-to-end.** A native peer now joins the same WebSocket
> signaling server the browser uses (`net::webrtc::connect_via_signaling`),
> speaking the browser's JSON protocol, and establishes WebRTC via `webrtc-rs`.
> `projects/kanban/e2e/run-bridge.sh` proves it across the stack with Playwright:
> a real browser (web-sys) and a native process (webrtc-rs) connect through the
> signaling server and exchange messages **both ways** over WebRTC. Native↔native
> via the signaling server is also a Rust integration test
> (`tests/networking.rs`).

Native still *prefers* iroh among native peers (the capability ladder), but the
two stacks are no longer islands — anything that speaks the signaling+WebRTC path
interoperates.

**Full board sync between a native peer and a browser is done, bidirectionally.**
The sync protocol lives in `riftpipe_core::sync` (shared native+wasm); `riftpipe
kanban connect <id> <dir>` joins a browser's room, receives edits to disk, and a
notify watcher pushes the native peer's own edits back. Verified end-to-end
(`projects/kanban/e2e/run-board-bridge.sh`): a card created in the browser UI lands
on the native disk, and editing the native `card.md` updates the browser UI. So a
native peer (edit files in any editor) and a browser peer collaborate on one board.

## What's actually left

Everything — the unified file-tree model, browser collaboration, the
browser↔native bridge, bidirectional board sync — is built and verified.

**The cross-NAT *fallback* (#5) is now verified too.** A hostile-NAT pair's
worst-case path is to relay through TURN; we prove that on one machine by forcing
**relay-only ICE** (host + srflx candidates banned) with a local TURN server
(coturn). `projects/kanban/e2e/run-relay.sh` (two native `webrtc-rs` peers) and
`run-bridge-relay.sh` (a real browser + a native process, both relay-only) connect
and exchange data through the relay. So the `RIFTPIPE_TURN` config path and the
relay guarantee are exercised end-to-end across both stacks.

The **only** thing still unproven is real NAT *traversal* between two **distinct
public IPs** (the medium case — direct hole-punch via STUN, which on one machine
can't be simulated because both peers share loopback). That needs two boxes on
different networks; and even there, the fallback is the relay we've already
proven. A genuine hardware requirement, not a code gap.

## Honestly deferred / can't verify here

- **Cross-NAT connectivity (#5).** Everything is verified on loopback / one page /
  host candidates. Real two-machine, hostile-NAT connection is untested and needs
  a TURN relay (the env-config exists, unexercised). This is the genuine unknown.
- **`EventSource('/api/events')` in no-server mode.** `App.tsx` opens an SSE stream
  the in-browser handler doesn't serve; live cross-peer updates need a local event
  channel fed by remote merges (ties into the sync wiring above). Until then the
  UI updates only from its own mutations.

## Smaller issues

- **Browser handler reads the whole tree per request** (lists `tickets/`, reads
  every card for `/api/board`). Fine for small boards; a cache or index file is
  the optimization if boards grow.
- **Two `opfs_root` helpers** (one in `lib.rs`, one in `kanban.rs`) — minor
  duplication to consolidate.
- **Tooling**: the headless harness hand-pins a chromedriver and starts the signal
  server out-of-band. Reproducible, but a CI story would want this packaged.

## Verdict

The unification was the right call and is done + verified. The architecture is now
coherent. The honest headline: **the data layer is serverless and portable; the
*collaboration* layer (syncing the browser's files over the link) is the next
build, and cross-NAT is the next thing to actually prove.**
