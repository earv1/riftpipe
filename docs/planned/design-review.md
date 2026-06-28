# Design review — the serverless-browser-kanban branch

A re-review after unifying the data model. What's solid, what's still broken, and
what's honestly deferred.

## Resolved by the unification

- **One data model.** Native (`std::fs`) and browser (OPFS) now use the *same*
  file tree (`board.md` + `tickets/<id>/{card.md,meta.toml,comments/*}`) through
  `riftpipe_core::kanban`. A board is portable between a native peer and a browser
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

*Caveat:* the full two-real-browser UI loop is verified by composition (sync core
+ handler + OPFS + bundle each tested), not a single end-to-end two-browser UI
test — one page shares one OPFS, so that needs two browser contexts (Playwright).

## Transport fragmentation (#3) — narrower than it looked

Native speaks iroh; the browser speaks WebRTC + the signaling server. They don't
interoperate **today**, but the bridge is *achievable, not fundamental*: native
already carries `webrtc-rs`, and the signaling server is a generic room relay. A
native peer can join a signaling room over a WebSocket and establish WebRTC
(`webrtc-rs`) with a browser peer — both sides speak the same WebRTC. So
"browser↔native" is a wiring task (native WS-signaling client reusing the existing
`webrtc-rs` establishment), not a research problem. Not yet built.

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
