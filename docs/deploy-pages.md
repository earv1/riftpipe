# Publish the kanban on GitHub Pages (and share with a friend)

The board runs entirely in the browser and syncs **peer-to-peer over iroh** — and
with iroh there's **nothing for you to host**. GitHub Pages serves the static
bundle; the peer-to-peer bootstrap *and* transport ride **n0's public relays**
(free, end-to-end encrypted — the relay can't read your board). No signaling
server, no STUN/TURN, no backend. Just publish the page and share a link.

## 1. Turn on GitHub Pages

In the repo: **Settings → Pages → Build and deployment → Source: GitHub Actions**.
The workflow `.github/workflows/pages.yml` builds the wasm core (iroh included) +
the SolidJS bundle and deploys it on every push to `main`. Your board is then at
`https://<user>.github.io/<repo>/`.

(The build compiles iroh to WebAssembly, which needs a wasm-capable clang — the
workflow installs it. Nothing for you to configure.)

## 2. Share a board

Open the published URL. The tab becomes the **host** and writes its connection
**ticket** into the address bar:

```
https://<user>.github.io/riftpipe/#<ticket>
```

Send that link to a friend. They open it, join over the relay, and you're
collaborating live — create cards and they sync both ways. Each browser keeps its
own copy in OPFS, so the board survives reloads.

That's the whole deployment. No server you run, anywhere.

## Reality check

- **Same `#ticket` = same board.** The host must keep its tab open while others
  join (it's the rendezvous point); after joining, edits flow peer-to-peer.
- **Privacy:** n0's relay sees connection metadata (who talks to whom, and volume)
  but **not** your board — traffic is end-to-end encrypted. To avoid n0 entirely,
  self-host an [iroh relay](https://www.iroh.computer/docs) and point the build at
  it; that's the only reason you'd run any server.
- **Bundle size:** ~4.5 MB wasm (≈1.8 MB gzipped) — the cost of bundling a full
  P2P stack. One-time, then browser-cached.
- **A host that reloads keeps its identity** — the iroh key is persisted in
  localStorage, so share links stay valid across reloads.

## Appendix: the WebSocket transport (LEGACY fallback)

> This path is **not** part of the normal deployment — iroh is the default and
> needs nothing hosted. Keep reading only if you specifically need the
> WebRTC-via-signaling bridge.

A second transport exists — `?transport=ws` (or `VITE_TRANSPORT=ws` at build) — that
uses a tiny WebSocket signaling server + WebRTC instead of iroh. You only need this
to bridge a **browser to a native `riftpipe` peer** (which speaks WebRTC). It *does*
require hosting a signaling server over `wss://`:

- `deploy/signal.Dockerfile` + `deploy/fly.toml` deploy `riftpipe signal` behind
  TLS (e.g. fly.io gives `wss://<app>.fly.dev`).
- Set repo variables `SIGNAL_URL` (and optional `STUN_URL`/`TURN_*`) and build with
  `VITE_TRANSPORT=ws`.

For browser-to-browser sharing you don't need any of this — iroh is the default.
