# Publish the kanban on GitHub Pages (and share with a friend)

The board runs entirely in the browser — GitHub Pages serves the static bundle,
and two browsers sync **peer-to-peer over WebRTC**. Pages is HTTPS, which satisfies
WebRTC + OPFS's secure-context requirement. The data never touches a server.

But two browsers on **different networks** need two small public helpers to *find*
each other (they never see your board data):

1. **A signaling server over `wss://`** — pairs the two peers by connection id so
   they can swap WebRTC SDP. Required. (An HTTPS page cannot talk to insecure
   `ws://`, and `localhost` isn't reachable by your friend.)
2. **STUN** — lets each browser learn its public address for hole-punching. A free
   public STUN is the default; nothing to host.
3. **TURN** — only for hostile/symmetric NATs (the relay fallback). Optional.

## 1. Host the signaling server (the one thing you must run)

`riftpipe signal` is a content-blind room relay. It speaks plain `ws://`; put TLS
in front of it. The easy path is **fly.io** (free tier, gives you `wss://` for free):

```sh
# from the repo root
cd deploy
fly launch --copy-config --no-deploy   # choose a unique app name
fly deploy
# → your server is at wss://<app>.fly.dev
```

(`deploy/signal.Dockerfile` builds the binary and runs `riftpipe signal` bound to
`0.0.0.0`; `deploy/fly.toml` wires fly's TLS edge to it.)

Any host works — a VPS with **Caddy** as a TLS reverse proxy is two lines:

```
signal.example.com {
    reverse_proxy 127.0.0.1:9000
}
```
…with `RIFTPIPE_BIND=0.0.0.0 riftpipe signal --port 9000` running behind it.

## 2. Turn on GitHub Pages

In the repo: **Settings → Pages → Build and deployment → Source: GitHub Actions**.
The workflow `.github/workflows/pages.yml` builds the wasm core + the SolidJS
bundle and deploys it on every push to `main`.

## 3. Point the app at your signaling server

**Settings → Secrets and variables → Actions → Variables**, add:

| name | value | required |
|------|-------|----------|
| `SIGNAL_URL` | `wss://<app>.fly.dev` | **yes** |
| `STUN_URL` | `stun:stun.l.google.com:19302` | no (this is the default) |
| `TURN_URL` | `turn:your-turn:3478` | no (hostile-NAT fallback) |
| `TURN_USER` | turn username | with TURN |
| `TURN_PASS` | *(add as a **Secret**, not a Variable)* | with TURN |

Push to `main` (or run the workflow manually). Your board is now at
`https://<user>.github.io/riftpipe/`.

## 4. Share a board

A board is a connection id in the URL hash — share the **same** link and you share
the board:

```
https://<user>.github.io/riftpipe/#standup-7f3a91
```

Pick any unique id, or mint one:

```sh
./web/share-link.sh https://<user>.github.io/riftpipe
# → https://<user>.github.io/riftpipe/#<random-id>
```

Both of you open that link; create cards and they sync live, no server in the data
path. (Each browser keeps its own copy in OPFS, so the board survives reloads.)

## Reality check

- **Same `#id` = same board.** Different ids are different boards.
- **First connection needs both peers online together** to pair through signaling;
  after that, edits flow directly browser-to-browser.
- **A really locked-down NAT** (symmetric/CGNAT on both ends) needs the TURN relay
  — set `TURN_URL`/`TURN_USER`/`TURN_PASS`. The relay path is verified
  (`projects/kanban/e2e/run-bridge-relay.sh`); STUN-only hole-punching between two
  arbitrary networks is the case we can't test without two machines.
- The signaling server only brokers the handshake; it never sees card content.
