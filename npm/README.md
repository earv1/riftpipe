# riftpipe (npm)

Typed TypeScript glue over the `riftpipe` CLI — build on riftpipe's P2P CRDT
sync without touching Rust. The package spawns the prebuilt binary and types
its language-agnostic seams:

| Seam | CLI | API |
|---|---|---|
| Editor edit-stream (JSON lines on stdio) | `share/join --pipe` | `PipeSession` |
| Tree sync breadcrumbs (`SYNCED:` / `DELETED:` lines) | `connect` / `connect --accept` | `TreeSession` |
| Auth (restricted boards) | `requests`/`approve`/`members`/`grant`/`revoke` | `requests`/`approve`/`members`/`grant`/`revoke`, `TreeSession.onAccessRequest`/`onReadKey` |
| Static hosting + SSE change events | `serve` | `Serve` |

## Binary resolution

`riftpipeBinary()` picks, in order: `$RIFTPIPE_BIN` → a packaged `bin/riftpipe`
(how a published release ships it) → `target/{release,debug}/riftpipe` in an
enclosing cargo checkout (the dev loop) → `riftpipe` on `$PATH`.

## Example

```ts
import { PipeSession, TreeSession, Serve, requests, approve, members } from "riftpipe";

// Live text sync between two processes, zero servers:
const host = PipeSession.share("notes.md");
const ticket = await host.ticket();
const peer = PipeSession.join(ticket, "notes.md");
peer.onRemote((op) => console.log("remote edit", op));
host.send({ op: "insert", pos: 0, text: "hello " });

// Host a folder on the gossip mesh (browsers join the same ticket):
const mesh = TreeSession.host("shared/");
console.log("join with:", await mesh.ticket());
mesh.onSynced((path) => console.log("peer wrote", path));
mesh.onDeleted((path) => console.log("peer deleted", path));

// Restricted board: approve a keyless joiner by its safety code.
const board = TreeSession.host("board/", { restricted: true });
board.onAccessRequest((r) => console.log("asking:", r.name, r.safety));
// … out-of-band, once r.safety matches what the joiner shows:
await approve("board/", "a1b2 c3d4");
console.log(await requests("board/"), await members("board/"));

// Live-updating static site over a synced dir:
const site = await Serve.start("shared/", { port: 8080 });
for await (const ev of site.events()) console.log("changed:", ev.path);
```

## Develop

```sh
npm install
npm run build       # tsc → dist/
cargo build         # in the repo root, so the wrapper finds the dev binary
npm test            # spawns the real binary: pipe round-trip, serve+SSE, mesh
```
