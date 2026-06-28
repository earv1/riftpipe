#!/usr/bin/env bash
# Full serverless P2P: two browsers collaborate on a kanban board over iroh — no
# signaling server, no TURN, no host you run. Bootstrap + transport ride n0's
# public relays (reached over HTTPS). Just a static server + the internet.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8127
# iroh/ring need a wasm-capable clang (Apple clang has no wasm backend).
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

echo "== build wasm (iroh) + kanban bundle (iroh transport) =="
( cd web && wasm-pack build --target web --out-dir pkg ) >/tmp/iroh-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/iroh-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/iroh-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/iroh-build.log; exit 1; }

echo "== serve dist (vite preview) — no signaling server needed =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/iroh-preview.log 2>&1 &
PREVIEW=$!
trap 'kill $PREVIEW 2>/dev/null' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done

echo "== two browsers over iroh (n0 relay) =="
( cd projects/kanban/e2e && PORT=$PORT node iroh-two-browser.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
