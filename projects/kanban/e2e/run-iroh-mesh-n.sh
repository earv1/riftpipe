#!/usr/bin/env bash
# N-browser gossip mesh (default PEERS=5) — staged joins + live edits, no
# reload crutch. See iroh-mesh-n.mjs.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8131
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

echo "== build wasm + bundle =="
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/meshn-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/meshn-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/meshn-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/meshn-build.log; exit 1; }

echo "== serve dist =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/meshn-preview.log 2>&1 &
PREVIEW=$!
trap 'kill $PREVIEW 2>/dev/null' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done

echo "== ${PEERS:-5} browsers over the gossip mesh =="
( cd projects/kanban/e2e && PORT=$PORT PEERS="${PEERS:-5}" node iroh-mesh-n.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
