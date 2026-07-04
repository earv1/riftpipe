#!/usr/bin/env bash
# 3-browser gossip mesh over iroh — all peers see all cards (no fixed hub).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8129
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

echo "== build wasm (iroh-gossip) + bundle =="
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/mesh-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/mesh-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/mesh-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/mesh-build.log; exit 1; }

echo "== serve dist =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/mesh-preview.log 2>&1 &
PREVIEW=$!
trap 'kill $PREVIEW 2>/dev/null' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done

echo "== three browsers over the gossip mesh =="
( cd projects/kanban/e2e && PORT=$PORT node iroh-mesh.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
