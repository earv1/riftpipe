#!/usr/bin/env bash
# "Merge both boards" over iroh: two browsers that each already have their own card
# connect and end up seeing BOTH — no signaling server, no host you run.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8128
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

echo "== build wasm (iroh) + kanban bundle =="
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/im-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/im-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/im-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/im-build.log; exit 1; }

echo "== serve dist =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/im-preview.log 2>&1 &
PREVIEW=$!
trap 'kill $PREVIEW 2>/dev/null' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done

echo "== two browsers, each with a pre-existing board, merge over iroh =="
( cd projects/kanban/e2e && PORT=$PORT node iroh-merge.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
