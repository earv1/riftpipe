#!/usr/bin/env bash
# Board isolation over iroh: a browser's claimed board's files never reach
# another board it opens (each board's files live in their own OPFS directory).
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

echo "== a claimed board's files stay out of another board =="
( cd projects/kanban/e2e && PORT=$PORT node board-isolation.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
