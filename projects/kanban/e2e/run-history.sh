#!/usr/bin/env bash
# History-view e2e over the real sealed mesh: the library event log
# (events.mjs) and the Activity panel + per-card History built on it
# (history.mjs).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"; cd "$ROOT"
PORT=8142
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/rh-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/rh-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/rh-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/rh-build.log; exit 1; }
( cd projects/kanban/e2e && [ -d node_modules ] || { npm install --no-audit --no-fund && npx playwright install chromium; } ) >/tmp/rh-npm.log 2>&1 || { echo "npm install failed"; tail -8 /tmp/rh-npm.log; exit 1; }
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/rh-preview.log 2>&1 &
PREVIEW=$!; trap 'kill $PREVIEW 2>/dev/null' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done
fail=0
( cd projects/kanban/e2e && PORT=$PORT node events.mjs ) || fail=1
( cd projects/kanban/e2e && PORT=$PORT node history.mjs ) || fail=1
echo "== exit: $fail =="; exit $fail
