#!/usr/bin/env bash
# Browser live-reconnection e2e over the real iroh mesh: host reload + long gap
# (host-reload.mjs), in-tab board switch both ways (hash-switch.mjs), and offline
# edits syncing after coming back online (offline.mjs).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"; cd "$ROOT"
PORT=8151
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/rrc-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/rrc-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/rrc-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/rrc-build.log; exit 1; }
( cd projects/kanban/e2e && [ -d node_modules ] || { npm install --no-audit --no-fund && npx playwright install chromium; } ) >/tmp/rrc-npm.log 2>&1 || { echo "npm install failed"; tail -8 /tmp/rrc-npm.log; exit 1; }
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/rrc-preview.log 2>&1 &
PREVIEW=$!; trap 'kill $PREVIEW 2>/dev/null' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done
fail=0
( cd projects/kanban/e2e && PORT=$PORT node host-reload.mjs ) || fail=1
( cd projects/kanban/e2e && PORT=$PORT node hash-switch.mjs ) || fail=1
( cd projects/kanban/e2e && PORT=$PORT node offline.mjs ) || fail=1
echo "== exit: $fail =="; exit $fail
