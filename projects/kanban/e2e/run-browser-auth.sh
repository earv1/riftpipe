#!/usr/bin/env bash
# Browser auth e2e over the real sealed mesh: claim/ask/approve with read gated
# (lock-grant.mjs) and read revocation by epoch rotation (epoch-revoke.mjs).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"; cd "$ROOT"
PORT=8141
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/rba-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/rba-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/rba-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/rba-build.log; exit 1; }
( cd projects/kanban/e2e && [ -d node_modules ] || { npm install --no-audit --no-fund && npx playwright install chromium; } ) >/tmp/rba-npm.log 2>&1 || { echo "npm install failed"; tail -8 /tmp/rba-npm.log; exit 1; }
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/rba-preview.log 2>&1 &
PREVIEW=$!; trap 'kill $PREVIEW 2>/dev/null' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done
fail=0
( cd projects/kanban/e2e && PORT=$PORT node lock-grant.mjs ) || fail=1
( cd projects/kanban/e2e && PORT=$PORT node epoch-revoke.mjs ) || fail=1
echo "== exit: $fail =="; exit $fail
