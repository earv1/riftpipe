#!/usr/bin/env bash
# Browser ↔ CLI request-to-join interop over the real sealed mesh, both
# directions: a browser owner approves a `connect --restricted --name` CLI
# joiner from its inbox, and a CLI `--restricted` founder approves a browser's
# "Ask to join" with `riftpipe approve <dir> <safety-code>`.
# See request-approve.mjs.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8143
WORK="$(mktemp -d)"
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

echo "== build wasm + bundle + native =="
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/rra-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/rra-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/rra-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/rra-build.log; exit 1; }
( cd projects/kanban/e2e && [ -d node_modules ] || { npm install --no-audit --no-fund && npx playwright install chromium; } ) >/tmp/rra-npm.log 2>&1 || { echo "npm install failed"; tail -8 /tmp/rra-npm.log; exit 1; }
cargo build --quiet --bin riftpipe || exit 1

echo "== serve dist on $PORT =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/rra-preview.log 2>&1 &
PREVIEW=$!
trap 'kill $PREVIEW 2>/dev/null; rm -rf "$WORK"' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done

echo "== request/approve: browser owner ← CLI joiner, CLI owner ← browser joiner =="
( cd projects/kanban/e2e && PORT=$PORT WORK="$WORK" BIN="$ROOT/target/debug/riftpipe" node request-approve.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
