#!/usr/bin/env bash
# CLI host restart with a browser joiner: `connect --accept` hosts a board, a
# browser joins its ticket, the CLI is killed, the browser edits during the gap,
# and the restarted CLI (same identity + board secret from <dir>/.riftpipe-host)
# must pull those edits and resume live sync. See cli-host-restart.mjs.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8153
NDIR="$(mktemp -d)"
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

echo "== build wasm + bundle + native =="
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/chr-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/chr-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/chr-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/chr-build.log; exit 1; }
( cd projects/kanban/e2e && [ -d node_modules ] || { npm install --no-audit --no-fund && npx playwright install chromium; } ) >/tmp/chr-npm.log 2>&1 || { echo "npm install failed"; tail -8 /tmp/chr-npm.log; exit 1; }
cargo build --quiet --bin riftpipe || exit 1

echo "== serve dist on $PORT =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/chr-preview.log 2>&1 &
PREVIEW=$!
NREAL="$(cd "$NDIR" && pwd -P)"
trap 'kill $PREVIEW 2>/dev/null; rm -rf "$NDIR"; rm -f "$NDIR.ticket" "$NREAL.ticket"' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done

echo "== CLI hosts, browser joins, CLI restarts =="
( cd projects/kanban/e2e && PORT=$PORT NDIR="$NDIR" BIN="$ROOT/target/debug/riftpipe" node cli-host-restart.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
