#!/usr/bin/env bash
# Tree/mesh deletes over the real mesh with three peers — browser A hosts,
# browser B joins the ticket, the CLI joins the share link: a comment delete, a
# card delete while B is offline (B rejoins with a stale OPFS copy and drops
# it; nothing comes back), and an on-disk delete by the CLI. See delete-sync.mjs.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT="${PORT:-8155}"
NDIR="$(mktemp -d)"
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

echo "== build wasm + bundle + native =="
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/del-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/del-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/del-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/del-build.log; exit 1; }
( cd projects/kanban/e2e && [ -d node_modules ] || { npm install --no-audit --no-fund && npx playwright install chromium; } ) >/tmp/del-npm.log 2>&1 || { echo "npm install failed"; tail -8 /tmp/del-npm.log; exit 1; }
cargo build --quiet --bin riftpipe || exit 1

echo "== serve dist on $PORT =="
# Another checkout's preview on this port would silently serve ITS bundle.
if curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1; then echo "port $PORT is already serving — rerun with PORT=<free port>"; exit 1; fi
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/del-preview.log 2>&1 &
PREVIEW=$!
trap 'kill $PREVIEW 2>/dev/null; rm -rf "$NDIR"' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done

echo "== browser A hosts, browser B + the CLI join; deletes =="
( cd projects/kanban/e2e && PORT=$PORT NDIR="$NDIR" BIN="$ROOT/target/debug/riftpipe" node delete-sync.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
