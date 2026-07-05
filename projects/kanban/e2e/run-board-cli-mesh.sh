#!/usr/bin/env bash
# The proof: a browser hosts a kanban board on the iroh gossip mesh, and the
# CLI joins the *browser share link* directly — zero signaling infrastructure.
# See board-cli-mesh.mjs.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8137
NDIR="$(mktemp -d)"
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

echo "== build wasm + bundle + native =="
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/bcm-wasm.log 2>&1 || { echo "wasm build failed"; tail -8 /tmp/bcm-wasm.log; exit 1; }
( cd projects/kanban && deno task build ) >/tmp/bcm-build.log 2>&1 || { echo "bundle build failed"; tail -10 /tmp/bcm-build.log; exit 1; }
cargo build --quiet --bin riftpipe || exit 1

echo "== serve dist on $PORT =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/bcm-preview.log 2>&1 &
PREVIEW=$!
trap 'kill $PREVIEW 2>/dev/null; rm -rf "$NDIR"' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done

echo "== browser hosts, CLI joins the share link =="
( cd projects/kanban/e2e && PORT=$PORT NDIR="$NDIR" BIN="$ROOT/target/debug/riftpipe" node board-cli-mesh.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
