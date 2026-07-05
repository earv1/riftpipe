#!/usr/bin/env bash
# riftpipe kanban — one script for the whole app lifecycle.
#
#   ./run.sh            dev server (builds the wasm payload if stale) → :5173
#   ./run.sh build      static bundle (wasm + vite) → dist/
#   ./run.sh serve      build, then host dist/ with `riftpipe serve` → :8080
#   ./run.sh join <share-link|ticket|room-id> [dir]
#                       join a board from the CLI — the board materializes as
#                       files in [dir] (default ./board-live); edit with vim,
#                       watch SYNCED: lines. Accepts a full browser share URL,
#                       a native ticket (connect --accept / share), or a
#                       signaling room id (pass --signal ws://… after [dir])
#   ./run.sh demo       two-browser convergence demo (e2e/run-iroh.sh)
#   ./run.sh mesh       three-browser gossip-mesh demo (e2e/run-iroh-mesh.sh)
#
#   WASM=force ./run.sh   rebuild the wasm payload even if it looks fresh
#   WASM=skip  ./run.sh   never rebuild it (fastest UI-only loop)
#   PORT=9090  ./run.sh serve
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
WASM_DIR="$HERE/wasm"
PKG="$WASM_DIR/pkg/kanban_wasm_bg.wasm"

# Apple clang can't compile `ring` to wasm — use Homebrew LLVM when present
# (same trick as web/test-headless.sh).
if command -v brew >/dev/null && brew --prefix llvm >/dev/null 2>&1; then
  export CC_wasm32_unknown_unknown="$(brew --prefix llvm)/bin/clang"
  export AR_wasm32_unknown_unknown="$(brew --prefix llvm)/bin/llvm-ar"
fi

wasm_stale() {
  [ ! -f "$PKG" ] && return 0
  # stale if any Rust source (app crate, riftpipe-web, or core) is newer
  [ -n "$(find "$WASM_DIR/src" "$WASM_DIR/Cargo.toml" "$ROOT/web/src" "$ROOT/core/src" \
          -newer "$PKG" -print -quit 2>/dev/null)" ]
}

build_wasm() {
  case "${WASM:-auto}" in
    skip) echo "== wasm: skipped (WASM=skip)";;
    force)
      echo "== wasm: building (forced)"
      (cd "$WASM_DIR" && wasm-pack build --target web --out-dir pkg);;
    *)
      if wasm_stale; then
        echo "== wasm: sources newer than pkg — building"
        (cd "$WASM_DIR" && wasm-pack build --target web --out-dir pkg)
      else
        echo "== wasm: pkg is fresh (WASM=force to rebuild)"
      fi;;
  esac
}

case "${1:-dev}" in
  dev)
    build_wasm
    echo "== vite dev server → http://localhost:5173 (API runs in-page)"
    cd "$HERE" && exec deno task dev
    ;;
  build)
    build_wasm
    echo "== vite build → dist/"
    cd "$HERE" && deno task build
    ;;
  serve)
    "$0" build
    BIN="$ROOT/target/debug/riftpipe"
    [ -x "$BIN" ] || (echo "== building riftpipe" && cd "$ROOT" && cargo build --bin riftpipe)
    echo "== riftpipe serve dist/ → http://localhost:${PORT:-8080}"
    exec "$BIN" serve "$HERE/dist" --port "${PORT:-8080}"
    ;;
  join)
    LINK="${2:?usage: ./run.sh join <share-link|ticket|room-id> [board-dir] [--signal ws://…]}"
    DIR="${3:-$HERE/board-live}"
    BIN="$ROOT/target/debug/riftpipe"
    [ -x "$BIN" ] || { echo "== building riftpipe"; (cd "$ROOT" && cargo build --bin riftpipe); }
    # Pass any extra flags (e.g. --signal, --metrics) straight through.
    EXTRA=(); [ $# -gt 3 ] && EXTRA=("${@:4}")
    echo "== joining board → $DIR"
    echo "   edit files there (e.g. \$EDITOR $DIR/tickets/<id>/card.md); SYNCED: lines show sync"
    exec "$BIN" connect "$LINK" "$DIR" ${EXTRA[@]+"${EXTRA[@]}"}
    ;;
  demo)  exec "$HERE/e2e/run-iroh.sh" ;;
  mesh)  exec "$HERE/e2e/run-iroh-mesh.sh" ;;
  *)
    sed -n '2,19p' "$0"; exit 1 ;;
esac
