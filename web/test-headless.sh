#!/usr/bin/env bash
# Headless-browser verification of the wasm crates: riftpipe-web (this dir) and
# kanban-wasm (projects/kanban/wasm) — one chromedriver setup, both suites.
#
# Runs the wasm-bindgen tests in a REAL (headless) Chrome — no GUI. The wrinkle:
# `wasm-pack test` always downloads the *latest* chromedriver, which mismatches a
# slightly-older local Chrome and fails the session (404 / SIGKILL). So we fetch
# the chromedriver matching THIS machine's Chrome build (from the Chrome-for-
# Testing index) and invoke the test runner directly with it.
#
# Prereqs: rustup `wasm32-unknown-unknown` target, `wasm-pack` (it installs the
# version-matched test runner), Google Chrome, python3, curl/unzip. macOS arm64.
set -euo pipefail
cd "$(dirname "$0")"

# iroh/ring need a wasm-capable clang — Apple clang has no WebAssembly backend.
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$(brew --prefix llvm)/bin/llvm-ar}"

CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PLATFORM="mac-arm64"

# 1. This machine's Chrome build (major.minor.build — chromedriver matches on this).
VER=$("$CHROME" --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
BUILD=$(echo "$VER" | cut -d. -f1-3)
echo "Chrome $VER (build $BUILD)"

# 2. Matching chromedriver, cached under .chromedriver/.
DRIVER="$PWD/.chromedriver/chromedriver-$PLATFORM/chromedriver"
if [ ! -x "$DRIVER" ]; then
  echo "fetching chromedriver for build $BUILD..."
  URL=$(curl -fsSL 'https://googlechromelabs.github.io/chrome-for-testing/latest-patch-versions-per-build-with-downloads.json' \
    | python3 -c "import json,sys; d=json.load(sys.stdin); b=d['builds']['$BUILD']; print(next(x['url'] for x in b['downloads']['chromedriver'] if x['platform']=='$PLATFORM'))")
  mkdir -p .chromedriver
  curl -fsSL "$URL" -o .chromedriver/cd.zip
  unzip -oq .chromedriver/cd.zip -d .chromedriver
  xattr -d com.apple.quarantine "$DRIVER" 2>/dev/null || true
  chmod +x "$DRIVER"
fi
"$DRIVER" --version

# 3. The wasm-bindgen test runner wasm-pack installed (version-matched to the crate).
RUNNER=$(ls -t ~/Library/Caches/.wasm-pack/wasm-bindgen-cargo-install-*/wasm-bindgen-test-runner 2>/dev/null | head -1 || true)
if [ -z "${RUNNER:-}" ] || [ ! -x "$RUNNER" ]; then
  echo "priming the test runner via wasm-pack (one-time; its own driver may fail — that's fine)..."
  wasm-pack test --headless --chrome >/dev/null 2>&1 || true
  RUNNER=$(ls -t ~/Library/Caches/.wasm-pack/wasm-bindgen-cargo-install-*/wasm-bindgen-test-runner 2>/dev/null | head -1 || true)
fi
[ -x "${RUNNER:-/nonexistent}" ] || { echo "could not locate wasm-bindgen-test-runner"; exit 1; }

# 4. Start the native signaling server on :9011 for the end-to-end signaling test.
echo "starting signaling server (riftpipe signal) on :9011..."
( cd .. && cargo build --quiet --bin riftpipe ) || { echo "native build failed"; exit 1; }
../target/debug/riftpipe signal --port 9011 >/tmp/riftpipe-signal.log 2>&1 &
SIGNAL_PID=$!
trap 'kill $SIGNAL_PID 2>/dev/null' EXIT
sleep 0.5

# 5. Run, forcing OUR matching driver (wasm-pack would otherwise use its own).
echo "running headless wasm tests (riftpipe-web)..."
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="$RUNNER" \
CHROMEDRIVER="$DRIVER" \
WASM_BINDGEN_TEST_ONLY_WEB=1 \
  cargo test --target wasm32-unknown-unknown "$@"

# 6. Same setup, the kanban app crate (format + OPFS handler tests).
echo "running headless wasm tests (kanban-wasm)..."
( cd ../projects/kanban/wasm && \
  CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="$RUNNER" \
  CHROMEDRIVER="$DRIVER" \
  WASM_BINDGEN_TEST_ONLY_WEB=1 \
    cargo test --target wasm32-unknown-unknown "$@" )
