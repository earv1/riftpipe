#!/usr/bin/env bash
# End-to-end: build the serverless kanban bundle, serve it statically, run the
# signaling server, and drive two real browser contexts with Playwright to prove
# a card syncs A->B over WebRTC with no server in the data path.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8123

echo "== building bundle (wasm + SolidJS) =="
# VITE_STUN="" — loopback peers use host candidates; a public STUN would stall
# non-trickle gathering on a host with no internet.
( cd projects/kanban && VITE_STUN="" VITE_TRANSPORT=ws deno task build ) >/tmp/kanban-build.log 2>&1 || { echo "build failed"; tail -20 /tmp/kanban-build.log; exit 1; }

echo "== building native binary (for the signal server) =="
cargo build --quiet --bin riftpipe || exit 1

echo "== starting static server (vite preview, correct wasm MIME) + signal =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/kanban-preview.log 2>&1 &
PREVIEW=$!
./target/debug/riftpipe signal --port 9000 >/tmp/kanban-signal.log 2>&1 &
SIGNAL=$!
trap 'kill $PREVIEW $SIGNAL 2>/dev/null' EXIT

# Wait for the static server AND the signal server (browsers connect once on load
# and don't retry, so the signal port must be up first).
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done
for _ in $(seq 1 50); do (echo > "/dev/tcp/127.0.0.1/9000") 2>/dev/null && break; sleep 0.2; done

echo "== running Playwright two-browser test =="
( cd projects/kanban/e2e && PORT=$PORT node two-browser.mjs )
RESULT=$?
echo "== exit: $RESULT =="
exit $RESULT
