#!/usr/bin/env bash
# Full browser↔native board collaboration: a browser runs the kanban app at a
# connection-id link; a native peer (`riftpipe kanban connect`) joins the same
# room. A card created through the browser UI must land on the native peer's disk.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8125
SIGPORT=9021
SIGNAL_URL="ws://127.0.0.1:$SIGPORT"
ROOM="board-$(openssl rand -hex 4)"
TITLE="from-browser-to-native"
NDIR="$(mktemp -d)"

echo "== building bundle + native =="
( cd projects/kanban && deno task build ) >/tmp/bb-build.log 2>&1 || { echo "build failed"; tail -10 /tmp/bb-build.log; exit 1; }
cargo build --quiet --bin riftpipe || exit 1

echo "== serving kanban dist + signaling on $SIGPORT =="
( cd projects/kanban && deno run -A npm:vite preview --port $PORT --strictPort ) >/tmp/bb-preview.log 2>&1 &
PREVIEW=$!
./target/debug/riftpipe signal --port $SIGPORT >/tmp/bb-signal.log 2>&1 &
SIGNAL=$!
NATIVE=""
trap 'kill $PREVIEW $SIGNAL $NATIVE 2>/dev/null; rm -rf "$NDIR"' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done
for _ in $(seq 1 50); do (echo > "/dev/tcp/127.0.0.1/$SIGPORT") 2>/dev/null && break; sleep 0.2; done

echo "== starting native peer: kanban connect $ROOM -> $NDIR =="
( ./target/debug/riftpipe kanban connect "$ROOM" "$NDIR" --signal "$SIGNAL_URL" ) >/tmp/bb-native.log 2>&1 &
NATIVE=$!
sleep 0.8

echo "== running browser (Playwright) — bidirectional check =="
( cd projects/kanban/e2e && PORT=$PORT ROOM="$ROOM" SIGNAL_URL="$SIGNAL_URL" NDIR="$NDIR" node board-bridge.mjs )
RESULT=$?
sleep 1

echo "== native log: =="; cat /tmp/bb-native.log
echo "== files on native disk: =="; ( cd "$NDIR" && find . -type f | sed 's|^\./||' )
echo "== exit: $RESULT =="
exit $RESULT
