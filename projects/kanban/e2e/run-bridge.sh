#!/usr/bin/env bash
# Cross-stack browser↔native bridge: a real browser (web-sys WebRTC) and a native
# process (webrtc-rs) connect through the signaling server and exchange a message
# over WebRTC. Proves the two transport stacks interoperate.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8124
SIGPORT=9020
SIGNAL_URL="ws://127.0.0.1:$SIGPORT"
ROOM="bridge-$(openssl rand -hex 4)"

echo "== building native binary + wasm pkg =="
cargo build --quiet --bin riftpipe || exit 1
( cd web && wasm-pack build --target web --out-dir pkg ) >/tmp/bridge-wasm.log 2>&1 || { echo "wasm build failed"; tail -5 /tmp/bridge-wasm.log; exit 1; }

echo "== serving repo root (static) + signaling server on $SIGPORT =="
( python3 -m http.server $PORT --bind 127.0.0.1 ) >/tmp/bridge-http.log 2>&1 &
HTTP=$!
./target/debug/riftpipe signal --port $SIGPORT >/tmp/bridge-signal.log 2>&1 &
SIGNAL=$!
trap 'kill $HTTP $SIGNAL 2>/dev/null' EXIT
# Wait for BOTH the static server and the signaling port to accept connections.
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done
for _ in $(seq 1 50); do (echo > "/dev/tcp/127.0.0.1/$SIGPORT") 2>/dev/null && break; sleep 0.2; done

echo "== starting native peer (webrtc-rs) in room $ROOM =="
( ./target/debug/riftpipe webrtc-echo "$ROOM" --signal "$SIGNAL_URL" --send hello-from-native ) >/tmp/bridge-native.log 2>&1 &
NATIVE=$!
sleep 0.8  # let it register as the first room member

echo "== running browser side (Playwright) =="
( cd projects/kanban/e2e && PORT=$PORT ROOM="$ROOM" SIGNAL_URL="$SIGNAL_URL" node bridge.mjs )
RESULT=$?

wait $NATIVE 2>/dev/null
echo "== native peer output: $(cat /tmp/bridge-native.log) =="
if grep -q "GOT:hello-from-browser" /tmp/bridge-native.log; then
  echo "native (webrtc-rs) received the browser (web-sys) message: OK"
else
  echo "native did NOT receive the browser message"
  RESULT=1
fi
echo "== exit: $RESULT =="
exit $RESULT
