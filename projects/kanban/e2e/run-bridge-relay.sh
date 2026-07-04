#!/usr/bin/env bash
# Cross-stack cross-NAT *fallback*: a real browser (web-sys) and a native process
# (webrtc-rs), BOTH forced to relay-only ICE, connect through a local TURN server
# (coturn) and exchange data over WebRTC — the worst-case path a hostile-NAT pair
# lands on, proven across the two stacks on one machine.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
PORT=8126
SIGPORT=9022
TURNPORT=3478
TURN_USER=testuser
TURN_PASS=testpass
SIGNAL_URL="ws://127.0.0.1:$SIGPORT"
TURN_URL="turn:127.0.0.1:$TURNPORT"
ROOM="brelay-$(openssl rand -hex 4)"

echo "== building native + wasm pkg =="
cargo build --quiet --bin riftpipe || exit 1
( cd projects/kanban/wasm && wasm-pack build --target web --out-dir pkg ) >/tmp/br-wasm.log 2>&1 || { echo "wasm build failed"; tail -5 /tmp/br-wasm.log; exit 1; }

echo "== starting coturn + signaling + static server =="
turnserver -n --lt-cred-mech --user "$TURN_USER:$TURN_PASS" --realm riftpipe \
  --listening-ip 127.0.0.1 --listening-port $TURNPORT --no-tls --no-dtls \
  --allow-loopback-peers --min-port 49200 --max-port 49260 \
  --log-file=/tmp/turn.log --simple-log --no-stdout-log >/dev/null 2>&1 &
TURN=$!
( python3 -m http.server $PORT --bind 127.0.0.1 ) >/tmp/br-http.log 2>&1 &
HTTP=$!
./target/debug/riftpipe signal --port $SIGPORT >/tmp/br-signal.log 2>&1 &
SIGNAL=$!
NATIVE=""
trap 'kill $TURN $HTTP $SIGNAL $NATIVE 2>/dev/null' EXIT
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && break; sleep 0.2; done
for _ in $(seq 1 50); do (echo > "/dev/tcp/127.0.0.1/$SIGPORT") 2>/dev/null && break; sleep 0.2; done
for _ in $(seq 1 40); do lsof -nP -iUDP:$TURNPORT >/dev/null 2>&1 && break; sleep 0.25; done

echo "== native peer, RELAY-ONLY =="
RIFTPIPE_FORCE_RELAY=1 RIFTPIPE_TURN="$TURN_URL" RIFTPIPE_TURN_USER="$TURN_USER" RIFTPIPE_TURN_PASS="$TURN_PASS" \
  ./target/debug/riftpipe webrtc-echo "$ROOM" --signal "$SIGNAL_URL" --send hello-from-native >/tmp/br-native.log 2>&1 &
NATIVE=$!
sleep 0.8

echo "== browser peer, RELAY-ONLY (Playwright) =="
( cd projects/kanban/e2e && PORT=$PORT ROOM="$ROOM" SIGNAL_URL="$SIGNAL_URL" \
   TURN_URL="$TURN_URL" TURN_USER="$TURN_USER" TURN_PASS="$TURN_PASS" node bridge.mjs )
RESULT=$?

wait $NATIVE 2>/dev/null
echo "== native: $(cat /tmp/br-native.log) =="
if [ $RESULT -eq 0 ] && grep -q "GOT:hello-from-browser" /tmp/br-native.log; then
  echo "PASS: browser (web-sys) <-> native (webrtc-rs) connected RELAY-ONLY through coturn"
  RESULT=0
else
  echo "FAIL: cross-stack relay-only did not complete"; RESULT=1
fi
echo "== exit: $RESULT =="
exit $RESULT
