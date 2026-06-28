#!/usr/bin/env bash
# Cross-NAT *fallback* test on one machine: force both peers to RELAY-ONLY ICE
# (host + srflx candidates banned) and run a local TURN server (coturn). If they
# still connect, the worst-case path a symmetric/CGNAT pair lands on works.
# This stage: two native webrtc-rs peers, relay-only, through coturn.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
SIGPORT=9025
TURNPORT=3478
TURN_USER=testuser
TURN_PASS=testpass
ROOM="relay-$(openssl rand -hex 4)"

cargo build --quiet --bin riftpipe || exit 1

echo "== starting coturn (TURN) + signaling =="
turnserver -n --lt-cred-mech --user "$TURN_USER:$TURN_PASS" --realm riftpipe \
  --listening-ip 127.0.0.1 --listening-port $TURNPORT --no-tls --no-dtls \
  --allow-loopback-peers \
  --min-port 49200 --max-port 49260 --log-file=/tmp/turn.log --simple-log --no-stdout-log >/dev/null 2>&1 &
TURN=$!
./target/debug/riftpipe signal --port $SIGPORT >/tmp/relay-signal.log 2>&1 &
SIGNAL=$!
trap 'kill $TURN $SIGNAL 2>/dev/null' EXIT
# Wait until coturn is actually listening on UDP 3478.
for _ in $(seq 1 40); do lsof -nP -iUDP:$TURNPORT >/dev/null 2>&1 && break; sleep 0.25; done
lsof -nP -iUDP:$TURNPORT >/dev/null 2>&1 && echo "coturn listening on UDP $TURNPORT" || echo "WARN: coturn not listening"

export RIFTPIPE_FORCE_RELAY=1
export RIFTPIPE_TURN="turn:127.0.0.1:$TURNPORT"
export RIFTPIPE_TURN_USER="$TURN_USER"
export RIFTPIPE_TURN_PASS="$TURN_PASS"

echo "== two native peers, RELAY-ONLY, room $ROOM =="
( ./target/debug/riftpipe webrtc-echo "$ROOM" --signal "ws://127.0.0.1:$SIGPORT" --send ping-a ) >/tmp/relay-a.log 2>&1 &
sleep 0.6
( ./target/debug/riftpipe webrtc-echo "$ROOM" --signal "ws://127.0.0.1:$SIGPORT" --send ping-b ) >/tmp/relay-b.log 2>&1 &

# Wait (relay setup is slower than direct) for both GOT lines.
for _ in $(seq 1 60); do
  grep -q "GOT:" /tmp/relay-a.log 2>/dev/null && grep -q "GOT:" /tmp/relay-b.log 2>/dev/null && break
  sleep 0.5
done

echo "== A: $(cat /tmp/relay-a.log) =="
echo "== B: $(cat /tmp/relay-b.log) =="
ALLOCS=$(grep -ci "allocat" /tmp/turn.log 2>/dev/null || echo 0)
echo "== coturn allocation log lines (proof traffic used TURN): $ALLOCS =="
if grep -q "GOT:ping-b" /tmp/relay-a.log && grep -q "GOT:ping-a" /tmp/relay-b.log; then
  echo "PASS: two webrtc-rs peers connected RELAY-ONLY through coturn (cross-NAT fallback)"
  exit 0
else
  echo "FAIL: relay-only peers did not connect"
  echo "-- coturn log tail --"; tail -15 /tmp/turn.log
  exit 1
fi
