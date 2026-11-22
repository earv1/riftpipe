#!/usr/bin/env bash
# End-to-end proof of device-linking auth with REAL `riftpipe` processes over the
# gossip mesh: a founder hosts a --restricted board, grants one device, and a
# second UNAUTHORIZED device is rejected.
#
#   founder (restricted, grants B) ── mesh ── B (authorized): reads + writes sync
#                                       └───── C (ungranted):  connected, but wrapped
#                                                               into nothing — no read, no write
#
# Read is now gated too: an un-granted device is a gossip member (it learns the
# founder's epoch) but is never wrapped into it, so it receives no content.
# Exit 0 on pass.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
BIN="$ROOT/target/debug/riftpipe"

cargo build --quiet --bin riftpipe || { echo "build failed"; exit 1; }

WORK="$(mktemp -d)"
FOUNDER="$WORK/founder"; MEMBER="$WORK/member"; OUTSIDER="$WORK/outsider"
mkdir -p "$FOUNDER" "$MEMBER" "$OUTSIDER"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; rm -rf "$WORK"; }
trap cleanup EXIT

# Pre-generate the member's + outsider's device identities so the founder can
# grant the member up front (stands in for the device-linking / request flow).
MEMBER_ID="$("$BIN" device-id "$MEMBER")"
"$BIN" device-id "$OUTSIDER" >/dev/null   # mint the outsider's identity too
echo "member device id: $MEMBER_ID"

echo "== founder hosts a restricted board, granting the member =="
"$BIN" connect --accept "$FOUNDER" --restricted --grant "$MEMBER_ID" >"$WORK/founder.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do [ -f "$FOUNDER.ticket" ] && break; sleep 0.3; done
TICKET="$(cat "$FOUNDER.ticket" 2>/dev/null || true)"
[ -n "$TICKET" ] || { echo "FAIL: founder never printed a ticket"; cat "$WORK/founder.log"; exit 1; }

# The founder writes a file — read is gated now, so it must reach the granted
# member but NOT the un-granted outsider.
echo "hello from founder" > "$FOUNDER/from_founder.txt"

echo "== authorized member joins =="
"$BIN" connect "$TICKET" "$MEMBER" --restricted >"$WORK/member.log" 2>&1 &
PIDS+=($!)
echo "== unauthorized outsider joins =="
"$BIN" connect "$TICKET" "$OUTSIDER" --restricted >"$WORK/outsider.log" 2>&1 &
PIDS+=($!)

# Both joiners write a file. The member's must reach the founder; the outsider's
# must be REJECTED.
sleep 4
echo "authorized member write" > "$MEMBER/from_member.txt"
echo "ungranted outsider write" > "$OUTSIDER/from_outsider.txt"

# Wait for convergence of the allowed read+write (the outsider must NOT read).
ok=0
for _ in $(seq 1 40); do
  if [ -f "$FOUNDER/from_member.txt" ] \
     && [ -f "$MEMBER/from_founder.txt" ]; then ok=1; break; fi
  sleep 0.5
done

echo "== results =="
fail=0
if [ "$ok" = 1 ]; then echo "OK: authorized member write reached founder; founder's write reached the member"; else
  echo "FAIL: authorized writes did not converge"; fail=1; fi

# The key security assertion: give the rejected write ample time, then confirm
# it never reached the founder or the member.
sleep 3
if [ -f "$FOUNDER/from_outsider.txt" ] || [ -f "$MEMBER/from_outsider.txt" ]; then
  echo "FAIL: an UNAUTHORIZED device's write was accepted!"; fail=1
else
  echo "OK: the unauthorized outsider's write was rejected everywhere"
fi

if [ -f "$OUTSIDER/from_founder.txt" ]; then echo "FAIL: un-granted outsider READ the founder's file (read must be gated)"; fail=1
else echo "OK: the un-granted outsider could not read the board"; fi
grep -q "no key for epoch 1" "$WORK/outsider.log" && echo "OK: outsider is connected (saw epoch 1, holds no key)" \
  || { echo "FAIL: outsider never saw the founder's epoch (not connected?)"; fail=1; }

grep -q "protocol-mismatch" "$WORK"/*.log && { echo "FAIL: unexpected protocol mismatch"; fail=1; }

[ "$fail" = 0 ] && echo "PASS: restricted mesh gates reads and writes" || echo "FAILED"
exit "$fail"
