#!/usr/bin/env bash
# End-to-end proof of READ REVOCATION with REAL `riftpipe` processes over the
# gossip mesh: a founder hosts a --restricted board, grants two devices (B and
# C), both read the founder's epoch-1 content, then the founder revokes C on
# the LIVE board with `riftpipe revoke` (the same control dotfile the running
# admin peer polls). That rotates to epoch 2, wrapped only to {founder, B}:
#
#   founder (restricted, grants B, C) ── mesh ── B (authorized): keeps reading + writing
#                                           └──── C (revoked):    connected, but holds
#                                                                  only the old epoch — no
#                                                                  new reads, no writes
#
# Exit 0 on pass.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
BIN="$ROOT/target/debug/riftpipe"

cargo build --quiet --bin riftpipe || { echo "build failed"; exit 1; }

WORK="$(mktemp -d)"
FOUNDER="$WORK/founder"; MEMBER="$WORK/member"; VICTIM="$WORK/victim"
mkdir -p "$FOUNDER" "$MEMBER" "$VICTIM"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; rm -rf "$WORK"; }
trap cleanup EXIT

# Pre-generate the member's + victim's device identities so the founder can
# grant both up front.
B_ID="$("$BIN" device-id "$MEMBER")"
C_ID="$("$BIN" device-id "$VICTIM")"
echo "member device id: $B_ID"
echo "victim device id: $C_ID"

echo "== founder hosts a restricted board, granting member and victim =="
"$BIN" connect --accept "$FOUNDER" --restricted --grant "$B_ID" --grant "$C_ID" >"$WORK/founder.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do [ -f "$FOUNDER.ticket" ] && break; sleep 0.3; done
TICKET="$(cat "$FOUNDER.ticket" 2>/dev/null || true)"
[ -n "$TICKET" ] || { echo "FAIL: founder never printed a ticket"; cat "$WORK/founder.log"; exit 1; }

echo "hello epoch one" > "$FOUNDER/before.txt"

echo "== member and victim join (both granted) =="
"$BIN" connect "$TICKET" "$MEMBER" --restricted >"$WORK/member.log" 2>&1 &
PIDS+=($!)
"$BIN" connect "$TICKET" "$VICTIM" --restricted >"$WORK/victim.log" 2>&1 &
PIDS+=($!)

fail=0

# Phase 1: both granted devices must read the founder's epoch-1 content.
ok=0
for _ in $(seq 1 60); do
  if [ -f "$MEMBER/before.txt" ] && [ -f "$VICTIM/before.txt" ]; then ok=1; break; fi
  sleep 0.5
done
if [ "$ok" = 1 ]; then echo "OK: member and victim read epoch-1 content"
else echo "FAIL: members never got epoch-1 content"; fail=1; fi

echo "== founder revokes the victim on the LIVE board =="
"$BIN" revoke "$FOUNDER" "$C_ID"

rotated=0
for _ in $(seq 1 20); do
  grep -q "rotated to epoch 2" "$WORK/founder.log" && { rotated=1; break; }
  sleep 0.5
done
if [ "$rotated" = 1 ]; then echo "OK: revoke triggered a rotation to epoch 2"
else echo "FAIL: founder never rotated after the revoke"; fail=1; fi

# The member seals under the highest epoch it HOLDS: wait until it has
# ingested epoch 2, or its write could go out under epoch 1 (which the victim
# still holds) and leak — a test race, not a protocol bug.
held=0
for _ in $(seq 1 40); do
  grep -q "holding epoch 2" "$WORK/member.log" && { held=1; break; }
  sleep 0.5
done
if [ "$held" = 1 ]; then echo "OK: member holds the epoch-2 key"
else echo "FAIL: member never received the epoch-2 key"; fail=1; fi

# Phase 2: post-rotation content from the founder and from the remaining
# member must still converge between them; the revoked victim writes too (its
# write must never land anywhere — it's unauthorized).
echo "epoch two" > "$FOUNDER/after.txt"
echo "member, epoch two" > "$MEMBER/after_b.txt"
echo "revoked write" > "$VICTIM/from_revoked.txt"

ok=0
for _ in $(seq 1 60); do
  if [ -f "$MEMBER/after.txt" ] && [ -f "$FOUNDER/after_b.txt" ]; then ok=1; break; fi
  sleep 0.5
done
if [ "$ok" = 1 ]; then echo "OK: remaining members keep syncing after the rotation"
else echo "FAIL: remaining members stopped syncing"; fail=1; fi

# The key security assertion: give the revoked victim ample time, then confirm
# it got none of the post-rotation content, and that its own write never landed.
sleep 4
for f in "$VICTIM/after.txt" "$VICTIM/after_b.txt" "$FOUNDER/from_revoked.txt" "$MEMBER/from_revoked.txt"; do
  if [ -f "$f" ]; then
    echo "FAIL: $f exists — the revoked victim leaked read or write access"
    fail=1
  fi
done
if [ "$fail" = 0 ]; then
  echo "OK: the revoked victim read none of the post-rotation content, and its write never landed"
fi

grep -q "no key for epoch 2" "$WORK/victim.log" && echo "OK: the revoked victim is connected (saw epoch 2, holds no key)" \
  || { echo "FAIL: the revoked victim never saw the rotation (not connected?)"; fail=1; }

grep -q "protocol-mismatch" "$WORK"/*.log && { echo "FAIL: unexpected protocol mismatch"; fail=1; }

if [ "$fail" = 0 ]; then
  echo "PASS: read revocation by epoch rotation works on a live board"
else
  echo "FAILED"
  for f in "$WORK"/*.log; do echo "--- $f ---"; tail -20 "$f"; done
fi
exit "$fail"
