#!/usr/bin/env bash
# The variant of run-cli-host-restart.sh on a `--restricted` board with a live
# revoke before the restart: native only, no browser or wasm. A founder hosts
# a restricted board, grants a member and a victim, both read epoch-1 content,
# the founder revokes the victim LIVE (rotating to epoch 2), then the founder
# is killed and restarted with IDENTICAL args. The restart must:
#   - resume the same board (same ticket secret);
#   - restore its trust log + epoch announces (`[auth] restored`);
#   - skip the replayed `--grant` for the still-revoked victim;
#   - keep the victim out of `members`, decrypt epoch-2 content, and still be
#     able to rotate to a fresh epoch on a NEW live revoke.
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

fail=0

B_ID="$("$BIN" device-id "$MEMBER")"
C_ID="$("$BIN" device-id "$VICTIM")"
echo "member device id: $B_ID"
echo "victim device id: $C_ID"

echo "== founder hosts a restricted board, granting member and victim =="
"$BIN" connect --accept "$FOUNDER" --restricted --grant "$B_ID" --grant "$C_ID" >"$WORK/founder1.log" 2>&1 &
FPID=$!
PIDS+=($FPID)
for _ in $(seq 1 30); do [ -f "$FOUNDER.ticket" ] && break; sleep 0.5; done
TICKET1="$(cat "$FOUNDER.ticket" 2>/dev/null || true)"
if [ -z "$TICKET1" ]; then echo "FAIL: founder never printed a ticket"; fail=1; fi

echo "hello epoch one" > "$FOUNDER/before.txt"

echo "== member and victim join (both granted) =="
"$BIN" connect "$TICKET1" "$MEMBER" --restricted >"$WORK/member.log" 2>&1 &
PIDS+=($!)
"$BIN" connect "$TICKET1" "$VICTIM" --restricted >"$WORK/victim.log" 2>&1 &
PIDS+=($!)

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
  grep -q "rotated to epoch 2" "$WORK/founder1.log" && { rotated=1; break; }
  sleep 0.5
done
if [ "$rotated" = 1 ]; then echo "OK: revoke triggered a rotation to epoch 2"
else echo "FAIL: founder never rotated after the revoke"; fail=1; fi

held=0
for _ in $(seq 1 40); do
  grep -q "holding epoch 2" "$WORK/member.log" && { held=1; break; }
  sleep 0.5
done
if [ "$held" = 1 ]; then echo "OK: member holds the epoch-2 key"
else echo "FAIL: member never received the epoch-2 key"; fail=1; fi

if ls "$FOUNDER"/.riftpipe-sync/*/auth.state >/dev/null 2>&1; then
  echo "OK: founder persisted its trust log + epochs"
else
  echo "FAIL: founder never wrote auth.state"; fail=1
fi

echo "== kill the founder (SIGTERM; no save at exit, so this proves state was saved at the time of the revoke) =="
kill "$FPID"
wait "$FPID" 2>/dev/null

# Gap edit: the member writes while the founder is down.
echo "member gap" > "$MEMBER/gap_b.txt"
sleep 3

echo "== restart the founder with identical args =="
rm -f "$FOUNDER.ticket"
"$BIN" connect --accept "$FOUNDER" --restricted --grant "$B_ID" --grant "$C_ID" >"$WORK/founder2.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do [ -f "$FOUNDER.ticket" ] && break; sleep 0.5; done
TICKET2="$(cat "$FOUNDER.ticket" 2>/dev/null || true)"
if [ -z "$TICKET2" ]; then echo "FAIL: restarted founder never printed a ticket"; fail=1; fi
if [ "${TICKET2:0:64}" = "${TICKET1:0:64}" ]; then
  echo "OK: restarted founder resumed the same board (matching ticket secret)"
else
  echo "FAIL: restarted founder's ticket secret changed"; fail=1
fi

if grep -q "\[auth\] restored" "$WORK/founder2.log"; then echo "OK: restarted founder restored its trust log + epochs"
else echo "FAIL: restarted founder did not print [auth] restored"; fail=1; fi
if grep -qF -- "--grant $C_ID skipped" "$WORK/founder2.log"; then echo "OK: the replayed --grant for the revoked victim was skipped"
else echo "FAIL: restarted founder did not skip the replayed --grant for the revoked victim"; fail=1; fi

gap=0
for _ in $(seq 1 180); do
  [ -f "$FOUNDER/gap_b.txt" ] && { gap=1; break; }
  sleep 0.5
done
if [ "$gap" = 1 ]; then echo "OK: the restarted host decrypts epoch-2 content"
else echo "FAIL: the restarted host never pulled the gap edit"; fail=1; fi

MEMBERS_JSON="$("$BIN" members "$FOUNDER" --json)"
if echo "$MEMBERS_JSON" | grep -q "$C_ID"; then echo "FAIL: the revoked victim is still listed as a member"; fail=1
else echo "OK: the revoked victim is not listed as a member"; fi
if echo "$MEMBERS_JSON" | grep -q "$B_ID"; then echo "OK: the authorized member is still listed"
else echo "FAIL: the authorized member is missing from members"; fail=1; fi

echo "after restart" > "$FOUNDER/after_restart.txt"
echo "revoked again" > "$VICTIM/from_revoked2.txt"

ok=0
for _ in $(seq 1 60); do
  [ -f "$MEMBER/after_restart.txt" ] && { ok=1; break; }
  sleep 0.5
done
if [ "$ok" = 1 ]; then echo "OK: the restarted host keeps syncing with the member"
else echo "FAIL: the restarted host stopped syncing with the member"; fail=1; fi

sleep 4
for f in "$VICTIM/after_restart.txt" "$VICTIM/gap_b.txt" "$FOUNDER/from_revoked2.txt" "$MEMBER/from_revoked2.txt"; do
  if [ -f "$f" ]; then
    echo "FAIL: $f exists — the revoked victim leaked read or write access across the restart"
    fail=1
  fi
done
if [ "$fail" = 0 ]; then
  echo "OK: the still-revoked victim read nothing new and its write never landed"
fi

if grep -q "rotated to epoch" "$WORK/founder2.log"; then
  echo "FAIL: the restarted host re-minted/rotated without a new live revoke"; fail=1
else
  echo "OK: no re-mint, no rotation yet"
fi

echo "== founder revokes the member on the LIVE (restarted) board =="
"$BIN" revoke "$FOUNDER" "$B_ID"

rotated2=0
for _ in $(seq 1 20); do
  grep -q "rotated to epoch 3" "$WORK/founder2.log" && { rotated2=1; break; }
  sleep 0.5
done
if [ "$rotated2" = 1 ]; then echo "OK: the restarted host still rotates on a new live revoke"
else echo "FAIL: the restarted host never rotated after the new revoke"; fail=1; fi

if grep -q "protocol-mismatch" "$WORK"/*.log; then
  echo "FAIL: unexpected protocol mismatch"; fail=1
fi

if [ "$fail" = 0 ]; then
  echo "PASS: restricted CLI host restart keeps a live revoke, reads and rotates"
else
  echo "FAILED"
  for f in "$WORK"/*.log; do echo "--- $f ---"; tail -20 "$f"; done
fi
exit "$fail"
