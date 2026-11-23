#!/usr/bin/env bash
# End-to-end proof of CLI REQUEST-TO-JOIN with REAL `riftpipe` processes over the
# gossip mesh: a founder hosts a --restricted board with no `--grant`s. A keyless
# joiner connects with `--restricted --name`, which auto-broadcasts a signed join
# request. The founder's `riftpipe requests` inbox and `.riftpipe-auth.json`
# surface it with a safety code; `riftpipe approve <dir> <safety-code>` (the same
# control-file path `grant`/`revoke` use) grants it, and the joiner reads the
# founder's pre-existing content and starts writing back.
#
#   founder (restricted, no grants) ── mesh ── joiner (keyless, --name "e2e joiner")
#       │ sees the request in `riftpipe requests`, approves by safety code
#       └── after approve: joiner reads + writes; `riftpipe members` lists it
#
# Exit 0 on pass.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
BIN="$ROOT/target/debug/riftpipe"

cargo build --quiet --bin riftpipe || { echo "build failed"; exit 1; }

WORK="$(mktemp -d)"
FOUNDER="$WORK/founder"; JOINER="$WORK/joiner"
mkdir -p "$FOUNDER" "$JOINER"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; rm -rf "$WORK"; }
trap cleanup EXIT

# Pre-generate the joiner's device id so we can check for it in the founder's
# inbox / member list by id, not just by name.
J_ID="$("$BIN" device-id "$JOINER")"
echo "joiner device id: $J_ID"

fail=0

echo "== founder hosts a restricted board, granting no one up front =="
"$BIN" connect --accept "$FOUNDER" --restricted >"$WORK/founder.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do [ -f "$FOUNDER.ticket" ] && break; sleep 0.3; done
TICKET="$(cat "$FOUNDER.ticket" 2>/dev/null || true)"
[ -n "$TICKET" ] || { echo "FAIL: founder never printed a ticket"; cat "$WORK/founder.log"; exit 1; }

echo "members only" > "$FOUNDER/secret.txt"

echo "== keyless joiner connects --restricted --name, auto-broadcasting a join request =="
"$BIN" connect "$TICKET" "$JOINER" --restricted --name "e2e joiner" >"$WORK/joiner.log" 2>&1 &
PIDS+=($!)

CODE=""
for _ in $(seq 1 20); do
  CODE="$(sed -n 's/.*join request name=.* safety=\([0-9a-f]\{4\} [0-9a-f]\{4\}\).*/\1/p' "$WORK/joiner.log" | head -1)"
  [ -n "$CODE" ] && break
  sleep 0.3
done
if [ -z "$CODE" ]; then
  echo "FAIL: joiner never printed a join-request safety code"
  cat "$WORK/joiner.log"
  exit 1
fi
echo "safety code: $CODE"

ok=0
for _ in $(seq 1 60); do
  if "$BIN" requests "$FOUNDER" 2>/dev/null | grep -q "^$CODE  $J_ID  e2e joiner$"; then ok=1; break; fi
  sleep 0.5
done
if [ "$ok" = 1 ]; then echo "OK: the request reached the founder's inbox"
else echo "FAIL: the founder's inbox never showed the joiner's request"; fail=1; fi

sleep 3
if [ -f "$JOINER/secret.txt" ]; then
  echo "FAIL: $JOINER/secret.txt exists — an unapproved joiner read restricted content"
  fail=1
fi

if "$BIN" approve "$FOUNDER" 00000000 >/dev/null 2>&1; then
  echo "FAIL: an unknown safety code was accepted"
  fail=1
fi

if ! "$BIN" approve "$FOUNDER" "$CODE"; then
  echo "FAIL: approve by safety code did not exit 0"
  fail=1
fi

ok=0
for _ in $(seq 1 60); do
  [ -f "$JOINER/secret.txt" ] && { ok=1; break; }
  sleep 0.5
done
if [ "$ok" = 1 ]; then echo "OK: the approved joiner reads the founder's pre-existing content"
else echo "FAIL: the approved joiner never read the founder's content"; fail=1; fi

grep -q 'approving request from "e2e joiner"' "$WORK/founder.log" \
  && echo "OK: founder logged approving the named request" \
  || { echo "FAIL: founder log missing the approving-request line"; fail=1; }
grep -q "read key acquired" "$WORK/joiner.log" \
  && echo "OK: joiner logged acquiring the read key" \
  || { echo "FAIL: joiner log missing the read-key-acquired line"; fail=1; }

echo "from the joiner" > "$JOINER/reply.txt"
ok=0
for _ in $(seq 1 60); do
  [ -f "$FOUNDER/reply.txt" ] && { ok=1; break; }
  sleep 0.5
done
if [ "$ok" = 1 ]; then echo "OK: the newly-approved joiner's write reaches the founder"
else echo "FAIL: the joiner's write never reached the founder"; fail=1; fi

ok=0
for _ in $(seq 1 20); do
  if [ -z "$("$BIN" requests "$FOUNDER" 2>/dev/null)" ] && "$BIN" members "$FOUNDER" 2>/dev/null | grep -q "^$J_ID  member$"; then
    ok=1
    break
  fi
  sleep 0.5
done
if [ "$ok" = 1 ]; then echo "OK: founder's inbox is empty and the joiner is listed as a member"
else echo "FAIL: founder's requests/members never settled"; fail=1; fi

if ! "$BIN" members "$JOINER" 2>/dev/null | grep -q "^$J_ID  member  (this device)$"; then
  echo "FAIL: joiner's own members view doesn't show itself as a member"
  fail=1
fi

grep -q "protocol-mismatch" "$WORK"/*.log && { echo "FAIL: unexpected protocol mismatch"; fail=1; }

if [ "$fail" = 0 ]; then
  echo "PASS: request → approve (by safety code) works between CLI peers"
else
  echo "FAILED"
  for f in "$WORK"/*.log; do echo "--- $f ---"; tail -20 "$f"; done
fi
exit "$fail"
