#!/usr/bin/env bash
# Pass-rate gate for a flaky-by-nature e2e (real relays, shared machine):
# run SCRIPT N times, each only once the 1-minute load is <= MAX_LOAD (a run
# under heavy load proves nothing), and exit 0 iff at least NEED runs passed.
# usage: run-repeat.sh <script.sh> [N=10] [NEED=9]   (env: MAX_LOAD, default 12)
set -uo pipefail
SCRIPT="${1:?usage: run-repeat.sh <script.sh> [N=10] [NEED=9]}"
N="${2:-10}"
NEED="${3:-9}"
MAX_LOAD="${MAX_LOAD:-12}"

# 1-minute load average; macOS locales may print a decimal comma.
load1() {
  if sysctl -n vm.loadavg >/dev/null 2>&1; then
    sysctl -n vm.loadavg | awk '{print $2}'
  else
    cut -d' ' -f1 /proc/loadavg
  fi | tr ',' '.'
}
load_ok() { awk -v l="$(load1)" -v m="$MAX_LOAD" 'BEGIN { exit !(l + 0 <= m + 0) }'; }

P=0
for i in $(seq 1 "$N"); do
  waited=0
  until load_ok; do
    if [ "$waited" -ge 600 ]; then
      echo "run $i: load $(load1) still > $MAX_LOAD after 600 s — giving up"
      echo "passed $P/$N"
      exit 1
    fi
    sleep 10
    waited=$((waited + 10))
  done
  LOG="/tmp/repeat-$(basename "$SCRIPT" .sh)-$i.log"
  echo "run $i: start (load $(load1)) → $LOG"
  if bash "$SCRIPT" >"$LOG" 2>&1; then
    echo "run $i: PASS"
    P=$((P + 1))
  else
    echo "run $i: FAIL"
  fi
done
echo "passed $P/$N"
[ "$P" -ge "$NEED" ]
