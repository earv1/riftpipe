#!/usr/bin/env bash
# Two-peer kanban demo: two independent kanban servers (A and B), each over its
# own board directory, kept in sync by riftpipe. Edit a card in one window and
# watch it appear in the other — no server, end-to-end encrypted.
#
#   peer A  ->  http://localhost:8000   (board: $WORK/A)
#   peer B  ->  http://localhost:8001   (board: $WORK/B)
#
# Env:
#   KANBAN_BROWSER=none   don't auto-open browser windows (use VS Code's
#                         "Simple Browser: Show" with the two URLs instead)
#   KANBAN_DEMO_DIR=...   where the throwaway boards live (default a temp dir)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"            # riftpipe repo root
RIFTPIPE="$ROOT/target/debug/riftpipe"
WORK="${KANBAN_DEMO_DIR:-${TMPDIR:-/tmp}/riftpipe-kanban-demo}"
PORT_A=8000
PORT_B=8001

PIDS=()
cleanup() {
  echo
  echo "stopping demo…"
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

# 1. build riftpipe (sync layer) if needed
if [ ! -x "$RIFTPIPE" ]; then
  echo "building riftpipe…"
  cargo build --manifest-path "$ROOT/Cargo.toml"
fi

# 2. build the kanban UI once (both servers serve the same dist/)
echo "building kanban UI…"
( cd "$HERE" && deno task build >/dev/null )

# 3. fresh boards: A seeded from the sample, B empty (its content arrives via sync)
rm -rf "$WORK"
mkdir -p "$WORK/A" "$WORK/B"
cp -R "$HERE/board/." "$WORK/A/"
cp "$HERE/board/riftpipe.toml" "$WORK/B/"    # B needs its own manifest (not synced)

# 4. riftpipe: share A, grab the ticket, join from B
echo "starting riftpipe (peer A shares)…"
( "$RIFTPIPE" share "$WORK/A" >"$WORK/share.log" 2>&1 ) & PIDS+=($!)
echo -n "waiting for ticket"
for _ in $(seq 1 50); do [ -s "$WORK/A.ticket" ] && break; echo -n "."; sleep 0.2; done
echo
TICKET="$(cat "$WORK/A.ticket")"
echo "starting riftpipe (peer B joins)…"
( "$RIFTPIPE" join "$TICKET" "$WORK/B" >"$WORK/join.log" 2>&1 ) & PIDS+=($!)

# 5. one kanban server per board, on its own port
echo "starting kanban server A on :$PORT_A …"
( cd "$HERE" && KANBAN_DIR="$WORK/A" KANBAN_PORT=$PORT_A deno run -A server/main.ts \
    >"$WORK/serverA.log" 2>&1 ) & PIDS+=($!)
echo "starting kanban server B on :$PORT_B …"
( cd "$HERE" && KANBAN_DIR="$WORK/B" KANBAN_PORT=$PORT_B deno run -A server/main.ts \
    >"$WORK/serverB.log" 2>&1 ) & PIDS+=($!)

# 6. wait for both servers to answer
for url in "http://localhost:$PORT_A/api/board" "http://localhost:$PORT_B/api/board"; do
  for _ in $(seq 1 50); do curl -sf "$url" >/dev/null 2>&1 && break; sleep 0.2; done
done

# 7. open one browser window per server
URL_A="http://localhost:$PORT_A"
URL_B="http://localhost:$PORT_B"
if [ "${KANBAN_BROWSER:-auto}" != "none" ]; then
  opener=""
  command -v open >/dev/null 2>&1 && opener="open"
  [ -z "$opener" ] && command -v xdg-open >/dev/null 2>&1 && opener="xdg-open"
  if [ -n "$opener" ]; then "$opener" "$URL_A" >/dev/null 2>&1 || true; "$opener" "$URL_B" >/dev/null 2>&1 || true; fi
fi

cat <<EOF

  riftpipe kanban — two-peer demo (Ctrl-C to stop)
    peer A   $URL_A     board: $WORK/A
    peer B   $URL_B     board: $WORK/B

  Edit a card in one window; it appears in the other (riftpipe syncs the files).
  B starts empty and fills in once the peers connect (a few seconds).

  VS Code: for the built-in browser, run
    Command Palette → "Simple Browser: Show"  and paste each URL
    (or use the Ports panel's preview). Set KANBAN_BROWSER=none to skip auto-open.

  logs: $WORK/{share,join,serverA,serverB}.log
EOF

wait
