#!/usr/bin/env bash
#
# Local two-peer demo of autoshare in tmux. Symmetric 2x2 grid:
#
#   +---------------------+---------------------+
#   |  peer A — EDITOR    |  peer B — EDITOR    |   <- write here ($EDITOR / vim)
#   |     (the writing)   |     (the writing)   |
#   +---------------------+---------------------+
#   |  A metrics (thin)   |  B metrics (thin)   |   <- live: direct/relay, bytes, rate
#   +---------------------+---------------------+
#
# Both peers live-sync a shared document over a real (loopback) iroh connection.
# Each top pane is an editor; the autoshare sync runs in the background of that
# pane. Save (:w) to push your edits; remote edits reload on idle (autoread).
#
# Stop everything:  tmux kill-session -t autoshare-demo

set -euo pipefail
command -v tmux >/dev/null || { echo "tmux is required (brew install tmux)"; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
echo "building..."
cargo build --quiet
BIN="$ROOT/target/debug/autoshare"
ED="${EDITOR:-vim}"

WORK="$(mktemp -d)"
A="$WORK/peer-a.txt"; B="$WORK/peer-b.txt"
AM="$WORK/a.metrics"; BM="$WORK/b.metrics"
printf 'Just type — it auto-saves and syncs.\n' > "$A"
: > "$B"; : > "$AM"; : > "$BM"; rm -f "$A.ticket"

# Editor that AUTO-SAVES on every change (no :w, push) and CONSTANTLY pulls remote
# edits via a repeating timer running `checktime` ~4x/sec (not just when idle).
# `noautocmd write` avoids re-triggering on the write itself.
edcmd() {
  printf "%s -c 'set autoread' -c 'au TextChanged,TextChangedI,InsertLeave * silent! noautocmd write' -c 'call timer_start(250, {-> execute(\"silent! checktime\")}, {\"repeat\": -1})' %q" "$ED" "$1"
}
# Portable "watch": redraw a file every 0.5s.
viewer() { printf 'while :; do clear; cat %q 2>/dev/null; sleep 0.5; done' "$1"; }

SESSION="autoshare-demo"
tmux kill-session -t "$SESSION" 2>/dev/null || true

# Build the 2x2 grid first (all panes), then launch into each.
PA=$(tmux new-session -d -s "$SESSION" -n demo -P -F '#{pane_id}' -x "$(tput cols)" -y "$(tput lines)")
PB=$(tmux split-window -h -t "$PA" -P -F '#{pane_id}')
PAM=$(tmux split-window -v -l 4 -t "$PA" -P -F '#{pane_id}')
PBM=$(tmux split-window -v -l 4 -t "$PB" -P -F '#{pane_id}')

# Peer A: share in background (writes the ticket sidecar), then edit.
tmux send-keys -t "$PA" "'$BIN' share '$A' --metrics '$AM' >/dev/null 2>'$WORK/a.log' & sleep 0.6; $(edcmd "$A")" C-m
# Peer B: wait for the ticket, join in background, then edit.
tmux send-keys -t "$PB" "while [ ! -s '$A.ticket' ]; do sleep 0.3; done; '$BIN' join \"\$(cat '$A.ticket')\" '$B' --metrics '$BM' >/dev/null 2>'$WORK/b.log' & sleep 0.6; $(edcmd "$B")" C-m
# Metrics viewers.
tmux send-keys -t "$PAM" "$(viewer "$AM")" C-m
tmux send-keys -t "$PBM" "$(viewer "$BM")" C-m

# Focus the writing.
tmux select-pane -t "$PA"

cat <<EOF

  autoshare local demo — 2x2 (editors on top, metrics below)
  ----------------------------------------------------------
  peer A: $A
  peer B: $B

  Just type in a top pane — it auto-saves and syncs; the other peer's editor
  reloads on idle. Bottom panes show live connection (direct/relay), bytes, rate.

  Stop all:  tmux kill-session -t $SESSION

EOF

sleep 1
tmux attach -t "$SESSION"
