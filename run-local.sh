#!/usr/bin/env bash
#
# Local two-peer demo of riftpipe in tmux. Symmetric 2x2 grid:
#
#   +---------------------+---------------------+
#   |  peer A — EDITOR    |  peer B — EDITOR    |   <- write here ($EDITOR / vim)
#   |     (the writing)   |     (the writing)   |
#   +---------------------+---------------------+
#   |  A metrics (thin)   |  B metrics (thin)   |   <- live: direct/relay, bytes, rate
#   +---------------------+---------------------+
#
# Both peers live-sync a shared document over a real (loopback) iroh connection.
# Each top pane is an editor; the riftpipe sync runs in the background of that
# pane. Save (:w) to push your edits; remote edits reload on idle (autoread).
#
# Stop everything:  tmux kill-session -t riftpipe-demo

set -euo pipefail
command -v tmux >/dev/null || { echo "tmux is required (brew install tmux)"; exit 1; }
command -v nvim >/dev/null || { echo "neovim is required (brew install neovim)"; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
echo "building..."
cargo build --quiet
BIN="$ROOT/target/debug/riftpipe"
LUA="$ROOT/nvim/riftpipe.lua"

WORK="$(mktemp -d)"
A="$WORK/peer-a.txt"; B="$WORK/peer-b.txt"
AM="$WORK/a.metrics"; BM="$WORK/b.metrics"
printf 'Just type — it syncs live (char by char).\n' > "$A"
: > "$B"; : > "$AM"; : > "$BM"; rm -f "$A.ticket"

# nvim with the riftpipe bridge loaded session-locally (no global install).
# $1 = file, $2 = riftpipe args (share/join ... --pipe --metrics ...)
# `-u NONE` isolates the demo from your nvim config (reproducible). For real use,
# just load the bridge in your normal nvim: nvim -c 'luafile .../riftpipe.lua' file
edcmd() {
  printf "RIFTPIPE_BIN=%q RIFTPIPE_ARGS=%q nvim -u NONE -c 'luafile %q' %q" "$BIN" "$2" "$LUA" "$1"
}
# Portable "watch": redraw a file every 0.5s.
viewer() { printf 'while :; do clear; cat %q 2>/dev/null; sleep 0.5; done' "$1"; }

SESSION="riftpipe-demo"
tmux kill-session -t "$SESSION" 2>/dev/null || true

# Build the 2x2 grid first (all panes), then launch into each.
PA=$(tmux new-session -d -s "$SESSION" -n demo -P -F '#{pane_id}' -x "$(tput cols)" -y "$(tput lines)")
PB=$(tmux split-window -h -t "$PA" -P -F '#{pane_id}')
PAM=$(tmux split-window -v -l 4 -t "$PA" -P -F '#{pane_id}')
PBM=$(tmux split-window -v -l 4 -t "$PB" -P -F '#{pane_id}')

# Peer A: nvim + bridge. The bridge spawns `riftpipe share --pipe`, which writes
# the ticket sidecar that peer B waits on.
tmux send-keys -t "$PA" "$(edcmd "$A" "share $A --pipe --metrics $AM")" C-m
# Peer B: wait for the ticket, then nvim + bridge joining with it.
tmux send-keys -t "$PB" "while [ ! -s '$A.ticket' ]; do sleep 0.3; done; RIFTPIPE_BIN='$BIN' RIFTPIPE_ARGS=\"join \$(cat '$A.ticket') $B --pipe --metrics $BM\" nvim -u NONE -c 'luafile $LUA' '$B'" C-m
# Metrics viewers.
tmux send-keys -t "$PAM" "$(viewer "$AM")" C-m
tmux send-keys -t "$PBM" "$(viewer "$BM")" C-m

# Focus the writing.
tmux select-pane -t "$PA"

cat <<EOF

  riftpipe local demo — 2x2 (editors on top, metrics below)
  ----------------------------------------------------------
  peer A: $A
  peer B: $B

  Just type in a top pane (neovim) — edits sync live, char by char, via the
  riftpipe --pipe bridge (no :w, no file, no polling). Bottom panes show each
  peer's live connection (direct/relay), bytes, and rate.

  Stop all:  tmux kill-session -t $SESSION

EOF

sleep 1
tmux attach -t "$SESSION"
