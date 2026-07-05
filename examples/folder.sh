#!/usr/bin/env bash
# riftpipe folder sync — a generic CLI showcase.
#
# riftpipe converges a *directory tree* between peers: text files (`*.md`) merge
# as CRDTs, everything else last-writer-wins, dot-paths never sync. This script
# is app-agnostic — it syncs whatever files are in the folder and never parses
# their meaning. Point it at a notes dir, a config dir, or a board an app hosts;
# it's all the same to riftpipe.
#
#   ./folder.sh host <dir>
#       Host a gossip-mesh swarm for <dir> and print a mesh ticket. It's the same
#       protocol a browser host speaks, so ANY peer — a browser or another CLI —
#       joins the same swarm with the ticket. N peers, edits converge live.
#
#   ./folder.sh join <ticket|share-link|connection-id> <dir> [-- extra flags]
#       Join a peer and sync <dir> with them. Accepts a mesh ticket (from `host`),
#       a browser share URL (…/#<id>) or its bare mesh ticket, a legacy base32
#       ticket, or a signaling connection-id (add `--signal ws://…` after the dir).
#
# Edit any file in <dir> with your $EDITOR while it runs; `SYNCED:` lines report
# convergence. This is the folder mode; see ./db.sh for the planned db mode.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
BIN="$ROOT/target/debug/riftpipe"

build() { [ -x "$BIN" ] || { echo "== building riftpipe"; (cd "$ROOT" && cargo build --bin riftpipe); }; }

case "${1:-}" in
  host)
    DIR="${2:?usage: ./folder.sh host <dir>}"
    build
    mkdir -p "$DIR"
    echo "== hosting $DIR — share the printed ticket, then edit files here"
    exec "$BIN" connect --accept "$DIR"
    ;;
  join)
    LINK="${2:?usage: ./folder.sh join <ticket|share-link|connection-id> <dir> [-- extra flags]}"
    DIR="${3:?usage: ./folder.sh join <ticket|share-link|connection-id> <dir> [-- extra flags]}"
    build
    mkdir -p "$DIR"
    EXTRA=(); [ $# -gt 3 ] && EXTRA=("${@:4}")
    echo "== joining peer → $DIR"
    echo "   edit any file there with \$EDITOR; SYNCED: lines show convergence"
    exec "$BIN" connect "$LINK" "$DIR" ${EXTRA[@]+"${EXTRA[@]}"}
    ;;
  *)
    sed -n '2,21p' "$0"; exit 1 ;;
esac
