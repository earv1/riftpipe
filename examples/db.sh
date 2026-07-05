#!/usr/bin/env bash
# riftpipe db sync — a generic CLI showcase (PLANNED — not wired yet).
#
# The db mode syncs a write-ahead log rather than a directory: each writer
# appends whole frames (rows, ops, transactions) and a deterministic linearizer
# folds every writer's frames into one agreed order, so N peers converge on the
# same table without a central db. This is the log-native counterpart to
# folder.sh — state (the WAL) synced separately from a view (the rendered rows).
# See docs/planned/wal-db.md for the model; the core linearizer exists, the CLI
# wiring does not yet.
#
# Intended shape (mirrors folder.sh exactly — same generic verbs):
#
#   ./db.sh host <db-path>
#       Go online and print a share ticket, then wait for one peer. The db's WAL
#       replicates to whoever joins.
#
#   ./db.sh join <ticket|share-link|connection-id> <db-path> [-- extra flags]
#       Join a peer and replicate <db-path>'s WAL with them. Local commits append
#       frames; remote frames linearize in and materialize as rows.
#
# This script is app-agnostic: riftpipe ships the ordering, the app supplies the
# reducer (frames -> rows). No schema lives in riftpipe.
set -euo pipefail

echo "db sync is planned, not wired yet." >&2
echo "The core linearizer exists (core/src/wal.rs); the CLI \`connect\` binding does not." >&2
echo "Design + intended CLI shape: docs/planned/wal-db.md and the header of this script." >&2
exit 2
