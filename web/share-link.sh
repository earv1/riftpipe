#!/usr/bin/env bash
# Mint a fresh connection id and print a shareable link. Two people who open the
# same link land in the same room (the id in the URL hash) and connect directly,
# peer-to-peer — no account, no database, just the link.
#
# Usage: ./share-link.sh [base-url]      (default: http://localhost:8080)
set -euo pipefail
BASE="${1:-http://localhost:8080}"
ID=$(openssl rand -hex 8)   # 16 hex chars; avoids the `tr|head` SIGPIPE under set -e
echo "${BASE%/}/#${ID}"
