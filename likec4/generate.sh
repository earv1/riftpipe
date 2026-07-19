#!/usr/bin/env bash
# Regenerate diagram images + model JSON from the LikeC4 sources in likec4/.
# Outputs land in docs/diagrams/ (PNG per view, flattened, + model.json), which
# the markdown files embed. Run via `npm run diagrams`; `npm run diagrams:watch`
# re-runs this on any change under likec4/.
#
# ANTHROPIC_API_KEY is cleared for the run: when it's set, likec4's dev server
# tries to pull in an optional @tanstack/ai-anthropic package and errors out.
# The export itself needs no AI, so we just run without it.
set -euo pipefail
cd "$(dirname "$0")/.."

SRC=likec4
OUT=docs/diagrams
mkdir -p "$OUT"

echo "[diagrams] exporting PNGs -> $OUT"
ANTHROPIC_API_KEY='' npx likec4 export png --output "$OUT" --flat "$SRC"

echo "[diagrams] exporting model JSON -> $OUT/model.json"
ANTHROPIC_API_KEY='' npx likec4 export json --outfile "$OUT/model.json" "$SRC"

echo "[diagrams] done"
