#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# V5.0 — Daily data collection pipeline
# Usage: ./scripts/daily-pipeline.sh [--sports=nba,nfl] [--providers=espn,oddsapi]
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
YEAR=$(date +%Y)

echo "═══════════════════════════════════════════════════"
echo "  V5.0 Daily Pipeline — $(date '+%Y-%m-%d %H:%M')"
echo "═══════════════════════════════════════════════════"

# Step 1: Import data
echo ""
echo "── Step 1: Importing data ──────────────────────────"
cd "$ROOT/importers"
npx tsx src/cli.ts --all --seasons="$YEAR" "$@"

# Step 2: Normalize
echo ""
echo "── Step 2: Normalizing data ────────────────────────"
cd "$ROOT/backend"
python3 -c "
from normalization import Normalizer
from config import ALL_SPORTS
n = Normalizer()
n.run_all(ALL_SPORTS, [$YEAR])
print('Normalization complete')
"

# Step 3: Extract features
echo ""
echo "── Step 3: Extracting features ─────────────────────"
python3 -c "
from features import extract_features
from config import ALL_SPORTS
from pathlib import Path
data_dir = Path('$ROOT/data')
for sport in ALL_SPORTS:
    try:
        df = extract_features(sport, $YEAR, data_dir)
        if df is not None and len(df) > 0:
            print(f'  {sport}: {len(df)} games, {len(df.columns)} features')
    except Exception as e:
        print(f'  {sport}: skipped ({e})')
print('Feature extraction complete')
"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Pipeline complete — $(date '+%H:%M')"
echo "═══════════════════════════════════════════════════"
