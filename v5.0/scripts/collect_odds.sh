#!/bin/bash
# ──────────────────────────────────────────────────────────
# SportStock V5.0 — Odds Collection Wrapper
# ──────────────────────────────────────────────────────────
#
# Wrapper for odds collection that handles opening/closing/current modes.
# Used by systemd timers and cron jobs.
#
# Usage:
#   ./scripts/collect_odds.sh                  # default: current mode
#   ./scripts/collect_odds.sh opening          # opening odds
#   ./scripts/collect_odds.sh closing          # closing odds (checks schedule)
#   ./scripts/collect_odds.sh current          # current snapshot
#   ./scripts/collect_odds.sh opening nba,nfl  # specific sports
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
MODE="${1:-current}"
SPORTS="${2:-nba,nfl,nhl,mlb,epl}"
LOGDIR="$ROOT/logs"
mkdir -p "$LOGDIR"
LOGFILE="$LOGDIR/odds_${MODE}.log"

cd "$ROOT"

# Load environment variables if .env exists
if [[ -f "$ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$ROOT/.env"
    set +a
fi

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOGFILE"
}

# For closing mode, check if any games are starting soon
if [[ "$MODE" == "closing" ]]; then
    # Check if games start within 10 minutes
    CLOSING_CHECK=$(python3 scripts/game_schedule.py --closing-window 10 --sports "$SPORTS" --json 2>/dev/null || echo "[]")

    if [[ "$CLOSING_CHECK" == "[]" ]]; then
        # No games starting soon — skip collection (only log at debug level)
        exit 0
    fi

    GAME_COUNT=$(echo "$CLOSING_CHECK" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
    log "Closing mode: $GAME_COUNT game(s) starting within 10 minutes"
fi

log "Starting odds collection: mode=$MODE, sports=$SPORTS"

python3 scripts/odds_scheduler.py \
    --mode "$MODE" \
    --sport "$SPORTS" \
    --once \
    --normalize \
    2>&1 | tee -a "$LOGFILE"

EXIT_CODE=${PIPESTATUS[0]}

if [[ $EXIT_CODE -eq 0 ]]; then
    log "Odds collection ($MODE) completed successfully"
else
    log "ERROR: Odds collection ($MODE) failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE
