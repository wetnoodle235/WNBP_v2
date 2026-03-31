#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# V5.0 — Backfill historical data (2023–2026)
# Usage: ./scripts/backfill.sh [--providers=espn,nbastats]
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

echo "═══════════════════════════════════════════════════"
echo "  V5.0 Historical Backfill — 2023–2026"
echo "═══════════════════════════════════════════════════"

cd "$ROOT/importers"

# Run all providers for all seasons
npx tsx src/cli.ts --all --seasons=2023,2024,2025,2026 "$@"

echo ""
echo "Backfill complete!"
