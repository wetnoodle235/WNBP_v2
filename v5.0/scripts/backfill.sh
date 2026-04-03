#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# V5.0 — Backfill historical data (2020–2026)
# Usage: ./scripts/backfill.sh [--providers=espn,nbastats]
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

echo "═══════════════════════════════════════════════════"
echo "  V5.0 Historical Backfill — 2020–2026"
echo "═══════════════════════════════════════════════════"

cd "$ROOT/importers"

# ── Standard backfill (2023–2026, all providers) ─────────
echo ""
echo "── All providers — 2023–2026 ───────────────────────"
npx tsx src/cli.ts --all --seasons=2023,2024,2025,2026 "$@"

# ── Extended backfill — 2020–2022 ────────────────────────
# These providers have good historical coverage back to 2020.
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Extended backfill — 2020–2022"
echo "═══════════════════════════════════════════════════"

# Historical stats providers with deep archives
echo ""
echo "── Statcast (MLB, 2020–2022) ────────────────────────"
npx tsx src/cli.ts --providers=statcast --seasons=2020,2021,2022 "$@"

echo ""
echo "── nflverse (NFL schedules/rosters/stats, 2020–2022) ─"
npx tsx src/cli.ts --providers=nflverse --sports=nfl --seasons=2020,2021,2022 "$@"

echo ""
echo "── Wikipedia (teams/leagues, season-neutral) ─────────"
# Wikipedia content is not season-scoped but we still run per season
# so data is written with the correct season tag
npx tsx src/cli.ts --providers=wikipedia --seasons=2025 "$@"

echo ""
echo "── nflverse full extended (2020–2022) ────────────────"
npx tsx src/cli.ts --providers=nflverse --sports=nfl --seasons=2020,2021,2022 "$@"

# ── Enhanced providers — full range 2020–2026 ────────────
# New providers added 2026-04 with historical data support.
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Enhanced providers — 2020–2026"
echo "═══════════════════════════════════════════════════"

ENHANCED_PROVIDERS="openmeteo,reddit,googlenews,rssnews,ticketmaster,youtube,googletrends,actionnetwork,draftkings"

echo ""
echo "── Current-day/recent data providers ─────────────"
# These providers fetch current/recent data — run once for today
# (they don't have per-season historical archives)
npx tsx src/cli.ts --providers=reddit,googlenews,rssnews,googletrends,actionnetwork,draftkings \
    --seasons=2025,2026 --recent-days=7 "$@"

echo ""
echo "── Event-based providers (tickets/highlights, 2023–2026) ─"
npx tsx src/cli.ts --providers=ticketmaster,youtube,openmeteo \
    --seasons=2023,2024,2025,2026 "$@"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Wave 2 enhanced providers — 2020–2026"
echo "═══════════════════════════════════════════════════"

echo ""
echo "── football-data.co.uk (soccer odds, 2020–2026) ──"
npx tsx src/cli.ts --providers=footballdotco \
    --sports=epl,bundesliga,laliga,seriea,ligue1 \
    --seasons=2020,2021,2022,2023,2024,2025 "$@"

echo ""
echo "── RotoWire injury/news RSS ──────────────────────"
npx tsx src/cli.ts --providers=rotowire \
    --sports=nfl,nba,mlb,nhl,ncaab,ncaaf,wnba \
    --seasons=2024,2025,2026 "$@"

echo ""
echo "── Retrosheet MLB game logs (2020–2024) ──────────"
npx tsx src/cli.ts --providers=retrosheet \
    --sports=mlb \
    --seasons=2020,2021,2022,2023,2024 "$@"

echo ""
echo "── OpenLigaDB Bundesliga (2020–2025) ─────────────"
npx tsx src/cli.ts --providers=openligadb \
    --sports=bundesliga \
    --seasons=2020,2021,2022,2023,2024,2025 "$@"

# ── Wave 3 providers ──────────────────────────────────
echo ""
echo "── TheSportsDB team metadata ─────────────────────"
npx tsx src/cli.ts --providers=thesportsdb \
    --seasons=2025 "$@"

echo ""
echo "── NFL PFR advanced stats (2020–2025) ────────────"
npx tsx src/cli.ts --providers=nflfastr \
    --sports=nfl \
    --seasons=2020,2021,2022,2023,2024,2025 "$@"

echo ""
echo "── LoL Esports schedule + standings (2020–2025) ──"
npx tsx src/cli.ts --providers=lolesports \
    --sports=lol \
    --seasons=2020,2021,2022,2023,2024,2025 "$@"

echo ""
echo "── Steam CS2/Dota2 player counts ─────────────────"
npx tsx src/cli.ts --providers=steam \
    --sports=csgo,dota2 \
    --seasons=2025 "$@"

echo ""
echo "── ESPN team/player metadata ─────────────────────"
npx tsx src/cli.ts --providers=espnmeta \
    --seasons=2025 "$@"

echo ""
echo "── Sleeper player registry + NFL weekly stats ────"
npx tsx src/cli.ts --providers=sleeper \
    --sports=nfl,nba,mlb,nhl \
    --seasons=2025 "$@"

echo ""
echo "── Sleeper NFL historical weekly stats ───────────"
npx tsx src/cli.ts --providers=sleeper \
    --sports=nfl \
    --seasons=2020,2021,2022,2023,2024 "$@"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Backfill complete!"
echo "═══════════════════════════════════════════════════"
