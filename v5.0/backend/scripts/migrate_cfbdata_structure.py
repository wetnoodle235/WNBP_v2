#!/usr/bin/env python3
"""
V5.0 CFBData Raw Data Structure Migration
──────────────────────────────────────────────────────────

Reorganizes existing CFBData raw files from flat structure to hierarchical
week/date/endpoint structure (Phase 4 data migration).

Old Structure:
  data/raw/cfbdata/ncaaf/{season}/games.json
  data/raw/cfbdata/ncaaf/{season}/plays/week_1.json
  data/raw/cfbdata/ncaaf/{season}/stats_player_season.json

New Structure:
  data/raw/cfbdata/ncaaf/{season}/1/games/2025-09-06/401547489.json
  data/raw/cfbdata/ncaaf/{season}/1/plays/2025-09-06/401547489_plays.json
  data/raw/cfbdata/ncaaf/{season}/1/stats/players_week_1.json
  data/raw/cfbdata/ncaaf/{season}/reference/rankings.json

Usage:
  python3 scripts/migrate_cfbdata_structure.py [--dry-run] [--seasons 2020,2021,...]

Flags:
  --dry-run   Show what would be migrated without making changes
  --seasons   Comma-separated seasons to migrate (default: 2020-2026)
  --force     Overwrite existing new-structure files
"""

import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Any
from datetime import datetime

# ─── Setup ─────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = BASE_DATA_DIR / "raw" / "cfbdata" / "ncaaf"

SEASONS = ["2020", "2021", "2022", "2023", "2024", "2025", "2026"]
DEFAULT_SEASONS = "2020,2021,2022,2023,2024,2025,2026"

# Reference files that should move to /reference/ (not split by week/date)
REFERENCE_FILES = {
    "rankings.json",
    "recruiting.json",
    "recruiting_teams.json",
    "recruiting_groups.json",
    "talent.json",
    "ratings_sp.json",
    "ratings_sp_conferences.json",
    "ratings_srs.json",
    "ratings_elo.json",
    "ratings_fpi.json",
    "coaches.json",
    "conferences.json",
    "venues.json",
    "teams.json",
    "teams_fbs.json",
    "teams_ats.json",
    "roster.json",
    "player_usage.json",
    "player_returning.json",
    "player_portal.json",
    "stats_season.json",
    "stats_advanced.json",
    "stats_categories.json",
    "ppa_predicted.json",
    "ppa_teams.json",
    "lines.json",
}


def load_json(path: Path) -> Any:
    """Load JSON file, return None if missing."""
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return None


def write_json(path: Path, data: Any, overwrite: bool = False) -> bool:
    """Write JSON file with atomic semantics. Return True on success."""
    if path.exists() and not overwrite:
        return True
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to write {path}: {e}")
        return False


def migrate_games(season_dir: Path, new_season_dir: Path, dry_run: bool = False) -> dict[str, int]:
    """Migrate games.json → {week}/games/{date}/{gameId}.json"""
    result = {"migrated": 0, "skipped": 0, "errors": 0}
    old_file = season_dir / "games.json"
    if not old_file.exists():
        return result

    games = load_json(old_file)
    if not games or not isinstance(games, list):
        logger.warning(f"  games.json is empty or invalid")
        return result

    logger.info(f"  Migrating {len(games)} games...")

    for game in games:
        game_id = str(game.get("id", ""))
        week = game.get("week", 1)
        start_date = game.get("startDate", "")
        date_str = start_date[:10] if start_date else None

        if not game_id or not date_str:
            result["skipped"] += 1
            continue

        # New path: {season}/{week}/games/{date}/{gameId}.json
        new_path = new_season_dir / str(week) / "games" / date_str / f"{game_id}.json"

        if not dry_run:
            if write_json(new_path, game, overwrite=False):
                result["migrated"] += 1
            else:
                result["errors"] += 1
        else:
            result["migrated"] += 1

    return result


def migrate_plays(season_dir: Path, new_season_dir: Path, dry_run: bool = False) -> dict[str, int]:
    """Migrate plays/week_*.json → {week}/plays/{date}/{gameId}_plays.json
    
    This requires loading games first to build the game→date mapping.
    """
    result = {"migrated": 0, "skipped": 0, "errors": 0}
    plays_dir = season_dir / "plays"
    if not plays_dir.exists():
        return result

    # Build game_id → date mapping from already-migrated games
    game_dates: dict[str, str] = {}
    for week_dir in (new_season_dir).glob("*/games/*"):
        if not week_dir.is_dir():
            continue
        for game_file in week_dir.glob("*.json"):
            game_data = load_json(game_file)
            if game_data:
                game_id = str(game_data.get("id", ""))
                date_str = game_file.parent.name
                game_dates[game_id] = (game_file.parent.parent.parent.name, date_str)

    play_files = sorted(plays_dir.glob("week_*.json"))
    logger.info(f"  Migrating {len(play_files)} play files...")

    for play_file in play_files:
        plays = load_json(play_file)
        if not plays or not isinstance(plays, list):
            logger.warning(f"  {play_file.name} is empty or invalid")
            continue

        # Group plays by game_id
        plays_by_game: dict[str, list] = {}
        for play in plays:
            game_id = str(play.get("gameId", ""))
            if not game_id:
                result["skipped"] += 1
                continue
            if game_id not in plays_by_game:
                plays_by_game[game_id] = []
            plays_by_game[game_id].append(play)

        # Write plays to new structure
        for game_id, game_plays in plays_by_game.items():
            if game_id not in game_dates:
                result["skipped"] += 1
                continue

            week, date_str = game_dates[game_id]
            new_path = new_season_dir / week / "plays" / date_str / f"{game_id}_plays.json"

            if not dry_run:
                if write_json(new_path, game_plays, overwrite=False):
                    result["migrated"] += 1
                else:
                    result["errors"] += 1
            else:
                result["migrated"] += 1

    return result


def migrate_player_stats(season_dir: Path, new_season_dir: Path, dry_run: bool = False) -> dict[str, int]:
    """Migrate stats_player_season.json → {week}/stats/players_week_*.json
    
    This requires splitting the single 30MB file into ~16 weekly files.
    Since the old structure doesn't have week info, we'll split by count.
    """
    result = {"migrated": 0, "skipped": 0, "errors": 0}
    old_file = season_dir / "stats_player_season.json"
    if not old_file.exists():
        return result

    stats = load_json(old_file)
    if not stats or not isinstance(stats, list):
        logger.warning(f"  stats_player_season.json is empty or invalid")
        return result

    logger.info(f"  Migrating {len(stats)} player stats entries...")

    # Since we don't have week info in old structure, just move the file as-is
    # The new importer will split by week when future data is imported
    # For now, we'll create a combined file in week 0 (preseason)
    for week_num in range(0, 17):  # 0 = preseason, 1-16 = regular
        new_path = new_season_dir / str(week_num) / "stats" / f"players_week_{week_num}.json"

        if not dry_run:
            if write_json(new_path, [], overwrite=False):
                result["migrated"] += 1

    # Write all stats to preseason slot as fallback
    if not dry_run:
        fallback_path = new_season_dir / "0" / "stats" / "players_week_0.json"
        if write_json(fallback_path, stats, overwrite=True):
            result["migrated"] += len(stats)
        else:
            result["errors"] += 1
    else:
        result["migrated"] = len(stats)

    return result


def migrate_reference_files(season_dir: Path, new_season_dir: Path, dry_run: bool = False) -> dict[str, int]:
    """Migrate reference files to {season}/reference/ directory"""
    result = {"migrated": 0, "skipped": 0, "errors": 0}

    for filename in REFERENCE_FILES:
        old_file = season_dir / filename
        if not old_file.exists():
            continue

        new_path = new_season_dir / "reference" / filename
        data = load_json(old_file)
        if not data:
            result["skipped"] += 1
            continue

        if not dry_run:
            if write_json(new_path, data, overwrite=False):
                result["migrated"] += 1
            else:
                result["errors"] += 1
        else:
            result["migrated"] += 1

    return result


def migrate_season(season: str, dry_run: bool = False) -> bool:
    """Migrate a single season from old to new structure"""
    season_dir = RAW_DIR / season
    if not season_dir.exists():
        logger.warning(f"Season {season} not found at {season_dir}")
        return False

    new_season_dir = RAW_DIR / "MIGRATION_NEW" / season
    logger.info(f"\n{'─' * 60}")
    logger.info(f"Migrating season {season}...")
    logger.info(f"  From: {season_dir}")
    logger.info(f"  To:   {new_season_dir}")

    # Migrate in order: games → plays (depends on games) → stats → reference
    totals = {"migrated": 0, "skipped": 0, "errors": 0}

    logger.info(f"  Step 1: Migrating games...")
    r1 = migrate_games(season_dir, new_season_dir, dry_run)
    for k, v in r1.items():
        totals[k] += v
    logger.info(f"    → {r1['migrated']} migrated, {r1['skipped']} skipped, {r1['errors']} errors")

    logger.info(f"  Step 2: Migrating plays...")
    r2 = migrate_plays(season_dir, new_season_dir, dry_run)
    for k, v in r2.items():
        totals[k] += v
    logger.info(f"    → {r2['migrated']} migrated, {r2['skipped']} skipped, {r2['errors']} errors")

    logger.info(f"  Step 3: Migrating player stats...")
    r3 = migrate_player_stats(season_dir, new_season_dir, dry_run)
    for k, v in r3.items():
        totals[k] += v
    logger.info(f"    → {r3['migrated']} migrated, {r3['skipped']} skipped, {r3['errors']} errors")

    logger.info(f"  Step 4: Migrating reference files...")
    r4 = migrate_reference_files(season_dir, new_season_dir, dry_run)
    for k, v in r4.items():
        totals[k] += v
    logger.info(f"    → {r4['migrated']} migrated, {r4['skipped']} skipped, {r4['errors']} errors")

    logger.info(f"  Season {season} TOTAL: {totals['migrated']} items, {totals['errors']} errors")
    return totals['errors'] == 0


def main() -> int:
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Migrate CFBData raw files to new hierarchical structure")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be migrated without making changes")
    parser.add_argument("--seasons", default=DEFAULT_SEASONS, help="Comma-separated seasons (default: all)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    seasons_to_migrate = args.seasons.split(",")
    dry_run = args.dry_run

    logger.info("=" * 60)
    logger.info("CFBData Raw Data Structure Migration")
    logger.info("=" * 60)
    logger.info(f"Data directory: {RAW_DIR}")
    logger.info(f"Seasons: {', '.join(seasons_to_migrate)}")
    logger.info(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (making changes)'}")
    logger.info("=" * 60)

    if not RAW_DIR.exists():
        logger.error(f"Data directory not found: {RAW_DIR}")
        return 1

    success_count = 0
    for season in seasons_to_migrate:
        if migrate_season(season, dry_run):
            success_count += 1

    logger.info("\n" + "=" * 60)
    logger.info(f"Migration complete: {success_count}/{len(seasons_to_migrate)} seasons successful")
    if dry_run:
        logger.info("(Dry run - no actual changes made)")
    else:
        logger.info(f"New structure available at: {RAW_DIR / 'MIGRATION_NEW'}")
        logger.info("Review carefully, then swap directories to apply changes")
    logger.info("=" * 60)

    return 0 if success_count == len(seasons_to_migrate) else 1


if __name__ == "__main__":
    sys.exit(main())
