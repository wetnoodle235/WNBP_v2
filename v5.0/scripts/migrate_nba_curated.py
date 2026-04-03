#!/usr/bin/env python3
"""
migrate_nba_curated.py

Migrates NBA normalized_curated parquet files from the OLD multi-level
hierarchy to the NEW consolidated 16-entity layout.  Copies files (does not
move) for safety.

Each source file is placed under the new entity folder in a subfolder named
after the old leaf directory to prevent collisions.  E.g.:
    nba/espn/athletes/season=2023/part.parquet
    → nba/players/athletes/season=2023/part.parquet

Usage:
    python migrate_nba_curated.py --dry-run
    python migrate_nba_curated.py --base-dir /path/to/normalized_curated
    python migrate_nba_curated.py --backup
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_BASE_DIR = Path("/home/derek/Documents/stock/v5.0/data/normalized_curated")

# ---------------------------------------------------------------------------
# Mapping definitions: (old_relative_path, new_entity, note)
#
# old_leaf is used as a sub-directory inside the new entity folder so that
# files from different old sources don't collide.
# ---------------------------------------------------------------------------

@dataclass
class PathMapping:
    old: str
    new: str
    note: str = ""


MAPPINGS: list[PathMapping] = [
    # ── teams (ESPN teams + NBA Stats teams) ──────────────────────────
    PathMapping("nba/espn/teams",                         "nba/teams",         "ESPN team metadata"),
    PathMapping("nba/nbastats/reference/teams",           "nba/teams",         "NBA Stats team reference"),
    PathMapping("nba/espn/reference/teams",               "nba/teams",         "ESPN reference teams"),

    # ── players (ESPN athletes/rosters + NBA Stats players) ───────────
    PathMapping("nba/espn/athletes",                      "nba/players",       "ESPN athlete profiles"),
    PathMapping("nba/espn/rosters",                       "nba/players",       "ESPN rosters"),
    PathMapping("nba/nbastats/reference/all_players",     "nba/players",       "NBA Stats all players"),
    PathMapping("nba/nbastats/reference/players",         "nba/players",       "NBA Stats player profiles"),
    PathMapping("nba/espn/snapshots/news",                "nba/players",       "ESPN player news"),
    PathMapping("nba/espn/snapshots/transactions",        "nba/players",       "ESPN transactions"),

    # ── games (ESPN events + NBA Stats summaries + OddsAPI scores) ────
    PathMapping("nba/espn/events",                        "nba/games",         "ESPN game events"),
    PathMapping("nba/espn/team_schedule",                 "nba/games",         "ESPN team schedules"),
    PathMapping("nba/espn/reference/games",               "nba/games",         "ESPN reference games"),
    PathMapping("nba/nbastats/games/summary",             "nba/games",         "NBA Stats game summaries"),
    PathMapping("nba/oddsapi/scores",                     "nba/games",         "OddsAPI scores"),

    # ── player_stats (NBA Stats player-stats + game-logs + aggregates)
    PathMapping("nba/nbastats/player-stats/base",         "nba/player_stats",  "NBA Stats base player stats"),
    PathMapping("nba/nbastats/player-stats/advanced",     "nba/player_stats",  "NBA Stats advanced player stats"),
    PathMapping("nba/nbastats/player-stats/defense",      "nba/player_stats",  "NBA Stats defense player stats"),
    PathMapping("nba/nbastats/player-stats/misc",         "nba/player_stats",  "NBA Stats misc player stats"),
    PathMapping("nba/nbastats/player-stats/scoring",      "nba/player_stats",  "NBA Stats scoring player stats"),
    PathMapping("nba/nbastats/player-stats/usage",        "nba/player_stats",  "NBA Stats usage player stats"),
    PathMapping("nba/nbastats/player-game-logs",          "nba/player_stats",  "NBA Stats player game logs"),
    PathMapping("nba/nbastats/season_aggregates",         "nba/player_stats",  "NBA Stats season aggregates"),

    # ── team_stats (NBA Stats team-stats + game-logs + ESPN snapshots)
    PathMapping("nba/nbastats/team-stats/base",           "nba/team_stats",    "NBA Stats base team stats"),
    PathMapping("nba/nbastats/team-stats/advanced",       "nba/team_stats",    "NBA Stats advanced team stats"),
    PathMapping("nba/nbastats/team-stats/defense",        "nba/team_stats",    "NBA Stats defense team stats"),
    PathMapping("nba/nbastats/team-stats/misc",           "nba/team_stats",    "NBA Stats misc team stats"),
    PathMapping("nba/nbastats/team-stats/scoring",        "nba/team_stats",    "NBA Stats scoring team stats"),
    PathMapping("nba/nbastats/team-stats/usage",          "nba/team_stats",    "NBA Stats usage team stats"),
    PathMapping("nba/nbastats/team-game-logs",            "nba/team_stats",    "NBA Stats team game logs"),
    PathMapping("nba/espn/snapshots/team_stats",          "nba/team_stats",    "ESPN team stats snapshots"),

    # ── standings ─────────────────────────────────────────────────────
    PathMapping("nba/espn/reference/standings",           "nba/standings",     "ESPN standings"),

    # ── odds (Odds baseline + OddsAPI events) ────────────────────────
    PathMapping("nba/odds/espn_baseline",                 "nba/odds",          "Odds ESPN baseline"),
    PathMapping("nba/oddsapi/events",                     "nba/odds",          "OddsAPI odds events"),

    # ── player_props (Odds + OddsAPI props) ──────────────────────────
    PathMapping("nba/odds/player_props",                  "nba/player_props",  "Odds player props"),
    PathMapping("nba/oddsapi/props",                      "nba/player_props",  "OddsAPI player props"),

    # ── advanced (NBA Stats shot-charts + advanced) ──────────────────
    PathMapping("nba/nbastats/shot-charts",               "nba/advanced",      "NBA Stats shot charts"),

    # ── plays (NBA Stats play-by-play) ───────────────────────────────
    PathMapping("nba/nbastats/games/playbyplay",          "nba/plays",         "NBA Stats play-by-play"),

    # ── box_scores (NBA Stats boxscores) ─────────────────────────────
    PathMapping("nba/nbastats/games/boxscore",            "nba/box_scores",    "NBA Stats boxscores"),

    # ── lineups (ESPN depth charts) ──────────────────────────────────
    PathMapping("nba/espn/depth_charts",                  "nba/lineups",       "ESPN depth charts"),

    # ── contracts (ESPN athlete contracts) ────────────────────────────
    PathMapping("nba/espn/athletes/contracts",            "nba/contracts",     "ESPN athlete contracts"),

    # ── injuries (ESPN injuries + snapshots) ─────────────────────────
    PathMapping("nba/espn/reference/injuries",            "nba/injuries",      "ESPN injuries reference"),
    PathMapping("nba/espn/snapshots/injuries",            "nba/injuries",      "ESPN injury snapshots"),

    # ── leaders (NBA Stats league-leaders) ────────────────────────────
    PathMapping("nba/nbastats/league-leaders",            "nba/leaders",       "NBA Stats league leaders"),

    # ── venues (ESPN arenas + NBA Stats arena data) ──────────────────
    PathMapping("nba/espn/reference/arenas",              "nba/venues",        "ESPN arenas"),
    PathMapping("nba/nbastats/reference/arenas",          "nba/venues",        "NBA Stats arenas"),
]


@dataclass
class MigrationStats:
    copied: int = 0
    skipped: int = 0
    errors: int = 0
    details: list = field(default_factory=list)  # (old, new, count)


def find_parquet_files(directory: Path) -> list[Path]:
    """Recursively find all .parquet files under *directory*."""
    if not directory.exists():
        return []
    return sorted(directory.rglob("*.parquet"))


def relative_partition_path(file_path: Path, source_root: Path) -> Path:
    """Return the portion of *file_path* relative to *source_root*.

    Preserves season=YYYY/filename.parquet structure.
    """
    return file_path.relative_to(source_root)


def migrate_mapping(
    mapping: PathMapping,
    base_dir: Path,
    dry_run: bool,
    backup: bool,
) -> tuple[int, int]:
    """Process a single mapping.  Returns (copied, skipped)."""
    src_dir = base_dir / mapping.old
    dst_dir = base_dir / mapping.new

    files = find_parquet_files(src_dir)
    if not files:
        if src_dir.exists():
            log.debug("No parquet files in %s", src_dir)
        else:
            log.warning("Source directory missing – skipping: %s", src_dir)
        return 0, 0

    copied = 0
    skipped = 0

    for src_file in files:
        rel = relative_partition_path(src_file, src_dir)
        # Prefix with the old leaf folder name to prevent collisions
        old_leaf = Path(mapping.old).name
        dst_file = dst_dir / old_leaf / rel

        if dst_file.exists():
            log.debug("Already exists, skipping: %s", dst_file)
            skipped += 1
            continue

        if dry_run:
            log.info("[DRY-RUN] %s → %s", src_file.relative_to(base_dir), dst_file.relative_to(base_dir))
            copied += 1
            continue

        dst_file.parent.mkdir(parents=True, exist_ok=True)

        if backup:
            backup_path = src_file.with_suffix(".parquet.bak")
            if not backup_path.exists():
                shutil.copy2(src_file, backup_path)
                log.debug("Backed up: %s", backup_path)

        shutil.copy2(src_file, dst_file)
        log.info("Copied: %s → %s", src_file.relative_to(base_dir), dst_file.relative_to(base_dir))
        copied += 1

    return copied, skipped


def print_summary(stats: MigrationStats) -> None:
    """Print a formatted summary table."""
    header = f"{'Old Path':<55} {'New Path':<35} {'Files':>6}"
    sep = "─" * len(header)

    print(f"\n{sep}")
    print("MIGRATION SUMMARY")
    print(sep)
    print(header)
    print(sep)

    for old, new, count in sorted(stats.details, key=lambda x: x[0]):
        print(f"{old:<55} {new:<35} {count:>6}")

    print(sep)
    print(f"Total copied : {stats.copied}")
    print(f"Total skipped: {stats.skipped}")
    print(f"Total errors : {stats.errors}")
    print(f"{sep}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate NBA parquet files from old multi-level hierarchy "
                    "to the consolidated 16-entity layout.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without actually copying.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=DEFAULT_BASE_DIR,
        help=f"Root directory of normalized_curated data (default: {DEFAULT_BASE_DIR}).",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create .bak copies of source files before copying.",
    )
    args = parser.parse_args()

    base_dir: Path = args.base_dir.resolve()
    if not base_dir.exists():
        log.error("Base directory does not exist: %s", base_dir)
        sys.exit(1)

    log.info("Base directory : %s", base_dir)
    log.info("Dry-run        : %s", args.dry_run)
    log.info("Backup         : %s", args.backup)
    log.info("Mappings       : %d entries → 16 consolidated entities", len(MAPPINGS))

    stats = MigrationStats()

    for mapping in MAPPINGS:
        try:
            copied, skipped = migrate_mapping(
                mapping, base_dir, args.dry_run, args.backup,
            )
            stats.copied += copied
            stats.skipped += skipped
            if copied or skipped:
                stats.details.append((mapping.old, mapping.new, copied + skipped))
        except Exception:
            log.exception("Error processing mapping %s → %s", mapping.old, mapping.new)
            stats.errors += 1

    print_summary(stats)

    if stats.errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
