#!/usr/bin/env python3
"""
migrate_ncaaf_curated.py

Migrates NCAAF normalized_curated parquet files from the OLD 3-level hierarchy
to the NEW BDL-style flat hierarchy. Copies files (does not move) for safety.

Usage:
    python migrate_ncaaf_curated.py --dry-run
    python migrate_ncaaf_curated.py --base-dir /path/to/normalized_curated
    python migrate_ncaaf_curated.py --backup
"""

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
# Mapping definitions: (old_relative_path, new_relative_path)
#
# For "merge" targets the new_relative_path is the same destination —
# all source parquets land in the same folder.  Actual schema merging
# (dedup / column alignment) is handled by a separate reconciliation step.
# ---------------------------------------------------------------------------

@dataclass
class PathMapping:
    old: str
    new: str
    note: str = ""


MAPPINGS: list[PathMapping] = [
    # ── Team Identity ──────────────────────────────────────────────────
    PathMapping("ncaaf/team/identity/base",          "ncaaf/teams",                    "base team records"),
    PathMapping("ncaaf/team/identity/fbs",           "ncaaf/teams",                    "merge fbs flag"),
    PathMapping("ncaaf/team/identity/roster",        "ncaaf/players",                  "roster → players"),

    # ── Team Context ───────────────────────────────────────────────────
    PathMapping("ncaaf/team/context/staff",          "ncaaf/coaches",                  "staff → coaches"),

    # ── Team Ratings ───────────────────────────────────────────────────
    PathMapping("ncaaf/team/ratings/elo",            "ncaaf/ratings/elo",              ""),
    PathMapping("ncaaf/team/ratings/fpi",            "ncaaf/ratings/fpi",              ""),
    PathMapping("ncaaf/team/ratings/sp",             "ncaaf/ratings/sp",               ""),
    PathMapping("ncaaf/team/ratings/sp_conference",  "ncaaf/ratings/sp_conference",    ""),
    PathMapping("ncaaf/team/ratings/srs",            "ncaaf/ratings/srs",              ""),
    PathMapping("ncaaf/team/ratings/talent",         "ncaaf/ratings/talent",           ""),

    # ── Team Records / Standings ───────────────────────────────────────
    PathMapping("ncaaf/team/record/wl",              "ncaaf/standings",                "win/loss records"),
    PathMapping("ncaaf/team/record/ats",             "ncaaf/standings",                "merge ATS data"),
    PathMapping("ncaaf/team/record/standings",       "ncaaf/standings",                "merge standings"),

    # ── Team Recruiting ────────────────────────────────────────────────
    PathMapping("ncaaf/team/recruiting/class",       "ncaaf/recruiting_classes",       ""),
    PathMapping("ncaaf/team/recruiting/groups",      "ncaaf/recruiting_groups",        ""),

    # ── Game Schedule ──────────────────────────────────────────────────
    PathMapping("ncaaf/game/schedule/base",          "ncaaf/games",                    ""),
    PathMapping("ncaaf/game/schedule/media",         "ncaaf/media",                    ""),

    # ── Game Box Scores ────────────────────────────────────────────────
    PathMapping("ncaaf/game/box/teams",              "ncaaf/team_stats",               ""),
    PathMapping("ncaaf/game/box/players",            "ncaaf/player_stats",             ""),
    PathMapping("ncaaf/game/box/advanced",           "ncaaf/team_stats",               "merge advanced into team_stats"),

    # ── Game Advanced ──────────────────────────────────────────────────
    PathMapping("ncaaf/game/advanced/epa",           "ncaaf/advanced/epa",             ""),
    PathMapping("ncaaf/game/advanced/havoc",         "ncaaf/advanced/havoc",           ""),
    PathMapping("ncaaf/game/advanced/ppa",           "ncaaf/advanced/ppa",             ""),
    PathMapping("ncaaf/game/advanced/win_prob",      "ncaaf/advanced/win_probability", "rename win_prob → win_probability"),

    # ── Game Play-by-Play ──────────────────────────────────────────────
    PathMapping("ncaaf/game/play_by_play/events",    "ncaaf/plays",                    "events → plays"),
    PathMapping("ncaaf/game/play_by_play/drives",    "ncaaf/drives",                   ""),
    PathMapping("ncaaf/game/play_by_play/plays",     "ncaaf/plays",                    "merge plays"),

    # ── Market / Odds ──────────────────────────────────────────────────
    PathMapping("ncaaf/market/odds/lines",           "ncaaf/odds",                     ""),
    PathMapping("ncaaf/market/odds/live",            "ncaaf/odds",                     "merge live odds"),
    PathMapping("ncaaf/market/odds_history/history", "ncaaf/odds",                     "merge odds history"),
    PathMapping("ncaaf/market/props/live",           "ncaaf/odds",                     "player props into odds"),
    PathMapping("ncaaf/market/signals/derived",      "ncaaf/market_signals",           "analytics signals"),

    # ── Player Identity ────────────────────────────────────────────────
    PathMapping("ncaaf/player/identity/base",        "ncaaf/players",                  "merge into players"),
    PathMapping("ncaaf/player/identity/recruit",     "ncaaf/recruiting_players",       ""),
    PathMapping("ncaaf/player/identity/draft_pick",  "ncaaf/draft",                    ""),
    PathMapping("ncaaf/player/identity/injury",      "ncaaf/injuries",                 ""),
    PathMapping("ncaaf/player/identity/news",        "ncaaf/news",                     "extra category"),

    # ── Player Game Stats ──────────────────────────────────────────────
    PathMapping("ncaaf/player/game_stats/ppa",       "ncaaf/player_stats",             "merge PPA into player_stats"),

    # ── Player Season Stats ────────────────────────────────────────────
    PathMapping("ncaaf/player/season_stats/base",    "ncaaf/player_season_stats",      ""),
    PathMapping("ncaaf/player/season_stats/ppa",     "ncaaf/player_season_stats",      "merge PPA"),
    PathMapping("ncaaf/player/season_stats/rollup",  "ncaaf/player_season_stats",      "merge rollup"),
    PathMapping("ncaaf/player/usage/base",           "ncaaf/player_season_stats",      "merge usage"),

    # ── Player Portal / Returning ──────────────────────────────────────
    PathMapping("ncaaf/player/portal/base",          "ncaaf/portal",                   ""),
    PathMapping("ncaaf/player/returning/base",       "ncaaf/returning_production",     ""),

    # ── Season-Level Aggregates ────────────────────────────────────────
    PathMapping("ncaaf/season/team_stats/base",      "ncaaf/team_season_stats",        ""),
    PathMapping("ncaaf/season/team_stats/advanced",  "ncaaf/team_season_stats",        "merge advanced"),
    PathMapping("ncaaf/season/team_stats/rollup",    "ncaaf/team_season_stats",        "merge rollup"),
    PathMapping("ncaaf/season/ppa/base",             "ncaaf/advanced/ppa",             "season-level PPA"),
    PathMapping("ncaaf/season/ppa/predicted",        "ncaaf/advanced/ppa",             "merge predicted PPA"),

    # ── Season Rankings ────────────────────────────────────────────────
    PathMapping("ncaaf/season/rankings/polls",       "ncaaf/rankings",                 ""),

    # ── Reference Data ─────────────────────────────────────────────────
    PathMapping("ncaaf/reference/conferences/base",  "ncaaf/conferences",              ""),
    PathMapping("ncaaf/reference/venues/base",       "ncaaf/venues",                   ""),
    PathMapping("ncaaf/reference/calendar/windows",  "ncaaf/reference/calendar",       "keep as reference"),
    PathMapping("ncaaf/reference/draft/positions",   "ncaaf/draft",                    "merge draft positions"),
    PathMapping("ncaaf/reference/draft/teams",       "ncaaf/draft",                    "merge draft teams"),
    PathMapping("ncaaf/reference/metadata/info",     "ncaaf/reference/metadata",       "keep as reference"),
    PathMapping("ncaaf/reference/metrics/categories","ncaaf/reference/metrics",        "keep as reference"),
    PathMapping("ncaaf/reference/metrics/fg_ep",     "ncaaf/reference/metrics",        "keep as reference"),
    PathMapping("ncaaf/reference/play_types/types",  "ncaaf/reference/play_types",     "keep as reference"),
    PathMapping("ncaaf/reference/play_types/stat_types","ncaaf/reference/play_types",  "keep as reference"),
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

    This preserves season=YYYY/week=WW/filename.parquet structure.
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
        # Prefix with the old leaf folder name to avoid collisions in merge targets
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
    header = f"{'Old Path':<55} {'New Path':<40} {'Files':>6}"
    sep = "-" * len(header)

    print("\n" + sep)
    print("MIGRATION SUMMARY")
    print(sep)
    print(header)
    print(sep)

    for old, new, count in sorted(stats.details, key=lambda x: x[0]):
        print(f"{old:<55} {new:<40} {count:>6}")

    print(sep)
    print(f"Total copied : {stats.copied}")
    print(f"Total skipped: {stats.skipped}")
    print(f"Total errors : {stats.errors}")
    print(sep + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate NCAAF normalized_curated parquet files from "
                    "the old 3-level hierarchy to the new BDL-style flat hierarchy.",
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
    log.info("Mappings       : %d", len(MAPPINGS))

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
