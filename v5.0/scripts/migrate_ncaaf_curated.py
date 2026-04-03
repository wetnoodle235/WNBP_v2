#!/usr/bin/env python3
"""
migrate_ncaaf_curated.py

Migrates NCAAF normalized_curated parquet files from the OLD multi-level
hierarchy to the NEW consolidated 14-entity layout.  Copies files (does not
move) for safety.

Each source file is placed under the new entity folder in a subfolder named
after the old leaf directory to prevent collisions.  E.g.:
    ncaaf/team/ratings/elo/season=2023/part.parquet
    → ncaaf/ratings/elo/season=2023/part.parquet

Usage:
    python migrate_ncaaf_curated.py --dry-run
    python migrate_ncaaf_curated.py --base-dir /path/to/normalized_curated
    python migrate_ncaaf_curated.py --backup
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
# Mapping definitions: (old_relative_path, new_entity, old_leaf_subfolder)
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
    # ── teams (team + coaches + roster identity) ──────────────────────
    PathMapping("ncaaf/team/identity/base",          "ncaaf/teams",          "base team records"),
    PathMapping("ncaaf/team/identity/fbs",           "ncaaf/teams",          "FBS flag merge"),
    PathMapping("ncaaf/team/context/staff",          "ncaaf/teams",          "coaches → teams"),

    # ── players (identity + portal + returning + draft + roster) ──────
    PathMapping("ncaaf/team/identity/roster",        "ncaaf/players",        "roster → players"),
    PathMapping("ncaaf/player/identity/base",        "ncaaf/players",        "player identity"),
    PathMapping("ncaaf/player/identity/draft_pick",  "ncaaf/players",        "draft picks"),
    PathMapping("ncaaf/player/identity/draft",       "ncaaf/players",        "draft records"),
    PathMapping("ncaaf/player/identity/news",        "ncaaf/players",        "player news"),
    PathMapping("ncaaf/player/portal/base",          "ncaaf/players",        "transfer portal"),
    PathMapping("ncaaf/player/returning/base",       "ncaaf/players",        "returning production"),
    PathMapping("ncaaf/reference/draft/positions",   "ncaaf/players",        "draft position ref"),
    PathMapping("ncaaf/reference/draft/teams",       "ncaaf/players",        "draft team ref"),

    # ── games (schedule + media + weather) ────────────────────────────
    PathMapping("ncaaf/game/schedule/base",          "ncaaf/games",          "game schedule"),
    PathMapping("ncaaf/game/schedule/media",         "ncaaf/games",          "media/broadcast → games"),

    # ── plays (play-by-play + drives) ─────────────────────────────────
    PathMapping("ncaaf/game/play_by_play/events",    "ncaaf/plays",          "events → plays"),
    PathMapping("ncaaf/game/play_by_play/drives",    "ncaaf/plays",          "drives → plays"),
    PathMapping("ncaaf/game/play_by_play/plays",     "ncaaf/plays",          "play details"),
    PathMapping("ncaaf/game/play_by_play/season_rollup", "ncaaf/plays",      "play season rollup"),
    PathMapping("ncaaf/reference/play_types/types",      "ncaaf/plays",      "play type ref"),
    PathMapping("ncaaf/reference/play_types/stat_types", "ncaaf/plays",      "stat type ref"),

    # ── player_stats (game + season + ppa + usage — scope discriminator) ──
    PathMapping("ncaaf/game/box/players",            "ncaaf/player_stats",   "game box player stats"),
    PathMapping("ncaaf/player/game_stats/ppa",       "ncaaf/player_stats",   "game PPA stats"),
    PathMapping("ncaaf/player/season_stats/base",    "ncaaf/player_stats",   "season stats base"),
    PathMapping("ncaaf/player/season_stats/ppa",     "ncaaf/player_stats",   "season PPA stats"),
    PathMapping("ncaaf/player/season_stats/rollup",  "ncaaf/player_stats",   "season rollup"),
    PathMapping("ncaaf/player/usage/base",           "ncaaf/player_stats",   "player usage"),
    PathMapping("ncaaf/player/identity/injury",      "ncaaf/player_stats",   "injury reports"),

    # ── team_stats (game + season + advanced box — scope discriminator) ──
    PathMapping("ncaaf/game/box/teams",              "ncaaf/team_stats",     "game box team stats"),
    PathMapping("ncaaf/game/box/advanced",           "ncaaf/team_stats",     "game advanced box"),
    PathMapping("ncaaf/season/team_stats/base",      "ncaaf/team_stats",     "season team stats"),
    PathMapping("ncaaf/season/team_stats/advanced",  "ncaaf/team_stats",     "season advanced stats"),
    PathMapping("ncaaf/season/team_stats/rollup",    "ncaaf/team_stats",     "season rollup"),

    # ── standings (standings + wl + ats records) ──────────────────────
    PathMapping("ncaaf/team/record/wl",              "ncaaf/standings",      "win/loss records"),
    PathMapping("ncaaf/team/record/ats",             "ncaaf/standings",      "ATS records"),
    PathMapping("ncaaf/team/record/standings",       "ncaaf/standings",      "conference standings"),

    # ── rankings ──────────────────────────────────────────────────────
    PathMapping("ncaaf/season/rankings/polls",       "ncaaf/rankings",       "poll rankings"),

    # ── odds (lines + live + history + props — line_type discriminator)
    PathMapping("ncaaf/market/odds/lines",           "ncaaf/odds",           "pre-game lines"),
    PathMapping("ncaaf/market/odds/live",            "ncaaf/odds",           "live odds"),
    PathMapping("ncaaf/market/odds_history/history", "ncaaf/odds",           "odds history"),
    PathMapping("ncaaf/market/props/live",           "ncaaf/odds",           "player props"),
    PathMapping("ncaaf/market/signals/derived",      "ncaaf/odds",           "derived market signals"),

    # ── ratings (elo + sp + fpi + srs + talent + sp_conference — rating_type) ──
    PathMapping("ncaaf/team/ratings/elo",            "ncaaf/ratings",        "Elo ratings"),
    PathMapping("ncaaf/team/ratings/fpi",            "ncaaf/ratings",        "FPI ratings"),
    PathMapping("ncaaf/team/ratings/sp",             "ncaaf/ratings",        "SP+ ratings"),
    PathMapping("ncaaf/team/ratings/sp_conference",  "ncaaf/ratings",        "SP+ conference"),
    PathMapping("ncaaf/team/ratings/srs",            "ncaaf/ratings",        "SRS ratings"),
    PathMapping("ncaaf/team/ratings/talent",         "ncaaf/ratings",        "talent composite"),

    # ── advanced (epa + ppa + havoc + win_prob — metric_type discriminator) ──
    PathMapping("ncaaf/game/advanced/epa",           "ncaaf/advanced",       "EPA per game"),
    PathMapping("ncaaf/game/advanced/havoc",         "ncaaf/advanced",       "havoc rates"),
    PathMapping("ncaaf/game/advanced/ppa",           "ncaaf/advanced",       "PPA per game"),
    PathMapping("ncaaf/game/advanced/win_prob",      "ncaaf/advanced",       "win probability"),
    PathMapping("ncaaf/season/ppa/base",             "ncaaf/advanced",       "season PPA base"),
    PathMapping("ncaaf/season/ppa/predicted",        "ncaaf/advanced",       "season PPA predicted"),
    PathMapping("ncaaf/reference/metrics/categories","ncaaf/advanced",       "metric category ref"),
    PathMapping("ncaaf/reference/metrics/fg_ep",     "ncaaf/advanced",       "FG/EP ref tables"),

    # ── recruiting (classes + groups — scope discriminator) ───────────
    PathMapping("ncaaf/team/recruiting/class",       "ncaaf/recruiting",     "team recruiting classes"),
    PathMapping("ncaaf/team/recruiting/groups",      "ncaaf/recruiting",     "position group recruiting"),
    PathMapping("ncaaf/player/identity/recruit",     "ncaaf/recruiting",     "individual recruit profiles"),

    # ── conferences ───────────────────────────────────────────────────
    PathMapping("ncaaf/reference/conferences/base",  "ncaaf/conferences",    "conference definitions"),
    PathMapping("ncaaf/reference/calendar/windows",  "ncaaf/conferences",    "season calendar"),
    PathMapping("ncaaf/reference/metadata/info",     "ncaaf/conferences",    "API/dataset metadata"),

    # ── venues ────────────────────────────────────────────────────────
    PathMapping("ncaaf/reference/venues/base",       "ncaaf/venues",         "venue/stadium info"),
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

    Preserves season=YYYY/week=WW/filename.parquet structure.
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
        description="Migrate NCAAF parquet files from old multi-level hierarchy "
                    "to the consolidated 14-entity layout.",
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
    log.info("Mappings       : %d entries → 14 consolidated entities", len(MAPPINGS))

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
