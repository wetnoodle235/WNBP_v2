#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────────────────
# Build NCAAF Normalized-Curated Folder Structure
# ──────────────────────────────────────────────────────────────────────
#
# Creates the complete directory tree for the redesigned NCAAF
# normalized_curated layer under:
#   data/normalized_curated/ncaaf/
#
# Each entity gets its own top-level folder (BDL-style flat layout).
# Leaf directories receive an empty .gitkeep so Git tracks them.
#
# Usage:
#   python scripts/build_ncaaf_curated_structure.py           # create dirs
#   python scripts/build_ncaaf_curated_structure.py --dry-run  # preview only
# ──────────────────────────────────────────────────────────────────────

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ── All entity paths relative to ncaaf/ ──────────────────────────────
# Mirrors NCAAF_ENTITY_PATHS in backend/normalization/ncaaf_schemas.py.

ENTITY_PATHS: list[str] = [
    # BDL-mirrored entities
    "conferences",
    "teams",
    "players",
    "games",
    "plays",
    "player_stats",
    "team_stats",
    "player_season_stats",
    "team_season_stats",
    "standings",
    "rankings",
    "odds",
    # WNBP-exclusive entities
    "coaches",
    "weather",
    "injuries",
    "recruiting_classes",
    "recruiting_players",
    "recruiting_groups",
    # Ratings (subfolder split)
    "ratings/elo",
    "ratings/sp",
    "ratings/fpi",
    "ratings/srs",
    "ratings/talent",
    "ratings/sp_conference",
    # Advanced (subfolder split)
    "advanced/epa",
    "advanced/ppa",
    "advanced/havoc",
    "advanced/win_probability",
    # Remaining entities
    "drives",
    "draft",
    "portal",
    "returning_production",
    "venues",
    "media",
]


def resolve_base(script_path: Path) -> Path:
    """Return the ncaaf root: <repo>/data/normalized_curated/ncaaf/."""
    repo_root = script_path.resolve().parent.parent  # scripts/ → v5.0/
    return repo_root / "data" / "normalized_curated" / "ncaaf"


def build_structure(base: Path, *, dry_run: bool = False) -> list[Path]:
    """Create entity directories and .gitkeep files.

    Returns the list of directories that were (or would be) created.
    """
    created: list[Path] = []

    for rel in sorted(ENTITY_PATHS):
        entity_dir = base / rel

        if dry_run:
            status = "exists" if entity_dir.exists() else "CREATE"
            print(f"  [{status}] {entity_dir}")
            created.append(entity_dir)
            continue

        entity_dir.mkdir(parents=True, exist_ok=True)
        gitkeep = entity_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()
        created.append(entity_dir)

    return created


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the NCAAF normalized_curated folder tree.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the directory structure without creating anything.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        help="Override the base ncaaf/ directory (default: auto-detect from script location).",
    )
    args = parser.parse_args()

    base = args.base_dir or resolve_base(Path(__file__))

    mode = "DRY RUN" if args.dry_run else "CREATE"
    print(f"[{mode}] NCAAF normalized_curated structure")
    print(f"  Base path: {base}\n")

    dirs = build_structure(base, dry_run=args.dry_run)

    print(f"\n{'Would create' if args.dry_run else 'Created'} {len(dirs)} entity directories.")

    if not args.dry_run:
        print("Done ✓")


if __name__ == "__main__":
    main()
