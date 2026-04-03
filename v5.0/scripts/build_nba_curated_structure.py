#!/usr/bin/env python3
"""
build_nba_curated_structure.py

Creates the NBA curated directory structure under data/normalized_curated/nba/.
Consolidated 16-entity layout (v2).

Usage:
    python scripts/build_nba_curated_structure.py           # create dirs
    python scripts/build_nba_curated_structure.py --dry-run  # preview only
    python scripts/build_nba_curated_structure.py --base-dir /custom/path
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ── 16 consolidated entities ────────────────────────────────────────────
ENTITIES: list[str] = [
    "teams",
    "players",
    "games",
    "player_stats",
    "team_stats",
    "standings",
    "odds",
    "player_props",
    "advanced",
    "plays",
    "box_scores",
    "lineups",
    "contracts",
    "injuries",
    "leaders",
    "venues",
]


def resolve_base(script_path: Path) -> Path:
    """Return the nba root: <repo>/data/normalized_curated/nba/."""
    repo_root = script_path.resolve().parent.parent  # scripts/ → v5.0/
    return repo_root / "data" / "normalized_curated" / "nba"


def build_structure(base: Path, *, dry_run: bool = False) -> list[Path]:
    """Create entity directories and .gitkeep files.

    Returns the list of directories that were (or would be) created.
    """
    created: list[Path] = []

    for entity in ENTITIES:
        entity_dir = base / entity

        if dry_run:
            status = "exists" if entity_dir.exists() else "CREATE"
            print(f"  [{status}] {entity_dir}")
            created.append(entity_dir)
            continue

        entity_dir.mkdir(parents=True, exist_ok=True)
        gitkeep = entity_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()
        print(f"  ✓ {entity_dir}")
        created.append(entity_dir)

    return created


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the NBA normalized_curated folder tree (16 consolidated entities).",
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
        help="Override the base nba/ directory (default: auto-detect from script location).",
    )
    args = parser.parse_args()

    base = args.base_dir or resolve_base(Path(__file__))

    mode = "DRY RUN" if args.dry_run else "CREATE"
    print(f"\n{'='*60}")
    print(f"  NBA Curated Structure Builder  [{mode}]")
    print(f"  Base path : {base}")
    print(f"  Entities  : {len(ENTITIES)}")
    print(f"{'='*60}\n")

    dirs = build_structure(base, dry_run=args.dry_run)

    print(f"\n{'─'*60}")
    print(f"  {'Would create' if args.dry_run else 'Created'} {len(dirs)} entity directories.")
    if not args.dry_run:
        print("  Done ✓")
    else:
        print("  (no changes made – dry-run mode)")
    print(f"{'─'*60}\n")


if __name__ == "__main__":
    main()
