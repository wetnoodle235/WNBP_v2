#!/usr/bin/env python3
"""
build_ncaaf_curated_structure.py

Creates the NCAAF curated directory structure under data/normalized_curated/ncaaf/.
Consolidated 14-entity layout (v2).

Usage:
    python scripts/build_ncaaf_curated_structure.py           # create dirs
    python scripts/build_ncaaf_curated_structure.py --dry-run  # preview only
    python scripts/build_ncaaf_curated_structure.py --base-dir /custom/path
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ── 14 consolidated entities ────────────────────────────────────────────
ENTITIES: list[str] = [
    "conferences",
    "teams",
    "players",
    "games",
    "plays",
    "player_stats",
    "team_stats",
    "standings",
    "rankings",
    "odds",
    "ratings",
    "advanced",
    "recruiting",
    "venues",
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
        description="Build the NCAAF normalized_curated folder tree (14 consolidated entities).",
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
    print(f"\n{'='*60}")
    print(f"  NCAAF Curated Structure Builder  [{mode}]")
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
