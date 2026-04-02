#!/usr/bin/env python3
"""Migrate legacy NHL raw files to the structured layout used by normalization.

Target layout per season:
  data/raw/nhl/nhl/{season}/
    schedule/{regular,playoffs}.json
    games/{regular,playoffs}/{game_id}/{summary,boxscore,pbp}.json

This script is idempotent and safe to re-run.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1] / "data" / "raw" / "nhl" / "nhl"


def _season_type_from_game_id(game_id: str) -> str:
    if len(game_id) >= 6:
        code = game_id[4:6]
        if code == "02":
            return "regular"
        if code == "03":
            return "playoffs"
    return "regular"


def _ensure_dir(path: Path, dry_run: bool) -> None:
    if not dry_run:
        path.mkdir(parents=True, exist_ok=True)


def _move_if_needed(src: Path, dest: Path, dry_run: bool) -> str:
    if not src.exists():
        return "missing"
    if dest.exists():
        return "exists"
    if dry_run:
        return "dry"
    _ensure_dir(dest.parent, dry_run=False)
    shutil.move(str(src), str(dest))
    return "moved"


def _remove_if_exists(path: Path, dry_run: bool) -> str:
    if not path.exists():
        return "missing"
    if dry_run:
        return "dry"
    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        return "removed"
    except Exception:
        return "error"


def migrate_season(season_dir: Path, dry_run: bool) -> dict[str, int]:
    counts = {
        "schedule_moved": 0,
        "schedule_exists": 0,
        "game_files_moved": 0,
        "game_files_exists": 0,
        "legacy_removed": 0,
        "legacy_pruned": 0,
    }

    # Move legacy top-level schedule.json -> schedule/regular.json (if not already present)
    schedule_dir = season_dir / "schedule"
    _ensure_dir(schedule_dir, dry_run)
    legacy_schedule = season_dir / "schedule.json"
    regular_schedule = schedule_dir / "regular.json"
    status = _move_if_needed(legacy_schedule, regular_schedule, dry_run)
    if status == "moved":
        counts["schedule_moved"] += 1
    elif status == "exists":
        counts["schedule_exists"] += 1
        # Keep only canonical schedule/regular.json once both are present.
        pruned = _remove_if_exists(legacy_schedule, dry_run)
        if pruned in {"removed", "dry"}:
            counts["legacy_pruned"] += 1

    games_dir = season_dir / "games"
    if not games_dir.exists():
        return counts

    # Detect flat legacy files in games/
    for src in sorted(games_dir.glob("*.json")):
        stem = src.stem
        game_id = stem.split("_")[0]
        if not game_id.isdigit():
            continue

        suffix = "summary"
        if stem.endswith("_boxscore"):
            suffix = "boxscore"
        elif stem.endswith("_pbp"):
            suffix = "pbp"

        season_type = _season_type_from_game_id(game_id)
        dest = games_dir / season_type / game_id / f"{suffix}.json"
        status = _move_if_needed(src, dest, dry_run)
        if status == "moved":
            counts["game_files_moved"] += 1
        elif status == "exists":
            counts["game_files_exists"] += 1
            # Remove duplicate legacy flat file when nested target already exists.
            pruned = _remove_if_exists(src, dry_run)
            if pruned in {"removed", "dry"}:
                counts["legacy_pruned"] += 1

    # Remove empty legacy container dirs that are no longer part of design contract.
    for obsolete in (season_dir / "scores",):
        if obsolete.exists() and obsolete.is_dir():
            if dry_run:
                counts["legacy_removed"] += 1
            else:
                try:
                    shutil.rmtree(obsolete)
                    counts["legacy_removed"] += 1
                except Exception:
                    pass

    return counts


def _iter_target_seasons(root: Path, seasons: Iterable[str]) -> list[Path]:
    out: list[Path] = []
    for season in seasons:
        season_dir = root / season
        if season_dir.is_dir():
            out.append(season_dir)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate NHL raw layout to structured format")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=["2020", "2021", "2022", "2023", "2024", "2025", "2026"],
        help="Season folders under data/raw/nhl/nhl to migrate",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying files")
    args = parser.parse_args()

    season_dirs = _iter_target_seasons(ROOT, args.seasons)
    if not season_dirs:
        print("No target seasons found")
        return 1

    totals = {
        "schedule_moved": 0,
        "schedule_exists": 0,
        "game_files_moved": 0,
        "game_files_exists": 0,
        "legacy_removed": 0,
        "legacy_pruned": 0,
    }

    for season_dir in season_dirs:
        result = migrate_season(season_dir, args.dry_run)
        for k, v in result.items():
            totals[k] += v
        print(
            f"{season_dir.name}: "
            f"schedule_moved={result['schedule_moved']} "
            f"schedule_exists={result['schedule_exists']} "
            f"game_files_moved={result['game_files_moved']} "
            f"game_files_exists={result['game_files_exists']} "
            f"legacy_removed={result['legacy_removed']} "
            f"legacy_pruned={result['legacy_pruned']}"
        )

    print("--- totals ---")
    for key, value in totals.items():
        print(f"{key}={value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
