#!/usr/bin/env python3
"""Normalize CFBData raw layout to endpoint-first structure.

Target layout (under each season):
- Weekly split data:
  - {season}/{week}/games/{date}/{game_id}.json
  - {season}/{week}/plays/{date}/{game_id}_plays.json
  - {season}/{week}/stats/players_week_*.json
- Season-wide endpoint data:
  - {season}/{endpoint}/{endpoint}.json (or original filename)

This script cleans legacy artifacts:
- root-level *.json files
- reference/ folder
- accidental nested season folder (e.g. 2025/2025/...)
- legacy plays/ week files
- week 0 fallback stats
"""

from __future__ import annotations

import shutil
from pathlib import Path

BASE = Path("/home/derek/Documents/stock/v5.0/data/raw/cfbdata/ncaaf")
SEASONS = ["2020", "2021", "2022", "2023", "2024", "2025", "2026"]


def move_file(src: Path, dst: Path) -> bool:
    if not src.exists() or not src.is_file():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return False
    shutil.move(str(src), str(dst))
    return True


def move_tree_contents(src_dir: Path, dst_dir: Path) -> int:
    moved = 0
    if not src_dir.exists() or not src_dir.is_dir():
        return moved
    for item in src_dir.iterdir():
        target = dst_dir / item.name
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(item), str(target))
        moved += 1
    return moved


def cleanup_season(season: str) -> dict[str, int]:
    season_dir = BASE / season
    stats = {
        "moved_root_json": 0,
        "moved_reference_json": 0,
        "moved_nested_items": 0,
        "moved_legacy_week0_stats": 0,
        "moved_legacy_plays": 0,
        "removed_dirs": 0,
    }

    if not season_dir.exists():
        return stats

    # 1) Flatten accidental nested season folder {season}/{season}/...
    nested = season_dir / season
    if nested.exists() and nested.is_dir():
        stats["moved_nested_items"] += move_tree_contents(nested, season_dir)
        if nested.exists() and not any(nested.iterdir()):
            nested.rmdir()
            stats["removed_dirs"] += 1

    # 2) Move root-level json files into endpoint folders: {season}/{stem}/{filename}
    for json_file in list(season_dir.glob("*.json")):
        endpoint = json_file.stem
        dst = season_dir / endpoint / json_file.name
        if move_file(json_file, dst):
            stats["moved_root_json"] += 1

    # 3) Move reference/*.json into endpoint folders, then remove reference/
    reference_dir = season_dir / "reference"
    if reference_dir.exists() and reference_dir.is_dir():
        for json_file in list(reference_dir.glob("*.json")):
            endpoint = json_file.stem
            dst = season_dir / endpoint / json_file.name
            if move_file(json_file, dst):
                stats["moved_reference_json"] += 1
        if not any(reference_dir.iterdir()):
            reference_dir.rmdir()
            stats["removed_dirs"] += 1

    # 4) Move legacy week 0 stats into endpoint folder and remove empty week0 dirs
    week0_stats = season_dir / "0" / "stats"
    if week0_stats.exists() and week0_stats.is_dir():
        for json_file in list(week0_stats.glob("*.json")):
            dst = season_dir / "stats_player_season" / json_file.name
            if move_file(json_file, dst):
                stats["moved_legacy_week0_stats"] += 1

        # Remove empty 0/stats and 0
        if week0_stats.exists() and not any(week0_stats.iterdir()):
            week0_stats.rmdir()
            stats["removed_dirs"] += 1
        week0 = season_dir / "0"
        if week0.exists() and week0.is_dir() and not any(week0.iterdir()):
            week0.rmdir()
            stats["removed_dirs"] += 1

    # 5) Move legacy plays/week_*.json into plays_legacy/
    legacy_plays = season_dir / "plays"
    if legacy_plays.exists() and legacy_plays.is_dir():
        for play_file in list(legacy_plays.glob("week_*.json")):
            dst = season_dir / "plays_legacy" / play_file.name
            if move_file(play_file, dst):
                stats["moved_legacy_plays"] += 1
        if not any(legacy_plays.iterdir()):
            legacy_plays.rmdir()
            stats["removed_dirs"] += 1

    return stats


def main() -> int:
    print("Cleaning CFBData layout to endpoint-first structure...")
    for season in SEASONS:
        stats = cleanup_season(season)
        print(
            f"{season}: "
            f"root_json={stats['moved_root_json']} "
            f"reference_json={stats['moved_reference_json']} "
            f"nested={stats['moved_nested_items']} "
            f"week0_stats={stats['moved_legacy_week0_stats']} "
            f"legacy_plays={stats['moved_legacy_plays']} "
            f"removed_dirs={stats['removed_dirs']}"
        )
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
