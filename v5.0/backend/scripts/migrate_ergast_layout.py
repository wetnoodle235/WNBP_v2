#!/usr/bin/env python3
"""Reorganize Ergast raw data into reference/standings/rounds layout.

Target layout per season:
  data/raw/ergast/f1/{season}/
    reference/{drivers,constructors,circuits}.json
    standings/{driver_standings,constructor_standings}.json
    rounds/round_XX/{race,results,qualifying,sprint,laps,pitstops}.json

Legacy layout supported by this migrator:
  {season}/drivers.json
  {season}/constructors.json
  {season}/circuits.json
  {season}/driver_standings.json
  {season}/constructor_standings.json
  {season}/races.json
  {season}/results.json
  {season}/qualifying.json
  {season}/sprint.json
  {season}/laps/round_N.json
  {season}/pitstops/round_N.json
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2] / "data" / "raw" / "ergast" / "f1"

REFERENCE_ENDPOINTS = {
    "drivers": "reference/drivers.json",
    "constructors": "reference/constructors.json",
    "circuits": "reference/circuits.json",
}

STANDINGS_ENDPOINTS = {
    "driver_standings": "standings/driver_standings.json",
    "constructor_standings": "standings/constructor_standings.json",
}

ROUND_SCOPED_ENDPOINTS = {
    "races": "race.json",
    "results": "results.json",
    "qualifying": "qualifying.json",
    "sprint": "sprint.json",
}

ROUND_DIR_ENDPOINTS = {
    "laps": "laps.json",
    "pitstops": "pitstops.json",
}


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def _round_dir(season_dir: Path, round_value: Any) -> Path | None:
    try:
        round_num = int(round_value)
    except (TypeError, ValueError):
        return None
    if round_num <= 0:
        return None
    return season_dir / "rounds" / f"round_{round_num:02d}"


def _move_file(src: Path, dst: Path, dry_run: bool, keep_legacy: bool) -> int:
    if not src.exists():
        return 0
    if dry_run:
        print(f"MOVE {src} -> {dst}")
        return 1
    dst.parent.mkdir(parents=True, exist_ok=True)
    if keep_legacy:
        shutil.copy2(src, dst)
    else:
        shutil.move(src, dst)
    return 1


def migrate_reference_and_standings(season_dir: Path, dry_run: bool, keep_legacy: bool) -> int:
    moved = 0
    for endpoint, rel_target in {**REFERENCE_ENDPOINTS, **STANDINGS_ENDPOINTS}.items():
        moved += _move_file(
            season_dir / f"{endpoint}.json",
            season_dir / rel_target,
            dry_run,
            keep_legacy,
        )
    return moved


def migrate_round_scoped_endpoint(season_dir: Path, endpoint: str, file_name: str, dry_run: bool, keep_legacy: bool) -> int:
    src = season_dir / f"{endpoint}.json"
    payload = _load_json(src)
    if not payload:
        return 0

    mr_data = payload.get("MRData", {}) if isinstance(payload, dict) else {}
    race_table = mr_data.get("RaceTable", {}) if isinstance(mr_data, dict) else {}
    races = race_table.get("Races", []) if isinstance(race_table, dict) else []
    if not isinstance(races, list):
        return 0

    migrated = 0
    for race in races:
        if not isinstance(race, dict):
            continue
        round_dir = _round_dir(season_dir, race.get("round"))
        if round_dir is None:
            continue

        round_payload = {
            "MRData": {
                **mr_data,
                "total": "1",
                "RaceTable": {
                    **race_table,
                    "round": str(race.get("round", "")),
                    "Races": [race],
                },
            },
        }
        dst = round_dir / file_name
        if dry_run:
            print(f"WRITE {dst}")
            migrated += 1
            continue
        _write_json(dst, round_payload)
        migrated += 1

    if migrated > 0 and not dry_run and not keep_legacy:
        src.unlink(missing_ok=True)
    return migrated


def migrate_round_directory_endpoint(season_dir: Path, endpoint: str, file_name: str, dry_run: bool, keep_legacy: bool) -> int:
    legacy_dir = season_dir / endpoint
    if not legacy_dir.is_dir():
        return 0

    migrated = 0
    for src in sorted(legacy_dir.glob("round_*.json")):
        round_token = src.stem.replace("round_", "")
        round_dir = _round_dir(season_dir, round_token)
        if round_dir is None:
            continue
        migrated += _move_file(src, round_dir / file_name, dry_run, keep_legacy)

    if migrated > 0 and not dry_run and not keep_legacy:
        try:
            legacy_dir.rmdir()
        except OSError:
            pass
    return migrated


def migrate_season(season: str, dry_run: bool, keep_legacy: bool) -> None:
    season_dir = ROOT / season
    if not season_dir.exists():
        print(f"SKIP {season}: missing")
        return

    moved = migrate_reference_and_standings(season_dir, dry_run, keep_legacy)
    for endpoint, file_name in ROUND_SCOPED_ENDPOINTS.items():
        moved += migrate_round_scoped_endpoint(season_dir, endpoint, file_name, dry_run, keep_legacy)
    for endpoint, file_name in ROUND_DIR_ENDPOINTS.items():
        moved += migrate_round_directory_endpoint(season_dir, endpoint, file_name, dry_run, keep_legacy)

    print(f"{season}: migrated {moved} Ergast files")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reorganize Ergast raw layout by round")
    parser.add_argument("--seasons", default="", help="Comma-separated seasons to migrate (default: all existing)")
    parser.add_argument("--dry-run", action="store_true", help="Show planned moves without changing files")
    parser.add_argument("--keep-legacy", action="store_true", help="Copy into new layout instead of moving legacy files")
    args = parser.parse_args()

    seasons = [s for s in args.seasons.split(",") if s] if args.seasons else sorted(p.name for p in ROOT.iterdir() if p.is_dir())
    for season in seasons:
        migrate_season(season, args.dry_run, args.keep_legacy)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())