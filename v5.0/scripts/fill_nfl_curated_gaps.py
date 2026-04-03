#!/usr/bin/env python3
"""Fill empty NFL curated entities by reading raw JSON data.

Handles: advanced, roster, rankings, weather
Reads from data/raw/nflfastr/, data/raw/nflverse/, data/raw/espn/
Writes to data/normalized_curated/nfl/{entity}/season={YYYY}/

Usage:
    python scripts/fill_nfl_curated_gaps.py
    python scripts/fill_nfl_curated_gaps.py --entity advanced
    python scripts/fill_nfl_curated_gaps.py --entity roster --entity advanced
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_NFLFASTR = PROJECT_ROOT / "data" / "raw" / "nflfastr" / "nfl"
RAW_NFLVERSE = PROJECT_ROOT / "data" / "raw" / "nflverse" / "nfl"
RAW_ESPN = PROJECT_ROOT / "data" / "raw" / "espn" / "nfl"
CURATED_BASE = PROJECT_ROOT / "data" / "normalized_curated" / "nfl"

ENTITIES = ["advanced", "roster", "rankings", "weather"]

NFLFASTR_YEARS = range(2020, 2025)
NFLVERSE_YEARS = range(2020, 2026)
ESPN_ROSTER_YEARS = range(2023, 2027)

# nflfastr file configurations: (scope, stat_type, filename)
ADVSTATS_FILES = []
for _scope in ("season", "week"):
    for _stat in ("pass", "rush", "rec", "def"):
        ADVSTATS_FILES.append((_scope, _stat, f"advstats_{_scope}_{_stat}.json"))

# Mapping from stat_type short names to canonical names
STAT_TYPE_MAP = {"pass": "passing", "rush": "rushing", "rec": "receiving", "def": "defense"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict | None:
    """Load JSON with graceful error handling."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  WARNING: skipping {path.relative_to(PROJECT_ROOT)}: {e}", file=sys.stderr)
        return None


def write_parquet(df: pd.DataFrame, entity: str, season_year: int) -> None:
    """Write DataFrame as zstd-compressed parquet."""
    out_dir = CURATED_BASE / entity / f"season={season_year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="zstd")
    print(f"    → wrote {out_path.relative_to(PROJECT_ROOT)} ({len(df):,} rows)")


def snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all column names are lowercase snake_case."""
    df.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df


# ---------------------------------------------------------------------------
# Entity: advanced  (40 nflfastr files: 5 years × 8 stat files)
# ---------------------------------------------------------------------------

# Column mappings to normalise inconsistent field names across stat types.
# Season-level pass files use 'player'/'team'; season rush/rec/def use 'player'/'tm'.
# Week-level files use 'pfr_player_name'/'pfr_player_id'/'team'.

def _normalize_advanced_row(row: dict, scope: str, stat_type: str) -> dict:
    """Extract the canonical player_id, player_name, team, season, week from a raw row."""
    meta: dict = {}

    if scope == "season":
        meta["player_name"] = row.get("player", "")
        meta["player_id"] = row.get("pfr_id", "")
        meta["team"] = row.get("team") or row.get("tm", "")
        meta["season"] = row.get("season")
        meta["week"] = None
    else:
        meta["player_name"] = row.get("pfr_player_name", "")
        meta["player_id"] = row.get("pfr_player_id", "")
        meta["team"] = row.get("team", "")
        meta["season"] = row.get("season")
        meta["week"] = row.get("week")

    meta["stat_type"] = STAT_TYPE_MAP[stat_type]
    meta["scope"] = "season" if scope == "season" else "game"
    return meta


# Keys that are metadata, not stats — will be replaced by normalised versions
_META_KEYS_SEASON = {
    "player", "team", "tm", "season", "pfr_id", "loaded",
    "age", "pos", "g", "gs",
}
_META_KEYS_WEEK = {
    "game_id", "pfr_game_id", "season", "week", "game_type",
    "team", "opponent", "pfr_player_name", "pfr_player_id",
}


def fill_advanced() -> None:
    print("\n═══ advanced (nflfastr) ═══")
    for year in NFLFASTR_YEARS:
        year_dir = RAW_NFLFASTR / str(year)
        if not year_dir.is_dir():
            print(f"  {year}: raw dir not found, skipping")
            continue

        all_rows: list[dict] = []

        for scope, stat_type, filename in ADVSTATS_FILES:
            fpath = year_dir / filename
            if not fpath.is_file():
                print(f"  {year}/{filename}: not found, skipping")
                continue

            data = load_json(fpath)
            if not data or "data" not in data or not data["data"]:
                print(f"  {year}/{filename}: empty or malformed, skipping")
                continue

            records = data["data"]
            meta_keys = _META_KEYS_SEASON if scope == "season" else _META_KEYS_WEEK
            stat_count = 0

            for raw_row in records:
                meta = _normalize_advanced_row(raw_row, scope, stat_type)
                # Collect stat columns (everything not in meta keys)
                stat_cols = {
                    k: v for k, v in raw_row.items() if k not in meta_keys
                }
                row = {**meta, **stat_cols}
                all_rows.append(row)
                stat_count += 1

            print(f"  {year}/{filename}: {stat_count:,} rows")

        if all_rows:
            df = pd.DataFrame(all_rows)
            df = snake_case_columns(df)
            df["source"] = "nflfastr"
            write_parquet(df, "advanced", year)
        else:
            print(f"  {year}: no data collected")


# ---------------------------------------------------------------------------
# Entity: roster  (nflverse + ESPN rosters)
# ---------------------------------------------------------------------------

def _parse_nflverse_rosters(year: int) -> list[dict]:
    """Parse nflverse rosters.json for a given year."""
    fpath = RAW_NFLVERSE / str(year) / "rosters.json"
    if not fpath.is_file():
        return []

    data = load_json(fpath)
    if not data or "rosters" not in data:
        return []

    rows = []
    for r in data["rosters"]:
        rows.append({
            "player_id": r.get("gsis_id") or r.get("espn_id") or "",
            "player_name": r.get("full_name", ""),
            "team_id": r.get("team", ""),
            "team_name": "",  # nflverse uses abbreviation only
            "position": r.get("position", ""),
            "jersey_number": r.get("jersey_number"),
            "status": r.get("status", ""),
            "height": r.get("height"),
            "weight": r.get("weight"),
            "age": None,
            "experience": r.get("years_exp"),
            "college": r.get("college", ""),
            "source": "nflverse",
        })
    return rows


def _parse_espn_rosters(year: int) -> list[dict]:
    """Parse ESPN roster JSON files for a given year."""
    rosters_dir = RAW_ESPN / str(year) / "rosters"
    if not rosters_dir.is_dir():
        return []

    rows = []
    team_files = sorted(rosters_dir.iterdir())
    for tf in team_files:
        if not tf.suffix == ".json":
            continue

        data = load_json(tf)
        if not data or "athletes" not in data:
            continue

        team_id = str(data.get("teamId", tf.stem))
        team_name = data.get("teamName", "")

        for group in data["athletes"]:
            for athlete in group.get("items", []):
                pos_info = athlete.get("position") or {}
                status_info = athlete.get("status") or {}
                exp_info = athlete.get("experience") or {}
                college_info = athlete.get("college") or {}

                rows.append({
                    "player_id": str(athlete.get("id", "")),
                    "player_name": athlete.get("fullName") or athlete.get("displayName", ""),
                    "team_id": team_id,
                    "team_name": team_name,
                    "position": pos_info.get("abbreviation", ""),
                    "jersey_number": athlete.get("jersey"),
                    "status": status_info.get("name", ""),
                    "height": athlete.get("height"),
                    "weight": athlete.get("weight"),
                    "age": athlete.get("age"),
                    "experience": exp_info.get("years"),
                    "college": college_info.get("name", "") if isinstance(college_info, dict) else str(college_info),
                    "source": "espn",
                })
    return rows


def fill_roster() -> None:
    print("\n═══ roster ═══")

    # Collect all years across both sources
    all_years = sorted(set(NFLVERSE_YEARS) | set(ESPN_ROSTER_YEARS))

    for year in all_years:
        rows: list[dict] = []

        nflverse_rows = _parse_nflverse_rosters(year)
        if nflverse_rows:
            print(f"  {year}/nflverse: {len(nflverse_rows):,} players")
            rows.extend(nflverse_rows)

        espn_rows = _parse_espn_rosters(year)
        if espn_rows:
            print(f"  {year}/espn: {len(espn_rows):,} players")
            rows.extend(espn_rows)

        if rows:
            df = pd.DataFrame(rows)
            df = snake_case_columns(df)
            # Ensure consistent types across nflverse (str) and ESPN (int) sources
            for col in ("height", "weight", "age", "experience", "jersey_number"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            write_parquet(df, "roster", year)
        else:
            print(f"  {year}: no roster data found")


# ---------------------------------------------------------------------------
# Entity: rankings
# ---------------------------------------------------------------------------

def fill_rankings() -> None:
    print("\n═══ rankings ═══")

    # Check common locations for rankings data
    candidates = [
        RAW_ESPN,
        PROJECT_ROOT / "data" / "raw" / "espn" / "nfl",
        PROJECT_ROOT / "data" / "raw" / "nflfastr" / "nfl",
        PROJECT_ROOT / "data" / "raw" / "nflverse" / "nfl",
    ]

    found = False
    for base in candidates:
        if not base.is_dir():
            continue
        for year_dir in sorted(base.iterdir()):
            if not year_dir.is_dir():
                continue
            rankings_dir = year_dir / "rankings"
            if rankings_dir.is_dir() and any(rankings_dir.iterdir()):
                found = True
                print(f"  Found rankings data at {rankings_dir.relative_to(PROJECT_ROOT)}")
                break
        if found:
            break

    if not found:
        print("  No raw NFL rankings data found in any provider — entity stays empty.")
        print("  Checked: espn/nfl/*/rankings/, nflfastr/nfl/*/rankings/, nflverse/nfl/*/rankings/")


# ---------------------------------------------------------------------------
# Entity: weather
# ---------------------------------------------------------------------------

def fill_weather() -> None:
    print("\n═══ weather ═══")

    candidates = [
        PROJECT_ROOT / "data" / "raw" / "weather" / "nfl",
        PROJECT_ROOT / "data" / "raw" / "openmeteo" / "nfl",
    ]

    found = False
    for path in candidates:
        if path.is_dir() and any(path.iterdir()):
            found = True
            print(f"  Found weather data at {path.relative_to(PROJECT_ROOT)}")
            break

    if not found:
        print("  No raw NFL weather data found — entity stays empty.")
        print("  Checked: weather/nfl/, openmeteo/nfl/")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

FILL_FUNCTIONS = {
    "advanced": fill_advanced,
    "roster": fill_roster,
    "rankings": fill_rankings,
    "weather": fill_weather,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill empty NFL curated entities from raw JSON data."
    )
    parser.add_argument(
        "--entity",
        action="append",
        dest="entities",
        choices=ENTITIES,
        help="Entity to fill (repeatable). Omit to fill all.",
    )
    args = parser.parse_args()

    targets = args.entities or ENTITIES
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Filling entities: {', '.join(targets)}")

    for entity in targets:
        try:
            FILL_FUNCTIONS[entity]()
        except Exception:
            print(f"\nERROR processing {entity}:", file=sys.stderr)
            traceback.print_exc()
            continue

    print("\n✓ Done.")


if __name__ == "__main__":
    main()
