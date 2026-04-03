#!/usr/bin/env python3
"""Fill MLB curated entities the normalizer doesn't handle.

Covers: advanced, pitches, weather, coaches, leaders, lineups, transactions
Reads from data/raw/{statcast,weather,openmeteo,lahman,espn,mlbstats}/mlb/
Writes to data/normalized_curated/mlb/{entity}/season={YYYY}/

Usage:
    python scripts/fill_mlb_curated_gaps.py
    python scripts/fill_mlb_curated_gaps.py --entity advanced
    python scripts/fill_mlb_curated_gaps.py --entity weather --entity coaches
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_STATCAST = PROJECT_ROOT / "data" / "raw" / "statcast" / "mlb"
RAW_WEATHER = PROJECT_ROOT / "data" / "raw" / "weather" / "mlb"
RAW_OPENMETEO = PROJECT_ROOT / "data" / "raw" / "openmeteo" / "mlb"
RAW_LAHMAN = PROJECT_ROOT / "data" / "raw" / "lahman" / "mlb" / "all"
RAW_ESPN = PROJECT_ROOT / "data" / "raw" / "espn" / "mlb"
RAW_MLBSTATS = PROJECT_ROOT / "data" / "raw" / "mlbstats" / "mlb"
CURATED_BASE = PROJECT_ROOT / "data" / "normalized_curated" / "mlb"

ENTITIES = [
    "advanced",
    "pitches",
    "weather",
    "coaches",
    "leaders",
    "lineups",
    "transactions",
]

STATCAST_YEARS = range(2020, 2027)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict | list | None:
    """Load JSON with graceful error handling."""
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  WARNING: skipping {path}: {exc}", file=sys.stderr)
        return None


def write_parquet(df: pd.DataFrame, entity: str, season_year: int) -> None:
    """Write DataFrame as ZSTD-compressed parquet under season partition."""
    if df.empty:
        return
    out_dir = CURATED_BASE / entity / f"season={season_year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="zstd")
    print(f"    → wrote {out_path.relative_to(PROJECT_ROOT)} ({len(df):,} rows)")


def snake_case(name: str) -> str:
    """Normalise column name to snake_case."""
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(",", "")
        .replace(".", "_")
    )


def parse_player_name(raw: str) -> str:
    """'Soto, Juan' → 'Juan Soto'."""
    if not raw or not isinstance(raw, str):
        return ""
    parts = [p.strip() for p in raw.split(",", 1)]
    if len(parts) == 2:
        return f"{parts[1]} {parts[0]}"
    return raw.strip()


# ---------------------------------------------------------------------------
# Entity: advanced (Statcast batters + pitchers)
# ---------------------------------------------------------------------------

def fill_advanced() -> None:
    print("\n═══ advanced (Statcast) ═══")
    if not RAW_STATCAST.is_dir():
        print("  SKIP: statcast raw directory not found")
        return

    for year in STATCAST_YEARS:
        year_dir = RAW_STATCAST / str(year)
        if not year_dir.is_dir():
            continue

        records: list[dict] = []

        for fname, stat_type in [
            ("batters.json", "batting"),
            ("pitchers.json", "pitching"),
        ]:
            data = load_json(year_dir / fname)
            if not data or not isinstance(data, list):
                continue
            for row in data:
                rec: dict = {
                    "player_id": row.get("player_id"),
                    "player_name": parse_player_name(
                        row.get("last_name, first_name", "")
                    ),
                    "season": int(row.get("year", row.get("season", year))),
                    "stat_type": stat_type,
                    "source": "statcast",
                }
                # Carry over every stat column
                skip = {
                    "last_name, first_name",
                    "player_id",
                    "year",
                    "season",
                    "player_type",
                    "source",
                }
                for key, val in row.items():
                    if key in skip:
                        continue
                    col = snake_case(key)
                    rec[col] = val
                records.append(rec)

        if records:
            df = pd.DataFrame(records)
            # Coerce stat columns to numeric (raw JSON has "" for missing)
            skip_cols = {"player_id", "player_name", "season", "stat_type", "source"}
            for col in df.columns:
                if col not in skip_cols:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            print(f"  {year}: {len(df):,} rows ({df['stat_type'].value_counts().to_dict()})")
            write_parquet(df, "advanced", year)
        else:
            print(f"  {year}: no data")


# ---------------------------------------------------------------------------
# Entity: pitches (Statcast pitch-level)
# ---------------------------------------------------------------------------

def fill_pitches() -> None:
    print("\n═══ pitches (Statcast pitch-level) ═══")
    if not RAW_STATCAST.is_dir():
        print("  SKIP: statcast raw directory not found")
        return

    found = False
    for year in STATCAST_YEARS:
        year_dir = RAW_STATCAST / str(year)
        if not year_dir.is_dir():
            continue
        # Look for any file beyond the two aggregate files
        extra = [
            f
            for f in year_dir.iterdir()
            if f.is_file() and f.name not in ("batters.json", "pitchers.json")
        ]
        if extra:
            found = True
            print(f"  {year}: found pitch-level files: {[f.name for f in extra]}")

    if not found:
        print("  No pitch-level data found in statcast directories. Entity stays empty.")


# ---------------------------------------------------------------------------
# Entity: weather (VisualCrossing + OpenMeteo)
# ---------------------------------------------------------------------------

def _load_weather_files() -> list[dict]:
    """Load all VisualCrossing weather JSON files."""
    records: list[dict] = []
    if not RAW_WEATHER.is_dir():
        return records

    for json_path in sorted(RAW_WEATHER.rglob("*.json")):
        data = load_json(json_path)
        if not data or not isinstance(data, dict):
            continue
        rec: dict = {
            "game_id": str(data.get("game_id", "")),
            "game_date": data.get("date", ""),
            "season": int(data.get("season", 0)),
            "venue_name": data.get("venue", ""),
            "city": data.get("city", ""),
            "state": data.get("state", ""),
            "country": data.get("country"),
            "temperature": data.get("temp_f"),
            "wind_speed": data.get("wind_mph"),
            "wind_direction": data.get("wind_direction", ""),
            "humidity": data.get("humidity_pct"),
            "precipitation": data.get("precipitation"),
            "condition": data.get("condition", ""),
            "dome": data.get("dome"),
            "source": "weather",
        }
        records.append(rec)
    return records


def _load_openmeteo_files() -> list[dict]:
    """Load all OpenMeteo weather JSON files."""
    records: list[dict] = []
    if not RAW_OPENMETEO.is_dir():
        return records

    for json_path in sorted(RAW_OPENMETEO.rglob("*.json")):
        data = load_json(json_path)
        if not data or not isinstance(data, dict):
            continue
        # Infer season from parent path: openmeteo/mlb/{year}/dates/...
        path_season = data.get("season")
        if path_season is not None:
            path_season = int(path_season)
        else:
            # Fallback: extract from directory structure
            try:
                parts = json_path.relative_to(RAW_OPENMETEO).parts
                path_season = int(parts[0])
            except (ValueError, IndexError):
                path_season = 0

        # Average min/max temp if both present
        temp_max = data.get("temp_max_f")
        temp_min = data.get("temp_min_f")
        temperature = None
        if temp_max is not None and temp_min is not None:
            try:
                temperature = round((float(temp_max) + float(temp_min)) / 2, 1)
            except (ValueError, TypeError):
                pass
        elif temp_max is not None:
            temperature = temp_max

        # Convert numeric wind direction to cardinal
        wind_dir_deg = data.get("wind_direction_deg")
        wind_direction = _deg_to_cardinal(wind_dir_deg) if wind_dir_deg is not None else ""

        rec: dict = {
            "game_id": str(data.get("game_id", "")),
            "game_date": data.get("date", ""),
            "season": path_season,
            "venue_name": data.get("venue", ""),
            "city": data.get("city", ""),
            "state": data.get("state", ""),
            "country": data.get("country"),
            "temperature": temperature,
            "wind_speed": data.get("wind_mph"),
            "wind_direction": wind_direction,
            "humidity": None,  # openmeteo doesn't have humidity
            "precipitation": data.get("precipitation_in"),
            "condition": data.get("condition", ""),
            "dome": data.get("dome"),
            "source": "openmeteo",
        }
        records.append(rec)
    return records


def _deg_to_cardinal(deg: float | int | None) -> str:
    """Convert degrees (0-360) to 16-point cardinal direction."""
    if deg is None:
        return ""
    try:
        deg = float(deg) % 360
    except (ValueError, TypeError):
        return ""
    dirs = [
        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
    ]
    idx = int((deg + 11.25) / 22.5) % 16
    return dirs[idx]


def fill_weather() -> None:
    print("\n═══ weather (VisualCrossing + OpenMeteo) ═══")
    weather_recs = _load_weather_files()
    openmeteo_recs = _load_openmeteo_files()

    print(f"  Loaded {len(weather_recs)} weather + {len(openmeteo_recs)} openmeteo records")

    all_recs = weather_recs + openmeteo_recs
    if not all_recs:
        print("  No weather data found. Entity stays empty.")
        return

    df = pd.DataFrame(all_recs)

    # Group by season and write
    for season, group in df.groupby("season"):
        season_int = int(season)
        if season_int < 2020:
            continue
        print(f"  {season_int}: {len(group):,} rows")
        write_parquet(group.reset_index(drop=True), "weather", season_int)


# ---------------------------------------------------------------------------
# Entity: coaches (Lahman Managers.csv)
# ---------------------------------------------------------------------------

def fill_coaches() -> None:
    print("\n═══ coaches (Lahman Managers) ═══")
    csv_path = RAW_LAHMAN / "Managers.csv"
    if not csv_path.exists():
        print(f"  SKIP: {csv_path} not found")
        return

    df_raw = pd.read_csv(csv_path, low_memory=False)
    df_raw = df_raw[df_raw["yearID"] >= 2020].copy()
    if df_raw.empty:
        print("  No managers found for years >= 2020")
        return

    print(f"  Loaded {len(df_raw):,} manager records (>= 2020)")

    df = pd.DataFrame(
        {
            "manager_id": df_raw["playerID"].astype(str),
            "season": df_raw["yearID"].astype(int),
            "team_id": df_raw["teamID"].astype(str),
            "league": df_raw["lgID"].astype(str),
            "games": pd.to_numeric(df_raw["G"], errors="coerce").astype("Int64"),
            "wins": pd.to_numeric(df_raw["W"], errors="coerce").astype("Int64"),
            "losses": pd.to_numeric(df_raw["L"], errors="coerce").astype("Int64"),
            "rank": pd.to_numeric(df_raw["rank"], errors="coerce").astype("Int64"),
            "player_manager": df_raw["plyrMgr"].map({"Y": True, "N": False}).fillna(False),
            "source": "lahman",
        }
    )

    for season, group in df.groupby("season"):
        season_int = int(season)
        print(f"  {season_int}: {len(group):,} rows")
        write_parquet(group.reset_index(drop=True), "coaches", season_int)


# ---------------------------------------------------------------------------
# Entity: leaders (top 20 per stat from Statcast)
# ---------------------------------------------------------------------------

# Stat categories and sort direction (True = descending / higher is better)
BATTER_LEADER_STATS: dict[str, bool] = {
    "batting_avg": True,
    "home_run": True,
    "hit": True,
    "slg_percent": True,
    "on_base_percent": True,
    "on_base_plus_slg": True,
    "woba": True,
    "xwoba": True,
    "xba": True,
    "exit_velocity_avg": True,
    "barrel_batted_rate": True,
    "hard_hit_percent": True,
    "sprint_speed": True,
    "walk": True,
    "strikeout": False,  # lower is better
    "k_percent": False,
    "bb_percent": True,
}

PITCHER_LEADER_STATS: dict[str, bool] = {
    "k_percent": True,
    "bb_percent": False,  # lower is better
    "whiff_percent": True,
    "batting_avg": False,  # lower against is better
    "slg_percent": False,
    "woba": False,
    "xwoba": False,
    "xba": False,
    "exit_velocity_avg": False,
    "barrel_batted_rate": False,
    "hard_hit_percent": False,
    "strikeout": True,
}

TOP_N = 20


def _build_leaders_for_year(year: int) -> list[dict]:
    """Derive top-N leaders from statcast for a single year."""
    year_dir = RAW_STATCAST / str(year)
    if not year_dir.is_dir():
        return []

    records: list[dict] = []

    for fname, stat_type, stat_map in [
        ("batters.json", "batting", BATTER_LEADER_STATS),
        ("pitchers.json", "pitching", PITCHER_LEADER_STATS),
    ]:
        data = load_json(year_dir / fname)
        if not data or not isinstance(data, list):
            continue

        df = pd.DataFrame(data)
        if df.empty:
            continue

        for stat_col, ascending_bad in stat_map.items():
            if stat_col not in df.columns:
                continue

            subset = df.dropna(subset=[stat_col]).copy()
            # Filter out empty strings
            subset = subset[subset[stat_col].apply(lambda v: v != "" and v is not None)]
            if subset.empty:
                continue

            subset[stat_col] = pd.to_numeric(subset[stat_col], errors="coerce")
            subset = subset.dropna(subset=[stat_col])
            if subset.empty:
                continue

            sorted_df = subset.sort_values(
                stat_col, ascending=not ascending_bad
            ).head(TOP_N)

            for rank, (_, row) in enumerate(sorted_df.iterrows(), start=1):
                records.append(
                    {
                        "player_id": row.get("player_id"),
                        "player_name": parse_player_name(
                            row.get("last_name, first_name", "")
                        ),
                        "season": year,
                        "stat_type": stat_type,
                        "stat_category": stat_col,
                        "value": row[stat_col],
                        "rank": rank,
                        "source": "statcast",
                    }
                )

    return records


def fill_leaders() -> None:
    print("\n═══ leaders (Statcast top 20) ═══")
    if not RAW_STATCAST.is_dir():
        print("  SKIP: statcast raw directory not found")
        return

    for year in STATCAST_YEARS:
        records = _build_leaders_for_year(year)
        if records:
            df = pd.DataFrame(records)
            cats = df["stat_category"].nunique()
            print(f"  {year}: {len(df):,} rows across {cats} categories")
            write_parquet(df, "leaders", year)
        else:
            print(f"  {year}: no data")


# ---------------------------------------------------------------------------
# Entity: lineups
# ---------------------------------------------------------------------------

def fill_lineups() -> None:
    print("\n═══ lineups ═══")
    # Check ESPN and MLBStats for lineup-specific data
    found_source = False

    for year in STATCAST_YEARS:
        # Check ESPN depth charts
        espn_dir = RAW_ESPN / str(year) / "depth_charts"
        if espn_dir.is_dir():
            files = list(espn_dir.glob("*.json"))
            if files and not found_source:
                print(f"  Found ESPN depth chart data ({len(files)} files)")
                found_source = True

        # Check MLBStats for dedicated lineup files
        mlb_dir = RAW_MLBSTATS / str(year)
        lineup_patterns = ["lineups", "lineup", "starting_lineups"]
        for pattern in lineup_patterns:
            check_dir = mlb_dir / pattern
            if check_dir.is_dir():
                found_source = True
                print(f"  Found MLBStats lineup data at {check_dir}")

    if not found_source:
        print("  No dedicated lineup data source found. Entity stays empty.")
        print("  (Depth charts exist in ESPN but are roster projections, not game lineups)")


# ---------------------------------------------------------------------------
# Entity: transactions
# ---------------------------------------------------------------------------

def fill_transactions() -> None:
    print("\n═══ transactions ═══")
    # Check if entity already has real (non-synthetic) data
    entity_dir = CURATED_BASE / "transactions"
    if entity_dir.is_dir():
        parquets = list(entity_dir.rglob("*.parquet"))
        if parquets:
            try:
                sample = pd.read_parquet(parquets[0])
                sources = sample["source"].unique() if "source" in sample.columns else []
                non_synthetic = [s for s in sources if s != "synthetic"]
                if non_synthetic:
                    total_files = len(parquets)
                    print(
                        f"  Transactions already populated ({total_files} parquet files, "
                        f"sources: {non_synthetic}). Skipping."
                    )
                    return
            except Exception:
                pass

    # If we get here, transactions are empty/synthetic — check for ESPN raw data
    found = False
    for year in STATCAST_YEARS:
        tx_dir = RAW_ESPN / str(year) / "transactions"
        if tx_dir.is_dir() and any(tx_dir.iterdir()):
            found = True
            print(f"  Found ESPN transaction data for {year}")

    if not found:
        print("  No raw transaction data found to backfill. Entity stays empty.")
    else:
        print("  Transaction data exists in ESPN raw but should be handled by normalizer.")
        print("  Run the normalizer first; re-run this script to verify.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ENTITY_HANDLERS: dict[str, callable] = {
    "advanced": fill_advanced,
    "pitches": fill_pitches,
    "weather": fill_weather,
    "coaches": fill_coaches,
    "leaders": fill_leaders,
    "lineups": fill_lineups,
    "transactions": fill_transactions,
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fill MLB curated entities the normalizer doesn't handle."
    )
    parser.add_argument(
        "--entity",
        action="append",
        choices=ENTITIES,
        help="Entity to fill (repeatable). Default: all entities.",
    )
    args = parser.parse_args()

    targets = args.entity or ENTITIES
    print(f"Filling MLB curated gaps for: {', '.join(targets)}")
    print(f"Project root: {PROJECT_ROOT}")

    errors: list[str] = []
    for entity in targets:
        handler = ENTITY_HANDLERS.get(entity)
        if not handler:
            print(f"\n  WARNING: no handler for '{entity}'", file=sys.stderr)
            continue
        try:
            handler()
        except Exception as exc:
            msg = f"{entity}: {exc}"
            errors.append(msg)
            print(f"\n  ERROR in {entity}:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

    print("\n" + "═" * 50)
    if errors:
        print(f"Completed with {len(errors)} error(s):")
        for e in errors:
            print(f"  • {e}")
        return 1
    print("Done — all entities processed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
