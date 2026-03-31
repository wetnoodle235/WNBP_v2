#!/usr/bin/env python3
"""Generate players_{season}.parquet files for college sports (NCAAB, NCAAF, NCAAW)
by extracting unique player info from player_stats parquet files."""

import glob
import re
import sys
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent / "data" / "normalized"
SPORTS = ["ncaab", "ncaaf", "ncaaw"]

# Map player_stats columns -> players output columns
COL_MAP = {
    "player_id": "id",
    "player_name": "name",
    "team_id": "team_id",
    "team": "team_id",       # fallback alias
    "team_name": "team_id",  # fallback alias
    "sport": "sport",
}

# Output schema matching NBA players parquet
OUTPUT_COLS = [
    "source", "id", "sport", "name", "team_id", "position",
    "jersey_number", "height", "weight", "birth_date", "birth_place",
    "nationality", "experience_years", "college", "age", "status", "headshot_url",
]


def extract_season(filepath: str) -> str:
    m = re.search(r"player_stats_(\d{4})\.parquet$", filepath)
    return m.group(1) if m else None


def build_players_df(stats_df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique players from a player_stats DataFrame."""
    # Resolve column names
    rename = {}
    for src, dst in COL_MAP.items():
        if src in stats_df.columns and dst not in rename.values():
            rename[src] = dst

    df = stats_df.rename(columns=rename)

    if "id" not in df.columns:
        print("  WARNING: no player_id column found, skipping")
        return pd.DataFrame()

    # Keep last occurrence per player (most recent game has latest team)
    df = df.sort_values("date") if "date" in df.columns else df
    dedup_cols = ["id"]
    keep_cols = [c for c in ["id", "name", "team_id", "sport", "source"] if c in df.columns]
    players = df[keep_cols].drop_duplicates(subset="id", keep="last").copy()

    # Fill in output schema columns
    for col in OUTPUT_COLS:
        if col not in players.columns:
            players[col] = None

    players["status"] = "active"
    players = players[OUTPUT_COLS].reset_index(drop=True)
    return players


def main():
    total = 0
    print(f"Base directory: {BASE_DIR}\n")

    for sport in SPORTS:
        sport_dir = BASE_DIR / sport
        pattern = str(sport_dir / "player_stats_*.parquet")
        files = sorted(glob.glob(pattern))

        if not files:
            print(f"[{sport.upper()}] No player_stats files found")
            continue

        print(f"[{sport.upper()}] Found {len(files)} player_stats file(s)")

        for filepath in files:
            season = extract_season(filepath)
            if not season:
                continue

            out_path = sport_dir / f"players_{season}.parquet"
            if out_path.exists():
                existing = pd.read_parquet(out_path)
                print(f"  {season}: SKIP (already exists with {len(existing)} players)")
                total += len(existing)
                continue

            stats = pd.read_parquet(filepath)
            players = build_players_df(stats)

            if players.empty:
                print(f"  {season}: SKIP (no player data)")
                continue

            players.to_parquet(out_path, index=False, engine="pyarrow")
            print(f"  {season}: wrote {len(players)} players -> {out_path.name}")
            total += len(players)

        print()

    print(f"Total players across all sports/seasons: {total}")


if __name__ == "__main__":
    main()
