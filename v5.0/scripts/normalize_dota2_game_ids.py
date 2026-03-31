#!/usr/bin/env python3
"""Normalize Dota2 player_stats game_id values to match games ids.

This script targets the 2025 mismatch where games parquet uses mostly short ids
while player_stats uses long ids. It derives a mapping using a stable event key:

  event_key = (date, sorted_team_id_pair)

For each player_stats game_id, we infer the team pair from grouped rows and match
it against games rows with the same date and team pair.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _norm_team_id(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _event_key(date_value: object, team_a: object, team_b: object) -> tuple[str, str, str]:
    d = "" if date_value is None else str(date_value)[:10]
    t1 = _norm_team_id(team_a)
    t2 = _norm_team_id(team_b)
    if t1 <= t2:
        return (d, t1, t2)
    return (d, t2, t1)


def build_mapping(games_df: pd.DataFrame, stats_df: pd.DataFrame) -> dict[str, str]:
    required_game_cols = {"id", "date", "home_team_id", "away_team_id"}
    required_stats_cols = {"game_id", "date", "team_id"}

    if not required_game_cols.issubset(games_df.columns):
        missing = sorted(required_game_cols - set(games_df.columns))
        raise ValueError(f"games parquet missing columns: {missing}")
    if not required_stats_cols.issubset(stats_df.columns):
        missing = sorted(required_stats_cols - set(stats_df.columns))
        raise ValueError(f"player_stats parquet missing columns: {missing}")

    games_key_to_ids: dict[tuple[str, str, str], list[str]] = {}
    for row in games_df.itertuples(index=False):
        key = _event_key(getattr(row, "date", None), getattr(row, "home_team_id", None), getattr(row, "away_team_id", None))
        gid = str(getattr(row, "id", ""))
        if not gid:
            continue
        games_key_to_ids.setdefault(key, []).append(gid)

    # Infer match-level team pairs from player rows grouped by stats game_id.
    grouped = stats_df.groupby("game_id", dropna=False)
    mapping: dict[str, str] = {}

    for stats_gid, grp in grouped:
        if pd.isna(stats_gid):
            continue
        teams = sorted({_norm_team_id(v) for v in grp["team_id"].dropna().tolist() if _norm_team_id(v)})
        if len(teams) != 2:
            continue
        date_value = grp["date"].dropna().astype(str).head(1)
        if date_value.empty:
            continue

        key = _event_key(date_value.iloc[0], teams[0], teams[1])
        candidates = games_key_to_ids.get(key, [])
        if len(candidates) == 1:
            mapping[str(stats_gid)] = candidates[0]

    return mapping


def run(
    games_path: Path,
    stats_path: Path,
    *,
    output_path: Path | None,
    write: bool,
) -> int:
    games_df = pd.read_parquet(games_path)
    stats_df = pd.read_parquet(stats_path)

    mapping = build_mapping(games_df, stats_df)
    total_stats_games = stats_df["game_id"].astype(str).nunique()

    mapped_before = stats_df["game_id"].astype(str).isin(set(games_df["id"].astype(str))).sum()

    print(f"Unique player_stats game_ids: {total_stats_games:,}")
    print(f"Mapped stats game_ids: {len(mapping):,}")
    print(f"Rows already matching games ids before mapping: {mapped_before:,}")

    if not mapping:
        print("No deterministic mapping candidates found; no file written.")
        return 0

    updated = stats_df.copy()
    updated["game_id"] = updated["game_id"].astype(str).map(lambda gid: mapping.get(gid, gid))

    overlap_after = updated["game_id"].astype(str).isin(set(games_df["id"].astype(str))).sum()
    print(f"Rows matching games ids after mapping: {overlap_after:,}")

    target = output_path or stats_path
    if write:
        updated.to_parquet(target, index=False)
        print(f"Wrote normalized player_stats parquet: {target}")
    else:
        print("Dry run complete. Use --write to persist changes.")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize Dota2 player_stats game_id format")
    parser.add_argument(
        "--games",
        type=Path,
        default=Path("/home/derek/Documents/stock/v5.0/data/normalized/dota2/games_2025.parquet"),
        help="Path to Dota2 games parquet",
    )
    parser.add_argument(
        "--player-stats",
        type=Path,
        default=Path("/home/derek/Documents/stock/v5.0/data/normalized/dota2/player_stats_2025.parquet"),
        help="Path to Dota2 player_stats parquet",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output path (defaults to input file when --write is used)",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist changes to parquet (default is dry run)",
    )
    args = parser.parse_args()

    if not args.games.exists():
        raise FileNotFoundError(f"Games parquet not found: {args.games}")
    if not args.player_stats.exists():
        raise FileNotFoundError(f"Player stats parquet not found: {args.player_stats}")

    return run(args.games, args.player_stats, output_path=args.output, write=args.write)


if __name__ == "__main__":
    raise SystemExit(main())
