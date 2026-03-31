#!/usr/bin/env python3
"""Generate a Dota2 game-id reconciliation report.

This script diagnoses why normalized `player_stats_{season}` game IDs fail to
match normalized `games_{season}` IDs. It writes a JSON report and a CSV sample
for unmatched IDs, including date/team-pair candidate checks.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _norm(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _key(date_value: object, team_a: object, team_b: object) -> tuple[str, str, str]:
    d = _norm(date_value)[:10]
    a = _norm(team_a)
    b = _norm(team_b)
    return (d, a, b) if a <= b else (d, b, a)


def _id_len_dist(ids: pd.Series) -> dict[str, int]:
    lens = ids.dropna().astype(str).str.len()
    vc = lens.value_counts().to_dict()
    return {str(k): int(v) for k, v in sorted(vc.items(), key=lambda kv: kv[0])}


def run(games_path: Path, stats_path: Path, out_dir: Path) -> int:
    games_df = pd.read_parquet(games_path)
    stats_df = pd.read_parquet(stats_path)

    required_games = {"id", "date", "home_team_id", "away_team_id"}
    required_stats = {"game_id", "date", "team_id"}
    if not required_games.issubset(games_df.columns):
        missing = sorted(required_games - set(games_df.columns))
        raise ValueError(f"games parquet missing columns: {missing}")
    if not required_stats.issubset(stats_df.columns):
        missing = sorted(required_stats - set(stats_df.columns))
        raise ValueError(f"player_stats parquet missing columns: {missing}")

    games_ids = set(games_df["id"].dropna().astype(str).tolist())
    stats_ids = set(stats_df["game_id"].dropna().astype(str).tolist())
    direct_overlap = stats_ids & games_ids
    unmatched = sorted(stats_ids - games_ids)

    games_key_to_ids: dict[tuple[str, str, str], list[str]] = {}
    for row in games_df.itertuples(index=False):
        key = _key(getattr(row, "date", None), getattr(row, "home_team_id", None), getattr(row, "away_team_id", None))
        gid = _norm(getattr(row, "id", None))
        if gid:
            games_key_to_ids.setdefault(key, []).append(gid)

    rows = []
    grouped = stats_df.groupby("game_id", dropna=False)
    candidate_stats = {"none": 0, "single": 0, "multiple": 0}

    for gid, grp in grouped:
        gid_s = _norm(gid)
        if not gid_s or gid_s in direct_overlap:
            continue

        date_values = grp["date"].dropna().astype(str)
        teams = sorted({_norm(t) for t in grp["team_id"].dropna().tolist() if _norm(t)})

        if date_values.empty or len(teams) != 2:
            candidate_count = 0
            candidates: list[str] = []
            key_str = ""
            date_value = date_values.iloc[0] if not date_values.empty else ""
        else:
            date_value = date_values.iloc[0]
            k = _key(date_value, teams[0], teams[1])
            candidates = games_key_to_ids.get(k, [])
            candidate_count = len(candidates)
            key_str = f"{k[0]}|{k[1]}|{k[2]}"

        if candidate_count == 0:
            candidate_stats["none"] += 1
        elif candidate_count == 1:
            candidate_stats["single"] += 1
        else:
            candidate_stats["multiple"] += 1

        rows.append(
            {
                "stats_game_id": gid_s,
                "stats_id_length": len(gid_s),
                "date": _norm(date_value)[:10],
                "team_1": teams[0] if len(teams) > 0 else "",
                "team_2": teams[1] if len(teams) > 1 else "",
                "group_rows": int(len(grp)),
                "event_key": key_str,
                "candidate_games_count": candidate_count,
                "candidate_game_ids": "|".join(candidates[:10]),
            }
        )

    sample_df = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "dota2_id_reconciliation_sample.csv"
    if not sample_df.empty:
        sample_df.sort_values(["candidate_games_count", "date"], ascending=[True, True]).head(5000).to_csv(csv_path, index=False)
    else:
        pd.DataFrame(columns=[
            "stats_game_id", "stats_id_length", "date", "team_1", "team_2",
            "group_rows", "event_key", "candidate_games_count", "candidate_game_ids",
        ]).to_csv(csv_path, index=False)

    report = {
        "games_path": str(games_path),
        "player_stats_path": str(stats_path),
        "games_rows": int(len(games_df)),
        "player_stats_rows": int(len(stats_df)),
        "unique_games_ids": int(len(games_ids)),
        "unique_player_stats_game_ids": int(len(stats_ids)),
        "direct_overlap_unique_ids": int(len(direct_overlap)),
        "unmatched_unique_ids": int(len(unmatched)),
        "games_id_length_distribution": _id_len_dist(games_df["id"]),
        "player_stats_id_length_distribution": _id_len_dist(stats_df["game_id"]),
        "candidate_match_stats": candidate_stats,
        "sample_csv": str(csv_path),
    }

    json_path = out_dir / "dota2_id_reconciliation_report.json"
    json_path.write_text(json.dumps(report, indent=2))

    print(json.dumps(report, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Dota2 game-id reconciliation diagnostics")
    parser.add_argument(
        "--games",
        type=Path,
        default=Path("/home/derek/Documents/stock/v5.0/data/normalized/dota2/games_2025.parquet"),
    )
    parser.add_argument(
        "--player-stats",
        type=Path,
        default=Path("/home/derek/Documents/stock/v5.0/data/normalized/dota2/player_stats_2025.parquet"),
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("/home/derek/Documents/stock/v5.0/data/reports"),
    )
    args = parser.parse_args()

    if not args.games.exists():
        raise FileNotFoundError(f"Games parquet missing: {args.games}")
    if not args.player_stats.exists():
        raise FileNotFoundError(f"Player stats parquet missing: {args.player_stats}")

    return run(args.games, args.player_stats, args.out_dir)


if __name__ == "__main__":
    raise SystemExit(main())
