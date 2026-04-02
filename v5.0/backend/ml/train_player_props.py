#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────
# V5.0 Backend — Player Props ML Training Module
# ──────────────────────────────────────────────────────────
"""
Train player prop prediction models for NBA, NFL, MLB, and NHL.

For each sport, loads:
  - Game features from data/features/{sport}_all.parquet
  - Player stats from data/normalized/{sport}/player_stats_{year}.parquet

Computes team-level rolling averages (last 5 games) per team as features,
then trains classifiers/regressors for sport-specific prop targets.

Models saved to: ml/models/{sport}/player_props.pkl

Usage
-----
::

    python3 backend/ml/train_player_props.py --sport nba --seasons 2022,2023,2024,2025
    python3 backend/ml/train_player_props.py --sport nfl --seasons 2022,2023,2024,2025
    python3 backend/ml/train_player_props.py --sport mlb --seasons 2022,2023,2024
    python3 backend/ml/train_player_props.py --sport nhl --seasons 2022,2023,2024,2025
"""
from __future__ import annotations

import argparse
import logging
import pickle
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger("train_player_props")

# ── Path setup ───────────────────────────────────────────

_HERE = Path(__file__).resolve().parent
_BACKEND_DIR = _HERE.parent
_PROJECT_ROOT = _BACKEND_DIR.parent
_DATA_DIR = _PROJECT_ROOT / "data"
_MODELS_ROOT = _PROJECT_ROOT / "ml" / "models"

sys.path.insert(0, str(_BACKEND_DIR))

# ── Sport-specific prop definitions ─────────────────────

# Each entry: (target_name, stat_col, threshold, model_type, description)
# model_type: "cls" = binary classifier, "reg" = regressor
_PROP_SPECS: dict[str, list[tuple[str, str, float, str, str]]] = {
    "nba": [
        # Classification: over/under threshold props (ESPN data has pts/reb/ast/stl/blk/to/oreb/dreb/pf)
        ("pts_over_20", "pts", 20.0, "cls", "Top scorer >20 pts"),
        ("pts_over_25", "pts", 25.0, "cls", "Top scorer >25 pts"),
        ("pts_over_30", "pts", 30.0, "cls", "Top scorer >30 pts"),
        ("reb_over_8", "reb", 8.0, "cls", "Top rebounder >8 reb"),
        ("reb_over_10", "reb", 10.0, "cls", "Top rebounder >10 reb"),
        ("ast_over_6", "ast", 6.0, "cls", "Top assister >6 ast"),
        ("ast_over_8", "ast", 8.0, "cls", "Top assister >8 ast"),
        ("stl_over_1", "stl", 1.0, "cls", "Top player >1 steal"),
        ("blk_over_1", "blk", 1.0, "cls", "Top player >1 block"),
        ("to_over_3", "to", 3.0, "cls", "Top player >3 turnovers"),
        ("pts_reb_ast_over_35", "_pts_reb_ast", 35.0, "cls", "Top player pts+reb+ast >35"),
        ("pts_reb_ast_over_45", "_pts_reb_ast", 45.0, "cls", "Top player pts+reb+ast >45"),
        ("double_double", "_double_double", 0.5, "cls", "Top player double-double"),
        ("oreb_over_3", "oreb", 3.0, "cls", "Top player >3 offensive rebounds"),
        ("dreb_over_8", "dreb", 8.0, "cls", "Top player >8 defensive rebounds"),
        # Regression: predicted stat value
        ("home_top_scorer_pts", "pts", 0.0, "reg", "Top home scorer pts"),
        ("top_scorer_pts_reg", "pts", 0.0, "reg", "Top scorer pts (regression)"),
        ("top_reb_reg", "reb", 0.0, "reg", "Top rebounder rebounds (regression)"),
        ("top_ast_reg", "ast", 0.0, "reg", "Top assister assists (regression)"),
    ],
    "nfl": [
        ("pass_yds_over_250", "pass_yds", 250.0, "cls", "QB pass yards >250"),
        ("pass_yds_over_300", "pass_yds", 300.0, "cls", "QB pass yards >300"),
        ("pass_td_over_2", "pass_td", 2.0, "cls", "QB passing TDs >2"),
        ("pass_td_over_3", "pass_td", 3.0, "cls", "QB passing TDs >3"),
        ("rush_yds_over_75", "rush_yds", 75.0, "cls", "RB rush yards >75"),
        ("rush_yds_over_100", "rush_yds", 100.0, "cls", "RB rush yards >100"),
        ("rec_yds_over_75", "rec_yds", 75.0, "cls", "WR receiving yards >75"),
        ("rec_yds_over_100", "rec_yds", 100.0, "cls", "WR receiving yards >100"),
        ("receptions_over_5", "receptions", 5.0, "cls", "Player >5 receptions"),
        ("completions_over_20", "pass_cmp", 20.0, "cls", "QB >20 completions"),
        ("rush_td_over_0", "rush_td", 0.5, "cls", "Rushing TD scorer"),
        ("rec_td_over_0", "rec_td", 0.5, "cls", "Receiving TD scorer"),
        # Regression
        ("top_passer_yds_reg", "pass_yds", 0.0, "reg", "Top passer yards (regression)"),
        ("top_rusher_yds_reg", "rush_yds", 0.0, "reg", "Top rusher yards (regression)"),
        ("top_receiver_yds_reg", "rec_yds", 0.0, "reg", "Top receiver yards (regression)"),
    ],
    "mlb": [
        ("pitcher_k_over_6", "strikeouts", 6.0, "cls", "Starting pitcher strikeouts >6"),
        ("pitcher_k_over_8", "strikeouts", 8.0, "cls", "Starting pitcher strikeouts >8"),
        ("batter_hit_over_1", "hits", 1.0, "cls", "Top batter >1 hit"),
        ("batter_hr", "hr", 0.5, "cls", "Top batter hits a HR"),
        ("batter_rbi_over_1", "rbi", 1.0, "cls", "Top batter >1 RBI"),
        ("batter_runs_over_1", "runs", 1.0, "cls", "Top batter >1 run scored"),
        ("batter_bb_over_1", "bb", 1.0, "cls", "Top batter >1 walk"),
        ("total_runs_over_8", "runs", 8.0, "cls", "Game >8 total runs"),
        ("total_hits_over_10", "hits", 10.0, "cls", "Game >10 total hits"),
        # Regression
        ("top_batter_hits_reg", "hits", 0.0, "reg", "Top batter hits (regression)"),
        ("pitcher_k_reg", "strikeouts", 0.0, "reg", "Starting pitcher Ks (regression)"),
        ("top_batter_rbi_reg", "rbi", 0.0, "reg", "Top batter RBIs (regression)"),
    ],
    "nhl": [
        # Team-level (sum aggregation)
        ("team_goals_over_3", "goals", 3.0, "cls", "Team scores >3 goals"),
        ("team_goals_over_4", "goals", 4.0, "cls", "Team scores >4 goals"),
        ("team_goals_over_6", "goals", 6.0, "cls", "Combined >6 goals"),
        ("team_shots_over_60", "shots", 60.0, "cls", "Combined >60 shots"),
        ("team_shots_over_65", "shots", 65.0, "cls", "Combined >65 shots"),
        # Player-level (max aggregation)
        ("player_goals_over_1", "goals", 1.0, "cls", "Top player >1 goal"),
        ("player_assists_over_1", "assists", 1.0, "cls", "Top player >1 assist"),
        ("player_shots_over_3", "shots", 3.0, "cls", "Top player >3 shots"),
        ("player_shots_over_5", "shots", 5.0, "cls", "Top player >5 shots"),
        ("goalie_saves_over_25", "saves", 25.0, "cls", "Goalie >25 saves"),
        ("goalie_saves_over_30", "saves", 30.0, "cls", "Goalie >30 saves"),
        ("player_hits_over_3", "hits", 3.0, "cls", "Top player >3 hits"),
        ("player_blocked_over_3", "blocked_shots", 3.0, "cls", "Top player >3 blocked shots"),
        # Regression
        ("team_goals_reg", "goals", 0.5, "reg", "Team goal total (regression)"),
        ("goalie_saves_reg", "saves", 0.0, "reg", "Goalie saves (regression)"),
        ("player_shots_reg", "shots", 0.0, "reg", "Top player shots (regression)"),
    ],
    # ── College basketball (same ESPN stat columns as NBA) ──
    "ncaab": [
        ("pts_over_15", "pts", 15.0, "cls", "Top scorer >15 pts"),
        ("pts_over_20", "pts", 20.0, "cls", "Top scorer >20 pts"),
        ("pts_over_25", "pts", 25.0, "cls", "Top scorer >25 pts"),
        ("reb_over_6", "reb", 6.0, "cls", "Top rebounder >6 reb"),
        ("reb_over_10", "reb", 10.0, "cls", "Top rebounder >10 reb"),
        ("ast_over_4", "ast", 4.0, "cls", "Top assister >4 ast"),
        ("ast_over_6", "ast", 6.0, "cls", "Top assister >6 ast"),
        ("stl_over_1", "stl", 1.0, "cls", "Top player >1 steal"),
        ("blk_over_1", "blk", 1.0, "cls", "Top player >1 block"),
        ("three_m_over_3", "three_m", 3.0, "cls", "Top player >3 three-pointers"),
        ("to_over_3", "to", 3.0, "cls", "Top player >3 turnovers"),
        ("top_scorer_pts_reg", "pts", 0.0, "reg", "Top scorer pts (regression)"),
        ("top_reb_reg", "reb", 0.0, "reg", "Top rebounder rebounds (regression)"),
        ("top_ast_reg", "ast", 0.0, "reg", "Top assister assists (regression)"),
    ],
    "ncaaw": [
        ("pts_over_15", "pts", 15.0, "cls", "Top scorer >15 pts"),
        ("pts_over_20", "pts", 20.0, "cls", "Top scorer >20 pts"),
        ("reb_over_6", "reb", 6.0, "cls", "Top rebounder >6 reb"),
        ("reb_over_10", "reb", 10.0, "cls", "Top rebounder >10 reb"),
        ("ast_over_4", "ast", 4.0, "cls", "Top assister >4 ast"),
        ("stl_over_1", "stl", 1.0, "cls", "Top player >1 steal"),
        ("blk_over_1", "blk", 1.0, "cls", "Top player >1 block"),
        ("three_m_over_3", "three_m", 3.0, "cls", "Top player >3 three-pointers"),
        ("top_scorer_pts_reg", "pts", 0.0, "reg", "Top scorer pts (regression)"),
        ("top_reb_reg", "reb", 0.0, "reg", "Top rebounder rebounds (regression)"),
    ],
    "wnba": [
        ("pts_over_15", "pts", 15.0, "cls", "Top scorer >15 pts"),
        ("pts_over_20", "pts", 20.0, "cls", "Top scorer >20 pts"),
        ("reb_over_6", "reb", 6.0, "cls", "Top rebounder >6 reb"),
        ("reb_over_8", "reb", 8.0, "cls", "Top rebounder >8 reb"),
        ("ast_over_4", "ast", 4.0, "cls", "Top assister >4 ast"),
        ("ast_over_6", "ast", 6.0, "cls", "Top assister >6 ast"),
        ("stl_over_1", "stl", 1.0, "cls", "Top player >1 steal"),
        ("blk_over_1", "blk", 1.0, "cls", "Top player >1 block"),
        ("three_m_over_2", "three_m", 2.0, "cls", "Top player >2 three-pointers"),
        ("top_scorer_pts_reg", "pts", 0.0, "reg", "Top scorer pts (regression)"),
        ("top_reb_reg", "reb", 0.0, "reg", "Top rebounder rebounds (regression)"),
        ("top_ast_reg", "ast", 0.0, "reg", "Top assister assists (regression)"),
    ],
    # ── Soccer leagues (shared columns: goals, assists, shots, shots_on_target, etc.) ──
    "epl": [
        ("player_goal", "goals", 0.5, "cls", "Player scores a goal"),
        ("player_assist", "assists", 0.5, "cls", "Player records an assist"),
        ("player_shots_over_2", "shots", 2.0, "cls", "Player >2 shots"),
        ("player_shots_over_4", "shots", 4.0, "cls", "Player >4 shots"),
        ("player_sot_over_1", "shots_on_target", 1.0, "cls", "Player >1 shot on target"),
        ("player_yellow", "yellow_cards", 0.5, "cls", "Player gets yellow card"),
        ("goalie_saves_over_3", "saves", 3.0, "cls", "Keeper >3 saves"),
        ("goalie_saves_over_5", "saves", 5.0, "cls", "Keeper >5 saves"),
        ("top_scorer_goals_reg", "goals", 0.0, "reg", "Top scorer goals (regression)"),
        ("goalie_saves_reg", "saves", 0.0, "reg", "Keeper saves (regression)"),
    ],
    "laliga": [
        ("player_goal", "goals", 0.5, "cls", "Player scores a goal"),
        ("player_assist", "assists", 0.5, "cls", "Player records an assist"),
        ("player_shots_over_2", "shots", 2.0, "cls", "Player >2 shots"),
        ("player_shots_over_4", "shots", 4.0, "cls", "Player >4 shots"),
        ("player_sot_over_1", "shots_on_target", 1.0, "cls", "Player >1 shot on target"),
        ("player_yellow", "yellow_cards", 0.5, "cls", "Player gets yellow card"),
        ("goalie_saves_over_3", "saves", 3.0, "cls", "Keeper >3 saves"),
        ("top_scorer_goals_reg", "goals", 0.0, "reg", "Top scorer goals (regression)"),
    ],
    "bundesliga": [
        ("player_goal", "goals", 0.5, "cls", "Player scores a goal"),
        ("player_assist", "assists", 0.5, "cls", "Player records an assist"),
        ("player_shots_over_2", "shots", 2.0, "cls", "Player >2 shots"),
        ("player_sot_over_1", "shots_on_target", 1.0, "cls", "Player >1 shot on target"),
        ("player_yellow", "yellow_cards", 0.5, "cls", "Player gets yellow card"),
        ("goalie_saves_over_3", "saves", 3.0, "cls", "Keeper >3 saves"),
        ("top_scorer_goals_reg", "goals", 0.0, "reg", "Top scorer goals (regression)"),
    ],
    "seriea": [
        ("player_goal", "goals", 0.5, "cls", "Player scores a goal"),
        ("player_assist", "assists", 0.5, "cls", "Player records an assist"),
        ("player_shots_over_2", "shots", 2.0, "cls", "Player >2 shots"),
        ("player_sot_over_1", "shots_on_target", 1.0, "cls", "Player >1 shot on target"),
        ("player_yellow", "yellow_cards", 0.5, "cls", "Player gets yellow card"),
        ("goalie_saves_over_3", "saves", 3.0, "cls", "Keeper >3 saves"),
        ("top_scorer_goals_reg", "goals", 0.0, "reg", "Top scorer goals (regression)"),
    ],
    "ligue1": [
        ("player_goal", "goals", 0.5, "cls", "Player scores a goal"),
        ("player_assist", "assists", 0.5, "cls", "Player records an assist"),
        ("player_shots_over_2", "shots", 2.0, "cls", "Player >2 shots"),
        ("player_sot_over_1", "shots_on_target", 1.0, "cls", "Player >1 shot on target"),
        ("player_yellow", "yellow_cards", 0.5, "cls", "Player gets yellow card"),
        ("goalie_saves_over_3", "saves", 3.0, "cls", "Keeper >3 saves"),
        ("top_scorer_goals_reg", "goals", 0.0, "reg", "Top scorer goals (regression)"),
    ],
    "mls": [
        ("player_goal", "goals", 0.5, "cls", "Player scores a goal"),
        ("player_assist", "assists", 0.5, "cls", "Player records an assist"),
        ("player_shots_over_2", "shots", 2.0, "cls", "Player >2 shots"),
        ("player_sot_over_1", "shots_on_target", 1.0, "cls", "Player >1 shot on target"),
        ("player_yellow", "yellow_cards", 0.5, "cls", "Player gets yellow card"),
        ("goalie_saves_over_3", "saves", 3.0, "cls", "Keeper >3 saves"),
        ("top_scorer_goals_reg", "goals", 0.0, "reg", "Top scorer goals (regression)"),
    ],
    "ucl": [
        ("player_goal", "goals", 0.5, "cls", "Player scores a goal"),
        ("player_assist", "assists", 0.5, "cls", "Player records an assist"),
        ("player_shots_over_2", "shots", 2.0, "cls", "Player >2 shots"),
        ("player_sot_over_1", "shots_on_target", 1.0, "cls", "Player >1 shot on target"),
        ("player_yellow", "yellow_cards", 0.5, "cls", "Player gets yellow card"),
        ("goalie_saves_over_3", "saves", 3.0, "cls", "Keeper >3 saves"),
        ("top_scorer_goals_reg", "goals", 0.0, "reg", "Top scorer goals (regression)"),
    ],
    # ── Esports ──
    "dota2": [
        ("kills_over_5", "kills", 5.0, "cls", "Player >5 kills"),
        ("kills_over_10", "kills", 10.0, "cls", "Player >10 kills"),
        ("deaths_over_5", "deaths", 5.0, "cls", "Player >5 deaths"),
        ("assists_over_10", "assists", 10.0, "cls", "Player >10 assists"),
        ("assists_over_15", "assists", 15.0, "cls", "Player >15 assists"),
        ("kda_over_3", "kda", 3.0, "cls", "Player KDA >3.0"),
        ("kda_over_5", "kda", 5.0, "cls", "Player KDA >5.0"),
        ("top_kills_reg", "kills", 0.0, "reg", "Top player kills (regression)"),
        ("top_assists_reg", "assists", 0.0, "reg", "Top player assists (regression)"),
    ],
    "lol": [
        ("kills_over_3", "kills", 3.0, "cls", "Player >3 kills"),
        ("kills_over_5", "kills", 5.0, "cls", "Player >5 kills"),
        ("deaths_over_3", "deaths", 3.0, "cls", "Player >3 deaths"),
        ("assists_over_5", "assists", 5.0, "cls", "Player >5 assists"),
        ("assists_over_10", "assists", 10.0, "cls", "Player >10 assists"),
        ("kda_over_3", "kda", 3.0, "cls", "Player KDA >3.0"),
        ("top_kills_reg", "kills", 0.0, "reg", "Top player kills (regression)"),
    ],
    "csgo": [
        ("kills_over_15", "kills", 15.0, "cls", "Player >15 kills"),
        ("kills_over_20", "kills", 20.0, "cls", "Player >20 kills"),
        ("deaths_over_15", "deaths", 15.0, "cls", "Player >15 deaths"),
        ("assists_over_3", "assists", 3.0, "cls", "Player >3 assists"),
        ("kda_over_1", "kda", 1.0, "cls", "Player KDA >1.0"),
        ("top_kills_reg", "kills", 0.0, "reg", "Top player kills (regression)"),
    ],
    "valorant": [
        ("kills_over_15", "kills", 15.0, "cls", "Player >15 kills"),
        ("kills_over_20", "kills", 20.0, "cls", "Player >20 kills"),
        ("deaths_over_12", "deaths", 12.0, "cls", "Player >12 deaths"),
        ("assists_over_5", "assists", 5.0, "cls", "Player >5 assists"),
        ("kda_over_1", "kda", 1.0, "cls", "Player KDA >1.0"),
        ("kda_over_2", "kda", 2.0, "cls", "Player KDA >2.0"),
        ("top_kills_reg", "kills", 0.0, "reg", "Top player kills (regression)"),
    ],
    # ── UFC ──
    "ufc": [
        ("strikes_over_50", "strikes_landed", 50.0, "cls", "Fighter >50 strikes landed"),
        ("strikes_over_100", "strikes_landed", 100.0, "cls", "Fighter >100 strikes landed"),
        ("sig_strikes_over_40", "sig_strikes_landed", 40.0, "cls", "Fighter >40 sig strikes"),
        ("knockdown", "knockdowns", 0.5, "cls", "Fighter scores a knockdown"),
        ("top_strikes_reg", "strikes_landed", 0.0, "reg", "Top striker strikes (regression)"),
    ],
    # ── Golf ── (uses min aggregation logic - position/score lower is better)
    # Note: golf scoring inverted — under_par means top_stat_value (max score_to_par
    # across players) is negative; top_10 means min(position) <= 10.
    # These rely on "max" aggregation which doesn't fit golf semantics well.
    # Only regression targets are safe here.
    "golf": [
        ("score_reg", "score_to_par", 0.0, "reg", "Player score to par (regression)"),
    ],
}


def _load_player_stats(sport: str, seasons: list[int]) -> pd.DataFrame | None:
    """Load and concatenate player stats for given seasons."""
    norm_dir = _DATA_DIR / "normalized" / sport
    if not norm_dir.exists():
        logger.warning("No normalized directory for %s", sport)
        return None

    frames: list[pd.DataFrame] = []
    for season in seasons:
        path = norm_dir / f"player_stats_{season}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            if len(df) > 0:
                frames.append(df)
                logger.info("  Loaded player stats: %s (%d rows)", path.name, len(df))
        else:
            logger.debug("  No player stats for %s season %d", sport, season)

    if not frames:
        # Try single combined file
        combined = norm_dir / "player_stats.parquet"
        if combined.exists():
            df = pd.read_parquet(combined)
            if len(df) > 0:
                frames.append(df)
                logger.info("  Loaded player stats: %s (%d rows)", combined.name, len(df))

    return pd.concat(frames, ignore_index=True) if frames else None


def _load_game_features(sport: str, seasons: list[int]) -> pd.DataFrame | None:
    """Load pre-computed game features."""
    features_dir = _DATA_DIR / "features"
    if not features_dir.exists():
        return None

    combined = features_dir / f"{sport}_all.parquet"
    if combined.exists():
        df = pd.read_parquet(combined)
        if len(df) > 0:
            logger.info("Loaded features: %s (%d rows)", combined.name, len(df))
            return df

    frames: list[pd.DataFrame] = []
    for season in seasons:
        path = features_dir / f"{sport}_{season}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            if len(df) > 0:
                frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else None


def _build_team_rolling_stats(
    player_stats: pd.DataFrame,
    stat_cols: list[str],
    window: int = 5,
) -> pd.DataFrame:
    """Compute team-level rolling mean of player stats (last N games).

    Groups player stats by (team_id, game_id) to get per-game team aggregates,
    then computes a rolling mean over the last `window` games per team.

    Returns a DataFrame indexed by (team_id, game_id) with rolling features.
    """
    required = {"game_id", "team_id", "date"}
    missing = required - set(player_stats.columns)
    if missing:
        logger.warning("Player stats missing columns: %s", missing)
        return pd.DataFrame()

    available_stats = [c for c in stat_cols if c in player_stats.columns]
    if not available_stats:
        return pd.DataFrame()

    # Convert numeric
    ps = player_stats.copy()
    for col in available_stats:
        ps[col] = pd.to_numeric(ps[col], errors="coerce")

    # Team per-game aggregates: sum all player stats for that team in that game
    agg_cols = {col: "sum" for col in available_stats}
    agg_cols["date"] = "first"
    team_game = (
        ps.groupby(["team_id", "game_id"])
        .agg(agg_cols)
        .reset_index()
    )

    # Sort by team + date for rolling
    team_game["date"] = pd.to_datetime(team_game["date"], errors="coerce")
    team_game = team_game.sort_values(["team_id", "date"])

    # Rolling mean over last N games (shift by 1 so we use prior games as features)
    rolling_frames: list[pd.DataFrame] = []
    for team_id, grp in team_game.groupby("team_id"):
        rolled = grp[available_stats].shift(1).rolling(window, min_periods=1).mean()
        rolled.columns = [f"roll_{c}" for c in available_stats]
        rolled["team_id"] = team_id
        rolled["game_id"] = grp["game_id"].values
        rolling_frames.append(rolled)

    if not rolling_frames:
        return pd.DataFrame()

    roll_df = pd.concat(rolling_frames, ignore_index=True)
    return roll_df.set_index(["team_id", "game_id"])


def _make_composite_stats(player_stats: pd.DataFrame, sport: str) -> pd.DataFrame:
    """Add composite/derived stat columns for specific prop targets."""
    ps = player_stats.copy()

    if sport == "nba":
        # pts + reb + ast
        for col in ["pts", "reb", "ast"]:
            if col in ps.columns:
                ps[col] = pd.to_numeric(ps[col], errors="coerce").fillna(0)
        if all(c in ps.columns for c in ["pts", "reb", "ast"]):
            ps["_pts_reb_ast"] = ps["pts"] + ps["reb"] + ps["ast"]
            # Double-double: >=10 in at least 2 of pts/reb/ast
            cats = [(ps["pts"] >= 10).astype(int), (ps["reb"] >= 10).astype(int), (ps["ast"] >= 10).astype(int)]
            ps["_double_double"] = (sum(cats) >= 2).astype(int)

    elif sport == "nhl":
        # goals + assists = points
        for col in ["goals", "assists"]:
            if col in ps.columns:
                ps[col] = pd.to_numeric(ps[col], errors="coerce").fillna(0)
        if all(c in ps.columns for c in ["goals", "assists"]):
            ps["_goals_assists"] = ps["goals"] + ps["assists"]

    return ps


def _build_prop_targets(
    player_stats: pd.DataFrame,
    game_features: pd.DataFrame,
    spec: tuple[str, str, float, str, str],
    sport: str,
) -> tuple[pd.DataFrame, pd.Series] | None:
    """Build (X, y) for a single prop target.

    Strategy:
    - For each game, get the top player stat (max across all players for that game)
      per team, then join with game features on game_id.
    - The target y is whether the threshold was exceeded in the CURRENT game.
    - Features X are the rolling team-level averages of the relevant stat
      (representing prior performance), plus the game features.
    """
    target_name, stat_col, threshold, model_type, _ = spec

    # Resolve actual stat column
    actual_col = stat_col if not stat_col.startswith("_") else stat_col
    if actual_col.startswith("_") and actual_col not in player_stats.columns:
        logger.debug("Composite col %s not in player stats — skipping %s", actual_col, target_name)
        return None

    if not actual_col.startswith("_") and actual_col not in player_stats.columns:
        logger.debug("Stat col %s not in player stats — skipping %s", actual_col, target_name)
        return None

    ps = player_stats.copy()
    ps[actual_col] = pd.to_numeric(ps[actual_col], errors="coerce").fillna(0)

    if "game_id" not in ps.columns or "game_id" not in game_features.columns:
        return None

    # For each game, aggregate per-game player stats.
    # For team-level sports (NHL goals/shots) use sum; for top-performer props use max.
    if "team_id" not in ps.columns:
        return None

    _TEAM_SUM_TARGETS = {"team_goals_over_3", "team_goals_over_4", "team_goals_over_6",
                         "team_shots_over_60", "team_shots_over_65",
                         "team_goals_reg", "total_runs_over_8", "total_hits_over_10"}
    agg_fn = "sum" if target_name in _TEAM_SUM_TARGETS else "max"
    top_per_game = (
        ps.groupby("game_id")[actual_col]
        .agg(agg_fn)
        .reset_index()
        .rename(columns={actual_col: "top_stat_value"})
    )

    # Join with game features on game_id
    gf = game_features.copy()
    if "game_id" not in gf.columns:
        return None

    merged = gf.merge(top_per_game, on="game_id", how="inner")
    if len(merged) < 50:
        logger.debug("Too few rows after merge for %s (%d)", target_name, len(merged))
        return None

    # Build target y
    if model_type == "cls":
        if target_name == "batter_hit":
            y = (merged["top_stat_value"] > 0).astype(int)
        elif target_name == "double_double":
            y = (merged["top_stat_value"] > 0.5).astype(int)
        else:
            y = (merged["top_stat_value"] > threshold).astype(int)
    else:
        y = merged["top_stat_value"].fillna(0)

    # Build feature matrix from game features (drop metadata + score cols)
    _META = {"game_id", "date", "home_team_id", "away_team_id", "home_score", "away_score",
              "home_q1", "home_q2", "home_q3", "home_q4", "home_ot",
              "away_q1", "away_q2", "away_q3", "away_q4", "away_ot", "season",
              "top_stat_value"}
    feat_cols = [c for c in merged.columns if c not in _META]
    X = merged[feat_cols].select_dtypes(include=[np.number]).fillna(0)

    if X.shape[1] == 0 or len(X) < 50:
        return None

    # Temporal sort for consistent train/test split
    if "date" in merged.columns:
        order = pd.to_datetime(merged["date"], errors="coerce").argsort()
        X = X.iloc[order].reset_index(drop=True)
        y = y.iloc[order].reset_index(drop=True)

    return X, y


def _fit_cls_safe(
    name: str,
    X_tr: pd.DataFrame,
    y_tr: pd.Series,
    X_va: pd.DataFrame,
    y_va: pd.Series,
    min_samples: int = 60,
) -> Any:
    """Fit a classifier using EnsembleVoter, or return None on failure."""
    try:
        from ml.models.ensemble import EnsembleVoter
    except ImportError:
        logger.error("Cannot import EnsembleVoter — is PYTHONPATH set to backend/?")
        return None

    if len(X_tr) < min_samples or len(X_va) < 15:
        logger.debug("Skipping %s — too few samples (%d/%d)", name, len(X_tr), len(X_va))
        return None
    if y_tr.nunique() < 2 or y_va.nunique() < 2:
        logger.debug("Skipping %s — only one class present", name)
        return None
    model = EnsembleVoter()
    model.fit_classifiers(X_tr, y_tr, X_va, y_va)
    return model


def _fit_reg_safe(
    name: str,
    X_tr: pd.DataFrame,
    y_tr: pd.Series,
    X_va: pd.DataFrame,
    y_va: pd.Series,
    min_samples: int = 60,
) -> Any:
    """Fit a regressor using EnsembleVoter, or return None on failure."""
    try:
        from ml.models.ensemble import EnsembleVoter
    except ImportError:
        logger.error("Cannot import EnsembleVoter")
        return None

    if len(X_tr) < min_samples or len(X_va) < 15:
        logger.debug("Skipping %s — too few samples", name)
        return None
    model = EnsembleVoter()
    model.fit_regressors(X_tr, y_tr, X_va, y_va)
    return model


def train_player_props(sport: str, seasons: list[int]) -> dict[str, Any]:
    """Train player prop models for the given sport and seasons.

    Returns
    -------
    Bundle dict with ``models`` (name → EnsembleVoter), ``feature_names``,
    ``sport``, ``trained_at``, and ``prop_specs``.
    """
    logger.info("=" * 60)
    logger.info("Player props training — sport=%s  seasons=%s", sport, seasons)
    logger.info("=" * 60)

    specs = _PROP_SPECS.get(sport)
    if not specs:
        raise ValueError(f"No prop specs defined for sport '{sport}'. "
                         f"Supported: {sorted(_PROP_SPECS)}")

    # Load data
    logger.info("Loading player stats …")
    player_stats = _load_player_stats(sport, seasons)
    if player_stats is None or len(player_stats) == 0:
        raise RuntimeError(f"No player stats found for {sport} seasons {seasons}")
    logger.info("Player stats loaded: %d rows", len(player_stats))

    logger.info("Loading game features …")
    game_features = _load_game_features(sport, seasons)
    if game_features is None or len(game_features) == 0:
        raise RuntimeError(f"No game features found for {sport}")
    logger.info("Game features loaded: %d rows", len(game_features))

    # Add composite stats
    player_stats = _make_composite_stats(player_stats, sport)

    models: dict[str, Any] = {}
    feature_names: list[str] = []
    trained_specs: list[str] = []

    for spec in specs:
        target_name, stat_col, threshold, model_type, description = spec
        logger.info("Training prop: %s (%s) …", target_name, description)

        result = _build_prop_targets(player_stats, game_features, spec, sport)
        if result is None:
            logger.warning("  Skipping %s — could not build targets", target_name)
            continue

        X, y = result
        n = len(X)
        split_idx = int(n * 0.80)
        if split_idx < 60 or (n - split_idx) < 15:
            logger.warning("  Skipping %s — not enough data (%d rows)", target_name, n)
            continue

        X_tr, X_va = X.iloc[:split_idx], X.iloc[split_idx:]
        y_tr, y_va = y.iloc[:split_idx], y.iloc[split_idx:]

        if model_type == "cls":
            m = _fit_cls_safe(target_name, X_tr, y_tr, X_va, y_va)
        else:
            m = _fit_reg_safe(target_name, X_tr, y_tr, X_va, y_va)

        if m is not None:
            models[target_name] = m
            if not feature_names:
                feature_names = list(X_tr.columns)
            trained_specs.append(target_name)
            if model_type == "cls":
                rate = float(y_tr.mean())
                logger.info(
                    "  ✓ %s fitted  (n_train=%d  n_val=%d  base_rate=%.1f%%)",
                    target_name, len(X_tr), len(X_va), 100 * rate,
                )
            else:
                logger.info(
                    "  ✓ %s fitted  (n_train=%d  n_val=%d  mean_target=%.2f)",
                    target_name, len(X_tr), len(X_va), float(y_tr.mean()),
                )
        else:
            logger.warning("  ✗ %s — model training failed or skipped", target_name)

    if not models:
        raise RuntimeError(f"No player prop models could be trained for {sport}")

    logger.info("Player props training complete — %d models: %s", len(models), list(models.keys()))

    return {
        "models": models,
        "feature_names": feature_names,
        "sport": sport,
        "seasons": seasons,
        "prop_specs": trained_specs,
        "trained_at": datetime.utcnow().isoformat(),
    }


def save_models(bundle: dict[str, Any], sport: str) -> Path:
    """Save trained player prop models to disk."""
    models_dir = _MODELS_ROOT / sport
    models_dir.mkdir(parents=True, exist_ok=True)
    path = models_dir / "player_props.pkl"
    with open(path, "wb") as fh:
        pickle.dump(bundle, fh, protocol=pickle.HIGHEST_PROTOCOL)
    size_mb = path.stat().st_size / (1024 * 1024)
    logger.info("Player prop models saved to %s (%.1f MB)", path, size_mb)
    return path


def load_player_props(sport: str) -> dict[str, Any] | None:
    """Load player prop models for a sport, returning None if not found."""
    path = _MODELS_ROOT / sport / "player_props.pkl"
    if not path.exists():
        return None
    with open(path, "rb") as fh:
        return pickle.load(fh)  # noqa: S301


# ── CLI ──────────────────────────────────────────────────


def _parse_seasons(raw: str) -> list[int]:
    return [int(s.strip()) for s in raw.split(",")]


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="train_player_props",
        description="V5.0 Player Props ML Training",
    )
    parser.add_argument(
        "--sport",
        required=True,
        choices=sorted(_PROP_SPECS.keys()),
        help="Sport key (nba, nfl, mlb, nhl)",
    )
    parser.add_argument(
        "--seasons",
        required=True,
        help="Comma-separated seasons, e.g. 2022,2023,2024",
    )
    args = parser.parse_args(argv)

    sport = args.sport.lower()
    seasons = _parse_seasons(args.seasons)

    bundle = train_player_props(sport, seasons)
    save_models(bundle, sport)
    logger.info(
        "Done — trained %d player prop models for %s",
        len(bundle["models"]),
        sport,
    )


if __name__ == "__main__":
    main()
