#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────
# V5.0 Backend — Player Props ML Training Module
# ──────────────────────────────────────────────────────────
"""
Train INDIVIDUAL player prop prediction models for all sports.

Architecture (player-centric, separate from game prediction):
  - Per-player rolling features (last 5/10 games): form, consistency, streak
  - Opponent defensive context: how well the opponent suppresses this stat
  - Usage/role context: minutes, position, starter status
  - Home/away splits, rest days
  - Game-level context features appended from pre-computed game features

This is intentionally SEPARATE from game prediction because:
  - Game models predict team-level outcomes (win/loss, total, spread)
  - Player models predict individual performance vs a threshold
  - Feature selection differs: player form matters far more than team momentum
    for prop prediction; meanwhile game prediction cares about team matchup depth

Models saved to: ml/models/{sport}/player_props.pkl

Usage
-----
::

    python3 backend/ml/train_player_props.py --sport nba --seasons 2022,2023,2024,2025
    python3 backend/ml/train_player_props.py --sport nfl --seasons 2022,2023,2024,2025
    python3 backend/ml/train_player_props.py --sport mlb --seasons 2022,2023,2024
    python3 backend/ml/train_player_props.py --sport nhl --seasons 2022,2023,2024,2025
    python3 backend/ml/train_player_props.py --sport epl  --seasons 2022,2023,2024,2025
    python3 backend/ml/train_player_props.py --sport ufc  --seasons 2022,2023,2024,2025
    python3 backend/ml/train_player_props.py --all
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
    """Load and concatenate player stats for given seasons via DuckDB curated reader."""
    from features.data_reader import get_reader
    reader = get_reader(_DATA_DIR)
    frames: list[pd.DataFrame] = []
    for season in seasons:
        try:
            df = reader.load(sport, "player_stats", season=season)
            if len(df) > 0:
                frames.append(df)
                logger.info("  Loaded player_stats %s/%d: %d rows", sport, season, len(df))
        except Exception as e:
            logger.debug("  No player stats for %s season %d: %s", sport, season, e)

    if not frames:
        # Fallback: load all seasons at once
        try:
            df = reader.load_all_seasons(sport, "player_stats")
            if len(df) > 0:
                frames.append(df)
                logger.info("  Loaded player_stats %s (all seasons): %d rows", sport, len(df))
        except Exception:
            pass

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
    """LEGACY: Compute team-level rolling mean of player stats (last N games).
    Kept as a fallback path; primary per-player features use _build_player_rolling_features.
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

    ps = player_stats.copy()
    for col in available_stats:
        ps[col] = pd.to_numeric(ps[col], errors="coerce")

    agg_cols: dict[str, Any] = {col: "sum" for col in available_stats}
    agg_cols["date"] = "first"
    team_game = (
        ps.groupby(["team_id", "game_id"])
        .agg(agg_cols)
        .reset_index()
    )

    team_game["date"] = pd.to_datetime(team_game["date"], errors="coerce")
    team_game = team_game.sort_values(["team_id", "date"])

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


def _build_player_rolling_features(
    player_stats: pd.DataFrame,
    stat_cols: list[str],
    window_short: int = 5,
    window_long: int = 15,
) -> pd.DataFrame:
    """Build per-player, per-game rolling feature vectors.

    For each (player, game) pair, computes:
      - Rolling mean/std/max over last 5 and 15 games (prior to this game)
      - Short-window vs long-window trend (form vs baseline)
      - Consistency score (1 / (1 + std))
      - Streak: consecutive games at or above player's median
      - Games played count (sample size indicator)
      - Rolling average minutes and starter rate
      - Days rest since last game

    Returns DataFrame with one row per (game_id, player_id, team_id).
    """
    required = {"game_id", "player_id", "team_id", "date"}
    if not required.issubset(player_stats.columns):
        logger.warning("player_stats missing required cols: %s", required - set(player_stats.columns))
        return pd.DataFrame()

    ps = player_stats.copy()
    ps["date"] = pd.to_datetime(ps["date"], errors="coerce")
    ps = ps.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    available = [c for c in stat_cols if c in ps.columns]
    if not available:
        return pd.DataFrame()

    for col in available:
        ps[col] = pd.to_numeric(ps[col], errors="coerce").fillna(0.0)

    ps["minutes"] = pd.to_numeric(ps.get("minutes", ps.get("min", 0)), errors="coerce").fillna(0.0)
    ps["starter"] = pd.to_numeric(ps.get("starter", 0), errors="coerce").fillna(0.0)

    records = []
    for player_id, pgrp in ps.groupby("player_id", sort=False):
        pgrp = pgrp.sort_values("date").reset_index(drop=True)
        n = len(pgrp)

        for i in range(n):
            row_data = pgrp.iloc[i]
            hist_short = pgrp.iloc[max(0, i - window_short):i]
            hist_long  = pgrp.iloc[max(0, i - window_long):i]

            feat: dict[str, Any] = {
                "game_id":        row_data["game_id"],
                "player_id":      player_id,
                "team_id":        row_data["team_id"],
                "player_name":    row_data.get("player_name", ""),
                "p_games_played": i,
                "p_minutes_avg":  float(hist_short["minutes"].mean()) if len(hist_short) > 0 else 0.0,
                "p_starter_rate": float(hist_short["starter"].mean()) if len(hist_short) > 0 else 0.0,
                "p_rest_days":    float(min((pgrp["date"].iloc[i] - pgrp["date"].iloc[i - 1]).days, 14)) if i > 0 else 7.0,
            }

            for col in available:
                s_vals = hist_short[col].values if len(hist_short) > 0 else np.array([], dtype=float)
                l_vals = hist_long[col].values  if len(hist_long)  > 0 else np.array([], dtype=float)

                s_mean = float(np.mean(s_vals)) if len(s_vals) > 0 else 0.0
                l_mean = float(np.mean(l_vals)) if len(l_vals) > 0 else 0.0
                s_std  = float(np.std(s_vals))  if len(s_vals) > 1 else 0.0

                feat[f"p_{col}_avg5"]        = s_mean
                feat[f"p_{col}_avg15"]       = l_mean
                feat[f"p_{col}_max5"]        = float(np.max(s_vals)) if len(s_vals) > 0 else 0.0
                feat[f"p_{col}_std5"]        = s_std
                feat[f"p_{col}_trend"]       = s_mean - l_mean
                feat[f"p_{col}_consistency"] = 1.0 / (1.0 + s_std)

                # Hot-streak: consecutive games at/above player's long-term median
                if len(l_vals) >= 3:
                    thresh = float(np.median(l_vals))
                    streak = 0
                    for v in reversed(s_vals.tolist()):
                        if v >= thresh:
                            streak += 1
                        else:
                            break
                    feat[f"p_{col}_streak"] = float(streak)
                else:
                    feat[f"p_{col}_streak"] = 0.0

            records.append(feat)

    return pd.DataFrame(records) if records else pd.DataFrame()


def _build_opponent_defensive_features(
    player_stats: pd.DataFrame,
    games_df: pd.DataFrame,
    stat_cols: list[str],
    window: int = 10,
) -> pd.DataFrame:
    """Build opponent-defensive matchup features per (game_id, team_id).

    For each team in each game, computes how many of the target stats the
    opponent has allowed in their last N games (from all attackers).
    """
    if games_df.empty or player_stats.empty:
        return pd.DataFrame()

    required_g = {"id", "date", "home_team_id", "away_team_id"}
    if not required_g.issubset(games_df.columns):
        return pd.DataFrame()

    ps = player_stats.copy()
    ps["date"] = pd.to_datetime(ps["date"], errors="coerce")
    ps = ps.dropna(subset=["date"]).sort_values("date")
    available = [c for c in stat_cols if c in ps.columns]
    if not available:
        return pd.DataFrame()
    for col in available:
        ps[col] = pd.to_numeric(ps[col], errors="coerce").fillna(0.0)

    gdf = games_df[["id", "date", "home_team_id", "away_team_id"]].copy()
    gdf["date"] = pd.to_datetime(gdf["date"], errors="coerce")
    gdf = gdf.dropna(subset=["date"]).rename(columns={"id": "game_id"})
    gdf["home_team_id"] = gdf["home_team_id"].astype(str)
    gdf["away_team_id"] = gdf["away_team_id"].astype(str)
    ps["team_id"] = ps["team_id"].astype(str)

    gdf_map = gdf.set_index("game_id")[["home_team_id", "away_team_id", "date"]].to_dict("index")

    records = []
    for game_id, info in gdf_map.items():
        g_date = info["date"]
        for batting_team, opp_team in [
            (info["home_team_id"], info["away_team_id"]),
            (info["away_team_id"], info["home_team_id"]),
        ]:
            opp_past = gdf[
                ((gdf["home_team_id"] == opp_team) | (gdf["away_team_id"] == opp_team)) &
                (gdf["date"] < g_date)
            ].tail(window)["game_id"].tolist()

            if not opp_past:
                continue

            allowed = ps[(ps["game_id"].isin(opp_past)) & (ps["team_id"] != opp_team)]
            row: dict[str, Any] = {"game_id": game_id, "team_id": batting_team}
            for col in available:
                vals = allowed[col].values
                row[f"opp_def_{col}_pg"]  = float(np.mean(vals)) if len(vals) > 0 else 0.0
                row[f"opp_def_{col}_max"] = float(np.max(vals))  if len(vals) > 0 else 0.0
            records.append(row)

    return pd.DataFrame(records) if records else pd.DataFrame()


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
    """Build (X, y) for a single prop target using per-player rolling features.

    Strategy (PER-PLAYER design):
    1. Add composite stat columns if needed.
    2. Build per-player rolling feature vectors (form, consistency, streak, rest).
    3. Build opponent-defensive matchup features per (game_id, team_id).
    4. Append lightweight game-context features (home/away, elo, pace, rest).
    5. Target y = did this player exceed threshold in THIS game?

    This is intentionally different from the game-prediction pipeline.
    Game models care about team matchup depth; player models care about
    individual form, role, and opponent defensive weakness at their position.
    """
    target_name, stat_col, threshold, model_type, _ = spec
    actual_col = stat_col  # composite cols are already built by _make_composite_stats

    if actual_col not in player_stats.columns:
        logger.debug("Stat col %s not in player stats — skipping %s", actual_col, target_name)
        return None

    ps = player_stats.copy()
    ps[actual_col] = pd.to_numeric(ps[actual_col], errors="coerce").fillna(0.0)

    if "game_id" not in ps.columns or "team_id" not in ps.columns:
        return None

    # ── 1. Per-player rolling features ───────────────────────────────
    # Use all numeric stat cols from this sport to build rolling features
    # (not just the target stat — correlated stats add signal)
    _SPORT_STAT_COLS: dict[str, list[str]] = {
        "nba":  ["pts", "reb", "ast", "stl", "blk", "to", "minutes", "fga", "three_m", "plus_minus",
                 "usg_pct", "ts_pct", "_pts_reb_ast", "_double_double"],
        "nfl":  ["pass_yds", "pass_td", "rush_yds", "rush_td", "rec_yds", "rec_td", "receptions",
                 "targets", "pass_rating", "sacks", "tackles"],
        "mlb":  ["hits", "hr", "rbi", "runs", "sb", "bb", "so", "avg",
                 "strikeouts", "innings", "era", "whip", "total_bases"],
        "nhl":  ["goals", "assists", "shots", "saves", "toi", "hits", "plus_minus",
                 "blocked_shots", "_goals_assists", "pp_goals"],
        "epl":  ["goals", "assists", "shots", "shots_on_target", "fouls_committed", "saves",
                 "goals_conceded", "yellow_cards"],
        "ufc":  ["strikes_landed", "strikes_attempted", "sig_strikes", "knockdowns"],
        "bundesliga": ["goals", "assists", "shots", "shots_on_target", "saves", "goals_conceded"],
        "laliga": ["goals", "assists", "shots", "shots_on_target", "saves", "goals_conceded"],
        "ligue1": ["goals", "assists", "shots", "shots_on_target", "saves", "goals_conceded"],
        "mls":  ["goals", "assists", "shots", "shots_on_target", "saves", "goals_conceded"],
    }
    stat_cols_for_sport = _SPORT_STAT_COLS.get(sport, [actual_col])
    stat_cols_for_sport = [c for c in stat_cols_for_sport if c in ps.columns]
    if actual_col not in stat_cols_for_sport:
        stat_cols_for_sport = [actual_col] + stat_cols_for_sport

    player_feats = _build_player_rolling_features(ps, stat_cols_for_sport)
    if player_feats.empty:
        logger.debug("  Could not build player rolling features for %s", target_name)
        return None

    # ── 2. Target: did THIS player exceed the threshold in THIS game? ──
    actual_values = ps[["game_id", "player_id", actual_col]].copy()
    actual_values = actual_values.rename(columns={actual_col: "_actual"})
    player_feats = player_feats.merge(actual_values, on=["game_id", "player_id"], how="inner")

    if model_type == "cls":
        if target_name == "batter_hit":
            y = (player_feats["_actual"] > 0).astype(int)
        elif "double_double" in target_name:
            y = (player_feats["_actual"] > 0.5).astype(int)
        else:
            y = (player_feats["_actual"] > threshold).astype(int)
    else:
        y = player_feats["_actual"].fillna(0.0)

    # ── 3. Game-context features (lightweight — home/away + key pace/rating) ──
    _GAME_CONTEXT_COLS = [
        "is_home", "days_rest",
        "home_pace", "away_pace", "home_off_rating", "away_def_rating",
        "home_elo", "away_elo", "elo_diff",
        "home_l5_win_pct", "away_l5_win_pct",
        "home_season_pts_pg", "away_season_pts_pg",
    ]
    if game_features is not None and "game_id" in game_features.columns:
        ctx_cols = [c for c in _GAME_CONTEXT_COLS if c in game_features.columns]
        if ctx_cols:
            game_ctx = game_features[["game_id"] + ctx_cols].copy()
            # Each player row is from one team; join on game_id
            player_feats = player_feats.merge(game_ctx, on="game_id", how="left")

    # ── 4. Opponent-defensive features (optional — only if games available) ──
    if game_features is not None and not game_features.empty:
        games_with_id = game_features.copy()
        if "game_id" in games_with_id.columns and "id" not in games_with_id.columns:
            games_with_id = games_with_id.rename(columns={"game_id": "id"})
        if {"id", "home_team_id", "away_team_id", "date"}.issubset(games_with_id.columns):
            opp_feats = _build_opponent_defensive_features(
                ps, games_with_id, [actual_col], window=10
            )
            if not opp_feats.empty:
                player_feats = player_feats.merge(
                    opp_feats, on=["game_id", "team_id"], how="left"
                )

    # ── 5. Drop non-numeric / metadata columns ────────────────────────
    _META = {"game_id", "player_id", "team_id", "player_name", "_actual", "date",
             "home_team_id", "away_team_id", "season"}
    feat_cols = [c for c in player_feats.columns if c not in _META]
    X = player_feats[feat_cols].select_dtypes(include=[np.number]).fillna(0.0)

    if X.shape[1] == 0 or len(X) < 50:
        logger.debug("  Too few features/rows for %s", target_name)
        return None

    # Filter out rows with zero history (cold-start) to avoid noise
    if "p_games_played" in X.columns:
        mask = X["p_games_played"] >= 3
        X = X[mask].reset_index(drop=True)
        y = y[mask].reset_index(drop=True)

    if len(X) < 50:
        return None

    # Temporal sort (player_feats was built in order but confirm)
    if "date" in player_feats.columns:
        date_col = pd.to_datetime(player_feats.loc[X.index, "date"] if "date" in player_feats.columns else pd.Series(dtype="datetime64[ns]"), errors="coerce")
        if not date_col.isna().all():
            order = date_col.argsort()
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
