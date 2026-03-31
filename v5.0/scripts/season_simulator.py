#!/usr/bin/env python3
"""
V5.0 Season Simulator — Monte Carlo season projection engine.

Supports 10+ sports with Bradley-Terry game simulation, Pythagorean win
expectation, multi-award modeling, bracket simulation (including March
Madness 68-team and CFP 12-team), draft lottery, relegation, and more.

Usage:
    python scripts/season_simulator.py --sport all --simulations 10000
    python scripts/season_simulator.py --sport nba --simulations 5000
    python scripts/season_simulator.py --sport ncaab --simulations 1000
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import exp, log
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Path setup — allow importing v5.0 backend modules
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

from backend.config import (
    SPORT_DEFINITIONS,
    SPORT_SEASON_START,
    get_current_season,
    get_settings,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("season_sim")

DATA_DIR = PROJECT_ROOT / "data" / "normalized"
SIM_OUT = PROJECT_ROOT / "data" / "simulations"

# ═══════════════════════════════════════════════════════════════════════════
# Data-classes
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Team:
    name: str
    team_id: str
    conference: str
    division: str
    rating: float
    abbr: str = ""
    wins: int = 0
    losses: int = 0
    ties: int = 0
    otl: int = 0
    games_played: int = 0
    points_for: float = 0.0
    points_against: float = 0.0
    pyth_pct: float = 0.5
    recent_form: float = 0.5


@dataclass
class AwardCandidate:
    name: str
    team: str
    weight: float  # relative probability weight


@dataclass
class SportContext:
    sport: str
    season: str
    games_per_team: int
    playoff_slots_per_conf: int
    has_play_in: bool = False
    has_draft_lottery: bool = False
    remaining_games: list[tuple[str, str, str]] = field(default_factory=list)
    completed_wins: dict[str, int] = field(default_factory=dict)
    award_candidates: dict[str, list[AwardCandidate]] = field(default_factory=dict)
    teams: list[Team] = field(default_factory=list)
    conferences: list[str] = field(default_factory=list)
    divisions: dict[str, str] = field(default_factory=dict)  # team_name -> div
    season_completion_pct: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

SPORT_CONFIG = {
    # ── North American Team Sports ──────────────────────────────────────
    "nba":        {"games": 82,  "playoff_slots": 8,  "play_in": True,  "lottery": True,
                   "pyth_exp": 13.91, "home_adv": 1.08,
                   "awards": ["mvp","roty","dpoy","6moy","mip","scoring_title","assists_title","rebounds_title"],
                   "ot_rate": 0.07, "draw_rate": 0.0,
                   "player_stat_leaders": ["pts","ast","reb","stl","blk","three_m"],
                   "game_pred_types": ["winner","spread","total","ot","first_basket","period_winner","halftime_winner","second_half_winner","dominant_win","comeback","team_totals","regulation_winner","margin_band"]},
    "nfl":        {"games": 17,  "playoff_slots": 7,  "play_in": False, "lottery": False,
                   "pyth_exp": 2.37, "home_adv": 1.06,
                   "awards": ["mvp","oroy","droy","opoy","dpoy","passing_title","rushing_title","receiving_title"],
                   "ot_rate": 0.10, "draw_rate": 0.01,
                   "player_stat_leaders": ["pass_yds","pass_td","rush_yds","rush_td","rec_yds","rec_td","sacks"],
                   "game_pred_types": ["winner","spread","total","ot","first_td","halftime_winner","period_winner","second_half_winner","dominant_win","comeback","team_totals","regulation_winner","margin_band","draw"]},
    "nhl":        {"games": 82,  "playoff_slots": 8,  "play_in": False, "lottery": True,
                   "pyth_exp": 2.0, "home_adv": 1.05,
                   "awards": ["hart","vezina","norris","calder","points_leader","goals_leader","assists_leader"],
                   "ot_rate": 0.24, "draw_rate": 0.0,
                   "player_stat_leaders": ["goals","assists","points","shots","saves","plus_minus"],
                   "game_pred_types": ["winner","spread","total","ot","btts","first_goal","period_winner","regulation_winner","regulation_draw_ot","second_half_winner","dominant_win","team_totals","margin_band"]},
    "mlb":        {"games": 162, "playoff_slots": 6,  "play_in": False, "lottery": False,
                   "pyth_exp": 2.0, "home_adv": 1.04,
                   "awards": ["mvp_al","mvp_nl","cy_young_al","cy_young_nl","roy","batting_title","hr_leader","rbi_leader","era_leader"],
                   "ot_rate": 0.08, "draw_rate": 0.0,
                   "player_stat_leaders": ["hits","hr","rbi","sb","avg","era","strikeouts","saves"],
                   "game_pred_types": ["winner","spread","total","btts","first_run","inning_winner","clean_sheet_home","clean_sheet_away","dominant_win","team_totals","margin_band","total_band"]},
    "ncaab":      {"games": 30,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 11.5, "home_adv": 1.10,
                   "awards": ["scoring_title","assists_title"],
                   "ot_rate": 0.06, "draw_rate": 0.0,
                   "player_stat_leaders": ["pts","ast","reb"],
                   "game_pred_types": ["winner","spread","total","ot","halftime_winner","first_basket","second_half_winner","dominant_win","comeback","period_winner","team_totals"]},
    "ncaaf":      {"games": 12,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 2.7, "home_adv": 1.07,
                   "awards": ["heisman"],
                   "ot_rate": 0.09, "draw_rate": 0.0,
                   "player_stat_leaders": ["pass_yds","rush_yds","pass_td","rush_td"],
                   "game_pred_types": ["winner","spread","total","ot","halftime_winner","first_td","second_half_winner","dominant_win","comeback","period_winner","team_totals","margin_band"]},
    "wnba":       {"games": 40,  "playoff_slots": 8,  "play_in": False, "lottery": True,
                   "pyth_exp": 13.91, "home_adv": 1.06,
                   "awards": ["mvp","roty","scoring_title"],
                   "ot_rate": 0.06, "draw_rate": 0.0,
                   "player_stat_leaders": ["pts","ast","reb"],
                   "game_pred_types": ["winner","spread","total","ot","halftime_winner","period_winner","second_half_winner","dominant_win","comeback"]},
    "ncaaw":      {"games": 30,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 11.5, "home_adv": 1.10,
                   "awards": ["scoring_title","assists_title","player_of_year"],
                   "ot_rate": 0.05, "draw_rate": 0.001,
                   "player_stat_leaders": ["pts","ast","reb","stl","blk"],
                   "game_pred_types": ["winner","spread","total","ot","halftime_winner","period_winner","second_half_winner","dominant_win","comeback","first_basket"]},
    # ── Soccer ──────────────────────────────────────────────────────────
    "epl":        {"games": 38,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.4, "home_adv": 1.10,
                   "awards": ["golden_boot","golden_glove","player_of_season"],
                   "ot_rate": 0.0, "draw_rate": 0.26,
                   "player_stat_leaders": ["goals","assists","xg"],
                   "game_pred_types": ["winner","draw","three_way","btts","clean_sheet_home","clean_sheet_away","halftime_winner","double_chance","total_goals","both_score_first_half","dominant_win","total_band","margin_band"]},
    "laliga":     {"games": 38,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.4, "home_adv": 1.10,
                   "awards": ["golden_boot","player_of_season"],
                   "ot_rate": 0.0, "draw_rate": 0.25,
                   "player_stat_leaders": ["goals","assists","xg"],
                   "game_pred_types": ["winner","draw","three_way","btts","clean_sheet_home","clean_sheet_away","halftime_winner","double_chance","total_goals","dominant_win","total_band","margin_band"]},
    "bundesliga": {"games": 34,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.4, "home_adv": 1.08,
                   "awards": ["golden_boot","player_of_season"],
                   "ot_rate": 0.0, "draw_rate": 0.24,
                   "player_stat_leaders": ["goals","assists","xg"],
                   "game_pred_types": ["winner","draw","three_way","btts","clean_sheet_home","clean_sheet_away","halftime_winner","double_chance","total_goals","dominant_win","total_band","margin_band"]},
    "ligue1":     {"games": 38,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.4, "home_adv": 1.10,
                   "awards": ["golden_boot","player_of_season"],
                   "ot_rate": 0.0, "draw_rate": 0.25,
                   "player_stat_leaders": ["goals","assists","xg"],
                   "game_pred_types": ["winner","draw","three_way","btts","clean_sheet_home","clean_sheet_away","halftime_winner","double_chance","total_goals","dominant_win","total_band","margin_band"]},
    "seriea":     {"games": 38,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.4, "home_adv": 1.09,
                   "awards": ["golden_boot","player_of_season"],
                   "ot_rate": 0.0, "draw_rate": 0.25,
                   "player_stat_leaders": ["goals","assists","xg"],
                   "game_pred_types": ["winner","draw","three_way","btts","clean_sheet_home","clean_sheet_away","halftime_winner","double_chance","total_goals","dominant_win","total_band","margin_band"]},
    "ucl":        {"games": 8,   "playoff_slots": 16, "play_in": False, "lottery": False,
                   "pyth_exp": 1.4, "home_adv": 1.05,
                   "awards": ["golden_boot","player_of_tournament"],
                   "ot_rate": 0.12, "draw_rate": 0.27,
                   "player_stat_leaders": ["goals","assists"],
                   "game_pred_types": ["winner","draw","three_way","btts","clean_sheet_home","clean_sheet_away","ot","total_goals","dominant_win","total_band","margin_band"]},
    "mls":        {"games": 34,  "playoff_slots": 9,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.4, "home_adv": 1.12,
                   "awards": ["mvp","golden_boot","best_goalkeeper"],
                   "ot_rate": 0.04, "draw_rate": 0.22,
                   "player_stat_leaders": ["goals","assists","xg"],
                   "game_pred_types": ["winner","draw","three_way","btts","clean_sheet_home","clean_sheet_away","halftime_winner","double_chance","total_goals","dominant_win","total_band","margin_band"]},
    "nwsl":       {"games": 26,  "playoff_slots": 8,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.4, "home_adv": 1.08,
                   "awards": ["mvp","golden_boot"],
                   "ot_rate": 0.05, "draw_rate": 0.24,
                   "player_stat_leaders": ["goals","assists"],
                   "game_pred_types": ["winner","draw","three_way","btts","clean_sheet_home","clean_sheet_away","total_goals","dominant_win","total_band","margin_band"]},
    # ── Combat Sports ───────────────────────────────────────────────────
    "ufc":        {"games": 12,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.0, "home_adv": 1.0,
                   "awards": ["fighter_of_year","ko_of_year","submission_of_year","performance_of_year"],
                   "ot_rate": 0.0, "draw_rate": 0.01,
                   "player_stat_leaders": ["knockdowns","sig_strikes","takedowns","sub_attempts"],
                   "game_pred_types": ["winner","method_of_victory","ko_tko","submission","decision","round_stoppage","finish_prob"]},
    # ── Tennis ──────────────────────────────────────────────────────────
    "atp":        {"games": 80,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.0, "home_adv": 1.0,
                   "awards": ["year_end_no1","slam_titles","masters_titles"],
                   "ot_rate": 0.0, "draw_rate": 0.0,
                   "player_stat_leaders": ["aces","winners","sets_won","games_won"],
                   "game_pred_types": ["winner","straight_sets","total_sets","break_point_conversion","ace_over_under"]},
    "wta":        {"games": 60,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.0, "home_adv": 1.0,
                   "awards": ["year_end_no1","slam_titles"],
                   "ot_rate": 0.0, "draw_rate": 0.0,
                   "player_stat_leaders": ["aces","winners","sets_won","games_won"],
                   "game_pred_types": ["winner","straight_sets","total_sets","break_point_conversion","ace_over_under"]},
    # ── Motor Racing ────────────────────────────────────────────────────
    "f1":         {"games": 24,  "playoff_slots": 0,  "play_in": False, "lottery": False,
                   "pyth_exp": 1.0, "home_adv": 1.0,
                   "awards": ["wdc","wcc"],
                   "ot_rate": 0.0, "draw_rate": 0.0,
                   "player_stat_leaders": [],
                   "game_pred_types": ["winner","podium","points_finish"]},
}

SIMULATABLE_SPORTS = list(SPORT_CONFIG.keys())

NBA_LOTTERY_ODDS = [14.0, 14.0, 14.0, 12.5, 10.5, 9.0, 7.5, 6.0, 4.5, 3.0, 2.0, 1.5, 1.0, 0.5]

NHL_LOTTERY_ODDS = [18.5, 13.5, 11.5, 9.5, 8.5, 7.5, 6.5, 6.0, 5.0, 3.5, 3.0, 2.5, 2.0, 1.5, 1.0]

ROUND_LABELS = {
    "nba":        ["play_in", "first_round", "conf_semis", "conf_finals", "finals"],
    "nfl":        ["wild_card", "divisional", "conf_championship", "super_bowl"],
    "nhl":        ["first_round", "second_round", "conf_finals", "stanley_cup_final"],
    "mlb":        ["wild_card", "division_series", "championship_series", "world_series"],
    "mls":        ["round_one", "conf_semis", "conf_finals", "mls_cup"],
    "nwsl":       ["round_one", "conf_semis", "conf_finals", "championship"],
    "ucl":        ["round_of_16", "quarter_finals", "semi_finals", "final"],
    "wnba":       ["first_round", "semis", "finals"],
}

# Stat label → human readable (for output)
STAT_LABELS: dict[str, str] = {
    # NBA / NCAAB
    "pts":          "Points Per Game",
    "reb":          "Rebounds Per Game",
    "ast":          "Assists Per Game",
    "stl":          "Steals Per Game",
    "blk":          "Blocks Per Game",
    "three_m":      "3-Pointers Made Per Game",
    # NFL
    "pass_yds":     "Passing Yards",
    "pass_td":      "Passing Touchdowns",
    "rush_yds":     "Rushing Yards",
    "rush_td":      "Rushing Touchdowns",
    "rec_yds":      "Receiving Yards",
    "rec_td":       "Receiving Touchdowns",
    "sacks":        "Sacks",
    # NHL
    "goals":        "Goals",
    "assists":      "Assists",
    "points":       "Points",
    "shots":        "Shots",
    "saves":        "Saves",
    "plus_minus":   "Plus/Minus",
    # MLB
    "hits":         "Hits",
    "hr":           "Home Runs",
    "rbi":          "RBI",
    "sb":           "Stolen Bases",
    "avg":          "Batting Average",
    "era":          "ERA",
    "strikeouts":   "Strikeouts",
    # Soccer
    "xg":           "Expected Goals",
    # Tennis
    "aces":         "Aces",
    "winners":      "Winners",
    "sets_won":     "Sets Won",
    "games_won":    "Games Won",
    # UFC
    "knockdowns":   "Knockdowns",
    "sig_strikes":  "Significant Strikes",
    "takedowns":    "Takedowns",
    "sub_attempts": "Submission Attempts",
}

# ═══════════════════════════════════════════════════════════════════════════
# Data Loading
# ═══════════════════════════════════════════════════════════════════════════

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        v = float(val)
        return v if np.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _load_parquet(sport: str, kind: str, season: str) -> pd.DataFrame:
    """Load a parquet file, trying season-specific first, then generic."""
    path_season = DATA_DIR / sport / f"{kind}_{season}.parquet"
    path_generic = DATA_DIR / sport / f"{kind}.parquet"
    for p in (path_season, path_generic):
        if p.exists():
            try:
                df = pd.read_parquet(p)
                if not df.empty and "season" in df.columns:
                    df_filt = df[df["season"].astype(str) == str(season)]
                    if not df_filt.empty:
                        return df_filt
                return df
            except Exception as e:
                log.warning("Failed to read %s: %s", p, e)
    return pd.DataFrame()


def _load_parquet_raw(sport: str, kind: str) -> pd.DataFrame:
    """Load generic (non-season-specific) parquet, all data."""
    path = DATA_DIR / sport / f"{kind}.parquet"
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            pass
    return pd.DataFrame()


def _pyth_pct(pf: float, pa: float, exp_: float) -> float:
    if pf <= 0 or pa <= 0:
        return 0.5
    return pf ** exp_ / (pf ** exp_ + pa ** exp_)


def _rating_from_blend(pyth: float, win_pct: float, recent: float,
                       gp: int, total_games: int, sport: str) -> float:
    """Produce a 1-99 rating from blended components."""
    frac = min(gp / max(total_games, 1), 1.0)
    confidence = 1 / (1 + exp(-12 * (frac - 0.15)))

    if sport in ("nba", "wnba", "ncaab", "ncaaw"):
        blend = 0.50 * pyth + 0.15 * win_pct + 0.20 * recent + 0.15 * pyth
    elif sport == "mlb":
        blend = 0.55 * pyth + 0.20 * win_pct + 0.25 * recent
    elif sport in ("nhl",):
        blend = 0.55 * pyth + 0.30 * win_pct + 0.15 * recent
    elif sport in ("nfl", "ncaaf"):
        blend = 0.60 * pyth + 0.40 * win_pct
    elif sport in ("epl", "mls"):
        blend = 0.45 * pyth + 0.30 * win_pct + 0.25 * recent
    elif sport == "f1":
        blend = 0.70 * win_pct + 0.30 * recent
    else:
        blend = 0.50 * pyth + 0.25 * win_pct + 0.25 * recent

    raw = confidence * blend + (1 - confidence) * 0.5
    return max(1.0, min(99.0, raw * 98 + 1))


def load_sport_context(sport: str, season: str | None = None) -> SportContext:
    """Load all data for a sport and build a SportContext."""
    cfg = SPORT_CONFIG[sport]
    if season is None:
        season = get_current_season(sport)

    standings = _load_parquet(sport, "standings", season)
    games = _load_parquet(sport, "games", season)
    teams_df = _load_parquet(sport, "teams", season)

    # Build team ID → name mapping from teams parquet
    id_to_name: dict[str, str] = {}
    id_to_abbr: dict[str, str] = {}
    if not teams_df.empty:
        for _, r in teams_df.iterrows():
            tid = str(r.get("id", r.get("team_id", "")))
            id_to_name[tid] = str(r.get("name", ""))
            id_to_abbr[tid] = str(r.get("abbreviation", "") or "")

    # F1 special handling: standings use driver slugs, need players parquet
    if sport == "f1":
        players_df = _load_parquet(sport, "players", season)
        if not players_df.empty and "name" in players_df.columns:
            for _, r in players_df.iterrows():
                name = str(r.get("name", ""))
                # Create slug variants to match standings team_id
                slug = name.lower().replace(" ", "_")
                parts = name.split()
                if parts:
                    last_slug = parts[-1].lower()
                    id_to_name[last_slug] = name.title()
                    id_to_abbr[last_slug] = parts[-1][:3].upper()
                id_to_name[slug] = name.title()
                id_to_abbr[slug] = name[:3].upper()

    # Also try to resolve team names from games data (for sports with partial teams parquet)
    for games_src in [games]:
        if games_src.empty:
            continue
        for col_name, col_id in [("home_team", "home_team_id"), ("away_team", "away_team_id")]:
            if col_name in games_src.columns and col_id in games_src.columns:
                for _, g in games_src.drop_duplicates(subset=[col_id]).iterrows():
                    tid = str(g.get(col_id, ""))
                    name = str(g.get(col_name, ""))
                    if tid and name and tid not in id_to_name:
                        id_to_name[tid] = name
                        id_to_abbr[tid] = name[:3].upper()

    # Last resort: scan all season game parquets for additional name mappings
    if sport in ("ncaab", "ncaaf", "ncaaw"):
        sport_dir = DATA_DIR / sport
        if sport_dir.exists():
            for gf in sorted(sport_dir.glob("games_*.parquet")):
                try:
                    gdf = pd.read_parquet(gf)
                    for col_name, col_id in [("home_team", "home_team_id"), ("away_team", "away_team_id")]:
                        if col_name in gdf.columns and col_id in gdf.columns:
                            for _, g in gdf.drop_duplicates(subset=[col_id]).iterrows():
                                tid = str(g.get(col_id, ""))
                                name = str(g.get(col_name, ""))
                                if tid and name and tid not in id_to_name:
                                    id_to_name[tid] = name
                                    id_to_abbr[tid] = name[:3].upper()
                except Exception:
                    pass
    teams: list[Team] = []
    name_set: set[str] = set()

    if not standings.empty:
        for _, row in standings.iterrows():
            tid = str(row.get("team_id", ""))
            name = id_to_name.get(tid, tid)
            if not name or name in name_set:
                continue
            name_set.add(name)

            w = _safe_int(row.get("wins"), 0)
            l = _safe_int(row.get("losses"), 0)
            t = _safe_int(row.get("ties"), 0)
            otl = _safe_int(row.get("otl"), 0)
            gp = _safe_int(row.get("games_played"), w + l + t)
            pf = _safe_float(row.get("points_for"), 0)
            pa = _safe_float(row.get("points_against"), 0)
            conf = str(row.get("conference", ""))
            div = str(row.get("division", ""))

            # F1 uses 'pct' as championship points
            if sport == "f1":
                win_pct = w / max(gp, 1) if gp > 0 else 0.0
            else:
                win_pct = w / max(gp, 1) if gp > 0 else 0.5

            pyth = _pyth_pct(pf, pa, cfg["pyth_exp"]) if pf > 0 and pa > 0 else win_pct
            recent = win_pct  # fallback — no separate recent window in standings

            rating = _rating_from_blend(
                pyth, win_pct, recent, gp, cfg["games"], sport
            )

            teams.append(Team(
                name=name, team_id=tid,
                conference=conf, division=div,
                rating=rating, abbr=id_to_abbr.get(tid, ""),
                wins=w, losses=l, ties=t, otl=otl,
                games_played=gp, points_for=pf, points_against=pa,
                pyth_pct=pyth, recent_form=recent,
            ))

    # Remaining schedule from games
    remaining: list[tuple[str, str, str]] = []
    completed_wins: dict[str, int] = {t.name: t.wins for t in teams}

    # ── UFC / Tennis: build "teams" from player_stats if standings empty ──
    if not teams and sport in ("ufc", "atp", "wta"):
        # Try loading player_stats — fall back through recent seasons for best data
        stats_df = pd.DataFrame()
        for try_season in [season, str(int(season) - 1), str(int(season) - 2)]:
            candidate = _load_parquet(sport, "player_stats", try_season)
            if candidate.empty:
                continue
            # UFC: require fight stats; tennis: require set stats
            if sport == "ufc" and "result" not in candidate.columns:
                continue
            if sport in ("atp", "wta") and "result" not in candidate.columns and "sets_won" not in candidate.columns:
                continue
            stats_df = candidate
            log.info("  %s: using player_stats season %s", sport.upper(), try_season)
            break

        if stats_df.empty:
            # Fall back to generic player_stats.parquet with all seasons combined
            stats_df = _load_parquet_raw(sport, "player_stats")

        if not stats_df.empty and "player_name" in stats_df.columns:
            agg: dict[str, str] = {}
            if "game_id" in stats_df.columns:
                agg["game_id"] = "count"
            win_col = "result" if "result" in stats_df.columns else None
            # Normalize result to binary 1/0 for win counting
            if win_col and stats_df[win_col].dtype == object:
                stats_df = stats_df.copy()
                stats_df["_win"] = stats_df[win_col].str.upper().map({"W": 1, "WIN": 1, "1": 1}).fillna(0)
                win_col = "_win"
            if win_col:
                agg[win_col] = "sum"

            try:
                grp = stats_df.groupby("player_name").agg(agg).reset_index()
                if "game_id" in grp.columns:
                    grp = grp.rename(columns={"game_id": "games_played"})
                    # UFC/tennis: fighters fight infrequently — use min 1
                    min_games = 1 if sport in ("ufc", "atp", "wta") else 3
                    grp = grp[grp["games_played"] >= min_games]
                else:
                    grp["games_played"] = 1

                if win_col and win_col in grp.columns:
                    grp["wins"] = pd.to_numeric(grp[win_col], errors="coerce").fillna(0).astype(int)
                else:
                    grp["wins"] = 0

                for _, r in grp.nlargest(min(200, len(grp)), "games_played").iterrows():
                    pname = str(r["player_name"])
                    if pname in name_set:
                        continue
                    name_set.add(pname)
                    gp = _safe_int(r.get("games_played"), 1)
                    w = _safe_int(r.get("wins"), 0)
                    lss = max(0, gp - w)
                    win_pct = w / max(gp, 1)
                    rating = win_pct * 80 + 20  # map 0-100 to 20-100 range
                    teams.append(Team(
                        name=pname, team_id=pname,
                        conference="", division="",
                        rating=rating, abbr=pname[:3].upper(),
                        wins=w, losses=lss, ties=0, otl=0,
                        games_played=gp, points_for=0, points_against=0,
                        pyth_pct=win_pct, recent_form=win_pct,
                    ))
                completed_wins = {t.name: t.wins for t in teams}
                log.info("  %s: built %d fighters/players from player_stats", sport.upper(), len(teams))
            except Exception as e:
                log.warning("Failed to load %s entities from player_stats: %s", sport, e)
    if not games.empty and "status" in games.columns:
        future = games[~games["status"].isin(["final"])]
        for _, g in future.iterrows():
            home = str(g.get("home_team", ""))
            away = str(g.get("away_team", ""))
            if home and away and home in name_set and away in name_set:
                remaining.append((home, away, "neutral" if g.get("is_neutral_site") else "home"))

    # Season completion
    avg_gp = np.mean([t.games_played for t in teams]) if teams else 0
    completion = (avg_gp / cfg["games"] * 100) if cfg["games"] > 0 else 0

    # Conferences
    confs = sorted({t.conference for t in teams if t.conference})
    divs = {t.name: t.division for t in teams}

    # Award candidates from player stats
    award_candidates: dict[str, list[AwardCandidate]] = {}
    for award_key in cfg.get("awards", []):
        candidates = _load_award_candidates(sport, season, award_key, teams)
        if candidates:
            award_candidates[award_key] = candidates

    return SportContext(
        sport=sport, season=season,
        games_per_team=cfg["games"],
        playoff_slots_per_conf=cfg["playoff_slots"],
        has_play_in=cfg.get("play_in", False),
        has_draft_lottery=cfg.get("lottery", False),
        remaining_games=remaining,
        completed_wins=completed_wins,
        award_candidates=award_candidates,
        teams=teams,
        conferences=confs,
        divisions=divs,
        season_completion_pct=min(completion, 100.0),
    )


def _load_award_candidates(sport: str, season: str, award_key: str,
                           teams: list[Team]) -> list[AwardCandidate]:
    """Build award candidates from player stats (best-effort)."""
    stats_df = _load_parquet(sport, "player_stats", season)
    if stats_df.empty:
        return _fallback_award_candidates(sport, award_key, teams)

    team_names = {t.name for t in teams}
    team_by_id = {t.team_id: t.name for t in teams}

    try:
        # Aggregate per player
        if "player_name" not in stats_df.columns:
            return _fallback_award_candidates(sport, award_key, teams)

        agg_cols = {}
        for c in ("pts", "reb", "ast", "stl", "blk", "fgm", "fga"):
            if c in stats_df.columns:
                agg_cols[c] = "mean"
        if "game_id" in stats_df.columns:
            agg_cols["game_id"] = "count"

        if not agg_cols:
            return _fallback_award_candidates(sport, award_key, teams)

        grouped = stats_df.groupby(["player_name", "team_id"]).agg(agg_cols).reset_index()
        if "game_id" in grouped.columns:
            grouped = grouped.rename(columns={"game_id": "games"})
            grouped = grouped[grouped["games"] >= 10]

        if grouped.empty:
            return _fallback_award_candidates(sport, award_key, teams)

        # Score depends on award type
        if award_key in ("mvp", "mvp_al", "mvp_nl", "hart", "opoy", "player_of_season", "player_of_tournament"):
            if "pts" in grouped.columns:
                grouped["score"] = grouped["pts"]
            elif "goals" in grouped.columns and "assists" in grouped.columns:
                grouped["score"] = grouped["goals"] * 1.5 + grouped["assists"]
            elif "goals" in grouped.columns:
                grouped["score"] = grouped["goals"]
            elif "pass_yds" in grouped.columns:
                grouped["score"] = grouped["pass_yds"] / 25 + grouped.get("pass_td", 0) * 6
            else:
                grouped["score"] = 1.0
        elif award_key in ("dpoy",):
            for c in ("stl", "blk"):
                if c not in grouped.columns:
                    grouped[c] = 0
            grouped["score"] = grouped["stl"] + grouped["blk"] * 1.5
        elif award_key in ("roty", "calder", "oroy", "droy", "roy"):
            grouped["score"] = grouped.get("pts", grouped.get("goals", 1.0))
        elif award_key in ("6moy", "mip"):
            grouped["score"] = grouped.get("pts", 1.0)
        elif award_key in ("golden_boot", "scoring_title", "hr_leader", "rbi_leader"):
            stat = next((c for c in ("goals", "pts", "hr", "rbi") if c in grouped.columns), None)
            grouped["score"] = grouped[stat] if stat else 1.0
        elif award_key in ("assists_title",):
            grouped["score"] = grouped.get("ast", grouped.get("assists", 1.0))
        elif award_key in ("rebounds_title",):
            grouped["score"] = grouped.get("reb", 1.0)
        elif award_key in ("golden_glove", "best_goalkeeper", "vezina"):
            grouped["score"] = grouped.get("saves", grouped.get("save_pct", 1.0))
        elif award_key in ("passing_title",):
            grouped["score"] = grouped.get("pass_yds", 1.0)
        elif award_key in ("rushing_title",):
            grouped["score"] = grouped.get("rush_yds", 1.0)
        elif award_key in ("receiving_title",):
            grouped["score"] = grouped.get("rec_yds", 1.0)
        elif award_key in ("points_leader", "goals_leader"):
            stat = next((c for c in ("points", "goals", "pts") if c in grouped.columns), None)
            grouped["score"] = grouped[stat] if stat else 1.0
        elif award_key in ("assists_leader",):
            grouped["score"] = grouped.get("assists", grouped.get("ast", 1.0))
        elif award_key in ("cy_young_al", "cy_young_nl", "era_leader"):
            # Lower ERA is better — invert
            era_col = grouped.get("era")
            if era_col is not None and hasattr(era_col, "mean"):
                grouped["score"] = 1.0 / (grouped["era"].clip(lower=0.1))
            else:
                grouped["score"] = grouped.get("strikeouts", 1.0)
        elif award_key in ("batting_title",):
            grouped["score"] = grouped.get("avg", 1.0)
        elif award_key in ("norris",):
            # Best defenceman: use pts (or blocks+assists)
            grouped["score"] = grouped.get("pts", grouped.get("points", 1.0))
        elif award_key in ("heisman",):
            if "pass_yds" in grouped.columns:
                grouped["score"] = grouped["pass_yds"] / 25 + grouped.get("pass_td", 0) * 6 + grouped.get("rush_yds", 0) / 10
            else:
                grouped["score"] = grouped.get("pts", 1.0)
        elif award_key in ("fighter_of_year", "ko_of_year", "performance_of_year"):
            grouped["score"] = grouped.get("knockdowns", 0) * 3 + grouped.get("sig_strikes", 0) / 100
        elif award_key in ("submission_of_year",):
            grouped["score"] = grouped.get("sub_attempts", 1.0)
        elif award_key in ("year_end_no1", "slam_titles", "masters_titles"):
            grouped["score"] = grouped.get("sets_won", grouped.get("games_won", 1.0))
        else:
            grouped["score"] = grouped.get("pts", grouped.get("goals", grouped.get("points", 1.0)))

        grouped = grouped.nlargest(15, "score")

        candidates = []
        max_score = grouped["score"].max()
        for _, r in grouped.iterrows():
            tname = team_by_id.get(str(r["team_id"]), str(r["team_id"]))
            weight = _safe_float(r["score"], 1.0) / max(max_score, 1) if max_score > 0 else 1.0
            candidates.append(AwardCandidate(
                name=str(r["player_name"]), team=tname, weight=max(0.01, weight)
            ))
        return candidates if candidates else _fallback_award_candidates(sport, award_key, teams)

    except Exception as e:
        log.debug("Award candidate loading failed for %s/%s: %s", sport, award_key, e)
        return _fallback_award_candidates(sport, award_key, teams)


def _fallback_award_candidates(sport: str, award_key: str,
                               teams: list[Team]) -> list[AwardCandidate]:
    """Generate placeholder candidates from top teams when player data is missing."""
    sorted_teams = sorted(teams, key=lambda t: t.rating, reverse=True)[:10]
    candidates = []
    for i, t in enumerate(sorted_teams):
        weight = (10 - i) / 10
        candidates.append(AwardCandidate(
            name=f"Top Player ({t.abbr or t.name})",
            team=t.name,
            weight=weight
        ))
    return candidates


# ═══════════════════════════════════════════════════════════════════════════
# Player Stat Leader Projections
# ═══════════════════════════════════════════════════════════════════════════

def _load_player_stat_leaders(sport: str, season: str, stat_cols: list[str],
                               teams: list[Team], n_leaders: int = 10) -> dict[str, list[dict]]:
    """Load per-player season stat leaders from player_stats parquet.

    Returns dict keyed by stat_col → ranked list of player dicts:
      {name, team, value_per_game, games, projected_season_total, probability_lead}
    """
    stats_df = _load_parquet(sport, "player_stats", season)
    if stats_df.empty or "player_name" not in stats_df.columns:
        return {}

    team_by_id = {t.team_id: t.name for t in teams}
    results: dict[str, list[dict]] = {}

    # Build aggregation
    agg_map: dict[str, str] = {}
    for c in stat_cols:
        if c in stats_df.columns:
            agg_map[c] = "mean"
    if "game_id" in stats_df.columns:
        agg_map["game_id"] = "count"
    if not agg_map:
        return {}

    try:
        grp = stats_df.groupby(["player_name", "team_id"]).agg(agg_map).reset_index()
        if "game_id" in grp.columns:
            grp = grp.rename(columns={"game_id": "games"})
            grp = grp[grp["games"] >= 5]
        else:
            grp["games"] = 1
    except Exception as e:
        log.debug("stat leader groupby failed: %s", e)
        return {}

    cfg = SPORT_CONFIG.get(sport, {})
    games_in_season = cfg.get("games", 82)

    for stat in stat_cols:
        if stat not in grp.columns:
            continue
        col = grp[stat]
        if col.isna().all():
            continue

        # For ERA/save_pct lower is not better to sort — handle specially
        ascending = stat in ("era", "double_faults")
        top = grp.nsmallest(n_leaders, stat) if ascending else grp.nlargest(n_leaders, stat)

        max_val = float(top[stat].max()) if not ascending else float(top[stat].min())
        entries = []
        for i, (_, r) in enumerate(top.iterrows()):
            val = _safe_float(r[stat])
            games = _safe_int(r.get("games", 1), 1)
            projected = round(val * games_in_season, 1)
            # Simple probability: proportional softmax-like weight
            rank_weight = max(1.0, n_leaders - i) / n_leaders
            denom = sum(max(1.0, n_leaders - j) / n_leaders for j in range(min(n_leaders, len(top))))
            prob = round(rank_weight / denom * 100, 1) if denom > 0 else 0.0
            tname = team_by_id.get(str(r["team_id"]), str(r.get("team_id", "")))
            entries.append({
                "rank": i + 1,
                "name": str(r["player_name"]),
                "team": tname,
                "per_game": round(val, 3),
                "games_played": games,
                "projected_season": projected,
                "lead_probability_pct": prob,
                "stat": stat,
                "stat_label": STAT_LABELS.get(stat, stat.replace("_", " ").title()),
            })
        if entries:
            results[stat] = entries

    return results


def _load_player_props(sport: str, season: str, teams: list[Team]) -> dict[str, Any]:
    """Compute game-level player prop over/under thresholds from historical data.

    Returns a dict of stat → {median, p25, p75, typical_over_under_line}
    Used by autobet to set O/U lines for player prop bets.
    """
    stats_df = _load_parquet(sport, "player_stats", season)
    if stats_df.empty:
        return {}

    cfg = SPORT_CONFIG.get(sport, {})
    stat_cols = cfg.get("player_stat_leaders", [])
    props: dict[str, Any] = {}

    for stat in stat_cols:
        if stat not in stats_df.columns:
            continue
        col = stats_df[stat].dropna()
        if len(col) < 20:
            continue
        props[stat] = {
            "stat_label": STAT_LABELS.get(stat, stat),
            "median": round(float(col.median()), 2),
            "mean": round(float(col.mean()), 2),
            "p25": round(float(col.quantile(0.25)), 2),
            "p75": round(float(col.quantile(0.75)), 2),
            "p90": round(float(col.quantile(0.90)), 2),
            "typical_ou_line": round(float(col.median()), 1),
            "sample_games": len(col),
        }
    return props


# ═══════════════════════════════════════════════════════════════════════════
# Game-Level Prediction Types (season-aggregate)
# ═══════════════════════════════════════════════════════════════════════════

def _compute_game_pred_averages(sport: str, season: str,
                                 teams: list[Team]) -> dict[str, Any]:
    """Compute game-level prediction type averages from historical games.

    Outputs season-wide aggregate probabilities and per-team breakdowns for:
      winner, draw, btts, clean_sheet, ot, halftime_winner, total_goals,
      method_of_victory (UFC), straight_sets (tennis), etc.
    """
    games_df = _load_parquet(sport, "games", season)
    if games_df.empty:
        return {}

    cfg = SPORT_CONFIG.get(sport, {})
    pred_types = cfg.get("game_pred_types", [])
    out: dict[str, Any] = {}

    hs_col = "home_score"
    as_col = "away_score"
    has_scores = hs_col in games_df.columns and as_col in games_df.columns

    if not has_scores:
        return {}

    # Filter to completed games only
    completed = games_df[games_df[hs_col].notna() & games_df[as_col].notna()].copy()
    if completed.empty:
        return {}

    completed["_hs"] = pd.to_numeric(completed[hs_col], errors="coerce")
    completed["_as"] = pd.to_numeric(completed[as_col], errors="coerce")
    completed = completed[completed["_hs"].notna() & completed["_as"].notna()]
    n = len(completed)
    if n == 0:
        return {}

    # ── BTTS ─────────────────────────────────────────────────────────
    if "btts" in pred_types or "btts" in (pred_types or []):
        btts_rate = float(((completed["_hs"] > 0) & (completed["_as"] > 0)).mean())
        out["btts"] = {"season_rate": round(btts_rate, 3), "games": n,
                       "description": "Both teams score in game"}

    # ── Clean Sheet ───────────────────────────────────────────────────
    if "clean_sheet_home" in pred_types:
        cs_h = float((completed["_as"] == 0).mean())
        out["clean_sheet_home"] = {"season_rate": round(cs_h, 3), "games": n,
                                    "description": "Home team keeps clean sheet"}
    if "clean_sheet_away" in pred_types:
        cs_a = float((completed["_hs"] == 0).mean())
        out["clean_sheet_away"] = {"season_rate": round(cs_a, 3), "games": n,
                                    "description": "Away team keeps clean sheet"}

    # ── Draw ──────────────────────────────────────────────────────────
    if "draw" in pred_types or "three_way" in pred_types:
        draw_rate = float((completed["_hs"] == completed["_as"]).mean())
        home_win_rate = float((completed["_hs"] > completed["_as"]).mean())
        away_win_rate = float((completed["_hs"] < completed["_as"]).mean())
        out["three_way_split"] = {
            "home_win_rate": round(home_win_rate, 3),
            "draw_rate": round(draw_rate, 3),
            "away_win_rate": round(away_win_rate, 3),
            "games": n,
        }

    # ── OT / Extra Time ───────────────────────────────────────────────
    if "ot" in pred_types:
        ot_rate = cfg.get("ot_rate", 0.0)
        # If ot column available, use actual data
        if "home_ot" in completed.columns:
            actual_ot = completed["home_ot"].notna() & (completed["home_ot"].fillna(0) > 0)
            ot_rate = float(actual_ot.mean())
        out["overtime"] = {"season_rate": round(ot_rate, 3), "games": n,
                           "description": "Game goes to overtime/extra time"}

    # ── Total Goals / Points ──────────────────────────────────────────
    if "total_goals" in pred_types or "total" in pred_types:
        total = (completed["_hs"] + completed["_as"])
        out["total_score"] = {
            "mean": round(float(total.mean()), 2),
            "median": round(float(total.median()), 2),
            "p25": round(float(total.quantile(0.25)), 2),
            "p75": round(float(total.quantile(0.75)), 2),
            "typical_ou_line": round(float(total.median()), 1),
            "games": n,
        }

    # ── Double Chance ─────────────────────────────────────────────────
    if "double_chance" in pred_types:
        draw_rate = float((completed["_hs"] == completed["_as"]).mean())
        home_win_rate = float((completed["_hs"] > completed["_as"]).mean())
        away_win_rate = float((completed["_hs"] < completed["_as"]).mean())
        out["double_chance"] = {
            "home_or_draw": round(home_win_rate + draw_rate, 3),
            "away_or_draw": round(away_win_rate + draw_rate, 3),
            "games": n,
            "description": "Home/Draw or Away/Draw probability",
        }

    # ── Halftime Results ──────────────────────────────────────────────
    if "halftime_winner" in pred_types:
        q1h = "home_q1"
        q1a = "away_q1"
        if q1h in completed.columns and q1a in completed.columns:
            ht_df = completed[[q1h, q1a]].copy()
            ht_df["_hq1"] = pd.to_numeric(ht_df[q1h], errors="coerce")
            ht_df["_aq1"] = pd.to_numeric(ht_df[q1a], errors="coerce")
            ht_valid = ht_df[ht_df["_hq1"].notna() & ht_df["_aq1"].notna()]
            if len(ht_valid) >= 20:
                ht_home = float((ht_valid["_hq1"] > ht_valid["_aq1"]).mean())
                ht_draw = float((ht_valid["_hq1"] == ht_valid["_aq1"]).mean())
                ht_away = float((ht_valid["_hq1"] < ht_valid["_aq1"]).mean())
                out["halftime_winner"] = {
                    "home_rate": round(ht_home, 3),
                    "draw_rate": round(ht_draw, 3),
                    "away_rate": round(ht_away, 3),
                    "games": len(ht_valid),
                }

    # ── UFC Method of Victory ─────────────────────────────────────────
    if "method_of_victory" in pred_types or "ko_tko" in pred_types:
        player_df = _load_parquet(sport, "player_stats", season)
        if not player_df.empty and "method" in player_df.columns:
            methods = player_df["method"].dropna().str.lower()
            total_m = len(methods)
            if total_m > 0:
                ko_rate = float((methods.str.contains("ko|tko", na=False)).mean())
                sub_rate = float((methods.str.contains("sub", na=False)).mean())
                dec_rate = float((methods.str.contains("dec|decision", na=False)).mean())
                out["method_of_victory"] = {
                    "ko_tko_rate": round(ko_rate, 3),
                    "submission_rate": round(sub_rate, 3),
                    "decision_rate": round(dec_rate, 3),
                    "total_fights": total_m,
                }
                if "round_finished" in player_df.columns:
                    rounds = pd.to_numeric(player_df["round_finished"], errors="coerce").dropna()
                    if len(rounds) > 0:
                        out["method_of_victory"]["avg_round_stoppage"] = round(float(rounds.mean()), 2)
                        out["method_of_victory"]["finish_in_r1_rate"] = round(float((rounds == 1).mean()), 3)
                        out["method_of_victory"]["goes_to_decision_rate"] = round(float((rounds >= 3).mean()), 3)

    # ── Tennis Straight Sets ──────────────────────────────────────────
    if "straight_sets" in pred_types:
        player_df = _load_parquet(sport, "player_stats", season)
        if not player_df.empty and "sets_won" in player_df.columns and "sets_lost" in player_df.columns:
            winner_rows = player_df[player_df.get("result", pd.Series()) == 1] if "result" in player_df.columns else player_df
            sw = pd.to_numeric(winner_rows["sets_won"], errors="coerce")
            sl = pd.to_numeric(winner_rows.get("sets_lost", pd.Series()), errors="coerce")
            if len(sw.dropna()) >= 10:
                straight_2 = float(((sw == 2) & (sl == 0)).mean()) if len(sl.dropna()) > 0 else 0.0
                straight_3 = float(((sw == 3) & (sl == 0)).mean()) if len(sl.dropna()) > 0 else 0.0
                out["straight_sets"] = {
                    "2_0_rate": round(straight_2, 3),
                    "3_0_rate": round(straight_3, 3),
                    "combined_straight_sets_rate": round(straight_2 + straight_3, 3),
                    "description": "Winner takes straight sets (no dropped set)",
                }
                if "aces" in player_df.columns:
                    aces = pd.to_numeric(player_df["aces"], errors="coerce").dropna()
                    out["serve_stats"] = {
                        "avg_aces_per_match": round(float(aces.mean()), 2),
                        "ace_p75": round(float(aces.quantile(0.75)), 2),
                        "typical_ace_ou_line": round(float(aces.median()), 1),
                    }
                if "first_serve_pct" in player_df.columns:
                    fsp = pd.to_numeric(player_df["first_serve_pct"], errors="coerce").dropna()
                    if len(fsp) > 0:
                        out["serve_stats"] = out.get("serve_stats", {})
                        out["serve_stats"]["avg_first_serve_pct"] = round(float(fsp.mean()), 3)

    return out


# ═══════════════════════════════════════════════════════════════════════════
# Simulation Core
# ═══════════════════════════════════════════════════════════════════════════

def _bt_win_prob(r_home: float, r_away: float, home_adv: float) -> float:
    """Bradley-Terry home-win probability with home advantage."""
    h = r_home * home_adv
    return h / (h + r_away) if (h + r_away) > 0 else 0.5


def _simulate_season_schedule(ctx: SportContext, rng: np.random.Generator) -> dict[str, int]:
    """Simulate remaining games using Bradley-Terry; anchor to current wins."""
    wins = dict(ctx.completed_wins)
    ratings = {t.name: t.rating for t in ctx.teams}
    home_adv = SPORT_CONFIG[ctx.sport]["home_adv"]

    for home, away, site in ctx.remaining_games:
        rh = ratings.get(home, 50)
        ra = ratings.get(away, 50)
        adv = home_adv if site != "neutral" else 1.0
        p = _bt_win_prob(rh, ra, adv)
        if rng.random() < p:
            wins[home] = wins.get(home, 0) + 1
        else:
            wins[away] = wins.get(away, 0) + 1
    return wins


def _simulate_season_random(ctx: SportContext, rng: np.random.Generator) -> dict[str, int]:
    """Simulate full season with Gaussian noise around expected win rate."""
    wins: dict[str, int] = {}
    ratings = {t.name: t.rating for t in ctx.teams}
    max_r = max(ratings.values()) if ratings else 99
    min_r = min(ratings.values()) if ratings else 1
    span = max(max_r - min_r, 1)

    for t in ctx.teams:
        norm = (t.rating - min_r) / span
        expected = 0.26 + 0.48 * norm
        sigma = 0.06 + 0.02 * (1 - abs(norm * 2 - 1))
        noisy = np.clip(expected + rng.normal(0, sigma), 0.10, 0.90)
        wins[t.name] = int(round(noisy * ctx.games_per_team))
    return wins


def _simulate_wins(ctx: SportContext, rng: np.random.Generator) -> dict[str, int]:
    """Choose schedule-based or random simulation."""
    if ctx.remaining_games and ctx.season_completion_pct < 99:
        return _simulate_season_schedule(ctx, rng)
    return _simulate_season_random(ctx, rng)


def _champ_weight(rating: float, wins: int, games: int) -> float:
    return exp(rating / 12.0) * (1.0 + wins / max(games, 1))


def _weighted_pick(items: list[str], weights: list[float],
                   rng: np.random.Generator) -> str:
    w = np.array(weights, dtype=np.float64)
    total = w.sum()
    if total <= 0:
        return rng.choice(items)
    w /= total
    return items[rng.choice(len(items), p=w)]


def _simulate_bracket(seeds: list[str], ratings: dict[str, float],
                      wins: dict[str, int], games: int,
                      rng: np.random.Generator, has_bye: bool = False) -> tuple[str, dict[str, int]]:
    """Simulate a playoff bracket. Returns (champion, round_reached_map)."""
    if not seeds:
        return ("", {})

    round_reached: dict[str, int] = {s: 1 for s in seeds}
    current = list(seeds)

    if has_bye and len(current) >= 7:
        # NFL-style: seed 1 gets bye
        bye_team = current[0]
        wild_card = current[1:]
        # Standard matchups: 2v7, 3v6, 4v5
        matchups = []
        while len(wild_card) >= 2:
            matchups.append((wild_card.pop(0), wild_card.pop(-1)))
        wc_winners = []
        for a, b in matchups:
            wa = _champ_weight(ratings.get(a, 50), wins.get(a, 0), games)
            wb = _champ_weight(ratings.get(b, 50), wins.get(b, 0), games)
            winner = _weighted_pick([a, b], [wa, wb], rng)
            loser = b if winner == a else a
            round_reached[winner] = 2
            wc_winners.append(winner)
        # Divisional: bye team vs worst remaining, others paired
        wc_winners.sort(key=lambda x: seeds.index(x) if x in seeds else 99)
        current = [bye_team] + wc_winners
        round_reached[bye_team] = 2
    else:
        pass  # standard bracket below

    # Standard bracket rounds
    rnd = 2 if has_bye else 1
    while len(current) > 1:
        next_round = []
        matchups = []
        while len(current) >= 2:
            matchups.append((current.pop(0), current.pop(-1)))
        if current:  # odd team gets bye
            next_round.append(current.pop())

        for a, b in matchups:
            wa = _champ_weight(ratings.get(a, 50), wins.get(a, 0), games)
            wb = _champ_weight(ratings.get(b, 50), wins.get(b, 0), games)
            winner = _weighted_pick([a, b], [wa, wb], rng)
            round_reached[winner] = rnd + 1
            next_round.append(winner)

        current = sorted(next_round,
                         key=lambda x: seeds.index(x) if x in seeds else 99)
        rnd += 1

    if current:
        champion = current[0]
        round_reached[champion] = rnd
    else:
        champion = ""
    return champion, round_reached


def _simulate_play_in(ranked: list[str], ratings: dict[str, float],
                      wins: dict[str, int], games: int,
                      rng: np.random.Generator) -> list[str]:
    """NBA-style play-in: seeds 7-10 compete for seeds 7-8."""
    if len(ranked) < 10:
        return ranked[:8] if len(ranked) >= 8 else ranked

    direct = ranked[:6]
    pool = ranked[6:10]

    # 7 vs 8 → winner is seed 7
    w78 = _weighted_pick(
        [pool[0], pool[1]],
        [_champ_weight(ratings.get(pool[0], 50), wins.get(pool[0], 0), games),
         _champ_weight(ratings.get(pool[1], 50), wins.get(pool[1], 0), games)],
        rng,
    )
    l78 = pool[1] if w78 == pool[0] else pool[0]

    # 9 vs 10 → winner advances
    w910 = _weighted_pick(
        [pool[2], pool[3]],
        [_champ_weight(ratings.get(pool[2], 50), wins.get(pool[2], 0), games),
         _champ_weight(ratings.get(pool[3], 50), wins.get(pool[3], 0), games)],
        rng,
    )

    # Loser 7-8 vs Winner 9-10 → seed 8
    seed8 = _weighted_pick(
        [l78, w910],
        [_champ_weight(ratings.get(l78, 50), wins.get(l78, 0), games),
         _champ_weight(ratings.get(w910, 50), wins.get(w910, 0), games)],
        rng,
    )
    return direct + [w78, seed8]


def _award_pick(candidates: list[AwardCandidate], wins: dict[str, int],
                games: int, rng: np.random.Generator) -> str:
    """Pick an award winner using weight-based probability."""
    if not candidates:
        return ""
    names = [c.name for c in candidates]
    weights = []
    for c in candidates:
        tw = wins.get(c.team, 0) / max(games, 1)
        w = exp((c.weight - 0.5) * 6) * (0.5 + tw)
        weights.append(max(w, 0.001))
    return _weighted_pick(names, weights, rng)


# ═══════════════════════════════════════════════════════════════════════════
# Sport-Specific Simulators
# ═══════════════════════════════════════════════════════════════════════════

def _sim_league_sport(ctx: SportContext, n_sims: int,
                      rng: np.random.Generator) -> dict[str, Any]:
    """Generic simulation for NBA, NFL, NHL, MLB, MLS, WNBA."""
    teams = ctx.teams
    if not teams:
        return {"error": f"No team data for {ctx.sport}"}

    name_list = [t.name for t in teams]
    ratings = {t.name: t.rating for t in teams}
    n = len(teams)

    # Accumulators
    champ_counts: dict[str, int] = {t: 0 for t in name_list}
    conf_finals_counts: dict[str, int] = {t: 0 for t in name_list}
    playoff_counts: dict[str, int] = {t: 0 for t in name_list}
    div_counts: dict[str, int] = {t: 0 for t in name_list}
    round_reached_totals: dict[str, dict[str, int]] = {t: {} for t in name_list}
    wins_history: dict[str, list[int]] = {t: [] for t in name_list}
    award_counts: dict[str, dict[str, int]] = {}
    lottery_counts: dict[str, float] = {}

    for award_key in ctx.award_candidates:
        award_counts[award_key] = {}
        for c in ctx.award_candidates[award_key]:
            award_counts[award_key][c.name] = 0

    has_bye = ctx.sport == "nfl"
    use_play_in = ctx.has_play_in

    for _ in range(n_sims):
        wins = _simulate_wins(ctx, rng)
        for t in name_list:
            wins_history[t].append(wins.get(t, 0))

        # Conference seeding
        conf_teams: dict[str, list[str]] = {}
        for t in teams:
            c = t.conference
            if c not in conf_teams:
                conf_teams[c] = []
            conf_teams[c].append(t.name)

        # Sort each conference by wins
        for c in conf_teams:
            conf_teams[c].sort(key=lambda x: wins.get(x, 0), reverse=True)

        # Division winners
        div_best: dict[str, tuple[str, int]] = {}
        for t in teams:
            d = ctx.divisions.get(t.name, "")
            w = wins.get(t.name, 0)
            if d and (d not in div_best or w > div_best[d][1]):
                div_best[d] = (t.name, w)
        for d, (tname, _) in div_best.items():
            div_counts[tname] = div_counts.get(tname, 0) + 1

        # Playoffs per conference
        conf_champs: list[str] = []
        for c, ranked in conf_teams.items():
            slots = ctx.playoff_slots_per_conf
            if slots <= 0:
                continue

            if use_play_in and len(ranked) >= 10:
                bracket_teams = _simulate_play_in(ranked, ratings, wins, ctx.games_per_team, rng)
            else:
                bracket_teams = ranked[:slots]

            for t in bracket_teams:
                playoff_counts[t] = playoff_counts.get(t, 0) + 1

            champ, rr = _simulate_bracket(
                bracket_teams, ratings, wins, ctx.games_per_team, rng, has_bye=has_bye
            )
            if champ:
                conf_champs.append(champ)
                conf_finals_counts[champ] = conf_finals_counts.get(champ, 0) + 1
            for t, rd in rr.items():
                round_reached_totals[t][str(rd)] = round_reached_totals[t].get(str(rd), 0) + 1

        # Championship
        if len(conf_champs) >= 2:
            cw = [_champ_weight(ratings.get(c, 50), wins.get(c, 0), ctx.games_per_team)
                  for c in conf_champs]
            champion = _weighted_pick(conf_champs, cw, rng)
            champ_counts[champion] += 1
        elif len(conf_champs) == 1:
            champ_counts[conf_champs[0]] += 1

        # Awards
        for award_key, candidates in ctx.award_candidates.items():
            winner = _award_pick(candidates, wins, ctx.games_per_team, rng)
            if winner and winner in award_counts[award_key]:
                award_counts[award_key][winner] += 1

        # Draft lottery
        if ctx.has_draft_lottery:
            non_playoff = [t for t in name_list if playoff_counts.get(t, 0) <= sum(
                1 for t2 in name_list if playoff_counts.get(t2, 0) > 0
            ) * 0]  # simplify: use current sim's result
            # Determine who missed playoffs in this sim
            this_playoff = set()
            for c, ranked in conf_teams.items():
                slots = ctx.playoff_slots_per_conf
                if use_play_in and len(ranked) >= 10:
                    this_playoff.update(ranked[:10])
                else:
                    this_playoff.update(ranked[:slots])
            non_playoff_this = [t for t in name_list if t not in this_playoff]
            non_playoff_this.sort(key=lambda x: wins.get(x, 0))

            odds = NBA_LOTTERY_ODDS if ctx.sport in ("nba", "wnba") else NHL_LOTTERY_ODDS
            for i, t in enumerate(non_playoff_this[:len(odds)]):
                lottery_counts[t] = lottery_counts.get(t, 0) + odds[i]

    # Build output
    result = _build_league_output(
        ctx, n_sims, champ_counts, conf_finals_counts, playoff_counts,
        div_counts, round_reached_totals, wins_history, award_counts, lottery_counts
    )
    return result


def _build_league_output(
    ctx: SportContext, n_sims: int,
    champ_counts: dict, conf_finals_counts: dict, playoff_counts: dict,
    div_counts: dict, round_reached_totals: dict, wins_history: dict,
    award_counts: dict, lottery_counts: dict,
) -> dict[str, Any]:
    """Format league simulation results."""

    def _pct(count: int) -> float:
        return round(count / n_sims * 100, 2)

    teams_sorted = sorted(ctx.teams, key=lambda t: champ_counts.get(t.name, 0), reverse=True)

    championship = [
        {"name": t.name, "abbreviation": t.abbr, "simulations_won": champ_counts.get(t.name, 0),
         "probability": _pct(champ_counts.get(t.name, 0))}
        for t in teams_sorted if champ_counts.get(t.name, 0) > 0
    ]

    conf_finals = [
        {"name": t.name, "abbreviation": t.abbr,
         "probability": _pct(conf_finals_counts.get(t.name, 0))}
        for t in teams_sorted if conf_finals_counts.get(t.name, 0) > 0
    ]

    playoff_odds = [
        {"name": t.name, "abbreviation": t.abbr,
         "probability": _pct(playoff_counts.get(t.name, 0))}
        for t in sorted(ctx.teams, key=lambda t: playoff_counts.get(t.name, 0), reverse=True)
        if playoff_counts.get(t.name, 0) > 0
    ]

    division_odds = [
        {"name": t.name, "abbreviation": t.abbr, "division": ctx.divisions.get(t.name, ""),
         "probability": _pct(div_counts.get(t.name, 0))}
        for t in sorted(ctx.teams, key=lambda t: div_counts.get(t.name, 0), reverse=True)
        if div_counts.get(t.name, 0) > 0
    ]

    # Projected wins
    projected_wins: dict[str, Any] = {}
    for t in ctx.teams:
        wh = wins_history.get(t.name, [])
        if wh:
            arr = np.array(wh)
            projected_wins[t.name] = {
                "mean": round(float(np.mean(arr)), 1),
                "median": float(np.median(arr)),
                "std": round(float(np.std(arr)), 2),
                "p10": int(np.percentile(arr, 10)),
                "p90": int(np.percentile(arr, 90)),
                "current_wins": t.wins,
                "current_losses": t.losses,
                "current_win_pct": round(t.wins / max(t.games_played, 1), 4),
            }

    # Team strengths
    strengths = [
        {
            "name": t.name, "abbreviation": t.abbr,
            "conference": t.conference, "division": t.division,
            "rating": round(t.rating, 1),
            "wins": t.wins, "losses": t.losses,
            "win_pct": round(t.wins / max(t.games_played, 1), 4),
            "pyth_win_pct": round(t.pyth_pct, 4),
            "games_played": t.games_played,
        }
        for t in sorted(ctx.teams, key=lambda t: t.rating, reverse=True)
    ]

    # Awards
    awards_out: dict[str, list] = {}
    for award_key, counts in award_counts.items():
        sorted_candidates = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        awards_out[award_key] = [
            {"name": name, "simulations_won": c, "probability": _pct(c)}
            for name, c in sorted_candidates if c > 0
        ]

    # Round-by-round odds
    labels = ROUND_LABELS.get(ctx.sport, ["round_1", "round_2", "round_3", "finals"])
    round_by_round = []
    for t in teams_sorted:
        entry = {"name": t.name, "abbreviation": t.abbr}
        entry["make_playoffs"] = _pct(playoff_counts.get(t.name, 0))
        for i, label in enumerate(labels):
            rr = round_reached_totals.get(t.name, {})
            cumulative = sum(v for k, v in rr.items() if int(k) >= i + 1)
            entry[label] = _pct(cumulative)
        entry["championship"] = _pct(champ_counts.get(t.name, 0))
        if any(v for k, v in entry.items() if k not in ("name", "abbreviation") and isinstance(v, (int, float)) and v > 0):
            round_by_round.append(entry)

    # Draft lottery
    draft_lottery = []
    if lottery_counts:
        # lottery_counts[team] = sum of lottery-odds assigned across sims
        # Normalize: average the accumulated odds across simulations
        for name in sorted(lottery_counts, key=lambda x: lottery_counts[x], reverse=True):
            draft_lottery.append({
                "name": name,
                "top_pick_probability": round(lottery_counts[name] / n_sims, 2),
            })

    # Player stat leaders
    cfg = SPORT_CONFIG.get(ctx.sport, {})
    stat_cols = cfg.get("player_stat_leaders", [])
    season = getattr(ctx, "season", str(datetime.now().year))
    player_stat_leaders = _load_player_stat_leaders(ctx.sport, season, stat_cols, ctx.teams)
    player_props = _load_player_props(ctx.sport, season, ctx.teams)
    game_pred_averages = _compute_game_pred_averages(ctx.sport, season, ctx.teams)

    return {
        "championship_probabilities": championship,
        "conference_finals_odds": conf_finals,
        "playoff_odds": playoff_odds,
        "division_winner_odds": division_odds,
        "awards": awards_out,
        "round_by_round_odds": round_by_round,
        "projected_wins": projected_wins,
        "team_strengths": strengths,
        "draft_lottery_odds": draft_lottery,
        "player_stat_leaders": player_stat_leaders,
        "player_props_ou_lines": player_props,
        "game_prediction_types": {
            "season_averages": game_pred_averages,
            "supported_types": cfg.get("game_pred_types", []),
            "description": "Historical rates used as baseline for game-level predictions",
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# EPL / Soccer — Title, Top 4, Relegation
# ═══════════════════════════════════════════════════════════════════════════

def _sim_epl(ctx: SportContext, n_sims: int,
             rng: np.random.Generator) -> dict[str, Any]:
    """Simulate EPL: title, top 4, relegation, awards."""
    teams = ctx.teams
    if not teams:
        return {"error": "No EPL team data"}

    name_list = [t.name for t in teams]
    ratings = {t.name: t.rating for t in teams}

    title_counts: dict[str, int] = {t: 0 for t in name_list}
    top4_counts: dict[str, int] = {t: 0 for t in name_list}
    relegation_counts: dict[str, int] = {t: 0 for t in name_list}
    wins_history: dict[str, list[int]] = {t: [] for t in name_list}
    award_counts: dict[str, dict[str, int]] = {}
    for ak in ctx.award_candidates:
        award_counts[ak] = {c.name: 0 for c in ctx.award_candidates[ak]}

    for _ in range(n_sims):
        wins = _simulate_wins(ctx, rng)
        # EPL uses points: W=3, D=1, L=0. Approximate via wins + ties
        # Since we simulate wins, compute points as wins*3 + (gp - wins - losses)*1
        points: dict[str, float] = {}
        for t in teams:
            w = wins.get(t.name, 0)
            total_g = ctx.games_per_team
            # Estimate draws as proportion of remaining
            draw_rate = t.ties / max(t.games_played, 1) if t.ties else 0.25
            draws = int(round((total_g - w) * draw_rate))
            pts = w * 3 + draws
            points[t.name] = pts

        ranked = sorted(name_list, key=lambda x: points.get(x, 0), reverse=True)
        title_counts[ranked[0]] += 1
        for t in ranked[:4]:
            top4_counts[t] += 1
        for t in ranked[-3:]:
            relegation_counts[t] += 1

        for t in name_list:
            wins_history[t].append(wins.get(t, 0))

        for ak, candidates in ctx.award_candidates.items():
            winner = _award_pick(candidates, wins, ctx.games_per_team, rng)
            if winner and winner in award_counts[ak]:
                award_counts[ak][winner] += 1

    def _pct(c: int) -> float:
        return round(c / n_sims * 100, 2)

    awards_out = {}
    for ak, counts in award_counts.items():
        awards_out[ak] = [
            {"name": n, "simulations_won": c, "probability": _pct(c)}
            for n, c in sorted(counts.items(), key=lambda x: x[1], reverse=True) if c > 0
        ]

    cfg = SPORT_CONFIG.get(ctx.sport, {})
    stat_cols = cfg.get("player_stat_leaders", [])
    season = getattr(ctx, "season", str(datetime.now().year))

    return {
        "championship_probabilities": [
            {"name": t, "probability": _pct(title_counts[t])}
            for t in sorted(name_list, key=lambda x: title_counts[x], reverse=True)
            if title_counts[t] > 0
        ],
        "top_4_odds": [
            {"name": t, "probability": _pct(top4_counts[t])}
            for t in sorted(name_list, key=lambda x: top4_counts[x], reverse=True)
            if top4_counts[t] > 0
        ],
        "relegation_odds": [
            {"name": t, "probability": _pct(relegation_counts[t])}
            for t in sorted(name_list, key=lambda x: relegation_counts[x], reverse=True)
            if relegation_counts[t] > 0
        ],
        "awards": awards_out,
        "projected_wins": {
            t.name: {
                "mean": round(float(np.mean(wins_history[t.name])), 1),
                "median": float(np.median(wins_history[t.name])),
                "std": round(float(np.std(wins_history[t.name])), 2),
                "current_wins": t.wins, "current_losses": t.losses,
            }
            for t in teams if wins_history[t.name]
        },
        "team_strengths": [
            {"name": t.name, "rating": round(t.rating, 1), "wins": t.wins,
             "losses": t.losses, "ties": t.ties, "games_played": t.games_played}
            for t in sorted(teams, key=lambda t: t.rating, reverse=True)
        ],
        "player_stat_leaders": _load_player_stat_leaders(ctx.sport, season, stat_cols, teams),
        "player_props_ou_lines": _load_player_props(ctx.sport, season, teams),
        "game_prediction_types": {
            "season_averages": _compute_game_pred_averages(ctx.sport, season, teams),
            "supported_types": cfg.get("game_pred_types", []),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# F1 — WDC / WCC
# ═══════════════════════════════════════════════════════════════════════════

def _sim_f1(ctx: SportContext, n_sims: int,
            rng: np.random.Generator) -> dict[str, Any]:
    """Simulate F1 WDC standings — simplified points-based projection."""
    drivers = ctx.teams
    if not drivers:
        return {"error": "No F1 data"}

    name_list = [d.name for d in drivers]
    ratings = {d.name: d.rating for d in drivers}

    wdc_counts: dict[str, int] = {d: 0 for d in name_list}
    points_history: dict[str, list[float]] = {d: [] for d in name_list}

    # F1 points per position (top 10)
    f1_points = [25, 18, 15, 12, 10, 8, 6, 4, 2, 1]

    # Current points from standings (stored in 'pct' field as hack)
    current_pts: dict[str, float] = {}
    for d in drivers:
        current_pts[d.name] = _safe_float(d.pyth_pct, 0) * 98 + 1  # approximation from rating
        # Use actual points if available (stored as F1 uses 'pct' for points)
        if hasattr(d, 'points_for') and d.points_for > 0:
            current_pts[d.name] = d.points_for

    # Estimate remaining races
    avg_races = np.mean([d.games_played for d in drivers]) if drivers else 0
    remaining_races = max(0, int(ctx.games_per_team - avg_races))

    for _ in range(n_sims):
        pts = dict(current_pts)
        for _ in range(remaining_races):
            # Weighted random finish order
            order = list(name_list)
            rng.shuffle(order)
            weights = [ratings.get(d, 50) + rng.normal(0, 10) for d in order]
            finish = sorted(zip(order, weights), key=lambda x: x[1], reverse=True)
            for pos, (driver, _) in enumerate(finish):
                if pos < len(f1_points):
                    pts[driver] = pts.get(driver, 0) + f1_points[pos]

        ranked = sorted(name_list, key=lambda x: pts.get(x, 0), reverse=True)
        wdc_counts[ranked[0]] += 1
        for d in name_list:
            points_history[d].append(pts.get(d, 0))

    def _pct(c: int) -> float:
        return round(c / n_sims * 100, 2)

    return {
        "championship_probabilities": [
            {"name": d, "probability": _pct(wdc_counts[d])}
            for d in sorted(name_list, key=lambda x: wdc_counts[x], reverse=True)
            if wdc_counts[d] > 0
        ],
        "awards": {"wdc": [
            {"name": d, "simulations_won": wdc_counts[d], "probability": _pct(wdc_counts[d])}
            for d in sorted(name_list, key=lambda x: wdc_counts[x], reverse=True) if wdc_counts[d] > 0
        ]},
        "projected_points": {
            d.name: {
                "mean": round(float(np.mean(points_history[d.name])), 1),
                "current_points": current_pts.get(d.name, 0),
            }
            for d in drivers if points_history[d.name]
        },
        "team_strengths": [
            {"name": d.name, "rating": round(d.rating, 1),
             "wins": d.wins, "games_played": d.games_played}
            for d in sorted(drivers, key=lambda d: d.rating, reverse=True)
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
# NCAAB — March Madness 68-team bracket
# ═══════════════════════════════════════════════════════════════════════════

def _sim_ncaab(ctx: SportContext, n_sims: int,
               rng: np.random.Generator) -> dict[str, Any]:
    """Simulate March Madness bracket with upset probabilities."""
    teams = ctx.teams
    if not teams:
        return {"error": "No NCAAB data"}

    # Rank all teams by rating
    ranked = sorted(teams, key=lambda t: t.rating, reverse=True)
    ratings = {t.name: t.rating for t in teams}

    # Take top 68 for tournament field
    field = ranked[:68]
    field_names = [t.name for t in field]

    champ_counts: dict[str, int] = {t.name: 0 for t in field}
    final_four_counts: dict[str, int] = {t.name: 0 for t in field}
    elite_eight_counts: dict[str, int] = {t.name: 0 for t in field}
    sweet_sixteen_counts: dict[str, int] = {t.name: 0 for t in field}

    for _ in range(n_sims):
        # First Four: seeds 65-68 play in (4 games to get to 64)
        bracket_64 = list(field_names[:64])
        play_in_pairs = [(field_names[64], field_names[65]),
                         (field_names[66], field_names[67])] if len(field_names) >= 68 else []
        for a, b in play_in_pairs:
            wa = _champ_weight(ratings.get(a, 30), 15, 30)
            wb = _champ_weight(ratings.get(b, 30), 15, 30)
            winner = _weighted_pick([a, b], [wa, wb], rng)
            # Replace lowest seeds
            if len(bracket_64) >= 64:
                bracket_64[-1] = winner
            else:
                bracket_64.append(winner)

        bracket_64 = bracket_64[:64]

        # Split into 4 regions of 16
        regions = [bracket_64[i:i + 16] for i in range(0, 64, 16)]
        final_four_teams = []

        for region in regions:
            current_round = list(region)
            round_num = 1
            while len(current_round) > 1:
                next_rnd = []
                while len(current_round) >= 2:
                    a = current_round.pop(0)
                    b = current_round.pop(-1)
                    # Upset factor: lower-rated teams get a boost
                    ra = ratings.get(a, 50)
                    rb = ratings.get(b, 50)
                    # March Madness upset scaling
                    upset_factor = 0.85  # Slight boost to underdogs
                    wa = ra ** upset_factor
                    wb = rb ** upset_factor
                    winner = _weighted_pick([a, b], [wa, wb], rng)
                    next_rnd.append(winner)
                if current_round:
                    next_rnd.append(current_round.pop())
                current_round = next_rnd

                if round_num == 2:  # Sweet 16
                    for t in current_round:
                        sweet_sixteen_counts[t] = sweet_sixteen_counts.get(t, 0) + 1
                elif round_num == 3:  # Elite 8
                    for t in current_round:
                        elite_eight_counts[t] = elite_eight_counts.get(t, 0) + 1
                round_num += 1

            if current_round:
                final_four_teams.append(current_round[0])
                final_four_counts[current_round[0]] = final_four_counts.get(current_round[0], 0) + 1

        # Final Four
        if len(final_four_teams) >= 4:
            semi1 = _weighted_pick(
                [final_four_teams[0], final_four_teams[1]],
                [ratings.get(final_four_teams[0], 50), ratings.get(final_four_teams[1], 50)],
                rng,
            )
            semi2 = _weighted_pick(
                [final_four_teams[2], final_four_teams[3]],
                [ratings.get(final_four_teams[2], 50), ratings.get(final_four_teams[3], 50)],
                rng,
            )
            champion = _weighted_pick(
                [semi1, semi2],
                [ratings.get(semi1, 50), ratings.get(semi2, 50)],
                rng,
            )
            champ_counts[champion] = champ_counts.get(champion, 0) + 1

    def _pct(c: int) -> float:
        return round(c / n_sims * 100, 2)

    sorted_by_champ = sorted(field, key=lambda t: champ_counts.get(t.name, 0), reverse=True)

    return {
        "championship_probabilities": [
            {"name": t.name, "seed": i + 1, "probability": _pct(champ_counts.get(t.name, 0))}
            for i, t in enumerate(sorted_by_champ) if champ_counts.get(t.name, 0) > 0
        ],
        "march_madness_bracket": {
            "field_size": min(68, len(field)),
            "final_four_odds": [
                {"name": t.name, "probability": _pct(final_four_counts.get(t.name, 0))}
                for t in sorted(field, key=lambda t: final_four_counts.get(t.name, 0), reverse=True)[:16]
            ],
            "elite_eight_odds": [
                {"name": t.name, "probability": _pct(elite_eight_counts.get(t.name, 0))}
                for t in sorted(field, key=lambda t: elite_eight_counts.get(t.name, 0), reverse=True)[:16]
            ],
            "sweet_sixteen_odds": [
                {"name": t.name, "probability": _pct(sweet_sixteen_counts.get(t.name, 0))}
                for t in sorted(field, key=lambda t: sweet_sixteen_counts.get(t.name, 0), reverse=True)[:20]
            ],
        },
        "team_strengths": [
            {"name": t.name, "rating": round(t.rating, 1), "wins": t.wins,
             "losses": t.losses, "conference": t.conference}
            for t in sorted_by_champ[:30]
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
# NCAAF — CFP 12-team bracket + Bowl Predictions + Heisman
# ═══════════════════════════════════════════════════════════════════════════

BOWL_GAMES = [
    "Rose Bowl", "Sugar Bowl", "Orange Bowl", "Cotton Bowl", "Peach Bowl",
    "Fiesta Bowl", "Citrus Bowl", "Alamo Bowl", "Holiday Bowl", "Sun Bowl",
    "Las Vegas Bowl", "Music City Bowl", "Liberty Bowl", "Texas Bowl",
    "Pinstripe Bowl", "Gator Bowl", "Mayo Bowl", "Fenway Bowl",
    "Birmingham Bowl", "Gasparilla Bowl", "Cure Bowl", "New Mexico Bowl",
    "Armed Forces Bowl", "Independence Bowl", "Guaranteed Rate Bowl",
    "First Responder Bowl", "Hawaii Bowl", "Camellia Bowl", "Boca Raton Bowl",
    "New Orleans Bowl", "Myrtle Beach Bowl", "Bahamas Bowl",
    "Frisco Bowl", "LA Bowl", "Pop-Tarts Bowl", "Military Bowl",
    "Quick Lane Bowl", "Detroit Bowl", "Idaho Potato Bowl", "Famous Toastery Bowl",
]


def _sim_ncaaf(ctx: SportContext, n_sims: int,
               rng: np.random.Generator) -> dict[str, Any]:
    """Simulate CFP 12-team bracket, bowl games, and Heisman."""
    teams = ctx.teams
    if not teams:
        return {"error": "No NCAAF data"}

    ranked = sorted(teams, key=lambda t: t.rating, reverse=True)
    ratings = {t.name: t.rating for t in teams}

    cfp_field = ranked[:12]
    cfp_names = [t.name for t in cfp_field]

    champ_counts: dict[str, int] = {t: 0 for t in cfp_names}
    semifinal_counts: dict[str, int] = {t: 0 for t in cfp_names}
    quarterfinal_counts: dict[str, int] = {t: 0 for t in cfp_names}

    # Heisman
    heisman_counts: dict[str, int] = {}
    heisman_candidates = ctx.award_candidates.get("heisman", [])
    for c in heisman_candidates:
        heisman_counts[c.name] = 0

    for _ in range(n_sims):
        # CFP 12-team: top 4 get byes, 5-12 play first round
        bye_teams = cfp_names[:4]
        first_round_teams = cfp_names[4:12]

        # First round: 5v12, 6v11, 7v10, 8v9
        fr_winners = []
        matchups = []
        temp = list(first_round_teams)
        while len(temp) >= 2:
            matchups.append((temp.pop(0), temp.pop(-1)))
        for a, b in matchups:
            wa = _champ_weight(ratings.get(a, 50), 8, 12)
            wb = _champ_weight(ratings.get(b, 50), 8, 12)
            winner = _weighted_pick([a, b], [wa, wb], rng)
            fr_winners.append(winner)

        # Quarterfinals: bye teams vs first round winners
        qf_teams = list(bye_teams) + fr_winners
        for t in qf_teams:
            quarterfinal_counts[t] = quarterfinal_counts.get(t, 0) + 1

        qf_matchups = [(qf_teams[0], qf_teams[-1]), (qf_teams[1], qf_teams[-2]),
                       (qf_teams[2], qf_teams[-3]), (qf_teams[3], qf_teams[-4])] \
            if len(qf_teams) >= 8 else [(qf_teams[i], qf_teams[i + 1]) for i in range(0, len(qf_teams) - 1, 2)]

        sf_teams = []
        for a, b in qf_matchups:
            wa = _champ_weight(ratings.get(a, 50), 8, 12)
            wb = _champ_weight(ratings.get(b, 50), 8, 12)
            winner = _weighted_pick([a, b], [wa, wb], rng)
            sf_teams.append(winner)
            semifinal_counts[winner] = semifinal_counts.get(winner, 0) + 1

        # Semifinals
        if len(sf_teams) >= 4:
            s1 = _weighted_pick([sf_teams[0], sf_teams[1]],
                                [ratings.get(sf_teams[0], 50), ratings.get(sf_teams[1], 50)], rng)
            s2 = _weighted_pick([sf_teams[2], sf_teams[3]],
                                [ratings.get(sf_teams[2], 50), ratings.get(sf_teams[3], 50)], rng)
            champion = _weighted_pick([s1, s2], [ratings.get(s1, 50), ratings.get(s2, 50)], rng)
            champ_counts[champion] = champ_counts.get(champion, 0) + 1
        elif len(sf_teams) == 2:
            champion = _weighted_pick(sf_teams,
                                      [ratings.get(sf_teams[0], 50), ratings.get(sf_teams[1], 50)], rng)
            champ_counts[champion] = champ_counts.get(champion, 0) + 1

        # Heisman
        if heisman_candidates:
            wins_map = {t.name: t.wins for t in teams}
            winner = _award_pick(heisman_candidates, wins_map, ctx.games_per_team, rng)
            if winner in heisman_counts:
                heisman_counts[winner] += 1

    def _pct(c: int) -> float:
        return round(c / n_sims * 100, 2)

    # Bowl predictions (static based on rankings)
    bowl_predictions = []
    min_bowl_wins = max(1, ctx.games_per_team // 2)
    bowl_eligible = [t for t in ranked if t.wins >= min_bowl_wins][:80]
    if not bowl_eligible or len(bowl_eligible) < 2:
        bowl_eligible = ranked[:80]  # fallback: use all ranked teams
    for i, bowl in enumerate(BOWL_GAMES):
        if i * 2 + 1 < len(bowl_eligible):
            t1 = bowl_eligible[i * 2]
            t2 = bowl_eligible[i * 2 + 1]
            p1 = ratings.get(t1.name, 50) / (ratings.get(t1.name, 50) + ratings.get(t2.name, 50))
            bowl_predictions.append({
                "bowl": bowl,
                "team_1": t1.name, "team_2": t2.name,
                "team_1_win_prob": round(p1 * 100, 1),
                "team_2_win_prob": round((1 - p1) * 100, 1),
            })

    return {
        "championship_probabilities": [
            {"name": t, "probability": _pct(champ_counts.get(t, 0))}
            for t in sorted(cfp_names, key=lambda x: champ_counts.get(x, 0), reverse=True)
            if champ_counts.get(t, 0) > 0
        ],
        "cfp_bracket": {
            "field_size": 12,
            "semifinal_odds": [
                {"name": t, "probability": _pct(semifinal_counts.get(t, 0))}
                for t in sorted(cfp_names, key=lambda x: semifinal_counts.get(x, 0), reverse=True)
                if semifinal_counts.get(t, 0) > 0
            ],
            "quarterfinal_odds": [
                {"name": t, "probability": _pct(quarterfinal_counts.get(t, 0))}
                for t in sorted(cfp_names, key=lambda x: quarterfinal_counts.get(x, 0), reverse=True)
                if quarterfinal_counts.get(t, 0) > 0
            ],
        },
        "awards": {
            "heisman": [
                {"name": n, "simulations_won": c, "probability": _pct(c)}
                for n, c in sorted(heisman_counts.items(), key=lambda x: x[1], reverse=True)
                if c > 0
            ],
        },
        "bowl_predictions": bowl_predictions,
        "team_strengths": [
            {"name": t.name, "rating": round(t.rating, 1), "wins": t.wins,
             "losses": t.losses, "conference": t.conference}
            for t in ranked[:25]
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
# Dispatcher — route sport to correct simulator
# ═══════════════════════════════════════════════════════════════════════════

def _sim_soccer_league(ctx: SportContext, n_sims: int,
                       rng: np.random.Generator) -> dict[str, Any]:
    """Generic soccer simulator: title, top-N (UCL spots), relegation, BTTS/clean-sheet stats."""
    teams = ctx.teams
    sport = ctx.sport
    if not teams:
        return {"error": f"No team data for {sport}"}

    cfg = SPORT_CONFIG.get(sport, {})
    n_ucl = 4 if sport not in ("ucl",) else 1
    n_relegate = 3 if len(teams) >= 18 else 0
    name_list = [t.name for t in teams]

    title_counts: dict[str, int] = {t: 0 for t in name_list}
    top_n_counts: dict[str, int] = {t: 0 for t in name_list}
    relegation_counts: dict[str, int] = {t: 0 for t in name_list}
    wins_history: dict[str, list[int]] = {t: [] for t in name_list}

    award_counts: dict[str, dict[str, int]] = {}
    for ak in ctx.award_candidates:
        award_counts[ak] = {c.name: 0 for c in ctx.award_candidates[ak]}

    for _ in range(n_sims):
        wins = _simulate_wins(ctx, rng)
        draw_rate_default = cfg.get("draw_rate", 0.25)
        points: dict[str, float] = {}
        for t in teams:
            w = wins.get(t.name, 0)
            draw_rate = t.ties / max(t.games_played, 1) if t.ties else draw_rate_default
            draws = int(round((ctx.games_per_team - w) * draw_rate))
            points[t.name] = w * 3 + draws

        ranked = sorted(name_list, key=lambda x: points.get(x, 0), reverse=True)
        title_counts[ranked[0]] += 1
        for t in ranked[:n_ucl]:
            top_n_counts[t] += 1
        if n_relegate > 0:
            for t in ranked[-n_relegate:]:
                relegation_counts[t] += 1
        for t in name_list:
            wins_history[t].append(wins.get(t, 0))
        for ak, candidates in ctx.award_candidates.items():
            winner = _award_pick(candidates, wins, ctx.games_per_team, rng)
            if winner and winner in award_counts[ak]:
                award_counts[ak][winner] += 1

    def _pct(c: int) -> float:
        return round(c / n_sims * 100, 2)

    awards_out = {}
    for ak, counts in award_counts.items():
        awards_out[ak] = [
            {"name": n, "simulations_won": c, "probability": _pct(c)}
            for n, c in sorted(counts.items(), key=lambda x: x[1], reverse=True) if c > 0
        ]

    season = getattr(ctx, "season", str(datetime.now().year))
    stat_cols = cfg.get("player_stat_leaders", [])

    out: dict[str, Any] = {
        "championship_probabilities": [
            {"name": t, "probability": _pct(title_counts[t])}
            for t in sorted(name_list, key=lambda x: title_counts[x], reverse=True)
            if title_counts[t] > 0
        ],
        f"top_{n_ucl}_odds": [
            {"name": t, "probability": _pct(top_n_counts[t])}
            for t in sorted(name_list, key=lambda x: top_n_counts[x], reverse=True)
            if top_n_counts[t] > 0
        ],
        "awards": awards_out,
        "projected_wins": {
            t.name: {
                "mean": round(float(np.mean(wins_history[t.name])), 1),
                "median": float(np.median(wins_history[t.name])),
                "std": round(float(np.std(wins_history[t.name])), 2),
                "current_wins": t.wins, "current_losses": t.losses, "current_ties": t.ties,
            }
            for t in teams if wins_history[t.name]
        },
        "team_strengths": [
            {"name": t.name, "rating": round(t.rating, 1), "wins": t.wins,
             "losses": t.losses, "ties": t.ties, "games_played": t.games_played}
            for t in sorted(teams, key=lambda t: t.rating, reverse=True)
        ],
        "player_stat_leaders": _load_player_stat_leaders(sport, season, stat_cols, teams),
        "player_props_ou_lines": _load_player_props(sport, season, teams),
        "game_prediction_types": {
            "season_averages": _compute_game_pred_averages(sport, season, teams),
            "supported_types": cfg.get("game_pred_types", []),
        },
    }
    if n_relegate > 0:
        out["relegation_odds"] = [
            {"name": t, "probability": _pct(relegation_counts[t])}
            for t in sorted(name_list, key=lambda x: relegation_counts[x], reverse=True)
            if relegation_counts[t] > 0
        ]
    return out


def _sim_ufc(ctx: SportContext, n_sims: int,
             rng: np.random.Generator) -> dict[str, Any]:
    """UFC simulator: champion, method-of-victory rates, fighter stats, performance awards."""
    fighters = ctx.teams
    if not fighters:
        return {"error": "No UFC fighter data"}

    cfg = SPORT_CONFIG.get("ufc", {})
    name_list = [f.name for f in fighters]
    ratings = {f.name: f.rating for f in fighters}

    champ_counts: dict[str, int] = {n: 0 for n in name_list}
    title_defense_counts: dict[str, int] = {n: 0 for n in name_list}
    award_counts: dict[str, dict[str, int]] = {}
    for ak in ctx.award_candidates:
        award_counts[ak] = {c.name: 0 for c in ctx.award_candidates[ak]}

    # Method of victory from historical data
    season = getattr(ctx, "season", str(datetime.now().year))
    mov_data = _compute_game_pred_averages("ufc", season, fighters)
    mov = mov_data.get("method_of_victory", {})
    ko_rate = mov.get("ko_tko_rate", 0.29)
    sub_rate = mov.get("submission_rate", 0.18)
    dec_rate = mov.get("decision_rate", 0.53)

    for _ in range(n_sims):
        # Simulate a mini-tournament among top-rated fighters
        contenders = sorted(name_list, key=lambda x: ratings.get(x, 50) + rng.normal(0, 5), reverse=True)
        if len(contenders) >= 2:
            winner = contenders[0]
        else:
            winner = contenders[0] if contenders else "Unknown"
        champ_counts[winner] += 1

        # Title defense projection
        for ak, candidates in ctx.award_candidates.items():
            wins = {f.name: f.wins for f in fighters}
            w = _award_pick(candidates, wins, ctx.games_per_team, rng)
            if w and w in award_counts[ak]:
                award_counts[ak][w] += 1

    def _pct(c: int) -> float:
        return round(c / n_sims * 100, 2)

    awards_out = {}
    for ak, counts in award_counts.items():
        awards_out[ak] = [
            {"name": n, "simulations_won": c, "probability": _pct(c)}
            for n, c in sorted(counts.items(), key=lambda x: x[1], reverse=True) if c > 0
        ]

    stat_cols = cfg.get("player_stat_leaders", [])
    player_leaders = _load_player_stat_leaders("ufc", season, stat_cols, fighters)
    player_props = _load_player_props("ufc", season, fighters)

    return {
        "championship_probabilities": [
            {"name": n, "probability": _pct(champ_counts[n])}
            for n in sorted(name_list, key=lambda x: champ_counts[x], reverse=True)
            if champ_counts[n] > 0
        ],
        "fighter_ratings": [
            {"name": f.name, "rating": round(f.rating, 1), "wins": f.wins,
             "losses": f.losses, "games_played": f.games_played}
            for f in sorted(fighters, key=lambda f: f.rating, reverse=True)
        ],
        "method_of_victory_rates": mov,
        "game_prediction_types": {
            "season_averages": mov_data,
            "supported_types": cfg.get("game_pred_types", []),
        },
        "player_stat_leaders": player_leaders,
        "player_props_ou_lines": player_props,
        "awards": awards_out,
    }


def _sim_tennis(ctx: SportContext, n_sims: int,
                rng: np.random.Generator) -> dict[str, Any]:
    """Tennis simulator (ATP/WTA): year-end No.1, slam probabilities, serve/match stats."""
    players = ctx.teams
    sport = ctx.sport
    if not players:
        return {"error": f"No tennis player data for {sport}"}

    cfg = SPORT_CONFIG.get(sport, {})
    name_list = [p.name for p in players]
    ratings = {p.name: p.rating for p in players}

    no1_counts: dict[str, int] = {n: 0 for n in name_list}
    slam_counts: dict[str, int] = {n: 0 for n in name_list}
    award_counts: dict[str, dict[str, int]] = {}
    for ak in ctx.award_candidates:
        award_counts[ak] = {c.name: 0 for c in ctx.award_candidates[ak]}

    # Season still has remaining tournaments
    remaining_tournaments = max(0, ctx.games_per_team - int(np.mean([p.games_played for p in players])))
    slam_count_in_season = 4

    for _ in range(n_sims):
        pts: dict[str, float] = {}
        for p in players:
            pts[p.name] = _safe_float(p.pyth_pct, 0) * 2000  # rough ranking points proxy

        # Simulate remaining titles
        for _ in range(remaining_tournaments):
            order = sorted(name_list, key=lambda x: ratings.get(x, 50) + rng.normal(0, 8), reverse=True)
            winner = order[0] if order else name_list[0]
            pts[winner] = pts.get(winner, 0) + rng.integers(100, 1000)

        ranked_year = sorted(name_list, key=lambda x: pts.get(x, 0), reverse=True)
        no1_counts[ranked_year[0]] += 1

        # Simulate slam title (1 per sim — simplified)
        slam_probs = [ratings.get(n, 50) for n in name_list]
        total_sp = sum(slam_probs)
        if total_sp > 0:
            slam_probs_norm = [w / total_sp for w in slam_probs]
            slam_winner = rng.choice(name_list, p=slam_probs_norm)
            slam_counts[slam_winner] += 1

        for ak, candidates in ctx.award_candidates.items():
            wins = {p.name: p.wins for p in players}
            w = _award_pick(candidates, wins, ctx.games_per_team, rng)
            if w and w in award_counts[ak]:
                award_counts[ak][w] += 1

    def _pct(c: int) -> float:
        return round(c / n_sims * 100, 2)

    awards_out = {}
    for ak, counts in award_counts.items():
        awards_out[ak] = [
            {"name": n, "simulations_won": c, "probability": _pct(c)}
            for n, c in sorted(counts.items(), key=lambda x: x[1], reverse=True) if c > 0
        ]

    season = getattr(ctx, "season", str(datetime.now().year))
    stat_cols = cfg.get("player_stat_leaders", [])
    player_leaders = _load_player_stat_leaders(sport, season, stat_cols, players)
    player_props = _load_player_props(sport, season, players)
    match_stats = _compute_game_pred_averages(sport, season, players)

    return {
        "year_end_no1_probabilities": [
            {"name": n, "probability": _pct(no1_counts[n])}
            for n in sorted(name_list, key=lambda x: no1_counts[x], reverse=True)
            if no1_counts[n] > 0
        ],
        "slam_title_probabilities": [
            {"name": n, "probability": _pct(slam_counts[n])}
            for n in sorted(name_list, key=lambda x: slam_counts[x], reverse=True)
            if slam_counts[n] > 0
        ],
        "championship_probabilities": [
            {"name": n, "probability": _pct(no1_counts[n])}
            for n in sorted(name_list, key=lambda x: no1_counts[x], reverse=True)
            if no1_counts[n] > 0
        ],
        "player_ratings": [
            {"name": p.name, "rating": round(p.rating, 1), "wins": p.wins,
             "losses": p.losses, "win_pct": round(p.wins / max(p.games_played, 1), 3)}
            for p in sorted(players, key=lambda p: p.rating, reverse=True)
        ],
        "match_stats": match_stats,
        "game_prediction_types": {
            "season_averages": match_stats,
            "supported_types": cfg.get("game_pred_types", []),
        },
        "player_stat_leaders": player_leaders,
        "player_props_ou_lines": player_props,
        "awards": awards_out,
    }



def simulate_sport(sport: str, n_sims: int = 10_000,
                   season: str | None = None,
                   seed: int | None = None) -> dict[str, Any]:
    """Run Monte Carlo simulation for a single sport."""
    t0 = time.time()
    log.info("Loading data for %s …", sport.upper())

    ctx = load_sport_context(sport, season)
    if not ctx.teams:
        log.warning("No teams loaded for %s — skipping", sport)
        return {"sport": sport, "error": "insufficient_data"}

    log.info("  %d teams loaded, season %s (%.0f%% complete), %d remaining games",
             len(ctx.teams), ctx.season, ctx.season_completion_pct, len(ctx.remaining_games))

    rng = np.random.default_rng(seed)

    # Route to sport-specific simulator
    SOCCER_LEAGUES = {"laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl"}
    if sport == "epl":
        result = _sim_epl(ctx, n_sims, rng)
    elif sport in SOCCER_LEAGUES:
        result = _sim_soccer_league(ctx, n_sims, rng)
    elif sport == "f1":
        result = _sim_f1(ctx, n_sims, rng)
    elif sport == "ncaab":
        result = _sim_ncaab(ctx, n_sims, rng)
    elif sport == "ncaaw":
        result = _sim_ncaab(ctx, n_sims, rng)  # uses same March Madness-style bracket logic
    elif sport == "ncaaf":
        result = _sim_ncaaf(ctx, n_sims, rng)
    elif sport == "ufc":
        result = _sim_ufc(ctx, n_sims, rng)
    elif sport in ("atp", "wta"):
        result = _sim_tennis(ctx, n_sims, rng)
    else:
        result = _sim_league_sport(ctx, n_sims, rng)

    elapsed = time.time() - t0
    log.info("  %s done in %.1fs (%d sims)", sport.upper(), elapsed, n_sims)

    # Wrap with metadata
    return {
        "sport": sport.upper(),
        "sport_key": sport,
        "simulations": n_sims,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "season": ctx.season,
        "season_completion_pct": round(ctx.season_completion_pct, 1),
        "schedule_based": len(ctx.remaining_games) > 0,
        "teams_count": len(ctx.teams),
        **result,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Output
# ═══════════════════════════════════════════════════════════════════════════

def _save_results(sport: str, data: dict[str, Any]) -> Path:
    """Save simulation results to dated JSON file."""
    today = datetime.now().strftime("%Y-%m-%d")
    out_dir = SIM_OUT / today
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{sport}.json"

    # Also save to a "latest" symlink-like file
    latest_dir = SIM_OUT / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_path = latest_dir / f"{sport}.json"

    for path in (out_path, latest_path):
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    log.info("  Saved → %s", out_path)
    return out_path


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="V5.0 Season Simulator — Monte Carlo projection engine"
    )
    parser.add_argument(
        "--sport", type=str, default="all",
        help=f"Sport to simulate (or 'all'). Options: {', '.join(SIMULATABLE_SPORTS)}",
    )
    parser.add_argument("--simulations", "-n", type=int, default=10_000)
    parser.add_argument("--season", type=str, default=None,
                        help="Season year (default: auto-detect current)")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed for reproducibility")
    parser.add_argument("--live", action="store_true", default=True,
                        help="Use current season data (default)")
    parser.add_argument("--output", type=str, default=None,
                        help="Custom output path (default: data/simulations/)")
    parser.add_argument("--workers", type=int, default=4,
                        help="Max parallel workers for multi-sport")
    args = parser.parse_args()

    sports = SIMULATABLE_SPORTS if args.sport == "all" else [s.strip().lower() for s in args.sport.split(",")]
    for s in sports:
        if s not in SIMULATABLE_SPORTS:
            log.error("Unknown sport: %s. Valid: %s", s, ", ".join(SIMULATABLE_SPORTS))
            sys.exit(1)

    log.info("Season Simulator v5.0 — %d simulations, sports: %s",
             args.simulations, ", ".join(s.upper() for s in sports))

    t_start = time.time()
    results: dict[str, dict] = {}

    if len(sports) > 1:
        with ThreadPoolExecutor(max_workers=min(args.workers, len(sports))) as pool:
            futures = {
                pool.submit(simulate_sport, s, args.simulations, args.season, args.seed): s
                for s in sports
            }
            for future in as_completed(futures):
                sport_key = futures[future]
                try:
                    data = future.result()
                    results[sport_key] = data
                except Exception as e:
                    log.error("  %s failed: %s", sport_key.upper(), e)
                    results[sport_key] = {"sport": sport_key, "error": str(e)}
    else:
        for s in sports:
            try:
                results[s] = simulate_sport(s, args.simulations, args.season, args.seed)
            except Exception as e:
                log.error("  %s failed: %s", s.upper(), e)
                results[s] = {"sport": s, "error": str(e)}

    # Save all results
    for sport_key, data in results.items():
        if args.output:
            out = Path(args.output)
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w") as f:
                json.dump(data, f, indent=2, default=str)
            log.info("Saved → %s", out)
        else:
            _save_results(sport_key, data)

    total_time = time.time() - t_start

    # Write manifest
    manifest = {
        "sports_simulated": list(results.keys()),
        "simulations_per_sport": args.simulations,
        "total_time_seconds": round(total_time, 1),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = SIM_OUT / "latest" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    log.info("══════════════════════════════════════════")
    log.info("Done! %d sport(s) in %.1fs", len(results), total_time)
    for s, d in results.items():
        status = "✓" if "error" not in d else f"✗ {d['error']}"
        log.info("  %s: %s", s.upper(), status)
    log.info("══════════════════════════════════════════")


if __name__ == "__main__":
    main()
