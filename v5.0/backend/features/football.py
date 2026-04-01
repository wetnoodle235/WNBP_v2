# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Football
# ──────────────────────────────────────────────────────────
#
# Covers NFL and NCAAF.  Produces ~45 features per game
# including EPA-based metrics, rushing/passing balance,
# turnovers, red zone, third down, special teams, and
# weather impact.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)


class FootballExtractor(BaseFeatureExtractor):
    """Feature extractor for American football (NFL, NCAAF)."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._all_games_cache: pd.DataFrame | None = None

    def _load_all_games(self) -> pd.DataFrame:
        """Load and cache all seasons' game data for cross-season form calculations."""
        if self._all_games_cache is not None:
            return self._all_games_cache
        sport_dir = self.data_dir / "normalized" / self.sport
        frames = []
        for p in sorted(sport_dir.glob("games_*.parquet")):
            try:
                df = pd.read_parquet(p)
                frames.append(df)
            except Exception:
                pass
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not combined.empty and "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
            combined.sort_values("date", inplace=True, ignore_index=True)
        self._all_games_cache = combined
        return combined

    # ── Helpers ────────────────────────────────────────────

    def _stat(self, game: dict, prefix: str, key: str, default: float = 0.0) -> float:
        return float(pd.to_numeric(game.get(f"{prefix}{key}", default), errors="coerce") or default)

    def _team_stat_averages(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Rolling averages of key box-score stats."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "pass_yds_pg": 0.0, "rush_yds_pg": 0.0, "total_yds_pg": 0.0,
                "pass_yds_allowed_pg": 0.0, "rush_yds_allowed_pg": 0.0,
                "turnovers_pg": 0.0, "takeaways_pg": 0.0,
                "sacks_pg": 0.0, "penalties_pg": 0.0,
                "sacks_allowed_pg": 0.0, "completion_pct": 0.0,
                "first_downs_pg": 0.0, "penalty_yards_pg": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        def _avg(stat: str, opp: bool = False) -> float:
            vals = []
            for row, h in zip(records, is_home):
                if opp:
                    p = "away_" if h else "home_"
                else:
                    p = "home_" if h else "away_"
                vals.append(pd.to_numeric(row.get(f"{p}{stat}", 0), errors="coerce") or 0.0)
            return float(np.mean(vals)) if vals else 0.0

        return {
            "pass_yds_pg": _avg("passing_yards"),
            "rush_yds_pg": _avg("rushing_yards"),
            "total_yds_pg": _avg("passing_yards") + _avg("rushing_yards"),
            "pass_yds_allowed_pg": _avg("passing_yards", opp=True),
            "rush_yds_allowed_pg": _avg("rushing_yards", opp=True),
            "turnovers_pg": _avg("turnovers"),
            "takeaways_pg": _avg("turnovers", opp=True),
            "sacks_pg": _avg("sacks"),
            "penalties_pg": _avg("penalties"),
            "sacks_allowed_pg": _avg("sacks_allowed"),
            "completion_pct": _avg("completion_pct"),
            "first_downs_pg": _avg("first_downs"),
            "penalty_yards_pg": _avg("penalty_yards"),
            "passing_tds_pg": _avg("passing_touchdowns"),
            "rushing_tds_pg": _avg("rushing_touchdowns"),
            "total_tds_pg": _avg("passing_touchdowns") + _avg("rushing_touchdowns"),
            "tds_allowed_pg": _avg("passing_touchdowns", opp=True) + _avg("rushing_touchdowns", opp=True),
            "yards_per_play": _avg("yards_per_play"),
            "yards_per_play_allowed": _avg("yards_per_play", opp=True),
            "receiving_yds_pg": _avg("receiving_yards"),
        }

    def _epa_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """EPA (Expected Points Added) features from nflfastr-style data."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "epa_pass_off": 0.0, "epa_rush_off": 0.0, "epa_total_off": 0.0,
                "epa_pass_def": 0.0, "epa_rush_def": 0.0, "epa_total_def": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        def _epa_avg(stat: str, opp: bool = False) -> float:
            vals = []
            for row, h in zip(records, is_home):
                p = ("away_" if h else "home_") if opp else ("home_" if h else "away_")
                vals.append(pd.to_numeric(row.get(f"{p}{stat}", 0), errors="coerce") or 0.0)
            return float(np.mean(vals)) if vals else 0.0

        epa_pass_off = _epa_avg("passing_epa")
        epa_rush_off = _epa_avg("rushing_epa")
        epa_pass_def = _epa_avg("passing_epa", opp=True)
        epa_rush_def = _epa_avg("rushing_epa", opp=True)

        air_off = _epa_avg("air_yards")
        yac_off = _epa_avg("yac")
        air_def = _epa_avg("air_yards", opp=True)
        yac_def = _epa_avg("yac", opp=True)

        return {
            "epa_pass_off": epa_pass_off,
            "epa_rush_off": epa_rush_off,
            "epa_total_off": epa_pass_off + epa_rush_off,
            "epa_pass_def": epa_pass_def,
            "epa_rush_def": epa_rush_def,
            "epa_total_def": epa_pass_def + epa_rush_def,
            "air_yards_pg": air_off,
            "yac_pg": yac_off,
            "air_yards_allowed_pg": air_def,
            "yac_allowed_pg": yac_def,
        }

    def _efficiency_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Third-down, fourth-down, red-zone, and time-of-possession features."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "third_down_pct": 0.0, "red_zone_pct": 0.0, "fourth_down_pct": 0.0,
                "time_of_possession": 0.0, "turnover_margin": 0.0, "defensive_td_pg": 0.0,
            }

        is_home = (recent["home_team_id"] == team_id).values
        n = len(recent)

        def _col_team(home_col: str, away_col: str, default: float = 0.0) -> pd.Series:
            zero = pd.Series(np.full(n, default))
            h = pd.to_numeric(recent[home_col], errors="coerce").fillna(default) if home_col in recent.columns else zero
            a = pd.to_numeric(recent[away_col], errors="coerce").fillna(default) if away_col in recent.columns else zero
            return pd.Series(np.where(is_home, h.values, a.values))

        third_conv = _col_team("home_third_down_conv", "away_third_down_conv").sum()
        third_att = _col_team("home_third_down_att", "away_third_down_att").sum()
        fourth_conv = _col_team("home_fourth_down_conv", "away_fourth_down_conv").sum()
        fourth_att = _col_team("home_fourth_down_att", "away_fourth_down_att").sum()
        red_zone_pct = _col_team("home_red_zone_pct", "away_red_zone_pct", default=0.0)
        top_secs = _col_team("home_possession_seconds", "away_possession_seconds", default=1800.0)
        top_mins = top_secs / 60.0  # convert to minutes
        team_to = _col_team("home_turnovers", "away_turnovers")
        opp_to = _col_team("away_turnovers", "home_turnovers")
        def_td = _col_team("home_defensive_tds", "away_defensive_tds", default=0.0)

        rz_valid = red_zone_pct[red_zone_pct > 0]
        rz_mean = float(rz_valid.mean()) if len(rz_valid) > 0 else 0.0

        return {
            "third_down_pct": float(third_conv / third_att) if third_att > 0 else 0.0,
            "fourth_down_pct": float(fourth_conv / fourth_att) if fourth_att > 0 else 0.0,
            "red_zone_pct": rz_mean,
            "time_of_possession": float(top_mins.mean()),
            "turnover_margin": float((opp_to - team_to).mean()),
            "defensive_td_pg": float(def_td.mean()),
        }

    def _rushing_passing_balance(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Ratio of rushing to passing plays and yards."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"rush_pct": 0.5, "pass_pct": 0.5, "yards_per_play": 0.0}

        is_home = recent["home_team_id"] == team_id
        is_home = (recent["home_team_id"] == team_id).values
        n = len(recent)

        def _col_team(home_col: str, away_col: str, default: float = 0.0) -> pd.Series:
            zero = pd.Series(np.full(n, default))
            h = pd.to_numeric(recent[home_col], errors="coerce").fillna(default) if home_col in recent.columns else zero
            a = pd.to_numeric(recent[away_col], errors="coerce").fillna(default) if away_col in recent.columns else zero
            return pd.Series(np.where(is_home, h.values, a.values))

        ra = _col_team("home_rushing_attempts", "away_rushing_attempts")
        pa = _col_team("home_pass_attempts", "away_pass_attempts")
        ry = _col_team("home_rushing_yards", "away_rushing_yards")
        py = _col_team("home_passing_yards", "away_passing_yards")
        rush_att = ra.sum()
        pass_att = pa.sum()
        total_yds = (ry + py).sum()
        total_plays = (ra + pa).sum()

        total_att = rush_att + pass_att
        return {
            "rush_pct": float(rush_att / total_att) if total_att > 0 else 0.5,
            "pass_pct": float(pass_att / total_att) if total_att > 0 else 0.5,
            "yards_per_play": float(total_yds / total_plays) if total_plays > 0 else 0.0,
        }

    def _special_teams(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Punt return, kick return, and field goal features."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"fg_pct": 0.0, "punt_avg": 0.0, "kick_return_avg": 0.0}

        is_home = (recent["home_team_id"] == team_id).values
        n = len(recent)

        def _col_team(home_col: str, away_col: str) -> pd.Series:
            zero = pd.Series(np.zeros(n))
            h = pd.to_numeric(recent[home_col], errors="coerce").fillna(0) if home_col in recent.columns else zero
            a = pd.to_numeric(recent[away_col], errors="coerce").fillna(0) if away_col in recent.columns else zero
            return pd.Series(np.where(is_home, h.values, a.values))

        fg_made = _col_team("home_field_goals_made", "away_field_goals_made").sum()
        fg_att = _col_team("home_field_goals_attempted", "away_field_goals_attempted").sum()
        punt_yds = _col_team("home_punt_yards", "away_punt_yards").sum()
        # No punt count col; approximate from punt_yards (avg ~40 yards/punt)
        punt_count = n  # normalized value: punt_yards / n ≈ avg punt yardage

        return {
            "fg_pct": float(fg_made / fg_att) if fg_att > 0 else 0.0,
            "punt_avg": float(punt_yds / (n * 1.0)) if n > 0 else 0.0,
            "kick_return_avg": 0.0,
        }

    def _weather_impact(self, game: dict[str, Any]) -> dict[str, float]:
        """Weather features that affect gameplay (wind, temp, dome)."""
        return {
            "temperature": float(pd.to_numeric(game.get("temperature", 70), errors="coerce") or 70.0),
            "wind_speed": float(pd.to_numeric(game.get("wind_speed", 0), errors="coerce") or 0.0),
            "is_dome": 1.0 if str(game.get("dome", game.get("is_dome", ""))).lower() in ("true", "1", "yes") else 0.0,
            "is_rain": 1.0 if "rain" in str(game.get("weather", "")).lower() else 0.0,
            "is_snow": 1.0 if "snow" in str(game.get("weather", "")).lower() else 0.0,
        }

    def _get_ps_team_index(self, season: int) -> dict[str, pd.DataFrame]:
        """Build and cache per-team player stats index for O(1) team lookups."""
        if not hasattr(self, "_ps_team_index_cache"):
            self._ps_team_index_cache: dict[str, dict[str, pd.DataFrame]] = {}
        key = str(season)
        if key not in self._ps_team_index_cache:
            ps = self.load_player_stats(season)
            if ps.empty or "team_id" not in ps.columns:
                self._ps_team_index_cache[key] = {}
            else:
                ps = ps.copy()
                ps["_team_id_str"] = ps["team_id"].astype(str)
                if "date" in ps.columns:
                    ps["_date_dt"] = pd.to_datetime(ps["date"], errors="coerce")
                    ps.sort_values("_date_dt", inplace=True, ignore_index=True)
                self._ps_team_index_cache[key] = {
                    tid: grp for tid, grp in ps.groupby("_team_id_str")
                }
        return self._ps_team_index_cache[key]

    def _nfl_player_features(
        self,
        team_id: str,
        season: int,
        date: str,
    ) -> dict[str, float]:
        """NFL-specific individual player stats: QB rating, rushing, defensive production."""
        defaults = {
            "nfl_ps_qb_rating": 0.0,
            "nfl_ps_pass_yds_pg": 0.0,
            "nfl_ps_rush_yds_pg": 0.0,
            "nfl_ps_sacks_pg": 0.0,
            "nfl_ps_def_int_pg": 0.0,
            "nfl_ps_rush_td_pg": 0.0,
        }
        team_index = self._get_ps_team_index(season)
        if not team_index:
            return defaults

        tid = str(team_id)
        team_ps = team_index.get(tid)
        if team_ps is None or team_ps.empty:
            self._build_team_id_map(season)
            mapped_id = self._team_id_map.get(tid)
            if mapped_id:
                team_ps = team_index.get(str(mapped_id))

        if team_ps is None or team_ps.empty or "date" not in team_ps.columns:
            return defaults

        # Date filter: only use games before this game (O(k) on small per-team subset)
        game_date = pd.to_datetime(date, errors="coerce")
        if not pd.isna(game_date):
            date_col = team_ps.get("_date_dt", pd.to_datetime(team_ps["date"], errors="coerce"))
            team_ps = team_ps.loc[date_col < game_date]

        if team_ps.empty:
            return defaults

        def _mean(col: str) -> float:
            if col in team_ps.columns:
                return float(pd.to_numeric(team_ps[col], errors="coerce").fillna(0).mean())
            return 0.0

        # Passing: QB passer rating and yards per game
        qb_mask = pd.to_numeric(team_ps.get("pass_att", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0
        qb_rows = team_ps.loc[qb_mask]
        qb_rating = float(pd.to_numeric(qb_rows.get("pass_rating", pd.Series(dtype=float)), errors="coerce").fillna(0).mean()) if not qb_rows.empty else 0.0
        qb_yds = float(pd.to_numeric(qb_rows.get("pass_yds", pd.Series(dtype=float)), errors="coerce").fillna(0).mean()) if not qb_rows.empty else 0.0

        # Rushing: average rush yards per game  
        rush_yds = _mean("rush_yds")
        rush_td = _mean("rush_td")

        # Defensive: sacks and interceptions per player per game
        sacks = _mean("sacks")
        def_int = _mean("interceptions")

        return {
            "nfl_ps_qb_rating": qb_rating,
            "nfl_ps_pass_yds_pg": qb_yds,
            "nfl_ps_rush_yds_pg": rush_yds,
            "nfl_ps_sacks_pg": sacks,
            "nfl_ps_def_int_pg": def_int,
            "nfl_ps_rush_td_pg": rush_td,
        }

    # ── Main Extraction ───────────────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        games_df = self._load_all_games()
        current_season_games = self.load_games(season)
        odds_df = self.load_odds(season)

        h_id, a_id = self._resolve_game_team_ids(game, current_season_games)
        date = str(game.get("date", ""))
        game_id = str(game.get("id", ""))
        home_team = str(game.get("home_team", ""))
        away_team = str(game.get("away_team", ""))

        features: dict[str, Any] = {
            "game_id": game_id,
            "date": date,
            "home_team_id": h_id,
            "away_team_id": a_id,
            "home_score": pd.to_numeric(game.get("home_score"), errors="coerce"),
            "away_score": pd.to_numeric(game.get("away_score"), errors="coerce"),
            # Period/quarter scores — passed through as targets for extra-market models
            "home_q1": pd.to_numeric(game.get("home_q1"), errors="coerce"),
            "home_q2": pd.to_numeric(game.get("home_q2"), errors="coerce"),
            "home_q3": pd.to_numeric(game.get("home_q3"), errors="coerce"),
            "home_q4": pd.to_numeric(game.get("home_q4"), errors="coerce"),
            "home_ot": pd.to_numeric(game.get("home_ot"), errors="coerce"),
            "away_q1": pd.to_numeric(game.get("away_q1"), errors="coerce"),
            "away_q2": pd.to_numeric(game.get("away_q2"), errors="coerce"),
            "away_q3": pd.to_numeric(game.get("away_q3"), errors="coerce"),
            "away_q4": pd.to_numeric(game.get("away_q4"), errors="coerce"),
            "away_ot": pd.to_numeric(game.get("away_ot"), errors="coerce"),
        }

        # Common features
        h_form = self.team_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        a_form = self.team_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_form.items()})

        h2h = self.head_to_head(h_id, a_id, games_df, date=date)
        features.update(h2h)

        features["home_momentum"] = self.momentum(h_id, date, games_df)
        features["away_momentum"] = self.momentum(a_id, date, games_df)
        features["momentum_diff"] = features["home_momentum"] - features["away_momentum"]

        h_splits = self.home_away_splits(h_id, games_df, season)
        features["home_home_win_pct"] = h_splits["home_win_pct"]
        a_splits = self.home_away_splits(a_id, games_df, season)
        features["away_away_win_pct"] = a_splits["away_win_pct"]

        # Rest
        features["home_rest_days"] = float(self.rest_days(h_id, date, games_df))
        features["away_rest_days"] = float(self.rest_days(a_id, date, games_df))
        features["rest_advantage"] = features["home_rest_days"] - features["away_rest_days"]
        features["short_week_home"] = 1.0 if features["home_rest_days"] < 7 else 0.0
        features["short_week_away"] = 1.0 if features["away_rest_days"] < 7 else 0.0

        # EPA
        h_epa = self._epa_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_epa.items()})
        a_epa = self._epa_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_epa.items()})

        # Yardage & stat averages
        h_stats = self._team_stat_averages(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_stats.items()})
        a_stats = self._team_stat_averages(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_stats.items()})

        # Efficiency
        h_eff = self._efficiency_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_eff.items()})
        a_eff = self._efficiency_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_eff.items()})

        # Stat differentials (key predictive signals)
        features["total_yds_diff"] = features.get("home_total_yds_pg", 0.0) - features.get("away_total_yds_pg", 0.0)
        features["turnovers_diff"] = features.get("away_turnovers_pg", 0.0) - features.get("home_turnovers_pg", 0.0)
        features["sacks_diff"] = features.get("home_sacks_pg", 0.0) - features.get("away_sacks_pg", 0.0)
        features["tds_diff"] = features.get("home_total_tds_pg", 0.0) - features.get("away_total_tds_pg", 0.0)
        features["yards_per_play_diff"] = features.get("home_yards_per_play", 0.0) - features.get("away_yards_per_play", 0.0)
        features["first_downs_diff"] = features.get("home_first_downs_pg", 0.0) - features.get("away_first_downs_pg", 0.0)
        # EPA differentials — net expected points advantage
        features["epa_pass_off_diff"] = features.get("home_epa_pass_off", 0.0) - features.get("away_epa_pass_off", 0.0)
        features["epa_rush_off_diff"] = features.get("home_epa_rush_off", 0.0) - features.get("away_epa_rush_off", 0.0)
        features["epa_total_off_diff"] = features.get("home_epa_total_off", 0.0) - features.get("away_epa_total_off", 0.0)
        features["epa_net_diff"] = features.get("home_epa_total_off", 0.0) - features.get("away_epa_total_off", 0.0) \
            - features.get("home_epa_total_def", 0.0) + features.get("away_epa_total_def", 0.0)
        # Air yards & YAC differential (passing depth and after-catch efficiency)
        features["air_yards_diff"] = features.get("home_air_yards_pg", 0.0) - features.get("away_air_yards_pg", 0.0)
        features["yac_diff"] = features.get("home_yac_pg", 0.0) - features.get("away_yac_pg", 0.0)
        # Efficiency differentials
        features["completion_pct_diff"] = features.get("home_completion_pct", 0.0) - features.get("away_completion_pct", 0.0)
        features["third_down_pct_diff"] = features.get("home_third_down_pct", 0.0) - features.get("away_third_down_pct", 0.0)
        features["red_zone_pct_diff"] = features.get("home_red_zone_pct", 0.0) - features.get("away_red_zone_pct", 0.0)
        features["turnover_margin_diff"] = features.get("home_turnover_margin", 0.0) - features.get("away_turnover_margin", 0.0)

        # Rush/pass balance
        h_bal = self._rushing_passing_balance(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_bal.items()})
        a_bal = self._rushing_passing_balance(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_bal.items()})

        # Special teams
        h_st = self._special_teams(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_st.items()})
        a_st = self._special_teams(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_st.items()})

        # Weather
        weather = self._weather_impact(game)
        features.update(weather)

        # ELO ratings
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Odds — with team-name fallback
        odds = self._odds_features(game_id, odds_df, home_team=home_team, away_team=away_team, date=date)
        features.update(odds)

        # Market movement enrichment
        market = self._market_signal_features(game_id, season, home_team=home_team, away_team=away_team, date=date)
        features.update(market)

        # Schedule fatigue enrichment
        h_fat = self._schedule_fatigue_features(game_id, h_id, season)
        features.update({f"home_{k}": v for k, v in h_fat.items()})
        a_fat = self._schedule_fatigue_features(game_id, a_id, season)
        features.update({f"away_{k}": v for k, v in a_fat.items()})
        features["fatigue_score_diff"] = h_fat["fatigue_score"] - a_fat["fatigue_score"]

        # NFL Player stats (QB rating, rushing, defensive production)
        h_nfl_ps = self._nfl_player_features(h_id, season, date)
        features.update({f"home_{k}": v for k, v in h_nfl_ps.items()})
        a_nfl_ps = self._nfl_player_features(a_id, season, date)
        features.update({f"away_{k}": v for k, v in a_nfl_ps.items()})
        # NFL player stats differentials
        for key in h_nfl_ps:
            features[f"{key}_diff"] = h_nfl_ps[key] - a_nfl_ps[key]

        # Season kicker FG% from team_stats (100% filled for NFL)
        team_stats = self.load_team_stats(season)
        if not team_stats.empty and "team_id" in team_stats.columns and "field_goal_pct" in team_stats.columns:
            ts = team_stats.copy()
            ts["_tid"] = ts["team_id"].astype(str)
            h_row = ts.loc[ts["_tid"] == str(h_id)]
            a_row = ts.loc[ts["_tid"] == str(a_id)]
            h_kfg = float(pd.to_numeric(h_row["field_goal_pct"].iloc[0], errors="coerce")) / 100.0 if not h_row.empty else 0.0
            a_kfg = float(pd.to_numeric(a_row["field_goal_pct"].iloc[0], errors="coerce")) / 100.0 if not a_row.empty else 0.0
        else:
            h_kfg, a_kfg = 0.0, 0.0
        features["home_kicker_fg_pct"] = h_kfg
        features["away_kicker_fg_pct"] = a_kfg
        features["kicker_fg_pct_diff"] = h_kfg - a_kfg

        # ── Quarter Rolling Stats (NFL: Q1/Q2/Q3/Q4) ─────
        h_per = self._period_rolling_stats(h_id, date, games_df, n=10, period_scheme="quarters")
        features.update({f"home_{k}": v for k, v in h_per.items()})
        a_per = self._period_rolling_stats(a_id, date, games_df, n=10, period_scheme="quarters")
        features.update({f"away_{k}": v for k, v in a_per.items()})
        features["period_first_ppg_diff"] = h_per["period_first_ppg"] - a_per["period_first_ppg"]
        features["period_first_win_pct_diff"] = h_per["period_first_win_pct"] - a_per["period_first_win_pct"]
        features["period_first_half_win_pct_diff"] = h_per["period_first_half_win_pct"] - a_per["period_first_half_win_pct"]
        features["period_comeback_diff"] = h_per["period_comeback_rate"] - a_per["period_comeback_rate"]
        features["period_first_opp_ppg_diff"] = h_per["period_first_opp_ppg"] - a_per["period_first_opp_ppg"]
        features["period_second_half_ppg_diff"] = h_per["period_second_half_ppg"] - a_per["period_second_half_ppg"]
        features["period_ot_rate_diff"] = h_per["period_ot_rate"] - a_per["period_ot_rate"]

        # Injury burden (uses shared base._injury_features)
        h_inj = self._injury_features(h_id, season)
        features.update({f"home_{k}": v for k, v in h_inj.items()})
        a_inj = self._injury_features(a_id, season)
        features.update({f"away_{k}": v for k, v in a_inj.items()})
        features["injury_advantage"] = a_inj["injury_severity_score"] - h_inj["injury_severity_score"]

        return features

    def get_feature_names(self) -> list[str]:
        return [
            # Home/away form
            "home_form_win_pct", "home_form_ppg", "home_form_opp_ppg",
            "home_form_avg_margin", "home_form_games_played",
            "away_form_win_pct", "away_form_ppg", "away_form_opp_ppg",
            "away_form_avg_margin", "away_form_games_played",
            # H2H
            "h2h_games", "h2h_win_pct", "h2h_avg_margin",
            # Momentum & splits
            "home_momentum", "away_momentum", "momentum_diff",
            "home_home_win_pct", "away_away_win_pct",
            # Rest
            "home_rest_days", "away_rest_days", "rest_advantage",
            "short_week_home", "short_week_away",
            # EPA
            "home_epa_pass_off", "home_epa_rush_off", "home_epa_total_off",
            "home_epa_pass_def", "home_epa_rush_def", "home_epa_total_def",
            "away_epa_pass_off", "away_epa_rush_off", "away_epa_total_off",
            "away_epa_pass_def", "away_epa_rush_def", "away_epa_total_def",
            # Stat averages
            "home_pass_yds_pg", "home_rush_yds_pg", "home_total_yds_pg",
            "home_pass_yds_allowed_pg", "home_rush_yds_allowed_pg",
            "home_turnovers_pg", "home_takeaways_pg", "home_sacks_pg", "home_penalties_pg",
            "home_sacks_allowed_pg", "home_completion_pct", "home_first_downs_pg", "home_penalty_yards_pg",
            "away_pass_yds_pg", "away_rush_yds_pg", "away_total_yds_pg",
            "away_pass_yds_allowed_pg", "away_rush_yds_allowed_pg",
            "away_turnovers_pg", "away_takeaways_pg", "away_sacks_pg", "away_penalties_pg",
            "away_sacks_allowed_pg", "away_completion_pct", "away_first_downs_pg", "away_penalty_yards_pg",
            # Efficiency
            "home_third_down_pct", "home_red_zone_pct", "home_fourth_down_pct",
            "home_time_of_possession", "home_turnover_margin", "home_defensive_td_pg",
            "away_third_down_pct", "away_red_zone_pct", "away_fourth_down_pct",
            "away_time_of_possession", "away_turnover_margin", "away_defensive_td_pg",
            # Rush/pass balance
            "home_rush_pct", "home_pass_pct", "home_yards_per_play",
            "away_rush_pct", "away_pass_pct", "away_yards_per_play",
            # Special teams
            "home_fg_pct", "home_punt_avg", "home_kick_return_avg",
            "away_fg_pct", "away_punt_avg", "away_kick_return_avg",
            # Weather
            "temperature", "wind_speed", "is_dome", "is_rain", "is_snow",
            # ELO ratings
            "home_elo", "home_elo_diff", "home_elo_expected_win",
            "away_elo", "away_elo_diff", "away_elo_expected_win",
            # Odds
            "home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob",
            # Market signals
            "market_aggregate_abs_move", "market_h2h_home_move", "market_h2h_away_move",
            "market_spread_home_move", "market_total_line_move",
            "market_observation_count", "market_source_count",
            "market_regime_stable", "market_regime_moving", "market_regime_volatile",
            # Schedule fatigue
            "home_fatigue_rest_days", "home_fatigue_is_back_to_back", "home_fatigue_games_last_7d",
            "home_fatigue_games_last_14d", "home_fatigue_score",
            "away_fatigue_rest_days", "away_fatigue_is_back_to_back", "away_fatigue_games_last_7d",
            "away_fatigue_games_last_14d", "away_fatigue_score", "fatigue_score_diff",
            # NFL Player stats
            "home_nfl_ps_qb_rating", "home_nfl_ps_pass_yds_pg", "home_nfl_ps_rush_yds_pg",
            "home_nfl_ps_sacks_pg", "home_nfl_ps_def_int_pg", "home_nfl_ps_rush_td_pg",
            "away_nfl_ps_qb_rating", "away_nfl_ps_pass_yds_pg", "away_nfl_ps_rush_yds_pg",
            "away_nfl_ps_sacks_pg", "away_nfl_ps_def_int_pg", "away_nfl_ps_rush_td_pg",
            # NFL player stats differentials
            "nfl_ps_qb_rating_diff", "nfl_ps_pass_yds_pg_diff", "nfl_ps_rush_yds_pg_diff",
            "nfl_ps_sacks_pg_diff", "nfl_ps_def_int_pg_diff", "nfl_ps_rush_td_pg_diff",
            # Kicker FG% from season team_stats
            "home_kicker_fg_pct", "away_kicker_fg_pct", "kicker_fg_pct_diff",
            # Injury burden
            "home_injury_count", "home_injury_severity_score", "home_injury_out_count",
            "home_injury_dtd_count", "home_injury_questionable_count",
            "away_injury_count", "away_injury_severity_score", "away_injury_out_count",
            "away_injury_dtd_count", "away_injury_questionable_count",
            "injury_advantage",
            # Stat differentials
            "total_yds_diff", "turnovers_diff", "sacks_diff", "tds_diff",
            "yards_per_play_diff", "first_downs_diff",
            # EPA differentials
            "epa_pass_off_diff", "epa_rush_off_diff", "epa_total_off_diff", "epa_net_diff",
            # Air yards & YAC differentials
            "air_yards_diff", "yac_diff",
            # Efficiency differentials
            "completion_pct_diff", "third_down_pct_diff", "red_zone_pct_diff", "turnover_margin_diff",
            # Air yards per play
            "home_air_yards_pg", "home_yac_pg", "home_air_yards_allowed_pg", "home_yac_allowed_pg",
            "away_air_yards_pg", "away_yac_pg", "away_air_yards_allowed_pg", "away_yac_allowed_pg",
            # New stat averages
            "home_passing_tds_pg", "home_rushing_tds_pg", "home_total_tds_pg",
            "home_tds_allowed_pg", "home_yards_per_play", "home_receiving_yds_pg",
            "away_passing_tds_pg", "away_rushing_tds_pg", "away_total_tds_pg",
            "away_tds_allowed_pg", "away_yards_per_play", "away_receiving_yds_pg",
            # Quarter rolling stats
            "home_period_first_ppg", "away_period_first_ppg",
            "home_period_first_opp_ppg", "away_period_first_opp_ppg",
            "home_period_first_win_pct", "away_period_first_win_pct",
            "home_period_first_half_ppg", "away_period_first_half_ppg",
            "home_period_first_half_opp_ppg", "away_period_first_half_opp_ppg",
            "home_period_first_half_win_pct", "away_period_first_half_win_pct",
            "home_period_second_half_ppg", "away_period_second_half_ppg",
            "home_period_comeback_rate", "away_period_comeback_rate",
            "home_period_ot_rate", "away_period_ot_rate",
            "period_first_ppg_diff", "period_first_opp_ppg_diff",
            "period_first_win_pct_diff", "period_first_half_win_pct_diff",
            "period_second_half_ppg_diff", "period_comeback_diff", "period_ot_rate_diff",
        ]
