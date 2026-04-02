# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Hockey
# ──────────────────────────────────────────────────────────
#
# Covers NHL.  Produces ~35 features per game including
# Corsi/Fenwick, expected goals, power play / penalty kill,
# goalie save %, faceoffs, hits, and blocked shots.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)


class HockeyExtractor(BaseFeatureExtractor):
    """Feature extractor for hockey (NHL)."""

    # Common NHL abbreviation variants → canonical abbreviation
    _ABR_NORM: dict[str, str] = {
        "NJ": "NJD", "SJ": "SJS", "LA": "LAK", "TB": "TBL",
        "TBL": "TBL", "UTAH": "UTA", "FIN": "FIN", "SWE": "SWE",
    }
    # Canonical abbr → full team name (for matching to games table)
    _ABR_TO_NAME: dict[str, str] = {
        "ANA": "Anaheim Ducks", "ARI": "Arizona Coyotes", "BOS": "Boston Bruins",
        "BUF": "Buffalo Sabres", "CGY": "Calgary Flames", "CAR": "Carolina Hurricanes",
        "CHI": "Chicago Blackhawks", "COL": "Colorado Avalanche", "CBJ": "Columbus Blue Jackets",
        "DAL": "Dallas Stars", "DET": "Detroit Red Wings", "EDM": "Edmonton Oilers",
        "FLA": "Florida Panthers", "LAK": "Los Angeles Kings", "MIN": "Minnesota Wild",
        "MTL": "Montreal Canadiens", "NSH": "Nashville Predators", "NJD": "New Jersey Devils",
        "NYI": "New York Islanders", "NYR": "New York Rangers", "OTT": "Ottawa Senators",
        "PHI": "Philadelphia Flyers", "PIT": "Pittsburgh Penguins", "STL": "St. Louis Blues",
        "SJS": "San Jose Sharks", "SEA": "Seattle Kraken", "TBL": "Tampa Bay Lightning",
        "TOR": "Toronto Maple Leafs", "VAN": "Vancouver Canucks", "VGK": "Vegas Golden Knights",
        "WSH": "Washington Capitals", "WPG": "Winnipeg Jets", "UTA": "Utah Hockey Club",
        "CAN": "Canada", "USA": "USA", "FIN": "Finland", "SWE": "Sweden",
    }

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._ps_abr_to_id_cache: dict[int, dict[str, str]] = {}
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

    def _build_ps_abr_to_id(self, season: int) -> dict[str, str]:
        """Build abbreviation → ESPN team_id map from games data for PS lookup."""
        if season in self._ps_abr_to_id_cache:
            return self._ps_abr_to_id_cache[season]
        games = self.load_games(season)
        if games.empty:
            return {}
        name_to_id = {}
        for _, row in games[["home_team", "home_team_id"]].drop_duplicates().iterrows():
            name_to_id[str(row["home_team"])] = str(row["home_team_id"])
        for _, row in games[["away_team", "away_team_id"]].drop_duplicates().iterrows():
            name_to_id[str(row["away_team"])] = str(row["away_team_id"])
        abr_to_id: dict[str, str] = {}
        for abr, name in self._ABR_TO_NAME.items():
            if name in name_to_id:
                abr_to_id[abr] = name_to_id[name]
        # Also add normalized variants
        for variant, canonical in self._ABR_NORM.items():
            if canonical in abr_to_id:
                abr_to_id[variant] = abr_to_id[canonical]
        self._ps_abr_to_id_cache[season] = abr_to_id
        return abr_to_id

    # ── Helpers ────────────────────────────────────────────

    @staticmethod
    def _n(val: Any, default: float = 0.0) -> float:
        """Safely parse numeric value."""
        v = pd.to_numeric(val, errors="coerce")
        return float(v) if pd.notna(v) else default

    @staticmethod
    def _col_sum(df: pd.DataFrame, col: str) -> float:
        """Vectorized sum of a column, handling missing columns gracefully."""
        if col not in df.columns:
            return 0.0
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

    @staticmethod
    def _col_mean_nonnan(df: pd.DataFrame, col: str) -> list[float]:
        """Return non-NaN numeric values from a column as a list."""
        if col not in df.columns:
            return []
        vals = pd.to_numeric(df[col], errors="coerce").dropna()
        return vals.tolist()

    def _split_home_away(self, recent: pd.DataFrame, team_id: str):
        """Return (home_rows, away_rows) sub-DataFrames for team_id."""
        is_home = recent["home_team_id"] == team_id
        return recent[is_home], recent[~is_home]

    def _possession_metrics(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Shot-based possession metrics (Corsi proxy + xG proxy via blocked shots)."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "corsi_pct": 50.0, "fenwick_pct": 50.0,
                "xgf_pct": 50.0, "shots_for_pg": 0.0, "shots_against_pg": 0.0,
            }

        home, away = self._split_home_away(recent, team_id)
        n = len(recent)

        # Shots for / against (vectorized)
        sf = (self._col_sum(home, "home_shots_on_goal") +
              self._col_sum(away, "away_shots_on_goal"))
        sa = (self._col_sum(home, "away_shots_on_goal") +
              self._col_sum(away, "home_shots_on_goal"))
        bf = (self._col_sum(home, "home_blocked_shots") +
              self._col_sum(away, "away_blocked_shots"))
        ba = (self._col_sum(home, "away_blocked_shots") +
              self._col_sum(away, "home_blocked_shots"))

        total_shots = sf + sa
        shot_pct = sf / total_shots * 100 if total_shots > 0 else 50.0
        unblocked_f = max(sf - bf, 0)
        unblocked_a = max(sa - ba, 0)
        total_fen = unblocked_f + unblocked_a
        fen_pct = unblocked_f / total_fen * 100 if total_fen > 0 else 50.0

        return {
            "corsi_pct": float(shot_pct),
            "fenwick_pct": float(fen_pct),
            "xgf_pct": float(shot_pct),
            "shots_for_pg": float(sf / n),
            "shots_against_pg": float(sa / n),
        }

    def _special_teams(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Power play and penalty kill percentages."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"pp_pct": 0.0, "pk_pct": 0.0, "pp_opportunities_pg": 0.0, "pim_pg": 0.0}

        home, away = self._split_home_away(recent, team_id)
        n = len(recent)

        # Direct PP/PK percentages (vectorized)
        pp_pcts = (self._col_mean_nonnan(home, "home_power_play_pct") +
                   self._col_mean_nonnan(away, "away_power_play_pct"))
        pk_pcts = (self._col_mean_nonnan(home, "home_penalty_kill_pct") +
                   self._col_mean_nonnan(away, "away_penalty_kill_pct"))

        # Fallback: compute from goals/opportunities
        pp_goals = (self._col_sum(home, "home_power_play_goals") +
                    self._col_sum(home, "home_pp_goals") +
                    self._col_sum(away, "away_power_play_goals") +
                    self._col_sum(away, "away_pp_goals"))
        # Normalized data uses home_power_play_attempts; fall back to home_pp_opportunities
        def _pp_opp(home_col: str, away_col: str, home_df: pd.DataFrame, away_df: pd.DataFrame) -> float:
            v = self._col_sum(home_df, home_col) + self._col_sum(away_df, away_col)
            return v

        def _pp_opp_from_pim(opp_home: str, opp_away: str, h: pd.DataFrame, a: pd.DataFrame) -> float:
            """Estimate PP opportunities from opponent penalty minutes (2 min = 1 opp)."""
            pim = self._col_sum(h, opp_home) + self._col_sum(a, opp_away)
            return pim / 2.0 if pim > 0 else 0.0

        pp_opp = (
            _pp_opp("home_power_play_attempts", "away_power_play_attempts", home, away) or
            _pp_opp("home_pp_opportunities", "away_pp_opportunities", home, away) or
            # Fallback: opponent penalty minutes / 2 ≈ team's PP opportunities (99% fill rate)
            _pp_opp_from_pim("away_penalty_minutes", "home_penalty_minutes", home, away)
        )
        pk_opp = (
            _pp_opp("away_power_play_attempts", "home_power_play_attempts", home, away) or
            _pp_opp("away_pp_opportunities", "home_pp_opportunities", home, away) or
            # Fallback: own penalty minutes / 2 ≈ opponent's PP opportunities (our PK opportunities)
            _pp_opp_from_pim("home_penalty_minutes", "away_penalty_minutes", home, away)
        )
        pk_ga = (self._col_sum(home, "away_power_play_goals") +
                 self._col_sum(home, "away_pp_goals") +
                 self._col_sum(away, "home_power_play_goals") +
                 self._col_sum(away, "home_pp_goals"))
        pim_total = (self._col_sum(home, "home_penalty_minutes") +
                     self._col_sum(home, "home_pim") +
                     self._col_sum(away, "away_penalty_minutes") +
                     self._col_sum(away, "away_pim"))

        pp_pct_val = float(np.mean(pp_pcts)) if pp_pcts else (
            float(pp_goals / pp_opp * 100) if pp_opp > 0 else 0.0)
        pk_pct_val = float(np.mean(pk_pcts)) if pk_pcts else (
            float((1 - pk_ga / pk_opp) * 100) if pk_opp > 0 else 0.0)

        # PP trend: recent 3 vs overall window (positive = improving)
        r3 = recent.tail(3)
        h3, a3 = self._split_home_away(r3, team_id)
        pp_pcts3 = (self._col_mean_nonnan(h3, "home_power_play_pct") +
                    self._col_mean_nonnan(a3, "away_power_play_pct"))
        pp_goals3 = (self._col_sum(h3, "home_power_play_goals") + self._col_sum(h3, "home_pp_goals") +
                     self._col_sum(a3, "away_power_play_goals") + self._col_sum(a3, "away_pp_goals"))
        pp_opp3 = (_pp_opp("home_power_play_attempts", "away_power_play_attempts", h3, a3) or
                   _pp_opp("home_pp_opportunities", "away_pp_opportunities", h3, a3) or
                   _pp_opp_from_pim("away_penalty_minutes", "home_penalty_minutes", h3, a3))
        pp_pct3 = float(np.mean(pp_pcts3)) if pp_pcts3 else (
            float(pp_goals3 / pp_opp3 * 100) if pp_opp3 > 0 else pp_pct_val)
        pp_trend = pp_pct3 - pp_pct_val  # positive = improving PP recently

        # Net special teams advantage (PP% - (100 - PK%)); positive = special teams advantage
        net_st = pp_pct_val - (100.0 - pk_pct_val) if pk_pct_val > 0 else 0.0

        return {
            "pp_pct": pp_pct_val,
            "pk_pct": pk_pct_val,
            "pp_opportunities_pg": float(pp_opp / n),
            "pim_pg": float(pim_total / n),
            "pp_trend": pp_trend,
            "net_special_teams": net_st,
        }

    def _goalie_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Goalie save percentage derived from rolling game data."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"goalie_sv_pct": np.nan, "goalie_gaa": 0.0, "goalie_win_pct": 0.0}

        home, away = self._split_home_away(recent, team_id)
        n = len(recent)

        # Goals against (opponent score)
        goals_against = (self._col_sum(home, "away_score") +
                         self._col_sum(away, "home_score"))

        # Save pct — prefer direct columns
        sv_pct_vals = (self._col_mean_nonnan(home, "home_save_pct") +
                       self._col_mean_nonnan(away, "away_save_pct"))
        sv_pct_vals = [v * 100 if v <= 1.0 else v for v in sv_pct_vals if v > 0]

        # Wins
        home_wins = 0
        if not home.empty and "home_score" in home.columns and "away_score" in home.columns:
            h_s = pd.to_numeric(home["home_score"], errors="coerce").fillna(0)
            a_s = pd.to_numeric(home["away_score"], errors="coerce").fillna(0)
            home_wins += int((h_s > a_s).sum())
        away_wins = 0
        if not away.empty and "home_score" in away.columns and "away_score" in away.columns:
            h_s = pd.to_numeric(away["home_score"], errors="coerce").fillna(0)
            a_s = pd.to_numeric(away["away_score"], errors="coerce").fillna(0)
            away_wins += int((a_s > h_s).sum())
        wins_total = home_wins + away_wins

        gaa = float(goals_against / n) if n > 0 else 0.0
        if sv_pct_vals:
            sv_pct = float(np.mean(sv_pct_vals))
        else:
            # Fallback: opponent shots on goal
            shots_faced = (self._col_sum(home, "away_shots_on_goal") +
                           self._col_sum(away, "home_shots_on_goal"))
            sv_pct = float((1 - goals_against / shots_faced) * 100) if shots_faced > goals_against else np.nan

        return {
            "goalie_sv_pct": sv_pct,
            "goalie_gaa": gaa,
            "goalie_win_pct": float(wins_total / n) if n > 0 else 0.0,
        }

    def _ot_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 30,
    ) -> dict[str, float]:
        """Overtime tendency and win rate in OT/shootout situations."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {"ot_rate": 0.25, "ot_win_rate": 0.5, "one_goal_rate": 0.3}
        if recent.empty:
            return defaults

        hs = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0)
        as_ = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0)
        ot_col = "overtime" if "overtime" in recent.columns else None
        n = len(recent)

        if ot_col:
            ot_games = recent[ot_col].fillna(False).astype(bool)
        else:
            # Approximate: one-goal final that went to OT won't have explicit column
            ot_games = pd.Series(False, index=recent.index)
        ot_count = int(ot_games.sum())

        # OT win/loss for this team
        home_rows = recent["home_team_id"].astype(str) == str(team_id)
        away_rows = ~home_rows
        ot_wins = 0
        if ot_count > 0:
            ot_h = recent[ot_games & home_rows]
            ot_a = recent[ot_games & away_rows]
            if not ot_h.empty:
                ot_wins += int((pd.to_numeric(ot_h["home_score"], errors="coerce").fillna(0) >
                                pd.to_numeric(ot_h["away_score"], errors="coerce").fillna(0)).sum())
            if not ot_a.empty:
                ot_wins += int((pd.to_numeric(ot_a["away_score"], errors="coerce").fillna(0) >
                                pd.to_numeric(ot_a["home_score"], errors="coerce").fillna(0)).sum())

        # One-goal game rate (margin == 1 in regulation)
        margin = (hs - as_).abs()
        one_goal_count = int((margin <= 1).sum())

        return {
            "ot_rate": float(ot_count / n),
            "ot_win_rate": float(ot_wins / ot_count) if ot_count > 0 else 0.5,
            "one_goal_rate": float(one_goal_count / n),
        }

    def _lead_protection_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 20,
    ) -> dict[str, float]:
        """How often team holds a lead after each period."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {"lead_p2_protection_rate": 0.75, "lead_p1_protection_rate": 0.70}
        if recent.empty:
            return defaults

        home, away = self._split_home_away(recent, team_id)
        n = len(recent)

        total_lead_p1 = 0
        held_p1 = 0
        total_lead_p2 = 0
        held_p2 = 0

        # Home games: team is home
        for df_side, team_col, opp_col in [(home, "home", "away"), (away, "away", "home")]:
            if df_side.empty:
                continue
            t_p1 = pd.to_numeric(df_side.get(f"{team_col}_p1"), errors="coerce").fillna(np.nan)
            o_p1 = pd.to_numeric(df_side.get(f"{opp_col}_p1"), errors="coerce").fillna(np.nan)
            t_p2 = pd.to_numeric(df_side.get(f"{team_col}_p2"), errors="coerce").fillna(np.nan)
            o_p2 = pd.to_numeric(df_side.get(f"{opp_col}_p2"), errors="coerce").fillna(np.nan)
            t_sc = pd.to_numeric(df_side["home_score" if team_col == "home" else "away_score"], errors="coerce").fillna(0)
            o_sc = pd.to_numeric(df_side["away_score" if team_col == "home" else "home_score"], errors="coerce").fillna(0)

            for i in range(len(df_side)):
                # After P1 lead protection
                if not np.isnan(t_p1.iloc[i]) and not np.isnan(o_p1.iloc[i]):
                    if t_p1.iloc[i] > o_p1.iloc[i]:
                        total_lead_p1 += 1
                        if t_sc.iloc[i] > o_sc.iloc[i]:
                            held_p1 += 1
                # After P2 lead protection
                if not np.isnan(t_p2.iloc[i]) and not np.isnan(o_p2.iloc[i]):
                    t_thru2 = t_p1.iloc[i] + t_p2.iloc[i] if not np.isnan(t_p1.iloc[i]) else np.nan
                    o_thru2 = o_p1.iloc[i] + o_p2.iloc[i] if not np.isnan(o_p1.iloc[i]) else np.nan
                    if not np.isnan(t_thru2) and not np.isnan(o_thru2) and t_thru2 > o_thru2:
                        total_lead_p2 += 1
                        if t_sc.iloc[i] > o_sc.iloc[i]:
                            held_p2 += 1

        return {
            "lead_p1_protection_rate": float(held_p1 / total_lead_p1) if total_lead_p1 > 0 else 0.70,
            "lead_p2_protection_rate": float(held_p2 / total_lead_p2) if total_lead_p2 > 0 else 0.75,
        }

    def _starting_goalie_form(
        self,
        team_id: str,
        date: str,
        season: int,
        window: int = 5,
    ) -> dict[str, float]:
        """Identify likely starting goalie and return their personal rolling sv_pct."""
        ps = self.load_player_stats(season)
        defaults = {"starter_sv_pct": np.nan, "starter_gaa": 0.0, "goalie_consistency": 0.0}
        if ps.empty or "team_id" not in ps.columns:
            return defaults

        abr_map = self._build_ps_abr_to_id(season)

        cache_key = f"_pg_cache_{season}"
        if not hasattr(self, cache_key):
            raw_ids = ps["team_id"].astype(str)
            mapped_ids = raw_ids.map(lambda t: abr_map.get(t, abr_map.get(self._ABR_NORM.get(t, t), t)))
            pg_cache = {
                "team_ids": mapped_ids.values,
                "dates_ns": pd.to_datetime(ps["date"], errors="coerce").values.astype("int64"),
                "goalie_mask": (ps["saves"].notna() & (ps["saves"] > 0)).values if "saves" in ps.columns else np.zeros(len(ps), dtype=bool),
                "saves": pd.to_numeric(ps.get("saves", 0), errors="coerce").fillna(0).values if "saves" in ps.columns else np.zeros(len(ps)),
                "ga": pd.to_numeric(ps.get("goals_against", 0), errors="coerce").fillna(0).values if "goals_against" in ps.columns else np.zeros(len(ps)),
                "player_ids": ps.get("player_id", pd.Series(dtype=str)).astype(str).values,
                "dates_dt": pd.to_datetime(ps["date"], errors="coerce").values,
            }
            setattr(self, cache_key, pg_cache)
        pc = getattr(self, cache_key)

        game_date_ns = pd.Timestamp(date).value if date else 0
        if game_date_ns == 0:
            return defaults

        mask = (pc["team_ids"] == str(team_id)) & pc["goalie_mask"] & (pc["dates_ns"] < game_date_ns)
        if not mask.any():
            return defaults

        indices = np.where(mask)[0]
        sorted_idx = indices[np.argsort(pc["dates_ns"][indices])[::-1]][:window * 3]

        # Find the most recently played goalie (likely starter)
        if len(sorted_idx) == 0:
            return defaults

        # Identify likely starter = goalie who appeared in most recent game
        most_recent_date = pc["dates_ns"][sorted_idx[0]]
        recent_game_mask = pc["dates_ns"][sorted_idx] == most_recent_date
        starter_ids = pc["player_ids"][sorted_idx[recent_game_mask]]
        if len(starter_ids) == 0:
            return defaults
        likely_starter = starter_ids[0]

        # Get starter's last `window` games
        starter_mask = (pc["team_ids"] == str(team_id)) & pc["goalie_mask"] & \
                       (pc["dates_ns"] < game_date_ns) & (pc["player_ids"] == likely_starter)
        if not starter_mask.any():
            return defaults

        starter_idx = np.where(starter_mask)[0]
        sorted_starter = starter_idx[np.argsort(pc["dates_ns"][starter_idx])[::-1]][:window]

        saves = pc["saves"][sorted_starter].sum()
        ga = pc["ga"][sorted_starter].sum()
        total = saves + ga
        sv_pct = float(saves / total * 100) if total > 0 else np.nan
        n = len(sorted_starter)
        gaa = float(ga / n) if n > 0 else 0.0

        # Consistency: std dev of per-game sv_pct (lower = more consistent)
        per_game_sv = np.array([
            float(pc["saves"][i] / (pc["saves"][i] + pc["ga"][i]) * 100)
            if (pc["saves"][i] + pc["ga"][i]) > 0 else np.nan
            for i in sorted_starter
        ])
        valid = per_game_sv[~np.isnan(per_game_sv)]
        consistency = float(np.std(valid)) if len(valid) > 1 else 0.0

        return {
            "starter_sv_pct": sv_pct,
            "starter_gaa": gaa,
            "goalie_consistency": consistency,
        }

    def _player_goalie_features(
        self,
        team_id: str,
        date: str,
        season: int,
        window: int = 5,
    ) -> dict[str, float]:
        """Goalie stats from per-game player_stats parquet (last `window` games).

        NHL player_stats use abbreviations (e.g. 'TB', 'NJ') while games use
        ESPN numeric IDs.  We build an abbr→ID map per season so lookups match.
        """
        ps = self.load_player_stats(season)
        defaults = {"ps_sv_pct": np.nan, "ps_gaa": 0.0}
        if ps.empty or "team_id" not in ps.columns:
            return defaults

        # Translate PS abbreviation team_ids to ESPN numeric team_ids
        abr_map = self._build_ps_abr_to_id(season)

        # Cache converted columns per season to avoid repeat type conversions
        cache_key = f"_pg_cache_{season}"
        if not hasattr(self, cache_key):
            raw_ids = ps["team_id"].astype(str)
            # Map abbreviations → ESPN IDs (keep original if no mapping found)
            mapped_ids = raw_ids.map(lambda t: abr_map.get(t, abr_map.get(self._ABR_NORM.get(t, t), t)))
            pg_cache = {
                "team_ids": mapped_ids.values,
                "dates_ns": pd.to_datetime(ps["date"], errors="coerce").values.astype("int64"),
                "goalie_mask": (ps["saves"].notna() & (ps["saves"] > 0)).values if "saves" in ps.columns else np.zeros(len(ps), dtype=bool),
                "saves": pd.to_numeric(ps.get("saves", 0), errors="coerce").fillna(0).values if "saves" in ps.columns else np.zeros(len(ps)),
                "ga": pd.to_numeric(ps.get("goals_against", 0), errors="coerce").fillna(0).values if "goals_against" in ps.columns else np.zeros(len(ps)),
                "dates_dt": pd.to_datetime(ps["date"], errors="coerce").values,
                "player_ids": ps.get("player_id", pd.Series(dtype=str)).astype(str).values,
            }
            setattr(self, cache_key, pg_cache)
        pc = getattr(self, cache_key)

        game_date_ns = pd.Timestamp(date).value if date else 0
        if game_date_ns == 0:
            return defaults

        mask = (pc["team_ids"] == str(team_id)) & pc["goalie_mask"] & (pc["dates_ns"] < game_date_ns)
        if not mask.any():
            return defaults

        # Get indices, sort by date descending, take top window
        indices = np.where(mask)[0]
        sorted_idx = indices[np.argsort(pc["dates_ns"][indices])[::-1]][:window]

        saves = pc["saves"][sorted_idx].sum()
        ga = pc["ga"][sorted_idx].sum()
        total_shots = saves + ga
        sv_pct = float(saves / total_shots * 100) if total_shots > 0 else np.nan
        n = len(sorted_idx)
        gaa = float(ga / n) if n > 0 else 0.0

        return {"ps_sv_pct": sv_pct, "ps_gaa": gaa}

    def _short_window_goals(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 5,
    ) -> dict[str, float]:
        """Rolling last-`window` goals for/against per game."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"gf_l5": 0.0, "ga_l5": 0.0, "gf_ga_ratio_l5": 1.0}

        home, away = self._split_home_away(recent, team_id)
        n = len(recent)
        gf = self._col_sum(home, "home_score") + self._col_sum(away, "away_score")
        ga = self._col_sum(home, "away_score") + self._col_sum(away, "home_score")
        gf_pg = gf / n
        ga_pg = ga / n
        ratio = gf_pg / ga_pg if ga_pg > 0 else 1.0
        return {"gf_l5": float(gf_pg), "ga_l5": float(ga_pg), "gf_ga_ratio_l5": float(ratio)}

    def _close_game_record(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 20,
    ) -> dict[str, float]:
        """Win% in 1-goal games and blown-lead rate (highly predictive in NHL)."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {"close_game_win_pct": 0.5, "blowout_win_pct": 0.5, "one_goal_rate": 0.0}
        if recent.empty:
            return defaults

        home, away = self._split_home_away(recent, team_id)
        results = []
        for _, row in recent.iterrows():
            h_s = pd.to_numeric(row.get("home_score"), errors="coerce")
            a_s = pd.to_numeric(row.get("away_score"), errors="coerce")
            if pd.isna(h_s) or pd.isna(a_s):
                continue
            is_home_team = str(row.get("home_team_id")) == str(team_id)
            won = (is_home_team and h_s > a_s) or (not is_home_team and a_s > h_s)
            diff = abs(h_s - a_s)
            results.append({"won": won, "diff": diff})

        if not results:
            return defaults

        close = [r for r in results if r["diff"] <= 1]
        blow = [r for r in results if r["diff"] >= 3]
        close_win_pct = sum(1 for r in close if r["won"]) / len(close) if close else 0.5
        blowout_win_pct = sum(1 for r in blow if r["won"]) / len(blow) if blow else 0.5
        one_goal_rate = len(close) / len(results) if results else 0.0

        return {
            "close_game_win_pct": float(close_win_pct),
            "blowout_win_pct": float(blowout_win_pct),
            "one_goal_rate": float(one_goal_rate),
        }

    def _save_pct_trend(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
    ) -> dict[str, float]:
        """Goalie save% trend: recent 5 vs prior 15 (positive = improving)."""
        short = self._team_games_before(games, team_id, date, limit=5)
        longer = self._team_games_before(games, team_id, date, limit=20)
        defaults = {"sv_pct_trend": 0.0, "sv_pct_short": 0.0}

        def _sv(df: pd.DataFrame) -> float:
            if df.empty:
                return 0.0
            home, away = self._split_home_away(df, team_id)
            vals = (self._col_mean_nonnan(home, "home_save_pct") +
                    self._col_mean_nonnan(away, "away_save_pct"))
            vals = [v * 100 if v <= 1.0 else v for v in vals if v > 0]
            return float(np.mean(vals)) if vals else 0.0

        sv_short = _sv(short)
        sv_long = _sv(longer.iloc[5:] if len(longer) > 5 else longer)  # prior 15 after removing short
        trend = sv_short - sv_long
        return {"sv_pct_trend": float(trend), "sv_pct_short": float(sv_short)}

    def _goal_streak_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
    ) -> dict[str, float]:
        """Hot/cold streak: recent 3-game GF-GA vs prior 7-game baseline."""
        defaults = {"goal_streak_gf": 0.0, "goal_streak_ga": 0.0,
                    "goal_trend_gf": 0.0, "goal_trend_ga": 0.0}
        long = self._team_games_before(games, team_id, date, limit=10)
        if len(long) < 3:
            return defaults
        short = long.iloc[:3]
        baseline = long.iloc[3:] if len(long) > 3 else long

        def _gf_ga(df: pd.DataFrame) -> tuple[float, float]:
            if df.empty:
                return 0.0, 0.0
            home, away = self._split_home_away(df, team_id)
            n = max(len(df), 1)
            gf = (self._col_sum(home, "home_score") + self._col_sum(away, "away_score")) / n
            ga = (self._col_sum(home, "away_score") + self._col_sum(away, "home_score")) / n
            return float(gf), float(ga)

        s_gf, s_ga = _gf_ga(short)
        b_gf, b_ga = _gf_ga(baseline)
        return {
            "goal_streak_gf": s_gf,
            "goal_streak_ga": s_ga,
            "goal_trend_gf": s_gf - b_gf,   # positive = getting hotter offensively
            "goal_trend_ga": s_ga - b_ga,   # positive = conceding more recently
        }

    def _physical_stats(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Hits, blocked shots, faceoff pct, takeaway/giveaway, shooting pct, shorthanded goals."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "hits_pg": 0.0, "blocked_shots_pg": 0.0,
                "faceoff_pct": 50.0, "takeaway_giveaway_ratio": 1.0,
                "shooting_pct": 8.0, "shorthanded_goals_pg": 0.0,
                "shootout_goals_pg": 0.0,
            }

        home, away = self._split_home_away(recent, team_id)
        n = len(recent)

        # Use home_hits only (home_hits_nhl is identical — avoid double-counting)
        hits = (self._col_sum(home, "home_hits") +
                self._col_sum(away, "away_hits"))
        blocks = (self._col_sum(home, "home_blocked_shots") +
                  self._col_sum(away, "away_blocked_shots"))
        fo_won = (self._col_sum(home, "home_faceoffs_won") +
                  self._col_sum(home, "home_faceoff_wins") +
                  self._col_sum(away, "away_faceoffs_won") +
                  self._col_sum(away, "away_faceoff_wins"))
        fo_lost = (self._col_sum(home, "home_faceoffs_lost") +
                   self._col_sum(away, "away_faceoffs_lost"))
        # Direct faceoff_pct column (86% fill, more reliable)
        fo_pcts_direct = (self._col_mean_nonnan(home, "home_faceoff_pct") +
                          self._col_mean_nonnan(away, "away_faceoff_pct"))
        takeaways = (self._col_sum(home, "home_takeaways") +
                     self._col_sum(away, "away_takeaways"))
        giveaways = (self._col_sum(home, "home_giveaways") +
                     self._col_sum(away, "away_giveaways"))
        shooting_pcts = (self._col_mean_nonnan(home, "home_shooting_pct") +
                         self._col_mean_nonnan(away, "away_shooting_pct"))
        sh_goals = (self._col_sum(home, "home_shorthanded_goals") +
                    self._col_sum(away, "away_shorthanded_goals"))
        shootout_goals = (self._col_sum(home, "home_shootout_goals") +
                          self._col_sum(away, "away_shootout_goals"))

        fo_total = fo_won + fo_lost
        tg_ratio = float(takeaways / giveaways) if giveaways > 0 else 1.0
        # Use direct faceoff_pct if available, otherwise compute from won/lost
        if fo_pcts_direct:
            faceoff_pct_val = float(sum(fo_pcts_direct) / len(fo_pcts_direct))
        else:
            faceoff_pct_val = float(fo_won / fo_total * 100) if fo_total > 0 else 50.0

        return {
            "hits_pg": float(hits / n),
            "blocked_shots_pg": float(blocks / n),
            "faceoff_pct": faceoff_pct_val,
            "takeaway_giveaway_ratio": tg_ratio,
            "shooting_pct": float(sum(shooting_pcts) / len(shooting_pcts)) if shooting_pcts else 8.0,
            "shorthanded_goals_pg": float(sh_goals / n),
            "shootout_goals_pg": float(shootout_goals / n),
        }

    # ── NHL-specific Standings ────────────────────────────

    def _skater_features(
        self,
        team_id: str,
        date: str,
        season: int,
        window: int = 10,
    ) -> dict[str, float]:
        """Star skater stats: top-player points, plus/minus, high-danger scoring."""
        ps = self.load_player_stats(season)
        defaults = {
            "avg_plus_minus": 0.0,
            "top_scorer_pts_pg": 0.0,
            "team_pts_pg": 0.0,
            "team_shots_pg": 0.0,
            "team_pp_goals_pg": 0.0,
        }
        if ps.empty or "team_id" not in ps.columns:
            return defaults

        abr_map = self._build_ps_abr_to_id(season)

        # Build skater cache
        cache_key = f"_sk_cache_{season}"
        if not hasattr(self, cache_key):
            raw_ids = ps["team_id"].astype(str)
            mapped = raw_ids.map(lambda t: abr_map.get(t, abr_map.get(self._ABR_NORM.get(t, t), t)))
            n_ps = len(ps)

            def _col(col_name: str) -> "np.ndarray":
                col = ps.get(col_name)
                if col is None or not isinstance(col, pd.Series):
                    return np.zeros(n_ps)
                return pd.to_numeric(col, errors="coerce").fillna(0).values

            sk_cache = {
                "team_ids": mapped.values,
                "dates_ns": pd.to_datetime(ps["date"], errors="coerce").values.astype("int64"),
                "plus_minus": _col("plus_minus"),
                "goals": _col("goals"),
                "assists": _col("assists"),
                "shots": _col("shots"),
                "pp_goals": _col("pp_goals"),
                "player_ids": ps.get("player_id", pd.Series(dtype=str)).astype(str).values if isinstance(ps.get("player_id"), pd.Series) else np.array([""] * n_ps),
                "is_skater": (ps["saves"].isna() | (ps["saves"] == 0)).values if "saves" in ps.columns else np.ones(len(ps), dtype=bool),
            }
            setattr(self, cache_key, sk_cache)
        sc = getattr(self, cache_key)

        game_date_ns = pd.Timestamp(date).value if date else 0
        if game_date_ns == 0:
            return defaults

        mask = (sc["team_ids"] == str(team_id)) & sc["is_skater"] & (sc["dates_ns"] < game_date_ns)
        if not mask.any():
            return defaults

        indices = np.where(mask)[0]
        # Sort by date desc, take window
        sorted_idx = indices[np.argsort(sc["dates_ns"][indices])[::-1]][:window * 20]  # more rows for player grouping

        pm = sc["plus_minus"][sorted_idx]
        goals = sc["goals"][sorted_idx]
        assists = sc["assists"][sorted_idx]
        shots = sc["shots"][sorted_idx]
        pp_goals = sc["pp_goals"][sorted_idx]
        pids = sc["player_ids"][sorted_idx]

        n_games = min(window, len(np.unique(sc["dates_ns"][sorted_idx])))
        if n_games == 0:
            return defaults

        avg_pm = float(pm.mean()) if len(pm) > 0 else 0.0
        pts = goals + assists
        total_pts = float(pts.sum())
        total_shots = float(shots.sum())

        # Top scorer contribution
        if len(pids) > 0:
            unique_pids = np.unique(pids)
            top_pts = max(
                float(pts[pids == pid].sum())
                for pid in unique_pids
            ) if len(unique_pids) > 0 else 0.0
        else:
            top_pts = 0.0

        return {
            "avg_plus_minus": avg_pm,
            "top_scorer_pts_pg": top_pts / n_games,
            "team_pts_pg": total_pts / n_games,
            "team_shots_pg": total_shots / n_games,
            "team_pp_goals_pg": float(pp_goals.sum()) / n_games,
        }

    def _nhl_standings_features(self, team_id: str, season: int) -> dict[str, float]:
        """Parse NHL standings: uses points, wins/losses/otl, W-L-OTL last-ten."""
        defaults = {
            "stnd_win_pct": 0.5,
            "stnd_points": 82.0,
            "stnd_l10_win_pct": 0.5,
            "stnd_streak": 0.0,
            "stnd_pts_diff": 0.0,
            "stnd_gp": 0.0,
            "stnd_conf_rank": 16.0,
            "stnd_div_rank": 4.0,
        }
        standings = self.load_standings(season)
        if standings.empty or "team_id" not in standings.columns:
            return defaults

        row_df = standings.loc[standings["team_id"] == str(team_id)]
        if row_df.empty:
            return defaults
        row = row_df.iloc[0]

        wins = self._n(row.get("wins"))
        losses = self._n(row.get("losses"))
        otl = self._n(row.get("otl"))
        gp = wins + losses + otl
        win_pct = wins / gp if gp > 0 else 0.5

        points = self._n(row.get("points"))

        # Parse L10: "4-5-1, 0 PTS" → W/(W+L+OTL)
        l10_str = str(row.get("last_ten", ""))
        try:
            parts = l10_str.split(",")[0].split("-")
            l10w, l10l, l10otl = float(parts[0]), float(parts[1]), float(parts[2]) if len(parts) > 2 else 0.0
            l10_denom = l10w + l10l + l10otl
            l10_pct = l10w / l10_denom if l10_denom > 0 else 0.5
        except (ValueError, TypeError, IndexError):
            l10_pct = 0.5

        # Streak: "W3" → +3, "L2" → -2
        streak_val = 0.0
        streak_str = str(row.get("streak", ""))
        if streak_str and streak_str[0] in ("W", "w"):
            streak_val = self._n(streak_str[1:])
        elif streak_str and streak_str[0] in ("L", "l"):
            streak_val = -self._n(streak_str[1:])

        # Points differential (goals for - against is a proxy for quality)
        pf = self._n(row.get("points_for"))
        pa = self._n(row.get("points_against"))
        pts_diff = pf - pa

        # Conference and division rank (available in richer standings files)
        conf_rank = self._n(row.get("conference_rank", 16.0), 16.0)
        div_rank = self._n(row.get("division_rank", 4.0), 4.0)

        # Also use the pre-computed win% (pct) if available (more reliable than our manual calc)
        standings_pct = self._n(row.get("pct", win_pct), win_pct)
        if 0.0 < standings_pct <= 1.0:
            win_pct = standings_pct

        return {
            "stnd_win_pct": win_pct,
            "stnd_points": points,
            "stnd_l10_win_pct": l10_pct,
            "stnd_streak": streak_val,
            "stnd_pts_diff": pts_diff,
            "stnd_gp": gp,
            "stnd_conf_rank": conf_rank,
            "stnd_div_rank": div_rank,
        }

    def _quality_weighted_form(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Win form weighted by opponent quality (opponent win% up to prediction date).

        Uses a single lookup per unique opponent — O(n × log n) not O(n²).
        """
        defaults = {"nhl_quality_form": 0.0, "nhl_quality_win_rate": 0.5}
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return defaults

        is_home = recent["home_team_id"].astype(str) == str(team_id)
        h_sc = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0)
        a_sc = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0)
        tm_sc = np.where(is_home, h_sc, a_sc)
        op_sc = np.where(is_home, a_sc, h_sc)
        wins = (tm_sc > op_sc).astype(float)
        opp_ids = np.where(is_home, recent["away_team_id"].astype(str), recent["home_team_id"].astype(str))

        opp_qual: dict[str, float] = {}
        for opp_id in set(opp_ids):
            opp_hist = self._team_games_before(games, str(opp_id), date, limit=20)
            if opp_hist.empty:
                opp_qual[str(opp_id)] = 0.5
            else:
                oh = opp_hist["home_team_id"].astype(str) == str(opp_id)
                ohs = pd.to_numeric(opp_hist["home_score"], errors="coerce").fillna(0)
                oas = pd.to_numeric(opp_hist["away_score"], errors="coerce").fillna(0)
                ow = np.where(oh, ohs > oas, oas > ohs).astype(float)
                opp_qual[str(opp_id)] = float(ow.mean()) if len(ow) else 0.5

        opp_arr = np.array([opp_qual.get(str(o), 0.5) for o in opp_ids])
        n = max(len(recent), 1)
        quality_form = float(np.dot(wins * 2.0 - 1.0, opp_arr) / n)
        quality_win_rate = float(np.dot(wins, opp_arr) / n)
        return {"nhl_quality_form": quality_form, "nhl_quality_win_rate": quality_win_rate}

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
            # Period scores — passed through as targets for extra-market models
            # NHL uses home_p1/p2/p3; fall back to home_q1/q2/q3 for generic schema
            "home_p1": pd.to_numeric(game.get("home_p1", game.get("home_q1")), errors="coerce"),
            "home_p2": pd.to_numeric(game.get("home_p2", game.get("home_q2")), errors="coerce"),
            "home_p3": pd.to_numeric(game.get("home_p3", game.get("home_q3")), errors="coerce"),
            "home_q1": pd.to_numeric(game.get("home_p1", game.get("home_q1")), errors="coerce"),
            "home_q2": pd.to_numeric(game.get("home_p2", game.get("home_q2")), errors="coerce"),
            "home_q3": pd.to_numeric(game.get("home_p3", game.get("home_q3")), errors="coerce"),
            "home_q4": pd.to_numeric(game.get("home_q4"), errors="coerce"),
            "home_ot": pd.to_numeric(game.get("home_ot"), errors="coerce"),
            "away_p1": pd.to_numeric(game.get("away_p1", game.get("away_q1")), errors="coerce"),
            "away_p2": pd.to_numeric(game.get("away_p2", game.get("away_q2")), errors="coerce"),
            "away_p3": pd.to_numeric(game.get("away_p3", game.get("away_q3")), errors="coerce"),
            "away_q1": pd.to_numeric(game.get("away_p1", game.get("away_q1")), errors="coerce"),
            "away_q2": pd.to_numeric(game.get("away_p2", game.get("away_q2")), errors="coerce"),
            "away_q3": pd.to_numeric(game.get("away_p3", game.get("away_q3")), errors="coerce"),
            "away_q4": pd.to_numeric(game.get("away_q4"), errors="coerce"),
            "away_ot": pd.to_numeric(game.get("away_ot"), errors="coerce"),
            # Raw per-game totals for specialty market targets (excluded from feature matrix)
            "home_shots_game": pd.to_numeric(game.get("home_shots_on_goal"), errors="coerce"),
            "away_shots_game": pd.to_numeric(game.get("away_shots_on_goal"), errors="coerce"),
            "season": season,
        }

        # Form (rolling recent games)
        h_form = self.team_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        a_form = self.team_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_form.items()})

        # Home/Away split rolling form (richer than season-wide splits)
        h_home_form = self.home_away_form(h_id, date, games_df, is_home=True)
        features.update({f"home_home_{k}": v for k, v in h_home_form.items()})
        a_away_form = self.home_away_form(a_id, date, games_df, is_home=False)
        features.update({f"away_away_{k}": v for k, v in a_away_form.items()})
        features["ha_win_pct_diff"] = h_home_form["ha_win_pct"] - a_away_form["ha_win_pct"]
        features["ha_ppg_diff"] = h_home_form["ha_ppg"] - a_away_form["ha_ppg"]

        # Season-wide home/away splits (for feature list compatibility)
        h_splits = self.home_away_splits(h_id, games_df, season)
        features["home_home_win_pct"] = h_splits["home_win_pct"]
        a_splits = self.home_away_splits(a_id, games_df, season)
        features["away_away_win_pct"] = a_splits["away_win_pct"]

        h2h = self.head_to_head(h_id, a_id, games_df, date=date)
        features.update(h2h)
        features["home_momentum"] = self.momentum(h_id, date, games_df)
        features["away_momentum"] = self.momentum(a_id, date, games_df)
        features["momentum_diff"] = features["home_momentum"] - features["away_momentum"]

        # Rest / B2B
        h_rest = self.rest_days(h_id, date, games_df)
        a_rest = self.rest_days(a_id, date, games_df)
        features["home_rest_days"] = float(h_rest)
        features["away_rest_days"] = float(a_rest)
        features["home_is_b2b"] = 1.0 if h_rest <= 1 else 0.0
        features["away_is_b2b"] = 1.0 if a_rest <= 1 else 0.0

        # Possession
        h_poss = self._possession_metrics(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_poss.items()})
        a_poss = self._possession_metrics(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_poss.items()})

        # Special teams
        h_st = self._special_teams(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_st.items()})
        a_st = self._special_teams(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_st.items()})

        # Goalie (rolling history-based)
        h_g = self._goalie_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_g.items()})
        a_g = self._goalie_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_g.items()})

        # Physical stats (hits, blocks, faceoffs, takeaways)
        h_phys = self._physical_stats(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_phys.items()})
        a_phys = self._physical_stats(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_phys.items()})

        # Standings-based team strength (NHL-specific: uses points/wins/OTL)
        h_std = self._nhl_standings_features(h_id, season)
        features.update({f"home_std_{k}": v for k, v in h_std.items()})
        a_std = self._nhl_standings_features(a_id, season)
        features.update({f"away_std_{k}": v for k, v in a_std.items()})

        # Short-window goals for/against (last 5)
        h_sw = self._short_window_goals(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_sw.items()})
        a_sw = self._short_window_goals(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_sw.items()})

        # Player-level goalie stats
        h_pg = self._player_goalie_features(h_id, date, season)
        features.update({f"home_{k}": v for k, v in h_pg.items()})
        a_pg = self._player_goalie_features(a_id, date, season)
        features.update({f"away_{k}": v for k, v in a_pg.items()})

        # Skater star-player and plus/minus features
        h_sk = self._skater_features(h_id, date, season)
        features.update({f"home_{k}": v for k, v in h_sk.items()})
        a_sk = self._skater_features(a_id, date, season)
        features.update({f"away_{k}": v for k, v in a_sk.items()})
        # Skater differentials
        for key in h_sk:
            features[f"sk_{key}_diff"] = h_sk[key] - a_sk[key]

        # ELO ratings
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Odds — with team-name fallback for hex-vs-numeric ID mismatch
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

        # ── Period Rolling Stats (NHL: P1/P2/P3) ─────────
        h_per = self._period_rolling_stats(h_id, date, games_df, n=10, period_scheme="periods")
        features.update({f"home_{k}": v for k, v in h_per.items()})
        a_per = self._period_rolling_stats(a_id, date, games_df, n=10, period_scheme="periods")
        features.update({f"away_{k}": v for k, v in a_per.items()})
        features["period_first_ppg_diff"] = h_per["period_first_ppg"] - a_per["period_first_ppg"]
        features["period_first_win_pct_diff"] = h_per["period_first_win_pct"] - a_per["period_first_win_pct"]
        features["period_comeback_diff"] = h_per["period_comeback_rate"] - a_per["period_comeback_rate"]
        features["period_first_half_win_pct_diff"] = h_per["period_first_half_win_pct"] - a_per["period_first_half_win_pct"]
        features["period_first_opp_ppg_diff"] = h_per["period_first_opp_ppg"] - a_per["period_first_opp_ppg"]
        features["period_second_half_ppg_diff"] = h_per["period_second_half_ppg"] - a_per["period_second_half_ppg"]
        features["period_ot_rate_diff"] = h_per["period_ot_rate"] - a_per["period_ot_rate"]

        # ── Key differentials ─────────────────────────────
        features["faceoff_pct_diff"] = features.get("home_faceoff_pct", 0.0) - features.get("away_faceoff_pct", 0.0)
        features["takeaway_giveaway_diff"] = features.get("home_takeaway_giveaway_ratio", 1.0) - features.get("away_takeaway_giveaway_ratio", 1.0)
        features["save_pct_diff"] = features.get("home_goalie_sv_pct", 0.0) - features.get("away_goalie_sv_pct", 0.0)
        features["shots_on_goal_diff"] = features.get("home_shots_for_pg", 0.0) - features.get("away_shots_for_pg", 0.0)
        features["pp_pct_diff"] = features.get("home_pp_pct", 0.0) - features.get("away_pp_pct", 0.0)
        features["pk_pct_diff"] = features.get("home_pk_pct", 0.0) - features.get("away_pk_pct", 0.0)
        features["net_special_teams_diff"] = features.get("home_net_special_teams", 0.0) - features.get("away_net_special_teams", 0.0)
        features["pp_trend_diff"] = features.get("home_pp_trend", 0.0) - features.get("away_pp_trend", 0.0)
        features["hits_pg_diff"] = features.get("home_hits_pg", 0.0) - features.get("away_hits_pg", 0.0)
        features["blocks_pg_diff"] = features.get("home_blocked_shots_pg", 0.0) - features.get("away_blocked_shots_pg", 0.0)
        features["standing_diff"] = features.get("home_std_stnd_win_pct", 0.0) - features.get("away_std_stnd_win_pct", 0.0)

        # ── Scoring Trend (last 5) ────────────────────────
        h_l5 = self._scoring_last_n(h_id, date, games_df, n=5)
        features["home_last5_ppg"] = h_l5["last_n_ppg"]
        features["home_last5_opp_ppg"] = h_l5["last_n_opp_ppg"]
        features["home_last5_margin"] = h_l5["last_n_margin"]
        a_l5 = self._scoring_last_n(a_id, date, games_df, n=5)
        features["away_last5_ppg"] = a_l5["last_n_ppg"]
        features["away_last5_opp_ppg"] = a_l5["last_n_opp_ppg"]
        features["away_last5_margin"] = a_l5["last_n_margin"]
        features["last5_ppg_diff"] = h_l5["last_n_ppg"] - a_l5["last_n_ppg"]
        features["last5_margin_diff"] = h_l5["last_n_margin"] - a_l5["last_n_margin"]

        # Injury burden
        h_inj = self._injury_features(h_id, season)
        a_inj = self._injury_features(a_id, season)
        for k, v in h_inj.items():
            features[f"home_{k}"] = v
        for k, v in a_inj.items():
            features[f"away_{k}"] = v
        features["injury_severity_diff"] = a_inj["injury_severity_score"] - h_inj["injury_severity_score"]

        # Strength of schedule (average opponent win% over recent games)
        features["home_sos"] = self._strength_of_schedule(h_id, date, games_df, season)
        features["away_sos"] = self._strength_of_schedule(a_id, date, games_df, season)
        features["sos_diff"] = features["home_sos"] - features["away_sos"]

        # ── OT tendencies ─────────────────────────────────
        h_ot = self._ot_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_ot.items()})
        a_ot = self._ot_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_ot.items()})
        features["ot_rate_diff"] = h_ot["ot_rate"] - a_ot["ot_rate"]
        features["ot_win_rate_diff"] = h_ot["ot_win_rate"] - a_ot["ot_win_rate"]

        # ── Lead protection ────────────────────────────────
        h_lp = self._lead_protection_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_lp.items()})
        a_lp = self._lead_protection_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_lp.items()})
        features["lead_p2_protection_diff"] = h_lp["lead_p2_protection_rate"] - a_lp["lead_p2_protection_rate"]

        # ── Starting goalie individual form ────────────────
        h_sg = self._starting_goalie_form(h_id, date, season)
        features.update({f"home_{k}": v for k, v in h_sg.items()})
        a_sg = self._starting_goalie_form(a_id, date, season)
        features.update({f"away_{k}": v for k, v in a_sg.items()})
        features["starter_sv_pct_diff"] = (
            (h_sg["starter_sv_pct"] or 0.0) - (a_sg["starter_sv_pct"] or 0.0)
        )

        # ── Close game record and save% trend ─────────────
        h_cg = self._close_game_record(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_cg.items()})
        a_cg = self._close_game_record(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_cg.items()})
        features["close_game_win_pct_diff"] = h_cg["close_game_win_pct"] - a_cg["close_game_win_pct"]
        features["one_goal_rate_diff"] = h_cg["one_goal_rate"] - a_cg["one_goal_rate"]

        h_svt = self._save_pct_trend(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_svt.items()})
        a_svt = self._save_pct_trend(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_svt.items()})
        features["sv_pct_trend_diff"] = h_svt["sv_pct_trend"] - a_svt["sv_pct_trend"]

        h_gs = self._goal_streak_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_gs.items()})
        a_gs = self._goal_streak_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_gs.items()})
        features["goal_trend_gf_diff"] = h_gs["goal_trend_gf"] - a_gs["goal_trend_gf"]
        features["goal_trend_ga_diff"] = h_gs["goal_trend_ga"] - a_gs["goal_trend_ga"]

        # Divisional/conference context + playoff position
        try:
            stnd = self.load_standings(season)
            if not stnd.empty and "team_id" in stnd.columns:
                def _sr(tid: str) -> "pd.Series":
                    r = stnd.loc[stnd["team_id"] == str(tid)]
                    return r.iloc[0] if not r.empty else pd.Series(dtype=object)
                h_sr = _sr(h_id)
                a_sr = _sr(a_id)
                h_conf = str(h_sr.get("conference", "")) if not h_sr.empty else ""
                a_conf = str(a_sr.get("conference", "")) if not a_sr.empty else ""
                h_div = str(h_sr.get("division", "")) if not h_sr.empty else ""
                a_div = str(a_sr.get("division", "")) if not a_sr.empty else ""
                features["is_nhl_divisional"] = 1.0 if (h_div and h_div == a_div) else 0.0
                features["is_nhl_conference"] = 1.0 if (h_conf and h_conf == a_conf) else 0.0
                # Top-8 conference = in playoff position
                h_cr = float(self._n(h_sr.get("conference_rank", 16), 16))
                a_cr = float(self._n(a_sr.get("conference_rank", 16), 16))
                features["home_in_playoff_pos"] = 1.0 if h_cr <= 8 else 0.0
                features["away_in_playoff_pos"] = 1.0 if a_cr <= 8 else 0.0
                features["playoff_pos_diff"] = features["home_in_playoff_pos"] - features["away_in_playoff_pos"]
            else:
                features.update({"is_nhl_divisional": 0.0, "is_nhl_conference": 0.0,
                                  "home_in_playoff_pos": 0.0, "away_in_playoff_pos": 0.0,
                                  "playoff_pos_diff": 0.0})
        except Exception:
            features.update({"is_nhl_divisional": 0.0, "is_nhl_conference": 0.0,
                              "home_in_playoff_pos": 0.0, "away_in_playoff_pos": 0.0,
                              "playoff_pos_diff": 0.0})

        # ── PP vs PK matchup (cross-side special teams advantage) ─────────
        h_pp = features.get("home_pp_pct", 0.0)
        a_pp = features.get("away_pp_pct", 0.0)
        h_pk = features.get("home_pk_pct", 0.85)
        a_pk = features.get("away_pk_pct", 0.85)
        # Home PP executing against away PK: higher = home advantage on PP
        features["pp_vs_pk_home_adv"] = h_pp - (1.0 - a_pk)
        features["pp_vs_pk_away_adv"] = a_pp - (1.0 - h_pk)
        features["pp_pk_matchup_diff"] = features["pp_vs_pk_home_adv"] - features["pp_vs_pk_away_adv"]

        # ── Quality-weighted form ──────────────────────────
        h_qf = self._quality_weighted_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_qf.items()})
        a_qf = self._quality_weighted_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_qf.items()})
        features["nhl_quality_form_diff"] = h_qf["nhl_quality_form"] - a_qf["nhl_quality_form"]
        features["nhl_quality_win_rate_diff"] = h_qf["nhl_quality_win_rate"] - a_qf["nhl_quality_win_rate"]

        return features

    def get_feature_names(self) -> list[str]:
        return [
            # Form
            "home_form_win_pct", "home_form_ppg", "home_form_opp_ppg",
            "home_form_avg_margin", "home_form_games_played",
            "away_form_win_pct", "away_form_ppg", "away_form_opp_ppg",
            "away_form_avg_margin", "away_form_games_played",
            # H2H
            "h2h_games", "h2h_win_pct", "h2h_avg_margin",
            # Momentum & splits
            "home_momentum", "away_momentum", "momentum_diff",
            "home_home_win_pct", "away_away_win_pct",
            # Home/Away rolling form
            "home_home_ha_win_pct", "home_home_ha_ppg", "home_home_ha_opp_ppg",
            "home_home_ha_avg_margin", "home_home_ha_games_played",
            "away_away_ha_win_pct", "away_away_ha_ppg", "away_away_ha_opp_ppg",
            "away_away_ha_avg_margin", "away_away_ha_games_played",
            "ha_win_pct_diff", "ha_ppg_diff",
            # Rest
            "home_rest_days", "away_rest_days", "home_is_b2b", "away_is_b2b",
            # Possession
            "home_corsi_pct", "home_fenwick_pct", "home_xgf_pct",
            "home_shots_for_pg", "home_shots_against_pg",
            "away_corsi_pct", "away_fenwick_pct", "away_xgf_pct",
            "away_shots_for_pg", "away_shots_against_pg",
            # Special teams
            "home_pp_pct", "home_pk_pct", "home_pp_opportunities_pg", "home_pim_pg",
            "home_pp_trend", "home_net_special_teams",
            "away_pp_pct", "away_pk_pct", "away_pp_opportunities_pg", "away_pim_pg",
            "away_pp_trend", "away_net_special_teams",
            # Goalie
            "home_goalie_sv_pct", "home_goalie_gaa", "home_goalie_win_pct",
            "away_goalie_sv_pct", "away_goalie_gaa", "away_goalie_win_pct",
            # Physical
            "home_hits_pg", "home_blocked_shots_pg", "home_faceoff_pct",
            "home_takeaway_giveaway_ratio", "home_shooting_pct",
            "home_shorthanded_goals_pg", "home_shootout_goals_pg",
            "away_hits_pg", "away_blocked_shots_pg", "away_faceoff_pct",
            "away_takeaway_giveaway_ratio", "away_shooting_pct",
            "away_shorthanded_goals_pg", "away_shootout_goals_pg",
            # Standings (NHL-specific: points, win%, L10, streak, rank)
            "home_std_stnd_win_pct", "home_std_stnd_points",
            "home_std_stnd_l10_win_pct", "home_std_stnd_streak",
            "home_std_stnd_pts_diff", "home_std_stnd_gp",
            "home_std_stnd_conf_rank", "home_std_stnd_div_rank",
            "away_std_stnd_win_pct", "away_std_stnd_points",
            "away_std_stnd_l10_win_pct", "away_std_stnd_streak",
            "away_std_stnd_pts_diff", "away_std_stnd_gp",
            "away_std_stnd_conf_rank", "away_std_stnd_div_rank",
            # Short-window goals (last 5 games)
            "home_gf_l5", "home_ga_l5", "home_gf_ga_ratio_l5",
            "away_gf_l5", "away_ga_l5", "away_gf_ga_ratio_l5",
            # Player goalie stats
            "home_ps_sv_pct", "home_ps_gaa",
            "away_ps_sv_pct", "away_ps_gaa",
            # Skater star-player features
            "home_avg_plus_minus", "home_top_scorer_pts_pg", "home_team_pts_pg", "home_team_shots_pg",
            "home_team_pp_goals_pg",
            "away_avg_plus_minus", "away_top_scorer_pts_pg", "away_team_pts_pg", "away_team_shots_pg",
            "away_team_pp_goals_pg",
            # Skater differentials
            "sk_avg_plus_minus_diff", "sk_top_scorer_pts_pg_diff",
            "sk_team_pts_pg_diff", "sk_team_shots_pg_diff", "sk_team_pp_goals_pg_diff",
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
            # Key differentials
            "faceoff_pct_diff", "takeaway_giveaway_diff", "save_pct_diff",
            "shots_on_goal_diff", "pp_pct_diff", "pk_pct_diff",
            "net_special_teams_diff", "pp_trend_diff",
            "hits_pg_diff", "blocks_pg_diff", "standing_diff",
            # Period rolling stats
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
            # Scoring trends (last 5)
            "home_last5_ppg", "home_last5_opp_ppg", "home_last5_margin",
            "away_last5_ppg", "away_last5_opp_ppg", "away_last5_margin",
            "last5_ppg_diff", "last5_margin_diff",
            # Injury burden
            "home_injury_count", "home_injury_severity_score", "home_injury_out_count",
            "home_injury_dtd_count", "home_injury_questionable_count",
            "away_injury_count", "away_injury_severity_score", "away_injury_out_count",
            "away_injury_dtd_count", "away_injury_questionable_count",
            "injury_severity_diff",
            # Strength of schedule
            "home_sos", "away_sos", "sos_diff",
            # OT tendencies
            "home_ot_rate", "home_ot_win_rate", "home_one_goal_rate",
            "away_ot_rate", "away_ot_win_rate", "away_one_goal_rate",
            "ot_rate_diff", "ot_win_rate_diff",
            # Lead protection
            "home_lead_p1_protection_rate", "home_lead_p2_protection_rate",
            "away_lead_p1_protection_rate", "away_lead_p2_protection_rate",
            "lead_p2_protection_diff",
            # Starting goalie individual form
            "home_starter_sv_pct", "home_starter_gaa", "home_goalie_consistency",
            "away_starter_sv_pct", "away_starter_gaa", "away_goalie_consistency",
            "starter_sv_pct_diff",
            # Close-game record and save% trend
            "home_close_game_win_pct", "home_blowout_win_pct", "home_one_goal_rate",
            "away_close_game_win_pct", "away_blowout_win_pct", "away_one_goal_rate",
            "close_game_win_pct_diff", "one_goal_rate_diff",
            "home_sv_pct_trend", "home_sv_pct_short",
            "away_sv_pct_trend", "away_sv_pct_short",
            "sv_pct_trend_diff",
            # Goal hot/cold streak
            "home_goal_streak_gf", "home_goal_streak_ga",
            "home_goal_trend_gf", "home_goal_trend_ga",
            "away_goal_streak_gf", "away_goal_streak_ga",
            "away_goal_trend_gf", "away_goal_trend_ga",
            "goal_trend_gf_diff", "goal_trend_ga_diff",
            # Divisional / conference / playoff position
            "is_nhl_divisional", "is_nhl_conference",
            "home_in_playoff_pos", "away_in_playoff_pos", "playoff_pos_diff",
        ]
