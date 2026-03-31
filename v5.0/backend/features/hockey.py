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

        pp_opp = (
            _pp_opp("home_power_play_attempts", "away_power_play_attempts", home, away) or
            _pp_opp("home_pp_opportunities", "away_pp_opportunities", home, away)
        )
        pk_opp = (
            _pp_opp("away_power_play_attempts", "home_power_play_attempts", home, away) or
            _pp_opp("away_pp_opportunities", "home_pp_opportunities", home, away)
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

        return {
            "pp_pct": pp_pct_val,
            "pk_pct": pk_pct_val,
            "pp_opportunities_pg": float(pp_opp / n),
            "pim_pg": float(pim_total / n),
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

        hits = (self._col_sum(home, "home_hits_nhl") + self._col_sum(home, "home_hits") +
                self._col_sum(away, "away_hits_nhl") + self._col_sum(away, "away_hits"))
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

    def _nhl_standings_features(self, team_id: str, season: int) -> dict[str, float]:
        """Parse NHL standings: uses points, wins/losses/otl, W-L-OTL last-ten."""
        defaults = {
            "stnd_win_pct": 0.5,
            "stnd_points": 82.0,
            "stnd_l10_win_pct": 0.5,
            "stnd_streak": 0.0,
            "stnd_pts_diff": 0.0,
            "stnd_gp": 0.0,
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

        return {
            "stnd_win_pct": win_pct,
            "stnd_points": points,
            "stnd_l10_win_pct": l10_pct,
            "stnd_streak": streak_val,
            "stnd_pts_diff": pts_diff,
            "stnd_gp": gp,
        }

    # ── Main Extraction ───────────────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        games_df = self.load_games(season)
        odds_df = self.load_odds(season)

        h_id, a_id = self._resolve_game_team_ids(game, games_df)
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
            "season": season,
        }

        # Form (rolling recent games)
        h_form = self.team_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        a_form = self.team_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_form.items()})

        h2h = self.head_to_head(h_id, a_id, games_df, date=date)
        features.update(h2h)
        features["home_momentum"] = self.momentum(h_id, date, games_df)
        features["away_momentum"] = self.momentum(a_id, date, games_df)

        h_splits = self.home_away_splits(h_id, games_df, season)
        features["home_home_win_pct"] = h_splits["home_win_pct"]
        a_splits = self.home_away_splits(a_id, games_df, season)
        features["away_away_win_pct"] = a_splits["away_win_pct"]

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
            "home_momentum", "away_momentum",
            "home_home_win_pct", "away_away_win_pct",
            # Rest
            "home_rest_days", "away_rest_days", "home_is_b2b", "away_is_b2b",
            # Possession
            "home_corsi_pct", "home_fenwick_pct", "home_xgf_pct",
            "home_shots_for_pg", "home_shots_against_pg",
            "away_corsi_pct", "away_fenwick_pct", "away_xgf_pct",
            "away_shots_for_pg", "away_shots_against_pg",
            # Special teams
            "home_pp_pct", "home_pk_pct", "home_pp_opportunities_pg", "home_pim_pg",
            "away_pp_pct", "away_pk_pct", "away_pp_opportunities_pg", "away_pim_pg",
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
            # Standings (NHL-specific: points, win%, L10, streak)
            "home_std_stnd_win_pct", "home_std_stnd_points",
            "home_std_stnd_l10_win_pct", "home_std_stnd_streak",
            "home_std_stnd_pts_diff", "home_std_stnd_gp",
            "away_std_stnd_win_pct", "away_std_stnd_points",
            "away_std_stnd_l10_win_pct", "away_std_stnd_streak",
            "away_std_stnd_pts_diff", "away_std_stnd_gp",
            # Short-window goals (last 5 games)
            "home_gf_l5", "home_ga_l5", "home_gf_ga_ratio_l5",
            "away_gf_l5", "away_ga_l5", "away_gf_ga_ratio_l5",
            # Player goalie stats
            "home_ps_sv_pct", "home_ps_gaa",
            "away_ps_sv_pct", "away_ps_gaa",
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
        ]
