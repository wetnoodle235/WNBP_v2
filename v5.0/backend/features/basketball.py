# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Basketball
# ──────────────────────────────────────────────────────────
#
# Covers NBA, WNBA, NCAAB, NCAAW.  Produces ~55 features per
# game built from normalized parquet data.
#
# All features are derived from data that is reliably available
# in scoreboard feeds: teams, scores, dates, and standings.
# Box-score-dependent features (FGM, FGA, etc.) are NOT used
# because most game records lack detailed box-score columns.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)

# Altitude venues (feet above sea level) — meaningful for fatigue
_HIGH_ALTITUDE_VENUES: dict[str, int] = {
    "denver": 5280,
    "salt_lake_city": 4226,
    "mexico_city": 7382,
}


class BasketballExtractor(BaseFeatureExtractor):
    """Feature extractor for basketball (NBA, WNBA, NCAAB, NCAAW).

    All features are computable from scores, dates, and team
    identifiers — no box-score columns required.
    """

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._standings_conf_cache: dict[int, dict[str, str]] = {}
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

    # ── Score-Based Advanced Helpers ──────────────────────

    def _pythagorean_win_pct(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> float:
        """Pythagorean expected win % from points scored/allowed.

        Uses the NBA-calibrated exponent of 13.91 (Morey).
        """
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return 0.5

        team_pts, opp_pts = self._vec_team_scores(recent, team_id)
        pts_for = float(team_pts.sum())
        pts_against = float(opp_pts.sum())
        if pts_for + pts_against == 0:
            return 0.5
        exp = 13.91
        return pts_for ** exp / (pts_for ** exp + pts_against ** exp)

    def _score_volatility(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Scoring consistency — std dev of points and margins."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if len(recent) < 2:
            return {"score_std": 0.0, "margin_std": 0.0}

        team_pts, opp_pts = self._vec_team_scores(recent, team_id)
        return {
            "score_std": float(team_pts.std()),
            "margin_std": float((team_pts - opp_pts).std()),
        }

    def _scoring_trend(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> float:
        """Points-per-game trend (slope). Positive = improving."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if len(recent) < 3:
            return 0.0

        team_pts, opp_pts = self._vec_team_scores(recent, team_id)
        # recent is newest-first; reverse so index 0 = oldest
        pts = team_pts.values[::-1].astype(float)
        x = np.arange(len(pts), dtype=float)
        slope = float(np.polyfit(x, pts, 1)[0])
        return slope

    def _win_streak(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
    ) -> int:
        """Current win/loss streak. Positive = wins, negative = losses."""
        recent = self._team_games_before(games, team_id, date)
        if recent.empty:
            return 0

        streak = 0
        first_result = None
        for _, game in recent.iterrows():
            won = self._win_flag(game, team_id)
            if first_result is None:
                first_result = won
            if won == first_result:
                streak += 1
            else:
                break
        return streak if first_result else -streak

    def _avg_total_points(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> float:
        """Average total points in team's recent games (pace proxy)."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return 0.0

        team_pts, opp_pts = self._vec_team_scores(recent, team_id)
        return float((team_pts + opp_pts).mean())

    def _close_game_pct(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 20,
        margin: int = 5,
    ) -> float:
        """Win % in close games (decided by ≤ *margin* points)."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return 0.5

        team_pts, opp_pts = self._vec_team_scores(recent, team_id)
        close_mask = abs(team_pts - opp_pts) <= margin
        close_games = recent[close_mask.values]
        if close_games.empty:
            return 0.5
        close_wins = self._vec_win_flags(close_games, team_id)
        return float(close_wins.mean())

    def _box_score_advanced(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Rolling advanced box score stats: fast-break pts, paint pts, 2nd-chance pts,
        offensive/defensive rebounds, turnovers, steals, blocks, eFG%, TS%, pace."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {
            "fast_break_pg": 0.0, "paint_pts_pg": 0.0, "second_chance_pg": 0.0,
            "off_reb_pg": 0.0, "def_reb_pg": 0.0, "to_pg": 0.0,
            "stl_pg": 0.0, "blk_pg": 0.0,
            "efg_pct": 0.0, "ts_pct": 0.0, "possessions_pg": 0.0,
            "fouls_pg": 0.0, "largest_lead_pg": 0.0,
            "off_rtg_pg": 0.0, "def_rtg_pg": 0.0, "net_rtg_pg": 0.0,
            "three_pct": 0.0, "three_a_pg": 0.0, "ft_pct": 0.0,
            "ast_to_ratio": 0.0,
        }
        if recent.empty:
            return defaults

        is_home = (recent["home_team_id"] == team_id).values
        n = len(recent)

        def _col_team(home_col: str, away_col: str) -> pd.Series:
            zero = pd.Series(np.zeros(n))
            h = pd.to_numeric(recent[home_col], errors="coerce").fillna(0) if home_col in recent.columns else zero
            a = pd.to_numeric(recent[away_col], errors="coerce").fillna(0) if away_col in recent.columns else zero
            return pd.Series(np.where(is_home, h.values, a.values))

        fb = _col_team("home_fast_break_points", "away_fast_break_points").sum()
        pp = _col_team("home_points_in_paint", "away_points_in_paint").sum()
        sc = _col_team("home_second_chance_points", "away_second_chance_points").sum()
        orb = _col_team("home_offensive_rebounds", "away_offensive_rebounds").sum()
        drb = _col_team("home_defensive_rebounds", "away_defensive_rebounds").sum()
        to_ = _col_team("home_turnovers", "away_turnovers").sum()
        stl = _col_team("home_steals", "away_steals").sum()
        blk = _col_team("home_blocks", "away_blocks").sum()
        fouls = _col_team("home_fouls", "away_fouls").sum()
        largest_lead = _col_team("home_largest_lead", "away_largest_lead").sum()

        # Effective FG% = (FGM + 0.5*3PM) / FGA
        fgm = _col_team("home_fgm", "away_fgm").sum()
        fga = _col_team("home_fga", "away_fga").sum()
        three_m = _col_team("home_three_m", "away_three_m").sum()
        ftm = _col_team("home_ftm", "away_ftm").sum()
        fta = _col_team("home_fta", "away_fta").sum()
        pts = _col_team("home_score", "away_score").sum()
        efg = float((fgm + 0.5 * three_m) / max(fga, 1))
        # True Shooting% = PTS / (2 * (FGA + 0.44*FTA))
        ts_denom = 2 * (fga + 0.44 * fta)
        ts = float(pts / max(ts_denom, 1))

        # Possessions from game data if available, otherwise estimate
        poss_raw = _col_team("home_possessions", "away_possessions")
        valid_poss = poss_raw[poss_raw > 0]
        possessions = float(valid_poss.mean()) if len(valid_poss) > 0 else \
            float((fga + 0.44 * fta - orb + to_) / max(n, 1))

        # Offensive / Defensive / Net ratings (per-100-possessions from game data)
        off_rtg_raw = _col_team("home_offensive_rating", "away_offensive_rating")
        def_rtg_raw = _col_team("home_defensive_rating", "away_defensive_rating")
        net_rtg_raw = _col_team("home_net_rating", "away_net_rating")
        off_rtg = float(off_rtg_raw[off_rtg_raw > 0].mean()) if len(off_rtg_raw[off_rtg_raw > 0]) > 0 else 0.0
        def_rtg = float(def_rtg_raw[def_rtg_raw > 0].mean()) if len(def_rtg_raw[def_rtg_raw > 0]) > 0 else 0.0
        net_rtg = float(net_rtg_raw.mean()) if len(net_rtg_raw) > 0 else 0.0

        # 3-point shooting
        three_a = _col_team("home_three_a", "away_three_a").sum()
        three_pct = float(three_m / max(three_a, 1))

        # Free throw efficiency
        ft_pct = float(ftm / max(fta, 1))

        # Assist-to-turnover ratio
        ast = _col_team("home_assists", "away_assists").sum()
        ast_to = float(ast / max(to_, 1))

        return {
            "fast_break_pg": float(fb / n),
            "paint_pts_pg": float(pp / n),
            "second_chance_pg": float(sc / n),
            "off_reb_pg": float(orb / n),
            "def_reb_pg": float(drb / n),
            "to_pg": float(to_ / n),
            "stl_pg": float(stl / n),
            "blk_pg": float(blk / n),
            "efg_pct": efg,
            "ts_pct": ts,
            "possessions_pg": possessions,
            "fouls_pg": float(fouls / n),
            "largest_lead_pg": float(largest_lead / n),
            "off_rtg_pg": off_rtg,
            "def_rtg_pg": def_rtg,
            "net_rtg_pg": net_rtg,
            "three_pct": three_pct,
            "three_a_pg": float(three_a / n),
            "ft_pct": ft_pct,
            "ast_to_ratio": ast_to,
        }

    # ── Conference / Standings ────────────────────────────

    def _team_conference_map(self, season: int) -> dict[str, str]:
        """Build team_id → conference mapping from standings."""
        if season not in self._standings_conf_cache:
            standings = self.load_team_stats(season)
            mapping: dict[str, str] = {}
            if not standings.empty and "team_id" in standings.columns and "conference" in standings.columns:
                for _, row in standings.iterrows():
                    tid = str(row["team_id"])
                    conf = str(row.get("conference", ""))
                    if conf and conf.lower() not in ("", "none", "nan"):
                        mapping[tid] = conf
            self._standings_conf_cache[season] = mapping
        return self._standings_conf_cache[season]

    def _conference_features(
        self,
        game: dict[str, Any],
        season: int,
        games_df: "pd.DataFrame | None" = None,
    ) -> dict[str, float]:
        """Conference/division features from standings data."""
        conf_map = self._team_conference_map(season)
        if games_df is not None:
            h_id, a_id = self._resolve_game_team_ids(game, games_df)
        else:
            h_id = str(game.get("home_team_id", ""))
            a_id = str(game.get("away_team_id", ""))

        h_conf = conf_map.get(h_id, "")
        a_conf = conf_map.get(a_id, "")
        same_conf = 1.0 if (h_conf and a_conf and h_conf == a_conf) else 0.0

        standings = self.load_team_stats(season)
        feats: dict[str, float] = {"same_conference": same_conf}

        if standings.empty or "team_id" not in standings.columns:
            feats["home_standings_win_pct"] = 0.0
            feats["away_standings_win_pct"] = 0.0
            return feats

        # Season team stats columns that are relevant
        stat_cols = [
            "avg_points", "avg_rebounds", "avg_assists", "avg_turnovers",
            "avg_blocks", "avg_steals", "field_goal_pct", "three_point_pct",
            "free_throw_pct", "scoring_efficiency", "shooting_efficiency",
            "assist_turnover_ratio",
        ]

        for side, tid in [("home", h_id), ("away", a_id)]:
            row = standings.loc[standings["team_id"].astype(str) == tid]
            if not row.empty:
                r = row.iloc[0]
                pct = pd.to_numeric(r.get("pct", 0), errors="coerce")
                feats[f"{side}_standings_win_pct"] = float(pct) if pd.notna(pct) else 0.0
                for col in stat_cols:
                    v = pd.to_numeric(r.get(col), errors="coerce")
                    feats[f"{side}_ts_{col}"] = float(v) if pd.notna(v) else 0.0
            else:
                feats[f"{side}_standings_win_pct"] = 0.0
                for col in stat_cols:
                    feats[f"{side}_ts_{col}"] = 0.0

        # Differentials for key stats
        feats["ts_fg_pct_diff"] = feats.get("home_ts_field_goal_pct", 0) - feats.get("away_ts_field_goal_pct", 0)
        feats["ts_ppg_diff"] = feats.get("home_ts_avg_points", 0) - feats.get("away_ts_avg_points", 0)
        feats["ts_to_diff"] = feats.get("away_ts_avg_turnovers", 0) - feats.get("home_ts_avg_turnovers", 0)

        return feats

    # ── Schedule Helpers ──────────────────────────────────

    def _back_to_back(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
    ) -> dict[str, float]:
        """Back-to-back and schedule density features."""
        rest = self.rest_days(team_id, date, games)
        ts = pd.Timestamp(date)
        week_ago = ts - pd.Timedelta(days=7)
        recent_mask = (
            ((games.get("home_team_id") == team_id) | (games.get("away_team_id") == team_id))
            & (games["date"] >= week_ago)
            & (games["date"] < ts)
        )
        games_in_7 = int(recent_mask.sum())

        return {
            "rest_days": float(rest),
            "is_back_to_back": 1.0 if rest <= 1 else 0.0,
            "games_in_last_7": float(games_in_7),
        }

    # ── Special Features ──────────────────────────────────

    def _ist_feature(self, game: dict[str, Any]) -> float:
        """NBA In-Season Tournament indicator."""
        if self.sport != "nba":
            return 0.0
        ist_stage = game.get("ist_stage", game.get("tournament_stage", ""))
        if ist_stage and str(ist_stage).lower() not in ("", "nan", "none"):
            return 1.0
        return 0.0

    def _altitude_advantage(self, game: dict[str, Any]) -> float:
        """Altitude advantage for home team (Denver, SLC, etc.)."""
        venue = str(game.get("venue", game.get("arena", ""))).lower().replace(" ", "_")
        city = str(game.get("home_city", "")).lower().replace(" ", "_")
        home_team = str(game.get("home_team", "")).lower()
        for key, altitude in _HIGH_ALTITUDE_VENUES.items():
            if key in venue or key in city or key in home_team:
                return float(altitude)
        return 0.0

    def _odds_features_mapped(
        self,
        game_id: str,
        odds_df: pd.DataFrame,
        home_team: str = "",
        away_team: str = "",
        date: str = "",
    ) -> dict[str, float]:
        """Extract odds features, handling common column name variants.

        Falls back to team-name + date matching when game_id doesn't resolve.
        """
        defaults = {
            "home_moneyline": 0.0,
            "away_moneyline": 0.0,
            "spread": 0.0,
            "total": 0.0,
            "home_implied_prob": 0.5,
        }
        if odds_df.empty or "game_id" not in odds_df.columns:
            return defaults

        match = odds_df.loc[odds_df["game_id"] == game_id]

        # Fallback: match by team names + date
        if match.empty and home_team and away_team and date:
            def _norm(s: str) -> str:
                return (
                    str(s).lower().strip()
                    .replace("é", "e").replace("è", "e").replace("ê", "e")
                )
            date_str = str(date)[:10]
            hn, an = _norm(home_team), _norm(away_team)
            if "home_team" in odds_df.columns and "away_team" in odds_df.columns and "date" in odds_df.columns:
                mask = (
                    odds_df["date"].astype(str).str[:10] == date_str
                ) & (
                    odds_df["home_team"].apply(_norm) == hn
                ) & (
                    odds_df["away_team"].apply(_norm) == an
                )
                match = odds_df.loc[mask]

        if match.empty:
            return defaults
        row = match.iloc[0]

        h_ml = pd.to_numeric(
            row.get("home_moneyline", row.get("h2h_home", 0)), errors="coerce"
        ) or 0.0
        a_ml = pd.to_numeric(
            row.get("away_moneyline", row.get("h2h_away", 0)), errors="coerce"
        ) or 0.0
        spread_val = pd.to_numeric(
            row.get("spread", row.get("spread_home", 0)), errors="coerce"
        ) or 0.0
        total_val = pd.to_numeric(
            row.get("total", row.get("total_line", 0)), errors="coerce"
        ) or 0.0

        if h_ml < 0:
            h_prob = abs(h_ml) / (abs(h_ml) + 100)
        elif h_ml > 0:
            h_prob = 100 / (h_ml + 100)
        else:
            h_prob = 0.5

        return {
            "home_moneyline": float(h_ml),
            "away_moneyline": float(a_ml),
            "spread": float(spread_val),
            "total": float(total_val),
            "home_implied_prob": float(h_prob),
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

        # Metadata (not features, but needed for joining)
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

        # ── Form features (10-game and 5-game windows) ───
        h_form = self.team_form(h_id, date, games_df, window=10)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        h_form_5 = self.team_form(h_id, date, games_df, window=5)
        features["home_form_win_pct_l5"] = h_form_5["form_win_pct"]
        features["home_form_ppg_l5"] = h_form_5["form_ppg"]

        a_form = self.team_form(a_id, date, games_df, window=10)
        features.update({f"away_{k}": v for k, v in a_form.items()})
        a_form_5 = self.team_form(a_id, date, games_df, window=5)
        features["away_form_win_pct_l5"] = a_form_5["form_win_pct"]
        features["away_form_ppg_l5"] = a_form_5["form_ppg"]

        # ── Advanced score-based features ─────────────────
        features["home_pythag_win_pct"] = self._pythagorean_win_pct(h_id, date, games_df)
        features["away_pythag_win_pct"] = self._pythagorean_win_pct(a_id, date, games_df)

        h_vol = self._score_volatility(h_id, date, games_df)
        features["home_score_std"] = h_vol["score_std"]
        features["home_margin_std"] = h_vol["margin_std"]
        a_vol = self._score_volatility(a_id, date, games_df)
        features["away_score_std"] = a_vol["score_std"]
        features["away_margin_std"] = a_vol["margin_std"]

        features["home_scoring_trend"] = self._scoring_trend(h_id, date, games_df)
        features["away_scoring_trend"] = self._scoring_trend(a_id, date, games_df)

        features["home_win_streak"] = float(self._win_streak(h_id, date, games_df))
        features["away_win_streak"] = float(self._win_streak(a_id, date, games_df))

        features["home_avg_total_pts"] = self._avg_total_points(h_id, date, games_df)
        features["away_avg_total_pts"] = self._avg_total_points(a_id, date, games_df)

        features["home_close_game_pct"] = self._close_game_pct(h_id, date, games_df)
        features["away_close_game_pct"] = self._close_game_pct(a_id, date, games_df)

        # ── Head-to-head ──────────────────────────────────
        h2h = self.head_to_head(h_id, a_id, games_df, date=date)
        features.update(h2h)

        # ── Momentum ──────────────────────────────────────
        features["home_momentum"] = self.momentum(h_id, date, games_df)
        features["away_momentum"] = self.momentum(a_id, date, games_df)
        features["momentum_diff"] = features["home_momentum"] - features["away_momentum"]

        # ── Home/away splits ──────────────────────────────
        h_splits = self.home_away_splits(h_id, games_df, season)
        features["home_home_win_pct"] = h_splits["home_win_pct"]
        a_splits = self.home_away_splits(a_id, games_df, season)
        features["away_away_win_pct"] = a_splits["away_win_pct"]

        # ── Season stats ──────────────────────────────────
        h_season = self.season_stats(h_id, games_df, season)
        features["home_season_win_pct"] = h_season["season_win_pct"]
        features["home_season_ppg"] = h_season["season_ppg"]
        features["home_season_opp_ppg"] = h_season["season_opp_ppg"]
        a_season = self.season_stats(a_id, games_df, season)
        features["away_season_win_pct"] = a_season["season_win_pct"]
        features["away_season_ppg"] = a_season["season_ppg"]
        features["away_season_opp_ppg"] = a_season["season_opp_ppg"]

        # ── Conference / standings ────────────────────────
        conf = self._conference_features(game, season, games_df=games_df)
        features.update(conf)

        # ── Schedule / rest ───────────────────────────────
        h_b2b = self._back_to_back(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_b2b.items()})
        a_b2b = self._back_to_back(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_b2b.items()})
        features["any_b2b"] = max(h_b2b["is_back_to_back"], a_b2b["is_back_to_back"])
        features["both_b2b"] = h_b2b["is_back_to_back"] * a_b2b["is_back_to_back"]
        features["rest_advantage"] = h_b2b["rest_days"] - a_b2b["rest_days"]

        # ── Special ───────────────────────────────────────
        features["ist_game"] = self._ist_feature(game)
        features["altitude"] = self._altitude_advantage(game)

        # ── Odds (legacy) ────────────────────────────────
        odds = self._odds_features_mapped(game_id, odds_df, home_team=home_team, away_team=away_team, date=date)
        features.update(odds)

        # ── NEW: Injury Features ─────────────────────────
        h_inj = self._injury_features(h_id, season)
        features.update({f"home_{k}": v for k, v in h_inj.items()})
        a_inj = self._injury_features(a_id, season)
        features.update({f"away_{k}": v for k, v in a_inj.items()})
        features["injury_advantage"] = a_inj["injury_severity_score"] - h_inj["injury_severity_score"]

        # ── NEW: Enhanced Odds Features ──────────────────
        enh_odds = self._enhanced_odds_features(game_id, odds_df, home_team=home_team, away_team=away_team, date=date)
        features.update(enh_odds)

        # ── NEW: Market Movement Signals ────────────────
        market = self._market_signal_features(game_id, season, home_team=home_team, away_team=away_team, date=date)
        features.update(market)

        # ── NEW: Schedule Fatigue Signals ───────────────
        h_fat = self._schedule_fatigue_features(game_id, h_id, season)
        features.update({f"home_{k}": v for k, v in h_fat.items()})
        a_fat = self._schedule_fatigue_features(game_id, a_id, season)
        features.update({f"away_{k}": v for k, v in a_fat.items()})
        features["fatigue_score_diff"] = h_fat["fatigue_score"] - a_fat["fatigue_score"]

        # ── NEW: Standings Features ──────────────────────
        h_stnd = self._standings_features(h_id, season)
        features.update({f"home_{k}": v for k, v in h_stnd.items()})
        a_stnd = self._standings_features(a_id, season)
        features.update({f"away_{k}": v for k, v in a_stnd.items()})
        features["stnd_win_pct_diff"] = h_stnd["stnd_win_pct"] - a_stnd["stnd_win_pct"]
        features["stnd_rank_diff"] = a_stnd["stnd_overall_rank"] - h_stnd["stnd_overall_rank"]

        # ── NEW: Player Stats Features ───────────────────
        h_ps = self._player_stats_features(h_id, season, date=date)
        features.update({f"home_{k}": v for k, v in h_ps.items()})
        a_ps = self._player_stats_features(a_id, season, date=date)
        features.update({f"away_{k}": v for k, v in a_ps.items()})

        # ── NEW: Strength of Schedule ────────────────────
        features["home_sos"] = self._strength_of_schedule(h_id, date, games_df, season)
        features["away_sos"] = self._strength_of_schedule(a_id, date, games_df, season)
        features["sos_diff"] = features["home_sos"] - features["away_sos"]

        # ── NEW: Scoring Trends (last 5) ─────────────────
        h_l5 = self._scoring_last_n(h_id, date, games_df, n=5)
        features["home_last5_ppg"] = h_l5["last_n_ppg"]
        features["home_last5_opp_ppg"] = h_l5["last_n_opp_ppg"]
        features["home_last5_margin"] = h_l5["last_n_margin"]
        a_l5 = self._scoring_last_n(a_id, date, games_df, n=5)
        features["away_last5_ppg"] = a_l5["last_n_ppg"]
        features["away_last5_opp_ppg"] = a_l5["last_n_opp_ppg"]
        features["away_last5_margin"] = a_l5["last_n_margin"]

        # ── ELO Ratings ──────────────────────────────────
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # ── Advanced Box Score Stats ─────────────────────
        h_box = self._box_score_advanced(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_box.items()})
        a_box = self._box_score_advanced(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_box.items()})
        # Differentials
        features["fast_break_diff"] = h_box["fast_break_pg"] - a_box["fast_break_pg"]
        features["paint_pts_diff"] = h_box["paint_pts_pg"] - a_box["paint_pts_pg"]
        features["off_reb_diff"] = h_box["off_reb_pg"] - a_box["off_reb_pg"]
        features["to_diff"] = a_box["to_pg"] - h_box["to_pg"]  # positive = home advantage
        features["efg_pct_diff"] = h_box["efg_pct"] - a_box["efg_pct"]
        features["ts_pct_diff"] = h_box["ts_pct"] - a_box["ts_pct"]
        features["pace_diff"] = h_box["possessions_pg"] - a_box["possessions_pg"]
        # Advanced rating differentials
        features["off_rtg_diff"] = h_box["off_rtg_pg"] - a_box["off_rtg_pg"]
        features["def_rtg_diff"] = a_box["def_rtg_pg"] - h_box["def_rtg_pg"]  # lower def_rtg is better → invert
        features["net_rtg_diff"] = h_box["net_rtg_pg"] - a_box["net_rtg_pg"]
        features["three_pct_diff"] = h_box["three_pct"] - a_box["three_pct"]
        features["ft_pct_diff"] = h_box["ft_pct"] - a_box["ft_pct"]
        features["ast_to_diff"] = h_box["ast_to_ratio"] - a_box["ast_to_ratio"]
        features["stl_diff"] = h_box["stl_pg"] - a_box["stl_pg"]
        features["blk_diff"] = h_box["blk_pg"] - a_box["blk_pg"]
        features["def_reb_diff"] = h_box["def_reb_pg"] - a_box["def_reb_pg"]

        # ── Period/Quarter Rolling Stats ─────────────────
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

        # Player-stats differentials
        features["pstats_plus_minus_diff"] = (
            features.get("home_pstats_avg_plus_minus", 0.0) - features.get("away_pstats_avg_plus_minus", 0.0)
        )
        features["pstats_top_scorer_diff"] = (
            features.get("home_pstats_top_scorer_ppg", 0.0) - features.get("away_pstats_top_scorer_ppg", 0.0)
        )
        features["pstats_ast_to_diff"] = (
            features.get("home_pstats_ast_to_ratio", 0.0) - features.get("away_pstats_ast_to_ratio", 0.0)
        )
        features["pstats_ts_pct_diff"] = (
            features.get("home_pstats_ts_pct", 0.0) - features.get("away_pstats_ts_pct", 0.0)
        )

        return features

    def get_feature_names(self) -> list[str]:
        return [
            # Home form (10-game + 5-game)
            "home_form_win_pct", "home_form_ppg", "home_form_opp_ppg",
            "home_form_avg_margin", "home_form_games_played",
            "home_form_win_pct_l5", "home_form_ppg_l5",
            # Away form
            "away_form_win_pct", "away_form_ppg", "away_form_opp_ppg",
            "away_form_avg_margin", "away_form_games_played",
            "away_form_win_pct_l5", "away_form_ppg_l5",
            # Advanced score-based (home + away)
            "home_pythag_win_pct", "away_pythag_win_pct",
            "home_score_std", "home_margin_std",
            "away_score_std", "away_margin_std",
            "home_scoring_trend", "away_scoring_trend",
            "home_win_streak", "away_win_streak",
            "home_avg_total_pts", "away_avg_total_pts",
            "home_close_game_pct", "away_close_game_pct",
            # H2H
            "h2h_games", "h2h_win_pct", "h2h_avg_margin",
            # Momentum
            "home_momentum", "away_momentum", "momentum_diff",
            # Splits
            "home_home_win_pct", "away_away_win_pct",
            # Season stats
            "home_season_win_pct", "home_season_ppg", "home_season_opp_ppg",
            "away_season_win_pct", "away_season_ppg", "away_season_opp_ppg",
            # Conference / standings
            "same_conference", "home_standings_win_pct", "away_standings_win_pct",
            # Schedule
            "home_rest_days", "home_is_back_to_back", "home_games_in_last_7",
            "away_rest_days", "away_is_back_to_back", "away_games_in_last_7",
            "any_b2b", "both_b2b", "rest_advantage",
            # Special
            "ist_game", "altitude",
            # ELO ratings
            "home_elo", "home_elo_diff", "home_elo_expected_win",
            "away_elo", "away_elo_diff", "away_elo_expected_win",
            # Odds (legacy)
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
            # ── NEW: Injury features ──
            "home_injury_count", "home_injury_severity_score",
            "home_injury_out_count", "home_injury_dtd_count", "home_injury_questionable_count",
            "away_injury_count", "away_injury_severity_score",
            "away_injury_out_count", "away_injury_dtd_count", "away_injury_questionable_count",
            "injury_advantage",
            # ── NEW: Enhanced odds features ──
            "odds_spread", "odds_total",
            "odds_home_ml", "odds_away_ml",
            "odds_home_implied_prob", "odds_away_implied_prob",
            "odds_spread_home_line", "odds_spread_away_line",
            "odds_source_count", "odds_favorite_agreement",
            # ── NEW: Standings features ──
            "home_stnd_win_pct", "home_stnd_home_win_pct", "home_stnd_away_win_pct",
            "home_stnd_l10_win_pct", "home_stnd_games_behind",
            "home_stnd_conf_rank", "home_stnd_div_rank", "home_stnd_overall_rank",
            "home_stnd_streak", "home_stnd_pts_diff", "home_stnd_sos",
            "away_stnd_win_pct", "away_stnd_home_win_pct", "away_stnd_away_win_pct",
            "away_stnd_l10_win_pct", "away_stnd_games_behind",
            "away_stnd_conf_rank", "away_stnd_div_rank", "away_stnd_overall_rank",
            "away_stnd_streak", "away_stnd_pts_diff", "away_stnd_sos",
            "stnd_win_pct_diff", "stnd_rank_diff",
            # ── NEW: Player stats features ──
            "home_pstats_team_ppg", "home_pstats_team_rpg", "home_pstats_team_apg",
            "home_pstats_top_scorer_ppg", "home_pstats_top_scorer_share", "home_pstats_player_count",
            "home_pstats_team_spg", "home_pstats_team_bpg", "home_pstats_team_topg",
            "home_pstats_avg_plus_minus", "home_pstats_efg_pct", "home_pstats_ts_pct",
            "home_pstats_ast_to_ratio",
            "away_pstats_team_ppg", "away_pstats_team_rpg", "away_pstats_team_apg",
            "away_pstats_top_scorer_ppg", "away_pstats_top_scorer_share", "away_pstats_player_count",
            "away_pstats_team_spg", "away_pstats_team_bpg", "away_pstats_team_topg",
            "away_pstats_avg_plus_minus", "away_pstats_efg_pct", "away_pstats_ts_pct",
            "away_pstats_ast_to_ratio",
            # ── NEW: Strength of schedule ──
            "home_sos", "away_sos", "sos_diff",
            # ── NEW: Scoring trends (last 5) ──
            "home_last5_ppg", "home_last5_opp_ppg", "home_last5_margin",
            "away_last5_ppg", "away_last5_opp_ppg", "away_last5_margin",
            # ── NEW: Advanced box score stats ──
            "home_fast_break_pg", "home_paint_pts_pg", "home_second_chance_pg",
            "home_off_reb_pg", "home_def_reb_pg", "home_to_pg", "home_stl_pg", "home_blk_pg",
            "home_efg_pct", "home_ts_pct", "home_possessions_pg", "home_fouls_pg", "home_largest_lead_pg",
            "home_off_rtg_pg", "home_def_rtg_pg", "home_net_rtg_pg",
            "home_three_pct", "home_three_a_pg", "home_ft_pct", "home_ast_to_ratio",
            "away_fast_break_pg", "away_paint_pts_pg", "away_second_chance_pg",
            "away_off_reb_pg", "away_def_reb_pg", "away_to_pg", "away_stl_pg", "away_blk_pg",
            "away_efg_pct", "away_ts_pct", "away_possessions_pg", "away_fouls_pg", "away_largest_lead_pg",
            "away_off_rtg_pg", "away_def_rtg_pg", "away_net_rtg_pg",
            "away_three_pct", "away_three_a_pg", "away_ft_pct", "away_ast_to_ratio",
            "fast_break_diff", "paint_pts_diff", "off_reb_diff", "to_diff",
            "efg_pct_diff", "ts_pct_diff", "pace_diff",
            "off_rtg_diff", "def_rtg_diff", "net_rtg_diff",
            "three_pct_diff", "ft_pct_diff", "ast_to_diff",
            "stl_diff", "blk_diff", "def_reb_diff",
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
            # Player-stats differentials
            "pstats_plus_minus_diff", "pstats_top_scorer_diff",
            "pstats_ast_to_diff", "pstats_ts_pct_diff",
        ]
