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
        self._team_id_alias_cache: dict[str, list[str]] | None = None

    def _build_team_id_aliases(self) -> dict[str, list[str]]:
        """Build a mapping from numeric NBA API team IDs to all known aliases.

        NBA player_stats uses a mix of numeric IDs ('1610612737') and
        abbreviations ('PHI', 'BOS'). Games only have numeric IDs.
        This maps numeric_id -> [numeric_id, abbreviation] so lookups
        work regardless of which format a given season uses.
        """
        if self._team_id_alias_cache is not None:
            return self._team_id_alias_cache
        aliases: dict[str, list[str]] = {}
        # Load all teams data via DuckDB reader
        name_to_abbrev: dict[str, str] = {}
        try:
            tf = self._reader.load_all_seasons(self.sport, "teams")
            if not tf.empty and "name" in tf.columns and "abbreviation" in tf.columns:
                for _, row in tf.iterrows():
                    name_to_abbrev[str(row["name"]).lower()] = str(row["abbreviation"])
        except Exception:
            pass
        # Build numeric_id -> full_name from games, then map to abbrev
        try:
            gf = self._reader.load_all_seasons(self.sport, "games")
            if not gf.empty:
                for col_id, col_name in [("home_team_id", "home_team"), ("away_team_id", "away_team")]:
                    if col_id not in gf.columns:
                        continue
                    pairs = gf[[col_id, col_name]].drop_duplicates()
                    for _, row in pairs.iterrows():
                        nid = str(row[col_id])
                        if not nid.isdigit():
                            continue
                        abbrev = name_to_abbrev.get(str(row[col_name]).lower())
                        if abbrev:
                            existing = aliases.get(nid, [nid])
                            if abbrev not in existing:
                                existing.append(abbrev)
                            aliases[nid] = existing
                        else:
                            aliases.setdefault(nid, [nid])
        except Exception:
            pass
        self._team_id_alias_cache = aliases
        return aliases

    def _filter_ps_by_team(self, ps: pd.DataFrame, team_id: str) -> pd.DataFrame:
        """Filter player_stats for a team, handling numeric/abbreviation ID mismatch.

        NBA player_stats contains a mix of numeric API IDs ('1610612737')
        and abbreviations ('PHI'). Always match ALL known aliases so we
        get the full set of player rows regardless of storage format.
        """
        stid = str(team_id)
        aliases = self._build_team_id_aliases()
        # Get all known IDs for this team (numeric + abbreviation + any others)
        all_ids = set(aliases.get(stid, [stid]))
        # If stid is an abbreviation, also check reverse (abbrev -> numeric)
        if not stid.isdigit():
            for nid, alias_list in aliases.items():
                if stid in alias_list:
                    all_ids.update(alias_list)
        mask = ps["team_id"].astype(str).isin(all_ids)
        return ps.loc[mask]

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
            "turnover_pts_pg": 0.0,
            "off_reb_pg": 0.0, "def_reb_pg": 0.0, "to_pg": 0.0,
            "stl_pg": 0.0, "blk_pg": 0.0,
            "efg_pct": 0.0, "ts_pct": 0.0, "possessions_pg": 0.0,
            "fouls_pg": 0.0, "largest_lead_pg": 0.0,
            "off_rtg_pg": 0.0, "def_rtg_pg": 0.0, "net_rtg_pg": 0.0,
            "three_pct": 0.0, "three_a_pg": 0.0, "ft_pct": 0.0,
            "ast_to_ratio": 0.0,
            "ftr": 0.0, "tov_pct": 0.0,
            "three_p_rate": 0.0, "pts_per_fga": 0.0,
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
        tp = _col_team("home_turnover_points", "away_turnover_points").sum()  # pts off turnovers
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
        # home_defensive_rating missing in NBA 2021-2026; fall back to opponent's offensive_rating
        if "home_defensive_rating" in recent.columns:
            def_rtg_raw = _col_team("home_defensive_rating", "away_defensive_rating")
        else:
            # defensive_rating(home) ≈ away_offensive_rating (opponent's offense against us)
            # defensive_rating(away) = already present as away_defensive_rating
            zero = pd.Series(np.zeros(n))
            h_def = pd.to_numeric(recent.get("away_offensive_rating", zero), errors="coerce").fillna(0)
            a_def = pd.to_numeric(recent.get("away_defensive_rating", zero), errors="coerce").fillna(0)
            def_rtg_raw = pd.Series(np.where(is_home, h_def.values, a_def.values))
        # home_net_rating missing in NBA 2021-2026; compute from off_rtg - def_rtg
        if "home_net_rating" in recent.columns:
            net_rtg_raw = _col_team("home_net_rating", "away_net_rating")
        else:
            net_rtg_raw = off_rtg_raw - def_rtg_raw
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

        # Free throw rate: FTA/FGA (ability to draw fouls and get to the line)
        ftr = float(fta / max(fga, 1))
        # Turnover % per possession: TO / (FGA + 0.44*FTA + TO)
        tov_denom = fga + 0.44 * fta + to_
        tov_pct = float(to_ / max(tov_denom, 1)) * 100.0

        # 3-point attempt rate: 3PA / FGA (how 3-point heavy is this team)
        three_p_rate = float(three_a / max(fga, 1))

        # Points per shot attempt (offensive efficiency relative to attempts)
        pts_per_fga = float(pts / max(fga, 1))

        return {
            "fast_break_pg": float(fb / n),
            "paint_pts_pg": float(pp / n),
            "second_chance_pg": float(sc / n),
            "turnover_pts_pg": float(tp / n),
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
            "ftr": ftr,
            "tov_pct": tov_pct,
            "three_p_rate": three_p_rate,
            "pts_per_fga": pts_per_fga,
        }

    def _shooting_trend(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
    ) -> dict[str, float]:
        """Shooting hot/cold: last 3 games vs prior 7 games FG% and 3P%."""
        defaults = {"fg_pct_trend": 0.0, "three_pct_trend": 0.0,
                    "pts_trend": 0.0, "short_pts_pg": 0.0}
        long = self._team_games_before(games, team_id, date, limit=10)
        if len(long) < 3:
            return defaults

        short = long.iloc[:3]
        baseline = long.iloc[3:] if len(long) > 3 else long

        def _stat(df: pd.DataFrame, col: str, default: float = 0.0) -> float:
            home, away = self._split_home_away(df, team_id)
            home_vals = pd.to_numeric(home.get(f"home_{col}", pd.Series()), errors="coerce").dropna()
            away_vals = pd.to_numeric(away.get(f"away_{col}", pd.Series()), errors="coerce").dropna()
            combined = pd.concat([home_vals, away_vals])
            return float(combined.mean()) if len(combined) > 0 else default

        s_fg = _stat(short, "field_goal_pct", 0.45)
        b_fg = _stat(baseline, "field_goal_pct", 0.45)
        s_3p = _stat(short, "three_point_pct", 0.35)
        b_3p = _stat(baseline, "three_point_pct", 0.35)
        s_pts = _stat(short, "score", 105.0)
        b_pts = _stat(baseline, "score", 105.0)

        return {
            "fg_pct_trend": s_fg - b_fg,
            "three_pct_trend": s_3p - b_3p,
            "pts_trend": s_pts - b_pts,
            "short_pts_pg": s_pts,
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

        # Derived pace / style features from extended team_stats
        for side in ("home", "away"):
            row = standings.loc[standings["team_id"].astype(str) == (h_id if side == "home" else a_id)]
            if not row.empty:
                r = row.iloc[0]
                fga = float(pd.to_numeric(r.get("avg_field_goals_attempted", 0), errors="coerce") or 0)
                tpa = float(pd.to_numeric(r.get("avg_three_point_attempted", 0), errors="coerce") or 0)
                reb_o = float(pd.to_numeric(r.get("avg_offensive_rebounds", 0), errors="coerce") or 0)
                reb_d = float(pd.to_numeric(r.get("avg_defensive_rebounds", 0), errors="coerce") or 0)
                blk = float(pd.to_numeric(r.get("avg_blocks", 0), errors="coerce") or 0)
                stl = float(pd.to_numeric(r.get("avg_steals", 0), errors="coerce") or 0)
                feats[f"{side}_ts_pace"] = fga  # shots-per-game proxy for pace
                feats[f"{side}_ts_3pa_rate"] = tpa / fga if fga > 0 else 0.33
                feats[f"{side}_ts_oreb_rate"] = reb_o / (reb_o + reb_d) if (reb_o + reb_d) > 0 else 0.25
                feats[f"{side}_ts_defense_score"] = blk + stl  # defensive disruption
            else:
                feats[f"{side}_ts_pace"] = 0.0
                feats[f"{side}_ts_3pa_rate"] = 0.33
                feats[f"{side}_ts_oreb_rate"] = 0.25
                feats[f"{side}_ts_defense_score"] = 0.0

        feats["ts_pace_diff"] = feats.get("home_ts_pace", 0) - feats.get("away_ts_pace", 0)
        feats["ts_3pa_rate_diff"] = feats.get("home_ts_3pa_rate", 0) - feats.get("away_ts_3pa_rate", 0)
        feats["ts_defense_diff"] = feats.get("home_ts_defense_score", 0) - feats.get("away_ts_defense_score", 0)

        return feats

    # ── Schedule Helpers ──────────────────────────────────

    def _quality_weighted_form(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        season: int,
        window: int = 10,
    ) -> dict[str, float]:
        """Recent form weighted by opponent quality.

        For each recent game, weights the win(+1)/loss(-1) by the
        opponent's season win-percentage (computed up to the prediction
        date, not the individual game date — fast O(log n) single lookup).
        """
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {"quality_form": 0.0, "quality_wins": 0.0, "top_opp_win_pct": 0.0}
        if recent.empty:
            return defaults

        wins = self._vec_win_flags(recent, team_id)
        results = np.where(wins.values, 1.0, -1.0)  # +1 win / -1 loss

        # Get unique opponents from recent games, compute their season win% once each
        is_home_team = recent["home_team_id"] == team_id
        opp_ids = np.where(is_home_team, recent["away_team_id"], recent["home_team_id"])

        opp_wpcents: dict[str, float] = {}
        for opp_id in set(opp_ids):
            opp_str = str(opp_id)
            opp_recent = self._team_games_before(games, opp_str, date, limit=30)
            if opp_recent.empty:
                opp_wpcents[opp_str] = 0.5
            else:
                opp_wins = self._vec_win_flags(opp_recent, opp_str)
                opp_wpcents[opp_str] = float(opp_wins.mean())

        opp_arr = np.array([opp_wpcents.get(str(o), 0.5) for o in opp_ids])
        quality_form = float(np.dot(results, opp_arr) / max(len(recent), 1))
        quality_wins = float(np.sum((results > 0) * opp_arr) / max(len(recent), 1))
        top_opp_win_pct = float(np.percentile(opp_arr, 75)) if len(opp_arr) >= 4 else float(np.mean(opp_arr))

        return {
            "quality_form": quality_form,
            "quality_wins": quality_wins,
            "top_opp_win_pct": top_opp_win_pct,
        }

    def _opp_adjusted_efficiency(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Opponent-adjusted offensive/defensive efficiency.

        Computes how this team's net margin compares to the average net
        margin of the opponents they've faced recently.  Positive values
        indicate the team is outperforming its schedule.
        Uses a single lookup per unique opponent for O(n×log n) efficiency.
        """
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {"adj_net_rtg": 0.0, "adj_off_rtg": 0.0, "pace_vs_opp": 0.0}
        if recent.empty:
            return defaults

        team_pts, opp_pts = self._vec_team_scores(recent, team_id)
        avg_margin = float((team_pts - opp_pts).mean())

        # Get unique opponents, compute their recent avg margin once each
        is_home_team = recent["home_team_id"] == team_id
        opp_ids = np.where(is_home_team, recent["away_team_id"], recent["home_team_id"])

        opp_margin_cache: dict[str, float] = {}
        for opp_id in set(opp_ids):
            opp_str = str(opp_id)
            opp_hist = self._team_games_before(games, opp_str, date, limit=10)
            if opp_hist.empty:
                opp_margin_cache[opp_str] = 0.0
            else:
                tp, op = self._vec_team_scores(opp_hist, opp_str)
                opp_margin_cache[opp_str] = float((tp - op).mean())

        mean_opp_margin = float(np.mean([opp_margin_cache.get(str(o), 0.0) for o in opp_ids]))
        adj_net_rtg = avg_margin - mean_opp_margin
        team_pts_mean = float(team_pts.mean()) if hasattr(team_pts, "mean") else float(np.mean(team_pts))
        opp_pts_mean = float(opp_pts.mean()) if hasattr(opp_pts, "mean") else float(np.mean(opp_pts))
        adj_off_rtg = team_pts_mean - opp_pts_mean
        total_pts_mean = team_pts_mean + opp_pts_mean
        pace_vs_opp = total_pts_mean - 210.0  # baseline ~210 pts/game

        return {
            "adj_net_rtg": adj_net_rtg,
            "adj_off_rtg": adj_off_rtg,
            "pace_vs_opp": pace_vs_opp,
        }

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

    def _standings_features(self, team_id: str, season: int) -> dict[str, float]:
        """NBA standings: win%, home/away splits, conference/division rank, streak."""
        defaults = {
            "stnd_win_pct": 0.5,
            "stnd_home_win_pct": 0.5,
            "stnd_away_win_pct": 0.5,
            "stnd_pts_margin": 0.0,
            "stnd_conf_rank": 8.0,
            "stnd_div_rank": 3.0,
            "stnd_overall_rank": 15.0,
            "stnd_streak": 0.0,
            "stnd_l10_win_pct": 0.5,
        }
        standings = self.load_team_stats(season)
        if standings.empty or "team_id" not in standings.columns:
            return defaults

        row_df = standings.loc[standings["team_id"].astype(str) == str(team_id)]
        if row_df.empty:
            return defaults
        row = row_df.iloc[0]

        def _pct(val) -> float:
            v = pd.to_numeric(val, errors="coerce")
            return float(v) if pd.notna(v) else 0.5

        def _n(val, default: float = 0.0) -> float:
            v = pd.to_numeric(val, errors="coerce")
            return float(v) if pd.notna(v) else default

        def _parse_record(rec: str) -> float:
            try:
                parts = str(rec).split("-")
                w, l = float(parts[0]), float(parts[1])
                t = float(parts[2]) if len(parts) > 2 else 0.0
                total = w + l + t
                return (w + 0.5 * t) / total if total > 0 else 0.5
            except (ValueError, IndexError):
                return 0.5

        wins = _n(row.get("wins"))
        losses = _n(row.get("losses"))
        gp = wins + losses
        win_pct = wins / gp if gp > 0 else 0.5

        pf = _n(row.get("points_for"))
        pa = _n(row.get("points_against"))
        pts_margin = (pf - pa) / gp if gp > 0 else 0.0

        conf_rank = _n(row.get("rank", row.get("conference_rank", 8.0)), 8.0)
        overall_rank = _n(row.get("overall_rank", 15.0), 15.0)
        div_rank = _n(row.get("division_rank", 3.0), 3.0)

        streak_val = 0.0
        streak_str = str(row.get("streak", ""))
        if streak_str and streak_str[0] in ("W", "w"):
            streak_val = _n(streak_str[1:])
        elif streak_str and streak_str[0] in ("L", "l"):
            streak_val = -_n(streak_str[1:])

        l10_pct = 0.5
        l10_str = str(row.get("last_ten", ""))
        if l10_str and l10_str not in ("nan", "None", ""):
            try:
                parts = l10_str.split("-")
                lw, ll = float(parts[0]), float(parts[1])
                lt = float(parts[2]) if len(parts) > 2 else 0.0
                ltd = lw + ll + lt
                l10_pct = lw / ltd if ltd > 0 else 0.5
            except (ValueError, IndexError):
                l10_pct = 0.5

        return {
            "stnd_win_pct": win_pct,
            "stnd_home_win_pct": _parse_record(row.get("home_record", "")),
            "stnd_away_win_pct": _parse_record(row.get("away_record", "")),
            "stnd_pts_margin": pts_margin,
            "stnd_conf_rank": conf_rank,
            "stnd_div_rank": div_rank,
            "stnd_overall_rank": overall_rank,
            "stnd_streak": streak_val,
            "stnd_l10_win_pct": l10_pct,
        }

    def _player_stats_features(self, team_id: str, season: int, date: str = "") -> dict[str, float]:
        """Team-level advanced stats from individual player_stats data (pre-date filter)."""
        defaults = {
            "ps_net_rating": 0.0,
            "ps_off_rating": 0.0,
            "ps_def_rating": 0.0,
            "ps_efg_pct": 0.5,
            "ps_ts_pct": 0.5,
            "ps_usg_pct": 0.2,
            "ps_plus_minus": 0.0,
            "ps_ast_ratio": 0.0,
            "ps_reb_pg": 0.0,
            "ps_pts_pg": 0.0,
            "ps_fg_pct": 0.45,
            "ps_three_pct": 0.35,
            "ps_stl_blk_pg": 0.0,
            "ps_to_pg": 0.0,
        }
        try:
            ps = self.load_player_stats(season)
            if ps.empty or "team_id" not in ps.columns:
                return defaults

            team_ps = self._filter_ps_by_team(ps, team_id).copy()
            if team_ps.empty:
                return defaults

            if date:
                game_date = pd.to_datetime(date, errors="coerce")
                if pd.notna(game_date) and "date" in team_ps.columns:
                    team_ps = team_ps.loc[
                        pd.to_datetime(team_ps["date"], errors="coerce") < game_date
                    ]

            if team_ps.empty:
                return defaults

            def _mean(col: str, default: float = 0.0) -> float:
                if col in team_ps.columns:
                    vals = pd.to_numeric(team_ps[col], errors="coerce").dropna()
                    return float(vals.mean()) if len(vals) > 0 else default
                return default

            stl = _mean("stl")
            blk = _mean("blk")
            return {
                "ps_net_rating": _mean("net_rating"),
                "ps_off_rating": _mean("off_rating"),
                "ps_def_rating": _mean("def_rating"),
                "ps_efg_pct": _mean("efg_pct", 0.5),
                "ps_ts_pct": _mean("ts_pct", 0.5),
                "ps_usg_pct": _mean("usg_pct", 0.2),
                "ps_plus_minus": _mean("plus_minus"),
                "ps_ast_ratio": _mean("ast_to"),
                "ps_reb_pg": _mean("reb"),
                "ps_pts_pg": _mean("pts"),
                "ps_fg_pct": _mean("fg_pct", 0.45),
                "ps_three_pct": _mean("three_pct", 0.35),
                "ps_stl_blk_pg": stl + blk,
                "ps_to_pg": _mean("to"),
            }
        except Exception:
            return defaults

    def _star_player_features(
        self,
        team_id: str,
        season: int,
        date: str,
        window: int = 7,
    ) -> dict[str, float]:
        """Usage-weighted efficiency of top-3 players over recent games."""
        defaults = {
            "star_net_rating": 0.0,
            "star_pts_pg": 0.0,
            "star_usg_pct": 0.0,
            "star_form_pm": 0.0,
            "depth_score": 0.0,
            "top_player_ts_pct": 0.5,
        }
        try:
            ps = self.load_player_stats(season)
            if ps.empty or "team_id" not in ps.columns:
                return defaults

            team_ps = self._filter_ps_by_team(ps, team_id).copy()
            if team_ps.empty or "date" not in team_ps.columns:
                return defaults

            team_ps["_date"] = pd.to_datetime(team_ps["date"], errors="coerce")
            cutoff = pd.Timestamp(date)
            team_ps = team_ps.loc[team_ps["_date"] < cutoff].sort_values("_date", ascending=False)
            if team_ps.empty:
                return defaults

            # Get most recent game_ids (up to window)
            recent_games = team_ps["game_id"].unique()[:window] if "game_id" in team_ps.columns else []
            if len(recent_games) == 0:
                return defaults
            recent_ps = team_ps.loc[team_ps["game_id"].isin(recent_games)]

            # Identify top-3 players by usage (fallback: minutes, then pts scored)
            def _rank_col(df: pd.DataFrame, cols: list) -> pd.Series:
                for col in cols:
                    if col not in df.columns:
                        continue
                    ranked = (
                        df.groupby("player_id")[col]
                        .apply(lambda x: pd.to_numeric(x, errors="coerce").dropna().mean())
                        .dropna()
                        .nlargest(3)
                    )
                    if not ranked.empty:
                        return ranked
                return pd.Series(dtype=float)

            player_usg = _rank_col(recent_ps, ["usg_pct", "minutes", "min", "pts"])
            if player_usg.empty:
                return defaults

            top_pids = player_usg.index.tolist()
            top_ps = recent_ps.loc[recent_ps["player_id"].isin(top_pids)]

            def _m(col: str, default: float = 0.0) -> float:
                if col not in top_ps.columns:
                    return default
                vals = pd.to_numeric(top_ps[col], errors="coerce").dropna()
                return float(vals.mean()) if len(vals) > 0 else default

            # Depth score: number of players averaging >= 10 pts in recent games
            pts_by_player = (
                recent_ps.groupby("player_id")["pts"]
                .apply(lambda x: pd.to_numeric(x, errors="coerce").dropna().mean())
                if "pts" in recent_ps.columns else pd.Series(dtype=float)
            )
            depth = float((pts_by_player >= 10).sum()) if len(pts_by_player) > 0 else 0.0

            # net_rating has 72% NaN in player_stats; fallback to plus_minus if unavailable
            star_net_rtg = _m("net_rating")
            if star_net_rtg == 0.0:
                # try computed off_rating - def_rating if available
                off_r = _m("off_rating", float("nan"))
                def_r = _m("def_rating", float("nan"))
                if not (pd.isna(off_r) or pd.isna(def_r)):
                    star_net_rtg = off_r - def_r
                else:
                    star_net_rtg = _m("plus_minus")
            return {
                "star_net_rating": star_net_rtg,
                "star_pts_pg": _m("pts"),
                "star_usg_pct": float(player_usg.mean()),
                "star_form_pm": _m("plus_minus"),
                "depth_score": depth,
                "top_player_ts_pct": _m("ts_pct", 0.5),
            }
        except Exception:
            return defaults

    def _star_absence_features(
        self,
        team_id: str,
        game_id: str,
        season: int,
        date: str,
        window: int = 10,
    ) -> dict[str, float]:
        """Detect if star players are absent from a specific game.

        Identifies the top-3 players by average minutes over the previous ``window``
        game appearances, then checks if those players appear in the player_stats
        for ``game_id``. Returns:
        - ``stars_present_ratio``: fraction of top-3 stars present (0-1)
        - ``stars_missing``: count of top-3 stars NOT in this game
        - ``missing_star_pts_pg``: avg pts-per-game of absent stars (higher = worse)
        """
        defaults = {
            "stars_present_ratio": 1.0,
            "stars_missing": 0.0,
            "missing_star_pts_pg": 0.0,
        }
        try:
            ps = self.load_player_stats(season)
            if ps.empty or "team_id" not in ps.columns or "game_id" not in ps.columns:
                return defaults

            team_ps = self._filter_ps_by_team(ps, team_id).copy()
            if team_ps.empty:
                return defaults

            team_ps["_date"] = pd.to_datetime(team_ps.get("date", pd.NaT), errors="coerce")
            cutoff = pd.Timestamp(date) if date else pd.NaT

            # Identify "regular starters" from the prior N games (before cutoff)
            if pd.isna(cutoff):
                return defaults
            prior_ps = team_ps.loc[team_ps["_date"] < cutoff].sort_values("_date", ascending=False)
            prior_games = prior_ps["game_id"].unique()[:window] if "game_id" in prior_ps.columns else []
            if len(prior_games) == 0:
                return defaults

            prior_in_window = prior_ps.loc[prior_ps["game_id"].isin(prior_games)]

            # Average pts and minutes per player in prior window
            def _col_mean(df: pd.DataFrame, col: str) -> pd.Series:
                return (
                    df.groupby("player_id")[col]
                    .apply(lambda x: pd.to_numeric(x, errors="coerce").dropna().mean())
                    .dropna()
                )

            min_col = "minutes" if "minutes" in prior_in_window.columns else None
            pts_col = "pts" if "pts" in prior_in_window.columns else None
            if min_col is None or pts_col is None:
                return defaults

            avg_min = _col_mean(prior_in_window, min_col)
            avg_pts = _col_mean(prior_in_window, pts_col)

            # Stars = top-3 by minutes (min threshold: >15 avg min)
            significant = avg_min[avg_min >= 15]
            if significant.empty:
                return defaults
            top_pids = significant.nlargest(3).index.tolist()

            # Check which stars appear in the current game
            current_game_ps = team_ps.loc[team_ps["game_id"] == str(game_id)]
            present_pids = set(current_game_ps["player_id"].astype(str).unique()) if not current_game_ps.empty else set()

            missing = [p for p in top_pids if str(p) not in present_pids]
            present_ratio = (len(top_pids) - len(missing)) / len(top_pids)
            missing_pts = float(avg_pts.reindex(missing).dropna().mean()) if missing and len(avg_pts) > 0 else 0.0

            return {
                "stars_present_ratio": float(present_ratio),
                "stars_missing": float(len(missing)),
                "missing_star_pts_pg": float(missing_pts) if not pd.isna(missing_pts) else 0.0,
            }
        except Exception:
            return defaults

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
            # NCAAB uses home_h1_score/home_h2_score (Q1/Q2 are 0.7% fill for NCAAB)
            # NBA/WNBA/NCAAW use native home_q1-q4 (100% fill)
            "home_q1": pd.to_numeric(game.get("home_q1", game.get("home_h1_score")), errors="coerce"),
            "home_q2": pd.to_numeric(game.get("home_q2", game.get("home_h2_score")), errors="coerce"),
            "home_q3": pd.to_numeric(game.get("home_q3"), errors="coerce"),
            "home_q4": pd.to_numeric(game.get("home_q4"), errors="coerce"),
            "home_ot": pd.to_numeric(game.get("home_ot"), errors="coerce"),
            "away_q1": pd.to_numeric(game.get("away_q1", game.get("away_h1_score")), errors="coerce"),
            "away_q2": pd.to_numeric(game.get("away_q2", game.get("away_h2_score")), errors="coerce"),
            "away_q3": pd.to_numeric(game.get("away_q3"), errors="coerce"),
            "away_q4": pd.to_numeric(game.get("away_q4"), errors="coerce"),
            "away_ot": pd.to_numeric(game.get("away_ot"), errors="coerce"),
            # Raw per-game totals for specialty market targets (excluded from feature matrix)
            "home_three_m_game": pd.to_numeric(
                game.get("home_three_m", game.get("home_three_pointers_made")), errors="coerce"
            ),
            "away_three_m_game": pd.to_numeric(
                game.get("away_three_m", game.get("away_three_pointers_made")), errors="coerce"
            ),
            # Rebounds market (NBA/WNBA: total rebounds O/U)
            "home_reb_game": pd.to_numeric(game.get("home_rebounds"), errors="coerce"),
            "away_reb_game": pd.to_numeric(game.get("away_rebounds"), errors="coerce"),
            # Turnovers market (NBA/WNBA: total turnovers O/U — live betting signal)
            "home_to_game": pd.to_numeric(game.get("home_turnovers"), errors="coerce"),
            "away_to_game": pd.to_numeric(game.get("away_turnovers"), errors="coerce"),
            # Assists market (NBA: total assists O/U)
            "home_ast_game": pd.to_numeric(game.get("home_assists"), errors="coerce"),
            "away_ast_game": pd.to_numeric(game.get("away_assists"), errors="coerce"),
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

        # ── Home/Away split form (key signal for court advantage) ─
        h_home_form = self.home_away_form(h_id, date, games_df, is_home=True)
        features.update({f"home_home_{k}": v for k, v in h_home_form.items()})
        a_away_form = self.home_away_form(a_id, date, games_df, is_home=False)
        features.update({f"away_away_{k}": v for k, v in a_away_form.items()})
        features["ha_win_pct_diff"] = h_home_form["ha_win_pct"] - a_away_form["ha_win_pct"]
        features["ha_ppg_diff"] = h_home_form["ha_ppg"] - a_away_form["ha_ppg"]

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
        # Player stats differentials (home advantage encoded into these)
        features["ps_pts_pg_diff"] = h_ps["ps_pts_pg"] - a_ps["ps_pts_pg"]
        features["ps_reb_pg_diff"] = h_ps["ps_reb_pg"] - a_ps["ps_reb_pg"]
        features["ps_fg_pct_diff"] = h_ps["ps_fg_pct"] - a_ps["ps_fg_pct"]
        features["ps_three_pct_diff"] = h_ps["ps_three_pct"] - a_ps["ps_three_pct"]
        features["ps_stl_blk_diff"] = h_ps["ps_stl_blk_pg"] - a_ps["ps_stl_blk_pg"]
        features["ps_to_pg_diff"] = h_ps["ps_to_pg"] - a_ps["ps_to_pg"]
        features["ps_off_rating_diff"] = h_ps["ps_off_rating"] - a_ps["ps_off_rating"]
        features["ps_def_rating_diff"] = h_ps["ps_def_rating"] - a_ps["ps_def_rating"]

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
        features["turnover_pts_diff"] = h_box["turnover_pts_pg"] - a_box["turnover_pts_pg"]
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
        features["ftr_diff"] = h_box["ftr"] - a_box["ftr"]
        features["tov_pct_diff"] = a_box["tov_pct"] - h_box["tov_pct"]  # lower TO% is better → invert
        features["three_p_rate_diff"] = h_box["three_p_rate"] - a_box["three_p_rate"]
        features["pts_per_fga_diff"] = h_box["pts_per_fga"] - a_box["pts_per_fga"]

        # Shooting hot/cold streak (last 3 vs prior 7)
        h_st = self._shooting_trend(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_st.items()})
        a_st = self._shooting_trend(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_st.items()})
        features["fg_pct_trend_diff"] = h_st["fg_pct_trend"] - a_st["fg_pct_trend"]
        features["pts_trend_diff"] = h_st["pts_trend"] - a_st["pts_trend"]

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

        # Star player rolling form (top-3 by usage%)
        h_star = self._star_player_features(h_id, season, date)
        features.update({f"home_{k}": v for k, v in h_star.items()})
        a_star = self._star_player_features(a_id, season, date)
        features.update({f"away_{k}": v for k, v in a_star.items()})
        features["star_net_rating_diff"] = h_star["star_net_rating"] - a_star["star_net_rating"]
        features["star_pts_pg_diff"] = h_star["star_pts_pg"] - a_star["star_pts_pg"]
        features["star_form_pm_diff"] = h_star["star_form_pm"] - a_star["star_form_pm"]
        features["depth_score_diff"] = h_star["depth_score"] - a_star["depth_score"]
        features["top_player_ts_diff"] = h_star["top_player_ts_pct"] - a_star["top_player_ts_pct"]

        # Star player absence detection (are top players actually in this game?)
        h_abs = self._star_absence_features(h_id, game_id, season, date)
        features.update({f"home_{k}": v for k, v in h_abs.items()})
        a_abs = self._star_absence_features(a_id, game_id, season, date)
        features.update({f"away_{k}": v for k, v in a_abs.items()})
        features["stars_absent_diff"] = a_abs["stars_missing"] - h_abs["stars_missing"]  # +ve = away missing more

        # ── Quality-weighted form ────────────────────────
        h_qwf = self._quality_weighted_form(h_id, date, games_df, season)
        features.update({f"home_{k}": v for k, v in h_qwf.items()})
        a_qwf = self._quality_weighted_form(a_id, date, games_df, season)
        features.update({f"away_{k}": v for k, v in a_qwf.items()})
        features["quality_form_diff"] = h_qwf["quality_form"] - a_qwf["quality_form"]
        features["quality_wins_diff"] = h_qwf["quality_wins"] - a_qwf["quality_wins"]

        # ── Opponent-adjusted efficiency ─────────────────
        h_oae = self._opp_adjusted_efficiency(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_oae.items()})
        a_oae = self._opp_adjusted_efficiency(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_oae.items()})
        features["adj_net_rtg_diff"] = h_oae["adj_net_rtg"] - a_oae["adj_net_rtg"]
        features["pace_mismatch"] = abs(h_oae["pace_vs_opp"] - a_oae["pace_vs_opp"])

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
            # Home/Away rolling form
            "home_home_ha_win_pct", "home_home_ha_ppg", "home_home_ha_opp_ppg",
            "home_home_ha_avg_margin", "home_home_ha_games_played",
            "away_away_ha_win_pct", "away_away_ha_ppg", "away_away_ha_opp_ppg",
            "away_away_ha_avg_margin", "away_away_ha_games_played",
            "ha_win_pct_diff", "ha_ppg_diff",
            # Season stats
            "home_season_win_pct", "home_season_ppg", "home_season_opp_ppg",
            "away_season_win_pct", "away_season_ppg", "away_season_opp_ppg",
            # Conference / standings
            "same_conference", "home_standings_win_pct", "away_standings_win_pct",
            # Team-stats (season aggregates from team_stats parquet)
            "home_ts_avg_points", "home_ts_avg_rebounds", "home_ts_avg_assists",
            "home_ts_avg_turnovers", "home_ts_avg_blocks", "home_ts_avg_steals",
            "home_ts_field_goal_pct", "home_ts_three_point_pct", "home_ts_free_throw_pct",
            "home_ts_scoring_efficiency", "home_ts_shooting_efficiency",
            "home_ts_assist_turnover_ratio",
            "away_ts_avg_points", "away_ts_avg_rebounds", "away_ts_avg_assists",
            "away_ts_avg_turnovers", "away_ts_avg_blocks", "away_ts_avg_steals",
            "away_ts_field_goal_pct", "away_ts_three_point_pct", "away_ts_free_throw_pct",
            "away_ts_scoring_efficiency", "away_ts_shooting_efficiency",
            "away_ts_assist_turnover_ratio",
            "ts_fg_pct_diff", "ts_ppg_diff", "ts_to_diff",
            # Extended pace/style features from team_stats
            "home_ts_pace", "home_ts_3pa_rate", "home_ts_oreb_rate", "home_ts_defense_score",
            "away_ts_pace", "away_ts_3pa_rate", "away_ts_oreb_rate", "away_ts_defense_score",
            "ts_pace_diff", "ts_3pa_rate_diff", "ts_defense_diff",
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
            "home_stnd_l10_win_pct", "home_stnd_pts_margin",
            "home_stnd_conf_rank", "home_stnd_div_rank", "home_stnd_overall_rank",
            "home_stnd_streak",
            "away_stnd_win_pct", "away_stnd_home_win_pct", "away_stnd_away_win_pct",
            "away_stnd_l10_win_pct", "away_stnd_pts_margin",
            "away_stnd_conf_rank", "away_stnd_div_rank", "away_stnd_overall_rank",
            "away_stnd_streak",
            "stnd_win_pct_diff", "stnd_rank_diff",
            # ── NEW: Player stats features ──
            "home_ps_net_rating", "home_ps_off_rating", "home_ps_def_rating",
            "home_ps_efg_pct", "home_ps_ts_pct",
            "home_ps_usg_pct", "home_ps_plus_minus", "home_ps_ast_ratio",
            "home_ps_reb_pg", "home_ps_pts_pg",
            "home_ps_fg_pct", "home_ps_three_pct", "home_ps_stl_blk_pg", "home_ps_to_pg",
            "away_ps_net_rating", "away_ps_off_rating", "away_ps_def_rating",
            "away_ps_efg_pct", "away_ps_ts_pct",
            "away_ps_usg_pct", "away_ps_plus_minus", "away_ps_ast_ratio",
            "away_ps_reb_pg", "away_ps_pts_pg",
            "away_ps_fg_pct", "away_ps_three_pct", "away_ps_stl_blk_pg", "away_ps_to_pg",
            # Player stats differentials
            "ps_pts_pg_diff", "ps_reb_pg_diff", "ps_fg_pct_diff",
            "ps_three_pct_diff", "ps_stl_blk_diff", "ps_to_pg_diff",
            "ps_off_rating_diff", "ps_def_rating_diff",
            # ── NEW: Strength of schedule ──
            "home_sos", "away_sos", "sos_diff",
            # ── NEW: Scoring trends (last 5) ──
            "home_last5_ppg", "home_last5_opp_ppg", "home_last5_margin",
            "away_last5_ppg", "away_last5_opp_ppg", "away_last5_margin",
            # ── NEW: Advanced box score stats ──
            "home_fast_break_pg", "home_paint_pts_pg", "home_second_chance_pg",
            "home_turnover_pts_pg",
            "home_off_reb_pg", "home_def_reb_pg", "home_to_pg", "home_stl_pg", "home_blk_pg",
            "home_efg_pct", "home_ts_pct", "home_possessions_pg", "home_fouls_pg", "home_largest_lead_pg",
            "home_off_rtg_pg", "home_def_rtg_pg", "home_net_rtg_pg",
            "home_three_pct", "home_three_a_pg", "home_ft_pct", "home_ast_to_ratio",
            "home_ftr", "home_tov_pct", "home_three_p_rate", "home_pts_per_fga",
            "away_fast_break_pg", "away_paint_pts_pg", "away_second_chance_pg",
            "away_turnover_pts_pg",
            "away_off_reb_pg", "away_def_reb_pg", "away_to_pg", "away_stl_pg", "away_blk_pg",
            "away_efg_pct", "away_ts_pct", "away_possessions_pg", "away_fouls_pg", "away_largest_lead_pg",
            "away_off_rtg_pg", "away_def_rtg_pg", "away_net_rtg_pg",
            "away_three_pct", "away_three_a_pg", "away_ft_pct", "away_ast_to_ratio",
            "away_ftr", "away_tov_pct", "away_three_p_rate", "away_pts_per_fga",
            "fast_break_diff", "paint_pts_diff", "turnover_pts_diff", "off_reb_diff", "to_diff",
            "efg_pct_diff", "ts_pct_diff", "pace_diff",
            "off_rtg_diff", "def_rtg_diff", "net_rtg_diff",
            "three_pct_diff", "ft_pct_diff", "ast_to_diff",
            "stl_diff", "blk_diff", "def_reb_diff",
            "ftr_diff", "tov_pct_diff", "three_p_rate_diff", "pts_per_fga_diff",
            # Shooting hot/cold streak
            "home_fg_pct_trend", "home_three_pct_trend", "home_pts_trend", "home_short_pts_pg",
            "away_fg_pct_trend", "away_three_pct_trend", "away_pts_trend", "away_short_pts_pg",
            "fg_pct_trend_diff", "pts_trend_diff",
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
            # ── Star player rolling-form features ──
            "home_star_net_rating", "home_star_pts_pg", "home_star_usg_pct",
            "home_star_form_pm", "home_depth_score", "home_top_player_ts_pct",
            "away_star_net_rating", "away_star_pts_pg", "away_star_usg_pct",
            "away_star_form_pm", "away_depth_score", "away_top_player_ts_pct",
            "star_net_rating_diff", "star_pts_pg_diff", "star_form_pm_diff",
            "depth_score_diff", "top_player_ts_diff",
            # ── Star player absence detection ──
            "home_stars_present_ratio", "home_stars_missing", "home_missing_star_pts_pg",
            "away_stars_present_ratio", "away_stars_missing", "away_missing_star_pts_pg",
            "stars_absent_diff",
        ]
