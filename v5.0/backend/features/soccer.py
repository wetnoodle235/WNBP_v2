# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Soccer
# ──────────────────────────────────────────────────────────
#
# Covers EPL, La Liga, Bundesliga, Serie A, Ligue 1, MLS,
# UCL, NWSL.  Produces ~40 features per match including xG,
# possession, shots, passing, pressing, and set pieces.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor, _COMPLETED_STATUSES

logger = logging.getLogger(__name__)


class SoccerExtractor(BaseFeatureExtractor):
    """Feature extractor for soccer / football."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport
        self._all_games_cache: pd.DataFrame | None = None
        self._european_lookup: dict[str, list] | None = None  # team → sorted list of play dates

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

    def _xg_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Expected goals (if available) plus reliable scoring pattern features."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "xg": 0.0, "xga": 0.0, "xg_diff": 0.0, "xg_overperformance": 0.0,
                "clean_sheet_rate": 0.0, "scoring_rate": 0.0, "avg_goals_scored": 0.0,
                "avg_goals_conceded": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        xg_vals, xga_vals, goals, goals_conceded = [], [], [], []
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            opp = "away_" if h else "home_"
            xg_vals.append(pd.to_numeric(row.get(f"{p}xg", 0), errors="coerce") or 0.0)
            xga_vals.append(pd.to_numeric(row.get(f"{opp}xg", 0), errors="coerce") or 0.0)
            pts, opp_pts = self._team_score(pd.Series(row), team_id)
            goals.append(pts)
            goals_conceded.append(opp_pts)

        avg_xg = float(np.mean(xg_vals))
        avg_xga = float(np.mean(xga_vals))
        avg_goals = float(np.mean(goals))
        avg_conc = float(np.mean(goals_conceded))
        clean_sheet_rate = float(sum(1 for g in goals_conceded if g == 0) / max(len(goals_conceded), 1))
        scoring_rate = float(sum(1 for g in goals if g > 0) / max(len(goals), 1))

        return {
            "xg": avg_xg,
            "xga": avg_xga,
            "xg_diff": avg_xg - avg_xga,
            "xg_overperformance": avg_goals - avg_xg,
            "clean_sheet_rate": clean_sheet_rate,
            "scoring_rate": scoring_rate,
            "avg_goals_scored": avg_goals,
            "avg_goals_conceded": avg_conc,
        }

    def _possession_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Possession %, pass completion, and pressing intensity."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "possession_pct": 50.0, "pass_completion_pct": 0.0,
                "passes_pg": 0.0, "pressing_intensity": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        poss, pass_comp, pass_total, pressures = [], [], [], []
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            poss.append(pd.to_numeric(row.get(f"{p}possession", 50), errors="coerce") or 50.0)
            # Support both legacy and actual column names
            pc = (pd.to_numeric(row.get(f"{p}passes_completed", 0), errors="coerce") or
                  pd.to_numeric(row.get(f"{p}accurate_passes", 0), errors="coerce") or 0.0)
            pt = (pd.to_numeric(row.get(f"{p}passes_attempted", 0), errors="coerce") or
                  pd.to_numeric(row.get(f"{p}total_passes", 0), errors="coerce") or 0.0)
            pass_comp.append(pc)
            pass_total.append(pt)
            pressures.append(pd.to_numeric(row.get(f"{p}pressures", 0), errors="coerce") or 0.0)

        total_passes = sum(pass_total)
        n = len(recent)

        return {
            "possession_pct": float(np.mean(poss)),
            "pass_completion_pct": float(sum(pass_comp) / total_passes * 100) if total_passes > 0 else 0.0,
            "passes_pg": float(sum(pass_total) / n),
            "pressing_intensity": float(np.mean(pressures)),
        }

    def _shot_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Shots, shots on target, shot conversion, and advanced chance quality metrics."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "shots_pg": 0.0, "shots_on_target_pg": 0.0,
                "shot_accuracy": 0.0, "shot_conversion": 0.0,
                "xg_proxy": 0.0, "shot_quality_idx": 0.0,
                "saves_pg": 0.0, "crosses_pg": 0.0,
                "cross_accuracy": 0.0, "non_pen_goals_pg": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        shots, sot, goals_scored = 0.0, 0.0, 0.0
        saves, crosses, accurate_crosses, pen_goals = 0.0, 0.0, 0.0, 0.0
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            # Support both legacy and actual column names
            shots += (pd.to_numeric(row.get(f"{p}shots", 0), errors="coerce") or
                      pd.to_numeric(row.get(f"{p}total_shots", 0), errors="coerce") or 0.0)
            sot += pd.to_numeric(row.get(f"{p}shots_on_target", 0), errors="coerce") or 0.0
            pts, _ = self._team_score(pd.Series(row), team_id)
            goals_scored += pts
            # Goalkeeper quality proxy: saves made
            saves += pd.to_numeric(row.get(f"{p}saves", 0), errors="coerce") or 0.0
            # Chance creation: crosses and crossing accuracy
            crosses += (pd.to_numeric(row.get(f"{p}total_crosses", 0), errors="coerce") or
                        pd.to_numeric(row.get(f"{p}crosses", 0), errors="coerce") or 0.0)
            accurate_crosses += (pd.to_numeric(row.get(f"{p}accurate_crosses", 0), errors="coerce") or 0.0)
            # Penalty goals (non-organic scoring)
            pen_goals += pd.to_numeric(row.get(f"{p}penalty_goals", 0), errors="coerce") or 0.0

        n = len(recent)
        shots_pg = float(shots / n)
        sot_pg = float(sot / n)
        shot_accuracy = float(sot / shots * 100) if shots > 0 else 0.0
        shot_conversion = float(goals_scored / shots * 100) if shots > 0 else 0.0
        # xG proxy: shots on target × conversion rate (crude but measurable without StatsBomb data)
        xg_proxy = float(sot_pg * shot_conversion / 100.0)
        # Shot quality index: ratio of shots on target to total shots, weighted by conversion
        shot_quality_idx = float(shot_accuracy * shot_conversion / 100.0) if shot_accuracy > 0 else 0.0
        # Non-penalty goals per game (better indicator of open-play quality)
        non_pen_goals_pg = float((goals_scored - pen_goals) / n)
        return {
            "shots_pg": shots_pg,
            "shots_on_target_pg": sot_pg,
            "shot_accuracy": shot_accuracy,
            "shot_conversion": shot_conversion,
            "xg_proxy": xg_proxy,
            "shot_quality_idx": shot_quality_idx,
            "saves_pg": float(saves / n),
            "crosses_pg": float(crosses / n),
            "cross_accuracy": float(accurate_crosses / crosses * 100) if crosses > 0 else 0.0,
            "non_pen_goals_pg": non_pen_goals_pg,
        }

    def _short_window_goals(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 5,
    ) -> dict[str, float]:
        """Goals for/against last `window` games, plus draw rate and half-specific patterns."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "gf_l5": 0.0, "ga_l5": 0.0, "draw_rate_l5": 0.0,
                "scored_both_halves_rate": 0.0, "conceded_both_halves_rate": 0.0,
                "first_half_goals_pg": 0.0, "second_half_goals_pg": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")
        gf, ga, draws = 0.0, 0.0, 0
        both_halves_scored = 0
        both_halves_conceded = 0
        h1_goals = 0.0
        h2_goals = 0.0
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            op = "away_" if h else "home_"
            gs = pd.to_numeric(row.get(f"{p}score"), errors="coerce") or 0.0
            gc = pd.to_numeric(row.get(f"{op}score"), errors="coerce") or 0.0
            gf += gs
            ga += gc
            if gs == gc:
                draws += 1
            # Half-specific goals — try h1_score then ht_score (EPL/Eredivisie/Championship naming)
            h1 = pd.to_numeric(row.get(f"{p}h1_score") or row.get(f"{p}ht_score"), errors="coerce")
            h2_val = pd.to_numeric(row.get(f"{p}h2_score"), errors="coerce")
            if h1 is not None and not (isinstance(h1, float) and np.isnan(h1)):
                h1_goals += float(h1)
                if h1 > 0:
                    both_halves_scored += 0  # Only count if both halves > 0
            if h2_val is not None and not (isinstance(h2_val, float) and np.isnan(h2_val)):
                h2_goals += float(h2_val)
            # Scored in both halves
            if (h1 is not None and not (isinstance(h1, float) and np.isnan(h1)) and
                    h2_val is not None and not (isinstance(h2_val, float) and np.isnan(h2_val)) and
                    h1 > 0 and h2_val > 0):
                both_halves_scored += 1
            # Conceded in both halves — try h1_score then ht_score
            op_h1 = pd.to_numeric(row.get(f"{op}h1_score") or row.get(f"{op}ht_score"), errors="coerce")
            op_h2 = pd.to_numeric(row.get(f"{op}h2_score"), errors="coerce")
            if (op_h1 is not None and not (isinstance(op_h1, float) and np.isnan(op_h1)) and
                    op_h2 is not None and not (isinstance(op_h2, float) and np.isnan(op_h2)) and
                    op_h1 > 0 and op_h2 > 0):
                both_halves_conceded += 1

        n = len(recent)
        return {
            "gf_l5": float(gf / n),
            "ga_l5": float(ga / n),
            "draw_rate_l5": float(draws / n),
            "scored_both_halves_rate": float(both_halves_scored / n),
            "conceded_both_halves_rate": float(both_halves_conceded / n),
            "first_half_goals_pg": float(h1_goals / n),
            "second_half_goals_pg": float(h2_goals / n),
        }

    def _set_piece_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Corners, set piece goals, fouls."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {"corners_pg": 0.0, "set_piece_goals_pg": 0.0, "fouls_pg": 0.0}

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")

        corners, sp_goals, fouls = 0.0, 0.0, 0.0
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            corners += pd.to_numeric(row.get(f"{p}corners", 0), errors="coerce") or 0.0
            sp_goals += pd.to_numeric(row.get(f"{p}set_piece_goals", 0), errors="coerce") or 0.0
            fouls += pd.to_numeric(row.get(f"{p}fouls", 0), errors="coerce") or 0.0

        n = len(recent)
        return {
            "corners_pg": float(corners / n),
            "set_piece_goals_pg": float(sp_goals / n),
            "fouls_pg": float(fouls / n),
        }

    def _discipline_and_distribution(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Cards, tackles, clearances, and long-ball distribution features."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "yellow_cards_pg": 0.0, "red_cards_pg": 0.0,
                "tackle_pct": 0.0, "clearances_pg": 0.0,
                "longball_pct": 0.0, "cross_pct": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")
        n = len(recent)

        yellows, reds = 0.0, 0.0
        tackle_pct_vals, clearances, longball_pct_vals, cross_pct_vals = [], [], [], []
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            yellows += pd.to_numeric(row.get(f"{p}yellow_cards", 0), errors="coerce") or 0.0
            reds += pd.to_numeric(row.get(f"{p}red_cards", 0), errors="coerce") or 0.0
            tp = pd.to_numeric(row.get(f"{p}tackle_pct"), errors="coerce")
            if pd.notna(tp) and tp > 0:
                tackle_pct_vals.append(float(tp))
            clr = pd.to_numeric(row.get(f"{p}clearances", 0), errors="coerce") or 0.0
            clearances.append(clr)
            lbp = pd.to_numeric(row.get(f"{p}longball_pct"), errors="coerce")
            if pd.notna(lbp) and lbp > 0:
                longball_pct_vals.append(float(lbp))
            cp = pd.to_numeric(row.get(f"{p}cross_pct"), errors="coerce")
            if pd.notna(cp) and cp > 0:
                cross_pct_vals.append(float(cp))

        return {
            "yellow_cards_pg": float(yellows / n),
            "red_cards_pg": float(reds / n),
            "tackle_pct": float(np.mean(tackle_pct_vals)) if tackle_pct_vals else 0.0,
            "clearances_pg": float(np.mean(clearances)),
            "longball_pct": float(np.mean(longball_pct_vals)) if longball_pct_vals else 0.0,
            "cross_pct": float(np.mean(cross_pct_vals)) if cross_pct_vals else 0.0,
        }

    def _goalkeeper_and_defensive(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Goalkeeper saves, offsides, interceptions, and blocked shots features."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return {
                "saves_pg": 0.0, "offsides_pg": 0.0,
                "interceptions_pg": 0.0, "blocked_shots_pg": 0.0,
            }

        is_home = recent["home_team_id"] == team_id
        records = recent.to_dict("records")
        n = len(recent)

        saves, offsides, interceptions, blocked = 0.0, 0.0, 0.0, 0.0
        for row, h in zip(records, is_home):
            p = "home_" if h else "away_"
            saves += pd.to_numeric(row.get(f"{p}saves", 0), errors="coerce") or 0.0
            offsides += pd.to_numeric(row.get(f"{p}offsides", 0), errors="coerce") or 0.0
            interceptions += pd.to_numeric(row.get(f"{p}interceptions", 0), errors="coerce") or 0.0
            blocked += pd.to_numeric(row.get(f"{p}blocked_shots", 0), errors="coerce") or 0.0

        return {
            "saves_pg": float(saves / n),
            "offsides_pg": float(offsides / n),
            "interceptions_pg": float(interceptions / n),
            "blocked_shots_pg": float(blocked / n),
        }

    def _league_position_features(
        self,
        game: dict[str, Any],
        standings: pd.DataFrame,
    ) -> dict[str, float]:
        """League table position and computed soccer points."""
        feats: dict[str, float] = {
            "home_league_pos": 0.0, "away_league_pos": 0.0,
            "home_league_pts": 0.0, "away_league_pts": 0.0,
            "home_league_goals_pg": 0.0, "away_league_goals_pg": 0.0,
            "home_league_goals_against_pg": 0.0, "away_league_goals_against_pg": 0.0,
            "home_league_goal_diff": 0.0, "away_league_goal_diff": 0.0,
            "league_pos_diff": 0.0,
        }
        if standings.empty or "team_id" not in standings.columns:
            return feats

        def _num(v: Any, fb: float = 0.0) -> float:
            n = pd.to_numeric(v, errors="coerce")
            return float(n) if pd.notna(n) else fb

        cols = standings.columns.tolist()
        for side, tid in [("home", game.get("home_team_id")), ("away", game.get("away_team_id"))]:
            row = standings.loc[standings["team_id"] == str(tid)]
            if row.empty:
                continue
            r = row.iloc[0]
            wins = _num(r.get("wins"))
            losses = _num(r.get("losses"))
            ties = _num(r.get("ties"))
            # Games played: explicit column, or compute from W+L+D
            if "games_played" in cols:
                gp = _num(r.get("games_played"), 1.0)
            else:
                gp = max(1.0, wins + losses + ties)
            # Goals: try points_for / points_against (LaLiga/Bundesliga), else 0
            gf = _num(r.get("points_for", r.get("goals_for", 0.0)))
            ga = _num(r.get("points_against", r.get("goals_against", 0.0)))
            # Use explicit points if available, else compute soccer points (3W+1D)
            if "points" in cols:
                soccer_pts = _num(r.get("points"))
                if soccer_pts == 0.0:
                    soccer_pts = wins * 3.0 + ties
            else:
                soccer_pts = wins * 3.0 + ties
            # League position: rank > overall_rank > computed from points
            if "rank" in cols:
                pos = _num(r.get("rank"))
            elif "overall_rank" in cols:
                pos = _num(r.get("overall_rank"))
            else:
                pos = 0.0  # will be corrected below if 0
            feats[f"{side}_league_pos"] = pos
            feats[f"{side}_league_pts"] = soccer_pts
            feats[f"{side}_league_pts_pg"] = soccer_pts / gp if gp > 0 else 0.0
            feats[f"{side}_league_goals_pg"] = gf / gp if gp > 0 else 0.0
            feats[f"{side}_league_goals_against_pg"] = ga / gp if gp > 0 else 0.0
            feats[f"{side}_league_goal_diff"] = (gf - ga) / gp if gp > 0 else 0.0

        feats["league_pos_diff"] = feats["home_league_pos"] - feats["away_league_pos"]
        feats["league_pts_diff"] = feats.get("home_league_pts", 0.0) - feats.get("away_league_pts", 0.0)

        # Relegation/European pressure context
        total_teams = float(max(len(standings), 10))
        relegation_cut = total_teams - 2  # bottom 3
        for side in ("home", "away"):
            pos = feats[f"{side}_league_pos"]
            pts = feats[f"{side}_league_pts"]
            # Normalized position (0=top, 1=bottom)
            feats[f"{side}_pos_normalized"] = (pos - 1.0) / (total_teams - 1.0) if total_teams > 1 else 0.5
            feats[f"{side}_in_relegation_zone"] = 1.0 if pos >= relegation_cut else 0.0
            feats[f"{side}_in_top4"] = 1.0 if 0.0 < pos <= 4.0 else 0.0
            feats[f"{side}_in_top6"] = 1.0 if 0.0 < pos <= 6.0 else 0.0
            # Relegation pressure: how close in points to 3rd-from-bottom
            if not standings.empty and "points" in standings.columns:
                try:
                    pts_vals = pd.to_numeric(standings["points"], errors="coerce").dropna().sort_values()
                    relegation_threshold = float(pts_vals.iloc[2]) if len(pts_vals) >= 3 else 0.0
                    feats[f"{side}_pts_above_relegation"] = max(0.0, pts - relegation_threshold)
                except Exception:
                    feats[f"{side}_pts_above_relegation"] = 0.0
            else:
                feats[f"{side}_pts_above_relegation"] = 0.0

        feats["relegation_pressure_diff"] = (
            feats.get("away_in_relegation_zone", 0.0) - feats.get("home_in_relegation_zone", 0.0)
        )
        feats["pts_above_rel_diff"] = (
            feats.get("home_pts_above_relegation", 0.0) - feats.get("away_pts_above_relegation", 0.0)
        )
        return feats

    def _home_away_league_form(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        is_home_side: bool,
        window: int = 10,
    ) -> dict[str, float]:
        """Win rate specifically for home or away matches in the league."""
        if games.empty:
            return {"league_venue_win_pct": 0.0, "league_venue_goals_pg": 0.0}

        ts = pd.Timestamp(date)
        if is_home_side:
            venue_mask = games.get("home_team_id") == team_id
        else:
            venue_mask = games.get("away_team_id") == team_id

        status_mask = games.get("status", pd.Series("final", index=games.index)).str.lower().isin(
            _COMPLETED_STATUSES
        )
        venue_games = games.loc[venue_mask & status_mask & (games["date"] < ts)].sort_values(
            "date", ascending=False
        ).head(window)

        if venue_games.empty:
            return {"league_venue_win_pct": 0.0, "league_venue_goals_pg": 0.0}

        wins = self._vec_win_flags(venue_games, team_id)
        team_pts, opp_pts = self._vec_team_scores(venue_games, team_id)

        return {
            "league_venue_win_pct": float(wins.mean()),
            "league_venue_goals_pg": float(team_pts.mean()),
        }

    def _player_discipline_features(
        self,
        team_id: str,
        date: str,
        season: int,
        window: int = 10,
        games: pd.DataFrame | None = None,
    ) -> dict[str, float]:
        """Rolling card discipline, attacking depth, and shots stats from game-level/player stats data."""
        defaults = {
            "yellow_cards_pg": 0.0,
            "red_cards_pg": 0.0,
            "attacking_depth": 0.0,
            "top_scorer_form": 0.0,
            "shots_pg": 0.0,
            "shots_on_target_pg": 0.0,
            "saves_pg": 0.0,
            "assists_pg": 0.0,
        }
        # Primary: use game-level yellow/red card columns (much more reliable than player stats)
        if games is not None and not games.empty:
            recent = self._team_games_before(games, team_id, date, limit=window)
            if not recent.empty:
                n = len(recent)
                is_home = (recent["home_team_id"] == team_id).values

                def _col_team(home_col: str, away_col: str) -> pd.Series:
                    zero = pd.Series(np.zeros(n))
                    h = pd.to_numeric(recent[home_col], errors="coerce").fillna(0) if home_col in recent.columns else zero
                    a = pd.to_numeric(recent[away_col], errors="coerce").fillna(0) if away_col in recent.columns else zero
                    return pd.Series(np.where(is_home, h.values, a.values))

                yellows = _col_team("home_yellow_cards", "away_yellow_cards")
                reds = _col_team("home_red_cards", "away_red_cards")
                if yellows.sum() > 0 or reds.sum() > 0:
                    # Found game-level card data; save it and fall through to get shots from player stats
                    defaults["yellow_cards_pg"] = float(yellows.mean())
                    defaults["red_cards_pg"] = float(reds.mean())

        try:
            ps = self.load_player_stats(season)
            if ps.empty or "team_id" not in ps.columns:
                return defaults
            ps = ps[ps["team_id"].astype(str) == str(team_id)].copy()
            if ps.empty or "date" not in ps.columns or "game_id" not in ps.columns:
                return defaults

            ps["date"] = pd.to_datetime(ps["date"], errors="coerce")
            cutoff = pd.Timestamp(date)
            ps = ps[ps["date"] < cutoff].sort_values("date", ascending=False)

            # Take last `window` unique games
            recent_games = ps["game_id"].unique()[:window]
            ps = ps[ps["game_id"].isin(recent_games)]
            if ps.empty:
                return defaults

            n_games = len(ps["game_id"].unique())

            # Card rates
            yellow_total = pd.to_numeric(ps.get("yellow_cards", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
            red_total = pd.to_numeric(ps.get("red_cards", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()

            # Attacking depth: how many unique scorers contributed in last window games
            goals_s = pd.to_numeric(ps.get("goals", pd.Series(dtype=float)), errors="coerce").fillna(0)
            scorers = ps.loc[goals_s > 0, "player_id"].nunique() if "player_id" in ps.columns else 0

            # Top scorer form: avg goals per game for the highest-scoring player
            if "player_id" in ps.columns and goals_s.sum() > 0:
                ps_g = ps.copy()
                ps_g["_goals"] = goals_s.values
                top = ps_g.groupby("player_id")["_goals"].sum().max()
                top_scorer_rate = float(top) / n_games
            else:
                top_scorer_rate = 0.0

            # Shots and GK stats (aggregate per game, then average across games)
            shots_pg, sot_pg, saves_pg, assists_pg = 0.0, 0.0, 0.0, 0.0
            agg_cols: dict[str, tuple] = {}
            if "shots" in ps.columns:
                agg_cols["shots"] = ("shots", "sum")
            if "shots_on_target" in ps.columns:
                agg_cols["sot"] = ("shots_on_target", "sum")
            if "saves" in ps.columns:
                agg_cols["saves"] = ("saves", "sum")
            if "assists" in ps.columns:
                agg_cols["assists"] = ("assists", "sum")
            if agg_cols:
                per_game = ps.groupby("game_id").agg(**{k: v for k, v in agg_cols.items()})
                if "shots" in per_game.columns:
                    shots_pg = float(pd.to_numeric(per_game["shots"], errors="coerce").fillna(0).mean())
                if "sot" in per_game.columns:
                    sot_pg = float(pd.to_numeric(per_game["sot"], errors="coerce").fillna(0).mean())
                if "saves" in per_game.columns:
                    saves_pg = float(pd.to_numeric(per_game["saves"], errors="coerce").fillna(0).mean())
                if "assists" in per_game.columns:
                    assists_pg = float(pd.to_numeric(per_game["assists"], errors="coerce").fillna(0).mean())

            return {
                "yellow_cards_pg": float(yellow_total) / n_games,
                "red_cards_pg": float(red_total) / n_games,
                "attacking_depth": float(scorers),
                "top_scorer_form": top_scorer_rate,
                "shots_pg": shots_pg,
                "shots_on_target_pg": sot_pg,
                "saves_pg": saves_pg,
                "assists_pg": assists_pg,
            }
        except Exception:
            return defaults

    def _halftime_lead_features(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 15,
    ) -> dict[str, float]:
        """Halftime leading rate, come-from-behind win rate, first-half clean sheet rate."""
        recent = self._team_games_before(games, team_id, date, limit=window)
        defaults = {
            "ht_lead_rate": 0.0,
            "ht_draw_rate": 0.0,
            "ht_trail_rate": 0.0,
            "ht_first_goal_rate": 0.0,
            "ht_clean_sheet_rate": 0.0,
            "comeback_win_rate": 0.0,
            "lead_hold_rate": 0.0,
        }
        if recent.empty:
            return defaults

        zero_s = pd.Series(0.0, index=recent.index)
        ht_h_raw = recent["home_ht_score"] if "home_ht_score" in recent.columns else (
                   recent["home_h1"] if "home_h1" in recent.columns else zero_s)
        ht_a_raw = recent["away_ht_score"] if "away_ht_score" in recent.columns else (
                   recent["away_h1"] if "away_h1" in recent.columns else zero_s)
        ht_h = pd.to_numeric(ht_h_raw, errors="coerce").fillna(0)
        ht_a = pd.to_numeric(ht_a_raw, errors="coerce").fillna(0)
        full_h = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0)
        full_a = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0)

        is_home = recent["home_team_id"].astype(str) == str(team_id)
        team_ht = np.where(is_home, ht_h, ht_a)
        opp_ht = np.where(is_home, ht_a, ht_h)
        team_ft = np.where(is_home, full_h, full_a)
        opp_ft = np.where(is_home, full_a, full_h)

        n = len(recent)
        ht_lead = (team_ht > opp_ht).sum()
        ht_draw_cnt = (team_ht == opp_ht).sum()
        ht_trail = (team_ht < opp_ht).sum()

        # Come-from-behind: was trailing at HT but won at FT
        trailing_mask = team_ht < opp_ht
        if trailing_mask.sum() > 0:
            comeback_wins = ((trailing_mask) & (team_ft > opp_ft)).sum()
            comeback_rate = float(comeback_wins / trailing_mask.sum())
        else:
            comeback_rate = 0.0

        # Lead-hold: was leading at HT and won at FT
        leading_mask = team_ht > opp_ht
        if leading_mask.sum() > 0:
            lead_holds = ((leading_mask) & (team_ft > opp_ft)).sum()
            lead_hold_rate = float(lead_holds / leading_mask.sum())
        else:
            lead_hold_rate = 0.75  # prior

        return {
            "ht_lead_rate": float(ht_lead / n),
            "ht_draw_rate": float(ht_draw_cnt / n),
            "ht_trail_rate": float(ht_trail / n),
            "ht_first_goal_rate": float((team_ht > 0).sum() / n),
            "ht_clean_sheet_rate": float((opp_ht == 0).sum() / n),
            "comeback_win_rate": comeback_rate,
            "lead_hold_rate": lead_hold_rate,
        }

    def _load_european_lookup(self) -> dict[str, list]:
        """Build a team → sorted list of European competition play dates lookup.

        Loads UCL and Europa League parquets and indexes every team's participation
        dates so we can quickly count how many European games a team played in the
        N days before a domestic league fixture.
        """
        if self._european_lookup is not None:
            return self._european_lookup

        from datetime import date as dt_date
        lookup: dict[str, list] = {}
        comps = [("ucl", list(range(2020, 2026))), ("europa", [2024, 2025])]
        for comp, seasons in comps:
            for season in seasons:
                p = self.data_dir / "normalized" / comp / f"games_{season}.parquet"
                if not p.exists():
                    continue
                try:
                    df = pd.read_parquet(p, columns=["home_team", "away_team", "date", "status"])
                    df = df[df["status"].isin(["final", "completed", "closed", "STATUS_FINAL"]) | df["status"].isna()]
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    df = df.dropna(subset=["date"])
                    for _, row in df.iterrows():
                        d = row["date"].date() if hasattr(row["date"], "date") else row["date"]
                        for col in ("home_team", "away_team"):
                            team = row[col]
                            if pd.isna(team) or not team:
                                continue
                            if team not in lookup:
                                lookup[team] = []
                            lookup[team].append(d)
                except Exception:
                    pass

        # Sort each team's dates
        for team in lookup:
            lookup[team] = sorted(set(lookup[team]))
        self._european_lookup = lookup
        return lookup

    def _european_competition_features(
        self,
        team_name: str | None,
        game_date: str | None,
        window_days: int = 7,
    ) -> dict[str, float]:
        """Return features capturing European competition fatigue.

        - ``cl_games_last_Nd``: # of CL/Europa games the team played in the
          ``window_days`` before this fixture (max meaningful is 1-2)
        - ``in_cl_this_week``: 1 if the team played CL/Europa within 7 days
        - ``cl_game_days_ago``: days since last European game (0 if none → 99)
        """
        zeros = {
            "cl_games_last_7d": 0.0,
            "in_cl_this_week": 0.0,
            "cl_game_days_ago": 99.0,
        }
        if not team_name or not game_date:
            return zeros

        try:
            gd = pd.to_datetime(game_date).date()
        except Exception:
            return zeros

        lookup = self._load_european_lookup()
        dates = lookup.get(team_name, [])
        if not dates:
            return zeros

        from datetime import timedelta
        cutoff = gd - timedelta(days=window_days)
        recent = [d for d in dates if cutoff <= d < gd]
        days_ago = min((gd - d).days for d in dates if d < gd) if any(d < gd for d in dates) else 99

        return {
            "cl_games_last_7d": float(len(recent)),
            "in_cl_this_week": 1.0 if recent else 0.0,
            "cl_game_days_ago": float(min(days_ago, 99)),
        }

    def _matchday_context_features(
        self,
        game: dict[str, Any],
        season_games: pd.DataFrame,
    ) -> dict[str, float]:
        """Matchday number, season phase, and attendance proxy."""
        matchday = float(pd.to_numeric(game.get("matchday", 0), errors="coerce") or 0)
        attendance = float(pd.to_numeric(game.get("attendance", 0), errors="coerce") or 0)

        # Season phase: how far through the season (0=start, 1=end)
        max_md = 38.0
        if not season_games.empty and "matchday" in season_games.columns:
            md_max = pd.to_numeric(season_games["matchday"], errors="coerce").max()
            if pd.notna(md_max) and md_max > 0:
                max_md = float(md_max)

        season_progress = matchday / max_md if max_md > 0 else 0.0
        is_late_season = float(season_progress >= 0.7)
        is_early_season = float(season_progress <= 0.15)

        # Attendance proxy (normalised by typical EPL/top-league average ~35k)
        attendance_norm = min(1.0, attendance / 40000.0) if attendance > 0 else 0.5

        return {
            "matchday": matchday,
            "season_progress": season_progress,
            "is_late_season": is_late_season,
            "is_early_season": is_early_season,
            "attendance_norm": attendance_norm,
        }

    def _quality_weighted_form(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Win/draw/loss form weighted by opponent quality (win percentage).

        Weights each result by how strong the opponent was, using a single
        batch lookup per unique opponent for efficiency.
        """
        defaults = {"quality_win_rate": 0.5, "quality_form": 0.0, "quality_unbeaten": 0.5}
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return defaults

        is_home = recent["home_team_id"].astype(str) == str(team_id)
        opp_ids = np.where(is_home, recent["away_team_id"].astype(str), recent["home_team_id"].astype(str))
        h_scores = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0)
        a_scores = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0)
        team_scores = np.where(is_home, h_scores, a_scores)
        opp_scores = np.where(is_home, a_scores, h_scores)
        wins = (team_scores > opp_scores).astype(float)
        draws = (team_scores == opp_scores).astype(float)

        # Compute each unique opponent's win% up to this date
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
        quality_win_rate = float(np.dot(wins, opp_arr) / n)
        quality_form = float(np.dot(wins + 0.4 * draws - (1 - wins - draws), opp_arr) / n)
        quality_unbeaten = float(np.dot(wins + draws, opp_arr) / n)
        return {
            "quality_win_rate": quality_win_rate,
            "quality_form": quality_form,
            "quality_unbeaten": quality_unbeaten,
        }

    def _opp_adjusted_xg(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Opponent-adjusted xG attack and defense.

        Computes team's xG per game minus opponents' typical xG conceded
        (how much better/worse the attack is vs schedule difficulty).
        Same for defensive side.
        """
        defaults = {"adj_xg_attack": 0.0, "adj_xg_defense": 0.0, "adj_xg_net": 0.0}
        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty or "home_xg" not in games.columns:
            return defaults

        is_home = recent["home_team_id"].astype(str) == str(team_id)
        xg_col_h = pd.to_numeric(recent.get("home_xg", pd.Series(dtype=float)), errors="coerce").fillna(0)
        xg_col_a = pd.to_numeric(recent.get("away_xg", pd.Series(dtype=float)), errors="coerce").fillna(0)
        team_xg = np.where(is_home, xg_col_h, xg_col_a)
        opp_xg_conceded = np.where(is_home, xg_col_a, xg_col_h)

        avg_team_xg = float(np.mean(team_xg)) if len(team_xg) else 0.0
        # Get opponent defensive quality: how much xG they typically concede
        opp_ids = np.where(is_home, recent["away_team_id"].astype(str), recent["home_team_id"].astype(str))
        opp_def_xg: dict[str, float] = {}
        for opp_id in set(opp_ids):
            opp_hist = self._team_games_before(games, str(opp_id), date, limit=10)
            if opp_hist.empty or "home_xg" not in opp_hist.columns:
                opp_def_xg[str(opp_id)] = 1.5
            else:
                oh = opp_hist["home_team_id"].astype(str) == str(opp_id)
                oa_xg = pd.to_numeric(opp_hist.get("away_xg", pd.Series(dtype=float)), errors="coerce").fillna(0)
                oh_xg = pd.to_numeric(opp_hist.get("home_xg", pd.Series(dtype=float)), errors="coerce").fillna(0)
                # xG conceded by opponent = the other team's xG in their games
                opp_conc = np.where(oh, oa_xg.values, oh_xg.values)
                opp_def_xg[str(opp_id)] = float(np.mean(opp_conc)) if len(opp_conc) else 1.5

        mean_opp_def_xg = float(np.mean([opp_def_xg.get(str(o), 1.5) for o in opp_ids]))
        adj_xg_attack = avg_team_xg - mean_opp_def_xg  # +ve = team attacks well vs schedule
        adj_xg_defense = float(np.mean(opp_xg_conceded)) - 1.5  # how much opp xG team concedes vs baseline
        adj_xg_net = adj_xg_attack - adj_xg_defense

        return {
            "adj_xg_attack": adj_xg_attack,
            "adj_xg_defense": -adj_xg_defense,  # positive = better defense (fewer xG conceded vs baseline)
            "adj_xg_net": adj_xg_net,
        }

    def _form_vs_tiers(
        self,
        team_id: str,
        date: str,
        games: pd.DataFrame,
        standings: pd.DataFrame,
        window: int = 15,
    ) -> dict[str, float]:
        """Win rate vs top-tier, mid-tier, bottom-tier opponents (if standings available)."""
        defaults = {
            "form_vs_top": 0.5,
            "form_vs_mid": 0.5,
            "form_vs_bottom": 0.5,
        }
        if games.empty or standings.empty or "team_id" not in standings.columns:
            return defaults

        recent = self._team_games_before(games, team_id, date, limit=window)
        if recent.empty:
            return defaults

        # Build tier dict from standings
        if "rank" in standings.columns:
            rank_col = "rank"
        elif "points" in standings.columns:
            rank_col = "points"
        else:
            return defaults

        ranked = standings.copy()
        ranked["_rank"] = pd.to_numeric(ranked[rank_col], errors="coerce").fillna(999)
        if rank_col == "points":
            ranked["_rank"] = ranked["_rank"].rank(ascending=False)
        n_teams = len(ranked)
        top_n = max(1, n_teams // 3)
        bottom_n = max(1, n_teams // 3)
        top_ids = set(ranked.nsmallest(top_n, "_rank")["team_id"].astype(str))
        bottom_ids = set(ranked.nlargest(bottom_n, "_rank")["team_id"].astype(str))

        wins = self._vec_win_flags(recent, team_id)
        is_home = recent["home_team_id"].astype(str) == str(team_id)
        opp_ids = np.where(is_home, recent["away_team_id"].astype(str), recent["home_team_id"].astype(str))

        def _tier_win_rate(tier_set: set) -> float:
            mask = np.array([o in tier_set for o in opp_ids])
            if mask.sum() == 0:
                return 0.5
            return float(wins[mask].mean())

        return {
            "form_vs_top": _tier_win_rate(top_ids),
            "form_vs_mid": _tier_win_rate(set(ranked["team_id"].astype(str)) - top_ids - bottom_ids),
            "form_vs_bottom": _tier_win_rate(bottom_ids),
        }

    # ── Main Extraction ───────────────────────────────────

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        games_df = self._load_all_games()
        current_season_games = self.load_games(season)
        standings = self.load_team_stats(season)
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
            # Half scores — correct soccer column names
            "home_h1": pd.to_numeric(game.get("home_h1_score") or game.get("home_ht_score"), errors="coerce"),
            "home_h2": pd.to_numeric(game.get("home_h2_score"), errors="coerce"),
            "home_ot": pd.to_numeric(game.get("home_ot"), errors="coerce"),
            "away_h1": pd.to_numeric(game.get("away_h1_score") or game.get("away_ht_score"), errors="coerce"),
            "away_h2": pd.to_numeric(game.get("away_h2_score"), errors="coerce"),
            "away_ot": pd.to_numeric(game.get("away_ot"), errors="coerce"),
            # Raw game totals — used as market targets, excluded from feature matrix
            "home_corners_total": pd.to_numeric(game.get("home_corners"), errors="coerce"),
            "away_corners_total": pd.to_numeric(game.get("away_corners"), errors="coerce"),
            "home_yellow_total": pd.to_numeric(game.get("home_yellow_cards"), errors="coerce"),
            "away_yellow_total": pd.to_numeric(game.get("away_yellow_cards"), errors="coerce"),
            "home_red_total": pd.to_numeric(game.get("home_red_cards"), errors="coerce"),
            "away_red_total": pd.to_numeric(game.get("away_red_cards"), errors="coerce"),
        }

        # Common
        h_form = self.team_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_form.items()})
        a_form = self.team_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_form.items()})

        # Home/Away split form (home teams vs away teams separately — strong signal)
        h_home_form = self.home_away_form(h_id, date, games_df, is_home=True)
        features.update({f"home_home_{k}": v for k, v in h_home_form.items()})
        a_away_form = self.home_away_form(a_id, date, games_df, is_home=False)
        features.update({f"away_away_{k}": v for k, v in a_away_form.items()})
        features["ha_win_pct_diff"] = h_home_form["ha_win_pct"] - a_away_form["ha_win_pct"]
        features["ha_ppg_diff"] = h_home_form["ha_ppg"] - a_away_form["ha_ppg"]

        h2h = self.head_to_head(h_id, a_id, games_df, date=date)
        features.update(h2h)
        # Venue-specific H2H (where home team hosts)
        h2h_home = self.head_to_head_at_home(h_id, a_id, games_df, date=date)
        features.update(h2h_home)
        features["home_momentum"] = self.momentum(h_id, date, games_df)
        features["away_momentum"] = self.momentum(a_id, date, games_df)
        features["momentum_diff"] = features["home_momentum"] - features["away_momentum"]

        # Rest
        features["home_rest_days"] = float(self.rest_days(h_id, date, games_df))
        features["away_rest_days"] = float(self.rest_days(a_id, date, games_df))

        # xG
        h_xg = self._xg_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_xg.items()})
        a_xg = self._xg_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_xg.items()})

        # Possession & passing
        h_poss = self._possession_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_poss.items()})
        a_poss = self._possession_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_poss.items()})

        # Shots
        h_shots = self._shot_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_shots.items()})
        a_shots = self._shot_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_shots.items()})

        # Set pieces
        h_sp = self._set_piece_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_sp.items()})
        a_sp = self._set_piece_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_sp.items()})

        # Discipline & distribution (cards, tackles, clearances, long balls)
        h_dd = self._discipline_and_distribution(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_dd.items()})
        a_dd = self._discipline_and_distribution(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_dd.items()})

        # Goalkeeper & defensive (saves, offsides, interceptions, blocked shots)
        h_gk = self._goalkeeper_and_defensive(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_gk.items()})
        a_gk = self._goalkeeper_and_defensive(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_gk.items()})

        # League position
        league_pos = self._league_position_features(game, standings)
        features.update(league_pos)

        # Home/away league-specific form
        h_venue = self._home_away_league_form(h_id, date, games_df, is_home_side=True)
        features["home_league_home_win_pct"] = h_venue["league_venue_win_pct"]
        features["home_league_home_goals_pg"] = h_venue["league_venue_goals_pg"]
        a_venue = self._home_away_league_form(a_id, date, games_df, is_home_side=False)
        features["away_league_away_win_pct"] = a_venue["league_venue_win_pct"]
        features["away_league_away_goals_pg"] = a_venue["league_venue_goals_pg"]

        # Short-window goals (last 5)
        h_sw = self._short_window_goals(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_sw.items()})
        a_sw = self._short_window_goals(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_sw.items()})
        features["draw_rate_diff"] = h_sw["draw_rate_l5"] - a_sw["draw_rate_l5"]
        features["first_half_goal_advantage"] = h_sw["first_half_goals_pg"] - a_sw["first_half_goals_pg"]
        features["both_halves_scored_diff"] = h_sw["scored_both_halves_rate"] - a_sw["scored_both_halves_rate"]

        # Player discipline & attacking depth (from player_stats)
        h_disc = self._player_discipline_features(h_id, date, season, games=games_df)
        features.update({f"home_{k}": v for k, v in h_disc.items()})
        a_disc = self._player_discipline_features(a_id, date, season, games=games_df)
        features.update({f"away_{k}": v for k, v in a_disc.items()})
        features["card_rate_diff"] = h_disc["yellow_cards_pg"] - a_disc["yellow_cards_pg"]
        features["attacking_depth_diff"] = h_disc.get("attacking_depth", 0.0) - a_disc.get("attacking_depth", 0.0)
        features["top_scorer_form_diff"] = h_disc.get("top_scorer_form", 0.0) - a_disc.get("top_scorer_form", 0.0)
        features["assists_pg_diff"] = h_disc.get("assists_pg", 0.0) - a_disc.get("assists_pg", 0.0)

        # ELO ratings
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Half-time rolling stats (first-half goals, comeback rate, OT rate)
        h_half = self._period_rolling_stats(h_id, date, games_df, period_scheme="halves")
        features.update({f"home_{k}": v for k, v in h_half.items()})
        a_half = self._period_rolling_stats(a_id, date, games_df, period_scheme="halves")
        features.update({f"away_{k}": v for k, v in a_half.items()})
        features["period_first_ppg_diff"] = h_half["period_first_ppg"] - a_half["period_first_ppg"]
        features["period_first_opp_ppg_diff"] = h_half["period_first_opp_ppg"] - a_half["period_first_opp_ppg"]
        features["period_first_win_pct_diff"] = h_half["period_first_win_pct"] - a_half["period_first_win_pct"]
        features["period_first_half_win_pct_diff"] = h_half["period_first_half_win_pct"] - a_half["period_first_half_win_pct"]
        features["period_second_half_ppg_diff"] = h_half["period_second_half_ppg"] - a_half["period_second_half_ppg"]
        features["period_comeback_diff"] = h_half["period_comeback_rate"] - a_half["period_comeback_rate"]
        features["period_ot_rate_diff"] = h_half["period_ot_rate"] - a_half["period_ot_rate"]

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

        # European competition fatigue (UCL / Europa League)
        h_euro = self._european_competition_features(home_team, date)
        features.update({f"home_{k}": v for k, v in h_euro.items()})
        a_euro = self._european_competition_features(away_team, date)
        features.update({f"away_{k}": v for k, v in a_euro.items()})
        features["euro_fatigue_diff"] = h_euro["cl_games_last_7d"] - a_euro["cl_games_last_7d"]
        features["euro_advantage"] = (a_euro["cl_games_last_7d"] - h_euro["cl_games_last_7d"])  # +ve = home has CL advantage

        # ── Key differentials ─────────────────────────────
        features["xg_diff_adv"] = features.get("home_xg", 0.0) - features.get("away_xg", 0.0)
        features["xg_over_perf_diff"] = features.get("home_xg_overperformance", 0.0) - features.get("away_xg_overperformance", 0.0)
        features["possession_diff"] = features.get("home_possession_pct", 50.0) - features.get("away_possession_pct", 50.0)
        features["shot_accuracy_diff"] = features.get("home_shot_accuracy", 0.0) - features.get("away_shot_accuracy", 0.0)
        features["shot_conversion_diff"] = features.get("home_shot_conversion", 0.0) - features.get("away_shot_conversion", 0.0)
        features["shots_on_target_diff"] = features.get("home_shots_on_target_pg", 0.0) - features.get("away_shots_on_target_pg", 0.0)
        features["corners_diff"] = features.get("home_corners_pg", 0.0) - features.get("away_corners_pg", 0.0)
        features["saves_diff"] = features.get("home_saves_pg", 0.0) - features.get("away_saves_pg", 0.0)
        features["pass_completion_diff"] = features.get("home_pass_completion_pct", 0.0) - features.get("away_pass_completion_pct", 0.0)
        features["league_pts_diff"] = features.get("home_league_pts", 0.0) - features.get("away_league_pts", 0.0)
        # Additional defensive/physical differentials
        features["tackles_diff"] = features.get("home_tackle_pct", 0.0) - features.get("away_tackle_pct", 0.0)
        features["clearances_diff"] = features.get("home_clearances_pg", 0.0) - features.get("away_clearances_pg", 0.0)
        features["interceptions_diff"] = features.get("home_interceptions_pg", 0.0) - features.get("away_interceptions_pg", 0.0)
        features["yellow_cards_diff"] = (
            features.get("away_yellow_cards_pg", 0.0) - features.get("home_yellow_cards_pg", 0.0)
        )
        features["goals_conceded_diff"] = (
            features.get("away_goals_conceded_pg", 0.0) - features.get("home_goals_conceded_pg", 0.0)
        )
        # Play style differential: positive = home plays more direct (long ball), negative = away more direct
        features["long_ball_style_diff"] = features.get("home_longball_pct", 0.0) - features.get("away_longball_pct", 0.0)
        # xG proxy differentials (using shots-on-target × conversion rate)
        features["xg_proxy_diff"] = features.get("home_xg_proxy", 0.0) - features.get("away_xg_proxy", 0.0)
        features["shot_quality_diff"] = features.get("home_shot_quality_idx", 0.0) - features.get("away_shot_quality_idx", 0.0)
        features["saves_diff"] = features.get("home_saves_pg", 0.0) - features.get("away_saves_pg", 0.0)
        features["crosses_diff"] = features.get("home_crosses_pg", 0.0) - features.get("away_crosses_pg", 0.0)
        features["non_pen_goals_diff"] = features.get("home_non_pen_goals_pg", 0.0) - features.get("away_non_pen_goals_pg", 0.0)

        # Scoring trends (last 5 games)
        h_last5 = self._scoring_last_n(h_id, date, games_df, n=5)
        features.update({f"home_{k}": v for k, v in h_last5.items()})
        a_last5 = self._scoring_last_n(a_id, date, games_df, n=5)
        features.update({f"away_{k}": v for k, v in a_last5.items()})
        features["last5_ppg_diff"] = h_last5["last_n_ppg"] - a_last5["last_n_ppg"]
        features["last5_margin_diff"] = h_last5["last_n_margin"] - a_last5["last_n_margin"]

        # Strength of schedule (average opponent win% over recent games)
        features["home_sos"] = self._strength_of_schedule(h_id, date, games_df, season)
        features["away_sos"] = self._strength_of_schedule(a_id, date, games_df, season)
        features["sos_diff"] = features["home_sos"] - features["away_sos"]

        # Halftime lead features
        h_ht = self._halftime_lead_features(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_ht.items()})
        a_ht = self._halftime_lead_features(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_ht.items()})
        features["ht_lead_rate_diff"] = h_ht["ht_lead_rate"] - a_ht["ht_lead_rate"]
        features["ht_first_goal_rate_diff"] = h_ht["ht_first_goal_rate"] - a_ht["ht_first_goal_rate"]
        features["comeback_rate_diff"] = h_ht["comeback_win_rate"] - a_ht["comeback_win_rate"]
        features["lead_hold_rate_diff"] = h_ht["lead_hold_rate"] - a_ht["lead_hold_rate"]

        # Matchday / season context
        md_feats = self._matchday_context_features(game, current_season_games)
        features.update(md_feats)

        # Form vs opponent tiers
        h_tier = self._form_vs_tiers(h_id, date, games_df, standings)
        features.update({f"home_{k}": v for k, v in h_tier.items()})
        a_tier = self._form_vs_tiers(a_id, date, games_df, standings)
        features.update({f"away_{k}": v for k, v in a_tier.items()})
        features["form_vs_top_diff"] = h_tier["form_vs_top"] - a_tier["form_vs_top"]
        features["form_vs_mid_diff"] = h_tier["form_vs_mid"] - a_tier["form_vs_mid"]
        features["form_vs_bottom_diff"] = h_tier["form_vs_bottom"] - a_tier["form_vs_bottom"]

        # Quality-weighted form (win rate vs strong opponents)
        h_qform = self._quality_weighted_form(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_qform.items()})
        a_qform = self._quality_weighted_form(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_qform.items()})
        features["quality_form_diff"] = h_qform["quality_form"] - a_qform["quality_form"]
        features["quality_win_rate_diff"] = h_qform["quality_win_rate"] - a_qform["quality_win_rate"]
        features["quality_unbeaten_diff"] = h_qform["quality_unbeaten"] - a_qform["quality_unbeaten"]

        # Opponent-adjusted xG (strength-of-schedule adjusted attack/defense rating)
        h_axg = self._opp_adjusted_xg(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_axg.items()})
        a_axg = self._opp_adjusted_xg(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_axg.items()})
        features["adj_xg_attack_diff"] = h_axg["adj_xg_attack"] - a_axg["adj_xg_attack"]
        features["adj_xg_defense_diff"] = h_axg["adj_xg_defense"] - a_axg["adj_xg_defense"]
        features["adj_xg_net_diff"] = h_axg["adj_xg_net"] - a_axg["adj_xg_net"]

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
            "h2h_home_games", "h2h_home_win_pct", "h2h_home_avg_margin",
            # Momentum & rest
            "home_momentum", "away_momentum", "momentum_diff",
            "home_rest_days", "away_rest_days",
            # Home/Away rolling form
            "home_home_ha_win_pct", "home_home_ha_ppg", "home_home_ha_opp_ppg",
            "home_home_ha_avg_margin", "home_home_ha_games_played",
            "away_away_ha_win_pct", "away_away_ha_ppg", "away_away_ha_opp_ppg",
            "away_away_ha_avg_margin", "away_away_ha_games_played",
            "ha_win_pct_diff", "ha_ppg_diff",
            # xG and scoring patterns
            "home_xg", "home_xga", "home_xg_diff", "home_xg_overperformance",
            "home_clean_sheet_rate", "home_scoring_rate",
            "home_avg_goals_scored", "home_avg_goals_conceded",
            "away_xg", "away_xga", "away_xg_diff", "away_xg_overperformance",
            "away_clean_sheet_rate", "away_scoring_rate",
            "away_avg_goals_scored", "away_avg_goals_conceded",
            # Possession
            "home_possession_pct", "home_pass_completion_pct", "home_passes_pg", "home_pressing_intensity",
            "away_possession_pct", "away_pass_completion_pct", "away_passes_pg", "away_pressing_intensity",
            # Shots
            "home_shots_pg", "home_shots_on_target_pg", "home_shot_accuracy", "home_shot_conversion",
            "home_xg_proxy", "home_shot_quality_idx",
            "home_saves_pg", "home_crosses_pg", "home_cross_accuracy", "home_non_pen_goals_pg",
            "away_shots_pg", "away_shots_on_target_pg", "away_shot_accuracy", "away_shot_conversion",
            "away_xg_proxy", "away_shot_quality_idx",
            "away_saves_pg", "away_crosses_pg", "away_cross_accuracy", "away_non_pen_goals_pg",
            # Set pieces
            "home_corners_pg", "home_set_piece_goals_pg", "home_fouls_pg",
            "away_corners_pg", "away_set_piece_goals_pg", "away_fouls_pg",
            # Discipline & distribution (cards, tackles, clearances, long balls)
            "home_yellow_cards_pg", "home_red_cards_pg",
            "home_tackle_pct", "home_clearances_pg", "home_longball_pct", "home_cross_pct",
            "away_yellow_cards_pg", "away_red_cards_pg",
            "away_tackle_pct", "away_clearances_pg", "away_longball_pct", "away_cross_pct",
            # Goalkeeper & defensive
            "home_offsides_pg", "home_interceptions_pg", "home_blocked_shots_pg",
            "away_offsides_pg", "away_interceptions_pg", "away_blocked_shots_pg",
            # League table
            "home_league_pos", "away_league_pos", "home_league_pts", "away_league_pts",
            "home_league_pts_pg", "away_league_pts_pg",
            "home_league_goals_pg", "away_league_goals_pg",
            "home_league_goals_against_pg", "away_league_goals_against_pg",
            "home_league_goal_diff", "away_league_goal_diff",
            "league_pos_diff", "league_pts_diff",
            # Relegation / European pressure context
            "home_pos_normalized", "away_pos_normalized",
            "home_in_relegation_zone", "away_in_relegation_zone",
            "home_in_top4", "away_in_top4", "home_in_top6", "away_in_top6",
            "home_pts_above_relegation", "away_pts_above_relegation",
            "relegation_pressure_diff", "pts_above_rel_diff",
            # Venue-specific form
            "home_league_home_win_pct", "home_league_home_goals_pg",
            "away_league_away_win_pct", "away_league_away_goals_pg",
            # Player discipline & attacking depth (from player_stats)
            "home_attacking_depth", "home_top_scorer_form", "home_assists_pg",
            "away_attacking_depth", "away_top_scorer_form", "away_assists_pg",
            "card_rate_diff", "attacking_depth_diff", "top_scorer_form_diff", "assists_pg_diff",
            # Short-window goals & draw tendency (last 5)
            "home_gf_l5", "home_ga_l5", "home_draw_rate_l5",
            "away_gf_l5", "away_ga_l5", "away_draw_rate_l5",
            "draw_rate_diff",
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
            "xg_diff_adv", "xg_over_perf_diff", "possession_diff",
            "shot_accuracy_diff", "shot_conversion_diff", "shots_on_target_diff",
            "xg_proxy_diff", "shot_quality_diff",
            "saves_diff", "crosses_diff", "non_pen_goals_diff",
            "corners_diff", "pass_completion_diff",
            "tackles_diff", "clearances_diff", "interceptions_diff",
            "yellow_cards_diff", "goals_conceded_diff", "long_ball_style_diff",
            # Half-time rolling stats
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
            "home_last_n_ppg", "home_last_n_opp_ppg", "home_last_n_margin",
            "away_last_n_ppg", "away_last_n_opp_ppg", "away_last_n_margin",
            "last5_ppg_diff", "last5_margin_diff",
            # Strength of schedule
            "home_sos", "away_sos", "sos_diff",
            # Halftime lead features
            "home_ht_lead_rate", "home_ht_draw_rate", "home_ht_trail_rate",
            "home_ht_first_goal_rate", "home_ht_clean_sheet_rate",
            "home_comeback_win_rate", "home_lead_hold_rate",
            "away_ht_lead_rate", "away_ht_draw_rate", "away_ht_trail_rate",
            "away_ht_first_goal_rate", "away_ht_clean_sheet_rate",
            "away_comeback_win_rate", "away_lead_hold_rate",
            "ht_lead_rate_diff", "ht_first_goal_rate_diff",
            "comeback_rate_diff", "lead_hold_rate_diff",
            # Matchday / season context
            "matchday", "season_progress", "is_late_season", "is_early_season", "attendance_norm",
            # Form vs opponent tiers
            "home_form_vs_top", "home_form_vs_mid", "home_form_vs_bottom",
            "away_form_vs_top", "away_form_vs_mid", "away_form_vs_bottom",
            "form_vs_top_diff", "form_vs_mid_diff", "form_vs_bottom_diff",
            # European competition fatigue (UCL / Europa League)
            "home_cl_games_last_7d", "home_in_cl_this_week", "home_cl_game_days_ago",
            "away_cl_games_last_7d", "away_in_cl_this_week", "away_cl_game_days_ago",
            "euro_fatigue_diff", "euro_advantage",
        ]
