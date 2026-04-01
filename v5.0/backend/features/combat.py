# ──────────────────────────────────────────────────────────
# V5.0 Backend — Feature Extraction: Combat Sports
# ──────────────────────────────────────────────────────────
#
# Covers UFC / MMA.  Produces ~30 features per fight including
# striking accuracy, takedown defense, submissions, reach,
# age, win streaks, finishing rate, and style matchup.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.base import BaseFeatureExtractor

logger = logging.getLogger(__name__)

# Style archetype encoding
_STYLE_MAP: dict[str, int] = {
    "striker": 0,
    "grappler": 1,
    "wrestler": 2,
    "bjj": 3,
    "balanced": 4,
    "kickboxer": 5,
}


class CombatExtractor(BaseFeatureExtractor):
    """Feature extractor for MMA / UFC fights."""

    def __init__(self, sport: str, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.sport = sport

    # ── Fighter-level helpers ─────────────────────────────

    def _fighter_record(
        self,
        fighter_id: str,
        date: str,
        games: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Recent fight record, win streak, and finishing rate (vectorized)."""
        recent = self._team_games_before(games, fighter_id, date, limit=window)
        if recent.empty:
            return {
                "win_pct": 0.0, "win_streak": 0, "loss_streak": 0,
                "finish_rate": 0.0, "decision_rate": 0.0, "fights_total": 0,
                "early_finish_rate": 0.0, "avg_finish_round": 3.0,
            }

        wins = self._vec_win_flags(recent, fighter_id)

        # Win/loss streak (must remain sequential)
        win_streak, loss_streak = 0, 0
        for w in wins:
            if w:
                win_streak += 1
            else:
                break
        for w in wins:
            if not w:
                loss_streak += 1
            else:
                break

        # Finishing rate — vectorized using finish_round column
        is_home = recent["home_team_id"] == fighter_id

        # method column (rarely populated but check it)
        method_raw = recent.get("method", recent.get("result_method", pd.Series("", index=recent.index)))
        method_ser = method_raw.fillna("").str.lower()

        finish_round = pd.to_numeric(recent.get("home_finish_round", np.nan), errors="coerce")

        finish_mask = (
            method_ser.str.contains("ko|tko|sub|submission", regex=True) |
            ((method_ser == "") & (finish_round <= 2))
        ) & wins.values
        decision_mask = (
            method_ser.str.contains("dec") |
            ((method_ser == "") & (finish_round > 2) & finish_round.notna())
        ) & wins.values

        finishes = int(finish_mask.sum())
        decisions = int(decision_mask.sum())
        total_wins = int(wins.sum())

        won_rounds = finish_round[wins.values].dropna()
        avg_finish_round = float(won_rounds.mean()) if len(won_rounds) > 0 else 3.0
        early_finish_rate = float((won_rounds <= 2).sum() / total_wins) if total_wins > 0 else 0.0

        return {
            "win_pct": float(wins.mean()),
            "win_streak": float(win_streak),
            "loss_streak": float(loss_streak),
            "finish_rate": float(finishes / total_wins) if total_wins > 0 else 0.0,
            "decision_rate": float(decisions / total_wins) if total_wins > 0 else 0.0,
            "early_finish_rate": early_finish_rate,
            "avg_finish_round": avg_finish_round,
            "fights_total": float(len(recent)),
        }

    def _fighter_rolling_stats(
        self,
        fighter_id: str,
        date: str,
        games_df: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Rolling averages of striking/grappling stats (vectorized, no data leakage)."""
        recent = self._team_games_before(games_df, fighter_id, date, limit=window)
        _empty = {
            "sig_strike_pct": 0.0, "sig_strikes_per_fight": 0.0,
            "total_strikes_per_fight": 0.0, "strike_differential": 0.0,
            "takedown_pct": 0.0, "submission_attempts": 0.0,
            "knockdowns_per_fight": 0.0, "control_time_avg": 0.0,
        }
        if recent.empty:
            return _empty

        is_home = recent["home_team_id"] == fighter_id

        def _col(home_col: str, away_col: str) -> pd.Series:
            h = pd.to_numeric(recent.get(home_col, 0), errors="coerce").fillna(0)
            a = pd.to_numeric(recent.get(away_col, 0), errors="coerce").fillna(0)
            return h.where(is_home, a)

        sig_landed = _col("home_sig_strikes_landed", "away_sig_strikes_landed")
        sig_attempted = _col("home_sig_strikes_attempted", "away_sig_strikes_attempted")
        opp_sig_landed = _col("away_sig_strikes_landed", "home_sig_strikes_landed")
        total_landed = _col("home_total_strikes_landed", "away_total_strikes_landed")
        td_pct = _col("home_takedown_pct", "away_takedown_pct")
        sub_att = _col("home_submission_attempts", "away_submission_attempts")
        knockdowns = _col("home_knockdowns", "away_knockdowns")
        ctrl_time = _col("home_control_time_seconds", "away_control_time_seconds")

        # Sig strike pct per fight (avoid division by zero)
        sig_pct = pd.Series(
            np.where(sig_attempted > 0, sig_landed / sig_attempted * 100, 0.0)
        )

        def _mean(s: pd.Series) -> float:
            return float(s.mean()) if len(s) > 0 else 0.0

        return {
            "sig_strike_pct": _mean(sig_pct),
            "sig_strikes_per_fight": _mean(sig_landed),
            "total_strikes_per_fight": _mean(total_landed),
            "strike_differential": _mean(sig_landed) - _mean(opp_sig_landed),
            "takedown_pct": _mean(td_pct),
            "submission_attempts": _mean(sub_att),
            "knockdowns_per_fight": _mean(knockdowns),
            "control_time_avg": _mean(ctrl_time),
        }

    def _physical_features(
        self,
        game: dict[str, Any],
        prefix: str,
    ) -> dict[str, float]:
        """Reach, height, age - from game dict (sparse in current data)."""
        reach = pd.to_numeric(game.get(f"{prefix}reach", 0), errors="coerce") or 0.0
        height = pd.to_numeric(game.get(f"{prefix}height", 0), errors="coerce") or 0.0
        age = pd.to_numeric(game.get(f"{prefix}age", 0), errors="coerce") or 0.0

        return {
            "reach": reach,
            "height": height,
            "age": age,
        }

    def _style_matchup(
        self,
        game: dict[str, Any],
    ) -> dict[str, float]:
        """Encode fighter styles for matchup analysis."""
        h_style = str(game.get("home_style", game.get("red_style", "balanced"))).lower()
        a_style = str(game.get("away_style", game.get("blue_style", "balanced"))).lower()

        return {
            "home_style_code": float(_STYLE_MAP.get(h_style, 4)),
            "away_style_code": float(_STYLE_MAP.get(a_style, 4)),
            "same_style": 1.0 if h_style == a_style else 0.0,
        }

    # ── Main Extraction ───────────────────────────────────

    def _load_all_games(self) -> pd.DataFrame:
        """Load and concatenate games from all available seasons for history lookups."""
        if hasattr(self, "_all_games_cache") and self._all_games_cache is not None:
            return self._all_games_cache
        sport_dir = self.data_dir / "normalized" / self.sport
        frames = []
        for p in sorted(sport_dir.glob("games_*.parquet")):
            try:
                season = int(p.stem.split("_")[-1])
            except ValueError:
                continue
            df = self.load_games(season)
            if not df.empty:
                frames.append(df)
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not combined.empty and "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
            combined.sort_values("date", inplace=True, ignore_index=True)
        self._all_games_cache = combined
        return combined

    def extract_game_features(self, game: dict[str, Any]) -> dict[str, Any]:
        season = game.get("season", 0)
        games_df = self._load_all_games()  # cross-season fighter history
        odds_df = self.load_odds(season)

        # In UFC data, "home" = red corner, "away" = blue corner
        h_id = str(game.get("home_team_id", game.get("red_fighter_id", "")))
        a_id = str(game.get("away_team_id", game.get("blue_fighter_id", "")))
        date = str(game.get("date", ""))
        game_id = str(game.get("id", ""))

        features: dict[str, Any] = {
            "game_id": game_id,
            "date": date,
            "home_team_id": h_id,
            "away_team_id": a_id,
            "home_score": pd.to_numeric(game.get("home_score"), errors="coerce"),
            "away_score": pd.to_numeric(game.get("away_score"), errors="coerce"),
        }

        # Fight records
        h_rec = self._fighter_record(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_rec.items()})
        a_rec = self._fighter_record(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_rec.items()})

        # H2H
        h2h = self.head_to_head(h_id, a_id, games_df, date=date, n=5)
        features.update(h2h)

        # Rolling striking/grappling stats from historical games (no data leakage)
        h_stats = self._fighter_rolling_stats(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_stats.items()})
        a_stats = self._fighter_rolling_stats(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_stats.items()})

        # Physical
        h_phys = self._physical_features(game, "home_")
        features.update({f"home_{k}": v for k, v in h_phys.items()})
        a_phys = self._physical_features(game, "away_")
        features.update({f"away_{k}": v for k, v in a_phys.items()})
        features["reach_advantage"] = h_phys["reach"] - a_phys["reach"]
        features["height_advantage"] = h_phys["height"] - a_phys["height"]
        features["age_diff"] = h_phys["age"] - a_phys["age"]

        # Style
        style = self._style_matchup(game)
        features.update(style)

        # ELO ratings
        h_elo = self.elo_features(h_id, a_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_elo.items()})
        a_elo = self.elo_features(a_id, h_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_elo.items()})

        # Momentum (win streak)
        features["home_momentum"] = self.momentum(h_id, date, games_df)
        features["away_momentum"] = self.momentum(a_id, date, games_df)

        # Differential features — critical for UFC which has no home field advantage
        features["win_pct_diff"] = h_rec["win_pct"] - a_rec["win_pct"]
        features["win_streak_diff"] = h_rec["win_streak"] - a_rec["win_streak"]
        features["finish_rate_diff"] = h_rec["finish_rate"] - a_rec["finish_rate"]
        features["sig_strike_pct_diff"] = h_stats["sig_strike_pct"] - a_stats["sig_strike_pct"]
        features["sig_strikes_per_fight_diff"] = h_stats["sig_strikes_per_fight"] - a_stats["sig_strikes_per_fight"]
        features["strike_differential_diff"] = h_stats["strike_differential"] - a_stats["strike_differential"]
        features["knockdowns_diff"] = h_stats["knockdowns_per_fight"] - a_stats["knockdowns_per_fight"]
        features["control_time_diff"] = h_stats["control_time_avg"] - a_stats["control_time_avg"]
        features["takedown_pct_diff"] = h_stats["takedown_pct"] - a_stats["takedown_pct"]
        features["elo_diff"] = features.get("home_elo", 1500.0) - features.get("away_elo", 1500.0)
        features["early_finish_rate_diff"] = h_rec["early_finish_rate"] - a_rec["early_finish_rate"]

        # Odds
        odds = self._odds_features(game_id, odds_df)
        features.update(odds)

        return features

    def get_feature_names(self) -> list[str]:
        return [
            # Record
            "home_win_pct", "home_win_streak", "home_loss_streak",
            "home_finish_rate", "home_decision_rate", "home_early_finish_rate",
            "home_avg_finish_round", "home_fights_total",
            "away_win_pct", "away_win_streak", "away_loss_streak",
            "away_finish_rate", "away_decision_rate", "away_early_finish_rate",
            "away_avg_finish_round", "away_fights_total",
            # H2H
            "h2h_games", "h2h_win_pct", "h2h_avg_margin",
            # Rolling striking stats (from historical games)
            "home_sig_strike_pct", "home_sig_strikes_per_fight",
            "home_total_strikes_per_fight", "home_strike_differential",
            "away_sig_strike_pct", "away_sig_strikes_per_fight",
            "away_total_strikes_per_fight", "away_strike_differential",
            # Rolling grappling stats
            "home_takedown_pct", "home_submission_attempts",
            "home_knockdowns_per_fight", "home_control_time_avg",
            "away_takedown_pct", "away_submission_attempts",
            "away_knockdowns_per_fight", "away_control_time_avg",
            # Physical (sparse, mostly 0 in current data)
            "home_reach", "home_height", "home_age",
            "away_reach", "away_height", "away_age",
            "reach_advantage", "height_advantage", "age_diff",
            # Style
            "home_style_code", "away_style_code", "same_style",
            # Key differentials
            "win_pct_diff", "win_streak_diff", "finish_rate_diff",
            "sig_strike_pct_diff", "sig_strikes_per_fight_diff",
            "strike_differential_diff", "knockdowns_diff",
            "control_time_diff", "takedown_pct_diff", "elo_diff",
            "early_finish_rate_diff",
            # ELO & momentum
            "home_elo", "home_elo_diff", "home_elo_expected_win",
            "away_elo", "away_elo_diff", "away_elo_expected_win",
            "home_momentum", "away_momentum",
            # Odds
            "home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob",
        ]
