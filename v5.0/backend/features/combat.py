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
        """Recent fight record, win streak, finishing rate, and career-level stats."""
        recent = self._team_games_before(games, fighter_id, date, limit=window)
        if recent.empty:
            return {
                "win_pct": 0.0, "win_streak": 0, "loss_streak": 0,
                "finish_rate": 0.0, "decision_rate": 0.0, "fights_total": 0,
                "early_finish_rate": 0.0, "avg_finish_round": 3.0,
                "days_since_last_fight": 365.0,
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

        # Days since last fight (inactivity / rust factor)
        try:
            fight_dates = pd.to_datetime(recent["date"], errors="coerce").dropna().sort_values()
            if len(fight_dates) > 0:
                ref_date = pd.to_datetime(date, errors="coerce")
                days_off = float((ref_date - fight_dates.iloc[-1]).days) if not pd.isna(ref_date) else 365.0
                days_off = max(0.0, min(days_off, 730.0))  # cap at 2 years
            else:
                days_off = 365.0
        except Exception:
            days_off = 365.0

        return {
            "win_pct": float(wins.mean()),
            "win_streak": float(win_streak),
            "loss_streak": float(loss_streak),
            "finish_rate": float(finishes / total_wins) if total_wins > 0 else 0.0,
            "decision_rate": float(decisions / total_wins) if total_wins > 0 else 0.0,
            "early_finish_rate": early_finish_rate,
            "avg_finish_round": avg_finish_round,
            "fights_total": float(len(recent)),
            # KO/TKO vs submission breakdown
            "ko_tko_rate": float(sum(
                1 for m in method_ser[wins.values] if "ko" in m or "tko" in m
            ) / total_wins) if total_wins > 0 else 0.0,
            "submission_rate": float(sum(
                1 for m in method_ser[wins.values] if "sub" in m
            ) / total_wins) if total_wins > 0 else 0.0,
            # Career record via all-time window (use larger window than default 10)
            "career_win_pct": float(wins.mean()),  # same calc, differentiated at call site
            "days_since_last_fight": days_off,
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
            "sig_strikes_absorbed": 0.0,
            "takedown_pct": 0.0, "submission_attempts": 0.0,
            "knockdowns_per_fight": 0.0, "control_time_avg": 0.0,
            "total_strikes_per_min": 0.0, "sig_strike_defense": 0.5,
            "volume_vs_accuracy": 0.0,
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

        opp_sig_attempted = _col("away_sig_strikes_attempted", "home_sig_strikes_attempted")
        total_strikes_pm_series = pd.Series(
            np.where(ctrl_time > 0, total_landed / (ctrl_time / 60.0), 0.0)
        )
        opp_sig_att_mean = _mean(opp_sig_attempted)
        opp_sig_land_mean = _mean(opp_sig_landed)
        sig_def = float(1.0 - (opp_sig_land_mean / opp_sig_att_mean)) if opp_sig_att_mean > 0 else 0.5
        sig_pct_mean = _mean(sig_pct)
        total_str_mean = _mean(total_landed)

        return {
            "sig_strike_pct": sig_pct_mean,
            "sig_strikes_per_fight": _mean(sig_landed),
            "total_strikes_per_fight": total_str_mean,
            "strike_differential": _mean(sig_landed) - opp_sig_land_mean,
            "sig_strikes_absorbed": opp_sig_land_mean,
            "takedown_pct": _mean(td_pct),
            "submission_attempts": _mean(sub_att),
            "knockdowns_per_fight": _mean(knockdowns),
            "control_time_avg": _mean(ctrl_time),
            "total_strikes_per_min": _mean(total_strikes_pm_series),
            "sig_strike_defense": sig_def,
            "volume_vs_accuracy": float(total_str_mean * sig_pct_mean / 100.0),
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

    def _quality_weighted_record(
        self,
        fighter_id: str,
        date: str,
        games_df: pd.DataFrame,
        window: int = 10,
    ) -> dict[str, float]:
        """Win record weighted by opponent quality (opponent win% up to prediction date).

        High-quality wins get weighted more than wins vs poor fighters.
        """
        defaults = {"quality_win_rate": 0.5, "quality_form": 0.0, "quality_finish_rate": 0.0}
        recent = self._team_games_before(games_df, fighter_id, date, limit=window)
        if recent.empty:
            return defaults

        is_home = recent["home_team_id"].astype(str) == str(fighter_id)
        h_sc = pd.to_numeric(recent["home_score"], errors="coerce").fillna(0)
        a_sc = pd.to_numeric(recent["away_score"], errors="coerce").fillna(0)
        wins = np.where(is_home, h_sc > a_sc, a_sc > h_sc).astype(float)
        opp_ids = np.where(is_home, recent["away_team_id"].astype(str), recent["home_team_id"].astype(str))

        # Finish detected from score > 0 as proxy (knockdowns, sub, TKO = decisive score)
        tm_sc = np.where(is_home, h_sc, a_sc)
        finishes = (tm_sc > 0).astype(float)  # scores recorded = fight ending decisively

        opp_qual: dict[str, float] = {}
        for opp_id in set(opp_ids):
            opp_hist = self._team_games_before(games_df, str(opp_id), date, limit=20)
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
        quality_finish_rate = float(np.dot(wins * finishes, opp_arr) / max(wins.sum(), 1.0))
        return {
            "quality_win_rate": quality_win_rate,
            "quality_form": quality_form,
            "quality_finish_rate": quality_finish_rate,
        }

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
        home_team = str(game.get("home_team", game.get("red_fighter", "")))
        away_team = str(game.get("away_team", game.get("blue_fighter", "")))

        features: dict[str, Any] = {
            "game_id": game_id,
            "date": date,
            "home_team_id": h_id,
            "away_team_id": a_id,
            "home_score": pd.to_numeric(game.get("home_score"), errors="coerce"),
            "away_score": pd.to_numeric(game.get("away_score"), errors="coerce"),
        }

        # Fight records (recent 10 fights)
        h_rec = self._fighter_record(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_rec.items()})
        a_rec = self._fighter_record(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_rec.items()})

        # Career record (50-fight all-time window for long-term quality signal)
        h_career = self._fighter_record(h_id, date, games_df, window=50)
        features["home_career_win_pct"] = h_career["win_pct"]
        features["home_career_finish_rate"] = h_career["finish_rate"]
        features["home_career_fights"] = h_career["fights_total"]
        a_career = self._fighter_record(a_id, date, games_df, window=50)
        features["away_career_win_pct"] = a_career["win_pct"]
        features["away_career_finish_rate"] = a_career["finish_rate"]
        features["away_career_fights"] = a_career["fights_total"]

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
        features["submission_attempts_diff"] = h_stats["submission_attempts"] - a_stats["submission_attempts"]
        # Absorption rate differential: lower absorption is better → invert
        features["sig_absorbed_diff"] = a_stats["sig_strikes_absorbed"] - h_stats["sig_strikes_absorbed"]
        features["elo_diff"] = features.get("home_elo", 1500.0) - features.get("away_elo", 1500.0)
        features["early_finish_rate_diff"] = h_rec["early_finish_rate"] - a_rec["early_finish_rate"]
        features["momentum_diff"] = features["home_momentum"] - features["away_momentum"]
        # New differentials for new stats
        features["total_strikes_per_min_diff"] = h_stats["total_strikes_per_min"] - a_stats["total_strikes_per_min"]
        features["sig_strike_defense_diff"] = h_stats["sig_strike_defense"] - a_stats["sig_strike_defense"]
        features["volume_vs_accuracy_diff"] = h_stats["volume_vs_accuracy"] - a_stats["volume_vs_accuracy"]
        # KO/Sub differentials
        features["ko_tko_rate_diff"] = h_rec.get("ko_tko_rate", 0.0) - a_rec.get("ko_tko_rate", 0.0)
        features["submission_rate_diff"] = h_rec.get("submission_rate", 0.0) - a_rec.get("submission_rate", 0.0)
        features["career_win_pct_diff"] = features["home_career_win_pct"] - features["away_career_win_pct"]
        # Inactivity: negative = home fighter was more inactive (more rusty)
        features["inactivity_diff"] = h_rec.get("days_since_last_fight", 365.0) - a_rec.get("days_since_last_fight", 365.0)

        # Quality-weighted record (form vs elite opponents)
        h_qr = self._quality_weighted_record(h_id, date, games_df)
        features.update({f"home_{k}": v for k, v in h_qr.items()})
        a_qr = self._quality_weighted_record(a_id, date, games_df)
        features.update({f"away_{k}": v for k, v in a_qr.items()})
        features["quality_form_diff"] = h_qr["quality_form"] - a_qr["quality_form"]
        features["quality_win_rate_diff"] = h_qr["quality_win_rate"] - a_qr["quality_win_rate"]
        features["quality_finish_rate_diff"] = h_qr["quality_finish_rate"] - a_qr["quality_finish_rate"]

        # Odds
        odds = self._odds_features(game_id, odds_df)
        features.update(odds)

        # Market signals (line movement, public betting indicators)
        market = self._market_signal_features(game_id, season, home_team=home_team, away_team=away_team, date=date)
        features.update(market)

        # Raw game outcome columns needed for UFC extra market training targets
        # These are excluded from the feature matrix by _META_COLS in train.py
        features["home_finish_round"] = pd.to_numeric(game.get("home_finish_round"), errors="coerce")
        features["away_finish_round"] = pd.to_numeric(game.get("away_finish_round"), errors="coerce")
        features["home_knockdowns"] = pd.to_numeric(game.get("home_knockdowns"), errors="coerce")
        features["away_knockdowns"] = pd.to_numeric(game.get("away_knockdowns"), errors="coerce")
        features["home_submission_attempts"] = pd.to_numeric(game.get("home_submission_attempts"), errors="coerce")
        features["away_submission_attempts"] = pd.to_numeric(game.get("away_submission_attempts"), errors="coerce")
        features["home_control_time_seconds"] = pd.to_numeric(game.get("home_control_time_seconds"), errors="coerce")
        features["away_control_time_seconds"] = pd.to_numeric(game.get("away_control_time_seconds"), errors="coerce")

        return features

    def get_feature_names(self) -> list[str]:
        return [
            # Record
            "home_win_pct", "home_win_streak", "home_loss_streak",
            "home_finish_rate", "home_decision_rate", "home_early_finish_rate",
            "home_avg_finish_round", "home_fights_total",
            "home_ko_tko_rate", "home_submission_rate", "home_days_since_last_fight",
            "away_win_pct", "away_win_streak", "away_loss_streak",
            "away_finish_rate", "away_decision_rate", "away_early_finish_rate",
            "away_avg_finish_round", "away_fights_total",
            "away_ko_tko_rate", "away_submission_rate", "away_days_since_last_fight",
            # Career record (all-time signal)
            "home_career_win_pct", "home_career_finish_rate", "home_career_fights",
            "away_career_win_pct", "away_career_finish_rate", "away_career_fights",
            # H2H
            "h2h_games", "h2h_win_pct", "h2h_avg_margin",
            # Rolling striking stats (from historical games)
            "home_sig_strike_pct", "home_sig_strikes_per_fight",
            "home_total_strikes_per_fight", "home_strike_differential", "home_sig_strikes_absorbed",
            "away_sig_strike_pct", "away_sig_strikes_per_fight",
            "away_total_strikes_per_fight", "away_strike_differential", "away_sig_strikes_absorbed",
            "home_total_strikes_per_min", "home_sig_strike_defense", "home_volume_vs_accuracy",
            "away_total_strikes_per_min", "away_sig_strike_defense", "away_volume_vs_accuracy",
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
            "early_finish_rate_diff", "submission_attempts_diff", "sig_absorbed_diff", "momentum_diff",
            "total_strikes_per_min_diff", "sig_strike_defense_diff", "volume_vs_accuracy_diff",
            "ko_tko_rate_diff", "submission_rate_diff", "career_win_pct_diff",
            "inactivity_diff",
            # ELO & momentum
            "home_elo", "home_elo_diff", "home_elo_expected_win",
            "away_elo", "away_elo_diff", "away_elo_expected_win",
            "home_momentum", "away_momentum",
            # Odds
            "home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob",
            # Market signals
            "market_aggregate_abs_move", "market_h2h_home_move", "market_h2h_away_move",
            "market_spread_home_move", "market_total_line_move",
            "market_observation_count", "market_source_count",
            "market_regime_stable", "market_regime_moving", "market_regime_volatile",
        ]
