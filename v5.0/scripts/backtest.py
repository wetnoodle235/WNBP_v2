#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────
# V5.0 — 30-Day Backtest & Analysis
# ──────────────────────────────────────────────────────────
#
# For each sport with a trained model, load recent games from
# normalized parquet data, run predictions via the same pipeline
# used in production, and compare against actual results.
#
# Outputs: accuracy by sport, confidence tier, ROI simulation,
# calibration, and best/worst sport rankings.
#
# Usage:
#   python3 scripts/backtest.py
#   python3 scripts/backtest.py --sport nba --days 30
#   python3 scripts/backtest.py --sport nba --days 14 --verbose
# ──────────────────────────────────────────────────────────
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")

# ── Path setup ───────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
DATA_DIR = PROJECT_ROOT / "data"
MODELS_ROOT = PROJECT_ROOT / "ml" / "models"

sys.path.insert(0, str(BACKEND_DIR))

logger = logging.getLogger("backtest")

ALL_SPORTS = [
    # Core team sports — models exist
    "nba", "nhl", "mlb", "nfl",
    # Soccer leagues — models exist or trainable
    "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl",
    "europa", "ligamx",
    # Combat / individual
    "ufc",
    # Tennis
    "atp", "wta",
    # College / minor
    "ncaab", "ncaaw", "ncaaf", "wnba",
    # Esports
    "csgo", "dota2", "lol", "valorant",
    # Motorsport
    "f1", "indycar",
    # Golf — player-centric, uses GolfPredictor (not GamePredictor)
    "golf", "lpga",
]

# Sports that use a player-centric model (not home vs away)
_PLAYER_CENTRIC_SPORTS: frozenset[str] = frozenset(["golf", "lpga"])
BET_AMOUNT = 100.0  # flat bet size for ROI simulation

# Status values that indicate a completed game across all sports.
# Soccer uses "full_time", "final_aet", "final_pen"; others use "final".
_COMPLETED_STATUSES: frozenset[str] = frozenset(
    [
        "final",
        "closed",
        "complete",
        "finished",
        "full_time",
        "final_aet",
        "final_pen",
        "ft",
    ]
)

# Sports that can legitimately finish 0-0
_SOCCER_SPORTS: frozenset[str] = frozenset(
    ["epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl"]
)

# Month ranges (inclusive) for each sport's regular season.
# Sports not listed here are assumed year-round.
# If today is outside the range, the backtest automatically uses the most
# recent completed regular season window instead of "last N days".
_REGULAR_SEASON_MONTHS: dict[str, tuple[int, int]] = {
    "nfl":   (9, 2),   # Sep → Feb (wraps year boundary)
    "mlb":   (3, 11),  # Mar → Nov (spring training + postseason)
    "nhl":   (10, 6),  # Oct → Jun (wraps)
    "nba":   (10, 6),  # Oct → Jun (wraps)
    "ncaab": (11, 4),  # Nov → Apr (wraps)
    "ncaaw": (11, 4),  # Nov → Apr (wraps)
    "ncaaf": (8, 1),   # Aug → Jan (wraps)
    "wnba":  (5, 10),  # May → Oct
    "nwsl":  (3, 11),  # Mar → Nov
    "mls":   (2, 11),  # Feb → Nov
    # Soccer European leagues: Aug → May (wraps)
    "epl":       (8, 5),
    "laliga":    (8, 5),
    "bundesliga":(8, 5),
    "ligue1":    (8, 5),
    "seriea":    (8, 5),
    "ucl":       (9, 6),
    "europa":    (9, 5),
    "ligamx":    (1, 12),  # year-round (Apertura + Clausura)
    # Esports — year-round (no season restriction)
    # F1 — year-round
}


def _sport_in_season(sport: str, check_date: date) -> bool:
    """Return True if *check_date* falls within the sport's regular season."""
    if sport not in _REGULAR_SEASON_MONTHS:
        return True  # assume in-season (tennis, UFC, etc.)
    start_m, end_m = _REGULAR_SEASON_MONTHS[sport]
    m = check_date.month
    if start_m <= end_m:
        return start_m <= m <= end_m
    # Wraps year boundary (e.g., Oct → Jun)
    return m >= start_m or m <= end_m


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, stream=sys.stderr)
    logging.getLogger("features").setLevel(logging.WARNING)
    logging.getLogger("ml").setLevel(logging.WARNING)
    logger.setLevel(level)


# ── Helpers ──────────────────────────────────────────────


def _brier_score(prob: float, actual: bool) -> float:
    """Single-observation Brier score."""
    return (prob - (1.0 if actual else 0.0)) ** 2


def _confidence_tier(confidence: float) -> str:
    """Map confidence to 5 tiers for finer-grained filtering."""
    if confidence > 0.80:
        return "elite_above_80"
    if confidence > 0.70:
        return "high_70_80"
    if confidence > 0.60:
        return "medium_high_60_70"
    if confidence >= 0.55:
        return "medium_55_60"
    return "low_below_55"


def _moneyline_payout(prob: float) -> float:
    """Estimate fair-odds payout multiplier from win probability.

    Uses the predicted probability as a proxy for implied odds.
    A correct $100 bet at fair odds returns $100 / prob.
    """
    prob = max(min(prob, 0.99), 0.01)
    return BET_AMOUNT / prob


# ── Data loading ─────────────────────────────────────────


def _load_recent_games(
    sport: str,
    days: int,
    end_date: date | None = None,
) -> pd.DataFrame:
    """Load completed games from the last *days* days (ending at *end_date*)
    across all season parquet files for *sport*.

    Returns a DataFrame with at least: id, date, home_team,
    away_team, home_score, away_score, home_team_id, away_team_id.
    """
    sport_dir = DATA_DIR / "normalized" / sport
    parquet_files = sorted(sport_dir.glob("games_*.parquet"))
    if not parquet_files:
        logger.warning("No game parquets found for %s in %s", sport, sport_dir)
        return pd.DataFrame()

    # Column-pruning: only read the ~17 columns the backtest actually uses
    # instead of all 100+ stats columns — big I/O savings.
    _BACKTEST_COLS = [
        "id", "game_id", "date", "status", "season",
        "home_team", "away_team", "home_team_id", "away_team_id",
        "home_score", "away_score",
        "home_q1", "home_q2", "home_q3", "home_q4", "home_ot",
        "away_q1", "away_q2", "away_q3", "away_q4", "away_ot",
        # golf-specific
        "won", "top_10", "player_id",
    ]

    frames = []
    for p in parquet_files:
        try:
            # Only read columns that exist in this file
            import pyarrow.parquet as pq
            schema = pq.read_schema(p)
            available = [c for c in _BACKTEST_COLS if c in schema.names]
            frames.append(pd.read_parquet(p, columns=available))
        except Exception:
            frames.append(pd.read_parquet(p))
    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Guard: sports without score columns (motorsport, golf) can't be backtested
    if "home_score" not in df.columns or "away_score" not in df.columns:
        logger.info("%s: no home_score/away_score columns — skipping backtest", sport)
        return pd.DataFrame()

    if end_date is None:
        end_date = date.today()
    end_ts = pd.Timestamp(end_date)
    cutoff  = end_ts - pd.Timedelta(days=days)

    # Keep only completed games with real scores in the window.
    # Also accept games where status='scheduled' but scores exist and date is past
    # (StatsBomb and some other sources never update status after a match finishes).
    has_scores = df["home_score"].notna() & df["away_score"].notna()
    status_complete = df["status"].str.lower().isin(_COMPLETED_STATUSES)
    mask = (
        (df["date"] >= cutoff)
        & (df["date"] <= end_ts)
        & has_scores
        & (status_complete | has_scores)
    )
    recent = df.loc[mask].copy()

    # Exclude 0-0 scheduled placeholders; 0-0 draws are valid in all soccer
    recent["home_score"] = pd.to_numeric(recent["home_score"], errors="coerce")
    recent["away_score"] = pd.to_numeric(recent["away_score"], errors="coerce")
    recent = recent[
        ~((recent["home_score"] == 0) & (recent["away_score"] == 0))
        | (sport in _SOCCER_SPORTS)
    ]

    logger.debug(
        "%s: %d completed games in last %d days (from %d total rows)",
        sport,
        len(recent),
        days,
        len(df),
    )
    # Deduplicate: the same game_id can appear in multiple season parquets.
    id_col = "id" if "id" in recent.columns else None
    if id_col and recent[id_col].duplicated().any():
        recent = recent.drop_duplicates(subset=id_col, keep="last")
    return recent.sort_values("date").reset_index(drop=True)


def _load_precomputed_features(sport: str) -> pd.DataFrame | None:
    """Load pre-computed feature parquet for *sport*, indexed by game_id."""
    feat_file = DATA_DIR / "features" / f"{sport}_all.parquet"
    if not feat_file.exists():
        return None
    try:
        df = pd.read_parquet(feat_file)
        if "game_id" not in df.columns:
            return None
        df["game_id"] = df["game_id"].astype(str)
        # Deduplicate: same game can appear in multiple season files.
        # Keep the row with the latest season (most complete data).
        if "season" in df.columns and df["game_id"].duplicated().any():
            df = df.sort_values("season", ascending=True).drop_duplicates(
                subset="game_id", keep="last"
            )
        return df.set_index("game_id")
    except Exception:
        return None


def _load_player_props_models(sport: str) -> dict[str, Any] | None:
    """Load player prop models for *sport* if they exist, otherwise None."""
    path = MODELS_ROOT / sport / "player_props.pkl"
    if not path.exists():
        return None
    try:
        import pickle
        with open(path, "rb") as fh:
            bundle = pickle.load(fh)  # noqa: S301
        logger.info("Loaded player prop models for %s (%d props)", sport, len(bundle.get("models", {})))
        return bundle
    except Exception as exc:
        logger.debug("Could not load player props for %s: %s", sport, exc)
        return None


# ── Backtest engine ──────────────────────────────────────


class Backtester:
    """N-day backtest using the live prediction pipeline."""

    def __init__(
        self,
        sports: list[str],
        days: int = 30,
        end_date: date | None = None,
        verbose: bool = False,
    ) -> None:
        self.sports = sports
        self.days = days
        self.end_date = end_date  # None = today
        self.verbose = verbose
        self.records: list[dict[str, Any]] = []

    # ── Main entry point ─────────────────────────────────

    def run(self) -> dict[str, Any]:
        from ml.predictors.game_predictor import GamePredictor

        # Resolve effective end date and auto-adjust for out-of-season sports.
        base_end = self.end_date or date.today()
        end_date  = base_end
        start_date = end_date - timedelta(days=self.days)

        # ── Golf: player-centric — handled separately ─────
        team_sports = [s for s in self.sports if s not in _PLAYER_CENTRIC_SPORTS]
        golf_sports = [s for s in self.sports if s in _PLAYER_CENTRIC_SPORTS]

        for sport in golf_sports:
            models_dir = MODELS_ROOT / sport
            if not (models_dir / "joint_models.pkl").exists():
                logger.info("No trained models for %s — skipping", sport)
                continue
            sport_end = base_end
            if self.end_date is None and not _sport_in_season(sport, base_end):
                probe = base_end
                for _ in range(540):
                    probe -= timedelta(days=1)
                    if _sport_in_season(sport, probe):
                        sport_end = probe
                        break
            self._run_golf(sport, models_dir, start_date, sport_end)

        # Run predictions per sport — parallelized across sports
        # Each sport gets its own predictor (no shared state → thread-safe).
        def _backtest_one_sport(sport: str) -> list[dict[str, Any]]:
            """Backtest a single sport; returns list of records."""
            models_dir = MODELS_ROOT / sport
            if not (models_dir / "joint_models.pkl").exists() and not (
                models_dir / "separate_models.pkl"
            ).exists():
                return []

            sport_end = base_end
            if self.end_date is None and not _sport_in_season(sport, base_end):
                probe = base_end
                for _ in range(540):
                    probe -= timedelta(days=1)
                    if _sport_in_season(sport, probe):
                        sport_end = probe
                        break
                logger.info(
                    "%s is out of season today — using %s as window end",
                    sport, sport_end,
                )

            games_df = _load_recent_games(sport, self.days, end_date=sport_end)
            if games_df.empty:
                logger.debug("No recent completed games for %s — skipping", sport)
                return []

            try:
                predictor = GamePredictor(sport, models_dir, DATA_DIR)
            except Exception as exc:
                logger.warning("Could not load predictor for %s: %s", sport, exc)
                return []

            logger.info(
                "Backtesting %s: %d games (%s to %s)",
                sport,
                len(games_df),
                games_df["date"].min().date(),
                games_df["date"].max().date(),
            )

            records: list[dict[str, Any]] = []

            # ── Fast path: use pre-computed features (batch) ──
            precomp = _load_precomputed_features(sport)
            if precomp is not None:
                try:
                    preds = predictor.predict_batch_precomputed(precomp, games_df)
                    # Build game dicts for evaluation
                    id_col = "game_id" if "game_id" in games_df.columns else "id"
                    game_lookup = {}
                    for _, row in games_df.iterrows():
                        g = row.to_dict()
                        gid = str(g.get("game_id") or g.get("id", ""))
                        game_lookup[gid] = g
                    for pred in preds:
                        gid = str(pred.game_id)
                        game = game_lookup.get(gid, {})
                        if not game:
                            continue
                        record = self._evaluate(pred, game)
                        if record:
                            records.append(record)
                    logger.info("  %s: %d predictions (pre-computed)", sport, len(records))
                    return records
                except Exception:
                    logger.debug(
                        "Pre-computed batch failed for %s — falling back to per-game",
                        sport, exc_info=True,
                    )

            # ── Slow fallback: per-game feature extraction ──
            for _, row in games_df.iterrows():
                game = row.to_dict()
                if "game_id" not in game and "id" in game:
                    game["game_id"] = game["id"]
                try:
                    pred = predictor.predict_game(game)
                except Exception:
                    logger.debug(
                        "Prediction failed for %s game %s",
                        sport, game.get("game_id", "?"), exc_info=True,
                    )
                    continue
                record = self._evaluate(pred, game)
                if record:
                    records.append(record)

            logger.info("  %s: %d predictions evaluated", sport, len(records))
            return records

        any_loaded = False
        # Use up to 4 parallel threads — each sport loads its own data/model
        with ThreadPoolExecutor(max_workers=min(4, len(team_sports))) as pool:
            futures = {
                pool.submit(_backtest_one_sport, sport): sport
                for sport in team_sports
            }
            for fut in as_completed(futures):
                sport = futures[fut]
                try:
                    records = fut.result()
                    if records:
                        any_loaded = True
                        self.records.extend(records)
                except Exception:
                    logger.warning(
                        "Backtest failed for %s", sport, exc_info=True,
                    )

        if not any_loaded and not golf_sports:
            logger.warning("No trained models found — nothing to backtest")
            return self._empty_report(start_date, end_date)

        return self._build_report(start_date, end_date)

        return self._build_report(start_date, end_date)

    # ── Golf backtest ────────────────────────────────────

    def _run_golf(
        self,
        sport: str,
        models_dir: Path,
        start_date: date,
        end_date: date,
    ) -> None:
        """Evaluate golf predictions from pre-computed features.

        Golf is player-centric: each tournament produces one record per
        player (top-10 accuracy) plus one record per tournament (winner
        accuracy).  Both contribute to `self.records` using the same
        schema as team-sport records so the existing report machinery
        handles them transparently.
        """
        from ml.predictors.golf_predictor import GolfPredictor

        try:
            predictor = GolfPredictor(models_dir)
        except Exception as exc:
            logger.warning("Could not load golf predictor: %s", exc)
            return

        feat_file = DATA_DIR / "features" / f"{sport}_all.parquet"
        if not feat_file.exists():
            logger.info("No precomputed features for %s — skipping", sport)
            return

        df = pd.read_parquet(feat_file)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        mask = (df["date"] >= start_ts) & (df["date"] <= end_ts)
        window_df = df.loc[mask].copy()

        if window_df.empty:
            logger.info("No golf data in backtest window (%s – %s)", start_date, end_date)
            return

        unique_tournaments = window_df["game_id"].unique()
        logger.info(
            "Backtesting %s: %d tournaments (%s to %s)",
            sport,
            len(unique_tournaments),
            str(window_df["date"].min().date()),
            str(window_df["date"].max().date()),
        )

        n_tournament_records = 0
        n_player_records = 0

        for gid in unique_tournaments:
            tournament_df = window_df[window_df["game_id"] == gid]
            if tournament_df.empty:
                continue
            # Only evaluate tournaments with a known winner
            if tournament_df["won"].sum() == 0:
                continue

            try:
                preds = predictor.predict_tournament(tournament_df)
            except Exception:
                logger.debug("Golf prediction failed for tournament %s", gid, exc_info=True)
                continue

            if not preds:
                continue

            tour_date = str(tournament_df["date"].iloc[0])[:10]

            # Build lookup: player_id → actual row
            actual_lookup: dict[str, Any] = {
                str(row["player_id"]): row
                for _, row in tournament_df.iterrows()
            }

            # ── Winner record (one per tournament) ───────
            ranked_preds = sorted(preds, key=lambda p: p.win_prob, reverse=True)
            predicted_winner = ranked_preds[0]
            actual_winner_row = actual_lookup.get(predicted_winner.player_id)
            winner_correct = bool(
                actual_winner_row is not None
                and float(actual_winner_row.get("won", 0)) == 1.0
            )

            # Average Brier score across all players
            player_briers = []
            for p in preds:
                act = actual_lookup.get(p.player_id)
                if act is not None:
                    actual_won = float(act.get("won", 0))
                    player_briers.append((p.win_prob - actual_won) ** 2)
            brier = float(np.mean(player_briers)) if player_briers else 0.25

            # ROI: flat bet on predicted winner at long-shot odds
            # Approximate fair odds from win_prob (floor to avoid infinity)
            wp = max(predicted_winner.win_prob, 0.02)
            roi_profit = (BET_AMOUNT / wp - BET_AMOUNT) if winner_correct else -BET_AMOUNT

            self.records.append({
                "date": tour_date,
                "game_id": str(gid),
                "sport": sport,
                "home_team": predicted_winner.player_name,
                "away_team": "",
                "home_win_prob": round(predicted_winner.win_prob, 4),
                "confidence": round(predicted_winner.confidence, 4),
                "confidence_tier": _confidence_tier(predicted_winner.confidence),
                "predicted_home": True,
                "actual_home_win": winner_correct,
                "is_draw": False,
                "winner_correct": winner_correct,
                "brier_score": round(brier, 4),
                "home_score": 1.0 if winner_correct else 0.0,
                "away_score": 0.0,
                "roi_profit": round(roi_profit, 2),
                "golf_winner_predicted": predicted_winner.player_name,
                "golf_n_players": len(preds),
            })
            n_tournament_records += 1

            # ── Top-10 records (one per player) ──────────
            for p in preds:
                act = actual_lookup.get(p.player_id)
                if act is None:
                    continue
                actual_top10 = float(act.get("top_10", 0)) == 1.0
                pred_top10 = p.top10_prob >= 0.5
                top10_brier = (p.top10_prob - (1.0 if actual_top10 else 0.0)) ** 2

                self.records.append({
                    "date": tour_date,
                    "game_id": f"{gid}_{p.player_id}",
                    "sport": sport,
                    "home_team": p.player_name,
                    "away_team": "",
                    "home_win_prob": round(p.top10_prob, 4),
                    "confidence": round(p.top10_prob, 4),
                    "confidence_tier": _confidence_tier(p.top10_prob),
                    "predicted_home": pred_top10,
                    "actual_home_win": actual_top10,
                    "is_draw": False,
                    "winner_correct": pred_top10 == actual_top10,
                    "brier_score": round(top10_brier, 4),
                    "home_score": 1.0 if actual_top10 else 0.0,
                    "away_score": 0.0,
                    "roi_profit": 0.0,  # top-10 has no direct ROI analog here
                    "golf_top10_correct": pred_top10 == actual_top10,
                })
                n_player_records += 1

        logger.info(
            "  %s: %d tournament winner records + %d player top-10 records",
            sport, n_tournament_records, n_player_records,
        )

    def _evaluate(
        self,
        pred: Any,
        actual: dict[str, Any],
    ) -> dict[str, Any] | None:
        pred_dict = asdict(pred) if hasattr(pred, "__dataclass_fields__") else pred

        home_score = actual.get("home_score")
        away_score = actual.get("away_score")
        try:
            home_score = float(home_score)
            away_score = float(away_score)
        except (ValueError, TypeError):
            return None

        actual_home_win = home_score > away_score
        is_draw = home_score == away_score
        home_prob = float(pred_dict.get("home_win_prob", 0.5))
        predicted_home = home_prob >= 0.5
        confidence = float(pred_dict.get("confidence", 0.5))
        winner_correct = predicted_home == actual_home_win

        # ROI: flat $100 bet on the predicted winner at fair odds
        if is_draw:
            roi_profit = -BET_AMOUNT  # draws lose the bet
        elif winner_correct:
            pick_prob = home_prob if predicted_home else (1.0 - home_prob)
            roi_profit = _moneyline_payout(pick_prob) - BET_AMOUNT
        else:
            roi_profit = -BET_AMOUNT

        record: dict[str, Any] = {
            "date": str(actual.get("date", ""))[:10],
            "game_id": str(pred_dict.get("game_id", actual.get("id", ""))),
            "sport": pred_dict.get("sport", "unknown"),
            "home_team": pred_dict.get("home_team", ""),
            "away_team": pred_dict.get("away_team", ""),
            "home_win_prob": round(home_prob, 4),
            "confidence": round(confidence, 4),
            "confidence_tier": _confidence_tier(confidence),
            "predicted_home": predicted_home,
            "actual_home_win": actual_home_win,
            "is_draw": is_draw,
            "winner_correct": winner_correct,
            "brier_score": _brier_score(home_prob, actual_home_win),
            "home_score": home_score,
            "away_score": away_score,
            "roi_profit": round(roi_profit, 2),
        }

        # ── Full-game spread ─────────────────────────────
        pred_spread = pred_dict.get("predicted_spread")
        if pred_spread is not None:
            actual_spread = home_score - away_score
            record["predicted_spread"] = float(pred_spread)
            record["actual_spread"] = actual_spread
            record["spread_error"] = abs(float(pred_spread) - actual_spread)

        # ── Full-game total ──────────────────────────────
        pred_total = pred_dict.get("predicted_total")
        if pred_total is not None:
            actual_total = home_score + away_score
            record["predicted_total"] = float(pred_total)
            record["actual_total"] = actual_total
            record["total_error"] = abs(float(pred_total) - actual_total)

        # ── Draw prediction ──────────────────────────────
        draw_prob = pred_dict.get("draw_prob")
        if draw_prob is not None:
            pred_draw = draw_prob >= 0.5
            record["draw_prob"] = round(float(draw_prob), 4)
            record["pred_draw"] = pred_draw
            record["draw_correct"] = pred_draw == is_draw

        # ── OT probability ───────────────────────────────
        ot_prob = pred_dict.get("ot_prob")
        if ot_prob is not None:
            # Actual OT: check home_ot or away_ot columns in game row
            home_ot = actual.get("home_ot")
            away_ot = actual.get("away_ot")
            actual_ot = False
            try:
                actual_ot = (float(home_ot or 0) + float(away_ot or 0)) > 0
            except (ValueError, TypeError):
                # For sports without OT columns, use draw as proxy
                actual_ot = is_draw
            pred_ot = float(ot_prob) >= 0.5
            record["ot_prob"] = round(float(ot_prob), 4)
            record["pred_ot"] = pred_ot
            record["actual_ot"] = actual_ot
            record["ot_correct"] = pred_ot == actual_ot

        # ── Halftime winner ──────────────────────────────
        ht_home_win_prob = pred_dict.get("halftime_home_win_prob")
        ht_home_score = actual.get("home_q1")
        ht_away_score = actual.get("home_q2")  # will fix below
        # Derive actual halftime from q1+q2 if available
        def _safe_float(v):
            """Return float or None if v is None/NaN."""
            if v is None:
                return None
            try:
                f = float(v)
                return None if pd.isna(f) else f
            except (ValueError, TypeError):
                return None

        try:
            _q1h = _safe_float(actual.get("home_q1"))
            _q2h = _safe_float(actual.get("home_q2"))
            _q1a = _safe_float(actual.get("away_q1"))
            _q2a = _safe_float(actual.get("away_q2"))
            if _q1h is not None and _q2h is not None and _q1a is not None and _q2a is not None:
                ah_home_q1 = _q1h
                ah_home_q2 = _q2h
                ah_away_q1 = _q1a
                ah_away_q2 = _q2a
                has_ht_data = True
            else:
                ah_home_q1 = ah_home_q2 = ah_away_q1 = ah_away_q2 = 0.0
                has_ht_data = False
        except (ValueError, TypeError):
            ah_home_q1 = ah_home_q2 = ah_away_q1 = ah_away_q2 = 0.0
            has_ht_data = False

        if ht_home_win_prob is not None and has_ht_data:
            actual_ht_home = ah_home_q1 + ah_home_q2
            actual_ht_away = ah_away_q1 + ah_away_q2
            actual_ht_home_win = actual_ht_home > actual_ht_away
            pred_ht_home = float(ht_home_win_prob) >= 0.5
            record["halftime_home_win_prob"] = round(float(ht_home_win_prob), 4)
            record["ht_winner_correct"] = pred_ht_home == actual_ht_home_win
            record["actual_ht_home_score"] = actual_ht_home
            record["actual_ht_away_score"] = actual_ht_away

        # ── Halftime score predictions ───────────────────
        pred_ht_home_sc = pred_dict.get("halftime_home_score")
        pred_ht_away_sc = pred_dict.get("halftime_away_score")
        if pred_ht_home_sc is not None and has_ht_data:
            actual_ht_home = ah_home_q1 + ah_home_q2
            actual_ht_away = ah_away_q1 + ah_away_q2
            record["ht_home_score_error"] = abs(float(pred_ht_home_sc) - actual_ht_home)
            record["ht_away_score_error"] = abs(float(pred_ht_away_sc) - actual_ht_away)
            pred_ht_spread = pred_dict.get("halftime_spread")
            if pred_ht_spread is not None:
                record["ht_spread_error"] = abs(float(pred_ht_spread) - (actual_ht_home - actual_ht_away))
            pred_ht_total = pred_dict.get("halftime_total")
            if pred_ht_total is not None:
                record["ht_total_error"] = abs(float(pred_ht_total) - (actual_ht_home + actual_ht_away))

        # ── Per-period predictions ───────────────────────
        period_preds = pred_dict.get("period_predictions")
        if period_preds:
            period_winner_correct = []
            period_total_errors = []
            for pp in period_preds:
                i = pp.get("period")
                qname = f"q{i}"
                act_h = actual.get(f"home_{qname}")
                act_a = actual.get(f"away_{qname}")
                if act_h is None or act_a is None or pd.isna(act_h) or pd.isna(act_a):
                    continue
                try:
                    act_h = float(act_h)
                    act_a = float(act_a)
                except (ValueError, TypeError):
                    continue
                pw_p = pp.get("home_win_prob")
                if pw_p is not None and act_h != act_a:
                    pred_period_home = float(pw_p) >= 0.5
                    actual_period_home = act_h > act_a
                    period_winner_correct.append(pred_period_home == actual_period_home)
                pt = pp.get("total")
                if pt is not None:
                    period_total_errors.append(abs(float(pt) - (act_h + act_a)))
            if period_winner_correct:
                record["period_winner_correct_count"] = sum(period_winner_correct)
                record["period_winner_total_count"] = len(period_winner_correct)
                record["period_winner_accuracy"] = sum(period_winner_correct) / len(period_winner_correct)
            if period_total_errors:
                record["period_total_mae"] = float(np.mean(period_total_errors))

        # ── First / last score ───────────────────────────
        fs_home_prob = pred_dict.get("first_score_home_prob")
        if fs_home_prob is not None and has_ht_data:
            # Proxy: Q1 score comparison as first-score indicator
            pred_fs_home = float(fs_home_prob) >= 0.5
            actual_fs_home = ah_home_q1 > ah_away_q1
            record["first_score_correct"] = pred_fs_home == actual_fs_home

        ls_home_prob = pred_dict.get("last_score_home_prob")
        if ls_home_prob is not None:
            # Last score proxy: q4 (or q3 if q4 null)
            act_lq_h = act_lq_a = None
            for qn in ["q4", "q3", "q2"]:
                h = actual.get(f"home_{qn}")
                a = actual.get(f"away_{qn}")
                if h is not None and a is not None and not pd.isna(h) and not pd.isna(a):
                    try:
                        act_lq_h = float(h)
                        act_lq_a = float(a)
                        break
                    except (ValueError, TypeError):
                        pass
            if act_lq_h is not None:
                pred_ls_home = float(ls_home_prob) >= 0.5
                actual_ls_home = act_lq_h > act_lq_a
                record["last_score_correct"] = pred_ls_home == actual_ls_home

        # ── BTTS / Clean Sheet ────────────────────────────
        ah = actual.get("home_score")
        aa = actual.get("away_score")
        try:
            ah_f = float(ah) if ah is not None and not pd.isna(ah) else None
            aa_f = float(aa) if aa is not None and not pd.isna(aa) else None
        except (ValueError, TypeError):
            ah_f = aa_f = None

        btts_prob = pred_dict.get("btts_prob")
        if btts_prob is not None and ah_f is not None and aa_f is not None:
            pred_btts = float(btts_prob) >= 0.5
            actual_btts = (ah_f > 0) and (aa_f > 0)
            record["btts_correct"] = pred_btts == actual_btts

        cs_h_prob = pred_dict.get("home_clean_sheet_prob")
        if cs_h_prob is not None and aa_f is not None:
            pred_cs_h = float(cs_h_prob) >= 0.5
            actual_cs_h = aa_f == 0
            record["clean_sheet_home_correct"] = pred_cs_h == actual_cs_h

        cs_a_prob = pred_dict.get("away_clean_sheet_prob")
        if cs_a_prob is not None and ah_f is not None:
            pred_cs_a = float(cs_a_prob) >= 0.5
            actual_cs_a = ah_f == 0
            record["clean_sheet_away_correct"] = pred_cs_a == actual_cs_a

        # ── UFC Method of Victory ─────────────────────────
        dec_prob = pred_dict.get("decision_prob")
        ko_prob  = pred_dict.get("ko_tko_prob")
        sub_prob = pred_dict.get("submission_prob")
        if dec_prob is not None and ah_f is not None and aa_f is not None:
            # Method proxy: see train.py _train_ufc_method for encoding
            raw_ot = actual.get("home_ot")
            try:
                method_code = int(float(raw_ot)) if raw_ot is not None and not pd.isna(raw_ot) else None
            except (ValueError, TypeError):
                method_code = None
            if method_code is not None:
                # Predict decision if dec_prob is highest
                probs = {"decision": float(dec_prob)}
                if ko_prob is not None:  probs["ko_tko"] = float(ko_prob)
                if sub_prob is not None: probs["submission"] = float(sub_prob)
                pred_method_key = max(probs, key=probs.get)
                actual_method_map = {0: "decision", 1: "ko_tko", 2: "submission"}
                actual_method_key = actual_method_map.get(method_code)
                if actual_method_key is not None:
                    record["ufc_method_correct"] = pred_method_key == actual_method_key

        # ── Tennis Straight Sets ──────────────────────────
        ss_prob = pred_dict.get("straight_sets_prob")
        if ss_prob is not None and has_ht_data:
            # Proxy: both q1 and q2 won by same side
            hq1 = ah_home_q1
            aq1 = ah_away_q1
            hq2_raw = actual.get("home_q2")
            aq2_raw = actual.get("away_q2")
            try:
                hq2 = float(hq2_raw) if hq2_raw is not None and not pd.isna(hq2_raw) else None
                aq2 = float(aq2_raw) if aq2_raw is not None and not pd.isna(aq2_raw) else None
            except (ValueError, TypeError):
                hq2 = aq2 = None
            if hq1 is not None and hq2 is not None and aq1 is not None and aq2 is not None:
                home_won = ah_f > aa_f if (ah_f is not None and aa_f is not None) else None
                if home_won is not None:
                    actual_ss = (
                        (home_won and hq1 > aq1 and hq2 > aq2) or
                        (not home_won and aq1 > hq1 and aq2 > hq2)
                    )
                    record["straight_sets_correct"] = (float(ss_prob) >= 0.5) == actual_ss

        # ── Dominant win / large margin ────────────────────
        sport_key = record.get("sport", "").lower()
        _DOM_THRESHOLDS = {
            "nba": 10, "wnba": 10, "ncaab": 10, "ncaaw": 10,
            "nfl": 14, "ncaaf": 14,
            "nhl": 3,  "mlb": 4,
            "epl": 3, "laliga": 3, "bundesliga": 3, "ligue1": 3,
            "seriea": 3, "ucl": 3, "mls": 3, "nwsl": 3,
        }
        dom_thresh = _DOM_THRESHOLDS.get(sport_key, 7)
        actual_margin = abs(home_score - away_score) if (ah_f is not None and aa_f is not None) else None

        dom_prob = pred_dict.get("dominant_win_prob")
        if dom_prob is not None and actual_margin is not None:
            actual_dominant = actual_margin >= dom_thresh
            record["dominant_win_correct"] = (float(dom_prob) >= 0.5) == actual_dominant

        large_prob = pred_dict.get("large_margin_prob")
        if large_prob is not None and actual_margin is not None:
            actual_large = actual_margin >= dom_thresh
            record["large_margin_correct"] = (float(large_prob) >= 0.5) == actual_large

        # ── Margin band ────────────────────────────────────
        mb_probs = pred_dict.get("margin_band_probs")
        if mb_probs and isinstance(mb_probs, dict) and actual_margin is not None:
            band_keys = sorted(mb_probs.keys())
            # Determine actual band from margin value
            def _pick_band(margin, keys):
                for k in keys:
                    try:
                        up = float(k.split("_")[-1]) if "_" in k else float(k)
                        if margin <= up:
                            return k
                    except (ValueError, TypeError):
                        pass
                return keys[-1] if keys else None
            pred_band = max(mb_probs, key=lambda k: mb_probs[k])
            actual_band = _pick_band(actual_margin, band_keys)
            if actual_band is not None:
                record["margin_band_correct"] = pred_band == actual_band

        # ── Total score band ────────────────────────────────
        tb_probs = pred_dict.get("total_band_probs")
        actual_total = (ah_f + aa_f) if (ah_f is not None and aa_f is not None) else None
        if tb_probs and isinstance(tb_probs, dict) and actual_total is not None:
            pred_tb = max(tb_probs, key=lambda k: tb_probs[k])
            def _pick_total_band(tot, keys):
                for k in keys:
                    try:
                        up = float(k.split("_")[-1]) if "_" in k else float(k)
                        if tot <= up:
                            return k
                    except (ValueError, TypeError):
                        pass
                return keys[-1] if keys else None
            actual_tb = _pick_total_band(actual_total, sorted(tb_probs.keys()))
            if actual_tb is not None:
                record["total_band_correct"] = pred_tb == actual_tb

        # ── Total over median ──────────────────────────────
        tot_over_prob = pred_dict.get("total_over_median_prob")
        if tot_over_prob is not None and actual_total is not None:
            pred_total_num = pred_dict.get("predicted_total")
            if pred_total_num is not None:
                median_proxy = float(pred_total_num)
                record["total_over_median_correct"] = (float(tot_over_prob) >= 0.5) == (actual_total > median_proxy)

        # ── Second half winner ─────────────────────────────
        sh_prob = pred_dict.get("second_half_home_win_prob")
        if sh_prob is not None:
            # Try q3+q4 first, then q2 alone
            _q3h = _safe_float(actual.get("home_q3"))
            _q3a = _safe_float(actual.get("away_q3"))
            _q4h = _safe_float(actual.get("home_q4"))
            _q4a = _safe_float(actual.get("away_q4"))
            _q2h2 = _safe_float(actual.get("home_q2"))
            _q2a2 = _safe_float(actual.get("away_q2"))
            if _q3h is not None and _q3a is not None and _q4h is not None and _q4a is not None:
                sh_home = _q3h + _q4h
                sh_away = _q3a + _q4a
                record["second_half_home_win_correct"] = (float(sh_prob) >= 0.5) == (sh_home > sh_away)
            elif _q2h2 is not None and _q2a2 is not None:
                record["second_half_home_win_correct"] = (float(sh_prob) >= 0.5) == (_q2h2 > _q2a2)

        sh_total_pred = pred_dict.get("second_half_total")
        if sh_total_pred is not None:
            _q3h = _safe_float(actual.get("home_q3"))
            _q3a = _safe_float(actual.get("away_q3"))
            _q4h = _safe_float(actual.get("home_q4"))
            _q4a = _safe_float(actual.get("away_q4"))
            _q2h2 = _safe_float(actual.get("home_q2"))
            _q2a2 = _safe_float(actual.get("away_q2"))
            if _q3h is not None and _q3a is not None and _q4h is not None and _q4a is not None:
                actual_sh_total = _q3h + _q4h + _q3a + _q4a
                record["second_half_total_error"] = abs(float(sh_total_pred) - actual_sh_total)
            elif _q2h2 is not None and _q2a2 is not None:
                record["second_half_total_error"] = abs(float(sh_total_pred) - (_q2h2 + _q2a2))

        # ── Regulation winner ──────────────────────────────
        reg_home_prob = pred_dict.get("regulation_home_win_prob")
        reg_draw_prob = pred_dict.get("regulation_draw_prob")
        if reg_home_prob is not None:
            # Regulation result: use period scores excluding OT
            _ot_h = _safe_float(actual.get("home_ot"))
            _ot_a = _safe_float(actual.get("away_ot"))
            if _ot_h is not None and _ot_a is not None and (home_score - away_score) != 0:
                reg_home = home_score - (_ot_h or 0)
                reg_away = away_score - (_ot_a or 0)
            else:
                reg_home = home_score
                reg_away = away_score
            reg_actual_home_win = reg_home > reg_away
            reg_actual_draw = reg_home == reg_away
            pred_reg_home = float(reg_home_prob) >= 0.5
            record["regulation_home_win_correct"] = pred_reg_home == reg_actual_home_win
            if reg_draw_prob is not None:
                record["regulation_draw_correct"] = (float(reg_draw_prob) >= 0.5) == reg_actual_draw

        # ── Team totals ────────────────────────────────────
        home_total_pred = pred_dict.get("home_team_total")
        away_total_pred = pred_dict.get("away_team_total")
        if home_total_pred is not None and ah_f is not None:
            record["home_team_total_error"] = abs(float(home_total_pred) - ah_f)
        if away_total_pred is not None and aa_f is not None:
            record["away_team_total_error"] = abs(float(away_total_pred) - aa_f)

        home_tot_over_prob = pred_dict.get("home_team_total_over_prob")
        if home_tot_over_prob is not None and ah_f is not None and home_total_pred is not None:
            median_home = float(home_total_pred)
            record["home_team_total_over_correct"] = (float(home_tot_over_prob) >= 0.5) == (ah_f > median_home)

        away_tot_over_prob = pred_dict.get("away_team_total_over_prob")
        if away_tot_over_prob is not None and aa_f is not None and away_total_pred is not None:
            median_away = float(away_total_pred)
            record["away_team_total_over_correct"] = (float(away_tot_over_prob) >= 0.5) == (aa_f > median_away)

        # ── Comeback probability ────────────────────────────
        comeback_h_prob = pred_dict.get("comeback_home_prob")
        comeback_a_prob = pred_dict.get("comeback_away_prob")
        if (comeback_h_prob is not None or comeback_a_prob is not None) and has_ht_data:
            actual_ht_home2 = ah_home_q1 + ah_home_q2
            actual_ht_away2 = ah_away_q1 + ah_away_q2
            home_trailing_ht = actual_ht_home2 < actual_ht_away2
            away_trailing_ht = actual_ht_away2 < actual_ht_home2
            if comeback_h_prob is not None and home_trailing_ht and ah_f is not None and aa_f is not None:
                actual_comeback_h = (ah_f > aa_f)
                record["comeback_home_correct"] = (float(comeback_h_prob) >= 0.5) == actual_comeback_h
            if comeback_a_prob is not None and away_trailing_ht and ah_f is not None and aa_f is not None:
                actual_comeback_a = (aa_f > ah_f)
                record["comeback_away_correct"] = (float(comeback_a_prob) >= 0.5) == actual_comeback_a

        # ── Double Chance ────────────────────────────────────
        if ah_f is not None and aa_f is not None:
            dc_1x = pred_dict.get("double_chance_1X_prob")
            if dc_1x is not None:
                actual_1x = (ah_f >= aa_f)
                record["double_chance_1X_correct"] = (float(dc_1x) >= 0.5) == actual_1x
            dc_x2 = pred_dict.get("double_chance_X2_prob")
            if dc_x2 is not None:
                actual_x2 = (aa_f >= ah_f)
                record["double_chance_X2_correct"] = (float(dc_x2) >= 0.5) == actual_x2
            dc_12 = pred_dict.get("double_chance_12_prob")
            if dc_12 is not None:
                actual_12 = (ah_f != aa_f)
                record["double_chance_12_correct"] = (float(dc_12) >= 0.5) == actual_12

        # ── NRFI / YRFI ─────────────────────────────────────
        if "home_i1" in actual:
            hi1 = actual.get("home_i1")
            ai1 = actual.get("away_i1")
            if hi1 is not None and ai1 is not None:
                nrfi_p_bt = pred_dict.get("nrfi_prob")
                if nrfi_p_bt is not None:
                    actual_nrfi = (float(hi1) == 0 and float(ai1) == 0)
                    record["nrfi_correct"] = (float(nrfi_p_bt) >= 0.5) == actual_nrfi
                yrfi_p_bt = pred_dict.get("yrfi_prob")
                if yrfi_p_bt is not None:
                    actual_yrfi = (float(hi1) > 0 or float(ai1) > 0)
                    record["yrfi_correct"] = (float(yrfi_p_bt) >= 0.5) == actual_yrfi

        # ── Shutout ──────────────────────────────────────────
        if ah_f is not None and aa_f is not None:
            so_h = pred_dict.get("shutout_home_prob")
            if so_h is not None:
                record["shutout_home_correct"] = (float(so_h) >= 0.5) == (aa_f == 0)
            so_a = pred_dict.get("shutout_away_prob")
            if so_a is not None:
                record["shutout_away_correct"] = (float(so_a) >= 0.5) == (ah_f == 0)

        # ── Asian Handicap ───────────────────────────────────
        if ah_f is not None and aa_f is not None:
            margin = ah_f - aa_f
            ah_m1h = pred_dict.get("ah_minus1_home_prob")
            if ah_m1h is not None:
                record["ah_minus1_home_correct"] = (float(ah_m1h) >= 0.5) == (margin >= 2)
            ah_m1a = pred_dict.get("ah_minus1_away_prob")
            if ah_m1a is not None:
                record["ah_minus1_away_correct"] = (float(ah_m1a) >= 0.5) == (margin <= -2)
            ah_p1h = pred_dict.get("ah_plus1_home_prob")
            if ah_p1h is not None:
                record["ah_plus1_home_correct"] = (float(ah_p1h) >= 0.5) == (margin > -2)
            ah_p1a = pred_dict.get("ah_plus1_away_prob")
            if ah_p1a is not None:
                record["ah_plus1_away_correct"] = (float(ah_p1a) >= 0.5) == (margin < 2)

        return record


    # ── Report building ──────────────────────────────────

    def _build_report(
        self,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        if not self.records:
            return self._empty_report(start_date, end_date)

        non_draw = [r for r in self.records if not r.get("is_draw")]
        total = len(non_draw)
        correct = sum(1 for r in non_draw if r["winner_correct"])

        report: dict[str, Any] = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "days": self.days,
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "overall": {
                "total_predictions": total,
                "correct": correct,
                "accuracy": round(correct / total, 4) if total else None,
                "avg_brier_score": round(
                    float(np.mean([r["brier_score"] for r in non_draw])), 4
                )
                if non_draw
                else None,
                "draws_excluded": len(self.records) - total,
            },
            "by_sport": self._sport_accuracy(non_draw),
            "by_confidence_tier": self._tier_accuracy(non_draw),
            "best_worst_sports": self._best_worst(non_draw),
            "roi_simulation": self._roi_report(non_draw),
            "calibration": self._calibration_report(non_draw),
            "by_bet_type": self._bet_type_report(non_draw),
        }

        # Save to disk
        reports_dir = DATA_DIR / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        out_path = reports_dir / f"backtest_{end_date.isoformat()}.json"
        with open(out_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("Report saved → %s", out_path)

        # Print summary to console
        self._print_summary(report)

        return report

    # ── Accuracy by sport ────────────────────────────────

    def _sport_accuracy(
        self,
        records: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in records:
            groups[r["sport"]].append(r)

        result: dict[str, dict[str, Any]] = {}
        for sport, recs in sorted(groups.items()):
            total = len(recs)
            correct = sum(1 for r in recs if r["winner_correct"])
            briers = [r["brier_score"] for r in recs]
            result[sport] = {
                "total": total,
                "correct": correct,
                "accuracy": round(correct / total, 4) if total else None,
                "avg_brier": round(float(np.mean(briers)), 4) if briers else None,
            }
        return result

    # ── Accuracy by confidence tier ──────────────────────

    def _tier_accuracy(
        self,
        records: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in records:
            groups[r["confidence_tier"]].append(r)

        tier_order = ["elite_above_80", "high_70_80", "medium_high_60_70", "medium_55_60", "low_below_55"]
        result: dict[str, dict[str, Any]] = {}
        for tier in tier_order:
            recs = groups.get(tier, [])
            if not recs:
                continue
            total = len(recs)
            correct = sum(1 for r in recs if r["winner_correct"])
            result[tier] = {
                "total": total,
                "correct": correct,
                "accuracy": round(correct / total, 4) if total else None,
                "avg_confidence": round(
                    float(np.mean([r["confidence"] for r in recs])), 4
                ),
            }
        return result

    # ── Best / worst performing sports ───────────────────

    def _best_worst(
        self,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        sport_acc = self._sport_accuracy(records)
        if not sport_acc:
            return {"best": None, "worst": None}

        ranked = sorted(
            sport_acc.items(),
            key=lambda kv: (kv[1]["accuracy"] or 0, kv[1]["total"]),
            reverse=True,
        )
        best_sport, best_data = ranked[0]
        worst_sport, worst_data = ranked[-1]

        return {
            "best": {
                "sport": best_sport,
                "accuracy": best_data["accuracy"],
                "total": best_data["total"],
            },
            "worst": {
                "sport": worst_sport,
                "accuracy": worst_data["accuracy"],
                "total": worst_data["total"],
            },
        }

    # ── ROI simulation ───────────────────────────────────

    def _roi_report(
        self,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not records:
            return {}

        total_wagered = len(records) * BET_AMOUNT
        total_profit = sum(r["roi_profit"] for r in records)

        # Per-sport ROI
        sport_roi: dict[str, dict[str, Any]] = {}
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in records:
            groups[r["sport"]].append(r)

        for sport, recs in sorted(groups.items()):
            wagered = len(recs) * BET_AMOUNT
            profit = sum(r["roi_profit"] for r in recs)
            sport_roi[sport] = {
                "bets": len(recs),
                "wagered": round(wagered, 2),
                "profit": round(profit, 2),
                "roi_pct": round(profit / wagered * 100, 2) if wagered else None,
            }

        # Per-tier ROI
        tier_roi: dict[str, dict[str, Any]] = {}
        tier_groups: dict[str, list[dict]] = defaultdict(list)
        for r in records:
            tier_groups[r["confidence_tier"]].append(r)

        for tier, recs in tier_groups.items():
            wagered = len(recs) * BET_AMOUNT
            profit = sum(r["roi_profit"] for r in recs)
            tier_roi[tier] = {
                "bets": len(recs),
                "wagered": round(wagered, 2),
                "profit": round(profit, 2),
                "roi_pct": round(profit / wagered * 100, 2) if wagered else None,
            }

        return {
            "bet_amount": BET_AMOUNT,
            "total_bets": len(records),
            "total_wagered": round(total_wagered, 2),
            "total_profit": round(total_profit, 2),
            "roi_pct": round(total_profit / total_wagered * 100, 2)
            if total_wagered
            else None,
            "by_sport": sport_roi,
            "by_confidence_tier": tier_roi,
        }

    # ── Calibration ──────────────────────────────────────

    def _calibration_report(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        bins = [
            (0.50, 0.55),
            (0.55, 0.60),
            (0.60, 0.65),
            (0.65, 0.70),
            (0.70, 0.75),
            (0.75, 0.80),
            (0.80, 0.85),
            (0.85, 0.90),
            (0.90, 1.01),
        ]

        calibration: list[dict[str, Any]] = []
        for lo, hi in bins:
            # Use max(prob, 1-prob) so both home and away picks map to 0.50+
            recs = [
                r
                for r in records
                if lo <= max(r["home_win_prob"], 1.0 - r["home_win_prob"]) < hi
            ]
            if not recs:
                continue

            pred_avg = float(
                np.mean([max(r["home_win_prob"], 1.0 - r["home_win_prob"]) for r in recs])
            )
            actual_avg = float(
                np.mean([1.0 if r["winner_correct"] else 0.0 for r in recs])
            )
            calibration.append(
                {
                    "bin": f"{lo:.2f}-{hi:.2f}",
                    "count": len(recs),
                    "predicted_win_rate": round(pred_avg, 4),
                    "actual_win_rate": round(actual_avg, 4),
                    "gap": round(abs(pred_avg - actual_avg), 4),
                }
            )
        return calibration

    # ── Bet type breakdown ───────────────────────────────

    def _bet_type_report(
        self,
        records: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}

        total = len(records)
        correct = sum(1 for r in records if r["winner_correct"])
        result["winner"] = {
            "total": total,
            "correct": correct,
            "accuracy": round(correct / total, 4) if total else None,
        }

        spread_recs = [r for r in records if "spread_error" in r]
        if spread_recs:
            errors = [r["spread_error"] for r in spread_recs]
            result["spread"] = {
                "total": len(spread_recs),
                "mae": round(float(np.mean(errors)), 2),
                "rmse": round(float(np.sqrt(np.mean(np.square(errors)))), 2),
            }

        total_recs = [r for r in records if "total_error" in r]
        if total_recs:
            errors = [r["total_error"] for r in total_recs]
            result["total"] = {
                "total": len(total_recs),
                "mae": round(float(np.mean(errors)), 2),
                "rmse": round(float(np.sqrt(np.mean(np.square(errors)))), 2),
            }

        # ── Draw prediction ──────────────────────────────
        draw_recs = [r for r in records if "draw_correct" in r]
        if draw_recs:
            dc = sum(1 for r in draw_recs if r["draw_correct"])
            result["draw"] = {
                "total": len(draw_recs),
                "correct": dc,
                "accuracy": round(dc / len(draw_recs), 4),
                "draw_rate": round(float(np.mean([r.get("is_draw", False) for r in draw_recs])), 4),
            }

        # ── Overtime prediction ──────────────────────────
        ot_recs = [r for r in records if "ot_correct" in r]
        if ot_recs:
            oc = sum(1 for r in ot_recs if r["ot_correct"])
            result["overtime"] = {
                "total": len(ot_recs),
                "correct": oc,
                "accuracy": round(oc / len(ot_recs), 4),
                "ot_rate": round(float(np.mean([r.get("actual_ot", False) for r in ot_recs])), 4),
            }

        # ── Halftime winner ──────────────────────────────
        ht_recs = [r for r in records if "ht_winner_correct" in r]
        if ht_recs:
            htc = sum(1 for r in ht_recs if r["ht_winner_correct"])
            result["halftime_winner"] = {
                "total": len(ht_recs),
                "correct": htc,
                "accuracy": round(htc / len(ht_recs), 4),
            }

        # ── Halftime scores ──────────────────────────────
        ht_sc_recs = [r for r in records if "ht_home_score_error" in r]
        if ht_sc_recs:
            result["halftime_home_score"] = {
                "total": len(ht_sc_recs),
                "mae": round(float(np.mean([r["ht_home_score_error"] for r in ht_sc_recs])), 2),
            }
            result["halftime_away_score"] = {
                "total": len(ht_sc_recs),
                "mae": round(float(np.mean([r["ht_away_score_error"] for r in ht_sc_recs])), 2),
            }

        ht_spread_recs = [r for r in records if "ht_spread_error" in r]
        if ht_spread_recs:
            result["halftime_spread"] = {
                "total": len(ht_spread_recs),
                "mae": round(float(np.mean([r["ht_spread_error"] for r in ht_spread_recs])), 2),
                "rmse": round(float(np.sqrt(np.mean(np.square([r["ht_spread_error"] for r in ht_spread_recs])))), 2),
            }

        ht_total_recs = [r for r in records if "ht_total_error" in r]
        if ht_total_recs:
            result["halftime_total"] = {
                "total": len(ht_total_recs),
                "mae": round(float(np.mean([r["ht_total_error"] for r in ht_total_recs])), 2),
                "rmse": round(float(np.sqrt(np.mean(np.square([r["ht_total_error"] for r in ht_total_recs])))), 2),
            }

        # ── Per-period winner ────────────────────────────
        pw_recs = [r for r in records if "period_winner_accuracy" in r]
        if pw_recs:
            all_correct = sum(r["period_winner_correct_count"] for r in pw_recs)
            all_total = sum(r["period_winner_total_count"] for r in pw_recs)
            result["period_winner"] = {
                "total_period_bets": all_total,
                "correct": all_correct,
                "accuracy": round(all_correct / all_total, 4) if all_total else None,
            }

        pt_recs = [r for r in records if "period_total_mae" in r]
        if pt_recs:
            result["period_total"] = {
                "total": len(pt_recs),
                "avg_mae_per_game": round(float(np.mean([r["period_total_mae"] for r in pt_recs])), 2),
            }

        # ── First / last score ───────────────────────────
        fs_recs = [r for r in records if "first_score_correct" in r]
        if fs_recs:
            fsc = sum(1 for r in fs_recs if r["first_score_correct"])
            result["first_score"] = {
                "total": len(fs_recs),
                "correct": fsc,
                "accuracy": round(fsc / len(fs_recs), 4),
            }

        ls_recs = [r for r in records if "last_score_correct" in r]
        if ls_recs:
            lsc = sum(1 for r in ls_recs if r["last_score_correct"])
            result["last_score"] = {
                "total": len(ls_recs),
                "correct": lsc,
                "accuracy": round(lsc / len(ls_recs), 4),
            }

        # ── BTTS / Clean Sheet ────────────────────────────
        btts_recs = [r for r in records if "btts_correct" in r]
        if btts_recs:
            btts_c = sum(1 for r in btts_recs if r["btts_correct"])
            result["btts"] = {
                "total": len(btts_recs),
                "correct": btts_c,
                "accuracy": round(btts_c / len(btts_recs), 4),
            }

        cs_h_recs = [r for r in records if "clean_sheet_home_correct" in r]
        if cs_h_recs:
            cs_h_c = sum(1 for r in cs_h_recs if r["clean_sheet_home_correct"])
            result["clean_sheet_home"] = {
                "total": len(cs_h_recs),
                "correct": cs_h_c,
                "accuracy": round(cs_h_c / len(cs_h_recs), 4),
            }

        cs_a_recs = [r for r in records if "clean_sheet_away_correct" in r]
        if cs_a_recs:
            cs_a_c = sum(1 for r in cs_a_recs if r["clean_sheet_away_correct"])
            result["clean_sheet_away"] = {
                "total": len(cs_a_recs),
                "correct": cs_a_c,
                "accuracy": round(cs_a_c / len(cs_a_recs), 4),
            }

        # ── UFC Method of Victory ─────────────────────────
        ufc_m_recs = [r for r in records if "ufc_method_correct" in r]
        if ufc_m_recs:
            ufc_mc = sum(1 for r in ufc_m_recs if r["ufc_method_correct"])
            result["ufc_method"] = {
                "total": len(ufc_m_recs),
                "correct": ufc_mc,
                "accuracy": round(ufc_mc / len(ufc_m_recs), 4),
            }

        # ── Golf Top-10 ───────────────────────────────────
        golf_t10_recs = [r for r in records if "golf_top10_correct" in r]
        if golf_t10_recs:
            golf_t10_c = sum(1 for r in golf_t10_recs if r["golf_top10_correct"])
            result["golf_top10"] = {
                "total": len(golf_t10_recs),
                "correct": golf_t10_c,
                "accuracy": round(golf_t10_c / len(golf_t10_recs), 4),
            }

        # ── Tennis Straight Sets ──────────────────────────
        ss_recs = [r for r in records if "straight_sets_correct" in r]
        if ss_recs:
            ss_c = sum(1 for r in ss_recs if r["straight_sets_correct"])
            result["straight_sets"] = {
                "total": len(ss_recs),
                "correct": ss_c,
                "accuracy": round(ss_c / len(ss_recs), 4),
            }

        # ── Dominant win ──────────────────────────────────
        dom_recs = [r for r in records if "dominant_win_correct" in r]
        if dom_recs:
            dom_c = sum(1 for r in dom_recs if r["dominant_win_correct"])
            result["dominant_win"] = {
                "total": len(dom_recs),
                "correct": dom_c,
                "accuracy": round(dom_c / len(dom_recs), 4),
            }

        # ── Margin band ───────────────────────────────────
        mb_recs = [r for r in records if "margin_band_correct" in r]
        if mb_recs:
            mb_c = sum(1 for r in mb_recs if r["margin_band_correct"])
            result["margin_band"] = {
                "total": len(mb_recs),
                "correct": mb_c,
                "accuracy": round(mb_c / len(mb_recs), 4),
            }

        # ── Total score band ──────────────────────────────
        tb_recs = [r for r in records if "total_band_correct" in r]
        if tb_recs:
            tb_c = sum(1 for r in tb_recs if r["total_band_correct"])
            result["total_band"] = {
                "total": len(tb_recs),
                "correct": tb_c,
                "accuracy": round(tb_c / len(tb_recs), 4),
            }

        # ── Total over median ─────────────────────────────
        tom_recs = [r for r in records if "total_over_median_correct" in r]
        if tom_recs:
            tom_c = sum(1 for r in tom_recs if r["total_over_median_correct"])
            result["total_over_median"] = {
                "total": len(tom_recs),
                "correct": tom_c,
                "accuracy": round(tom_c / len(tom_recs), 4),
            }

        # ── Second half winner ────────────────────────────
        sh_recs = [r for r in records if "second_half_home_win_correct" in r]
        if sh_recs:
            sh_c = sum(1 for r in sh_recs if r["second_half_home_win_correct"])
            result["second_half_winner"] = {
                "total": len(sh_recs),
                "correct": sh_c,
                "accuracy": round(sh_c / len(sh_recs), 4),
            }

        sh_tot_recs = [r for r in records if "second_half_total_error" in r]
        if sh_tot_recs:
            result["second_half_total"] = {
                "total": len(sh_tot_recs),
                "mae": round(float(np.mean([r["second_half_total_error"] for r in sh_tot_recs])), 2),
            }

        # ── Regulation winner ─────────────────────────────
        reg_recs = [r for r in records if "regulation_home_win_correct" in r]
        if reg_recs:
            reg_c = sum(1 for r in reg_recs if r["regulation_home_win_correct"])
            result["regulation_winner"] = {
                "total": len(reg_recs),
                "correct": reg_c,
                "accuracy": round(reg_c / len(reg_recs), 4),
            }

        reg_draw_recs = [r for r in records if "regulation_draw_correct" in r]
        if reg_draw_recs:
            rd_c = sum(1 for r in reg_draw_recs if r["regulation_draw_correct"])
            result["regulation_draw_ot"] = {
                "total": len(reg_draw_recs),
                "correct": rd_c,
                "accuracy": round(rd_c / len(reg_draw_recs), 4),
            }

        # ── Team totals ───────────────────────────────────
        ht_recs2 = [r for r in records if "home_team_total_error" in r]
        if ht_recs2:
            result["home_team_total"] = {
                "total": len(ht_recs2),
                "mae": round(float(np.mean([r["home_team_total_error"] for r in ht_recs2])), 2),
            }

        at_recs2 = [r for r in records if "away_team_total_error" in r]
        if at_recs2:
            result["away_team_total"] = {
                "total": len(at_recs2),
                "mae": round(float(np.mean([r["away_team_total_error"] for r in at_recs2])), 2),
            }

        hto_recs = [r for r in records if "home_team_total_over_correct" in r]
        if hto_recs:
            hto_c = sum(1 for r in hto_recs if r["home_team_total_over_correct"])
            result["home_team_total_over"] = {
                "total": len(hto_recs),
                "correct": hto_c,
                "accuracy": round(hto_c / len(hto_recs), 4),
            }

        ato_recs = [r for r in records if "away_team_total_over_correct" in r]
        if ato_recs:
            ato_c = sum(1 for r in ato_recs if r["away_team_total_over_correct"])
            result["away_team_total_over"] = {
                "total": len(ato_recs),
                "correct": ato_c,
                "accuracy": round(ato_c / len(ato_recs), 4),
            }

        # ── Comeback ──────────────────────────────────────
        cb_h_recs = [r for r in records if "comeback_home_correct" in r]
        if cb_h_recs:
            cb_h_c = sum(1 for r in cb_h_recs if r["comeback_home_correct"])
            result["comeback_home"] = {
                "total": len(cb_h_recs),
                "correct": cb_h_c,
                "accuracy": round(cb_h_c / len(cb_h_recs), 4),
            }

        cb_a_recs = [r for r in records if "comeback_away_correct" in r]
        if cb_a_recs:
            cb_a_c = sum(1 for r in cb_a_recs if r["comeback_away_correct"])
            result["comeback_away"] = {
                "total": len(cb_a_recs),
                "correct": cb_a_c,
                "accuracy": round(cb_a_c / len(cb_a_recs), 4),
            }

        # ── Double Chance ─────────────────────────────────
        for dc_key in ("double_chance_1X", "double_chance_X2", "double_chance_12"):
            recs_dc = [r for r in records if f"{dc_key}_correct" in r]
            if recs_dc:
                c_dc = sum(1 for r in recs_dc if r[f"{dc_key}_correct"])
                result[dc_key] = {
                    "total": len(recs_dc),
                    "correct": c_dc,
                    "accuracy": round(c_dc / len(recs_dc), 4),
                }

        # ── NRFI / YRFI ──────────────────────────────────
        for nrfi_key in ("nrfi", "yrfi"):
            recs_nr = [r for r in records if f"{nrfi_key}_correct" in r]
            if recs_nr:
                c_nr = sum(1 for r in recs_nr if r[f"{nrfi_key}_correct"])
                result[nrfi_key] = {
                    "total": len(recs_nr),
                    "correct": c_nr,
                    "accuracy": round(c_nr / len(recs_nr), 4),
                }

        # ── Shutout ───────────────────────────────────────
        for so_key in ("shutout_home", "shutout_away"):
            recs_so = [r for r in records if f"{so_key}_correct" in r]
            if recs_so:
                c_so = sum(1 for r in recs_so if r[f"{so_key}_correct"])
                result[so_key] = {
                    "total": len(recs_so),
                    "correct": c_so,
                    "accuracy": round(c_so / len(recs_so), 4),
                }

        # ── Asian Handicap ────────────────────────────────
        for ah_key in ("ah_minus1_home", "ah_minus1_away", "ah_plus1_home", "ah_plus1_away"):
            recs_ah = [r for r in records if f"{ah_key}_correct" in r]
            if recs_ah:
                c_ah = sum(1 for r in recs_ah if r[f"{ah_key}_correct"])
                result[ah_key] = {
                    "total": len(recs_ah),
                    "correct": c_ah,
                    "accuracy": round(c_ah / len(recs_ah), 4),
                }

        # ── Esports map-win markets ───────────────────────
        sweep_recs = [r for r in records if "esports_clean_sweep_correct" in r]
        if sweep_recs:
            sweep_c = sum(1 for r in sweep_recs if r["esports_clean_sweep_correct"])
            result["esports_clean_sweep"] = {
                "total": len(sweep_recs),
                "correct": sweep_c,
                "accuracy": round(sweep_c / len(sweep_recs), 4),
            }

        mt_recs = [r for r in records if "esports_map_total_error" in r]
        if mt_recs:
            result["esports_map_total"] = {
                "total": len(mt_recs),
                "mae": round(float(np.mean([r["esports_map_total_error"] for r in mt_recs])), 2),
            }

        mt2_recs = [r for r in records if "esports_map_total_over2_correct" in r]
        if mt2_recs:
            mt2_c = sum(1 for r in mt2_recs if r["esports_map_total_over2_correct"])
            result["esports_map_total_over2"] = {
                "total": len(mt2_recs),
                "correct": mt2_c,
                "accuracy": round(mt2_c / len(mt2_recs), 4),
            }

        # ── Player props ──────────────────────────────────
        _PLAYER_PROP_KEYS = [
            ("nba_pts_over_20", "nba_pts_over_20_correct"),
            ("nba_double_double", "nba_double_double_correct"),
            ("nfl_pass_yds_over_250", "nfl_pass_yds_over_250_correct"),
            ("nfl_rush_yds_over_75", "nfl_rush_yds_over_75_correct"),
            ("mlb_pitcher_k_over_6", "mlb_pitcher_k_over_6_correct"),
            ("nhl_player_point", "nhl_player_point_correct"),
            ("nhl_shots_over_3", "nhl_shots_over_3_correct"),
        ]
        for report_key, record_key in _PLAYER_PROP_KEYS:
            prop_recs = [r for r in records if record_key in r]
            if prop_recs:
                prop_c = sum(1 for r in prop_recs if r[record_key])
                result[report_key] = {
                    "total": len(prop_recs),
                    "correct": prop_c,
                    "accuracy": round(prop_c / len(prop_recs), 4),
                }

        return result

    # ── Console summary ──────────────────────────────────

    def _print_summary(self, report: dict[str, Any]) -> None:
        overall = report.get("overall", {})
        logger.info("═" * 60)
        logger.info("  BACKTEST RESULTS  (%d days)", self.days)
        logger.info("═" * 60)
        logger.info(
            "  Total predictions:  %d", overall.get("total_predictions", 0)
        )
        acc = overall.get("accuracy")
        if acc is not None:
            logger.info("  Overall accuracy:   %.2f%%", acc * 100)
        brier = overall.get("avg_brier_score")
        if brier is not None:
            logger.info("  Avg Brier score:    %.4f", brier)

        # Per-sport table
        by_sport = report.get("by_sport", {})
        if by_sport:
            logger.info("─" * 60)
            logger.info("  %-12s %6s %6s %8s", "Sport", "Total", "Right", "Acc%")
            logger.info("  %-12s %6s %6s %8s", "─" * 12, "─" * 6, "─" * 6, "─" * 8)
            for sport, data in sorted(
                by_sport.items(),
                key=lambda kv: (kv[1].get("accuracy") or 0),
                reverse=True,
            ):
                a = data.get("accuracy")
                logger.info(
                    "  %-12s %6d %6d %7.2f%%",
                    sport.upper(),
                    data["total"],
                    data["correct"],
                    (a or 0) * 100,
                )

        # Confidence tiers (5-tier)
        by_tier = report.get("by_confidence_tier", {})
        if by_tier:
            logger.info("─" * 60)
            logger.info("  Confidence tiers (5-tier):")
            tier_order = ["elite_above_80", "high_70_80", "medium_high_60_70", "medium_55_60", "low_below_55"]
            for tier in tier_order:
                data = by_tier.get(tier)
                if data:
                    a = data.get("accuracy")
                    label = tier.replace("_", " ").title()
                    logger.info(
                        "    %-26s  %4d games  %.2f%% accuracy",
                        label,
                        data["total"],
                        (a or 0) * 100,
                    )

        # ROI
        roi = report.get("roi_simulation", {})
        if roi:
            logger.info("─" * 60)
            logger.info(
                "  ROI ($%d flat):  $%.2f profit  (%.2f%%)",
                int(BET_AMOUNT),
                roi.get("total_profit", 0),
                roi.get("roi_pct", 0),
            )

        # Best / worst
        bw = report.get("best_worst_sports", {})
        best = bw.get("best")
        worst = bw.get("worst")
        if best and worst and best["sport"] != worst["sport"]:
            logger.info("─" * 60)
            logger.info(
                "  Best:  %s (%.2f%%  n=%d)",
                best["sport"].upper(),
                (best["accuracy"] or 0) * 100,
                best["total"],
            )
            logger.info(
                "  Worst: %s (%.2f%%  n=%d)",
                worst["sport"].upper(),
                (worst["accuracy"] or 0) * 100,
                worst["total"],
            )

        # Extra market accuracy summary
        by_bet = report.get("by_bet_type", {})
        extra_markets = {k: v for k, v in by_bet.items() if k not in ("winner", "spread", "total")}
        if extra_markets:
            logger.info("─" * 60)
            logger.info("  Extra-market accuracy:")
            for bt, data in sorted(extra_markets.items()):
                if "accuracy" in data and data["accuracy"] is not None:
                    logger.info(
                        "    %-22s  %4d games  %.2f%%",
                        bt.replace("_", " ").title(),
                        data.get("total", data.get("total_period_bets", 0)),
                        (data["accuracy"] or 0) * 100,
                    )
                elif "mae" in data:
                    logger.info(
                        "    %-22s  %4d games  MAE=%.2f",
                        bt.replace("_", " ").title(),
                        data.get("total", 0),
                        data["mae"],
                    )

        logger.info("═" * 60)

    # ── Empty report ─────────────────────────────────────

    def _empty_report(
        self,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "days": self.days,
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "overall": {
                "total_predictions": 0,
                "correct": 0,
                "accuracy": None,
                "avg_brier_score": None,
                "draws_excluded": 0,
            },
            "by_sport": {},
            "by_confidence_tier": {},
            "best_worst_sports": {"best": None, "worst": None},
            "roi_simulation": {},
            "calibration": [],
            "by_bet_type": {},
            "message": "No predictions or actual results found in date range",
        }


# ── CLI ──────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backtest",
        description="V5.0 30-Day Backtest & Analysis — evaluate prediction accuracy on recent games",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python3 scripts/backtest.py                          # all sports, 30 days
  python3 scripts/backtest.py --sport nba              # NBA only
  python3 scripts/backtest.py --sport nba --days 14    # NBA, last 14 days
  python3 scripts/backtest.py --verbose                # debug logging

Output:
  data/reports/backtest_{YYYY-MM-DD}.json
""",
    )
    parser.add_argument(
        "--sport",
        type=str,
        default=None,
        help="Single sport to backtest (e.g. nba, nhl, mlb, nfl, epl, ufc, atp, wta, laliga …). Default: all with models.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Last date (inclusive) for the backtest window. "
            "Default: today.  Use this to evaluate a historical period, "
            "e.g. '--end-date 2024-09-30' for end of MLB regular season."
        ),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug-level logging",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(verbose=args.verbose)

    if args.days < 1:
        logger.error("--days must be >= 1")
        return 1

    end_date: date | None = None
    if args.end_date:
        try:
            end_date = date.fromisoformat(args.end_date)
        except ValueError:
            logger.error("--end-date must be YYYY-MM-DD, got %r", args.end_date)
            return 1

    if args.sport:
        sport = args.sport.strip().lower()
        # Accept any sport that has a feature extractor (even if no model yet)
        from features.registry import EXTRACTORS
        valid_sports = set(ALL_SPORTS) | set(EXTRACTORS.keys())
        if sport not in valid_sports:
            logger.error(
                "Unknown sport %r. Choose from: %s",
                sport,
                ", ".join(sorted(valid_sports)),
            )
            return 1
        sports = [sport]
    else:
        sports = list(ALL_SPORTS)

    t0 = time.monotonic()
    backtester = Backtester(sports=sports, days=args.days, end_date=end_date, verbose=args.verbose)
    report = backtester.run()
    elapsed = round(time.monotonic() - t0, 2)
    logger.info("Completed in %.1fs", elapsed)

    # Dump full JSON to stdout for piping / inspection
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
