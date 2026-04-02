# ──────────────────────────────────────────────────────────
# V5.0 Backend — ML Training Pipeline
# ──────────────────────────────────────────────────────────
"""
CLI entry-point for training, prediction, backtesting and
data-quality verification.

Usage
-----
::

    # Train a single sport (joint mode — default)
    python -m ml.train train --sport nba --seasons 2023,2024,2025

    # Train separate winner / total / spread models
    python -m ml.train train --sport nba --seasons 2023,2024 --mode separate

    # Train all registered sports
    python -m ml.train train-all --seasons 2023,2024,2025

    # Predict today's games
    python -m ml.train predict --sport nba

    # Predict a specific date
    python -m ml.train predict --sport nba --date 2026-03-25

    # Walk-forward backtest
    python -m ml.train backtest --sport nba --start 2023-10-01 --end 2024-06-30

    # Data quality check
    python -m ml.train verify --sport nba
"""

from __future__ import annotations

import argparse
import json
import logging
import pickle
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.registry import EXTRACTORS, get_extractor
from ml.models.base import PredictionResult, TrainingConfig
from ml.models.ensemble import EnsembleVoter

logger = logging.getLogger(__name__)

# ── Metadata columns (not used as features) ─────────────

_META_COLS = {
    "game_id",
    "date",
    "home_team_id",
    "away_team_id",
    "home_score",
    "away_score",
    # quarter/period/inning scores — targets for extra markets, not input features
    "home_q1", "home_q2", "home_q3", "home_q4", "home_ot",
    "away_q1", "away_q2", "away_q3", "away_q4", "away_ot",
    # MLB inning-by-inning scores (outcome data — must not be input features)
    "home_i1", "home_i2", "home_i3", "home_i4", "home_i5",
    "home_i6", "home_i7", "home_i8", "home_i9", "home_extras",
    "away_i1", "away_i2", "away_i3", "away_i4", "away_i5",
    "away_i6", "away_i7", "away_i8", "away_i9", "away_extras",
    # Soccer half scores (outcome data)
    "home_h1", "home_h2", "away_h1", "away_h2",
    "home_h1_score", "home_h2_score", "away_h1_score", "away_h2_score",
    # Soccer corners/cards raw totals (market targets, not input features)
    "home_corners_total", "away_corners_total",
    "home_yellow_total", "away_yellow_total",
    "home_red_total", "away_red_total",
    # Hockey period scores (outcome data)
    "home_p1", "home_p2", "home_p3", "away_p1", "away_p2", "away_p3",
    # NBA 3-pointer totals (market targets)
    "home_three_m_game", "away_three_m_game",
    # NHL shots totals (market targets)
    "home_shots_game", "away_shots_game",
    # MLB hits totals (market targets)
    "home_hits_game", "away_hits_game",
    # NBA rebounds/turnovers/assists totals (market targets)
    "home_reb_game", "away_reb_game",
    "home_to_game", "away_to_game",
    "home_ast_game", "away_ast_game",
    # Motorsport (F1) current-race outcome columns — NOT available pre-race
    "podium", "points_finish", "dnf", "fastest_lap",
    "laps_completed", "laps_completion_pct",
    "avg_speed_kph", "pit_stops", "avg_pit_time_s",
    "safety_car_count", "dnf_count", "red_flag_count", "race_pit_stops_total",
    # UFC fight outcome columns — known only after the fight, not usable as features
    "home_finish_round", "away_finish_round",
    "home_knockdowns", "away_knockdowns",
    "home_submission_attempts", "away_submission_attempts",
    "home_control_time_seconds", "away_control_time_seconds",
    # Soccer shot totals — used as target for shots market, exclude from features
    "home_shots", "away_shots",
}

# Sports that can legitimately end in a draw/tie at full time
_DRAW_SPORTS = frozenset({"epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "nfl", "europa", "ligamx", "championship", "eredivisie", "primeiraliga", "euros"})
# Sports that use q1–q4 (or periods 1–3) for halftime/period markets
_PERIOD_SPORTS = frozenset({"nba", "nfl", "nhl", "mlb", "ncaab", "ncaaf", "wnba", "ncaaw"})
# Per-sport period label for display
_PERIOD_LABEL: dict[str, str] = {
    "nba": "quarter", "ncaab": "half", "nfl": "quarter", "ncaaf": "quarter",
    "wnba": "quarter", "ncaaw": "half",
    "nhl": "period", "mlb": "inning",
}
# Soccer sports use h1/h2 columns for halftime (not q1/q2)
_SOCCER_HALF_SPORTS = frozenset({"epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "europa", "ligamx", "championship", "eredivisie", "primeiraliga", "euros"})
# Two-half sports: halftime score = first half only (not sum of two periods)
# NBA/NFL/NCAAF/WNBA/NCAAW are 4-quarter; their halftime = Q1+Q2
# Soccer and NCAAB are 2-half; their halftime = H1 only
_TWO_HALF_SPORTS = _SOCCER_HALF_SPORTS | frozenset({"ncaab"})
# Sports with meaningful halftime (first-half data in q1+q2 or h1)
_HALFTIME_SPORTS = frozenset({"nba", "nfl", "ncaab", "ncaaf", "wnba", "ncaaw", "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "europa", "ligamx", "championship", "eredivisie", "primeiraliga", "euros"})
# Soccer-family sports (BTTS, clean sheet)
_SOCCER_SPORTS = frozenset({"epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "europa", "ligamx", "championship", "eredivisie", "primeiraliga", "euros"})
# Sports with OT/extra time
_OT_SPORTS = frozenset({"nba", "nfl", "nhl", "mlb", "ncaab", "ncaaf", "wnba", "ncaaw", "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "europa", "ligamx", "championship", "eredivisie", "primeiraliga", "euros"})
# Sports with meaningful margin/total bands (team-vs-team scoring games)
_MARGIN_SPORTS = frozenset({"nba", "nfl", "nhl", "mlb", "ncaab", "ncaaf", "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "wnba", "ncaaw", "csgo", "dota2", "lol", "valorant", "europa", "ligamx", "championship", "eredivisie", "primeiraliga", "euros"})
# Sports where 2nd-half/comeback models are meaningful
_SECOND_HALF_SPORTS = _HALFTIME_SPORTS
# Sports to skip ALL extra markets (pure binary outcome — no subgame structure)
_NO_EXTRA_MARKETS = frozenset({"golf", "lpga"})
# Motorsport sports: use podium/points/DNF/fastest-lap markets
_MOTORSPORT_SPORTS = frozenset({"f1", "indycar"})
# Esports: home_score/away_score represent MAP WINS in a series (max 3)
_ESPORTS_SPORTS = frozenset({"csgo", "dota2", "lol", "valorant"})
# Sports with double-chance markets (1X, 12, X2) — any sport that allows draws
_DOUBLE_CHANCE_SPORTS = _DRAW_SPORTS
# Sports where shutout/clean-sheet models are meaningful beyond soccer (hockey, low-scoring)
_SHUTOUT_SPORTS = _SOCCER_SPORTS | frozenset({"nhl"})
# Sports where NRFI/YRFI (no/yes run first inning) is a valid market
_NRFI_SPORTS = frozenset({"mlb"})
# Sports where Asian Handicap lines (−1, −1.5) are meaningful
_ASIAN_HANDICAP_SPORTS = _SOCCER_SPORTS | frozenset({"nhl"})
# Sports with three-pointer over/under market
_THREE_POINTER_SPORTS = frozenset({"nba", "wnba", "ncaab", "ncaaw"})
# Sports with total shots market
_SHOTS_MARKET_SPORTS = frozenset({"nhl"})
# Sports with total hits market
_HITS_MARKET_SPORTS = frozenset({"mlb"})
# Soccer sports with second-half goals market
_SOCCER_H2_SPORTS = _SOCCER_HALF_SPORTS
# Sports with total rebounds market
_REBOUNDS_MARKET_SPORTS = frozenset({"nba", "wnba"})
# Sports with total turnovers market
_TURNOVERS_MARKET_SPORTS = frozenset({"nba", "wnba", "ncaab"})
# Sports with total assists market
_ASSISTS_MARKET_SPORTS = frozenset({"nba", "wnba"})
# NHL period-by-period goals O/U market
_NHL_PERIOD_GOALS_SPORTS = frozenset({"nhl"})
# Soccer total shots O/U market
_SOCCER_SHOTS_TOTAL_SPORTS = frozenset({"epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "europa", "mls", "nwsl", "ligamx", "championship", "eredivisie", "primeiraliga", "euros"})
# UFC total fight distance (rounds O/U 2.5)
_UFC_ROUNDS_SPORTS = frozenset({"ufc"})
# NFL first quarter total O/U market
_NFL_Q1_MARKET_SPORTS = frozenset({"nfl", "ncaaf"})
# Basketball/football Q4 (last-quarter) total O/U market
_Q4_TOTAL_SPORTS = frozenset({"nba", "nfl", "ncaaf", "wnba"})
# Soccer BTTS-in-both-halves market
_BTTS_BOTH_HALVES_SPORTS = _SOCCER_HALF_SPORTS
# HT/FT double-result (halftime result paired with full-time result)
_HT_FT_SPORTS = _HALFTIME_SPORTS
# NFL/NCAAF total turnovers (INT + fumbles lost) O/U market
_NFL_TURNOVERS_SPORTS = frozenset({"nfl", "ncaaf"})


def _add_delta_features(X: pd.DataFrame) -> pd.DataFrame:
    """Add home-minus-away delta features for all matched home_*/away_* pairs.

    Delta features make the magnitude of team advantage explicit, which helps
    tree models and linear models alike. Only adds deltas for columns with
    both home_ and away_ variants present.
    """
    home_cols = {c[5:]: c for c in X.columns if c.startswith("home_")}
    away_cols = {c[5:]: c for c in X.columns if c.startswith("away_")}
    shared = set(home_cols) & set(away_cols)
    deltas = {}
    for key in sorted(shared):
        hc, ac = home_cols[key], away_cols[key]
        delta_name = f"delta_{key}"
        if delta_name not in X.columns:
            deltas[delta_name] = X[hc].values - X[ac].values
    if deltas:
        delta_df = pd.DataFrame(deltas, index=X.index)
        X = pd.concat([X, delta_df], axis=1)
    return X


# ── Trainer ──────────────────────────────────────────────


class Trainer:
    """Orchestrates the full train → evaluate → serialise loop."""

    def __init__(self, config: TrainingConfig, data_dir: Path) -> None:
        self.config = config
        self.data_dir = data_dir
        self.models_dir = data_dir.parent / "ml" / "models" / config.sport
        self.models_dir.mkdir(parents=True, exist_ok=True)
        self.extractor = get_extractor(config.sport, data_dir)

    # ── Data preparation ─────────────────────────────────

    def _load_precomputed_features(self) -> pd.DataFrame | None:
        """Try to load pre-computed feature parquets instead of extracting live.

        Checks for: data/features/{sport}_all.parquet (combined multi-season)
        then falls back to data/features/{sport}_{season}.parquet per season.
        """
        features_dir = self.data_dir / "features"
        if not features_dir.exists():
            return None

        # Try combined multi-season file first
        combined = features_dir / f"{self.config.sport}_all.parquet"
        if combined.exists():
            df = pd.read_parquet(combined)
            if len(df) > 0:
                logger.info(
                    "Loaded pre-computed features: %s (%d rows, %d cols)",
                    combined.name, len(df), len(df.columns),
                )
                return df

        # Fall back to per-season files
        frames: list[pd.DataFrame] = []
        for season in self.config.seasons:
            path = features_dir / f"{self.config.sport}_{season}.parquet"
            if path.exists():
                df = pd.read_parquet(path)
                if len(df) > 0:
                    if "season" not in df.columns:
                        df["season"] = season
                    frames.append(df)
                    logger.info("  Loaded cached features: %s (%d rows)", path.name, len(df))

        if frames:
            return pd.concat(frames, ignore_index=True)
        return None

    def prepare_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load features for all configured seasons, apply temporal split.

        First tries pre-computed feature parquets (fast). Falls back to
        live extraction from normalized data (slow but comprehensive).

        Returns
        -------
        (train_df, val_df) : tuple of DataFrames
            Each DataFrame contains both feature columns and the target
            columns (``home_score``, ``away_score``).  Metadata columns
            are preserved for downstream introspection but should be
            dropped before fitting.
        """
        # Fast path: use pre-computed feature parquets
        full = self._load_precomputed_features()
        if full is not None and len(full) > 0:
            logger.info("Using pre-computed features (%d rows)", len(full))
        else:
            # Slow path: extract features live
            frames: list[pd.DataFrame] = []
            for season in self.config.seasons:
                logger.info("Extracting features for %s season %d …", self.config.sport, season)
                try:
                    df = self.extractor.extract_all(season)
                    if df is not None and len(df) > 0:
                        df["season"] = season
                        frames.append(df)
                        logger.info("  → %d games", len(df))
                except FileNotFoundError:
                    logger.warning("  → data not found for season %d — skipping", season)
                except Exception:
                    logger.error("  → error extracting season %d", season, exc_info=True)
            full = pd.concat(frames, ignore_index=True) if frames else None

        if full is None or len(full) == 0:
            raise RuntimeError(
                f"No feature data for {self.config.sport} "
                f"seasons {self.config.seasons}"
            )

        # Drop rows with missing scores (future/unplayed games).
        # Also drop rows where BOTH scores are exactly 0 for sports that cannot
        # legitimately end 0-0 (e.g. MLB, NBA, NFL) — these are scheduled games
        # whose scores defaulted to 0 instead of NaN in the feature parquet.
        if "home_score" in full.columns and "away_score" in full.columns:
            before = len(full)
            full = full.dropna(subset=["home_score", "away_score"]).reset_index(drop=True)
            _NO_ZERO_ZERO_SPORTS = {"mlb", "nba", "nfl", "ncaab", "ncaaf", "ncaaw", "wnba", "nhl", "atp", "wta"}
            if self.config.sport in _NO_ZERO_ZERO_SPORTS:
                zero_mask = (full["home_score"] == 0) & (full["away_score"] == 0)
                full = full[~zero_mask].reset_index(drop=True)
            dropped = before - len(full)
            if dropped:
                logger.info("Dropped %d rows with missing/invalid scores", dropped)

        # Sort chronologically for temporal split
        if "date" in full.columns:
            full = full.sort_values("date").reset_index(drop=True)

        if len(full) < self.config.min_samples:
            raise RuntimeError(
                f"Only {len(full)} samples — minimum is {self.config.min_samples}"
            )

        # Drop dead features
        if self.config.drop_dead_features:
            feature_cols = [c for c in full.columns if c not in _META_COLS and c != "season"]
            numeric = full[feature_cols].select_dtypes(include=[np.number])
            nonzero_pct = (numeric != 0).mean()
            dead = nonzero_pct[nonzero_pct <= self.config.dead_feature_threshold].index.tolist()
            if dead:
                logger.info("Dropping %d dead features: %s", len(dead), dead[:10])
                full = full.drop(columns=dead)

        # Temporal split — last test_size fraction of rows
        split_idx = int(len(full) * (1 - self.config.test_size))
        train_df = full.iloc[:split_idx].reset_index(drop=True)
        val_df = full.iloc[split_idx:].reset_index(drop=True)

        logger.info(
            "Data prepared: %d train / %d val (%.0f%% / %.0f%%)",
            len(train_df),
            len(val_df),
            100 * len(train_df) / len(full),
            100 * len(val_df) / len(full),
        )
        return train_df, val_df

    @staticmethod
    def _split_xy(
        df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
        """Split a DataFrame into (X_features, home_score, away_score)."""
        feature_cols = [c for c in df.columns if c not in _META_COLS and c != "season"]
        X = df[feature_cols].select_dtypes(include=[np.number]).fillna(0)
        X = _add_delta_features(X)
        home_score = df["home_score"] if "home_score" in df.columns else pd.Series(dtype=float)
        away_score = df["away_score"] if "away_score" in df.columns else pd.Series(dtype=float)
        # Ensure aligned indices after delta feature construction
        X = X.reset_index(drop=True)
        home_score = home_score.reset_index(drop=True)
        away_score = away_score.reset_index(drop=True)
        return X, home_score, away_score

    @staticmethod
    def _apply_variance_filter(X_train: pd.DataFrame, X_val: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Remove near-zero-variance columns from X_train and align X_val to same columns.

        Variance is computed from training data only. Val set is trimmed to match.
        Odds/implied-prob columns are always kept.
        """
        odds_cols = {"home_moneyline", "away_moneyline", "spread", "total", "home_implied_prob"}
        keep_cols = [c for c in X_train.columns if X_train[c].std() > 1e-6 or c in odds_cols]
        X_train_filt = X_train[keep_cols]
        # Align val to same columns (add missing as 0, drop extra)
        shared = [c for c in keep_cols if c in X_val.columns]
        X_val_filt = X_val.reindex(columns=keep_cols, fill_value=0)
        dropped = len(X_train.columns) - len(keep_cols)
        if dropped > 0:
            logger.debug("Variance filter dropped %d near-zero-variance features", dropped)
        return X_train_filt, X_val_filt

    # ── Joint training ───────────────────────────────────

    def train_joint(self) -> dict[str, Any]:
        """Train joint home_score + away_score regression models.

        Winner is derived from the predicted score difference.
        Golf uses a special path: predict position (regression) and top_10 (classification).

        Returns
        -------
        Dict with keys ``home_ensemble``, ``away_ensemble``,
        ``winner_ensemble``, ``metrics``, ``feature_names``.
        """
        train_df, val_df = self.prepare_data()

        # ── Golf: per-player tournament model ──────────────────────
        if self.config.sport in ("golf", "lpga"):
            return self._train_golf(train_df, val_df)

        X_train, home_train, away_train = self._split_xy(train_df)
        X_val, home_val, away_val = self._split_xy(val_df)
        # Apply consistent variance filter: compute from train, align val
        X_train, X_val = self._apply_variance_filter(X_train, X_val)

        # Winner label: 1 = home win, 0 = away win
        # Exclude draws from classification training: draws encode the same
        # "balanced match" signal as uncertain outcomes but label them 0 (away),
        # which biases classifiers — especially for soccer where draws are ~25%.
        train_nodraw_mask = home_train != away_train
        val_nodraw_mask = home_val != away_val

        X_train_cls = X_train[train_nodraw_mask]
        X_val_cls = X_val[val_nodraw_mask]
        y_train_cls = (home_train[train_nodraw_mask] > away_train[train_nodraw_mask]).astype(int)
        y_val_cls = (home_val[val_nodraw_mask] > away_val[val_nodraw_mask]).astype(int)

        # Guard: if the val set has no non-draw games (e.g. very small/all-draw val split),
        # carve out the last 20% of training games as a fallback validation set.
        if len(X_val_cls) == 0:
            logger.warning(
                "Validation set has 0 non-draw games — carving last 20%% of training set for validation."
            )
            split_idx = max(1, int(len(X_train_cls) * 0.8))
            X_val_cls = X_train_cls.iloc[split_idx:]
            y_val_cls = y_train_cls.iloc[split_idx:]
            X_train_cls = X_train_cls.iloc[:split_idx]
            y_train_cls = y_train_cls.iloc[:split_idx]

        n_draws_train = (~train_nodraw_mask).sum()
        n_draws_val = (~val_nodraw_mask).sum()
        if n_draws_train > 0:
            logger.info(
                "Excluded %d draws (%.1f%%) from classification training",
                n_draws_train,
                100 * n_draws_train / len(home_train),
            )

        logger.info("Training winner classifier …")
        winner_ensemble = EnsembleVoter()
        cls_metrics = winner_ensemble.fit_classifiers(X_train_cls, y_train_cls, X_val_cls, y_val_cls)

        # Log top feature importances after fitting
        try:
            fi = winner_ensemble.get_feature_importances()
            if fi:
                top10 = list(fi.items())[:10]
                logger.info("Top-10 winner features: %s", top10)
                fi_path = self.models_dir / "feature_importances.json"
                fi_path.write_text(json.dumps(fi, indent=2))
        except Exception:
            pass

        logger.info("Training home-score regressor …")
        home_ensemble = EnsembleVoter()
        home_metrics = home_ensemble.fit_regressors(X_train, home_train, X_val, home_val)

        logger.info("Training away-score regressor …")
        away_ensemble = EnsembleVoter()
        away_metrics = away_ensemble.fit_regressors(X_train, away_train, X_val, away_val)

        result = {
            "winner_ensemble": winner_ensemble,
            "home_ensemble": home_ensemble,
            "away_ensemble": away_ensemble,
            "feature_names": list(X_train.columns),
            "metrics": {
                "classification": cls_metrics,
                "home_regression": home_metrics,
                "away_regression": away_metrics,
            },
            "config": self.config,
            "trained_at": datetime.utcnow().isoformat(),
        }

        self.save_models(result, self.models_dir / "joint_models.pkl")
        logger.info("Joint training complete ✓")

        # Train extra-market models (halftime, OT, draw, period, first-score)
        try:
            extra = self.train_extra_markets(train_df, val_df, X_train, X_val)
            if extra.get("models"):
                self.save_models(extra, self.models_dir / "extra_models.pkl")
                logger.info("Extra-market models saved ✓ (%d markets)", len(extra["models"]))
        except Exception:
            logger.warning("Extra-market training failed (non-fatal)", exc_info=True)

        return result

    # ── Golf-specific training ───────────────────────────

    def _train_golf(self, train_df: pd.DataFrame, val_df: pd.DataFrame) -> dict[str, Any]:
        """Golf: predict finish position (regression) and multiple finish-tier classifiers.

        Golf features are per-player-per-tournament, not head-to-head.
        We train:
        - position regressor: predicted finish position
        - top_10 classifier: P(finish in top 10)
        - top_5 classifier: P(finish in top 5)
        - top_3 classifier: P(finish on podium / top 3)
        - top_20 classifier: P(finish in top 20)
        - made_cut classifier: P(player makes the cut, finish position <= 70)
        - winner classifier: P(player wins tournament)
        """
        _GOLF_META = {"game_id", "date", "player_id", "player_name", "position",
                       "score_to_par", "won", "top_10", "season"}

        def _split_golf(df: pd.DataFrame):
            feat_cols = [c for c in df.columns if c not in _GOLF_META]
            X = df[feat_cols].select_dtypes(include=[np.number]).fillna(0).reset_index(drop=True)
            position = df["position"].reset_index(drop=True) if "position" in df.columns else pd.Series(dtype=float)
            top10 = df["top_10"].reset_index(drop=True) if "top_10" in df.columns else pd.Series(dtype=float)
            return X, position, top10

        X_train, pos_train, top10_train = _split_golf(train_df)
        X_val, pos_val, top10_val = _split_golf(val_df)

        if X_train.shape[1] == 0:
            raise ValueError("Golf feature matrix is empty — no numeric feature columns found")

        extra_models: dict = {}

        def _train_position_classifier(threshold: int, name: str) -> None:
            """Train a binary classifier for finishing within threshold."""
            if "position" not in train_df.columns:
                return
            pos_tr = train_df["position"].reset_index(drop=True)
            pos_va = val_df["position"].reset_index(drop=True)
            valid_tr = pos_tr.notna()
            valid_va = pos_va.notna()
            if valid_tr.sum() < 80 or valid_va.sum() < 20:
                return
            y_tr = (pos_tr[valid_tr] <= threshold).astype(int)
            y_va = (pos_va[valid_va] <= threshold).astype(int)
            if y_tr.mean() < 0.02 or y_tr.mean() > 0.98:
                logger.debug("Golf %s skipped — rate=%.1f%%", name, 100 * y_tr.mean())
                return
            ens = EnsembleVoter()
            logger.info("Training golf %s classifier (n=%d, rate=%.1f%%) …", name, int(valid_tr.sum()), 100 * y_tr.mean())
            ens.fit_classifiers(
                X_train.loc[valid_tr], y_tr,
                X_val.loc[valid_va], y_va,
                class_weight="balanced",
            )
            extra_models[name] = ens

        # Tier classifiers
        _train_position_classifier(3, "top_3")
        _train_position_classifier(5, "top_5")
        _train_position_classifier(20, "top_20")
        _train_position_classifier(70, "made_cut")

        # Winner classifier (from 'won' column if available)
        if "won" in train_df.columns:
            won_tr = train_df["won"].reset_index(drop=True).fillna(0).astype(int)
            won_va = val_df["won"].reset_index(drop=True).fillna(0).astype(int)
            if won_tr.mean() > 0.002 and won_tr.nunique() >= 2 and won_va.nunique() >= 2:
                winner_ens = EnsembleVoter()
                logger.info("Training golf winner classifier (rate=%.2f%%) …", 100 * won_tr.mean())
                winner_ens.fit_classifiers(X_train, won_tr, X_val, won_va, class_weight="balanced")
                extra_models["winner"] = winner_ens

        # Classification: top-10 finish (use balanced weights — top-10 rate is ~6.7%)
        logger.info("Training golf top-10 classifier …")
        top10_ensemble = EnsembleVoter()
        cls_metrics: dict = {}
        if top10_train.notna().any() and top10_train.nunique() >= 2 and top10_val.nunique() >= 2:
            cls_metrics = top10_ensemble.fit_classifiers(
                X_train, top10_train.astype(int),
                X_val, top10_val.astype(int),
                class_weight="balanced",
            )
        else:
            logger.warning("Golf top-10: skipping classifier — only one class in training/validation split")

        # Regression: finish position
        logger.info("Training golf position regressor …")
        pos_ensemble = EnsembleVoter()
        pos_metrics = pos_ensemble.fit_regressors(X_train, pos_train, X_val, pos_val)

        result = {
            "winner_ensemble": top10_ensemble,  # Re-use key for prediction compatibility
            "home_ensemble": pos_ensemble,       # Position regressor
            "away_ensemble": EnsembleVoter(),    # Placeholder (unused for golf)
            "feature_names": list(X_train.columns),
            "metrics": {
                "classification": cls_metrics,
                "home_regression": pos_metrics,
                "away_regression": {},
            },
            "config": self.config,
            "trained_at": datetime.utcnow().isoformat(),
            "golf_mode": True,
            "extra_models": extra_models,
        }

        self.save_models(result, self.models_dir / "joint_models.pkl")
        tiers = list(extra_models.keys())
        logger.info("Golf training complete ✓ (top-10 + position + tiers: %s)", tiers)
        return result

    # ── Extra-market training ────────────────────────────

    def _fit_cls_safe(
        self,
        name: str,
        X_tr: "pd.DataFrame",
        y_tr: "pd.Series",
        X_va: "pd.DataFrame",
        y_va: "pd.Series",
        min_samples: int = 80,
    ) -> "EnsembleVoter | None":
        """Fit a classifier, returning None if too few samples."""
        if len(X_tr) < min_samples or len(X_va) < 20:
            logger.debug("Skipping %s classifier — too few samples (%d/%d)", name, len(X_tr), len(X_va))
            return None
        if y_tr.nunique() < 2 or y_va.nunique() < 2:
            logger.debug("Skipping %s classifier — only one class present", name)
            return None
        model = EnsembleVoter()
        model.fit_classifiers(X_tr, y_tr, X_va, y_va)
        return model

    def _fit_reg_safe(
        self,
        name: str,
        X_tr: "pd.DataFrame",
        y_tr: "pd.Series",
        X_va: "pd.DataFrame",
        y_va: "pd.Series",
        min_samples: int = 80,
    ) -> "EnsembleVoter | None":
        """Fit a regressor, returning None if too few samples."""
        if len(X_tr) < min_samples or len(X_va) < 20:
            logger.debug("Skipping %s regressor — too few samples", name)
            return None
        model = EnsembleVoter()
        model.fit_regressors(X_tr, y_tr, X_va, y_va)
        return model

    def train_extra_markets(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
    ) -> dict[str, Any]:
        """Train additional betting-market models: halftime, OT/draw, period, first-score.

        Returns a bundle dict with ``models`` (name → EnsembleVoter), ``feature_names``,
        ``sport_meta`` (period_label, available_periods), and ``trained_at``.

        All models use the same feature matrix as joint training — they just predict
        different targets derived from the q1-q4 / ot columns where available.
        """
        sport = self.config.sport
        models: dict[str, EnsembleVoter] = {}
        meta: dict[str, Any] = {}

        # Skip all extra markets for sports with no subgame structure
        if sport in _NO_EXTRA_MARKETS:
            logger.info("Skipping extra-market training for %s (no subgame structure)", sport)
            meta["period_label"] = "period"
            meta["available_periods"] = []
            return {"models": models, "feature_names": list(X_train.columns),
                    "sport": sport, "sport_meta": meta, "config": self.config,
                    "trained_at": datetime.utcnow().isoformat()}

        # Motorsport: podium / points-finish / DNF / fastest-lap markets
        if sport in _MOTORSPORT_SPORTS:
            self._train_motorsport_markets(train_df, val_df, X_train, X_val, models)
            meta["period_label"] = "lap"
            meta["available_periods"] = []
            return {"models": models, "feature_names": list(X_train.columns),
                    "sport": sport, "sport_meta": meta, "config": self.config,
                    "trained_at": datetime.utcnow().isoformat()}

        home_train = train_df["home_score"] if "home_score" in train_df.columns else pd.Series(dtype=float)
        away_train = train_df["away_score"] if "away_score" in train_df.columns else pd.Series(dtype=float)
        home_val = val_df["home_score"] if "home_score" in val_df.columns else pd.Series(dtype=float)
        away_val = val_df["away_score"] if "away_score" in val_df.columns else pd.Series(dtype=float)

        # ── 1. Draw / Tie probability ─────────────────────
        if sport in _DRAW_SPORTS:
            logger.info("Training draw probability model …")
            y_draw_tr = (home_train == away_train).astype(int)
            y_draw_va = (home_val == away_val).astype(int)
            m = self._fit_cls_safe("draw", X_train, y_draw_tr, X_val, y_draw_va)
            if m:
                models["draw"] = m
                draw_rate = float(y_draw_tr.mean())
                meta["draw_rate"] = draw_rate
                logger.info("  draw model fitted (draw rate=%.1f%%)", 100 * draw_rate)

        # ── 2. OT probability ─────────────────────────────
        if sport in _OT_SPORTS and "home_ot" in train_df.columns and "away_ot" in val_df.columns:
            ot_tr_raw = train_df["home_ot"].fillna(0) + train_df["away_ot"].fillna(0)
            ot_va_raw = val_df["home_ot"].fillna(0) + val_df["away_ot"].fillna(0)
            y_ot_tr = (ot_tr_raw > 0).astype(int)
            y_ot_va = (ot_va_raw > 0).astype(int)
            ot_rate = float(y_ot_tr.mean())
            if ot_rate > 0.02:  # at least 2% OT rate
                logger.info("Training OT model (OT rate=%.1f%%) …", 100 * ot_rate)
                m = self._fit_cls_safe("overtime", X_train, y_ot_tr, X_val, y_ot_va)
                if m:
                    models["overtime"] = m
                    meta["ot_rate"] = ot_rate

        # ── 3. Halftime models ────────────────────────────
        # Soccer uses home_h1/home_h2; other sports use home_q1/home_q2
        if sport in _SOCCER_HALF_SPORTS:
            half_col1_h, half_col1_a = "home_h1", "away_h1"
            half_col2_h, half_col2_a = "home_h2", "away_h2"
        else:
            half_col1_h, half_col1_a = "home_q1", "away_q1"
            half_col2_h, half_col2_a = "home_q2", "away_q2"

        # Two-half sports (soccer, NCAAB, NCAAW, WNBA): halftime = first half only
        # Four-quarter sports (NBA, NFL, NCAAF): halftime = Q1+Q2
        is_two_half = sport in _TWO_HALF_SPORTS

        q_cols_avail = [c for c in [half_col1_h, half_col1_a] if c in train_df.columns]
        if len(q_cols_avail) == 2 and sport in _HALFTIME_SPORTS:
            # Only use rows where first-half data is actually populated AND non-zero
            half_mask_tr = train_df[half_col1_h].notna() & (train_df[half_col1_h] != 0)
            half_mask_va = val_df[half_col1_h].notna() & (val_df[half_col1_h] != 0)
            if not is_two_half:
                # Four-quarter sports: also require Q2 data
                if half_col2_h in train_df.columns:
                    half_mask_tr &= train_df[half_col2_h].notna()
                    half_mask_va &= val_df[half_col2_h].notna()
            n_ht_tr = int(half_mask_tr.sum())
            n_ht_va = int(half_mask_va.sum())

            if n_ht_tr >= 80 and n_ht_va >= 20:
                logger.info("Training halftime models (%d/%d rows with half data) …", n_ht_tr, n_ht_va)
                if is_two_half:
                    # Two-half sports: halftime score = H1 goals/points only
                    hh_tr = train_df.loc[half_mask_tr, half_col1_h].fillna(0)
                    ah_tr = train_df.loc[half_mask_tr, half_col1_a].fillna(0)
                    hh_va = val_df.loc[half_mask_va, half_col1_h].fillna(0)
                    ah_va = val_df.loc[half_mask_va, half_col1_a].fillna(0)
                else:
                    # Four-quarter sports: halftime = Q1+Q2
                    hh_tr = train_df.loc[half_mask_tr, half_col1_h].fillna(0) + train_df.loc[half_mask_tr, half_col2_h].fillna(0)
                    ah_tr = train_df.loc[half_mask_tr, half_col1_a].fillna(0) + train_df.loc[half_mask_tr, half_col2_a].fillna(0)
                    hh_va = val_df.loc[half_mask_va, half_col1_h].fillna(0) + val_df.loc[half_mask_va, half_col2_h].fillna(0)
                    ah_va = val_df.loc[half_mask_va, half_col1_a].fillna(0) + val_df.loc[half_mask_va, half_col2_a].fillna(0)

                Xht_tr = X_train.loc[half_mask_tr]
                Xht_va = X_val.loc[half_mask_va]

                # Halftime score regressors
                m = self._fit_reg_safe("halftime_home_score", Xht_tr, hh_tr, Xht_va, hh_va)
                if m:
                    models["halftime_home_score"] = m
                m = self._fit_reg_safe("halftime_away_score", Xht_tr, ah_tr, Xht_va, ah_va)
                if m:
                    models["halftime_away_score"] = m

                # Halftime spread regressor
                m = self._fit_reg_safe("halftime_spread", Xht_tr, hh_tr - ah_tr, Xht_va, hh_va - ah_va)
                if m:
                    models["halftime_spread"] = m

                # Halftime total regressor
                m = self._fit_reg_safe("halftime_total", Xht_tr, hh_tr + ah_tr, Xht_va, hh_va + ah_va)
                if m:
                    models["halftime_total"] = m

                # Halftime winner classifier (exclude draws)
                ht_nodraw_tr = hh_tr != ah_tr
                ht_nodraw_va = hh_va != ah_va
                if ht_nodraw_tr.sum() >= 80 and ht_nodraw_va.sum() >= 20:
                    y_ht_tr = (hh_tr[ht_nodraw_tr] > ah_tr[ht_nodraw_tr]).astype(int)
                    y_ht_va = (hh_va[ht_nodraw_va] > ah_va[ht_nodraw_va]).astype(int)
                    m = self._fit_cls_safe(
                        "halftime_winner",
                        Xht_tr.loc[ht_nodraw_tr.index[ht_nodraw_tr]],
                        y_ht_tr,
                        Xht_va.loc[ht_nodraw_va.index[ht_nodraw_va]],
                        y_ht_va,
                    )
                    if m:
                        models["halftime_winner"] = m

                logger.info("  halftime models: %s", [k for k in models if "halftime" in k])

        # ── 4a. Esports-specific map win models ──────────
        if sport in _ESPORTS_SPORTS:
            self._train_esports_markets(train_df, val_df, X_train, X_val, models)

        # ── 4. Per-period / quarter / inning models ───────
        period_label = _PERIOD_LABEL.get(sport, "period")
        available_periods: list[int] = []
        if sport in _PERIOD_SPORTS:
            # Determine the period column prefix based on sport
            if sport == "nhl":
                period_prefixes = ["p1", "p2", "p3"]
            elif sport == "mlb":
                period_prefixes = [f"i{i}" for i in range(1, 10)]
            else:
                period_prefixes = ["q1", "q2", "q3", "q4"]

            period_cols_tr = [
                c for c in period_prefixes
                if f"home_{c}" in train_df.columns and f"away_{c}" in train_df.columns
            ]
            for i, qname in enumerate(period_cols_tr, start=1):
                hcol, acol = f"home_{qname}", f"away_{qname}"
                mask_tr = train_df[hcol].notna() & (train_df[hcol] != 0)
                mask_va = val_df[hcol].notna() & (val_df[hcol] != 0)
                n_tr = int(mask_tr.sum())
                n_va = int(mask_va.sum())
                if n_tr < 80 or n_va < 20:
                    continue
                # Skip if target has no variance (all-zero/constant data = missing)
                h_check = train_df.loc[mask_tr, hcol]
                if h_check.nunique() <= 1:
                    logger.debug("Skipping period %d (%s) — constant target", i, qname)
                    continue

                logger.info("Training period %d (%s) models (%d rows) …", i, qname, n_tr)
                h_tr_p = train_df.loc[mask_tr, hcol].fillna(0)
                a_tr_p = train_df.loc[mask_tr, acol].fillna(0)
                h_va_p = val_df.loc[mask_va, hcol].fillna(0)
                a_va_p = val_df.loc[mask_va, acol].fillna(0)
                Xp_tr = X_train.loc[mask_tr]
                Xp_va = X_val.loc[mask_va]

                m = self._fit_reg_safe(f"period_{i}_home", Xp_tr, h_tr_p, Xp_va, h_va_p)
                if m:
                    models[f"period_{i}_home"] = m
                m = self._fit_reg_safe(f"period_{i}_away", Xp_tr, a_tr_p, Xp_va, a_va_p)
                if m:
                    models[f"period_{i}_away"] = m
                m = self._fit_reg_safe(f"period_{i}_total", Xp_tr, h_tr_p + a_tr_p, Xp_va, h_va_p + a_va_p)
                if m:
                    models[f"period_{i}_total"] = m

                # Period winner classifier (exclude draws within the period)
                pd_nodraw_tr = h_tr_p != a_tr_p
                pd_nodraw_va = h_va_p != a_va_p
                if pd_nodraw_tr.sum() >= 80 and pd_nodraw_va.sum() >= 20:
                    y_pd_tr = (h_tr_p[pd_nodraw_tr] > a_tr_p[pd_nodraw_tr]).astype(int)
                    y_pd_va = (h_va_p[pd_nodraw_va] > a_va_p[pd_nodraw_va]).astype(int)
                    m = self._fit_cls_safe(
                        f"period_{i}_winner",
                        Xp_tr.loc[pd_nodraw_tr.index[pd_nodraw_tr]],
                        y_pd_tr,
                        Xp_va.loc[pd_nodraw_va.index[pd_nodraw_va]],
                        y_pd_va,
                    )
                    if m:
                        models[f"period_{i}_winner"] = m

                available_periods.append(i)

            if available_periods:
                logger.info("  period models trained for periods: %s", available_periods)

        meta["period_label"] = period_label
        meta["available_periods"] = available_periods

        # ── 5. First-score model ─────────────────────────
        # Determine first period column for this sport
        if sport == "nhl":
            first_period_col = "p1"
        elif sport == "mlb":
            first_period_col = "i1"
        elif sport in _SOCCER_HALF_SPORTS:
            first_period_col = "h1"
        else:
            first_period_col = "q1"
        fp_h, fp_a = f"home_{first_period_col}", f"away_{first_period_col}"
        if (sport in _PERIOD_SPORTS or sport in _SOCCER_HALF_SPORTS) and fp_h in train_df.columns and fp_a in train_df.columns:
            fs_mask_tr = train_df[fp_h].notna() & (train_df[fp_h].fillna(0) + train_df[fp_a].fillna(0) > 0)
            fs_mask_va = val_df[fp_h].notna() & (val_df[fp_h].fillna(0) + val_df[fp_a].fillna(0) > 0)
            n_fs_tr = int(fs_mask_tr.sum())
            n_fs_va = int(fs_mask_va.sum())
            if n_fs_tr >= 80 and n_fs_va >= 20:
                logger.info("Training first-score model (%d rows) …", n_fs_tr)
                y_fs_tr = (train_df.loc[fs_mask_tr, fp_h].fillna(0) > train_df.loc[fs_mask_tr, fp_a].fillna(0)).astype(int)
                y_fs_va = (val_df.loc[fs_mask_va, fp_h].fillna(0) > val_df.loc[fs_mask_va, fp_a].fillna(0)).astype(int)
                m = self._fit_cls_safe("first_score", X_train.loc[fs_mask_tr], y_fs_tr, X_val.loc[fs_mask_va], y_fs_va)
                if m:
                    models["first_score"] = m
                # Last score: try final period col
                if sport == "nhl":
                    last_period_candidates = ["p3", "p2", "p1"]
                elif sport == "mlb":
                    last_period_candidates = [f"i{i}" for i in range(9, 0, -1)]
                elif sport in _SOCCER_HALF_SPORTS:
                    last_period_candidates = ["h2", "h1"]
                else:
                    last_period_candidates = ["q4", "q3", "q2", "q1"]
                last_q = None
                for qn in last_period_candidates:
                    if f"home_{qn}" in train_df.columns and train_df[f"home_{qn}"].notna().mean() > 0.05:
                        last_q = qn
                        break
                if last_q:
                    ls_mask_tr = train_df[f"home_{last_q}"].notna() & (train_df[f"home_{last_q}"].fillna(0) + train_df[f"away_{last_q}"].fillna(0) > 0)
                    ls_mask_va = val_df[f"home_{last_q}"].notna() & (val_df[f"home_{last_q}"].fillna(0) + val_df[f"away_{last_q}"].fillna(0) > 0)
                    if ls_mask_tr.sum() >= 80 and ls_mask_va.sum() >= 20:
                        y_ls_tr = (train_df.loc[ls_mask_tr, f"home_{last_q}"].fillna(0) > train_df.loc[ls_mask_tr, f"away_{last_q}"].fillna(0)).astype(int)
                        y_ls_va = (val_df.loc[ls_mask_va, f"home_{last_q}"].fillna(0) > val_df.loc[ls_mask_va, f"away_{last_q}"].fillna(0)).astype(int)
                        m = self._fit_cls_safe("last_score", X_train.loc[ls_mask_tr], y_ls_tr, X_val.loc[ls_mask_va], y_ls_va)
                        if m:
                            models["last_score"] = m

        # ── 6. BTTS / Clean Sheet ─────────────────────────────
        if sport in _SOCCER_SPORTS:
            self._train_btts_clean_sheet(train_df, val_df, X_train, X_val, models)

        # ── 7. UFC Method of Victory ──────────────────────────
        if sport == "ufc":
            self._train_ufc_method(train_df, val_df, X_train, X_val, models)

        # ── 8. Tennis Straight Sets ───────────────────────────
        if sport in ("atp", "wta"):
            self._train_tennis_sets(train_df, val_df, X_train, X_val, models)

        # ── 9. Winning Margin Bands ───────────────────────────
        if sport in _MARGIN_SPORTS:
            self._train_margin_bands(train_df, val_df, X_train, X_val, models)

        # ── 10. Total Score Bands ─────────────────────────────
        if sport in _MARGIN_SPORTS:
            self._train_total_bands(train_df, val_df, X_train, X_val, models)

        # ── 11. Second Half Winner ────────────────────────────
        if sport in _SECOND_HALF_SPORTS:
            self._train_second_half_winner(train_df, val_df, X_train, X_val, models)

        # ── 12. Regulation Winner (pre-OT result) ─────────────
        if sport in _OT_SPORTS:
            self._train_regulation_winner(train_df, val_df, X_train, X_val, models)

        # ── 13. Team Totals (home/away over median) ───────────
        if sport in _MARGIN_SPORTS:
            self._train_team_totals(train_df, val_df, X_train, X_val, models)

        # ── 14. Large Margin / Dominant Win ──────────────────
        if sport in _MARGIN_SPORTS:
            self._train_dominant_win(train_df, val_df, X_train, X_val, models)

        # ── 15. Comeback Win (trailing at half → wins) ────────
        if sport in _SECOND_HALF_SPORTS:
            self._train_comeback(train_df, val_df, X_train, X_val, models)

        # ── 16. Double Chance (1X, X2, 12) ────────────────────
        if sport in _DOUBLE_CHANCE_SPORTS:
            self._train_double_chance(train_df, val_df, X_train, X_val, models)

        # ── 17. NRFI / YRFI (MLB: no run / yes run first inning)
        if sport in _NRFI_SPORTS:
            self._train_nrfi(train_df, val_df, X_train, X_val, models)

        # ── 18. Shutout / Clean Sheet (extended to hockey) ────
        if sport in _SHUTOUT_SPORTS and sport not in _SOCCER_SPORTS:
            self._train_shutout(train_df, val_df, X_train, X_val, models)

        # ── 19. Asian Handicap Lines (−1, −1.5) ───────────────
        if sport in _ASIAN_HANDICAP_SPORTS:
            self._train_asian_handicap(train_df, val_df, X_train, X_val, models)

        # ── 20. First 5 Innings (F5) — MLB only ──────────────
        if sport == "mlb":
            self._train_f5_innings(train_df, val_df, X_train, X_val, models)

        # ── 20b. Per-Inning (NRFI×9, F7, Late Innings) — MLB only ──
        if sport == "mlb":
            self._train_mlb_per_inning_markets(train_df, val_df, X_train, X_val, models)

        # ── 21. Correct Score Bands — Soccer ─────────────────
        if sport in _SOCCER_SPORTS:
            self._train_correct_score_bands(train_df, val_df, X_train, X_val, models)

        # ── 22. First Half Over/Under + Win Both Halves — Soccer ─
        if sport in _SOCCER_HALF_SPORTS:
            self._train_first_half_markets(train_df, val_df, X_train, X_val, models)

        # ── 23. Period BTTS (hockey/basketball) ──────────────
        if sport in _PERIOD_SPORTS:
            self._train_period_btts(train_df, val_df, X_train, X_val, models)

        # ── 24. Corners Total Markets — Soccer ───────────────
        if sport in _SOCCER_SPORTS:
            self._train_corners_market(train_df, val_df, X_train, X_val, models)

        # ── 25. Cards Total Markets — Soccer ─────────────────
        if sport in _SOCCER_SPORTS:
            self._train_cards_market(train_df, val_df, X_train, X_val, models)

        # ── 26. NBA/WNBA/NCAAB Three-Pointer O/U Market ──────
        if sport in _THREE_POINTER_SPORTS:
            self._train_three_pointer_market(train_df, val_df, X_train, X_val, models)

        # ── 27. NHL Total Shots O/U Market ───────────────────
        if sport in _SHOTS_MARKET_SPORTS:
            self._train_shots_market(train_df, val_df, X_train, X_val, models)

        # ── 28. MLB Total Hits O/U Market ────────────────────
        if sport in _HITS_MARKET_SPORTS:
            self._train_hits_market(train_df, val_df, X_train, X_val, models)

        # ── 29. Soccer Second-Half Goals O/U Market ──────────
        if sport in _SOCCER_H2_SPORTS:
            self._train_soccer_h2_market(train_df, val_df, X_train, X_val, models)

        # ── 30. NBA/WNBA Total Rebounds O/U Market ───────────
        if sport in _REBOUNDS_MARKET_SPORTS:
            self._train_rebounds_market(train_df, val_df, X_train, X_val, models)

        # ── 31. NBA/WNBA Total Turnovers O/U Market ──────────
        if sport in _TURNOVERS_MARKET_SPORTS:
            self._train_turnovers_market(train_df, val_df, X_train, X_val, models)

        # ── 32. NBA/WNBA Total Assists O/U Market ────────────
        if sport in _ASSISTS_MARKET_SPORTS:
            self._train_assists_market(train_df, val_df, X_train, X_val, models)

        # ── 33. NHL Period Goals O/U Market ──────────────────
        if sport in _NHL_PERIOD_GOALS_SPORTS:
            self._train_nhl_period_goals_market(train_df, val_df, X_train, X_val, models)

        # ── 34. Soccer Total Shots O/U Market ────────────────
        if sport in _SOCCER_SHOTS_TOTAL_SPORTS:
            self._train_soccer_total_shots_market(train_df, val_df, X_train, X_val, models)

        # ── 35. UFC Total Rounds O/U (Fight Distance) ─────────
        if sport in _UFC_ROUNDS_SPORTS:
            self._train_ufc_rounds_market(train_df, val_df, X_train, X_val, models)

        # ── 36. NFL/NCAAF First Quarter Total O/U ─────────────
        if sport in _NFL_Q1_MARKET_SPORTS:
            self._train_nfl_q1_total_market(train_df, val_df, X_train, X_val, models)

        # ── 37. Soccer BTTS in Both Halves ────────────────────
        if sport in _BTTS_BOTH_HALVES_SPORTS:
            self._train_btts_both_halves(train_df, val_df, X_train, X_val, models)

        # ── 38. HT/FT Double Result ───────────────────────────
        if sport in _HT_FT_SPORTS:
            self._train_htft_double_result(train_df, val_df, X_train, X_val, models)

        # ── 39. Q4 / Last-Quarter Total O/U ──────────────────
        if sport in _Q4_TOTAL_SPORTS:
            self._train_q4_total_market(train_df, val_df, X_train, X_val, models)

        # ── 40. NFL/NCAAF Total Turnovers O/U ────────────────
        if sport in _NFL_TURNOVERS_SPORTS:
            self._train_nfl_turnovers_market(train_df, val_df, X_train, X_val, models)

        # ── 41. NBA/WNBA/NCAAB Close Game & Blowout ───────────
        if sport in {"nba", "wnba", "ncaab", "ncaaw", "nfl", "ncaaf"}:
            self._train_close_game_market(train_df, val_df, X_train, X_val, models)

        # ── 42. NFL/NCAAF Second Half Total O/U ──────────────
        if sport in {"nfl", "ncaaf"}:
            self._train_nfl_second_half_total(train_df, val_df, X_train, X_val, models)

        # ── 43. NHL/Basketball High-Scoring Game ─────────────
        if sport in {"nhl", "nba", "wnba", "ncaab"}:
            self._train_high_low_scoring(train_df, val_df, X_train, X_val, models)

        # ── 44. NBA/WNBA/NHL Overtime Probability ─────────────
        if sport in {"nba", "wnba", "nhl"}:
            self._train_ot_market(train_df, val_df, X_train, X_val, models)

        # ── 45. NBA/WNBA/NCAAB First Quarter Winner + Total ───
        if sport in {"nba", "wnba", "ncaab", "ncaaw"}:
            self._train_q1_market(train_df, val_df, X_train, X_val, models)

        return {
            "models": models,
            "feature_names": list(X_train.columns),
            "sport": sport,
            "sport_meta": meta,
            "config": self.config,
            "trained_at": datetime.utcnow().isoformat(),
        }

    def _train_btts_clean_sheet(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train BTTS (both teams score) and clean-sheet models.

        Uses home_score / away_score directly — valid whenever total score > 0.
        BTTS: both_teams_score = (home_score > 0) AND (away_score > 0)
        Clean sheet home: away_score == 0
        Clean sheet away: home_score == 0
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_tr = train_df.get("away_score", pd.Series(dtype=float)).fillna(0)
        hs_va = val_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_va = val_df.get("away_score", pd.Series(dtype=float)).fillna(0)

        # Both teams must have valid score data
        valid_tr = (hs_tr + as_tr > 0)
        valid_va = (hs_va + as_va > 0)
        n_tr = int(valid_tr.sum())
        n_va = int(valid_va.sum())
        if n_tr < 80 or n_va < 20:
            return

        logger.info("Training BTTS/clean-sheet models (%d/%d rows) …", n_tr, n_va)

        # BTTS
        y_btts_tr = ((hs_tr[valid_tr] > 0) & (as_tr[valid_tr] > 0)).astype(int)
        y_btts_va = ((hs_va[valid_va] > 0) & (as_va[valid_va] > 0)).astype(int)
        m = self._fit_cls_safe("btts", X_train.loc[valid_tr], y_btts_tr, X_val.loc[valid_va], y_btts_va)
        if m:
            models["btts"] = m
            logger.info("  btts fitted (btts_rate=%.1f%%)", 100 * float(y_btts_tr.mean()))

        # Clean sheet home (away team scores 0)
        y_cs_h_tr = (as_tr[valid_tr] == 0).astype(int)
        y_cs_h_va = (as_va[valid_va] == 0).astype(int)
        m = self._fit_cls_safe("clean_sheet_home", X_train.loc[valid_tr], y_cs_h_tr, X_val.loc[valid_va], y_cs_h_va)
        if m:
            models["clean_sheet_home"] = m

        # Clean sheet away (home team scores 0)
        y_cs_a_tr = (hs_tr[valid_tr] == 0).astype(int)
        y_cs_a_va = (hs_va[valid_va] == 0).astype(int)
        m = self._fit_cls_safe("clean_sheet_away", X_train.loc[valid_tr], y_cs_a_tr, X_val.loc[valid_va], y_cs_a_va)
        if m:
            models["clean_sheet_away"] = m

        # BTTS + Over 2.5
        y_btts_ov_tr = (((hs_tr[valid_tr] > 0) & (as_tr[valid_tr] > 0)) & ((hs_tr[valid_tr] + as_tr[valid_tr]) > 2.5)).astype(int)
        y_btts_ov_va = (((hs_va[valid_va] > 0) & (as_va[valid_va] > 0)) & ((hs_va[valid_va] + as_va[valid_va]) > 2.5)).astype(int)
        if y_btts_ov_tr.sum() >= 40:
            m = self._fit_cls_safe("btts_over2_5", X_train.loc[valid_tr], y_btts_ov_tr, X_val.loc[valid_va], y_btts_ov_va)
            if m:
                models["btts_over2_5"] = m
                logger.info("  btts_over2_5 fitted (rate=%.1f%%)", 100 * float(y_btts_ov_tr.mean()))

        # Home over 1.5 goals (home scores 2+)
        y_h15_tr = (hs_tr[valid_tr] >= 2).astype(int)
        y_h15_va = (hs_va[valid_va] >= 2).astype(int)
        m = self._fit_cls_safe("home_over1_5", X_train.loc[valid_tr], y_h15_tr, X_val.loc[valid_va], y_h15_va)
        if m:
            models["home_over1_5"] = m

        # Away over 1.5 goals (away scores 2+)
        y_a15_tr = (as_tr[valid_tr] >= 2).astype(int)
        y_a15_va = (as_va[valid_va] >= 2).astype(int)
        m = self._fit_cls_safe("away_over1_5", X_train.loc[valid_tr], y_a15_tr, X_val.loc[valid_va], y_a15_va)
        if m:
            models["away_over1_5"] = m

        # Win and score 2+ (home dominant win with goals)
        y_h2w_tr = ((hs_tr[valid_tr] >= 2) & (hs_tr[valid_tr] > as_tr[valid_tr])).astype(int)
        y_h2w_va = ((hs_va[valid_va] >= 2) & (hs_va[valid_va] > as_va[valid_va])).astype(int)
        if y_h2w_tr.sum() >= 40:
            m = self._fit_cls_safe("home_win_score2plus", X_train.loc[valid_tr], y_h2w_tr, X_val.loc[valid_va], y_h2w_va)
            if m:
                models["home_win_score2plus"] = m

        # Away win and score 2+
        y_a2w_tr = ((as_tr[valid_tr] >= 2) & (as_tr[valid_tr] > hs_tr[valid_tr])).astype(int)
        y_a2w_va = ((as_va[valid_va] >= 2) & (as_va[valid_va] > hs_va[valid_va])).astype(int)
        if y_a2w_tr.sum() >= 40:
            m = self._fit_cls_safe("away_win_score2plus", X_train.loc[valid_tr], y_a2w_tr, X_val.loc[valid_va], y_a2w_va)
            if m:
                models["away_win_score2plus"] = m

        # BTTS + Home Win (popular 'GG and Win' market)
        y_btts_hw_tr = (y_btts_tr & (hs_tr[valid_tr] > as_tr[valid_tr])).astype(int)
        y_btts_hw_va = (y_btts_va & (hs_va[valid_va] > as_va[valid_va])).astype(int)
        if y_btts_hw_tr.sum() >= 40:
            m = self._fit_cls_safe("btts_home_win", X_train.loc[valid_tr], y_btts_hw_tr, X_val.loc[valid_va], y_btts_hw_va)
            if m:
                models["btts_home_win"] = m
                logger.info("  btts_home_win fitted (rate=%.1f%%)", 100 * float(y_btts_hw_tr.mean()))

        # BTTS + Away Win
        y_btts_aw_tr = (y_btts_tr & (as_tr[valid_tr] > hs_tr[valid_tr])).astype(int)
        y_btts_aw_va = (y_btts_va & (as_va[valid_va] > hs_va[valid_va])).astype(int)
        if y_btts_aw_tr.sum() >= 40:
            m = self._fit_cls_safe("btts_away_win", X_train.loc[valid_tr], y_btts_aw_tr, X_val.loc[valid_va], y_btts_aw_va)
            if m:
                models["btts_away_win"] = m
                logger.info("  btts_away_win fitted (rate=%.1f%%)", 100 * float(y_btts_aw_tr.mean()))

        # BTTS + Draw
        y_btts_dr_tr = (y_btts_tr & (hs_tr[valid_tr] == as_tr[valid_tr])).astype(int)
        y_btts_dr_va = (y_btts_va & (hs_va[valid_va] == as_va[valid_va])).astype(int)
        if y_btts_dr_tr.sum() >= 40:
            m = self._fit_cls_safe("btts_draw", X_train.loc[valid_tr], y_btts_dr_tr, X_val.loc[valid_va], y_btts_dr_va)
            if m:
                models["btts_draw"] = m
                logger.info("  btts_draw fitted (rate=%.1f%%)", 100 * float(y_btts_dr_tr.mean()))

    def _train_ufc_method(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train UFC/combat method-of-victory and round prediction models.

        Uses home_ot column as method proxy if present:
          home_ot == 1 → KO/TKO finish
          home_ot == 2 → Submission finish
          home_ot == 0 → Decision
        Falls back to home_finish_round / home_knockdowns / home_submission_attempts
        columns if home_ot is absent (which is now the common case with the UFC dataset).
        """
        sport = self.config.sport
        if sport not in ("ufc",):
            return

        hs_tr = train_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_tr = train_df.get("away_score", pd.Series(dtype=float)).fillna(0)
        hs_va = val_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_va = val_df.get("away_score", pd.Series(dtype=float)).fillna(0)

        valid_tr = (hs_tr + as_tr > 0)
        valid_va = (hs_va + as_va > 0)
        n_tr = int(valid_tr.sum())
        n_va = int(valid_va.sum())
        if n_tr < 80 or n_va < 20:
            return

        logger.info("Training UFC method/round models (%d/%d rows) …", n_tr, n_va)

        # home_ot column encodes method if provider populates it: 0=decision,1=KO,2=sub
        if "home_ot" in train_df.columns and train_df["home_ot"].notna().mean() > 0.5:
            method_tr = train_df.loc[valid_tr, "home_ot"].fillna(0).astype(int)
            method_va = val_df.loc[valid_va, "home_ot"].fillna(0).astype(int)

            # decision vs finish
            y_dec_tr = (method_tr == 0).astype(int)
            y_dec_va = (method_va == 0).astype(int)
            m = self._fit_cls_safe("ufc_decision", X_train.loc[valid_tr], y_dec_tr, X_val.loc[valid_va], y_dec_va)
            if m:
                models["ufc_decision"] = m

            # KO/TKO
            y_ko_tr = (method_tr == 1).astype(int)
            y_ko_va = (method_va == 1).astype(int)
            if y_ko_tr.sum() >= 40:
                m = self._fit_cls_safe("ufc_ko_tko", X_train.loc[valid_tr], y_ko_tr, X_val.loc[valid_va], y_ko_va)
                if m:
                    models["ufc_ko_tko"] = m

            # Submission
            y_sub_tr = (method_tr == 2).astype(int)
            y_sub_va = (method_va == 2).astype(int)
            if y_sub_tr.sum() >= 40:
                m = self._fit_cls_safe("ufc_submission", X_train.loc[valid_tr], y_sub_tr, X_val.loc[valid_va], y_sub_va)
                if m:
                    models["ufc_submission"] = m
        else:
            # Infer method from home_knockdowns, home_submission_attempts, home_finish_round
            has_finish_data = (
                "home_finish_round" in train_df.columns and
                train_df["home_finish_round"].notna().mean() > 0.5
            )
            if has_finish_data:
                # Goes to decision: fight ends in final round (3 for non-championship, 5 for championship)
                # Use max finish_round across dataset as proxy for scheduled rounds
                max_round = int(pd.to_numeric(train_df["home_finish_round"], errors="coerce").fillna(3).max())
                scheduled_rounds = max(3, min(max_round, 5))

                fr_tr = pd.to_numeric(train_df.loc[valid_tr, "home_finish_round"], errors="coerce").fillna(scheduled_rounds)
                fr_va = pd.to_numeric(val_df.loc[valid_va, "home_finish_round"], errors="coerce").fillna(scheduled_rounds)

                # Decision: fight goes the full scheduled rounds
                y_dec_tr = (fr_tr >= scheduled_rounds).astype(int)
                y_dec_va = (fr_va >= scheduled_rounds).astype(int)
                if y_dec_tr.sum() >= 40 and y_dec_va.sum() >= 5:
                    m = self._fit_cls_safe("ufc_decision", X_train.loc[valid_tr], y_dec_tr, X_val.loc[valid_va], y_dec_va)
                    if m:
                        models["ufc_decision"] = m

                # Early finish (rounds 1 or 2)
                y_early_tr = (fr_tr <= 2).astype(int)
                y_early_va = (fr_va <= 2).astype(int)
                if y_early_tr.sum() >= 40 and y_early_va.sum() >= 5:
                    m = self._fit_cls_safe("ufc_early_finish", X_train.loc[valid_tr], y_early_tr, X_val.loc[valid_va], y_early_va)
                    if m:
                        models["ufc_early_finish"] = m

                # Round 1 finish specifically (high value prop market)
                y_r1_tr = (fr_tr == 1).astype(int)
                y_r1_va = (fr_va == 1).astype(int)
                if y_r1_tr.sum() >= 30 and y_r1_va.sum() >= 5:
                    m = self._fit_cls_safe("ufc_round1_finish", X_train.loc[valid_tr], y_r1_tr, X_val.loc[valid_va], y_r1_va)
                    if m:
                        models["ufc_round1_finish"] = m

            # KO/TKO model: infer from home_knockdowns > 0
            if "home_knockdowns" in train_df.columns:
                h_kd_tr = pd.to_numeric(train_df.loc[valid_tr, "home_knockdowns"], errors="coerce").fillna(0)
                a_kd_tr = pd.to_numeric(train_df.loc[valid_tr, "away_knockdowns"], errors="coerce").fillna(0)
                h_kd_va = pd.to_numeric(val_df.loc[valid_va, "home_knockdowns"], errors="coerce").fillna(0)
                a_kd_va = pd.to_numeric(val_df.loc[valid_va, "away_knockdowns"], errors="coerce").fillna(0)
                y_ko_tr = ((h_kd_tr + a_kd_tr) > 0).astype(int)
                y_ko_va = ((h_kd_va + a_kd_va) > 0).astype(int)
                if y_ko_tr.sum() >= 40 and y_ko_va.sum() >= 5:
                    m = self._fit_cls_safe("ufc_ko_tko", X_train.loc[valid_tr], y_ko_tr, X_val.loc[valid_va], y_ko_va)
                    if m:
                        models["ufc_ko_tko"] = m

            # Submission model: home_submission_attempts > 0 (proxy for sub finish)
            if "home_submission_attempts" in train_df.columns:
                h_sub_tr = pd.to_numeric(train_df.loc[valid_tr, "home_submission_attempts"], errors="coerce").fillna(0)
                a_sub_tr = pd.to_numeric(train_df.loc[valid_tr, "away_submission_attempts"], errors="coerce").fillna(0)
                h_sub_va = pd.to_numeric(val_df.loc[valid_va, "home_submission_attempts"], errors="coerce").fillna(0)
                a_sub_va = pd.to_numeric(val_df.loc[valid_va, "away_submission_attempts"], errors="coerce").fillna(0)
                y_sub_tr = ((h_sub_tr + a_sub_tr) >= 3).astype(int)  # ≥3 attempts = grappling-heavy fight
                y_sub_va = ((h_sub_va + a_sub_va) >= 3).astype(int)
                if y_sub_tr.sum() >= 30 and y_sub_va.sum() >= 5:
                    m = self._fit_cls_safe("ufc_submission", X_train.loc[valid_tr], y_sub_tr, X_val.loc[valid_va], y_sub_va)
                    if m:
                        models["ufc_submission"] = m

    def _train_tennis_sets(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train tennis straight-sets probability model.

        Uses home_q1/home_q2 as set-score proxies (each q = one set).
        Straight sets = winner took 2 sets without dropping one (q1+q2 = 2-0).
        """
        sport = self.config.sport
        if sport not in ("atp", "wta"):
            return

        # Check if we have set-level data
        q_avail = all(c in train_df.columns for c in ["home_q1", "away_q1"])
        if not q_avail:
            return

        valid_tr = train_df["home_q1"].notna()
        valid_va = val_df["home_q1"].notna()
        n_tr = int(valid_tr.sum())
        n_va = int(valid_va.sum())
        if n_tr < 80 or n_va < 20:
            return

        logger.info("Training tennis straight-sets model (%d/%d rows) …", n_tr, n_va)
        # home_q1 and home_q2 are set wins (typically 0 or 1 per set)
        hq1_tr = train_df.loc[valid_tr, "home_q1"].fillna(0)
        aq1_tr = train_df.loc[valid_tr, "away_q1"].fillna(0)
        hq2_tr = train_df.loc[valid_tr, "home_q2"].fillna(0) if "home_q2" in train_df.columns else pd.Series(0, index=hq1_tr.index)
        aq2_tr = train_df.loc[valid_tr, "away_q2"].fillna(0) if "away_q2" in train_df.columns else pd.Series(0, index=aq1_tr.index)

        hq1_va = val_df.loc[valid_va, "home_q1"].fillna(0)
        aq1_va = val_df.loc[valid_va, "away_q1"].fillna(0)
        hq2_va = val_df.loc[valid_va, "home_q2"].fillna(0) if "home_q2" in val_df.columns else pd.Series(0, index=hq1_va.index)
        aq2_va = val_df.loc[valid_va, "away_q2"].fillna(0) if "away_q2" in val_df.columns else pd.Series(0, index=aq1_va.index)

        # Straight sets: winner won both set 1 and set 2 (2-0)
        home_wins_tr = train_df.loc[valid_tr, "home_score"].fillna(0) > train_df.loc[valid_tr, "away_score"].fillna(0)
        y_ss_tr = (
            (home_wins_tr & (hq1_tr > aq1_tr) & (hq2_tr > aq2_tr)) |
            (~home_wins_tr & (aq1_tr > hq1_tr) & (aq2_tr > hq2_tr))
        ).astype(int)

        home_wins_va = val_df.loc[valid_va, "home_score"].fillna(0) > val_df.loc[valid_va, "away_score"].fillna(0)
        y_ss_va = (
            (home_wins_va & (hq1_va > aq1_va) & (hq2_va > aq2_va)) |
            (~home_wins_va & (aq1_va > hq1_va) & (aq2_va > hq2_va))
        ).astype(int)

        m = self._fit_cls_safe("straight_sets", X_train.loc[valid_tr], y_ss_tr, X_val.loc[valid_va], y_ss_va)
        if m:
            models["straight_sets"] = m
            logger.info("  straight_sets fitted (rate=%.1f%%)", 100 * float(y_ss_tr.mean()))

        # over_2_sets: match goes to a deciding 3rd (or 4th/5th) set
        if "home_q3" in train_df.columns:
            valid_q3_tr = valid_tr & train_df["home_q3"].notna().reindex(train_df.index, fill_value=False)
            valid_q3_va = valid_va & val_df["home_q3"].notna().reindex(val_df.index, fill_value=False)
            # Target = 1 if q3 was played (home_q3 not null)
            y_o2s_tr = train_df.loc[valid_tr, "home_q3"].notna().astype(int)
            y_o2s_va = val_df.loc[valid_va, "home_q3"].notna().astype(int)
            if y_o2s_tr.mean() > 0.05 and y_o2s_tr.mean() < 0.95:
                m2 = self._fit_cls_safe("over_2_sets", X_train.loc[valid_tr], y_o2s_tr, X_val.loc[valid_va], y_o2s_va)
                if m2:
                    models["over_2_sets"] = m2
                    logger.info("  over_2_sets fitted (rate=%.1f%%)", 100 * float(y_o2s_tr.mean()))

        # tennis_total_games_over: total games played in match (sum of set game scores)
        game_cols = [(c, c.replace("home_", "away_")) for c in ["home_q1", "home_q2", "home_q3"] if c in train_df.columns]
        if game_cols:
            def _total_games_series(df: "pd.DataFrame") -> "pd.Series":
                total = pd.Series(0.0, index=df.index)
                for hc, ac in game_cols:
                    if hc in df.columns and ac in df.columns:
                        total += df[hc].fillna(0) + df[ac].fillna(0)
                return total

            tg_tr = _total_games_series(train_df.loc[valid_tr])
            tg_va = _total_games_series(val_df.loc[valid_va])
            median_tg = float(tg_tr.median())
            for offset, mname in [(-2.5, "tennis_total_games_under"), (0.0, "tennis_total_games_over_mid"), (2.5, "tennis_total_games_over_high")]:
                line = round(median_tg + offset) - 0.5
                y_tg_tr = (tg_tr > line).astype(int)
                y_tg_va = (tg_va > line).astype(int)
                if y_tg_tr.mean() < 0.05 or y_tg_tr.mean() > 0.95:
                    continue
                m3 = self._fit_cls_safe(mname, X_train.loc[valid_tr], y_tg_tr, X_val.loc[valid_va], y_tg_va)
                if m3:
                    models[mname] = m3
                    models[f"{mname}_line"] = line  # type: ignore[assignment]
                    logger.info("  %s fitted (line=%.1f, over_rate=%.1f%%)", mname, line, 100.0 * float(y_tg_tr.mean()))

    def _train_margin_bands(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train winning-margin band classifiers.

        Bands are sport-adaptive based on median margin:
          low-scoring (soccer/UFC): 1, 2, 3, 4+
          mid-scoring (NHL/MLB): 1-2, 3-4, 5-7, 8+
          high-scoring (NBA/NFL/NCAAB): 1-5, 6-10, 11-17, 18+
        A model is trained per band: P(margin lands in this band | home wins).
        Also trains: P(home wins by X+) as a single threshold model.
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float))
        as_tr = train_df.get("away_score", pd.Series(dtype=float))
        hs_va = val_df.get("home_score", pd.Series(dtype=float))
        as_va = val_df.get("away_score", pd.Series(dtype=float))

        valid_tr = hs_tr.notna() & as_tr.notna()
        valid_va = hs_va.notna() & as_va.notna()
        if valid_tr.sum() < 80 or valid_va.sum() < 20:
            return

        margin_tr = (hs_tr[valid_tr] - as_tr[valid_tr]).abs()
        margin_va = (hs_va[valid_va] - as_va[valid_va]).abs()
        median_m = float(margin_tr.median())

        # Determine band thresholds based on typical margin for sport
        if median_m <= 2:
            # soccer / UFC
            bands = [("margin_1", 1, 1), ("margin_2", 2, 2), ("margin_3", 3, 3), ("margin_4plus", 4, 9999)]
            dominant_thresh = 3
        elif median_m <= 5:
            # NHL, MLB, UFC
            bands = [("margin_1_2", 1, 2), ("margin_3_4", 3, 4), ("margin_5_7", 5, 7), ("margin_8plus", 8, 9999)]
            dominant_thresh = 5
        elif median_m <= 14:
            # NFL-like
            bands = [("margin_1_7", 1, 7), ("margin_8_14", 8, 14), ("margin_15_21", 15, 21), ("margin_22plus", 22, 9999)]
            dominant_thresh = 14
        else:
            # NBA / NCAAB
            bands = [("margin_1_5", 1, 5), ("margin_6_10", 6, 10), ("margin_11_17", 11, 17), ("margin_18plus", 18, 9999)]
            dominant_thresh = 15

        Xv_tr = X_train.loc[valid_tr]
        Xv_va = X_val.loc[valid_va]

        trained_any = False
        for name, lo, hi in bands:
            y_tr = ((margin_tr >= lo) & (margin_tr <= hi)).astype(int)
            y_va = ((margin_va >= lo) & (margin_va <= hi)).astype(int)
            if y_tr.sum() < 40:
                continue
            m = self._fit_cls_safe(name, Xv_tr, y_tr, Xv_va, y_va)
            if m:
                models[name] = m
                trained_any = True

        # Dominant win: winner covers dominant_thresh
        y_dom_tr = (margin_tr >= dominant_thresh).astype(int)
        y_dom_va = (margin_va >= dominant_thresh).astype(int)
        if y_dom_tr.sum() >= 40:
            m = self._fit_cls_safe("dominant_win", Xv_tr, y_dom_tr, Xv_va, y_dom_va)
            if m:
                models["dominant_win"] = m
                trained_any = True

        if trained_any:
            logger.info("  margin_band models fitted (median margin=%.1f, dominant_thresh=%d)", median_m, dominant_thresh)

    def _train_total_bands(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train total-score band classifiers (over/under buckets).

        3 bands determined from training data quartiles:
          low_total  = total <= p33
          mid_total  = p33 < total <= p67
          high_total = total > p67
        Also a simple over/under model at the median.
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float))
        as_tr = train_df.get("away_score", pd.Series(dtype=float))
        hs_va = val_df.get("home_score", pd.Series(dtype=float))
        as_va = val_df.get("away_score", pd.Series(dtype=float))

        valid_tr = hs_tr.notna() & as_tr.notna()
        valid_va = hs_va.notna() & as_va.notna()
        if valid_tr.sum() < 80 or valid_va.sum() < 20:
            return

        total_tr = hs_tr[valid_tr] + as_tr[valid_tr]
        total_va = hs_va[valid_va] + as_va[valid_va]

        p33 = float(total_tr.quantile(0.33))
        p67 = float(total_tr.quantile(0.67))
        median_t = float(total_tr.median())

        Xv_tr = X_train.loc[valid_tr]
        Xv_va = X_val.loc[valid_va]

        # Low / mid / high total bands
        for name, cond_tr, cond_va in [
            ("total_low", total_tr <= p33, total_va <= p33),
            ("total_mid", (total_tr > p33) & (total_tr <= p67), (total_va > p33) & (total_va <= p67)),
            ("total_high", total_tr > p67, total_va > p67),
        ]:
            y_tr = cond_tr.astype(int)
            y_va = cond_va.astype(int)
            if y_tr.sum() < 40:
                continue
            m = self._fit_cls_safe(name, Xv_tr, y_tr, Xv_va, y_va)
            if m:
                models[name] = m

        # Over/under median
        y_over_tr = (total_tr > median_t).astype(int)
        y_over_va = (total_va > median_t).astype(int)
        m = self._fit_cls_safe("total_over_median", Xv_tr, y_over_tr, Xv_va, y_over_va)
        if m:
            models["total_over_median"] = m
            logger.info("  total_band models fitted (p33=%.1f, p67=%.1f, median=%.1f)", p33, p67, median_t)

    def _train_second_half_winner(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train second-half winner model (q3+q4 combined for basketball/football, h2 for soccer).

        Identifies which team outscores the other in the second half.
        """
        sport = self.config.sport
        has_q3 = "home_q3" in train_df.columns and "away_q3" in train_df.columns
        has_q4 = "home_q4" in train_df.columns and "away_q4" in train_df.columns
        has_q2 = "home_q2" in train_df.columns and "away_q2" in train_df.columns
        has_h2 = "home_h2" in train_df.columns and "away_h2" in train_df.columns

        if has_q3 and has_q4:
            # Basketball/Football: second half = q3 + q4
            mask_tr = train_df["home_q3"].notna() & train_df["home_q4"].notna()
            mask_va = val_df["home_q3"].notna() & val_df["home_q4"].notna()
            if mask_tr.sum() < 80 or mask_va.sum() < 20:
                return
            sh_h_tr = train_df.loc[mask_tr, "home_q3"].fillna(0) + train_df.loc[mask_tr, "home_q4"].fillna(0)
            sh_a_tr = train_df.loc[mask_tr, "away_q3"].fillna(0) + train_df.loc[mask_tr, "away_q4"].fillna(0)
            sh_h_va = val_df.loc[mask_va, "home_q3"].fillna(0) + val_df.loc[mask_va, "home_q4"].fillna(0)
            sh_a_va = val_df.loc[mask_va, "away_q3"].fillna(0) + val_df.loc[mask_va, "away_q4"].fillna(0)
        elif has_h2 and sport in _SOCCER_HALF_SPORTS:
            # Soccer: second half = h2
            mask_tr = train_df["home_h2"].notna()
            mask_va = val_df["home_h2"].notna()
            if mask_tr.sum() < 80 or mask_va.sum() < 20:
                return
            sh_h_tr = train_df.loc[mask_tr, "home_h2"].fillna(0)
            sh_a_tr = train_df.loc[mask_tr, "away_h2"].fillna(0)
            sh_h_va = val_df.loc[mask_va, "home_h2"].fillna(0)
            sh_a_va = val_df.loc[mask_va, "away_h2"].fillna(0)
        elif has_q2:
            # NHL: second half = q2 (2nd period/half)
            mask_tr = train_df["home_q2"].notna()
            mask_va = val_df["home_q2"].notna()
            if mask_tr.sum() < 80 or mask_va.sum() < 20:
                return
            sh_h_tr = train_df.loc[mask_tr, "home_q2"].fillna(0)
            sh_a_tr = train_df.loc[mask_tr, "away_q2"].fillna(0)
            sh_h_va = val_df.loc[mask_va, "home_q2"].fillna(0)
            sh_a_va = val_df.loc[mask_va, "away_q2"].fillna(0)
        else:
            return

        nodraw_tr = sh_h_tr != sh_a_tr
        nodraw_va = sh_h_va != sh_a_va
        if nodraw_tr.sum() < 80 or nodraw_va.sum() < 20:
            return

        y_tr = (sh_h_tr[nodraw_tr] > sh_a_tr[nodraw_tr]).astype(int)
        y_va = (sh_h_va[nodraw_va] > sh_a_va[nodraw_va]).astype(int)
        Xs_tr = X_train.loc[nodraw_tr.index[nodraw_tr]]
        Xs_va = X_val.loc[nodraw_va.index[nodraw_va]]

        m = self._fit_cls_safe("second_half_winner", Xs_tr, y_tr, Xs_va, y_va)
        if m:
            models["second_half_winner"] = m
            logger.info("  second_half_winner fitted (home_rate=%.1f%%)", 100 * float(y_tr.mean()))

        # Also: second half total score regressor
        sh_total_tr = sh_h_tr + sh_a_tr
        sh_total_va = sh_h_va + sh_a_va
        Xall_tr = X_train.loc[mask_tr]
        Xall_va = X_val.loc[mask_va]
        m = self._fit_reg_safe("second_half_total", Xall_tr, sh_total_tr, Xall_va, sh_total_va)
        if m:
            models["second_half_total"] = m

    def _train_regulation_winner(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train regulation-time winner model (excludes OT/shootout).

        Derived from q1+q2+q3 (NHL: 3 periods) or q1+q2+q3+q4 (NBA/NFL).
        For sports where OT is common (NHL ~25%, NBA ~6%), this tells bettors
        who to back for the regulation 60/48 minutes.
        Also produces a 3-way regulation result (home/draw/away).
        """
        # Determine regulation periods
        sport = self.config.sport
        if sport in ("nhl",):
            reg_qs = ["q1", "q2", "q3"]
        elif sport in ("nba", "nfl", "ncaab", "ncaaf", "wnba"):
            reg_qs = ["q1", "q2", "q3", "q4"]
        else:
            return  # soccer is already full-time; MLB has no OT concept

        avail_qs = [q for q in reg_qs if f"home_{q}" in train_df.columns and f"away_{q}" in train_df.columns]
        if len(avail_qs) < 2:
            return

        mask_tr = train_df[f"home_{avail_qs[0]}"].notna()
        mask_va = val_df[f"home_{avail_qs[0]}"].notna()
        for q in avail_qs[1:]:
            mask_tr &= train_df[f"home_{q}"].notna()
            mask_va &= val_df[f"home_{q}"].notna()

        if mask_tr.sum() < 80 or mask_va.sum() < 20:
            return

        reg_h_tr = sum(train_df.loc[mask_tr, f"home_{q}"].fillna(0) for q in avail_qs)
        reg_a_tr = sum(train_df.loc[mask_tr, f"away_{q}"].fillna(0) for q in avail_qs)
        reg_h_va = sum(val_df.loc[mask_va, f"home_{q}"].fillna(0) for q in avail_qs)
        reg_a_va = sum(val_df.loc[mask_va, f"away_{q}"].fillna(0) for q in avail_qs)

        nodraw_tr = reg_h_tr != reg_a_tr
        nodraw_va = reg_h_va != reg_a_va

        if nodraw_tr.sum() >= 80 and nodraw_va.sum() >= 20:
            y_tr = (reg_h_tr[nodraw_tr] > reg_a_tr[nodraw_tr]).astype(int)
            y_va = (reg_h_va[nodraw_va] > reg_a_va[nodraw_va]).astype(int)
            m = self._fit_cls_safe(
                "regulation_winner",
                X_train.loc[nodraw_tr.index[nodraw_tr]],
                y_tr,
                X_val.loc[nodraw_va.index[nodraw_va]],
                y_va,
            )
            if m:
                models["regulation_winner"] = m
                logger.info("  regulation_winner fitted (home_reg_rate=%.1f%%)", 100 * float(y_tr.mean()))

        # Regulation draw (goes to OT)
        y_draw_reg_tr = (reg_h_tr == reg_a_tr).astype(int)
        y_draw_reg_va = (reg_h_va == reg_a_va).astype(int)
        if y_draw_reg_tr.sum() >= 40:
            m = self._fit_cls_safe(
                "regulation_draw",
                X_train.loc[mask_tr],
                y_draw_reg_tr,
                X_val.loc[mask_va],
                y_draw_reg_va,
            )
            if m:
                models["regulation_draw"] = m

    def _train_team_totals(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train team-total over/under models.

        For each team (home/away), trains P(team_score > median_team_score).
        Also trains exact score-band regressors for home/away team totals.
        This feeds directly into team-total betting lines.
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float))
        as_tr = train_df.get("away_score", pd.Series(dtype=float))
        hs_va = val_df.get("home_score", pd.Series(dtype=float))
        as_va = val_df.get("away_score", pd.Series(dtype=float))

        valid_tr = hs_tr.notna() & as_tr.notna()
        valid_va = hs_va.notna() & as_va.notna()
        if valid_tr.sum() < 80 or valid_va.sum() < 20:
            return

        Xv_tr = X_train.loc[valid_tr]
        Xv_va = X_val.loc[valid_va]

        home_median = float(hs_tr[valid_tr].median())
        away_median = float(as_tr[valid_tr].median())

        # Home team total over/under
        y_ht_over_tr = (hs_tr[valid_tr] > home_median).astype(int)
        y_ht_over_va = (hs_va[valid_va] > home_median).astype(int)
        m = self._fit_cls_safe("home_team_total_over", Xv_tr, y_ht_over_tr, Xv_va, y_ht_over_va)
        if m:
            models["home_team_total_over"] = m

        # Away team total over/under
        y_at_over_tr = (as_tr[valid_tr] > away_median).astype(int)
        y_at_over_va = (as_va[valid_va] > away_median).astype(int)
        m = self._fit_cls_safe("away_team_total_over", Xv_tr, y_at_over_tr, Xv_va, y_at_over_va)
        if m:
            models["away_team_total_over"] = m

        logger.info(
            "  team_total models fitted (home_median=%.1f, away_median=%.1f)",
            home_median, away_median,
        )

    def _train_esports_markets(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train esports-specific map win models.

        home_score/away_score represent MAP WINS in a series (0-3).
        Trains:
          esports_clean_sweep: P(winner wins without dropping a map)
          esports_map_total:   Regression for total maps played
          esports_map_total_over2: P(series goes to more than 2 maps)
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_tr = train_df.get("away_score", pd.Series(dtype=float)).fillna(0)
        hs_va = val_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_va = val_df.get("away_score", pd.Series(dtype=float)).fillna(0)

        valid_tr = hs_tr.notna() & as_tr.notna()
        valid_va = hs_va.notna() & as_va.notna()
        n_tr = int(valid_tr.sum())
        n_va = int(valid_va.sum())
        if n_tr < 80 or n_va < 20:
            return

        logger.info("Training esports map-win models (%d/%d rows) …", n_tr, n_va)

        Xv_tr = X_train.loc[valid_tr]
        Xv_va = X_val.loc[valid_va]

        # Clean sweep: winner wins without losing any maps (2-0 or 3-0)
        df_hs_tr = hs_tr[valid_tr]
        df_as_tr = as_tr[valid_tr]
        df_hs_va = hs_va[valid_va]
        df_as_va = as_va[valid_va]

        y_sweep_tr = ((df_hs_tr == 0) | (df_as_tr == 0)).astype(int)
        y_sweep_va = ((df_hs_va == 0) | (df_as_va == 0)).astype(int)
        m = self._fit_cls_safe("esports_clean_sweep", Xv_tr, y_sweep_tr, Xv_va, y_sweep_va)
        if m:
            models["esports_clean_sweep"] = m
            logger.info("  esports_clean_sweep fitted (rate=%.1f%%)", 100 * float(y_sweep_tr.mean()))

        # Map total regressor (total maps played in the series)
        map_total_tr = df_hs_tr + df_as_tr
        map_total_va = df_hs_va + df_as_va
        m = self._fit_reg_safe("esports_map_total", Xv_tr, map_total_tr, Xv_va, map_total_va)
        if m:
            models["esports_map_total"] = m
            logger.info("  esports_map_total fitted (mean_maps=%.2f)", float(map_total_tr.mean()))

        # Map total over 2 classifier (series goes longer than 2 maps)
        y_over2_tr = (map_total_tr > 2).astype(int)
        y_over2_va = (map_total_va > 2).astype(int)
        m = self._fit_cls_safe("esports_map_total_over2", Xv_tr, y_over2_tr, Xv_va, y_over2_va)
        if m:
            models["esports_map_total_over2"] = m
            logger.info("  esports_map_total_over2 fitted (rate=%.1f%%)", 100 * float(y_over2_tr.mean()))

        # Home 2-0 win (clean sweep for home team)
        y_h20_tr = ((df_hs_tr == 2) & (df_as_tr == 0)).astype(int)
        y_h20_va = ((df_hs_va == 2) & (df_as_va == 0)).astype(int)
        if y_h20_tr.sum() >= 40:
            m = self._fit_cls_safe("home_2_0_win", Xv_tr, y_h20_tr, Xv_va, y_h20_va)
            if m:
                models["home_2_0_win"] = m
                logger.info("  home_2_0_win fitted (rate=%.1f%%)", 100 * float(y_h20_tr.mean()))

        # Away 2-0 win (clean sweep for away team)
        y_a20_tr = ((df_as_tr == 2) & (df_hs_tr == 0)).astype(int)
        y_a20_va = ((df_as_va == 2) & (df_hs_va == 0)).astype(int)
        if y_a20_tr.sum() >= 40:
            m = self._fit_cls_safe("away_2_0_win", Xv_tr, y_a20_tr, Xv_va, y_a20_va)
            if m:
                models["away_2_0_win"] = m
                logger.info("  away_2_0_win fitted (rate=%.1f%%)", 100 * float(y_a20_tr.mean()))

        # Decider map: series goes to 3 maps in a BO3 (2-1 in any direction)
        y_decider_tr = ((df_hs_tr == 2) & (df_as_tr == 1) | (df_as_tr == 2) & (df_hs_tr == 1)).astype(int)
        y_decider_va = ((df_hs_va == 2) & (df_as_va == 1) | (df_as_va == 2) & (df_hs_va == 1)).astype(int)
        if y_decider_tr.sum() >= 40:
            m = self._fit_cls_safe("esports_decider_map", Xv_tr, y_decider_tr, Xv_va, y_decider_va)
            if m:
                models["esports_decider_map"] = m
                logger.info("  esports_decider_map fitted (rate=%.1f%%)", 100 * float(y_decider_tr.mean()))

    def _train_dominant_win(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train double-digit / blowout win probability model.

        For basketball: P(winner wins by 10+) → called 'double_digit_win'.
        For other sports: P(winner wins by sport-specific large threshold).
        This is already partially covered by _train_margin_bands dominant_win model,
        but here we target the sport-specific 'big win' threshold explicitly.
        """
        sport = self.config.sport
        # Sport-specific 'big win' thresholds
        big_win_thresholds = {
            "nba": 10, "wnba": 10, "ncaab": 10, "ncaaf": 14,
            "nfl": 14, "nhl": 3, "mlb": 4, "soccer": 3,
            "epl": 3, "laliga": 3, "bundesliga": 3, "ligue1": 3, "seriea": 3,
            "ucl": 3, "mls": 3, "nwsl": 3,
            "csgo": 2, "dota2": 2, "lol": 2, "valorant": 2,
        }
        thresh = big_win_thresholds.get(sport)
        if thresh is None:
            return

        hs_tr = train_df.get("home_score", pd.Series(dtype=float))
        as_tr = train_df.get("away_score", pd.Series(dtype=float))
        hs_va = val_df.get("home_score", pd.Series(dtype=float))
        as_va = val_df.get("away_score", pd.Series(dtype=float))

        valid_tr = hs_tr.notna() & as_tr.notna()
        valid_va = hs_va.notna() & as_va.notna()
        if valid_tr.sum() < 80 or valid_va.sum() < 20:
            return

        margin_tr = (hs_tr[valid_tr] - as_tr[valid_tr]).abs()
        margin_va = (hs_va[valid_va] - as_va[valid_va]).abs()
        y_tr = (margin_tr >= thresh).astype(int)
        y_va = (margin_va >= thresh).astype(int)

        if y_tr.sum() < 40:
            return

        Xv_tr = X_train.loc[valid_tr]
        Xv_va = X_val.loc[valid_va]
        m = self._fit_cls_safe("large_margin_win", Xv_tr, y_tr, Xv_va, y_va)
        if m:
            models["large_margin_win"] = m
            logger.info("  large_margin_win fitted (thresh=%d, rate=%.1f%%)", thresh, 100 * float(y_tr.mean()))

    def _train_comeback(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train comeback win probability (team trailing at halftime wins game)."""
        sport = self.config.sport
        # Determine half columns
        if sport in _SOCCER_HALF_SPORTS:
            h1_col, h2_col = "home_h1", "home_h2"
            a1_col, a2_col = "away_h1", "away_h2"
        else:
            h1_col, h2_col = "home_q1", "home_q2"
            a1_col, a2_col = "away_q1", "away_q2"

        is_two_half = sport in _TWO_HALF_SPORTS

        # Two-half sports: halftime score = H1 only; four-quarter: Q1+Q2
        if is_two_half:
            q_avail = all(c in train_df.columns for c in [h1_col, a1_col])
        else:
            q_avail = all(c in train_df.columns for c in [h1_col, h2_col, a1_col, a2_col])
        hs_avail = "home_score" in train_df.columns and "away_score" in train_df.columns
        if not q_avail or not hs_avail:
            return

        if is_two_half:
            mask_tr = train_df[h1_col].notna() & train_df["home_score"].notna()
            mask_va = val_df[h1_col].notna() & val_df["home_score"].notna()
        else:
            mask_tr = (
                train_df[h1_col].notna() & train_df[h2_col].notna() &
                train_df["home_score"].notna()
            )
            mask_va = (
                val_df[h1_col].notna() & val_df[h2_col].notna() &
                val_df["home_score"].notna()
            )
        if mask_tr.sum() < 80 or mask_va.sum() < 20:
            return

        if is_two_half:
            hh_tr = train_df.loc[mask_tr, h1_col].fillna(0)
            ah_tr = train_df.loc[mask_tr, a1_col].fillna(0)
            hh_va = val_df.loc[mask_va, h1_col].fillna(0)
            ah_va = val_df.loc[mask_va, a1_col].fillna(0)
        else:
            hh_tr = train_df.loc[mask_tr, h1_col].fillna(0) + train_df.loc[mask_tr, h2_col].fillna(0)
            ah_tr = train_df.loc[mask_tr, a1_col].fillna(0) + train_df.loc[mask_tr, a2_col].fillna(0)
            hh_va = val_df.loc[mask_va, h1_col].fillna(0) + val_df.loc[mask_va, h2_col].fillna(0)
            ah_va = val_df.loc[mask_va, a1_col].fillna(0) + val_df.loc[mask_va, a2_col].fillna(0)

        Xc_tr = X_train.loc[mask_tr]
        Xc_va = X_val.loc[mask_va]
        hf_tr = train_df.loc[mask_tr, "home_score"].fillna(0)
        af_tr = train_df.loc[mask_tr, "away_score"].fillna(0)
        hf_va = val_df.loc[mask_va, "home_score"].fillna(0)
        af_va = val_df.loc[mask_va, "away_score"].fillna(0)
        home_trails_tr = hh_tr < ah_tr
        home_trails_va = hh_va < ah_va

        if home_trails_tr.sum() >= 40:
            y_cb_h_tr = (home_trails_tr & (hf_tr > af_tr)).astype(int)
            y_cb_h_va = (home_trails_va & (hf_va > af_va)).astype(int)
            m = self._fit_cls_safe(
                "comeback_home",
                Xc_tr.loc[home_trails_tr.index[home_trails_tr]],
                y_cb_h_tr[home_trails_tr],
                Xc_va.loc[home_trails_va.index[home_trails_va]] if home_trails_va.sum() > 0 else Xc_va,
                y_cb_h_va[home_trails_va] if home_trails_va.sum() > 0 else y_cb_h_va,
            )
            if m:
                models["comeback_home"] = m

        away_trails_tr = hh_tr > ah_tr
        away_trails_va = hh_va > ah_va
        if away_trails_tr.sum() >= 40:
            y_cb_a_tr = (away_trails_tr & (af_tr > hf_tr)).astype(int)
            y_cb_a_va = (away_trails_va & (af_va > hf_va)).astype(int)
            m = self._fit_cls_safe(
                "comeback_away",
                Xc_tr.loc[away_trails_tr.index[away_trails_tr]],
                y_cb_a_tr[away_trails_tr],
                Xc_va.loc[away_trails_va.index[away_trails_va]] if away_trails_va.sum() > 0 else Xc_va,
                y_cb_a_va[away_trails_va] if away_trails_va.sum() > 0 else y_cb_a_va,
            )
            if m:
                models["comeback_away"] = m

        if "comeback_home" in models or "comeback_away" in models:
            logger.info(
                "  comeback models fitted (home_trails_rate=%.1f%%, away_trails_rate=%.1f%%)",
                100 * float(home_trails_tr.mean()),
                100 * float(away_trails_tr.mean()),
            )

    def _train_double_chance(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train double-chance classifiers: 1X, X2, 12.

        1X = home wins OR draw  (away doesn't win)
        X2 = away wins OR draw  (home doesn't win)
        12 = home wins OR away wins  (no draw / game decided outright)
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_tr = train_df.get("away_score", pd.Series(dtype=float)).fillna(0)
        hs_va = val_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_va = val_df.get("away_score", pd.Series(dtype=float)).fillna(0)

        valid_tr = (hs_tr + as_tr > 0)
        valid_va = (hs_va + as_va > 0)
        if valid_tr.sum() < 80 or valid_va.sum() < 20:
            return

        Xv_tr = X_train.loc[valid_tr]
        Xv_va = X_val.loc[valid_va]
        hs_t, as_t = hs_tr[valid_tr], as_tr[valid_tr]
        hs_v, as_v = hs_va[valid_va], as_va[valid_va]

        logger.info("Training double-chance models (%d/%d rows) …", int(valid_tr.sum()), int(valid_va.sum()))

        # 1X: home wins or draw → away doesn't win
        y1x_tr = (hs_t >= as_t).astype(int)
        y1x_va = (hs_v >= as_v).astype(int)
        m = self._fit_cls_safe("double_chance_1X", Xv_tr, y1x_tr, Xv_va, y1x_va)
        if m:
            models["double_chance_1X"] = m
            logger.info("  double_chance_1X fitted (rate=%.1f%%)", 100 * float(y1x_tr.mean()))

        # X2: away wins or draw → home doesn't win
        yx2_tr = (as_t >= hs_t).astype(int)
        yx2_va = (as_v >= hs_v).astype(int)
        m = self._fit_cls_safe("double_chance_X2", Xv_tr, yx2_tr, Xv_va, yx2_va)
        if m:
            models["double_chance_X2"] = m
            logger.info("  double_chance_X2 fitted (rate=%.1f%%)", 100 * float(yx2_tr.mean()))

        # 12: no draw → home or away wins
        y12_tr = (hs_t != as_t).astype(int)
        y12_va = (hs_v != as_v).astype(int)
        if y12_tr.sum() >= 40:
            m = self._fit_cls_safe("double_chance_12", Xv_tr, y12_tr, Xv_va, y12_va)
            if m:
                models["double_chance_12"] = m
                logger.info("  double_chance_12 fitted (rate=%.1f%%)", 100 * float(y12_tr.mean()))

    def _train_nrfi(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train MLB NRFI/YRFI models (No/Yes Run First Inning).

        NRFI = neither team scores in the 1st inning (P(home_i1==0 AND away_i1==0))
        YRFI = at least one team scores in the 1st inning (complement of NRFI)
        """
        if "home_i1" not in train_df.columns or "away_i1" not in train_df.columns:
            return

        mask_tr = train_df["home_i1"].notna() & train_df["away_i1"].notna()
        mask_va = val_df["home_i1"].notna() & val_df["away_i1"].notna()
        n_tr = int(mask_tr.sum())
        n_va = int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            return

        logger.info("Training NRFI/YRFI models (%d/%d rows) …", n_tr, n_va)

        hi1_tr = train_df.loc[mask_tr, "home_i1"].fillna(0)
        ai1_tr = train_df.loc[mask_tr, "away_i1"].fillna(0)
        hi1_va = val_df.loc[mask_va, "home_i1"].fillna(0)
        ai1_va = val_df.loc[mask_va, "away_i1"].fillna(0)

        Xn_tr = X_train.loc[mask_tr]
        Xn_va = X_val.loc[mask_va]

        # NRFI: no scoring in first inning
        y_nrfi_tr = ((hi1_tr == 0) & (ai1_tr == 0)).astype(int)
        y_nrfi_va = ((hi1_va == 0) & (ai1_va == 0)).astype(int)
        m = self._fit_cls_safe("nrfi", Xn_tr, y_nrfi_tr, Xn_va, y_nrfi_va)
        if m:
            models["nrfi"] = m
            logger.info("  nrfi fitted (nrfi_rate=%.1f%%)", 100 * float(y_nrfi_tr.mean()))

        # YRFI: at least one team scores in first inning
        y_yrfi_tr = ((hi1_tr > 0) | (ai1_tr > 0)).astype(int)
        y_yrfi_va = ((hi1_va > 0) | (ai1_va > 0)).astype(int)
        m = self._fit_cls_safe("yrfi", Xn_tr, y_yrfi_tr, Xn_va, y_yrfi_va)
        if m:
            models["yrfi"] = m
            logger.info("  yrfi fitted (yrfi_rate=%.1f%%)", 100 * float(y_yrfi_tr.mean()))

        # Home team scores first inning (home_i1 > 0)
        y_hyrfi_tr = (hi1_tr > 0).astype(int)
        y_hyrfi_va = (hi1_va > 0).astype(int)
        if y_hyrfi_tr.sum() >= 40:
            m = self._fit_cls_safe("home_scores_i1", Xn_tr, y_hyrfi_tr, Xn_va, y_hyrfi_va)
            if m:
                models["home_scores_i1"] = m

        # Away team scores first inning
        y_ayrfi_tr = (ai1_tr > 0).astype(int)
        y_ayrfi_va = (ai1_va > 0).astype(int)
        if y_ayrfi_tr.sum() >= 40:
            m = self._fit_cls_safe("away_scores_i1", Xn_tr, y_ayrfi_tr, Xn_va, y_ayrfi_va)
            if m:
                models["away_scores_i1"] = m

    def _train_shutout(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train shutout/clean-sheet models for low-scoring sports (e.g. NHL).

        shutout_home = P(away_score == 0) — home team shuts out the away team
        shutout_away = P(home_score == 0) — away team shuts out the home team
        btts        = P(both teams score ≥ 1 goal)
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_tr = train_df.get("away_score", pd.Series(dtype=float)).fillna(0)
        hs_va = val_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_va = val_df.get("away_score", pd.Series(dtype=float)).fillna(0)

        valid_tr = (hs_tr + as_tr > 0)
        valid_va = (hs_va + as_va > 0)
        if valid_tr.sum() < 80 or valid_va.sum() < 20:
            return

        logger.info("Training shutout/BTTS models (%d/%d rows) …", int(valid_tr.sum()), int(valid_va.sum()))
        Xv_tr = X_train.loc[valid_tr]
        Xv_va = X_val.loc[valid_va]
        hs_t, as_t = hs_tr[valid_tr], as_tr[valid_tr]
        hs_v, as_v = hs_va[valid_va], as_va[valid_va]

        # Shutout home (away scores 0)
        y_sh_tr = (as_t == 0).astype(int)
        y_sh_va = (as_v == 0).astype(int)
        if y_sh_tr.sum() >= 30:
            m = self._fit_cls_safe("shutout_home", Xv_tr, y_sh_tr, Xv_va, y_sh_va)
            if m:
                models["shutout_home"] = m
                logger.info("  shutout_home fitted (rate=%.1f%%)", 100 * float(y_sh_tr.mean()))

        # Shutout away (home scores 0)
        y_sa_tr = (hs_t == 0).astype(int)
        y_sa_va = (hs_v == 0).astype(int)
        if y_sa_tr.sum() >= 30:
            m = self._fit_cls_safe("shutout_away", Xv_tr, y_sa_tr, Xv_va, y_sa_va)
            if m:
                models["shutout_away"] = m
                logger.info("  shutout_away fitted (rate=%.1f%%)", 100 * float(y_sa_tr.mean()))

        # BTTS (both score)
        y_btts_tr = ((hs_t > 0) & (as_t > 0)).astype(int)
        y_btts_va = ((hs_v > 0) & (as_v > 0)).astype(int)
        m = self._fit_cls_safe("btts", Xv_tr, y_btts_tr, Xv_va, y_btts_va)
        if m:
            models["btts"] = m
            logger.info("  btts fitted (rate=%.1f%%)", 100 * float(y_btts_tr.mean()))

    def _train_asian_handicap(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train Asian Handicap line classifiers.

        AH -1 home  = home wins by 2+ goals (covers −1 handicap)
        AH -1 away  = away wins by 2+ goals
        AH -1.5 home = home wins by 2+ (same as AH -1 for integer sports)
        AH +1 home  = home wins or draws or loses by exactly 1 (covers +1)
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_tr = train_df.get("away_score", pd.Series(dtype=float)).fillna(0)
        hs_va = val_df.get("home_score", pd.Series(dtype=float)).fillna(0)
        as_va = val_df.get("away_score", pd.Series(dtype=float)).fillna(0)

        valid_tr = hs_tr.notna() & as_tr.notna() & ((hs_tr + as_tr) > 0)
        valid_va = hs_va.notna() & as_va.notna() & ((hs_va + as_va) > 0)
        if valid_tr.sum() < 80 or valid_va.sum() < 20:
            return

        margin_tr = hs_tr[valid_tr] - as_tr[valid_tr]
        margin_va = hs_va[valid_va] - as_va[valid_va]
        Xv_tr = X_train.loc[valid_tr]
        Xv_va = X_val.loc[valid_va]

        logger.info("Training Asian Handicap models (%d/%d rows) …", int(valid_tr.sum()), int(valid_va.sum()))

        # AH -1 home: home wins by 2+ goals
        y_ah1h_tr = (margin_tr >= 2).astype(int)
        y_ah1h_va = (margin_va >= 2).astype(int)
        if y_ah1h_tr.sum() >= 40:
            m = self._fit_cls_safe("ah_minus1_home", Xv_tr, y_ah1h_tr, Xv_va, y_ah1h_va)
            if m:
                models["ah_minus1_home"] = m
                logger.info("  ah_minus1_home fitted (rate=%.1f%%)", 100 * float(y_ah1h_tr.mean()))

        # AH -1 away: away wins by 2+ goals
        y_ah1a_tr = (margin_tr <= -2).astype(int)
        y_ah1a_va = (margin_va <= -2).astype(int)
        if y_ah1a_tr.sum() >= 40:
            m = self._fit_cls_safe("ah_minus1_away", Xv_tr, y_ah1a_tr, Xv_va, y_ah1a_va)
            if m:
                models["ah_minus1_away"] = m
                logger.info("  ah_minus1_away fitted (rate=%.1f%%)", 100 * float(y_ah1a_tr.mean()))

        # AH +1 home: home doesn't lose by 2+ (wins, draws, or 1-goal loss)
        y_ahp1h_tr = (margin_tr > -2).astype(int)
        y_ahp1h_va = (margin_va > -2).astype(int)
        m = self._fit_cls_safe("ah_plus1_home", Xv_tr, y_ahp1h_tr, Xv_va, y_ahp1h_va)
        if m:
            models["ah_plus1_home"] = m
            logger.info("  ah_plus1_home fitted (rate=%.1f%%)", 100 * float(y_ahp1h_tr.mean()))

        # AH +1 away: away doesn't lose by 2+
        y_ahp1a_tr = (margin_tr < 2).astype(int)
        y_ahp1a_va = (margin_va < 2).astype(int)
        m = self._fit_cls_safe("ah_plus1_away", Xv_tr, y_ahp1a_tr, Xv_va, y_ahp1a_va)
        if m:
            models["ah_plus1_away"] = m
            logger.info("  ah_plus1_away fitted (rate=%.1f%%)", 100 * float(y_ahp1a_tr.mean()))

    def _train_f5_innings(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train MLB First 5 Innings (F5) winner models.

        F5 home win  = home leads after 5 innings (home cumulative > away cumulative)
        F5 away win  = away leads after 5 innings
        F5 tie       = tied after 5 innings
        F5 over/under total = total runs in first 5 innings vs a line
        """
        inning_cols = [f"home_i{i}" for i in range(1, 6)] + [f"away_i{i}" for i in range(1, 6)]
        if not all(c in train_df.columns for c in inning_cols):
            return

        # Build mask where all 5 innings are available
        mask_tr = train_df[inning_cols].notna().all(axis=1)
        mask_va = val_df[inning_cols].notna().all(axis=1)
        if mask_tr.sum() < 100 or mask_va.sum() < 20:
            return

        logger.info("Training F5 Innings models (%d/%d rows) …", int(mask_tr.sum()), int(mask_va.sum()))

        h5_tr = sum(train_df.loc[mask_tr, f"home_i{i}"].fillna(0) for i in range(1, 6))
        a5_tr = sum(train_df.loc[mask_tr, f"away_i{i}"].fillna(0) for i in range(1, 6))
        h5_va = sum(val_df.loc[mask_va, f"home_i{i}"].fillna(0) for i in range(1, 6))
        a5_va = sum(val_df.loc[mask_va, f"away_i{i}"].fillna(0) for i in range(1, 6))

        Xf_tr = X_train.loc[mask_tr]
        Xf_va = X_val.loc[mask_va]

        # F5 home win
        y_f5h_tr = (h5_tr > a5_tr).astype(int)
        y_f5h_va = (h5_va > a5_va).astype(int)
        m = self._fit_cls_safe("f5_home_win", Xf_tr, y_f5h_tr, Xf_va, y_f5h_va)
        if m:
            models["f5_home_win"] = m
            logger.info("  f5_home_win fitted (rate=%.1f%%)", 100 * float(y_f5h_tr.mean()))

        # F5 away win
        y_f5a_tr = (a5_tr > h5_tr).astype(int)
        y_f5a_va = (a5_va > h5_va).astype(int)
        m = self._fit_cls_safe("f5_away_win", Xf_tr, y_f5a_tr, Xf_va, y_f5a_va)
        if m:
            models["f5_away_win"] = m
            logger.info("  f5_away_win fitted (rate=%.1f%%)", 100 * float(y_f5a_tr.mean()))

        # F5 tie (both have same runs through 5)
        y_f5t_tr = (h5_tr == a5_tr).astype(int)
        y_f5t_va = (h5_va == a5_va).astype(int)
        if y_f5t_tr.sum() >= 40:
            m = self._fit_cls_safe("f5_tie", Xf_tr, y_f5t_tr, Xf_va, y_f5t_va)
            if m:
                models["f5_tie"] = m
                logger.info("  f5_tie fitted (rate=%.1f%%)", 100 * float(y_f5t_tr.mean()))

        # F5 over/under 4.5 runs total
        total5_tr = h5_tr + a5_tr
        total5_va = h5_va + a5_va
        line = 4.5
        y_f5ov_tr = (total5_tr > line).astype(int)
        y_f5ov_va = (total5_va > line).astype(int)
        m = self._fit_cls_safe("f5_over4_5", Xf_tr, y_f5ov_tr, Xf_va, y_f5ov_va)
        if m:
            models["f5_over4_5"] = m
            logger.info("  f5_over4_5 fitted (over_rate=%.1f%%)", 100 * float(y_f5ov_tr.mean()))

        y_f5un_tr = (total5_tr <= line).astype(int)
        y_f5un_va = (total5_va <= line).astype(int)
        m = self._fit_cls_safe("f5_under4_5", Xf_tr, y_f5un_tr, Xf_va, y_f5un_va)
        if m:
            models["f5_under4_5"] = m

    def _train_mlb_per_inning_markets(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train MLB per-inning betting markets.

        For each of innings 1-9 trains:
          - nrfi_i{N}:    No Run in inning N (NRFI extension beyond just inning 1)
          - over_i{N}:    Over 0.5 total runs in inning N (== yrfi equivalent)
        Also trains:
          - f7_home_win:  Home team leads after 7 innings
          - f7_over6_5:   First 7 innings over 6.5 total runs
          - last3_over:   Over median runs in innings 7-8-9
        """
        # Check all innings exist
        all_inn = [f"home_i{i}" for i in range(1, 10)] + [f"away_i{i}" for i in range(1, 10)]
        if not all(c in train_df.columns for c in all_inn):
            logger.debug("MLB per-inning: not all inning columns present — skipping")
            return

        mask_tr = train_df[all_inn].notna().all(axis=1)
        mask_va = val_df[all_inn].notna().all(axis=1)
        if mask_tr.sum() < 150 or mask_va.sum() < 30:
            logger.debug("MLB per-inning: insufficient data (%d/%d) — skipping", int(mask_tr.sum()), int(mask_va.sum()))
            return

        logger.info("Training MLB per-inning markets (%d/%d rows) …", int(mask_tr.sum()), int(mask_va.sum()))
        Xm_tr = X_train.loc[mask_tr]
        Xm_va = X_val.loc[mask_va]

        for inn in range(1, 10):
            hcol = f"home_i{inn}"
            acol = f"away_i{inn}"
            h_tr = train_df.loc[mask_tr, hcol].fillna(0)
            a_tr = train_df.loc[mask_tr, acol].fillna(0)
            h_va = val_df.loc[mask_va, hcol].fillna(0)
            a_va = val_df.loc[mask_va, acol].fillna(0)
            inn_total_tr = h_tr + a_tr
            inn_total_va = h_va + a_va

            # NRFI (No Run in this inning)
            y_nrfi_tr = (inn_total_tr == 0).astype(int)
            y_nrfi_va = (inn_total_va == 0).astype(int)
            if 0.10 < float(y_nrfi_tr.mean()) < 0.90:
                m = self._fit_cls_safe(f"nrfi_i{inn}", Xm_tr, y_nrfi_tr, Xm_va, y_nrfi_va)
                if m:
                    models[f"nrfi_i{inn}"] = m
                    logger.info("  nrfi_i%d fitted (nrfi_rate=%.1f%%)", inn, 100 * float(y_nrfi_tr.mean()))

        # F7 markets
        h7_tr = sum(train_df.loc[mask_tr, f"home_i{i}"].fillna(0) for i in range(1, 8))
        a7_tr = sum(train_df.loc[mask_tr, f"away_i{i}"].fillna(0) for i in range(1, 8))
        h7_va = sum(val_df.loc[mask_va, f"home_i{i}"].fillna(0) for i in range(1, 8))
        a7_va = sum(val_df.loc[mask_va, f"away_i{i}"].fillna(0) for i in range(1, 8))
        total7_tr = h7_tr + a7_tr
        total7_va = h7_va + a7_va

        y_f7h_tr = (h7_tr > a7_tr).astype(int)
        y_f7h_va = (h7_va > a7_va).astype(int)
        m = self._fit_cls_safe("f7_home_win", Xm_tr, y_f7h_tr, Xm_va, y_f7h_va)
        if m:
            models["f7_home_win"] = m
            logger.info("  f7_home_win fitted (rate=%.1f%%)", 100 * float(y_f7h_tr.mean()))

        f7_line = float(total7_tr.median()) - 0.5
        y_f7ov_tr = (total7_tr > f7_line).astype(int)
        y_f7ov_va = (total7_va > f7_line).astype(int)
        if 0.10 < float(y_f7ov_tr.mean()) < 0.90:
            m = self._fit_cls_safe("f7_over", Xm_tr, y_f7ov_tr, Xm_va, y_f7ov_va)
            if m:
                models["f7_over"] = m
                models["f7_over_line"] = f7_line  # type: ignore[assignment]
                logger.info("  f7_over (line=%.1f) fitted (over_rate=%.1f%%)", f7_line, 100 * float(y_f7ov_tr.mean()))

        # Last 3 innings (7-8-9) total over/under
        h39_tr = sum(train_df.loc[mask_tr, f"home_i{i}"].fillna(0) for i in range(7, 10))
        a39_tr = sum(train_df.loc[mask_tr, f"away_i{i}"].fillna(0) for i in range(7, 10))
        h39_va = sum(val_df.loc[mask_va, f"home_i{i}"].fillna(0) for i in range(7, 10))
        a39_va = sum(val_df.loc[mask_va, f"away_i{i}"].fillna(0) for i in range(7, 10))
        late_total_tr = h39_tr + a39_tr
        late_total_va = h39_va + a39_va
        late_line = float(late_total_tr.median()) - 0.5
        y_late_tr = (late_total_tr > late_line).astype(int)
        y_late_va = (late_total_va > late_line).astype(int)
        if 0.10 < float(y_late_tr.mean()) < 0.90:
            m = self._fit_cls_safe("late_inning_over", Xm_tr, y_late_tr, Xm_va, y_late_va)
            if m:
                models["late_inning_over"] = m
                models["late_inning_line"] = late_line  # type: ignore[assignment]
                logger.info("  late_inning_over (line=%.1f, innings 7-9) fitted (rate=%.1f%%)", late_line, 100 * float(y_late_tr.mean()))

    def _train_correct_score_bands(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train soccer Correct Score Band classifiers.

        Bands: 0-0, 1-0/0-1 (one-goal home/away), 2-0+/0-2+ (multi-goal blowout),
        1-1, 2-1/1-2, 2-2+, 3+ goals total low-scoring vs high-scoring.
        Also trains: home scores 2+ goals, away scores 2+, over 2.5 with exact breakdown.
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float))
        as_tr = train_df.get("away_score", pd.Series(dtype=float))
        hs_va = val_df.get("home_score", pd.Series(dtype=float))
        as_va = val_df.get("away_score", pd.Series(dtype=float))

        valid_tr = hs_tr.notna() & as_tr.notna()
        valid_va = hs_va.notna() & as_va.notna()
        if valid_tr.sum() < 100 or valid_va.sum() < 20:
            return

        hs_tr = hs_tr[valid_tr].fillna(0)
        as_tr = as_tr[valid_tr].fillna(0)
        hs_va = hs_va[valid_va].fillna(0)
        as_va = as_va[valid_va].fillna(0)
        Xc_tr = X_train.loc[valid_tr]
        Xc_va = X_val.loc[valid_va]

        logger.info("Training Correct Score Band models (%d/%d rows) …", int(valid_tr.sum()), int(valid_va.sum()))

        def _fit(name, y_tr, y_va):
            if y_tr.sum() < 40:
                return
            m = self._fit_cls_safe(name, Xc_tr, y_tr, Xc_va, y_va)
            if m:
                models[name] = m
                logger.info("  %s fitted (rate=%.1f%%)", name, 100 * float(y_tr.mean()))

        # 0-0 draw (nil-nil)
        _fit("score_nil_nil",
             ((hs_tr == 0) & (as_tr == 0)).astype(int),
             ((hs_va == 0) & (as_va == 0)).astype(int))

        # Home wins 1-0
        _fit("score_1_0",
             ((hs_tr == 1) & (as_tr == 0)).astype(int),
             ((hs_va == 1) & (as_va == 0)).astype(int))

        # Away wins 0-1
        _fit("score_0_1",
             ((hs_tr == 0) & (as_tr == 1)).astype(int),
             ((hs_va == 0) & (as_va == 1)).astype(int))

        # 1-1 draw
        _fit("score_1_1",
             ((hs_tr == 1) & (as_tr == 1)).astype(int),
             ((hs_va == 1) & (as_va == 1)).astype(int))

        # Home wins 2-0 or more (dominant home)
        _fit("score_2plus_0",
             ((hs_tr >= 2) & (as_tr == 0)).astype(int),
             ((hs_va >= 2) & (as_va == 0)).astype(int))

        # Away wins 0-2 or more (dominant away)
        _fit("score_0_2plus",
             ((hs_tr == 0) & (as_tr >= 2)).astype(int),
             ((hs_va == 0) & (as_va >= 2)).astype(int))

        # Home wins 2-1
        _fit("score_2_1",
             ((hs_tr == 2) & (as_tr == 1)).astype(int),
             ((hs_va == 2) & (as_va == 1)).astype(int))

        # Away wins 1-2
        _fit("score_1_2",
             ((hs_tr == 1) & (as_tr == 2)).astype(int),
             ((hs_va == 1) & (as_va == 2)).astype(int))

        # High scoring (3+ total goals)
        _fit("score_3plus_total",
             ((hs_tr + as_tr) >= 3).astype(int),
             ((hs_va + as_va) >= 3).astype(int))

        # Low scoring (0 or 1 total goals)
        _fit("score_low_total",
             ((hs_tr + as_tr) <= 1).astype(int),
             ((hs_va + as_va) <= 1).astype(int))

    def _train_first_half_markets(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train soccer first-half O/U 0.5 / O/U 1.5 and win-both-halves models."""
        h1h = "home_h1"
        h1a = "away_h1"
        h2h = "home_h2"
        h2a = "away_h2"

        # Need both halves for win-both-halves; just H1 for H1 O/U
        mask_h1_tr = train_df[h1h].notna() & train_df[h1a].notna()
        mask_h1_va = val_df[h1h].notna() & val_df[h1a].notna()
        if mask_h1_tr.sum() < 80 or mask_h1_va.sum() < 20:
            return

        h1_total_tr = train_df.loc[mask_h1_tr, h1h].fillna(0) + train_df.loc[mask_h1_tr, h1a].fillna(0)
        h1_total_va = val_df.loc[mask_h1_va, h1h].fillna(0) + val_df.loc[mask_h1_va, h1a].fillna(0)
        Xh1_tr = X_train.loc[mask_h1_tr]
        Xh1_va = X_val.loc[mask_h1_va]

        def _fit(name: str, y_tr: "pd.Series", y_va: "pd.Series", Xt: "pd.DataFrame", Xv: "pd.DataFrame") -> None:
            m = self._fit_cls_safe(name, Xt, y_tr, Xv, y_va)
            if m:
                models[name] = m
                logger.info("  %s fitted (pos_rate=%.1f%%)", name, 100.0 * float(y_tr.mean()))

        # H1 over 0.5 (at least 1 goal in first half)
        _fit("h1_over0_5",
             (h1_total_tr >= 1).astype(int),
             (h1_total_va >= 1).astype(int),
             Xh1_tr, Xh1_va)

        # H1 over 1.5 (at least 2 goals in first half)
        _fit("h1_over1_5",
             (h1_total_tr >= 2).astype(int),
             (h1_total_va >= 2).astype(int),
             Xh1_tr, Xh1_va)

        # Win both halves — needs H1 and H2
        if h2h not in train_df.columns or h2a not in train_df.columns:
            return
        mask_both_tr = mask_h1_tr & train_df[h2h].notna() & train_df[h2a].notna()
        mask_both_va = mask_h1_va & val_df[h2h].notna() & val_df[h2a].notna()
        if mask_both_tr.sum() < 80 or mask_both_va.sum() < 20:
            return

        h1h_tr = train_df.loc[mask_both_tr, h1h].fillna(0)
        h1a_tr = train_df.loc[mask_both_tr, h1a].fillna(0)
        h2h_tr = train_df.loc[mask_both_tr, h2h].fillna(0)
        h2a_tr = train_df.loc[mask_both_tr, h2a].fillna(0)
        h1h_va = val_df.loc[mask_both_va, h1h].fillna(0)
        h1a_va = val_df.loc[mask_both_va, h1a].fillna(0)
        h2h_va = val_df.loc[mask_both_va, h2h].fillna(0)
        h2a_va = val_df.loc[mask_both_va, h2a].fillna(0)
        Xb_tr = X_train.loc[mask_both_tr]
        Xb_va = X_val.loc[mask_both_va]

        # Home wins both halves: scores more in H1 AND more in H2
        _fit("win_both_halves_home",
             ((h1h_tr > h1a_tr) & (h2h_tr > h2a_tr)).astype(int),
             ((h1h_va > h1a_va) & (h2h_va > h2a_va)).astype(int),
             Xb_tr, Xb_va)

        # Away wins both halves
        _fit("win_both_halves_away",
             ((h1a_tr > h1h_tr) & (h2a_tr > h2h_tr)).astype(int),
             ((h1a_va > h1h_va) & (h2a_va > h2h_va)).astype(int),
             Xb_tr, Xb_va)

    def _train_period_btts(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train 'both teams score in period N' classifiers for hockey/basketball.

        For NHL: P1/P2/P3 both score.
        For NBA/NFL: Q1/Q2/Q3 both score.
        """
        sport = self.config.sport
        if sport == "nhl":
            period_cols = [("p1", "btts_period1"), ("p2", "btts_period2"), ("p3", "btts_period3")]
        else:
            period_cols = [("q1", "btts_period1"), ("q2", "btts_period2"), ("q3", "btts_period3")]

        for period_name, model_name in period_cols:
            hcol, acol = f"home_{period_name}", f"away_{period_name}"
            if hcol not in train_df.columns or acol not in train_df.columns:
                continue
            mask_tr = train_df[hcol].notna() & train_df[acol].notna()
            mask_va = val_df[hcol].notna() & val_df[acol].notna()
            n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
            if n_tr < 80 or n_va < 20:
                continue
            h_tr = train_df.loc[mask_tr, hcol].fillna(0)
            a_tr = train_df.loc[mask_tr, acol].fillna(0)
            h_va = val_df.loc[mask_va, hcol].fillna(0)
            a_va = val_df.loc[mask_va, acol].fillna(0)
            # Skip if period data is all-zero (no actual scores recorded)
            if h_tr.sum() == 0 and a_tr.sum() == 0:
                continue
            y_tr = ((h_tr > 0) & (a_tr > 0)).astype(int)
            y_va = ((h_va > 0) & (a_va > 0)).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue  # trivial target
            m = self._fit_cls_safe(model_name, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[model_name] = m
                logger.info("  %s fitted (btts_rate=%.1f%%)", model_name, 100.0 * float(y_tr.mean()))

    def _train_corners_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train corners total over/under market classifiers for soccer.

        Requires home_corners_total + away_corners_total in feature data.
        Popular lines: over 9.5, over 10.5, over 11.5.
        """
        if "home_corners_total" not in train_df.columns or "away_corners_total" not in train_df.columns:
            logger.debug("Corners total columns missing — skipping corners market")
            return

        mask_tr = train_df["home_corners_total"].notna() & train_df["away_corners_total"].notna()
        mask_va = val_df["home_corners_total"].notna() & val_df["away_corners_total"].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient corners data (%d/%d rows) — skipping", n_tr, n_va)
            return

        total_tr = train_df.loc[mask_tr, "home_corners_total"].fillna(0) + \
                   train_df.loc[mask_tr, "away_corners_total"].fillna(0)
        total_va = val_df.loc[mask_va, "home_corners_total"].fillna(0) + \
                   val_df.loc[mask_va, "away_corners_total"].fillna(0)

        logger.info("Training corners markets (n=%d, mean_total=%.1f) …", n_tr, float(total_tr.mean()))
        for line, mname in [(9.5, "corners_over9_5"), (10.5, "corners_over10_5"), (11.5, "corners_over11_5")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                logger.info("  %s fitted (over_rate=%.1f%%)", mname, 100.0 * float(y_tr.mean()))

    def _train_cards_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train total cards (yellow + red) over/under classifiers for soccer.

        Requires home_yellow_total / away_yellow_total in feature data.
        Popular lines: over 3.5, over 4.5, over 5.5 total cards.
        """
        if "home_yellow_total" not in train_df.columns or "away_yellow_total" not in train_df.columns:
            logger.debug("Yellow card columns missing — skipping cards market")
            return

        mask_tr = train_df["home_yellow_total"].notna() & train_df["away_yellow_total"].notna()
        mask_va = val_df["home_yellow_total"].notna() & val_df["away_yellow_total"].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient cards data (%d/%d rows) — skipping", n_tr, n_va)
            return

        y_total_tr = train_df.loc[mask_tr, "home_yellow_total"].fillna(0) + \
                     train_df.loc[mask_tr, "away_yellow_total"].fillna(0)
        y_total_va = val_df.loc[mask_va, "home_yellow_total"].fillna(0) + \
                     val_df.loc[mask_va, "away_yellow_total"].fillna(0)
        # Red cards count as 2 (bookmaker standard for cards total markets)
        if "home_red_total" in train_df.columns:
            y_total_tr = y_total_tr + \
                train_df.loc[mask_tr, "home_red_total"].fillna(0) * 2 + \
                train_df.loc[mask_tr, "away_red_total"].fillna(0) * 2
            y_total_va = y_total_va + \
                val_df.loc[mask_va, "home_red_total"].fillna(0) * 2 + \
                val_df.loc[mask_va, "away_red_total"].fillna(0) * 2

        logger.info("Training cards markets (n=%d, mean_total=%.1f) …", n_tr, float(y_total_tr.mean()))
        for line, mname in [(3.5, "cards_over3_5"), (4.5, "cards_over4_5"), (5.5, "cards_over5_5")]:
            y_tr = (y_total_tr > line).astype(int)
            y_va = (y_total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                logger.info("  %s fitted (over_rate=%.1f%%)", mname, 100.0 * float(y_tr.mean()))

    def _train_three_pointer_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NBA/WNBA/NCAAB three-pointer total over/under classifiers.

        Lines: over 20.5, 23.5, 26.5 (NBA averages ~24-26 combined 3PM/game).
        """
        col_h, col_a = "home_three_m_game", "away_three_m_game"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("Three-pointer game columns missing — skipping 3P market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient 3P data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        median_3p = float(total_tr.median())
        logger.info("Training three-pointer markets (n=%d, median_total=%.1f) …", n_tr, median_3p)
        # Dynamically pick lines around the median
        low_line = round(median_3p - 2.5)
        mid_line = round(median_3p)
        high_line = round(median_3p + 2.5)
        for line, mname in [(low_line - 0.5, "threes_over_low"), (mid_line - 0.5, "threes_over_mid"), (high_line - 0.5, "threes_over_high")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

    def _train_shots_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NHL total shots on goal over/under classifiers.

        Lines: over 55.5, 60.5, 65.5 (NHL combined shots avg ~60/game).
        """
        col_h, col_a = "home_shots_game", "away_shots_game"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("Shots game columns missing — skipping shots market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient shots data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        median_shots = float(total_tr.median())
        logger.info("Training shots markets (n=%d, median_total=%.1f) …", n_tr, median_shots)
        low_line = round(median_shots - 5) - 0.5
        mid_line = median_shots - 0.5
        high_line = round(median_shots + 5) - 0.5
        for line, mname in [(low_line, "shots_over_low"), (mid_line, "shots_over_mid"), (high_line, "shots_over_high")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

        # Home team shots advantage (more shots than opponent)
        home_shots_tr = train_df.loc[mask_tr, col_h].fillna(0)
        away_shots_tr = train_df.loc[mask_tr, col_a].fillna(0)
        home_shots_va = val_df.loc[mask_va, col_h].fillna(0)
        away_shots_va = val_df.loc[mask_va, col_a].fillna(0)
        y_ha_tr = (home_shots_tr > away_shots_tr).astype(int)
        y_ha_va = (home_shots_va > away_shots_va).astype(int)
        if 0.10 < float(y_ha_tr.mean()) < 0.90:
            m = self._fit_cls_safe("home_shots_advantage", X_train.loc[mask_tr], y_ha_tr, X_val.loc[mask_va], y_ha_va)
            if m:
                models["home_shots_advantage"] = m
                logger.info("  home_shots_advantage fitted (rate=%.1f%%)", 100.0 * float(y_ha_tr.mean()))

    def _train_hits_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train MLB total hits over/under classifiers.

        Lines: over 14.5, 16.5, 18.5 (MLB combined hits avg ~15-17/game).
        """
        col_h, col_a = "home_hits_game", "away_hits_game"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("Hits game columns missing — skipping hits market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient hits data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        logger.info("Training hits markets (n=%d, mean_total=%.1f) …", n_tr, float(total_tr.mean()))
        for line, mname in [(14.5, "hits_over14_5"), (16.5, "hits_over16_5"), (18.5, "hits_over18_5")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                logger.info("  %s fitted (over_rate=%.1f%%)", mname, 100.0 * float(y_tr.mean()))

    def _train_soccer_h2_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train soccer second-half goals over/under classifiers.

        Markets: H2 over 0.5, over 1.5, over 2.5 goals.
        Second-half scoring is often higher than first-half (teams open up).
        """
        col_h, col_a = "home_h2", "away_h2"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("H2 score columns missing — skipping H2 goals market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient H2 data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        logger.info("Training soccer H2 goals markets (n=%d, mean_H2=%.2f) …", n_tr, float(total_tr.mean()))
        for line, mname in [(0.5, "soccer_h2_over0_5"), (1.5, "soccer_h2_over1_5"), (2.5, "soccer_h2_over2_5")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                logger.info("  %s fitted (over_rate=%.1f%%)", mname, 100.0 * float(y_tr.mean()))

    def _train_rebounds_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NBA/WNBA total rebounds over/under classifiers.

        Lines are set dynamically around the median (NBA ~85-90 combined/game).
        """
        col_h, col_a = "home_reb_game", "away_reb_game"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("Rebounds game columns missing — skipping rebounds market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient rebounds data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        median_r = float(total_tr.median())
        logger.info("Training rebounds markets (n=%d, median_total=%.1f) …", n_tr, median_r)
        low_line = round(median_r - 3.5)
        mid_line = round(median_r)
        high_line = round(median_r + 3.5)
        for line, mname in [(low_line - 0.5, "rebounds_over_low"), (mid_line - 0.5, "rebounds_over_mid"), (high_line - 0.5, "rebounds_over_high")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

    def _train_turnovers_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NBA/WNBA total turnovers over/under classifiers.

        Lines are set dynamically around the median (NBA ~27-30 combined/game).
        """
        col_h, col_a = "home_to_game", "away_to_game"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("Turnovers game columns missing — skipping turnovers market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient turnovers data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        median_to = float(total_tr.median())
        logger.info("Training turnovers markets (n=%d, median_total=%.1f) …", n_tr, median_to)
        low_line = round(median_to - 2.5)
        mid_line = round(median_to)
        high_line = round(median_to + 2.5)
        for line, mname in [(low_line - 0.5, "turnovers_over_low"), (mid_line - 0.5, "turnovers_over_mid"), (high_line - 0.5, "turnovers_over_high")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

    def _train_assists_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NBA/WNBA total assists over/under classifiers.

        Lines are set dynamically around the median (NBA ~48-52 combined/game).
        """
        col_h, col_a = "home_ast_game", "away_ast_game"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("Assists game columns missing — skipping assists market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient assists data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        median_ast = float(total_tr.median())
        logger.info("Training assists markets (n=%d, median_total=%.1f) …", n_tr, median_ast)
        low_line = round(median_ast - 3.0)
        mid_line = round(median_ast)
        high_line = round(median_ast + 3.0)
        for line, mname in [(low_line - 0.5, "assists_over_low"), (mid_line - 0.5, "assists_over_mid"), (high_line - 0.5, "assists_over_high")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

    def _train_nhl_period_goals_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NHL period-by-period goals over/under classifiers.

        For each of the 3 NHL periods trains an O/U on total goals in that period.
        Dynamic lines around the median (NHL ~1.0-1.3 goals/period on average).
        """
        for period, col_h, col_a in [("p1", "home_p1", "away_p1"), ("p2", "home_p2", "away_p2"), ("p3", "home_p3", "away_p3")]:
            if col_h not in train_df.columns or col_a not in train_df.columns:
                logger.debug("NHL period %s columns missing — skipping", period)
                continue
            mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
            mask_va = val_df[col_h].notna() & val_df[col_a].notna()
            n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
            if n_tr < 80 or n_va < 20:
                logger.debug("Insufficient NHL %s data (%d/%d rows) — skipping", period, n_tr, n_va)
                continue
            total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
            total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
            median_g = float(total_tr.median())
            logger.info("Training NHL %s goals market (n=%d, median=%.1f) …", period, n_tr, median_g)
            for offset, tier in [(-0.5, "low"), (0.0, "mid"), (0.5, "high")]:
                line = max(0.5, round(median_g + offset, 1))
                mname = f"nhl_{period}_goals_over_{tier}"
                y_tr = (total_tr > line).astype(int)
                y_va = (total_va > line).astype(int)
                if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                    continue
                m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
                if m:
                    models[mname] = m
                    models[f"{mname}_line"] = line  # type: ignore[assignment]
                    logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

    def _train_soccer_total_shots_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train soccer total shots O/U classifiers.

        EPL has 100% fill on home_shots/away_shots. Dynamic lines ~22-26 total shots/game.
        """
        col_h, col_a = "home_shots", "away_shots"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("Soccer shots columns missing — skipping total shots market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient shots data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        median_sh = float(total_tr.median())
        logger.info("Training soccer total shots market (n=%d, median=%.1f) …", n_tr, median_sh)
        low_line = round(median_sh - 4.0)
        mid_line = round(median_sh)
        high_line = round(median_sh + 4.0)
        for line, mname in [(low_line - 0.5, "shots_total_over_low"), (mid_line - 0.5, "shots_total_over_mid"), (high_line - 0.5, "shots_total_over_high")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

    def _train_ufc_rounds_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train UFC total rounds O/U classifiers.

        Lines: O/U 1.5 (fight ends round 1 vs continues), O/U 2.5 (goes 3+ rounds).
        home_finish_round = 1,2,3,4,5 where max = decision.
        """
        col = "home_finish_round"
        if col not in train_df.columns:
            logger.debug("UFC finish_round column missing — skipping rounds market")
            return
        mask_tr = train_df[col].notna()
        mask_va = val_df[col].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient UFC rounds data (%d/%d rows) — skipping", n_tr, n_va)
            return
        rounds_tr = train_df.loc[mask_tr, col]
        rounds_va = val_df.loc[mask_va, col]
        logger.info("Training UFC rounds markets (n=%d) …", n_tr)
        for line, mname in [(1.5, "ufc_rounds_over_1.5"), (2.5, "ufc_rounds_over_2.5"), (3.5, "ufc_rounds_over_3.5")]:
            y_tr = (rounds_tr > line).astype(int)
            y_va = (rounds_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

    def _train_nfl_q1_total_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NFL/NCAAF first quarter total points O/U classifiers.

        NFL has 100% fill on home_q1/away_q1. Dynamic lines ~10-14 first quarter points.
        """
        col_h, col_a = "home_q1", "away_q1"
        if col_h not in train_df.columns or col_a not in train_df.columns:
            logger.debug("NFL q1 columns missing — skipping q1 total market")
            return
        mask_tr = train_df[col_h].notna() & train_df[col_a].notna()
        mask_va = val_df[col_h].notna() & val_df[col_a].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("Insufficient NFL q1 data (%d/%d rows) — skipping", n_tr, n_va)
            return
        total_tr = train_df.loc[mask_tr, col_h].fillna(0) + train_df.loc[mask_tr, col_a].fillna(0)
        total_va = val_df.loc[mask_va, col_h].fillna(0) + val_df.loc[mask_va, col_a].fillna(0)
        median_q1 = float(total_tr.median())
        logger.info("Training NFL Q1 total market (n=%d, median=%.1f) …", n_tr, median_q1)
        low_line = round(median_q1 - 3.0)
        mid_line = round(median_q1)
        high_line = round(median_q1 + 3.0)
        for line, mname in [(low_line - 0.5, "q1_total_over_low"), (mid_line - 0.5, "q1_total_over_mid"), (high_line - 0.5, "q1_total_over_high")]:
            y_tr = (total_tr > line).astype(int)
            y_va = (total_va > line).astype(int)
            if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

    def _train_btts_both_halves(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Soccer: both teams score in EACH half (more restrictive than BTTS overall).

        Requires home_h1, away_h1, home_h2, away_h2 columns.
        btts_both_halves = (h1_home > 0) & (h1_away > 0) & (h2_home > 0) & (h2_away > 0)
        """
        needed = ["home_h1", "away_h1", "home_h2", "away_h2"]
        if not all(c in train_df.columns for c in needed):
            logger.debug("BTTS-both-halves: missing columns — skipping")
            return
        mask_tr = train_df[needed].notna().all(axis=1)
        mask_va = val_df[needed].notna().all(axis=1)
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            logger.debug("BTTS-both-halves: insufficient data (%d/%d) — skipping", n_tr, n_va)
            return
        h1h_tr = train_df.loc[mask_tr, "home_h1"].fillna(0)
        h1a_tr = train_df.loc[mask_tr, "away_h1"].fillna(0)
        h2h_tr = train_df.loc[mask_tr, "home_h2"].fillna(0)
        h2a_tr = train_df.loc[mask_tr, "away_h2"].fillna(0)
        h1h_va = val_df.loc[mask_va, "home_h1"].fillna(0)
        h1a_va = val_df.loc[mask_va, "away_h1"].fillna(0)
        h2h_va = val_df.loc[mask_va, "home_h2"].fillna(0)
        h2a_va = val_df.loc[mask_va, "away_h2"].fillna(0)

        y_tr = ((h1h_tr > 0) & (h1a_tr > 0) & (h2h_tr > 0) & (h2a_tr > 0)).astype(int)
        y_va = ((h1h_va > 0) & (h1a_va > 0) & (h2h_va > 0) & (h2a_va > 0)).astype(int)
        if y_tr.mean() < 0.05 or y_tr.mean() > 0.95:
            logger.debug("BTTS-both-halves: degenerate class balance — skipping")
            return
        logger.info("Training btts_both_halves market (n=%d, rate=%.1f%%) …", n_tr, 100.0 * float(y_tr.mean()))
        m = self._fit_cls_safe("btts_both_halves", X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
        if m:
            models["btts_both_halves"] = m
            logger.info("  btts_both_halves fitted (rate=%.1f%%)", 100.0 * float(y_tr.mean()))

        # Also train first-half-only BTTS and second-half-only BTTS
        y_h1btts_tr = ((h1h_tr > 0) & (h1a_tr > 0)).astype(int)
        y_h1btts_va = ((h1h_va > 0) & (h1a_va > 0)).astype(int)
        if 0.05 < float(y_h1btts_tr.mean()) < 0.95:
            m2 = self._fit_cls_safe("btts_first_half", X_train.loc[mask_tr], y_h1btts_tr, X_val.loc[mask_va], y_h1btts_va)
            if m2:
                models["btts_first_half"] = m2
                logger.info("  btts_first_half fitted (rate=%.1f%%)", 100.0 * float(y_h1btts_tr.mean()))

        y_h2btts_tr = ((h2h_tr > 0) & (h2a_tr > 0)).astype(int)
        y_h2btts_va = ((h2h_va > 0) & (h2a_va > 0)).astype(int)
        if 0.05 < float(y_h2btts_tr.mean()) < 0.95:
            m3 = self._fit_cls_safe("btts_second_half", X_train.loc[mask_tr], y_h2btts_tr, X_val.loc[mask_va], y_h2btts_va)
            if m3:
                models["btts_second_half"] = m3
                logger.info("  btts_second_half fitted (rate=%.1f%%)", 100.0 * float(y_h2btts_tr.mean()))

    def _train_htft_double_result(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """HT/FT double-result: predict both halftime and full-time outcomes.

        Trains 6 binary classifiers for the most-bet HT/FT combinations:
          htft_HH, htft_DH, htft_AH (away-to-home comeback)
          htft_HA (home-to-away comeback), htft_DA, htft_AA
        Soccer uses home_h1/away_h1; 4-quarter sports use home_q1+home_q2.
        """
        sport = self.config.sport
        if sport in _SOCCER_HALF_SPORTS:
            ht_h_col, ht_a_col = "home_h1", "away_h1"
            required = [ht_h_col, ht_a_col, "home_score", "away_score"]
            if not all(c in train_df.columns for c in required):
                return
            mask_tr = train_df[required].notna().all(axis=1)
            mask_va = val_df[required].notna().all(axis=1)
            ht_home_tr = train_df.loc[mask_tr, ht_h_col].fillna(0)
            ht_away_tr = train_df.loc[mask_tr, ht_a_col].fillna(0)
            ht_home_va = val_df.loc[mask_va, ht_h_col].fillna(0)
            ht_away_va = val_df.loc[mask_va, ht_a_col].fillna(0)
        else:
            # 4-quarter sports: halftime = Q1+Q2
            needed = ["home_q1", "away_q1", "home_q2", "away_q2", "home_score", "away_score"]
            if not all(c in train_df.columns for c in needed):
                return
            mask_tr = train_df[needed].notna().all(axis=1)
            mask_va = val_df[needed].notna().all(axis=1)
            ht_home_tr = (train_df.loc[mask_tr, "home_q1"].fillna(0) + train_df.loc[mask_tr, "home_q2"].fillna(0))
            ht_away_tr = (train_df.loc[mask_tr, "away_q1"].fillna(0) + train_df.loc[mask_tr, "away_q2"].fillna(0))
            ht_home_va = (val_df.loc[mask_va, "home_q1"].fillna(0) + val_df.loc[mask_va, "home_q2"].fillna(0))
            ht_away_va = (val_df.loc[mask_va, "away_q1"].fillna(0) + val_df.loc[mask_va, "away_q2"].fillna(0))

        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 80 or n_va < 20:
            return

        ft_home_tr = train_df.loc[mask_tr, "home_score"].fillna(0)
        ft_away_tr = train_df.loc[mask_tr, "away_score"].fillna(0)
        ft_home_va = val_df.loc[mask_va, "home_score"].fillna(0)
        ft_away_va = val_df.loc[mask_va, "away_score"].fillna(0)

        # HT result: H=1, D=0, A=-1
        ht_res_tr = (ht_home_tr > ht_away_tr).astype(int) - (ht_away_tr > ht_home_tr).astype(int)
        ht_res_va = (ht_home_va > ht_away_va).astype(int) - (ht_away_va > ht_home_va).astype(int)
        # FT result
        ft_res_tr = (ft_home_tr > ft_away_tr).astype(int) - (ft_away_tr > ft_home_tr).astype(int)
        ft_res_va = (ft_home_va > ft_away_va).astype(int) - (ft_away_va > ft_home_va).astype(int)

        logger.info("Training HT/FT double-result markets (n=%d) …", n_tr)
        combos = [
            ("htft_HH", 1, 1),   # Home leads HT, Home wins FT
            ("htft_DH", 0, 1),   # Draw at HT, Home wins FT
            ("htft_AH", -1, 1),  # Away leads HT, Home wins FT (comeback)
            ("htft_HA", 1, -1),  # Home leads HT, Away wins FT (collapse)
            ("htft_DA", 0, -1),  # Draw at HT, Away wins FT
            ("htft_AA", -1, -1), # Away leads HT, Away wins FT
            ("htft_DD", 0, 0),   # Draw at HT, Draw at FT (soccer only)
        ]
        for mname, ht_val, ft_val in combos:
            y_tr = ((ht_res_tr == ht_val) & (ft_res_tr == ft_val)).astype(int)
            y_va = ((ht_res_va == ht_val) & (ft_res_va == ft_val)).astype(int)
            rate = float(y_tr.mean())
            if rate < 0.03 or rate > 0.97:
                continue  # Skip degenerate/impossible outcomes
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                logger.info("  %s fitted (rate=%.1f%%)", mname, 100.0 * rate)

    def _train_q4_total_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Q4 (last quarter) total points O/U market for basketball/football.

        Uses home_q4/away_q4 columns. Trains classifiers at dynamic lines
        derived from the training-set median and ±3-point offsets.
        Also trains a Q3 total market and a second-half total O/U.
        """
        sport = self.config.sport
        # Determine last-period column names based on sport
        if sport in ("nfl", "ncaaf"):
            q_last_h, q_last_a = "home_q4", "away_q4"
            q3_h, q3_a = "home_q3", "away_q3"
        else:
            q_last_h, q_last_a = "home_q4", "away_q4"
            q3_h, q3_a = "home_q3", "away_q3"

        def _train_period_ou(h_col: str, a_col: str, prefix: str) -> None:
            if h_col not in train_df.columns or a_col not in train_df.columns:
                return
            mask_tr = train_df[h_col].notna() & train_df[a_col].notna()
            mask_va = val_df[h_col].notna() & val_df[a_col].notna()
            n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
            if n_tr < 80 or n_va < 20:
                return
            tot_tr = train_df.loc[mask_tr, h_col].fillna(0) + train_df.loc[mask_tr, a_col].fillna(0)
            tot_va = val_df.loc[mask_va, h_col].fillna(0) + val_df.loc[mask_va, a_col].fillna(0)
            median_tot = float(tot_tr.median())
            logger.info("Training %s total market (n=%d, median=%.1f) …", prefix, n_tr, median_tot)
            # O/U at three lines (winner already handled by period model section)
            for offset, mname in [(-3.0, f"{prefix}_over_low"), (0.0, f"{prefix}_over_mid"), (3.0, f"{prefix}_over_high")]:
                line = round(median_tot + offset) - 0.5
                y_tr = (tot_tr > line).astype(int)
                y_va = (tot_va > line).astype(int)
                if float(y_tr.mean()) < 0.05 or float(y_tr.mean()) > 0.95:
                    continue
                m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
                if m:
                    models[mname] = m
                    models[f"{mname}_line"] = line  # type: ignore[assignment]
                    logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100.0 * float(y_tr.mean()))

        _train_period_ou(q_last_h, q_last_a, "q4")
        _train_period_ou(q3_h, q3_a, "q3")

    def _train_nfl_turnovers_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NFL/NCAAF total turnovers O/U market.

        Uses home_turnovers_game + away_turnovers_game (INT + fumbles lost)
        extracted from player_stats aggregation in football.py.
        Trains classifiers at median-1, median, median+1 lines.
        Also trains which team commits more turnovers (home vs away).
        """
        h_col, a_col = "home_turnovers_game", "away_turnovers_game"
        if h_col not in train_df.columns or a_col not in train_df.columns:
            logger.info("Turnovers market: columns %s/%s not in train_df — skipping", h_col, a_col)
            return

        mask_tr = train_df[h_col].notna() & train_df[a_col].notna()
        mask_va = val_df[h_col].notna() & val_df[a_col].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 100 or n_va < 20:
            logger.info("Turnovers market: insufficient data (%d/%d) — skipping", n_tr, n_va)
            return

        tot_tr = train_df.loc[mask_tr, h_col].fillna(0) + train_df.loc[mask_tr, a_col].fillna(0)
        tot_va = val_df.loc[mask_va, h_col].fillna(0) + val_df.loc[mask_va, a_col].fillna(0)
        median_tot = float(tot_tr.median())
        logger.info("Training NFL turnovers market (n=%d, median=%.1f) …", n_tr, median_tot)

        # O/U at three lines around median
        for offset, mname in [(-1.0, "turnovers_over_low"), (0.0, "turnovers_over_mid"), (1.0, "turnovers_over_high")]:
            line = round(median_tot + offset) - 0.5
            y_tr = (tot_tr > line).astype(int)
            y_va = (tot_va > line).astype(int)
            if float(y_tr.mean()) < 0.05 or float(y_tr.mean()) > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_tr, X_val.loc[mask_va], y_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) over_rate=%.1f%%", mname, line, 100.0 * float(y_tr.mean()))

        # Which team turns it over more (home commits more than away)
        h_tv_tr = train_df.loc[mask_tr, h_col].fillna(0)
        a_tv_tr = train_df.loc[mask_tr, a_col].fillna(0)
        h_tv_va = val_df.loc[mask_va, h_col].fillna(0)
        a_tv_va = val_df.loc[mask_va, a_col].fillna(0)
        y_more_tr = (h_tv_tr > a_tv_tr).astype(int)
        y_more_va = (h_tv_va > a_tv_va).astype(int)
        if y_more_tr.mean() > 0.05 and y_more_tr.mean() < 0.95:
            m = self._fit_cls_safe("home_more_turnovers", X_train.loc[mask_tr], y_more_tr, X_val.loc[mask_va], y_more_va)
            if m:
                models["home_more_turnovers"] = m
                logger.info("  home_more_turnovers fitted (home_rate=%.1f%%)", 100.0 * float(y_more_tr.mean()))

    def _train_close_game_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train close-game and blowout classifiers.

        close_game:   P(margin of victory ≤ sport-specific 'close' threshold)
        blowout_win:  P(margin of victory > sport-specific 'blowout' threshold)
        one_score_game (NFL only): P(final margin ≤ 8 points, e.g., one TD+2pt)
        """
        sport = self.config.sport
        # (close_thresh, blowout_thresh)
        thresholds = {
            "nba": (10, 20), "wnba": (10, 20), "ncaab": (10, 20), "ncaaw": (10, 20),
            "nfl": (7, 17), "ncaaf": (7, 17),
        }
        if sport not in thresholds:
            return
        close_t, blowout_t = thresholds[sport]

        hs_tr = train_df.get("home_score", pd.Series(dtype=float))
        as_tr = train_df.get("away_score", pd.Series(dtype=float))
        hs_va = val_df.get("home_score", pd.Series(dtype=float))
        as_va = val_df.get("away_score", pd.Series(dtype=float))
        valid_tr = hs_tr.notna() & as_tr.notna()
        valid_va = hs_va.notna() & as_va.notna()
        if valid_tr.sum() < 100 or valid_va.sum() < 20:
            return

        margin_tr = (hs_tr[valid_tr] - as_tr[valid_tr]).abs()
        margin_va = (hs_va[valid_va] - as_va[valid_va]).abs()
        logger.info("Training close-game / blowout markets (n=%d) …", int(valid_tr.sum()))

        # Close game
        y_close_tr = (margin_tr <= close_t).astype(int)
        y_close_va = (margin_va <= close_t).astype(int)
        if 0.10 < float(y_close_tr.mean()) < 0.90:
            m = self._fit_cls_safe("close_game", X_train.loc[valid_tr], y_close_tr, X_val.loc[valid_va], y_close_va)
            if m:
                models["close_game"] = m
                models["close_game_thresh"] = close_t  # type: ignore[assignment]
                logger.info("  close_game (margin≤%d) fitted (rate=%.1f%%)", close_t, 100 * float(y_close_tr.mean()))

        # Blowout
        y_blow_tr = (margin_tr > blowout_t).astype(int)
        y_blow_va = (margin_va > blowout_t).astype(int)
        if 0.05 < float(y_blow_tr.mean()) < 0.90:
            m = self._fit_cls_safe("blowout_win", X_train.loc[valid_tr], y_blow_tr, X_val.loc[valid_va], y_blow_va)
            if m:
                models["blowout_win"] = m
                logger.info("  blowout_win (margin>%d) fitted (rate=%.1f%%)", blowout_t, 100 * float(y_blow_tr.mean()))

        # NFL one-score game (≤8)
        if sport in {"nfl", "ncaaf"}:
            y_one_tr = (margin_tr <= 8).astype(int)
            y_one_va = (margin_va <= 8).astype(int)
            if 0.10 < float(y_one_tr.mean()) < 0.90:
                m = self._fit_cls_safe("one_score_game", X_train.loc[valid_tr], y_one_tr, X_val.loc[valid_va], y_one_va)
                if m:
                    models["one_score_game"] = m
                    logger.info("  one_score_game (margin≤8) fitted (rate=%.1f%%)", 100 * float(y_one_tr.mean()))

    def _train_nfl_second_half_total(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train NFL/NCAAF second-half total points O/U market.

        Second half = Q3 + Q4 combined total. Dynamic lines around median.
        Also trains a 'scoring surge' market: 2nd half > 1st half total.
        """
        q3_h, q3_a = "home_q3", "away_q3"
        q4_h, q4_a = "home_q4", "away_q4"
        q1_h, q1_a = "home_q1", "away_q1"
        q2_h, q2_a = "home_q2", "away_q2"
        required = [q3_h, q3_a, q4_h, q4_a]
        if not all(c in train_df.columns for c in required):
            logger.debug("NFL 2H total: Q3/Q4 columns missing — skipping")
            return

        mask_tr = train_df[required].notna().all(axis=1)
        mask_va = val_df[required].notna().all(axis=1)
        if mask_tr.sum() < 100 or mask_va.sum() < 20:
            logger.debug("NFL 2H total: insufficient data (%d/%d) — skipping", int(mask_tr.sum()), int(mask_va.sum()))
            return

        logger.info("Training NFL second-half total market (%d/%d rows) …", int(mask_tr.sum()), int(mask_va.sum()))
        Xs_tr = X_train.loc[mask_tr]
        Xs_va = X_val.loc[mask_va]

        h2_tr = train_df.loc[mask_tr, q3_h].fillna(0) + train_df.loc[mask_tr, q4_h].fillna(0)
        a2_tr = train_df.loc[mask_tr, q3_a].fillna(0) + train_df.loc[mask_tr, q4_a].fillna(0)
        h2_va = val_df.loc[mask_va, q3_h].fillna(0) + val_df.loc[mask_va, q4_h].fillna(0)
        a2_va = val_df.loc[mask_va, q3_a].fillna(0) + val_df.loc[mask_va, q4_a].fillna(0)
        total2h_tr = h2_tr + a2_tr
        total2h_va = h2_va + a2_va
        median_2h = float(total2h_tr.median())

        for offset, mname in [(-3.5, "nfl_2h_over_low"), (0, "nfl_2h_over_mid"), (3.5, "nfl_2h_over_high")]:
            line = round(median_2h + offset) - 0.5
            y_tr = (total2h_tr > line).astype(int)
            y_va = (total2h_va > line).astype(int)
            if 0.10 < float(y_tr.mean()) < 0.90:
                m = self._fit_cls_safe(mname, Xs_tr, y_tr, Xs_va, y_va)
                if m:
                    models[mname] = m
                    models[f"{mname}_line"] = line  # type: ignore[assignment]
                    logger.info("  %s (line=%.1f) fitted (rate=%.1f%%)", mname, line, 100 * float(y_tr.mean()))

        # Scoring surge: 2nd half > 1st half total
        if all(c in train_df.columns for c in [q1_h, q1_a, q2_h, q2_a]):
            mask2_tr = mask_tr & train_df[[q1_h, q1_a, q2_h, q2_a]].notna().all(axis=1)
            mask2_va = mask_va & val_df[[q1_h, q1_a, q2_h, q2_a]].notna().all(axis=1)
            if mask2_tr.sum() >= 80 and mask2_va.sum() >= 20:
                h1_tr = train_df.loc[mask2_tr, q1_h].fillna(0) + train_df.loc[mask2_tr, q2_h].fillna(0)
                a1_tr = train_df.loc[mask2_tr, q1_a].fillna(0) + train_df.loc[mask2_tr, q2_a].fillna(0)
                h1_va = val_df.loc[mask2_va, q1_h].fillna(0) + val_df.loc[mask2_va, q2_h].fillna(0)
                a1_va = val_df.loc[mask2_va, q1_a].fillna(0) + val_df.loc[mask2_va, q2_a].fillna(0)
                total1h_tr = h1_tr + a1_tr
                total1h_va = h1_va + a1_va
                y_surge_tr = (h2_tr.loc[mask2_tr] + a2_tr.loc[mask2_tr] > total1h_tr).astype(int)
                y_surge_va = (h2_va.loc[mask2_va] + a2_va.loc[mask2_va] > total1h_va).astype(int)
                if 0.10 < float(y_surge_tr.mean()) < 0.90:
                    m = self._fit_cls_safe("nfl_scoring_surge", X_train.loc[mask2_tr], y_surge_tr, X_val.loc[mask2_va], y_surge_va)
                    if m:
                        models["nfl_scoring_surge"] = m
                        logger.info("  nfl_scoring_surge (2H>1H) fitted (rate=%.1f%%)", 100 * float(y_surge_tr.mean()))

    def _train_high_low_scoring(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train high-scoring vs low-scoring game classifier.

        For each sport, trains P(total > 90th pct) and P(total < 10th pct)
        as 'high_scoring' and 'low_scoring' markets.
        Also trains a 'both_score_high' market: both teams score over their median.
        """
        hs_tr = train_df.get("home_score", pd.Series(dtype=float))
        as_tr = train_df.get("away_score", pd.Series(dtype=float))
        hs_va = val_df.get("home_score", pd.Series(dtype=float))
        as_va = val_df.get("away_score", pd.Series(dtype=float))
        valid_tr = hs_tr.notna() & as_tr.notna()
        valid_va = hs_va.notna() & as_va.notna()
        if valid_tr.sum() < 120 or valid_va.sum() < 25:
            return

        total_tr = hs_tr[valid_tr] + as_tr[valid_tr]
        total_va = hs_va[valid_va] + as_va[valid_va]
        p90 = float(total_tr.quantile(0.90))
        p10 = float(total_tr.quantile(0.10))
        median_h = float(hs_tr[valid_tr].median())
        median_a = float(as_tr[valid_tr].median())

        logger.info("Training high/low scoring markets (n=%d, p10=%.1f, p90=%.1f) …", int(valid_tr.sum()), p10, p90)

        y_hi_tr = (total_tr > p90).astype(int)
        y_hi_va = (total_va > p90).astype(int)
        if 0.05 < float(y_hi_tr.mean()) < 0.50:
            m = self._fit_cls_safe("high_scoring", X_train.loc[valid_tr], y_hi_tr, X_val.loc[valid_va], y_hi_va)
            if m:
                models["high_scoring"] = m
                models["high_scoring_thresh"] = p90  # type: ignore[assignment]
                logger.info("  high_scoring (>%.1f) fitted (rate=%.1f%%)", p90, 100 * float(y_hi_tr.mean()))

        y_lo_tr = (total_tr < p10).astype(int)
        y_lo_va = (total_va < p10).astype(int)
        if 0.05 < float(y_lo_tr.mean()) < 0.50:
            m = self._fit_cls_safe("low_scoring", X_train.loc[valid_tr], y_lo_tr, X_val.loc[valid_va], y_lo_va)
            if m:
                models["low_scoring"] = m
                models["low_scoring_thresh"] = p10  # type: ignore[assignment]
                logger.info("  low_scoring (<%.1f) fitted (rate=%.1f%%)", p10, 100 * float(y_lo_tr.mean()))

        # Both teams score high
        y_both_tr = ((hs_tr[valid_tr] > median_h) & (as_tr[valid_tr] > median_a)).astype(int)
        y_both_va = ((hs_va[valid_va] > median_h) & (as_va[valid_va] > median_a)).astype(int)
        if 0.10 < float(y_both_tr.mean()) < 0.90:
            m = self._fit_cls_safe("both_score_high", X_train.loc[valid_tr], y_both_tr, X_val.loc[valid_va], y_both_va)
            if m:
                models["both_score_high"] = m
                logger.info("  both_score_high fitted (rate=%.1f%%)", 100 * float(y_both_tr.mean()))

    def _train_motorsport_markets(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train F1 / IndyCar race-outcome markets.

        Markets:
          motor_podium:       P(driver finishes top 3)
          motor_points:       P(driver finishes in points / top 10)
          motor_dnf:          P(driver DNF / retires)
          motor_fastest_lap:  P(driver sets race's fastest lap)
          motor_safety_car:   P(safety car is deployed, from race-level dnf_count signal)
        """
        def _fit_binary(col: str, mname: str) -> None:
            if col not in train_df.columns:
                logger.debug("Motorsport %s: column %s missing — skipping", mname, col)
                return
            valid_tr = train_df[col].notna()
            valid_va = val_df[col].notna()
            if valid_tr.sum() < 80 or valid_va.sum() < 20:
                logger.debug("Motorsport %s: insufficient data (%d/%d) — skipping", mname, int(valid_tr.sum()), int(valid_va.sum()))
                return
            y_tr = train_df.loc[valid_tr, col].astype(int)
            y_va = val_df.loc[valid_va, col].astype(int)
            if y_tr.mean() < 0.02 or y_tr.mean() > 0.98:
                logger.debug("Motorsport %s: degenerate rate %.1f%% — skipping", mname, 100 * y_tr.mean())
                return
            m = self._fit_cls_safe(mname, X_train.loc[valid_tr], y_tr, X_val.loc[valid_va], y_va)
            if m:
                models[mname] = m
                logger.info("  motorsport %s fitted (rate=%.1f%%)", mname, 100.0 * float(y_tr.mean()))

        logger.info("Training motorsport extra markets …")
        _fit_binary("podium", "motor_podium")
        _fit_binary("points_finish", "motor_points")
        _fit_binary("dnf", "motor_dnf")
        _fit_binary("fastest_lap", "motor_fastest_lap")

        # Safety car market from race-level dnf_count (driver-row level: proxy = any dnf_count > 1)
        if "dnf_count" in train_df.columns:
            valid_tr = train_df["dnf_count"].notna()
            valid_va = val_df["dnf_count"].notna()
            if valid_tr.sum() >= 80 and valid_va.sum() >= 20:
                dnf_median = float(train_df.loc[valid_tr, "dnf_count"].median())
                y_tr = (train_df.loc[valid_tr, "dnf_count"] > max(dnf_median, 3)).astype(int)
                y_va = (val_df.loc[valid_va, "dnf_count"] > max(dnf_median, 3)).astype(int)
                if 0.05 < y_tr.mean() < 0.95:
                    m = self._fit_cls_safe("motor_high_dnf", X_train.loc[valid_tr], y_tr, X_val.loc[valid_va], y_va)
                    if m:
                        models["motor_high_dnf"] = m
                        logger.info("  motor_high_dnf fitted (rate=%.1f%%)", 100.0 * float(y_tr.mean()))

    def _train_ot_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train overtime/extra-period probability classifier for NBA/WNBA/NHL.

        Uses ``home_ot > 0`` (or ``away_ot > 0``) as the OT indicator.
        Typical OT rates: NBA ~5-8%, NHL ~20-25%.
        """
        ot_col_h = "home_ot" if "home_ot" in train_df.columns else None
        ot_col_a = "away_ot" if "away_ot" in train_df.columns else None
        if ot_col_h is None or ot_col_a is None:
            logger.debug("OT market: no home_ot/away_ot columns — skipping")
            return

        valid_tr = train_df[ot_col_h].notna() | train_df[ot_col_a].notna()
        valid_va = val_df[ot_col_h].notna() | val_df[ot_col_a].notna()
        n_tr, n_va = int(valid_tr.sum()), int(valid_va.sum())
        if n_tr < 100 or n_va < 20:
            logger.debug("OT market: insufficient data (%d/%d) — skipping", n_tr, n_va)
            return

        y_tr = (
            train_df.loc[valid_tr, ot_col_h].fillna(0).gt(0)
            | train_df.loc[valid_tr, ot_col_a].fillna(0).gt(0)
        ).astype(int)
        y_va = (
            val_df.loc[valid_va, ot_col_h].fillna(0).gt(0)
            | val_df.loc[valid_va, ot_col_a].fillna(0).gt(0)
        ).astype(int)

        ot_rate = float(y_tr.mean())
        if ot_rate < 0.03 or ot_rate > 0.60:
            logger.debug("OT market: degenerate OT rate %.1f%% — skipping", 100 * ot_rate)
            return

        logger.info("Training OT probability market (n=%d, OT_rate=%.1f%%) …", n_tr, 100 * ot_rate)
        m = self._fit_cls_safe("overtime_prob", X_train.loc[valid_tr], y_tr, X_val.loc[valid_va], y_va)
        if m:
            models["overtime_prob"] = m
            logger.info("  overtime_prob fitted (rate=%.1f%%)", 100 * ot_rate)

    def _train_q1_market(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train first-quarter winner + first-quarter total O/U for basketball.

        For two-half sports (NCAAB/NCAAW), uses home_h1/away_h1 as the first half.
        For four-quarter sports (NBA/WNBA), uses home_q1/away_q1.
        Markets trained:
          q1_winner       : P(home team wins first quarter)
          q1_total_over   : P(Q1 total > dynamic line)
        """
        sport = self.config.sport
        if sport in {"ncaab", "ncaaw"}:
            h_col, a_col = "home_h1", "away_h1"
        else:
            h_col, a_col = "home_q1", "away_q1"

        if h_col not in train_df.columns or a_col not in train_df.columns:
            logger.debug("Q1 market: columns %s/%s missing — skipping", h_col, a_col)
            return

        mask_tr = train_df[h_col].notna() & train_df[a_col].notna()
        mask_va = val_df[h_col].notna() & val_df[a_col].notna()
        n_tr, n_va = int(mask_tr.sum()), int(mask_va.sum())
        if n_tr < 100 or n_va < 20:
            logger.debug("Q1 market: insufficient data (%d/%d) — skipping", n_tr, n_va)
            return

        hq1_tr = train_df.loc[mask_tr, h_col].fillna(0)
        aq1_tr = train_df.loc[mask_tr, a_col].fillna(0)
        hq1_va = val_df.loc[mask_va, h_col].fillna(0)
        aq1_va = val_df.loc[mask_va, a_col].fillna(0)

        logger.info("Training Q1/H1 markets (n=%d) …", n_tr)

        # Q1 winner
        y_win_tr = (hq1_tr > aq1_tr).astype(int)
        y_win_va = (hq1_va > aq1_va).astype(int)
        home_q1_win_rate = float(y_win_tr.mean())
        if 0.30 < home_q1_win_rate < 0.70:
            m = self._fit_cls_safe("q1_winner", X_train.loc[mask_tr], y_win_tr, X_val.loc[mask_va], y_win_va)
            if m:
                models["q1_winner"] = m
                logger.info("  q1_winner fitted (home_win_rate=%.1f%%)", 100 * home_q1_win_rate)

        # Q1 total O/U at three dynamic lines
        tot_tr = hq1_tr + aq1_tr
        tot_va = hq1_va + aq1_va
        median_tot = float(tot_tr.median())
        for offset, mname in [(-3.0, "q1_total_over_low"), (0.0, "q1_total_over_mid"), (3.0, "q1_total_over_high")]:
            line = round(median_tot + offset) - 0.5
            y_ou_tr = (tot_tr > line).astype(int)
            y_ou_va = (tot_va > line).astype(int)
            if float(y_ou_tr.mean()) < 0.05 or float(y_ou_tr.mean()) > 0.95:
                continue
            m = self._fit_cls_safe(mname, X_train.loc[mask_tr], y_ou_tr, X_val.loc[mask_va], y_ou_va)
            if m:
                models[mname] = m
                models[f"{mname}_line"] = line  # type: ignore[assignment]
                logger.info("  %s (line=%.1f) fitted (over_rate=%.1f%%)", mname, line, 100 * float(y_ou_tr.mean()))

    def train_separate(self) -> dict[str, Any]:
        """Train separate winner / total / spread models.

        Returns
        -------
        Dict with keys ``winner_ensemble``, ``total_ensemble``,
        ``spread_ensemble``, ``metrics``, ``feature_names``.
        """
        train_df, val_df = self.prepare_data()
        X_train, home_train, away_train = self._split_xy(train_df)
        X_val, home_val, away_val = self._split_xy(val_df)
        X_train, X_val = self._apply_variance_filter(X_train, X_val)

        y_train_cls = (home_train > away_train).astype(int)
        y_val_cls = (home_val > away_val).astype(int)

        total_train = home_train + away_train
        total_val = home_val + away_val
        spread_train = home_train - away_train
        spread_val = home_val - away_val

        logger.info("Training winner classifier …")
        winner_ensemble = EnsembleVoter()
        cls_metrics = winner_ensemble.fit_classifiers(X_train, y_train_cls, X_val, y_val_cls)

        # Log top feature importances
        try:
            fi = winner_ensemble.get_feature_importances()
            if fi:
                top10 = list(fi.items())[:10]
                logger.info("Top-10 winner features: %s", top10)
                fi_path = self.models_dir / "feature_importances.json"
                fi_path.write_text(json.dumps(fi, indent=2))
        except Exception:
            pass
        total_ensemble = EnsembleVoter()
        total_metrics = total_ensemble.fit_regressors(X_train, total_train, X_val, total_val)

        logger.info("Training spread regressor …")
        spread_ensemble = EnsembleVoter()
        spread_metrics = spread_ensemble.fit_regressors(X_train, spread_train, X_val, spread_val)

        result = {
            "winner_ensemble": winner_ensemble,
            "total_ensemble": total_ensemble,
            "spread_ensemble": spread_ensemble,
            "feature_names": list(X_train.columns),
            "metrics": {
                "classification": cls_metrics,
                "total_regression": total_metrics,
                "spread_regression": spread_metrics,
            },
            "config": self.config,
            "trained_at": datetime.utcnow().isoformat(),
        }

        self.save_models(result, self.models_dir / "separate_models.pkl")
        logger.info("Separate training complete ✓")
        return result

    # ── Data verification ────────────────────────────────

    def verify_data(self) -> dict[str, Any]:
        """Check data quality: nulls, class balance, feature ranges.

        Returns
        -------
        Dict with quality report (``ok`` is True when all checks pass).
        """
        issues: list[str] = []
        try:
            train_df, val_df = self.prepare_data()
        except RuntimeError as exc:
            return {"ok": False, "issues": [str(exc)]}

        full = pd.concat([train_df, val_df], ignore_index=True)
        X, home_score, away_score = self._split_xy(full)

        report: dict[str, Any] = {
            "sport": self.config.sport,
            "seasons": self.config.seasons,
            "n_samples": len(full),
            "n_features": X.shape[1],
            "train_size": len(train_df),
            "val_size": len(val_df),
        }

        # Null check
        null_pct = X.isnull().mean()
        high_null = null_pct[null_pct > 0.3].to_dict()
        if high_null:
            issues.append(f"{len(high_null)} features with >30% nulls: {list(high_null.keys())[:5]}")
        report["null_summary"] = {"max_null_pct": float(null_pct.max()), "high_null_features": high_null}

        # Class balance
        if len(home_score) > 0 and len(away_score) > 0:
            home_wins = (home_score > away_score).mean()
            report["class_balance"] = {"home_win_pct": float(home_wins)}
            if home_wins < 0.3 or home_wins > 0.7:
                issues.append(f"Severe class imbalance: home_win_pct={home_wins:.2f}")

        # Feature ranges
        ranges = X.describe().loc[["min", "max", "mean", "std"]].to_dict()
        inf_cols = [c for c in X.columns if np.isinf(X[c]).any()]
        if inf_cols:
            issues.append(f"Inf values in: {inf_cols[:5]}")
        report["inf_columns"] = inf_cols

        # Constant features
        constant = [c for c in X.columns if X[c].nunique() <= 1]
        if constant:
            issues.append(f"{len(constant)} constant features: {constant[:5]}")
        report["constant_features"] = constant

        report["issues"] = issues
        report["ok"] = len(issues) == 0
        return report

    # ── Walk-forward backtest ────────────────────────────

    def backtest(self, start_date: str, end_date: str) -> dict[str, Any]:
        """Walk-forward backtest with an expanding training window.

        For each test window the model is retrained on all data up to
        that point, then evaluated on the next chunk.

        Parameters
        ----------
        start_date, end_date : str  (``YYYY-MM-DD``)

        Returns
        -------
        Dict with per-window and aggregate metrics.
        """
        train_df, val_df = self.prepare_data()
        full = pd.concat([train_df, val_df], ignore_index=True)

        if "date" not in full.columns:
            raise RuntimeError("Backtest requires a 'date' column")

        full["date"] = pd.to_datetime(full["date"])
        mask = (full["date"] >= start_date) & (full["date"] <= end_date)
        backtest_data = full[mask].sort_values("date").reset_index(drop=True)

        if len(backtest_data) < self.config.min_samples:
            raise RuntimeError(
                f"Only {len(backtest_data)} games in backtest window — "
                f"need at least {self.config.min_samples}"
            )

        # Walk-forward: 5 expanding windows
        n_folds = self.config.n_cv_folds
        fold_size = len(backtest_data) // n_folds
        results: list[dict[str, Any]] = []

        for fold in range(1, n_folds):
            train_end = fold * fold_size
            test_end = min(train_end + fold_size, len(backtest_data))
            if test_end <= train_end:
                break

            fold_train = backtest_data.iloc[:train_end]
            fold_test = backtest_data.iloc[train_end:test_end]

            X_tr, h_tr, a_tr = self._split_xy(fold_train)
            X_te, h_te, a_te = self._split_xy(fold_test)
            y_tr = (h_tr > a_tr).astype(int)
            y_te = (h_te > a_te).astype(int)

            if len(X_tr) < 50 or len(X_te) < 10:
                continue

            ensemble = EnsembleVoter()
            try:
                ensemble.fit_classifiers(X_tr, y_tr, X_te, y_te)
                probs, preds, details = ensemble.predict_class(X_te)
                accuracy = float(np.mean(preds == y_te.values))
                from sklearn.metrics import brier_score_loss

                brier = float(brier_score_loss(y_te.values, probs))
            except Exception:
                logger.error("Backtest fold %d failed", fold, exc_info=True)
                continue

            fold_result = {
                "fold": fold,
                "train_size": len(fold_train),
                "test_size": len(fold_test),
                "train_end_date": str(fold_train["date"].max().date()),
                "test_start_date": str(fold_test["date"].min().date()),
                "test_end_date": str(fold_test["date"].max().date()),
                "accuracy": accuracy,
                "brier_score": brier,
                "n_models": ensemble.n_classifiers,
            }
            results.append(fold_result)
            logger.info(
                "Fold %d: acc=%.4f  brier=%.4f  (%d train / %d test)",
                fold, accuracy, brier, len(fold_train), len(fold_test),
            )

        if not results:
            raise RuntimeError("No backtest folds completed successfully")

        avg_acc = np.mean([r["accuracy"] for r in results])
        avg_brier = np.mean([r["brier_score"] for r in results])

        return {
            "sport": self.config.sport,
            "start_date": start_date,
            "end_date": end_date,
            "n_folds": len(results),
            "folds": results,
            "avg_accuracy": float(avg_acc),
            "avg_brier_score": float(avg_brier),
        }

    # ── Model serialisation ──────────────────────────────

    @staticmethod
    def save_models(models: dict[str, Any], path: Path) -> None:
        """Pickle-serialise trained models to *path*."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as fh:
            pickle.dump(models, fh, protocol=pickle.HIGHEST_PROTOCOL)
        size_mb = path.stat().st_size / (1024 * 1024)
        logger.info("Models saved to %s (%.1f MB)", path, size_mb)

    @staticmethod
    def load_models(path: Path) -> dict[str, Any]:
        """Load previously trained models from *path*."""
        if not path.exists():
            raise FileNotFoundError(f"Model file not found: {path}")
        with open(path, "rb") as fh:
            bundle = pickle.load(fh)  # noqa: S301
        logger.info("Models loaded from %s", path)
        return bundle


# ── CLI ──────────────────────────────────────────────────


def _parse_seasons(raw: str) -> list[int]:
    """Parse ``'2023,2024,2025'`` into ``[2023, 2024, 2025]``."""
    return [int(s.strip()) for s in raw.split(",")]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ml.train",
        description="V5.0 ML training pipeline",
    )
    sub = parser.add_subparsers(dest="action", required=True)

    # ── train ────────────────────────────────────────────
    p_train = sub.add_parser("train", help="Train models for a single sport")
    p_train.add_argument("--sport", required=True, help="Sport key (e.g. nba, nfl)")
    p_train.add_argument("--seasons", required=True, help="Comma-separated seasons")
    p_train.add_argument("--mode", default="joint", choices=["joint", "separate"])
    p_train.add_argument("--test-size", type=float, default=0.2)
    p_train.add_argument("--min-samples", type=int, default=100,
                         help="Minimum training samples (default 100, lower for small sports)")

    # ── train-all ────────────────────────────────────────
    p_all = sub.add_parser("train-all", help="Train all registered sports")
    p_all.add_argument("--seasons", required=True, help="Comma-separated seasons")
    p_all.add_argument("--mode", default="joint", choices=["joint", "separate"])
    p_all.add_argument("--test-size", type=float, default=0.2)
    p_all.add_argument("--min-samples", type=int, default=100)

    # ── predict ──────────────────────────────────────────
    p_pred = sub.add_parser("predict", help="Generate predictions")
    p_pred.add_argument("--sport", required=True)
    p_pred.add_argument("--date", default=None,
                        help="YYYY-MM-DD or comma-separated dates (default: today)")

    # ── predict-props ────────────────────────────────────
    p_props = sub.add_parser("predict-props", help="Generate player prop predictions")
    p_props.add_argument("--sport", required=True)
    p_props.add_argument("--date", default=None,
                        help="YYYY-MM-DD or comma-separated dates (default: today)")

    # ── backtest ─────────────────────────────────────────
    p_bt = sub.add_parser("backtest", help="Walk-forward backtest")
    p_bt.add_argument("--sport", required=True)
    p_bt.add_argument("--seasons", required=True, help="Comma-separated seasons for data")
    p_bt.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    p_bt.add_argument("--end", required=True, help="End date YYYY-MM-DD")

    # ── verify ───────────────────────────────────────────
    p_ver = sub.add_parser("verify", help="Data quality check")
    p_ver.add_argument("--sport", required=True)
    p_ver.add_argument("--seasons", required=True, help="Comma-separated seasons")

    return parser


def _resolve_data_dir() -> Path:
    """Walk up from this file to find the project data directory."""
    here = Path(__file__).resolve().parent
    # Expected layout: …/v5.0/backend/ml/train.py
    backend_dir = here.parent
    data_dir = backend_dir.parent / "data"
    if not data_dir.exists():
        # Fallback: use config
        try:
            from config import get_settings

            return Path(get_settings().data_dir)
        except Exception:
            pass
    return data_dir


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = _build_parser()
    args = parser.parse_args(argv)
    data_dir = _resolve_data_dir()

    if args.action == "train":
        config = TrainingConfig(
            sport=args.sport.lower(),
            seasons=_parse_seasons(args.seasons),
            test_size=args.test_size,
            mode=args.mode,
            min_samples=args.min_samples,
        )
        trainer = Trainer(config, data_dir)
        if config.mode == "joint":
            result = trainer.train_joint()
        else:
            result = trainer.train_separate()
        _print_metrics(result.get("metrics", {}))

    elif args.action == "train-all":
        seasons = _parse_seasons(args.seasons)
        for sport in sorted(EXTRACTORS.keys()):
            logger.info("=" * 60)
            logger.info("Training %s", sport)
            logger.info("=" * 60)
            config = TrainingConfig(
                sport=sport,
                seasons=seasons,
                test_size=args.test_size,
                mode=args.mode,
                min_samples=args.min_samples,
            )
            try:
                trainer = Trainer(config, data_dir)
                if config.mode == "joint":
                    trainer.train_joint()
                else:
                    trainer.train_separate()
            except Exception:
                logger.error("Failed to train %s", sport, exc_info=True)

    elif args.action == "predict":
        from ml.predictors.game_predictor import GamePredictor

        raw_dates = args.date or date.today().isoformat()
        pred_dates = [d.strip() for d in raw_dates.split(",") if d.strip()]
        sport = args.sport.lower()
        models_dir = data_dir.parent / "ml" / "models" / sport
        predictor = GamePredictor(sport, models_dir, data_dir)

        for pred_date in pred_dates:
            predictions = predictor.predict_date(pred_date)
            for p in predictions:
                logger.info(
                    "%s vs %s  →  %s (conf=%.2f, consensus=%.0f%%)",
                    p.home_team,
                    p.away_team,
                    p.predicted_winner,
                    p.confidence,
                    p.consensus * 100,
                )

            # Save predictions to parquet for API access
            if predictions:
                import pyarrow as pa
                import pyarrow.parquet as pq
                from dataclasses import asdict

                norm_dir = data_dir / "normalized" / sport
                norm_dir.mkdir(parents=True, exist_ok=True)
                pred_path = norm_dir / "predictions.parquet"

                rows = []
                for p in predictions:
                    d = asdict(p)
                    d.pop("model_votes", None)
                    d["date"] = pred_date
                    rows.append(d)

                new_df = pd.DataFrame(rows)

                # Deduplicate: ESPN full names vs NHL abbreviated names
                import unicodedata

                def _norm_team(name: str) -> str:
                    nfkd = unicodedata.normalize("NFKD", str(name))
                    return nfkd.encode("ascii", "ignore").decode("ascii").lower().split()[0]

                seen: dict[tuple, int] = {}
                keep: list[int] = []
                for idx, row in new_df.iterrows():
                    key = (_norm_team(row["home_team"]), _norm_team(row["away_team"]))
                    if key in seen:
                        prev = seen[key]
                        prev_len = len(str(new_df.loc[prev, "home_team"])) + len(str(new_df.loc[prev, "away_team"]))
                        curr_len = len(str(row["home_team"])) + len(str(row["away_team"]))
                        if curr_len > prev_len:
                            keep.remove(prev)
                            keep.append(idx)
                            seen[key] = idx
                    else:
                        seen[key] = idx
                        keep.append(idx)
                new_df = new_df.loc[keep].reset_index(drop=True)

                # Append to existing predictions if any
                if pred_path.exists():
                    try:
                        old_df = pd.read_parquet(pred_path)
                        # Remove old predictions for same sport+date
                        old_df = old_df[old_df["date"] != pred_date]
                        new_df = pd.concat([old_df, new_df], ignore_index=True)
                    except Exception:
                        pass

                new_df.to_parquet(pred_path, index=False)
                logger.info("Saved %d predictions to %s", len(predictions), pred_path)

    elif args.action == "predict-props":
        from ml.predictors.props_predictor import PropsPredictor
        from dataclasses import asdict

        raw_dates = args.date or date.today().isoformat()
        pred_dates = [d.strip() for d in raw_dates.split(",") if d.strip()]
        sport = args.sport.lower()
        models_dir = data_dir.parent / "ml" / "models" / sport
        props_model_path = models_dir / "player_props.pkl"

        if not props_model_path.exists():
            logger.info("No player props model for %s — skipping", sport)
            sys.exit(0)

        predictor = PropsPredictor(sport, models_dir, data_dir)
        if not predictor.available_prop_types():
            logger.info("No prop types configured for %s — skipping", sport)
            sys.exit(0)

        norm_dir = data_dir / "normalized" / sport

        for pred_date in pred_dates:
            # Determine current season and load day's games
            from ml.predictors.game_predictor import GamePredictor
            pred_dt = pd.Timestamp(pred_date)
            season = GamePredictor._date_to_season_static(sport, pred_dt)

            day_games = pd.DataFrame()
            for s in [season, season + 1, season - 1]:
                games_path = norm_dir / f"games_{s}.parquet"
                if games_path.exists():
                    df = pd.read_parquet(games_path)
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"])
                        filtered = df[df["date"].dt.date.astype(str) == pred_date]
                        if not filtered.empty:
                            day_games = filtered
                            break

            if day_games.empty:
                logger.debug("No games for %s on %s — skipping date", sport, pred_date)
                continue

            # Load roster for team-based player discovery
            players_df = pd.DataFrame()
            for s in [season, season + 1, season - 1]:
                players_path = norm_dir / f"players_{s}.parquet"
                if players_path.exists():
                    try:
                        players_df = pd.read_parquet(players_path)
                        break
                    except Exception:
                        pass

            id_col = "game_id" if "game_id" in day_games.columns else "id"
            all_preds: list[dict] = []

            game_work: list[tuple[str, list[str], dict[str, str]]] = []
            for _, game in day_games.iterrows():
                gid = str(game.get(id_col, ""))
                if not gid:
                    continue

                home_team = str(game.get("home_team_id") or game.get("home_team", ""))
                away_team = str(game.get("away_team_id") or game.get("away_team", ""))

                player_ids: list[str] = []
                player_names: dict[str, str] = {}

                if not players_df.empty:
                    tid_col = next((c for c in ["team_id", "team"] if c in players_df.columns), None)
                    if tid_col:
                        roster = players_df[
                            players_df[tid_col].astype(str).isin([home_team, away_team])
                        ]
                        pid_col = next((c for c in ["player_id", "id"] if c in roster.columns), None)
                        if pid_col:
                            for _, prow in roster.head(30).iterrows():
                                pid = str(prow[pid_col])
                                player_ids.append(pid)
                                name_col = next(
                                    (c for c in ["full_name", "name", "player_name"] if c in prow.index),
                                    None,
                                )
                                if name_col:
                                    player_names[pid] = str(prow[name_col])

                if not player_ids:
                    player_ids = predictor._discover_game_players(gid)

                if not player_ids:
                    logger.debug("No players found for game %s (%s)", gid, sport)
                    continue
                game_work.append((gid, player_ids, player_names))

            # Pre-warm feature cache for all games
            predictor.warm_game_cache([gid for gid, _, _ in game_work])

            import time as _time
            t_games = _time.monotonic()
            for gid, player_ids, player_names in game_work:
                try:
                    preds = predictor.predict_game_props(gid, player_ids, player_names)
                except Exception as exc:
                    logger.warning("predict_game_props failed for %s/%s: %s", sport, gid, exc)
                    continue

                for p in preds:
                    d = asdict(p)
                    d["date"] = pred_date
                    d["game_id"] = gid
                    all_preds.append(d)
            logger.info("%s: %d games predicted in %.1fs (%s)", sport, len(game_work), _time.monotonic() - t_games, pred_date)

            if all_preds:
                import pyarrow  # noqa: F401
                norm_dir.mkdir(parents=True, exist_ok=True)
                props_path = norm_dir / "player_props.parquet"
                new_df = pd.DataFrame(all_preds)
                if props_path.exists():
                    try:
                        old_df = pd.read_parquet(props_path)
                        old_df = old_df[old_df["date"].astype(str) != pred_date]
                        new_df = pd.concat([old_df, new_df], ignore_index=True)
                    except Exception:
                        pass
                new_df.to_parquet(props_path, index=False)
                logger.info("Saved %d player prop predictions to %s", len(all_preds), props_path)
            else:
                logger.info("No player prop predictions generated for %s on %s", sport, pred_date)

    elif args.action == "backtest":
        config = TrainingConfig(
            sport=args.sport.lower(),
            seasons=_parse_seasons(args.seasons),
        )
        trainer = Trainer(config, data_dir)
        result = trainer.backtest(args.start, args.end)
        logger.info(
            "Backtest complete: avg_acc=%.4f  avg_brier=%.4f  (%d folds)",
            result["avg_accuracy"],
            result["avg_brier_score"],
            result["n_folds"],
        )
        print(json.dumps(result, indent=2, default=str))

    elif args.action == "verify":
        config = TrainingConfig(
            sport=args.sport.lower(),
            seasons=_parse_seasons(args.seasons),
        )
        trainer = Trainer(config, data_dir)
        report = trainer.verify_data()
        status = "✓ PASS" if report["ok"] else "✗ FAIL"
        logger.info("Verification %s for %s", status, config.sport)
        if report.get("issues"):
            for issue in report["issues"]:
                logger.warning("  • %s", issue)
        print(json.dumps(report, indent=2, default=str))


def _print_metrics(metrics: dict[str, Any]) -> None:
    """Pretty-print training metrics to the log."""
    for group, model_metrics in metrics.items():
        logger.info("── %s ──", group)
        if isinstance(model_metrics, dict):
            for name, vals in model_metrics.items():
                if isinstance(vals, dict):
                    parts = "  ".join(f"{k}={v:.4f}" for k, v in vals.items())
                    logger.info("  %-20s  %s", name, parts)


if __name__ == "__main__":
    main()
