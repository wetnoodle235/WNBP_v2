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
    # Hockey period scores (outcome data)
    "home_p1", "home_p2", "home_p3", "away_p1", "away_p2", "away_p3",
    # Motorsport (F1) current-race outcome columns — NOT available pre-race
    "podium", "points_finish", "dnf", "fastest_lap",
    "laps_completed", "laps_completion_pct",
    "avg_speed_kph", "pit_stops", "avg_pit_time_s",
    "safety_car_count", "dnf_count", "red_flag_count", "race_pit_stops_total",
}

# Sports that can legitimately end in a draw/tie at full time
_DRAW_SPORTS = frozenset({"epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "nfl", "europa", "ligamx"})
# Sports that use q1–q4 (or periods 1–3) for halftime/period markets
_PERIOD_SPORTS = frozenset({"nba", "nfl", "nhl", "mlb", "ncaab", "ncaaf", "wnba", "ncaaw"})
# Per-sport period label for display
_PERIOD_LABEL: dict[str, str] = {
    "nba": "quarter", "ncaab": "half", "nfl": "quarter", "ncaaf": "quarter",
    "wnba": "quarter", "ncaaw": "half",
    "nhl": "period", "mlb": "inning",
}
# Soccer sports use h1/h2 columns for halftime (not q1/q2)
_SOCCER_HALF_SPORTS = frozenset({"epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "europa", "ligamx"})
# Sports with meaningful halftime (first-half data in q1+q2 or h1)
_HALFTIME_SPORTS = frozenset({"nba", "nfl", "ncaab", "ncaaf", "wnba", "ncaaw", "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "europa", "ligamx"})
# Soccer-family sports (BTTS, clean sheet)
_SOCCER_SPORTS = frozenset({"epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "europa", "ligamx"})
# Sports with OT/extra time
_OT_SPORTS = frozenset({"nba", "nfl", "nhl", "mlb", "ncaab", "ncaaf", "wnba", "ncaaw", "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "europa", "ligamx"})
# Sports with meaningful margin/total bands (team-vs-team scoring games)
_MARGIN_SPORTS = frozenset({"nba", "nfl", "nhl", "mlb", "ncaab", "ncaaf", "epl", "laliga", "bundesliga", "ligue1", "seriea", "ucl", "mls", "nwsl", "wnba", "ncaaw", "csgo", "dota2", "lol", "valorant", "europa", "ligamx"})
# Sports where 2nd-half/comeback models are meaningful
_SECOND_HALF_SPORTS = _HALFTIME_SPORTS
# Sports to skip ALL extra markets (pure binary outcome — no subgame structure)
_NO_EXTRA_MARKETS = frozenset({"f1", "golf", "lpga", "indycar"})
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
            _NO_ZERO_ZERO_SPORTS = {"mlb", "nba", "nfl", "ncaab", "ncaaf", "ncaaw"}
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
        """Golf: predict finish position (regression) and top-10 (classification).

        Golf features are per-player-per-tournament, not head-to-head.
        We train:
        - position_ensemble: regressor for finish position
        - top10_ensemble: classifier for top-10 finish (binary)
        - winner_ensemble: classifier for tournament win (binary)
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

        # Classification: top-10 finish (use balanced weights — top-10 rate is ~6.7%)
        logger.info("Training golf top-10 classifier …")
        top10_ensemble = EnsembleVoter()
        cls_metrics = top10_ensemble.fit_classifiers(
            X_train, top10_train.astype(int),
            X_val, top10_val.astype(int),
            class_weight="balanced",
        )

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
        }

        self.save_models(result, self.models_dir / "joint_models.pkl")
        logger.info("Golf training complete ✓ (top-10 + position)")
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

        q_cols_avail = [c for c in [half_col1_h, half_col2_h, half_col1_a, half_col2_a]
                        if c in train_df.columns]
        if len(q_cols_avail) == 4 and sport in _HALFTIME_SPORTS:
            # Only use rows where first-half data is actually populated AND non-zero
            half_mask_tr = train_df[half_col1_h].notna() & train_df[half_col2_h].notna() & \
                           ((train_df[half_col1_h] != 0) | (train_df[half_col2_h] != 0))
            half_mask_va = val_df[half_col1_h].notna() & val_df[half_col2_h].notna() & \
                           ((val_df[half_col1_h] != 0) | (val_df[half_col2_h] != 0))
            n_ht_tr = int(half_mask_tr.sum())
            n_ht_va = int(half_mask_va.sum())

            if n_ht_tr >= 80 and n_ht_va >= 20:
                logger.info("Training halftime models (%d/%d rows with half data) …", n_ht_tr, n_ht_va)
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

        logger.info("Extra-market training complete — %d models: %s", len(models), list(models.keys()))
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

    def _train_ufc_method(
        self,
        train_df: "pd.DataFrame",
        val_df: "pd.DataFrame",
        X_train: "pd.DataFrame",
        X_val: "pd.DataFrame",
        models: dict,
    ) -> None:
        """Train UFC/combat method-of-victory models.

        Uses home_ot column as method proxy if present:
          home_ot == 1 → KO/TKO finish
          home_ot == 2 → Submission finish
          home_ot == 0 → Decision
        Falls back to basic decision/finish split if only final scores available.
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

        logger.info("Training UFC method models (%d/%d rows) …", n_tr, n_va)

        # home_ot column encodes method if provider populates it: 0=decision,1=KO,2=sub
        if "home_ot" in train_df.columns:
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
            # Fallback: use margin as decision proxy (tight margins → decision)
            margin_tr = (hs_tr[valid_tr] - as_tr[valid_tr]).abs()
            margin_va = (hs_va[valid_va] - as_va[valid_va]).abs()
            # Binary scores (0/1) yield margin == 1 for every fight → constant target → skip
            if margin_tr.nunique() <= 1:
                logger.info(
                    "  UFC binary scores detected – skipping decision proxy. "
                    "Add 'method_of_victory' column to UFC normalized data for finish predictions."
                )
                return
            y_dec_tr = (margin_tr <= 1).astype(int)  # 1-point margin = decision
            y_dec_va = (margin_va <= 1).astype(int)
            m = self._fit_cls_safe("ufc_decision", X_train.loc[valid_tr], y_dec_tr, X_val.loc[valid_va], y_dec_va)
            if m:
                models["ufc_decision"] = m

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

        q_avail = all(c in train_df.columns for c in [h1_col, h2_col, a1_col, a2_col])
        hs_avail = "home_score" in train_df.columns and "away_score" in train_df.columns
        if not q_avail or not hs_avail:
            return

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

        hh_tr = train_df.loc[mask_tr, h1_col].fillna(0) + train_df.loc[mask_tr, h2_col].fillna(0)
        ah_tr = train_df.loc[mask_tr, a1_col].fillna(0) + train_df.loc[mask_tr, a2_col].fillna(0)
        hf_tr = train_df.loc[mask_tr, "home_score"].fillna(0)
        af_tr = train_df.loc[mask_tr, "away_score"].fillna(0)

        hh_va = val_df.loc[mask_va, h1_col].fillna(0) + val_df.loc[mask_va, h2_col].fillna(0)
        ah_va = val_df.loc[mask_va, a1_col].fillna(0) + val_df.loc[mask_va, a2_col].fillna(0)
        hf_va = val_df.loc[mask_va, "home_score"].fillna(0)
        af_va = val_df.loc[mask_va, "away_score"].fillna(0)

        Xc_tr = X_train.loc[mask_tr]
        Xc_va = X_val.loc[mask_va]

        # P(comeback) = trailing at half but wins overall
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

    # ── Separate training ────────────────────────────────

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

    # ── train-all ────────────────────────────────────────
    p_all = sub.add_parser("train-all", help="Train all registered sports")
    p_all.add_argument("--seasons", required=True, help="Comma-separated seasons")
    p_all.add_argument("--mode", default="joint", choices=["joint", "separate"])
    p_all.add_argument("--test-size", type=float, default=0.2)

    # ── predict ──────────────────────────────────────────
    p_pred = sub.add_parser("predict", help="Generate predictions")
    p_pred.add_argument("--sport", required=True)
    p_pred.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")

    # ── predict-props ────────────────────────────────────
    p_props = sub.add_parser("predict-props", help="Generate player prop predictions")
    p_props.add_argument("--sport", required=True)
    p_props.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")

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

        pred_date = args.date or date.today().isoformat()
        sport = args.sport.lower()
        models_dir = data_dir.parent / "ml" / "models" / sport
        predictor = GamePredictor(sport, models_dir, data_dir)
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

        pred_date = args.date or date.today().isoformat()
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

        # Determine current season and load today's games
        from ml.predictors.game_predictor import GamePredictor
        pred_dt = pd.Timestamp(pred_date)
        game_pred = GamePredictor(sport, models_dir, data_dir)
        season = game_pred._date_to_season(pred_dt)

        norm_dir = data_dir / "normalized" / sport
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
            logger.info("No games for %s on %s — skipping", sport, pred_date)
            sys.exit(0)

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

        # Build per-game work items (players, names) upfront
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

        # Pre-warm feature cache for all games (biggest speed win)
        predictor.warm_game_cache([gid for gid, _, _ in game_work])

        # Process games — sequential (feature cache makes this fast)
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
        logger.info("%s: %d games predicted in %.1fs", sport, len(game_work), _time.monotonic() - t_games)

        if all_preds:
            import pyarrow  # noqa: F401 – ensure parquet engine available
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
