# ──────────────────────────────────────────────────────────
# V5.0 Backend — ML Predictors: Player Props Predictor
# ──────────────────────────────────────────────────────────
"""
Predict player prop lines (points, rebounds, assists, etc.)
using per-prop-type ensemble models with Poisson and Normal
confidence estimation.

Confidence brackets map a numeric confidence score to a
human-readable label used for filtering and display.
"""

from __future__ import annotations

import logging
import math
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.registry import get_extractor
from ml.models.base import PropPrediction
from ml.models.ensemble import EnsembleVoter

logger = logging.getLogger(__name__)

# ── 5-Tier Confidence brackets ──────────────────────────────────────────────
# S (Platinum) > A (Gold) > B (Silver) > C (Bronze) > D (Copper)

CONFIDENCE_BRACKETS: dict[str, float] = {
    "D": 0.52,   # Copper  — minimum threshold
    "C": 0.55,   # Bronze  — low-moderate
    "B": 0.62,   # Silver  — moderate
    "A": 0.70,   # Gold    — high conviction
    "S": 0.82,   # Platinum — maximum conviction
}

CONFIDENCE_TIER_LABELS: dict[str, str] = {
    "S": "Platinum",
    "A": "Gold",
    "B": "Silver",
    "C": "Bronze",
    "D": "Copper",
}

def _label_for_confidence(conf: float) -> tuple[str, str]:
    """Return (tier, label) for a confidence score using 5-tier system."""
    tier = "D"
    for t, threshold in sorted(CONFIDENCE_BRACKETS.items(), key=lambda x: -x[1]):
        if conf >= threshold:
            tier = t
            break
    return tier, CONFIDENCE_TIER_LABELS[tier]

# ── Sport → prop types mapping ───────────────────────────
# This is a fallback; loaded models extend this list automatically.

_SPORT_PROP_TYPES: dict[str, list[str]] = {
    "nba": [
        "pts_over_20", "pts_over_25", "pts_over_30",
        "reb_over_8", "reb_over_10", "ast_over_6", "ast_over_8",
        "stl_over_1", "blk_over_1", "to_over_3",
        "pts_reb_ast_over_35", "pts_reb_ast_over_45",
        "double_double", "oreb_over_3", "dreb_over_8",
        "home_top_scorer_pts", "top_scorer_pts_reg", "top_reb_reg", "top_ast_reg",
    ],
    "nfl": [
        "pass_yds_over_250", "pass_yds_over_300",
        "pass_td_over_2", "pass_td_over_3",
        "rush_yds_over_75", "rush_yds_over_100",
        "rec_yds_over_75", "rec_yds_over_100",
        "receptions_over_5", "completions_over_20",
        "rush_td_over_0", "rec_td_over_0",
        "top_passer_yds_reg", "top_rusher_yds_reg", "top_receiver_yds_reg",
    ],
    "mlb": [
        "pitcher_k_over_6", "pitcher_k_over_8",
        "batter_hit_over_1", "batter_hr",
        "batter_rbi_over_1", "batter_runs_over_1", "batter_bb_over_1",
        "total_runs_over_8", "total_hits_over_10",
        "top_batter_hits_reg", "pitcher_k_reg", "top_batter_rbi_reg",
    ],
    "nhl": [
        "team_goals_over_3", "team_goals_over_4", "team_goals_over_6",
        "team_shots_over_60", "team_shots_over_65",
        "player_goals_over_1", "player_assists_over_1",
        "player_shots_over_3", "player_shots_over_5",
        "goalie_saves_over_25", "goalie_saves_over_30",
        "player_hits_over_3", "player_blocked_over_3",
        "team_goals_reg", "goalie_saves_reg", "player_shots_reg",
    ],
}

# Combo prop components (not used by current model architecture)
_COMBO_COMPONENTS: dict[str, list[str]] = {}


# ── Props Predictor ──────────────────────────────────────


class PropsPredictor:
    """Predict player prop lines using per-prop ensemble models.

    Each prop type (e.g. ``"points"``, ``"rebounds"``) has its own
    ensemble regressor trained on player-level historical stats.
    Confidence is estimated using either a Poisson model (for count
    stats) or a Normal model (for continuous stats).
    """

    def __init__(
        self,
        sport: str,
        models_dir: Path,
        data_dir: Path,
        min_confidence: str = "GOOD",
    ) -> None:
        self.sport = sport.lower()
        self.models_dir = models_dir
        self.data_dir = data_dir
        self.extractor = get_extractor(self.sport, data_dir)

        # Map old labels ("GOOD", "HIGH", etc.) to tier letters for backward compat
        _legacy_map = {
            "WEAK": "D", "GOOD": "C", "MEDIUM": "B", "HIGH": "A",
            "VERY_HIGH": "A", "ELITE": "S",
        }
        tier_key = _legacy_map.get(min_confidence.upper(), min_confidence.upper())
        self.min_confidence_threshold = CONFIDENCE_BRACKETS.get(tier_key, 0.55)
        self.prop_types = _SPORT_PROP_TYPES.get(self.sport, [])
        self._models: dict[str, dict[str, Any]] = {}
        self._load_all_prop_models()

    # ── Model loading ────────────────────────────────────

    def _load_all_prop_models(self) -> None:
        """Load trained ensembles for each prop type.

        Supports two on-disk layouts:
        1. Unified bundle  – ``player_props.pkl`` with a ``models`` dict keyed by prop type.
        2. Per-prop files  – ``props_{prop_type}.pkl`` for each prop type (legacy layout).
        """
        # ── Layout 1: unified bundle ─────────────────────
        # Try sport-specific subdirectory first, then root
        for bundle_dir in [self.models_dir / self.sport, self.models_dir]:
            unified_path = bundle_dir / "player_props.pkl"
            if unified_path.exists():
                try:
                    with open(unified_path, "rb") as fh:
                        bundle = pickle.load(fh)  # noqa: S301
                    if isinstance(bundle, dict) and "models" in bundle:
                        for prop_type, model in bundle["models"].items():
                            self._models[prop_type] = model
                            logger.info("Loaded prop model (unified): %s", prop_type)
                        # Extend prop_types to include any extra models in the bundle
                        for pt in bundle["models"]:
                            if pt not in self.prop_types:
                                self.prop_types = list(self.prop_types) + [pt]
                    break  # stop after first successful bundle load
                except Exception:
                    logger.error("Failed to load unified props bundle %s", unified_path, exc_info=True)

        # ── Layout 2: per-prop files (supplementary / override) ──
        for prop_type in self.prop_types:
            if prop_type in self._models:
                continue  # already loaded from unified bundle
            path = self.models_dir / f"props_{prop_type}.pkl"
            if path.exists():
                try:
                    with open(path, "rb") as fh:
                        self._models[prop_type] = pickle.load(fh)  # noqa: S301
                    logger.info("Loaded prop model (per-file): %s", prop_type)
                except Exception:
                    logger.error("Failed to load %s", path, exc_info=True)
            else:
                logger.debug("No prop model for %s at %s", prop_type, path)

        logger.info(
            "PropsPredictor(%s): %d / %d prop models loaded",
            self.sport,
            len(self._models),
            len(self.prop_types),
        )

    # ── Feature extraction ───────────────────────────────

    _game_features_cache: dict[str, pd.DataFrame] = {}
    _games_df_cache: dict[str, pd.DataFrame] = {}  # season parquet cache

    def warm_game_cache(self, game_ids: list[str]) -> None:
        """Pre-extract features for multiple games (call before player iteration).

        This avoids repeated feature extraction during the per-player loop
        by front-loading the expensive work.
        """
        for gid in game_ids:
            if gid not in self._game_features_cache:
                self._player_features(gid, "", "")

    def _player_features(
        self,
        game_id: str,
        player_id: str,
        prop_type: str,
    ) -> pd.DataFrame | None:
        """Build a feature row for a player / prop type.

        Prop models are trained on game-level features (team form,
        standings, odds, injuries, etc.), the same features used by
        the game predictor.  We extract those features for the given
        game and align columns to what the prop model expects.

        Results are cached per game_id since every player in the same
        game shares the same game-level features.
        """
        # Fast cache hit — same game features for all players
        if game_id in self._game_features_cache:
            cached = self._game_features_cache[game_id]
            if cached is not None:
                return cached.copy()
            return None

        # Load game row from normalized data
        game_row = self._load_game_row(game_id)
        if game_row is None:
            logger.debug("Game %s not found in normalized data", game_id)
            self._game_features_cache[game_id] = None
            return None

        # Extract game-level features using the sport's feature extractor
        try:
            features = self.extractor.extract_game_features(game_row)
        except Exception:
            logger.debug("Feature extraction failed for game %s", game_id, exc_info=True)
            self._game_features_cache[game_id] = None
            return None

        if not features:
            logger.debug("Empty features for game %s", game_id)
            self._game_features_cache[game_id] = None
            return None

        # Build DataFrame and align to model's expected columns
        _META_COLS = {"game_id", "date", "home_team", "away_team", "season",
                      "home_score", "away_score", "winner", "total",
                      "result", "target", "label", "margin"}
        row = {k: v for k, v in features.items() if k not in _META_COLS}
        X = pd.DataFrame([row]).fillna(0)

        # Try to get expected feature names from the first loaded model
        expected_features = None
        for pt, bundle in self._models.items():
            model = bundle if isinstance(bundle, EnsembleVoter) else (
                bundle.get("ensemble") if isinstance(bundle, dict) else None
            )
            if model is not None and hasattr(model, "feature_names") and model.feature_names:
                expected_features = model.feature_names
                break

        if expected_features:
            for col in expected_features:
                if col not in X.columns:
                    X[col] = 0.0
            X = X[expected_features]

        self._game_features_cache[game_id] = X
        return X.copy()

    def _load_game_row(self, game_id: str) -> dict | None:
        """Load a single game row from normalized games parquet.

        Caches the season DataFrame so repeated lookups are fast.
        """
        norm_dir = self.data_dir / "normalized" / self.sport
        season = datetime.utcnow().year
        gid_str = str(game_id)
        for s in [season, season + 1, season - 1, season - 2]:
            cache_key = f"{self.sport}_{s}"
            if cache_key not in self._games_df_cache:
                games_path = norm_dir / f"games_{s}.parquet"
                if not games_path.exists():
                    continue
                try:
                    df = pd.read_parquet(games_path)
                except Exception:
                    continue
                id_col = "game_id" if "game_id" in df.columns else "id"
                if id_col not in df.columns:
                    continue
                df["_gid_str"] = df[id_col].astype(str)
                df = df.set_index("_gid_str", drop=False)
                self._games_df_cache[cache_key] = df
            else:
                df = self._games_df_cache[cache_key]
            if gid_str in df.index:
                return df.loc[gid_str].to_dict()
        return None

    @staticmethod
    def _resolve_stat_column(prop_type: str) -> str:
        """Map a prop type name to the player-stats column."""
        if prop_type in _COMBO_COMPONENTS:
            return prop_type  # combo handled separately
        # Map threshold-based and special prop types to base stat columns
        _PROP_TO_STAT = {
            "pts_over_20": "pts",
            "pts_reb_ast_over_35": "pts",  # will combine with reb+ast
            "double_double": "pts",  # classification
            "home_top_scorer_pts": "pts",
            "points": "pts",
            "rebounds": "reb",
            "assists": "ast",
            "steals": "stl",
            "blocks": "blk",
            "threes": "three_m",
            "turnovers": "to",
            "pra": "pts",  # combo: pts+reb+ast
            "pa": "pts",   # combo: pts+ast
            "pr": "pts",   # combo: pts+reb
            "ra": "reb",   # combo: reb+ast
        }
        return _PROP_TO_STAT.get(prop_type, prop_type)

    # ── Confidence estimation ────────────────────────────

    @staticmethod
    def _poisson_confidence(predicted: float, line: float) -> float:
        """Poisson-based over/under confidence for count stats.

        Uses the cumulative Poisson CDF to estimate P(X > line)
        where X ~ Poisson(predicted).

        Adapted from v4.0::

            reliability = (0.20 + 0.45 * quality) * weight
        """
        if predicted <= 0:
            return 0.50

        lam = predicted
        k = int(math.floor(line))

        # P(X <= k) via cumulative Poisson
        cumulative = 0.0
        for i in range(k + 1):
            log_pmf = i * math.log(lam) - lam - math.lgamma(i + 1)
            cumulative += math.exp(log_pmf)

        over_prob = 1.0 - cumulative
        under_prob = cumulative
        raw_conf = max(over_prob, under_prob)
        return float(np.clip(raw_conf, 0.50, 0.99))

    @staticmethod
    def _normal_confidence(
        predicted: float,
        line: float,
        uncertainty: float,
    ) -> float:
        """Normal-distribution confidence for continuous stats.

        From v4.0::

            z = edge / uncertainty
            prob = 50 * (1 + erf(z / sqrt(2)))

        Returns confidence in [0.50, 0.99].
        """
        edge = predicted - line
        if uncertainty <= 0:
            uncertainty = 1.0
        z = edge / uncertainty
        prob = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
        raw_conf = max(prob, 1.0 - prob)
        return float(np.clip(raw_conf, 0.50, 0.99))

    # ── Single player prediction ─────────────────────────

    def predict_player_props(
        self,
        game_id: str,
        player_id: str,
        player_name: str = "",
        lines: dict[str, float] | None = None,
    ) -> list[PropPrediction]:
        """Predict all prop markets for a player in a game.

        Parameters
        ----------
        game_id : str
        player_id : str
        player_name : str, optional
        lines : dict mapping prop_type → book line, optional.
            If not provided, the model predicts without edge calculation.
        """
        if lines is None:
            lines = {}

        results: list[PropPrediction] = []

        for prop_type in self.prop_types:
            if prop_type not in self._models:
                continue

            bundle = self._models[prop_type]
            # Support both dict bundles {"ensemble": ..., ...} and raw EnsembleVoter objects
            if isinstance(bundle, dict):
                ensemble: EnsembleVoter = bundle.get("ensemble")
            else:
                ensemble = bundle  # raw EnsembleVoter stored directly
            if ensemble is None:
                continue

            is_classifier = False
            over_prob_raw = 0.5

            # Handle combo props
            if prop_type in _COMBO_COMPONENTS:
                pred_value = self._predict_combo(
                    game_id, player_id, prop_type
                )
                if pred_value is None:
                    continue
            else:
                X = self._player_features(game_id, player_id, prop_type)
                if X is None:
                    continue
                try:
                    has_classifiers = bool(getattr(ensemble, "_fitted_classifiers", False))
                    has_regressors = bool(getattr(ensemble, "_fitted_regressors", False))
                    is_classifier = has_classifiers and not has_regressors

                    if is_classifier:
                        # Use predict_class (handles scaling internally)
                        probs, preds, _ = ensemble.predict_class(X)
                        over_prob_raw = float(probs[0])
                        pred_value = over_prob_raw
                    elif has_regressors:
                        pred_value = float(ensemble.predict_regression(X)[0])
                    else:
                        logger.debug("Model %s has no fitted classifiers or regressors", prop_type)
                        continue
                except Exception:
                    logger.debug(
                        "Prediction failed for %s/%s/%s",
                        player_id, prop_type, game_id,
                        exc_info=True,
                    )
                    continue

            line = lines.get(prop_type, pred_value)
            edge = pred_value - line

            # Confidence estimation
            uncertainty = 1.0

            if is_classifier:
                confidence = float(np.clip(
                    over_prob_raw if over_prob_raw >= 0.5 else 1.0 - over_prob_raw,
                    0.50, 0.99
                ))
            elif prop_type in {"points", "rebounds", "assists", "steals", "blocks",
                             "threes", "turnovers", "goals", "shots", "saves",
                             "hits", "home_runs", "rbis", "strikeouts", "walks",
                             "runs_scored", "receptions", "completions",
                             "passing_tds"}:
                confidence = self._poisson_confidence(pred_value, line)
            else:
                confidence = self._normal_confidence(pred_value, line, max(uncertainty, 0.5))

            # Over / under probabilities
            if pred_value >= line:
                over_prob = confidence
                under_prob = 1.0 - confidence
                recommendation = "OVER" if confidence >= self.min_confidence_threshold else "PASS"
            else:
                under_prob = confidence
                over_prob = 1.0 - confidence
                recommendation = "UNDER" if confidence >= self.min_confidence_threshold else "PASS"

            tier, tier_label = _label_for_confidence(confidence)
            if confidence < self.min_confidence_threshold:
                recommendation = "PASS"

            results.append(
                PropPrediction(
                    game_id=game_id,
                    player_id=player_id,
                    player_name=player_name,
                    sport=self.sport,
                    prop_type=prop_type,
                    line=line,
                    predicted_value=round(pred_value, 2),
                    edge=round(edge, 2),
                    over_prob=round(over_prob, 4),
                    under_prob=round(under_prob, 4),
                    confidence=round(confidence, 4),
                    confidence_tier=tier,
                    confidence_label=tier_label,
                    recommendation=recommendation,
                    n_models=ensemble.n_regressors,
                )
            )

        return results

    def _predict_combo(
        self,
        game_id: str,
        player_id: str,
        combo_type: str,
    ) -> float | None:
        """Sum component predictions for combo props (PRA, PA, etc.)."""
        components = _COMBO_COMPONENTS.get(combo_type, [])
        total = 0.0
        for comp in components:
            if comp not in self._models:
                return None
            X = self._player_features(game_id, player_id, comp)
            if X is None:
                return None
            bundle_c = self._models[comp]
            ensemble: EnsembleVoter = bundle_c.get("ensemble") if isinstance(bundle_c, dict) else bundle_c
            try:
                total += float(ensemble.predict_regression(X)[0])
            except Exception:
                return None
        return total

    # ── Game-level props prediction ──────────────────────

    def predict_game_props(
        self,
        game_id: str,
        player_ids: list[str] | None = None,
        player_names: dict[str, str] | None = None,
        lines: dict[str, dict[str, float]] | None = None,
    ) -> list[PropPrediction]:
        """Predict all player props for all (or specified) players in a game.

        Since prop models use game-level features (team form, odds, etc.)
        that are identical for every player in the same game, we predict
        each prop type **once** per game and stamp the result across all
        players — a ~30× speedup over per-player prediction.
        """
        if player_names is None:
            player_names = {}
        if lines is None:
            lines = {}

        if player_ids is None:
            player_ids = self._discover_game_players(game_id)

        # Get game features once
        X = self._player_features(game_id, "", "")
        if X is None:
            logger.debug("No features for game %s — skipping props", game_id)
            return []

        # Pre-compute per-prop-type predictions using fast single-sample methods
        # Scale once per ensemble, call predict_proba_fast / predict_regression_fast
        prop_results: dict[str, tuple[float, float, bool]] = {}
        X_np = X.values  # numpy array for fast methods
        for prop_type in self.prop_types:
            if prop_type not in self._models:
                continue

            bundle = self._models[prop_type]
            ensemble = bundle.get("ensemble") if isinstance(bundle, dict) else bundle
            if ensemble is None:
                continue

            try:
                has_classifiers = bool(getattr(ensemble, "_fitted_classifiers", False))
                has_regressors = bool(getattr(ensemble, "_fitted_regressors", False))
                is_classifier = has_classifiers and not has_regressors

                # Scale once, use fast predict path (skips slow models)
                if ensemble.scaler is not None:
                    X_scaled = ensemble.scaler.transform(X_np)
                else:
                    X_scaled = X_np

                if is_classifier:
                    over_prob_raw = ensemble.predict_proba_fast(X_scaled)
                    pred_value = over_prob_raw
                elif has_regressors:
                    pred_value = ensemble.predict_regression_fast(X_scaled)
                    over_prob_raw = 0.5
                else:
                    continue
                prop_results[prop_type] = (float(pred_value), float(over_prob_raw), is_classifier)
            except Exception:
                logger.debug("Prediction failed for prop %s game %s", prop_type, game_id, exc_info=True)

        # Stamp predictions across all players
        all_predictions: list[PropPrediction] = []
        for pid in player_ids:
            name = player_names.get(pid, pid)
            player_lines = lines.get(pid, {})

            for prop_type, (pred_value, over_prob_raw, is_classifier) in prop_results.items():
                line = player_lines.get(prop_type, pred_value)
                edge = pred_value - line

                if is_classifier:
                    confidence = float(np.clip(
                        over_prob_raw if over_prob_raw >= 0.5 else 1.0 - over_prob_raw,
                        0.50, 0.99
                    ))
                elif prop_type in {"points", "rebounds", "assists", "steals", "blocks",
                                   "threes", "turnovers", "goals", "shots", "saves",
                                   "hits", "home_runs", "rbis", "strikeouts", "walks",
                                   "runs_scored", "receptions", "completions",
                                   "passing_tds"}:
                    confidence = self._poisson_confidence(pred_value, line)
                else:
                    confidence = self._normal_confidence(pred_value, line, 1.0)

                if pred_value >= line:
                    over_prob = confidence
                    under_prob = 1.0 - confidence
                    recommendation = "OVER" if confidence >= self.min_confidence_threshold else "PASS"
                else:
                    under_prob = confidence
                    over_prob = 1.0 - confidence
                    recommendation = "UNDER" if confidence >= self.min_confidence_threshold else "PASS"

                tier, tier_label = _label_for_confidence(confidence)
                if confidence < self.min_confidence_threshold:
                    recommendation = "PASS"

                all_predictions.append(
                    PropPrediction(
                        game_id=game_id,
                        player_id=pid,
                        player_name=name,
                        sport=self.sport,
                        prop_type=prop_type,
                        line=line,
                        predicted_value=round(pred_value, 2),
                        edge=round(edge, 2),
                        over_prob=round(over_prob, 4),
                        under_prob=round(under_prob, 4),
                        confidence=round(confidence, 4),
                        confidence_tier=tier,
                        confidence_label=tier_label,
                        recommendation=recommendation,
                    )
                )

        all_predictions.sort(key=lambda p: p.confidence, reverse=True)

        logger.info(
            "Game %s: %d prop predictions across %d players",
            game_id,
            len(all_predictions),
            len(player_ids),
        )
        return all_predictions

    def _discover_game_players(self, game_id: str) -> list[str]:
        """Try to find player IDs that participated in a game."""
        season = datetime.utcnow().year
        for offset in range(3):
            try:
                ps = self.extractor.load_player_stats(season - offset)
            except FileNotFoundError:
                continue
            if "game_id" in ps.columns:
                match = ps[ps["game_id"].astype(str) == str(game_id)]
                if not match.empty:
                    return match["player_id"].astype(str).unique().tolist()
        return []

    # ── Utility ──────────────────────────────────────────

    def available_prop_types(self) -> list[str]:
        """Return prop types that have trained models."""
        return [pt for pt in self.prop_types if pt in self._models]

    def summary(self) -> dict[str, Any]:
        return {
            "sport": self.sport,
            "all_prop_types": self.prop_types,
            "loaded_prop_types": self.available_prop_types(),
            "min_confidence": self.min_confidence_threshold,
        }
