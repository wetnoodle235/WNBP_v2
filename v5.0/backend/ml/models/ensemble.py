# ──────────────────────────────────────────────────────────
# V5.0 Backend — ML Models: 12-Model Ensemble Voter
# ──────────────────────────────────────────────────────────
"""
Core ML engine.

* 12-model ensemble with Brier-score weighting (classification)
  and inverse-RMSE weighting (regression).
* Graceful degradation when optional libraries (xgboost, lightgbm,
  catboost) are not installed — the ensemble simply runs with fewer
  models.
* ``StandardScaler`` fitted on training data only, applied at
  predict time.
"""

from __future__ import annotations

import logging
import signal
import warnings
from typing import Any

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import (
    AdaBoostClassifier,
    AdaBoostRegressor,
    ExtraTreesClassifier,
    ExtraTreesRegressor,
    GradientBoostingClassifier,
    GradientBoostingRegressor,
    RandomForestClassifier,
    RandomForestRegressor,
)
from sklearn.linear_model import (
    ElasticNet,
    Lasso,
    LogisticRegression,
    Ridge,
)
from sklearn.metrics import brier_score_loss, mean_squared_error
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC, SVR, LinearSVC

logger = logging.getLogger(__name__)

# ── Optional boosting libraries ──────────────────────────

_XGBOOST_AVAILABLE = False
_LIGHTGBM_AVAILABLE = False
_CATBOOST_AVAILABLE = False

try:
    from xgboost import XGBClassifier, XGBRegressor

    _XGBOOST_AVAILABLE = True
except ImportError:
    pass

try:
    from lightgbm import LGBMClassifier, LGBMRegressor

    _LIGHTGBM_AVAILABLE = True
except ImportError:
    pass

try:
    from catboost import CatBoostClassifier, CatBoostRegressor

    _CATBOOST_AVAILABLE = True
except ImportError:
    pass


# ── Ensemble Voter ───────────────────────────────────────


class EnsembleVoter:
    """12-model ensemble with Brier-score / RMSE weighting.

    Classification
    --------------
    Each classifier is trained independently.  On the **validation** set
    the Brier score is computed and converted to a quality weight:

        weight = max(0.01, accuracy * max(0.1, 1 − 2 * brier_score))

    Predictions are a **weighted vote** across all classifiers.

    Regression
    ----------
    Each regressor is trained independently.  Weights are the inverse of
    the validation RMSE (with a floor of 0.01).

    Both branches use ``StandardScaler`` fitted on *training data only*.
    """

    # ── Classifier definitions ───────────────────────────

    BASE_CLASSIFIERS: list[tuple[str, Any]] = [
        ("logistic", LogisticRegression(max_iter=2000, C=0.5, solver="lbfgs")),
        ("random_forest", RandomForestClassifier(n_estimators=150, max_depth=8, n_jobs=8, random_state=42)),
        (
            "gradient_boosting",
            GradientBoostingClassifier(
                n_estimators=150, max_depth=4, learning_rate=0.05, random_state=42
            ),
        ),
        ("svc", SVC(probability=True, C=2.0, random_state=42)),
        ("knn", KNeighborsClassifier(n_neighbors=5, weights="distance")),
        ("extra_trees", ExtraTreesClassifier(n_estimators=150, max_depth=8, n_jobs=8, random_state=42)),
        (
            "adaboost",
            AdaBoostClassifier(n_estimators=100, learning_rate=0.8, random_state=42),
        ),
        ("naive_bayes", GaussianNB()),
        (
            "linear_svc",
            CalibratedClassifierCV(
                LinearSVC(max_iter=3000, C=0.5, dual="auto", random_state=42),
                cv=3,
            ),
        ),
    ]

    # ── Regressor definitions ────────────────────────────

    BASE_REGRESSORS: list[tuple[str, Any]] = [
        ("ridge", Ridge(alpha=1.0)),
        ("lasso", Lasso(alpha=0.1, max_iter=5000)),
        ("elastic_net", ElasticNet(alpha=0.1, l1_ratio=0.5, max_iter=5000)),
        ("random_forest", RandomForestRegressor(n_estimators=150, max_depth=8, n_jobs=8, random_state=42)),
        (
            "gradient_boosting",
            GradientBoostingRegressor(
                n_estimators=150, max_depth=4, learning_rate=0.05, random_state=42
            ),
        ),
        ("svr", SVR(C=2.0)),
        ("knn", KNeighborsRegressor(n_neighbors=5, weights="distance")),
        ("extra_trees", ExtraTreesRegressor(n_estimators=150, max_depth=8, n_jobs=8, random_state=42)),
        (
            "adaboost",
            AdaBoostRegressor(n_estimators=100, learning_rate=0.8, random_state=42),
        ),
    ]

    # ── Initialisation ───────────────────────────────────

    def __init__(self) -> None:
        self.classifiers: list[tuple[str, Any]] = []
        self.regressors: list[tuple[str, Any]] = []
        self.classifier_weights: dict[str, float] = {}
        self.regressor_weights: dict[str, float] = {}
        self.scaler: StandardScaler | None = None
        self.feature_names: list[str] = []
        self._fitted_classifiers = False
        self._fitted_regressors = False
        self.calibrator: Any = None  # isotonic regression calibrator
        self._load_models()

    # ── Private helpers ──────────────────────────────────

    def _load_models(self) -> None:
        """Populate classifier/regressor lists, adding optional boosters."""
        from copy import deepcopy

        self.classifiers = [(n, deepcopy(m)) for n, m in self.BASE_CLASSIFIERS]
        self.regressors = [(n, deepcopy(m)) for n, m in self.BASE_REGRESSORS]

        if _XGBOOST_AVAILABLE:
            self.classifiers.append(
                (
                    "xgboost",
                    XGBClassifier(
                        n_estimators=200,
                        max_depth=6,
                        learning_rate=0.05,
                        use_label_encoder=False,
                        eval_metric="logloss",
                        random_state=42,
                        verbosity=0,
                        n_jobs=8,
                    ),
                )
            )
            self.regressors.append(
                (
                    "xgboost",
                    XGBRegressor(
                        n_estimators=200,
                        max_depth=6,
                        learning_rate=0.05,
                        random_state=42,
                        verbosity=0,
                        n_jobs=8,
                    ),
                )
            )
            logger.info("XGBoost models loaded")

        if _LIGHTGBM_AVAILABLE:
            self.classifiers.append(
                (
                    "lightgbm",
                    LGBMClassifier(
                        n_estimators=200,
                        max_depth=6,
                        learning_rate=0.05,
                        random_state=42,
                        verbose=-1,
                        n_jobs=8,
                    ),
                )
            )
            self.regressors.append(
                (
                    "lightgbm",
                    LGBMRegressor(
                        n_estimators=200,
                        max_depth=6,
                        learning_rate=0.05,
                        random_state=42,
                        verbose=-1,
                        n_jobs=8,
                    ),
                )
            )
            logger.info("LightGBM models loaded")

        if _CATBOOST_AVAILABLE:
            self.classifiers.append(
                (
                    "catboost",
                    CatBoostClassifier(
                        iterations=200,
                        depth=6,
                        learning_rate=0.05,
                        random_seed=42,
                        verbose=0,
                    ),
                )
            )
            self.regressors.append(
                (
                    "catboost",
                    CatBoostRegressor(
                        iterations=200,
                        depth=6,
                        learning_rate=0.05,
                        random_seed=42,
                        verbose=0,
                    ),
                )
            )
            logger.info("CatBoost models loaded")

        n_cls = len(self.classifiers)
        n_reg = len(self.regressors)
        logger.info("Ensemble initialised: %d classifiers, %d regressors", n_cls, n_reg)

    @staticmethod
    def _safe_predict_proba(model: Any, X: np.ndarray) -> np.ndarray | None:
        """Return probability array or ``None`` on failure."""
        try:
            probs = model.predict_proba(X)
            # Binary classification — col-1 is P(home_win)
            if probs.ndim == 2 and probs.shape[1] == 2:
                return probs[:, 1]
            return probs.ravel()
        except Exception:
            logger.warning("predict_proba failed for %s", type(model).__name__, exc_info=True)
            return None

    @staticmethod
    def _safe_predict(model: Any, X: np.ndarray) -> np.ndarray | None:
        """Return regression prediction or ``None``."""
        try:
            return model.predict(X).ravel()
        except Exception:
            logger.warning("predict failed for %s", type(model).__name__, exc_info=True)
            return None

    # ── Classifier training ──────────────────────────────

    def fit_classifiers(
        self,
        X_train: pd.DataFrame | np.ndarray,
        y_train: pd.Series | np.ndarray,
        X_val: pd.DataFrame | np.ndarray,
        y_val: pd.Series | np.ndarray,
    ) -> dict[str, dict[str, float]]:
        """Train all classifiers and compute Brier-score weights.

        Parameters
        ----------
        X_train, y_train : training features / labels (binary 0/1).
        X_val, y_val     : hold-out validation set for weight estimation.

        Returns
        -------
        Dict mapping model name → {"brier": …, "accuracy": …, "weight": …}.
        """
        # Fit scaler on training data
        self.scaler = StandardScaler()
        X_tr = self.scaler.fit_transform(X_train)
        X_v = self.scaler.transform(X_val)

        if isinstance(X_train, pd.DataFrame):
            self.feature_names = list(X_train.columns)

        y_tr = np.asarray(y_train).ravel()
        y_v = np.asarray(y_val).ravel()

        metrics: dict[str, dict[str, float]] = {}
        fitted: list[tuple[str, Any]] = []

        n_samples = X_tr.shape[0]
        _SLOW_MODELS = {"svc", "linear_svc", "svr"}
        _SLOW_THRESHOLD = 5000

        unique_y = np.unique(y_tr).size

        for name, model in self.classifiers:
            if name in _SLOW_MODELS and n_samples > _SLOW_THRESHOLD:
                logger.info("  %-20s  SKIPPED (n=%d > %d)", name, n_samples, _SLOW_THRESHOLD)
                continue
            if name == "catboost" and unique_y <= 1:
                logger.info("  %-20s  SKIPPED (all targets equal)", name)
                continue
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    model.fit(X_tr, y_tr)

                probs = self._safe_predict_proba(model, X_v)
                if probs is None:
                    logger.warning("Skipping %s — cannot obtain probabilities", name)
                    continue

                brier = brier_score_loss(y_v, probs)
                preds = (probs >= 0.5).astype(int)
                accuracy = float(np.mean(preds == y_v))
                weight = max(0.01, accuracy * max(0.1, 1.0 - 2.0 * brier))

                self.classifier_weights[name] = weight
                metrics[name] = {"brier": brier, "accuracy": accuracy, "weight": weight}
                fitted.append((name, model))
                logger.info(
                    "  %-20s  brier=%.4f  acc=%.4f  weight=%.4f",
                    name, brier, accuracy, weight,
                )
            except Exception:
                logger.error("Failed to train classifier %s", name, exc_info=True)

        self.classifiers = fitted
        self._fitted_classifiers = True

        if not self.classifiers:
            raise RuntimeError("All classifiers failed to train")

        # Fit isotonic calibrator on validation-set ensemble probabilities
        try:
            from sklearn.isotonic import IsotonicRegression

            ensemble_val_probs = self._weighted_probs(X_v)
            cal = IsotonicRegression(out_of_bounds="clip")
            cal.fit(ensemble_val_probs, y_v)
            self.calibrator = cal
            cal_probs = cal.predict(ensemble_val_probs)
            cal_brier = brier_score_loss(y_v, cal_probs)
            logger.info("Isotonic calibrator fitted  val_brier=%.4f", cal_brier)
        except Exception:
            logger.warning("Could not fit isotonic calibrator", exc_info=True)

        logger.info(
            "Fitted %d classifiers (best: %s, weight=%.4f)",
            len(self.classifiers),
            max(self.classifier_weights, key=self.classifier_weights.get),
            max(self.classifier_weights.values()),
        )
        return metrics

    def _weighted_probs(self, X_scaled: np.ndarray) -> np.ndarray:
        """Return weighted-average ensemble probability for scaled input."""
        n = X_scaled.shape[0]
        weighted_sum = np.zeros(n, dtype=np.float64)
        total_w = 0.0
        for name, model in self.classifiers:
            probs = self._safe_predict_proba(model, X_scaled)
            if probs is not None:
                w = self.classifier_weights.get(name, 0.01)
                weighted_sum += w * probs
                total_w += w
        return weighted_sum / max(total_w, 1e-9)

    # ── Regressor training ───────────────────────────────

    def fit_regressors(
        self,
        X_train: pd.DataFrame | np.ndarray,
        y_train: pd.Series | np.ndarray,
        X_val: pd.DataFrame | np.ndarray,
        y_val: pd.Series | np.ndarray,
    ) -> dict[str, dict[str, float]]:
        """Train all regressors and compute inverse-RMSE weights.

        Returns
        -------
        Dict mapping model name → {"rmse": …, "weight": …}.
        """
        if self.scaler is None:
            self.scaler = StandardScaler()
            X_tr = self.scaler.fit_transform(X_train)
        else:
            X_tr = self.scaler.transform(X_train)
        X_v = self.scaler.transform(X_val)

        if isinstance(X_train, pd.DataFrame) and not self.feature_names:
            self.feature_names = list(X_train.columns)

        y_tr = np.asarray(y_train).ravel()
        y_v = np.asarray(y_val).ravel()

        metrics: dict[str, dict[str, float]] = {}
        fitted: list[tuple[str, Any]] = []

        n_samples = X_tr.shape[0]
        _SLOW_THRESHOLD = 5000
        unique_y = np.unique(y_tr).size

        for name, model in self.regressors:
            if name in {"svr"} and n_samples > _SLOW_THRESHOLD:
                logger.info("  %-20s  SKIPPED (n=%d > %d)", name, n_samples, _SLOW_THRESHOLD)
                continue
            if name == "catboost" and unique_y <= 1:
                logger.info("  %-20s  SKIPPED (all targets equal)", name)
                continue
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    model.fit(X_tr, y_tr)

                preds = self._safe_predict(model, X_v)
                if preds is None:
                    logger.warning("Skipping regressor %s — predict failed", name)
                    continue

                rmse = float(np.sqrt(mean_squared_error(y_v, preds)))
                weight = 1.0 / max(rmse, 0.01)

                self.regressor_weights[name] = weight
                metrics[name] = {"rmse": rmse, "weight": weight}
                fitted.append((name, model))
                logger.info("  %-20s  rmse=%.4f  weight=%.4f", name, rmse, weight)
            except Exception:
                logger.error("Failed to train regressor %s", name, exc_info=True)

        self.regressors = fitted
        self._fitted_regressors = True

        if not self.regressors:
            raise RuntimeError("All regressors failed to train")

        logger.info(
            "Fitted %d regressors (best: %s, weight=%.4f)",
            len(self.regressors),
            max(self.regressor_weights, key=self.regressor_weights.get),
            max(self.regressor_weights.values()),
        )
        return metrics

    # ── Classification prediction ────────────────────────

    def predict_class(
        self, X: pd.DataFrame | np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, dict[str, dict[str, Any]]]:
        """Generate ensemble classification predictions.

        Returns
        -------
        probabilities : ndarray of shape (n_samples,)
            Weighted average P(home_win).
        predictions : ndarray of int, shape (n_samples,)
            Binary predictions (1 = home win, 0 = away win).
        vote_details : dict
            Per-sample dict with model-level votes, probabilities,
            consensus, and confidence.
        """
        if not self._fitted_classifiers:
            raise RuntimeError("Classifiers not fitted — call fit_classifiers() first")

        X_scaled = self.scaler.transform(X)
        n_samples = X_scaled.shape[0]

        # Collect per-model probabilities (single pass — reuse for both
        # vote_details and weighted average to avoid double inference)
        model_probs: dict[str, np.ndarray] = {}
        for name, model in self.classifiers:
            probs = self._safe_predict_proba(model, X_scaled)
            if probs is not None:
                model_probs[name] = probs

        if not model_probs:
            raise RuntimeError("No classifier produced valid predictions")

        # Weighted average from already-computed model_probs (no second pass)
        n = X_scaled.shape[0]
        weighted_sum = np.zeros(n, dtype=np.float64)
        total_w = 0.0
        for name, probs in model_probs.items():
            w = self.classifier_weights.get(name, 0.01)
            weighted_sum += w * probs
            total_w += w
        raw_probs = weighted_sum / max(total_w, 1e-9)

        # Isotonic calibration then clip to [0.10, 0.90]
        if self.calibrator is not None:
            probabilities = np.clip(self.calibrator.predict(raw_probs), 0.10, 0.90)
        else:
            probabilities = raw_probs
        predictions = (probabilities >= 0.5).astype(int)

        # Per-sample vote details
        vote_details: dict[str, dict[str, Any]] = {}
        for i in range(n_samples):
            votes: dict[str, str] = {}
            model_p: dict[str, float] = {}
            for name, probs in model_probs.items():
                votes[name] = "home" if probs[i] >= 0.5 else "away"
                model_p[name] = float(probs[i])

            majority = "home" if predictions[i] == 1 else "away"
            agree_count = sum(1 for v in votes.values() if v == majority)
            consensus = agree_count / max(len(votes), 1)

            confidence = self.calculate_confidence(probabilities[i], consensus)

            vote_details[str(i)] = {
                "votes": votes,
                "probabilities": model_p,
                "consensus": consensus,
                "confidence": confidence,
                "n_models": len(votes),
            }

        return probabilities, predictions, vote_details

    # Slow classifiers to skip for fast prediction (RF/ExtraTrees/AdaBoost/KNN)
    _SLOW_CLASSIFIERS = frozenset({"random_forest", "extra_trees", "adaboost", "knn"})
    # Slow regressors to skip for fast prediction (KNN O(n) distance, RF/ET/AdaBoost tree overhead)
    _SLOW_REGRESSORS = frozenset({"random_forest", "extra_trees", "adaboost", "knn", "xgboost", "catboost"})

    def predict_proba_fast(self, X_scaled: "np.ndarray") -> float:
        """Fast single-sample probability for extra-market inference.

        Takes an *already-scaled* numpy array (shape (1, n_features)).
        Skips slow classifiers (RF, ExtraTrees, AdaBoost, KNN) and
        skips vote_details building — returns weighted P(positive) only.
        This is ~8× faster than predict_class() for n=1.
        """
        if not self._fitted_classifiers:
            raise RuntimeError("Classifiers not fitted — call fit_classifiers() first")

        weighted_sum = 0.0
        total_w = 0.0
        for name, model in self.classifiers:
            if name in self._SLOW_CLASSIFIERS:
                continue
            probs = self._safe_predict_proba(model, X_scaled)
            if probs is None:
                continue
            w = self.classifier_weights.get(name, 0.01)
            weighted_sum += w * float(probs[0])
            total_w += w

        if total_w < 1e-9:
            return 0.5
        raw = weighted_sum / total_w
        if self.calibrator is not None:
            raw = float(np.clip(self.calibrator.predict(np.array([raw])), 0.10, 0.90)[0])
        return float(raw)

    def predict_regression_fast(self, X_scaled: "np.ndarray") -> float:
        """Fast single-sample regression for extra-market inference.

        Takes an *already-scaled* numpy array (shape (1, n_features)).
        Skips slow regressors (KNN, RF, ExtraTrees, AdaBoost, XGBoost, CatBoost)
        and returns weighted regression value as a float.
        """
        if not self._fitted_regressors:
            raise RuntimeError("Regressors not fitted — call fit_regressors() first")

        weighted_sum = 0.0
        total_w = 0.0
        for name, model in self.regressors:
            if name in self._SLOW_REGRESSORS:
                continue
            preds = self._safe_predict(model, X_scaled)
            if preds is None:
                continue
            w = self.regressor_weights.get(name, 0.01)
            weighted_sum += w * float(preds[0])
            total_w += w

        return (weighted_sum / total_w) if total_w > 1e-9 else 0.0

    def predict_proba_fast_batch(self, X_scaled: "np.ndarray") -> "np.ndarray":
        """Batch version of predict_proba_fast.

        Parameters
        ----------
        X_scaled : ndarray of shape (N, F) — already scaled.

        Returns
        -------
        ndarray of shape (N,) — P(positive) for each sample.
        """
        if not self._fitted_classifiers:
            raise RuntimeError("Classifiers not fitted — call fit_classifiers() first")

        n = X_scaled.shape[0]
        weighted_sum = np.zeros(n, dtype=np.float64)
        total_w = 0.0
        for name, model in self.classifiers:
            if name in self._SLOW_CLASSIFIERS:
                continue
            probs = self._safe_predict_proba(model, X_scaled)
            if probs is None:
                continue
            w = self.classifier_weights.get(name, 0.01)
            weighted_sum += w * probs
            total_w += w

        if total_w < 1e-9:
            return np.full(n, 0.5)
        raw = weighted_sum / total_w
        if self.calibrator is not None:
            raw = np.clip(self.calibrator.predict(raw), 0.10, 0.90)
        return raw

    def predict_regression_fast_batch(self, X_scaled: "np.ndarray") -> "np.ndarray":
        """Batch version of predict_regression_fast.

        Parameters
        ----------
        X_scaled : ndarray of shape (N, F) — already scaled.

        Returns
        -------
        ndarray of shape (N,) — regression predictions.
        """
        if not self._fitted_regressors:
            raise RuntimeError("Regressors not fitted — call fit_regressors() first")

        n = X_scaled.shape[0]
        weighted_sum = np.zeros(n, dtype=np.float64)
        total_w = 0.0
        for name, model in self.regressors:
            if name in self._SLOW_REGRESSORS:
                continue
            preds = self._safe_predict(model, X_scaled)
            if preds is None:
                continue
            w = self.regressor_weights.get(name, 0.01)
            weighted_sum += w * preds
            total_w += w

        return (weighted_sum / total_w) if total_w > 1e-9 else np.zeros(n)

    # ── Regression prediction ────────────────────────────

    def predict_regression(self, X: pd.DataFrame | np.ndarray) -> np.ndarray:
        """Generate weighted ensemble regression predictions.

        Returns
        -------
        ndarray of shape (n_samples,) — weighted-average predictions.
        """
        if not self._fitted_regressors:
            raise RuntimeError("Regressors not fitted — call fit_regressors() first")

        X_scaled = self.scaler.transform(X)
        n_samples = X_scaled.shape[0]

        total_weight = 0.0
        weighted_sum = np.zeros(n_samples, dtype=np.float64)

        for name, model in self.regressors:
            preds = self._safe_predict(model, X_scaled)
            if preds is None:
                continue
            w = self.regressor_weights.get(name, 0.01)
            weighted_sum += w * preds
            total_weight += w

        if total_weight < 1e-9:
            raise RuntimeError("No regressor produced valid predictions")

        return weighted_sum / total_weight

    # ── Confidence ───────────────────────────────────────

    @staticmethod
    def calculate_confidence(prob: float, consensus: float) -> float:
        """Compute prediction confidence.

        Formula (from v4.0)::

            prob_margin  = abs(prob − 0.5)
            agreement    = consensus          # fraction of models agreeing
            amplifier    = 0.70 + 1.20 × (agreement − 0.5)   (clamped to [0.5, 1.3])
            confidence   = 0.50 + clip(prob_margin × amplifier, 0, 0.48)

        Returns a value in ``[0.50, 0.98]``.

        Parameters
        ----------
        prob : float
            Weighted average probability from the ensemble (0–1).
        consensus : float
            Fraction of models that agree with the majority vote (0–1).
        """
        prob_margin = abs(prob - 0.5)

        raw_amplifier = 0.70 + 1.20 * (consensus - 0.5)
        amplifier = float(np.clip(raw_amplifier, 0.5, 1.3))

        confidence = 0.50 + float(np.clip(prob_margin * amplifier, 0.0, 0.48))
        return round(confidence, 4)

    # ── Utility ──────────────────────────────────────────

    @property
    def n_classifiers(self) -> int:
        return len(self.classifiers)

    @property
    def n_regressors(self) -> int:
        return len(self.regressors)

    def classifier_names(self) -> list[str]:
        return [n for n, _ in self.classifiers]

    def regressor_names(self) -> list[str]:
        return [n for n, _ in self.regressors]

    def summary(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary of the ensemble state."""
        return {
            "n_classifiers": self.n_classifiers,
            "n_regressors": self.n_regressors,
            "classifier_names": self.classifier_names(),
            "regressor_names": self.regressor_names(),
            "classifier_weights": self.classifier_weights,
            "regressor_weights": self.regressor_weights,
            "fitted_classifiers": self._fitted_classifiers,
            "fitted_regressors": self._fitted_regressors,
            "xgboost": _XGBOOST_AVAILABLE,
            "lightgbm": _LIGHTGBM_AVAILABLE,
            "catboost": _CATBOOST_AVAILABLE,
        }
