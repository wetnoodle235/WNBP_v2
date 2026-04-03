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
        ("random_forest", RandomForestClassifier(n_estimators=150, max_depth=7, min_samples_leaf=3, n_jobs=8, random_state=42)),
        (
            "gradient_boosting",
            GradientBoostingClassifier(
                n_estimators=200, max_depth=4, learning_rate=0.05,
                subsample=0.8, min_samples_leaf=5, random_state=42
            ),
        ),
        ("svc", SVC(probability=True, C=1.0, random_state=42)),
        ("knn", KNeighborsClassifier(n_neighbors=7, weights="distance")),
        ("extra_trees", ExtraTreesClassifier(n_estimators=150, max_depth=7, min_samples_leaf=3, n_jobs=8, random_state=42)),
        (
            "adaboost",
            AdaBoostClassifier(n_estimators=100, learning_rate=0.5, random_state=42),
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
        ("ridge", Ridge(alpha=2.0)),
        ("lasso", Lasso(alpha=0.1, max_iter=2000, tol=1e-3)),
        ("elastic_net", ElasticNet(alpha=0.1, l1_ratio=0.5, max_iter=2000, tol=1e-3)),
        ("random_forest", RandomForestRegressor(n_estimators=150, max_depth=7, min_samples_leaf=3, n_jobs=8, random_state=42)),
        (
            "gradient_boosting",
            GradientBoostingRegressor(
                n_estimators=200, max_depth=4, learning_rate=0.05,
                subsample=0.8, min_samples_leaf=5, random_state=42
            ),
        ),
        ("svr", SVR(C=1.0)),
        ("knn", KNeighborsRegressor(n_neighbors=7, weights="distance")),
        ("extra_trees", ExtraTreesRegressor(n_estimators=150, max_depth=7, min_samples_leaf=3, n_jobs=8, random_state=42)),
        (
            "adaboost",
            AdaBoostRegressor(n_estimators=100, learning_rate=0.5, random_state=42),
        ),
    ]

    # ── Initialisation ───────────────────────────────────

    def __init__(self, lightweight: bool = False) -> None:
        """Initialise the ensemble.

        Parameters
        ----------
        lightweight:
            When True, only fast models are included (logistic + LightGBM/XGBoost
            gradient boosting).  Use for high-throughput training where many models
            are fit in a loop (e.g. player props — 35+ props × N sports).
        """
        self.lightweight = lightweight
        self.classifiers: list[tuple[str, Any]] = []
        self.regressors: list[tuple[str, Any]] = []
        self.classifier_weights: dict[str, float] = {}
        self.regressor_weights: dict[str, float] = {}
        self.scaler: StandardScaler | None = None
        self.feature_names: list[str] = []
        self._fitted_classifiers = False
        self._fitted_regressors = False
        self.calibrator: Any = None  # calibrator (isotonic or Platt)
        self._calibrator_type: str = "isotonic"
        self._load_models()

    def _calibrate(self, raw_probs: np.ndarray) -> np.ndarray:
        """Apply calibrator and clip to [0.05, 0.95]."""
        if self.calibrator is None:
            return raw_probs
        try:
            if self._calibrator_type == "platt":
                cal = self.calibrator.predict_proba(raw_probs.reshape(-1, 1))[:, 1]
            else:
                cal = self.calibrator.predict(raw_probs)
            return np.clip(cal, 0.05, 0.95)
        except Exception:
            return np.clip(raw_probs, 0.05, 0.95)

    # ── Private helpers ──────────────────────────────────

    def _load_models(self) -> None:
        """Populate classifier/regressor lists, adding optional boosters.

        In ``lightweight`` mode only logistic regression + LightGBM (or XGBoost
        as fallback) are included so that high-throughput loops (player props)
        complete in seconds per prop rather than minutes.
        """
        from copy import deepcopy

        if self.lightweight:
            # Fast ensemble: logistic + one gradient booster only
            light_cls = [("logistic", LogisticRegression(max_iter=2000, C=0.5, solver="lbfgs"))]
            light_reg = [("ridge", Ridge(alpha=2.0))]
            self.classifiers = [(n, deepcopy(m)) for n, m in light_cls]
            self.regressors  = [(n, deepcopy(m)) for n, m in light_reg]
            if _LIGHTGBM_AVAILABLE:
                self.classifiers.append((
                    "lightgbm",
                    LGBMClassifier(n_estimators=200, max_depth=5, learning_rate=0.05,
                                   num_leaves=31, min_child_samples=10,
                                   reg_alpha=0.1, reg_lambda=1.0,
                                   subsample=0.8, colsample_bytree=0.8,
                                   random_state=42, verbose=-1, n_jobs=4),
                ))
                self.regressors.append((
                    "lightgbm",
                    LGBMRegressor(n_estimators=200, max_depth=5, learning_rate=0.05,
                                  num_leaves=31, min_child_samples=10,
                                  reg_alpha=0.1, reg_lambda=1.0,
                                  subsample=0.8, colsample_bytree=0.8,
                                  random_state=42, verbose=-1, n_jobs=4),
                ))
            elif _XGBOOST_AVAILABLE:
                self.classifiers.append((
                    "xgboost",
                    XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.05,
                                  use_label_encoder=False, eval_metric="logloss",
                                  reg_alpha=0.1, reg_lambda=1.0,
                                  subsample=0.8, colsample_bytree=0.8,
                                  random_state=42, verbosity=0, n_jobs=4),
                ))
                self.regressors.append((
                    "xgboost",
                    XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.05,
                                 reg_alpha=0.1, reg_lambda=1.0,
                                 subsample=0.8, colsample_bytree=0.8,
                                 random_state=42, verbosity=0, n_jobs=4),
                ))
            n_cls = len(self.classifiers)
            n_reg = len(self.regressors)
            logger.debug("Lightweight ensemble: %d classifiers, %d regressors", n_cls, n_reg)
            return

        self.classifiers = [(n, deepcopy(m)) for n, m in self.BASE_CLASSIFIERS]
        self.regressors = [(n, deepcopy(m)) for n, m in self.BASE_REGRESSORS]

        if _XGBOOST_AVAILABLE:
            self.classifiers.append(
                (
                    "xgboost",
                    XGBClassifier(
                        n_estimators=300,
                        max_depth=5,
                        learning_rate=0.05,
                        use_label_encoder=False,
                        eval_metric="logloss",
                        reg_alpha=0.1,
                        reg_lambda=1.0,
                        subsample=0.8,
                        colsample_bytree=0.8,
                        min_child_weight=3,
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
                        n_estimators=300,
                        max_depth=5,
                        learning_rate=0.05,
                        reg_alpha=0.1,
                        reg_lambda=1.0,
                        subsample=0.8,
                        colsample_bytree=0.8,
                        min_child_weight=3,
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
                        n_estimators=300,
                        max_depth=5,
                        learning_rate=0.05,
                        num_leaves=31,
                        min_child_samples=10,
                        reg_alpha=0.1,
                        reg_lambda=1.0,
                        subsample=0.8,
                        colsample_bytree=0.8,
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
                        n_estimators=300,
                        max_depth=5,
                        learning_rate=0.05,
                        num_leaves=31,
                        min_child_samples=10,
                        reg_alpha=0.1,
                        reg_lambda=1.0,
                        subsample=0.8,
                        colsample_bytree=0.8,
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
                        iterations=300,
                        depth=5,
                        learning_rate=0.05,
                        l2_leaf_reg=5.0,
                        min_data_in_leaf=5,
                        subsample=0.8,
                        colsample_bylevel=0.8,
                        random_seed=42,
                        verbose=0,
                        od_type="Iter",
                        od_wait=30,
                    ),
                )
            )
            self.regressors.append(
                (
                    "catboost",
                    CatBoostRegressor(
                        iterations=300,
                        depth=5,
                        learning_rate=0.05,
                        l2_leaf_reg=5.0,
                        min_data_in_leaf=5,
                        subsample=0.8,
                        colsample_bylevel=0.8,
                        random_seed=42,
                        verbose=0,
                        od_type="Iter",
                        od_wait=30,
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
        class_weight: str | None = None,
    ) -> dict[str, dict[str, float]]:
        """Train all classifiers and compute Brier-score weights.

        Parameters
        ----------
        X_train, y_train : training features / labels (binary 0/1).
        X_val, y_val     : hold-out validation set for weight estimation.
        class_weight     : pass "balanced" to up-weight minority class (e.g. golf top-10).

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

        # Compute sample weights for imbalanced classes (e.g. golf top-10 at ~6.7%)
        sample_weight: np.ndarray | None = None
        if class_weight == "balanced":
            from sklearn.utils.class_weight import compute_sample_weight
            sample_weight = compute_sample_weight("balanced", y_tr)

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
                    if name == "catboost":
                        model.fit(X_tr, y_tr, eval_set=(X_v, y_v), verbose=False)
                    elif sample_weight is not None:
                        try:
                            model.fit(X_tr, y_tr, sample_weight=sample_weight)
                        except TypeError:
                            model.fit(X_tr, y_tr)
                    else:
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
            from sklearn.linear_model import LogisticRegression

            ensemble_val_probs = self._weighted_probs(X_v)

            # Fit both isotonic and Platt (sigmoid) calibrators; pick lower Brier
            cal_iso = IsotonicRegression(out_of_bounds="clip")
            cal_iso.fit(ensemble_val_probs, y_v)
            brier_iso = brier_score_loss(y_v, cal_iso.predict(ensemble_val_probs))

            cal_platt = LogisticRegression(max_iter=1000)
            cal_platt.fit(ensemble_val_probs.reshape(-1, 1), y_v)
            brier_platt = brier_score_loss(y_v, cal_platt.predict_proba(ensemble_val_probs.reshape(-1, 1))[:, 1])

            if brier_iso <= brier_platt:
                self.calibrator = cal_iso
                self._calibrator_type = "isotonic"
                cal_brier = brier_iso
            else:
                self.calibrator = cal_platt
                self._calibrator_type = "platt"
                cal_brier = brier_platt
            logger.info("Calibrator fitted (%s)  val_brier=%.4f", self._calibrator_type, cal_brier)
        except Exception:
            logger.warning("Could not fit calibrator", exc_info=True)

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
                    if name == "catboost":
                        model.fit(X_tr, y_tr, eval_set=(X_v, y_v), verbose=False)
                    else:
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

        # Calibration then clip to [0.05, 0.95]
        probabilities = self._calibrate(raw_probs)
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
        raw_arr = np.array([raw])
        return float(self._calibrate(raw_arr)[0])

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
        return self._calibrate(raw)

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

    def get_feature_importances(self) -> dict[str, float]:
        """Return averaged feature importances across all tree-based classifiers.

        Supports models with ``feature_importances_`` (RF, ExtraTrees,
        GradientBoosting, XGBoost, LightGBM, CatBoost).  Weights by each
        model's classifier weight.  Returns an empty dict if no tree models
        are fitted or ``feature_names`` is unknown.
        """
        if not self.feature_names:
            return {}

        importance_sum = np.zeros(len(self.feature_names), dtype=np.float64)
        total_w = 0.0
        for name, model in self.classifiers:
            mdl = model.estimator if hasattr(model, "estimator") else model
            if not hasattr(mdl, "feature_importances_"):
                continue
            try:
                imp = np.asarray(mdl.feature_importances_, dtype=np.float64)
                if imp.shape[0] != len(self.feature_names):
                    continue
                w = self.classifier_weights.get(name, 0.01)
                importance_sum += w * imp
                total_w += w
            except Exception:
                continue

        if total_w == 0:
            return {}

        normed = importance_sum / total_w
        return dict(sorted(zip(self.feature_names, normed.tolist()), key=lambda x: -x[1]))

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
