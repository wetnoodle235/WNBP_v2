# ──────────────────────────────────────────────────────────
# V5.0 Backend — Golf ML Training Pipeline
# ──────────────────────────────────────────────────────────
"""
Train player-level golf models using pre-computed features.

Golf is a player-centric sport (not home vs away), so standard
train.py cannot be used.  This script:

  1. Loads  data/features/golf_all.parquet  (player-tournament rows)
  2. Splits temporally (last 20 % as validation)
  3. Trains four EnsembleVoters:
       win_ensemble      – P(player wins outright)        classifier
       top10_ensemble    – P(player finishes top-10)      classifier
       score_ensemble    – predicted score-to-par         regressor
       position_ensemble – predicted finishing position   regressor
  4. Saves  ml/models/golf/joint_models.pkl

Usage
-----
::

    PYTHONPATH=backend python backend/ml/train_golf.py
    PYTHONPATH=backend python backend/ml/train_golf.py --data-dir data
"""

from __future__ import annotations

import argparse
import logging
import pickle
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    mean_absolute_error,
    roc_auc_score,
)

# ── Path setup so we can import backend modules ──────────

_HERE = Path(__file__).resolve().parent
_BACKEND = _HERE.parent
_PROJECT = _BACKEND.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from ml.models.ensemble import EnsembleVoter  # noqa: E402

# ── Logging ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("train_golf")

# ── Feature / target column definitions ─────────────────

FEATURE_COLS = [
    "form_avg_finish",
    "form_finish_std",
    "form_top_10_rate",
    "form_top_25_rate",
    "form_win_rate",
    "form_cut_made_rate",
    "form_avg_score_to_par",
    "form_avg_rounds",
    "form_tournaments_played",
    "scoring_avg",
    "scoring_consistency",
    "best_score_to_par",
    "worst_score_to_par",
    "field_size",
    "field_avg_finish",
    "rest_days",
    "tournaments_last_30d",
    "momentum_trend",
    "improving",
]

TARGET_WIN = "won"
TARGET_TOP10 = "top_10"
TARGET_SCORE = "score_to_par"
TARGET_POS = "position"

META_COLS = {"game_id", "date", "player_id", "player_name"}


# ── Helpers ──────────────────────────────────────────────


def _temporal_split(
    df: pd.DataFrame, val_frac: float = 0.20
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sort by date and take the last *val_frac* as validation."""
    df = df.sort_values("date").reset_index(drop=True)
    split = int(len(df) * (1.0 - val_frac))
    return df.iloc[:split].copy(), df.iloc[split:].copy()


def _xy(
    df: pd.DataFrame, target: str
) -> tuple[pd.DataFrame, pd.Series]:
    """Return (feature matrix, target series) for a DataFrame slice."""
    available = [c for c in FEATURE_COLS if c in df.columns]
    return df[available].fillna(0.0), df[target].fillna(0.0)


def _train_classifier(
    name: str,
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    target: str,
) -> tuple[EnsembleVoter, dict[str, Any]]:
    """Fit a classifier EnsembleVoter and return (voter, metrics)."""
    logger.info("── Training classifier: %s (target=%s) ──", name, target)
    X_tr, y_tr = _xy(train_df, target)
    X_v, y_v = _xy(val_df, target)

    voter = EnsembleVoter()
    cls_metrics = voter.fit_classifiers(X_tr, y_tr, X_v, y_v)

    # Evaluation on validation set
    probs, preds, _ = voter.predict_class(X_v)
    y_v_arr = y_v.values

    acc = accuracy_score(y_v_arr, preds)
    brier = brier_score_loss(y_v_arr, probs)
    try:
        auc = roc_auc_score(y_v_arr, probs)
    except ValueError:
        auc = float("nan")

    # Top-N accuracy: for each tournament, rank by predicted probability
    top_n_acc = _top_n_accuracy(val_df, probs, target, n=1 if target == TARGET_WIN else 10)

    metrics = {
        "accuracy": round(acc, 4),
        "brier_score": round(brier, 4),
        "roc_auc": round(auc, 4),
        "top_n_accuracy": round(top_n_acc, 4),
        "val_size": len(val_df),
        "train_size": len(train_df),
        "pos_rate_train": round(float(y_tr.mean()), 4),
        "pos_rate_val": round(float(y_v.mean()), 4),
        "models": cls_metrics,
    }

    logger.info(
        "  %s  acc=%.4f  brier=%.4f  auc=%.4f  top_n_acc=%.4f",
        name, acc, brier, auc, top_n_acc,
    )
    return voter, metrics


def _train_regressor(
    name: str,
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    target: str,
) -> tuple[EnsembleVoter, dict[str, Any]]:
    """Fit a regressor EnsembleVoter and return (voter, metrics)."""
    logger.info("── Training regressor: %s (target=%s) ──", name, target)
    X_tr, y_tr = _xy(train_df, target)
    X_v, y_v = _xy(val_df, target)

    voter = EnsembleVoter()
    reg_metrics = voter.fit_regressors(X_tr, y_tr, X_v, y_v)

    # Evaluation
    preds_arr = voter.predict_regression(X_v)
    y_v_arr = y_v.values
    mae = mean_absolute_error(y_v_arr, preds_arr)
    rmse = float(np.sqrt(np.mean((y_v_arr - preds_arr) ** 2)))

    metrics = {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "val_size": len(val_df),
        "train_size": len(train_df),
        "models": reg_metrics,
    }

    logger.info("  %s  mae=%.4f  rmse=%.4f", name, mae, rmse)
    return voter, metrics


def _top_n_accuracy(
    df: pd.DataFrame,
    probs: np.ndarray,
    target: str,
    n: int = 1,
) -> float:
    """Per-tournament ranking accuracy.

    For winner (n=1): fraction of tournaments where the highest-ranked
    player actually has target=1.
    For top-10 (n=10): fraction of player-rows where the top-10 predicted
    players overlap with the actual top-10 (Jaccard / recall proxy).
    """
    df = df.copy()
    df["_prob"] = probs
    df["_actual"] = df[target].values

    results: list[float] = []
    for _, grp in df.groupby("game_id"):
        if grp["_actual"].sum() == 0:
            continue  # skip if no positive examples in tournament
        ranked = grp.sort_values("_prob", ascending=False)
        top_pred_actual = ranked.head(n)["_actual"].values
        if n == 1:
            results.append(float(top_pred_actual[0]))
        else:
            actual_top = set(grp[grp["_actual"] == 1].index.tolist())
            pred_top = set(ranked.head(n).index.tolist())
            if not actual_top:
                continue
            recall = len(actual_top & pred_top) / len(actual_top)
            results.append(recall)

    return float(np.mean(results)) if results else 0.0


# ── Main training function ───────────────────────────────


def train_golf(
    data_dir: Path,
    models_dir: Path,
    val_frac: float = 0.20,
) -> dict[str, Any]:
    """End-to-end golf training pipeline.

    Parameters
    ----------
    data_dir  : project data directory (contains features/ subdirectory)
    models_dir: where to save ml/models/golf/joint_models.pkl
    val_frac  : temporal validation fraction (default 20 %)

    Returns
    -------
    Dict with training metrics for all four models.
    """
    feat_file = data_dir / "features" / "golf_all.parquet"
    if not feat_file.exists():
        raise FileNotFoundError(f"Golf features not found: {feat_file}")

    logger.info("Loading golf features from %s", feat_file)
    df = pd.read_parquet(feat_file)
    logger.info("  Loaded %d rows × %d cols", len(df), len(df.columns))

    # Validate required columns
    missing_feats = [c for c in FEATURE_COLS if c not in df.columns]
    if missing_feats:
        raise ValueError(f"Missing feature columns: {missing_feats}")

    for target in (TARGET_WIN, TARGET_TOP10, TARGET_SCORE, TARGET_POS):
        if target not in df.columns:
            raise ValueError(f"Missing target column: {target}")

    # Temporal split
    train_df, val_df = _temporal_split(df, val_frac)
    logger.info(
        "Temporal split: %d train rows (%s – %s), %d val rows (%s – %s)",
        len(train_df),
        str(train_df["date"].min())[:10],
        str(train_df["date"].max())[:10],
        len(val_df),
        str(val_df["date"].min())[:10],
        str(val_df["date"].max())[:10],
    )

    all_metrics: dict[str, Any] = {}

    # 1. Win probability (classifier)
    win_ens, win_metrics = _train_classifier(
        "win_ensemble", train_df, val_df, TARGET_WIN
    )
    all_metrics["win"] = win_metrics

    # 2. Top-10 probability (classifier)
    top10_ens, top10_metrics = _train_classifier(
        "top10_ensemble", train_df, val_df, TARGET_TOP10
    )
    all_metrics["top10"] = top10_metrics

    # 3. Score-to-par (regressor)
    score_ens, score_metrics = _train_regressor(
        "score_ensemble", train_df, val_df, TARGET_SCORE
    )
    all_metrics["score_to_par"] = score_metrics

    # 4. Finishing position (regressor)
    pos_ens, pos_metrics = _train_regressor(
        "position_ensemble", train_df, val_df, TARGET_POS
    )
    all_metrics["position"] = pos_metrics

    # Build bundle
    feature_names = [c for c in FEATURE_COLS if c in df.columns]
    bundle: dict[str, Any] = {
        "win_ensemble": win_ens,
        "top10_ensemble": top10_ens,
        "score_ensemble": score_ens,
        "position_ensemble": pos_ens,
        "feature_names": feature_names,
        "metrics": all_metrics,
        "trained_at": datetime.utcnow().isoformat(),
        "sport": "golf",
        "val_frac": val_frac,
        "n_train": len(train_df),
        "n_val": len(val_df),
    }

    # Save
    out_path = models_dir / "joint_models.pkl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as fh:
        pickle.dump(bundle, fh, protocol=pickle.HIGHEST_PROTOCOL)
    logger.info("Saved golf models → %s", out_path)

    # Summary
    logger.info("=" * 60)
    logger.info("Golf training summary")
    logger.info("  Win        acc=%.4f  brier=%.4f  auc=%.4f  top1_acc=%.4f",
                win_metrics["accuracy"], win_metrics["brier_score"],
                win_metrics["roc_auc"], win_metrics["top_n_accuracy"])
    logger.info("  Top-10     acc=%.4f  brier=%.4f  auc=%.4f  top10_recall=%.4f",
                top10_metrics["accuracy"], top10_metrics["brier_score"],
                top10_metrics["roc_auc"], top10_metrics["top_n_accuracy"])
    logger.info("  Score-to-par  mae=%.4f  rmse=%.4f",
                score_metrics["mae"], score_metrics["rmse"])
    logger.info("  Position      mae=%.4f  rmse=%.4f",
                pos_metrics["mae"], pos_metrics["rmse"])
    logger.info("=" * 60)

    return all_metrics


# ── CLI entry point ──────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train golf ML models (player-level win/top-10/score/position)"
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Project data directory (default: auto-detected relative to this script)",
    )
    parser.add_argument(
        "--models-dir",
        default=None,
        help="Output directory for golf models (default: ml/models/golf/ in project root)",
    )
    parser.add_argument(
        "--val-frac",
        type=float,
        default=0.20,
        help="Fraction of data to use as temporal validation set (default: 0.20)",
    )
    args = parser.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        data_dir = _PROJECT / "data"

    if args.models_dir:
        models_dir = Path(args.models_dir)
    else:
        models_dir = _PROJECT / "ml" / "models" / "golf"

    logger.info("data_dir   : %s", data_dir)
    logger.info("models_dir : %s", models_dir)

    try:
        metrics = train_golf(data_dir, models_dir, val_frac=args.val_frac)
        logger.info("Training complete.")
        sys.exit(0)
    except Exception:
        logger.exception("Golf training failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
