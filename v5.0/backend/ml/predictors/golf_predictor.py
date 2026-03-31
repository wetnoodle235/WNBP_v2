# ──────────────────────────────────────────────────────────
# V5.0 Backend — Golf Predictor
# ──────────────────────────────────────────────────────────
"""
Generate player-level golf predictions from trained models.

Unlike team sports, golf predictions are player-centric:
  win_prob     – probability of winning outright
  top10_prob   – probability of finishing top-10
  score_pred   – predicted score-to-par
  position_pred– predicted finishing position

The model bundle lives at ml/models/golf/joint_models.pkl and is
produced by backend/ml/train_golf.py.
"""

from __future__ import annotations

import logging
import pickle
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Column constants ─────────────────────────────────────

_META_COLS = {"game_id", "date", "player_id", "player_name",
              "position", "score_to_par", "won", "top_10", "season"}


@dataclass
class GolfPlayerPrediction:
    """Prediction for a single player in a tournament."""
    game_id: str
    player_id: str
    player_name: str
    sport: str = "golf"

    win_prob: float = 0.0
    top10_prob: float = 0.0
    score_pred: float = 0.0
    position_pred: float = 0.0

    confidence: float = 0.5
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class GolfPredictor:
    """Load golf models and produce player-level predictions."""

    def __init__(self, models_dir: Path | str) -> None:
        self.models_dir = Path(models_dir)
        self._bundle: dict[str, Any] | None = None
        self._load_models()

    # ── Model loading ────────────────────────────────────

    def _load_models(self) -> None:
        joint_path = self.models_dir / "joint_models.pkl"
        if not joint_path.exists():
            logger.warning(
                "No golf models found at %s — predictions will fail", joint_path
            )
            return
        try:
            with open(joint_path, "rb") as fh:
                self._bundle = pickle.load(fh)  # noqa: S301
            logger.info("Loaded golf models from %s", joint_path)
        except Exception:
            logger.error("Could not load golf models from %s", joint_path, exc_info=True)

    # ── Prediction ───────────────────────────────────────

    def _align_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Align DataFrame columns to training feature names."""
        if self._bundle is None:
            return df
        expected = self._bundle.get("feature_names", [])
        for col in expected:
            if col not in df.columns:
                df[col] = 0.0
        return df[[c for c in expected if c in df.columns]].fillna(0.0)

    def predict_tournament(
        self,
        player_rows: pd.DataFrame,
    ) -> list[GolfPlayerPrediction]:
        """Predict all players in a tournament.

        Parameters
        ----------
        player_rows : DataFrame with one row per player.
            Must include feature columns and optionally
            game_id, player_id, player_name metadata.

        Returns
        -------
        List of GolfPlayerPrediction, one per player row.
        """
        if self._bundle is None:
            raise RuntimeError("No golf models loaded — run train_golf.py first")

        X = self._align_features(player_rows.copy())
        if X.empty:
            return []

        win_ens = self._bundle["win_ensemble"]
        top10_ens = self._bundle["top10_ensemble"]
        score_ens = self._bundle["score_ensemble"]
        pos_ens = self._bundle["position_ensemble"]

        # Batch inference
        win_probs, _, _ = win_ens.predict_class(X)
        top10_probs, _, _ = top10_ens.predict_class(X)
        score_preds = score_ens.predict_regression(X)
        pos_preds = pos_ens.predict_regression(X)

        results: list[GolfPlayerPrediction] = []
        for i, (_, row) in enumerate(player_rows.iterrows()):
            game_id = str(row.get("game_id", ""))
            player_id = str(row.get("player_id", str(i)))
            player_name = str(row.get("player_name", ""))
            wp = float(np.clip(win_probs[i], 0.0, 1.0))
            t10p = float(np.clip(top10_probs[i], 0.0, 1.0))
            score = float(score_preds[i])
            pos = float(pos_preds[i])

            # Confidence: average of classification confidences
            confidence = float(np.clip(0.5 + abs(wp - 0.5) + abs(t10p - 0.5), 0.5, 0.99))

            results.append(GolfPlayerPrediction(
                game_id=game_id,
                player_id=player_id,
                player_name=player_name,
                win_prob=round(wp, 4),
                top10_prob=round(t10p, 4),
                score_pred=round(score, 2),
                position_pred=round(max(1.0, pos), 1),
                confidence=round(confidence, 4),
            ))

        return results

    def predict_from_precomputed(
        self,
        precomp_df: pd.DataFrame,
        game_ids: list[str] | None = None,
    ) -> dict[str, list[GolfPlayerPrediction]]:
        """Predict all players for each tournament in precomp_df.

        Parameters
        ----------
        precomp_df : Pre-computed feature DataFrame (golf_all.parquet format).
                     Can be indexed by game_id or have a game_id column.
        game_ids   : If provided, restrict to these tournament IDs.

        Returns
        -------
        Dict mapping game_id → list of GolfPlayerPrediction.
        """
        # Normalise index
        df = precomp_df.reset_index() if precomp_df.index.name == "game_id" else precomp_df.copy()
        if "game_id" not in df.columns:
            logger.warning("precomp_df has no game_id column — cannot predict")
            return {}

        if game_ids is not None:
            df = df[df["game_id"].isin(game_ids)]

        out: dict[str, list[GolfPlayerPrediction]] = {}
        for gid, grp in df.groupby("game_id"):
            try:
                preds = self.predict_tournament(grp)
                out[str(gid)] = preds
            except Exception:
                logger.warning("Prediction failed for tournament %s", gid, exc_info=True)
        return out
