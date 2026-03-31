# ──────────────────────────────────────────────────────────
# V5.0 Backend — ML Package
# ──────────────────────────────────────────────────────────
"""
Machine-learning pipeline: ensemble training, game prediction
and player-prop prediction.

Quick start::

    from ml.train import Trainer
    from ml.models.base import TrainingConfig
    from ml.predictors import GamePredictor, PropsPredictor
"""

from ml.models.base import PredictionResult, PropPrediction, TrainingConfig
from ml.models.ensemble import EnsembleVoter
from ml.predictors.game_predictor import GamePredictor
from ml.predictors.props_predictor import PropsPredictor
from ml.train import Trainer

__all__ = [
    "EnsembleVoter",
    "GamePredictor",
    "PredictionResult",
    "PropPrediction",
    "PropsPredictor",
    "Trainer",
    "TrainingConfig",
]
