"""
Model health monitoring and diagnostics.

Provides real-time metrics on:
- Model accuracy and win rates
- Feature freshness and data staleness
- Inference latency and throughput
- Ensemble health (mean confidence, tier breakdown)
- Data quality and completeness
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import json

logger = logging.getLogger(__name__)

@dataclass
class ModelMetrics:
    """Health metrics for a single model."""
    sport: str
    model_type: str  # "game_prediction", "player_props", "live_prediction"
    status: str  # "healthy", "degraded", "unhealthy"
    accuracy: Optional[float]  # Latest accuracy rate
    win_rate: Optional[float]  # Percentage of correct predictions
    total_predictions: int  # Total predictions ever made
    predictions_today: int  # Predictions made today
    mean_confidence: Optional[float]  # Average model confidence
    avg_inference_time_ms: Optional[float]  # Average inference latency
    last_training_date: Optional[str]  # ISO format
    last_prediction_date: Optional[str]  # ISO format
    data_freshness_hours: Optional[float]  # Hours since last data update
    feature_completeness: Optional[float]  # 0-1 percent of features available
    ensemble_health: Optional[dict]  # Details on ensemble voting patterns
    warnings: list[str]  # Health alerts
    cached_at: str


@dataclass
class PlatformHealth:
    """Overall platform health summary."""
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: str
    sports_monitored: int
    models_healthy: int
    models_degraded: int
    models_unhealthy: int
    avg_accuracy: Optional[float]
    avg_inference_time_ms: Optional[float]
    total_predictions_today: int
    data_sources_healthy: int
    data_sources_total: int
    alerts: list[str]


class ModelHealthMonitor:
    """Monitor and report on model health across sports."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.predictions_dir = data_dir / "predictions"
        self.live_predictions_dir = data_dir / "live_predictions"

    @staticmethod
    def _parse_timestamp(value: str | None) -> Optional[datetime]:
        """Parse ISO timestamp or YYYY-MM-DD date to timezone-aware datetime."""
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def get_sport_model_health(self, sport: str) -> dict[str, ModelMetrics]:
        """Get health metrics for all models for a sport."""
        metrics = {}

        # Game prediction model
        game_metrics = self._get_game_prediction_metrics(sport)
        if game_metrics:
            metrics["game_prediction"] = game_metrics

        # Player props model
        props_metrics = self._get_player_props_metrics(sport)
        if props_metrics:
            metrics["player_props"] = props_metrics

        # Live prediction model
        live_metrics = self._get_live_prediction_metrics(sport)
        if live_metrics:
            metrics["live_prediction"] = live_metrics

        return metrics

    def _get_game_prediction_metrics(self, sport: str) -> Optional[ModelMetrics]:
        """Analyze game prediction model health.

        Predictions are stored in date-keyed files: predictions/{YYYY-MM-DD}.json
        Each file contains {"predictions": [...], "date": "..."} where each prediction
        object has a "sport" field.  We scan the three most recent files and aggregate
        all predictions for the requested sport.
        """
        if not self.predictions_dir.exists():
            return None

        # Collect the 3 most-recent date-keyed prediction files
        date_files = sorted(self.predictions_dir.glob("[0-9][0-9][0-9][0-9]-*.json"), reverse=True)
        if not date_files:
            return None

        preds: list[dict] = []
        for pred_file in date_files[:3]:
            try:
                with open(pred_file) as f:
                    data = json.load(f)
                raw = data.get("predictions", data if isinstance(data, list) else [])
                preds.extend(p for p in raw if p.get("sport") == sport)
            except Exception:
                continue

        if not preds:
            return None

        try:
            # Calculate metrics
            total = len(preds)
            today = datetime.now(timezone.utc).date().isoformat()
            today_count = sum(
                1 for p in preds
                if (p.get("date") or p.get("created_at") or "").startswith(today)
            )

            # Accuracy calculation
            evaluable = [p for p in preds if self._is_evaluable(p)]
            correct = sum(1 for p in evaluable if self._is_correct(p))
            accuracy = correct / len(evaluable) if evaluable else None
            win_rate = accuracy

            # Confidence metrics
            confidences = [p.get("confidence") for p in preds if p.get("confidence") is not None]
            mean_conf = sum(confidences) / len(confidences) if confidences else None

            # Data freshness
            parsed_dates = [
                self._parse_timestamp(p.get("created_at") or p.get("date"))
                for p in preds
                if p.get("created_at") or p.get("date")
            ]
            parsed_dates = [d for d in parsed_dates if d is not None]
            last_pred_dt = max(parsed_dates) if parsed_dates else None
            last_pred_date = last_pred_dt.isoformat() if last_pred_dt else None
            freshness_hours = None
            if last_pred_dt:
                freshness_hours = (datetime.now(timezone.utc) - last_pred_dt).total_seconds() / 3600

            status = "healthy"
            warnings = []
            if today_count == 0:
                status = "degraded"
                warnings.append("No predictions made today")
            if accuracy and accuracy < 0.52:
                status = "degraded"
                warnings.append(f"Accuracy ({accuracy:.1%}) below threshold")
            if freshness_hours and freshness_hours > 48:
                status = "degraded"
                warnings.append(f"Data is {freshness_hours:.0f} hours stale")

            return ModelMetrics(
                sport=sport,
                model_type="game_prediction",
                status=status,
                accuracy=accuracy,
                win_rate=win_rate,
                total_predictions=total,
                predictions_today=today_count,
                mean_confidence=mean_conf,
                avg_inference_time_ms=None,  # Would come from logs
                last_training_date=None,  # Would come from metadata
                last_prediction_date=last_pred_date,
                data_freshness_hours=freshness_hours,
                feature_completeness=0.95,  # Placeholder
                ensemble_health={"confidence_tiers": self._compute_confidence_tiers(preds)},
                warnings=warnings,
                cached_at=datetime.now(timezone.utc).isoformat(),
            )

        except Exception as e:
            logger.exception("Failed to get game prediction metrics for %s", sport)
            return ModelMetrics(
                sport=sport,
                model_type="game_prediction",
                status="unhealthy",
                accuracy=None,
                win_rate=None,
                total_predictions=0,
                predictions_today=0,
                mean_confidence=None,
                avg_inference_time_ms=None,
                last_training_date=None,
                last_prediction_date=None,
                data_freshness_hours=None,
                feature_completeness=None,
                ensemble_health=None,
                warnings=[f"Error loading data: {str(e)}"],
                cached_at=datetime.now(timezone.utc).isoformat(),
            )

    def _get_player_props_metrics(self, sport: str) -> Optional[ModelMetrics]:
        """Analyze player props model health."""
        # Check if model bundle exists
        ml_dir = self.data_dir.parent.parent / "ml" / "models"
        bundle_path = ml_dir / sport / "player_props.pkl"

        if not bundle_path.exists():
            return None

        try:
            import pickle
            with open(bundle_path, "rb") as f:
                bundle = pickle.load(f)  # noqa: S301

            models = bundle.get("models", {})
            trained_at = bundle.get("trained_at")
            feature_count = len(bundle.get("feature_names", []))
            seasons = bundle.get("seasons", [])

            # Check model freshness
            freshness_hours = None
            if trained_at:
                trained_time = self._parse_timestamp(trained_at)
                if trained_time:
                    freshness_hours = (datetime.now(timezone.utc) - trained_time).total_seconds() / 3600

            status = "healthy"
            warnings = []
            if freshness_hours and freshness_hours > 168:  # older than 1 week
                status = "degraded"
                warnings.append(f"Model trained {freshness_hours/24:.0f} days ago")
            if not models:
                status = "degraded"
                warnings.append("No trained props in bundle")

            return ModelMetrics(
                sport=sport,
                model_type="player_props",
                status=status,
                accuracy=None,
                win_rate=None,
                total_predictions=len(models),
                predictions_today=len(models),
                mean_confidence=None,
                avg_inference_time_ms=None,
                last_training_date=trained_at,
                last_prediction_date=None,
                data_freshness_hours=freshness_hours,
                feature_completeness=None,
                ensemble_health={
                    "trained_props": len(models),
                    "supported_seasons": seasons,
                    "feature_count": feature_count,
                },
                warnings=warnings,
                cached_at=datetime.now(timezone.utc).isoformat(),
            )

        except Exception as e:
            logger.exception("Failed to get player props metrics for %s", sport)
            return ModelMetrics(
                sport=sport,
                model_type="player_props",
                status="unhealthy",
                accuracy=None,
                win_rate=None,
                total_predictions=0,
                predictions_today=0,
                mean_confidence=None,
                avg_inference_time_ms=None,
                last_training_date=None,
                last_prediction_date=None,
                data_freshness_hours=None,
                feature_completeness=None,
                ensemble_health=None,
                warnings=[f"Error loading model: {str(e)}"],
                cached_at=datetime.now(timezone.utc).isoformat(),
            )

    def _get_live_prediction_metrics(self, sport: str) -> Optional[ModelMetrics]:
        """Analyze live prediction model health."""
        live_file = self.live_predictions_dir / f"{sport}_live.json"
        if not live_file.exists():
            return None

        try:
            with open(live_file) as f:
                data = json.load(f)

            games = data.get("games", [])
            last_update = data.get("updated_at")

            freshness_hours = None
            if last_update:
                update_time = self._parse_timestamp(last_update)
                if update_time:
                    freshness_hours = (datetime.now(timezone.utc) - update_time).total_seconds() / 3600

            status = "healthy"
            warnings = []

            if len(games) == 0:
                status = "degraded"
                warnings.append("No live games currently tracked")

            if freshness_hours and freshness_hours > 2:
                status = "degraded"
                warnings.append(f"Live data {freshness_hours:.1f} hours stale")

            return ModelMetrics(
                sport=sport,
                model_type="live_prediction",
                status=status,
                accuracy=None,
                win_rate=None,
                total_predictions=len(games),
                predictions_today=len(games),
                mean_confidence=None,
                avg_inference_time_ms=None,
                last_training_date=None,
                last_prediction_date=last_update,
                data_freshness_hours=freshness_hours,
                feature_completeness=0.9,
                ensemble_health={"active_games": len(games)},
                warnings=warnings,
                cached_at=datetime.now(timezone.utc).isoformat(),
            )

        except Exception as e:
            logger.exception("Failed to get live prediction metrics for %s", sport)
            return None

    @staticmethod
    def _is_evaluable(pred: dict) -> bool:
        """Check if prediction has all required fields for evaluation."""
        return (
            pred.get("home_win_prob") is not None
            and pred.get("away_win_prob") is not None
            and pred.get("home_score") is not None
            and pred.get("away_score") is not None
        )

    @staticmethod
    def _is_correct(pred: dict) -> bool:
        """Check if prediction was correct."""
        return (pred["home_win_prob"] > pred["away_win_prob"]) == (pred["home_score"] > pred["away_score"])

    @staticmethod
    def _compute_confidence_tiers(preds: list[dict]) -> dict:
        """Break down predictions by confidence tier."""
        tiers = {
            "80-100%": [],
            "70-80%": [],
            "60-70%": [],
            "<60%": [],
        }

        for p in preds:
            conf = p.get("confidence", 0)
            if conf >= 0.8:
                tier = "80-100%"
            elif conf >= 0.7:
                tier = "70-80%"
            elif conf >= 0.6:
                tier = "60-70%"
            else:
                tier = "<60%"
            tiers[tier].append(p)

        return {
            tier: {
                "count": len(preds_list),
                "accuracy": (
                    sum(1 for p in preds_list if ModelHealthMonitor._is_evaluable(p) and ModelHealthMonitor._is_correct(p))
                    / len([p for p in preds_list if ModelHealthMonitor._is_evaluable(p)])
                    if any(ModelHealthMonitor._is_evaluable(p) for p in preds_list)
                    else None
                ),
            }
            for tier, preds_list in tiers.items()
        }
