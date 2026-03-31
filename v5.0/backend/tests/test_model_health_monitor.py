"""Unit tests for model health monitor edge cases."""

from __future__ import annotations

import json
import pickle
from datetime import datetime, timezone

from services.model_health_monitor import ModelHealthMonitor


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_game_metrics_reads_date_keyed_prediction_files(tmp_path):
    data_dir = tmp_path
    monitor = ModelHealthMonitor(data_dir)

    _write_json(
        data_dir / "predictions" / "2026-03-30.json",
        {
            "date": "2026-03-30",
            "predictions": [
                {
                    "sport": "nba",
                    "confidence": 0.81,
                    "home_win_prob": 0.6,
                    "away_win_prob": 0.4,
                    "home_score": 110,
                    "away_score": 104,
                    "created_at": "2026-03-30T21:10:00Z",
                },
                {
                    "sport": "nfl",
                    "confidence": 0.72,
                },
            ],
        },
    )

    metrics = monitor._get_game_prediction_metrics("nba")

    assert metrics is not None
    assert metrics.total_predictions == 1
    assert metrics.model_type == "game_prediction"
    assert metrics.status in {"healthy", "degraded", "unhealthy"}


def test_game_metrics_tolerate_unevaluable_predictions_in_confidence_tiers(tmp_path):
    data_dir = tmp_path
    monitor = ModelHealthMonitor(data_dir)

    _write_json(
        data_dir / "predictions" / "2026-03-29.json",
        {
            "predictions": [
                {
                    "sport": "nba",
                    "confidence": 0.82,
                    "created_at": "2026-03-29T18:00:00Z",
                },
                {
                    "sport": "nba",
                    "confidence": 0.66,
                    "home_win_prob": 0.65,
                    "away_win_prob": 0.35,
                    "home_score": 101,
                    "away_score": 95,
                    "created_at": "2026-03-29T19:30:00Z",
                },
            ]
        },
    )

    metrics = monitor._get_game_prediction_metrics("nba")

    assert metrics is not None
    assert metrics.status != "unhealthy"
    tiers = metrics.ensemble_health["confidence_tiers"]
    assert "80-100%" in tiers
    assert "60-70%" in tiers
    assert tiers["80-100%"]["accuracy"] is None
    assert tiers["60-70%"]["accuracy"] == 1.0


def test_game_metrics_support_date_only_timestamps_for_freshness(tmp_path):
    data_dir = tmp_path
    monitor = ModelHealthMonitor(data_dir)

    today = datetime.now(timezone.utc).date().isoformat()
    _write_json(
        data_dir / "predictions" / f"{today}.json",
        {
            "predictions": [
                {
                    "sport": "nba",
                    "confidence": 0.7,
                    "home_win_prob": 0.55,
                    "away_win_prob": 0.45,
                    "home_score": 100,
                    "away_score": 99,
                    "date": today,
                }
            ]
        },
    )

    metrics = monitor._get_game_prediction_metrics("nba")

    assert metrics is not None
    assert metrics.last_prediction_date is not None
    assert metrics.data_freshness_hours is not None
    assert metrics.data_freshness_hours >= 0


def test_game_metrics_ignore_malformed_files(tmp_path):
    data_dir = tmp_path
    monitor = ModelHealthMonitor(data_dir)

    bad_file = data_dir / "predictions" / "2026-03-28.json"
    bad_file.parent.mkdir(parents=True, exist_ok=True)
    bad_file.write_text("not-json", encoding="utf-8")

    _write_json(
        data_dir / "predictions" / "2026-03-27.json",
        {
            "predictions": [
                {
                    "sport": "nba",
                    "confidence": 0.63,
                    "home_win_prob": 0.52,
                    "away_win_prob": 0.48,
                    "home_score": 99,
                    "away_score": 90,
                    "created_at": "2026-03-27T10:00:00Z",
                }
            ]
        },
    )

    metrics = monitor._get_game_prediction_metrics("nba")

    assert metrics is not None
    assert metrics.total_predictions == 1


def test_player_props_metrics_tolerate_date_only_trained_at(tmp_path):
    data_dir = tmp_path / "runtime" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monitor = ModelHealthMonitor(data_dir)

    bundle_path = tmp_path / "ml" / "models" / "nba" / "player_props.pkl"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle = {
        "models": {"points": {"model": "dummy"}},
        "trained_at": datetime.now(timezone.utc).date().isoformat(),
        "feature_names": ["f1", "f2"],
        "seasons": ["2026"],
    }
    with open(bundle_path, "wb") as f:
        pickle.dump(bundle, f)

    metrics = monitor._get_player_props_metrics("nba")

    assert metrics is not None
    assert metrics.model_type == "player_props"
    assert metrics.status in {"healthy", "degraded", "unhealthy"}
    assert metrics.data_freshness_hours is not None


def test_live_metrics_tolerate_date_only_updated_at(tmp_path):
    data_dir = tmp_path / "runtime" / "data"
    monitor = ModelHealthMonitor(data_dir)

    today = datetime.now(timezone.utc).date().isoformat()
    _write_json(
        data_dir / "live_predictions" / "nba_live.json",
        {
            "updated_at": today,
            "games": [{"id": "g1"}],
        },
    )

    metrics = monitor._get_live_prediction_metrics("nba")

    assert metrics is not None
    assert metrics.model_type == "live_prediction"
    assert metrics.data_freshness_hours is not None
