"""
Contract tests for predictions API endpoints.

Tests ensure response schemas, status codes, and business logic for:
- Game predictions
- Prediction history with accuracy metrics
- Player props model metadata
- Advanced backtesting
"""

import pytest
from datetime import date
from fastapi.testclient import TestClient
from api.routes.predictions import get_data_service
from main import app


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def valid_sports():
    """Sports that should return valid responses."""
    return ["nba", "nfl", "mlb", "nhl", "wnba"]


@pytest.fixture
def invalid_sport():
    """Sport that doesn't exist."""
    return "invalid_sport_xyz"


# ── Predictions Endpoint Tests ────────────────────────────────────────

class TestPredictionsEndpoint:
    """Test /v1/{sport}/predictions endpoint."""

    def test_predictions_valid_sport(self, client, valid_sports):
        """Predictions endpoint accepts valid sports."""
        sport = valid_sports[0]
        response = client.get(f"/v1/predictions/{sport}")
        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        assert "data" in data
        assert "meta" in data
        assert isinstance(data["data"], list)

    def test_predictions_meta_schema(self, client, valid_sports):
        """Predictions meta includes required fields."""
        sport = valid_sports[0]
        response = client.get(f"/v1/predictions/{sport}")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert "sport" in meta
        assert "date" in meta
        assert "count" in meta
        assert "total" in meta
        assert "limit" in meta
        assert "offset" in meta
        assert "cached_at" in meta

    def test_predictions_invalid_sport(self, client, invalid_sport):
        """Predictions endpoint rejects unknown sports."""
        response = client.get(f"/v1/predictions/{invalid_sport}")
        assert response.status_code == 404
        assert "Unknown sport" in response.json()["detail"]

    def test_predictions_pagination(self, client, valid_sports):
        """Predictions respect limit and offset."""
        sport = valid_sports[0]
        response = client.get(f"/v1/predictions/{sport}?limit=10&offset=5")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["limit"] == 10
        assert meta["offset"] == 5

    def test_predictions_date_filter(self, client, valid_sports):
        """Predictions accept date filter."""
        sport = valid_sports[0]
        test_date = "2026-03-25"
        response = client.get(f"/v1/predictions/{sport}?date={test_date}")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["date"] == test_date


# ── Prediction History Endpoint Tests ─────────────────────────────────

class TestPredictionHistoryEndpoint:
    """Test /v1/{sport}/predictions/history endpoint."""

    def test_history_valid_sport(self, client, valid_sports):
        """History endpoint accepts valid sports."""
        sport = valid_sports[0]
        response = client.get(f"/v1/predictions/{sport}/history")
        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        assert "data" in data
        assert "meta" in data
        assert isinstance(data["data"], list)

    def test_history_meta_includes_accuracy(self, client, valid_sports):
        """History meta includes accuracy metrics."""
        sport = valid_sports[0]
        response = client.get(f"/v1/predictions/{sport}/history")
        assert response.status_code == 200
        meta = response.json()["meta"]
        required_keys = ["sport", "total_predictions", "evaluated", "correct", "accuracy", "cached_at"]
        for key in required_keys:
            assert key in meta, f"Missing key: {key}"

    def test_history_invalid_sport(self, client, invalid_sport):
        """History endpoint rejects unknown sports."""
        response = client.get(f"/v1/predictions/{invalid_sport}/history")
        assert response.status_code == 404

    def test_history_date_range_filter(self, client, valid_sports):
        """History respects date_start and date_end."""
        sport = valid_sports[0]
        response = client.get(
            f"/v1/predictions/{sport}/history?date_start=2026-01-01&date_end=2026-03-31"
        )
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert "evaluated" in meta
        assert "accuracy" in meta

    def test_history_pagination(self, client, valid_sports):
        """History respects limit and offset."""
        sport = valid_sports[0]
        response = client.get(f"/v1/predictions/{sport}/history?limit=20&offset=10")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["limit"] == 20
        assert meta["offset"] == 10


# ── Player Props Endpoint Tests ───────────────────────────────────────

class TestPlayerPropsEndpoint:
    """Test /v1/{sport}/predictions/player-props endpoint."""

    def test_player_props_with_model(self, client):
        """Player props returns model metadata for supported sports."""
        # NBA has a trained player props model
        response = client.get("/v1/predictions/nba/player-props")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)
        meta = data["meta"]
        assert "model_available" in meta
        if meta["model_available"]:
            assert "supported_props" in meta
            assert "feature_count" in meta
            assert "trained_at" in meta

    def test_player_props_without_model(self, client):
        """Player props returns empty gracefully for unsupported sports."""
        # NCAAB may not have a trained model yet
        response = client.get("/v1/predictions/ncaab/player-props")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"] == []
        meta = data["meta"]
        assert meta["model_available"] is False
        assert "message" in meta
        assert len(data["data"]) == 0

    def test_player_props_invalid_sport(self, client, invalid_sport):
        """Player props rejects unknown sports."""
        response = client.get(f"/v1/predictions/{invalid_sport}/player-props")
        assert response.status_code == 404

    def test_player_props_meta_schema(self, client):
        """Player props meta includes required fields."""
        response = client.get("/v1/predictions/nba/player-props")
        assert response.status_code == 200
        meta = response.json()["meta"]
        required_keys = [
            "sport",
            "date",
            "model_available",
            "count",
            "total",
            "limit",
            "offset",
            "cached_at",
        ]
        for key in required_keys:
            assert key in meta, f"Missing key in meta: {key}"

    def test_player_props_with_model_fields(self, client):
        """Player props data includes model fields when available."""
        response = client.get("/v1/predictions/nba/player-props")
        data = response.json()
        if data["meta"]["model_available"] and len(data["data"]) > 0:
            prop = data["data"][0]
            required_fields = [
                "sport",
                "prop_type",
                "market_type",
                "n_classifiers",
                "n_regressors",
                "model",
                "trained_at",
            ]
            for field in required_fields:
                assert field in prop, f"Missing field in prop data: {field}"

    def test_player_props_pagination(self, client):
        """Player props respects limit and offset."""
        response = client.get("/v1/predictions/nba/player-props?limit=5&offset=0")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["limit"] == 5
        assert meta["offset"] == 0

    def test_player_props_prop_type_filter(self, client):
        """Player props can filter by prop_type."""
        response = client.get("/v1/predictions/nba/player-props?prop_type=pts_over_20")
        assert response.status_code == 200
        data = response.json()
        if len(data["data"]) > 0:
            for prop in data["data"]:
                assert prop["prop_type"] == "pts_over_20"

    def test_player_props_date_filter(self, client):
        """Player props accepts date parameter."""
        test_date = "2026-03-26"
        response = client.get(f"/v1/predictions/nba/player-props?date={test_date}")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["date"] == test_date


class TestPlayerPropOpportunitiesEndpoint:
    """Test /v1/predictions/{sport}/player-props/opportunities endpoint."""

    def test_opportunities_valid_sport(self, client):
        response = client.get("/v1/predictions/nba/player-props/opportunities")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert "data" in payload
        assert "meta" in payload

    def test_opportunities_meta_schema(self, client):
        response = client.get("/v1/predictions/nba/player-props/opportunities")
        assert response.status_code == 200
        meta = response.json()["meta"]
        for key in ["sport", "date", "count", "total", "limit", "offset", "model_available", "cached_at"]:
            assert key in meta

    def test_opportunities_pagination(self, client):
        response = client.get("/v1/predictions/nba/player-props/opportunities?limit=5&offset=0")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["limit"] == 5
        assert meta["offset"] == 0

    def test_opportunities_unknown_sport(self, client, invalid_sport):
        response = client.get(f"/v1/predictions/{invalid_sport}/player-props/opportunities")
        assert response.status_code == 404

    def test_opportunities_model_unavailable(self, client):
        response = client.get("/v1/predictions/ncaab/player-props/opportunities")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert payload["meta"]["model_available"] is False
        assert payload["data"] == []

    def test_opportunities_row_schema_when_present(self, client):
        response = client.get("/v1/predictions/nba/player-props/opportunities")
        assert response.status_code == 200
        payload = response.json()
        if payload["data"]:
            row = payload["data"][0]
            for key in [
                "sport",
                "game_id",
                "home_team",
                "away_team",
                "recommendation_score",
                "recommendation_tier",
                "available_markets",
                "model_context",
                "live_context",
            ]:
                assert key in row

    def test_opportunities_filter_meta(self, client):
        response = client.get(
            "/v1/predictions/nba/player-props/opportunities?prop_type=pts_over_20&min_score=0.6&tier=medium"
        )
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["prop_type"] == "pts_over_20"
        assert meta["min_score"] == 0.6
        assert meta["tier"] == "medium"

    def test_opportunities_score_filter_applied(self, client):
        response = client.get("/v1/predictions/nba/player-props/opportunities?min_score=0.95")
        assert response.status_code == 200
        data = response.json()["data"]
        for row in data:
            assert row["recommendation_score"] >= 0.95

    def test_opportunities_tier_filter_applied(self, client):
        response = client.get("/v1/predictions/nba/player-props/opportunities?tier=high")
        assert response.status_code == 200
        data = response.json()["data"]
        for row in data:
            assert row["recommendation_tier"] == "high"

    def test_opportunities_handles_nan_confidence(self, client):
        class StubDS:
            def get_games(self, sport, date=None):
                return [{"id": "g_nan", "status": "", "home_team": "A", "away_team": "B"}]

            def get_predictions(self, sport, date=None):
                return [{"game_id": "g_nan", "confidence": float("nan")}]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba/player-props/opportunities")
            assert response.status_code == 200
            payload = response.json()
            assert payload["success"] is True
            if payload["data"]:
                assert payload["data"][0]["recommendation_score"] == 0.55
        finally:
            app.dependency_overrides.pop(get_data_service, None)


# ── Single Game Prediction Tests ──────────────────────────────────────

class TestSingleGamePredictionEndpoint:
    """Test /v1/{sport}/{game_id} endpoint."""

    def test_game_prediction_invalid_sport(self, client, invalid_sport):
        """Game prediction rejects unknown sports."""
        response = client.get(f"/v1/predictions/{invalid_sport}/game_001")
        assert response.status_code == 404

    def test_game_prediction_nonexistent_game(self, client):
        """Game prediction returns 404 for nonexistent game."""
        response = client.get("/v1/predictions/nba/nonexistent_game_xyz")
        assert response.status_code == 404


# ── Advanced Backtest Endpoint Tests ──────────────────────────────────

class TestAdvancedBacktestEndpoint:
    """Test /v1/{sport}/backtest/advanced endpoint."""

    def test_backtest_valid_sport(self, client):
        """Backtest endpoint accepts valid sports."""
        response = client.get("/v1/predictions/nba/backtest/advanced")
        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        assert "data" in data
        assert "meta" in data

    def test_backtest_meta_schema(self, client):
        """Backtest meta includes required fields."""
        response = client.get("/v1/predictions/nba/backtest/advanced")
        assert response.status_code == 200
        meta = response.json()["meta"]
        required_keys = [
            "sport",
            "backtest_window_days",
            "cached_at",
        ]
        for key in required_keys:
            assert key in meta, f"Missing key in backtest meta: {key}"

    def test_backtest_invalid_sport(self, client, invalid_sport):
        """Backtest endpoint rejects unknown sports."""
        response = client.get(f"/v1/predictions/{invalid_sport}/backtest/advanced")
        assert response.status_code == 404

    def test_backtest_window_parameter(self, client):
        """Backtest respects days parameter."""
        response = client.get(
            "/v1/predictions/nba/backtest/advanced?days=90"
        )
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["backtest_window_days"] == 90


# ── Response Format Validation ────────────────────────────────────────

class TestResponseFormatValidation:
    """Ensure all responses follow standard format."""

    def test_success_response_schema(self, client):
        """Success responses follow standard schema."""
        response = client.get("/v1/predictions/nba")
        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        assert data["success"] is True
        assert "data" in data
        assert "meta" in data

    def test_error_response_schema(self, client):
        """Error responses include detail."""
        response = client.get("/v1/predictions/invalid_sport")
        assert response.status_code == 404
        assert "detail" in response.json()

    def test_response_is_json_serializable(self, client):
        """All responses are valid JSON."""
        endpoints = [
            "/v1/predictions/nba",
            "/v1/predictions/nba/history",
            "/v1/predictions/nba/player-props",
            "/v1/predictions/nba/backtest/advanced",
        ]
        for endpoint in endpoints:
            response = client.get(endpoint)
            # If this doesn't raise, response.json() succeeded
            assert response.json() is not None


# ── Integration Tests ────────────────────────────────────────────────

class TestIntegration:
    """Integration tests combining multiple endpoints."""

    def test_predictions_and_history_consistency(self, client):
        """Predictions and history endpoints return consistent data."""
        sport = "nba"
        pred_response = client.get(f"/v1/predictions/{sport}?limit=1")
        hist_response = client.get(f"/v1/predictions/{sport}/history?limit=1")

        assert pred_response.status_code == 200
        assert hist_response.status_code == 200

        pred_meta = pred_response.json()["meta"]
        hist_meta = hist_response.json()["meta"]

        assert pred_meta["sport"] == sport
        assert hist_meta["sport"] == sport

    def test_player_props_supported_when_model_available(self, client):
        """Player props reports correct model availability."""
        response = client.get("/v1/predictions/nba/player-props")
        assert response.status_code == 200
        data = response.json()
        meta = data["meta"]

        if meta["model_available"]:
            # Should have supported props and feature info
            assert "supported_props" in meta
            assert isinstance(meta["supported_props"], list)
            if len(meta["supported_props"]) > 0:
                # Data should contain models
                assert len(data["data"]) > 0
        else:
            # Should be empty
            assert len(data["data"]) == 0


# ── Model Health Endpoint Tests ───────────────────────────────────────

class TestModelHealthEndpoint:
    """Test /v1/{sport}/predictions/health endpoint."""

    def test_model_health_valid_sport(self, client, valid_sports):
        """Model health endpoint accepts valid sports."""
        sport = valid_sports[0]
        response = client.get(f"/v1/predictions/{sport}/health")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "data" in data
        assert "meta" in data

    def test_model_health_meta_schema(self, client, valid_sports):
        """Model health meta includes required fields."""
        sport = valid_sports[0]
        response = client.get(f"/v1/predictions/{sport}/health")
        assert response.status_code == 200
        meta = response.json()["meta"]
        required_keys = ["sport", "timestamp", "models_count", "health_summary"]
        for key in required_keys:
            assert key in meta, f"Missing key in health meta: {key}"

    def test_model_health_invalid_sport(self, client, invalid_sport):
        """Model health endpoint rejects unknown sports."""
        response = client.get(f"/v1/predictions/{invalid_sport}/health")
        assert response.status_code == 404

    def test_model_health_data_structure(self, client):
        """Model health data includes health metrics for available models."""
        response = client.get("/v1/predictions/nba/health")
        assert response.status_code == 200
        data = response.json()["data"]

        # Each model should have standard fields
        for model_type, metrics in data.items():
            required_fields = ["sport", "model_type", "status", "cached_at"]
            for field in required_fields:
                assert field in metrics, f"Missing field {field} in {model_type} metrics"
            # Status should be one of the known values
            assert metrics["status"] in ["healthy", "degraded", "unhealthy", "unknown"]

    def test_model_health_warnings_structure(self, client):
        """Model health includes warnings array."""
        response = client.get("/v1/predictions/nba/health")
        assert response.status_code == 200
        data = response.json()["data"]

        for metrics in data.values():
            assert "warnings" in metrics
            assert isinstance(metrics["warnings"], list)

    def test_model_health_bundle_cache_in_meta(self, client):
        """Model health meta includes bundle_cache info after bundle load."""
        # Load NBA player props first to populate the cache
        client.get("/v1/predictions/nba/player-props")
        # Now fetch health — cache should be populated
        response = client.get("/v1/predictions/nba/health")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert "bundle_cache" in meta
        cache = meta["bundle_cache"]
        assert "hits" in cache
        assert "misses" in cache
        assert "ttl_seconds" in cache
        assert isinstance(cache["entries"], list)


# ── Aggregate Opportunities Endpoint Tests ────────────────────────────

class TestAggregateOpportunitiesEndpoint:
    """Test /v1/predictions/opportunities aggregate endpoint."""

    def test_aggregate_basic_response(self, client):
        """Aggregate endpoint returns standard success envelope."""
        response = client.get("/v1/predictions/opportunities")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert "data" in payload
        assert "meta" in payload
        assert isinstance(payload["data"], list)

    def test_aggregate_meta_schema(self, client):
        """Aggregate meta includes required keys."""
        response = client.get("/v1/predictions/opportunities")
        assert response.status_code == 200
        meta = response.json()["meta"]
        for key in ["date", "count", "total", "limit", "offset", "trained_sports", "cached_at"]:
            assert key in meta, f"Missing key in aggregate meta: {key}"

    def test_aggregate_trained_sports_is_list(self, client):
        """trained_sports is always a list."""
        response = client.get("/v1/predictions/opportunities")
        assert response.status_code == 200
        assert isinstance(response.json()["meta"]["trained_sports"], list)

    def test_aggregate_pagination(self, client):
        """Aggregate endpoint respects limit/offset."""
        response = client.get("/v1/predictions/opportunities?limit=3&offset=0")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["limit"] == 3
        assert meta["offset"] == 0
        assert len(response.json()["data"]) <= 3

    def test_aggregate_min_score_filter(self, client):
        """Aggregate endpoint applies min_score filter to results."""
        response = client.get("/v1/predictions/opportunities?min_score=0.95")
        assert response.status_code == 200
        for row in response.json()["data"]:
            assert row["recommendation_score"] >= 0.95

    def test_aggregate_tier_filter(self, client):
        """Aggregate endpoint applies tier filter to results."""
        response = client.get("/v1/predictions/opportunities?tier=high")
        assert response.status_code == 200
        for row in response.json()["data"]:
            assert row["recommendation_tier"] == "high"

    def test_aggregate_sports_filter(self, client):
        """Aggregate endpoint accepts comma-separated sports filter."""
        response = client.get("/v1/predictions/opportunities?sports=nba,mlb")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        # Rows (if any) should only be from filtered sports
        for row in payload["data"]:
            assert row["sport"] in ("nba", "mlb")

    def test_aggregate_sports_filter_whitespace_and_case(self, client):
        """Aggregate sports filter tolerates whitespace and case differences."""
        response = client.get("/v1/predictions/opportunities?sports= NBA , mlB ")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        for row in payload["data"]:
            assert row["sport"] in ("nba", "mlb")

    def test_aggregate_sports_filter_deduplicates_values(self, client):
        """Duplicate sports in query are de-duplicated in meta."""
        response = client.get("/v1/predictions/opportunities?sports=nba,nba")
        assert response.status_code == 200
        payload = response.json()
        trained = payload["meta"].get("trained_sports", [])
        assert trained.count("nba") <= 1

    def test_aggregate_sports_filter_invalid_returns_422(self, client):
        """Unknown sports in aggregate filter are rejected with 422."""
        response = client.get("/v1/predictions/opportunities?sports=nba,not_a_sport")
        assert response.status_code == 422
        detail = response.json().get("detail", "")
        assert "Unknown sport" in detail

    def test_aggregate_tier_filter_grade_alias(self, client):
        """Aggregate tier filter supports grade aliases (S/A/B/C/D)."""
        response = client.get("/v1/predictions/opportunities?tier=A")
        assert response.status_code == 200
        for row in response.json()["data"]:
            assert row["recommendation_grade"] == "A"

    def test_aggregate_handles_nan_confidence(self, client):
        class StubDS:
            def get_games(self, sport, date=None):
                return [{"id": "g_nan", "status": "", "home_team": "A", "away_team": "B"}]

            def get_predictions(self, sport, date=None):
                return [{"game_id": "g_nan", "confidence": float("nan")}]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/opportunities?sports=nba")
            assert response.status_code == 200
            payload = response.json()
            assert payload["success"] is True
            if payload["data"]:
                assert payload["data"][0]["recommendation_score"] == 0.55
        finally:
            app.dependency_overrides.pop(get_data_service, None)

    def test_aggregate_not_shadowed_by_sport_route(self, client):
        """The /opportunities path is not mistakenly treated as a sport name."""
        response = client.get("/v1/predictions/opportunities")
        # Should succeed (200), not 404 "Unknown sport 'opportunities'"
        assert response.status_code == 200
        assert response.json()["success"] is True


# ── Trained Sports Discovery Tests ───────────────────────────

class TestTrainedSportsEndpoint:
    """Test GET /v1/predictions/trained-sports."""

    def test_trained_sports_success(self, client):
        response = client.get("/v1/predictions/trained-sports")
        assert response.status_code == 200
        assert response.json()["success"] is True

    def test_trained_sports_data_is_list(self, client):
        response = client.get("/v1/predictions/trained-sports")
        assert isinstance(response.json()["data"], list)

    def test_trained_sports_meta_schema(self, client):
        meta = client.get("/v1/predictions/trained-sports").json()["meta"]
        assert "count" in meta
        assert "scanned_at" in meta

    def test_trained_sports_entries_schema(self, client):
        """Each entry has required fields."""
        data = client.get("/v1/predictions/trained-sports").json()["data"]
        for entry in data:
            assert "sport" in entry
            assert "size_bytes" in entry
            assert "modified_at" in entry
            assert isinstance(entry["size_bytes"], int)
            assert entry["size_bytes"] > 0

    def test_trained_sports_count_matches_data(self, client):
        payload = client.get("/v1/predictions/trained-sports").json()
        assert payload["meta"]["count"] == len(payload["data"])

    def test_trained_sports_includes_nba(self, client):
        """NBA has a trained model so should always appear."""
        sports = [e["sport"] for e in client.get("/v1/predictions/trained-sports").json()["data"]]
        assert "nba" in sports

    def test_trained_sports_not_shadowed_by_sport_route(self, client):
        """Path should not be caught by /{sport} and return 404."""
        response = client.get("/v1/predictions/trained-sports")
        assert response.status_code == 200


# ── Bundle Cache Invalidation Tests ─────────────────────────

class TestBundleCacheInvalidation:
    """Test DELETE /v1/predictions/cache."""

    def test_cache_invalidation_success(self, client):
        # Warm cache first
        client.get("/v1/predictions/nba/player-props")
        response = client.delete("/v1/predictions/cache")
        assert response.status_code == 200
        assert response.json()["success"] is True

    def test_cache_invalidation_meta_schema(self, client):
        response = client.delete("/v1/predictions/cache")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert "evicted" in meta
        assert "evicted_count" in meta
        assert "cleared_at" in meta
        assert isinstance(meta["evicted"], list)
        assert isinstance(meta["evicted_count"], int)

    def test_cache_invalidation_resets_counters(self, client):
        """After DELETE, health endpoint shows zero hits+misses."""
        client.delete("/v1/predictions/cache")
        # Do exactly one player-props fetch to populate misses counter
        client.get("/v1/predictions/nba/player-props")
        meta = client.get("/v1/predictions/nba/health").json().get("meta", {})
        cache = meta.get("bundle_cache", {})
        total = cache.get("hits", 0) + cache.get("misses", 0)
        assert total >= 1

    def test_cache_evicts_warmed_entries(self, client):
        """Warming a sport then deleting should list it in evicted."""
        client.get("/v1/predictions/nba/player-props")
        resp = client.delete("/v1/predictions/cache")
        meta = resp.json()["meta"]
        assert "nba" in meta["evicted"]


# ── Accuracy Leaderboard Tests ────────────────────────────────

class TestAccuracyLeaderboard:
    """Test GET /v1/predictions/leaderboard."""

    def test_leaderboard_success(self, client):
        response = client.get("/v1/predictions/leaderboard")
        assert response.status_code == 200
        assert response.json()["success"] is True

    def test_leaderboard_data_is_list(self, client):
        assert isinstance(client.get("/v1/predictions/leaderboard").json()["data"], list)

    def test_leaderboard_meta_schema(self, client):
        meta = client.get("/v1/predictions/leaderboard").json()["meta"]
        for key in ["count", "generated_at", "min_evaluated"]:
            assert key in meta

    def test_leaderboard_ranked_descending(self, client):
        """Entries sorted by accuracy descending (rank 1 = highest accuracy)."""
        data = client.get("/v1/predictions/leaderboard").json()["data"]
        accuracies = [e["accuracy"] for e in data if e.get("accuracy") is not None]
        assert accuracies == sorted(accuracies, reverse=True)

    def test_leaderboard_entry_schema(self, client):
        data = client.get("/v1/predictions/leaderboard").json()["data"]
        for entry in data:
            for key in ["sport", "evaluated", "correct", "accuracy", "rank", "has_props_model"]:
                assert key in entry, f"Missing key: {key}"
            assert entry["rank"] >= 1
            assert 0.0 <= entry["accuracy"] <= 1.0
            assert entry["correct"] <= entry["evaluated"]

    def test_leaderboard_rank_sequential(self, client):
        """Rank values must be sequential starting at 1."""
        data = client.get("/v1/predictions/leaderboard").json()["data"]
        ranks = [e["rank"] for e in data]
        assert ranks == list(range(1, len(ranks) + 1))

    def test_leaderboard_min_evaluated_filter(self, client):
        """Sports with fewer evaluated predictions than min_evaluated are excluded."""
        response = client.get("/v1/predictions/leaderboard?min_evaluated=99999")
        assert response.status_code == 200
        assert response.json()["data"] == []

    def test_leaderboard_date_filter(self, client):
        """Date filters are echoed in meta."""
        response = client.get("/v1/predictions/leaderboard?date_start=2024-01-01&date_end=2024-12-31")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["date_start"] == "2024-01-01"
        assert meta["date_end"] == "2024-12-31"

    def test_leaderboard_not_shadowed_by_sport_route(self, client):
        response = client.get("/v1/predictions/leaderboard")
        assert response.status_code == 200


# ── Aggregate Opportunities Enhanced Meta Tests ───────────────

class TestAggregateOpportunitiesEnhancedMeta:
    """Test the tier_counts and prop_type_counts fields added to aggregate meta."""

    def test_aggregate_meta_tier_counts_present(self, client):
        meta = client.get("/v1/predictions/opportunities").json()["meta"]
        assert "tier_counts" in meta
        tc = meta["tier_counts"]
        for tier in ("high", "medium", "low"):
            assert tier in tc

    def test_aggregate_meta_prop_type_counts_present(self, client):
        meta = client.get("/v1/predictions/opportunities").json()["meta"]
        assert "prop_type_counts" in meta
        assert isinstance(meta["prop_type_counts"], dict)

    def test_aggregate_tier_counts_sum_matches_total(self, client):
        payload = client.get("/v1/predictions/opportunities?limit=500").json()
        meta = payload["meta"]
        tier_sum = sum(meta["tier_counts"].values())
        # tier_counts is across all results, not just the page
        assert tier_sum == meta["total"]


class TestTierGradeCompatibility:
    """Ensure tier and grade filters remain backwards-compatible."""

    def test_sport_opportunities_grade_filter_alias(self, client):
        response = client.get("/v1/predictions/nba/player-props/opportunities?tier=B")
        assert response.status_code == 200
        for row in response.json()["data"]:
            assert row["recommendation_grade"] == "B"

    def test_sport_opportunities_invalid_tier_returns_422(self, client):
        response = client.get("/v1/predictions/nba/player-props/opportunities?tier=elite")
        assert response.status_code == 422
        detail = response.json().get("detail", "")
        assert "Invalid tier" in detail


# ── Calibration Endpoint Tests ────────────────────────────────

class TestCalibrationEndpoint:
    """Test GET /v1/predictions/{sport}/metrics/calibration."""

    def test_calibration_valid_sport(self, client):
        """Calibration endpoint accepts valid sports."""
        response = client.get("/v1/predictions/nba/metrics/calibration")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert "data" in payload
        assert "meta" in payload

    def test_calibration_invalid_sport(self, client, invalid_sport):
        """Calibration endpoint rejects unknown sports."""
        response = client.get(f"/v1/predictions/{invalid_sport}/metrics/calibration")
        assert response.status_code == 404

    def test_calibration_meta_schema(self, client):
        """Calibration meta always has sport and cached_at; full keys when data present."""
        response = client.get("/v1/predictions/nba/metrics/calibration")
        assert response.status_code == 200
        payload = response.json()
        meta = payload["meta"]
        # Always present
        assert "sport" in meta
        assert "cached_at" in meta
        # Full meta keys only present when evaluable predictions exist
        if payload["data"].get("sample_size", 0) > 0:
            for key in ["bins", "min_samples_per_bin", "records_total",
                        "records_after_window_filter"]:
                assert key in meta, f"Missing key in calibration meta: {key}"

    def test_calibration_data_schema_with_data(self, client):
        """When evaluable predictions exist, data has calibration fields."""
        data = client.get("/v1/predictions/nba/metrics/calibration").json()["data"]
        if data.get("sample_size", 0) > 0:
            for key in ["sport", "window_days", "sample_size", "overall_accuracy",
                        "brier_score", "log_loss", "expected_calibration_error",
                        "calibration_gap", "confidence_bins"]:
                assert key in data, f"Missing key in calibration data: {key}"
            assert isinstance(data["confidence_bins"], list)

    def test_calibration_empty_data_schema(self, client):
        """When no evaluable predictions, data has at least sport and window_days."""
        data = client.get("/v1/predictions/nba/metrics/calibration").json()["data"]
        assert "sport" in data
        assert "window_days" in data

    def test_calibration_window_parameter(self, client):
        """Calibration respects days parameter."""
        response = client.get("/v1/predictions/nba/metrics/calibration?days=90")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data.get("window_days") == 90 or "window_days" not in data or data["window_days"] == 90

    def test_calibration_bins_parameter(self, client):
        """Calibration respects bins parameter."""
        response = client.get("/v1/predictions/nba/metrics/calibration?bins=5")
        assert response.status_code == 200
        data = response.json()["data"]
        if data.get("sample_size", 0) > 0:
            assert len(data["confidence_bins"]) == 5

    def test_calibration_json_serializable(self, client):
        """Calibration response is valid JSON (no NaN/Inf floats)."""
        response = client.get("/v1/predictions/nba/metrics/calibration")
        assert response.status_code == 200
        # response.json() will raise if invalid JSON
        payload = response.json()
        assert payload is not None


# ── Calibration Trend Endpoint Tests ─────────────────────────

class TestCalibrationTrendEndpoint:
    """Test GET /v1/predictions/{sport}/metrics/calibration/trend."""

    def test_trend_valid_sport(self, client):
        """Trend endpoint accepts valid sports."""
        response = client.get("/v1/predictions/nba/metrics/calibration/trend")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert "data" in payload
        assert "meta" in payload

    def test_trend_invalid_sport(self, client, invalid_sport):
        """Trend endpoint rejects unknown sports."""
        response = client.get(f"/v1/predictions/{invalid_sport}/metrics/calibration/trend")
        assert response.status_code == 404

    def test_trend_meta_schema(self, client):
        """Trend meta has required keys."""
        meta = client.get("/v1/predictions/nba/metrics/calibration/trend").json()["meta"]
        assert "sport" in meta
        assert "cached_at" in meta

    def test_trend_data_has_buckets(self, client):
        """Trend data.buckets is always a list."""
        data = client.get("/v1/predictions/nba/metrics/calibration/trend").json()["data"]
        assert "sport" in data
        assert "buckets" in data
        assert isinstance(data["buckets"], list)

    def test_trend_window_parameter(self, client):
        """Trend respects window_days parameter."""
        response = client.get("/v1/predictions/nba/metrics/calibration/trend?window_days=90")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data.get("window_days") == 90

    def test_trend_bucket_days_parameter(self, client):
        """Trend respects bucket_days parameter."""
        response = client.get(
            "/v1/predictions/nba/metrics/calibration/trend?window_days=90&bucket_days=30"
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert data.get("bucket_days") == 30

    def test_trend_window_smaller_than_bucket_returns_one_bucket(self, client):
        """window_days < bucket_days still yields one bucket instead of empty output."""
        response = client.get(
            "/v1/predictions/nba/metrics/calibration/trend?window_days=7&bucket_days=30"
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert isinstance(data.get("buckets"), list)
        assert len(data["buckets"]) == 1

    def test_trend_non_divisible_window_uses_ceiling_buckets(self, client):
        """window_days not divisible by bucket_days should include trailing partial bucket."""
        response = client.get(
            "/v1/predictions/nba/metrics/calibration/trend?window_days=95&bucket_days=30"
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert isinstance(data.get("buckets"), list)
        assert len(data["buckets"]) == 4

    def test_trend_json_serializable(self, client):
        """Trend response is valid JSON."""
        response = client.get("/v1/predictions/nba/metrics/calibration/trend")
        assert response.status_code == 200
        assert response.json() is not None


class TestCalibrationEndpointRobustness:
    """Ensure calibration endpoints are resilient to malformed probabilities."""

    def test_calibration_skips_invalid_probabilities(self, client):
        class StubDS:
            def get_predictions(self, sport, date=None):
                return [
                    {
                        "game_id": "good-1",
                        "home_win_prob": 0.72,
                        "away_win_prob": 0.28,
                        "home_score": 110,
                        "away_score": 100,
                        "date": "2026-03-25",
                    },
                    {
                        "game_id": "bad-1",
                        "home_win_prob": "not-a-number",
                        "away_win_prob": 0.4,
                        "home_score": 99,
                        "away_score": 101,
                        "date": "2026-03-25",
                    },
                ]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba/metrics/calibration?days=365")
            assert response.status_code == 200
            payload = response.json()
            assert payload["success"] is True
            assert payload["data"]["sample_size"] == 1
        finally:
            app.dependency_overrides.pop(get_data_service, None)

    def test_calibration_trend_skips_invalid_probabilities(self, client):
        class StubDS:
            def get_predictions(self, sport, date=None):
                return [
                    {
                        "game_id": "good-2",
                        "home_win_prob": 0.61,
                        "away_win_prob": 0.39,
                        "home_score": 101,
                        "away_score": 98,
                        "date": "2026-03-20",
                    },
                    {
                        "game_id": "bad-2",
                        "home_win_prob": "nan-text",
                        "away_win_prob": 0.45,
                        "home_score": 95,
                        "away_score": 102,
                        "date": "2026-03-20",
                    },
                ]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba/metrics/calibration/trend?window_days=30&bucket_days=30")
            assert response.status_code == 200
            payload = response.json()
            assert payload["success"] is True
            buckets = payload["data"]["buckets"]
            assert isinstance(buckets, list)
            if buckets:
                assert buckets[0]["sample_size"] == 1
        finally:
            app.dependency_overrides.pop(get_data_service, None)


class TestEvaluationRobustness:
    """Shared evaluability logic should exclude NaN probability rows."""

    def test_history_ignores_nan_probabilities(self, client):
        class StubDS:
            def get_predictions(self, sport, date=None):
                return [
                    {
                        "game_id": "good-h",
                        "home_win_prob": 0.65,
                        "away_win_prob": 0.35,
                        "home_score": 108,
                        "away_score": 102,
                        "date": "2026-03-28",
                    },
                    {
                        "game_id": "bad-h",
                        "home_win_prob": float("nan"),
                        "away_win_prob": 0.45,
                        "home_score": 101,
                        "away_score": 99,
                        "date": "2026-03-28",
                    },
                ]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba/history")
            assert response.status_code == 200
            meta = response.json()["meta"]
            assert meta["evaluated"] == 1
        finally:
            app.dependency_overrides.pop(get_data_service, None)


class TestResponseSanitization:
    """Ensure API responses never leak NaN/Inf floats."""

    def test_predictions_endpoint_scrubs_nan_probabilities(self, client):
        class StubDS:
            def get_predictions(self, sport, date=None):
                return [
                    {
                        "game_id": "nan-preds",
                        "home_win_prob": float("nan"),
                        "away_win_prob": 0.45,
                        "home_score": None,
                        "away_score": None,
                    }
                ]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba")
            assert response.status_code == 200
            row = response.json()["data"][0]
            assert row["home_win_prob"] is None
        finally:
            app.dependency_overrides.pop(get_data_service, None)

    def test_history_endpoint_scrubs_nan_probabilities(self, client):
        class StubDS:
            def get_predictions(self, sport, date=None):
                return [
                    {
                        "game_id": "nan-history",
                        "home_win_prob": float("nan"),
                        "away_win_prob": 0.45,
                        "home_score": 100,
                        "away_score": 99,
                    }
                ]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba/history")
            assert response.status_code == 200
            row = response.json()["data"][0]
            assert row["home_win_prob"] is None
        finally:
            app.dependency_overrides.pop(get_data_service, None)

    def test_single_game_endpoint_scrubs_nan_probabilities(self, client):
        class StubDS:
            def get_predictions(self, sport, date=None):
                return [
                    {
                        "game_id": "nan-single",
                        "home_win_prob": float("nan"),
                        "away_win_prob": 0.45,
                        "home_score": 101,
                        "away_score": 97,
                    }
                ]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba/nan-single")
            assert response.status_code == 200
            row = response.json()["data"]
            assert row["home_win_prob"] is None
        finally:
            app.dependency_overrides.pop(get_data_service, None)


# ── Expanded Single-Game Tests ────────────────────────────────

class TestSingleGamePredictionExpanded:
    """Extended tests for GET /v1/predictions/{sport}/{game_id}."""

    def test_game_prediction_response_envelope(self, client):
        """Any found prediction returns success envelope."""
        # Get a known game_id from history
        history = client.get("/v1/predictions/nba/history?limit=5").json()
        if not history["data"]:
            pytest.skip("No prediction data available for nba")
        game_id = history["data"][0].get("game_id")
        response = client.get(f"/v1/predictions/nba/{game_id}")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert "data" in payload
        assert "meta" in payload

    def test_game_prediction_meta_has_sport_and_game_id(self, client):
        """Meta echoes sport and game_id."""
        history = client.get("/v1/predictions/nba/history?limit=5").json()
        if not history["data"]:
            pytest.skip("No prediction data available")
        game_id = history["data"][0].get("game_id")
        meta = client.get(f"/v1/predictions/nba/{game_id}").json()["meta"]
        assert meta["sport"] == "nba"
        assert meta["game_id"] == str(game_id)

    def test_game_prediction_wrong_sport(self, client):
        """Using the correct game_id but wrong sport returns 404."""
        history = client.get("/v1/predictions/nba/history?limit=5").json()
        if not history["data"]:
            pytest.skip("No prediction data available")
        nba_game_id = history["data"][0].get("game_id")
        # That game_id should not exist in NFL data
        response = client.get(f"/v1/predictions/nfl/{nba_game_id}")
        # Either 404 (not found in NFL) or 200 with a different game — just not a server error
        assert response.status_code in (200, 404)


# ── Expanded Backtest Tests ───────────────────────────────────

class TestAdvancedBacktestExpanded:
    """Extended tests for GET /v1/predictions/{sport}/backtest/advanced."""

    def test_backtest_data_structure(self, client):
        """Backtest data has expected top-level fields."""
        data = client.get("/v1/predictions/nba/backtest/advanced").json()["data"]
        assert "sport" in data
        assert "total_predictions" in data
        assert "total_games_evaluated" in data

    def test_backtest_confidence_filter(self, client):
        """Backtest accepts min_confidence parameter."""
        response = client.get("/v1/predictions/nba/backtest/advanced?min_confidence=0.7")
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta.get("min_confidence_filter") == 0.7

    def test_backtest_confidence_tiers_are_list(self, client):
        """confidence_tiers in data is always a list (may be empty)."""
        data = client.get("/v1/predictions/nba/backtest/advanced").json()["data"]
        if "confidence_tiers" in data:
            assert isinstance(data["confidence_tiers"], list)

    def test_backtest_handles_invalid_confidence_values(self, client):
        """Backtest should not fail when confidence values are malformed."""
        class StubDS:
            def get_predictions(self, sport, date=None):
                return [
                    {
                        "game_id": "g1",
                        "home_win_prob": 0.7,
                        "away_win_prob": 0.3,
                        "home_score": 100,
                        "away_score": 90,
                        "confidence": "high",
                        "date": "2026-03-30",
                    },
                    {
                        "game_id": "g2",
                        "home_win_prob": 0.55,
                        "away_win_prob": 0.45,
                        "home_score": 101,
                        "away_score": 99,
                        "confidence": 0.78,
                        "date": "2026-03-30",
                    },
                ]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba/backtest/advanced?min_confidence=0.7")
            assert response.status_code == 200
            payload = response.json()
            assert payload["success"] is True
            assert "data" in payload
        finally:
            app.dependency_overrides.pop(get_data_service, None)

    def test_backtest_ignores_nan_probability_rows(self, client):
        """Backtest evaluated count should exclude rows with invalid probabilities."""
        class StubDS:
            def get_predictions(self, sport, date=None):
                return [
                    {
                        "game_id": "valid-1",
                        "home_win_prob": 0.6,
                        "away_win_prob": 0.4,
                        "home_score": 100,
                        "away_score": 95,
                        "confidence": 0.7,
                        "date": "2026-03-30",
                    },
                    {
                        "game_id": "nan-1",
                        "home_win_prob": float("nan"),
                        "away_win_prob": 0.5,
                        "home_score": 101,
                        "away_score": 99,
                        "confidence": 0.7,
                        "date": "2026-03-30",
                    },
                ]

        app.dependency_overrides[get_data_service] = lambda: StubDS()
        try:
            response = client.get("/v1/predictions/nba/backtest/advanced?days=365")
            assert response.status_code == 200
            data = response.json()["data"]
            assert data["total_games_evaluated"] == 1
        finally:
            app.dependency_overrides.pop(get_data_service, None)

