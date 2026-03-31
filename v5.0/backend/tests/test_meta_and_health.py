"""
Contract tests for meta and health API endpoints.

Tests ensure response schemas, status codes, and field invariants for:
- Root health check                  GET /health
- Detailed system health             GET /v1/health
- Sports catalogue                   GET /v1/sports
- Sport detail                       GET /v1/sports/{sport}
- Meta sports (data-on-disk subset)  GET /v1/meta/sports
- Meta data-status (freshness)       GET /v1/meta/data-status
- Meta providers                     GET /v1/meta/providers
"""

import pytest
from fastapi.testclient import TestClient
from main import app


@pytest.fixture
def client():
    """FastAPI test client (auth disabled — anonymous enterprise key)."""
    return TestClient(app)


@pytest.fixture
def valid_sports():
    """Sports that must appear in the catalogue."""
    return ["nba", "nfl", "mlb", "nhl", "wnba"]


@pytest.fixture
def invalid_sport():
    return "not_a_real_sport_xyz"


# ── Root health check ─────────────────────────────────────────────────

class TestRootHealth:
    """Test GET /health (minimal liveness probe)."""

    def test_returns_200(self, client):
        assert client.get("/health").status_code == 200

    def test_schema(self, client):
        body = client.get("/health").json()
        assert body["success"] is True
        assert body["data"]["status"] == "ok"


# ── Detailed health check ─────────────────────────────────────────────

class TestDetailedHealth:
    """Test GET /v1/health (full diagnostic health endpoint)."""

    def test_returns_200(self, client):
        assert client.get("/v1/health").status_code == 200

    def test_envelope(self, client):
        body = client.get("/v1/health").json()
        assert body["success"] is True
        assert "data" in body
        assert "meta" in body

    def test_status_ok(self, client):
        d = client.get("/v1/health").json()["data"]
        assert d["status"] == "ok"

    def test_uptime_non_negative(self, client):
        d = client.get("/v1/health").json()["data"]
        assert "uptime_seconds" in d
        assert isinstance(d["uptime_seconds"], (int, float))
        assert d["uptime_seconds"] >= 0

    def test_cache_stats_present(self, client):
        d = client.get("/v1/health").json()["data"]
        cache = d["cache"]
        assert "hits" in cache
        assert "misses" in cache

    def test_configured_sports_positive(self, client):
        d = client.get("/v1/health").json()["data"]
        assert "configured_sports" in d
        assert d["configured_sports"] > 0

    def test_meta_cached_at(self, client):
        meta = client.get("/v1/health").json()["meta"]
        assert "cached_at" in meta


# ── Sports catalogue ──────────────────────────────────────────────────

class TestSportsList:
    """Test GET /v1/sports endpoint."""

    def test_returns_200(self, client):
        assert client.get("/v1/sports").status_code == 200

    def test_envelope(self, client):
        body = client.get("/v1/sports").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_meta_fields(self, client):
        meta = client.get("/v1/sports").json()["meta"]
        assert "count" in meta
        assert "sports_with_data" in meta
        assert "cached_at" in meta

    def test_count_matches_data_length(self, client):
        body = client.get("/v1/sports").json()
        assert body["meta"]["count"] == len(body["data"])

    def test_known_sports_present(self, client, valid_sports):
        keys = {s["key"] for s in client.get("/v1/sports").json()["data"]}
        for sport in valid_sports:
            assert sport in keys, f"'{sport}' missing from /v1/sports"

    def test_each_sport_has_seasons(self, client):
        for sport in client.get("/v1/sports").json()["data"]:
            assert "key" in sport
            assert "available_seasons" in sport

    def test_sorted_alphabetically(self, client):
        keys = [s["key"] for s in client.get("/v1/sports").json()["data"]]
        assert keys == sorted(keys)


# ── Sport detail ──────────────────────────────────────────────────────

class TestSportDetail:
    """Test GET /v1/sports/{sport} endpoint."""

    def test_valid_sport_200(self, client, valid_sports):
        assert client.get(f"/v1/sports/{valid_sports[0]}").status_code == 200

    def test_envelope(self, client, valid_sports):
        body = client.get(f"/v1/sports/{valid_sports[0]}").json()
        assert body["success"] is True
        assert "data" in body
        assert "meta" in body

    def test_key_matches_request(self, client, valid_sports):
        sport = valid_sports[0]
        body = client.get(f"/v1/sports/{sport}").json()
        assert body["data"]["key"] == sport
        assert body["meta"]["sport"] == sport

    def test_available_data_structure(self, client, valid_sports):
        sport = valid_sports[0]
        avail = client.get(f"/v1/sports/{sport}").json()["data"]["available_data"]
        for kind in ("games", "teams", "standings", "players", "odds"):
            assert kind in avail
            entry = avail[kind]
            assert "available" in entry
            assert "seasons" in entry
            assert "file_count" in entry
            assert isinstance(entry["seasons"], list)

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/sports/{invalid_sport}")
        assert response.status_code == 404
        assert "Unknown sport" in response.json()["detail"]


# ── Meta sports ───────────────────────────────────────────────────────

class TestMetaSports:
    """Test GET /v1/meta/sports endpoint."""

    def test_returns_200(self, client):
        assert client.get("/v1/meta/sports").status_code == 200

    def test_envelope(self, client):
        body = client.get("/v1/meta/sports").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_meta_count(self, client):
        body = client.get("/v1/meta/sports").json()
        assert "count" in body["meta"]
        assert body["meta"]["count"] == len(body["data"])

    def test_each_sport_has_season_info(self, client):
        for sport in client.get("/v1/meta/sports").json()["data"]:
            assert "available_seasons" in sport, f"'available_seasons' missing for {sport.get('key')}"


# ── Meta data-status ──────────────────────────────────────────────────

class TestMetaDataStatus:
    """Test GET /v1/meta/data-status endpoint."""

    def test_returns_200(self, client):
        assert client.get("/v1/meta/data-status").status_code == 200

    def test_envelope(self, client):
        body = client.get("/v1/meta/data-status").json()
        assert body["success"] is True
        assert isinstance(body["data"], dict)

    def test_meta_fields(self, client):
        meta = client.get("/v1/meta/data-status").json()["meta"]
        assert "sports_count" in meta
        assert "cached_at" in meta

    def test_sports_count_matches_data(self, client):
        body = client.get("/v1/meta/data-status").json()
        assert body["meta"]["sports_count"] == len(body["data"])


# ── Meta providers ────────────────────────────────────────────────────

class TestMetaProviders:
    """Test GET /v1/meta/providers endpoint."""

    def test_returns_200(self, client):
        assert client.get("/v1/meta/providers").status_code == 200

    def test_envelope(self, client):
        body = client.get("/v1/meta/providers").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_meta_count_matches(self, client):
        body = client.get("/v1/meta/providers").json()
        assert body["meta"]["count"] == len(body["data"])

    def test_sorted(self, client):
        providers = client.get("/v1/meta/providers").json()["data"]
        assert providers == sorted(providers)
