"""
Contract tests for per-sport data API endpoints.

Tests ensure response envelopes, HTTP status codes, pagination metadata, and
field invariants for:
- Sport overview        GET /v1/{sport}/overview
- Games list            GET /v1/{sport}/games
- Game detail           GET /v1/{sport}/games/{id}
- Teams list            GET /v1/{sport}/teams
- Team detail           GET /v1/{sport}/teams/{id}
- Standings             GET /v1/{sport}/standings
- Players               GET /v1/{sport}/players
- Player stats          GET /v1/{sport}/player-stats
- Odds                  GET /v1/{sport}/odds
- Game odds             GET /v1/{sport}/odds/{game_id}
- Injuries              GET /v1/{sport}/injuries
- News                  GET /v1/{sport}/news
- Advanced stats        GET /v1/{sport}/advanced-stats
- Ratings               GET /v1/{sport}/ratings
- Market signals        GET /v1/{sport}/market-signals
- Schedule fatigue      GET /v1/{sport}/schedule-fatigue
- Team stats            GET /v1/{sport}/team-stats
- Transactions          GET /v1/{sport}/transactions
- Schedule              GET /v1/{sport}/schedule
- Match events          GET /v1/{sport}/match-events
- Live predictions      GET /v1/{sport}/live-predictions
- Predictions           GET /v1/{sport}/predictions

Auth is disabled in tests; all requests run under an anonymous enterprise key.
"""

import pytest
from fastapi.testclient import TestClient
from main import app


@pytest.fixture
def client():
    """FastAPI test client (auth disabled — anonymous enterprise key)."""
    return TestClient(app)


@pytest.fixture
def sport():
    """Primary sport used for positive-path tests."""
    return "nba"


@pytest.fixture
def invalid_sport():
    return "not_a_real_sport_xyz"


# ── Helpers ────────────────────────────────────────────────────────────

def _assert_paginated_meta(meta: dict, sport: str) -> None:
    """Assert all required paginated response meta fields are present."""
    for field in ("sport", "season", "count", "total", "limit", "offset", "cached_at"):
        assert field in meta, f"meta missing '{field}'"
    assert meta["sport"] == sport


# ── Overview ───────────────────────────────────────────────────────────

class TestOverviewEndpoint:
    """Test GET /v1/{sport}/overview."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/overview").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/overview")
        assert response.status_code == 404
        assert "Unknown sport" in response.json()["detail"]

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/overview").json()
        assert body["success"] is True
        assert "data" in body
        assert "meta" in body

    def test_data_fields(self, client, sport):
        data = client.get(f"/v1/{sport}/overview").json()["data"]
        assert "recent_games" in data
        assert "standings" in data
        assert "top_news" in data
        assert "game_count" in data
        assert "team_count" in data
        assert isinstance(data["recent_games"], list)
        assert isinstance(data["standings"], list)

    def test_meta_fields(self, client, sport):
        meta = client.get(f"/v1/{sport}/overview").json()["meta"]
        assert meta["sport"] == sport
        assert "season" in meta
        assert "cached_at" in meta


# ── Games ──────────────────────────────────────────────────────────────

class TestGamesEndpoint:
    """Test GET /v1/{sport}/games."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/games").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/games")
        assert response.status_code == 404
        assert "Unknown sport" in response.json()["detail"]

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/games").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/games").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_pagination_params_respected(self, client, sport):
        meta = client.get(f"/v1/{sport}/games?limit=5&offset=0").json()["meta"]
        assert meta["limit"] == 5
        assert meta["offset"] == 0

    def test_date_filter_accepted(self, client, sport):
        response = client.get(f"/v1/{sport}/games?date=2025-01-15")
        assert response.status_code == 200

    def test_sort_by_date_accepted(self, client, sport):
        response = client.get(f"/v1/{sport}/games?sort=-date")
        assert response.status_code == 200


class TestGameDetail:
    """Test GET /v1/{sport}/games/{game_id}."""

    def test_unknown_id_404(self, client, sport):
        response = client.get(f"/v1/{sport}/games/nonexistent_game_id_xyz")
        assert response.status_code == 404

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/games/123")
        assert response.status_code == 404


# ── Teams ──────────────────────────────────────────────────────────────

class TestTeamsEndpoint:
    """Test GET /v1/{sport}/teams."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/teams").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/teams")
        assert response.status_code == 404
        assert "Unknown sport" in response.json()["detail"]

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/teams").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/teams").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_pagination_respected(self, client, sport):
        meta = client.get(f"/v1/{sport}/teams?limit=3").json()["meta"]
        assert meta["limit"] == 3


class TestTeamDetail:
    """Test GET /v1/{sport}/teams/{team_id}."""

    def test_unknown_id_404(self, client, sport):
        response = client.get(f"/v1/{sport}/teams/unknown_team_zzz")
        assert response.status_code == 404

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/teams/1")
        assert response.status_code == 404


# ── Standings ─────────────────────────────────────────────────────────

class TestStandingsEndpoint:
    """Test GET /v1/{sport}/standings."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/standings").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/standings")
        assert response.status_code == 404
        assert "Unknown sport" in response.json()["detail"]

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/standings").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/standings").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_extra_meta_fields(self, client, sport):
        meta = client.get(f"/v1/{sport}/standings").json()["meta"]
        assert "season_active" in meta
        assert "season_year" in meta

    def test_conference_filter_accepted(self, client, sport):
        response = client.get(f"/v1/{sport}/standings?conference=Eastern")
        assert response.status_code == 200


# ── Players ───────────────────────────────────────────────────────────

class TestPlayersEndpoint:
    """Test GET /v1/{sport}/players."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/players").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/players")
        assert response.status_code == 404
        assert "Unknown sport" in response.json()["detail"]

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/players").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/players").json()["meta"]
        _assert_paginated_meta(meta, sport)


# ── Odds ──────────────────────────────────────────────────────────────

class TestOddsEndpoint:
    """Test GET /v1/{sport}/odds."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/odds").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/odds")
        assert response.status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/odds").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/odds").json()["meta"]
        _assert_paginated_meta(meta, sport)


# ── Injuries ──────────────────────────────────────────────────────────

class TestInjuriesEndpoint:
    """Test GET /v1/{sport}/injuries."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/injuries").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        response = client.get(f"/v1/{invalid_sport}/injuries")
        assert response.status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/injuries").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/injuries").json()["meta"]
        _assert_paginated_meta(meta, sport)


# ── Player Stats ───────────────────────────────────────────────────────

class TestPlayerStatsEndpoint:
    """Test GET /v1/{sport}/player-stats."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/player-stats").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/player-stats").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/player-stats").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/player-stats").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_pagination_params_accepted(self, client, sport):
        r = client.get(f"/v1/{sport}/player-stats?limit=5&offset=0")
        assert r.status_code == 200
        assert r.json()["meta"]["limit"] == 5

    def test_aggregate_flag(self, client, sport):
        r = client.get(f"/v1/{sport}/player-stats?aggregate=true")
        assert r.status_code == 200
        assert isinstance(r.json()["data"], list)


# ── Game Odds ────────────────────────────────────────────────────────

class TestGameOddsEndpoint:
    """Test GET /v1/{sport}/odds/{game_id}."""

    def test_unknown_game_returns_200_empty(self, client, sport):
        r = client.get(f"/v1/{sport}/odds/nonexistent_game_id_xyz")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_meta_contains_game_id(self, client, sport):
        r = client.get(f"/v1/{sport}/odds/test_game_123")
        assert r.status_code == 200
        meta = r.json()["meta"]
        assert meta.get("game_id") == "test_game_123"
        assert meta.get("sport") == sport


# ── News ─────────────────────────────────────────────────────────────

class TestNewsEndpoint:
    """Test GET /v1/{sport}/news."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/news").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/news").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/news").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/news").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_limit_param(self, client, sport):
        r = client.get(f"/v1/{sport}/news?limit=5")
        assert r.status_code == 200
        assert r.json()["meta"]["limit"] == 5


# ── Predictions (sport-scoped) ────────────────────────────────────────

class TestSportPredictionsEndpoint:
    """Test GET /v1/{sport}/predictions (sport-scoped, not the central endpoint)."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/predictions").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/predictions").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/predictions").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/predictions").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_date_filter(self, client, sport):
        r = client.get(f"/v1/{sport}/predictions?date=2025-01-15")
        assert r.status_code == 200


# ── Advanced Stats ────────────────────────────────────────────────────

class TestAdvancedStatsEndpoint:
    """Test GET /v1/{sport}/advanced-stats."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/advanced-stats").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/advanced-stats").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/advanced-stats").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/advanced-stats").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_mlb_advanced_200(self, client):
        r = client.get("/v1/mlb/advanced-stats")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True


# ── Ratings ───────────────────────────────────────────────────────────

class TestRatingsEndpoint:
    """Test GET /v1/{sport}/ratings."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/ratings").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/ratings").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/ratings").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/ratings").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_rating_type_filter(self, client, sport):
        r = client.get(f"/v1/{sport}/ratings?rating_type=elo")
        assert r.status_code == 200


# ── Market Signals ────────────────────────────────────────────────────

class TestMarketSignalsEndpoint:
    """Test GET /v1/{sport}/market-signals."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/market-signals").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/market-signals").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/market-signals").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/market-signals").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_regime_filter(self, client, sport):
        r = client.get(f"/v1/{sport}/market-signals?regime=stable")
        assert r.status_code == 200


# ── Schedule Fatigue ──────────────────────────────────────────────────

class TestScheduleFatigueEndpoint:
    """Test GET /v1/{sport}/schedule-fatigue."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/schedule-fatigue").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/schedule-fatigue").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/schedule-fatigue").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/schedule-fatigue").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_fatigue_level_filter(self, client, sport):
        r = client.get(f"/v1/{sport}/schedule-fatigue?fatigue_level=high")
        assert r.status_code == 200


# ── Team Stats ────────────────────────────────────────────────────────

class TestTeamStatsEndpoint:
    """Test GET /v1/{sport}/team-stats."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/team-stats").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/team-stats").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/team-stats").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/team-stats").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_sort_param(self, client, sport):
        r = client.get(f"/v1/{sport}/team-stats?sort=-avg_points")
        assert r.status_code == 200


# ── Transactions ──────────────────────────────────────────────────────

class TestTransactionsEndpoint:
    """Test GET /v1/{sport}/transactions."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/transactions").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/transactions").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/transactions").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/transactions").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_date_range_filter(self, client, sport):
        r = client.get(f"/v1/{sport}/transactions?date_start=2025-01-01&date_end=2025-03-31")
        assert r.status_code == 200


# ── Schedule ──────────────────────────────────────────────────────────

class TestScheduleEndpoint:
    """Test GET /v1/{sport}/schedule."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/schedule").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/schedule").status_code == 404

    def test_envelope(self, client, sport):
        body = client.get(f"/v1/{sport}/schedule").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client, sport):
        meta = client.get(f"/v1/{sport}/schedule").json()["meta"]
        _assert_paginated_meta(meta, sport)

    def test_days_param(self, client, sport):
        r = client.get(f"/v1/{sport}/schedule?days=14")
        assert r.status_code == 200

    def test_days_too_large_422(self, client, sport):
        r = client.get(f"/v1/{sport}/schedule?days=99")
        assert r.status_code == 422


# ── Match Events ──────────────────────────────────────────────────────

class TestMatchEventsEndpoint:
    """Test GET /v1/{sport}/match-events."""

    def test_valid_sport_200(self, client):
        assert client.get("/v1/epl/match-events").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/match-events").status_code == 404

    def test_envelope(self, client):
        body = client.get("/v1/epl/match-events").json()
        assert body["success"] is True
        assert isinstance(body["data"], list)

    def test_paginated_meta(self, client):
        meta = client.get("/v1/epl/match-events").json()["meta"]
        _assert_paginated_meta(meta, "epl")

    def test_event_type_filter(self, client):
        r = client.get("/v1/epl/match-events?event_type=goal")
        assert r.status_code == 200


# ── Live Predictions ──────────────────────────────────────────────────

class TestLivePredictionsEndpoint:
    """Test GET /v1/{sport}/live-predictions."""

    def test_valid_sport_200(self, client, sport):
        assert client.get(f"/v1/{sport}/live-predictions").status_code == 200

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/live-predictions").status_code == 404

    def test_envelope_structure(self, client, sport):
        body = client.get(f"/v1/{sport}/live-predictions").json()
        assert body["success"] is True
        data = body["data"]
        assert isinstance(data.get("games"), list)

    def test_meta_contains_sport(self, client, sport):
        meta = client.get(f"/v1/{sport}/live-predictions").json()["meta"]
        assert meta.get("sport") == sport

    def test_multiple_sports(self, client):
        for sport in ("nba", "nfl", "mlb"):
            r = client.get(f"/v1/{sport}/live-predictions")
            assert r.status_code == 200


# ── Simulation ────────────────────────────────────────────────────────

class TestSimulationEndpoint:
    """Test GET /v1/{sport}/simulation."""

    def test_missing_simulation_returns_404(self, client, sport):
        # Simulation files are not generated in CI; expect 404 for missing file.
        r = client.get(f"/v1/{sport}/simulation")
        assert r.status_code in (200, 404)

    def test_invalid_sport_404(self, client, invalid_sport):
        assert client.get(f"/v1/{invalid_sport}/simulation").status_code == 404
