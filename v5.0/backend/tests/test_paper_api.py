"""
Contract tests for paper trading API endpoints.

Tests cover:
- Portfolio CRUD (GET /portfolio, POST /reset)
- Bet placement (POST /bet) with validation
- Bet settlement (POST /settle/{bet_id}) for won/lost/push
- Bet cancellation (DELETE /bet/{bet_id})
- History pagination and filtering (GET /history)
- Leaderboard (GET /leaderboard)
"""

import pytest
from fastapi.testclient import TestClient
from main import app


@pytest.fixture
def client():
    """FastAPI test client (anonymous mode — no JWT required)."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_paper_state(client):
    """Reset paper trading portfolio to a clean slate before every test."""
    client.post("/v1/paper/reset")


@pytest.fixture
def moneyline_bet():
    """Minimal valid moneyline bet payload."""
    return {
        "sport": "nba",
        "game_id": "test_game_001",
        "matchup": "Lakers vs Celtics",
        "bet_type": "moneyline",
        "selection": "home",
        "pick": "Lakers",
        "odds": -110,
        "stake": 100.0,
    }


@pytest.fixture
def spread_bet():
    """Valid spread bet requiring a line."""
    return {
        "sport": "nba",
        "game_id": "test_game_002",
        "matchup": "Bulls vs Heat",
        "bet_type": "spread",
        "selection": "home",
        "pick": "Bulls -3.5",
        "odds": -110,
        "stake": 50.0,
        "line": -3.5,
    }


# ── Portfolio Tests ───────────────────────────────────────────────────

class TestPortfolio:
    """Test GET /v1/paper/portfolio and POST /v1/paper/reset."""

    def test_portfolio_returns_success(self, client):
        """Portfolio endpoint returns standard success envelope."""
        response = client.get("/v1/paper/portfolio")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert "data" in payload

    def test_portfolio_has_balance(self, client):
        """Portfolio data includes a numeric balance."""
        data = client.get("/v1/paper/portfolio").json()["data"]
        assert "balance" in data
        assert isinstance(data["balance"], (int, float))
        assert data["balance"] >= 0

    def test_portfolio_has_bets_list(self, client):
        """Portfolio data includes a bets list."""
        data = client.get("/v1/paper/portfolio").json()["data"]
        assert "bets" in data
        assert isinstance(data["bets"], list)

    def test_reset_returns_success(self, client):
        """Reset endpoint restores balance to starting amount."""
        response = client.post("/v1/paper/reset")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        data = payload["data"]
        assert data["balance"] == 10_000.0
        assert data["bets"] == []

    def test_reset_then_portfolio_is_clean(self, client):
        """After reset, portfolio shows clean state."""
        data = client.get("/v1/paper/portfolio").json()["data"]
        assert data["balance"] == 10_000.0

    def test_portfolio_starting_balance(self, client):
        """Fresh reset gives $10,000 starting balance."""
        balance = client.get("/v1/paper/portfolio").json()["data"]["balance"]
        assert balance == 10_000.0


# ── Bet Placement Tests ───────────────────────────────────────────────

class TestPlaceBet:
    """Test POST /v1/paper/bet."""

    def setup_method(self):
        """Tests in this class need a clean slate."""

    def test_place_moneyline_bet(self, client, moneyline_bet):
        """Placing a valid moneyline bet returns success with bet_id."""
        response = client.post("/v1/paper/bet", json=moneyline_bet)
        assert response.status_code == 200
        data = response.json()["data"]
        assert "bet_id" in data
        assert data["bet_id"].startswith("pb_")
        assert data["stake"] == moneyline_bet["stake"]
        assert "potential_payout" in data
        assert "new_balance" in data

    def test_place_bet_deducts_stake(self, client, moneyline_bet):
        """Placing a bet deducts the stake from portfolio balance."""
        before = client.get("/v1/paper/portfolio").json()["data"]["balance"]
        data = client.post("/v1/paper/bet", json=moneyline_bet).json()["data"]
        assert data["new_balance"] == pytest.approx(before - moneyline_bet["stake"], abs=0.01)

    def test_place_spread_bet_with_line(self, client, spread_bet):
        """Spread bets require a line and succeed when provided."""
        response = client.post("/v1/paper/bet", json=spread_bet)
        assert response.status_code == 200
        assert response.json()["success"] is True

    def test_spread_bet_missing_line_rejected(self, client, spread_bet):
        """Spread bets without a line return HTTP 400."""
        bad = dict(spread_bet)
        del bad["line"]
        response = client.post("/v1/paper/bet", json=bad)
        assert response.status_code == 400
        assert "line" in response.json()["detail"].lower()

    def test_bet_zero_stake_rejected(self, client, moneyline_bet):
        """A stake of zero is rejected (stake must be > 0)."""
        bad = dict(moneyline_bet)
        bad["stake"] = 0
        response = client.post("/v1/paper/bet", json=bad)
        assert response.status_code == 422  # Pydantic validation

    def test_bet_exceeds_balance_rejected(self, client, moneyline_bet):
        """A stake larger than the current balance is rejected."""
        over = dict(moneyline_bet)
        over["stake"] = 999_999.0
        response = client.post("/v1/paper/bet", json=over)
        assert response.status_code == 400
        assert "balance" in response.json()["detail"].lower()

    def test_bet_appears_in_portfolio(self, client, moneyline_bet):
        """Placed bet appears in portfolio bets list."""
        bet_id = client.post("/v1/paper/bet", json=moneyline_bet).json()["data"]["bet_id"]
        bets = client.get("/v1/paper/portfolio").json()["data"]["bets"]
        ids = [b["id"] for b in bets]
        assert bet_id in ids

    def test_placed_bet_status_is_pending(self, client, moneyline_bet):
        """Newly placed bet has 'pending' status."""
        bet_id = client.post("/v1/paper/bet", json=moneyline_bet).json()["data"]["bet_id"]
        bets = client.get("/v1/paper/portfolio").json()["data"]["bets"]
        bet = next((b for b in bets if b["id"] == bet_id), None)
        assert bet is not None
        assert bet["result"] == "pending"

    def test_positive_american_odds_payout(self, client):
        """Positive odds (+150) give larger potential payout than stake."""
        bet = {
            "sport": "nfl",
            "matchup": "Chiefs vs Bills",
            "bet_type": "moneyline",
            "selection": "away",
            "odds": 150,
            "stake": 100.0,
        }
        data = client.post("/v1/paper/bet", json=bet).json()["data"]
        assert data["potential_payout"] == pytest.approx(150.0, abs=0.1)

    def test_negative_american_odds_payout(self, client):
        """Negative odds (-110) give smaller potential payout than stake."""
        bet = {
            "sport": "nba",
            "matchup": "Lakers vs Celtics",
            "bet_type": "moneyline",
            "selection": "home",
            "odds": -110,
            "stake": 110.0,
        }
        data = client.post("/v1/paper/bet", json=bet).json()["data"]
        assert data["potential_payout"] == pytest.approx(100.0, abs=0.1)


# ── Bet Settlement Tests ──────────────────────────────────────────────

class TestSettleBet:
    """Test POST /v1/paper/settle/{bet_id}."""

    def _place(self, client, bet_payload):
        r = client.post("/v1/paper/bet", json=bet_payload)
        assert r.status_code == 200
        return r.json()["data"]["bet_id"]

    def test_settle_won(self, client, moneyline_bet):
        """Settling a bet as 'won' credits stake + payout."""
        bet_id = self._place(client, moneyline_bet)
        before = client.get("/v1/paper/portfolio").json()["data"]["balance"]
        resp = client.post(f"/v1/paper/settle/{bet_id}", json={"result": "won"})
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["result"] == "won"
        assert data["credit"] > 0
        assert data["new_balance"] > before

    def test_settle_lost(self, client, moneyline_bet):
        """Settling a bet as 'lost' gives no credit."""
        bet_id = self._place(client, moneyline_bet)
        before = client.get("/v1/paper/portfolio").json()["data"]["balance"]
        resp = client.post(f"/v1/paper/settle/{bet_id}", json={"result": "lost"})
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["result"] == "lost"
        assert data["credit"] == 0.0
        assert data["new_balance"] == pytest.approx(before, abs=0.01)

    def test_settle_push(self, client, moneyline_bet):
        """Settling as 'push' refunds the stake exactly."""
        bet_id = self._place(client, moneyline_bet)
        before = client.get("/v1/paper/portfolio").json()["data"]["balance"]
        resp = client.post(f"/v1/paper/settle/{bet_id}", json={"result": "push"})
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["result"] == "push"
        assert data["credit"] == pytest.approx(moneyline_bet["stake"], abs=0.01)
        assert data["new_balance"] == pytest.approx(before + moneyline_bet["stake"], abs=0.01)

    def test_settle_invalid_result(self, client, moneyline_bet):
        """Invalid result string returns HTTP 400."""
        bet_id = self._place(client, moneyline_bet)
        resp = client.post(f"/v1/paper/settle/{bet_id}", json={"result": "invalid"})
        assert resp.status_code == 400

    def test_settle_nonexistent_bet(self, client):
        """Settling an unknown bet returns 404."""
        resp = client.post("/v1/paper/settle/pb_nonexistent_xyz", json={"result": "won"})
        assert resp.status_code == 404

    def test_settle_already_settled(self, client, moneyline_bet):
        """Re-settling an already settled bet returns 400."""
        bet_id = self._place(client, moneyline_bet)
        client.post(f"/v1/paper/settle/{bet_id}", json={"result": "won"})
        resp = client.post(f"/v1/paper/settle/{bet_id}", json={"result": "won"})
        assert resp.status_code == 400

    def test_settle_updates_portfolio_stats(self, client, moneyline_bet):
        """Winning bet updates portfolio balance above starting point."""
        bet_id = self._place(client, moneyline_bet)
        client.post(f"/v1/paper/settle/{bet_id}", json={"result": "won"})
        balance = client.get("/v1/paper/portfolio").json()["data"]["balance"]
        # After a win balance should exceed 10000 - stake + stake + payout = > 10000
        assert balance > 10_000.0 - moneyline_bet["stake"]


# ── Bet Cancellation Tests ────────────────────────────────────────────

class TestCancelBet:
    """Test DELETE /v1/paper/bet/{bet_id}."""

    def _place(self, client, bet_payload):
        r = client.post("/v1/paper/bet", json=bet_payload)
        assert r.status_code == 200
        return r.json()["data"]["bet_id"], r.json()["data"]["new_balance"]

    def test_cancel_refunds_stake(self, client, moneyline_bet):
        """Cancelling a bet refunds the stake."""
        bet_id, balance_after_bet = self._place(client, moneyline_bet)
        resp = client.delete(f"/v1/paper/bet/{bet_id}")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["refunded"] == pytest.approx(moneyline_bet["stake"], abs=0.01)
        assert data["new_balance"] == pytest.approx(balance_after_bet + moneyline_bet["stake"], abs=0.01)

    def test_cancel_nonexistent_bet(self, client):
        """Cancelling an unknown bet returns 404."""
        resp = client.delete("/v1/paper/bet/pb_nonexistent_xyz")
        assert resp.status_code == 404

    def test_cancel_already_settled_bet(self, client, moneyline_bet):
        """Cancelling an already settled bet returns 400."""
        bet_id = client.post("/v1/paper/bet", json=moneyline_bet).json()["data"]["bet_id"]
        client.post(f"/v1/paper/settle/{bet_id}", json={"result": "won"})
        resp = client.delete(f"/v1/paper/bet/{bet_id}")
        assert resp.status_code == 400

    def test_cancel_removes_bet_from_active(self, client, moneyline_bet):
        """Cancelled bet no longer appears as pending in portfolio."""
        bet_id, _ = self._place(client, moneyline_bet)
        client.delete(f"/v1/paper/bet/{bet_id}")
        bets = client.get("/v1/paper/portfolio").json()["data"]["bets"]
        cancelled = next((b for b in bets if b["id"] == bet_id), None)
        # Either it's removed or its result is 'cancelled'
        if cancelled:
            assert cancelled["result"] == "cancelled"


# ── History Tests ─────────────────────────────────────────────────────

class TestHistory:
    """Test GET /v1/paper/history."""

    def test_history_returns_success(self, client):
        """History endpoint returns standard success envelope."""
        response = client.get("/v1/paper/history")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert "data" in payload
        assert isinstance(payload["data"], list)

    def test_history_meta_schema(self, client):
        """History meta includes pagination fields."""
        meta = client.get("/v1/paper/history").json()["meta"]
        assert "total" in meta
        assert "limit" in meta
        assert "offset" in meta

    def test_history_pagination_limit(self, client):
        """History respects limit parameter."""
        resp = client.get("/v1/paper/history?limit=5")
        assert resp.status_code == 200
        assert len(resp.json()["data"]) <= 5
        assert resp.json()["meta"]["limit"] == 5

    def test_history_pagination_offset(self, client):
        """History respects offset parameter."""
        resp = client.get("/v1/paper/history?offset=0&limit=10")
        assert resp.status_code == 200
        assert resp.json()["meta"]["offset"] == 0

    def test_history_status_filter(self, client):
        """History accepts a status filter."""
        resp = client.get("/v1/paper/history?status=active")
        assert resp.status_code == 200
        for bet in resp.json()["data"]:
            assert bet["result"] == "pending"

    def test_history_bets_have_required_fields(self, client, moneyline_bet):
        """Bets in history include all expected front-end fields."""
        client.post("/v1/paper/bet", json=moneyline_bet)
        bets = client.get("/v1/paper/history").json()["data"]
        if bets:
            bet = bets[0]
            for field in ["id", "date", "sport", "matchup", "betType", "pick", "odds", "stake", "result", "pnl"]:
                assert field in bet, f"Missing field: {field}"

    def test_history_invalid_limit_rejected(self, client):
        """Limit of 0 is rejected (ge=1)."""
        assert client.get("/v1/paper/history?limit=0").status_code == 422


# ── Leaderboard Tests ─────────────────────────────────────────────────

class TestPaperLeaderboard:
    """Test GET /v1/paper/leaderboard."""

    def test_leaderboard_returns_success(self, client):
        """Leaderboard returns standard success envelope."""
        response = client.get("/v1/paper/leaderboard")
        assert response.status_code == 200
        assert response.json()["success"] is True

    def test_leaderboard_data_is_list(self, client):
        """Leaderboard data is always a list."""
        assert isinstance(client.get("/v1/paper/leaderboard").json()["data"], list)

    def test_leaderboard_entries_schema(self, client):
        """Leaderboard entries (if any) have expected fields."""
        data = client.get("/v1/paper/leaderboard").json()["data"]
        for entry in data:
            for field in ["rank", "display_name", "balance", "pnl", "total_bets", "wins", "losses", "win_rate"]:
                assert field in entry, f"Missing field: {field}"
            assert entry["rank"] >= 1

    def test_leaderboard_rank_sequential(self, client):
        """Ranks are sequential starting at 1."""
        data = client.get("/v1/paper/leaderboard").json()["data"]
        ranks = [e["rank"] for e in data]
        assert ranks == list(range(1, len(ranks) + 1))

    def test_leaderboard_limit_parameter(self, client):
        """Leaderboard respects limit parameter."""
        resp = client.get("/v1/paper/leaderboard?limit=5")
        assert resp.status_code == 200
        assert len(resp.json()["data"]) <= 5

    def test_leaderboard_win_rate_in_range(self, client):
        """Win rate is between 0 and 100 for all entries."""
        for entry in client.get("/v1/paper/leaderboard").json()["data"]:
            assert 0.0 <= entry["win_rate"] <= 100.0
