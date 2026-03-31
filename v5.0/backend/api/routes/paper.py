# ──────────────────────────────────────────────────────────
# V5.0 Backend — Paper Trading Routes
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Annotated, Any, Optional
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from api.middleware.auth import get_current_user, verify_token
from auth.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/paper", tags=["Paper Trading"])

STARTING_BALANCE = 10_000.0

# ── Helpers ──────────────────────────────────────────────

_PAPER_SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_portfolio (
    user_id TEXT PRIMARY KEY,
    balance REAL NOT NULL DEFAULT 10000.0,
    total_bets INTEGER NOT NULL DEFAULT 0,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    pushes INTEGER NOT NULL DEFAULT 0,
    total_wagered REAL NOT NULL DEFAULT 0,
    total_won REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS paper_bets (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    sport TEXT NOT NULL,
    game_id TEXT,
    matchup TEXT NOT NULL DEFAULT '',
    bet_type TEXT NOT NULL,
    selection TEXT NOT NULL,
    pick TEXT NOT NULL DEFAULT '',
    odds INTEGER NOT NULL,
    line REAL,
    stake REAL NOT NULL,
    potential_payout REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    placed_at TEXT NOT NULL DEFAULT (datetime('now')),
    settled_at TEXT,
    result_details TEXT
);

CREATE INDEX IF NOT EXISTS idx_paper_bets_user ON paper_bets(user_id);
CREATE INDEX IF NOT EXISTS idx_paper_bets_status ON paper_bets(status);
"""


async def _ensure_tables() -> None:
    """Create paper trading tables if they don't exist."""
    async with get_db() as db:
        await db.executescript(_PAPER_SCHEMA)

        # Backward-compatible schema migration for existing deployments.
        cur = await db.execute("PRAGMA table_info(paper_bets)")
        cols = {row["name"] for row in await cur.fetchall()}
        if "line" not in cols:
            await db.execute("ALTER TABLE paper_bets ADD COLUMN line REAL")

        await db.commit()


def _require_auth() -> bool:
    """Return True when auth is enforced."""
    val = os.getenv("REQUIRE_AUTH", "false").lower()
    return val in ("true", "1", "yes")


async def _get_user_id(user: dict[str, Any] | None) -> str:
    """Extract user_id from JWT payload, defaulting to 'anonymous'."""
    if user and user.get("sub"):
        return user["sub"]
    return "anonymous"


def _calc_payout(stake: float, odds: int) -> float:
    """Calculate potential payout from American odds."""
    if odds > 0:
        return round(stake * (odds / 100), 2)
    else:
        return round(stake * (100 / abs(odds)), 2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _bet_to_frontend(row: dict) -> dict:
    """Convert a paper_bets DB row to the frontend PaperBet format."""
    status_map = {"active": "pending", "won": "win", "lost": "loss", "push": "push", "cancelled": "cancelled"}
    result = status_map.get(row["status"], row["status"])

    pnl = 0.0
    if row["status"] == "won":
        pnl = row["potential_payout"]
    elif row["status"] == "lost":
        pnl = -row["stake"]
    # push = 0

    return {
        "id": row["id"],
        "date": (row["placed_at"] or "")[:10],
        "sport": row["sport"],
        "matchup": row.get("matchup") or row.get("game_id") or "",
        "betType": row["bet_type"],
        "pick": row.get("pick") or row["selection"],
        "line": row.get("line"),
        "odds": row["odds"],
        "stake": row["stake"],
        "result": result,
        "pnl": pnl,
    }


# ── Flexible auth dependency (works with or without token) ──

async def _optional_user(
    credentials: Annotated[Any, Depends(get_current_user)] = None,
) -> dict[str, Any]:
    return credentials  # type: ignore[return-value]


from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_bearer = HTTPBearer(auto_error=False)


async def get_user_flexible(
    credentials: Annotated[Optional[HTTPAuthorizationCredentials], Depends(_bearer)] = None,
) -> dict[str, Any]:
    """Get user from JWT if present; return anonymous fallback otherwise."""
    if credentials is not None:
        payload = verify_token(credentials.credentials)
        if payload:
            return {
                "sub": payload.get("sub", "anonymous"),
                "tier": payload.get("tier", "free"),
            }
    if _require_auth():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {"sub": "anonymous", "tier": "free"}


User = Annotated[dict[str, Any], Depends(get_user_flexible)]


# ── Request models ───────────────────────────────────────

class PlaceBetRequest(BaseModel):
    sport: str
    game_id: Optional[str] = None
    matchup: str = ""
    bet_type: str  # moneyline, spread, total, parlay
    selection: str  # home, away, over, under
    pick: str = ""
    line: Optional[float] = None
    odds: int  # American odds
    stake: float = Field(gt=0)


class SettleBetRequest(BaseModel):
    result: str  # won, lost, push
    details: Optional[str] = None


# ── Endpoints ────────────────────────────────────────────

@router.get("/portfolio")
async def get_portfolio(user: User):
    """Get user's paper trading portfolio with balance and all bets."""
    await _ensure_tables()
    user_id = await _get_user_id(user)

    async with get_db() as db:
        # Get or create portfolio
        cursor = await db.execute(
            "SELECT * FROM paper_portfolio WHERE user_id = ?", (user_id,)
        )
        portfolio_row = await cursor.fetchone()

        if not portfolio_row:
            await db.execute(
                "INSERT INTO paper_portfolio (user_id, balance) VALUES (?, ?)",
                (user_id, STARTING_BALANCE),
            )
            await db.commit()
            balance = STARTING_BALANCE
        else:
            balance = portfolio_row["balance"]

        # Get all bets
        cursor = await db.execute(
            "SELECT * FROM paper_bets WHERE user_id = ? ORDER BY placed_at DESC",
            (user_id,),
        )
        rows = await cursor.fetchall()
        bets = [_bet_to_frontend(dict(r)) for r in rows]

    return {
        "success": True,
        "data": {
            "balance": balance,
            "bets": bets,
        },
    }


@router.post("/bet")
async def place_bet(req: PlaceBetRequest, user: User):
    """Place a new paper bet."""
    await _ensure_tables()
    user_id = await _get_user_id(user)

    if req.bet_type in {"spread", "total"} and req.line is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="'line' is required for spread and total bets",
        )

    potential_payout = _calc_payout(req.stake, req.odds)
    bet_id = f"pb_{uuid4().hex[:12]}"

    async with get_db() as db:
        # Get current balance
        cursor = await db.execute(
            "SELECT balance FROM paper_portfolio WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()

        if not row:
            await db.execute(
                "INSERT INTO paper_portfolio (user_id, balance) VALUES (?, ?)",
                (user_id, STARTING_BALANCE),
            )
            await db.commit()
            current_balance = STARTING_BALANCE
        else:
            current_balance = row["balance"]

        if req.stake > current_balance:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Insufficient balance. Available: ${current_balance:.2f}",
            )

        # Deduct stake from balance
        new_balance = round(current_balance - req.stake, 2)
        now = _now_iso()

        await db.execute(
            """INSERT INTO paper_bets
               (id, user_id, sport, game_id, matchup, bet_type, selection, pick, odds, line, stake, potential_payout, status, placed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)""",
            (
                bet_id,
                user_id,
                req.sport,
                req.game_id,
                req.matchup,
                req.bet_type,
                req.selection,
                req.pick or req.selection,
                req.odds,
                req.line,
                req.stake,
                potential_payout,
                now,
            ),
        )

        await db.execute(
            """UPDATE paper_portfolio
               SET balance = ?, total_bets = total_bets + 1, total_wagered = total_wagered + ?
               WHERE user_id = ?""",
            (new_balance, req.stake, user_id),
        )
        await db.commit()

    return {
        "success": True,
        "data": {
            "bet_id": bet_id,
            "stake": req.stake,
            "potential_payout": potential_payout,
            "new_balance": new_balance,
        },
    }


@router.post("/settle/{bet_id}")
async def settle_bet(bet_id: str, req: SettleBetRequest, user: User):
    """Settle an active bet (won, lost, push)."""
    await _ensure_tables()
    user_id = await _get_user_id(user)

    if req.result not in ("won", "lost", "push"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Result must be 'won', 'lost', or 'push'",
        )

    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM paper_bets WHERE id = ? AND user_id = ?",
            (bet_id, user_id),
        )
        bet = await cursor.fetchone()

        if not bet:
            raise HTTPException(status_code=404, detail="Bet not found")
        if bet["status"] != "active":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Bet already settled: {bet['status']}",
            )

        now = _now_iso()
        details_json = json.dumps({"result": req.result, "details": req.details, "settled_at": now})

        # Calculate balance adjustment
        if req.result == "won":
            credit = bet["stake"] + bet["potential_payout"]
            col_update = "wins = wins + 1, total_won = total_won + ?"
            col_vals: list[Any] = [bet["potential_payout"]]
        elif req.result == "lost":
            credit = 0.0
            col_update = "losses = losses + 1"
            col_vals = []
        else:  # push
            credit = bet["stake"]
            col_update = "pushes = pushes + 1"
            col_vals = []

        await db.execute(
            "UPDATE paper_bets SET status = ?, settled_at = ?, result_details = ? WHERE id = ?",
            (req.result, now, details_json, bet_id),
        )

        await db.execute(
            f"UPDATE paper_portfolio SET balance = balance + ?, {col_update} WHERE user_id = ?",
            (credit, *col_vals, user_id),
        )
        await db.commit()

        # Get updated balance
        cursor = await db.execute(
            "SELECT balance FROM paper_portfolio WHERE user_id = ?", (user_id,)
        )
        updated = await cursor.fetchone()

    return {
        "success": True,
        "data": {
            "bet_id": bet_id,
            "result": req.result,
            "credit": credit,
            "new_balance": updated["balance"] if updated else 0,
        },
    }


@router.delete("/bet/{bet_id}")
async def cancel_bet(bet_id: str, user: User):
    """Cancel an active bet and refund the stake."""
    await _ensure_tables()
    user_id = await _get_user_id(user)

    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM paper_bets WHERE id = ? AND user_id = ?",
            (bet_id, user_id),
        )
        bet = await cursor.fetchone()

        if not bet:
            raise HTTPException(status_code=404, detail="Bet not found")
        if bet["status"] != "active":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot cancel: bet is {bet['status']}",
            )

        now = _now_iso()
        await db.execute(
            "UPDATE paper_bets SET status = 'cancelled', settled_at = ? WHERE id = ?",
            (now, bet_id),
        )
        await db.execute(
            "UPDATE paper_portfolio SET balance = balance + ?, total_bets = total_bets - 1, total_wagered = total_wagered - ? WHERE user_id = ?",
            (bet["stake"], bet["stake"], user_id),
        )
        await db.commit()

        cursor = await db.execute(
            "SELECT balance FROM paper_portfolio WHERE user_id = ?", (user_id,)
        )
        updated = await cursor.fetchone()

    return {
        "success": True,
        "data": {
            "bet_id": bet_id,
            "refunded": bet["stake"],
            "new_balance": updated["balance"] if updated else 0,
        },
    }


@router.get("/history")
async def get_history(
    user: User,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status_filter: Optional[str] = Query(None, alias="status"),
):
    """Get bet history with pagination."""
    await _ensure_tables()
    user_id = await _get_user_id(user)

    async with get_db() as db:
        base_where = "WHERE user_id = ?"
        params: list[Any] = [user_id]

        if status_filter:
            base_where += " AND status = ?"
            params.append(status_filter)

        # Count
        cursor = await db.execute(
            f"SELECT COUNT(*) as cnt FROM paper_bets {base_where}", params
        )
        total = (await cursor.fetchone())["cnt"]

        # Fetch page
        cursor = await db.execute(
            f"SELECT * FROM paper_bets {base_where} ORDER BY placed_at DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        )
        rows = await cursor.fetchall()
        bets = [_bet_to_frontend(dict(r)) for r in rows]

    return {
        "success": True,
        "data": bets,
        "meta": {
            "total": total,
            "limit": limit,
            "offset": offset,
        },
    }


@router.get("/leaderboard")
async def get_leaderboard(
    limit: int = Query(20, ge=1, le=100),
):
    """Top paper traders by profit."""
    await _ensure_tables()

    async with get_db() as db:
        cursor = await db.execute(
            """SELECT p.user_id,
                      p.balance,
                      p.total_bets,
                      p.wins,
                      p.losses,
                      p.pushes,
                      p.total_wagered,
                      p.total_won,
                      (p.balance - 10000.0) as pnl,
                      COALESCE(u.display_name, p.user_id) as display_name
               FROM paper_portfolio p
               LEFT JOIN users u ON p.user_id = u.id
               WHERE p.total_bets > 0
               ORDER BY pnl DESC
               LIMIT ?""",
            (limit,),
        )
        rows = await cursor.fetchall()

    leaders = []
    for i, r in enumerate(rows, 1):
        leaders.append({
            "rank": i,
            "display_name": r["display_name"],
            "balance": r["balance"],
            "pnl": r["pnl"],
            "total_bets": r["total_bets"],
            "wins": r["wins"],
            "losses": r["losses"],
            "win_rate": round(r["wins"] / max(r["wins"] + r["losses"], 1) * 100, 1),
        })

    return {
        "success": True,
        "data": leaders,
    }


@router.post("/reset")
async def reset_portfolio(user: User):
    """Reset portfolio to starting balance and clear all bets."""
    await _ensure_tables()
    user_id = await _get_user_id(user)

    async with get_db() as db:
        await db.execute("DELETE FROM paper_bets WHERE user_id = ?", (user_id,))
        await db.execute(
            """UPDATE paper_portfolio
               SET balance = ?, total_bets = 0, wins = 0, losses = 0,
                   pushes = 0, total_wagered = 0, total_won = 0
               WHERE user_id = ?""",
            (STARTING_BALANCE, user_id),
        )
        await db.commit()

    return {
        "success": True,
        "data": {"balance": STARTING_BALANCE, "bets": []},
    }


# ── Auto-settlement helper ──────────────────────────────

async def auto_settle_bets(game_results: list[dict]) -> int:
    """Settle paper bets matching completed games.

    game_results format:
        [{"game_id": "...", "winner": "home"|"away", "home_score": N, "away_score": N, "total": N}]

    Returns number of bets settled.
    """
    await _ensure_tables()
    settled_count = 0

    async with get_db() as db:
        for game in game_results:
            game_id = game.get("game_id")
            if not game_id:
                continue

            cursor = await db.execute(
                "SELECT * FROM paper_bets WHERE game_id = ? AND status = 'active'",
                (game_id,),
            )
            active_bets = await cursor.fetchall()

            for bet in active_bets:
                bet_dict = dict(bet)
                result = _determine_result(bet_dict, game)
                if result is None:
                    continue

                now = _now_iso()
                details = json.dumps({"game": game, "auto_settled": True})

                if result == "won":
                    credit = bet_dict["stake"] + bet_dict["potential_payout"]
                    col_update = "wins = wins + 1, total_won = total_won + ?"
                    col_vals: list[Any] = [bet_dict["potential_payout"]]
                elif result == "lost":
                    credit = 0.0
                    col_update = "losses = losses + 1"
                    col_vals = []
                else:
                    credit = bet_dict["stake"]
                    col_update = "pushes = pushes + 1"
                    col_vals = []

                await db.execute(
                    "UPDATE paper_bets SET status = ?, settled_at = ?, result_details = ? WHERE id = ?",
                    (result, now, details, bet_dict["id"]),
                )
                await db.execute(
                    f"UPDATE paper_portfolio SET balance = balance + ?, {col_update} WHERE user_id = ?",
                    (credit, *col_vals, bet_dict["user_id"]),
                )
                settled_count += 1

        if settled_count:
            await db.commit()

    logger.info("Auto-settled %d paper bets", settled_count)
    return settled_count


def _determine_result(bet: dict, game: dict) -> Optional[str]:
    """Determine bet outcome from game result."""

    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    winner = game.get("winner")  # "home" or "away"
    bet_type = bet["bet_type"]
    selection = str(bet["selection"]).lower()

    if bet_type == "moneyline":
        if selection == winner:
            return "won"
        elif winner:
            return "lost"
    elif bet_type == "spread":
        line = _to_float(bet.get("line"))
        home_score = _to_float(game.get("home_score"))
        away_score = _to_float(game.get("away_score"))
        if line is None or home_score is None or away_score is None:
            return None

        margin = home_score - away_score
        spread_result = margin + line

        if spread_result == 0:
            return "push"

        home_covers = spread_result > 0
        if selection == "home":
            return "won" if home_covers else "lost"
        if selection == "away":
            return "won" if not home_covers else "lost"
    elif bet_type == "total":
        line = _to_float(bet.get("line"))
        total = _to_float(game.get("total"))
        if total is None:
            home_score = _to_float(game.get("home_score"))
            away_score = _to_float(game.get("away_score"))
            if home_score is not None and away_score is not None:
                total = home_score + away_score
        if line is None or total is None:
            return None

        if total == line:
            return "push"
        if selection == "over":
            return "won" if total > line else "lost"
        if selection == "under":
            return "won" if total < line else "lost"

    # Default: can't determine automatically
    return None
