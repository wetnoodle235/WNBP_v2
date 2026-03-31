"""SQLite ledger for paper bet tracking.

Stores all bets, grades, and bankroll snapshots in a local SQLite database
with WAL mode for safe concurrent access.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Generator

from .models import BetLeg, PaperBet

logger = logging.getLogger("autobet.ledger")

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS paper_bets (
    id               TEXT PRIMARY KEY,
    placed_at        TEXT NOT NULL,
    sport            TEXT NOT NULL,
    bet_type         TEXT NOT NULL,
    leg_count        INTEGER NOT NULL DEFAULT 1,
    legs             TEXT NOT NULL,
    stake_units      REAL NOT NULL,
    model_confidence REAL,
    model_edge       REAL,
    implied_odds     REAL,
    status           TEXT NOT NULL DEFAULT 'pending',
    result_at        TEXT,
    pnl_units        REAL DEFAULT 0.0,
    rationale        TEXT DEFAULT '',
    game_id          TEXT,
    strategy         TEXT NOT NULL DEFAULT 'core',
    notify_sent      INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pb_status ON paper_bets(status);
CREATE INDEX IF NOT EXISTS idx_pb_sport  ON paper_bets(sport);
CREATE INDEX IF NOT EXISTS idx_pb_placed ON paper_bets(placed_at);
CREATE INDEX IF NOT EXISTS idx_pb_strategy ON paper_bets(strategy);

CREATE TABLE IF NOT EXISTS bankroll_snapshots (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               TEXT NOT NULL,
    balance          REAL NOT NULL,
    peak             REAL NOT NULL,
    pending_exposure REAL NOT NULL DEFAULT 0.0,
    event            TEXT NOT NULL DEFAULT 'cycle'
);
"""


class Ledger:
    """SQLite-backed paper bet ledger."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ── connection helpers ──────────────────────────────────────────────

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(str(self.db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(_SCHEMA)
        logger.info("Ledger DB ready at %s", self.db_path)

    # ── write operations ────────────────────────────────────────────────

    def place_bet(self, bet: PaperBet) -> str:
        """Insert a new pending bet. Returns the bet ID."""
        legs_json = json.dumps([leg.to_dict() for leg in bet.legs])
        game_id = bet.legs[0].game_id if len(bet.legs) == 1 else None

        with self._conn() as conn:
            # Dedup: skip if same game + bet_type + strategy placed today
            if game_id:
                today = date.today().isoformat()
                dup = conn.execute(
                    "SELECT id FROM paper_bets "
                    "WHERE game_id = ? AND bet_type = ? AND strategy = ? "
                    "AND placed_at >= ? AND status = 'pending'",
                    (game_id, bet.bet_type, bet.strategy, today),
                ).fetchone()
                if dup:
                    logger.debug(
                        "Skipping duplicate bet on %s %s", game_id, bet.bet_type
                    )
                    return dup["id"]

            conn.execute(
                "INSERT INTO paper_bets "
                "(id, placed_at, sport, bet_type, leg_count, legs, stake_units, "
                " model_confidence, model_edge, implied_odds, status, rationale, "
                " game_id, strategy, notify_sent) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    bet.id,
                    bet.placed_at,
                    bet.sport,
                    bet.bet_type,
                    bet.leg_count,
                    legs_json,
                    bet.stake_units,
                    bet.model_confidence,
                    bet.model_edge,
                    bet.implied_odds,
                    bet.status,
                    bet.rationale,
                    game_id,
                    bet.strategy,
                    0,
                ),
            )
        logger.info(
            "Placed %s bet %s: %s %.2fu @ %.3f (edge %.3f)",
            bet.strategy,
            bet.id,
            bet.bet_type,
            bet.stake_units,
            bet.implied_odds,
            bet.model_edge,
        )
        return bet.id

    def grade_bet(self, bet_id: str, status: str, pnl: float) -> None:
        """Update a bet with its final result."""
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._conn() as conn:
            conn.execute(
                "UPDATE paper_bets SET status=?, result_at=?, pnl_units=? WHERE id=?",
                (status, now, pnl, bet_id),
            )
        logger.info("Graded %s → %s (P&L: %+.2f)", bet_id, status, pnl)

    def mark_notified(self, bet_id: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE paper_bets SET notify_sent=1 WHERE id=?", (bet_id,)
            )

    def snapshot_bankroll(
        self, balance: float, peak: float, pending: float, event: str = "cycle"
    ) -> None:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO bankroll_snapshots (ts, balance, peak, pending_exposure, event) "
                "VALUES (?,?,?,?,?)",
                (now, balance, peak, pending, event),
            )

    # ── read operations ─────────────────────────────────────────────────

    def get_pending(self) -> list[PaperBet]:
        """All bets with status = 'pending'."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM paper_bets WHERE status='pending' ORDER BY placed_at"
            ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def get_today_bets(self, sport: str | None = None) -> list[PaperBet]:
        """Bets placed today, optionally filtered by sport."""
        today = date.today().isoformat()
        with self._conn() as conn:
            if sport:
                rows = conn.execute(
                    "SELECT * FROM paper_bets WHERE placed_at >= ? AND sport = ? "
                    "ORDER BY placed_at",
                    (today, sport),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM paper_bets WHERE placed_at >= ? ORDER BY placed_at",
                    (today,),
                ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def get_daily_count(self, bet_type: str | None = None, strategy: str | None = None) -> int:
        """Count of bets placed today by type and/or strategy."""
        today = date.today().isoformat()
        query = "SELECT COUNT(*) as cnt FROM paper_bets WHERE placed_at >= ?"
        params: list = [today]
        if bet_type:
            query += " AND bet_type = ?"
            params.append(bet_type)
        if strategy:
            query += " AND strategy = ?"
            params.append(strategy)
        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
        return row["cnt"] if row else 0

    def get_bankroll(self, starting: float) -> float:
        """Current bankroll = starting + sum(pnl) of all graded bets."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(pnl_units), 0.0) as total_pnl "
                "FROM paper_bets WHERE status IN ('won','lost','push')"
            ).fetchone()
        return starting + (row["total_pnl"] if row else 0.0)

    def get_pending_exposure(self) -> float:
        """Sum of stake_units for all pending bets."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(stake_units), 0.0) as exposure "
                "FROM paper_bets WHERE status='pending'"
            ).fetchone()
        return row["exposure"] if row else 0.0

    def get_recent_bets(
        self, sport: str | None = None, bet_type: str | None = None, limit: int = 50
    ) -> list[PaperBet]:
        """Most recent graded bets for performance analysis."""
        query = (
            "SELECT * FROM paper_bets "
            "WHERE status IN ('won','lost','push') "
        )
        params: list = []
        if sport:
            query += " AND sport = ?"
            params.append(sport)
        if bet_type:
            query += " AND bet_type = ?"
            params.append(bet_type)
        query += " ORDER BY result_at DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def get_stats(self, sport: str | None = None, days: int = 30) -> dict:
        """Win rate, ROI, total P&L, breakdown by sport and bet_type."""
        cutoff = datetime.now(timezone.utc).isoformat(timespec="seconds")
        # We use a simple date subtraction since SQLite doesn't have dateadd
        from datetime import timedelta

        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
        cutoff = cutoff_dt.isoformat(timespec="seconds")

        query = (
            "SELECT sport, bet_type, status, "
            "COUNT(*) as cnt, SUM(pnl_units) as pnl, SUM(stake_units) as wagered "
            "FROM paper_bets WHERE result_at >= ? "
        )
        params: list = [cutoff]
        if sport:
            query += " AND sport = ?"
            params.append(sport)
        query += " GROUP BY sport, bet_type, status"

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()

        total_won = 0
        total_lost = 0
        total_push = 0
        total_pnl = 0.0
        total_wagered = 0.0
        by_sport: dict[str, dict] = {}
        by_type: dict[str, dict] = {}

        for r in rows:
            s, bt, status = r["sport"], r["bet_type"], r["status"]
            cnt, pnl, wagered = r["cnt"], r["pnl"] or 0.0, r["wagered"] or 0.0

            if status == "won":
                total_won += cnt
            elif status == "lost":
                total_lost += cnt
            elif status == "push":
                total_push += cnt
            total_pnl += pnl
            total_wagered += wagered

            for key, bucket in [(s, by_sport), (bt, by_type)]:
                if key not in bucket:
                    bucket[key] = {"won": 0, "lost": 0, "push": 0, "pnl": 0.0, "wagered": 0.0}
                if status in ("won", "lost", "push"):
                    bucket[key][status] += cnt
                bucket[key]["pnl"] += pnl
                bucket[key]["wagered"] += wagered

        total_graded = total_won + total_lost + total_push
        win_rate = total_won / total_graded if total_graded > 0 else 0.0
        roi = total_pnl / total_wagered if total_wagered > 0 else 0.0

        return {
            "days": days,
            "total_graded": total_graded,
            "won": total_won,
            "lost": total_lost,
            "push": total_push,
            "win_rate": round(win_rate, 4),
            "total_pnl": round(total_pnl, 2),
            "total_wagered": round(total_wagered, 2),
            "roi": round(roi, 4),
            "by_sport": by_sport,
            "by_bet_type": by_type,
        }

    # ── internal ────────────────────────────────────────────────────────

    @staticmethod
    def _row_to_bet(row: sqlite3.Row) -> PaperBet:
        d = dict(row)
        return PaperBet.from_dict(d)
