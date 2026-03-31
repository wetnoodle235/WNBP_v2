"""Data models for the autobet paper-trading system."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class BetLeg:
    """A single leg within a paper bet.

    bet_type values:
      Full-game:    "winner" | "spread" | "total" | "prop"
      Draw/OT:      "draw" | "three_way" | "double_chance" | "overtime"
      Halftime:     "halftime_winner" | "halftime_spread" | "halftime_total"
      Period:       "period_winner" | "period_total" | "period_spread"
      First/Last:   "first_score" | "last_score"
      Method:       "method_of_victory" | "ko_tko" | "submission" | "decision"
      BTTS:         "btts" (both teams to score)
      Clean Sheet:  "clean_sheet_home" | "clean_sheet_away"
      Margin:       "winning_margin"
      Tennis:       "straight_sets"

    For period bets, ``period_number`` indicates which period/quarter/inning.
    """

    game_id: str
    sport: str
    bet_type: str
    selection: str  # team name, "over"/"under", "draw", "home_draw", "away_draw", etc.
    line: float | None  # spread or total line
    odds_american: int
    odds_decimal: float
    bookmaker: str
    model_confidence: float
    model_edge: float
    home_team: str = ""
    away_team: str = ""
    # period number for quarter/period/inning bets (1-based)
    period_number: int | None = None
    player_id: str | None = None
    player_name: str | None = None
    prop_market: str | None = None
    result: str | None = None  # "won" | "lost" | "push" | "void"

    def to_dict(self) -> dict:
        return {
            "game_id": self.game_id,
            "sport": self.sport,
            "bet_type": self.bet_type,
            "selection": self.selection,
            "line": self.line,
            "odds_american": self.odds_american,
            "odds_decimal": self.odds_decimal,
            "bookmaker": self.bookmaker,
            "model_confidence": self.model_confidence,
            "model_edge": self.model_edge,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "period_number": self.period_number,
            "player_id": self.player_id,
            "player_name": self.player_name,
            "prop_market": self.prop_market,
            "result": self.result,
        }

    @classmethod
    def from_dict(cls, d: dict) -> BetLeg:
        return cls(
            game_id=d["game_id"],
            sport=d["sport"],
            bet_type=d["bet_type"],
            selection=d["selection"],
            line=d.get("line"),
            odds_american=d.get("odds_american", 0),
            odds_decimal=d.get("odds_decimal", 0.0),
            bookmaker=d.get("bookmaker", ""),
            model_confidence=d.get("model_confidence", 0.0),
            model_edge=d.get("model_edge", 0.0),
            home_team=d.get("home_team", ""),
            away_team=d.get("away_team", ""),
            period_number=d.get("period_number"),
            player_id=d.get("player_id"),
            player_name=d.get("player_name"),
            prop_market=d.get("prop_market"),
            result=d.get("result"),
        )


@dataclass
class BetCandidate:
    """A filtered bet candidate ready for sizing."""

    leg: BetLeg
    tier: str  # "S" (Platinum) | "A" (Gold) | "B" (Silver) | "C" (Bronze) | "D" (Copper)
    kelly_stake: float = 0.0
    ev_per_unit: float = 0.0
    rationale: str = ""


@dataclass
class PaperBet:
    """A placed paper bet (one or more legs)."""

    id: str
    placed_at: str  # ISO-8601 UTC
    sport: str
    bet_type: str  # "winner" | "spread" | "total" | "prop" | "parlay"
    legs: list[BetLeg]
    stake_units: float
    model_confidence: float
    model_edge: float
    implied_odds: float  # combined decimal odds
    status: str = "pending"  # "pending" | "won" | "lost" | "push" | "void"
    result_at: str | None = None
    pnl_units: float = 0.0
    rationale: str = ""
    strategy: str = "core"  # "core" | "lotto_daily" | "ladder_daily"
    notify_sent: bool = False

    @property
    def leg_count(self) -> int:
        return len(self.legs)

    @property
    def is_parlay(self) -> bool:
        return len(self.legs) > 1

    @staticmethod
    def new_id() -> str:
        return f"ab_{uuid.uuid4().hex[:12]}"

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "placed_at": self.placed_at,
            "sport": self.sport,
            "bet_type": self.bet_type,
            "leg_count": self.leg_count,
            "legs": [leg.to_dict() for leg in self.legs],
            "stake_units": self.stake_units,
            "model_confidence": self.model_confidence,
            "model_edge": self.model_edge,
            "implied_odds": self.implied_odds,
            "status": self.status,
            "result_at": self.result_at,
            "pnl_units": self.pnl_units,
            "rationale": self.rationale,
            "strategy": self.strategy,
            "notify_sent": self.notify_sent,
        }

    @classmethod
    def from_dict(cls, d: dict) -> PaperBet:
        legs_raw = d.get("legs", [])
        if isinstance(legs_raw, str):
            import json

            legs_raw = json.loads(legs_raw)
        return cls(
            id=d["id"],
            placed_at=d["placed_at"],
            sport=d["sport"],
            bet_type=d["bet_type"],
            legs=[BetLeg.from_dict(lg) for lg in legs_raw],
            stake_units=d.get("stake_units", 0.0),
            model_confidence=d.get("model_confidence", 0.0),
            model_edge=d.get("model_edge", 0.0),
            implied_odds=d.get("implied_odds", 0.0),
            status=d.get("status", "pending"),
            result_at=d.get("result_at"),
            pnl_units=d.get("pnl_units", 0.0),
            rationale=d.get("rationale", ""),
            strategy=d.get("strategy", "core"),
            notify_sent=bool(d.get("notify_sent", False)),
        )
