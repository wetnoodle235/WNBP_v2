# ──────────────────────────────────────────────────────────
# V5.0 Backend — AutoBet API Routes
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from api.middleware.auth import get_current_user, require_tier
from autobet.config import AutobetConfig
from autobet.ledger import Ledger

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/autobet", tags=["AutoBet"])

# ── Shared dependencies ──────────────────────────────────────────────

_DB_PATH = Path(__file__).resolve().parent.parent.parent / "autobet" / "data" / "paper_bets.db"


def _get_ledger() -> Ledger:
    return Ledger(_DB_PATH)


def _get_config() -> AutobetConfig:
    return AutobetConfig.from_env()


LedgerDep = Annotated[Ledger, Depends(_get_ledger)]
ConfigDep = Annotated[AutobetConfig, Depends(_get_config)]
User = Annotated[dict[str, Any], Depends(get_current_user)]
PremiumUser = Annotated[dict[str, Any], Depends(require_tier("premium"))]


# ── GET /v1/autobet/status ───────────────────────────────────────────

@router.get(
    "/status",
    summary="AutoBet bot status and active bets",
    description="Returns the current bot status, bankroll, active (pending) bets, and today's P/L summary.",
    responses={200: {"description": "Bot status with active bets and daily summary"}},
)
async def get_status(ledger: LedgerDep, config: ConfigDep):
    pending = ledger.get_pending()
    today_bets = ledger.get_today_bets()
    bankroll = ledger.get_bankroll(config.bankroll_dollars)
    exposure = ledger.get_pending_exposure()
    stats = ledger.get_stats(days=30)

    today_won = sum(1 for b in today_bets if b.status == "won")
    today_lost = sum(1 for b in today_bets if b.status == "lost")
    today_push = sum(1 for b in today_bets if b.status == "push")
    today_pending = sum(1 for b in today_bets if b.status == "pending")
    today_pnl = sum(b.pnl_units for b in today_bets if b.status in ("won", "lost", "push"))

    return {
        "success": True,
        "data": {
            "bot": {
                "enabled": config.enabled,
                "sports": config.sports,
                "betting_cycle_seconds": config.betting_cycle_seconds,
                "grading_cycle_seconds": config.grading_cycle_seconds,
            },
            "bankroll": {
                "starting": config.bankroll_dollars,
                "current": round(bankroll, 2),
                "pending_exposure": round(exposure, 2),
            },
            "today": {
                "total": len(today_bets),
                "pending": today_pending,
                "won": today_won,
                "lost": today_lost,
                "push": today_push,
                "pnl": round(today_pnl, 2),
            },
            "stats_30d": stats,
            "active_bets": [b.to_dict() for b in pending],
        },
    }


# ── GET /v1/autobet/history ──────────────────────────────────────────

@router.get(
    "/history",
    summary="AutoBet bet history",
    description="Returns recent completed bets with outcomes and P/L.",
    responses={200: {"description": "Paginated bet history"}},
)
async def get_history(
    ledger: LedgerDep,
    sport: Optional[str] = Query(None, description="Filter by sport"),
    bet_type: Optional[str] = Query(None, description="Filter by bet type"),
    limit: int = Query(50, ge=1, le=200, description="Number of bets to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    bets = ledger.get_recent_bets(sport=sport, bet_type=bet_type, limit=limit + offset)
    page = bets[offset : offset + limit]

    return {
        "success": True,
        "data": [b.to_dict() for b in page],
        "meta": {
            "count": len(page),
            "total": len(bets),
            "limit": limit,
            "offset": offset,
        },
    }


# ── GET /v1/autobet/config ───────────────────────────────────────────

@router.get(
    "/config",
    summary="AutoBet configuration",
    description="Returns the current bot configuration. Requires premium tier.",
    responses={200: {"description": "Current configuration"}},
)
async def get_config(user: PremiumUser, config: ConfigDep):
    cfg_dict = dataclasses.asdict(config)
    # Convert sets to lists for JSON serialization
    cfg_dict["excluded_vendors"] = sorted(cfg_dict.get("excluded_vendors", []))
    # Convert tuple to list
    cfg_dict["pregame_window_minutes"] = list(cfg_dict.get("pregame_window_minutes", (60, 90)))
    # Redact internal API key
    cfg_dict.pop("internal_api_key", None)
    cfg_dict.pop("discord_webhook_url", None)

    return {
        "success": True,
        "data": cfg_dict,
    }


# ── POST /v1/autobet/config ──────────────────────────────────────────

class ConfigUpdateRequest(BaseModel):
    bankroll_dollars: Optional[float] = Field(None, gt=0, le=100000)
    min_confidence: Optional[float] = Field(None, ge=0.50, le=0.99)
    min_edge: Optional[float] = Field(None, ge=0.01, le=0.50)
    max_stake_units: Optional[float] = Field(None, ge=0.10, le=50.0)
    min_stake_units: Optional[float] = Field(None, ge=0.10, le=50.0)
    kelly_fraction: Optional[float] = Field(None, ge=0.05, le=1.0)
    sports: Optional[list[str]] = None
    parlay_enabled: Optional[bool] = None
    lotto_enabled: Optional[bool] = None
    ladder_enabled: Optional[bool] = None


@router.post(
    "/config",
    summary="Update AutoBet configuration",
    description="Updates bot configuration fields. Requires premium tier. "
    "Note: changes are applied via environment variables and take effect on next cycle.",
    responses={200: {"description": "Updated configuration fields"}},
)
async def update_config(req: ConfigUpdateRequest, user: PremiumUser):
    import os

    updates: dict[str, Any] = {}
    for field_name, value in req.model_dump(exclude_none=True).items():
        env_key = f"AUTOBET_{field_name.upper()}"
        if isinstance(value, list):
            os.environ[env_key] = ",".join(value)
        elif isinstance(value, bool):
            os.environ[env_key] = "true" if value else "false"
        else:
            os.environ[env_key] = str(value)
        updates[field_name] = value

    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No configuration fields provided",
        )

    return {
        "success": True,
        "data": {
            "updated_fields": updates,
            "message": "Configuration updated. Changes take effect on next cycle.",
        },
    }
