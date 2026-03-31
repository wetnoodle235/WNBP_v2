# ──────────────────────────────────────────────────────────
# V5.0 Backend — Server-Sent Events (Live Data)
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Annotated, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Request, status
from starlette.responses import StreamingResponse

from config import SPORT_DEFINITIONS
from services.data_service import DataService, get_data_service

logger = logging.getLogger(__name__)

_LIVE_PRED_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "live_predictions"

router = APIRouter(prefix="/v1/sse")

_SSE_HEADERS = {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


def _validate_sport(sport: str) -> str:
    if sport not in SPORT_DEFINITIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown sport '{sport}'",
        )
    return sport


ValidSport = Annotated[str, Depends(_validate_sport)]
DS = Annotated[DataService, Depends(get_data_service)]


def _sse_event(data: dict, event: str | None = None, event_id: str | None = None) -> str:
    """Format a single SSE message."""
    lines: list[str] = []
    if event_id:
        lines.append(f"id: {event_id}")
    if event:
        lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(data, default=str)}")
    lines.append("")  # trailing blank line terminates the event
    return "\n".join(lines) + "\n"


def _sse_comment(message: str) -> str:
    """Format an SSE comment line (ignored by EventSource clients)."""
    return f": {message}\n\n"


def _load_live_predictions(sport: str) -> dict:
    """Read the latest live prediction file for a sport."""
    path = _LIVE_PRED_DIR / f"{sport}_live.json"
    if not path.exists():
        logger.debug("No live predictions file for %s at %s", sport, path)
        return {}
    try:
        data = json.loads(path.read_text())
        logger.debug("Loaded live predictions for %s: %d entries", sport, len(data))
        return data
    except Exception:
        logger.warning("Failed to parse live predictions for %s", sport, exc_info=True)
        return {}


async def _live_game_stream(
    sport: str,
    ds: DataService,
    request: Request,
    poll_interval: float = 10.0,
) -> AsyncGenerator[str, None]:
    """Yield SSE events for live game updates.

    Polls the data service at ``poll_interval`` seconds and emits
    changed records.  Also emits live prediction updates when available.
    Terminates cleanly when the client disconnects.
    """
    prev_snapshot: dict[str, dict] = {}
    prev_pred_snapshot: dict[str, dict] = {}
    seq = 0
    heartbeat_every = 30.0
    last_sent = time.monotonic()

    # Initial keepalive
    yield _sse_event({"type": "connected", "sport": sport}, event="system")
    last_sent = time.monotonic()

    while True:
        if await request.is_disconnected():
            break

        try:
            games = ds.get_games(sport)
            live = [g for g in games if g.get("status") == "in_progress"]

            for game in live:
                gid = str(game.get("id", ""))
                prev = prev_snapshot.get(gid)
                if prev != game:
                    seq += 1
                    yield _sse_event(game, event="game_update", event_id=str(seq))
                    last_sent = time.monotonic()
                    prev_snapshot[gid] = game

            # Emit live prediction updates
            preds = _load_live_predictions(sport)
            for pg in preds.get("games", []):
                pgid = str(pg.get("game_id", ""))
                if pgid and prev_pred_snapshot.get(pgid) != pg:
                    seq += 1
                    yield _sse_event(pg, event="prediction_update", event_id=str(seq))
                    last_sent = time.monotonic()
                    prev_pred_snapshot[pgid] = pg

        except Exception:
            logger.exception("Error in live game stream for %s", sport)
            yield _sse_event({"error": "temporary data error"}, event="error")
            last_sent = time.monotonic()

        if time.monotonic() - last_sent >= heartbeat_every:
            yield _sse_comment("heartbeat")
            last_sent = time.monotonic()

        await asyncio.sleep(poll_interval)


async def _odds_change_stream(
    sport: str,
    ds: DataService,
    request: Request,
    poll_interval: float = 15.0,
) -> AsyncGenerator[str, None]:
    """Yield SSE events when odds data changes."""
    prev_snapshot: dict[str, dict] = {}
    seq = 0
    heartbeat_every = 30.0
    last_sent = time.monotonic()

    yield _sse_event({"type": "connected", "sport": sport}, event="system")
    last_sent = time.monotonic()

    while True:
        if await request.is_disconnected():
            break

        try:
            odds = ds.get_odds(sport)
            for entry in odds:
                key = f"{entry.get('game_id')}:{entry.get('bookmaker')}"
                if prev_snapshot.get(key) != entry:
                    seq += 1
                    yield _sse_event(entry, event="odds_update", event_id=str(seq))
                    last_sent = time.monotonic()
                    prev_snapshot[key] = entry

        except Exception:
            logger.exception("Error in odds stream for %s", sport)
            yield _sse_event({"error": "temporary data error"}, event="error")
            last_sent = time.monotonic()

        if time.monotonic() - last_sent >= heartbeat_every:
            yield _sse_comment("heartbeat")
            last_sent = time.monotonic()

        await asyncio.sleep(poll_interval)


@router.get(
    "/{sport}/live",
    summary="Live game score stream (SSE)",
    description="Server-Sent Event stream of live game updates for a sport. The server polls for in-progress games and pushes score changes to the client in real-time. Also emits `prediction_update` events with live-adjusted win probabilities. Connect with an EventSource client.",
    tags=["Live"],
    responses={
        200: {
            "description": "SSE stream of game updates (text/event-stream)",
            "content": {
                "text/event-stream": {
                    "example": (
                        "event: system\n"
                        "data: {\"type\": \"connected\", \"sport\": \"nba\"}\n\n"
                        "id: 1\n"
                        "event: game_update\n"
                        "data: {\"id\": \"401710904\", \"home_team\": \"Cleveland Cavaliers\", \"away_team\": \"Orlando Magic\", \"home_score\": 85, \"away_score\": 72, \"status\": \"in_progress\", \"period\": \"Q3\"}\n\n"
                    )
                }
            },
        }
    },
)
async def live_games(sport: ValidSport, request: Request, ds: DS):
    """SSE stream of live game updates for a sport.

    The server polls for in-progress games and pushes changes to the
    client.  Send a heartbeat comment every ~30 s to keep the
    connection alive through proxies.
    """
    return StreamingResponse(
        _live_game_stream(sport, ds, request),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )


@router.get(
    "/{sport}/odds",
    summary="Live odds movement stream (SSE)",
    description="Server-Sent Event stream of odds line movements for a sport. Emits `odds_update` events when any sportsbook changes its moneyline, spread, or total for an active game. Connect with an EventSource client.",
    tags=["Live"],
    responses={
        200: {
            "description": "SSE stream of odds changes (text/event-stream)",
            "content": {
                "text/event-stream": {
                    "example": (
                        "event: system\n"
                        "data: {\"type\": \"connected\", \"sport\": \"nba\"}\n\n"
                        "id: 1\n"
                        "event: odds_update\n"
                        "data: {\"game_id\": \"401710904\", \"bookmaker\": \"DraftKings\", \"h2h_home\": -200, \"h2h_away\": 170, \"spread_home\": -5.5, \"total_line\": 214.5, \"is_live\": true}\n\n"
                    )
                }
            },
        }
    },
)
async def live_odds(sport: ValidSport, request: Request, ds: DS):
    """SSE stream of odds changes for a sport."""
    return StreamingResponse(
        _odds_change_stream(sport, ds, request),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )
