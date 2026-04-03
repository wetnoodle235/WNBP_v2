# ──────────────────────────────────────────────────────────
# V5.0 Backend — Sport Data Routes
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from auth.middleware import (
    check_tier_access,
    clamp_limit,
    rate_limit_check,
    require_api_key,
)
from auth.models import APIKeyInfo
from config import SPORT_DEFINITIONS, get_available_seasons, get_current_season, get_settings, is_season_active
from services.data_service import DataService, get_data_service
from services.media_mirror import get_media_mirror_service

router = APIRouter(prefix="/v1/{sport}")
_settings = get_settings()
_media = get_media_mirror_service()


# ── Helpers ───────────────────────────────────────────────

def _validate_sport(sport: str) -> str:
    if sport not in SPORT_DEFINITIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown sport '{sport}'. Valid: {', '.join(sorted(SPORT_DEFINITIONS))}",
        )
    return sport


ValidSport = Annotated[str, Depends(_validate_sport)]
DS = Annotated[DataService, Depends(get_data_service)]
ApiKey = Annotated[APIKeyInfo, Depends(require_api_key)]


def _resolve_season(sport: str, season: str | None) -> str:
    """Return the effective season — smart default when None."""
    if season and season != "all":
        return season
    if season == "all":
        return "all"
    return get_current_season(sport)


def _enrich_odds_consensus(odds: list[dict]) -> list[dict]:
    """Compute per-game bookmaker consensus score and inject into each record.

    consensus_score: 0-100 where 100 = perfect bookmaker agreement on moneyline.
    consensus_warning: True when std-dev of implied probability exceeds 3 ppts.
    """
    import math
    from collections import defaultdict

    def american_to_implied(odds_val: float | None) -> float | None:
        if odds_val is None:
            return None
        try:
            o = float(odds_val)
            if o > 0:
                return 100.0 / (o + 100.0)
            else:
                return abs(o) / (abs(o) + 100.0)
        except (TypeError, ValueError):
            return None

    by_game: dict[str, list[float]] = defaultdict(list)
    for o in odds:
        gid = str(o.get("game_id", ""))
        implied = american_to_implied(o.get("h2h_home"))
        if implied is not None:
            by_game[gid].append(implied)

    scores: dict[str, dict] = {}
    for gid, probs in by_game.items():
        if len(probs) < 2:
            scores[gid] = {"consensus_score": None, "consensus_warning": False}
            continue
        mean = sum(probs) / len(probs)
        variance = sum((p - mean) ** 2 for p in probs) / len(probs)
        std = math.sqrt(variance)
        # Map std to 0-100 score: 0 std → 100, 10ppt std → 0
        score = max(0.0, min(100.0, round(100.0 - std * 1000, 1)))
        scores[gid] = {"consensus_score": score, "consensus_warning": std > 0.03}

    result = []
    for o in odds:
        gid = str(o.get("game_id", ""))
        if gid in scores:
            o = {**o, **scores[gid]}
        result.append(o)
    return result


def _require_internal_access(api_key: APIKeyInfo) -> None:
    """Restrict internal/dev endpoints to platform or local-anonymous keys only."""
    if api_key.key_id in {"__platform__", "__anonymous__"}:
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="This endpoint is internal and not available to customer API tiers.",
    )


def _paginated_response(
    data: list[dict],
    sport: str,
    *,
    limit: int = 50,
    offset: int = 0,
    season: str | None = None,
    extra_meta: dict | None = None,
    api_key: APIKeyInfo | None = None,
    response: Response | None = None,
) -> dict[str, Any]:
    """Build a consistent paginated API response with tier-based limiting."""
    # Apply tier-based limit clamping
    if api_key:
        limit = clamp_limit(limit, api_key.tier)
        if response:
            response.headers["X-User-Tier"] = api_key.tier
            if api_key.tier == "enterprise":
                response.headers["X-Export-Enabled"] = "true"

    total = len(data)
    page = data[offset : offset + limit]
    effective_season = _resolve_season(sport, season)
    meta: dict[str, Any] = {
        "sport": sport,
        "season": effective_season,
        "available_seasons": get_available_seasons(sport),
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "cached_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra_meta:
        meta.update(extra_meta)
    return {"success": True, "data": page, "meta": meta}


def _apply_team_logo_mirror(sport: str, team: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(team, dict):
        return team
    team_id = str(team.get("id", ""))
    source_url = team.get("logo_url")
    team["logo_url"] = _media.team_logo_url(
        sport=sport,
        team_id=team_id,
        source_url=source_url,
        auto_sync=_settings.media_auto_sync,
    )
    return team


def _apply_player_headshot_mirror(sport: str, player: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(player, dict):
        return player
    player_id = str(player.get("id", ""))
    source_url = player.get("headshot_url")
    player["headshot_url"] = _media.player_headshot_url(
        sport=sport,
        player_id=player_id,
        source_url=source_url,
        auto_sync=_settings.media_auto_sync,
    )
    return player


# ── Overview ──────────────────────────────────────────────

@router.get(
    "/overview",
    summary="Sport dashboard overview",
    description="Aggregated dashboard data: recent games, current standings snapshot, top headlines, and injury count. Ideal for rendering a single-sport home page.",
    tags=["Overview"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Dashboard data for the requested sport",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "recent_games": [
                                {
                                    "id": "401710904",
                                    "sport": "nba",
                                    "date": "2025-03-25",
                                    "home_team": "Cleveland Cavaliers",
                                    "away_team": "Orlando Magic",
                                    "home_score": 118,
                                    "away_score": 105,
                                    "status": "final",
                                    "venue": "Rocket Mortgage FieldHouse",
                                }
                            ],
                            "standings": [
                                {"team_id": "5", "wins": 58, "losses": 12, "pct": 0.829, "conference": "Eastern", "conference_rank": 1}
                            ],
                            "top_news": [
                                {"headline": "Cavaliers clinch top seed in Eastern Conference", "published_at": "2025-03-25T18:30:00Z"}
                            ],
                            "injury_count": 14,
                            "team_count": 30,
                            "game_count": 1230,
                        },
                        "meta": {"sport": "nba", "season": "2025", "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def sport_overview(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year (e.g. 2025). Defaults to current season based on sport calendar."),
):
    """Dashboard-style overview for a single sport."""
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)

    effective_season = _resolve_season(sport, season)
    games = ds.get_games(sport, season=effective_season)
    standings = ds.get_standings(sport, season=effective_season)
    news = ds.get_news(sport, limit=5)
    injuries = ds.get_injuries(sport)
    teams = ds.get_teams(sport, season=effective_season)

    result_limit = clamp_limit(10, api_key.tier)
    recent_games = sorted(
        games, key=lambda g: str(g.get("date", "")), reverse=True,
    )[:result_limit]

    response.headers["X-User-Tier"] = api_key.tier
    return {
        "success": True,
        "data": {
            "recent_games": recent_games,
            "standings": standings[:result_limit],
            "top_news": news,
            "injury_count": len(injuries),
            "team_count": len(teams),
            "game_count": len(games),
        },
        "meta": {
            "sport": sport,
            "season": effective_season,
            "available_seasons": get_available_seasons(sport),
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


# ── Games ─────────────────────────────────────────────────

@router.get(
    "/games",
    summary="List games",
    description=(
        "Retrieve games for a sport with optional season, date, team, and status filters. "
        "Supports pagination and sorting. Returns scores, venues, broadcasts, and game status.\n\n"
        "**Season**: Defaults to current season based on sport calendar. "
        "Use `?season=all` to load all seasons.\n\n"
        "**Sorting**: `?sort=date` (ascending) or `?sort=-date` (descending). "
        "Prefix `-` for descending order. Sortable fields: `date`, `confidence`, `status`."
    ),
    tags=["Games"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated list of games",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "id": "401710904",
                                "sport": "nba",
                                "season": "2025",
                                "date": "2025-03-25",
                                "home_team": "Cleveland Cavaliers",
                                "away_team": "Orlando Magic",
                                "home_score": 118,
                                "away_score": 105,
                                "status": "final",
                                "venue": "Rocket Mortgage FieldHouse",
                                "broadcast": "ESPN",
                                "home_team_id": "5",
                                "away_team_id": "19",
                            },
                            {
                                "id": "401710905",
                                "sport": "nba",
                                "season": "2025",
                                "date": "2025-03-25",
                                "home_team": "Boston Celtics",
                                "away_team": "Milwaukee Bucks",
                                "home_score": 112,
                                "away_score": 108,
                                "status": "final",
                                "venue": "TD Garden",
                                "broadcast": "TNT",
                                "home_team_id": "2",
                                "away_team_id": "15",
                            },
                        ],
                        "meta": {
                            "sport": "nba",
                            "count": 2,
                            "total": 1230,
                            "limit": 50,
                            "offset": 0,
                            "season": "2025",
                            "cached_at": "2025-03-25T20:00:00Z",
                        },
                    }
                }
            },
        }
    },
)
async def list_games(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(
        None,
        description="Season year (e.g. 2025). Defaults to current season based on sport calendar. Use 'all' for every season.",
    ),
    date: Optional[str] = Query(None, description="Filter by exact date (YYYY-MM-DD)"),
    date_start: Optional[str] = Query(None, description="Start of date range (YYYY-MM-DD, inclusive)"),
    date_end: Optional[str] = Query(None, description="End of date range (YYYY-MM-DD, inclusive)"),
    team: Optional[str] = Query(None, description="Filter by team name or ID"),
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by game status (scheduled, in_progress, final)"),
    sort: Optional[str] = Query(None, description="Sort field. Prefix '-' for descending (e.g. '-date', 'date', '-confidence')"),
    limit: int = Query(50, ge=1, le=1000, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)
    games = ds.get_games(sport, season=effective_season, date=date)
    if date_start:
        games = [g for g in games if str(g.get("date", "")) >= date_start]
    if date_end:
        games = [g for g in games if str(g.get("date", "")) <= date_end]
    if team:
        tl = team.lower()
        games = [
            g for g in games
            if tl in str(g.get("home_team", "")).lower()
            or tl in str(g.get("away_team", "")).lower()
            or str(g.get("home_team_id", "")) == team
            or str(g.get("away_team_id", "")) == team
        ]
    if status_filter:
        games = [g for g in games if g.get("status") == status_filter]
    if sort:
        descending = sort.startswith("-")
        field = sort.lstrip("-")
        games = sorted(
            games,
            key=lambda g: (g.get(field) is None, g.get(field, "")),
            reverse=descending,
        )
    return _paginated_response(games, sport, limit=limit, offset=offset, season=effective_season, api_key=api_key, response=response)


@router.get(
    "/games/{game_id}",
    summary="Get game detail",
    description="Retrieve a single game by its unique ID, including scores, venue, broadcast info, and period details.",
    tags=["Games"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Single game object",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "id": "401710904",
                            "sport": "nba",
                            "season": "2025",
                            "date": "2025-03-25",
                            "home_team": "Cleveland Cavaliers",
                            "away_team": "Orlando Magic",
                            "home_score": 118,
                            "away_score": 105,
                            "status": "final",
                            "venue": "Rocket Mortgage FieldHouse",
                            "attendance": 20562,
                            "broadcast": "ESPN",
                            "home_team_id": "5",
                            "away_team_id": "19",
                        },
                        "meta": {"sport": "nba"},
                    }
                }
            },
        },
        404: {
            "description": "Game not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Game '999999' not found"}
                }
            },
        },
    },
)
async def get_game(sport: ValidSport, game_id: str, ds: DS, api_key: ApiKey, response: Response):
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    response.headers["X-User-Tier"] = api_key.tier
    games = ds.get_games(sport)
    match = [g for g in games if str(g.get("id")) == game_id or str(g.get("game_id", "")) == game_id]
    if not match:
        raise HTTPException(status_code=404, detail=f"Game '{game_id}' not found")
    return {"success": True, "data": match[0], "meta": {"sport": sport, "season": _resolve_season(sport, None)}}


# ── Teams ─────────────────────────────────────────────────

@router.get(
    "/teams",
    summary="List teams",
    description="Retrieve all teams for a sport including abbreviation, city, conference, division, venue, and logo. Optionally filter by season.",
    tags=["Teams"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated list of teams",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "id": "5",
                                "sport": "nba",
                                "name": "Cleveland Cavaliers",
                                "abbreviation": "CLE",
                                "city": "Cleveland",
                                "conference": "Eastern",
                                "division": "Central",
                                "venue_name": "Rocket Mortgage FieldHouse",
                                "logo_url": "https://a.espncdn.com/i/teamlogos/nba/500/cle.png",
                                "color_primary": "#6F263D",
                            },
                            {
                                "id": "2",
                                "sport": "nba",
                                "name": "Boston Celtics",
                                "abbreviation": "BOS",
                                "city": "Boston",
                                "conference": "Eastern",
                                "division": "Atlantic",
                                "venue_name": "TD Garden",
                                "logo_url": "https://a.espncdn.com/i/teamlogos/nba/500/bos.png",
                                "color_primary": "#007A33",
                            },
                        ],
                        "meta": {"sport": "nba", "count": 2, "total": 30, "limit": 50, "offset": 0, "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def list_teams(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year (e.g. 2025). Defaults to current season based on sport calendar."),
    limit: int = Query(50, ge=1, le=1000, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "teams")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)
    teams = ds.get_teams(sport, season=effective_season)
    teams = [_apply_team_logo_mirror(sport, t) for t in teams]
    return _paginated_response(teams, sport, limit=limit, offset=offset, season=effective_season, api_key=api_key, response=response)


@router.get(
    "/teams/{team_id}",
    summary="Get team detail with roster",
    description="Retrieve a single team by ID or abbreviation. Includes the full roster (up to 50 players) when available.",
    tags=["Teams"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Team detail with attached roster",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "id": "5",
                            "sport": "nba",
                            "name": "Cleveland Cavaliers",
                            "abbreviation": "CLE",
                            "city": "Cleveland",
                            "conference": "Eastern",
                            "division": "Central",
                            "venue_name": "Rocket Mortgage FieldHouse",
                            "roster": [
                                {"id": "4066261", "name": "Donovan Mitchell", "position": "SG", "jersey_number": 45},
                                {"id": "3155526", "name": "Darius Garland", "position": "PG", "jersey_number": 10},
                                {"id": "4432166", "name": "Evan Mobley", "position": "PF", "jersey_number": 4},
                            ],
                        },
                        "meta": {"sport": "nba", "season": "2025"},
                    }
                }
            },
        },
        404: {
            "description": "Team not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Team 'XYZ' not found"}
                }
            },
        },
    },
)
async def get_team(
    sport: ValidSport,
    team_id: str,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season for roster/stats context. Defaults to current season based on sport calendar."),
):
    await check_tier_access(api_key, sport, "teams")
    await rate_limit_check(api_key)
    response.headers["X-User-Tier"] = api_key.tier
    effective_season = _resolve_season(sport, season)
    team_index = ds.get_teams_index(sport, season=effective_season)
    match = team_index.get(team_id) or team_index.get(team_id.lower())
    if not match:
        raise HTTPException(status_code=404, detail=f"Team '{team_id}' not found")
    team_data = match.copy()
    team_data = _apply_team_logo_mirror(sport, team_data)
    players = ds.get_players(sport, season=effective_season, team_id=team_id)
    players = [_apply_player_headshot_mirror(sport, p) for p in players]
    team_data["roster"] = players[:50] if players else []
    return {"success": True, "data": team_data, "meta": {"sport": sport, "season": effective_season}}


# ── Standings ─────────────────────────────────────────────

@router.get(
    "/standings",
    summary="Get standings",
    description=(
        "Current standings for a sport and season, including wins, losses, win percentage, "
        "conference/division rankings, streaks, and clinch status.\n\n"
        "**Season**: Defaults to current season based on sport calendar."
    ),
    tags=["Standings"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated standings table",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "team_id": "5",
                                "sport": "nba",
                                "season": "2025",
                                "wins": 58,
                                "losses": 12,
                                "pct": 0.829,
                                "conference": "Eastern",
                                "division": "Central",
                                "conference_rank": 1,
                                "division_rank": 1,
                                "streak": "W7",
                                "home_record": "32-4",
                                "away_record": "26-8",
                                "clinch_status": "z",
                            },
                            {
                                "team_id": "2",
                                "sport": "nba",
                                "season": "2025",
                                "wins": 52,
                                "losses": 18,
                                "pct": 0.743,
                                "conference": "Eastern",
                                "division": "Atlantic",
                                "conference_rank": 2,
                                "division_rank": 1,
                                "streak": "W3",
                                "home_record": "30-6",
                                "away_record": "22-12",
                                "clinch_status": "x",
                            },
                        ],
                        "meta": {"sport": "nba", "count": 2, "total": 30, "limit": 50, "offset": 0, "season": "2025", "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def list_standings(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(
        None,
        description="Season year (e.g. 2025). Defaults to current season based on sport calendar.",
    ),
    conference: Optional[str] = Query(None, description="Filter by conference (e.g. Eastern, Western, AFC, NFC)"),
    limit: int = Query(50, ge=1, le=200, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "standings")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    active = is_season_active(sport)

    standings = ds.get_standings(sport, season=effective_season)
    if conference:
        cl = conference.lower()
        standings = [s for s in standings if cl in str(s.get("conference", "")).lower()]
    return _paginated_response(
        standings, sport, limit=limit, offset=offset,
        season=effective_season, api_key=api_key, response=response,
        extra_meta={"season_active": active, "season_year": effective_season},
    )


# ── Players ───────────────────────────────────────────────

@router.get(
    "/players",
    summary="Search players",
    description="List and search players for a sport. Filter by team ID, player name (case-insensitive substring), or season. Returns biographical data, position, status, and headshot URLs.",
    tags=["Players"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated list of players",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "id": "4066261",
                                "sport": "nba",
                                "name": "Donovan Mitchell",
                                "team_id": "5",
                                "position": "SG",
                                "jersey_number": 45,
                                "height": "6-1",
                                "weight": 215,
                                "birth_date": "1996-09-07",
                                "nationality": "USA",
                                "experience_years": 7,
                                "status": "active",
                            },
                            {
                                "id": "3155526",
                                "sport": "nba",
                                "name": "Darius Garland",
                                "team_id": "5",
                                "position": "PG",
                                "jersey_number": 10,
                                "height": "6-1",
                                "weight": 192,
                                "birth_date": "2000-01-26",
                                "nationality": "USA",
                                "experience_years": 5,
                                "status": "active",
                            },
                        ],
                        "meta": {"sport": "nba", "count": 2, "total": 540, "limit": 50, "offset": 0, "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def list_players(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year. Defaults to current season based on sport calendar."),
    team_id: Optional[str] = Query(None, description="Filter by team ID"),
    search: Optional[str] = Query(None, description="Search by player name (case-insensitive)"),
    limit: int = Query(50, ge=1, le=1000, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "players")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)
    players = ds.get_players(sport, season=effective_season, team_id=team_id, search=search)
    players = [_apply_player_headshot_mirror(sport, p) for p in players]
    return _paginated_response(players, sport, limit=limit, offset=offset, season=effective_season, api_key=api_key, response=response)


@router.get(
    "/players/{player_id}",
    summary="Get player detail",
    description="Retrieve a single player by their unique ID. Returns full biographical data including height, weight, birth date, nationality, and current status.",
    tags=["Players"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Single player object",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "id": "4066261",
                            "sport": "nba",
                            "name": "Donovan Mitchell",
                            "team_id": "5",
                            "position": "SG",
                            "jersey_number": 45,
                            "height": "6-1",
                            "weight": 215,
                            "birth_date": "1996-09-07",
                            "nationality": "USA",
                            "experience_years": 7,
                            "status": "active",
                            "headshot_url": "https://a.espncdn.com/combiner/i?img=/i/headshots/nba/players/full/3908809.png",
                        },
                        "meta": {"sport": "nba"},
                    }
                }
            },
        },
        404: {
            "description": "Player not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Player '999999' not found"}
                }
            },
        },
    },
)
async def get_player(sport: ValidSport, player_id: str, ds: DS, api_key: ApiKey, response: Response):
    await check_tier_access(api_key, sport, "players")
    await rate_limit_check(api_key)
    response.headers["X-User-Tier"] = api_key.tier
    players = ds.get_players(sport)
    match = [p for p in players if str(p.get("id")) == player_id]
    if not match:
        raise HTTPException(status_code=404, detail=f"Player '{player_id}' not found")
    player_data = _apply_player_headshot_mirror(sport, match[0])
    return {"success": True, "data": player_data, "meta": {"sport": sport, "season": _resolve_season(sport, None)}}


# ── Player Stats ──────────────────────────────────────────

@router.get("/player-stats", include_in_schema=False, deprecated=True)
@router.get(
    "/player_stats",
    summary="Player box-score statistics",
    description="Per-game player statistics with sport-specific stat categories (points/rebounds/assists for basketball, passing/rushing yards for football, etc.). Filter by season and/or player ID.",
    tags=["Stats"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated list of player game stats",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "game_id": "401710904",
                                "player_id": "4066261",
                                "sport": "nba",
                                "player_name": "Donovan Mitchell",
                                "team_id": "5",
                                "season": "2025",
                                "date": "2025-03-25",
                                "minutes": 36.5,
                                "category": "basketball",
                                "pts": 32,
                                "reb": 5,
                                "ast": 8,
                                "stl": 2,
                                "blk": 0,
                                "fg_pct": 0.545,
                                "three_m": 4,
                                "three_a": 9,
                                "plus_minus": 15,
                            },
                            {
                                "game_id": "401710904",
                                "player_id": "4432166",
                                "sport": "nba",
                                "player_name": "Evan Mobley",
                                "team_id": "5",
                                "season": "2025",
                                "date": "2025-03-25",
                                "minutes": 34.0,
                                "category": "basketball",
                                "pts": 22,
                                "reb": 11,
                                "ast": 3,
                                "stl": 1,
                                "blk": 3,
                                "fg_pct": 0.600,
                                "plus_minus": 12,
                            },
                        ],
                        "meta": {"sport": "nba", "count": 2, "total": 15420, "limit": 50, "offset": 0, "season": "2025", "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def list_player_stats(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year (e.g. 2025). Defaults to current season based on sport calendar."),
    player_id: Optional[str] = Query(None, description="Filter by player ID"),
    aggregate: bool = Query(False, description="If true, returns per-player season averages instead of per-game stats"),
    limit: int = Query(50, ge=1, le=1000, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "stats")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)
    stats = await ds.get_player_stats_async(sport, season=effective_season, player_id=player_id, aggregate=aggregate)
    return _paginated_response(stats, sport, limit=limit, offset=offset, season=effective_season, api_key=api_key, response=response)


# ── Odds ──────────────────────────────────────────────────

@router.get(
    "/odds",
    summary="Betting odds",
    description="Pre-game betting odds from multiple sportsbooks including moneyline (h2h), point spread, and over/under totals. Filter by game ID or date.",
    tags=["Odds"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated list of odds entries",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "game_id": "401710904",
                                "sport": "nba",
                                "bookmaker": "DraftKings",
                                "h2h_home": -185,
                                "h2h_away": 155,
                                "spread_home": -4.5,
                                "spread_away": 4.5,
                                "spread_home_line": -110,
                                "spread_away_line": -110,
                                "total_line": 215.5,
                                "total_over": -110,
                                "total_under": -110,
                                "timestamp": "2025-03-25T17:30:00Z",
                                "is_live": False,
                            },
                            {
                                "game_id": "401710904",
                                "sport": "nba",
                                "bookmaker": "FanDuel",
                                "h2h_home": -190,
                                "h2h_away": 160,
                                "spread_home": -4.5,
                                "spread_away": 4.5,
                                "spread_home_line": -108,
                                "spread_away_line": -112,
                                "total_line": 216.0,
                                "total_over": -112,
                                "total_under": -108,
                                "timestamp": "2025-03-25T17:32:00Z",
                                "is_live": False,
                            },
                        ],
                        "meta": {"sport": "nba", "count": 2, "total": 4800, "limit": 50, "offset": 0, "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def list_odds(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year (e.g. 2025). Default: current season"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=1000, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "odds")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)
    odds = ds.get_odds(sport, game_id=game_id, season=effective_season)
    if date:
        odds = [
            o for o in odds
            if str(o.get("date", "")).startswith(date)
            or str(o.get("timestamp", "")).startswith(date)
        ]
    odds = _enrich_odds_consensus(odds)
    return _paginated_response(odds, sport, limit=limit, offset=offset, season=effective_season, api_key=api_key, response=response)


@router.get(
    "/odds/{game_id}",
    summary="Odds for a specific game",
    description="All sportsbook odds for a single game, including moneyline, spread, and totals from every available bookmaker.",
    tags=["Odds"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "All odds entries for the specified game",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "game_id": "401710904",
                                "sport": "nba",
                                "bookmaker": "DraftKings",
                                "h2h_home": -185,
                                "h2h_away": 155,
                                "spread_home": -4.5,
                                "total_line": 215.5,
                            },
                            {
                                "game_id": "401710904",
                                "sport": "nba",
                                "bookmaker": "BetMGM",
                                "h2h_home": -180,
                                "h2h_away": 150,
                                "spread_home": -4.0,
                                "total_line": 215.0,
                            },
                        ],
                        "meta": {"sport": "nba", "game_id": "401710904", "count": 2},
                    }
                }
            },
        }
    },
)
async def get_game_odds(sport: ValidSport, game_id: str, ds: DS, api_key: ApiKey, response: Response):
    await check_tier_access(api_key, sport, "odds")
    await rate_limit_check(api_key)
    response.headers["X-User-Tier"] = api_key.tier
    odds = ds.get_odds(sport, game_id=game_id)
    return {
        "success": True,
        "data": odds,
        "meta": {"sport": sport, "game_id": game_id, "count": len(odds)},
    }


# ── Predictions ───────────────────────────────────────────

@router.get(
    "/predictions",
    summary="Model predictions for a sport",
    description="Machine learning model predictions for upcoming and recent games, including win probabilities and spread forecasts. Filter by date.",
    tags=["Predictions"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated list of predictions",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "game_id": "401710910",
                                "sport": "nba",
                                "model": "catboost_v5.2",
                                "home_win_prob": 0.72,
                                "away_win_prob": 0.28,
                                "predicted_spread": -6.5,
                                "predicted_total": 218.5,
                                "confidence": 0.81,
                            }
                        ],
                        "meta": {"sport": "nba", "count": 1, "total": 6, "limit": 50, "offset": 0, "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def list_predictions(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD). Defaults to today."),
    limit: int = Query(50, ge=1, le=1000, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "predictions")
    await rate_limit_check(api_key)
    if not date:
        from datetime import date as date_type
        date = date_type.today().isoformat()
    preds = ds.get_predictions(sport, date=date)
    return _paginated_response(preds, sport, limit=limit, offset=offset, api_key=api_key, response=response)


# ── Injuries ──────────────────────────────────────────────

@router.get(
    "/injuries",
    summary="Active injury reports",
    description="Current injury reports for all teams in a sport. Includes player name, injury status (out, doubtful, questionable, probable), body part, and expected return date.",
    tags=["Injuries"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated list of injury reports",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "player_id": "3112335",
                                "sport": "nba",
                                "player_name": "Kawhi Leonard",
                                "team_id": "12",
                                "status": "out",
                                "description": "Right knee inflammation",
                                "body_part": "Knee",
                                "return_date": "2025-04-15",
                                "reported_at": "2025-03-24T14:00:00Z",
                            },
                            {
                                "player_id": "4395725",
                                "sport": "nba",
                                "player_name": "Paolo Banchero",
                                "team_id": "19",
                                "status": "day_to_day",
                                "description": "Left oblique strain",
                                "body_part": "Oblique",
                                "reported_at": "2025-03-25T10:00:00Z",
                            },
                        ],
                        "meta": {"sport": "nba", "count": 2, "total": 14, "limit": 100, "offset": 0, "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def list_injuries(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    limit: int = Query(100, ge=1, le=1000, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "injuries")
    await rate_limit_check(api_key)
    injuries = ds.get_injuries(sport)
    return _paginated_response(injuries, sport, limit=limit, offset=offset, api_key=api_key, response=response)


# ── News ──────────────────────────────────────────────────

@router.get(
    "/news",
    summary="Latest news articles",
    description="Latest news articles for a sport, sorted by publication date. Includes headline, summary, author, and links to related teams/players.",
    tags=["News"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Paginated list of news articles",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "id": "41085432",
                                "sport": "nba",
                                "headline": "Cavaliers clinch No. 1 seed with dominant win over Magic",
                                "summary": "Cleveland's 118-105 victory secures home-court advantage throughout the Eastern Conference playoffs.",
                                "url": "https://www.espn.com/nba/story/_/id/41085432",
                                "author": "Brian Windhorst",
                                "published_at": "2025-03-25T23:15:00Z",
                                "related_team": "Cleveland Cavaliers",
                                "tags": ["NBA", "Playoffs", "Cavaliers"],
                            },
                            {
                                "id": "41085490",
                                "sport": "nba",
                                "headline": "Celtics extend winning streak to 8 games with narrow Bucks victory",
                                "summary": "Jayson Tatum's 38 points lead Boston past Milwaukee 112-108 at TD Garden.",
                                "url": "https://www.espn.com/nba/story/_/id/41085490",
                                "author": "Tim Bontemps",
                                "published_at": "2025-03-25T22:45:00Z",
                                "related_team": "Boston Celtics",
                                "tags": ["NBA", "Celtics", "Bucks"],
                            },
                        ],
                        "meta": {"sport": "nba", "count": 2, "total": 50, "limit": 20, "offset": 0, "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def list_news(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    limit: int = Query(20, ge=1, le=100, description="Number of articles to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "news")
    await rate_limit_check(api_key)
    news = ds.get_news(sport, limit=limit + offset)
    return _paginated_response(news, sport, limit=limit, offset=offset, api_key=api_key, response=response)


# ── Advanced Stats ────────────────────────────────────────

@router.get("/advanced-stats", include_in_schema=False, deprecated=True)
@router.get(
    "/advanced_stats",
    summary="Sport-specific advanced statistics",
    description=(
        "Return sport-specific advanced stats. For MLB: ISO, BABIP, BB%, K%, wOBA, wRC+. "
        "For NBA: PER, TS%, usage%, assist ratio, rebound rate. "
        "For soccer: returns empty — use /match-events instead."
    ),
    tags=["Advanced"],
    responses={
        200: {
            "description": "Paginated advanced statistics",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "player_id": "ohtMDL01",
                                "season": "2025",
                                "plate_appearances": 636,
                                "iso": 0.312,
                                "babip": 0.310,
                                "bb_pct": 0.098,
                                "k_pct": 0.237,
                                "woba": 0.411,
                                "wrc_plus": 174,
                            }
                        ],
                        "meta": {"sport": "mlb", "count": 1, "total": 450},
                    }
                }
            },
        }
    },
)
async def list_advanced_stats(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    player_id: Optional[str] = Query(None, description="Filter by player ID"),
    team_id: Optional[str] = Query(None, description="Filter by team ID"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Sport-specific advanced statistics (MLB batting, NBA advanced, etc.)."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "advanced-stats")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)
    kind = "advanced_batting" if sport == "mlb" else "advanced_stats"

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, kind, season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if player_id and "player_id" in df.columns:
            df = df[df["player_id"].astype(str) == player_id]
        if team_id and "team_id" in df.columns:
            df = df[df["team_id"].astype(str) == team_id]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Match Events ──────────────────────────────────────────

@router.get("/match-events", include_in_schema=False, deprecated=True)
@router.get(
    "/match_events",
    summary="Event-level match data",
    description=(
        "Goals, assists, cards, substitutions with timestamps. "
        "Primarily for soccer sports but returns data for any sport with event-level records."
    ),
    tags=["Advanced"],
    responses={
        200: {
            "description": "Paginated match events",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "match_id": "3869685",
                                "event_type": "goal",
                                "minute": 23,
                                "player_name": "Mohamed Salah",
                                "team_name": "Liverpool",
                            }
                        ],
                        "meta": {"sport": "epl", "count": 1, "total": 1200},
                    }
                }
            },
        }
    },
)
async def list_match_events(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    match_id: Optional[str] = Query(None, description="Filter by match ID"),
    event_type: Optional[str] = Query(None, description="Filter by event type (goal, assist, yellow_card, red_card, substitution)"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Event-level data for matches — goals, cards, substitutions."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "match-events")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "match_events", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if match_id and "match_id" in df.columns:
            df = df[df["match_id"].astype(str) == match_id]
        if event_type and "event_type" in df.columns:
            df = df[df["event_type"] == event_type]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get("/play-by-play", include_in_schema=False, deprecated=True)
@router.get(
    "/play_by_play",
    summary="Unified play-by-play feed",
    description=(
        "Unified per-play feed across available providers (CFBData plays and ESPN event summaries)."
    ),
    tags=["Advanced"],
)
async def list_play_by_play(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "match-events")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "play_by_play", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        if event_type and "event_type" in df.columns:
            df = df[df["event_type"].astype(str).str.lower() == event_type.lower()]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/coaches",
    summary="Coaches records",
    description="Normalized coaches records for sports/providers that expose coach metadata.",
    tags=["Advanced"],
)
async def list_coaches(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    team: Optional[str] = Query(None, description="Filter by team name/id"),
    coach_name: Optional[str] = Query(None, description="Filter by coach name (substring)"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "coaches", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if team:
            tl = team.lower()
            if "team_name" in df.columns:
                df = df[df["team_name"].astype(str).str.lower().str.contains(tl, na=False)]
            elif "team_id" in df.columns:
                df = df[df["team_id"].astype(str).str.lower().str.contains(tl, na=False)]
        if coach_name and "coach_name" in df.columns:
            df = df[df["coach_name"].astype(str).str.contains(coach_name, case=False, na=False)]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/draft",
    summary="Draft records",
    description="Normalized draft picks/rankings where provider draft datasets exist.",
    tags=["Advanced"],
)
async def list_draft(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    draft_year: Optional[str] = Query(None, description="Filter by draft year"),
    team: Optional[str] = Query(None, description="Filter by team name/id"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "draft", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if draft_year and "draft_year" in df.columns:
            df = df[df["draft_year"].astype(str) == str(draft_year)]
        if team:
            tl = team.lower()
            if "team_name" in df.columns:
                df = df[df["team_name"].astype(str).str.lower().str.contains(tl, na=False)]
            elif "team_id" in df.columns:
                df = df[df["team_id"].astype(str).str.lower().str.contains(tl, na=False)]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/draft/picks",
    summary="Draft picks records",
    description="Normalized draft picks with cross-league IDs when available.",
    tags=["Advanced"],
)
async def list_draft_picks(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    draft_year: Optional[str] = Query(None, description="Filter by draft year"),
    team: Optional[str] = Query(None, description="Filter by team name/id"),
    player: Optional[str] = Query(None, description="Filter by player name"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "draft_picks", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if draft_year and "draft_year" in df.columns:
            df = df[df["draft_year"].astype(str) == str(draft_year)]
        if team:
            tl = team.lower()
            if "team_name" in df.columns:
                df = df[df["team_name"].astype(str).str.lower().str.contains(tl, na=False)]
            elif "team_id" in df.columns:
                df = df[df["team_id"].astype(str).str.lower().str.contains(tl, na=False)]
        if player and "player_name" in df.columns:
            df = df[df["player_name"].astype(str).str.contains(player, case=False, na=False)]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/draft/positions",
    summary="Draft positions records",
    description="Normalized draft position metadata.",
    tags=["Advanced"],
)
async def list_draft_positions(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    draft_year: Optional[str] = Query(None, description="Filter by draft year"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "draft_positions", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if draft_year and "draft_year" in df.columns:
            df = df[df["draft_year"].astype(str) == str(draft_year)]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/draft/teams",
    summary="Draft teams records",
    description="Normalized draft team metadata.",
    tags=["Advanced"],
)
async def list_draft_teams(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    draft_year: Optional[str] = Query(None, description="Filter by draft year"),
    team: Optional[str] = Query(None, description="Filter by team name/id"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "draft_teams", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if draft_year and "draft_year" in df.columns:
            df = df[df["draft_year"].astype(str) == str(draft_year)]
        if team:
            tl = team.lower()
            if "team_name" in df.columns:
                df = df[df["team_name"].astype(str).str.lower().str.contains(tl, na=False)]
            elif "team_id" in df.columns:
                df = df[df["team_id"].astype(str).str.lower().str.contains(tl, na=False)]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/drives",
    summary="Drive-level records",
    description="Normalized drive-level datasets for sports/providers that expose drives.",
    tags=["Advanced"],
)
async def list_drives(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game identifier"),
    team: Optional[str] = Query(None, description="Filter by offense or defense team"),
    scoring_only: bool = Query(False, description="Only scoring drives"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "drives", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == str(game_id)]
        if team:
            tl = team.lower()
            keep = None
            for col in ("offense_team_name", "defense_team_name", "offense_team_id", "defense_team_id"):
                if col in df.columns:
                    col_mask = df[col].astype(str).str.lower().str.contains(tl, na=False)
                    keep = col_mask if keep is None else (keep | col_mask)
            if keep is not None:
                df = df[keep]
            else:
                df = df.iloc[0:0]
        if scoring_only and "scoring" in df.columns:
            df = df[df["scoring"].astype(bool)]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/player_categories/portal",
    include_in_schema=False,
    deprecated=True,
)
@router.get(
    "/players/categories/portal",
    summary="Player portal records",
    description="Normalized player transfer-portal style records.",
    tags=["Advanced"],
)
async def list_player_portal(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    team: Optional[str] = Query(None, description="Filter by origin or destination team"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "player_portal", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if team:
            tl = team.lower()
            for col in ("origin_team", "destination_team", "team_name", "team_id"):
                if col in df.columns:
                    df = df[df[col].astype(str).str.lower().str.contains(tl, na=False)]
                    break
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/player_categories/returning",
    include_in_schema=False,
    deprecated=True,
)
@router.get(
    "/players/categories/returning",
    summary="Returning production records",
    description="Normalized returning-production style team/player category records.",
    tags=["Advanced"],
)
async def list_player_returning(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    team: Optional[str] = Query(None, description="Filter by team"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "player_returning", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if team:
            tl = team.lower()
            for col in ("team_name", "team_id"):
                if col in df.columns:
                    df = df[df[col].astype(str).str.lower().str.contains(tl, na=False)]
                    break
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/player_categories/usage",
    include_in_schema=False,
    deprecated=True,
)
@router.get(
    "/players/categories/usage",
    summary="Player usage category records",
    description="Normalized player usage category records from available providers.",
    tags=["Advanced"],
)
async def list_player_usage(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    player_id: Optional[str] = Query(None, description="Filter by player ID"),
    team: Optional[str] = Query(None, description="Filter by team"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "player_usage", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if player_id and "player_id" in df.columns:
            df = df[df["player_id"].astype(str) == player_id]
        if team:
            tl = team.lower()
            for col in ("team_name", "team_id"):
                if col in df.columns:
                    df = df[df[col].astype(str).str.lower().str.contains(tl, na=False)]
                    break
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Ratings ───────────────────────────────────────────────

@router.get(
    "/ratings",
    summary="Advanced ratings",
    description=(
        "Advanced team/player rating records when available. "
        "Filterable by rating type and team/player."
    ),
    tags=["Advanced"],
    responses={
        200: {
            "description": "Paginated ratings",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "rating_type": "elo",
                                "team_or_player": "Golden State Warriors",
                                "value": 1612.5,
                                "date": "2024-01-15",
                                "season": "2025",
                            }
                        ],
                        "meta": {"sport": "nba", "count": 1, "total": 500},
                    }
                }
            },
        }
    },
)
async def list_ratings(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    rating_type: Optional[str] = Query(None, description="Filter by rating type (elo, spi, raptor)"),
    team_or_player: Optional[str] = Query(None, description="Search by team or player name (substring match)"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Advanced team/player rating records."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "ratings")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "ratings", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if rating_type and "rating_type" in df.columns:
            df = df[df["rating_type"] == rating_type]
        if team_or_player and "team_or_player" in df.columns:
            df = df[df["team_or_player"].str.contains(team_or_player, case=False, na=False)]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get("/market-signals", include_in_schema=False, deprecated=True)
@router.get(
    "/market_signals",
    summary="Market line movement signals",
    description=(
        "Bookmaker-level line movement enrichment derived from odds snapshots. "
        "Returns open/close deltas, movement ranges, and market regime labels."
    ),
    tags=["Advanced"],
)
async def list_market_signals(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    bookmaker: Optional[str] = Query(None, description="Filter by bookmaker"),
    regime: Optional[str] = Query(None, description="Filter by market regime (stable, moving, volatile)"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "market-signals")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "market_signals", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        if bookmaker and "bookmaker" in df.columns:
            df = df[df["bookmaker"].astype(str).str.lower() == bookmaker.lower()]
        if regime and "market_regime" in df.columns:
            df = df[df["market_regime"].astype(str).str.lower() == regime.lower()]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get("/sharp-signals", include_in_schema=False, deprecated=True)
@router.get(
    "/sharp_signals",
    summary="Sharp money & public betting split signals",
    description=(
        "Combines ActionNetwork public/sharp split percentages with OddsAPI line movement "
        "to expose consensus betting direction. Returns bet%, handle%, and line drift per game."
    ),
    tags=["Advanced"],
)
async def list_sharp_signals(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    min_handle_pct: Optional[float] = Query(None, ge=0, le=100, description="Min handle % on favorite"),
    limit: int = Query(50, ge=1, le=500, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "market-signals")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        import pandas as pd

        svc = _gds()

        # Load ActionNetwork trends (public/sharp split)
        an_df = svc._load_kind(sport, "action_network_trends", season=effective_season)
        if an_df.empty:
            an_df = svc._load_kind(sport, "trends", season=effective_season)

        # Load market signals (line movement deltas)
        ms_df = svc._load_kind(sport, "market_signals", season=effective_season)

        # Load odds history for line drift
        oh_df = svc._load_kind(sport, "odds_history", season=effective_season)

        results = []

        # Prefer ActionNetwork if available
        if not an_df.empty:
            df = an_df.copy()
            if game_id and "game_id" in df.columns:
                df = df[df["game_id"].astype(str) == game_id]
            if date and "date" in df.columns:
                df = df[df["date"].astype(str).str.startswith(date)]
            if min_handle_pct is not None and "home_handle_pct" in df.columns:
                df = df[df["home_handle_pct"] >= min_handle_pct]
            results = svc._df_to_records(df)

        # Enrich with line movement context from market_signals
        if results and not ms_df.empty and "game_id" in ms_df.columns:
            ms_index: dict[str, dict] = {}
            for row in svc._df_to_records(ms_df):
                gid = str(row.get("game_id", ""))
                if gid and gid not in ms_index:
                    ms_index[gid] = row
            for rec in results:
                gid = str(rec.get("game_id", ""))
                if gid in ms_index:
                    ms = ms_index[gid]
                    rec["spread_open"] = ms.get("spread_open")
                    rec["spread_close"] = ms.get("spread_close")
                    rec["spread_drift"] = ms.get("spread_delta") or ms.get("spread_drift")
                    rec["market_regime"] = ms.get("market_regime")

        # If no ActionNetwork data, fall back to market_signals only
        if not results and not ms_df.empty:
            df = ms_df.copy()
            if game_id and "game_id" in df.columns:
                df = df[df["game_id"].astype(str) == game_id]
            if date and "date" in df.columns:
                df = df[df["date"].astype(str).str.startswith(date)]
            results = svc._df_to_records(df)

    except Exception:
        results = []

    return _paginated_response(results, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get("/schedule-fatigue", include_in_schema=False, deprecated=True)
@router.get(
    "/schedule_fatigue",
    summary="Team schedule fatigue signals",
    description=(
        "Team-level rest and congestion enrichment keyed by game/team. "
        "Includes back-to-back flags, rolling game density, and fatigue score."
    ),
    tags=["Advanced"],
)
async def list_schedule_fatigue(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    team_id: Optional[str] = Query(None, description="Filter by team ID"),
    fatigue_level: Optional[str] = Query(None, description="Filter by fatigue level (low, medium, high)"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "schedule-fatigue")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "schedule_fatigue", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        if team_id and "team_id" in df.columns:
            df = df[df["team_id"].astype(str) == team_id]
        if fatigue_level and "fatigue_level" in df.columns:
            df = df[df["fatigue_level"].astype(str).str.lower() == fatigue_level.lower()]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/odds-history",
    include_in_schema=False,
    deprecated=True,
)
@router.get(
    "/odds_history",
    summary="Historical odds snapshots",
    description="Historical bookmaker odds snapshots for trend and movement analysis.",
    tags=["Odds"],
)
async def list_odds_history(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    bookmaker: Optional[str] = Query(None, description="Filter by bookmaker"),
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "odds")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "odds_history", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        if bookmaker and "bookmaker" in df.columns:
            df = df[df["bookmaker"].astype(str).str.lower() == bookmaker.lower()]
        if date:
            if "date" in df.columns:
                df = df[df["date"].astype(str).str.startswith(date)]
            elif "timestamp" in df.columns:
                df = df[df["timestamp"].astype(str).str.startswith(date)]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/player-props-history",
    include_in_schema=False,
    deprecated=True,
)
@router.get(
    "/player_props_history",
    summary="Historical player-props snapshots",
    description="Historical player prop snapshots for line movement and model analysis.",
    tags=["Odds"],
)
async def list_player_props_history(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    player_id: Optional[str] = Query(None, description="Filter by player ID"),
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "odds")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "player_props_history", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        if player_id and "player_id" in df.columns:
            df = df[df["player_id"].astype(str) == player_id]
        if market and "market" in df.columns:
            df = df[df["market"].astype(str).str.lower() == market.lower()]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/market-history",
    include_in_schema=False,
    deprecated=True,
)
@router.get(
    "/market_history",
    summary="Unified market history",
    description="Unified market-level history across odds, props, and market signals.",
    tags=["Advanced"],
)
async def list_market_history(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    market_scope: Optional[str] = Query(None, description="Filter by market scope"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "market-signals")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "market_history", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if market_scope and "market_scope" in df.columns:
            df = df[df["market_scope"].astype(str).str.lower() == market_scope.lower()]
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get(
    "/all-stats",
    include_in_schema=False,
    deprecated=True,
)
@router.get(
    "/all_stats",
    summary="Unified stats feed",
    description="Unified stats feed across general/team/advanced stats categories.",
    tags=["stats"],
)
async def list_all_stats(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    stats_category: Optional[str] = Query(None, description="Filter by stats category"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    await check_tier_access(api_key, sport, "team-stats")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "all_stats", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if stats_category and "stats_category" in df.columns:
            df = df[df["stats_category"].astype(str).str.lower() == stats_category.lower()]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Team Stats ────────────────────────────────────────────────
@router.get("/team-stats", include_in_schema=False, deprecated=True)
@router.get(
    "/team_stats",
    summary="Team season stats",
    description="Aggregated team statistics for the season: points, rebounds, FG%, 3P%, etc.",
    tags=["stats"],
)
async def list_team_stats(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    team: Optional[str] = Query(None, description="Filter by team name (substring)"),
    sort: Optional[str] = Query(None, description="Sort field, prefix with - for desc (e.g. -avg_points)"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Per-team aggregated stats for the season."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "team-stats")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "team_stats", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if team:
            for col in ("team_name", "team"):
                if col in df.columns:
                    df = df[df[col].str.contains(team, case=False, na=False)]
                    break
        if sort:
            desc = sort.startswith("-")
            col = sort.lstrip("-")
            if col in df.columns:
                df = df.sort_values(col, ascending=not desc, na_position="last")
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get("/team-game-stats", include_in_schema=False, deprecated=True)
@router.get(
    "/team_game_stats",
    summary="Per-team game stats",
    description="Game-level team stat lines (MLB-focused).",
    tags=["stats"],
)
async def list_team_game_stats(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    team_id: Optional[str] = Query(None, description="Filter by team ID"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "stats")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "team_game_stats", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        if team_id and "team_id" in df.columns:
            df = df[df["team_id"].astype(str) == team_id]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get("/batter-game-stats", include_in_schema=False, deprecated=True)
@router.get(
    "/batter_game_stats",
    summary="Per-batter game stats",
    description="Game-level batter stat lines (MLB-focused).",
    tags=["stats"],
)
async def list_batter_game_stats(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    player_id: Optional[str] = Query(None, description="Filter by player ID"),
    team_id: Optional[str] = Query(None, description="Filter by team ID"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "stats")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "batter_game_stats", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        if player_id and "player_id" in df.columns:
            df = df[df["player_id"].astype(str) == player_id]
        if team_id and "team_id" in df.columns:
            df = df[df["team_id"].astype(str) == team_id]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


@router.get("/pitcher-game-stats", include_in_schema=False, deprecated=True)
@router.get(
    "/pitcher_game_stats",
    summary="Per-pitcher game stats",
    description="Game-level pitcher stat lines (MLB-focused).",
    tags=["stats"],
)
async def list_pitcher_game_stats(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    game_id: Optional[str] = Query(None, description="Filter by game ID"),
    player_id: Optional[str] = Query(None, description="Filter by player ID"),
    team_id: Optional[str] = Query(None, description="Filter by team ID"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "stats")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "pitcher_game_stats", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if game_id and "game_id" in df.columns:
            df = df[df["game_id"].astype(str) == game_id]
        if player_id and "player_id" in df.columns:
            df = df[df["player_id"].astype(str) == player_id]
        if team_id and "team_id" in df.columns:
            df = df[df["team_id"].astype(str) == team_id]
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Depth Charts ───────────────────────────────────────────────
@router.get(
    "/depth_charts",
    summary="Team depth charts",
    description="Current depth chart positions for teams, grouped by position.",
    tags=["roster"],
)
async def list_depth_charts(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    team_id: Optional[str] = Query(None, description="Filter by team ID"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Depth chart positions from ESPN — ordered starters through reserves by position."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "depth_charts")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    import glob as _glob, json as _json, os as _os

    raw_dir = _os.path.join("data", "raw", "espn", sport, str(effective_season), "depth_charts")
    records: list[dict] = []

    if _os.path.isdir(raw_dir):
        files = sorted(_glob.glob(_os.path.join(raw_dir, "*.json")))
        for fpath in files:
            try:
                with open(fpath) as f:
                    raw = _json.load(f)
                t_id = str(raw.get("teamId", ""))
                if team_id and t_id != str(team_id):
                    continue
                depth = raw.get("depthCharts", {})
                team_info = depth.get("team", {})
                for chart in (depth.get("depthchart") or []):
                    positions = chart.get("positions") or {}
                    if isinstance(positions, dict):
                        for pos_key, pos_data in positions.items():
                            pos_name = pos_data.get("position", {}).get("displayName", pos_key.upper())
                            for rank, athlete in enumerate((pos_data.get("athletes") or []), start=1):
                                records.append({
                                    "team_id": t_id,
                                    "team_name": team_info.get("displayName", ""),
                                    "team_abbr": team_info.get("abbreviation", ""),
                                    "position": pos_name,
                                    "position_key": pos_key,
                                    "rank": rank,
                                    "athlete_id": str(athlete.get("id", "")),
                                    "athlete_name": (
                                        athlete.get("athlete", {}).get("displayName", "")
                                        or athlete.get("displayName", "")
                                    ),
                                    "season": effective_season,
                                })
            except Exception:
                continue

    # Fall back to normalized data_service if no raw ESPN files
    if not records:
        try:
            svc = ds
            df = svc._load_kind(sport, "depth_charts", season=effective_season)
            if not df.empty:
                if team_id and "team_id" in df.columns:
                    df = df[df["team_id"].astype(str) == str(team_id)]
                records = svc._df_to_records(df)
        except Exception:
            records = []

    return _paginated_response(records, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Rankings ───────────────────────────────────────────────────
@router.get(
    "/rankings",
    summary="Power rankings / polls",
    description="AP Poll, Coaches Poll, and power rankings for college and pro sports.",
    tags=["meta"],
)
async def list_rankings(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    week: Optional[str] = Query(None, description="Week number or 'current'"),
    limit: int = Query(50, ge=1, le=500, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """AP Poll, Coaches Poll, and power rankings. Primarily for college sports."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "rankings")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        svc = ds
        df = svc._load_kind(sport, "rankings", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if week and week.lower() != "current" and "week" in df.columns:
            df = df[df["week"].astype(str) == str(week)]
        elif week and week.lower() == "current" and "week" in df.columns:
            max_week = df["week"].max()
            df = df[df["week"] == max_week]
        if "week" in df.columns:
            df = df.sort_values(["week", "rank"], ascending=[False, True], na_position="last")
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Futures / Outrights ────────────────────────────────────────
@router.get(
    "/futures",
    summary="Futures and outright odds",
    description="Championship, award, and long-term betting markets (e.g. Super Bowl winner, MVP).",
    tags=["odds"],
)
async def list_futures(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    market: Optional[str] = Query(None, description="Market type filter (e.g. 'championship', 'mvp')"),
    limit: int = Query(50, ge=1, le=500, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Futures and outright odds from ESPN and OddsAPI."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "odds")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        svc = ds
        df = svc._load_kind(sport, "futures", season=effective_season)
        if df.empty:
            # Try ESPN futures data
            df = svc._load_kind(sport, "odds_futures", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if market and "market" in df.columns:
            df = df[df["market"].str.contains(market, case=False, na=False)]
        if "odds" in df.columns:
            df = df.sort_values("odds", ascending=True, na_position="last")
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Transactions ──────────────────────────────────────────────
@router.get(
    "/transactions",
    summary="Team transactions",
    description="Trades, signings, waivers, and other roster moves.",
    tags=["roster"],
)
async def list_transactions(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    team: Optional[str] = Query(None, description="Filter by team name (substring)"),
    date_start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Roster transactions: trades, signings, waivers, etc."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "transactions")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "transactions", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)
        if team:
            for col in ("team_name", "team"):
                if col in df.columns:
                    df = df[df[col].str.contains(team, case=False, na=False)]
                    break
        if date_start and "date" in df.columns:
            df = df[df["date"] >= date_start]
        if date_end and "date" in df.columns:
            df = df[df["date"] <= date_end]
        if "date" in df.columns:
            df = df.sort_values("date", ascending=False, na_position="last")
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Schedule (Upcoming) ──────────────────────────────────────
@router.get(
    "/schedule",
    summary="Upcoming schedule",
    description="Games scheduled for coming days. Uses games with 'scheduled' or future status.",
    tags=["games"],
)
async def list_schedule(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    season: Optional[str] = Query(None, description="Season year"),
    team: Optional[str] = Query(None, description="Filter by team name (substring)"),
    days: int = Query(7, ge=1, le=30, description="Number of days ahead to include"),
    limit: int = Query(50, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Upcoming scheduled games for the next N days."""
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "games")
    await rate_limit_check(api_key)
    effective_season = _resolve_season(sport, season)
    from datetime import datetime, timedelta

    try:
        from services.data_service import get_data_service as _gds
        svc = _gds()
        df = svc._load_kind(sport, "games", season=effective_season)
        if df.empty:
            return _paginated_response([], sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)

        today = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")

        if "date" in df.columns:
            df["_date_str"] = df["date"].astype(str).str[:10]
            df = df[(df["_date_str"] >= today) & (df["_date_str"] <= future)]
            df = df.drop(columns=["_date_str"])

        if team:
            for col in ("home_team", "away_team"):
                if col in df.columns:
                    mask = df[col].str.contains(team, case=False, na=False)
                    df = df[mask] if not df[mask].empty else df
                    break

        if "date" in df.columns:
            df = df.sort_values("date", ascending=True)
        data = svc._df_to_records(df)
    except Exception:
        data = []

    return _paginated_response(data, sport, limit=limit, offset=offset, season=season, api_key=api_key, response=response)


# ── Simulation Results ────────────────────────────────────

_SIM_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "simulations"


@router.get("/simulation")
async def get_simulation(
    sport: ValidSport,
    api_key: ApiKey,
    response: Response,
) -> dict[str, Any]:
    """Return the latest season simulation for a sport.

    Monte Carlo projections including championship odds, playoff
    probabilities, award predictions, draft lottery odds, and more.
    """
    _require_internal_access(api_key)
    await check_tier_access(api_key, sport, "simulation")
    await rate_limit_check(api_key)

    latest = _SIM_DIR / "latest" / f"{sport}.json"
    if not latest.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No simulation results available for {sport}. "
                   "Run: python scripts/season_simulator.py --sport " + sport,
        )

    try:
        data = json.loads(latest.read_text())
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read simulation file: {exc}",
        )

    response.headers["X-User-Tier"] = api_key.tier
    return {
        "success": True,
        "data": data,
        "meta": {
            "sport": sport,
            "generated_at": data.get("generated_at"),
            "simulations": data.get("simulations"),
            "season": data.get("season"),
        },
    }


# ── Live Predictions ──────────────────────────────────────

_LIVE_PRED_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "live_predictions"


@router.get("/live-predictions", include_in_schema=False, deprecated=True)
@router.get(
    "/live_predictions",
    summary="Live-adjusted predictions for in-progress games",
    description=(
        "Returns real-time adjusted win probabilities, projected final scores, "
        "and momentum indicators for games currently in progress. Data is generated "
        "by the live_model daemon which polls every 60 seconds."
    ),
    tags=["Predictions"],
    responses={
        200: {
            "description": "Live prediction data",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "updated_at": "2026-03-26T20:30:00Z",
                            "games": [
                                {
                                    "game_id": "401810919",
                                    "home_team": "Lakers",
                                    "away_team": "Celtics",
                                    "home_score": 55,
                                    "away_score": 48,
                                    "period": "3rd Quarter",
                                    "time_remaining": "8:42",
                                    "pre_game_home_wp": 0.52,
                                    "live_home_wp": 0.68,
                                    "live_away_wp": 0.32,
                                    "predicted_final_home": 108,
                                    "predicted_final_away": 99,
                                    "momentum": "home",
                                    "momentum_score": 0.15,
                                    "key_factors": ["Home team leads by 7"],
                                }
                            ],
                        },
                        "meta": {"sport": "nba"},
                    }
                }
            },
        }
    },
)
async def live_predictions(
    sport: ValidSport,
    api_key: ApiKey,
    response: Response,
) -> dict[str, Any]:
    """Return current live-adjusted predictions for in-progress games."""
    await rate_limit_check(api_key)

    live_file = _LIVE_PRED_DIR / f"{sport}_live.json"
    if not live_file.exists():
        response.headers["X-User-Tier"] = api_key.tier
        return {
            "success": True,
            "data": {"updated_at": None, "games": []},
            "meta": {"sport": sport},
        }

    try:
        data = json.loads(live_file.read_text())
    except Exception:
        data = {"updated_at": None, "games": []}

    response.headers["X-User-Tier"] = api_key.tier
    return {
        "success": True,
        "data": data,
        "meta": {"sport": sport, "updated_at": data.get("updated_at")},
    }


# ── Injuries with Impact ──────────────────────────────────

@router.get("/injuries-impact", include_in_schema=False, deprecated=True)
@router.get(
    "/injuries_impact",
    summary="Active injury reports",
    description="Current injury reports for a sport. Includes player status, body part, expected return date, and estimated impact on game outcomes.",
    tags=["Injuries"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Active injury list with impact estimates",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "sport": "nba",
                                "player_id": "2544",
                                "player_name": "LeBron James",
                                "team_id": "5",
                                "team_name": "Cleveland Cavaliers",
                                "position": "SF",
                                "status": "out",
                                "body_part": "Ankle",
                                "reported_date": "2025-03-20",
                                "expected_return_date": "2025-04-01",
                                "days_out": 12,
                                "confidence": 0.85,
                                "impact_analysis": {
                                    "minutes_per_game_avg": 32.5,
                                    "estimated_minutes_replacement": 15.2,
                                    "win_probability_delta": -0.04,
                                    "spread_impact_points": -1.8,
                                    "total_impact_points": -2.1,
                                    "severity": "high",
                                    "recommendation": "Consider fading games without this player",
                                },
                                "source": "nba_official",
                            },
                            {
                                "sport": "nba",
                                "player_id": "203999",
                                "player_name": "Kevin Durant",
                                "team_id": "1",
                                "team_name": "Boston Celtics",
                                "position": "SF",
                                "status": "questionable",
                                "body_part": "Hamstring",
                                "reported_date": "2025-03-24",
                                "expected_return_date": "2025-03-26",
                                "days_out": 2,
                                "confidence": 0.65,
                                "impact_analysis": {
                                    "minutes_per_game_avg": 28.1,
                                    "estimated_minutes_replacement": 12.3,
                                    "win_probability_delta": -0.02,
                                    "spread_impact_points": -1.2,
                                    "total_impact_points": -1.4,
                                    "severity": "medium",
                                    "recommendation": "Monitor status updates until game time",
                                },
                                "source": "nba_official",
                            },
                        ],
                        "meta": {
                            "sport": "nba",
                            "total_injuries": 24,
                            "critical_count": 3,
                            "high_count": 8,
                            "medium_count": 10,
                            "low_count": 3,
                            "last_updated": "2025-03-25T15:00:00Z",
                            "cached_at": "2025-03-25T20:00:00Z",
                        },
                    }
                }
            },
        }
    },
)
async def get_injuries(
    sport: ValidSport,
    ds: DS,
    api_key: ApiKey,
    response: Response,
    team_id: Optional[str] = Query(None, description="Filter by team ID"),
    severity: Optional[str] = Query(None, description="Filter by severity (critical, high, medium, low)"),
    limit: int = Query(50, ge=1, le=500, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Get active injuries with impact analysis on game predictions."""
    await check_tier_access(api_key, sport, "injuries")
    await rate_limit_check(api_key)
    
    injuries = ds.get_injuries(sport)
    
    if team_id:
        injuries = [inj for inj in injuries if str(inj.get("team_id")) == team_id]
    
    if severity:
        injuries = [inj for inj in injuries if inj.get("impact_analysis", {}).get("severity") == severity.lower()]
    
    return _paginated_response(
        injuries, sport, limit=limit, offset=offset,
        extra_meta={
            "total_injuries": len(injuries),
            "critical_count": sum(1 for i in injuries if i.get("impact_analysis", {}).get("severity") == "critical"),
            "high_count": sum(1 for i in injuries if i.get("impact_analysis", {}).get("severity") == "high"),
        },
        api_key=api_key, response=response,
    )


# ── Weather endpoint ─────────────────────────────────────────────────────────

_VENUE_COORDS: dict[str, tuple[float, float]] = {
    # MLB
    "fenway park": (42.3467, -71.0972),
    "wrigley field": (41.9484, -87.6553),
    "yankee stadium": (40.8296, -73.9262),
    "dodger stadium": (34.0739, -118.2400),
    "oracle park": (37.7786, -122.3893),
    "petco park": (32.7076, -117.1570),
    "tropicana field": (27.7683, -82.6534),
    # NFL
    "lambeau field": (44.5013, -88.0622),
    "soldier field": (41.8623, -87.6167),
    "arrowhead stadium": (39.0489, -94.4839),
    "gillette stadium": (42.0909, -71.2643),
    "metlife stadium": (40.8135, -74.0745),
    "m&t bank stadium": (39.2780, -76.6227),
    "bank of america stadium": (35.2258, -80.8527),
    "levi's stadium": (37.4033, -121.9694),
    "sofi stadium": (33.9535, -118.3392),
    "empower field": (39.7439, -105.0200),
    # Soccer/outdoor
    "wembley stadium": (51.5560, -0.2796),
    "old trafford": (53.4631, -2.2913),
    "camp nou": (41.3809, 2.1228),
    "anfield": (53.4308, -2.9608),
    # F1 circuits
    "bahrain international circuit": (26.0325, 50.5106),
    "jeddah corniche circuit": (21.6319, 39.1044),
    "albert park circuit": (-37.8497, 144.9680),
    "circuit de monaco": (43.7338, 7.4215),
    "circuit de spa-francorchamps": (50.4372, 5.9714),
    "silverstone circuit": (52.0786, -1.0169),
    "autodromo nazionale monza": (45.6156, 9.2811),
    "circuit of the americas": (30.1328, -97.6411),
    "autodromo jose carlos pace": (-23.7036, -46.6997),
    "yas marina circuit": (24.4672, 54.6031),
    "suzuka circuit": (34.8431, 136.5407),
    "marina bay street circuit": (1.2914, 103.8639),
}


async def _fetch_open_meteo(lat: float, lon: float) -> dict:
    """Fetch current weather from Open-Meteo (free, no API key)."""
    import httpx
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&current=temperature_2m,wind_speed_10m,wind_direction_10m,"
        f"precipitation_probability,weather_code,relative_humidity_2m"
        f"&temperature_unit=fahrenheit&wind_speed_unit=mph&timezone=auto"
    )
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            curr = data.get("current", {})
            wmo = curr.get("weather_code", 0)
            # WMO weather interpretation
            if wmo == 0:
                condition = "Clear"
            elif wmo in range(1, 4):
                condition = "Partly Cloudy"
            elif wmo in range(4, 50):
                condition = "Cloudy / Fog"
            elif wmo in range(51, 70):
                condition = "Drizzle / Light Rain"
            elif wmo in range(71, 80):
                condition = "Snow"
            elif wmo in range(80, 100):
                condition = "Rain / Storms"
            else:
                condition = "Unknown"
            return {
                "temp_f": curr.get("temperature_2m"),
                "wind_mph": curr.get("wind_speed_10m"),
                "wind_direction_deg": curr.get("wind_direction_10m"),
                "humidity_pct": curr.get("relative_humidity_2m"),
                "precipitation_pct": curr.get("precipitation_probability"),
                "condition": condition,
                "wmo_code": wmo,
                "source": "open-meteo",
            }
    except Exception:
        return {}


def _deg_to_compass(deg: float | None) -> str:
    if deg is None:
        return ""
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return dirs[round(deg / 22.5) % 16]


def _find_venue_coords(venue_name: str) -> tuple[float, float] | None:
    v = venue_name.lower()
    for key, coords in _VENUE_COORDS.items():
        if key in v or v in key:
            return coords
    return None


@router.get(
    "/games/{game_id}/weather",
    summary="Weather data for a game venue",
    tags=["games"],
)
async def get_game_weather(
    sport: ValidSport,
    game_id: str,
    ds: DS,
    api_key: ApiKey,
    response: Response,
):
    """Return current/forecast weather for a game's venue.

    Checks the normalized weather parquet first; falls back to
    a live Open-Meteo fetch using a known-coordinates lookup.
    """
    await rate_limit_check(api_key)
    response.headers["Cache-Control"] = "public, max-age=900"

    # 1. Try normalized weather parquet
    season = get_current_season(sport)
    weather_records = ds.get_weather(sport, season, game_id)
    if weather_records:
        w = weather_records[0]
        dome = bool(w.get("dome"))
        wind_dir = str(w.get("wind_direction") or "")
        result = {
            "game_id": game_id,
            "sport": sport,
            "dome": dome,
            "temp_f": w.get("temp_f"),
            "wind_mph": None if dome else w.get("wind_mph"),
            "wind_direction": None if dome else wind_dir,
            "wind_direction_deg": None,
            "humidity_pct": w.get("humidity_pct"),
            "precipitation_pct": w.get("precipitation"),
            "condition": w.get("condition"),
            "source": "normalized",
        }
        return result

    # 2. Look up game to get venue
    game = ds.get_game(sport, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    venue_name: str = game.get("venue") or ""
    coords = _find_venue_coords(venue_name)

    # 3. Fetch from Open-Meteo if coords found
    if coords:
        weather = await _fetch_open_meteo(coords[0], coords[1])
        if weather:
            wind_dir_deg: float | None = weather.get("wind_direction_deg")
            return {
                "game_id": game_id,
                "sport": sport,
                "venue": venue_name,
                "dome": False,
                "temp_f": weather.get("temp_f"),
                "wind_mph": weather.get("wind_mph"),
                "wind_direction": _deg_to_compass(wind_dir_deg),
                "wind_direction_deg": wind_dir_deg,
                "humidity_pct": weather.get("humidity_pct"),
                "precipitation_pct": weather.get("precipitation_pct"),
                "condition": weather.get("condition"),
                "source": "open-meteo",
            }

    raise HTTPException(
        status_code=404,
        detail=f"No weather data available for venue: {venue_name!r}",
    )
