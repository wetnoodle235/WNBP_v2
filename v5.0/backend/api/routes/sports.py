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
from config import SPORT_DEFINITIONS, get_available_seasons, get_current_season, is_season_active
from services.data_service import DataService, get_data_service

router = APIRouter(prefix="/v1/{sport}")


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
    players = ds.get_players(sport, season=effective_season, team_id=team_id)
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
    return {"success": True, "data": match[0], "meta": {"sport": sport, "season": _resolve_season(sport, None)}}


# ── Player Stats ──────────────────────────────────────────

@router.get(
    "/player-stats",
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

@router.get(
    "/advanced-stats",
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

@router.get(
    "/match-events",
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


# ── Ratings ───────────────────────────────────────────────

@router.get(
    "/ratings",
    summary="ELO, SPI, RAPTOR ratings",
    description=(
        "FiveThirtyEight-sourced ratings: ELO (NBA, NFL), SPI (soccer), RAPTOR (NBA players). "
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
    """ELO, SPI, and RAPTOR ratings from FiveThirtyEight."""
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


@router.get(
    "/market-signals",
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


@router.get(
    "/schedule-fatigue",
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


# ── Team Stats ────────────────────────────────────────────────
@router.get(
    "/team-stats",
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


@router.get(
    "/live-predictions",
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

@router.get(
    "/injuries",
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
