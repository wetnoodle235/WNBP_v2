# ──────────────────────────────────────────────────────────
# V5.0 Backend — Meta / Utility Routes
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from config import SPORT_DEFINITIONS, get_available_seasons, get_current_season, get_settings
from services.data_service import DataService, get_data_service
from services.media_mirror import get_media_mirror_service

router = APIRouter(prefix="/v1")

_start_time = time.time()

DS = Annotated[DataService, Depends(get_data_service)]
_settings = get_settings()
_media = get_media_mirror_service()

_DEFAULT_AGGREGATE_SPORTS = (
    "nba",
    "nfl",
    "mlb",
    "nhl",
    "ncaab",
    "wnba",
    "epl",
    "ufc",
    "f1",
    "csgo",
)


def _parse_sports_filter(raw_sports: str | None) -> list[str]:
    if not raw_sports:
        return list(_DEFAULT_AGGREGATE_SPORTS)
    parsed = [s.strip().lower() for s in raw_sports.split(",") if s.strip()]
    invalid = [s for s in parsed if s not in SPORT_DEFINITIONS]
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unknown sport(s): {', '.join(sorted(set(invalid)))}",
        )
    deduped: list[str] = []
    seen: set[str] = set()
    for s in parsed:
        if s not in seen:
            deduped.append(s)
            seen.add(s)
    return deduped


def _resolve_target_date(raw_date: str | None) -> str:
    if not raw_date:
        return date.today().isoformat()
    return raw_date


def _is_final_status(value: object) -> bool:
    status_value = str(value or "").strip().lower()
    if not status_value:
        return False
    return any(token in status_value for token in ("final", "completed", "closed", "postponed", "cancel"))


def _top_confidence(predictions: list[dict]) -> float:
    best = 0.0
    for pred in predictions:
        try:
            conf = float(pred.get("confidence") or 0.0)
        except (TypeError, ValueError):
            conf = 0.0
        if conf > best:
            best = conf
    return best


def _apply_league_image_mirror(sport_key: str, payload: dict) -> dict:
    source_url = payload.get("image_url") or payload.get("logo_url")
    if not source_url:
        return payload
    payload["image_url"] = _media.league_image_url(
        sport=sport_key,
        source_url=str(source_url),
        auto_sync=_settings.media_auto_sync,
    )
    return payload


# ── /api/v1/sports ────────────────────────────────────────

@router.get(
    "/sports",
    summary="List all available sports",
    description="Returns every sport in the catalogue with data type counts and metadata. Includes both sports with data on disk and configured sports awaiting data ingestion.",
    tags=["Meta"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Complete sports catalogue",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {
                                "key": "nba",
                                "name": "NBA Basketball",
                                "league": "NBA",
                                "data_types": {"games": 1230, "teams": 30, "players": 540, "standings": 30, "odds": 4800},
                                "file_count": 12,
                            },
                            {
                                "key": "nfl",
                                "name": "NFL Football",
                                "league": "NFL",
                                "data_types": {"games": 285, "teams": 32, "players": 1800, "standings": 32, "odds": 2100},
                                "file_count": 10,
                            },
                            {
                                "key": "mlb",
                                "name": "MLB Baseball",
                                "league": "MLB",
                                "data_types": {"games": 2430, "teams": 30, "players": 1200},
                                "file_count": 8,
                            },
                        ],
                        "meta": {
                            "count": 12,
                            "sports_with_data": 10,
                            "cached_at": "2025-03-25T20:00:00Z",
                        },
                    }
                }
            },
        }
    },
)
async def list_sports(ds: DS):
    """List all available sports with data counts."""
    available = ds.list_available_sports()
    # Merge in definition metadata for sports without data too
    sport_keys_with_data = {s["key"] for s in available}
    all_sports = list(available)
    for key, defn in SPORT_DEFINITIONS.items():
        if key not in sport_keys_with_data:
            all_sports.append({"key": key, **defn, "data_types": {}, "file_count": 0})
    # Enrich each sport with available_seasons and current_season
    for s in all_sports:
        key = s["key"]
        _apply_league_image_mirror(key, s)
        s["available_seasons"] = get_available_seasons(key)
        try:
            s["current_season"] = get_current_season(key)
        except Exception:
            s["current_season"] = None
    all_sports.sort(key=lambda s: s["key"])
    return {
        "success": True,
        "data": all_sports,
        "meta": {
            "count": len(all_sports),
            "sports_with_data": len(sport_keys_with_data),
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/aggregate/games",
    summary="Aggregate games across multiple sports",
    description="Returns combined games for multiple sports using backend DataService reads (DuckDB-backed when enabled).",
    tags=["Meta"],
    response_model_exclude_none=True,
)
async def aggregate_games(
    ds: DS,
    sports: str | None = None,
    date_filter: str | None = None,
    limit_per_sport: int = 50,
    exclude_final: bool = False,
):
    target_sports = _parse_sports_filter(sports)
    target_date = _resolve_target_date(date_filter)
    per_sport = max(1, min(limit_per_sport, 500))

    combined = ds.query_cross_sport(
        "games", target_sports, date_filter=target_date, limit_per_sport=per_sport
    )
    if exclude_final:
        combined = [r for r in combined if not _is_final_status(r.get("status"))]

    return {
        "success": True,
        "data": combined,
        "meta": {
            "sports": target_sports,
            "date": target_date,
            "count": len(combined),
            "limit_per_sport": per_sport,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/aggregate/news",
    summary="Aggregate news across multiple sports",
    description="Returns combined news rows for multiple sports using backend DataService reads (DuckDB-backed when enabled).",
    tags=["Meta"],
    response_model_exclude_none=True,
)
async def aggregate_news(
    ds: DS,
    sports: str | None = None,
    limit_per_sport: int = 15,
):
    target_sports = _parse_sports_filter(sports)
    per_sport = max(1, min(limit_per_sport, 100))

    combined = ds.query_cross_sport("news", target_sports, limit_per_sport=per_sport)
    combined.sort(key=lambda r: str(r.get("published_at") or r.get("published") or ""), reverse=True)

    return {
        "success": True,
        "data": combined,
        "meta": {
            "sports": target_sports,
            "count": len(combined),
            "limit_per_sport": per_sport,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/aggregate/predictions",
    summary="Aggregate predictions across multiple sports",
    description="Returns combined predictions for multiple sports for a target date.",
    tags=["Meta"],
    response_model_exclude_none=True,
)
async def aggregate_predictions(
    ds: DS,
    sports: str | None = None,
    date_filter: str | None = None,
    limit_per_sport: int = 100,
):
    target_sports = _parse_sports_filter(sports)
    target_date = _resolve_target_date(date_filter)
    per_sport = max(1, min(limit_per_sport, 1000))

    combined = ds.query_cross_sport(
        "predictions", target_sports, date_filter=target_date, limit_per_sport=per_sport
    )
    combined.sort(key=lambda r: float(r.get("confidence") or 0.0), reverse=True)

    return {
        "success": True,
        "data": combined,
        "meta": {
            "sports": target_sports,
            "date": target_date,
            "count": len(combined),
            "limit_per_sport": per_sport,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/aggregate/odds",
    summary="Aggregate odds across multiple sports",
    description="Returns combined odds rows for multiple sports for a target date.",
    tags=["Meta"],
    response_model_exclude_none=True,
)
async def aggregate_odds(
    ds: DS,
    sports: str | None = None,
    date_filter: str | None = None,
    limit_per_sport: int = 250,
):
    target_sports = _parse_sports_filter(sports)
    target_date = _resolve_target_date(date_filter)
    per_sport = max(1, min(limit_per_sport, 2000))

    combined = ds.query_cross_sport(
        "odds", target_sports, date_filter=target_date, limit_per_sport=per_sport
    )

    return {
        "success": True,
        "data": combined,
        "meta": {
            "sports": target_sports,
            "date": target_date,
            "count": len(combined),
            "limit_per_sport": per_sport,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/aggregate/teams",
    summary="Aggregate teams across multiple sports",
    description="Returns combined teams for multiple sports.",
    tags=["Meta"],
    response_model_exclude_none=True,
)
async def aggregate_teams(
    ds: DS,
    sports: str | None = None,
    limit_per_sport: int = 500,
):
    target_sports = _parse_sports_filter(sports)
    per_sport = max(1, min(limit_per_sport, 5000))

    combined = ds.query_cross_sport("teams", target_sports, limit_per_sport=per_sport)

    return {
        "success": True,
        "data": combined,
        "meta": {
            "sports": target_sports,
            "count": len(combined),
            "limit_per_sport": per_sport,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/aggregate/players",
    summary="Aggregate players across multiple sports",
    description="Returns combined players for multiple sports.",
    tags=["Meta"],
    response_model_exclude_none=True,
)
async def aggregate_players(
    ds: DS,
    sports: str | None = None,
    limit_per_sport: int = 2000,
):
    target_sports = _parse_sports_filter(sports)
    per_sport = max(1, min(limit_per_sport, 10000))

    combined = ds.query_cross_sport("players", target_sports, limit_per_sport=per_sport)

    return {
        "success": True,
        "data": combined,
        "meta": {
            "sports": target_sports,
            "count": len(combined),
            "limit_per_sport": per_sport,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/aggregate/stats",
    summary="Aggregate player/team stats across multiple sports",
    description="Returns per-sport player and team stats in one response.",
    tags=["Meta"],
    response_model_exclude_none=True,
)
async def aggregate_stats(
    ds: DS,
    sports: str | None = None,
    player_limit_per_sport: int = 200,
    team_limit_per_sport: int = 200,
):
    target_sports = _parse_sports_filter(sports)
    player_limit = max(1, min(player_limit_per_sport, 2000))
    team_limit = max(1, min(team_limit_per_sport, 2000))

    # Two DuckDB UNION ALL queries replace a per-sport Python loop.
    # When DuckDB views are unavailable the base class falls back to per-sport reads.
    all_player_rows = ds.query_cross_sport("player_stats", target_sports)
    all_team_rows = ds.query_cross_sport("team_stats", target_sports)

    # Group already-fetched rows by sport in a single O(n) pass.
    sport_players: dict[str, list[dict]] = {s: [] for s in target_sports}
    sport_teams: dict[str, list[dict]] = {s: [] for s in target_sports}
    for r in all_player_rows:
        s = str(r.get("sport", ""))
        if s in sport_players:
            sport_players[s].append(r)
    for r in all_team_rows:
        s = str(r.get("sport", ""))
        if s in sport_teams:
            sport_teams[s].append(r)

    data: dict[str, dict] = {
        sport: {
            "season": (
                (sport_players[sport][0].get("season") if sport_players[sport] else None)
                or (sport_teams[sport][0].get("season") if sport_teams[sport] else None)
                or get_current_season(sport)
            ),
            "player_stats": sport_players[sport][:player_limit],
            "team_stats": sport_teams[sport][:team_limit],
        }
        for sport in target_sports
    }

    return {
        "success": True,
        "data": data,
        "meta": {
            "sports": target_sports,
            "player_limit_per_sport": player_limit,
            "team_limit_per_sport": team_limit,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/aggregate/home-feed",
    summary="Single-query home feed",
    description="Returns games, news, and predictions for the homepage in one request.",
    tags=["Meta"],
    response_model_exclude_none=True,
)
async def aggregate_home_feed(
    ds: DS,
    sports: str | None = None,
    date_filter: str | None = None,
    games_per_sport: int = 6,
    news_per_sport: int = 2,
    predictions_per_sport: int = 12,
):
    target_sports = _parse_sports_filter(sports)
    target_date = _resolve_target_date(date_filter)

    # Three DuckDB UNION ALL queries replace a per-sport Python loop for each kind.
    g_limit = max(1, min(games_per_sport, 100))
    n_limit = max(1, min(news_per_sport, 25))
    p_limit = max(1, min(predictions_per_sport, 200))

    games = ds.query_cross_sport(
        "games", target_sports, date_filter=target_date, limit_per_sport=g_limit
    )
    games = [g for g in games if not _is_final_status(g.get("status"))]

    news = ds.query_cross_sport("news", target_sports, limit_per_sport=n_limit)
    news.sort(key=lambda r: str(r.get("published_at") or r.get("published") or ""), reverse=True)

    predictions = ds.query_cross_sport(
        "predictions", target_sports, date_filter=target_date, limit_per_sport=p_limit
    )
    predictions.sort(key=lambda r: float(r.get("confidence") or 0.0), reverse=True)

    # Derive per-sport metrics from data already fetched — no extra reads.
    sport_metrics: list[dict] = [
        {
            "sport": sport,
            "games_today": sum(1 for g in games if g.get("sport") == sport),
            "predictions": sum(1 for p in predictions if p.get("sport") == sport),
            "top_confidence": _top_confidence([p for p in predictions if p.get("sport") == sport]),
        }
        for sport in target_sports
        if any(r.get("sport") == sport for r in (*games, *predictions))
    ]

    return {
        "success": True,
        "data": {
            "games": games,
            "news": news,
            "predictions": predictions,
            "sport_metrics": sport_metrics,
        },
        "meta": {
            "sports": target_sports,
            "date": target_date,
            "counts": {
                "games": len(games),
                "news": len(news),
                "predictions": len(predictions),
            },
            "reader": _settings.backend_reader,
            "duckdb_curated": _settings.duckdb_use_curated,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/sports/{sport}",
    summary="Sport detail with available data",
    description="Detailed metadata for a single sport: which data types have files on disk, available seasons per data type, and file counts. Useful for discovering what data is queryable.",
    tags=["Meta"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Sport metadata with data availability breakdown",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "key": "nba",
                            "name": "NBA Basketball",
                            "league": "NBA",
                            "available_data": {
                                "games": {"available": True, "seasons": ["2023", "2024"], "file_count": 2},
                                "teams": {"available": True, "seasons": ["2024"], "file_count": 1},
                                "standings": {"available": True, "seasons": ["2024"], "file_count": 1},
                                "players": {"available": True, "seasons": ["2024"], "file_count": 1},
                                "player_stats": {"available": True, "seasons": ["2024"], "file_count": 1},
                                "odds": {"available": True, "seasons": ["2024"], "file_count": 1},
                                "predictions": {"available": True, "seasons": ["2024"], "file_count": 1},
                                "injuries": {"available": True, "seasons": ["latest"], "file_count": 1},
                                "news": {"available": True, "seasons": ["latest"], "file_count": 1},
                            },
                        },
                        "meta": {"sport": "nba"},
                    }
                }
            },
        },
        404: {
            "description": "Sport not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Unknown sport 'curling'"}
                }
            },
        },
    },
)
async def get_sport(sport: str, ds: DS):
    if sport not in SPORT_DEFINITIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown sport '{sport}'",
        )
    defn = SPORT_DEFINITIONS[sport]

    available: dict[str, dict] = {}
    sport_inventory = next((row for row in ds.list_available_sports() if row.get("key") == sport), None)
    kind_counts = sport_inventory.get("data_types", {}) if sport_inventory else {}

    for kind in (
        "games",
        "teams",
        "standings",
        "players",
        "player_stats",
        "odds",
        "odds_history",
        "odds_all",
        "player_props",
        "player_props_history",
        "player_props_all",
        "predictions",
        "injuries",
        "news",
        "advanced_stats",
        "advanced_batting",
        "all_stats",
        "match_events",
        "play_by_play",
        "coaches",
        "draft",
        "player_portal",
        "player_returning",
        "player_usage",
        "ratings",
        "market_signals",
        "market_history",
        "schedule_fatigue",
        "team_stats",
        "team_game_stats",
        "batter_game_stats",
        "pitcher_game_stats",
        "transactions",
        "weather",
    ):
        count = int(kind_counts.get(kind, 0))
        seasons = ds.get_seasons(sport, kind=kind) if count else []
        available[kind] = {"available": count > 0, "seasons": seasons, "file_count": count}

    return {
        "success": True,
        "data": {
            "key": sport,
            **defn,
            "available_data": available,
        },
        "meta": {"sport": sport},
    }


# ── /api/v1/meta/* ───────────────────────────────────────

@router.get(
    "/meta/sports",
    summary="Available sports with metadata",
    description="Returns only sports that have actual data files on disk, with per-data-type breakdowns. A subset of the full catalogue endpoint.",
    tags=["Meta"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Sports with data on disk",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": [
                            {"key": "nba", "name": "NBA Basketball", "file_count": 12, "data_types": {"games": 2, "teams": 1, "players": 1}},
                            {"key": "nfl", "name": "NFL Football", "file_count": 10, "data_types": {"games": 2, "teams": 1, "players": 1}},
                        ],
                        "meta": {"count": 10},
                    }
                }
            },
        }
    },
)
async def meta_sports(ds: DS):
    available = ds.list_available_sports()
    # Enrich with season info (same as /v1/sports)
    for s in available:
        key = s["key"]
        _apply_league_image_mirror(key, s)
        s["available_seasons"] = get_available_seasons(key)
        try:
            s["current_season"] = get_current_season(key)
        except Exception:
            s["current_season"] = None
    return {
        "success": True,
        "data": available,
        "meta": {"count": len(available)},
    }


@router.get(
    "/meta/providers",
    summary="Active data providers",
    description="Lists unique data source providers observed across all normalized parquet files. Useful for understanding data provenance and licensing attribution.",
    tags=["Meta"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Sorted list of provider names",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": ["espn", "odds_api", "sportradar"],
                        "meta": {"count": 3},
                    }
                }
            },
        }
    },
)
async def meta_providers(ds: DS):
    """Scan just the *source* column from one parquet file per sport.

    The previous implementation loaded full games/teams/players datasets for
    every sport (up to 75 full parquet loads), causing 30-120 second responses.
    Now we read only the 'source' column from a single file per sport.
    """
    settings = get_settings()
    providers: set[str] = set()
    norm = settings.normalized_dir
    if norm.exists():
        for sport_dir in sorted(norm.iterdir()):
            if not sport_dir.is_dir():
                continue
            # Check one file across preferred kinds — stop as soon as we get sources.
            for kind in ("games", "teams", "players"):
                files = sorted(sport_dir.glob(f"{kind}_*.parquet"))
                if not files:
                    continue
                try:
                    import pyarrow.parquet as pq
                    schema = pq.read_schema(files[0])
                    if "source" in schema.names:
                        import pandas as _pd
                        df = _pd.read_parquet(files[0], columns=["source"], engine="pyarrow")
                        for src in df["source"].dropna().unique():
                            if src:
                                providers.add(str(src))
                        break  # Got sources for this sport; move to next sport.
                except Exception:
                    continue
    return {
        "success": True,
        "data": sorted(providers),
        "meta": {"count": len(providers)},
    }


@router.get(
    "/meta/data-status",
    summary="Data freshness per sport and type",
    description="File modification timestamps for every normalized parquet file, grouped by sport. Use this to monitor data pipeline health and identify stale data.",
    tags=["Meta"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Data freshness map keyed by sport",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "nba": {
                                "games_2024": "2025-03-25T18:00:00Z",
                                "teams_2024": "2025-03-24T06:00:00Z",
                                "odds_2024": "2025-03-25T19:45:00Z",
                            },
                            "nfl": {
                                "games_2024": "2025-02-10T12:00:00Z",
                                "teams_2024": "2025-02-10T12:00:00Z",
                            },
                        },
                        "meta": {"sports_count": 10, "cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def meta_data_status(ds: DS):
    freshness = ds.get_data_freshness()
    return {
        "success": True,
        "data": freshness,
        "meta": {
            "sports_count": len(freshness),
            "cached_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get(
    "/media/status",
    summary="Media mirror status (admin)",
    description="Operational status for mirrored media assets including catalog readiness, asset counts, and per-sport/entity breakdowns.",
    tags=["System"],
    response_model_exclude_none=True,
)
async def media_status():
    stats = _media.stats()
    return {
        "success": True,
        "data": stats,
        "meta": {"cached_at": datetime.now(timezone.utc).isoformat()},
    }


# ── /api/v1/health ────────────────────────────────────────

@router.get(
    "/health",
    summary="Detailed health check",
    description="System health with uptime, cache hit/miss statistics, per-sport data freshness timestamps, and storage configuration. More detailed than the root /health endpoint.",
    tags=["System"],
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Detailed system health information",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "data": {
                            "status": "ok",
                            "uptime_seconds": 86432.5,
                            "cache": {"hits": 15420, "misses": 312, "size": 48},
                            "data_freshness": {
                                "nba": "2025-03-25T18:00:00Z",
                                "nfl": "2025-02-10T12:00:00Z",
                                "mlb": "2025-03-25T17:30:00Z",
                            },
                            "configured_sports": 12,
                            "normalized_dir": "/data/normalized",
                            "normalized_dir_exists": True,
                        },
                        "meta": {"cached_at": "2025-03-25T20:00:00Z"},
                    }
                }
            },
        }
    },
)
async def health_check(ds: DS):
    settings = get_settings()
    uptime = time.time() - _start_time
    media_stats = _media.stats()

    data_freshness: dict[str, str | None] = {}
    if settings.normalized_dir.exists():
        for sport_dir in sorted(settings.normalized_dir.iterdir()):
            if sport_dir.is_dir():
                latest = None
                for f in sport_dir.glob("*.parquet"):
                    mtime = f.stat().st_mtime
                    if latest is None or mtime > latest:
                        latest = mtime
                data_freshness[sport_dir.name] = (
                    datetime.fromtimestamp(latest, tz=timezone.utc).isoformat()
                    if latest
                    else None
                )

    return {
        "success": True,
        "data": {
            "status": "ok",
            "uptime_seconds": round(uptime, 1),
            "cache": ds.cache_stats,
            "media_mirror": media_stats,
            "data_freshness": data_freshness,
            "configured_sports": len(SPORT_DEFINITIONS),
            "normalized_dir": str(settings.normalized_dir),
            "normalized_dir_exists": settings.normalized_dir.exists(),
        },
        "meta": {"cached_at": datetime.now(timezone.utc).isoformat()},
    }


# ── Legacy /v1/status (kept for backward compat) ─────────

@router.get("/status", summary="System status (legacy)", include_in_schema=False)
async def system_status():
    settings = get_settings()
    uptime = time.time() - _start_time

    data_freshness: dict[str, str | None] = {}
    if settings.normalized_dir.exists():
        for sport_dir in sorted(settings.normalized_dir.iterdir()):
            if sport_dir.is_dir():
                latest = None
                for f in sport_dir.rglob("*.parquet"):
                    mtime = f.stat().st_mtime
                    if latest is None or mtime > latest:
                        latest = mtime
                data_freshness[sport_dir.name] = (
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(latest))
                    if latest
                    else None
                )

    return {
        "status": "ok",
        "uptime_seconds": round(uptime, 1),
        "cache": {},
        "data_freshness": data_freshness,
        "configured_sports": len(SPORT_DEFINITIONS),
    }


# ── Upcoming Events (racing / golf / event-based sports) ─────────

_EVENT_SPORTS = ("f1", "indycar", "golf", "lpga", "pga", "mma", "ufc", "atp", "wta")


@router.get("/events/upcoming", summary="Upcoming events for racing, golf, and other event-based sports")
async def upcoming_events(
    ds: DS,
    days: int = 60,
    sports: str | None = None,
):
    """Return upcoming events for racing/golf/individual sports within the next N days.

    Unlike /schedule this endpoint:
    - Works across multiple sports at once
    - Uses date comparison (not just status) to find future events
    - Includes race_name, venue, circuit_name, broadcast fields
    - Supports up to 90 days ahead (default 60)
    """
    from datetime import date, timedelta
    import pandas as pd
    from config import get_current_season
    from pathlib import Path

    days = max(1, min(days, 90))
    today = date.today()
    future_cutoff = today + timedelta(days=days)

    target_sports = [s.strip().lower() for s in sports.split(",")] if sports else list(_EVENT_SPORTS)

    settings = get_settings()
    events: list[dict] = []

    for sport in target_sports:
        try:
            season = get_current_season(sport)
            df = ds._load_kind(sport, "games", season=season)
            if df is None or (hasattr(df, "empty") and df.empty):
                continue

            if "date" not in df.columns:
                continue

            df = df.copy()
            df["_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            mask = (df["_date"] >= today) & (df["_date"] <= future_cutoff)
            upcoming = df[mask].copy()

            if upcoming.empty:
                continue

            upcoming = upcoming.sort_values("_date")

            keep_cols = [
                "id", "date", "start_time", "status", "sport",
                "home_team", "away_team", "venue", "broadcast",
                "race_name", "circuit_name", "round_number",
                "winner_name", "day_of_week",
            ]
            available = [c for c in keep_cols if c in upcoming.columns]
            upcoming = upcoming[available]
            if "sport" not in upcoming.columns:
                upcoming = upcoming.assign(sport=sport)

            records = upcoming.where(upcoming.notna(), None).to_dict("records")
            for r in records:
                # Normalize date to string
                if r.get("date") is not None:
                    r["date"] = str(r["date"])
                # Convert start_time timestamp to ISO string
                if r.get("start_time") is not None:
                    try:
                        r["start_time"] = str(r["start_time"])
                    except Exception:
                        r["start_time"] = None
                r["sport"] = sport
            events.extend(records)
        except Exception:
            continue

    # Sort all events by date
    events.sort(key=lambda e: e.get("date") or "")

    return {
        "success": True,
        "data": events,
        "meta": {
            "count": len(events),
            "sports": target_sports,
            "days_ahead": days,
            "from_date": today.isoformat(),
            "to_date": future_cutoff.isoformat(),
        },
    }
