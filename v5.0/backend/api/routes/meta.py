# ──────────────────────────────────────────────────────────
# V5.0 Backend — Meta / Utility Routes
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from config import SPORT_DEFINITIONS, get_settings, get_available_seasons, get_current_season
from services.data_service import DataService, get_data_service

router = APIRouter(prefix="/v1")

_start_time = time.time()

DS = Annotated[DataService, Depends(get_data_service)]


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
    settings = get_settings()
    defn = SPORT_DEFINITIONS[sport]

    available: dict[str, dict] = {}
    for kind in ("games", "teams", "standings", "players", "player_stats", "odds", "predictions", "injuries", "news"):
        sport_dir = settings.normalized_dir / sport
        files = sorted(sport_dir.glob(f"{kind}_*.parquet")) if sport_dir.is_dir() else []
        if files:
            seasons = [f.stem.replace(f"{kind}_", "") for f in files]
            available[kind] = {"available": True, "seasons": seasons, "file_count": len(files)}
        else:
            available[kind] = {"available": False, "seasons": [], "file_count": 0}

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
    providers: set[str] = set()
    for sport_info in ds.list_available_sports():
        sport = sport_info["key"]
        for kind in ("teams", "games", "players"):
            records = getattr(ds, f"get_{kind}", lambda s: [])(sport)
            for r in records[:5]:
                src = r.get("source")
                if src:
                    providers.add(src)
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
            "data_freshness": data_freshness,
            "configured_sports": len(SPORT_DEFINITIONS),
            "normalized_dir": str(settings.normalized_dir),
            "normalized_dir_exists": settings.normalized_dir.exists(),
        },
        "meta": {"cached_at": datetime.now(timezone.utc).isoformat()},
    }


# ── Legacy /v1/status (kept for backward compat) ─────────

@router.get("/status", summary="System status (legacy)", include_in_schema=False)
async def system_status(ds: DS):
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
        "cache": ds.cache_stats,
        "data_freshness": data_freshness,
        "configured_sports": len(SPORT_DEFINITIONS),
    }
