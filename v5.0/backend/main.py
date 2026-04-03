# ──────────────────────────────────────────────────────────
# V5.0 Sports Data Platform API
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from ipaddress import ip_address

from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_oauth2_redirect_html
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from config import ALL_SPORTS, SPORT_DEFINITIONS, get_current_season, get_settings
from services.data_service import get_data_service
from services.media_mirror import MediaTarget, get_media_mirror_service
from api.routes import (
    autobet_router,
    features_router,
    live_router,
    meta_router,
    paper_router,
    predictions_router,
    sports_router,
    stripe_router,
)
from api.middleware.rate_limit import RateLimitConfig, RateLimitMiddleware
from auth.database import init_db

logger = logging.getLogger(__name__)

INTERNAL_DOC_PATH_SUFFIXES = {
    "/advanced-stats",
    "/advanced_stats",
    "/media/status",
    "/match-events",
    "/match_events",
    "/ratings",
    "/team-stats",
    "/team_stats",
    "/transactions",
    "/schedule",
    "/simulation",
}

SELLABLE_OPENAPI_PATHS = {
    "/v1/{sport}/overview",
    "/v1/{sport}/games",
    "/v1/{sport}/teams",
    "/v1/{sport}/players",
    "/v1/{sport}/standings",
    "/v1/{sport}/odds",
    "/v1/{sport}/injuries",
    "/v1/{sport}/market_signals",
    "/v1/{sport}/schedule_fatigue",
}

SELLABLE_OPENAPI_TAGS = {
    "Overview",
    "Games",
    "Teams",
    "Players",
    "Standings",
    "Odds",
    "Injuries",
    "Advanced",
}

OPENAPI_PATH = "/openapi.json"


def _is_local_request(request: Request) -> bool:
    """Allow docs/schema access only from localhost clients."""
    host = (request.client.host if request.client else "") or ""
    if host in {"127.0.0.1", "::1", "localhost"}:
        return True
    try:
        return ip_address(host).is_loopback
    except Exception:
        return False


DOCS_CSS = """
body {
    margin: 0;
    background: #f7f9fc;
    color: #0f172a;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif;
}
.docs-shell {
    max-width: 1180px;
    margin: 0 auto;
    padding: 1.25rem 1rem 2rem;
}
.docs-hero {
    background:
      radial-gradient(circle at 10% 10%, rgba(56, 189, 248, 0.22), transparent 38%),
      radial-gradient(circle at 90% 20%, rgba(244, 114, 182, 0.24), transparent 35%),
      linear-gradient(135deg, #0f172a, #1d4ed8);
    color: #fff;
    border-radius: 14px;
    padding: 1.25rem 1.25rem;
    margin-bottom: 1rem;
    box-shadow: 0 10px 24px rgba(15, 23, 42, 0.2);
}
.docs-hero h1 {
    margin: 0 0 0.5rem;
    font-size: 1.3rem;
}
.docs-hero p {
    margin: 0;
    opacity: 0.9;
    font-size: 0.95rem;
    line-height: 1.45;
}
.docs-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-top: 0.75rem;
}
.docs-chip {
    background: rgba(255, 255, 255, 0.16);
    border: 1px solid rgba(255, 255, 255, 0.32);
    color: #fff;
    border-radius: 999px;
    padding: 0.2rem 0.55rem;
    font-size: 0.76rem;
}
.docs-card {
    background: #fff;
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    padding: 0.8rem 0.9rem;
    margin-bottom: 0.8rem;
}
.docs-card h2 {
    margin: 0 0 0.5rem;
    font-size: 0.95rem;
}
.docs-card code {
    background: #f1f5f9;
    border-radius: 6px;
    padding: 0.18rem 0.35rem;
    font-size: 0.8rem;
}
.docs-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 0.8rem;
    margin-bottom: 0.8rem;
}
#swagger-ui {
    background: #fff;
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    padding: 0.2rem;
}
.swagger-ui .topbar {
    display: none;
}
.swagger-ui .opblock-tag {
    border-bottom: 1px solid #e2e8f0;
}
.swagger-ui .opblock .opblock-summary {
    border-radius: 8px;
}
"""


def _build_docs_html(openapi_url: str, oauth2_redirect_url: str | None, base_url: str) -> str:
        oauth2_redirect = f'oauth2RedirectUrl: window.location.origin + "{oauth2_redirect_url}",' if oauth2_redirect_url else ""
        return f"""<!DOCTYPE html>
<html lang=\"en\">
    <head>
        <meta charset=\"UTF-8\" />
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
        <title>SportStock API Docs</title>
        <link rel=\"stylesheet\" href=\"https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css\" />
        <style>{DOCS_CSS}</style>
    </head>
    <body>
        <main class=\"docs-shell\">
            <section class=\"docs-hero\">
                <h1>SportStock API Reference</h1>
                <p>Interactive API explorer for normalized games, odds, standings, predictions, injuries, news, and model diagnostics across 30+ sports.</p>
                <div class=\"docs-chips\">
                    <span class=\"docs-chip\">Base URL: {base_url}</span>
                    <span class="docs-chip">Auth: localhost bypass, remote uses X-API-Key</span>
                    <span class=\"docs-chip\">Version: v5.0</span>
                    <span class=\"docs-chip\">Schema: {openapi_url}</span>
                    <span class=\"docs-chip\">Discover: /v1/sports + /v1/meta/sports</span>
                    <span class="docs-chip">Local-only developer reference</span>
                </div>
            </section>

            <section class=\"docs-grid\">
                <section class=\"docs-card\">
                    <h2>Quick Start</h2>
                    <div>1) Generate key at <code>/auth/register</code> or dashboard</div>
                    <div>2) Click Authorize and set <code>X-API-Key</code></div>
                    <div>3) Start with <code>/v1/sports</code> and <code>/v1/meta/sports</code> before sport-specific routes</div>
                </section>
                <section class=\"docs-card\">
                    <h2>Visibility Rules</h2>
                    <div>Internal/dev endpoints are hidden from this reference by default.</div>
                    <div>Customer API keys cannot execute internal-only endpoints.</div>
                    <div>Internal teams can opt-in docs visibility via <code>V5_INCLUDE_INTERNAL_DOCS=true</code>.</div>
                </section>
            </section>

            <section id=\"swagger-ui\"></section>
        </main>

        <script src=\"https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js\"></script>
        <script src=\"https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js\"></script>
        <script>
            window.ui = SwaggerUIBundle({{
                url: "{openapi_url}",
                dom_id: "#swagger-ui",
                deepLinking: true,
                displayRequestDuration: true,
                persistAuthorization: true,
                filter: true,
                docExpansion: "none",
                defaultModelsExpandDepth: -1,
                operationsSorter: "alpha",
                tagsSorter: "alpha",
                presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
                layout: "BaseLayout",
                {oauth2_redirect}
            }});
        </script>
    </body>
</html>
"""


TAGS_METADATA = [
    {
        "name": "Games",
        "description": "Scheduled, live, and completed games with scores, venues, and broadcast info across all sports.",
    },
    {
        "name": "Teams",
        "description": "Team profiles, rosters, logos, and organizational metadata.",
    },
    {
        "name": "Players",
        "description": "Player profiles with biographical data, status, and team affiliation.",
    },
    {
        "name": "Standings",
        "description": "Conference, division, and overall standings with win-loss records and rankings.",
    },
    {
        "name": "Odds",
        "description": "Pre-game and live betting odds from multiple sportsbooks — moneyline, spread, and totals.",
    },
    {
        "name": "Stats",
        "description": "Per-game player box-score statistics with sport-specific stat categories.",
    },
    {
        "name": "Predictions",
        "description": "Machine learning model predictions for upcoming games, including win probabilities, spreads, and accuracy tracking.",
    },
    {
        "name": "Injuries",
        "description": "Active injury reports with player status, body part, and expected return dates.",
    },
    {
        "name": "News",
        "description": "Latest sports news articles sourced from major outlets, filterable by sport.",
    },
    {
        "name": "Overview",
        "description": "Aggregated dashboard endpoints combining games, standings, news, and injuries for a single sport.",
    },
    {
        "name": "Meta",
        "description": "Platform metadata: available sports catalogue, data providers, and data freshness timestamps.",
    },
    {
        "name": "Live",
        "description": "Server-Sent Event (SSE) streams for real-time game score updates and odds line movements.",
    },
    {
        "name": "System",
        "description": "Health checks, uptime, cache statistics, and system diagnostics.",
    },
    {
        "name": "Authentication",
        "description": "User registration, API key management, and subscription billing.",
    },
    {
        "name": "Features",
        "description": "ML feature datasets for data scientists. Requires pro tier or higher.",
    },
    {
        "name": "Advanced",
        "description": "Advanced analytics endpoints including event-level data and ratings.",
    },
    {
        "name": "Roster",
        "description": "Roster movement and transaction endpoints.",
    },
    {
        "name": "Paper Trading",
        "description": "Paper-betting portfolio, bet placement, settlement, and history.",
    },
    {
        "name": "Stripe",
        "description": "Billing and subscription checkout/customer portal endpoints.",
    },
    {
        "name": "AutoBet",
        "description": "Automated betting bot status, config, and history.",
    },
]

TAG_CANONICAL_NAMES = {
    "games": "Games",
    "teams": "Teams",
    "players": "Players",
    "standings": "Standings",
    "odds": "Odds",
    "stats": "Stats",
    "predictions": "Predictions",
    "injuries": "Injuries",
    "news": "News",
    "overview": "Overview",
    "meta": "Meta",
    "live": "Live",
    "system": "System",
    "authentication": "Authentication",
    "features": "Features",
    "advanced": "Advanced",
    "roster": "Roster",
    "paper trading": "Paper Trading",
    "stripe": "Stripe",
    "autobet": "AutoBet",
}


def _canonicalize_tag(tag: str) -> str:
    return TAG_CANONICAL_NAMES.get(tag.strip().lower(), tag.strip())


def _normalize_schema_tags(schema: dict) -> None:
    seen_tags: set[str] = set()
    for path_item in schema.get("paths", {}).values():
        for operation in path_item.values():
            if not isinstance(operation, dict):
                continue
            raw_tags = operation.get("tags", [])
            normalized_tags: list[str] = []
            for raw_tag in raw_tags:
                canonical = _canonicalize_tag(raw_tag)
                if canonical not in normalized_tags:
                    normalized_tags.append(canonical)
                seen_tags.add(canonical)
            if normalized_tags:
                operation["tags"] = normalized_tags

    metadata_by_name = {t["name"]: t for t in TAGS_METADATA}
    schema["tags"] = [metadata_by_name[name] for name in metadata_by_name if name in seen_tags]


def _build_sellable_openapi(app: FastAPI) -> dict:
    schema = get_openapi(
        title="SportStock Sellable Data API",
        version=app.version,
        description=(
            "Customer-safe sports data schema for fast client and AI integration. "
            "This schema contains only normalized sellable datasets: overview, games, teams, players, standings, odds, injuries, market-signals, and schedule-fatigue."
        ),
        routes=app.routes,
        tags=TAGS_METADATA,
    )

    paths = schema.get("paths", {})
    for path in list(paths.keys()):
        if path not in SELLABLE_OPENAPI_PATHS:
            del paths[path]

    _normalize_schema_tags(schema)
    schema["tags"] = [tag for tag in schema.get("tags", []) if tag.get("name") in SELLABLE_OPENAPI_TAGS]
    schema["servers"] = [{"url": "/", "description": "Same origin"}]
    return schema


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init auth DB, warm cache. Shutdown: cleanup."""
    import asyncio
    from auth.database import close_pool
    from auth.subscription_lifecycle import SubscriptionLifecycleService

    logger.info("Initialising auth database …")
    await init_db()
    ds_ref: dict[str, object | None] = {"value": None}

    # Warm cache without blocking startup indefinitely on large datasets.
    # Disabled by default to avoid startup contention in multi-process/dev runs.
    warm_task: asyncio.Task | None = None
    warm_enabled = os.getenv("V5_WARM_CACHE_ON_STARTUP", "false").lower() in {"1", "true", "yes"}
    warm_timeout_s = int(os.getenv("V5_WARM_CACHE_STARTUP_TIMEOUT", "15"))

    def _init_and_warm_cache() -> None:
        ds = get_data_service()
        ds_ref["value"] = ds
        ds.warm_cache(ALL_SPORTS)

    if warm_enabled:
        try:
            logger.info("Warming data cache …")
            warm_task = asyncio.create_task(asyncio.to_thread(_init_and_warm_cache))
            await asyncio.wait_for(warm_task, timeout=warm_timeout_s)
            logger.info("Cache warm-up complete.")
            warm_task = None
        except asyncio.TimeoutError:
            logger.warning(
                "Cache warm-up still running after %ss; continuing startup and finishing in background.",
                warm_timeout_s,
            )
        except Exception:
            logger.exception("Cache warm-up failed; continuing without warm cache")
            warm_task = None
    else:
        logger.info("Startup cache warm-up disabled (V5_WARM_CACHE_ON_STARTUP=false)")

    # Periodically evict stale rate-limit buckets to prevent unbounded growth.
    _rl_middleware: RateLimitMiddleware | None = None

    async def _cleanup_rate_limiter() -> None:
        while True:
            await asyncio.sleep(3600)  # run every hour
            try:
                if _rl_middleware is not None:
                    removed = _rl_middleware.cleanup_stale()
                    if removed:
                        logger.debug("Rate-limit cleanup removed %d stale buckets", removed)
            except Exception:
                pass

    cleanup_task = asyncio.create_task(_cleanup_rate_limiter())

    media_sync_task: asyncio.Task | None = None

    async def _media_sync_loop() -> None:
        interval_s = int(os.getenv("V5_MEDIA_SYNC_INTERVAL_SECONDS", "0") or "0")
        if interval_s <= 0:
            return

        configured = (os.getenv("V5_MEDIA_SYNC_SPORTS", "") or "").strip()
        sports = [s.strip().lower() for s in configured.split(",") if s.strip()] if configured else ALL_SPORTS
        media = get_media_mirror_service()

        logger.info("Media mirror sync loop enabled (interval=%ss, sports=%s)", interval_s, ",".join(sports))
        while True:
            try:
                ds = get_data_service()
                for sport in sports:
                    try:
                        season = get_current_season(sport)

                        sport_meta = SPORT_DEFINITIONS.get(sport, {})
                        league_image = sport_meta.get("image_url") or sport_meta.get("logo_url")
                        if league_image:
                            media.sync_target(
                                MediaTarget(
                                    sport=sport,
                                    entity_type="league",
                                    entity_id=sport,
                                    field_name="image_url",
                                    source_url=str(league_image),
                                )
                            )

                        teams = ds.get_teams(sport, season=season)
                        for team in teams:
                            source_url = team.get("logo_url")
                            team_id = str(team.get("id", ""))
                            if not source_url or not team_id:
                                continue
                            media.sync_target(
                                MediaTarget(
                                    sport=sport,
                                    entity_type="team",
                                    entity_id=team_id,
                                    field_name="logo_url",
                                    source_url=str(source_url),
                                )
                            )

                        players = ds.get_players(sport, season=season)
                        for player in players:
                            source_url = player.get("headshot_url")
                            player_id = str(player.get("id", ""))
                            if not source_url or not player_id:
                                continue
                            media.sync_target(
                                MediaTarget(
                                    sport=sport,
                                    entity_type="player",
                                    entity_id=player_id,
                                    field_name="headshot_url",
                                    source_url=str(source_url),
                                )
                            )
                    except Exception:
                        logger.exception("Media mirror sync failed for sport=%s", sport)
            except Exception:
                logger.exception("Media mirror sync loop iteration failed")

            await asyncio.sleep(interval_s)

    maybe_media_task = asyncio.create_task(_media_sync_loop())
    media_sync_task = maybe_media_task

    subscription_lifecycle_task: asyncio.Task | None = None

    async def _subscription_lifecycle_loop() -> None:
        interval_s = int(os.getenv("V5_SUBSCRIPTION_LIFECYCLE_INTERVAL_SECONDS", "3600") or "3600")
        if interval_s <= 0:
            return

        service = SubscriptionLifecycleService()
        logger.info("Subscription lifecycle loop enabled (interval=%ss)", interval_s)
        while True:
            try:
                stats = await service.run_cycle()
                if any(stats.values()):
                    logger.info("Subscription lifecycle processed: %s", stats)
            except Exception:
                logger.exception("Subscription lifecycle loop iteration failed")

            await asyncio.sleep(interval_s)

    subscription_lifecycle_task = asyncio.create_task(_subscription_lifecycle_loop())

    # ── DuckDB deferred refresh loop ─────────────────────────────────
    # When the auto_curated_sync pipeline runs externally and can't acquire
    # the DuckDB write lock (because this server holds it), it writes a
    # `.duckdb_refresh_pending.json` file.  This loop picks it up and
    # refreshes the affected sport views inside the server's connection.
    duckdb_refresh_task: asyncio.Task | None = None

    async def _duckdb_deferred_refresh_loop() -> None:
        """Poll for deferred DuckDB refresh requests from the auto-sync pipeline.

        When the sync pipeline writes new curated parquets and refreshes DuckDB
        views (in its own write connection), it drops a pending file.  This loop
        picks it up and tells the API server's DataServiceDuckDB to reconnect
        in read-only mode so new views become visible immediately.
        """
        from pathlib import Path as _Path
        import json as _json

        poll_s = int(os.getenv("V5_DUCKDB_REFRESH_POLL_SECONDS", "30"))
        if poll_s <= 0:
            return

        pending_file = _Path(settings.normalized_curated_dir).parent / ".duckdb_refresh_pending.json"
        logger.info("DuckDB deferred-refresh loop enabled (poll every %ss)", poll_s)

        while True:
            try:
                if pending_file.exists():
                    payload = _json.loads(pending_file.read_text(encoding="utf-8"))
                    sports_to_refresh = payload.get("sports", [])
                    if sports_to_refresh:
                        logger.info("DuckDB deferred refresh: reconnecting for %s", sports_to_refresh)

                        def _do_refresh():
                            # The sync pipeline already wrote views in its own
                            # write connection.  We just need to reconnect the
                            # API server's read-only connection to see them.
                            ds = get_data_service()
                            if hasattr(ds, "reconnect"):
                                ds.reconnect()

                        await asyncio.to_thread(_do_refresh)
                        pending_file.unlink(missing_ok=True)
                        logger.info("DuckDB deferred refresh complete for %s", sports_to_refresh)
            except Exception:
                logger.exception("DuckDB deferred refresh loop error")

            await asyncio.sleep(poll_s)

    duckdb_refresh_task = asyncio.create_task(_duckdb_deferred_refresh_loop())

    yield

    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    if media_sync_task is not None and not media_sync_task.done():
        media_sync_task.cancel()
        try:
            await media_sync_task
        except asyncio.CancelledError:
            pass
    if subscription_lifecycle_task is not None and not subscription_lifecycle_task.done():
        subscription_lifecycle_task.cancel()
        try:
            await subscription_lifecycle_task
        except asyncio.CancelledError:
            pass
    if duckdb_refresh_task is not None and not duckdb_refresh_task.done():
        duckdb_refresh_task.cancel()
        try:
            await duckdb_refresh_task
        except asyncio.CancelledError:
            pass
    logger.info("Shutting down — clearing cache and closing DB pool.")
    ds = ds_ref.get("value")
    if ds is not None:
        try:
            ds.clear_cache()
        except Exception:
            logger.exception("Data service cache clear failed during shutdown")
    if warm_task is not None and not warm_task.done():
        warm_task.cancel()
        try:
            await warm_task
        except asyncio.CancelledError:
            pass
    await close_pool()


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="SportStock Data API",
        version="5.0.0",
        description=(
            "Professional sports data API providing real-time and historical data "
            "across 30+ sports. Includes games, predictions, odds, player stats, "
            "standings, news, and ML-powered insights.\n\n"
            "## Data Coverage\n"
            "- **Games**: schedules, live scores, final results, venues, broadcasts\n"
            "- **Teams & Players**: rosters, bios, logos, jersey numbers\n"
            "- **Standings**: conference/division rankings, win-loss records, streaks\n"
            "- **Odds**: moneyline, spread, totals from 10+ sportsbooks\n"
            "- **Predictions**: ML model win probabilities, spread forecasts, accuracy tracking\n"
            "- **Player Stats**: per-game box scores with sport-specific stat categories\n"
            "- **Injuries & News**: real-time injury reports and news articles\n"
            "- **Features**: ML feature datasets for data scientists (pro tier+)\n\n"
            "## Discovery Endpoints\n"
            "- **/v1/sports**: live catalog of available sports\n"
            "- **/v1/meta/sports**: per-sport endpoint capability map\n"
            "- **/v1/health**: cache stats, uptime, and data freshness\n\n"
            "## Supported Sports\n"
            "NBA, NFL, MLB, NHL, NCAA Football, NCAA Basketball, major soccer leagues "
            "(EPL, LaLiga, Bundesliga, Serie A), Tennis tours, Golf, MMA/UFC, F1, esports, and more.\n\n"
            "## Authentication\n"
            "Register at /auth/register for a free API key. "
            "Pass your key via `X-API-Key` header or `?api_key=` query parameter.\n\n"
            "## Tiers\n"
            "- **Free**: 100 req/day, NBA only, basic endpoints\n"
            "- **Starter** ($19.99/mo): 1,000 req/day, 4 major US sports\n"
            "- **Pro** ($49.99/mo): 10,000 req/day, all sports, ML features\n"
            "- **Enterprise** ($149.99/mo): 100,000 req/day, priority support"
        ),
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        openapi_tags=TAGS_METADATA,
        lifespan=lifespan,
    )

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        include_internal_docs = os.getenv("V5_INCLUDE_INTERNAL_DOCS", "true").lower() in {"1", "true", "yes"}
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
            tags=TAGS_METADATA,
        )

        if not include_internal_docs:
            paths = schema.get("paths", {})
            for path in list(paths.keys()):
                if any(path.endswith(suffix) for suffix in INTERNAL_DOC_PATH_SUFFIXES):
                    del paths[path]

        _normalize_schema_tags(schema)
        schema["servers"] = [{"url": "/", "description": "Same origin"}]
        app.openapi_schema = schema
        return app.openapi_schema

    app.openapi = custom_openapi

    # ── Middleware (order matters — outermost first) ───────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=5000)
    app.add_middleware(
        RateLimitMiddleware,
        config=RateLimitConfig(
            requests_per_second=20.0,
            burst=60,
            group_limits={
                "/v1/sse": (2.0, 5),
            },
        ),
    )

    # ── Routers ───────────────────────────────────────────
    from api.auth import router as auth_router

    app.include_router(auth_router)
    app.include_router(meta_router)
    app.include_router(sports_router)
    app.include_router(predictions_router)
    app.include_router(features_router)
    app.include_router(live_router)
    app.include_router(paper_router)
    app.include_router(stripe_router)
    app.include_router(autobet_router)

    # Serve mirrored media assets as static files.
    media_dir = settings.media_dir
    media_dir.mkdir(parents=True, exist_ok=True)
    app.mount(settings.media_public_base_path, StaticFiles(directory=str(media_dir)), name="media")

    # ── Root health check ─────────────────────────────────
    @app.get(
        "/health",
        tags=["System"],
        summary="Quick health check",
        description="Lightweight liveness probe. Returns 200 if the application is running.",
        response_model_exclude_none=True,
        responses={
            200: {
                "description": "Service is healthy",
                "content": {
                    "application/json": {
                        "example": {
                            "success": True,
                            "data": {"status": "ok"},
                        }
                    }
                },
            }
        },
    )
    async def health():
        return {"success": True, "data": {"status": "ok"}}

    @app.api_route("/docs", methods=["GET", "HEAD"], include_in_schema=False)
    async def custom_docs(request: Request) -> HTMLResponse:
        if not _is_local_request(request):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
        base_url = str(request.base_url).rstrip("/")
        return HTMLResponse(
            _build_docs_html(
                openapi_url=OPENAPI_PATH,
                oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
                base_url=base_url,
            )
        )

    @app.get(OPENAPI_PATH, include_in_schema=False)
    async def openapi_schema(request: Request) -> dict:
        if not _is_local_request(request):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
        return app.openapi()

    @app.get("/openapi-sellable.json", include_in_schema=False)
    async def sellable_openapi(request: Request) -> dict:
        if not _is_local_request(request):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
        return _build_sellable_openapi(app)

    @app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
    async def swagger_ui_redirect(request: Request) -> HTMLResponse:
        if not _is_local_request(request):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
        return get_swagger_ui_oauth2_redirect_html()

    return app


app = create_app()
