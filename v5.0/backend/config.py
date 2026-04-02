# ──────────────────────────────────────────────────────────
# V5.0 Backend — Configuration
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Paths
    project_root: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parent.parent
    )
    data_dir: Optional[Path] = Field(default=None)
    raw_dir: Optional[Path] = Field(default=None)
    normalized_dir: Optional[Path] = Field(default=None)
    normalized_curated_dir: Optional[Path] = Field(default=None)

    # Data reader backend
    backend_reader: str = "duckdb"  # parquet | duckdb
    duckdb_path: Optional[Path] = Field(default=None)
    duckdb_enabled_sports: str = ""  # comma-separated, empty = all sports
    duckdb_use_curated: bool = True

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    workers: int = 1

    # Auth
    secret_key: str = "dev-secret-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60

    # CORS
    cors_origins: list[str] = [
        "http://localhost:3000", "http://localhost:3001",
        "http://localhost:3002", "http://localhost:3003",
        "http://127.0.0.1:3000", "http://127.0.0.1:3002",
        "http://127.0.0.1:3003",
    ]

    # Cache TTLs (seconds)
    cache_ttl_games: int = 300          # 5 min
    cache_ttl_standings: int = 3600     # 1 hour
    cache_ttl_odds: int = 120           # 2 min
    cache_ttl_players: int = 1800       # 30 min
    cache_ttl_predictions: int = 600    # 10 min
    cache_ttl_stats: int = 900          # 15 min

    model_config = {"env_prefix": "V5_", "env_file": ".env", "extra": "ignore"}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.data_dir is None:
            self.data_dir = self.project_root / "data"
        if self.raw_dir is None:
            self.raw_dir = self.data_dir / "raw"
        if self.normalized_dir is None:
            self.normalized_dir = self.data_dir / "normalized"
        if self.normalized_curated_dir is None:
            self.normalized_curated_dir = self.data_dir / "normalized_curated"
        if self.duckdb_path is None:
            self.duckdb_path = self.data_dir / "normalized.duckdb"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# ──────────────────────────────────────────────────────────
# Sport Definitions — Canonical source of truth
# ──────────────────────────────────────────────────────────

SPORT_DEFINITIONS: dict[str, dict] = {
    "nba":        {"label": "NBA",        "category": "basketball", "country": "US"},
    "wnba":       {"label": "WNBA",       "category": "basketball", "country": "US"},
    "ncaab":      {"label": "NCAAB",      "category": "basketball", "country": "US"},
    "ncaaw":      {"label": "NCAAW",      "category": "basketball", "country": "US"},
    "nfl":        {"label": "NFL",        "category": "football",   "country": "US"},
    "ncaaf":      {"label": "NCAAF",      "category": "football",   "country": "US"},
    "mlb":        {"label": "MLB",        "category": "baseball",   "country": "US"},
    "nhl":        {"label": "NHL",        "category": "hockey",     "country": "US"},
    "epl":        {"label": "EPL",        "category": "soccer",     "country": "GB"},
    "laliga":     {"label": "La Liga",    "category": "soccer",     "country": "ES"},
    "bundesliga": {"label": "Bundesliga", "category": "soccer",     "country": "DE"},
    "seriea":     {"label": "Serie A",    "category": "soccer",     "country": "IT"},
    "ligue1":     {"label": "Ligue 1",    "category": "soccer",     "country": "FR"},
    "mls":        {"label": "MLS",        "category": "soccer",     "country": "US"},
    "ucl":        {"label": "UCL",        "category": "soccer",     "country": "EU"},
    "nwsl":       {"label": "NWSL",       "category": "soccer",     "country": "US"},
    "ligamx":     {"label": "Liga MX",    "category": "soccer",     "country": "MX"},
    "europa":     {"label": "Europa League", "category": "soccer",  "country": "EU"},
    "eredivisie":  {"label": "Eredivisie",  "category": "soccer",  "country": "NL"},
    "primeiraliga":{"label": "Primeira Liga","category": "soccer",  "country": "PT"},
    "championship":{"label": "Championship","category": "soccer",  "country": "GB"},
    "bundesliga2": {"label": "Bundesliga 2","category": "soccer",  "country": "DE"},
    "serieb":      {"label": "Serie B",     "category": "soccer",  "country": "IT"},
    "ligue2":      {"label": "Ligue 2",     "category": "soccer",  "country": "FR"},
    "worldcup":    {"label": "World Cup",   "category": "soccer",  "country": "INT"},
    "euros":       {"label": "Euros",       "category": "soccer",  "country": "EU"},
    "f1":         {"label": "F1",         "category": "motorsport", "country": "INT"},
    "indycar":    {"label": "IndyCar",    "category": "motorsport", "country": "US"},
    "atp":        {"label": "ATP",        "category": "tennis",     "country": "INT"},
    "wta":        {"label": "WTA",        "category": "tennis",     "country": "INT"},
    "ufc":        {"label": "UFC",        "category": "mma",        "country": "US"},
    "lol":        {"label": "LoL",        "category": "esports",    "country": "INT"},
    "csgo":       {"label": "CS2",        "category": "esports",    "country": "INT"},
    "dota2":      {"label": "Dota 2",     "category": "esports",    "country": "INT"},
    "valorant":   {"label": "Valorant",   "category": "esports",    "country": "INT"},
    "golf":       {"label": "PGA",        "category": "golf",       "country": "US"},
    "lpga":       {"label": "LPGA",       "category": "golf",       "country": "US"},
}

ALL_SPORTS = [s for s, d in SPORT_DEFINITIONS.items() if not d.get("disabled")]
DISABLED_SPORTS = [s for s, d in SPORT_DEFINITIONS.items() if d.get("disabled")]


# ──────────────────────────────────────────────────────────
# Sport Season Detection
# ──────────────────────────────────────────────────────────

# Season start months — when the season for "year X" begins
SPORT_SEASON_START = {
    # Fall-start sports: "2024" season starts Oct 2024, ends ~Jun 2025
    "nba": 10, "nhl": 10, "ncaab": 11, "ncaaw": 11, "wnba": 5,
    # Spring/Summer-start sports: "2024" season = Mar-Nov 2024
    "mlb": 3, "mls": 2, "nwsl": 3,
    # Fall-start football: "2024" season = Sep 2024 - Feb 2025
    "nfl": 9, "ncaaf": 8,
    # Year-round: season = calendar year
    "ufc": 1, "atp": 1, "wta": 1, "f1": 3, "indycar": 3,
    # Esports: year-round, season = calendar year
    "dota2": 1, "lol": 1, "csgo": 1, "valorant": 1,
    # Other
    "golf": 1, "lpga": 1,
    # European football: Aug-May
    "epl": 8, "laliga": 8, "bundesliga": 8, "seriea": 8, "ligue1": 8, "ucl": 9,
    "ligamx": 7, "europa": 9,
    "eredivisie": 8, "primeiraliga": 8, "championship": 8,
    "bundesliga2": 8, "serieb": 8, "ligue2": 8,
    "worldcup": 6, "euros": 6,
}


def get_available_seasons(sport: str) -> list[str]:
    """Return seasons that actually exist in the normalized data.

    Scans two locations:
    1. Flat ``games_<year>.parquet`` files in ``normalized_dir/<sport>/``
    2. Hive-partitioned ``season=YYYY`` sub-dirs in
       ``normalized_curated_dir/<sport>/games/`` (and ``standings/`` as fallback)
    """
    settings = get_settings()
    seasons: set[str] = set()

    # 1. Flat parquet layout (legacy)
    sport_dir = settings.normalized_dir / sport
    if sport_dir.is_dir():
        for f in sport_dir.glob("games_*.parquet"):
            s = f.stem.replace("games_", "")
            if s.isdigit():
                seasons.add(s)

    # 2. Curated Hive-partitioned layout: sport/category/season=YYYY/
    if hasattr(settings, "normalized_curated_dir"):
        for category in ("games", "standings"):
            cat_dir = settings.normalized_curated_dir / sport / category
            if cat_dir.is_dir():
                for d in cat_dir.iterdir():
                    if d.is_dir() and d.name.startswith("season="):
                        s = d.name[len("season="):]
                        if s.isdigit():
                            seasons.add(s)
                if seasons:
                    break  # found seasons from first existing category

    return sorted(seasons)


# Cache so we don't re-scan disk on every API call.
_available_seasons_cache: dict[str, tuple[float, list[str]]] = {}
_SEASON_CACHE_TTL = 600  # 10 min


def _cached_available_seasons(sport: str) -> list[str]:
    """Cached wrapper around get_available_seasons."""
    import time

    now = time.monotonic()
    entry = _available_seasons_cache.get(sport)
    if entry and (now - entry[0]) < _SEASON_CACHE_TTL:
        return entry[1]
    result = get_available_seasons(sport)
    _available_seasons_cache[sport] = (now, result)
    return result


def get_current_season(sport: str) -> str:
    """Determine the current season year for a given sport.

    Cross-year leagues use *end-year* labelling (e.g. NBA 2025-26 → "2026").
    Calendar-year sports simply return the current year.

    After computing the season, validates against seasons that actually have
    data on disk.  If the computed season has no completed games yet (e.g.
    preseason projections only), falls back to the previous season.
    """
    now = datetime.now()
    year = now.year
    month = now.month

    # Cross-year sports: if month >= threshold, new season has started → year+1
    CROSS_YEAR_THRESHOLDS = {
        "nba": 10, "nhl": 10,
        "nfl": 9,
        "ncaab": 8, "ncaaf": 8, "ncaaw": 8,
        "epl": 8, "laliga": 8, "bundesliga": 8, "seriea": 8, "ligue1": 8, "ucl": 8,
        "eredivisie": 8, "primeiraliga": 8, "championship": 8,
        "bundesliga2": 8, "serieb": 8, "ligue2": 8,
    }

    if sport in CROSS_YEAR_THRESHOLDS:
        threshold = CROSS_YEAR_THRESHOLDS[sport]
        computed = str(year + 1) if month >= threshold else str(year)
    else:
        # Calendar-year sports: MLB, MLS, NWSL, WNBA, esports, tennis, F1, UFC
        computed = str(year)

    available = _cached_available_seasons(sport)
    if not available:
        return computed

    # If computed season exists AND has completed games, use it.
    if computed in available and _season_has_completed_games(sport, computed):
        return computed

    # Computed season has no completed games yet (preseason/future only).
    # Fall back to the most recent season that has completed games.
    for s in reversed(available):
        if _season_has_completed_games(sport, s):
            return s

    # No completed games anywhere — return most recent available
    return available[-1] if available else computed


def _season_has_completed_games(sport: str, season: str) -> bool:
    """Check if a season has at least one completed game."""
    import time

    cache_key = f"{sport}_{season}"
    now = time.monotonic()
    entry = _completed_games_cache.get(cache_key)
    if entry and (now - entry[0]) < _SEASON_CACHE_TTL:
        return entry[1]

    s = get_settings()
    games_file = s.normalized_dir / sport / f"games_{season}.parquet"
    result = False
    if games_file.exists():
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(games_file, columns=["status"])
            statuses = table.column("status").to_pylist()
            result = any(
                s and str(s).lower() in ("final", "completed", "closed", "ft", "finished")
                for s in statuses[:200]  # check first 200 for speed
            )
        except Exception:
            result = True  # assume valid if we can't check
    elif hasattr(s, "normalized_curated_dir"):
        # Check curated Hive-partitioned layout: sport/games/season=YYYY/
        curated_season_dir = s.normalized_curated_dir / sport / "games" / f"season={season}"
        if curated_season_dir.is_dir():
            # Presence of the directory implies data exists; assume completed.
            result = True
    _completed_games_cache[cache_key] = (now, result)
    return result


_completed_games_cache: dict[str, tuple[float, bool]] = {}


# Season end months — when the season for a given year effectively finishes
SPORT_SEASON_END = {
    # Fall-start sports that end in the next calendar year
    "nba": 6, "nhl": 6, "ncaab": 4, "ncaaw": 4,
    # Spring/Summer-start sports ending same calendar year
    "wnba": 10, "mlb": 11, "mls": 12, "nwsl": 11,
    # Football: cross-year
    "nfl": 2, "ncaaf": 1,
    # Year-round / calendar-year
    "ufc": 12, "atp": 11, "wta": 11, "f1": 12, "indycar": 10,
    "dota2": 12, "lol": 12, "csgo": 12, "valorant": 12,
    "golf": 12, "lpga": 12,
    # European football: cross-year (Aug → May/Jun)
    "epl": 5, "laliga": 5, "bundesliga": 5, "seriea": 5, "ligue1": 5, "ucl": 6,
    "ligamx": 6, "europa": 5,
    "eredivisie": 5, "primeiraliga": 5, "championship": 5,
    "bundesliga2": 5, "serieb": 5, "ligue2": 5,
    "worldcup": 7, "euros": 7,
}


def is_season_active(sport: str) -> bool:
    """Check if a sport currently has games being played.

    Uses season start/end month ranges.  For sports whose season crosses
    the calendar-year boundary (e.g. NFL Sep → Feb), the function checks
    whether the current month falls in either tail of the range.
    """
    now = datetime.now()
    start_month = SPORT_SEASON_START.get(sport, 1)
    end_month = SPORT_SEASON_END.get(sport, 12)

    if start_month <= end_month:
        # Season within a single calendar year (e.g. MLB Mar–Nov)
        return start_month <= now.month <= end_month
    else:
        # Season crosses year boundary (e.g. NFL Sep–Feb)
        return now.month >= start_month or now.month <= end_month
