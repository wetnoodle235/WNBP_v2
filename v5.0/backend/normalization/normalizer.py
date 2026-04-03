# ──────────────────────────────────────────────────────────
# V5.0 Backend — Normalization Pipeline
# ──────────────────────────────────────────────────────────
#
# Reads raw data produced by importers (JSON, CSV, CSV.GZ)
# from ``data/raw/{provider}/{sport}/{season}/...``, validates
# and merges records through Pydantic schemas, and writes
# Apache Parquet files to
# ``data/normalized/{sport}/{data_type}_{season}.parquet``.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import SPORT_DEFINITIONS, get_settings
from api.models.schemas import (
    CATEGORY_STATS_MAP,
    Game,
    Injury,
    MarketSignal,
    News,
    Odds,
    PitcherGameStats,
    Player,
    PlayerProp,
    Prediction,
    BatterGameStats,
    ScheduleFatigue,
    Standing,
    Team,
    TeamGameStats,
    Weather,
    _Base,
)
from normalization.provider_map import providers_for
from normalization.provider_map import providers_for_label

logger = logging.getLogger(__name__)

LoaderFn = Callable[[Path, str, str], list[dict[str, Any]]]


# ── Sport → provider sub-directory mapping ────────────────
# Maps *sport_key* → *provider_name* → sub-directory under
# ``data/raw/{provider}/``.

SPORT_PROVIDER_DIR: dict[str, dict[str, str]] = {
    "nba":        {"espn": "nba",        "nbastats": "nba",        "odds": "nba",  "oddsapi": "nba"},
    "wnba":       {"espn": "wnba",       "nbastats": "wnba",       "odds": "wnba"},
    "ncaab":      {"espn": "ncaab",                                 "odds": "ncaab"},
    "ncaaw":      {"espn": "ncaaw"},
    "nfl":        {"espn": "nfl",                           "odds": "nfl",  "oddsapi": "nfl"},
    "ncaaf":      {"espn": "ncaaf",      "cfbdata": "ncaaf",        "odds": "ncaaf"},
    "mlb":        {"espn": "mlb",        "lahman": "mlb",          "odds": "mlb",  "oddsapi": "mlb",  "mlbstats": "mlb"},
    "nhl":        {"espn": "nhl",        "nhl": "nhl",             "odds": "nhl",  "oddsapi": "nhl"},
    "epl":        {"espn": "epl",        "statsbomb": "epl",       "odds": "epl",  "oddsapi": "epl",  "footballdata": "epl",  "understat": "epl"},
    "laliga":     {"espn": "laliga",     "statsbomb": "laliga",    "odds": "laliga",  "oddsapi": "laliga",  "footballdata": "laliga",  "understat": "laliga"},
    "bundesliga": {"espn": "bundesliga", "statsbomb": "bundesliga","odds": "bundesliga",  "oddsapi": "bundesliga",  "footballdata": "bundesliga",  "understat": "bundesliga"},
    "seriea":     {"espn": "seriea",     "statsbomb": "seriea",    "odds": "seriea",  "oddsapi": "seriea",  "footballdata": "seriea",  "understat": "seriea"},
    "ligue1":     {"espn": "ligue1",     "statsbomb": "ligue1",    "odds": "ligue1",  "oddsapi": "ligue1",  "footballdata": "ligue1",  "understat": "ligue1"},
    "mls":        {"espn": "mls",        "statsbomb": "mls",       "odds": "mls",  "oddsapi": "mls",  "footballdata": "mls"},
    "ucl":        {"espn": "ucl",        "statsbomb": "ucl",       "odds": "ucl",  "oddsapi": "ucl",  "footballdata": "ucl"},
    "nwsl":       {"espn": "nwsl",       "statsbomb": "nwsl"},
    "ligamx":     {"espn": "ligamx"},
    "europa":     {"espn": "europa",     "footballdata": "europa",  "odds": "europa",  "oddsapi": "europa"},
    # Additional top-flight leagues
    "eredivisie":   {"footballdata": "eredivisie",   "espn": "eredivisie",   "odds": "eredivisie",   "oddsapi": "eredivisie"},
    "primeiraliga": {"footballdata": "primeiraliga", "espn": "primeiraliga", "odds": "primeiraliga", "oddsapi": "primeiraliga"},
    # Second divisions
    "championship": {"footballdata": "championship", "espn": "championship", "odds": "championship", "oddsapi": "championship"},
    "bundesliga2":  {"footballdata": "bundesliga2",  "espn": "bundesliga2",  "odds": "bundesliga2",  "oddsapi": "bundesliga2"},
    "serieb":       {"footballdata": "serieb",       "espn": "serieb",       "odds": "serieb",       "oddsapi": "serieb"},
    "ligue2":       {"footballdata": "ligue2",       "espn": "ligue2",       "odds": "ligue2",       "oddsapi": "ligue2"},
    # International tournaments (WC/EC — specific years only)
    "worldcup":     {"footballdata": "worldcup",     "espn": "worldcup"},
    "euros":        {"footballdata": "euros",         "espn": "euros"},
    "f1":         {"ergast": "f1",       "openf1": "f1",      "espn": "f1"},
    "indycar":    {"espn": "indycar"},
    "atp":        {"tennisabstract": "atp",  "espn": "atp",           "odds": "atp"},
    "wta":        {"tennisabstract": "wta",  "espn": "wta"},
    "ufc":        {"ufcstats": "ufc",                               "odds": "ufc"},
    "dota2":      {"opendota": "dota2", "pandascore": "dota2"},
    "golf":       {"espn": "golf"},
    "lpga":       {"espn": "lpga"},
    "lol":        {"pandascore": "lol"},
    "csgo":       {"pandascore": "csgo"},
    "valorant":   {"pandascore": "valorant"},
}


# ── Season format helpers ─────────────────────────────────

def _nbastats_season(season: str) -> str:
    """Convert normalisation season ``'2025'`` → ``'2024-25'`` for NBA Stats.

    ESPN uses start-year convention (folder 2024 = Oct 2024-Jun 2025 season).
    NBA Stats uses hyphenated start-year format (folder 2024-25 = same season).
    Normalization uses end-year (season "2025" = 2024-25 season).
    So normalization season S maps to nbastats ``(S-1)-(S[-2:])``.
    """
    if "-" in season:
        return season
    try:
        year = int(season)
        return f"{year - 1}-{str(year)[-2:]}"
    except ValueError:
        return season


def _provider_base_dir(
    raw_dir: Path, provider: str, sport_dir: str, season: str,
) -> Path:
    """Resolve ``data/raw/{provider}/{sport_dir}/{season_dir}``."""
    if provider == "nbastats":
        season = _nbastats_season(season)
    elif provider == "lahman":
        season = "all"
    elif provider == "odds":
        # Odds collector stores data as raw/odds/{sport}/{date}/ — no season level
        return raw_dir / provider / sport_dir
    elif provider in {"oddsapi", "sgo"}:
        # Canonical odds provider namespace (with legacy fallback):
        # raw/odds/providers/{provider}/{sport}/{season}/...
        canonical = raw_dir / "odds" / "providers" / provider / sport_dir / season
        legacy = raw_dir / provider / sport_dir / season
        if canonical.exists() or not legacy.exists():
            return canonical
        return legacy
    return raw_dir / provider / sport_dir / season


# ── File I/O helpers ──────────────────────────────────────

def _load_json(path: Path) -> Any:
    """Load a JSON file; return *None* when the file is missing."""
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_csv(path: Path, **kwargs: Any) -> pd.DataFrame:
    """Load a CSV (plain or ``.csv.gz``).  Returns an empty DataFrame on miss."""
    if not path.exists():
        return pd.DataFrame()
    compression = "gzip" if path.name.endswith(".gz") else None
    return pd.read_csv(path, compression=compression, low_memory=False, **kwargs)


def _load_cfbdata_json_compat(base: Path, season: str, endpoint: str, filename: str) -> list[dict[str, Any]]:
    """Load CFBData JSON with fallback from new hierarchical to old flat structure.
    
    Tries new structure first (by week/date), then falls back to old flat structure.
    For split files, automatically merges all partitions into a single list.
    
    Args:
        base: Season directory (e.g., `data/raw/cfbdata/ncaaf/2025`)
        season: Season year (e.g., '2025')
        endpoint: Endpoint name (e.g., 'games', 'plays', 'stats')
        filename: Fallback filename in old structure (e.g., 'games.json')
    
    Returns:
        Merged list of records from new or old structure, empty list if not found.
    """
    records: list[dict[str, Any]] = []
    season_dir = base

    # Preferred season-wide endpoint layout: {season}/{endpoint}/{filename}
    endpoint_file = season_dir / endpoint / filename
    endpoint_data = _load_json(endpoint_file)
    if endpoint_data is not None:
        if isinstance(endpoint_data, list):
            return endpoint_data
        if isinstance(endpoint_data, dict):
            return [endpoint_data]
    
    if endpoint == "games":
        # Endpoint-first structure: {season}/games/{seasonType}/week_xx/date/*.json
        for season_type_dir in sorted((season_dir / "games").glob("*")):
            if not season_type_dir.is_dir():
                continue
            for week_dir in sorted(season_type_dir.glob("week_*/*")):
                if not week_dir.is_dir():
                    continue
                for game_file in week_dir.glob("*.json"):
                    data = _load_json(game_file)
                    if isinstance(data, dict):
                        records.append(data)
        if records:
            return records

        # Legacy transition structure: {season}/week/games/date/*.json
        for week_dir in sorted(season_dir.glob("*/games/*")):
            if not week_dir.is_dir():
                continue
            for game_file in week_dir.glob("*.json"):
                data = _load_json(game_file)
                if isinstance(data, dict):
                    records.append(data)
        if records:
            return records
    
    elif endpoint == "plays":
        # Endpoint-first structure: {season}/plays/{seasonType}/week_xx/date/*.json
        for season_type_dir in sorted((season_dir / "plays").glob("*")):
            if not season_type_dir.is_dir():
                continue
            for week_dir in sorted(season_type_dir.glob("week_*/*")):
                if not week_dir.is_dir():
                    continue
                for play_file in week_dir.glob("*.json"):
                    data = _load_json(play_file)
                    if isinstance(data, list):
                        records.extend(data)
                    elif isinstance(data, dict):
                        records.append(data)
        if records:
            return records

        # Legacy transition structure: {season}/week/plays/date/*.json
        for week_dir in sorted(season_dir.glob("*/plays/*")):
            if not week_dir.is_dir():
                continue
            for play_file in week_dir.glob("*.json"):
                data = _load_json(play_file)
                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)
        if records:
            return records
    
    elif endpoint == "stats_player_season":
        # Endpoint-first structure: {season}/stats_player_season/{seasonType}/week_xx/stats.json
        for season_type_dir in sorted((season_dir / "stats_player_season").glob("*")):
            if not season_type_dir.is_dir():
                continue
            for stat_file in sorted(season_type_dir.glob("week_*/stats.json")):
                data = _load_json(stat_file)
                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)
        if records:
            return records

        # Legacy transition structure: {season}/week/stats/*.json
        for week_dir in sorted(season_dir.glob("*/stats")):
            if not week_dir.is_dir():
                continue
            for stat_file in sorted(week_dir.glob("players_*.json")):
                data = _load_json(stat_file)
                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)
        if records:
            return records
    
    elif endpoint == "rankings":
        # Endpoint-first structure: {season}/rankings/{seasonType}/week_xx/rankings.json
        for season_type_dir in sorted((season_dir / "rankings").glob("*")):
            if not season_type_dir.is_dir():
                continue
            for rankings_file in sorted(season_type_dir.glob("week_*/rankings.json")):
                data = _load_json(rankings_file)
                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)
        if records:
            return records

        # Legacy transition structure: {season}/reference/rankings.json
        ref_file = season_dir / "reference" / "rankings.json"
        data = _load_json(ref_file)
        if data:
            if isinstance(data, list):
                return data
            return [data]
    
    elif endpoint == "recruiting":
        # Legacy transition structure: {season}/reference/recruiting.json
        ref_file = season_dir / "reference" / "recruiting.json"
        data = _load_json(ref_file)
        if data:
            if isinstance(data, list):
                return data
            return [data]

    elif endpoint == "games_players":
        for season_type_dir in sorted((season_dir / "games_players").glob("*")):
            if not season_type_dir.is_dir():
                continue
            for week_dir in sorted(season_type_dir.glob("week_*/*")):
                if not week_dir.is_dir():
                    continue
                for game_file in week_dir.glob("*.json"):
                    data = _load_json(game_file)
                    if isinstance(data, list):
                        records.extend(data)
                    elif isinstance(data, dict):
                        records.append(data)
        if records:
            return records

    elif endpoint in (
        "games_teams", "games_media", "ppa_games",
        "stats_game_advanced", "stats_game_havoc", "wp_pregame",
    ):
        # Per-game endpdoint: {endpoint}/{season_type}/week_XX/{date}/{game_id}.json
        for season_type_dir in sorted((season_dir / endpoint).glob("*")):
            if not season_type_dir.is_dir():
                continue
            for week_dir in sorted(season_type_dir.glob("week_*/*")):
                if not week_dir.is_dir():
                    continue
                for game_file in week_dir.glob("*.json"):
                    data = _load_json(game_file)
                    if isinstance(data, list):
                        records.extend(data)
                    elif isinstance(data, dict):
                        records.append(data)
        if records:
            return records

    elif endpoint == "lines":
        for season_type_dir in sorted((season_dir / "lines").glob("*")):
            if not season_type_dir.is_dir():
                continue
            for week_dir in sorted(season_type_dir.glob("week_*/*")):
                if not week_dir.is_dir():
                    continue
                for line_file in week_dir.glob("*.json"):
                    data = _load_json(line_file)
                    if isinstance(data, dict):
                        records.append(data)
                    elif isinstance(data, list):
                        records.extend(data)
        if records:
            return records
    
    # Fallback to old flat structure
    old_file = season_dir / filename
    data = _load_json(old_file)
    if data:
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
    
    return []


def _load_cfbdata_endpoint_json(base: Path, filename: str) -> Any:
    """Load CFBData season-wide endpoint JSON.

    Preferred layout is endpoint-first: {season}/{endpoint}/{filename}
    with fallback to legacy flat layout: {season}/{filename}.
    """
    endpoint = filename.replace(".json", "")
    data = _load_json(base / endpoint / filename)
    if data is not None:
        return data
    return _load_json(base / filename)

def _safe_int(val: Any) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_str(val: Any) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


def _safe_datetime(val: Any) -> str | None:
    """Parse ISO datetime string, returning ISO format for Pydantic datetime."""
    if not val or not isinstance(val, str):
        return None
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        return None


def _normalized_stat_label(label: Any) -> str:
    """Return a compact comparison key for provider stat labels."""
    text = _safe_str(label)
    if not text:
        return ""
    text = text.lower().replace("%", "pct").replace("+/-", "plusminus").replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "", text)


def _build_espn_stat_map(
    labels: Sequence[Any],
    raw_stats: Sequence[Any],
    aliases: dict[str, tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    """Build a tolerant stat lookup map from ESPN label/value arrays.

    Keys include original labels, basic case variants, normalized labels, and
    sport-specific canonical aliases so downstream extraction can tolerate more
    incoming label variants without changing record schemas.
    """
    stat_map: dict[str, Any] = {}
    alias_map = aliases or {}
    for idx, lbl in enumerate(labels):
        if idx >= len(raw_stats):
            continue
        value = raw_stats[idx]
        label = _safe_str(lbl)
        if not label:
            continue

        stat_map.setdefault(label, value)
        stat_map.setdefault(label.upper(), value)
        stat_map.setdefault(label.lower(), value)

        norm = _normalized_stat_label(label)
        if norm:
            stat_map.setdefault(norm, value)
            for canonical in alias_map.get(norm, ()):
                stat_map.setdefault(canonical, value)
    return stat_map


_ESPN_MLB_LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "atbats": ("AB",),
    "hits": ("H",),
    "runs": ("R",),
    "rbis": ("RBI",),
    "runsbattedin": ("RBI",),
    "homeruns": ("HR",),
    "walks": ("BB",),
    "basesonballs": ("BB",),
    "strikeouts": ("K",),
    "so": ("K",),
    "inningspitched": ("IP",),
    "innings": ("IP",),
    "earnedruns": ("ER",),
}

_ESPN_BASKETBALL_LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "fgma": ("FG",),
    "3ptma": ("3PT",),
    "3pma": ("3PT",),
    "3pm3pa": ("3PT",),
    "ftma": ("FT",),
    "minutes": ("MIN",),
    "mins": ("MIN",),
    "tov": ("TO",),
    "turnovers": ("TO",),
    "plusminus": ("+/-",),
}

_ESPN_FOOTBALL_LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "catt": ("C/ATT",),
    "compatt": ("C/ATT",),
    "completionsattempts": ("C/ATT",),
    "targets": ("TGTS",),
    "target": ("TGTS",),
    "rating": ("RTG",),
    "qbrating": ("QBR",),
    "lng": ("LONG",),
    "in20": ("IN 20",),
    "inside20": ("IN 20",),
    "touchbacks": ("TB",),
    "attempts": ("ATT",),
}

_ESPN_HOCKEY_LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "savepct": ("SV%",),
    "svpct": ("SV%",),
    "plusminus": ("+/-",),
    "shotsongoal": ("S",),
    "blocks": ("BS",),
    "hits": ("HT",),
    "powerplaytoi": ("PPTOI",),
    "shorthandedtoi": ("SHTOI",),
}

_ESPN_SOCCER_NAME_ALIASES: dict[str, tuple[str, ...]] = {
    "goals": ("totalGoals",),
    "goalassists": ("goalAssists",),
    "assists": ("goalAssists",),
    "shots": ("totalShots",),
    "shotsontarget": ("shotsOnTarget",),
    "foulscommitted": ("foulsCommitted",),
    "foulssuffered": ("foulsSuffered",),
    "foulsdrawn": ("foulsSuffered",),
    "yellowcards": ("yellowCards",),
    "redcards": ("redCards",),
    "offsides": ("offsides",),
    "owngoals": ("ownGoals",),
    "saves": ("saves",),
    "goalsconceded": ("goalsConceded",),
    "shotsfaced": ("shotsFaced",),
    "passes": ("passes",),
    "passpct": ("passAccuracy",),
    "passaccuracy": ("passAccuracy",),
    "tackles": ("tackles",),
    "interceptions": ("interceptions",),
    "xg": ("xg",),
    "xa": ("xa",),
    "keypasses": ("keyPasses",),
    "dribblescompleted": ("dribblesCompleted",),
    "aerialduelswon": ("aerialDuelsWon",),
}


def _build_espn_name_stat_map(
    stats: Sequence[dict[str, Any]],
    aliases: dict[str, tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    """Build a tolerant stat map from ESPN ``[{name, value}, ...]`` arrays."""
    stat_map: dict[str, Any] = {}
    alias_map = aliases or {}
    for item in stats:
        if not isinstance(item, dict):
            continue
        name = _safe_str(item.get("name"))
        if not name:
            continue
        value = item.get("value")
        stat_map.setdefault(name, value)
        stat_map.setdefault(name.lower(), value)
        stat_map.setdefault(name.upper(), value)
        norm = _normalized_stat_label(name)
        if norm:
            stat_map.setdefault(norm, value)
            for canonical in alias_map.get(norm, ()):  # map normalized variants back to canonical keys
                stat_map.setdefault(canonical, value)
    return stat_map


def _maybe_pct(value: Any) -> float | None:
    """Parse percentages from values like ``'73.4%'`` or ``73.4``."""
    text = _safe_str(value)
    if text and text.endswith("%"):
        text = text[:-1].strip()
    return _safe_float(text if text is not None else value)


def _coalesce_player_stat_aliases(rec: dict[str, Any], category: str) -> None:
    """Normalize common cross-provider stat aliases into canonical schema fields."""

    def _set_if_missing(dest: str, *src_keys: str) -> None:
        if rec.get(dest) is not None:
            return
        for key in src_keys:
            if rec.get(key) is not None:
                rec[dest] = rec[key]
                return

    if category == "basketball":
        _set_if_missing("pts", "points")
        _set_if_missing("reb", "rebounds", "total_rebounds")
        _set_if_missing("ast", "assists")
        _set_if_missing("stl", "steals")
        _set_if_missing("blk", "blocks")
        _set_if_missing("to", "turnovers", "tov")
        _set_if_missing("plus_minus", "plusminus", "+/-")
        _set_if_missing("fgm", "field_goals_made", "fg_made")
        _set_if_missing("fga", "field_goals_attempted", "fg_attempted")
        _set_if_missing("ftm", "free_throws_made", "ft_made")
        _set_if_missing("fta", "free_throws_attempted", "ft_attempted")
        _set_if_missing("three_m", "three_pointers_made", "3pm", "fg3_made")
        _set_if_missing("three_a", "three_pointers_attempted", "3pa", "fg3_attempted")
        _set_if_missing("oreb", "off_reb", "offensive_rebounds")
        _set_if_missing("dreb", "def_reb", "defensive_rebounds")
        _set_if_missing("pf", "personal_fouls")
        _set_if_missing("min", "minutes_played", "mp")
        _set_if_missing("minutes", "min")
        if rec.get("fg_pct") is None:
            rec["fg_pct"] = _maybe_pct(rec.get("field_goal_pct") or rec.get("fg_percentage"))
        if rec.get("ft_pct") is None:
            rec["ft_pct"] = _maybe_pct(rec.get("free_throw_pct") or rec.get("ft_percentage"))
        if rec.get("three_pct") is None:
            rec["three_pct"] = _maybe_pct(rec.get("three_point_pct") or rec.get("3p_percentage"))

    elif category == "football":
        _set_if_missing("pass_yds", "passing_yards")
        _set_if_missing("pass_td", "passing_touchdowns")
        _set_if_missing("pass_att", "passing_attempts")
        _set_if_missing("pass_cmp", "passing_completions")
        _set_if_missing("pass_int", "interceptions_thrown", "ints")
        _set_if_missing("rush_yds", "rushing_yards")
        _set_if_missing("rush_td", "rushing_touchdowns")
        _set_if_missing("rush_att", "rushing_attempts")
        _set_if_missing("rec_yds", "receiving_yards")
        _set_if_missing("rec_td", "receiving_touchdowns")
        _set_if_missing("receptions", "catches")
        _set_if_missing("targets", "target")
        _set_if_missing("fumbles", "fumbles_total", "fumbles_forced")
        _set_if_missing("fumbles_lost", "fumbles_lost_total")
        _set_if_missing("fumbles_rec", "fumbles_recovered")
        _set_if_missing("pass_rating", "rating", "qb_rating")
        _set_if_missing("rec_avg", "receiving_avg", "yards_per_reception")
        _set_if_missing("rec_long", "receiving_long", "longest_reception")
        _set_if_missing("rush_avg", "rushing_avg", "yards_per_carry")
        _set_if_missing("rush_long", "rushing_long", "longest_rush")
        _set_if_missing("kr_no", "kick_return_no", "kick_returns")
        _set_if_missing("kr_yds", "kick_return_yds", "kick_return_yards")
        _set_if_missing("kr_avg", "kick_return_avg", "kick_return_average")
        _set_if_missing("kr_long", "kick_return_long")
        _set_if_missing("pr_no", "punt_return_no", "punt_returns")
        _set_if_missing("pr_yds", "punt_return_yds", "punt_return_yards")
        _set_if_missing("pr_avg", "punt_return_avg", "punt_return_average")

    elif category == "baseball":
        _set_if_missing("ab", "at_bats")
        _set_if_missing("hr", "home_runs")
        _set_if_missing("bb", "walks")
        _set_if_missing("so", "strike_outs", "strikeouts")
        _set_if_missing("sb", "stolen_bases")
        _set_if_missing("avg", "batting_avg", "batting_average")
        _set_if_missing("obp", "on_base_pct", "on_base_percentage")
        _set_if_missing("slg", "slugging_pct", "slugging_percentage")
        _set_if_missing("ops", "onbase_plus_slugging")
        _set_if_missing("innings", "innings_pitched")
        _set_if_missing("earned_runs", "er")
        _set_if_missing("whip", "walks_hits_per_inning")

    elif category == "hockey":
        _set_if_missing("shots", "shots_on_goal")
        _set_if_missing("save_pct", "save_percentage")
        _set_if_missing("toi", "time_on_ice")
        _set_if_missing("pim", "penalty_minutes")
        _set_if_missing("plus_minus", "plusminus")
        _set_if_missing("pp_goals", "power_play_goals")
        _set_if_missing("sh_goals", "short_handed_goals")
        _set_if_missing("blocked_shots", "blocks", "shots_blocked")
        _set_if_missing("goals_against", "ga")
        _set_if_missing("faceoff_pct", "faceoff_percentage", "fo_pct")
        _set_if_missing("pp_toi", "power_play_toi", "pp_time_on_ice")
        _set_if_missing("sh_toi", "short_handed_toi", "sh_time_on_ice")

    elif category == "soccer":
        _set_if_missing("shots_on_target", "shots_on_goal")
        _set_if_missing("fouls", "fouls_committed")
        _set_if_missing("key_passes", "chances_created")
        _set_if_missing("dribbles_completed", "successful_dribbles")
        if rec.get("pass_pct") is None:
            rec["pass_pct"] = _maybe_pct(rec.get("pass_accuracy"))

    elif category == "tennis":
        _set_if_missing("double_faults", "df")
        _set_if_missing("first_serve_pct", "first_serve_percentage")
        _set_if_missing("second_serve_pct", "second_serve_percentage")
        _set_if_missing("break_points_won", "bp_won")
        _set_if_missing("break_points_faced", "bp_faced")
        _set_if_missing("tiebreaks_won", "tb_won")
        _set_if_missing("games_won", "total_games_won")
        _set_if_missing("games_lost", "total_games_lost")
        _set_if_missing("sets_won", "total_sets_won")
        _set_if_missing("sets_lost", "total_sets_lost")

    elif category == "motorsport":
        _set_if_missing("grid_position", "start_position")
        _set_if_missing("finish_position", "position", "end_position")
        _set_if_missing("team_name", "team", "constructor_name")
        _set_if_missing("constructor", "team_name", "team")

    elif category == "mma":
        _set_if_missing("sig_strikes", "sig_strikes_landed", "significant_strikes")
        _set_if_missing("strikes_landed", "total_strikes_landed", "total_strikes")
        _set_if_missing("strikes_attempted", "total_strikes_attempted")
        _set_if_missing("sig_strikes_attempted", "significant_strikes_attempted")
        _set_if_missing("round_finished", "finish_round", "round")
        _set_if_missing("takedowns", "successful_takedowns")
        _set_if_missing("control_time", "ground_time", "ctrl_time")

    elif category == "esports":
        _set_if_missing("damage", "damage_dealt")
        _set_if_missing("first_bloods", "first_blood")
        _set_if_missing("turrets_destroyed", "towers_destroyed")
        _set_if_missing("team_name", "team")
        _set_if_missing("opponent_name", "opponent")

    elif category == "golf":
        _set_if_missing("score", "total_strokes")
        _set_if_missing("score_to_par", "to_par")
        _set_if_missing("rounds", "rounds_played")


def _utc_to_et_date(iso_str: str) -> str | None:
    """Convert a UTC ISO datetime to a US-Eastern date string (YYYY-MM-DD).

    ESPN event dates are in UTC (e.g. ``2024-11-05T00:00Z`` for an evening
    game played Nov 4 ET).  Converting to ET before taking the date gives
    the correct local game date, matching nbastats and other US-centric
    providers.
    """
    if not iso_str or not isinstance(iso_str, str):
        return None
    try:
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        et = dt.astimezone(ZoneInfo("America/New_York"))
        return et.strftime("%Y-%m-%d")
    except (ValueError, TypeError, KeyError):
        return iso_str[:10] if len(iso_str) >= 10 else None


def _extract_broadcast(comp: dict[str, Any]) -> str | None:
    """Extract broadcast network name from ESPN competition data."""
    broadcasts = comp.get("broadcasts", [])
    if broadcasts:
        names = broadcasts[0].get("names", [])
        if names:
            return ", ".join(names)
    geo = comp.get("geoBroadcasts", [])
    if geo:
        media = geo[0].get("media", {})
        return media.get("shortName")
    return None


def _iter_candidate_urls(obj: Any) -> Iterator[str]:
    """Yield URL-like strings found recursively in nested provider payloads."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and key in {"href", "url", "link", "mobile", "web"}:
                yield value
            yield from _iter_candidate_urls(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_candidate_urls(item)


def _is_watch_url(url: str) -> bool:
    """Return True when *url* looks like a user-facing watch/live stream page."""
    if not isinstance(url, str):
        return False
    candidate = url.strip().lower()
    if not candidate.startswith(("http://", "https://")):
        return False

    blocked = (
        "odds", "sportsbook", "draftkings", "fanduel", "betmgm", "caesars", "pointsbet", "wynn",
    )
    if any(tok in candidate for tok in blocked):
        return False

    hints = (
        "watch", "stream", "live", "tv.apple.com", "mlb.com/live", "nba.com/watch", "espn.com/watch",
        "foxsports.com/live", "paramountplus.com/live", "peacocktv.com/sports", "fubo.tv",
    )
    return any(tok in candidate for tok in hints)


def _extract_broadcast_url(*payloads: Any) -> str | None:
    """Extract first plausible live-view URL from one or more provider payload objects."""
    for payload in payloads:
        for url in _iter_candidate_urls(payload):
            if _is_watch_url(url):
                return url
    return None


# ── Merge / Validate / Write helpers ──────────────────────

def _merge_records(
    records_by_provider: dict[str, list[dict[str, Any]]],
    id_field: str,
    providers: list[str],
    sport: str | None = None,
    data_type: str | None = None,
) -> list[dict[str, Any]]:
    """Merge records from multiple providers by *id_field*.

    Iterates providers from lowest to highest priority so that
    higher-priority values overwrite lower-priority ones.  Non-null
    fields from lower-priority providers are preserved when the
    higher-priority record has ``None`` for that field.

    Supports composite keys with ``+`` separator (e.g. ``game_id+player_id``).
    """
    id_fields = id_field.split("+")
    merged: dict[str, dict[str, Any]] = {}
    field_owner: dict[str, dict[str, str]] = {}

    def _rank_for_label(label: str, provider: str) -> int:
        if sport and data_type:
            label_order = providers_for_label(sport, data_type, label)
        else:
            label_order = providers
        try:
            return label_order.index(provider)
        except ValueError:
            return 10_000

    for provider in reversed(providers):
        for rec in records_by_provider.get(provider, []):
            key_parts = [str(rec.get(f, "")) for f in id_fields]
            key = "|".join(key_parts)
            if not key or key == "|" * (len(id_fields) - 1):
                continue
            if key in merged:
                for k, v in rec.items():
                    if v is None:
                        continue
                    owners = field_owner.setdefault(key, {})
                    current_owner = owners.get(k)
                    if current_owner is None:
                        merged[key][k] = v
                        owners[k] = provider
                        continue

                    if _rank_for_label(k, provider) <= _rank_for_label(k, current_owner):
                        merged[key][k] = v
                        owners[k] = provider
            else:
                merged[key] = dict(rec)
                owners: dict[str, str] = {}
                for k, v in rec.items():
                    if v is not None:
                        owners[k] = provider
                field_owner[key] = owners
    return list(merged.values())


def _filter_season_date_range(
    records: list[dict[str, Any]], sport: str, season: str,
) -> list[dict[str, Any]]:
    """Drop games whose date falls outside the expected range for *season*.

    Three conventions exist:

    **End-year** (ESPN basketball/hockey): season "2025" = Oct 2024 – Aug 2025
        (season folder N → data from ~Jul N-1 to ~Aug N)

    **Start-year cross-year** (ESPN/footballdata soccer): season "2025" = Aug 2025 – Jul 2026
        (season folder N → data from ~Jul N to ~Aug N+1)

    **Same-year** (MLB, NFL, NCAAF, etc.): season "2025" = Jan 2025 – Feb 2026

    A generous buffer keeps legitimate pre/post-season games while removing
    clearly wrong-season data (e.g. ESPN scoreboard from next year).
    """
    try:
        yr = int(season)
    except (ValueError, TypeError):
        return records

    # End-year convention: folder N ≈ Jul(N-1) to Aug(N)
    _end_year = {"nhl", "nba", "wnba", "ncaab", "ncaaw"}

    # Start-year cross-year: folder N ≈ Jul(N) to Aug(N+1)
    _start_year_cross = {
        "epl", "laliga", "bundesliga", "seriea", "ligue1", "ucl",
        "mls", "nwsl",
    }

    if sport in _end_year:
        date_min = f"{yr - 1}-07-01"
        date_max = f"{yr}-08-31"
    elif sport in _start_year_cross:
        date_min = f"{yr}-07-01"
        date_max = f"{yr + 1}-08-31"
    else:
        # Same-year sports (MLB, NFL, NCAAF, F1, golf, tennis, MMA, etc.)
        date_min = f"{yr}-01-01"
        date_max = f"{yr + 1}-02-28"

    filtered: list[dict[str, Any]] = []
    dropped = 0
    for rec in records:
        d = str(rec.get("date", ""))[:10]
        if d and (d < date_min or d > date_max):
            dropped += 1
            continue
        filtered.append(rec)

    if dropped:
        logger.info(
            "Filtered %d out-of-range games for %s/%s (kept %d)",
            dropped, sport, season, len(filtered),
        )
    return filtered


def _validate_batch(
    raw: list[dict[str, Any]],
    schema_cls: type[_Base],
    sport: str,
    source: str = "unknown",
) -> list[dict[str, Any]]:
    """Validate a list of dicts through a Pydantic model, dropping bad rows."""
    validated: list[dict[str, Any]] = []
    for idx, rec in enumerate(raw):
        try:
            rec.setdefault("sport", sport)
            rec.setdefault("source", source)
            obj = schema_cls.model_validate(rec)
            validated.append(obj.model_dump())
        except Exception:
            logger.debug(
                "Validation failed for row %d in %s/%s",
                idx, sport, schema_cls.__name__,
            )
    return validated


def _write_parquet(rows: list[dict[str, Any]], dest: Path) -> int:
    """Write *rows* to a Parquet file at *dest*.  Returns row count.

    Enhancements vs original:
    - Drops columns that are entirely null (no information value, saves RAM on load)
    - Uses zstd compression (better ratio than snappy, similar speed)
    - Downcasts float64 → float32 and large int64 → int32 to halve numeric memory
    """
    if not rows:
        return 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)

    # --- Drop all-null columns ---
    # A column where every value is null wastes schema space and loads as 0 values
    # in pandas. For sports parquets this removes ~400 sport-irrelevant columns.
    non_null_cols = [
        name for name in table.schema.names
        if table.column(name).null_count < len(table)
    ]
    if len(non_null_cols) < len(table.schema.names):
        table = table.select(non_null_cols)

    # --- Optimise numeric dtypes ---
    # float64 → float32: halves memory for all stat columns (adequate precision)
    # int64  → int32:    adequate for scores, attendance, etc.
    new_fields = []
    new_columns = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_float64(field.type):
            col = col.cast(pa.float32())
            field = field.with_type(pa.float32())
        elif pa.types.is_int64(field.type):
            col = col.cast(pa.int32())
            field = field.with_type(pa.int32())
        new_fields.append(field)
        new_columns.append(col)
    table = pa.table(new_columns, schema=pa.schema(new_fields))

    pq.write_table(table, dest, compression="zstd", compression_level=9)
    logger.info("Wrote %d rows → %s", len(rows), dest)
    return len(rows)


# ═════════════════════════════════════════════════════════
#  Provider-Specific Loader Functions
#  Each signature:  (base_dir: Path, sport: str, season: str)
#                    → list[dict[str, Any]]
# ═════════════════════════════════════════════════════════


# ── ESPN ──────────────────────────────────────────────────

def _espn_reference_path(base: Path, endpoint: str, legacy_name: str) -> Path:
    candidate = base / "reference" / endpoint / f"{endpoint}.json"
    if candidate.exists():
        return candidate
    index_candidate = base / endpoint / "index.json"
    if index_candidate.exists():
        return index_candidate
    return base / legacy_name


def _espn_team_profile_files(base: Path) -> list[Path]:
    new_files = sorted((base / "teams").glob("*/profile.json"))
    if new_files:
        return new_files
    legacy_dir = base / "teams"
    if legacy_dir.is_dir():
        return sorted(p for p in legacy_dir.glob("*.json") if p.name != "index.json")
    return []


def _espn_team_entity_files(base: Path, file_name: str, legacy_dir_name: str) -> list[Path]:
    new_files = sorted((base / "teams").glob(f"*/{file_name}"))
    if new_files:
        return new_files
    legacy_dir = base / legacy_dir_name
    if legacy_dir.is_dir():
        return sorted(legacy_dir.glob("*.json"))
    return []


def _espn_player_profile_files(base: Path) -> list[Path]:
    new_files = sorted((base / "players").glob("*/profile.json"))
    if new_files:
        return new_files
    legacy_dir = base / "players"
    if legacy_dir.is_dir():
        return sorted(p for p in legacy_dir.glob("*.json") if p.name not in {"index.json", "all_players.json"})
    return []


def _espn_athlete_profile_files(base: Path) -> list[Path]:
    new_files = sorted((base / "athletes").glob("*/profile.json"))
    if new_files:
        return new_files
    legacy_dir = base / "athletes"
    if legacy_dir.is_dir():
        return sorted(p for p in legacy_dir.glob("*.json") if p.name not in {"index.json", "all_athletes.json"})
    return []


def _espn_event_files(base: Path, file_name: str, legacy_dir_name: str, ignored_names: set[str] | None = None) -> list[Path]:
    ignored = ignored_names or set()
    new_files = sorted(base.glob(f"events/*/*/*/*/{file_name}"))
    if new_files:
        return new_files
    legacy_dir = base / legacy_dir_name
    if legacy_dir.is_dir():
        return sorted(p for p in legacy_dir.glob("*.json") if p.name not in ignored)
    return []


def _espn_snapshot_files(base: Path, endpoint: str) -> list[Path]:
    new_files = sorted(base.glob(f"snapshots/{endpoint}/*/*.json"))
    if new_files:
        return new_files
    legacy_dir = base / endpoint
    if legacy_dir.is_dir():
        return sorted(legacy_dir.glob("*.json"))
    return []


def _espn_game_files(base: Path) -> list[Path]:
    return _espn_event_files(base, "game.json", "games", {"all_games.json", "index.json"})


def _espn_odds_files(base: Path) -> list[Path]:
    return _espn_event_files(base, "odds.json", "odds", {"all_odds.json", "index.json"})

def _espn_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    data = _load_json(_espn_reference_path(base, "teams", "teams.json"))
    if data and isinstance(data.get("teams"), list):
        for t in data["teams"]:
            rec: dict[str, Any] = {
                "id": str(t.get("id", "")),
                "name": t.get("displayName", t.get("name", "")),
                "abbreviation": t.get("abbreviation"),
                "city": t.get("location"),
                "color_primary": t.get("color"),
                "color_secondary": t.get("alternateColor"),
            }
            logos = t.get("logos", [])
            if logos:
                rec["logo_url"] = logos[0].get("href")
            records.append(rec)

    # Enrich from individual team detail files
    for p in _espn_team_profile_files(base):
        tdata = _load_json(p)
        if not tdata:
            continue
        team = tdata.get("team", tdata)
        tid = str(team.get("id", tdata.get("teamId", "")))
        existing = next((r for r in records if r["id"] == tid), None)
        if existing is None:
            existing = {"id": tid}
            records.append(existing)
        existing.setdefault("name", team.get("displayName", ""))
        existing.setdefault("abbreviation", team.get("abbreviation"))
        existing.setdefault("city", team.get("location"))
        existing.setdefault("color_primary", team.get("color"))
        existing.setdefault("color_secondary", team.get("alternateColor"))
        logos = team.get("logos", [])
        if logos:
            existing.setdefault("logo_url", logos[0].get("href"))
        venue = team.get("venue")
        if isinstance(venue, dict):
            existing.setdefault("venue_name", venue.get("fullName"))
    return records


def _athlete_to_record(
    athlete: dict[str, Any], team_id: str | None = None,
    team_name: str | None = None,
) -> dict[str, Any]:
    """Convert an ESPN athlete dict into a normalised player record."""
    pos = athlete.get("position")
    if isinstance(pos, dict):
        pos = pos.get("abbreviation")
    jersey = athlete.get("jersey")
    weight_raw = athlete.get("displayWeight") or athlete.get("weight")
    weight = None
    if isinstance(weight_raw, str):
        m = re.match(r"(\d+)", weight_raw)
        weight = int(m.group(1)) if m else None
    elif weight_raw is not None:
        weight = _safe_int(weight_raw)
    headshot = athlete.get("headshot")
    if isinstance(headshot, dict):
        headshot = headshot.get("href")

    # Birth date
    dob = athlete.get("dateOfBirth")
    if isinstance(dob, str) and dob:
        dob = dob[:10]  # trim to YYYY-MM-DD
    else:
        dob = None

    # Birth place
    bp = athlete.get("birthPlace")
    birth_place = None
    if isinstance(bp, dict):
        parts = [bp.get("city"), bp.get("state"), bp.get("country")]
        birth_place = ", ".join(p for p in parts if p) or None
    elif isinstance(bp, str):
        birth_place = bp or None

    # Experience
    exp = athlete.get("experience")
    experience_years = None
    if isinstance(exp, dict):
        experience_years = _safe_int(exp.get("years"))
    elif exp is not None:
        experience_years = _safe_int(exp)

    # College — often a $ref link, sometimes a dict with name
    college = athlete.get("college")
    college_name = None
    if isinstance(college, dict):
        college_name = college.get("name") or college.get("shortName")
    elif isinstance(college, str) and not college.startswith("http"):
        college_name = college or None

    return {
        "id": str(athlete.get("id", "")),
        "name": athlete.get("displayName", athlete.get("fullName", "")),
        "team_id": _safe_str(team_id),
        "team_name": team_name,
        "position": pos,
        "jersey_number": _safe_int(jersey),
        "height": athlete.get("displayHeight"),
        "weight": weight,
        "headshot_url": headshot if isinstance(headshot, str) else None,
        "birth_date": dob,
        "birth_place": birth_place,
        "experience_years": experience_years,
        "college": college_name,
        "age": _safe_int(athlete.get("age")),
    }


def _espn_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    # Golf/F1: players are embedded in game files, not roster/player files
    if sport == "golf":
        return _espn_golf_players(base, sport, season)
    if sport in ("atp", "wta"):
        return _espn_tennis_players(base, sport, season)
    if sport == "f1":
        return _espn_f1_players(base, sport, season)

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    records_by_id: dict[str, dict[str, Any]] = {}

    # 1) Individual player profile files (players/{id}.json)
    for p in _espn_player_profile_files(base):
        data = _load_json(p)
        if not data:
            continue
        athlete = data.get("athlete", data)
        rec = _athlete_to_record(
            athlete, team_id=data.get("teamId"),
        )
        if not rec["id"]:
            rec["id"] = str(data.get("playerId", ""))
        if rec["id"] and rec["id"] not in seen_ids:
            records.append(rec)
            seen_ids.add(rec["id"])
            records_by_id[rec["id"]] = rec

    # 2) Team roster files (rosters/{team_id}.json)
    for p in _espn_team_entity_files(base, "roster.json", "rosters"):
        data = _load_json(p)
        if not data:
            continue
        team_id = str(data.get("teamId", p.parent.name if p.parent != base else p.stem))
        roster_team_name = None
        team_meta = data.get("team")
        if isinstance(team_meta, dict):
            roster_team_name = team_meta.get("displayName") or team_meta.get("name") or team_meta.get("shortDisplayName")
        for group in data.get("athletes", []):
            if isinstance(group, dict) and "items" in group:
                items = group["items"]
            elif isinstance(group, dict) and "id" in group:
                items = [group]
            else:
                items = []
            for athlete in items:
                rec = _athlete_to_record(athlete, team_id=team_id, team_name=roster_team_name)
                if rec["id"] and rec["id"] not in seen_ids:
                    records.append(rec)
                    seen_ids.add(rec["id"])
                    records_by_id[rec["id"]] = rec
                elif rec["id"] and rec["id"] in records_by_id:
                    existing = records_by_id[rec["id"]]
                    for k, v in rec.items():
                        if v is not None and existing.get(k) is None:
                            existing[k] = v

    # 3) Individual athlete detail files (athletes/{id}.json)
    for p in _espn_athlete_profile_files(base):
        try:
            data = _load_json(p)
        except Exception:
            continue
        if not data or not isinstance(data, dict):
            continue
        athlete = data.get("athlete", data)
        aid = str(athlete.get("id", p.parent.name if p.parent != base else p.stem))
        rec = _athlete_to_record(athlete, team_id=None)
        if aid in records_by_id:
            existing = records_by_id[aid]
            for k, v in rec.items():
                if v is not None and existing.get(k) is None:
                    existing[k] = v
        elif aid and aid not in seen_ids:
            team_obj = athlete.get("team")
            if isinstance(team_obj, dict):
                rec["team_id"] = _safe_str(team_obj.get("id")) or rec.get("team_id")
                if not rec.get("team_name"):
                    rec["team_name"] = team_obj.get("displayName") or team_obj.get("name")
            records.append(rec)
            seen_ids.add(aid)
            records_by_id[aid] = rec

    return records


def _espn_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    # Tennis: individual match files
    if sport in ("atp", "wta"):
        return _espn_tennis_games(base, sport, season)

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    # 1) Game detail files (games/ directory)
    for p in _espn_game_files(base):
        data = _load_json(p)
        if not data:
            continue
            event_id = str(data.get("eventId", p.stem))
            summary = data.get("summary", {})
            boxscore = summary.get("boxscore", {})
            # Fallback: some sports (MLS, NWSL) put boxscore at top level
            if not boxscore.get("teams"):
                boxscore = data.get("boxscore", {})
            game_info = summary.get("gameInfo", data.get("gameInfo", {}))
            header = summary.get("header", data.get("header", {}))

            # Defaults from boxscore teams (index 0 = away, 1 = home)
            bs_teams = boxscore.get("teams", [])
            away_td: dict = bs_teams[0].get("team", {}) if len(bs_teams) > 0 else {}
            home_td: dict = bs_teams[1].get("team", {}) if len(bs_teams) > 1 else {}
            home_score: int | None = None
            away_score: int | None = None
            status = "scheduled"
            game_date: str | None = None
            game_start_time: str | None = None

            # Prefer header competitions for scores and status
            competitions = header.get("competitions", [])
            if competitions:
                comp = competitions[0]
                # F1: find the Race competition (not FP1/FP2/FP3/Qual)
                if sport == "f1" and len(competitions) > 1:
                    for c in competitions:
                        ct = c.get("type", {}).get("abbreviation", "")
                        if ct.lower() == "race":
                            comp = c
                            break
                    else:
                        # No Race found — use last competition (usually the race)
                        comp = competitions[-1]
                has_home_away = False
                for c in comp.get("competitors", []):
                    if c.get("homeAway") == "home":
                        home_td = c.get("team", home_td)
                        home_score = _safe_int(c.get("score"))
                        has_home_away = True
                    elif c.get("homeAway") == "away":
                        away_td = c.get("team", away_td)
                        away_score = _safe_int(c.get("score"))
                        has_home_away = True

                # Golf / F1 / individual sports: competitors are athletes, not teams
                if not has_home_away and sport in ("golf", "f1", "indycar", "lpga", "mma", "ufc"):
                    competitors = comp.get("competitors", [])
                    # Sort by order (1 = leader/winner)
                    sorted_comps = sorted(
                        competitors,
                        key=lambda x: _safe_int(x.get("order")) or 9999,
                    )
                    if sorted_comps:
                        winner = sorted_comps[0]
                        runner = sorted_comps[1] if len(sorted_comps) > 1 else {}
                        w_ath = winner.get("athlete", winner.get("team", {}))
                        r_ath = runner.get("athlete", runner.get("team", {}))
                        home_td = {"displayName": w_ath.get("displayName", ""), "id": w_ath.get("id", "")}
                        away_td = {"displayName": r_ath.get("displayName", ""), "id": r_ath.get("id", "")}
                        home_score = _safe_int(winner.get("score"))
                        away_score = _safe_int(runner.get("score"))
                st = comp.get("status", {}).get("type", {}).get("name", "")
                status = {
                    "STATUS_FINAL": "final",
                    "STATUS_FULL_TIME": "final",
                    "STATUS_END_OF_REGULATION": "final",
                    "STATUS_IN_PROGRESS": "in_progress",
                    "STATUS_HALFTIME": "in_progress",
                    "STATUS_FIRST_HALF": "in_progress",
                    "STATUS_SECOND_HALF": "in_progress",
                    "STATUS_OVERTIME": "in_progress",
                    "STATUS_EXTRA_TIME": "in_progress",
                    "STATUS_PENALTIES": "in_progress",
                    "STATUS_SCHEDULED": "scheduled",
                    "STATUS_POSTPONED": "postponed",
                    "STATUS_CANCELED": "cancelled",
                    "STATUS_DELAYED": "postponed",
                    "STATUS_SUSPENDED": "postponed",
                    "STATUS_ABANDONED": "cancelled",
                }.get(st, "scheduled")
                # Fallback: if status type has completed=true, treat as final
                if status == "scheduled" and comp.get("status", {}).get("type", {}).get("completed"):
                    status = "final"
                game_date = _utc_to_et_date(comp.get("date") or "") or (comp.get("date") or "")[:10] or None
                # For racing/event sports: if date is in the future, override erroneous 'final' status
                if status == "final" and game_date:
                    try:
                        from datetime import date as _date
                        if str(game_date) > _date.today().isoformat():
                            status = "scheduled"
                    except Exception:
                        pass
                # Extract start_time from full ISO datetime
                comp_date_str = comp.get("date") or ""
                game_start_time = _safe_datetime(comp_date_str) if len(comp_date_str) > 10 else None

            if not game_date:
                gi_date = game_info.get("date") or ""
                game_date = _utc_to_et_date(gi_date) or gi_date[:10] or None

            venue_obj = game_info.get("venue")
            venue_name = venue_obj.get("fullName") if isinstance(venue_obj, dict) else None
            venue_city = venue_obj.get("address", {}).get("city") if isinstance(venue_obj, dict) else None
            venue_state = venue_obj.get("address", {}).get("state") if isinstance(venue_obj, dict) else None
            venue_country = venue_obj.get("address", {}).get("country") if isinstance(venue_obj, dict) else None

            if not venue_name and competitions:
                comp_venue = competitions[0].get("venue")
                if isinstance(comp_venue, dict):
                    venue_name = comp_venue.get("fullName") or comp_venue.get("name")
                    caddr = comp_venue.get("address", {}) if isinstance(comp_venue.get("address"), dict) else {}
                    venue_city = venue_city or caddr.get("city")
                    venue_state = venue_state or caddr.get("state")
                    venue_country = venue_country or caddr.get("country")

            # Season type and week from header
            _st_code = header.get("season", {}).get("type") or 2
            _st_map = {1: "preseason", 2: "regular", 3: "postseason", 4: "off-season"}
            season_type_str = _st_map.get(int(_st_code), f"type_{_st_code}")
            week_num = _safe_int(header.get("week"))

            rec: dict[str, Any] = {
                "id": event_id,
                "season": str(data.get("season", season)),
                "season_type": season_type_str,
                "week": week_num,
                "date": game_date,
                "start_time": game_start_time,
                "status": status,
                "home_team": home_td.get("displayName", home_td.get("name", "")),
                "away_team": away_td.get("displayName", away_td.get("name", "")),
                "home_team_id": str(home_td.get("id", "")),
                "away_team_id": str(away_td.get("id", "")),
                "home_score": home_score,
                "away_score": away_score,
                "venue": venue_name,
                "venue_city": venue_city,
                "venue_state": venue_state,
                "venue_country": venue_country,
                "broadcast": _extract_broadcast(competitions[0]) if competitions else None,
                "broadcast_url": _extract_broadcast_url(competitions[0], game_info) if competitions else _extract_broadcast_url(game_info),
                "attendance": _safe_int(game_info.get("attendance")),
            }
            # Normalise empty team IDs to None
            if not rec.get("home_team_id"):
                rec["home_team_id"] = None
            if not rec.get("away_team_id"):
                rec["away_team_id"] = None

            # Golf / F1 / IndyCar / LPGA: store race/tournament name and winner info
            if sport in ("golf", "f1", "indycar", "lpga"):
                scoreboard = data.get("scoreboard", {})
                event_obj = summary.get("event", scoreboard)
                tournament_name = (event_obj.get("name") or scoreboard.get("name")
                                   or event_obj.get("shortName") or "")
                rec["race_name"] = tournament_name
                rec["winner_name"] = rec["home_team"]  # winner = home (order=1)
                if sport == "f1":
                    circuit = scoreboard.get("circuit", {})
                    rec["venue"] = circuit.get("fullName") or rec.get("venue")
                elif sport == "indycar":
                    # IndyCar: use venue from competition location field
                    comps_data = scoreboard.get("competitions", [])
                    if comps_data:
                        loc = comps_data[0].get("venue") or comps_data[0].get("location", {})
                        if isinstance(loc, dict):
                            rec["venue"] = loc.get("fullName") or loc.get("name") or rec.get("venue")
                        elif isinstance(loc, str):
                            rec["venue"] = loc or rec.get("venue")

            # Extract quarter/period scores and team stats from header competitors
            if competitions:
                comp = competitions[0]
                for c in comp.get("competitors", []):
                    if c.get("homeAway") == "home":
                        rec.update(_extract_linescores(c, "home", sport=sport))
                        rec.update(_extract_competitor_stats(c, "home"))
                    elif c.get("homeAway") == "away":
                        rec.update(_extract_linescores(c, "away", sport=sport))
                        rec.update(_extract_competitor_stats(c, "away"))

            # Extract team stats from boxscore.teams[].statistics[]
            # (detailed team-level stats: turnovers, steals, blocks, etc.)
            _fill_boxscore_team_level_stats(boxscore, rec, home_td, away_td)

            # Extract team stats from boxscore player totals (fills gaps
            # when header competitors lack statistics, and MLB batting/pitching)
            _fill_boxscore_team_stats(boxscore, rec, home_td, away_td, sport)

            # Consolidate short-form → long-form stat names
            _consolidate_stat_aliases(rec, sport)
            # Compute derived stats (faceoff_pct, shot_pct, fg_pct, etc.)
            _compute_derived_stats(rec, sport)

            records.append(rec)
            seen_ids.add(rec["id"])

    # 2) Merge in scoreboard-derived games (avoid duplicates)
    sb_games = _espn_scoreboard_games(base, sport, season)
    for g in sb_games:
        if g["id"] not in seen_ids:
            records.append(g)
            seen_ids.add(g["id"])

    return records


def _extract_linescores(
    competitor: dict[str, Any], prefix: str, *, sport: str = ""
) -> dict[str, Any]:
    """Extract period/quarter/inning scores from a competitor's linescores array.

    For MLB, stores innings 1-9 as ``{prefix}_i1`` … ``{prefix}_i9`` and
    extra-innings total in ``{prefix}_extras``.
    For NHL, stores periods 1-3 as ``{prefix}_p1`` … ``{prefix}_p3`` and
    overtime in ``{prefix}_ot``.
    For NCAAB/NCAAW (college basketball), stores halves as ``{prefix}_h1_score``
    and ``{prefix}_h2_score`` plus overtime in ``{prefix}_ot``.
    For soccer, stores halves as ``{prefix}_h1_score`` / ``{prefix}_h2_score``.
    For all other sports, stores quarters as ``{prefix}_q1`` … ``{prefix}_q4``
    and overtime in ``{prefix}_ot``.
    """
    result: dict[str, Any] = {}
    linescores = competitor.get("linescores", [])
    if not isinstance(linescores, list):
        return result

    if sport == "mlb":
        extras_total = 0
        has_extras = False
        for idx, ls in enumerate(linescores):
            period = _safe_int(ls.get("period")) or (idx + 1)
            value = _safe_int(ls.get("value")) or _safe_int(ls.get("displayValue"))
            if value is None:
                continue
            if period <= 9:
                result[f"{prefix}_i{period}"] = value
            else:
                extras_total += value
                has_extras = True
        if has_extras:
            result[f"{prefix}_extras"] = extras_total
    elif sport in ("nhl",):
        # Hockey: 3 periods + overtime
        ot_total = 0
        has_ot = False
        for idx, ls in enumerate(linescores):
            period = _safe_int(ls.get("period")) or (idx + 1)
            value = _safe_int(ls.get("value")) or _safe_int(ls.get("displayValue"))
            if value is None:
                continue
            if period <= 3:
                result[f"{prefix}_p{period}"] = value
            else:
                ot_total += value
                has_ot = True
        if has_ot:
            result[f"{prefix}_ot"] = ot_total
    elif sport in ("ncaab",) and len(linescores) <= 3:
        # College basketball: 2 halves + possible overtime
        ot_total = 0
        has_ot = False
        for idx, ls in enumerate(linescores):
            period = _safe_int(ls.get("period")) or (idx + 1)
            value = _safe_int(ls.get("value")) or _safe_int(ls.get("displayValue"))
            if value is None:
                continue
            if period <= 2:
                result[f"{prefix}_h{period}_score"] = value
            else:
                ot_total += value
                has_ot = True
        if has_ot:
            result[f"{prefix}_ot"] = ot_total
    elif sport in ("epl", "laliga", "bundesliga", "seriea", "ligue1", "mls",
                    "ucl", "nwsl", "liga_mx"):
        # Soccer: 2 halves
        for idx, ls in enumerate(linescores):
            period = _safe_int(ls.get("period")) or (idx + 1)
            value = _safe_int(ls.get("value")) or _safe_int(ls.get("displayValue"))
            if value is None:
                continue
            if period <= 2:
                result[f"{prefix}_h{period}_score"] = value
    else:
        # NBA, NFL, NCAAF etc: 4 quarters + overtime
        ot_total = 0
        has_ot = False
        for idx, ls in enumerate(linescores):
            period = _safe_int(ls.get("period")) or (idx + 1)
            value = _safe_int(ls.get("value")) or _safe_int(ls.get("displayValue"))
            if value is None:
                continue
            if period <= 4:
                result[f"{prefix}_q{period}"] = value
            else:
                ot_total += value
                has_ot = True
        if has_ot:
            result[f"{prefix}_ot"] = ot_total
    return result


def _extract_competitor_stats(competitor: dict[str, Any], prefix: str) -> dict[str, Any]:
    """Extract per-game team statistics from a scoreboard competitor."""
    result: dict[str, Any] = {}
    stats_list = competitor.get("statistics", [])
    if not isinstance(stats_list, list):
        return result
    stat_map: dict[str, str] = {
        "rebounds": f"{prefix}_rebounds",
        "assists": f"{prefix}_assists",
        "fieldGoalPct": f"{prefix}_fg_pct",
        "freeThrowPct": f"{prefix}_ft_pct",
        "threePointPct": f"{prefix}_three_pct",
        "threePointFieldGoalPct": f"{prefix}_three_pct",
        "fieldGoalsMade": f"{prefix}_fgm",
        "fieldGoalsAttempted": f"{prefix}_fga",
        "freeThrowsMade": f"{prefix}_ftm",
        "freeThrowsAttempted": f"{prefix}_fta",
        "threePointFieldGoalsMade": f"{prefix}_three_m",
        "threePointFieldGoalsAttempted": f"{prefix}_three_a",
        "turnovers": f"{prefix}_turnovers",
    }
    for s in stats_list:
        name = s.get("name", "")
        col = stat_map.get(name)
        if col:
            result[col] = _safe_float(s.get("displayValue", s.get("value")))
    return result


def _fill_boxscore_team_level_stats(
    boxscore: dict[str, Any],
    rec: dict[str, Any],
    home_td: dict[str, Any],
    away_td: dict[str, Any],
) -> None:
    """Extract team-level stats from ``boxscore.teams[].statistics[]``.

    ESPN game detail files include ``summary.boxscore.teams`` with each team
    having a ``statistics`` array of ``{name, displayValue}`` groups.
    Handles basketball (turnovers, steals, blocks, etc.), football (yards,
    first downs, possession time, etc.), and other sport-specific stats.
    """
    bs_teams = boxscore.get("teams", [])
    if not bs_teams:
        return

    home_id = str(home_td.get("id", ""))
    for team_block in bs_teams:
        team_info = team_block.get("team", {})
        tid = str(team_info.get("id", ""))
        prefix = "home" if tid == home_id else "away"

        # Universal + basketball stats
        stat_map = {
            "turnovers":              f"{prefix}_turnovers",
            "totalTurnovers":         f"{prefix}_turnovers",
            "steals":                 f"{prefix}_steals",
            "blocks":                 f"{prefix}_blocks",
            "totalRebounds":          f"{prefix}_rebounds",
            "offensiveRebounds":      f"{prefix}_offensive_rebounds",
            "defensiveRebounds":      f"{prefix}_defensive_rebounds",
            "assists":                f"{prefix}_assists",
            "fouls":                  f"{prefix}_fouls",
            "technicalFouls":         f"{prefix}_technical_fouls",
            "flagrantFouls":          f"{prefix}_flagrant_fouls",
            "turnoverPoints":         f"{prefix}_turnover_points",
            "fastBreakPoints":        f"{prefix}_fast_break_points",
            "pointsInPaint":          f"{prefix}_points_in_paint",
            "largestLead":            f"{prefix}_largest_lead",
            # NFL stats
            "firstDowns":             f"{prefix}_first_downs",
            "firstDownsPassing":      f"{prefix}_first_downs_passing",
            "firstDownsRushing":      f"{prefix}_first_downs_rushing",
            "firstDownsPenalty":      f"{prefix}_first_downs_penalty",
            "totalOffensivePlays":    f"{prefix}_total_plays",
            "totalYards":             f"{prefix}_total_yards",
            "netPassingYards":        f"{prefix}_passing_yards",
            "rushingYards":           f"{prefix}_rushing_yards",
            "rushingAttempts":        f"{prefix}_rushing_attempts",
            "fumblesLost":            f"{prefix}_fumbles_lost",
            "defensiveTouchdowns":    f"{prefix}_defensive_tds",
            "totalDrives":            f"{prefix}_total_drives",
            # NHL stats
            "penaltyMinutes":         f"{prefix}_penalty_minutes",
            "powerPlayGoals":         f"{prefix}_power_play_goals",
            "powerPlayOpportunities": f"{prefix}_power_play_attempts",
            "shortHandedGoals":       f"{prefix}_shorthanded_goals",
            "shootoutGoals":          f"{prefix}_shootout_goals",
            "blockedShots":           f"{prefix}_blocked_shots",
            "hits":                   f"{prefix}_hits_nhl",
            "takeaways":              f"{prefix}_takeaways",
            "giveaways":              f"{prefix}_giveaways",
            "shotsTotal":             f"{prefix}_shots_on_goal",
            "faceoffsWon":            f"{prefix}_faceoffs_won",
            "penalties":              f"{prefix}_penalties_nhl",
            # Soccer stats
            "totalGoals":             f"{prefix}_total_goals",
            "goalAssists":            f"{prefix}_goal_assists",
            "goalsConceded":          f"{prefix}_goals_conceded",
            "foulsCommitted":         f"{prefix}_fouls",
            "yellowCards":            f"{prefix}_yellow_cards",
            "redCards":               f"{prefix}_red_cards",
            "offsides":               f"{prefix}_offsides",
            "wonCorners":             f"{prefix}_corners",
            "saves":                  f"{prefix}_saves",
            "totalShots":             f"{prefix}_total_shots",
            "shotsOnTarget":          f"{prefix}_shots_on_target",
            "accuratePasses":         f"{prefix}_accurate_passes",
            "totalPasses":            f"{prefix}_total_passes",
            "accurateCrosses":        f"{prefix}_accurate_crosses",
            "totalCrosses":           f"{prefix}_total_crosses",
            "effectiveTackles":       f"{prefix}_effective_tackles",
            "totalTackles":           f"{prefix}_tackles",
            "interceptions":          f"{prefix}_interceptions",
            "effectiveClearance":     f"{prefix}_clearances",
            "totalClearance":         f"{prefix}_total_clearances",
            "totalLongBalls":         f"{prefix}_long_balls",
            "accurateLongBalls":      f"{prefix}_accurate_long_balls",
            "penaltyKickGoals":       f"{prefix}_penalty_goals",
            "penaltyKickShots":       f"{prefix}_penalty_shots",
        }
        pct_map = {
            "fieldGoalPct":              f"{prefix}_fg_pct",
            "threePointFieldGoalPct":    f"{prefix}_three_pct",
            "freeThrowPct":              f"{prefix}_ft_pct",
            "penaltyKillPct":            f"{prefix}_penalty_kill_pct",
            "powerPlayPct":              f"{prefix}_power_play_pct",
            "yardsPerPlay":              f"{prefix}_yards_per_play",
            "yardsPerPass":              f"{prefix}_yards_per_pass",
            "yardsPerRushAttempt":       f"{prefix}_yards_per_rush",
            # Soccer percentages
            "possessionPct":             f"{prefix}_possession",
            "shotPct":                   f"{prefix}_shot_pct",
            "passPct":                   f"{prefix}_pass_pct",
            "crossPct":                  f"{prefix}_cross_pct",
            "tacklePct":                 f"{prefix}_tackle_pct",
            "longballPct":               f"{prefix}_longball_pct",
        }
        combo_map = {
            "fieldGoalsMade-fieldGoalsAttempted": (f"{prefix}_fgm", f"{prefix}_fga"),
            "threePointFieldGoalsMade-threePointFieldGoalsAttempted": (f"{prefix}_three_m", f"{prefix}_three_a"),
            "freeThrowsMade-freeThrowsAttempted": (f"{prefix}_ftm", f"{prefix}_fta"),
            "completionAttempts": (f"{prefix}_completions", f"{prefix}_pass_attempts"),
            "thirdDownEff": (f"{prefix}_third_down_conv", f"{prefix}_third_down_att"),
            "fourthDownEff": (f"{prefix}_fourth_down_conv", f"{prefix}_fourth_down_att"),
            "redZoneAttempts": (f"{prefix}_red_zone_conv", f"{prefix}_red_zone_att"),
        }
        # NFL penalty format: "9-47" → count and yards
        penalty_map = {
            "totalPenaltiesYards": (f"{prefix}_penalties", f"{prefix}_penalty_yards"),
            "sacksYardsLost": (f"{prefix}_sacks_allowed", f"{prefix}_sack_yards_lost"),
        }

        for sg in team_block.get("statistics", []):
            stat_name = sg.get("name", "")
            display_val = sg.get("displayValue", "")

            # Direct numeric stats (only fill if not already set)
            dest = stat_map.get(stat_name)
            if dest and rec.get(dest) is None:
                rec[dest] = _safe_float(display_val)

            # Percentage/rate stats
            pct_dest = pct_map.get(stat_name)
            if pct_dest and rec.get(pct_dest) is None:
                rec[pct_dest] = _safe_float(display_val)

            # Combined "made-attempted" or "completions/attempts" strings
            combo_dest = combo_map.get(stat_name)
            if combo_dest:
                made_key, att_key = combo_dest
                if rec.get(made_key) is None:
                    # Handle both "X-Y" and "X/Y" formats
                    sep = "/" if "/" in str(display_val) else "-"
                    parts = str(display_val).split(sep)
                    if len(parts) == 2:
                        rec[made_key] = _safe_int(parts[0])
                        rec[att_key] = _safe_int(parts[1])

            # Penalty format "count-yards"
            pen_dest = penalty_map.get(stat_name)
            if pen_dest:
                count_key, yards_key = pen_dest
                if rec.get(count_key) is None:
                    parts = str(display_val).split("-")
                    if len(parts) == 2:
                        rec[count_key] = _safe_int(parts[0])
                        rec[yards_key] = _safe_int(parts[1])

            # Possession time "MM:SS" → seconds
            if stat_name == "possessionTime" and rec.get(f"{prefix}_possession_seconds") is None:
                try:
                    parts = str(display_val).split(":")
                    if len(parts) == 2:
                        rec[f"{prefix}_possession_seconds"] = int(parts[0]) * 60 + int(parts[1])
                except (ValueError, TypeError):
                    pass

            # NHL faceoff lost derivation: faceoffsWon + faceoffPercent → faceoffsLost
            if stat_name == "faceoffPercent" and rec.get(f"{prefix}_faceoffs_lost") is None:
                fo_pct = _safe_float(display_val)
                fo_won = rec.get(f"{prefix}_faceoffs_won")
                if fo_pct and fo_pct > 0 and fo_won:
                    total = round(fo_won / (fo_pct / 100))
                    rec[f"{prefix}_faceoffs_lost"] = total - int(fo_won)


def _consolidate_stat_aliases(rec: dict[str, Any], sport: str = "") -> None:
    """Copy short-form stat names to long-form equivalents.

    ESPN boxscore extraction uses abbreviated names (fgm, fga, three_m) while
    the schema also has long-form names (field_goals_made, field_goals_attempted).
    This ensures both are populated so consumers can use either.
    """
    for prefix in ("home", "away"):
        _alias_pairs = [
            (f"{prefix}_fgm", f"{prefix}_field_goals_made"),
            (f"{prefix}_fga", f"{prefix}_field_goals_attempted"),
            (f"{prefix}_three_m", f"{prefix}_three_pointers_made"),
            (f"{prefix}_three_a", f"{prefix}_three_pointers_attempted"),
            (f"{prefix}_ftm", f"{prefix}_free_throws_made"),
            (f"{prefix}_fta", f"{prefix}_free_throws_attempted"),
            (f"{prefix}_blocks", f"{prefix}_blocked_shots"),
            # MLB: rbis → rbi (normalize naming)
            (f"{prefix}_rbis", f"{prefix}_rbi"),
            (f"{prefix}_runs", f"{prefix}_runs_scored"),
            # Soccer: total_shots → shots
            (f"{prefix}_total_shots", f"{prefix}_shots"),
            # Soccer: pass_pct → pass_accuracy
            (f"{prefix}_pass_pct", f"{prefix}_pass_accuracy"),
            # Soccer: accurate_passes → passes_completed
            (f"{prefix}_accurate_passes", f"{prefix}_passes_completed"),
        ]
        for short, long in _alias_pairs:
            if rec.get(long) is None and rec.get(short) is not None:
                rec[long] = rec[short]
            elif rec.get(short) is None and rec.get(long) is not None:
                rec[short] = rec[long]

        # NHL-only: hits_nhl → hits (one-directional, avoid polluting MLB/others)
        if sport == "nhl":
            nhl_hits = f"{prefix}_hits_nhl"
            generic_hits = f"{prefix}_hits"
            if rec.get(generic_hits) is None and rec.get(nhl_hits) is not None:
                rec[generic_hits] = rec[nhl_hits]
            elif rec.get(nhl_hits) is None and rec.get(generic_hits) is not None:
                rec[nhl_hits] = rec[generic_hits]


def _compute_derived_stats(rec: dict[str, Any], sport: str = "") -> None:
    """Compute derived stats from existing fields (faceoff_pct, shot_pct, etc.)."""
    for prefix in ("home", "away"):
        # Sport category flags
        _basketball = sport in ("nba", "ncaab", "wnba", "ncaaw")
        _football = sport in ("nfl", "ncaaf")
        _baseball = sport in ("mlb",)
        _hockey = sport in ("nhl",)
        _soccer = sport in ("epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "nwsl", "ucl")

        # NHL: faceoff_pct from faceoffs_won / (faceoffs_won + faceoffs_lost)
        if _hockey and rec.get(f"{prefix}_faceoff_pct") is None:
            fw = rec.get(f"{prefix}_faceoffs_won")
            fl = rec.get(f"{prefix}_faceoffs_lost")
            if fw is not None and fl is not None:
                total = fw + fl
                if total > 0:
                    rec[f"{prefix}_faceoff_pct"] = round(fw / total * 100, 1)

        # Soccer: shot_pct from shots_on_target / total_shots
        if _soccer and rec.get(f"{prefix}_shot_pct") is None:
            sot = rec.get(f"{prefix}_shots_on_target")
            ts = rec.get(f"{prefix}_total_shots") or rec.get(f"{prefix}_shots")
            if sot is not None and ts is not None and ts > 0:
                rec[f"{prefix}_shot_pct"] = round(sot / ts * 100, 1)

        # Basketball-only: fg_pct from fgm / fga
        if _basketball and rec.get(f"{prefix}_fg_pct") is None:
            fgm = rec.get(f"{prefix}_fgm") or rec.get(f"{prefix}_field_goals_made")
            fga = rec.get(f"{prefix}_fga") or rec.get(f"{prefix}_field_goals_attempted")
            if fgm is not None and fga is not None and fga > 0:
                rec[f"{prefix}_fg_pct"] = round(fgm / fga * 100, 1)

        # Basketball: three_pct from three_m / three_a
        if _basketball and rec.get(f"{prefix}_three_pct") is None:
            tm = rec.get(f"{prefix}_three_m") or rec.get(f"{prefix}_three_pointers_made")
            ta = rec.get(f"{prefix}_three_a") or rec.get(f"{prefix}_three_pointers_attempted")
            if tm is not None and ta is not None and ta > 0:
                rec[f"{prefix}_three_pct"] = round(tm / ta * 100, 1)

        # Basketball: ft_pct from ftm / fta
        if _basketball and rec.get(f"{prefix}_ft_pct") is None:
            ftm = rec.get(f"{prefix}_ftm") or rec.get(f"{prefix}_free_throws_made")
            fta = rec.get(f"{prefix}_fta") or rec.get(f"{prefix}_free_throws_attempted")
            if ftm is not None and fta is not None and fta > 0:
                rec[f"{prefix}_ft_pct"] = round(ftm / fta * 100, 1)

        # MLB: batting_avg from hits / at_bats
        if _baseball and rec.get(f"{prefix}_batting_avg") is None:
            h = rec.get(f"{prefix}_hits")
            ab = rec.get(f"{prefix}_at_bats")
            if h is not None and ab is not None and ab > 0:
                rec[f"{prefix}_batting_avg"] = round(h / ab, 3)

        # Football: completion_pct from completions / pass_attempts
        if _football and rec.get(f"{prefix}_completion_pct") is None:
            comp = rec.get(f"{prefix}_completions")
            att = rec.get(f"{prefix}_pass_attempts")
            if comp is not None and att is not None and att > 0:
                rec[f"{prefix}_completion_pct"] = round(comp / att * 100, 1)

        # Football: third_down_pct from third_down_conv / third_down_att
        if _football and rec.get(f"{prefix}_third_down_pct") is None:
            conv = rec.get(f"{prefix}_third_down_conv")
            att = rec.get(f"{prefix}_third_down_att")
            if conv is not None and att is not None and att > 0:
                rec[f"{prefix}_third_down_pct"] = round(conv / att * 100, 1)

        # Football: fourth_down_pct from fourth_down_conv / fourth_down_att
        if _football and rec.get(f"{prefix}_fourth_down_pct") is None:
            conv = rec.get(f"{prefix}_fourth_down_conv")
            att = rec.get(f"{prefix}_fourth_down_att")
            if conv is not None and att is not None and att > 0:
                rec[f"{prefix}_fourth_down_pct"] = round(conv / att * 100, 1)

        # Football: red_zone_pct from red_zone_conv / red_zone_att
        if _football and rec.get(f"{prefix}_red_zone_pct") is None:
            conv = rec.get(f"{prefix}_red_zone_conv")
            att = rec.get(f"{prefix}_red_zone_att")
            if conv is not None and att is not None and att > 0:
                rec[f"{prefix}_red_zone_pct"] = round(conv / att * 100, 1)

        # Football: h1_score = q1 + q2, h2_score = q3 + q4
        if _football and rec.get(f"{prefix}_h1_score") is None:
            q1 = rec.get(f"{prefix}_q1")
            q2 = rec.get(f"{prefix}_q2")
            if q1 is not None and q2 is not None:
                rec[f"{prefix}_h1_score"] = q1 + q2
        if _football and rec.get(f"{prefix}_h2_score") is None:
            q3 = rec.get(f"{prefix}_q3")
            q4 = rec.get(f"{prefix}_q4")
            if q3 is not None and q4 is not None:
                rec[f"{prefix}_h2_score"] = q3 + q4

        # NBA/NCAAB/WNBA: true_shooting_pct = pts / (2 * (fga + 0.44 * fta))
        if _basketball and rec.get(f"{prefix}_true_shooting_pct") is None:
            pts = rec.get(f"{prefix}_score")
            fga = rec.get(f"{prefix}_fga") or rec.get(f"{prefix}_field_goals_attempted")
            fta = rec.get(f"{prefix}_fta") or rec.get(f"{prefix}_free_throws_attempted")
            if pts is not None and fga is not None and fta is not None:
                denom = 2 * (fga + 0.44 * fta)
                if denom > 0:
                    rec[f"{prefix}_true_shooting_pct"] = round(pts / denom * 100, 1)

        # NBA/NCAAB/WNBA: effective_fg_pct = (fgm + 0.5 * 3pm) / fga
        if _basketball and rec.get(f"{prefix}_effective_fg_pct") is None:
            fgm = rec.get(f"{prefix}_fgm") or rec.get(f"{prefix}_field_goals_made")
            fga = rec.get(f"{prefix}_fga") or rec.get(f"{prefix}_field_goals_attempted")
            tpm = rec.get(f"{prefix}_three_m") or rec.get(f"{prefix}_three_pointers_made")
            if fgm is not None and fga is not None and fga > 0:
                tpm = tpm or 0
                rec[f"{prefix}_effective_fg_pct"] = round((fgm + 0.5 * tpm) / fga * 100, 1)

        # NBA/NCAAB/WNBA: possessions ≈ fga - oreb + tov + 0.44 * fta
        if _basketball and rec.get(f"{prefix}_possessions") is None:
            fga = rec.get(f"{prefix}_fga") or rec.get(f"{prefix}_field_goals_attempted")
            fta = rec.get(f"{prefix}_fta") or rec.get(f"{prefix}_free_throws_attempted")
            oreb = rec.get(f"{prefix}_offensive_rebounds")
            tov = rec.get(f"{prefix}_turnovers")
            if fga is not None and fta is not None and tov is not None:
                oreb = oreb or 0
                rec[f"{prefix}_possessions"] = round(fga - oreb + tov + 0.44 * fta, 1)


        # Soccer: goals_conceded = opponent's score (per-game)
        if _soccer and rec.get(f"{prefix}_goals_conceded") is None:
            opp = "away" if prefix == "home" else "home"
            opp_score = rec.get(f"{opp}_score")
            if opp_score is not None:
                rec[f"{prefix}_goals_conceded"] = int(opp_score)

        # Soccer: clean_sheet = 1 if opponent scored 0
        if _soccer and rec.get(f"{prefix}_clean_sheet") is None:
            opp = "away" if prefix == "home" else "home"
            opp_score = rec.get(f"{opp}_score")
            if opp_score is not None:
                rec[f"{prefix}_clean_sheet"] = 1 if int(opp_score) == 0 else 0

        # Soccer: shot_conversion_rate = goals / total_shots
        if _soccer and rec.get(f"{prefix}_shot_conversion_rate") is None:
            goals = rec.get(f"{prefix}_score")
            shots = rec.get(f"{prefix}_total_shots") or rec.get(f"{prefix}_shots")
            if goals is not None and shots is not None and shots > 0:
                rec[f"{prefix}_shot_conversion_rate"] = round(goals / shots * 100, 1)

        # Soccer: shots_on_target_pct = shots_on_target / total_shots
        if _soccer and rec.get(f"{prefix}_shots_on_target_pct") is None:
            sot = rec.get(f"{prefix}_shots_on_target")
            shots = rec.get(f"{prefix}_total_shots") or rec.get(f"{prefix}_shots")
            if sot is not None and shots is not None and shots > 0:
                rec[f"{prefix}_shots_on_target_pct"] = round(sot / shots * 100, 1)

        # Soccer: pass_accuracy from passes_completed / passes_attempted (if available)
        if _soccer and rec.get(f"{prefix}_pass_accuracy") is None:
            pc = rec.get(f"{prefix}_passes_completed") or rec.get(f"{prefix}_accurate_passes")
            pa = rec.get(f"{prefix}_passes_attempted") or rec.get(f"{prefix}_total_passes")
            if pc is not None and pa is not None and pa > 0:
                rec[f"{prefix}_pass_accuracy"] = round(pc / pa * 100, 1)


        # MLB: whip = (walks + hits) / innings_pitched
        if _baseball and rec.get(f"{prefix}_whip") is None:
            walks = rec.get(f"{prefix}_walks")
            hits = rec.get(f"{prefix}_hits_allowed") or rec.get(f"{prefix}_hits")
            ip = rec.get(f"{prefix}_innings_pitched")
            if walks is not None and hits is not None and ip is not None and ip > 0:
                rec[f"{prefix}_whip"] = round((walks + hits) / ip, 2)

        # MLB: obp = (hits + walks) / (at_bats + walks + sac_flies)
        if _baseball and rec.get(f"{prefix}_obp") is None:
            h = rec.get(f"{prefix}_hits")
            bb = rec.get(f"{prefix}_walks")
            ab = rec.get(f"{prefix}_at_bats")
            sf = rec.get(f"{prefix}_sac_flies") or 0
            if h is not None and bb is not None and ab is not None and (ab + bb + sf) > 0:
                rec[f"{prefix}_obp"] = round((h + bb) / (ab + bb + sf), 3)

        # MLB: slg = total_bases / at_bats
        if _baseball and rec.get(f"{prefix}_slg") is None:
            tb = rec.get(f"{prefix}_total_bases")
            ab = rec.get(f"{prefix}_at_bats")
            if tb is not None and ab is not None and ab > 0:
                rec[f"{prefix}_slg"] = round(tb / ab, 3)

        # MLB: ops = obp + slg
        if _baseball and rec.get(f"{prefix}_ops") is None:
            obp = rec.get(f"{prefix}_obp")
            slg = rec.get(f"{prefix}_slg")
            if obp is not None and slg is not None:
                rec[f"{prefix}_ops"] = round(obp + slg, 3)


        # NHL: save_pct = (shots_against - goals_against) / shots_against
        if _hockey and rec.get(f"{prefix}_save_pct") is None:
            opp = "away" if prefix == "home" else "home"
            sa = rec.get(f"{opp}_shots_on_goal")
            ga = rec.get(f"{prefix}_goals_against") or rec.get(f"{opp}_score")
            if sa is not None and ga is not None and sa > 0:
                rec[f"{prefix}_save_pct"] = round((sa - ga) / sa, 3)

        # NHL: saves = opponent_shots_on_goal - goals_against
        if _hockey and rec.get(f"{prefix}_saves") is None:
            opp = "away" if prefix == "home" else "home"
            sa = rec.get(f"{opp}_shots_on_goal")
            ga = rec.get(f"{prefix}_goals_against") or rec.get(f"{opp}_score")
            if sa is not None and ga is not None:
                saves = sa - int(ga)
                if saves >= 0:
                    rec[f"{prefix}_saves"] = saves

        # NHL: power_play_pct = power_play_goals / power_play_attempts
        if _hockey and rec.get(f"{prefix}_power_play_pct") is None:
            ppg = rec.get(f"{prefix}_power_play_goals")
            ppa = rec.get(f"{prefix}_power_play_attempts")
            if ppg is not None and ppa is not None and ppa > 0:
                rec[f"{prefix}_power_play_pct"] = round(ppg / ppa * 100, 1)

        # NHL: penalty_kill_pct = 1 - (opp_pp_goals / opp_pp_attempts)
        if _hockey and rec.get(f"{prefix}_penalty_kill_pct") is None:
            opp = "away" if prefix == "home" else "home"
            opp_ppg = rec.get(f"{opp}_power_play_goals")
            opp_ppa = rec.get(f"{opp}_power_play_attempts")
            if opp_ppg is not None and opp_ppa is not None and opp_ppa > 0:
                rec[f"{prefix}_penalty_kill_pct"] = round((1 - opp_ppg / opp_ppa) * 100, 1)

        # NHL: shooting_pct = goals / shots_on_goal
        if _hockey and rec.get(f"{prefix}_shooting_pct") is None:
            goals = rec.get(f"{prefix}_score")
            sog = rec.get(f"{prefix}_shots_on_goal")
            if goals is not None and sog is not None and sog > 0:
                rec[f"{prefix}_shooting_pct"] = round(goals / sog * 100, 1)

        # ── Tennis derived stats ──
        _tennis = sport in ("atp", "wta")

        # Tennis: first_serve_pct
        if _tennis and rec.get(f"{prefix}_first_serve_pct") is None:
            fs_in = rec.get(f"{prefix}_first_serves_in") or rec.get(f"{prefix}_first_serve_made")
            fs_total = rec.get(f"{prefix}_first_serves_total") or rec.get(f"{prefix}_total_service_points")
            if fs_in is not None and fs_total is not None and fs_total > 0:
                rec[f"{prefix}_first_serve_pct"] = round(fs_in / fs_total * 100, 1)

        # Tennis: break_point_conversion_pct
        if _tennis and rec.get(f"{prefix}_break_point_conversion_pct") is None:
            bp_won = rec.get(f"{prefix}_break_points_won") or rec.get(f"{prefix}_break_points_converted")
            bp_total = rec.get(f"{prefix}_break_points_total") or rec.get(f"{prefix}_break_points_faced")
            if bp_won is not None and bp_total is not None and bp_total > 0:
                rec[f"{prefix}_break_point_conversion_pct"] = round(bp_won / bp_total * 100, 1)

        # Tennis: break_point_save_pct
        if _tennis and rec.get(f"{prefix}_break_point_save_pct") is None:
            bp_faced = rec.get(f"{prefix}_break_points_against") or rec.get(f"{prefix}_break_points_faced_opp")
            bp_saved = rec.get(f"{prefix}_break_points_saved")
            if bp_saved is not None and bp_faced is not None and bp_faced > 0:
                rec[f"{prefix}_break_point_save_pct"] = round(bp_saved / bp_faced * 100, 1)

        # Tennis: ace_df_ratio = aces / double_faults
        if _tennis and rec.get(f"{prefix}_ace_df_ratio") is None:
            aces = rec.get(f"{prefix}_aces")
            dfs = rec.get(f"{prefix}_double_faults")
            if aces is not None and dfs is not None and dfs > 0:
                rec[f"{prefix}_ace_df_ratio"] = round(aces / dfs, 2)

        # ── Esports derived stats ──
        _esports = sport in ("lol", "csgo", "valorant", "dota2")

        # Esports: kill_death_ratio = kills / deaths
        if _esports and rec.get(f"{prefix}_kill_death_ratio") is None:
            kills = rec.get(f"{prefix}_kills") or rec.get(f"{prefix}_team_kills")
            deaths = rec.get(f"{prefix}_deaths") or rec.get(f"{prefix}_team_deaths")
            if kills is not None and deaths is not None and deaths > 0:
                rec[f"{prefix}_kill_death_ratio"] = round(kills / deaths, 2)

        # Esports LoL/Dota2: gold_diff_per_min (if gold and duration available)
        if sport in ("lol", "dota2") and rec.get(f"{prefix}_gold_per_min") is None:
            gold = rec.get(f"{prefix}_gold") or rec.get(f"{prefix}_total_gold")
            dur = rec.get("duration") or rec.get("game_duration")
            if gold is not None and dur is not None and dur > 0:
                rec[f"{prefix}_gold_per_min"] = round(gold / dur, 1)

        # MLB: k_rate = strikeouts / at_bats (batting K%)
        if _baseball and rec.get(f"{prefix}_k_rate") is None:
            so = rec.get(f"{prefix}_strikeouts")
            ab = rec.get(f"{prefix}_at_bats")
            if so is not None and ab is not None and ab > 0:
                rec[f"{prefix}_k_rate"] = round(so / ab * 100, 1)

        # MLB: bb_rate = walks / plate_appearances (walk%)
        if _baseball and rec.get(f"{prefix}_bb_rate") is None:
            bb = rec.get(f"{prefix}_walks")
            pa = rec.get(f"{prefix}_plate_appearances")
            if bb is not None and pa is not None and pa > 0:
                rec[f"{prefix}_bb_rate"] = round(bb / pa * 100, 1)

        # MLB: iso = slg - batting_avg (isolated power)
        if _baseball and rec.get(f"{prefix}_iso") is None:
            slg = rec.get(f"{prefix}_slg")
            avg = rec.get(f"{prefix}_batting_avg")
            if slg is not None and avg is not None:
                rec[f"{prefix}_iso"] = round(slg - avg, 3)

        # Football: yards_per_play
        if _football and rec.get(f"{prefix}_yards_per_play") is None:
            yards = rec.get(f"{prefix}_total_yards")
            plays = rec.get(f"{prefix}_total_plays")
            if yards is not None and plays is not None and plays > 0:
                rec[f"{prefix}_yards_per_play"] = round(yards / plays, 1)

        # Football: turnover_margin
        if _football and rec.get(f"{prefix}_turnover_margin") is None:
            opp = "away" if prefix == "home" else "home"
            own_to = rec.get(f"{prefix}_turnovers") or 0
            opp_to = rec.get(f"{opp}_turnovers") or 0
            if own_to > 0 or opp_to > 0:
                rec[f"{prefix}_turnover_margin"] = opp_to - own_to

        # Football: total yards advantage
        if _football and rec.get(f"{prefix}_yards_diff") is None:
            opp = "away" if prefix == "home" else "home"
            own_y = rec.get(f"{prefix}_total_yards")
            opp_y = rec.get(f"{opp}_total_yards")
            if own_y is not None and opp_y is not None:
                rec[f"{prefix}_yards_diff"] = int(own_y - opp_y)

        # Football: scoring efficiency = score / total_plays
        if _football and rec.get(f"{prefix}_scoring_efficiency") is None:
            score = rec.get(f"{prefix}_score")
            plays = rec.get(f"{prefix}_total_plays")
            if score is not None and plays is not None and plays > 0:
                rec[f"{prefix}_scoring_efficiency"] = round(score / plays, 3)

        # Basketball: pace = possessions × (game_minutes / minutes_played)
        # Possessions per 48 min (NBA) or 40 min (NCAAB/WNBA/NCAAW)
        if _basketball and rec.get(f"{prefix}_pace") is None:
            poss = rec.get(f"{prefix}_possessions")
            if poss is not None:
                game_min = 40 if sport in ("ncaab", "ncaaw") else 48
                rec[f"{prefix}_pace"] = round(poss * game_min / 20, 1)

        # Basketball: offensive_rating = points / possessions * 100
        if _basketball and rec.get(f"{prefix}_offensive_rating") is None:
            pts = rec.get(f"{prefix}_score")
            poss = rec.get(f"{prefix}_possessions")
            if pts is not None and poss is not None and poss > 0:
                rec[f"{prefix}_offensive_rating"] = round(pts / poss * 100, 1)

        # Basketball: defensive_rating = opp points / opp possessions * 100
        if _basketball and rec.get(f"{prefix}_defensive_rating") is None:
            opp = "away" if prefix == "home" else "home"
            opp_pts = rec.get(f"{opp}_score")
            opp_poss = rec.get(f"{opp}_possessions")
            if opp_pts is not None and opp_poss is not None and opp_poss > 0:
                rec[f"{prefix}_defensive_rating"] = round(opp_pts / opp_poss * 100, 1)

        # Basketball: net_rating = off_rating - def_rating
        if _basketball and rec.get(f"{prefix}_net_rating") is None:
            off_r = rec.get(f"{prefix}_offensive_rating")
            def_r = rec.get(f"{prefix}_defensive_rating")
            if off_r is not None and def_r is not None:
                rec[f"{prefix}_net_rating"] = round(off_r - def_r, 1)

        # Hockey: goals_per_shot = score / shots_on_goal
        if _hockey and rec.get(f"{prefix}_goals_per_shot") is None:
            goals = rec.get(f"{prefix}_score")
            sog = rec.get(f"{prefix}_shots_on_goal")
            if goals is not None and sog is not None and sog > 0:
                rec[f"{prefix}_goals_per_shot"] = round(goals / sog, 3)

        # Soccer: goals_per_shot = score / total_shots
        if _soccer and rec.get(f"{prefix}_goals_per_shot") is None:
            goals = rec.get(f"{prefix}_score")
            shots = rec.get(f"{prefix}_total_shots") or rec.get(f"{prefix}_shots")
            if goals is not None and shots is not None and shots > 0:
                rec[f"{prefix}_goals_per_shot"] = round(goals / shots, 3)

        # Tennis: sets_won derived from home_score / away_score where score = sets
        if _tennis:
            # home_score / away_score in tennis parquets represent sets won
            if rec.get("home_sets_won") is None and rec.get("home_score") is not None:
                rec["home_sets_won"] = int(rec["home_score"])
            if rec.get("away_sets_won") is None and rec.get("away_score") is not None:
                rec["away_sets_won"] = int(rec["away_score"])

    # ── Universal post-loop derived stats (cross-team) ──
    hs = rec.get("home_score")
    as_ = rec.get("away_score")

    # result, score_diff, total_score
    if hs is not None and as_ is not None:
        if rec.get("result") is None:
            if hs > as_:
                rec["result"] = "home_win"
            elif as_ > hs:
                rec["result"] = "away_win"
            else:
                rec["result"] = "draw"
        if rec.get("score_diff") is None:
            rec["score_diff"] = int(hs - as_)
        if rec.get("total_score") is None:
            rec["total_score"] = int(hs + as_)

    # overtime flag
    if rec.get("overtime") is False or rec.get("overtime") is None:
        sport_key = rec.get("sport", "")
        has_ot = (rec.get("home_ot") or 0) > 0 or (rec.get("away_ot") or 0) > 0
        # Hockey/basketball OT also visible via period count
        has_ot_nhl = sport_key in ("nhl",) and rec.get("home_p3") is not None and (
            # if periods beyond 3 have scores, it went to OT
            any(rec.get(f"home_p{p}") is not None for p in [4, 5])
        )
        rec["overtime"] = bool(has_ot or has_ot_nhl)

    # day_of_week and is_weekend from date field
    if rec.get("day_of_week") is None:
        raw_date = rec.get("date")
        if raw_date is not None:
            try:
                import datetime as _dt
                if isinstance(raw_date, str):
                    d = _dt.date.fromisoformat(raw_date[:10])
                elif hasattr(raw_date, "timetuple"):
                    d = raw_date if isinstance(raw_date, _dt.date) else raw_date.date()
                else:
                    d = None
                if d is not None:
                    rec["day_of_week"] = d.weekday()   # 0=Mon, 6=Sun
                    rec["is_weekend"] = d.weekday() >= 5
            except (ValueError, AttributeError):
                pass

    # Soccer: xg_diff and xg_total
    hxg = rec.get("home_xg")
    axg = rec.get("away_xg")
    if hxg is not None and axg is not None:
        if rec.get("xg_diff") is None:
            rec["xg_diff"] = round(hxg - axg, 2)
        if rec.get("xg_total") is None:
            rec["xg_total"] = round(hxg + axg, 2)




def _fill_boxscore_team_stats(
    boxscore: dict[str, Any],
    rec: dict[str, Any],
    home_td: dict[str, Any],
    away_td: dict[str, Any],
    sport: str = "",
) -> None:
    """Extract team aggregate stats from boxscore player totals.

    ESPN game detail files store per-player stats with a ``totals`` array
    at the team level.  The keys array tells us what each index means.
    This fills ``rec`` in-place for rebounds, assists, turnovers, etc.
    """
    players_groups = boxscore.get("players", [])
    if not players_groups:
        return

    home_id = str(home_td.get("id", ""))
    for pg in players_groups:
        team = pg.get("team", {})
        tid = str(team.get("id", ""))
        prefix = "home" if tid == home_id else "away"

        for stat_group in pg.get("statistics", []):
            keys = stat_group.get("keys", [])
            totals = stat_group.get("totals", [])
            if not keys or not totals:
                continue

            # Use min length — ESPN sometimes has more keys than totals
            # (e.g. NCAAF passing has adjQBR key with no total value)
            stat_lookup = dict(zip(keys[:len(totals)], totals[:len(keys)]))
            key_map = {
                "rebounds":    f"{prefix}_rebounds",
                "assists":     f"{prefix}_assists",
                "turnovers":   f"{prefix}_turnovers",
                "steals":      f"{prefix}_steals",
                "blocks":      f"{prefix}_blocks",
            }
            for src_key, dest_key in key_map.items():
                if rec.get(dest_key) is None and src_key in stat_lookup:
                    rec[dest_key] = _safe_int(stat_lookup[src_key])

            # Parse combined stat strings like "50-99" → made, attempted
            combo_map = {
                "fieldGoalsMade-fieldGoalsAttempted": (f"{prefix}_fgm", f"{prefix}_fga"),
                "threePointFieldGoalsMade-threePointFieldGoalsAttempted": (f"{prefix}_three_m", f"{prefix}_three_a"),
                "freeThrowsMade-freeThrowsAttempted": (f"{prefix}_ftm", f"{prefix}_fta"),
            }
            for combo_key, (made_key, att_key) in combo_map.items():
                if rec.get(made_key) is None and combo_key in stat_lookup:
                    parts = str(stat_lookup[combo_key]).split("-")
                    if len(parts) == 2:
                        rec[made_key] = _safe_int(parts[0])
                        rec[att_key] = _safe_int(parts[1])
                        # Calculate percentage
                        made = rec[made_key]
                        att = rec[att_key]
                        if made is not None and att and att > 0:
                            pct_key = {
                                f"{prefix}_fgm": f"{prefix}_fg_pct",
                                f"{prefix}_three_m": f"{prefix}_three_pct",
                                f"{prefix}_ftm": f"{prefix}_ft_pct",
                            }.get(made_key)
                            if pct_key and rec.get(pct_key) is None:
                                rec[pct_key] = round(made / att * 100, 1)

            # MLB batting totals (from player-level totals row)
            is_mlb = sport == "mlb"
            is_football = sport in ("nfl", "ncaaf")
            is_basketball = sport in ("nba", "ncaab", "ncaaw", "wnba")

            if is_mlb and ("atBats" in stat_lookup or "hits-atBats" in stat_lookup):
                mlb_batting = {
                    "atBats": f"{prefix}_at_bats",
                    "runs": f"{prefix}_runs",
                    "hits": f"{prefix}_hits",
                    "RBIs": f"{prefix}_rbis",
                    "homeRuns": f"{prefix}_home_runs",
                    "walks": f"{prefix}_walks",
                    "strikeouts": f"{prefix}_strikeouts",
                    "stolenBases": f"{prefix}_stolen_bases",
                }
                for src_key, dest_key in mlb_batting.items():
                    if rec.get(dest_key) is None and src_key in stat_lookup:
                        rec[dest_key] = _safe_int(stat_lookup[src_key])
                # hits-atBats combo: "14-39"
                if "hits-atBats" in stat_lookup and rec.get(f"{prefix}_hits") is None:
                    parts = str(stat_lookup["hits-atBats"]).split("-")
                    if len(parts) == 2:
                        rec[f"{prefix}_hits"] = _safe_int(parts[0])
                        rec[f"{prefix}_at_bats"] = _safe_int(parts[1])
                # Batting averages from totals row (may be empty for team totals)
                for avg_key, dest in [
                    ("avg", f"{prefix}_batting_avg"),
                    ("onBasePct", f"{prefix}_obp"),
                    ("slugAvg", f"{prefix}_slg"),
                ]:
                    if rec.get(dest) is None and avg_key in stat_lookup:
                        rec[dest] = _safe_float(stat_lookup[avg_key])
                # Derive batting_avg from hits/atBats if ESPN totals row was empty
                ab = rec.get(f"{prefix}_at_bats")
                h = rec.get(f"{prefix}_hits")
                w = rec.get(f"{prefix}_walks")
                if rec.get(f"{prefix}_batting_avg") is None and h is not None and ab and ab > 0:
                    rec[f"{prefix}_batting_avg"] = round(h / ab, 3)
                if rec.get(f"{prefix}_obp") is None and h is not None and ab and ab > 0:
                    total_on = (h or 0) + (w or 0)
                    total_pa = ab + (w or 0)
                    if total_pa > 0:
                        rec[f"{prefix}_obp"] = round(total_on / total_pa, 3)

            # MLB pitching totals
            if is_mlb and ("earnedRuns" in stat_lookup or "fullInnings.partInnings" in stat_lookup):
                mlb_pitching = {
                    "earnedRuns": f"{prefix}_earned_runs",
                    "walks": f"{prefix}_pitching_walks",
                    "homeRuns": f"{prefix}_pitching_home_runs",
                }
                for src_key, dest_key in mlb_pitching.items():
                    if rec.get(dest_key) is None and src_key in stat_lookup:
                        rec[dest_key] = _safe_int(stat_lookup[src_key])
                # Pitching strikeouts (K)
                if rec.get(f"{prefix}_pitching_strikeouts") is None and "strikeouts" in stat_lookup:
                    rec[f"{prefix}_pitching_strikeouts"] = _safe_int(stat_lookup["strikeouts"])
                # Innings pitched from "fullInnings.partInnings" ("9.0")
                if rec.get(f"{prefix}_innings_pitched") is None and "fullInnings.partInnings" in stat_lookup:
                    rec[f"{prefix}_innings_pitched"] = _safe_float(stat_lookup["fullInnings.partInnings"])
                # pitches-strikes combo from pitching group ("129-76")
                if "pitches-strikes" in stat_lookup:
                    parts = str(stat_lookup["pitches-strikes"]).split("-")
                    if len(parts) == 2:
                        if rec.get(f"{prefix}_pitches") is None:
                            rec[f"{prefix}_pitches"] = _safe_int(parts[0])
                        strikes_val = _safe_int(parts[1])
                        if rec.get(f"{prefix}_strikes") is None and strikes_val and strikes_val > 0:
                            rec[f"{prefix}_strikes"] = strikes_val
                # Derive ERA: earned_runs / innings_pitched × 9
                er = rec.get(f"{prefix}_earned_runs")
                ip = rec.get(f"{prefix}_innings_pitched")
                if rec.get(f"{prefix}_era") is None and er is not None and ip and ip > 0:
                    rec[f"{prefix}_era"] = round(er / ip * 9, 2)

            # Football (NFL/NCAAF) player totals — passing, rushing, receiving, kicking
            if is_football:
                football_passing = {
                    "passingYards": f"{prefix}_passing_yards",
                    "passingTouchdowns": f"{prefix}_passing_touchdowns",
                    "interceptions": f"{prefix}_interceptions_thrown",
                }
                for src_key, dest_key in football_passing.items():
                    if rec.get(dest_key) is None and src_key in stat_lookup:
                        rec[dest_key] = _safe_int(stat_lookup[src_key])
                # completions/passingAttempts combo: "22/40"
                if "completions/passingAttempts" in stat_lookup:
                    parts = str(stat_lookup["completions/passingAttempts"]).split("/")
                    if len(parts) == 2:
                        if rec.get(f"{prefix}_completions") is None:
                            rec[f"{prefix}_completions"] = _safe_int(parts[0])
                        if rec.get(f"{prefix}_pass_attempts") is None:
                            rec[f"{prefix}_pass_attempts"] = _safe_int(parts[1])

                football_rushing = {
                    "rushingYards": f"{prefix}_rushing_yards",
                    "rushingAttempts": f"{prefix}_rushing_attempts",
                    "rushingTouchdowns": f"{prefix}_rushing_touchdowns",
                }
                for src_key, dest_key in football_rushing.items():
                    if rec.get(dest_key) is None and src_key in stat_lookup:
                        rec[dest_key] = _safe_int(stat_lookup[src_key])

                football_receiving = {
                    "receivingYards": f"{prefix}_receiving_yards",
                }
                for src_key, dest_key in football_receiving.items():
                    if rec.get(dest_key) is None and src_key in stat_lookup:
                        rec[dest_key] = _safe_int(stat_lookup[src_key])

                football_defense = {
                    "totalTackles": f"{prefix}_tackles",
                    "sacks": f"{prefix}_sacks",
                }
                for src_key, dest_key in football_defense.items():
                    if rec.get(dest_key) is None and src_key in stat_lookup:
                        rec[dest_key] = _safe_float(stat_lookup[src_key])

                football_fumbles = {
                    "fumblesLost": f"{prefix}_fumbles_lost",
                }
                for src_key, dest_key in football_fumbles.items():
                    if rec.get(dest_key) is None and src_key in stat_lookup:
                        rec[dest_key] = _safe_int(stat_lookup[src_key])

                # kicking: fieldGoalsMade/fieldGoalAttempts "2/3"
                if "fieldGoalsMade/fieldGoalAttempts" in stat_lookup and rec.get(f"{prefix}_field_goals_made") is None:
                    parts = str(stat_lookup["fieldGoalsMade/fieldGoalAttempts"]).split("/")
                    if len(parts) == 2:
                        rec[f"{prefix}_field_goals_made"] = _safe_int(parts[0])
                        rec[f"{prefix}_field_goals_attempted"] = _safe_int(parts[1])
                # extraPointsMade/extraPointAttempts "5/5"
                if "extraPointsMade/extraPointAttempts" in stat_lookup and rec.get(f"{prefix}_extra_points_made") is None:
                    parts = str(stat_lookup["extraPointsMade/extraPointAttempts"]).split("/")
                    if len(parts) == 2:
                        rec[f"{prefix}_extra_points_made"] = _safe_int(parts[0])
                        rec[f"{prefix}_extra_points_attempted"] = _safe_int(parts[1])

                # punting
                if rec.get(f"{prefix}_punt_yards") is None and "puntYards" in stat_lookup:
                    rec[f"{prefix}_punt_yards"] = _safe_int(stat_lookup["puntYards"])


def _espn_sb_season_slug(event: dict) -> str:
    """Safely extract season type slug from an ESPN scoreboard event."""
    s = event.get("season")
    if not isinstance(s, dict):
        return ""
    t = s.get("type")
    if isinstance(t, dict):
        return str(t.get("slug") or "").lower()
    # season.slug is the readable slug; season.type is the int code
    return str(s.get("slug") or "").lower()


def _espn_scoreboard_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract games from ESPN daily scoreboard files, including quarter scores and team stats."""
    sb_files = _espn_snapshot_files(base, "scoreboard")
    if not sb_files:
        return []
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for sb_file in sb_files:
        data = _load_json(sb_file)
        if not data:
            continue
        for event in data.get("events", []):
            eid = str(event.get("id", ""))
            if not eid or eid in seen_ids:
                continue
            seen_ids.add(eid)
            comps = event.get("competitions", [{}])
            c = comps[0] if comps else {}
            competitors = c.get("competitors", [])
            home = next((x for x in competitors if x.get("homeAway") == "home"), {})
            away = next((x for x in competitors if x.get("homeAway") == "away"), {})
            ht = home.get("team", {})
            at = away.get("team", {})
            status_raw = event.get("status", {}).get("type", {})
            status_name = status_raw.get("name", "")
            _completed = status_raw.get("completed", False)
            status_map = {
                "STATUS_FINAL": "final",
                "STATUS_FULL_TIME": "final",
                "STATUS_END_OF_REGULATION": "final",
                "STATUS_IN_PROGRESS": "in_progress",
                "STATUS_HALFTIME": "in_progress",
                "STATUS_FIRST_HALF": "in_progress",
                "STATUS_SECOND_HALF": "in_progress",
                "STATUS_OVERTIME": "in_progress",
                "STATUS_EXTRA_TIME": "in_progress",
                "STATUS_PENALTIES": "in_progress",
                "STATUS_SCHEDULED": "scheduled",
                "STATUS_POSTPONED": "postponed",
                "STATUS_CANCELED": "cancelled",
                "STATUS_DELAYED": "postponed",
                "STATUS_SUSPENDED": "postponed",
                "STATUS_ABANDONED": "cancelled",
            }
            game_status = status_map.get(status_name, "scheduled")
            if game_status == "scheduled" and _completed:
                game_status = "final"
            rec: dict[str, Any] = {
                "source": "espn",
                "id": eid,
                "sport": sport,
                "season": season,
                "date": _utc_to_et_date(event.get("date", "")) or event.get("date", "")[:10],
                "start_time": _safe_datetime(event.get("date", "")),
                "status": game_status,
                "home_team": ht.get("displayName") or ht.get("shortDisplayName", ""),
                "away_team": at.get("displayName") or at.get("shortDisplayName", ""),
                "home_team_id": str(ht.get("id", "")),
                "away_team_id": str(at.get("id", "")),
                "home_score": _safe_int(home.get("score")),
                "away_score": _safe_int(away.get("score")),
                "venue": c.get("venue", {}).get("fullName"),
                "broadcast": _extract_broadcast(c),
                "broadcast_url": _extract_broadcast_url(c, event),
                "attendance": _safe_int(c.get("attendance")),
                "season_type": (_sb_st_map := {"preseason": "preseason", "regular-season": "regular", "post-season": "postseason", "postseason": "postseason"}).get(
                    _espn_sb_season_slug(event), "regular"
                ),
                "week": _safe_int((event.get("week") or {}).get("number") if isinstance(event.get("week"), dict) else event.get("week")),
            }
            # Normalise empty team IDs to None
            if not rec.get("home_team_id"):
                rec["home_team_id"] = None
            if not rec.get("away_team_id"):
                rec["away_team_id"] = None

            # Quarter/period/inning scores
            rec.update(_extract_linescores(home, "home", sport=sport))
            rec.update(_extract_linescores(away, "away", sport=sport))
            # Per-game team stats
            rec.update(_extract_competitor_stats(home, "home"))
            rec.update(_extract_competitor_stats(away, "away"))
            records.append(rec)
    return records


def _espn_injuries(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    top_level_files = _espn_snapshot_files(base, "injuries")
    team_files = _espn_team_entity_files(base, "injuries.json", "injuries")
    _status_map = {
        "out": "out", "o": "out",
        "doubtful": "doubtful", "d": "doubtful",
        "questionable": "questionable", "q": "questionable",
        "probable": "probable", "p": "probable",
        "day-to-day": "day_to_day", "dtd": "day_to_day",
    }
    for injury_file in top_level_files:
        data = _load_json(injury_file)
        if not data:
            continue
        wrapper = data.get("injuries", data)
        team_list: list = (
            wrapper.get("injuries", []) if isinstance(wrapper, dict) else
            wrapper if isinstance(wrapper, list) else []
        )
        for team_entry in team_list:
            team_id = str(team_entry.get("id", ""))
            for inj in team_entry.get("injuries", []):
                athlete = inj.get("athlete", {})
                raw_status = (inj.get("status") or "unknown").lower()
                rec: dict[str, Any] = {
                    "player_id": str(athlete.get("id", inj.get("id", ""))),
                    "player_name": athlete.get("displayName", ""),
                    "team_id": team_id,
                    "status": _status_map.get(raw_status, raw_status),
                    "description": inj.get("longComment") or inj.get("shortComment"),
                }
                details = inj.get("details")
                if isinstance(details, dict):
                    rec["body_part"] = details.get("type")
                records.append(rec)

    for injury_file in team_files:
        data = _load_json(injury_file)
        if not data:
            continue
        team_id = str(data.get("teamId", injury_file.parent.name if injury_file.parent != base else injury_file.stem))
        wrapper = data.get("injuries", data)
        if isinstance(wrapper, dict) and isinstance(wrapper.get("injuries"), list):
            team_injuries = wrapper.get("injuries", [])
        elif isinstance(wrapper, list):
            team_injuries = wrapper
        else:
            team_injuries = []
        for inj in team_injuries:
            athlete = inj.get("athlete", {}) if isinstance(inj, dict) else {}
            raw_status = (inj.get("status") if isinstance(inj, dict) else "unknown") or "unknown"
            raw_status = str(raw_status).lower()
            rec = {
                "player_id": str(athlete.get("id", inj.get("id", "")) if isinstance(inj, dict) else ""),
                "player_name": athlete.get("displayName", ""),
                "team_id": team_id,
                "status": _status_map.get(raw_status, raw_status),
                "description": (inj.get("longComment") or inj.get("shortComment")) if isinstance(inj, dict) else None,
            }
            details = inj.get("details") if isinstance(inj, dict) else None
            if isinstance(details, dict):
                rec["body_part"] = details.get("type")
            records.append(rec)
    return records


def _espn_news(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    news_files = _espn_snapshot_files(base, "news")
    if not news_files:
        return records
    for p in news_files:
        data = _load_json(p)
        if not data:
            continue
        for article in data.get("articles", []):
            links = article.get("links")
            url = (
                links.get("web", {}).get("href")
                if isinstance(links, dict) else None
            )
            images = article.get("images", [])
            image_url = (
                (images[0].get("url") or images[0].get("href"))
                if images else None
            )
            rec: dict[str, Any] = {
                "id": str(article.get("id", "")),
                "headline": article.get("headline", ""),
                "summary": article.get("description"),
                "url": url,
                "image_url": image_url,
                "published_at": article.get("published") or article.get("lastModified"),
                "author": article.get("byline"),
            }
            records.append(rec)
    return records


def _espn_odds(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    odds_files = _espn_odds_files(base)
    if not odds_files:
        return records

    # Build game_id → (home_team, away_team, date) lookup from game detail files
    team_lookup: dict[str, tuple[str, str, str]] = {}
    for gp in _espn_game_files(base):
        gd = _load_json(gp)
        if not gd:
            continue
        gid = str(gd.get("eventId", gp.parent.name if gp.parent != base else gp.stem))
        sm = gd.get("summary", {})
        hdr = sm.get("header", {})
        comps = hdr.get("competitions", [{}])
        ht = at = ""
        game_date = ""
        if comps:
            game_date = _utc_to_et_date(comps[0].get("date") or "") or (comps[0].get("date") or "")[:10]
            for c in comps[0].get("competitors", []):
                tn = c.get("team", {}).get("displayName", "")
                if c.get("homeAway") == "home":
                    ht = tn
                elif c.get("homeAway") == "away":
                    at = tn
        if ht and at:
            team_lookup[gid] = (ht, at, game_date)

    for p in odds_files:
        data = _load_json(p)
        if not data:
            continue
        event_id = str(data.get("eventId", p.stem))
        ht, at, gd = team_lookup.get(event_id, ("", "", ""))
        home_team = ht or None
        away_team = at or None
        game_date = gd or None
        for odd in data.get("odds", []):
            prov = odd.get("provider", {})
            home_odds = odd.get("homeTeamOdds", {})
            away_odds = odd.get("awayTeamOdds", {})
            rec: dict[str, Any] = {
                "game_id": event_id,
                "date": game_date,
                "home_team": home_team or None,
                "away_team": away_team or None,
                "bookmaker": prov.get("name", "unknown"),
                "h2h_home": _safe_float(
                    home_odds.get("moneyLine") if home_odds else None
                ),
                "h2h_away": _safe_float(
                    away_odds.get("moneyLine") if away_odds else None
                ),
                "spread_home": _safe_float(odd.get("spread")),
                "spread_home_line": _safe_float(
                    home_odds.get("spreadOdds") if home_odds else None
                ),
                "spread_away_line": _safe_float(
                    away_odds.get("spreadOdds") if away_odds else None
                ),
                "total_line": _safe_float(odd.get("overUnder")),
            }
            records.append(rec)
    return records


def _espn_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Parse ESPN v2 standings into normalized records."""
    path = _espn_reference_path(base, "standings", "standings.json")
    if not path.exists():
        return []
    data = _load_json(path)
    if not data:
        return []

    standings_data = data.get("standings", data)

    # Handle the broken format (old data files with only fullViewLink)
    if isinstance(standings_data, dict) and "fullViewLink" in standings_data:
        return []

    rows: list[dict[str, Any]] = []

    children: list = []
    if isinstance(standings_data, dict):
        children = standings_data.get("children", [])
    elif isinstance(standings_data, list):
        children = standings_data

    import re as _re_standings

    def _clean_group_name(name: str) -> str:
        """Strip trailing season patterns like '2025-2026' or '2025' from group names."""
        return _re_standings.sub(r'\s+\d{4}(?:-\d{4})?\s*$', '', name).strip()

    for group in children:
        group_name = _clean_group_name(group.get("name", ""))
        # Some sports have divisions inside conferences
        has_subgroups = bool(group.get("children"))
        entries_lists: list[tuple[str, str, list]] = []  # (conference, division, entries)
        if "standings" in group and "entries" in group.get("standings", {}):
            # Flat group (no sub-divisions) — conference = group_name, division = None
            entries_lists.append((group_name, "", group["standings"]["entries"]))
        if "children" in group:
            for subgroup in group["children"]:
                sub_name = _clean_group_name(subgroup.get("name", group_name))
                if "standings" in subgroup and "entries" in subgroup.get("standings", {}):
                    entries_lists.append((group_name, sub_name, subgroup["standings"]["entries"]))

        for conf_name, div_name, entries in entries_lists:
            for rank_idx, entry in enumerate(entries, 1):
                team_data = entry.get("team", {})
                stats_list = entry.get("stats", [])
                stats = {s["name"]: s.get("value") for s in stats_list if "name" in s}

                wins = _safe_int(stats.get("wins")) or 0
                losses = _safe_int(stats.get("losses")) or 0
                pf_raw = stats.get("avgPointsFor") or stats.get("pointsFor")
                pa_raw = stats.get("avgPointsAgainst") or stats.get("pointsAgainst")
                streak_val = stats.get("streak")
                streak_dv = next(
                    (s.get("displayValue") for s in stats_list if s.get("name") == "streak"),
                    str(int(streak_val)) if streak_val is not None else None,
                )

                team_name = _safe_str(
                    team_data.get("displayName")
                    or team_data.get("name")
                    or team_data.get("shortDisplayName")
                )

                # Helper to get displayValue by stat name
                def _dv(name: str) -> str | None:
                    return next(
                        (s.get("displayValue") for s in stats_list if s.get("name") == name),
                        None,
                    )

                gp = _safe_int(stats.get("gamesPlayed"))
                if not gp:
                    gp = wins + losses + (_safe_int(stats.get("ties")) or 0) + (_safe_int(stats.get("OTLosses")) or 0)

                rows.append({
                    "source": "espn",
                    "sport": sport,
                    "season": season,
                    "team_id": _safe_str(team_data.get("id")),
                    "team_name": team_name,
                    "conference": conf_name,
                    "division": div_name or conf_name,
                    "overall_rank": rank_idx,
                    "rank": _safe_int(stats.get("playoffSeed")),
                    "wins": wins,
                    "losses": losses,
                    "ties": _safe_int(stats.get("ties")),
                    "otl": _safe_int(stats.get("OTLosses")),
                    "pct": _safe_float(stats.get("winPercent")),
                    "games_played": gp,
                    "points": _safe_int(stats.get("points")),
                    "points_for": _safe_int(pf_raw) if pf_raw is not None else None,
                    "points_against": _safe_int(pa_raw) if pa_raw is not None else None,
                    "streak": streak_dv,
                    "last_ten": _safe_str(_dv("Last Ten Games")),
                    "home_record": _safe_str(_dv("Home")),
                    "away_record": _safe_str(_dv("Road")),
                    "clinch_status": _safe_str(stats.get("clincher")),
                    "group": conf_name,
                })

    return rows


def _espn_team_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Parse ESPN per-team season statistics from ``team_stats/{teamId}.json``."""
    stat_files = _espn_team_entity_files(base, "statistics.json", "team_stats")
    if not stat_files:
        return []
    records: list[dict[str, Any]] = []
    for p in stat_files:
        try:
            data = _load_json(p)
        except Exception:
            continue
        if not data or not isinstance(data, dict):
            continue
        team_id = _safe_str(data.get("teamId")) or (p.parent.name if p.parent != base else p.stem)
        stats_obj = data.get("statistics", {})
        if not isinstance(stats_obj, dict):
            continue
        results = stats_obj.get("results", {})
        if not isinstance(results, dict):
            continue
        inner_stats = results.get("stats", {})
        if not isinstance(inner_stats, dict):
            continue
        categories = inner_stats.get("categories", [])
        if not isinstance(categories, list):
            continue

        # Flatten all stats from all categories into a single dict
        flat: dict[str, Any] = {}
        for cat in categories:
            cat_name = cat.get("name", "")
            for s in cat.get("stats", []):
                stat_name = s.get("name", "")
                if stat_name:
                    flat[stat_name] = s.get("displayValue", s.get("value"))

        team_obj = stats_obj.get("team", {})
        team_name = None
        if isinstance(team_obj, dict):
            team_name = team_obj.get("displayName") or team_obj.get("name")

        rec: dict[str, Any] = {
            "source": "espn",
            "sport": sport,
            "season": season,
            "team_id": team_id,
            "team_name": _safe_str(team_name),
            "games_played": _safe_int(flat.get("gamesPlayed")),
            "avg_points": _safe_float(flat.get("avgPoints")),
            "avg_rebounds": _safe_float(flat.get("avgRebounds")),
            "avg_assists": _safe_float(flat.get("avgAssists")),
            "avg_turnovers": _safe_float(flat.get("avgTurnovers")),
            "avg_fouls": _safe_float(flat.get("avgFouls")),
            "avg_blocks": _safe_float(flat.get("avgBlocks")),
            "avg_steals": _safe_float(flat.get("avgSteals")),
            "field_goal_pct": _safe_float(flat.get("fieldGoalPct")),
            "three_point_pct": _safe_float(flat.get("threePointPct") or flat.get("threePointFieldGoalPct")),
            "free_throw_pct": _safe_float(flat.get("freeThrowPct")),
            "two_point_fg_pct": _safe_float(flat.get("twoPointFieldGoalPct")),
            "avg_field_goals_made": _safe_float(flat.get("avgFieldGoalsMade")),
            "avg_field_goals_attempted": _safe_float(flat.get("avgFieldGoalsAttempted")),
            "avg_three_point_made": _safe_float(flat.get("avgThreePointFieldGoalsMade")),
            "avg_three_point_attempted": _safe_float(flat.get("avgThreePointFieldGoalsAttempted")),
            "avg_free_throws_made": _safe_float(flat.get("avgFreeThrowsMade")),
            "avg_free_throws_attempted": _safe_float(flat.get("avgFreeThrowsAttempted")),
            "avg_offensive_rebounds": _safe_float(flat.get("avgOffensiveRebounds")),
            "avg_defensive_rebounds": _safe_float(flat.get("avgDefensiveRebounds")),
            "total_points": _safe_int(flat.get("points")),
            "total_rebounds": _safe_int(flat.get("totalRebounds") or flat.get("rebounds")),
            "total_assists": _safe_int(flat.get("assists")),
            "total_steals": _safe_int(flat.get("steals")),
            "total_blocks": _safe_int(flat.get("blocks")),
            "total_turnovers": _safe_int(flat.get("turnovers")),
            "scoring_efficiency": _safe_float(flat.get("scoringEfficiency")),
            "shooting_efficiency": _safe_float(flat.get("shootingEfficiency")),
            "assist_turnover_ratio": _safe_float(flat.get("assistTurnoverRatio")),
        }
        records.append(rec)
    return records


def _espn_transactions(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Parse ESPN transaction files from ``transactions/{date}.json``."""
    tx_files = _espn_snapshot_files(base, "transactions")
    if not tx_files:
        return []
    records: list[dict[str, Any]] = []
    for p in tx_files:
        try:
            data = _load_json(p)
        except Exception:
            continue
        if not data or not isinstance(data, dict):
            continue
        tx_list = data.get("transactions", [])
        if not isinstance(tx_list, list):
            continue
        for tx in tx_list:
            if not isinstance(tx, dict):
                continue
            team = tx.get("team", {})
            if not isinstance(team, dict):
                team = {}
            rec: dict[str, Any] = {
                "source": "espn",
                "sport": sport,
                "season": season,
                "date": _safe_str(tx.get("date")),
                "description": _safe_str(tx.get("description")),
                "team_id": _safe_str(team.get("id")),
                "team_name": _safe_str(team.get("displayName")),
                "team_abbreviation": _safe_str(team.get("abbreviation")),
            }
            records.append(rec)
    return records


# ── Odds Collector (new unified format) ───────────────────

def _odds_collector(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load odds from ``data/raw/odds/{sport}/{date}/`` directories.

    Reads opening.json, closing_*.json, espn_baseline.json, and
    snapshots/*.json produced by the odds collector provider.
    Each file contains a ``records`` array of :class:`OddsRecord` dicts.
    """
    records: list[dict[str, Any]] = []
    if not base.is_dir():
        return records

    # Iterate over date directories
    for date_dir in sorted(base.iterdir()):
        if not date_dir.is_dir():
            continue

        # Collect all JSON files (top-level + snapshots/)
        json_files = list(date_dir.glob("*.json"))
        snapshots_dir = date_dir / "snapshots"
        if snapshots_dir.is_dir():
            json_files.extend(snapshots_dir.glob("*.json"))

        for fp in json_files:
            data = _load_json(fp)
            if not data or not isinstance(data.get("records"), list):
                continue

            for rec in data["records"]:
                game_id = str(rec.get("game_id", ""))
                if not game_id:
                    continue
                bm = rec.get("sportsbook", rec.get("bookmaker", "unknown"))
                collection_type = rec.get("type", "snapshot")
                ts_raw = rec.get("timestamp")

                row: dict[str, Any] = {
                    "game_id": game_id,
                    "date": str(date_dir.name) if date_dir.name[:4].isdigit() else None,
                    "home_team": rec.get("home_team") or None,
                    "away_team": rec.get("away_team") or None,
                    "bookmaker": bm,
                    "h2h_home": _safe_float(rec.get("home_ml")),
                    "h2h_away": _safe_float(rec.get("away_ml")),
                    "spread_home": _safe_float(rec.get("home_spread")),
                    "spread_away": _safe_float(rec.get("away_spread")),
                    "spread_home_line": _safe_float(rec.get("home_spread_odds")),
                    "spread_away_line": _safe_float(rec.get("away_spread_odds")),
                    "total_line": _safe_float(rec.get("total")),
                    "total_over": _safe_float(rec.get("over_odds")),
                    "total_under": _safe_float(rec.get("under_odds")),
                }

                # Parse timestamp if available
                if ts_raw and isinstance(ts_raw, str):
                    try:
                        row["timestamp"] = datetime.fromisoformat(
                            ts_raw.replace("Z", "+00:00"),
                        )
                    except (ValueError, TypeError):
                        pass

                # Tag with collection type for downstream filtering
                if collection_type in ("opening", "closing"):
                    row["is_live"] = False
                else:
                    row["is_live"] = False

                records.append(row)

    return records


def _odds_collector_player_props(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load player props from ``data/raw/odds/{sport}/{date}/player_props/*.json``."""
    records: list[dict[str, Any]] = []
    if not base.is_dir():
        return records

    for date_dir in sorted(base.iterdir()):
        if not date_dir.is_dir():
            continue
        props_dir = date_dir / "player_props"
        if not props_dir.is_dir():
            continue

        for fp in sorted(props_dir.glob("*.json")):
            data = _load_json(fp)
            if not data or not isinstance(data.get("records"), list):
                continue
            for rec in data["records"]:
                game_id = _safe_str(rec.get("game_id"))
                player_id = _safe_str(rec.get("player_id"))
                market = _safe_str(rec.get("market"))
                line = _safe_float(rec.get("line"))
                if not game_id or not player_id or not market or line is None:
                    continue

                row: dict[str, Any] = {
                    "game_id": game_id,
                    "player_id": player_id,
                    "sport": sport,
                    "market": market,
                    "line": line,
                    "over_price": _safe_float(rec.get("over_price")),
                    "under_price": _safe_float(rec.get("under_price")),
                    "bookmaker": _safe_str(rec.get("bookmaker")) or "unknown",
                    "player_name": _safe_str(rec.get("player_name")) or None,
                    "team_id": _safe_str(rec.get("team_id")) or None,
                }

                ts_raw = rec.get("timestamp")
                if ts_raw and isinstance(ts_raw, str):
                    try:
                        row["timestamp"] = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        pass

                records.append(row)

    return records


# ── NBA Stats ─────────────────────────────────────────────

def _nbastats_result_rows(data: dict | None) -> list[dict[str, Any]]:
    """Convert NBA Stats header+rowSet format into a list of dicts."""
    if not data:
        return []
    result_sets = data.get("resultSets") or (
        [data["resultSet"]] if "resultSet" in data else []
    )
    rows: list[dict[str, Any]] = []
    for rs in result_sets:
        headers = [h.upper() for h in rs.get("headers", [])]
        for row in rs.get("rowSet", []):
            rows.append(dict(zip(headers, row)))
    return rows


def _nbastats_load_season_aggregate(base: Path, subdir: str, *segments: str) -> dict[str, Any] | None:
    structured = base / subdir / "season_aggregates"
    for segment in segments:
        structured /= segment
    data = _load_json(structured)
    if data:
        return data

    legacy = base / subdir
    for segment in segments:
        legacy /= segment
    return _load_json(legacy)


def _nbastats_live_stat_map(entity: dict[str, Any]) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for item in entity.get("statistics", []) or []:
        if not isinstance(item, dict):
            continue
        name = _safe_str(item.get("name") or item.get("stat"))
        if name:
            stats[name] = item.get("value")
    return stats


def _nbastats_team_name(team: dict[str, Any]) -> str:
    city = _safe_str(team.get("teamCity"))
    name = _safe_str(team.get("teamName"))
    display = f"{city} {name}".strip()
    return display or _safe_str(team.get("teamTricode")) or _safe_str(team.get("teamName"))


def _nbastats_parse_minutes(value: Any) -> float | None:
    text = _safe_str(value)
    if not text:
        return None
    if text.startswith("PT") and text.endswith("S"):
        minutes = 0.0
        seconds = 0.0
        body = text[2:-1]
        if "M" in body:
            minute_text, second_text = body.split("M", 1)
            minutes = float(minute_text or 0)
            seconds = float(second_text or 0)
            return round(minutes + (seconds / 60.0), 2)
    return _safe_float(value)


def _nbastats_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Build game records from NBA Stats team-game-logs.json.

    Each row is one team's stats for one game. We pair home/away rows by
    GAME_ID and emit a single record per game with full box-score stats
    (steals, blocks, turnovers, etc.) that ESPN scoreboard data lacks.
    """
    records: list[dict[str, Any]] = []
    seen_game_ids: set[str] = set()

    for subdir in ("regular-season", "playoffs"):
        games_dir = base / subdir / "games"
        if not games_dir.exists():
            continue
        for boxscore_path in sorted(games_dir.glob("*/boxscore.json")):
            data = _load_json(boxscore_path) or {}
            game = data.get("game") or {}
            home = game.get("homeTeam") or {}
            away = game.get("awayTeam") or {}
            gid = _safe_str(game.get("gameId")) or boxscore_path.parent.name
            if not gid or not home or not away:
                continue
            seen_game_ids.add(gid)

            home_stats = _nbastats_live_stat_map(home)
            away_stats = _nbastats_live_stat_map(away)
            game_date = (
                _safe_str(game.get("gameEt"))[:10]
                or _safe_str(game.get("gameDateEst"))[:10]
                or _safe_str(game.get("gameTimeUTC"))[:10]
            )
            game_status = _safe_str(game.get("gameStatusText")) or _safe_str(game.get("gameStatus"))
            status = "final" if str(game.get("gameStatus")) == "3" or "final" in game_status.lower() else game_status.lower() or "scheduled"

            rec: dict[str, Any] = {
                "source": "nbastats",
                "id": f"nba_{gid}",
                "sport": sport,
                "season": season,
                "date": game_date,
                "status": status,
                "home_team": _nbastats_team_name(home),
                "away_team": _nbastats_team_name(away),
                "home_team_id": _safe_str(home.get("teamId")),
                "away_team_id": _safe_str(away.get("teamId")),
                "home_score": _safe_int(home.get("score")),
                "away_score": _safe_int(away.get("score")),
            }
            for prefix, stat_map in (("home", home_stats), ("away", away_stats)):
                rec[f"{prefix}_rebounds"] = _safe_int(stat_map.get("reboundsTotal") or stat_map.get("rebounds"))
                rec[f"{prefix}_assists"] = _safe_int(stat_map.get("assists"))
                rec[f"{prefix}_steals"] = _safe_int(stat_map.get("steals"))
                rec[f"{prefix}_blocks"] = _safe_int(stat_map.get("blocks"))
                rec[f"{prefix}_turnovers"] = _safe_int(stat_map.get("turnovers"))
                rec[f"{prefix}_fouls"] = _safe_int(stat_map.get("foulsPersonal") or stat_map.get("fouls"))
                rec[f"{prefix}_fgm"] = _safe_int(stat_map.get("fieldGoalsMade"))
                rec[f"{prefix}_fga"] = _safe_int(stat_map.get("fieldGoalsAttempted"))
                rec[f"{prefix}_fg_pct"] = _safe_float(stat_map.get("fieldGoalsPercentage"))
                rec[f"{prefix}_three_m"] = _safe_int(stat_map.get("threePointersMade"))
                rec[f"{prefix}_three_a"] = _safe_int(stat_map.get("threePointersAttempted"))
                rec[f"{prefix}_three_pct"] = _safe_float(stat_map.get("threePointersPercentage"))
                rec[f"{prefix}_ftm"] = _safe_int(stat_map.get("freeThrowsMade"))
                rec[f"{prefix}_fta"] = _safe_int(stat_map.get("freeThrowsAttempted"))
                rec[f"{prefix}_ft_pct"] = _safe_float(stat_map.get("freeThrowsPercentage"))
                rec[f"{prefix}_offensive_rebounds"] = _safe_int(stat_map.get("reboundsOffensive"))
                rec[f"{prefix}_defensive_rebounds"] = _safe_int(stat_map.get("reboundsDefensive"))
            records.append(rec)

    game_teams: dict[str, dict[str, dict[str, Any]]] = {}  # gid -> {home/away -> row}

    for subdir in ("regular-season", "playoffs"):
        path_data = _nbastats_load_season_aggregate(base, subdir, "team-game-logs.json")
        for rd in _nbastats_result_rows(path_data):
            gid = str(rd.get("GAME_ID", ""))
            if not gid or gid in seen_game_ids:
                continue
            matchup = rd.get("MATCHUP", "")
            # "MEM vs. DAL" = home game, "MEM @ DAL" = away game
            is_home = " vs. " in matchup
            side = "home" if is_home else "away"
            game_teams.setdefault(gid, {})[side] = rd

    for gid, sides in game_teams.items():
        home = sides.get("home")
        away = sides.get("away")
        if not home or not away:
            continue
        # Parse date from GAME_DATE ("2025-04-13T00:00:00")
        game_date = str(home.get("GAME_DATE", ""))[:10]
        rec: dict[str, Any] = {
            "source": "nbastats",
            "id": f"nba_{gid}",
            "sport": sport,
            "season": season,
            "date": game_date,
            "status": "final",
            "home_team": home.get("TEAM_NAME", ""),
            "away_team": away.get("TEAM_NAME", ""),
            "home_team_id": _safe_str(home.get("TEAM_ID")),
            "away_team_id": _safe_str(away.get("TEAM_ID")),
            "home_score": _safe_int(home.get("PTS")),
            "away_score": _safe_int(away.get("PTS")),
        }
        for prefix, data in [("home", home), ("away", away)]:
            rec[f"{prefix}_rebounds"] = _safe_int(data.get("REB"))
            rec[f"{prefix}_assists"] = _safe_int(data.get("AST"))
            rec[f"{prefix}_steals"] = _safe_int(data.get("STL"))
            rec[f"{prefix}_blocks"] = _safe_int(data.get("BLK"))
            rec[f"{prefix}_turnovers"] = _safe_int(data.get("TOV"))
            rec[f"{prefix}_fouls"] = _safe_int(data.get("PF"))
            rec[f"{prefix}_fgm"] = _safe_int(data.get("FGM"))
            rec[f"{prefix}_fga"] = _safe_int(data.get("FGA"))
            rec[f"{prefix}_fg_pct"] = _safe_float(data.get("FG_PCT"))
            rec[f"{prefix}_three_m"] = _safe_int(data.get("FG3M"))
            rec[f"{prefix}_three_a"] = _safe_int(data.get("FG3A"))
            rec[f"{prefix}_three_pct"] = _safe_float(data.get("FG3_PCT"))
            rec[f"{prefix}_ftm"] = _safe_int(data.get("FTM"))
            rec[f"{prefix}_fta"] = _safe_int(data.get("FTA"))
            rec[f"{prefix}_ft_pct"] = _safe_float(data.get("FT_PCT"))
            rec[f"{prefix}_offensive_rebounds"] = _safe_int(data.get("OREB"))
            rec[f"{prefix}_defensive_rebounds"] = _safe_int(data.get("DREB"))
        records.append(rec)
    return records


def _nbastats_play_by_play(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Build play-by-play rows from NBA Stats per-game playbyplay files."""
    records: list[dict[str, Any]] = []

    for subdir in ("regular-season", "playoffs"):
        games_dir = base / subdir / "games"
        if not games_dir.exists():
            continue

        for pbp_path in sorted(games_dir.glob("*/playbyplay.json")):
            payload = _load_json(pbp_path)
            if not isinstance(payload, dict):
                continue

            game = payload.get("game") if isinstance(payload.get("game"), dict) else {}
            actions = game.get("actions") if isinstance(game.get("actions"), list) else payload.get("actions")
            if not isinstance(actions, list) or not actions:
                continue

            game_id = _safe_str(game.get("gameId")) or pbp_path.parent.name
            if not game_id:
                continue

            summary_payload = _load_json(pbp_path.parent / "summary.json")
            home_team_id = ""
            away_team_id = ""
            home_team_name = ""
            away_team_name = ""
            if isinstance(summary_payload, dict):
                home_team_id = _safe_str(summary_payload.get("homeTeamId")) or ""
                away_team_id = _safe_str(summary_payload.get("awayTeamId")) or ""
                home_team_name = _safe_str(summary_payload.get("homeTeamName")) or ""
                away_team_name = _safe_str(summary_payload.get("awayTeamName")) or ""

            for idx, action in enumerate(actions):
                if not isinstance(action, dict):
                    continue

                play_id = _safe_str(action.get("actionNumber") or action.get("orderNumber")) or str(idx + 1)
                possession_team_id = _safe_str(action.get("possession")) or ""
                offense_team_name = ""
                if possession_team_id and possession_team_id == home_team_id:
                    offense_team_name = home_team_name
                elif possession_team_id and possession_team_id == away_team_id:
                    offense_team_name = away_team_name

                rec: dict[str, Any] = {
                    "sport": sport,
                    "source": "nbastats",
                    "season": season,
                    "game_id": game_id,
                    "drive_id": "",
                    "play_id": play_id,
                    "sequence_number": _safe_int(action.get("orderNumber") or action.get("actionNumber")),
                    "event_type": _safe_str(action.get("actionType") or action.get("subType")) or "play",
                    "description": _safe_str(action.get("description")) or "",
                    "period": _safe_int(action.get("period")),
                    "clock": _safe_str(action.get("clock")) or "",
                    "down": None,
                    "distance": None,
                    "yards_gained": None,
                    "scoring_play": bool(action.get("scoreHome") is not None and action.get("scoreAway") is not None),
                    "offense_team_id": possession_team_id,
                    "offense_team_name": offense_team_name,
                    "home_score": _safe_int(action.get("scoreHome")),
                    "away_score": _safe_int(action.get("scoreAway")),
                    "player_id": _safe_str(action.get("personId")) or "",
                    "x": _safe_float(action.get("x")),
                    "y": _safe_float(action.get("y")),
                }
                records.append(rec)

    return records


def _nbastats_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    records: list[dict[str, Any]] = []

    for rd in _nbastats_result_rows(_load_json(base / "reference" / "all_players.json")):
        pid = str(rd.get("PERSON_ID", rd.get("PLAYER_ID", "")))
        if not pid or pid in seen:
            continue
        seen.add(pid)
        records.append({
            "id": pid,
            "name": rd.get("DISPLAY_FIRST_LAST", rd.get("PLAYER_NAME", "")),
            "team_id": _safe_str(rd.get("TEAM_ID")),
            "team_abbreviation": rd.get("TEAM_ABBREVIATION"),
        })

    for subdir in ("regular-season", "playoffs"):
        data = _nbastats_load_season_aggregate(base, subdir, "player-stats", "base.json")
        for rd in _nbastats_result_rows(data):
            pid = str(rd.get("PLAYER_ID", ""))
            if not pid or pid in seen:
                continue
            seen.add(pid)
            records.append({
                "id": pid,
                "name": rd.get("PLAYER_NAME", rd.get("PLAYER", "")),
                "team_id": _safe_str(rd.get("TEAM_ID")),
                "team_abbreviation": rd.get("TEAM_ABBREVIATION"),
            })
    return records


def _nbastats_team_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Build season team stats from NBA Stats aggregate team-stats files."""
    records: list[dict[str, Any]] = []
    for subdir in ("regular-season",):
        split_maps: dict[str, dict[str, Any]] = {}
        split_files = {
            "base": "base.json",
            "advanced": "advanced.json",
            "defense": "defense.json",
        }

        for split_name, file_name in split_files.items():
            data = _nbastats_load_season_aggregate(base, subdir, "team-stats", file_name)
            for rd in _nbastats_result_rows(data):
                team_id = _safe_str(rd.get("TEAM_ID"))
                if not team_id:
                    continue
                split_maps.setdefault(team_id, {})[split_name] = rd

        for team_id, parts in split_maps.items():
            base_row = parts.get("base", {})
            adv_row = parts.get("advanced", {})
            def_row = parts.get("defense", {})
            fgm = _safe_float(base_row.get("FGM"))
            fga = _safe_float(base_row.get("FGA"))
            fg3m = _safe_float(base_row.get("FG3M"))
            fg3a = _safe_float(base_row.get("FG3A"))
            two_pt_attempts = (fga - fg3a) if (fga is not None and fg3a is not None) else None
            two_pt_pct = None
            if two_pt_attempts and two_pt_attempts > 0 and fgm is not None and fg3m is not None:
                two_pt_pct = (fgm - fg3m) / two_pt_attempts
            team_name = (
                _safe_str(base_row.get("TEAM_NAME"))
                or _safe_str(adv_row.get("TEAM_NAME"))
                or _safe_str(def_row.get("TEAM_NAME"))
            )

            records.append({
                "source": "nbastats",
                "sport": sport,
                "season": season,
                "season_type": "regular",
                "team_id": team_id,
                "team_name": team_name,
                "games_played": _safe_int(base_row.get("GP") or adv_row.get("GP") or def_row.get("GP")),
                "wins": _safe_int(base_row.get("W") or adv_row.get("W") or def_row.get("W")),
                "losses": _safe_int(base_row.get("L") or adv_row.get("L") or def_row.get("L")),
                "win_pct": _safe_float(base_row.get("W_PCT") or adv_row.get("W_PCT") or def_row.get("W_PCT")),
                "avg_points": _safe_float(base_row.get("PTS")),
                "avg_rebounds": _safe_float(base_row.get("REB")),
                "avg_assists": _safe_float(base_row.get("AST")),
                "avg_turnovers": _safe_float(base_row.get("TOV")),
                "avg_fouls": _safe_float(base_row.get("PF")),
                "avg_blocks": _safe_float(base_row.get("BLK")),
                "avg_steals": _safe_float(base_row.get("STL")),
                "field_goal_pct": _safe_float(base_row.get("FG_PCT")),
                "three_point_pct": _safe_float(base_row.get("FG3_PCT")),
                "free_throw_pct": _safe_float(base_row.get("FT_PCT")),
                "two_point_fg_pct": round(two_pt_pct, 3) if two_pt_pct is not None else None,
                "avg_field_goals_made": _safe_float(base_row.get("FGM")),
                "avg_field_goals_attempted": _safe_float(base_row.get("FGA")),
                "avg_three_point_made": _safe_float(base_row.get("FG3M")),
                "avg_three_point_attempted": _safe_float(base_row.get("FG3A")),
                "avg_free_throws_made": _safe_float(base_row.get("FTM")),
                "avg_free_throws_attempted": _safe_float(base_row.get("FTA")),
                "avg_offensive_rebounds": _safe_float(base_row.get("OREB")),
                "avg_defensive_rebounds": _safe_float(base_row.get("DREB")),
                "total_points": _safe_int(base_row.get("PTS") * _safe_float(base_row.get("GP")) if base_row.get("PTS") is not None and base_row.get("GP") is not None else None),
                "total_rebounds": _safe_int(base_row.get("REB") * _safe_float(base_row.get("GP")) if base_row.get("REB") is not None and base_row.get("GP") is not None else None),
                "total_assists": _safe_int(base_row.get("AST") * _safe_float(base_row.get("GP")) if base_row.get("AST") is not None and base_row.get("GP") is not None else None),
                "total_steals": _safe_int(base_row.get("STL") * _safe_float(base_row.get("GP")) if base_row.get("STL") is not None and base_row.get("GP") is not None else None),
                "total_blocks": _safe_int(base_row.get("BLK") * _safe_float(base_row.get("GP")) if base_row.get("BLK") is not None and base_row.get("GP") is not None else None),
                "total_turnovers": _safe_int(base_row.get("TOV") * _safe_float(base_row.get("GP")) if base_row.get("TOV") is not None and base_row.get("GP") is not None else None),
                "scoring_efficiency": _safe_float(adv_row.get("OFF_RATING")),
                "shooting_efficiency": _safe_float(adv_row.get("EFG_PCT")),
                "assist_turnover_ratio": _safe_float(adv_row.get("AST_TO")),
            })

    return records


def _nbastats_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for subdir in ("regular-season", "playoffs"):
        # Build per-player advanced-stats lookup from split stat files
        adv_lookup: dict[str, dict[str, Any]] = {}
        for stat_file in ("advanced", "usage", "defense", "scoring"):
            data = _nbastats_load_season_aggregate(base, subdir, "player-stats", f"{stat_file}.json")
            for rd in _nbastats_result_rows(data):
                pid = str(rd.get("PLAYER_ID", ""))
                if pid:
                    adv_lookup.setdefault(pid, {}).update(rd)

        games_dir = base / subdir / "games"
        if games_dir.exists():
            for boxscore_path in sorted(games_dir.glob("*/boxscore.json")):
                data = _load_json(boxscore_path) or {}
                game = data.get("game") or {}
                game_id = _safe_str(game.get("gameId")) or boxscore_path.parent.name
                for team in (game.get("homeTeam") or {}, game.get("awayTeam") or {}):
                    team_id = _safe_str(team.get("teamId"))
                    for player in team.get("players", []) or []:
                        if not isinstance(player, dict):
                            continue
                        pid = _safe_str(player.get("personId") or player.get("playerId"))
                        if not pid:
                            continue
                        key = (game_id, pid)
                        if key in seen:
                            continue
                        seen.add(key)
                        stat_map = _nbastats_live_stat_map(player)
                        adv = adv_lookup.get(pid, {})
                        player_name = (
                            _safe_str(player.get("name"))
                            or f"{_safe_str(player.get('firstName'))} {_safe_str(player.get('familyName'))}".strip()
                        )
                        records.append({
                            "game_id": game_id,
                            "player_id": pid,
                            "player_name": player_name,
                            "team_id": team_id,
                            "season": season,
                            "category": "basketball",
                            "pts": _safe_int(stat_map.get("points")),
                            "reb": _safe_int(stat_map.get("reboundsTotal") or stat_map.get("rebounds")),
                            "ast": _safe_int(stat_map.get("assists")),
                            "stl": _safe_int(stat_map.get("steals")),
                            "blk": _safe_int(stat_map.get("blocks")),
                            "to": _safe_int(stat_map.get("turnovers")),
                            "fgm": _safe_int(stat_map.get("fieldGoalsMade")),
                            "fga": _safe_int(stat_map.get("fieldGoalsAttempted")),
                            "fg_pct": _safe_float(stat_map.get("fieldGoalsPercentage")),
                            "ftm": _safe_int(stat_map.get("freeThrowsMade")),
                            "fta": _safe_int(stat_map.get("freeThrowsAttempted")),
                            "ft_pct": _safe_float(stat_map.get("freeThrowsPercentage")),
                            "three_m": _safe_int(stat_map.get("threePointersMade")),
                            "three_a": _safe_int(stat_map.get("threePointersAttempted")),
                            "three_pct": _safe_float(stat_map.get("threePointersPercentage")),
                            "oreb": _safe_int(stat_map.get("reboundsOffensive")),
                            "dreb": _safe_int(stat_map.get("reboundsDefensive")),
                            "pf": _safe_int(stat_map.get("foulsPersonal") or stat_map.get("fouls")),
                            "plus_minus": _safe_int(player.get("plusMinusPoints") or stat_map.get("plusMinusPoints")),
                            "min": _nbastats_parse_minutes(stat_map.get("minutes")),
                            "off_rating": _safe_float(adv.get("OFF_RATING")),
                            "def_rating": _safe_float(adv.get("DEF_RATING")),
                            "net_rating": _safe_float(adv.get("NET_RATING")),
                            "ast_pct": _safe_float(adv.get("AST_PCT")),
                            "ast_to": _safe_float(adv.get("AST_TO")),
                            "ts_pct": _safe_float(adv.get("TS_PCT")),
                            "efg_pct": _safe_float(adv.get("EFG_PCT")),
                            "usg_pct": _safe_float(adv.get("USG_PCT")),
                            "pace": _safe_float(adv.get("PACE")),
                            "pie": _safe_float(adv.get("PIE")),
                            "def_ws": _safe_float(adv.get("DEF_WS")),
                            "pct_pts_fb": _safe_float(adv.get("PCT_PTS_FB")),
                            "pct_pts_paint": _safe_float(adv.get("PCT_PTS_PAINT")),
                            "pct_pts_ft": _safe_float(adv.get("PCT_PTS_FT")),
                            "pct_ast_fgm": _safe_float(adv.get("PCT_AST_FGM")),
                        })

        # Prefer game-level logs, fall back to aggregated leaders
        data = _nbastats_load_season_aggregate(base, subdir, "player-game-logs.json")
        if not data:
            data = _nbastats_load_season_aggregate(base, subdir, "league-leaders.json")
        for rd in _nbastats_result_rows(data):
            pid = str(rd.get("PLAYER_ID", ""))
            gid = str(rd.get("GAME_ID", f"{pid}_{season}_{subdir}"))
            if not pid:
                continue
            key = (gid, pid)
            if key in seen:
                continue
            seen.add(key)
            adv = adv_lookup.get(pid, {})
            records.append({
                "game_id": gid,
                "player_id": pid,
                "player_name": rd.get("PLAYER_NAME", rd.get("PLAYER", "")),
                "team_id": _safe_str(rd.get("TEAM_ID")),
                "season": season,
                "category": "basketball",
                "pts": _safe_int(rd.get("PTS")),
                "reb": _safe_int(rd.get("REB")),
                "ast": _safe_int(rd.get("AST")),
                "stl": _safe_int(rd.get("STL")),
                "blk": _safe_int(rd.get("BLK")),
                "to": _safe_int(rd.get("TOV", rd.get("TO"))),
                "fgm": _safe_int(rd.get("FGM")),
                "fga": _safe_int(rd.get("FGA")),
                "fg_pct": _safe_float(rd.get("FG_PCT")),
                "ftm": _safe_int(rd.get("FTM")),
                "fta": _safe_int(rd.get("FTA")),
                "ft_pct": _safe_float(rd.get("FT_PCT")),
                "three_m": _safe_int(rd.get("FG3M")),
                "three_a": _safe_int(rd.get("FG3A")),
                "three_pct": _safe_float(rd.get("FG3_PCT")),
                "oreb": _safe_int(rd.get("OREB")),
                "dreb": _safe_int(rd.get("DREB")),
                "pf": _safe_int(rd.get("PF")),
                "plus_minus": _safe_int(rd.get("PLUS_MINUS")),
                "min": _safe_float(rd.get("MIN")),
                # Advanced stats (season-level from split stat files)
                "off_rating": _safe_float(adv.get("OFF_RATING")),
                "def_rating": _safe_float(adv.get("DEF_RATING")),
                "net_rating": _safe_float(adv.get("NET_RATING")),
                "ast_pct": _safe_float(adv.get("AST_PCT")),
                "ast_to": _safe_float(adv.get("AST_TO")),
                "ts_pct": _safe_float(adv.get("TS_PCT")),
                "efg_pct": _safe_float(adv.get("EFG_PCT")),
                "usg_pct": _safe_float(adv.get("USG_PCT")),
                "pace": _safe_float(adv.get("PACE")),
                "pie": _safe_float(adv.get("PIE")),
                # Defense split
                "def_ws": _safe_float(adv.get("DEF_WS")),
                # Scoring split
                "pct_pts_fb": _safe_float(adv.get("PCT_PTS_FB")),
                "pct_pts_paint": _safe_float(adv.get("PCT_PTS_PAINT")),
                "pct_pts_ft": _safe_float(adv.get("PCT_PTS_FT")),
                "pct_ast_fgm": _safe_float(adv.get("PCT_AST_FGM")),
            })
    return records



# ── Soccer team name canonicalization ─────────────────────

_SOCCER_TEAM_CANON: dict[str, str] = {
    # EPL
    "afc bournemouth": "bournemouth",
    "brighton & hove albion": "brighton",
    "brighton hove": "brighton",
    "brighton hove albion": "brighton",
    "brighton and hove albion": "brighton",
    "ipswich town": "ipswich",
    "leicester city": "leicester",
    "man city": "manchester city",
    "man united": "manchester united",
    "man utd": "manchester united",
    "newcastle united": "newcastle",
    "newcastle utd": "newcastle",
    "nottingham": "nottingham forest",
    "tottenham hotspur": "tottenham",
    "spurs": "tottenham",
    "west ham united": "west ham",
    "wolverhampton wanderers": "wolves",
    "wolverhampton": "wolves",
    "sheffield united": "sheffield utd",
    "leeds united": "leeds",
    "norwich city": "norwich",
    "watford fc": "watford",
    "burnley fc": "burnley",
    "luton town": "luton",
    # LaLiga
    "atletico madrid": "atletico madrid",
    "atletico de madrid": "atletico madrid",
    "athletic bilbao": "athletic club",
    "real betis balompie": "real betis",
    "rc celta": "celta vigo",
    "rc celta de vigo": "celta vigo",
    "rcd espanyol": "espanyol",
    "rcd mallorca": "mallorca",
    "real sociedad de futbol": "real sociedad",
    "deportivo alaves": "alaves",
    "deportivo la coruna": "deportivo",
    "ca osasuna": "osasuna",
    "ud almeria": "almeria",
    "cd leganes": "leganes",
    "getafe cf": "getafe",
    "girona fc": "girona",
    "valencia cf": "valencia",
    "villarreal cf": "villarreal",
    "real valladolid": "valladolid",
    "real valladolid cf": "valladolid",
    "ud las palmas": "las palmas",
    "sevilla fc": "sevilla",
    # Bundesliga
    "bayern munchen": "bayern munich",
    "fc bayern munchen": "bayern munich",
    "fc bayern munich": "bayern munich",
    "bayern munich": "bayern munich",
    "borussia monchengladbach": "monchengladbach",
    "borussia m'gladbach": "monchengladbach",
    "bor. monchengladbach": "monchengladbach",
    "gladbach": "monchengladbach",
    "1. fc heidenheim 1846": "heidenheim",
    "1. fc heidenheim": "heidenheim",
    "fc heidenheim": "heidenheim",
    "1. fc union berlin": "union berlin",
    "fc union berlin": "union berlin",
    "1. fc koln": "koln",
    "1. fc cologne": "koln",
    "fc koln": "koln",
    "fc cologne": "koln",
    "cologne": "koln",
    "1899 hoffenheim": "hoffenheim",
    "tsg hoffenheim": "hoffenheim",
    "tsg 1899 hoffenheim": "hoffenheim",
    "vfb stuttgart": "stuttgart",
    "vfl wolfsburg": "wolfsburg",
    "vfl bochum": "bochum",
    "vfl bochum 1848": "bochum",
    "rb leipzig": "leipzig",
    "rasenballsport leipzig": "leipzig",
    "sc freiburg": "freiburg",
    "sport-club freiburg": "freiburg",
    "sv darmstadt 98": "darmstadt",
    "darmstadt 98": "darmstadt",
    "sv werder bremen": "werder bremen",
    "fc augsburg": "augsburg",
    "eintracht frankfurt": "eintracht frankfurt",
    "sg eintracht frankfurt": "eintracht frankfurt",
    "1. fsv mainz 05": "mainz",
    "fsv mainz 05": "mainz",
    "1. fsv mainz": "mainz",
    "mainz 05": "mainz",
    "fc st. pauli": "st pauli",
    "st. pauli": "st pauli",
    "fc schalke 04": "schalke 04",
    "schalke": "schalke 04",
    "hamburger sv": "hamburg",
    "hamburg sv": "hamburg",
    "hertha bsc": "hertha berlin",
    "hertha bsc berlin": "hertha berlin",
    "holstein kiel": "holstein kiel",
    "sv elversberg": "elversberg",
    "fortuna dusseldorf": "dusseldorf",
    "fortuna duesseldorf": "dusseldorf",
    "greuther furth": "greuther furth",
    "spvgg greuther furth": "greuther furth",
    # SerieA
    "inter milan": "inter",
    "fc internazionale": "inter",
    "internazionale": "inter",
    "fc internazionale milano": "inter",
    "ssc napoli": "napoli",
    "ss lazio": "lazio",
    "as roma": "roma",
    "ac milan": "milan",
    "juventus fc": "juventus",
    "uc sampdoria": "sampdoria",
    "us salernitana": "salernitana",
    "us salernitana 1919": "salernitana",
    "hellas verona": "verona",
    "hellas verona fc": "verona",
    "us lecce": "lecce",
    "us cremonese": "cremonese",
    "spe calcio": "spezia",
    "spezia calcio": "spezia",
    "acf fiorentina": "fiorentina",
    "genoa cfc": "genoa",
    "parma calcio 1913": "parma",
    "como 1907": "como",
    "venezia fc": "venezia",
    # Ligue1
    "aj auxerre": "auxerre",
    "as monaco": "monaco",
    "fc nantes": "nantes",
    "le havre ac": "le havre",
    "le havre athletic club": "le havre",
    "rc strasbourg alsace": "strasbourg",
    "rc strasbourg": "strasbourg",
    "rc lens": "lens",
    "stade brestois 29": "brest",
    "stade brestois": "brest",
    "stade de reims": "reims",
    "stade rennais fc": "rennes",
    "stade rennais": "rennes",
    "fc lorient": "lorient",
    "clermont foot 63": "clermont",
    "clermont foot": "clermont",
    "ogc nice": "nice",
    "as saint-etienne": "saint-etienne",
    "as st-etienne": "saint-etienne",
    "saint etienne": "saint-etienne",
    "saint-etienne": "saint-etienne",
    "st etienne": "saint-etienne",
    "st-etienne": "saint-etienne",
    "olympique lyonnais": "lyon",
    "olympique de marseille": "marseille",
    "olympique marseille": "marseille",
    "paris saint-germain": "paris saint germain",
    "paris saint germain fc": "paris saint germain",
    "paris sg": "paris saint germain",
    "psg": "paris saint germain",
    "paris fc": "paris fc",
    "toulouse fc": "toulouse",
    "montpellier hsc": "montpellier",
    "angers sco": "angers",
    "estac troyes": "troyes",
    "ac ajaccio": "ajaccio",
    "fc metz": "metz",
    "fc girondins de bordeaux": "bordeaux",
    "girondins de bordeaux": "bordeaux",
    "dijon fco": "dijon",
    "nimes olympique": "nimes",
    # MLS
    "inter miami cf": "inter miami",
    "la galaxy": "la galaxy",
    "los angeles galaxy": "la galaxy",
    "new york red bulls": "ny red bulls",
    "red bull new york": "ny red bulls",
    "new york city fc": "new york city",
    "atlanta united fc": "atlanta united",
    "columbus crew sc": "columbus crew",
    "minnesota united fc": "minnesota united",
    "fc cincinnati": "cincinnati",
    "fc dallas": "dallas",
    "sporting kansas city": "sporting kc",
    "portland timbers fc": "portland timbers",
    "seattle sounders fc": "seattle sounders",
    "orlando city sc": "orlando city",
    "cf montreal": "cf montreal",
    "club de foot montreal": "cf montreal",
    "real salt lake city": "real salt lake",
    "st. louis city sc": "st louis city",
    "st. louis city": "st louis city",
    "san jose earthquakes": "san jose",
    "los angeles fc": "lafc",
    "houston dynamo fc": "houston dynamo",
    "chicago fire fc": "chicago fire",
    "charlotte fc": "charlotte",
    "austin fc": "austin",
    "nashville sc": "nashville",
    "vancouver whitecaps fc": "vancouver whitecaps",
    "new england revolution": "new england",
    "new england rev": "new england",
    "toronto fc": "toronto",
    "philadelphia union": "philadelphia",
    "dc united": "dc united",
    "colorado rapids": "colorado",
    # UCL common
    "fc barcelona": "barcelona",
    "real madrid cf": "real madrid",
    "manchester city fc": "manchester city",
    "liverpool fc": "liverpool",
}


def _normalize_soccer_team_name(name: str) -> str:
    """Canonicalize soccer team names across providers."""
    import unicodedata
    if not name:
        return name
    # Strip accents
    stripped = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    low = stripped.strip().lower()
    # Direct lookup
    if low in _SOCCER_TEAM_CANON:
        return _SOCCER_TEAM_CANON[low]
    # Strip common suffixes for fuzzy match
    for suffix in (" fc", " cf", " sc", " afc"):
        if low.endswith(suffix):
            trimmed = low[: -len(suffix)].strip()
            if trimmed in _SOCCER_TEAM_CANON:
                return _SOCCER_TEAM_CANON[trimmed]
            return trimmed
    return low


_SOCCER_SPORTS = frozenset({
    "epl", "laliga", "bundesliga", "seriea", "ligue1",
    "ucl", "mls", "nwsl", "ligamx",
})

# ── NHL API ───────────────────────────────────────────────

_NHL_TEAM_NAME_MAP: dict[str, str] = {
    "Montréal Canadiens": "Montreal Canadiens",
    "Montréal": "Montreal",
}

def _nhl_normalize_team_name(name: str) -> str:
    """Normalize NHL API team names to match ESPN names."""
    import unicodedata
    # Replace accented characters
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return _NHL_TEAM_NAME_MAP.get(name, normalized or name)


def _nhl_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    # Prefer new organized schedule layout, but keep legacy fallback.
    regular = _load_json(base / "schedule" / "regular.json")
    playoffs = _load_json(base / "schedule" / "playoffs.json")
    games: list[dict[str, Any]] = []
    for payload in (regular, playoffs):
        if isinstance(payload, list):
            games.extend(payload)
        elif isinstance(payload, dict):
            items = payload.get("games")
            if isinstance(items, list):
                games.extend(items)
            else:
                games.append(payload)
    if not games:
        data = _load_json(base / "schedule.json")
        if not data:
            return []
        games = data if isinstance(data, list) else data.get("games", [data])

    # Build period-score lookup from scores/ directory (goals per period)
    period_scores: dict[str, dict[str, list[int]]] = {}  # gid -> {home: [p1,p2,p3], away: [p1,p2,p3]}
    scores_dir = base / "scores"
    if scores_dir.is_dir():
        for sf in scores_dir.glob("*.json"):
            sd = _load_json(sf)
            if not sd:
                continue
            for sg in sd.get("games", []):
                sgid = str(sg.get("id", ""))
                if not sgid:
                    continue
                goals = sg.get("goals", [])
                if not goals:
                    continue
                ht_id = str(sg.get("homeTeam", {}).get("id", ""))
                home_periods: dict[int, int] = {}
                away_periods: dict[int, int] = {}
                for goal in goals:
                    per = _safe_int(goal.get("period")) or 0
                    team_abbrev = goal.get("teamAbbrev", "")
                    ht_abbrev = sg.get("homeTeam", {}).get("abbrev", "")
                    if team_abbrev == ht_abbrev:
                        home_periods[per] = home_periods.get(per, 0) + 1
                    else:
                        away_periods[per] = away_periods.get(per, 0) + 1
                max_per = max(list(home_periods.keys()) + list(away_periods.keys()) + [0])
                period_scores[sgid] = {
                    "home": [home_periods.get(p, 0) for p in range(1, max_per + 1)],
                    "away": [away_periods.get(p, 0) for p in range(1, max_per + 1)],
                }

    # Build game-detail lookup from games/ directory for SOG, penalties, period scores
    # Supports both legacy flat files and new nested game directories.
    game_details: dict[str, dict] = {}
    games_dir = base / "games"
    if games_dir.is_dir():
        game_files: list[Path] = list(games_dir.glob("*.json"))
        for gt in ("regular", "playoffs"):
            gt_dir = games_dir / gt
            if gt_dir.is_dir():
                game_files.extend(gt_dir.glob("*/*.json"))
        for gf in game_files:
            gd = _load_json(gf)
            if not gd:
                continue
            gd_id = str(gd.get("id", ""))
            if not gd_id:
                continue
            detail: dict[str, Any] = {}
            # SOG (shots on goal)
            h_sog = gd.get("homeTeam", {}).get("sog")
            a_sog = gd.get("awayTeam", {}).get("sog")
            if h_sog is not None:
                detail["home_shots_on_goal"] = _safe_int(h_sog)
            if a_sog is not None:
                detail["away_shots_on_goal"] = _safe_int(a_sog)
            # Penalties from summary
            summary = gd.get("summary", {})
            if isinstance(summary, dict):
                pen_periods = summary.get("penalties", [])
                home_id = str(gd.get("homeTeam", {}).get("id", ""))
                home_abbrev = gd.get("homeTeam", {}).get("abbrev", "")
                h_pen_count = 0
                a_pen_count = 0
                h_pen_min = 0
                a_pen_min = 0
                for pp in pen_periods:
                    for pen in pp.get("penalties", []):
                        dur = _safe_int(pen.get("duration")) or 0
                        ta = pen.get("teamAbbrev", "")
                        if isinstance(ta, dict):
                            ta = ta.get("default", "")
                        if ta == home_abbrev:
                            h_pen_count += 1
                            h_pen_min += dur
                        else:
                            a_pen_count += 1
                            a_pen_min += dur
                if h_pen_count or a_pen_count:
                    detail["home_penalty_count"] = h_pen_count
                    detail["away_penalty_count"] = a_pen_count
                    detail["home_penalty_minutes"] = h_pen_min
                    detail["away_penalty_minutes"] = a_pen_min
                # Period scores from scoring goals
                scoring = summary.get("scoring", [])
                if scoring:
                    h_per: dict[int, int] = {}
                    a_per: dict[int, int] = {}
                    for sp in scoring:
                        per_num = sp.get("periodDescriptor", {}).get("number", 0)
                        for goal in sp.get("goals", []):
                            ta2 = goal.get("teamAbbrev", "")
                            if isinstance(ta2, dict):
                                ta2 = ta2.get("default", "")
                            if ta2 == home_abbrev:
                                h_per[per_num] = h_per.get(per_num, 0) + 1
                            else:
                                a_per[per_num] = a_per.get(per_num, 0) + 1
                    if h_per or a_per:
                        detail["_period_home"] = h_per
                        detail["_period_away"] = a_per
            game_details[gd_id] = detail

    # Build team-stat aggregates from boxscore files (PP goals, hits, blocked, faceoffs)
    if games_dir.is_dir():
        boxscore_files: list[Path] = list(games_dir.glob("*_boxscore.json"))
        for gt in ("regular", "playoffs"):
            gt_dir = games_dir / gt
            if gt_dir.is_dir():
                boxscore_files.extend(gt_dir.glob("*/boxscore.json"))
        for bf in boxscore_files:
            bd = _load_json(bf)
            if not bd:
                continue
            bx_id = str(bd.get("id", ""))
            if not bx_id:
                continue
            detail = game_details.setdefault(bx_id, {})
            pgs = bd.get("playerByGameStats", {})
            home_abbrev = bd.get("homeTeam", {}).get("abbrev", "")
            for side, prefix in [("homeTeam", "home"), ("awayTeam", "away")]:
                team_data = pgs.get(side, {})
                pp_goals = 0
                hits = 0
                blocked = 0
                pim = 0
                takeaways = 0
                giveaways = 0
                faceoff_wins = 0
                faceoff_total = 0
                for pos_group in ("forwards", "defense"):
                    for p in team_data.get(pos_group, []):
                        pp_goals += _safe_int(p.get("powerPlayGoals")) or 0
                        hits += _safe_int(p.get("hits")) or 0
                        blocked += _safe_int(p.get("blockedShots", p.get("blocked"))) or 0
                        pim += _safe_int(p.get("pim")) or 0
                        takeaways += _safe_int(p.get("takeaways")) or 0
                        giveaways += _safe_int(p.get("giveaways")) or 0
                        fw = _safe_int(p.get("faceoffWinningPctg"))
                        fo = _safe_int(p.get("faceoffs"))
                        if fo:
                            faceoff_wins += round(fo * (fw or 0) / 100)
                            faceoff_total += fo
                # Goalie saves / shots against for PP/EV/SH
                team_saves = 0
                team_shots_against = 0
                team_goals_against = 0
                starter_save_pct = None
                for g in team_data.get("goalies", []):
                    pp_sa_str = g.get("powerPlayShotsAgainst", "")
                    if isinstance(pp_sa_str, str) and "/" in pp_sa_str:
                        saves_str, total_str = pp_sa_str.split("/")
                        saves = _safe_int(saves_str) or 0
                        total = _safe_int(total_str) or 0
                        opp = "away" if prefix == "home" else "home"
                        if total > 0:
                            existing_pp = detail.get(f"{opp}_power_play_goals")
                            if existing_pp is None:
                                detail[f"{opp}_power_play_goals"] = total - saves
                    # Aggregate goalie stats
                    g_saves = _safe_int(g.get("saves")) or 0
                    g_sa = _safe_int(g.get("shotsAgainst")) or 0
                    g_ga = _safe_int(g.get("goalsAgainst")) or 0
                    team_saves += g_saves
                    team_shots_against += g_sa
                    team_goals_against += g_ga
                    if g.get("starter") and starter_save_pct is None:
                        starter_save_pct = _safe_float(g.get("savePctg"))
                if team_saves:
                    detail.setdefault(f"{prefix}_saves", team_saves)
                if team_shots_against:
                    detail.setdefault(f"{prefix}_shots_against", team_shots_against)
                if team_goals_against:
                    detail.setdefault(f"{prefix}_goals_against", team_goals_against)
                if starter_save_pct is not None:
                    detail.setdefault(f"{prefix}_save_pct", round(starter_save_pct, 4))
                if pp_goals:
                    detail.setdefault(f"{prefix}_power_play_goals", pp_goals)
                if hits:
                    detail.setdefault(f"{prefix}_hits_nhl", hits)
                if blocked:
                    detail.setdefault(f"{prefix}_blocked_shots", blocked)
                if pim:
                    detail.setdefault(f"{prefix}_penalty_minutes", pim)
                if takeaways:
                    detail.setdefault(f"{prefix}_takeaways", takeaways)
                if giveaways:
                    detail.setdefault(f"{prefix}_giveaways", giveaways)
                if faceoff_total:
                    detail.setdefault(f"{prefix}_faceoff_wins", faceoff_wins)
                    detail.setdefault(f"{prefix}_faceoffs_total", faceoff_total)

    records: list[dict[str, Any]] = []
    for g in games:
        gid = str(g.get("id", ""))
        if not gid:
            continue
        home = g.get("homeTeam", {})
        away = g.get("awayTeam", {})

        def _place(team: dict) -> str:
            pn = team.get("placeName")
            return pn.get("default", "") if isinstance(pn, dict) else str(pn or "")

        venue = g.get("venue")
        venue_name = venue.get("default", "") if isinstance(venue, dict) else str(venue or "")

        cn = home.get("commonName")
        home_name = cn.get("default", "") if isinstance(cn, dict) else str(cn or "")
        cn2 = away.get("commonName")
        away_name = cn2.get("default", "") if isinstance(cn2, dict) else str(cn2 or "")

        date_raw = g.get("gameDate") or g.get("startTimeUTC") or ""
        # NHL startTimeUTC is in UTC — convert to ET so dedup keys align
        # with ESPN dates (also converted to ET).
        game_date = _utc_to_et_date(date_raw) or date_raw[:10] or None
        rec: dict[str, Any] = {
            "id": gid,
            "season": season,
            "date": game_date,
            "status": "final" if g.get("gameState") in ("FINAL", "OFF") else "scheduled",
            "home_team": _nhl_normalize_team_name(f"{_place(home)} {home_name}".strip() or _place(home)),
            "away_team": _nhl_normalize_team_name(f"{_place(away)} {away_name}".strip() or _place(away)),
            "home_team_id": str(home.get("id", "")),
            "away_team_id": str(away.get("id", "")),
            "home_score": _safe_int(home.get("score")),
            "away_score": _safe_int(away.get("score")),
            "venue": venue_name or None,
            "start_time": g.get("startTimeUTC"),
        }

        # Add SOG, penalty, and boxscore-aggregated stats from detail files
        gd_extra = game_details.get(gid, {})
        for field in ("home_shots_on_goal", "away_shots_on_goal",
                       "home_penalty_count", "away_penalty_count",
                       "home_penalty_minutes", "away_penalty_minutes",
                       "home_power_play_goals", "away_power_play_goals",
                       "home_hits_nhl", "away_hits_nhl",
                       "home_blocked_shots", "away_blocked_shots",
                       "home_takeaways", "away_takeaways",
                       "home_giveaways", "away_giveaways",
                       "home_faceoff_wins", "away_faceoff_wins",
                       "home_faceoffs_total", "away_faceoffs_total",
                       "home_saves", "away_saves",
                       "home_shots_against", "away_shots_against",
                       "home_goals_against", "away_goals_against",
                       "home_save_pct", "away_save_pct"):
            if field in gd_extra:
                rec[field] = gd_extra[field]

        # Add period scores — prefer game detail (scoring goals), fall back to scores/
        h_per = gd_extra.get("_period_home", {})
        a_per = gd_extra.get("_period_away", {})
        ps = period_scores.get(gid, {})
        for prefix, per_map, ps_side in [
            ("home", h_per, ps.get("home", [])),
            ("away", a_per, ps.get("away", [])),
        ]:
            periods_from_detail = per_map
            ot_total = 0
            has_ot = False
            if periods_from_detail:
                max_per = max(periods_from_detail.keys()) if periods_from_detail else 0
                for p in range(1, max_per + 1):
                    val = periods_from_detail.get(p, 0)
                    if p <= 3:
                        rec[f"{prefix}_p{p}"] = val
                    else:
                        ot_total += val
                        has_ot = True
            elif ps_side:
                for idx, val in enumerate(ps_side):
                    per = idx + 1
                    if per <= 3:
                        rec[f"{prefix}_p{per}"] = val
                    else:
                        ot_total += val
                        has_ot = True
            if has_ot:
                rec[f"{prefix}_ot"] = ot_total
        records.append(rec)
    return records


def _nhl_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    standings_dir = base / "standings"
    if not standings_dir.is_dir():
        return []
    # Use the latest file that contains standings data
    for p in sorted(standings_dir.glob("*.json"), reverse=True):
        data = _load_json(p)
        if not data:
            continue
        entries = data.get("standings", [])
        if not entries:
            continue
        records: list[dict[str, Any]] = []
        for s in entries:
            abbrev = s.get("teamAbbrev")
            tid = abbrev.get("default", "") if isinstance(abbrev, dict) else str(abbrev or "")
            tname_obj = s.get("teamName")
            team_name = tname_obj.get("default", "") if isinstance(tname_obj, dict) else str(tname_obj or "")
            records.append({
                "team_id": tid,
                "team_name": team_name or tid,
                "season": season,
                "wins": _safe_int(s.get("wins")),
                "losses": _safe_int(s.get("losses")),
                "otl": _safe_int(s.get("otLosses")),
                "pct": _safe_float(s.get("pointPctg")),
                "games_played": _safe_int(s.get("gamesPlayed")),
                "points_for": _safe_int(s.get("goalFor")),
                "points_against": _safe_int(s.get("goalAgainst")),
                "conference": s.get("conferenceName"),
                "division": s.get("divisionName"),
                "conference_rank": _safe_int(s.get("conferenceSequence")),
                "division_rank": _safe_int(s.get("divisionSequence")),
                "streak": _safe_str(s.get("streakCode")),
                "clinch_status": _safe_str(s.get("clinchIndicator")),
            })
        return records
    return []


def _nhl_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load players from NHL API roster files (rosters/{TEAM}.json)."""
    rosters_dir = base / "rosters"
    if not rosters_dir.is_dir():
        return []
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for p in rosters_dir.glob("*.json"):
        data = _load_json(p)
        if not data:
            continue
        team_abbrev = p.stem
        for group in ("forwards", "defensemen", "goalies"):
            for player in data.get(group, []):
                pid = str(player.get("id", ""))
                if not pid or pid in seen:
                    continue
                seen.add(pid)
                first = player.get("firstName", {})
                last = player.get("lastName", {})
                fname = first.get("default", "") if isinstance(first, dict) else str(first)
                lname = last.get("default", "") if isinstance(last, dict) else str(last)
                h_in = player.get("heightInInches")
                height = None
                if h_in is not None:
                    feet, inches = divmod(int(h_in), 12)
                    height = f"{feet}' {inches}\""
                records.append({
                    "id": pid,
                    "name": f"{fname} {lname}".strip(),
                    "team_id": team_abbrev,
                    "position": player.get("positionCode"),
                    "jersey_number": _safe_int(player.get("sweaterNumber")),
                    "height": height,
                    "weight": _safe_int(player.get("weightInPounds")),
                    "headshot_url": player.get("headshot"),
                })
    return records


# ── StatsBomb ─────────────────────────────────────────────

def _statsbomb_load_matches(base: Path) -> list[dict[str, Any]]:
    for candidate in (base / "matches" / "index.json", base / "matches.json"):
        data = _load_json(candidate)
        if data and isinstance(data, list):
            return data

    by_comp = base / "matches" / "by_competition"
    if by_comp.is_dir():
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for fp in sorted(by_comp.glob("**/*.json")):
            chunk = _load_json(fp)
            if not chunk or not isinstance(chunk, list):
                continue
            for match in chunk:
                mid = str(match.get("match_id", ""))
                if not mid or mid in seen:
                    continue
                seen.add(mid)
                rows.append(match)
        if rows:
            return rows
    return []


def _statsbomb_event_files(base: Path) -> list[Path]:
    matches_dir = base / "matches"
    match_scoped = sorted(matches_dir.glob("*/events.json")) if matches_dir.is_dir() else []
    if match_scoped:
        return match_scoped

    legacy_events = base / "events"
    if legacy_events.is_dir():
        return sorted(legacy_events.glob("*.json"))
    return []


def _statsbomb_lineup_files(base: Path) -> list[Path]:
    matches_dir = base / "matches"
    match_scoped = sorted(matches_dir.glob("*/lineups.json")) if matches_dir.is_dir() else []
    if match_scoped:
        return match_scoped

    legacy_lineups = base / "lineups"
    if legacy_lineups.is_dir():
        return sorted(legacy_lineups.glob("*.json"))
    return []


def _statsbomb_lineup_file(base: Path, match_id: str) -> Path:
    match_scoped = base / "matches" / match_id / "lineups.json"
    if match_scoped.exists():
        return match_scoped
    return base / "lineups" / f"{match_id}.json"

def _statsbomb_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _statsbomb_load_matches(base)
    if not data or not isinstance(data, list):
        return []
    records: list[dict[str, Any]] = []
    for m in data:
        ht = m.get("home_team", {})
        at = m.get("away_team", {})
        stadium = m.get("stadium")
        records.append({
            "id": str(m.get("match_id", "")),
            "season": season,
            "date": m.get("match_date"),
            "status": "final",
            "home_team": ht.get("home_team_name", ""),
            "away_team": at.get("away_team_name", ""),
            "home_team_id": str(ht.get("home_team_id", "")),
            "away_team_id": str(at.get("away_team_id", "")),
            "home_score": _safe_int(m.get("home_score")),
            "away_score": _safe_int(m.get("away_score")),
            "venue": stadium.get("name") if isinstance(stadium, dict) else stadium,
        })
    return records


def _statsbomb_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract unique players from StatsBomb lineup files."""
    lineup_files = _statsbomb_lineup_files(base)
    if not lineup_files:
        return []
    seen: dict[str, dict[str, Any]] = {}
    for fp in lineup_files:
        data = _load_json(fp)
        if not data or not isinstance(data, list):
            continue
        for team_block in data:
            tid = str(team_block.get("team_id", ""))
            tname = team_block.get("team_name", "")
            for player in team_block.get("lineup", []):
                pid = str(player.get("player_id", ""))
                if not pid or pid in seen:
                    continue
                positions = player.get("positions", [])
                position = positions[0].get("position") if positions else None
                country = player.get("country", {})
                seen[pid] = {
                    "id": pid,
                    "name": player.get("player_name", ""),
                    "jersey_number": _safe_int(player.get("jersey_number")),
                    "nationality": country.get("name") if isinstance(country, dict) else None,
                    "position": position,
                    "team_id": tid,
                    "team_name": tname,
                }
    return list(seen.values())


def _statsbomb_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Aggregate per-player per-match stats from StatsBomb events & lineups."""
    # Match dates from matches index (or legacy matches.json).
    matches_data = _statsbomb_load_matches(base)
    match_dates: dict[str, str | None] = {}
    if matches_data and isinstance(matches_data, list):
        for m in matches_data:
            match_dates[str(m.get("match_id", ""))] = m.get("match_date")

    event_files = _statsbomb_event_files(base)
    if not event_files:
        return []

    records: list[dict[str, Any]] = []
    for events_file in event_files:
        match_id = events_file.parent.name if events_file.name == "events.json" else events_file.stem
        events = _load_json(events_file)
        if not events or not isinstance(events, list):
            continue

        # Minutes played from lineup positions
        minutes_map: dict[int, int] = {}
        lineup_data = _load_json(_statsbomb_lineup_file(base, match_id))
        if lineup_data and isinstance(lineup_data, list):
            for team_block in lineup_data:
                for pl in team_block.get("lineup", []):
                    pid = pl.get("player_id")
                    total_mins = 0
                    for pos in pl.get("positions", []):
                        from_str = pos.get("from", "00:00") or "00:00"
                        to_str = pos.get("to")
                        from_mins = int(from_str.split(":")[0]) if from_str else 0
                        to_mins = int(to_str.split(":")[0]) if to_str else 90
                        total_mins += max(0, to_mins - from_mins)
                    if pid is not None:
                        minutes_map[pid] = total_mins

        # Aggregate events per player
        player_agg: dict[int, dict[str, Any]] = {}
        for ev in events:
            player = ev.get("player")
            if not player:
                continue
            pid = player.get("id")
            if pid is None:
                continue
            etype = ev.get("type", {}).get("name", "")

            if pid not in player_agg:
                team = ev.get("team", {})
                player_agg[pid] = {
                    "player_name": player.get("name", ""),
                    "team_id": team.get("id"),
                    "team_name": team.get("name", ""),
                    "goals": 0, "assists": 0,
                    "shots": 0, "shots_on_target": 0,
                    "passes": 0, "passes_completed": 0,
                    "tackles": 0, "interceptions": 0,
                    "fouls_committed": 0,
                    "yellow_cards": 0, "red_cards": 0,
                    "xg": 0.0,
                }
            pa = player_agg[pid]

            if etype == "Shot":
                shot = ev.get("shot", {})
                pa["shots"] += 1
                outcome_name = shot.get("outcome", {}).get("name", "")
                if outcome_name == "Goal":
                    pa["goals"] += 1
                    pa["shots_on_target"] += 1
                elif outcome_name == "Saved":
                    pa["shots_on_target"] += 1
                xg_val = shot.get("statsbomb_xg")
                if xg_val is not None:
                    pa["xg"] += float(xg_val)

            elif etype == "Pass":
                pa["passes"] += 1
                pass_data = ev.get("pass", {})
                outcome = pass_data.get("outcome")
                if outcome is None or (isinstance(outcome, dict) and outcome.get("name") == "Complete"):
                    pa["passes_completed"] += 1
                if pass_data.get("goal_assist"):
                    pa["assists"] += 1

            elif etype == "Duel":
                duel_type = ev.get("duel", {}).get("type", {}).get("name", "")
                if "Tackle" in duel_type:
                    pa["tackles"] += 1

            elif etype == "Interception":
                pa["interceptions"] += 1

            elif etype == "Foul Committed":
                pa["fouls_committed"] += 1
                card_name = ev.get("foul_committed", {}).get("card", {})
                card_name = card_name.get("name", "") if isinstance(card_name, dict) else ""
                if "Yellow" in card_name:
                    pa["yellow_cards"] += 1
                elif "Red" in card_name:
                    pa["red_cards"] += 1

            elif etype == "Bad Behaviour":
                card_name = ev.get("bad_behaviour", {}).get("card", {})
                card_name = card_name.get("name", "") if isinstance(card_name, dict) else ""
                if "Yellow" in card_name:
                    pa["yellow_cards"] += 1
                elif "Red" in card_name:
                    pa["red_cards"] += 1

        match_date = match_dates.get(match_id)
        for pid, pa in player_agg.items():
            records.append({
                "id": f"{match_id}_{pid}",
                "player_id": str(pid),
                "player_name": pa["player_name"],
                "team_id": str(pa["team_id"]),
                "team_name": pa["team_name"],
                "game_id": match_id,
                "season": season,
                "date": match_date,
                "goals": pa["goals"],
                "assists": pa["assists"],
                "shots": pa["shots"],
                "shots_on_target": pa["shots_on_target"],
                "passes": pa["passes"],
                "passes_completed": pa["passes_completed"],
                "tackles": pa["tackles"],
                "interceptions": pa["interceptions"],
                "fouls_committed": pa["fouls_committed"],
                "yellow_cards": pa["yellow_cards"],
                "red_cards": pa["red_cards"],
                "xg": round(pa["xg"], 4),
                "minutes_played": minutes_map.get(pid, 0),
            })
    return records


# ── Ergast (F1) ───────────────────────────────────────────

def _ergast_unwrap(data: Any) -> dict:
    if isinstance(data, dict):
        return data.get("MRData", data)
    return {}


_ERGAST_REFERENCE_ENDPOINTS = {"drivers", "constructors", "circuits"}
_ERGAST_STANDINGS_ENDPOINTS = {"driver_standings", "constructor_standings"}
_ERGAST_ROUND_FILE_NAMES = {
    "races": "race.json",
    "results": "results.json",
    "qualifying": "qualifying.json",
    "sprint": "sprint.json",
    "laps": "laps.json",
    "pitstops": "pitstops.json",
}


def _ergast_round_dir(base: Path, round_num: Any) -> Path | None:
    round_id = _safe_int(round_num)
    if round_id is None or round_id <= 0:
        return None
    return base / "rounds" / f"round_{round_id:02d}"


def _ergast_round_file(base: Path, endpoint: str, round_num: Any) -> Path:
    round_dir = _ergast_round_dir(base, round_num)
    if round_dir is not None:
        candidate = round_dir / _ERGAST_ROUND_FILE_NAMES[endpoint]
        if candidate.exists():
            return candidate

    round_id = _safe_int(round_num)
    if round_id is None:
        return base / "__missing__"
    return base / endpoint / f"round_{round_id}.json"


def _ergast_round_files(base: Path, endpoint: str) -> list[Path]:
    file_name = _ERGAST_ROUND_FILE_NAMES[endpoint]
    rounds_dir = base / "rounds"
    files: list[Path] = []

    if rounds_dir.is_dir():
        for round_dir in sorted(rounds_dir.iterdir()):
            if not round_dir.is_dir():
                continue
            round_file = round_dir / file_name
            if round_file.exists():
                files.append(round_file)
        if files:
            return files

    legacy_dir = base / endpoint
    if legacy_dir.is_dir():
        return sorted(legacy_dir.glob("round_*.json"))
    return []


def _ergast_load_endpoint_compat(base: Path, endpoint: str) -> Any:
    if endpoint in _ERGAST_REFERENCE_ENDPOINTS:
        ref_file = base / "reference" / f"{endpoint}.json"
        data = _load_json(ref_file)
        if data is not None:
            return data

    if endpoint in _ERGAST_STANDINGS_ENDPOINTS:
        standings_file = base / "standings" / f"{endpoint}.json"
        data = _load_json(standings_file)
        if data is not None:
            return data

    if endpoint in _ERGAST_ROUND_FILE_NAMES:
        merged_rows: list[dict[str, Any]] = []
        merged_template: dict[str, Any] | None = None
        for round_file in _ergast_round_files(base, endpoint):
            data = _load_json(round_file)
            if data is None:
                continue
            unwrapped = _ergast_unwrap(data)
            race_table = unwrapped.get("RaceTable", {})
            races = race_table.get("Races", [])
            if not isinstance(races, list) or not races:
                continue
            merged_rows.extend(race for race in races if isinstance(race, dict))
            if merged_template is None:
                merged_template = unwrapped

        if merged_rows and merged_template is not None:
            race_table = dict(merged_template.get("RaceTable", {}))
            race_table["season"] = race_table.get("season", str(base.name))
            race_table["Races"] = merged_rows
            merged = dict(merged_template)
            merged["RaceTable"] = race_table
            merged["total"] = str(len(merged_rows))
            return {"MRData": merged}

    return _load_json(base / f"{endpoint}.json")


def _ergast_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _ergast_unwrap(_ergast_load_endpoint_compat(base, "drivers"))
    records: list[dict[str, Any]] = []
    for d in data.get("DriverTable", {}).get("Drivers", []):
        rec: dict[str, Any] = {
            "id": d.get("driverId", ""),
            "name": f"{d.get('givenName', '')} {d.get('familyName', '')}".strip(),
            "jersey_number": _safe_int(d.get("permanentNumber")),
            "nationality": d.get("nationality"),
        }
        if d.get("dateOfBirth"):
            rec["birth_date"] = d["dateOfBirth"]
        records.append(rec)
    return records


def _ergast_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _ergast_unwrap(_ergast_load_endpoint_compat(base, "constructors"))
    return [
        {"id": c.get("constructorId", ""), "name": c.get("name", "")}
        for c in data.get("ConstructorTable", {}).get("Constructors", [])
    ]


def _ergast_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _ergast_unwrap(
        _ergast_load_endpoint_compat(base, "results") or _ergast_load_endpoint_compat(base, "races")
    )
    records: list[dict[str, Any]] = []

    qualifying_pole_by_round: dict[int, str] = {}
    q_data = _ergast_unwrap(_ergast_load_endpoint_compat(base, "qualifying"))
    for race in q_data.get("RaceTable", {}).get("Races", []):
        round_num = _safe_int(race.get("round"))
        if round_num is None:
            continue
        for q in race.get("QualifyingResults", []):
            if str(q.get("position", "")) == "1":
                drv = q.get("Driver", {})
                qualifying_pole_by_round[round_num] = f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip()
                break

    sprint_winner_by_round: dict[int, tuple[str, str]] = {}
    sprint_data = _ergast_unwrap(_ergast_load_endpoint_compat(base, "sprint"))
    for race in sprint_data.get("RaceTable", {}).get("Races", []):
        round_num = _safe_int(race.get("round"))
        if round_num is None:
            continue
        for s in race.get("SprintResults", []):
            if str(s.get("position", "")) == "1":
                drv = s.get("Driver", {})
                cons = s.get("Constructor", {})
                sprint_winner_by_round[round_num] = (
                    f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip(),
                    _safe_str(cons.get("name")) or "",
                )
                break

    for r in data.get("RaceTable", {}).get("Races", []):
        circuit = r.get("Circuit", {})
        loc = circuit.get("Location", {})
        race_name = r.get("raceName", "")
        circuit_name = circuit.get("circuitName", "")
        round_num = _safe_int(r.get("round"))

        # Extract results if available
        results = r.get("Results", [])
        winner_name = winner_team = winner_time = None
        fastest_lap_driver = fastest_lap_time = None
        fastest_lap_num = None
        pole_driver = qualifying_pole_by_round.get(round_num or -1)
        dnf_count = 0
        total_laps = 0
        if results:
            # Winner (position 1)
            for res in results:
                pos = res.get("position", "")
                if str(pos) == "1":
                    drv = res.get("Driver", {})
                    winner_name = f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip()
                    cons = res.get("Constructor", {})
                    winner_team = cons.get("name", "")
                    t = res.get("Time", {})
                    winner_time = t.get("time") if isinstance(t, dict) else None
                # Grid position 1 = pole
                if str(res.get("grid", "")) == "1":
                    drv = res.get("Driver", {})
                    pole_driver = f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip()
                # Count DNFs
                if res.get("status") and res.get("status") not in ("Finished", "+1 Lap", "+2 Laps", "+3 Laps"):
                    laps_done = _safe_int(res.get("laps"))
                    total_race = _safe_int(results[0].get("laps")) if results else 0
                    if laps_done and total_race and laps_done < total_race:
                        dnf_count += 1
                # Total laps from winner
                if str(pos) == "1":
                    total_laps = _safe_int(res.get("laps")) or 0
                # Fastest lap
                fl = res.get("FastestLap", {})
                if isinstance(fl, dict) and str(fl.get("rank", "")) == "1":
                    drv = res.get("Driver", {})
                    fastest_lap_driver = f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip()
                    ft = fl.get("Time", {})
                    fastest_lap_time = ft.get("time") if isinstance(ft, dict) else None
                    fastest_lap_num = _safe_int(fl.get("lap"))

        # Load pitstop data for total pit stops
        pit_file = _ergast_round_file(base, "pitstops", round_num)
        pit_data = _ergast_unwrap(_load_json(pit_file))
        pit_races = pit_data.get("RaceTable", {}).get("Races", [])
        pit_total = len(pit_races[0].get("PitStops", [])) if pit_races else 0

        lap_leader_name = None
        lead_changes = 0
        unique_lap_leaders = 0
        if round_num is not None:
            lap_file = _ergast_round_file(base, "laps", round_num)
            lap_data = _ergast_unwrap(_load_json(lap_file))
            lap_races = lap_data.get("RaceTable", {}).get("Races", [])
            laps_arr = lap_races[0].get("Laps", []) if lap_races else []
            lap_numbers = [_safe_int(l.get("number")) for l in laps_arr if isinstance(l, dict)]
            lap_numbers = [n for n in lap_numbers if n is not None]
            if lap_numbers and not total_laps:
                total_laps = max(lap_numbers)

            driver_name_by_id: dict[str, str] = {}
            for res in results:
                drv = res.get("Driver", {}) if isinstance(res, dict) else {}
                driver_id = _safe_str(drv.get("driverId"))
                if not driver_id:
                    continue
                driver_name_by_id[driver_id] = f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip()

            leaders: list[str] = []
            for lap in laps_arr:
                if not isinstance(lap, dict):
                    continue
                timings = lap.get("Timings", [])
                if not isinstance(timings, list):
                    continue
                leader = next(
                    (_safe_str(t.get("driverId")) for t in timings if isinstance(t, dict) and str(t.get("position", "")) == "1"),
                    None,
                )
                if leader:
                    leaders.append(leader)

            if leaders:
                unique_lap_leaders = len(set(leaders))
                lead_changes = sum(1 for i in range(1, len(leaders)) if leaders[i] != leaders[i - 1])
                lap_leader_name = driver_name_by_id.get(leaders[-1]) or leaders[-1]

        sprint_winner_name = None
        sprint_winner_team = None
        if round_num is not None and round_num in sprint_winner_by_round:
            sprint_winner_name, sprint_winner_team = sprint_winner_by_round[round_num]

        locality = loc.get("locality") if isinstance(loc, dict) else None
        country = loc.get("country") if isinstance(loc, dict) else None
        _race_date = r.get("date", "")
        from datetime import date as _d_cls
        try:
            _ergast_status = "final" if _race_date <= _d_cls.today().isoformat() else "scheduled"
        except Exception:
            _ergast_status = "final" if results else "scheduled"
        records.append({
            "id": f"{r.get('season', season)}_{round_num or r.get('round', '')}",
            "season": season,
            "date": _race_date,
            "status": _ergast_status,
            "home_team": race_name,
            "away_team": f"{locality}, {country}" if locality and country else (country or locality or ""),
            "home_team_id": str(round_num or ""),
            "away_team_id": circuit.get("circuitId", ""),
            "venue": f"{circuit_name}, {locality}" if locality else circuit_name,
            "source": "ergast",
            "race_name": race_name,
            "circuit_name": circuit_name,
            "circuit_id": circuit.get("circuitId", ""),
            "round_number": round_num,
            "total_laps": total_laps or None,
            "winner_name": winner_name,
            "winner_team": winner_team,
            "winner_time": winner_time,
            "fastest_lap_driver": fastest_lap_driver,
            "fastest_lap_time": fastest_lap_time,
            "fastest_lap_number": fastest_lap_num,
            "pole_position_driver": pole_driver,
            "sprint_winner_name": sprint_winner_name,
            "sprint_winner_team": sprint_winner_team,
            "dnf_count": dnf_count or None,
            "pit_stops_total": pit_total or None,
            "lap_leader_name": lap_leader_name,
            "lead_changes": lead_changes or None,
            "unique_lap_leaders": unique_lap_leaders or None,
        })
    return records


def _ergast_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    # Driver standings
    data = _ergast_unwrap(_ergast_load_endpoint_compat(base, "driver_standings"))
    for sl in data.get("StandingsTable", {}).get("StandingsLists", []):
        for ds in sl.get("DriverStandings", []):
            driver = ds.get("Driver", {})
            given = driver.get("givenName", "")
            family = driver.get("familyName", "")
            driver_name = f"{given} {family}".strip() or driver.get("driverId", "")
            constructors = ds.get("Constructors") or []
            team = constructors[0].get("name", "") if constructors else ""
            records.append({
                "team_id": driver.get("driverId", ""),
                "team_name": f"{driver_name} ({team})" if team else driver_name,
                "season": season,
                "wins": _safe_int(ds.get("wins")),
                "losses": 0,
                "overall_rank": _safe_int(ds.get("position")),
                "pct": _safe_float(ds.get("points")),
                "group": "Drivers Championship",
            })
    # Constructor standings
    cdata = _ergast_unwrap(_ergast_load_endpoint_compat(base, "constructor_standings"))
    for sl in cdata.get("StandingsTable", {}).get("StandingsLists", []):
        for cs in sl.get("ConstructorStandings", []):
            cons = cs.get("Constructor", {})
            records.append({
                "team_id": cons.get("constructorId", ""),
                "team_name": cons.get("name", ""),
                "season": season,
                "wins": _safe_int(cs.get("wins")),
                "losses": 0,
                "overall_rank": _safe_int(cs.get("position")),
                "pct": _safe_float(cs.get("points")),
                "group": "Constructors Championship",
            })
    return records


def _ergast_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract per-driver per-race results from Ergast results.json."""
    data = _ergast_unwrap(
        _ergast_load_endpoint_compat(base, "results") or _ergast_load_endpoint_compat(base, "races")
    )
    records: list[dict[str, Any]] = []

    qualifying_pos: dict[tuple[str, str], int] = {}
    q_data = _ergast_unwrap(_ergast_load_endpoint_compat(base, "qualifying"))
    for race in q_data.get("RaceTable", {}).get("Races", []):
        round_num = _safe_str(race.get("round"))
        if not round_num:
            continue
        for q in race.get("QualifyingResults", []):
            drv = q.get("Driver", {})
            driver_id = _safe_str(drv.get("driverId"))
            pos = _safe_int(q.get("position"))
            if driver_id and pos is not None:
                qualifying_pos[(round_num, driver_id)] = pos

    sprint_pos: dict[tuple[str, str], int] = {}
    sprint_pts: dict[tuple[str, str], float] = {}
    sprint_data = _ergast_unwrap(_ergast_load_endpoint_compat(base, "sprint"))
    for race in sprint_data.get("RaceTable", {}).get("Races", []):
        round_num = _safe_str(race.get("round"))
        if not round_num:
            continue
        for s in race.get("SprintResults", []):
            drv = s.get("Driver", {})
            driver_id = _safe_str(drv.get("driverId"))
            if not driver_id:
                continue
            pos = _safe_int(s.get("position"))
            pts = _safe_float(s.get("points"))
            if pos is not None:
                sprint_pos[(round_num, driver_id)] = pos
            if pts is not None:
                sprint_pts[(round_num, driver_id)] = pts

    pit_stops_by_driver_round: dict[tuple[str, str], int] = {}
    for pit_file in _ergast_round_files(base, "pitstops"):
        pit_data = _ergast_unwrap(_load_json(pit_file))
        pit_races = pit_data.get("RaceTable", {}).get("Races", [])
        if not pit_races:
            continue
        race = pit_races[0]
        round_num = _safe_str(race.get("round"))
        if not round_num:
            continue
        for stop in race.get("PitStops", []):
            driver_id = _safe_str(stop.get("driverId"))
            if not driver_id:
                continue
            key = (round_num, driver_id)
            pit_stops_by_driver_round[key] = pit_stops_by_driver_round.get(key, 0) + 1

    lap_metrics_by_driver_round: dict[tuple[str, str], dict[str, float | int]] = {}
    for lap_file in _ergast_round_files(base, "laps"):
        lap_data = _ergast_unwrap(_load_json(lap_file))
        lap_races = lap_data.get("RaceTable", {}).get("Races", [])
        if not lap_races:
            continue
        race = lap_races[0]
        round_num = _safe_str(race.get("round"))
        if not round_num:
            continue
        for lap in race.get("Laps", []):
            if not isinstance(lap, dict):
                continue
            for timing in lap.get("Timings", []):
                if not isinstance(timing, dict):
                    continue
                driver_id = _safe_str(timing.get("driverId"))
                pos = _safe_int(timing.get("position"))
                if not driver_id or pos is None:
                    continue
                key = (round_num, driver_id)
                metric = lap_metrics_by_driver_round.setdefault(key, {
                    "laps_led": 0,
                    "position_sum": 0,
                    "position_count": 0,
                    "best_running_position": 999,
                    "worst_running_position": 0,
                })
                metric["position_sum"] = int(metric["position_sum"]) + pos
                metric["position_count"] = int(metric["position_count"]) + 1
                metric["best_running_position"] = min(int(metric["best_running_position"]), pos)
                metric["worst_running_position"] = max(int(metric["worst_running_position"]), pos)
                if pos == 1:
                    metric["laps_led"] = int(metric["laps_led"]) + 1

    for r in data.get("RaceTable", {}).get("Races", []):
        round_num = r.get("round", "")
        game_id = f"{r.get('season', season)}_{round_num}"
        date = r.get("date")
        race_name = r.get("raceName", "")
        for res in r.get("Results", []):
            drv = res.get("Driver", {})
            cons = res.get("Constructor", {})
            name = f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip()
            finish = _safe_int(res.get("position"))
            grid = _safe_int(res.get("grid"))
            laps = _safe_int(res.get("laps"))
            status = res.get("status", "")
            # Fastest lap
            fl = res.get("FastestLap", {}) or {}
            fl_time = None
            fl_speed = None
            if isinstance(fl, dict):
                t = fl.get("Time", {})
                fl_time = t.get("time") if isinstance(t, dict) else None
                asp = fl.get("AverageSpeed", {})
                fl_speed = _safe_float(asp.get("speed")) if isinstance(asp, dict) else None

            driver_id = _safe_str(drv.get("driverId"))
            lap_metrics = lap_metrics_by_driver_round.get((str(round_num), driver_id), {})
            position_count = int(lap_metrics.get("position_count", 0) or 0)
            avg_running_position = None
            if position_count > 0:
                avg_running_position = round(float(lap_metrics.get("position_sum", 0)) / position_count, 2)

            records.append({
                "player_id": driver_id,
                "player_name": name,
                "team_id": cons.get("constructorId", ""),
                "team_name": cons.get("name", ""),
                "game_id": game_id,
                "date": date,
                "opponent_name": race_name,
                "season": season,
                "source": "ergast",
                "category": "motorsport",
                "qualifying_position": qualifying_pos.get((str(round_num), driver_id)),
                "points": _f1_position_points(finish),
                "sprint_position": sprint_pos.get((str(round_num), driver_id)),
                "sprint_points": sprint_pts.get((str(round_num), driver_id)),
                "finish_position": finish,
                "grid_position": grid,
                "laps": laps,
                "pit_stops": pit_stops_by_driver_round.get((str(round_num), driver_id)),
                "status": status,
                "fastest_lap": fl_time,
                "avg_speed_kph": fl_speed,
                "laps_led": int(lap_metrics.get("laps_led", 0)) or None,
                "avg_running_position": avg_running_position,
                "best_running_position": (
                    int(lap_metrics.get("best_running_position", 999))
                    if int(lap_metrics.get("best_running_position", 999)) != 999 else None
                ),
                "worst_running_position": int(lap_metrics.get("worst_running_position", 0)) or None,
                "dnf": status not in ("Finished", "+1 Lap", "+2 Laps", "+3 Laps", ""),
                "constructor": cons.get("name", ""),
            })
    return records


# ── OpenF1 ────────────────────────────────────────────────

def _openf1_sessions_data(base: Path) -> list[dict[str, Any]]:
    for candidate in (base / "reference" / "sessions.json", base / "sessions.json"):
        data = _load_json(candidate)
        if isinstance(data, list):
            return data
    return []


def _openf1_session_dirs(base: Path) -> dict[int, Path]:
    session_dirs: dict[int, Path] = {}

    structured_root = base / "season_phases"
    if structured_root.exists():
        for session_dir in sorted(structured_root.glob("*/meetings/*/sessions/session_*")):
            if not session_dir.is_dir():
                continue
            match = re.match(r"session_(\d+)$", session_dir.name)
            if not match:
                continue
            session_dirs[int(match.group(1))] = session_dir
        if session_dirs:
            return session_dirs

    for session_dir in sorted(base.iterdir()):
        if session_dir.is_dir() and session_dir.name.isdigit():
            session_dirs[int(session_dir.name)] = session_dir

    return session_dirs


def _openf1_session_file(session_dirs: dict[int, Path], session_key: Any, file_name: str) -> Path | None:
    sk = _safe_int(session_key)
    if sk is None:
        return None
    session_dir = session_dirs.get(sk)
    if session_dir is None:
        return None
    return session_dir / file_name

def _openf1_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for session_dir in _openf1_session_dirs(base).values():
        data = _load_json(session_dir / "drivers.json")
        if not data or not isinstance(data, list):
            continue
        for d in data:
            dnum = str(d.get("driver_number", ""))
            if not dnum or dnum in seen:
                continue
            seen[dnum] = {
                "id": dnum,
                "name": d.get("full_name", d.get("broadcast_name", "")),
                "jersey_number": _safe_int(d.get("driver_number")),
                "nationality": d.get("country_code"),
                "headshot_url": _safe_str(d.get("headshot_url")),
            }
    return list(seen.values())


def _openf1_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Normalize OpenF1 race sessions → Game records with racing fields."""
    sessions_data = _openf1_sessions_data(base)
    if not sessions_data:
        return []

    races = [s for s in sessions_data if s.get("session_type") == "Race"]
    # Sort by date to derive round numbers (F1 rounds are sequential by date)
    races.sort(key=lambda s: s.get("date_start", ""))
    records: list[dict[str, Any]] = []
    session_dirs = _openf1_session_dirs(base)

    for round_idx, race in enumerate(races, start=1):
        sk = race.get("session_key")
        if not sk:
            continue
        date_str = race.get("date_start", "")
        date_only = date_str[:10] if date_str else None
        if not date_only:
            continue

        circuit = race.get("circuit_short_name", "")
        country = race.get("country_name", "")
        location = race.get("location", "")
        session_dir = session_dirs.get(_safe_int(sk) or -1)
        drivers_data = _load_json(session_dir / "drivers.json") if session_dir else []
        driver_map = {}
        for drv in drivers_data or []:
            driver_map[drv.get("driver_number")] = drv

        # Use official session results when available; fall back to last known position snapshots.
        result_data = _load_json(_openf1_session_file(session_dirs, sk, "session_result.json")) or []
        grid_data = _load_json(_openf1_session_file(session_dirs, sk, "starting_grid.json")) or []
        pos_data = _load_json(session_dir / "position.json") if session_dir else None
        winner_name = winner_team = None
        winner_time = None
        pole_position_driver = None
        final_pos: dict[int, int] = {}
        dnf_count = 0
        if isinstance(result_data, list):
            for row in result_data:
                driver_number = row.get("driver_number")
                position = _safe_int(row.get("position"))
                if driver_number is not None and position is not None:
                    final_pos[driver_number] = position
                if row.get("dnf"):
                    dnf_count += 1
                if position == 1 and driver_number in driver_map:
                    drv = driver_map[driver_number]
                    winner_name = drv.get("full_name") or drv.get("broadcast_name")
                    winner_team = drv.get("team_name", "")
                    winner_duration = row.get("duration")
                    winner_time = str(winner_duration) if winner_duration is not None else None

        if pos_data and isinstance(pos_data, list):
            for p in pos_data:
                dn = p.get("driver_number")
                pos = p.get("position")
                if dn and pos and dn not in final_pos:
                    final_pos[dn] = pos
            winner_num = min(final_pos, key=final_pos.get) if final_pos else None
            if not winner_name and winner_num and winner_num in driver_map:
                drv = driver_map[winner_num]
                winner_name = drv.get("full_name") or drv.get("broadcast_name")
                winner_team = drv.get("team_name", "")

        if isinstance(grid_data, list):
            for row in grid_data:
                if _safe_int(row.get("position")) != 1:
                    continue
                driver_number = row.get("driver_number")
                if driver_number in driver_map:
                    drv = driver_map[driver_number]
                    pole_position_driver = drv.get("full_name") or drv.get("broadcast_name")
                break

        # Lap counts for total laps and pit stops
        laps_data = (_load_json(session_dir / "laps.json") if session_dir else None) or []
        max_laps = 0
        fastest_lap_time = None
        fastest_lap_driver = None
        fastest_lap_number = None
        fastest_driver_number = None
        for lap in laps_data:
            ln = _safe_int(lap.get("lap_number"))
            if ln and ln > max_laps:
                max_laps = ln
            dur = _safe_float(lap.get("lap_duration"))
            driver_number = lap.get("driver_number")
            if dur is not None and driver_number is not None and (fastest_lap_time is None or dur < fastest_lap_time):
                fastest_lap_time = dur
                fastest_lap_number = ln
                fastest_driver_number = driver_number

        if fastest_driver_number in driver_map:
            drv = driver_map[fastest_driver_number]
            fastest_lap_driver = drv.get("full_name") or drv.get("broadcast_name")

        fastest_lap_time_str = None
        if fastest_lap_time is not None:
            mins = int(fastest_lap_time // 60)
            secs = fastest_lap_time % 60
            fastest_lap_time_str = f"{mins}:{secs:06.3f}" if mins else f"{secs:.3f}"

        pit_data = (_load_json(session_dir / "pit.json") if session_dir else None) or []
        pit_total = len(pit_data)

        # Race control events for safety cars and red flags
        rc_data = (_load_json(session_dir / "race_control.json") if session_dir else None) or []
        sc_count = sum(1 for e in rc_data if "SAFETY CAR" in str(e.get("message", "")).upper() and "DEPLOYED" in str(e.get("message", "")).upper())
        rf_count = sum(1 for e in rc_data if "RED FLAG" in str(e.get("message", "")).upper())

        race_name = f"{country} Grand Prix"
        from datetime import date as _d_cls
        _race_status = "final"
        if date_only:
            try:
                if date_only > _d_cls.today().isoformat():
                    _race_status = "scheduled"
            except Exception:
                pass
        records.append({
            "id": f"openf1_{sk}",
            "date": date_only,
            "start_time": date_str,
            "home_team": race_name,
            "away_team": f"{location}, {country}" if location and country else (country or location or ""),
            "home_team_id": str(race.get("circuit_key", "")),
            "away_team_id": None,
            "status": _race_status,
            "venue": f"{circuit}, {location}",
            "season": season,
            "source": "openf1",
            "race_name": race_name,
            "circuit_name": circuit,
            "circuit_id": str(race.get("circuit_key", "")),
            "round_number": round_idx,
            "total_laps": max_laps or None,
            "winner_name": winner_name,
            "winner_team": winner_team,
            "winner_time": winner_time,
            "fastest_lap_driver": fastest_lap_driver,
            "fastest_lap_time": fastest_lap_time_str,
            "fastest_lap_number": fastest_lap_number,
            "pole_position_driver": pole_position_driver,
            "dnf_count": dnf_count or None,
            "pit_stops_total": pit_total or None,
            "safety_car_count": sc_count or None,
            "red_flag_count": rf_count or None,
        })
    return records


def _openf1_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Normalize OpenF1 race data → per-driver stats per race using proper F1 fields."""
    sessions_data = _openf1_sessions_data(base)
    if not sessions_data:
        return []

    races = [s for s in sessions_data if s.get("session_type") == "Race"]
    records: list[dict[str, Any]] = []
    session_dirs = _openf1_session_dirs(base)

    for race in races:
        sk = race.get("session_key")
        if not sk:
            continue
        date_str = race.get("date_start", "")
        date_only = date_str[:10] if date_str else None
        country = race.get("country_name", "")
        game_id = f"openf1_{sk}"

        session_dir = session_dirs.get(_safe_int(sk) or -1)
        if session_dir is None or not session_dir.is_dir():
            continue

        drivers_data = _load_json(session_dir / "drivers.json")
        if not drivers_data or not isinstance(drivers_data, list):
            continue
        driver_map = {}
        for drv in drivers_data:
            dn = drv.get("driver_number")
            if dn:
                driver_map[dn] = {
                    "name": drv.get("full_name") or drv.get("broadcast_name", ""),
                    "team": drv.get("team_name", ""),
                }

        # Final positions
        result_data = _load_json(session_dir / "session_result.json") or []
        pos_data = _load_json(session_dir / "position.json") or []
        final_pos: dict[int, int] = {}
        laps_completed: dict[int, int] = {}
        result_status: dict[int, dict[str, Any]] = {}
        if isinstance(result_data, list):
            for row in result_data:
                dn = row.get("driver_number")
                pos = _safe_int(row.get("position"))
                if dn is not None and pos is not None:
                    final_pos[dn] = pos
                if dn is not None:
                    result_status[dn] = row
                    laps_value = _safe_int(row.get("number_of_laps"))
                    if laps_value is not None:
                        laps_completed[dn] = laps_value
        for p in pos_data:
            dn = p.get("driver_number")
            pos = p.get("position")
            if dn and pos and dn not in final_pos:
                final_pos[dn] = pos

        # Lap counts and fastest lap per driver
        laps_data = _load_json(session_dir / "laps.json") or []
        lap_counts: dict[int, int] = {}
        fastest_laps: dict[int, float] = {}
        for lap in laps_data:
            dn = lap.get("driver_number")
            if dn:
                lap_counts[dn] = lap_counts.get(dn, 0) + 1
                dur = lap.get("lap_duration")
                if dur and isinstance(dur, (int, float)):
                    if dn not in fastest_laps or dur < fastest_laps[dn]:
                        fastest_laps[dn] = dur

        # Pit stops
        pit_data = _load_json(session_dir / "pit.json") or []
        pit_counts: dict[int, int] = {}
        for pit in pit_data:
            dn = pit.get("driver_number")
            if dn:
                pit_counts[dn] = pit_counts.get(dn, 0) + 1

        # Stints for tire strategy
        stint_data = _load_json(session_dir / "stints.json") or []
        stint_counts: dict[int, int] = {}
        for stint in stint_data:
            dn = stint.get("driver_number")
            if dn:
                stint_counts[dn] = stint_counts.get(dn, 0) + 1

        max_laps = max(lap_counts.values()) if lap_counts else 0
        for dn, info in driver_map.items():
            finish = final_pos.get(dn)
            driver_laps = laps_completed.get(dn, lap_counts.get(dn, 0))
            result_row = result_status.get(dn, {})
            is_dnf = bool(result_row.get("dnf")) if result_row else (finish is None or (max_laps > 0 and driver_laps < max_laps - 2))
            fl = fastest_laps.get(dn)
            fl_str = None
            if fl:
                mins = int(fl // 60)
                secs = fl % 60
                fl_str = f"{mins}:{secs:06.3f}" if mins else f"{secs:.3f}"
            records.append({
                "player_id": str(dn),
                "player_name": info["name"],
                "team_id": None,
                "team_name": info.get("team", ""),
                "game_id": game_id,
                "date": date_only,
                "opponent_name": f"{country} Grand Prix",
                "season": season,
                "source": "openf1",
                "category": "motorsport",
                "points": _f1_position_points(finish),
                "finish_position": finish,
                "laps": driver_laps,
                "pit_stops": pit_counts.get(dn, 0),
                "fastest_lap": fl_str,
                "status": (
                    "DSQ" if result_row.get("dsq") else "DNS" if result_row.get("dns") else "DNF" if is_dnf else "Finished"
                ),
                "dnf": is_dnf,
                "constructor": info.get("team", ""),
            })
    return records


def _f1_position_points(pos: int | None) -> int:
    """Convert F1 finish position to championship points (2010+ system)."""
    if not pos:
        return 0
    pts_map = {1: 25, 2: 18, 3: 15, 4: 12, 5: 10, 6: 8, 7: 6, 8: 4, 9: 2, 10: 1}
    return pts_map.get(pos, 0)


def _espn_f1_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract unique F1 drivers from ESPN game files."""
    game_files = _espn_game_files(base)
    if not game_files:
        return []
    seen: dict[str, dict[str, Any]] = {}
    for p in game_files:
        data = _load_json(p)
        if not data:
            continue
        summary = data.get("summary", {})
        header = summary.get("header", {})
        for comp in header.get("competitions", []):
            for c in comp.get("competitors", []):
                ath = c.get("athlete", {})
                pid = str(ath.get("id") or ath.get("guid") or "")
                if not pid:
                    pid = ath.get("displayName", "").replace(" ", "_").lower()
                if not pid or pid in seen:
                    continue
                flag = ath.get("flag", {})
                team_obj = ath.get("team", {}) or {}
                seen[pid] = {
                    "id": pid,
                    "name": ath.get("displayName", ath.get("fullName", "")),
                    "first_name": ath.get("displayName", "").split(" ")[0] if ath.get("displayName") else "",
                    "last_name": " ".join(ath.get("displayName", "").split(" ")[1:]) if ath.get("displayName") else "",
                    "team_id": str(team_obj.get("id", "")),
                    "team_name": team_obj.get("displayName", team_obj.get("name", "")),
                    "nationality": flag.get("alt", ""),
                    "position": "Driver",
                    "season": str(season),
                }
    return list(seen.values())


def _espn_f1_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract per-race driver results from ESPN F1 game files."""
    game_files = _espn_game_files(base)
    if not game_files:
        return []
    records: list[dict[str, Any]] = []
    for p in game_files:
        data = _load_json(p)
        if not data:
            continue
        event_id = str(data.get("eventId", p.parent.name if p.parent != base else p.stem))
        sb = data.get("scoreboard", {})
        race_name = sb.get("name") or sb.get("shortName") or ""
        circuit = sb.get("circuit", {})
        circuit_name = circuit.get("fullName", "")
        summary = data.get("summary", {})
        header = summary.get("header", {})
        comps = header.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        game_date = _utc_to_et_date(comp.get("date", "")) or ""
        status = comp.get("status", {}).get("type", {}).get("name", "")
        if "FINAL" not in status.upper() and "COMPLETE" not in status.upper():
            continue
        for c in comp.get("competitors", []):
            ath = c.get("athlete", {})
            pid = str(ath.get("id") or ath.get("guid") or "")
            if not pid:
                pid = ath.get("displayName", "").replace(" ", "_").lower()
            if not pid:
                continue
            position = _safe_int(c.get("order"))
            points = _f1_position_points(position)
            team_obj = ath.get("team", {}) or {}
            # Extract statistics if available
            stats_dict: dict[str, Any] = {}
            for stat in c.get("statistics", []):
                sname = stat.get("name", "").lower().replace(" ", "_")
                if sname:
                    stats_dict[sname] = stat.get("displayValue", stat.get("value"))
            rec: dict[str, Any] = {
                "player_id": pid,
                "player_name": ath.get("displayName", ""),
                "game_id": event_id,
                "date": game_date,
                "season": str(season),
                "team_id": str(team_obj.get("id", "")),
                "team_name": team_obj.get("displayName", ""),
                "race_name": race_name,
                "circuit": circuit_name,
                "finish_position": position,
                "championship_points": points,
                "won": 1 if position == 1 else 0,
                "podium": 1 if position and position <= 3 else 0,
                "top_5": 1 if position and position <= 5 else 0,
                "top_10": 1 if position and position <= 10 else 0,
                "dnf": 1 if position and position > 20 else 0,
            }
            rec.update(stats_dict)
            records.append(rec)
    return records


# ── Lahman (MLB historical) ──────────────────────────────

def _lahman_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    df = _load_csv(base / "Teams.csv")
    if df.empty:
        return []
    try:
        df = df[df["yearID"] == int(season)]
    except (ValueError, KeyError):
        pass
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        records.append({
            "id": str(row.get("teamID", "")),
            "name": _safe_str(row.get("name")) or "",
            "league": _safe_str(row.get("lgID")),
            "division": _safe_str(row.get("divID")),
            "venue_name": _safe_str(row.get("park")),
            "franchise_id": _safe_str(row.get("franchID")),
        })
    return records


def _lahman_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    people_csv = base / "People.csv"
    if not people_csv.exists():
        people_csv = base / "Master.csv"
    df = _load_csv(people_csv)
    if df.empty:
        return []
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        pid = _safe_str(row.get("playerID"))
        if not pid:
            continue
        bdate: str | None = None
        by = _safe_int(row.get("birthYear"))
        if by:
            bm = _safe_int(row.get("birthMonth")) or 1
            bd = _safe_int(row.get("birthDay")) or 1
            bdate = f"{by}-{bm:02d}-{bd:02d}"
        birth_parts = [
            _safe_str(row.get("birthCity")),
            _safe_str(row.get("birthState")),
            _safe_str(row.get("birthCountry")),
        ]
        birth_place = ", ".join([p for p in birth_parts if p]) or None
        records.append({
            "id": pid,
            "name": f"{_safe_str(row.get('nameFirst')) or ''} {_safe_str(row.get('nameLast')) or ''}".strip(),
            "weight": _safe_int(row.get("weight")),
            "height": _safe_int(row.get("height")),
            "bats_hand": _safe_str(row.get("bats")),
            "throws_hand": _safe_str(row.get("throws")),
            "birth_date": bdate,
            "birth_place": birth_place,
            "nationality": _safe_str(row.get("birthCountry")),
        })
    return records


def _lahman_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    batting = _load_csv(base / "Batting.csv")
    if batting.empty:
        return []

    pitching = _load_csv(base / "Pitching.csv")
    try:
        batting = batting[batting["yearID"] == int(season)]
        if not pitching.empty and "yearID" in pitching.columns:
            pitching = pitching[pitching["yearID"] == int(season)]
    except (ValueError, KeyError):
        pass

    pitching_lookup: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if not pitching.empty:
        for _, prow in pitching.iterrows():
            p_pid = _safe_str(prow.get("playerID")) or ""
            p_year = _safe_str(prow.get("yearID")) or str(season)
            p_team = _safe_str(prow.get("teamID")) or ""
            p_stint = _safe_str(prow.get("stint")) or "1"
            if not p_pid:
                continue
            pitching_lookup[(p_pid, p_year, p_team, p_stint)] = {
                "era": _safe_float(prow.get("ERA")),
                "strikeouts": _safe_int(prow.get("SO")),
                "walks": _safe_int(prow.get("BB")),
                "earned_runs": _safe_int(prow.get("ER")),
                "wins": (_safe_int(prow.get("W")) or 0) > 0,
                "losses": (_safe_int(prow.get("L")) or 0) > 0,
                "saves": (_safe_int(prow.get("SV")) or 0) > 0,
                "innings": (
                    round((_safe_int(prow.get("IPouts")) or 0) / 3.0, 3)
                    if _safe_int(prow.get("IPouts")) is not None
                    else None
                ),
                "whip": (
                    round(
                        ((_safe_int(prow.get("H")) or 0) + (_safe_int(prow.get("BB")) or 0))
                        / ((_safe_int(prow.get("IPouts")) or 0) / 3.0),
                        3,
                    )
                    if (_safe_int(prow.get("IPouts")) or 0) > 0
                    else None
                ),
                "games_pitched": _safe_int(prow.get("G")),
                "games_started": _safe_int(prow.get("GS")),
                "complete_games": _safe_int(prow.get("CG")),
                "shutouts": _safe_int(prow.get("SHO")),
                "ibb_pitcher": _safe_int(prow.get("IBB")),
                "wp": _safe_int(prow.get("WP")),
            }

    records: list[dict[str, Any]] = []
    for _, row in batting.iterrows():
        pid = _safe_str(row.get("playerID")) or ""
        if not pid:
            continue
        year = _safe_str(row.get("yearID")) or str(season)
        team_id = _safe_str(row.get("teamID")) or ""
        stint = _safe_str(row.get("stint")) or "1"
        ab = _safe_int(row.get("AB"))
        hits = _safe_int(row.get("H"))
        doubles = _safe_int(row.get("2B"))
        triples = _safe_int(row.get("3B"))
        hr = _safe_int(row.get("HR"))
        bb = _safe_int(row.get("BB"))
        hbp = _safe_int(row.get("HBP"))
        sf = _safe_int(row.get("SF"))
        sh = _safe_int(row.get("SH"))

        ab_val = ab or 0
        hits_val = hits or 0
        doubles_val = doubles or 0
        triples_val = triples or 0
        hr_val = hr or 0
        bb_val = bb or 0
        hbp_val = hbp or 0
        sf_val = sf or 0
        sh_val = sh or 0
        singles_val = max(hits_val - doubles_val - triples_val - hr_val, 0)

        avg = round(hits_val / ab_val, 3) if ab_val > 0 else None
        obp_den = ab_val + bb_val + hbp_val + sf_val
        obp = round((hits_val + bb_val + hbp_val) / obp_den, 3) if obp_den > 0 else None
        total_bases = singles_val + (2 * doubles_val) + (3 * triples_val) + (4 * hr_val)
        slg = round(total_bases / ab_val, 3) if ab_val > 0 else None
        ops = round((obp or 0.0) + (slg or 0.0), 3) if obp is not None and slg is not None else None
        pa = ab_val + bb_val + hbp_val + sf_val + sh_val

        pitch = pitching_lookup.get((pid, year, team_id, stint), {})

        records.append({
            "game_id": f"lahman-{year}-{pid}-{team_id}-{stint}",
            "player_id": pid,
            "team_id": team_id,
            "season": year,
            "category": "baseball",
            "games": _safe_int(row.get("G")),
            "ab": ab,
            "hits": hits,
            "hr": hr,
            "rbi": _safe_int(row.get("RBI")),
            "sb": _safe_int(row.get("SB")),
            "runs": _safe_int(row.get("R")),
            "bb": bb,
            "ibb": _safe_int(row.get("IBB")),
            "so": _safe_int(row.get("SO")),
            "doubles": doubles,
            "triples": triples,
            "pa": pa,
            "cs": _safe_int(row.get("CS")),
            "hbp": hbp,
            "sac_flies": sf,
            "sac_bunts": sh,
            "gidp": _safe_int(row.get("GIDP")),
            "total_bases": total_bases,
            "avg": avg,
            "obp": obp,
            "slg": slg,
            "ops": ops,
            "era": pitch.get("era"),
            "strikeouts": pitch.get("strikeouts"),
            "walks": pitch.get("walks"),
            "innings": pitch.get("innings"),
            "earned_runs": pitch.get("earned_runs"),
            "whip": pitch.get("whip"),
            "win": pitch.get("wins"),
            "loss": pitch.get("losses"),
            "save": pitch.get("saves"),
            "games_pitched": pitch.get("games_pitched"),
            "games_started": pitch.get("games_started"),
            "complete_games": pitch.get("complete_games"),
            "shutouts": pitch.get("shutouts"),
            "ibb_pitcher": pitch.get("ibb_pitcher"),
            "wp": pitch.get("wp"),
        })
    return records


def _lahman_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Lahman does not provide per-game schedule/results in Teams.csv."""
    return []


def _lahman_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    teams_csv = base / "Teams.csv"
    if not teams_csv.exists():
        return []
    df = _load_csv(teams_csv)
    try:
        df = df[df["yearID"] == int(season)]
    except (ValueError, KeyError):
        return []

    rows: list[dict[str, Any]] = []
    for _, t in df.iterrows():
        wins = _safe_int(t.get("W"))
        losses = _safe_int(t.get("L"))
        gp = _safe_int(t.get("G"))
        pct = round((wins / gp), 3) if wins is not None and gp not in (None, 0) else None
        rows.append({
            "team_id": _safe_str(t.get("teamID")) or "",
            "team_name": _safe_str(t.get("name")),
            "sport": sport,
            "season": str(season),
            "wins": wins,
            "losses": losses,
            "pct": pct,
            "games_played": gp,
            "rank": _safe_int(t.get("Rank")),
            "division": _safe_str(t.get("divID")),
            "group": _safe_str(t.get("lgID")),
            "div_winner": _safe_str(t.get("DivWin")) == "Y",
            "wildcard": _safe_str(t.get("WCWin")) == "Y",
            "league_winner": _safe_str(t.get("LgWin")) == "Y",
            "ws_winner": _safe_str(t.get("WSWin")) == "Y",
            "runs_scored": _safe_int(t.get("R")),
            "runs_allowed": _safe_int(t.get("RA")),
            "attendance": _safe_int(t.get("attendance")),
        })
    return rows


# ── Tennis Abstract ───────────────────────────────────────


def _parse_tennis_score(
    score_str: str, swap: bool,
) -> dict[str, int | None]:
    """Parse a tennis score string into per-set game counts.

    Returns keys ``home_q1`` … ``home_q5``, ``away_q1`` … ``away_q5``.
    If *swap* is True, winner and loser sides are flipped.
    """
    result: dict[str, int | None] = {}
    for i in range(1, 6):
        result[f"home_q{i}"] = None
        result[f"away_q{i}"] = None

    if not score_str:
        return result

    # Walkovers – no set scores to parse
    cleaned = score_str.strip()
    if cleaned.upper() in ("W/O", "WO", "WALKOVER"):
        return result

    # Strip trailing retirement markers (e.g. "6-3 4-1 RET", "6-4 Def.")
    cleaned = cleaned.split(" RET")[0].split(" Ret")[0].split(" ret")[0]
    cleaned = cleaned.split(" Def")[0].split(" DEF")[0].split(" def")[0]
    cleaned = cleaned.split(" ABD")[0].split(" Abd")[0]
    cleaned = cleaned.strip()

    sets = cleaned.split()
    for idx, set_score in enumerate(sets):
        if idx >= 5:
            break
        # Strip tiebreak notation, e.g. "7-6(5)" → "7-6"
        set_score = set_score.split("(")[0]
        parts = set_score.split("-")
        if len(parts) != 2:
            continue
        winner_games = _safe_int(parts[0])
        loser_games = _safe_int(parts[1])
        if winner_games is None or loser_games is None:
            continue
        q = idx + 1
        if swap:
            result[f"home_q{q}"] = loser_games
            result[f"away_q{q}"] = winner_games
        else:
            result[f"home_q{q}"] = winner_games
            result[f"away_q{q}"] = loser_games

    return result


def _tennisabstract_games(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    import hashlib

    df = _load_csv(base / "matches.csv")
    if df.empty:
        return []
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        tdate = str(row.get("tourney_date", ""))
        date_str: str | None = None
        if len(tdate) >= 8:
            date_str = f"{tdate[:4]}-{tdate[4:6]}-{tdate[6:8]}"

        match_id = f"{row.get('tourney_id', '')}_{row.get('match_num', '')}"
        winner_name = _safe_str(row.get("winner_name")) or ""
        loser_name = _safe_str(row.get("loser_name")) or ""
        winner_id = str(row.get("winner_id", ""))
        loser_id = str(row.get("loser_id", ""))

        # Deterministically assign winner to home/away to avoid target bias
        swap = int(hashlib.md5(match_id.encode()).hexdigest(), 16) % 2 == 0
        if swap:
            home_name, away_name = loser_name, winner_name
            home_id, away_id = loser_id, winner_id
            home_score, away_score = 0, 1
        else:
            home_name, away_name = winner_name, loser_name
            home_id, away_id = winner_id, loser_id
            home_score, away_score = 1, 0

        set_scores = _parse_tennis_score(
            str(row.get("score", "")), swap,
        )

        # Extract per-match tennis stats (aces, DFs, serve stats, etc.)
        # CSV has w_ace, w_df, w_svpt, w_1stIn, w_1stWon, w_2ndWon, w_SvGms, w_bpSaved, w_bpFaced
        # and matching l_ columns for loser
        w_prefix = "home" if not swap else "away"
        l_prefix = "away" if not swap else "home"
        tennis_stats: dict[str, Any] = {}
        for csv_p, rec_p in (("w_", w_prefix), ("l_", l_prefix)):
            aces = _safe_int(row.get(f"{csv_p}ace"))
            dfs = _safe_int(row.get(f"{csv_p}df"))
            svpt = _safe_int(row.get(f"{csv_p}svpt"))
            first_in = _safe_int(row.get(f"{csv_p}1stIn"))
            first_won = _safe_int(row.get(f"{csv_p}1stWon"))
            second_won = _safe_int(row.get(f"{csv_p}2ndWon"))
            sv_gms = _safe_int(row.get(f"{csv_p}SvGms"))
            bp_saved = _safe_int(row.get(f"{csv_p}bpSaved"))
            bp_faced = _safe_int(row.get(f"{csv_p}bpFaced"))
            if aces is not None:
                tennis_stats[f"{rec_p}_aces"] = aces
            if dfs is not None:
                tennis_stats[f"{rec_p}_double_faults"] = dfs
            if svpt is not None:
                tennis_stats[f"{rec_p}_serve_points"] = svpt
            if first_in is not None and svpt:
                tennis_stats[f"{rec_p}_first_serve_pct"] = round(first_in / svpt * 100, 1)
            if first_in and first_won is not None:
                tennis_stats[f"{rec_p}_first_serve_won_pct"] = round(first_won / first_in * 100, 1) if first_in > 0 else None
            if first_in is not None and svpt and svpt > first_in:
                second_total = svpt - first_in
                if second_won is not None and second_total > 0:
                    tennis_stats[f"{rec_p}_second_serve_won_pct"] = round(second_won / second_total * 100, 1)
            if sv_gms is not None:
                tennis_stats[f"{rec_p}_service_games"] = sv_gms
            if bp_saved is not None:
                tennis_stats[f"{rec_p}_break_points_saved"] = bp_saved
            if bp_faced is not None:
                tennis_stats[f"{rec_p}_break_points_faced"] = bp_faced
            if bp_faced is not None and bp_saved is not None and bp_faced > 0:
                tennis_stats[f"{rec_p}_break_point_save_pct"] = round(bp_saved / bp_faced * 100, 1)
            # Break points won = opponent's bp_faced - opponent's bp_saved
        # Cross-reference break points won
        for csv_p, rec_p, opp_csv in (("w_", w_prefix, "l_"), ("l_", l_prefix, "w_")):
            opp_bp_faced = _safe_int(row.get(f"{opp_csv}bpFaced"))
            opp_bp_saved = _safe_int(row.get(f"{opp_csv}bpSaved"))
            if opp_bp_faced is not None and opp_bp_saved is not None:
                tennis_stats[f"{rec_p}_break_points_won"] = opp_bp_faced - opp_bp_saved
        minutes = _safe_int(row.get("minutes"))
        if minutes is not None:
            tennis_stats["duration_minutes"] = minutes

        records.append({
            "id": match_id,
            "season": season,
            "date": date_str,
            "status": "final",
            "home_team": home_name,
            "away_team": away_name,
            "home_score": home_score,
            "away_score": away_score,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "venue": _safe_str(row.get("tourney_name")),
            "surface": _safe_str(row.get("surface")),
            "round": _safe_str(row.get("round")),
            "best_of": _safe_int(row.get("best_of")),
            **set_scores,
            **tennis_stats,
        })
    return records


def _tennisabstract_players(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    df = _load_csv(base / "matches.csv")
    if df.empty:
        return []
    seen: set[str] = set()
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        for prefix, result in (("winner", "active"), ("loser", "active")):
            pid = str(row.get(f"{prefix}_id", ""))
            if not pid or pid in seen or pid == "nan":
                continue
            seen.add(pid)
            records.append({
                "id": pid,
                "name": _safe_str(row.get(f"{prefix}_name")) or "",
                "nationality": _safe_str(row.get(f"{prefix}_ioc")),
                "height": _safe_str(row.get(f"{prefix}_ht")),
            })
    return records


def _tennisabstract_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    df = _load_csv(base / "matches.csv")
    if df.empty:
        return []
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        mid = f"{row.get('tourney_id', '')}_{row.get('match_num', '')}"
        for prefix, result in (("w", "win"), ("l", "loss")):
            pid_col = "winner_id" if prefix == "w" else "loser_id"
            name_col = "winner_name" if prefix == "w" else "loser_name"
            pid = str(row.get(pid_col, ""))
            if not pid or pid == "nan":
                continue
            svpt = _safe_int(row.get(f"{prefix}_svpt"))
            first_in = _safe_int(row.get(f"{prefix}_1stIn"))
            first_pct = (
                round(first_in / svpt, 3) if svpt and first_in else None
            )
            records.append({
                "game_id": mid,
                "player_id": pid,
                "player_name": _safe_str(row.get(name_col)) or "",
                "season": season,
                "category": "tennis",
                "aces": _safe_int(row.get(f"{prefix}_ace")),
                "double_faults": _safe_int(row.get(f"{prefix}_df")),
                "first_serve_pct": first_pct,
                "result": result,
            })
    return records


def _tennisabstract_standings(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract ATP/WTA rankings from rankings_current.csv."""
    df = _load_csv(base / "rankings_current.csv")
    if df.empty:
        return []

    # Also load matches.csv to resolve player IDs → names
    mdf = _load_csv(base / "matches.csv")
    id_to_name: dict[str, str] = {}
    if not mdf.empty:
        for _, row in mdf.iterrows():
            for prefix in ("winner", "loser"):
                pid = str(row.get(f"{prefix}_id", ""))
                name = _safe_str(row.get(f"{prefix}_name"))
                if pid and pid != "nan" and name:
                    id_to_name[pid] = name

    records: list[dict[str, Any]] = []
    label = "ATP Rankings" if sport == "atp" else "WTA Rankings"
    for _, row in df.iterrows():
        rank = _safe_int(row.get("rank"))
        pid = str(row.get("player", ""))
        points = _safe_int(row.get("points"))
        if not rank or not pid or pid == "nan":
            continue
        player_name = id_to_name.get(pid, f"Player {pid}")
        records.append({
            "team_id": pid,
            "team_name": player_name,
            "season": season,
            "group": label,
            "rank": rank,
            "points": points,
            "wins": None,
            "losses": None,
        })
    return records


# ── UFC Stats ─────────────────────────────────────────────

def _parse_ufc_date(raw: str) -> str | None:
    m = re.search(r"(\w+ \d{1,2}, \d{4})", raw)
    if m:
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _ufcstats_fight_bundle_files(base: Path) -> list[Path]:
    files: list[Path] = []
    legacy_dir = base / "fights"
    if legacy_dir.is_dir():
        files.extend(sorted(legacy_dir.glob("*.json")))

    files.extend(sorted((base / "season_types").glob("**/events/*/fights.json")))
    dedup: dict[str, Path] = {}
    for fp in files:
        dedup[str(fp)] = fp
    return list(dedup.values())


def _ufcstats_fighter_stats_files(base: Path) -> list[Path]:
    files: list[Path] = []
    legacy_dir = base / "fighter_stats"
    if legacy_dir.is_dir():
        files.extend(sorted(legacy_dir.glob("*.json")))

    files.extend(sorted((base / "season_types").glob("**/events/*/fighter_stats/*.json")))
    dedup: dict[str, Path] = {}
    for fp in files:
        dedup[str(fp)] = fp
    return list(dedup.values())


def _ufcstats_path_context(path: Path) -> tuple[str | None, int | None, str | None]:
    parts = path.parts
    season_type: str | None = None
    week_num: int | None = None
    event_date: str | None = None

    if "season_types" in parts:
        idx = parts.index("season_types")
        if idx + 1 < len(parts):
            season_type = parts[idx + 1]
    if "weeks" in parts:
        idx = parts.index("weeks")
        if idx + 1 < len(parts):
            wk = parts[idx + 1]
            m = re.match(r"week_(\d+)", wk)
            if m:
                week_num = int(m.group(1))
    if "dates" in parts:
        idx = parts.index("dates")
        if idx + 1 < len(parts):
            dt = parts[idx + 1]
            if re.match(r"\d{4}-\d{2}-\d{2}", dt):
                event_date = dt

    return season_type, week_num, event_date


def _parse_ufc_inches(raw: Any) -> float | None:
    if not raw or not isinstance(raw, str):
        return None
    text = raw.strip()
    m = re.match(r"(\d+)'\s*(\d+)", text)
    if m:
        return float(int(m.group(1)) * 12 + int(m.group(2)))
    m2 = re.match(r"(\d+)", text)
    if m2:
        return float(int(m2.group(1)))
    return None


def _parse_ufc_weight_lbs(raw: Any) -> int | None:
    if not raw or not isinstance(raw, str):
        return None
    m = re.search(r"(\d+)", raw)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _ufcstats_profile_lookup(base: Path) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    prof_dir = base / "reference" / "fighters"
    if not prof_dir.is_dir():
        return lookup

    for fp in sorted(prof_dir.glob("*.json")):
        prof = _load_json(fp)
        if not prof or not isinstance(prof, dict):
            continue
        nm = _safe_str(prof.get("name", "")).lower()
        if nm:
            lookup[nm] = prof
    return lookup


def _ufcstats_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    import hashlib

    records: list[dict[str, Any]] = []
    fight_files = _ufcstats_fight_bundle_files(base)
    if not fight_files:
        return records

    # Build fighter_stats lookup: fight_id -> {fighter_name -> totals}
    fighter_stats_lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for sp in _ufcstats_fighter_stats_files(base):
        sd = _load_json(sp)
        if not sd or not isinstance(sd, dict):
            continue
        fid = str(sd.get("id", sp.stem))
        fighters = sd.get("fighters", [])
        totals_list = sd.get("totals", [])
        if not fighters or not totals_list:
            continue
        # totals is alternating: fighter1 total, fighter2 total, fighter1 r1, fighter2 r1, ...
        # First two entries are overall totals per fighter
        f_names = [f.get("name", "") if isinstance(f, dict) else str(f) for f in fighters]
        by_name: dict[str, dict[str, Any]] = {}
        if len(totals_list) >= 2:
            for i, fname in enumerate(f_names[:2]):
                if i < len(totals_list) and isinstance(totals_list[i], dict):
                    by_name[fname] = totals_list[i]
        if by_name:
            fighter_stats_lookup[fid] = by_name

    profile_lookup = _ufcstats_profile_lookup(base)

    for p in fight_files:
        data = _load_json(p)
        if not data:
            continue
        event = data.get("event", {})
        path_season_type, path_week, path_date = _ufcstats_path_context(p)
        event_date = _parse_ufc_date(event.get("date", "")) or path_date
        for fight in data.get("fights", []):
            fighters = fight.get("fighters", [])
            fight_id = str(fight.get("id", p.stem))
            result = fight.get("result", "")
            method = fight.get("method", "")
            finish_round = _safe_int(fight.get("round"))
            weight_class = fight.get("weightClass", "")

            f1 = fighters[0] if fighters else ""
            f2 = fighters[1] if len(fighters) > 1 else ""

            if result == "win":
                swap = int(hashlib.md5(fight_id.encode()).hexdigest(), 16) % 2 == 0
                if swap:
                    home, away = f2, f1
                    home_score, away_score = 0, 1
                else:
                    home, away = f1, f2
                    home_score, away_score = 1, 0
            elif result.startswith("draw"):
                home, away = f1, f2
                home_score, away_score = 0, 0
            else:
                home, away = f1, f2
                home_score, away_score = None, None

            # Stable per-fighter IDs derived from normalized name (not fight ID)
            home_fighter_id = hashlib.md5(str(home).strip().lower().encode()).hexdigest()[:16]
            away_fighter_id = hashlib.md5(str(away).strip().lower().encode()).hexdigest()[:16]

            rec: dict[str, Any] = {
                "id": fight_id,
                "season": season,
                "date": event_date,
                "status": "final",
                "season_type": path_season_type or "regular",
                "home_team": home,
                "away_team": away,
                "home_score": home_score,
                "away_score": away_score,
                "home_team_id": home_fighter_id,
                "away_team_id": away_fighter_id,
                "venue": event.get("location", ""),
            }
            if path_week is not None:
                rec["week"] = path_week

            # Merge fighter stats
            fstats = fighter_stats_lookup.get(fight_id, {})
            for prefix, fname in [("home", home), ("away", away)]:
                totals = fstats.get(fname, {})
                if not totals:
                    continue
                kd = _safe_int(totals.get("KD"))
                if kd is not None:
                    rec[f"{prefix}_knockdowns"] = kd

                sig = _parse_x_of_y(totals.get("Sig. str."))
                if sig:
                    rec[f"{prefix}_sig_strikes_landed"] = sig[0]
                    rec[f"{prefix}_sig_strikes_attempted"] = sig[1]
                    if sig[1] > 0:
                        rec[f"{prefix}_sig_strike_pct"] = round(sig[0] / sig[1] * 100, 1)

                total_str = _parse_x_of_y(totals.get("Total str."))
                if total_str:
                    rec[f"{prefix}_total_strikes_landed"] = total_str[0]
                    rec[f"{prefix}_total_strikes_attempted"] = total_str[1]

                td_pct = totals.get("Td %", "")
                if td_pct and td_pct != "---":
                    rec[f"{prefix}_takedown_pct"] = _safe_float(td_pct.replace("%", ""))

                sub_att = _safe_int(totals.get("Sub. att"))
                if sub_att is not None:
                    rec[f"{prefix}_submission_attempts"] = sub_att

                ctrl = _parse_ctrl_time(totals.get("Ctrl"))
                if ctrl:
                    rec[f"{prefix}_control_time_seconds"] = ctrl

                if finish_round is not None:
                    rec[f"{prefix}_finish_round"] = finish_round

            # Merge fighter profile traits used by combat feature extraction
            for prefix, fname in [("home", home), ("away", away)]:
                prof = profile_lookup.get(str(fname).lower())
                if not prof:
                    continue
                reach = _parse_ufc_inches(prof.get("reach"))
                if reach is not None:
                    rec[f"{prefix}_reach"] = reach
                height = _parse_ufc_inches(prof.get("height"))
                if height is not None:
                    rec[f"{prefix}_height"] = height

                if event_date and isinstance(prof.get("dob"), str):
                    dob = _parse_ufc_date(prof.get("dob", ""))
                    if dob:
                        try:
                            event_dt = datetime.strptime(event_date, "%Y-%m-%d").date()
                            dob_dt = datetime.strptime(dob, "%Y-%m-%d").date()
                            age = event_dt.year - dob_dt.year - ((event_dt.month, event_dt.day) < (dob_dt.month, dob_dt.day))
                            if age > 0:
                                rec[f"{prefix}_age"] = float(age)
                        except ValueError:
                            pass

            records.append(rec)
    return records


def _parse_x_of_y(val: Any) -> tuple[int, int] | None:
    """Parse ``'30 of 60'`` → ``(30, 60)``."""
    if not val or not isinstance(val, str):
        return None
    m = re.match(r"(\d+)\s+of\s+(\d+)", val.strip())
    return (int(m.group(1)), int(m.group(2))) if m else None


def _parse_ctrl_time(val: Any) -> int:
    """Parse ``'2:32'`` → 152 seconds."""
    if not val or not isinstance(val, str):
        return 0
    parts = val.strip().split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        if len(parts) == 1 and parts[0]:
            return int(parts[0])
    except ValueError:
        pass
    return 0


def _ufcstats_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract unique fighters from UFC fights and fighter_stats files."""
    seen: dict[str, dict[str, Any]] = {}
    for fp in _ufcstats_fight_bundle_files(base):
        data = _load_json(fp)
        if not data:
            continue
        for fight in data.get("fights", []):
            weight_class = fight.get("weightClass", "")
            for fname in fight.get("fighters", []):
                if not fname:
                    continue
                pid = fname.replace(" ", "_").lower()
                if pid not in seen:
                    seen[pid] = {
                        "id": pid,
                        "name": fname,
                        "weight_class": weight_class,
                    }
    for fp in _ufcstats_fighter_stats_files(base):
        data = _load_json(fp)
        if not data:
            continue
        weight_class = data.get("weightClass", "")
        for f in data.get("fighters", []):
            fname = f.get("name", "")
            if not fname:
                continue
            pid = fname.replace(" ", "_").lower()
            if pid not in seen:
                seen[pid] = {
                    "id": pid,
                    "name": fname,
                    "weight_class": weight_class,
                }

    for prof in _ufcstats_profile_lookup(base).values():
        name = _safe_str(prof.get("name", ""))
        if not name:
            continue
        pid = name.replace(" ", "_").lower()
        row = seen.setdefault(pid, {"id": pid, "name": name, "weight_class": ""})
        if _safe_str(row.get("name", "")) == "":
            row["name"] = name
        if _safe_str(prof.get("height", "")):
            row["height"] = _safe_str(prof.get("height", ""))
        w = _parse_ufc_weight_lbs(prof.get("weight"))
        if w is not None:
            row["weight"] = w
        dob = _parse_ufc_date(_safe_str(prof.get("dob", "")))
        if dob:
            row["birth_date"] = dob
        stance = _safe_str(prof.get("stance", ""))
        if stance:
            row["position"] = stance
    return list(seen.values())


def _ufcstats_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Per-fighter per-fight stats from UFC fighter_stats files."""
    stat_files = _ufcstats_fighter_stats_files(base)
    if not stat_files:
        return []

    # Build event-date lookup from fights directory
    event_dates: dict[str, str | None] = {}
    for fp in _ufcstats_fight_bundle_files(base):
        data = _load_json(fp)
        if not data:
            continue
        event = data.get("event", {})
        _, _, path_date = _ufcstats_path_context(fp)
        event_date = _parse_ufc_date(event.get("date", "")) or path_date
        for fight in data.get("fights", []):
            event_dates[str(fight.get("id", ""))] = event_date

    records: list[dict[str, Any]] = []
    for fp in stat_files:
        data = _load_json(fp)
        if not data:
            continue
        fight_id = str(data.get("id", fp.stem))
        method = data.get("method", "")
        rnd = data.get("round", "")
        fight_time = data.get("time", "")
        weight_class = data.get("weightClass", "")

        fighter_results: dict[str, str] = {}
        for f in data.get("fighters", []):
            fighter_results[f.get("name", "")] = f.get("result", "")

        # Aggregate per-round totals entries per fighter
        fighter_agg: dict[str, dict[str, Any]] = {}
        for entry in data.get("totals", []):
            fname = entry.get("Fighter", "")
            if not fname or "KD" not in entry:
                continue  # skip significant-strikes-breakdown rows
            if fname not in fighter_agg:
                fighter_agg[fname] = {
                    "knockdowns": 0,
                    "sig_strikes_landed": 0, "sig_strikes_attempted": 0,
                    "total_strikes_landed": 0, "total_strikes_attempted": 0,
                    "takedown_pct": None,
                    "submission_attempts": 0, "reversals": 0,
                    "control_time_secs": 0,
                }
            agg = fighter_agg[fname]
            agg["knockdowns"] += _safe_int(entry.get("KD")) or 0

            sig = _parse_x_of_y(entry.get("Sig. str."))
            if sig:
                agg["sig_strikes_landed"] += sig[0]
                agg["sig_strikes_attempted"] += sig[1]

            total = _parse_x_of_y(entry.get("Total str."))
            if total:
                agg["total_strikes_landed"] += total[0]
                agg["total_strikes_attempted"] += total[1]

            td_pct = entry.get("Td %", "")
            if td_pct and td_pct != "---":
                try:
                    agg["takedown_pct"] = int(td_pct.replace("%", ""))
                except ValueError:
                    pass

            agg["submission_attempts"] += _safe_int(entry.get("Sub. att")) or 0
            agg["reversals"] += _safe_int(entry.get("Rev.")) or 0
            agg["control_time_secs"] += _parse_ctrl_time(entry.get("Ctrl"))

        _, _, path_date = _ufcstats_path_context(fp)
        fight_date = event_dates.get(fight_id) or path_date
        for fname, agg in fighter_agg.items():
            pid = fname.replace(" ", "_").lower()
            records.append({
                "id": f"{fight_id}_{pid}",
                "player_id": pid,
                "player_name": fname,
                "team_id": "",
                "team_name": "",
                "game_id": fight_id,
                "season": season,
                "date": fight_date,
                "result": fighter_results.get(fname, ""),
                "method": method,
                "finish_round": _safe_int(rnd),
                "finish_time": fight_time,
                "weight_class": weight_class,
                "knockdowns": agg["knockdowns"],
                "sig_strikes_landed": agg["sig_strikes_landed"],
                "sig_strikes_attempted": agg["sig_strikes_attempted"],
                "total_strikes_landed": agg["total_strikes_landed"],
                "total_strikes_attempted": agg["total_strikes_attempted"],
                "takedown_pct": agg["takedown_pct"],
                "submission_attempts": agg["submission_attempts"],
                "reversals": agg["reversals"],
                "control_time_secs": agg["control_time_secs"],
            })
    return records


# ── OpenDota ──────────────────────────────────────────────

def _opendota_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load OpenDota ``teams.json`` → Team schema records."""
    data = _load_json(base / "reference" / "teams.json")
    if data is None:
        data = _load_json(base / "teams.json")
    if not data or not isinstance(data, list):
        return []
    return [
        {
            "id": str(t.get("team_id", "")),
            "name": t.get("name", ""),
            "abbreviation": t.get("tag"),
            "logo_url": t.get("logo_url"),
            "source": "opendota",
        }
        for t in data
        if t.get("team_id")
    ]


def _opendota_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load OpenDota ``pro_players.json`` → Player schema records."""
    data = _load_json(base / "reference" / "pro_players.json")
    if data is None:
        data = _load_json(base / "pro_players.json")
    if not data or not isinstance(data, list):
        return []
    return [
        {
            "id": str(p.get("account_id", "")),
            "name": p.get("name") or p.get("personaname", ""),
            "team_id": _safe_str(p.get("team_id")),
            "position": None,
            "nationality": p.get("loccountrycode"),
            "headshot_url": p.get("avatarfull"),
            "status": "active" if p.get("is_pro") else "inactive",
            "source": "opendota",
        }
        for p in data
        if p.get("account_id")
    ]


def _opendota_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load OpenDota individual match files → Game schema records.

    Match files are in ``matches/{match_id}.json`` with full game details.
    """
    from datetime import datetime, timezone

    match_files = sorted((base / "season_types").glob("*/weeks/week_*/dates/*/matches/*.json"))
    if not match_files:
        matches_dir = base / "matches"
        if not matches_dir.is_dir():
            return []
        match_files = sorted(matches_dir.glob("*.json"))

    if not match_files:
        return []

    records: list[dict[str, Any]] = []
    for fpath in match_files:
        m = _load_json(fpath)
        if not m or not isinstance(m, dict):
            continue
        mid = m.get("match_id")
        if not mid:
            continue

        start_ts = m.get("start_time")
        if not start_ts:
            continue
        try:
            game_dt = datetime.fromtimestamp(int(start_ts), tz=timezone.utc)
        except (ValueError, OSError):
            continue

        # Filter by season year
        game_year = str(game_dt.year)
        if game_year != season:
            continue

        radiant = m.get("radiant_team") or {}
        dire = m.get("dire_team") or {}
        radiant_win = m.get("radiant_win")
        duration = _safe_int(m.get("duration")) or 0

        home_score = _safe_int(m.get("radiant_score")) or 0
        away_score = _safe_int(m.get("dire_score")) or 0

        # Extract draft picks & bans (hero IDs, comma-separated)
        picks_bans = m.get("picks_bans") or []
        radiant_picks, dire_picks = [], []
        radiant_bans, dire_bans = [], []
        for pb in picks_bans:
            hero = pb.get("hero_id")
            if hero is None:
                continue
            team = pb.get("team")  # 0=radiant, 1=dire
            if pb.get("is_pick"):
                (radiant_picks if team == 0 else dire_picks).append(str(hero))
            else:
                (radiant_bans if team == 0 else dire_bans).append(str(hero))

        # Extract gold/xp advantages at 10/20/30 min marks
        gold_adv = m.get("radiant_gold_adv") or []
        xp_adv = m.get("radiant_xp_adv") or []

        records.append({
            "id": str(mid),
            "date": game_dt.strftime("%Y-%m-%d"),
            "start_time": game_dt.isoformat(),
            "home_team": radiant.get("name") or f"Radiant_{mid}",
            "away_team": dire.get("name") or f"Dire_{mid}",
            "home_team_id": _safe_str(radiant.get("team_id")),
            "away_team_id": _safe_str(dire.get("team_id")),
            "home_score": home_score,
            "away_score": away_score,
            "status": "Final",
            "venue": None,
            "attendance": None,
            "period_scores": None,
            "overtime": False,
            "duration_minutes": round(duration / 60, 1) if duration else None,
            "season": season,
            "source": "opendota",
            # Dota2 draft data
            "home_heroes": ",".join(radiant_picks) if radiant_picks else None,
            "away_heroes": ",".join(dire_picks) if dire_picks else None,
            "home_bans": ",".join(radiant_bans) if radiant_bans else None,
            "away_bans": ",".join(dire_bans) if dire_bans else None,
            # Economy snapshots (gold advantage at minute marks)
            "gold_adv_10": gold_adv[10] if len(gold_adv) > 10 else None,
            "gold_adv_20": gold_adv[20] if len(gold_adv) > 20 else None,
            "gold_adv_30": gold_adv[30] if len(gold_adv) > 30 else None,
            "xp_adv_10": xp_adv[10] if len(xp_adv) > 10 else None,
            "xp_adv_20": xp_adv[20] if len(xp_adv) > 20 else None,
            "xp_adv_30": xp_adv[30] if len(xp_adv) > 30 else None,
        })
    return records


def _opendota_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load OpenDota match files → per-player stat records.

    Each match has ~10 players with detailed performance stats.
    """
    from datetime import datetime, timezone

    match_files = sorted((base / "season_types").glob("*/weeks/week_*/dates/*/matches/*.json"))
    if not match_files:
        matches_dir = base / "matches"
        if not matches_dir.is_dir():
            return []
        match_files = sorted(matches_dir.glob("*.json"))

    if not match_files:
        return []

    records: list[dict[str, Any]] = []
    for fpath in match_files:
        m = _load_json(fpath)
        if not m or not isinstance(m, dict):
            continue
        mid = m.get("match_id")
        if not mid:
            continue

        start_ts = m.get("start_time")
        if not start_ts:
            continue
        try:
            game_dt = datetime.fromtimestamp(int(start_ts), tz=timezone.utc)
        except (ValueError, OSError):
            continue

        game_year = str(game_dt.year)
        if game_year != season:
            continue

        game_date = game_dt.strftime("%Y-%m-%d")
        radiant = m.get("radiant_team") or {}
        dire = m.get("dire_team") or {}

        players = m.get("players")
        if not players or not isinstance(players, list):
            continue

        for p in players:
            acct = p.get("account_id")
            if not acct:
                continue

            is_radiant = p.get("isRadiant", p.get("player_slot", 0) < 128)
            team_info = radiant if is_radiant else dire
            opp_info = dire if is_radiant else radiant

            records.append({
                "player_id": str(acct),
                "player_name": p.get("personaname") or p.get("name") or str(acct),
                "team_id": _safe_str(team_info.get("team_id")),
                "team_name": team_info.get("name") or "",
                "game_id": str(mid),
                "date": game_date,
                "opponent_id": _safe_str(opp_info.get("team_id")),
                "opponent_name": opp_info.get("name") or "",
                # Core stats (mapped to EsportsStats schema)
                "kills": _safe_int(p.get("kills")),
                "deaths": _safe_int(p.get("deaths")),
                "assists": _safe_int(p.get("assists")),
                # Schema fields
                "damage": _safe_int(p.get("hero_damage")),
                "turrets_destroyed": _safe_int(p.get("tower_damage")),
                "gold_per_min": _safe_int(p.get("gold_per_min")),
                "cs_per_min": round(
                    (_safe_int(p.get("last_hits")) or 0) / max((_safe_int(m.get("duration")) or 1) / 60, 1), 1
                ),
                "kda": round(
                    ((_safe_int(p.get("kills")) or 0) + (_safe_int(p.get("assists")) or 0))
                    / max(_safe_int(p.get("deaths")) or 1, 1), 2
                ),
                "objectives": (
                    (_safe_int(p.get("tower_damage")) or 0) > 0
                    and _safe_int(p.get("towers_killed"))
                ) or None,
                "minutes": round((_safe_int(m.get("duration")) or 0) / 60, 1),
                "season": season,
                "source": "opendota",
            })
    return records


def _opendota_draft(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract Dota draft-phase picks/bans into normalized draft rows."""
    from datetime import datetime, timezone

    match_files = sorted((base / "season_types").glob("*/weeks/week_*/dates/*/matches/*.json"))
    if not match_files:
        matches_dir = base / "matches"
        if not matches_dir.is_dir():
            return []
        match_files = sorted(matches_dir.glob("*.json"))

    out: list[dict[str, Any]] = []
    for fpath in match_files:
        m = _load_json(fpath)
        if not m or not isinstance(m, dict):
            continue

        mid = _safe_str(m.get("match_id"))
        if not mid:
            continue

        start_ts = m.get("start_time")
        if not start_ts:
            continue
        try:
            game_dt = datetime.fromtimestamp(int(start_ts), tz=timezone.utc)
        except (ValueError, OSError):
            continue
        if str(game_dt.year) != season:
            continue

        radiant = m.get("radiant_team") if isinstance(m.get("radiant_team"), dict) else {}
        dire = m.get("dire_team") if isinstance(m.get("dire_team"), dict) else {}

        picks_bans = m.get("picks_bans")
        if isinstance(picks_bans, list) and picks_bans:
            for idx, pb in enumerate(picks_bans, start=1):
                if not isinstance(pb, dict):
                    continue
                hero_id = _safe_str(pb.get("hero_id"))
                if not hero_id:
                    continue
                team_side = pb.get("team")
                team_info = radiant if team_side == 0 else dire
                is_pick = bool(pb.get("is_pick"))
                draft_id = f"opendota|{season}|{mid}|{idx}|{hero_id}"
                out.append({
                    "draft_id": draft_id,
                    "draft_year": season,
                    "round": None,
                    "pick": idx,
                    "overall_pick": idx,
                    "player_id": "",
                    "player_name": "",
                    "position": "pick" if is_pick else "ban",
                    "team_id": _safe_str(team_info.get("team_id")) or ("radiant" if team_side == 0 else "dire"),
                    "team_name": _safe_str(team_info.get("name")) or ("Radiant" if team_side == 0 else "Dire"),
                    "hero_id": hero_id,
                    "game_id": mid,
                    "sport": sport,
                    "season": season,
                    "source": "opendota",
                })
            continue

        draft_timings = m.get("draft_timings")
        if isinstance(draft_timings, list):
            for dt in draft_timings:
                if not isinstance(dt, dict):
                    continue
                hero_id = _safe_str(dt.get("hero_id"))
                order = _safe_int(dt.get("order"))
                if not hero_id or order is None:
                    continue
                team_code = _safe_int(dt.get("active_team"))
                # OpenDota active_team commonly maps 2=radiant, 3=dire
                team_info = radiant if team_code == 2 else dire
                is_pick = bool(dt.get("pick"))
                draft_id = f"opendota|{season}|{mid}|{order}|{hero_id}"
                out.append({
                    "draft_id": draft_id,
                    "draft_year": season,
                    "round": None,
                    "pick": order,
                    "overall_pick": order,
                    "player_id": "",
                    "player_name": "",
                    "position": "pick" if is_pick else "ban",
                    "team_id": _safe_str(team_info.get("team_id")) or ("radiant" if team_code == 2 else "dire"),
                    "team_name": _safe_str(team_info.get("name")) or ("Radiant" if team_code == 2 else "Dire"),
                    "hero_id": hero_id,
                    "game_id": mid,
                    "sport": sport,
                    "season": season,
                    "source": "opendota",
                })

    return out

def _oddsapi_odds(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load OddsAPI odds from ``.../odds/{date}/`` under the resolved provider base.

    Preferred path is ``data/raw/odds/providers/oddsapi/{sport}/{season}/``
    with fallback to legacy ``data/raw/oddsapi/{sport}/{season}/``.
    """
    records: list[dict[str, Any]] = []
    odds_dir = base / "odds"
    if not odds_dir.is_dir():
        return records
    for date_dir in sorted(odds_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        for fp in date_dir.glob("*.json"):
            data = _load_json(fp)
            if not data:
                continue
            events = data.get("events", [])
            if not isinstance(events, list):
                continue
            for ev in events:
                event_id = str(ev.get("id", ""))
                home_team = ev.get("homeTeam", "")
                away_team = ev.get("awayTeam", "")
                commence_time = ev.get("commenceTime", "")
                event_date = commence_time[:10] if commence_time else str(date_dir.name)
                if not event_id:
                    continue
                for bm in ev.get("bookmakers", []):
                    bm_name = bm.get("title") or bm.get("key", "unknown")
                    rec: dict[str, Any] = {
                        "game_id": event_id,
                        "date": event_date,
                        "commence_time": commence_time or None,
                        "home_team": home_team,
                        "away_team": away_team,
                        "bookmaker": bm_name,
                        "h2h_home": None,
                        "h2h_away": None,
                        "spread_home": None,
                        "spread_home_line": None,
                        "spread_away_line": None,
                        "total_line": None,
                        "total_over": None,
                        "total_under": None,
                    }
                    for market in bm.get("markets", []):
                        key = market.get("key", "")
                        outcomes = {
                            o.get("name", ""): o
                            for o in market.get("outcomes", [])
                        }
                        if key == "h2h":
                            rec["h2h_home"] = _safe_float(
                                outcomes.get(home_team, {}).get("price"),
                            )
                            rec["h2h_away"] = _safe_float(
                                outcomes.get(away_team, {}).get("price"),
                            )
                        elif key == "spreads":
                            h = outcomes.get(home_team, {})
                            a = outcomes.get(away_team, {})
                            rec["spread_home"] = _safe_float(h.get("point"))
                            rec["spread_home_line"] = _safe_float(h.get("price"))
                            rec["spread_away_line"] = _safe_float(a.get("price"))
                        elif key == "totals":
                            over = outcomes.get("Over", {})
                            under = outcomes.get("Under", {})
                            rec["total_line"] = _safe_float(over.get("point"))
                            rec["total_over"] = _safe_float(over.get("price"))
                            rec["total_under"] = _safe_float(under.get("price"))
                    records.append(rec)
    return records


def _sgo_odds(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load SGO odds from ``.../odds/{date}/`` under the resolved provider base.

    Preferred path is ``data/raw/odds/providers/sgo/{sport}/{season}/``
    with fallback to legacy ``data/raw/sgo/{sport}/{season}/``.

    SGO files contain a ``records`` list of per-bookmaker odds rows written by
    the v5.0 SGO TypeScript provider.  The ``game_id`` field is the SGO event
    ID; team names are stored in ``_home_team_long`` / ``_away_team_long``.
    """
    records: list[dict[str, Any]] = []
    odds_dir = base / "odds"
    if not odds_dir.is_dir():
        return records
    for date_dir in sorted(odds_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        for fp in date_dir.glob("*.json"):
            data = _load_json(fp)
            if not data:
                continue
            raw_records = data.get("records", [])
            if not isinstance(raw_records, list):
                continue
            snap_date = data.get("date", str(date_dir.name))
            for r in raw_records:
                game_id = str(r.get("game_id") or r.get("_sgo_event_id", ""))
                if not game_id:
                    continue
                event_date = r.get("_event_date") or snap_date
                home_team  = r.get("_home_team_long", "")
                away_team  = r.get("_away_team_long", "")
                records.append({
                    "game_id":        game_id,
                    "date":           event_date,
                    "commence_time":  None,
                    "home_team":      home_team,
                    "away_team":      away_team,
                    "bookmaker":      r.get("vendor", "unknown"),
                    "h2h_home":       _safe_float(r.get("moneyline_home_odds")),
                    "h2h_away":       _safe_float(r.get("moneyline_away_odds")),
                    "spread_home":    _safe_float(r.get("spread_home_value")),
                    "spread_home_line": _safe_float(r.get("spread_home_odds")),
                    "spread_away_line": _safe_float(r.get("spread_away_odds")),
                    "total_line":     _safe_float(r.get("total_value")),
                    "total_over":     _safe_float(r.get("total_over_odds")),
                    "total_under":    _safe_float(r.get("total_under_odds")),
                })
    return records


# ── CFBData (CollegeFootballData.com) ─────────────────────

def _cfbdata_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_json_compat(base, season, "games", "games.json")
    if not data or not isinstance(data, list):
        return []
    records: list[dict[str, Any]] = []
    for g in data:
        gid = str(g.get("id", ""))
        if not gid:
            continue
        rec: dict[str, Any] = {
            "id": gid,
            "season": str(g.get("season", season)),
            "date": (g.get("startDate") or "")[:10] or None,
            "status": "final" if g.get("completed") else "scheduled",
            "home_team": g.get("homeTeam", ""),
            "away_team": g.get("awayTeam", ""),
            "home_team_id": str(g.get("homeId", "")),
            "away_team_id": str(g.get("awayId", "")),
            "home_score": _safe_int(g.get("homePoints")),
            "away_score": _safe_int(g.get("awayPoints")),
            "venue": g.get("venue"),
            "attendance": _safe_int(g.get("attendance")),
        }
        # Season type & week number are embedded in the source JSON
        _cfb_st_raw = (g.get("seasonType") or "regular").lower()
        _cfb_st_map = {"regular": "regular", "postseason": "postseason", "bowl": "postseason", "spring": "preseason"}
        rec["season_type"] = _cfb_st_map.get(_cfb_st_raw, _cfb_st_raw)
        rec["week"] = _safe_int(g.get("week"))
        # Extract quarter scores from CFBData homeLineScores/awayLineScores
        for prefix, key in [("home", "homeLineScores"), ("away", "awayLineScores")]:
            ls = g.get(key)
            if isinstance(ls, list):
                ot_total = 0
                has_ot = False
                for idx, val in enumerate(ls):
                    v = _safe_int(val)
                    if v is None:
                        continue
                    period = idx + 1
                    if period <= 4:
                        rec[f"{prefix}_q{period}"] = v
                    else:
                        ot_total += v
                        has_ot = True
                if has_ot:
                    rec[f"{prefix}_ot"] = ot_total
        records.append(rec)
    return records


def _cfbdata_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Build standings-like records from CFBData rankings polls."""
    data = _load_cfbdata_json_compat(base, season, "rankings", "rankings.json")
    if not data or not isinstance(data, list):
        return []
    best_records: list[dict[str, Any]] = []
    # Choose the poll week with the richest unique-team coverage. This is more
    # stable than relying on file order after repartitioning by seasonType/week.
    for poll_week in data:
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        for poll in poll_week.get("polls", []):
            for entry in poll.get("ranks", []):
                tid = str(entry.get("teamId", ""))
                if not tid or tid in seen:
                    continue
                seen.add(tid)
                records.append({
                    "team_id": tid,
                    "team_name": entry.get("school", ""),
                    "conference": entry.get("conference", ""),
                    "rank": _safe_int(entry.get("rank")),
                    "season": season,
                })
        if len(records) > len(best_records):
            best_records = records
    return best_records


def _cfbdata_team_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Pivot CFBData season stats (one-row-per-stat) into one-row-per-team."""
    data = _load_cfbdata_endpoint_json(base, "stats_season.json")
    teams: dict[str, dict[str, Any]] = {}
    if data and isinstance(data, list):
        for entry in data:
            team = entry.get("team", "")
            if not team:
                continue
            if team not in teams:
                teams[team] = {
                    "team_id": team,
                    "team_name": team,
                    "conference": entry.get("conference", ""),
                    "season": season,
                }
            stat_name = entry.get("statName", "")
            if stat_name:
                teams[team][stat_name] = _safe_float(entry.get("statValue"))

    # Enrich with advanced stats (offense/defense top-level metrics)
    adv_data = _load_cfbdata_endpoint_json(base, "stats_advanced.json")
    if adv_data and isinstance(adv_data, list):
        for entry in adv_data:
            team = entry.get("team", "")
            if not team:
                continue
            if team not in teams:
                teams[team] = {
                    "team_id": team,
                    "team_name": team,
                    "conference": entry.get("conference", ""),
                    "season": season,
                }
            for side in ("offense", "defense"):
                block = entry.get(side, {})
                if not isinstance(block, dict):
                    continue
                prefix = "off_" if side == "offense" else "def_"
                for k, v in block.items():
                    if isinstance(v, (int, float)):
                        teams[team][f"{prefix}{k}"] = _safe_float(v)

    return list(teams.values())


def _cfbdata_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "teams.json")
    if not data or not isinstance(data, list):
        return []

    records: list[dict[str, Any]] = []
    for t in data:
        tid = _safe_str(t.get("id"))
        name = _safe_str(t.get("school")) or _safe_str(t.get("team"))
        if not tid or not name:
            continue
        records.append({
            "id": tid,
            "name": name,
            "abbreviation": _safe_str(t.get("abbreviation")),
            "city": _safe_str(t.get("location")),
            "conference": _safe_str(t.get("conference")),
            "division": _safe_str(t.get("division")),
            "source": "cfbdata",
        })
    return records


def _cfbdata_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    players: dict[str, dict[str, Any]] = {}

    def _fallback_pid(name: str, team: str | None = None) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        team_slug = re.sub(r"[^a-z0-9]+", "_", (team or "").lower()).strip("_")
        if team_slug:
            return f"cfb_{season}_{team_slug}_{slug}"
        return f"cfb_{season}_{slug}"

    def _upsert_player(
        pid: str | None,
        name: str | None,
        team_id: str | None = None,
        position: str | None = None,
    ) -> None:
        if not name:
            return
        final_pid = pid or _fallback_pid(name, team_id)
        rec = players.setdefault(final_pid, {
            "id": final_pid,
            "name": name,
            "source": "cfbdata",
        })
        if team_id:
            rec.setdefault("team_id", team_id)
        if position:
            rec.setdefault("position", position)

    roster = _load_cfbdata_endpoint_json(base, "roster.json")
    if roster and isinstance(roster, list):
        for p in roster:
            pid = _safe_str(p.get("id")) or _safe_str(p.get("playerId"))
            if not pid:
                continue
            full_name = (
                f"{_safe_str(p.get('firstName'))} {_safe_str(p.get('lastName'))}".strip()
                if _safe_str(p.get("firstName")) and _safe_str(p.get("lastName"))
                else _safe_str(p.get("name")) or ""
            )
            _upsert_player(
                pid=pid,
                name=full_name,
                team_id=_safe_str(p.get("team")) or _safe_str(p.get("school")),
                position=_safe_str(p.get("position")),
            )
            if pid in players:
                players[pid].setdefault("height", _safe_str(p.get("height")))
                players[pid].setdefault("weight", _safe_float(p.get("weight")))
                players[pid].setdefault("class_year", _safe_str(p.get("year")))

    portal = _load_cfbdata_endpoint_json(base, "player_portal.json")
    if portal and isinstance(portal, list):
        for p in portal:
            pid = _safe_str(p.get("playerId")) or _safe_str(p.get("id"))
            name = _safe_str(p.get("player")) or _safe_str(p.get("name"))
            if not pid or not name:
                continue
            _upsert_player(
                pid=pid,
                name=name,
                team_id=_safe_str(p.get("destination")) or _safe_str(p.get("school")),
                position=_safe_str(p.get("position")),
            )
            rec = players[pid]
            rec.setdefault("position", _safe_str(p.get("position")))
            rec.setdefault("team_id", _safe_str(p.get("destination")) or _safe_str(p.get("school")))

    returning = _load_cfbdata_endpoint_json(base, "player_returning.json")
    if returning and isinstance(returning, list):
        for p in returning:
            pid = _safe_str(p.get("playerId")) or _safe_str(p.get("id"))
            name = _safe_str(p.get("name"))
            if not pid or not name:
                continue
            _upsert_player(
                pid=pid,
                name=name,
                team_id=_safe_str(p.get("team")),
                position=_safe_str(p.get("position")),
            )
            rec = players[pid]
            rec.setdefault("position", _safe_str(p.get("position")))
            rec.setdefault("team_id", _safe_str(p.get("team")))

    # Fallback for seasons where roster endpoints were not imported yet.
    # stats_player_season is often available and includes player/team names.
    stats_player = _load_cfbdata_json_compat(base, season, "stats_player_season", "stats_player_season.json")
    if stats_player and isinstance(stats_player, list):
        for p in stats_player:
            name = _safe_str(p.get("player")) or _safe_str(p.get("name"))
            team = _safe_str(p.get("team")) or _safe_str(p.get("school"))
            pid = _safe_str(p.get("playerId")) or _safe_str(p.get("id"))
            _upsert_player(pid=pid, name=name, team_id=team)

    # Additional fallback from game-level player stats payloads.
    games_players = _load_cfbdata_json_compat(base, season, "games_players", "games_players.json")
    if games_players and isinstance(games_players, list):
        for game in games_players:
            teams = game.get("teams") if isinstance(game, dict) else None
            if not isinstance(teams, list):
                continue
            for t in teams:
                team_name = _safe_str(t.get("team")) if isinstance(t, dict) else None
                categories = t.get("categories") if isinstance(t, dict) else None
                if not isinstance(categories, list):
                    continue
                for cat in categories:
                    stats = cat.get("types") if isinstance(cat, dict) else None
                    if not isinstance(stats, list):
                        continue
                    for s in stats:
                        athletes = s.get("athletes") if isinstance(s, dict) else None
                        if not isinstance(athletes, list):
                            continue
                        for a in athletes:
                            if not isinstance(a, dict):
                                continue
                            pid = _safe_str(a.get("id")) or _safe_str(a.get("playerId"))
                            name = _safe_str(a.get("name")) or _safe_str(a.get("player"))
                            _upsert_player(pid=pid, name=name, team_id=team_name)

    return [r for r in players.values() if r.get("id") and r.get("name")]


def _cfbdata_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_json_compat(base, season, "stats_player_season", "stats_player_season.json")
    if not data or not isinstance(data, list):
        return []

    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in data:
        pid = _safe_str(entry.get("playerId")) or _safe_str(entry.get("id"))
        name = _safe_str(entry.get("player")) or _safe_str(entry.get("name"))
        if not pid or not name:
            continue
        team = _safe_str(entry.get("team")) or _safe_str(entry.get("school"))
        key = (pid, team or "")
        rec = rows.setdefault(key, {
            "game_id": f"cfb_season_{season}",
            "player_id": pid,
            "player_name": name,
            "team_id": team,
            "season": season,
            "source": "cfbdata",
        })

        stat_name = _safe_str(entry.get("statName"))
        stat_val = _safe_float(entry.get("statValue"))
        if stat_name and stat_val is not None:
            rec[stat_name] = stat_val

    usage = _load_cfbdata_endpoint_json(base, "player_usage.json")
    if usage and isinstance(usage, list):
        for entry in usage:
            pid = _safe_str(entry.get("playerId")) or _safe_str(entry.get("id"))
            if not pid:
                continue
            team = _safe_str(entry.get("team")) or ""
            key = (pid, team)
            rec = rows.setdefault(key, {
                "game_id": f"cfb_season_{season}",
                "player_id": pid,
                "player_name": _safe_str(entry.get("name")) or _safe_str(entry.get("player")) or pid,
                "team_id": team or None,
                "season": season,
                "source": "cfbdata",
            })
            for k, v in entry.items():
                if isinstance(v, (int, float)):
                    rec[f"usage_{k}"] = _safe_float(v)

    return list(rows.values())


def _cfbdata_odds(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_json_compat(base, season, "lines", "lines.json")
    if not data or not isinstance(data, list):
        return []

    records: list[dict[str, Any]] = []
    for r in data:
        if not isinstance(r, dict):
            continue
        game_id = _safe_str(r.get("id"))
        if not game_id:
            continue

        start_date = _safe_str(r.get("startDate") or r.get("date"))
        base_row = {
            "game_id": game_id,
            "date": start_date[:10] if start_date else None,
            "commence_time": _safe_datetime(start_date),
            "home_team": _safe_str(r.get("homeTeam")),
            "away_team": _safe_str(r.get("awayTeam")),
            "is_live": False,
            "source": "cfbdata",
        }

        lines = r.get("lines")
        if isinstance(lines, list) and lines:
            for ln in lines:
                if not isinstance(ln, dict):
                    continue
                records.append({
                    **base_row,
                    "bookmaker": _safe_str(ln.get("provider")) or "cfbdata",
                    "h2h_home": _safe_float(ln.get("homeMoneyline")),
                    "h2h_away": _safe_float(ln.get("awayMoneyline")),
                    "spread_home": _safe_float(ln.get("spread")),
                    "spread_home_line": _safe_float(ln.get("homeSpreadOdds")),
                    "spread_away_line": _safe_float(ln.get("awaySpreadOdds")),
                    "total_line": _safe_float(ln.get("overUnder")),
                    "total_over": _safe_float(ln.get("overOdds")),
                    "total_under": _safe_float(ln.get("underOdds")),
                    "spread_open": _safe_float(ln.get("spreadOpen")),
                    "total_open": _safe_float(ln.get("overUnderOpen")),
                })
            continue

        records.append({
            **base_row,
            "bookmaker": _safe_str(r.get("provider")) or "cfbdata",
            "h2h_home": _safe_float(r.get("homeMoneyline")),
            "h2h_away": _safe_float(r.get("awayMoneyline")),
            "spread_home": _safe_float(r.get("spread")),
            "spread_home_line": _safe_float(r.get("homeSpreadOdds")),
            "spread_away_line": _safe_float(r.get("awaySpreadOdds")),
            "total_line": _safe_float(r.get("overUnder")),
            "total_over": _safe_float(r.get("overOdds")),
            "total_under": _safe_float(r.get("underOdds")),
            "spread_open": _safe_float(r.get("spreadOpen")),
            "total_open": _safe_float(r.get("overUnderOpen")),
        })
    return records


def _cfbdata_drives(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    drives_root = base / "drives"
    if not drives_root.is_dir():
        return records

    for f in sorted(drives_root.glob("*/*/*/*.json")):
        payload = _load_json(f)
        if not isinstance(payload, list):
            continue
        for d in payload:
            if not isinstance(d, dict):
                continue
            game_id = _safe_str(d.get("gameId"))
            drive_id = _safe_str(d.get("id"))
            if not game_id or not drive_id:
                continue
            records.append({
                "sport": sport,
                "season": season,
                "source": "cfbdata",
                "game_id": game_id,
                "drive_id": drive_id,
                "drive_number": _safe_int(d.get("driveNumber")),
                "offense_team_name": _safe_str(d.get("offense")) or "",
                "offense_conference": _safe_str(d.get("offenseConference")) or "",
                "defense_team_name": _safe_str(d.get("defense")) or "",
                "defense_conference": _safe_str(d.get("defenseConference")) or "",
                "scoring": bool(d.get("scoring") or False),
                "start_period": _safe_int(d.get("startPeriod")),
                "start_yardline": _safe_int(d.get("startYardline")),
                "start_yards_to_goal": _safe_int(d.get("startYardsToGoal")),
                "end_period": _safe_int(d.get("endPeriod")),
                "end_yardline": _safe_int(d.get("endYardline")),
                "end_yards_to_goal": _safe_int(d.get("endYardsToGoal")),
                "plays": _safe_int(d.get("plays")),
                "yards": _safe_int(d.get("yards")),
                "drive_result": _safe_str(d.get("driveResult")) or "",
                "is_home_offense": bool(d.get("isHomeOffense") or False),
                "start_offense_score": _safe_int(d.get("startOffenseScore")),
                "start_defense_score": _safe_int(d.get("startDefenseScore")),
                "end_offense_score": _safe_int(d.get("endOffenseScore")),
                "end_defense_score": _safe_int(d.get("endDefenseScore")),
            })
    return records


def _cfbdata_draft_positions(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "draft_positions.json")
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        abbr = _safe_str(rec.get("abbreviation")) or ""
        name = _safe_str(rec.get("name")) or ""
        if not abbr and not name:
            continue
        out.append({
            "draft_position_id": f"{season}|{abbr or name.lower().replace(' ', '_')}",
            "draft_year": season,
            "position_abbreviation": abbr,
            "position_name": name,
            "sport": sport,
            "season": season,
            "source": "cfbdata",
        })
    return out


def _cfbdata_draft_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "draft_teams.json")
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        team_name = _safe_str(rec.get("displayName") or rec.get("nickname") or rec.get("location")) or ""
        if not team_name:
            continue
        team_id = _safe_str(rec.get("id") or rec.get("teamId") or team_name.lower().replace(" ", "_")) or ""
        out.append({
            "draft_team_id": f"{season}|{team_id}",
            "draft_year": season,
            "team_id": team_id,
            "team_name": team_name,
            "team_logo_url": _safe_str(rec.get("logo")),
            "sport": sport,
            "season": season,
            "source": "cfbdata",
        })
    return out


def _cfbdata_play_by_play(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Normalize CFBData play-level records into a common play_by_play shape."""
    data = _load_cfbdata_json_compat(base, season, "plays", "plays.json")
    out: list[dict[str, Any]] = []
    for r in data:
        if not isinstance(r, dict):
            continue
        game_id = _safe_str(r.get("gameId"))
        play_id = _safe_str(r.get("id"))
        if not game_id or not play_id:
            continue
        clock = r.get("clock") if isinstance(r.get("clock"), dict) else {}
        out.append({
            "sport": sport,
            "source": "cfbdata",
            "season": season,
            "game_id": game_id,
            "drive_id": _safe_str(r.get("driveId")) or "",
            "play_id": play_id,
            "sequence_number": _safe_int(r.get("playNumber")),
            "event_type": _safe_str(r.get("playType")) or "play",
            "description": _safe_str(r.get("playText")) or "",
            "period": _safe_int(r.get("period")),
            "clock": _safe_str(clock.get("displayValue") or clock.get("minutes")) or "",
            "down": _safe_int(r.get("down")),
            "distance": _safe_int(r.get("distance")),
            "yards_gained": _safe_int(r.get("yardsGained")),
            "scoring_play": bool(r.get("scoring") or False),
            "offense_team_id": _safe_str(r.get("offense")) or "",
            "offense_team_name": _safe_str(r.get("offense")) or "",
            "home_score": _safe_int(r.get("home")),
            "away_score": _safe_int(r.get("away")),
        })
    return out


def _cfbdata_coaches(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "coaches.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        first = _safe_str(rec.get("firstName")) or ""
        last = _safe_str(rec.get("lastName")) or ""
        name = (f"{first} {last}").strip()
        seasons = rec.get("seasons")
        if isinstance(seasons, list) and seasons:
            for s in seasons:
                if not isinstance(s, dict):
                    continue
                team_name = _safe_str(s.get("school")) or ""
                year = _safe_str(s.get("year")) or season
                coach_id = f"{name.lower().replace(' ', '_')}|{team_name.lower().replace(' ', '_')}|{year}"
                out.append({
                    "coach_id": coach_id,
                    "coach_name": name,
                    "first_name": first,
                    "last_name": last,
                    "team_id": team_name,
                    "team_name": team_name,
                    "season": year,
                    "sport": sport,
                    "games": _safe_int(s.get("games")),
                    "wins": _safe_int(s.get("wins")),
                    "losses": _safe_int(s.get("losses")),
                    "ties": _safe_int(s.get("ties")),
                    "hire_date": _safe_str(rec.get("hireDate")),
                    "source": "cfbdata",
                })
        elif name:
            coach_id = f"{name.lower().replace(' ', '_')}|{season}"
            out.append({
                "coach_id": coach_id,
                "coach_name": name,
                "first_name": first,
                "last_name": last,
                "team_id": "",
                "team_name": "",
                "season": season,
                "sport": sport,
                "hire_date": _safe_str(rec.get("hireDate")),
                "source": "cfbdata",
            })
    return out


def _cfbdata_draft(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "draft_picks.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        draft_year = _safe_str(rec.get("year")) or season
        overall = _safe_int(rec.get("overall"))
        player_name = _safe_str(rec.get("name")) or ""
        college_player_id = _safe_str(rec.get("collegeAthleteId")) or ""
        nfl_player_id = _safe_str(rec.get("nflAthleteId")) or ""
        nfl_team_id = _safe_str(rec.get("nflTeamId")) or ""
        if overall is None and not player_name:
            continue
        draft_id = f"{draft_year}|{overall if overall is not None else ''}|{player_name.lower().replace(' ', '_')}"
        out.append({
            "draft_id": draft_id,
            "draft_year": draft_year,
            "round": _safe_int(rec.get("round")),
            "pick": _safe_int(rec.get("pick")),
            "overall_pick": overall,
            "player_id": nfl_player_id or college_player_id,
            "college_player_id": college_player_id,
            "nfl_player_id": nfl_player_id,
            "player_name": player_name,
            "position": _safe_str(rec.get("position")) or "",
            "team_id": nfl_team_id,
            "nfl_team_id": nfl_team_id,
            "team_name": _safe_str(rec.get("nflTeam")) or "",
            "college_team": _safe_str(rec.get("collegeTeam")) or "",
            "college_conference": _safe_str(rec.get("collegeConference")) or "",
            "sport": sport,
            "season": draft_year,
            "source": "cfbdata",
        })
    return out


def _cfbdata_player_portal(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "player_portal.json")
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        year = _safe_str(rec.get("season")) or season
        player_name = ((
            f"{_safe_str(rec.get('firstName')) or ''} {_safe_str(rec.get('lastName')) or ''}"
        ).strip() or _safe_str(rec.get("name")) or "")
        if not player_name:
            continue
        pid = f"{player_name.lower().replace(' ', '_')}|{year}|{_safe_str(rec.get('origin')) or ''}|{_safe_str(rec.get('destination')) or ''}"
        out.append({
            "record_id": pid,
            "player_id": _safe_str(rec.get("id")) or "",
            "player_name": player_name,
            "position": _safe_str(rec.get("position")) or "",
            "origin_team": _safe_str(rec.get("origin")) or "",
            "destination_team": _safe_str(rec.get("destination")) or "",
            "transfer_date": _safe_str(rec.get("transferDate")) or "",
            "rating": _safe_float(rec.get("rating")),
            "stars": _safe_float(rec.get("stars")),
            "eligibility": _safe_str(rec.get("eligibility")) or "",
            "season": year,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_player_returning(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "player_returning.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        team_name = _safe_str(rec.get("team")) or ""
        year = _safe_str(rec.get("season")) or season
        if not team_name:
            continue
        rid = f"{team_name.lower().replace(' ', '_')}|{year}"
        out.append({
            "record_id": rid,
            "team_id": team_name,
            "team_name": team_name,
            "conference": _safe_str(rec.get("conference")) or "",
            "total_ppa": _safe_float(rec.get("totalPPA")),
            "passing_ppa": _safe_float(rec.get("totalPassingPPA")),
            "receiving_ppa": _safe_float(rec.get("totalReceivingPPA")),
            "rushing_ppa": _safe_float(rec.get("totalRushingPPA")),
            "percent_ppa": _safe_float(rec.get("percentPPA")),
            "percent_passing_ppa": _safe_float(rec.get("percentPassingPPA")),
            "percent_receiving_ppa": _safe_float(rec.get("percentReceivingPPA")),
            "percent_rushing_ppa": _safe_float(rec.get("percentRushingPPA")),
            "usage": _safe_float(rec.get("usage")),
            "passing_usage": _safe_float(rec.get("passingUsage")),
            "receiving_usage": _safe_float(rec.get("receivingUsage")),
            "rushing_usage": _safe_float(rec.get("rushingUsage")),
            "season": year,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_player_usage(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "player_usage.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        year = _safe_str(rec.get("season")) or season
        pid = _safe_str(rec.get("id")) or ""
        name = _safe_str(rec.get("name")) or ""
        if not pid and not name:
            continue
        rid = pid or f"{name.lower().replace(' ', '_')}|{year}|{_safe_str(rec.get('team')) or ''}"
        row: dict[str, Any] = {
            "record_id": rid,
            "player_id": pid,
            "player_name": name,
            "position": _safe_str(rec.get("position")) or "",
            "team_id": _safe_str(rec.get("team")) or "",
            "team_name": _safe_str(rec.get("team")) or "",
            "conference": _safe_str(rec.get("conference")) or "",
            "season": year,
            "sport": sport,
            "source": "cfbdata",
        }
        usage = rec.get("usage")
        if isinstance(usage, dict):
            for k, v in usage.items():
                row[f"usage_{k}"] = _safe_float(v)
        out.append(row)
    return out


def _cfbdata_rankings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Flatten week-partitioned rankings files into per-rank rows."""
    data = _load_cfbdata_json_compat(base, season, "rankings", "rankings.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        s = _safe_str(entry.get("season")) or season
        season_type = _safe_str(entry.get("seasonType")) or ""
        week = _safe_int(entry.get("week"))
        for poll in (entry.get("polls") or []):
            if not isinstance(poll, dict):
                continue
            poll_name = _safe_str(poll.get("poll")) or ""
            for rank_rec in (poll.get("ranks") or []):
                if not isinstance(rank_rec, dict):
                    continue
                rank = _safe_int(rank_rec.get("rank"))
                team_id = _safe_str(rank_rec.get("teamId")) or ""
                school = _safe_str(rank_rec.get("school")) or ""
                rid = f"cfbdata|{s}|{season_type}|w{week}|{poll_name}|{rank}"
                out.append({
                    "record_id": rid,
                    "season": s,
                    "season_type": season_type,
                    "week": week,
                    "poll": poll_name,
                    "rank": rank,
                    "team_id": team_id,
                    "team_name": school,
                    "conference": _safe_str(rank_rec.get("conference")) or "",
                    "first_place_votes": _safe_int(rank_rec.get("firstPlaceVotes")),
                    "points": _safe_int(rank_rec.get("points")),
                    "sport": sport,
                    "source": "cfbdata",
                })
    return out


def _cfbdata_records(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Team season records (W-L-T splits)."""
    data = _load_cfbdata_endpoint_json(base, "records.json")
    if not isinstance(data, list):
        return []

    def _wl(obj: Any) -> dict[str, Any]:
        if not isinstance(obj, dict):
            return {}
        return {
            "games": _safe_int(obj.get("games")),
            "wins": _safe_int(obj.get("wins")),
            "losses": _safe_int(obj.get("losses")),
            "ties": _safe_int(obj.get("ties")),
        }

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        team = _safe_str(rec.get("team")) or ""
        team_id = _safe_str(rec.get("teamId")) or ""
        rid = f"cfbdata|{yr}|{team_id or team.lower().replace(' ', '_')}"
        total = _wl(rec.get("total"))
        row: dict[str, Any] = {
            "record_id": rid,
            "team_id": team_id,
            "team_name": team,
            "season": yr,
            "classification": _safe_str(rec.get("classification")) or "",
            "conference": _safe_str(rec.get("conference")) or "",
            "division": _safe_str(rec.get("division")) or "",
            "expected_wins": _safe_float(rec.get("expectedWins")),
            "total_games": total.get("games"),
            "total_wins": total.get("wins"),
            "total_losses": total.get("losses"),
            "total_ties": total.get("ties"),
            "sport": sport,
            "source": "cfbdata",
        }
        for split_key, field_prefix in [
            ("conferenceGames", "conf"),
            ("homeGames", "home"),
            ("awayGames", "away"),
            ("neutralSiteGames", "neutral"),
            ("regularSeason", "regular"),
            ("postseason", "postseason"),
        ]:
            sub = _wl(rec.get(split_key))
            if sub:
                row[f"{field_prefix}_games"] = sub.get("games")
                row[f"{field_prefix}_wins"] = sub.get("wins")
                row[f"{field_prefix}_losses"] = sub.get("losses")
                row[f"{field_prefix}_ties"] = sub.get("ties")
        out.append(row)
    return out


def _cfbdata_recruiting(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "recruiting.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        rid = _safe_str(rec.get("id")) or _safe_str(rec.get("athleteId")) or ""
        recruit_id = f"cfbdata|{yr}|{rid}"
        hometown = rec.get("hometownInfo") or {}
        out.append({
            "record_id": recruit_id,
            "recruit_id": rid,
            "athlete_id": _safe_str(rec.get("athleteId")) or "",
            "recruit_type": _safe_str(rec.get("recruitType")) or "",
            "season": yr,
            "ranking": _safe_int(rec.get("ranking")),
            "name": _safe_str(rec.get("name")) or "",
            "school": _safe_str(rec.get("school")) or "",
            "committed_to": _safe_str(rec.get("committedTo")) or "",
            "position": _safe_str(rec.get("position")) or "",
            "height": _safe_int(rec.get("height")),
            "weight": _safe_int(rec.get("weight")),
            "stars": _safe_int(rec.get("stars")),
            "rating": _safe_float(rec.get("rating")),
            "city": _safe_str(rec.get("city")) or "",
            "state": _safe_str(rec.get("stateProvince")) or "",
            "country": _safe_str(rec.get("country")) or "",
            "latitude": _safe_float(hometown.get("latitude")),
            "longitude": _safe_float(hometown.get("longitude")),
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_recruiting_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "recruiting_teams.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|{yr}|{team.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "team_name": team,
            "season": yr,
            "rank": _safe_int(rec.get("rank")),
            "points": _safe_float(rec.get("points")),
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_recruiting_groups(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "recruiting_groups.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        team = _safe_str(rec.get("team")) or ""
        pos_group = _safe_str(rec.get("positionGroup")) or ""
        rid = f"cfbdata|{season}|{team.lower().replace(' ', '_')}|{pos_group.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "position_group": pos_group,
            "average_rating": _safe_float(rec.get("averageRating")),
            "total_rating": _safe_float(rec.get("totalRating")),
            "commits": _safe_int(rec.get("commits")),
            "average_stars": _safe_float(rec.get("averageStars")),
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_talent(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "talent.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|{yr}|{team.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "team_name": team,
            "season": yr,
            "talent": _safe_float(rec.get("talent")),
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_ratings_sp(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "ratings_sp.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|sp|{yr}|{team.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "season": yr,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "rating": _safe_float(rec.get("rating")),
            "ranking": _safe_int(rec.get("ranking")),
            "second_order_wins": _safe_float(rec.get("secondOrderWins")),
            "sos": _safe_float(rec.get("sos")),
            "sport": sport,
            "source": "cfbdata",
            "rating_system": "sp",
        }
        for phase in ("offense", "defense", "specialTeams"):
            sub = rec.get(phase)
            if isinstance(sub, dict):
                prefix = "special_teams" if phase == "specialTeams" else phase
                for k, v in sub.items():
                    row[f"{prefix}_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        out.append(row)
    return out


def _cfbdata_ratings_sp_conferences(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "ratings_sp_conferences.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        conf = _safe_str(rec.get("conference")) or ""
        rid = f"cfbdata|sp_conf|{yr}|{conf.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "season": yr,
            "conference": conf,
            "rating": _safe_float(rec.get("rating")),
            "second_order_wins": _safe_float(rec.get("secondOrderWins")),
            "sos": _safe_float(rec.get("sos")),
            "sport": sport,
            "source": "cfbdata",
            "rating_system": "sp_conferences",
        }
        for phase in ("offense", "defense", "specialTeams"):
            sub = rec.get(phase)
            if isinstance(sub, dict):
                prefix = "special_teams" if phase == "specialTeams" else phase
                for k, v in sub.items():
                    row[f"{prefix}_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        out.append(row)
    return out


def _cfbdata_ratings_srs(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "ratings_srs.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|srs|{yr}|{team.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "season": yr,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "division": _safe_str(rec.get("division")) or "",
            "ranking": _safe_int(rec.get("ranking")),
            "rating": _safe_float(rec.get("rating")),
            "sport": sport,
            "source": "cfbdata",
            "rating_system": "srs",
        })
    return out


def _cfbdata_ratings_elo(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "ratings_elo.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|elo|{yr}|{team.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "season": yr,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "elo": _safe_float(rec.get("elo")),
            "sport": sport,
            "source": "cfbdata",
            "rating_system": "elo",
        })
    return out


def _cfbdata_ratings_fpi(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "ratings_fpi.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|fpi|{yr}|{team.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "season": yr,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "fpi": _safe_float(rec.get("fpi")),
            "sport": sport,
            "source": "cfbdata",
            "rating_system": "fpi",
        }
        resume = rec.get("resumeRanks")
        if isinstance(resume, dict):
            for k, v in resume.items():
                row[f"resume_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        eff = rec.get("efficiencies")
        if isinstance(eff, dict):
            for k, v in eff.items():
                row[f"eff_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        out.append(row)
    return out


def _cfbdata_ppa_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "ppa_teams.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("season")) or season
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|ppa_teams|{yr}|{team.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "season": yr,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "sport": sport,
            "source": "cfbdata",
        }
        for phase in ("offense", "defense"):
            sub = rec.get(phase)
            if isinstance(sub, dict):
                for k, v in sub.items():
                    row[f"{phase}_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        out.append(row)
    return out


def _cfbdata_ppa_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load per-game PPA data (week-partitioned files)."""
    data = _load_cfbdata_json_compat(base, season, "ppa_games", "ppa_games.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        game_id = _safe_str(rec.get("gameId")) or ""
        team = _safe_str(rec.get("team")) or ""
        yr = _safe_str(rec.get("season")) or season
        rid = f"cfbdata|ppa_games|{yr}|{game_id}|{team.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "game_id": game_id,
            "season": yr,
            "week": _safe_int(rec.get("week")),
            "season_type": _safe_str(rec.get("seasonType")) or "",
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "opponent": _safe_str(rec.get("opponent")) or "",
            "sport": sport,
            "source": "cfbdata",
        }
        for phase in ("offense", "defense"):
            sub = rec.get(phase)
            if isinstance(sub, dict):
                for k, v in sub.items():
                    row[f"{phase}_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        out.append(row)
    return out


def _cfbdata_ppa_players_season(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_cfbdata_endpoint_json(base, "ppa_players_season.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("season")) or season
        pid = _safe_str(rec.get("id")) or ""
        name = _safe_str(rec.get("name")) or ""
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|ppa_players_season|{yr}|{pid or name.lower().replace(' ', '_')}|{team.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "player_id": pid,
            "player_name": name,
            "position": _safe_str(rec.get("position")) or "",
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "season": yr,
            "sport": sport,
            "source": "cfbdata",
        }
        avg = rec.get("averagePPA")
        if isinstance(avg, dict):
            for k, v in avg.items():
                row[f"avg_ppa_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        elif avg is not None:
            row["avg_ppa_all"] = _safe_float(avg)
        total = rec.get("totalPPA")
        if isinstance(total, dict):
            for k, v in total.items():
                row[f"total_ppa_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        elif total is not None:
            row["total_ppa_all"] = _safe_float(total)
        out.append(row)
    return out


def _cfbdata_plays_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Per-play stat contributions (season-level flat list)."""
    data = _load_cfbdata_json_compat(base, season, "plays_stats", "plays_stats.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        game_id = _safe_str(rec.get("gameId")) or ""
        play_id = _safe_str(rec.get("playId")) or ""
        team = _safe_str(rec.get("team")) or ""
        yr = _safe_str(rec.get("season")) or season
        rid = f"cfbdata|plays_stats|{yr}|{game_id}|{play_id}|{team.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "game_id": game_id,
            "play_id": play_id,
            "drive_id": _safe_str(rec.get("driveId")) or "",
            "season": yr,
            "week": _safe_int(rec.get("week")),
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "opponent": _safe_str(rec.get("opponent")) or "",
            "team_score": _safe_int(rec.get("teamScore")),
            "opponent_score": _safe_int(rec.get("opponentScore")),
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_plays_types(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Static lookup: play type definitions."""
    data = _load_cfbdata_endpoint_json(base, "plays_types.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        pid = _safe_str(rec.get("id")) or ""
        text = _safe_str(rec.get("text")) or ""
        rid = f"cfbdata|play_type|{pid}"
        out.append({
            "record_id": rid,
            "play_type_id": pid,
            "text": text,
            "abbreviation": _safe_str(rec.get("abbreviation")) or "",
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_plays_stats_types(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Static lookup: play stat type definitions."""
    data = _load_cfbdata_endpoint_json(base, "plays_stats_types.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        sid = _safe_str(rec.get("id")) or ""
        rid = f"cfbdata|plays_stats_type|{sid}"
        out.append({
            "record_id": rid,
            "stat_type_id": sid,
            "name": _safe_str(rec.get("name")) or "",
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_stats_game_advanced(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Per-game advanced team stats (offense/defense breakdowns)."""
    data = _load_cfbdata_json_compat(base, season, "stats_game_advanced", "stats_game_advanced.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("season")) or season
        game_id = _safe_str(rec.get("gameId")) or ""
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|stats_game_adv|{yr}|{game_id}|{team.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "game_id": game_id,
            "season": yr,
            "season_type": _safe_str(rec.get("seasonType")) or "",
            "week": _safe_int(rec.get("week")),
            "team_name": team,
            "opponent": _safe_str(rec.get("opponent")) or "",
            "sport": sport,
            "source": "cfbdata",
        }
        for phase in ("offense", "defense"):
            sub = rec.get(phase)
            if isinstance(sub, dict):
                for k, v in sub.items():
                    row[f"{phase}_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        out.append(row)
    return out


def _cfbdata_stats_game_havoc(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Per-game havoc stats."""
    data = _load_cfbdata_json_compat(base, season, "stats_game_havoc", "stats_game_havoc.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("season")) or season
        game_id = _safe_str(rec.get("gameId")) or ""
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|stats_game_havoc|{yr}|{game_id}|{team.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "game_id": game_id,
            "season": yr,
            "season_type": _safe_str(rec.get("seasonType")) or "",
            "week": _safe_int(rec.get("week")),
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "opponent": _safe_str(rec.get("opponent")) or "",
            "opponent_conference": _safe_str(rec.get("opponentConference")) or "",
            "sport": sport,
            "source": "cfbdata",
        }
        for phase in ("offense", "defense"):
            sub = rec.get(phase)
            if isinstance(sub, dict):
                for k, v in sub.items():
                    row[f"{phase}_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else _safe_str(v)
        out.append(row)
    return out


def _cfbdata_games_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Per-game team box stats (flat rows, one per team per game)."""
    data = _load_cfbdata_json_compat(base, season, "games_teams", "games_teams.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        game_id = _safe_str(entry.get("id")) or ""
        for team_rec in (entry.get("teams") or []):
            if not isinstance(team_rec, dict):
                continue
            team_id = _safe_str(team_rec.get("teamId")) or ""
            team_name = _safe_str(team_rec.get("team")) or ""
            home_away = _safe_str(team_rec.get("homeAway")) or ""
            rid = f"cfbdata|games_teams|{season}|{game_id}|{home_away}"
            row: dict[str, Any] = {
                "record_id": rid,
                "game_id": game_id,
                "team_id": team_id,
                "team_name": team_name,
                "conference": _safe_str(team_rec.get("conference")) or "",
                "home_away": home_away,
                "points": _safe_int(team_rec.get("points")),
                "season": season,
                "sport": sport,
                "source": "cfbdata",
            }
            for stat in (team_rec.get("stats") or []):
                if not isinstance(stat, dict):
                    continue
                sname = _safe_str(stat.get("category") or stat.get("stat_type") or stat.get("name")) or ""
                sval = stat.get("stat") or stat.get("value")
                if sname:
                    row[f"stat_{sname.lower().replace(' ', '_')}"] = (
                        _safe_float(sval) if isinstance(sval, (int, float)) else _safe_str(sval)
                    )
            out.append(row)
    return out


def _cfbdata_games_media(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Broadcast/media assignments per game."""
    data = _load_cfbdata_json_compat(base, season, "games_media", "games_media.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        game_id = _safe_str(rec.get("id")) or ""
        media_type = _safe_str(rec.get("mediaType")) or ""
        outlet = _safe_str(rec.get("outlet")) or ""
        rid = f"cfbdata|games_media|{season}|{game_id}|{media_type}|{outlet.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "game_id": game_id,
            "season": _safe_str(rec.get("season")) or season,
            "week": _safe_int(rec.get("week")),
            "season_type": _safe_str(rec.get("seasonType")) or "",
            "start_time": _safe_str(rec.get("startTime")) or "",
            "home_team": _safe_str(rec.get("homeTeam")) or "",
            "home_conference": _safe_str(rec.get("homeConference")) or "",
            "away_team": _safe_str(rec.get("awayTeam")) or "",
            "away_conference": _safe_str(rec.get("awayConference")) or "",
            "media_type": media_type,
            "outlet": outlet,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_conferences(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Conference reference data (static across seasons)."""
    data = _load_cfbdata_endpoint_json(base, "conferences.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        cid = _safe_str(rec.get("id")) or ""
        name = _safe_str(rec.get("name")) or ""
        rid = f"cfbdata|conf|{cid}"
        out.append({
            "record_id": rid,
            "conference_id": cid,
            "name": name,
            "short_name": _safe_str(rec.get("shortName")) or "",
            "abbreviation": _safe_str(rec.get("abbreviation")) or "",
            "classification": _safe_str(rec.get("classification")) or "",
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_metrics_fg_ep(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Field goal expected points lookup table (yards-to-goal × distance grid)."""
    data = _load_cfbdata_endpoint_json(base, "metrics_fg_ep.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yards = _safe_int(rec.get("yardsToGoal"))
        dist = _safe_int(rec.get("distance"))
        rid = f"cfbdata|metrics_fg_ep|{yards}|{dist}"
        out.append({
            "record_id": rid,
            "yards_to_goal": yards,
            "distance": dist,
            "expected_points": _safe_float(rec.get("expectedPoints")),
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_venues(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Stadium/venue reference data."""
    data = _load_cfbdata_endpoint_json(base, "venues.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        vid = _safe_str(rec.get("id")) or ""
        name = _safe_str(rec.get("name")) or ""
        rid = f"cfbdata|venue|{vid or name.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "venue_id": vid,
            "name": name,
            "capacity": _safe_int(rec.get("capacity")),
            "grass": bool(rec.get("grass")),
            "dome": bool(rec.get("dome")),
            "city": _safe_str(rec.get("city")) or "",
            "state": _safe_str(rec.get("state")) or "",
            "zip": _safe_str(rec.get("zip")) or "",
            "country_code": _safe_str(rec.get("countryCode")) or "",
            "timezone": _safe_str(rec.get("timezone")) or "",
            "latitude": _safe_float(rec.get("latitude")),
            "longitude": _safe_float(rec.get("longitude")),
            "elevation": _safe_float(rec.get("elevation")),
            "construction_year": _safe_int(rec.get("constructionYear")),
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_stats_categories(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Static list of all stat category name strings."""
    data = _load_cfbdata_endpoint_json(base, "stats_categories.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for i, item in enumerate(data):
        label = _safe_str(item) if isinstance(item, str) else _safe_str(item.get("name") or item.get("id") or "") if isinstance(item, dict) else ""
        if not label:
            continue
        rid = f"cfbdata|stats_cat|{label.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "sort_order": i,
            "name": label,
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_wp_pregame(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Pre-game win probability by game."""
    data = _load_cfbdata_json_compat(base, season, "wp_pregame", "wp_pregame.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("season")) or season
        game_id = _safe_str(rec.get("gameId")) or ""
        rid = f"cfbdata|wp_pregame|{yr}|{game_id}"
        out.append({
            "record_id": rid,
            "game_id": game_id,
            "season": yr,
            "week": _safe_int(rec.get("week")),
            "season_type": _safe_str(rec.get("seasonType")) or "",
            "home_team": _safe_str(rec.get("homeTeam")) or "",
            "away_team": _safe_str(rec.get("awayTeam")) or "",
            "spread": _safe_float(rec.get("spread")),
            "home_win_probability": _safe_float(rec.get("homeWinProbability")),
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_teams_ats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Teams against the spread records."""
    data = _load_cfbdata_endpoint_json(base, "teams_ats.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yr = _safe_str(rec.get("year")) or season
        team_id = _safe_str(rec.get("teamId")) or ""
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|teams_ats|{yr}|{team_id or team.lower().replace(' ', '_')}"
        out.append({
            "record_id": rid,
            "team_id": team_id,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "season": yr,
            "games": _safe_int(rec.get("games")),
            "ats_wins": _safe_int(rec.get("atsWins")),
            "ats_losses": _safe_int(rec.get("atsLosses")),
            "ats_pushes": _safe_int(rec.get("atsPushes")),
            "avg_cover_margin": _safe_float(rec.get("avgCoverMargin")),
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_teams_fbs(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """FBS teams reference (extended team metadata including logos, colors, location)."""
    data = _load_cfbdata_endpoint_json(base, "teams_fbs.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        team_id = _safe_str(rec.get("id")) or ""
        school = _safe_str(rec.get("school")) or ""
        rid = f"cfbdata|team|{team_id or school.lower().replace(' ', '_')}"
        location = rec.get("location") or {}
        logos = rec.get("logos")
        logo_url = ""
        if isinstance(logos, list) and logos:
            logo_url = _safe_str(logos[0]) if isinstance(logos[0], str) else _safe_str((logos[0] or {}).get("href")) or ""
        alt_names = rec.get("alternateNames")
        out.append({
            "record_id": rid,
            "team_id": team_id,
            "school": school,
            "mascot": _safe_str(rec.get("mascot")) or "",
            "abbreviation": _safe_str(rec.get("abbreviation")) or "",
            "alternate_names": "|".join(alt_names) if isinstance(alt_names, list) else _safe_str(alt_names) or "",
            "conference": _safe_str(rec.get("conference")) or "",
            "division": _safe_str(rec.get("division")) or "",
            "classification": _safe_str(rec.get("classification")) or "",
            "color": _safe_str(rec.get("color")) or "",
            "alternate_color": _safe_str(rec.get("alternateColor")) or "",
            "logo_url": logo_url,
            "twitter": _safe_str(rec.get("twitter")) or "",
            "venue_id": _safe_str((location if isinstance(location, dict) else {}).get("venueId")) or "",
            "venue_name": _safe_str((location if isinstance(location, dict) else {}).get("name")) or "",
            "city": _safe_str((location if isinstance(location, dict) else {}).get("city")) or "",
            "state": _safe_str((location if isinstance(location, dict) else {}).get("state")) or "",
            "latitude": _safe_float((location if isinstance(location, dict) else {}).get("latitude")),
            "longitude": _safe_float((location if isinstance(location, dict) else {}).get("longitude")),
            "capacity": _safe_int((location if isinstance(location, dict) else {}).get("capacity")),
            "grass": bool((location if isinstance(location, dict) else {}).get("grass")),
            "dome": bool((location if isinstance(location, dict) else {}).get("dome")),
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_games_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Per-game player box breakdown by team/category/stat type."""
    data = _load_cfbdata_json_compat(base, season, "games_players", "games_players.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        game_id = _safe_str(rec.get("id")) or ""
        teams = rec.get("teams")
        if not isinstance(teams, list):
            continue
        for team_rec in teams:
            if not isinstance(team_rec, dict):
                continue
            team_name = _safe_str(team_rec.get("team")) or ""
            team_conference = _safe_str(team_rec.get("conference")) or ""
            home_away = _safe_str(team_rec.get("homeAway")) or ""
            points = _safe_int(team_rec.get("points"))
            categories = team_rec.get("categories")
            if not isinstance(categories, list):
                continue
            for cat in categories:
                if not isinstance(cat, dict):
                    continue
                cat_name = _safe_str(cat.get("name")) or ""
                types = cat.get("types")
                if not isinstance(types, list):
                    continue
                for stat_type in types:
                    if not isinstance(stat_type, dict):
                        continue
                    stat_type_name = _safe_str(stat_type.get("name")) or ""
                    athletes = stat_type.get("athletes")
                    if not isinstance(athletes, list):
                        continue
                    for athlete in athletes:
                        if not isinstance(athlete, dict):
                            continue
                        player_id = _safe_str(athlete.get("id")) or ""
                        player_name = _safe_str(athlete.get("name")) or ""
                        stat_raw = athlete.get("stat")
                        rid = (
                            f"cfbdata|games_players|{season}|{game_id}|{team_name.lower().replace(' ', '_')}"
                            f"|{player_id or player_name.lower().replace(' ', '_')}|{cat_name.lower().replace(' ', '_')}"
                            f"|{stat_type_name.lower().replace(' ', '_')}"
                        )
                        out.append({
                            "record_id": rid,
                            "game_id": game_id,
                            "team_name": team_name,
                            "team_conference": team_conference,
                            "home_away": home_away,
                            "team_points": points,
                            "category": cat_name,
                            "stat_type": stat_type_name,
                            "player_id": player_id,
                            "player_name": player_name,
                            "stat": _safe_str(stat_raw) if not isinstance(stat_raw, (int, float)) else _safe_float(stat_raw),
                            "season": season,
                            "sport": sport,
                            "source": "cfbdata",
                        })
    return out


def _cfbdata_lines(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Per-game betting lines with one row per provider line snapshot."""
    data = _load_cfbdata_json_compat(base, season, "lines", "lines.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        game_id = _safe_str(rec.get("id")) or ""
        season_val = _safe_str(rec.get("season")) or season
        week = _safe_int(rec.get("week"))
        season_type = _safe_str(rec.get("seasonType")) or ""
        home_team = _safe_str(rec.get("homeTeam")) or ""
        away_team = _safe_str(rec.get("awayTeam")) or ""
        lines = rec.get("lines")
        if not isinstance(lines, list):
            continue
        for line in lines:
            if not isinstance(line, dict):
                continue
            provider = _safe_str(line.get("provider")) or ""
            rid = f"cfbdata|lines|{season_val}|{game_id}|{provider.lower().replace(' ', '_')}"
            out.append({
                "record_id": rid,
                "game_id": game_id,
                "season": season_val,
                "week": week,
                "season_type": season_type,
                "start_date": _safe_str(rec.get("startDate")) or "",
                "home_team_id": _safe_str(rec.get("homeTeamId")) or "",
                "home_team": home_team,
                "away_team_id": _safe_str(rec.get("awayTeamId")) or "",
                "away_team": away_team,
                "provider": provider,
                "spread": _safe_float(line.get("spread")),
                "spread_open": _safe_float(line.get("spreadOpen")),
                "formatted_spread": _safe_str(line.get("formattedSpread")) or "",
                "over_under": _safe_float(line.get("overUnder")),
                "over_under_open": _safe_float(line.get("overUnderOpen")),
                "home_moneyline": _safe_float(line.get("homeMoneyline")),
                "away_moneyline": _safe_float(line.get("awayMoneyline")),
                "sport": sport,
                "source": "cfbdata",
            })
    return out


def _cfbdata_plays(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Raw play records with expanded context fields."""
    data = _load_cfbdata_json_compat(base, season, "plays", "plays.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        game_id = _safe_str(rec.get("gameId")) or ""
        play_id = _safe_str(rec.get("id")) or ""
        if not game_id or not play_id:
            continue
        clock = rec.get("clock") if isinstance(rec.get("clock"), dict) else {}
        rid = f"cfbdata|plays|{season}|{game_id}|{play_id}"
        out.append({
            "record_id": rid,
            "game_id": game_id,
            "drive_id": _safe_str(rec.get("driveId")) or "",
            "play_id": play_id,
            "drive_number": _safe_int(rec.get("driveNumber")),
            "play_number": _safe_int(rec.get("playNumber")),
            "offense": _safe_str(rec.get("offense")) or "",
            "offense_conference": _safe_str(rec.get("offenseConference")) or "",
            "offense_score": _safe_int(rec.get("offenseScore")),
            "defense": _safe_str(rec.get("defense")) or "",
            "defense_conference": _safe_str(rec.get("defenseConference")) or "",
            "defense_score": _safe_int(rec.get("defenseScore")),
            "home_team": _safe_str(rec.get("home")) or "",
            "away_team": _safe_str(rec.get("away")) or "",
            "period": _safe_int(rec.get("period")),
            "clock_minutes": _safe_int(clock.get("minutes")),
            "clock_seconds": _safe_int(clock.get("seconds")),
            "offense_timeouts": _safe_int(rec.get("offenseTimeouts")),
            "defense_timeouts": _safe_int(rec.get("defenseTimeouts")),
            "yardline": _safe_int(rec.get("yardline")),
            "yards_to_goal": _safe_int(rec.get("yardsToGoal")),
            "down": _safe_int(rec.get("down")),
            "distance": _safe_int(rec.get("distance")),
            "yards_gained": _safe_int(rec.get("yardsGained")),
            "scoring": bool(rec.get("scoring") or False),
            "play_type": _safe_str(rec.get("playType")) or "",
            "play_text": _safe_str(rec.get("playText")) or "",
            "ppa": _safe_float(rec.get("ppa")),
            "wallclock": _safe_str(rec.get("wallclock")) or "",
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_roster(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Season roster records."""
    data = _load_cfbdata_endpoint_json(base, "roster.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        player_id = _safe_str(rec.get("id")) or ""
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|roster|{season}|{team.lower().replace(' ', '_')}|{player_id}"
        recruit_ids = rec.get("recruitIds")
        out.append({
            "record_id": rid,
            "player_id": player_id,
            "first_name": _safe_str(rec.get("firstName")) or "",
            "last_name": _safe_str(rec.get("lastName")) or "",
            "team_name": team,
            "weight": _safe_int(rec.get("weight")),
            "height": _safe_int(rec.get("height")),
            "jersey": _safe_str(rec.get("jersey")) or "",
            "year": _safe_int(rec.get("year")),
            "position": _safe_str(rec.get("position")) or "",
            "home_city": _safe_str(rec.get("homeCity")) or "",
            "home_state": _safe_str(rec.get("homeState")) or "",
            "home_country": _safe_str(rec.get("homeCountry")) or "",
            "home_latitude": _safe_float(rec.get("homeLatitude")),
            "home_longitude": _safe_float(rec.get("homeLongitude")),
            "home_county_fips": _safe_str(rec.get("homeCountyFIPS")) or "",
            "recruit_ids": "|".join(str(x) for x in recruit_ids) if isinstance(recruit_ids, list) else _safe_str(recruit_ids) or "",
            "season": season,
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_stats_advanced(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Season-level advanced team stats."""
    data = _load_cfbdata_endpoint_json(base, "stats_advanced.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []

    def _flatten(prefix: str, value: Any, row: dict[str, Any]) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                key = f"{prefix}_{k}" if prefix else str(k)
                _flatten(key, v, row)
            return
        if isinstance(value, (int, float)):
            row[prefix] = _safe_float(value)
            return
        text = _safe_str(value)
        row[prefix] = text if text else None

    for rec in data:
        if not isinstance(rec, dict):
            continue
        season_val = _safe_str(rec.get("season")) or season
        team = _safe_str(rec.get("team")) or ""
        rid = f"cfbdata|stats_advanced|{season_val}|{team.lower().replace(' ', '_')}"
        row: dict[str, Any] = {
            "record_id": rid,
            "season": season_val,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "sport": sport,
            "source": "cfbdata",
        }
        for side in ("offense", "defense"):
            sub = rec.get(side)
            if isinstance(sub, dict):
                _flatten(side, sub, row)
        out.append(row)
    return out


def _cfbdata_stats_player_season(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Season player stat rows by category/statType."""
    data = _load_cfbdata_json_compat(base, season, "stats_player_season", "stats_player_season.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        season_val = _safe_str(rec.get("season")) or season
        player_id = _safe_str(rec.get("playerId")) or ""
        category = _safe_str(rec.get("category")) or ""
        stat_type = _safe_str(rec.get("statType")) or ""
        rid = (
            f"cfbdata|stats_player_season|{season_val}|{player_id}|"
            f"{category.lower().replace(' ', '_')}|{stat_type.lower().replace(' ', '_')}"
        )
        stat_val = rec.get("stat")
        out.append({
            "record_id": rid,
            "season": season_val,
            "player_id": player_id,
            "player_name": _safe_str(rec.get("player")) or "",
            "position": _safe_str(rec.get("position")) or "",
            "team_name": _safe_str(rec.get("team")) or "",
            "conference": _safe_str(rec.get("conference")) or "",
            "category": category,
            "stat_type": stat_type,
            "stat": _safe_float(stat_val) if isinstance(stat_val, (int, float)) else _safe_str(stat_val) or "",
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_stats_season(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Season team stat rollups."""
    data = _load_cfbdata_endpoint_json(base, "stats_season.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        season_val = _safe_str(rec.get("season")) or season
        team = _safe_str(rec.get("team")) or ""
        stat_name = _safe_str(rec.get("statName")) or ""
        rid = f"cfbdata|stats_season|{season_val}|{team.lower().replace(' ', '_')}|{stat_name.lower().replace(' ', '_')}"
        stat_val = rec.get("statValue")
        out.append({
            "record_id": rid,
            "season": season_val,
            "team_name": team,
            "conference": _safe_str(rec.get("conference")) or "",
            "stat_name": stat_name,
            "stat_value": _safe_float(stat_val) if isinstance(stat_val, (int, float)) else _safe_str(stat_val) or "",
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_info(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Provider/account info metadata snapshot."""
    data = _load_cfbdata_endpoint_json(base, "info.json")
    if not isinstance(data, dict):
        return []

    return [{
        "record_id": f"cfbdata|info|{season}",
        "season": season,
        "patron_level": _safe_str(data.get("patronLevel")) or "",
        "remaining_calls": _safe_int(data.get("remainingCalls")),
        "sport": sport,
        "source": "cfbdata",
    }]


def _cfbdata_calendar(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Season calendar windows (week/start/end bounds)."""
    data = _load_cfbdata_endpoint_json(base, "calendar.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        season_val = _safe_str(rec.get("season")) or season
        week = _safe_int(rec.get("week"))
        season_type = _safe_str(rec.get("seasonType")) or ""
        rid = f"cfbdata|calendar|{season_val}|{season_type}|{week if week is not None else ''}"
        out.append({
            "record_id": rid,
            "season": season_val,
            "week": week,
            "season_type": season_type,
            "start_date": _safe_str(rec.get("startDate")) or "",
            "end_date": _safe_str(rec.get("endDate")) or "",
            "first_game_start": _safe_str(rec.get("firstGameStart")) or "",
            "last_game_start": _safe_str(rec.get("lastGameStart")) or "",
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_game_box_advanced(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Advanced game box metrics (team + player scopes) from per-game files."""
    root = base / "game_box_advanced"
    if not root.is_dir():
        return []

    out: list[dict[str, Any]] = []
    for f in sorted(root.glob("*.json")):
        payload = _load_json(f)
        if not isinstance(payload, dict):
            continue
        game_id = _safe_str(f.stem) or _safe_str(payload.get("id")) or ""
        if not game_id:
            continue

        game_info = payload.get("gameInfo") if isinstance(payload.get("gameInfo"), dict) else {}

        # Team scoped metrics (families under payload['teams']).
        teams_block = payload.get("teams") if isinstance(payload.get("teams"), dict) else {}
        for family_name, rows in teams_block.items():
            if not isinstance(rows, list):
                continue
            for rec in rows:
                if not isinstance(rec, dict):
                    continue
                team = _safe_str(rec.get("team")) or ""
                rid = (
                    f"cfbdata|game_box_advanced|team|{season}|{game_id}|"
                    f"{family_name.lower().replace(' ', '_')}|{team.lower().replace(' ', '_')}"
                )
                row: dict[str, Any] = {
                    "record_id": rid,
                    "game_id": game_id,
                    "entity_scope": "team",
                    "metric_family": _safe_str(family_name) or "",
                    "team_name": team,
                    "season": season,
                    "sport": sport,
                    "source": "cfbdata",
                    "home_team": _safe_str(game_info.get("homeTeam")) or "",
                    "away_team": _safe_str(game_info.get("awayTeam")) or "",
                    "home_points": _safe_int(game_info.get("homePoints")),
                    "away_points": _safe_int(game_info.get("awayPoints")),
                    "home_win_prob": _safe_float(game_info.get("homeWinProb")),
                    "away_win_prob": _safe_float(game_info.get("awayWinProb")),
                    "home_winner": bool(game_info.get("homeWinner") or False),
                    "excitement": _safe_float(game_info.get("excitement")),
                }
                for k, v in rec.items():
                    if isinstance(v, dict):
                        for sk, sv in v.items():
                            key = f"{k}_{sk}"
                            row[key] = _safe_float(sv) if isinstance(sv, (int, float)) else (_safe_str(sv) or None)
                    else:
                        row[k] = _safe_float(v) if isinstance(v, (int, float)) else (_safe_str(v) or None)
                out.append(row)

        # Player scoped metrics (families under payload['players']).
        players_block = payload.get("players") if isinstance(payload.get("players"), dict) else {}
        for family_name, rows in players_block.items():
            if not isinstance(rows, list):
                continue
            for rec in rows:
                if not isinstance(rec, dict):
                    continue
                player = _safe_str(rec.get("player")) or ""
                team = _safe_str(rec.get("team")) or ""
                rid = (
                    f"cfbdata|game_box_advanced|player|{season}|{game_id}|"
                    f"{family_name.lower().replace(' ', '_')}|{player.lower().replace(' ', '_')}"
                )
                row = {
                    "record_id": rid,
                    "game_id": game_id,
                    "entity_scope": "player",
                    "metric_family": _safe_str(family_name) or "",
                    "team_name": team,
                    "player_name": player,
                    "season": season,
                    "sport": sport,
                    "source": "cfbdata",
                }
                for k, v in rec.items():
                    row[k] = _safe_float(v) if isinstance(v, (int, float)) else (_safe_str(v) or None)
                out.append(row)

    return out


def _cfbdata_scoreboard(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Scoreboard snapshot rows; useful when present, empty-safe otherwise."""
    data = _load_cfbdata_endpoint_json(base, "scoreboard.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        game_id = _safe_str(rec.get("id") or rec.get("gameId")) or ""
        if not game_id:
            continue
        rid = f"cfbdata|scoreboard|{season}|{game_id}"
        out.append({
            "record_id": rid,
            "game_id": game_id,
            "season": _safe_str(rec.get("season")) or season,
            "week": _safe_int(rec.get("week")),
            "season_type": _safe_str(rec.get("seasonType")) or "",
            "status": _safe_str(rec.get("status")) or "",
            "start_date": _safe_str(rec.get("startDate") or rec.get("startTime")) or "",
            "home_team": _safe_str(rec.get("homeTeam")) or "",
            "away_team": _safe_str(rec.get("awayTeam")) or "",
            "home_points": _safe_int(rec.get("homePoints") or rec.get("homeScore")),
            "away_points": _safe_int(rec.get("awayPoints") or rec.get("awayScore")),
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_metrics_wp(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Win probability lookup metrics (guarded for no-data seasons)."""
    data = _load_cfbdata_endpoint_json(base, "metrics_wp.json")
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        yard_line = _safe_int(rec.get("yardLine"))
        time_remaining = _safe_int(rec.get("timeRemaining"))
        down = _safe_int(rec.get("down"))
        distance = _safe_int(rec.get("distance"))
        rid = f"cfbdata|metrics_wp|{season}|{yard_line}|{time_remaining}|{down}|{distance}"
        out.append({
            "record_id": rid,
            "season": season,
            "yard_line": yard_line,
            "time_remaining": time_remaining,
            "down": down,
            "distance": distance,
            "win_probability": _safe_float(rec.get("winProbability") or rec.get("wp")),
            "sport": sport,
            "source": "cfbdata",
        })
    return out


def _cfbdata_ppa_players_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Per-player game-level PPA summaries (week files)."""
    endpoint_dir = base / "ppa_players_games"
    if not endpoint_dir.is_dir():
        return []

    out: list[dict[str, Any]] = []
    # Layout: {endpoint}/{seasonType}/week_xx/players.json
    for season_type_dir in sorted(endpoint_dir.glob("*")):
        if not season_type_dir.is_dir():
            continue
        season_type = season_type_dir.name
        for stat_file in sorted(season_type_dir.glob("week_*/players.json")):
            data = _load_json(stat_file)
            if not isinstance(data, list):
                continue
            week_token = stat_file.parent.name.replace("week_", "")
            week = _safe_int(week_token)
            for rec in data:
                if not isinstance(rec, dict):
                    continue
                player_id = _safe_str(rec.get("id")) or _safe_str(rec.get("playerId")) or ""
                team = _safe_str(rec.get("team")) or ""
                opponent = _safe_str(rec.get("opponent")) or ""
                rid = (
                    f"cfbdata|ppa_players_games|{season}|{season_type}|{week if week is not None else ''}|"
                    f"{player_id or rec.get('name', '')}|{team.lower().replace(' ', '_')}|{opponent.lower().replace(' ', '_')}"
                )
                row: dict[str, Any] = {
                    "record_id": rid,
                    "season": _safe_str(rec.get("season")) or season,
                    "week": _safe_int(rec.get("week")) if rec.get("week") is not None else week,
                    "season_type": _safe_str(rec.get("seasonType")) or season_type,
                    "player_id": player_id,
                    "player_name": _safe_str(rec.get("name")) or "",
                    "position": _safe_str(rec.get("position")) or "",
                    "team_name": team,
                    "opponent": opponent,
                    "sport": sport,
                    "source": "cfbdata",
                }
                avg = rec.get("averagePPA")
                if isinstance(avg, dict):
                    for k, v in avg.items():
                        row[f"average_ppa_{k}"] = _safe_float(v) if isinstance(v, (int, float)) else (_safe_str(v) or None)
                else:
                    row["average_ppa"] = _safe_float(avg) if isinstance(avg, (int, float)) else (_safe_str(avg) or None)
                out.append(row)
    return out


def _cfbdata_ppa_predicted(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Predicted points lookup grids for down/distance/yardline contexts."""
    endpoint_dir = base / "ppa_predicted"
    if not endpoint_dir.is_dir():
        return []

    out: list[dict[str, Any]] = []

    # 1) Flat file layout: ppa_predicted.json
    flat = _load_json(endpoint_dir / "ppa_predicted.json")
    if isinstance(flat, list):
        for rec in flat:
            if not isinstance(rec, dict):
                continue
            yard_line = _safe_int(rec.get("yardLine"))
            down = _safe_int(rec.get("down"))
            distance = _safe_int(rec.get("distance"))
            rid = f"cfbdata|ppa_predicted|{season}|flat|{down}|{distance}|{yard_line}"
            out.append({
                "record_id": rid,
                "season": season,
                "down": down,
                "distance": distance,
                "yard_line": yard_line,
                "predicted_points": _safe_float(rec.get("predictedPoints")),
                "layout": "flat",
                "sport": sport,
                "source": "cfbdata",
            })

    # 2) Nested layout: down_X/distance_Y.json
    for down_dir in sorted(endpoint_dir.glob("down_*")):
        if not down_dir.is_dir():
            continue
        down_token = down_dir.name.replace("down_", "")
        down = _safe_int(down_token)
        for distance_file in sorted(down_dir.glob("distance_*.json")):
            distance_token = distance_file.stem.replace("distance_", "")
            distance = _safe_int(distance_token)
            data = _load_json(distance_file)
            if not isinstance(data, list):
                continue
            for rec in data:
                if not isinstance(rec, dict):
                    continue
                yard_line = _safe_int(rec.get("yardLine"))
                rid = f"cfbdata|ppa_predicted|{season}|nested|{down}|{distance}|{yard_line}"
                out.append({
                    "record_id": rid,
                    "season": season,
                    "down": down,
                    "distance": distance,
                    "yard_line": yard_line,
                    "predicted_points": _safe_float(rec.get("predictedPoints")),
                    "layout": "nested",
                    "sport": sport,
                    "source": "cfbdata",
                })

    return out


def _nhl_draft(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    draft_dir = base / "draft"
    if not draft_dir.is_dir():
        return []

    candidate_files = [
        draft_dir / "picks_all_rounds.json",
        draft_dir / "picks_now.json",
        draft_dir / "tracker_picks_now.json",
    ]
    candidate_files.extend(sorted((draft_dir / "picks_by_round").glob("*.json")))

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for f in candidate_files:
        if not f.exists():
            continue
        payload = _load_json(f)
        if not isinstance(payload, dict):
            continue
        picks = payload.get("picks")
        if not isinstance(picks, list):
            continue
        draft_year = _safe_str(payload.get("draftYear")) or season
        for p in picks:
            if not isinstance(p, dict):
                continue
            overall = _safe_int(p.get("overallPick"))
            player_name = ((
                f"{_safe_str(p.get('firstName')) or ''} {_safe_str(p.get('lastName')) or ''}"
            ).strip() or _safe_str(p.get("lastName")) or "")
            if overall is None and not player_name:
                continue
            draft_id = f"nhl|{draft_year}|{overall if overall is not None else ''}|{player_name.lower().replace(' ', '_')}"
            if draft_id in seen:
                continue
            seen.add(draft_id)
            out.append({
                "draft_id": draft_id,
                "draft_year": draft_year,
                "round": _safe_int(p.get("round")),
                "pick": _safe_int(p.get("pickInRound")),
                "overall_pick": overall,
                "player_id": _safe_str(p.get("playerId")) or "",
                "player_name": player_name,
                "position": _safe_str(p.get("positionCode")) or "",
                "team_id": _safe_str(p.get("teamId")) or "",
                "team_name": _safe_str(p.get("teamName") or p.get("teamFullName") or p.get("teamCommonName")) or "",
                "league": _safe_str(p.get("amateurLeague")) or "",
                "club": _safe_str(p.get("amateurClubName")) or "",
                "country": _safe_str(p.get("countryCode")) or "",
                "season": draft_year,
                "sport": sport,
                "source": "nhl",
            })
    return out


# ── Football-data.org ─────────────────────────────────────

def _footballdata_games(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Load football-data.org matches.

    Supports two layouts:
    - Legacy: ``games/all.json`` (single bulk file)
    - New:    ``games/matchday_{NN}/matches.json`` (per-matchday splits)
    """
    games_dir = base / "games"
    # Collect raw match dicts from all available sources (deduplicated by id)
    all_matches: dict[str, dict] = {}

    # New layout: per-matchday split files
    matchday_files = sorted(games_dir.glob("matchday_*/matches.json"))
    stage_files = sorted(games_dir.glob("stage_*/matches.json"))
    for mf in matchday_files + stage_files:
        chunk = _load_json(mf)
        if not chunk:
            continue
        for m in (chunk.get("matches", chunk) if isinstance(chunk, dict) else chunk):
            mid = str(m.get("id", ""))
            if mid:
                all_matches[mid] = m

    # Legacy bulk file (always read; deduplicates with new layout if both exist)
    bulk = _load_json(games_dir / "all.json")
    if bulk:
        for m in (bulk.get("matches", bulk) if isinstance(bulk, dict) else bulk):
            mid = str(m.get("id", ""))
            if mid and mid not in all_matches:
                all_matches[mid] = m

    if not all_matches:
        return []

    # Stage → season_type mapping for cups/tournaments
    _stage_to_type: dict[str, str] = {
        "REGULAR_SEASON": "regular",
        "GROUP_STAGE": "group_stage",
        "ROUND_OF_16": "knockout",
        "ROUND_OF_8": "knockout",
        "QUARTER_FINALS": "knockout",
        "SEMI_FINALS": "knockout",
        "FINAL": "final",
        "THIRD_PLACE": "knockout",
        "PLAY_OFF_ROUND": "playoff",
        "PRELIMINARY_ROUND": "qualifying",
        "LEAGUE_PHASE": "group_stage",
        "KNOCKOUT_PHASE_PLAY_OFFS": "knockout",
    }

    records: list[dict[str, Any]] = []
    for m in all_matches.values():
        mid = str(m.get("id", ""))
        home = m.get("homeTeam", {})
        away = m.get("awayTeam", {})
        ft = m.get("score", {}).get("fullTime", {})
        ht = m.get("score", {}).get("halfTime", {})
        status_raw = m.get("status", "")
        stage = (m.get("stage") or "REGULAR_SEASON").upper()
        matchday = _safe_int(m.get("matchday"))
        season_type = _stage_to_type.get(stage, "regular")
        records.append({
            "id": mid,
            "season": season,
            "season_type": season_type,
            "matchday": matchday,
            "stage": stage,
            "date": (m.get("utcDate") or "")[:10] or None,
            "status": "final" if status_raw == "FINISHED" else "scheduled",
            "home_team": home.get("shortName") or home.get("name", ""),
            "away_team": away.get("shortName") or away.get("name", ""),
            "home_team_id": str(home.get("id", "")),
            "away_team_id": str(away.get("id", "")),
            "home_score": _safe_int(ft.get("home")),
            "away_score": _safe_int(ft.get("away")),
            "home_ht_score": _safe_int(ht.get("home")),
            "away_ht_score": _safe_int(ht.get("away")),
            "venue": None,
        })
    return records


def _footballdata_standings(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    data = _load_json(base / "standings" / "current.json")
    if not data:
        return []
    standings_list = data.get("standings", [])
    records: list[dict[str, Any]] = []
    for group in standings_list:
        for entry in group.get("table", []):
            team = entry.get("team", {})
            tid = str(team.get("id", ""))
            if not tid:
                continue
            records.append({
                "team_id": tid,
                "team_name": team.get("shortName") or team.get("name", ""),
                "rank": _safe_int(entry.get("position")),
                "wins": _safe_int(entry.get("won")),
                "losses": _safe_int(entry.get("lost")),
                "ties": _safe_int(entry.get("draw")),
                "points": _safe_int(entry.get("points")),
                "season": season,
            })
    return records


def _footballdata_players(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract player records from football-data.org top-scorers."""
    data = _load_json(base / "stats" / "top-scorers.json")
    if not data:
        return []
    scorers = data.get("scorers", [])
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for s in scorers:
        p = s.get("player", {})
        pid = str(p.get("id", ""))
        if not pid or pid in seen:
            continue
        seen.add(pid)
        team = s.get("team", {})
        records.append({
            "id": pid,
            "name": p.get("name", ""),
            "team_id": _safe_str(team.get("id")),
            "nationality": p.get("nationality"),
            "position": p.get("section") or p.get("position"),
        })
    return records


def _footballdata_teams(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract team records from football-data.org teams/all.json."""
    data = _load_json(base / "teams" / "all.json")
    if not data:
        return []
    teams = data.get("teams", [])
    records: list[dict[str, Any]] = []
    for t in teams:
        tid = str(t.get("id", ""))
        if not tid:
            continue
        area = t.get("area", {})
        records.append({
            "id": tid,
            "name": t.get("name", ""),
            "abbreviation": t.get("tla") or t.get("shortName"),
            "city": area.get("name"),
            "venue_name": t.get("venue"),
            "founded_year": _safe_int(t.get("founded")),
        })
    return records


# ── Understat (soccer xG) ─────────────────────────────────

def _understat_games(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Load Understat match records, contributing home_xg / away_xg.

    Walks ``matches/season_type/{type}/week_NN/{date}/{match_id}/match.json``
    and falls back to the root ``league_matches.json`` bulk file when the
    structured layout is absent.
    """
    records: list[dict[str, Any]] = []
    seen: set[str] = set()

    matches_root = base / "matches" / "season_type"
    if matches_root.is_dir():
        for st_dir in sorted(matches_root.iterdir()):
            season_type = st_dir.name
            for week_dir in sorted(st_dir.iterdir()):
                for date_dir in sorted(week_dir.iterdir()):
                    for match_dir in sorted(date_dir.iterdir()):
                        mf = match_dir / "match.json"
                        if not mf.exists():
                            continue
                        m = _load_json(mf)
                        if not m or not isinstance(m, dict):
                            continue
                        mid = str(m.get("id", ""))
                        if not mid or mid in seen:
                            continue
                        seen.add(mid)
                        h = m.get("h", {})
                        a = m.get("a", {})
                        xg = m.get("xG", {})
                        goals = m.get("goals", {})
                        is_result = bool(m.get("isResult"))
                        dt = (m.get("datetime") or "")[:10] or None
                        records.append({
                            "id": mid,
                            "season": season,
                            "season_type": season_type,
                            "date": dt,
                            "status": "final" if is_result else "scheduled",
                            "home_team": h.get("title", ""),
                            "away_team": a.get("title", ""),
                            "home_team_id": str(h.get("id", "")),
                            "away_team_id": str(a.get("id", "")),
                            "home_score": _safe_int(goals.get("h")) if is_result else None,
                            "away_score": _safe_int(goals.get("a")) if is_result else None,
                            "home_xg": _safe_float(xg.get("h")),
                            "away_xg": _safe_float(xg.get("a")),
                        })

    # Fallback: root league_matches.json bulk file
    if not records:
        bulk = _load_json(base / "league_matches.json")
        if bulk and isinstance(bulk, list):
            for m in bulk:
                mid = str(m.get("id", ""))
                if not mid or mid in seen:
                    continue
                seen.add(mid)
                h = m.get("h", {})
                a = m.get("a", {})
                xg = m.get("xG", {})
                goals = m.get("goals", {})
                is_result = bool(m.get("isResult"))
                dt = (m.get("datetime") or "")[:10] or None
                records.append({
                    "id": mid,
                    "season": season,
                    "season_type": "regular",
                    "date": dt,
                    "status": "final" if is_result else "scheduled",
                    "home_team": h.get("title", ""),
                    "away_team": a.get("title", ""),
                    "home_team_id": str(h.get("id", "")),
                    "away_team_id": str(a.get("id", "")),
                    "home_score": _safe_int(goals.get("h")) if is_result else None,
                    "away_score": _safe_int(goals.get("a")) if is_result else None,
                    "home_xg": _safe_float(xg.get("h")),
                    "away_xg": _safe_float(xg.get("a")),
                })

    return records


def _understat_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Aggregate per-player per-match xG stats from Understat shots.json files.

    Each shots.json at ``matches/season_type/{type}/week_NN/{date}/{match_id}/shots.json``
    contains ``{"h": [...shots], "a": [...shots]}``.  Shots are aggregated by
    player_id × match_id to produce SoccerStats-compatible records.
    """
    # Collect all shots.json paths
    shot_files: list[Path] = []
    matches_root = base / "matches" / "season_type"
    if matches_root.is_dir():
        shot_files = sorted(matches_root.rglob("shots.json"))

    if not shot_files:
        return []

    records: list[dict[str, Any]] = []
    for sf in shot_files:
        data = _load_json(sf)
        if not data or not isinstance(data, dict):
            continue
        # Combine home and away shot lists
        all_shots: list[dict] = []
        for side in ("h", "a"):
            side_shots = data.get(side)
            if isinstance(side_shots, list):
                all_shots.extend(side_shots)

        # Aggregate per player within this match
        player_agg: dict[str, dict[str, Any]] = {}
        for shot in all_shots:
            pid = str(shot.get("player_id", ""))
            if not pid:
                continue
            if pid not in player_agg:
                player_agg[pid] = {
                    "player_name": shot.get("player", ""),
                    "team": shot.get("h_team", "") if shot.get("h_a") == "h" else shot.get("a_team", ""),
                    "match_id": str(shot.get("match_id", "")),
                    "date": (shot.get("date") or "")[:10] or None,
                    "xg": 0.0,
                    "shots": 0,
                    "goals": 0,
                }
            pa = player_agg[pid]
            pa["shots"] += 1
            xg_val = _safe_float(shot.get("xG"))
            if xg_val is not None:
                pa["xg"] += xg_val
            if (shot.get("result") or "").lower() == "goal":
                pa["goals"] += 1

        match_id_dir = sf.parent.name  # directory is the match_id
        for pid, pa in player_agg.items():
            mid = pa["match_id"] or match_id_dir
            records.append({
                "id": f"{mid}_{pid}",
                "player_id": pid,
                "player_name": pa["player_name"],
                "team_name": pa["team"],
                "game_id": mid,
                "season": season,
                "date": pa["date"],
                "xg": round(pa["xg"], 4),
                "shots": pa["shots"],
                "goals": pa["goals"],
            })

    return records


# ── NHL player stats from boxscores ──────────────────────

def _nhl_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract per-player game stats from NHL boxscore JSON files."""
    records: list[dict[str, Any]] = []
    games_dir = base / "games"
    if not games_dir.is_dir():
        return records
    boxscore_files: list[Path] = list(games_dir.glob("*_boxscore.json"))
    for gt in ("regular", "playoffs"):
        gt_dir = games_dir / gt
        if gt_dir.is_dir():
            boxscore_files.extend(gt_dir.glob("*/boxscore.json"))
    for fp in boxscore_files:
        data = _load_json(fp)
        if not data:
            continue
        game_id = str(data.get("id", fp.stem.split("_")[0]))
        game_date = _safe_str(data.get("gameDate"))
        pbg = data.get("playerByGameStats", {})
        for side in ("homeTeam", "awayTeam"):
            team_data = pbg.get(side, {})
            team_info = data.get(side, {})
            team_id = team_info.get("abbrev") or str(team_info.get("id", ""))
            for position_group in ("forwards", "defense", "goalies"):
                for p in team_data.get(position_group, []):
                    pid = str(p.get("playerId", ""))
                    if not pid:
                        continue
                    name = p.get("name", {})
                    player_name = (
                        name.get("default", "") if isinstance(name, dict)
                        else str(name or "")
                    )
                    toi_raw = _safe_str(p.get("toi")) or "0:00"
                    # Convert MM:SS to float minutes
                    try:
                        parts = toi_raw.split(":")
                        toi_min = float(parts[0]) + float(parts[1]) / 60 if len(parts) == 2 else 0.0
                    except (ValueError, IndexError):
                        toi_min = 0.0
                    rec: dict[str, Any] = {
                        "game_id": game_id,
                        "player_id": pid,
                        "player_name": player_name,
                        "team_id": team_id,
                        "season": season,
                        "sport": sport,
                        "source": "nhl",
                        "category": "hockey",
                        "date": game_date,
                        "position": p.get("position", ""),
                        "minutes": toi_min,
                        "toi": toi_raw,
                    }
                    if position_group == "goalies":
                        sa = _safe_int(p.get("shotsAgainst")) or 0
                        ga = _safe_int(p.get("goalsAgainst")) or 0
                        sv = sa - ga
                        rec.update({
                            "goals": 0,
                            "assists": 0,
                            "points": 0,
                            "shots": 0,
                            "saves": sv,
                            "save_pct": _safe_float(p.get("savePctg")) or (sv / sa if sa > 0 else 0.0),
                            "goals_against": ga,
                            "hits": 0,
                            "blocked_shots": 0,
                        })
                    else:
                        rec.update({
                            "goals": _safe_int(p.get("goals")),
                            "assists": _safe_int(p.get("assists")),
                            "points": _safe_int(p.get("points")),
                            "plus_minus": _safe_int(p.get("plusMinus")),
                            "pim": _safe_int(p.get("pim")),
                            "hits": _safe_int(p.get("hits")),
                            "shots": _safe_int(p.get("sog")),
                            "blocked_shots": _safe_int(p.get("blockedShots")),
                            "pp_goals": _safe_int(p.get("powerPlayGoals")),
                            "takeaways": _safe_int(p.get("takeaways")),
                            "giveaways": _safe_int(p.get("giveaways")),
                            "saves": 0,
                            "save_pct": 0.0,
                            "goals_against": 0,
                        })
                    records.append(rec)
    return records


# ── ESPN MLB player stats from boxscores ──────────────────

def _espn_mlb_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract per-player batting and pitching stats from ESPN MLB boxscore JSON."""
    records: list[dict[str, Any]] = []
    game_files = _espn_game_files(base)
    if not game_files:
        return records

    for fp in game_files:
        data = _load_json(fp)
        if not data:
            continue

        event_id = str(data.get("eventId", fp.parent.name if fp.parent != base else fp.stem))
        summary = data.get("summary", {})
        header = summary.get("header", {})
        boxscore = summary.get("boxscore", {})

        # Resolve game date from header competitions
        game_date: str | None = None
        competitions = header.get("competitions", [])
        if competitions:
            game_date = (competitions[0].get("date") or "")[:10] or None

        for team_block in boxscore.get("players", []):
            team_info = team_block.get("team", {})
            team_id = (
                team_info.get("abbreviation")
                or str(team_info.get("id", ""))
            )

            for stat_group in team_block.get("statistics", []):
                stat_type = stat_group.get("type", "")
                labels = stat_group.get("labels", [])

                for athlete_entry in stat_group.get("athletes", []):
                    ath = athlete_entry.get("athlete", {})
                    pid = str(ath.get("id", ""))
                    if not pid:
                        continue
                    player_name = ath.get("displayName", "")
                    position = (
                        ath.get("position", {}).get("abbreviation", "")
                        if isinstance(ath.get("position"), dict)
                        else str(ath.get("position") or "")
                    )
                    raw_stats = athlete_entry.get("stats", [])
                    stat_map = _build_espn_stat_map(
                        labels,
                        raw_stats,
                        _ESPN_MLB_LABEL_ALIASES,
                    )

                    rec: dict[str, Any] = {
                        "game_id": event_id,
                        "player_id": pid,
                        "player_name": player_name,
                        "team_id": team_id,
                        "season": season,
                        "sport": sport,
                        "source": "espn",
                        "category": "baseball",
                        "date": game_date,
                        "position": position,
                    }

                    if stat_type == "batting":
                        rec.update({
                            "ab": _safe_int(stat_map.get("AB")),
                            "hits": _safe_int(stat_map.get("H")),
                            "runs": _safe_int(stat_map.get("R")),
                            "rbi": _safe_int(stat_map.get("RBI")),
                            "hr": _safe_int(stat_map.get("HR")),
                            "bb": _safe_int(stat_map.get("BB")),
                            "so": _safe_int(stat_map.get("K")),
                            "sb": None,
                            "avg": _safe_float(stat_map.get("AVG")),
                            "obp": _safe_float(stat_map.get("OBP")),
                            "slg": _safe_float(stat_map.get("SLG")),
                            "ops": _safe_float(stat_map.get("OPS")),
                        })
                    elif stat_type == "pitching":
                        rec.update({
                            "innings": _safe_float(stat_map.get("IP")),
                            "hits": _safe_int(stat_map.get("H")),
                            "runs": _safe_int(stat_map.get("R")),
                            "earned_runs": _safe_int(stat_map.get("ER")),
                            "walks": _safe_int(stat_map.get("BB")),
                            "strikeouts": _safe_int(stat_map.get("K")),
                            "hr": _safe_int(stat_map.get("HR")),
                            "era": _safe_float(stat_map.get("ERA")),
                        })
                    else:
                        continue

                    records.append(rec)
    return records


def _espn_basketball_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract per-player basketball stats from ESPN boxscore JSON.

    Works for NBA, WNBA, NCAAB, and NCAAW.
    """
    records: list[dict[str, Any]] = []
    game_files = _espn_game_files(base)
    if not game_files:
        return records

    def _split_made_att(val: str | None) -> tuple[int | None, int | None]:
        """Split '8-17' into (8, 17)."""
        if not val or "-" not in str(val):
            return None, None
        parts = str(val).split("-", 1)
        return _safe_int(parts[0]), _safe_int(parts[1])

    for fp in game_files:
        data = _load_json(fp)
        if not data:
            continue

        event_id = str(data.get("eventId", fp.parent.name if fp.parent != base else fp.stem))
        summary = data.get("summary", {})
        header = summary.get("header", {})
        boxscore = summary.get("boxscore", {})

        game_date: str | None = None
        competitions = header.get("competitions", [])
        if competitions:
            game_date = (competitions[0].get("date") or "")[:10] or None

        for team_block in boxscore.get("players", []):
            team_info = team_block.get("team", {})
            team_id = (
                team_info.get("abbreviation")
                or str(team_info.get("id", ""))
            )

            for stat_group in team_block.get("statistics", []):
                labels = stat_group.get("labels", [])

                for athlete_entry in stat_group.get("athletes", []):
                    ath = athlete_entry.get("athlete", {})
                    pid = str(ath.get("id", ""))
                    if not pid:
                        continue
                    player_name = ath.get("displayName", "")
                    position = (
                        ath.get("position", {}).get("abbreviation", "")
                        if isinstance(ath.get("position"), dict)
                        else str(ath.get("position") or "")
                    )
                    raw_stats = athlete_entry.get("stats", [])
                    stat_map = _build_espn_stat_map(
                        labels,
                        raw_stats,
                        _ESPN_BASKETBALL_LABEL_ALIASES,
                    )

                    fg_m, fg_a = _split_made_att(stat_map.get("FG"))
                    fg3_m, fg3_a = _split_made_att(stat_map.get("3PT"))
                    ft_m, ft_a = _split_made_att(stat_map.get("FT"))

                    rec: dict[str, Any] = {
                        "game_id": event_id,
                        "player_id": pid,
                        "player_name": player_name,
                        "team_id": team_id,
                        "season": season,
                        "sport": sport,
                        "source": "espn",
                        "category": "basketball",
                        "date": game_date,
                        "position": position,
                        "minutes": _safe_int(stat_map.get("MIN")),
                        "fg_made": fg_m,
                        "fg_attempted": fg_a,
                        "fg3_made": fg3_m,
                        "fg3_attempted": fg3_a,
                        "ft_made": ft_m,
                        "ft_attempted": ft_a,
                        "oreb": _safe_int(stat_map.get("OREB")),
                        "dreb": _safe_int(stat_map.get("DREB")),
                        "reb": _safe_int(stat_map.get("REB")),
                        "ast": _safe_int(stat_map.get("AST")),
                        "stl": _safe_int(stat_map.get("STL")),
                        "blk": _safe_int(stat_map.get("BLK")),
                        "to": _safe_int(stat_map.get("TO")),
                        "pf": _safe_int(stat_map.get("PF")),
                        "pts": _safe_int(stat_map.get("PTS")),
                        "plus_minus": _safe_int(stat_map.get("+/-")),
                    }
                    records.append(rec)
    return records


def _espn_football_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract per-player football stats from ESPN boxscore JSON.

    Works for NFL and NCAAF.  Each statistical category (passing, rushing,
    receiving, etc.) produces separate records tagged with ``stat_type``.
    """
    records: list[dict[str, Any]] = []
    game_files = _espn_game_files(base)
    if not game_files:
        return records

    def _split_made_att(val: Any) -> tuple[int | None, int | None]:
        text = _safe_str(val)
        if not text:
            return None, None
        if "/" in text:
            parts = text.split("/", 1)
        elif "-" in text:
            parts = text.split("-", 1)
        else:
            return None, None
        return _safe_int(parts[0]), _safe_int(parts[1])

    for fp in game_files:
        data = _load_json(fp)
        if not data:
            continue

        event_id = str(data.get("eventId", fp.parent.name if fp.parent != base else fp.stem))
        summary = data.get("summary", {})
        header = summary.get("header", {})
        boxscore = summary.get("boxscore", {})

        game_date: str | None = None
        competitions = header.get("competitions", [])
        if competitions:
            game_date = (competitions[0].get("date") or "")[:10] or None

        for team_block in boxscore.get("players", []):
            team_info = team_block.get("team", {})
            team_id = (
                team_info.get("abbreviation")
                or str(team_info.get("id", ""))
            )

            for stat_group in team_block.get("statistics", []):
                stat_type = stat_group.get("type", stat_group.get("name", "")).lower()
                labels = stat_group.get("labels", [])

                for athlete_entry in stat_group.get("athletes", []):
                    ath = athlete_entry.get("athlete", {})
                    pid = str(ath.get("id", ""))
                    if not pid:
                        continue
                    player_name = ath.get("displayName", "")
                    position = (
                        ath.get("position", {}).get("abbreviation", "")
                        if isinstance(ath.get("position"), dict)
                        else str(ath.get("position") or "")
                    )
                    raw_stats = athlete_entry.get("stats", [])
                    stat_map = _build_espn_stat_map(
                        labels,
                        raw_stats,
                        _ESPN_FOOTBALL_LABEL_ALIASES,
                    )

                    rec: dict[str, Any] = {
                        "game_id": event_id,
                        "player_id": pid,
                        "player_name": player_name,
                        "team_id": team_id,
                        "season": season,
                        "sport": sport,
                        "source": "espn",
                        "category": "football",
                        "stat_type": stat_type,
                        "date": game_date,
                        "position": position,
                    }

                    if stat_type == "passing":
                        c_att = str(stat_map.get("C/ATT", ""))
                        parts = c_att.split("/", 1) if "/" in c_att else [None, None]
                        rec.update({
                            "pass_cmp": _safe_int(parts[0]),
                            "pass_att": _safe_int(parts[1]),
                            "pass_yds": _safe_int(stat_map.get("YDS")),
                            "pass_td": _safe_int(stat_map.get("TD")),
                            "pass_int": _safe_int(stat_map.get("INT")),
                            "sacks": _safe_float(stat_map.get("SACKS")),
                            "pass_rating": _safe_float(stat_map.get("RTG") or stat_map.get("QBR")),
                        })
                    elif stat_type == "rushing":
                        rec.update({
                            "rush_att": _safe_int(stat_map.get("CAR")),
                            "rush_yds": _safe_int(stat_map.get("YDS")),
                            "rush_td": _safe_int(stat_map.get("TD")),
                        })
                    elif stat_type == "receiving":
                        rec.update({
                            "receptions": _safe_int(stat_map.get("REC")),
                            "rec_yds": _safe_int(stat_map.get("YDS")),
                            "rec_avg": _safe_float(stat_map.get("AVG")),
                            "rec_td": _safe_int(stat_map.get("TD")),
                            "rec_long": _safe_int(stat_map.get("LONG")),
                            "targets": _safe_int(stat_map.get("TGTS")),
                        })
                    elif stat_type == "fumbles":
                        rec.update({
                            "fumbles": _safe_int(stat_map.get("FUM")),
                            "fumbles_lost": _safe_int(stat_map.get("LOST")),
                            "fumbles_rec": _safe_int(stat_map.get("REC")),
                        })
                    elif stat_type == "defensive":
                        rec.update({
                            "tackles": _safe_int(stat_map.get("TOT")),
                            "sacks": _safe_float(stat_map.get("SACKS")),
                            "interceptions": _safe_int(stat_map.get("INT")),
                            "fumbles": _safe_int(stat_map.get("FF")),
                        })
                    elif stat_type == "kicking":
                        fg_made, fg_attempted_from_fg = _split_made_att(stat_map.get("FG"))
                        xp_made, xp_attempted_from_xp = _split_made_att(stat_map.get("XP"))
                        rec.update({
                            "fg_made": fg_made if fg_made is not None else _safe_int(stat_map.get("FG")),
                            "fg_attempted": (
                                fg_attempted_from_fg
                                if fg_attempted_from_fg is not None
                                else _safe_int(stat_map.get("FGA") or stat_map.get("ATT"))
                            ),
                            "fg_long": _safe_int(stat_map.get("LONG")),
                            "xp_made": xp_made if xp_made is not None else _safe_int(stat_map.get("XP")),
                            "xp_attempted": (
                                xp_attempted_from_xp
                                if xp_attempted_from_xp is not None
                                else _safe_int(stat_map.get("XPA") or stat_map.get("ATT"))
                            ),
                            "kick_pts": _safe_int(stat_map.get("PTS")),
                        })
                    elif stat_type == "punting":
                        rec.update({
                            "punts": _safe_int(stat_map.get("NO")),
                            "punt_yds": _safe_int(stat_map.get("YDS")),
                            "punt_avg": _safe_float(stat_map.get("AVG")),
                            "punt_long": _safe_int(stat_map.get("LONG")),
                            "punt_in20": _safe_int(stat_map.get("IN 20")),
                            "punt_tb": _safe_int(stat_map.get("TB")),
                        })
                    elif stat_type in ("kick returns", "kickreturns"):
                        rec.update({
                            "kr_no": _safe_int(stat_map.get("NO")),
                            "kr_yds": _safe_int(stat_map.get("YDS")),
                            "kr_avg": _safe_float(stat_map.get("AVG")),
                            "kr_long": _safe_int(stat_map.get("LONG")),
                            "kr_td": _safe_int(stat_map.get("TD")),
                        })
                    elif stat_type in ("punt returns", "puntreturns"):
                        rec.update({
                            "pr_no": _safe_int(stat_map.get("NO")),
                            "pr_yds": _safe_int(stat_map.get("YDS")),
                            "pr_avg": _safe_float(stat_map.get("AVG")),
                            "pr_long": _safe_int(stat_map.get("LONG")),
                            "pr_td": _safe_int(stat_map.get("TD")),
                        })
                    else:
                        # Unknown category — store raw label→value pairs
                        for lbl, val in stat_map.items():
                            key = lbl.lower().replace(" ", "_").replace("/", "_")
                            rec[key] = _safe_str(val)

                    records.append(rec)
    return records


def _espn_hockey_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract per-player hockey stats from ESPN boxscore JSON (NHL)."""
    records: list[dict[str, Any]] = []
    game_files = _espn_game_files(base)
    if not game_files:
        return records

    for fp in game_files:
        data = _load_json(fp)
        if not data:
            continue

        event_id = str(data.get("eventId", fp.parent.name if fp.parent != base else fp.stem))
        summary = data.get("summary", {})
        header = summary.get("header", {})
        boxscore = summary.get("boxscore", {})

        game_date: str | None = None
        competitions = header.get("competitions", [])
        if competitions:
            game_date = (competitions[0].get("date") or "")[:10] or None

        for team_block in boxscore.get("players", []):
            team_info = team_block.get("team", {})
            team_id = (
                team_info.get("abbreviation")
                or str(team_info.get("id", ""))
            )

            for stat_group in team_block.get("statistics", []):
                stat_type = stat_group.get("type", stat_group.get("name", "")).lower()
                labels = stat_group.get("labels", [])
                is_goalie = "goalie" in stat_type or "goalkeeping" in stat_type

                for athlete_entry in stat_group.get("athletes", []):
                    ath = athlete_entry.get("athlete", {})
                    pid = str(ath.get("id", ""))
                    if not pid:
                        continue
                    player_name = ath.get("displayName", "")
                    position = (
                        ath.get("position", {}).get("abbreviation", "")
                        if isinstance(ath.get("position"), dict)
                        else str(ath.get("position") or "")
                    )
                    raw_stats = athlete_entry.get("stats", [])
                    stat_map = _build_espn_stat_map(
                        labels,
                        raw_stats,
                        _ESPN_HOCKEY_LABEL_ALIASES,
                    )

                    rec: dict[str, Any] = {
                        "game_id": event_id,
                        "player_id": pid,
                        "player_name": player_name,
                        "team_id": team_id,
                        "season": season,
                        "sport": sport,
                        "source": "espn",
                        "category": "hockey",
                        "stat_type": "goalie" if is_goalie else "skater",
                        "date": game_date,
                        "position": position,
                    }

                    if is_goalie:
                        sv_pct = stat_map.get("SV%", "")
                        if sv_pct and "%" in str(sv_pct):
                            sv_pct = sv_pct.replace("%", "")
                        rec.update({
                            "shots_against": _safe_int(stat_map.get("SA")),
                            "saves": _safe_int(stat_map.get("SV")),
                            "save_pct": _safe_float(sv_pct) if sv_pct else None,
                            "goals_against": _safe_int(stat_map.get("GA")),
                            "es_saves": _safe_int(stat_map.get("ESSV")),
                            "pp_saves": _safe_int(stat_map.get("PPSV")),
                            "sh_saves": _safe_int(stat_map.get("SHSV")),
                            "toi": _safe_str(stat_map.get("TOI")),
                        })
                    else:
                        rec.update({
                            "goals": _safe_int(stat_map.get("G")),
                            "assists": _safe_int(stat_map.get("A")),
                            "points": _safe_int(stat_map.get("PTS")),
                            "plus_minus": _safe_int(stat_map.get("+/-")),
                            "shots": _safe_int(stat_map.get("S")),
                            "shot_misses": _safe_int(stat_map.get("SM")),
                            "blocked_shots": _safe_int(stat_map.get("BS")),
                            "hits": _safe_int(stat_map.get("HT")),
                            "takeaways": _safe_int(stat_map.get("TK")),
                            "giveaways": _safe_int(stat_map.get("GV")),
                            "pp_toi": _safe_str(stat_map.get("PPTOI")),
                            "sh_toi": _safe_str(stat_map.get("SHTOI")),
                            "toi": _safe_str(stat_map.get("TOI")),
                        })

                    records.append(rec)
    return records


def _espn_soccer_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract per-player soccer stats from ESPN game JSON.

    ESPN soccer stores player stats in rosters[].roster[].
    Some leagues (EPL) nest under summary.rosters, others (UCL, NWSL, MLS)
    store rosters at root level.
    """
    records: list[dict[str, Any]] = []
    game_files = _espn_game_files(base)
    if not game_files:
        return records

    for fp in game_files:
        data = _load_json(fp)
        if not data:
            continue

        event_id = str(data.get("eventId", fp.parent.name if fp.parent != base else fp.stem))
        summary = data.get("summary", {})
        # Try root-level first (UCL, NWSL, MLS), fall back to summary (EPL)
        header = data.get("header") or summary.get("header", {})

        game_date: str | None = None
        competitions = header.get("competitions", [])
        if competitions:
            game_date = (competitions[0].get("date") or "")[:10] or None

        rosters = data.get("rosters") or summary.get("rosters", [])
        for roster_block in rosters:
            team_info = roster_block.get("team", {})
            team_id = (
                team_info.get("abbreviation")
                or str(team_info.get("id", ""))
            )

            for player_entry in roster_block.get("roster", []):
                ath = player_entry.get("athlete", {})
                pid = str(ath.get("id", ""))
                if not pid:
                    continue

                player_name = ath.get("displayName", "")
                pos = player_entry.get("position", {})
                position = (
                    pos.get("abbreviation", "")
                    if isinstance(pos, dict)
                    else str(pos or "")
                )

                stat_map = _build_espn_name_stat_map(
                    player_entry.get("stats", []),
                    _ESPN_SOCCER_NAME_ALIASES,
                )

                rec: dict[str, Any] = {
                    "game_id": event_id,
                    "player_id": pid,
                    "player_name": player_name,
                    "team_id": team_id,
                    "season": season,
                    "sport": sport,
                    "source": "espn",
                    "category": "soccer",
                    "date": game_date,
                    "position": position,
                    "starter": player_entry.get("starter"),
                    "goals": _safe_int(stat_map.get("totalGoals")),
                    "assists": _safe_int(stat_map.get("goalAssists")),
                    "shots": _safe_int(stat_map.get("totalShots")),
                    "shots_on_target": _safe_int(stat_map.get("shotsOnTarget")),
                    "fouls_committed": _safe_int(stat_map.get("foulsCommitted")),
                    "fouls_suffered": _safe_int(stat_map.get("foulsSuffered")),
                    "yellow_cards": _safe_int(stat_map.get("yellowCards")),
                    "red_cards": _safe_int(stat_map.get("redCards")),
                    "offsides": _safe_int(stat_map.get("offsides")),
                    "own_goals": _safe_int(stat_map.get("ownGoals")),
                    "saves": _safe_int(stat_map.get("saves")),
                    "goals_conceded": _safe_int(stat_map.get("goalsConceded")),
                    "shots_faced": _safe_int(stat_map.get("shotsFaced")),
                    "passes": _safe_int(stat_map.get("passes")),
                    "pass_pct": _maybe_pct(stat_map.get("passAccuracy")),
                    "tackles": _safe_int(stat_map.get("tackles")),
                    "interceptions": _safe_int(stat_map.get("interceptions")),
                    "fouls": _safe_int(stat_map.get("foulsCommitted")),
                    "xg": _safe_float(stat_map.get("xg")),
                    "xa": _safe_float(stat_map.get("xa")),
                    "key_passes": _safe_int(stat_map.get("keyPasses")),
                    "dribbles_completed": _safe_int(stat_map.get("dribblesCompleted")),
                    "aerial_duels_won": _safe_int(stat_map.get("aerialDuelsWon")),
                }
                records.append(rec)
    return records


def _espn_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Dispatch to sport-specific ESPN player stats extractor."""
    if sport in ("mlb",):
        return _espn_mlb_player_stats(base, sport, season)
    elif sport in ("nba", "wnba", "ncaab", "ncaaw"):
        return _espn_basketball_player_stats(base, sport, season)
    elif sport in ("nfl", "ncaaf"):
        return _espn_football_player_stats(base, sport, season)
    elif sport in ("nhl",):
        return _espn_hockey_player_stats(base, sport, season)
    elif sport in ("epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "nwsl", "ucl"):
        return _espn_soccer_player_stats(base, sport, season)
    elif sport in ("golf",):
        return _espn_golf_player_stats(base, sport, season)
    elif sport in ("atp", "wta"):
        return _espn_tennis_player_stats(base, sport, season)
    elif sport in ("f1",):
        return _espn_f1_player_stats(base, sport, season)
    return []


def _espn_play_by_play(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract play-by-play rows from ESPN event summaries (drives/plays + scoring plays)."""
    records: list[dict[str, Any]] = []
    for p in _espn_game_files(base):
        payload = _load_json(p)
        if not isinstance(payload, dict):
            continue

        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else payload
        if not isinstance(summary, dict):
            continue

        game_id = _safe_str(payload.get("eventId") or summary.get("eventId") or p.parent.name or p.stem)
        if not game_id:
            continue

        drives_block = summary.get("drives")
        drives: list[dict[str, Any]] = []
        if isinstance(drives_block, dict):
            previous = drives_block.get("previous")
            if isinstance(previous, list):
                drives = [d for d in previous if isinstance(d, dict)]

        for drv in drives:
            drive_id = _safe_str(drv.get("id")) or ""
            team = drv.get("team") if isinstance(drv.get("team"), dict) else {}
            offense_team_id = _safe_str(team.get("id")) or ""
            offense_team_name = _safe_str(team.get("displayName") or team.get("name")) or ""

            plays = drv.get("plays")
            if not isinstance(plays, list):
                continue

            for play in plays:
                if not isinstance(play, dict):
                    continue
                clock = play.get("clock") if isinstance(play.get("clock"), dict) else {}
                ptype = play.get("type") if isinstance(play.get("type"), dict) else {}
                start = play.get("start") if isinstance(play.get("start"), dict) else {}
                records.append({
                    "sport": sport,
                    "source": "espn",
                    "season": season,
                    "game_id": game_id,
                    "drive_id": drive_id,
                    "play_id": _safe_str(play.get("id")) or "",
                    "sequence_number": _safe_int(play.get("sequenceNumber")),
                    "event_type": _safe_str(ptype.get("text") or ptype.get("name") or play.get("type")) or "play",
                    "description": _safe_str(play.get("text")) or _safe_str(drv.get("description")) or "",
                    "period": _safe_int(play.get("period")),
                    "clock": _safe_str(clock.get("displayValue") or clock.get("value")) or "",
                    "down": _safe_int(start.get("down")),
                    "distance": _safe_int(start.get("distance")),
                    "yards_gained": _safe_int(play.get("statYardage")),
                    "scoring_play": bool(play.get("scoringPlay") or False),
                    "offense_team_id": offense_team_id,
                    "offense_team_name": offense_team_name,
                    "home_score": _safe_int(play.get("homeScore")),
                    "away_score": _safe_int(play.get("awayScore")),
                })

        scoring_plays = summary.get("scoringPlays")
        if isinstance(scoring_plays, list):
            for sp in scoring_plays:
                if not isinstance(sp, dict):
                    continue
                clock = sp.get("clock") if isinstance(sp.get("clock"), dict) else {}
                team = sp.get("team") if isinstance(sp.get("team"), dict) else {}
                stype = sp.get("scoringType") if isinstance(sp.get("scoringType"), dict) else {}
                records.append({
                    "sport": sport,
                    "source": "espn",
                    "season": season,
                    "game_id": game_id,
                    "drive_id": "",
                    "play_id": _safe_str(sp.get("id")) or "",
                    "sequence_number": None,
                    "event_type": _safe_str(stype.get("name") or sp.get("type")) or "scoring_play",
                    "description": _safe_str(sp.get("text")) or "",
                    "period": _safe_int(sp.get("period")),
                    "clock": _safe_str(clock.get("displayValue") or clock.get("value")) or "",
                    "down": None,
                    "distance": None,
                    "yards_gained": None,
                    "scoring_play": True,
                    "offense_team_id": _safe_str(team.get("id")) or "",
                    "offense_team_name": _safe_str(team.get("displayName") or team.get("name")) or "",
                    "home_score": _safe_int(sp.get("homeScore")),
                    "away_score": _safe_int(sp.get("awayScore")),
                })

    return records


def _espn_drives(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for p in _espn_game_files(base):
        payload = _load_json(p)
        if not isinstance(payload, dict):
            continue

        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else payload
        if not isinstance(summary, dict):
            continue

        game_id = _safe_str(payload.get("eventId") or summary.get("eventId") or p.parent.name or p.stem)
        if not game_id:
            continue

        drives_block = summary.get("drives") if isinstance(summary.get("drives"), dict) else {}
        drives = drives_block.get("previous") if isinstance(drives_block.get("previous"), list) else []
        for d in drives:
            if not isinstance(d, dict):
                continue
            drive_id = _safe_str(d.get("id"))
            if not drive_id:
                continue
            team = d.get("team") if isinstance(d.get("team"), dict) else {}
            records.append({
                "sport": sport,
                "season": season,
                "source": "espn",
                "game_id": game_id,
                "drive_id": drive_id,
                "offense_team_id": _safe_str(team.get("id")) or "",
                "offense_team_name": _safe_str(team.get("displayName") or team.get("name")) or "",
                "description": _safe_str(d.get("description")) or "",
                "plays": _safe_int(d.get("plays")),
                "yards": _safe_int(d.get("yards")),
                "drive_result": _safe_str(d.get("result")) or "",
                "scoring": bool(d.get("isScore") or False),
            })
    return records


def _espn_coaches(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Best-effort extraction of coaches from ESPN team payloads.

    ESPN coverage is inconsistent by sport; this parser opportunistically
    captures fields when present and otherwise returns an empty list.
    """
    teams_dir = base / "teams"
    if not teams_dir.is_dir():
        return []

    records: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _emit(team_id: str, team_name: str, coach_name: str) -> None:
        coach_name = (coach_name or "").strip()
        if not coach_name:
            return
        cid = f"espn|{sport}|{season}|{team_id}|{coach_name.lower().replace(' ', '_')}"
        if cid in seen:
            return
        seen.add(cid)
        records.append({
            "coach_id": cid,
            "coach_name": coach_name,
            "team_id": team_id,
            "team_name": team_name,
            "season": season,
            "sport": sport,
            "source": "espn",
        })

    for f in teams_dir.glob("*.json"):
        payload = _load_json(f)
        if not isinstance(payload, dict):
            continue
        team_id = _safe_str(payload.get("id") or payload.get("team_id")) or f.stem
        team_name = _safe_str(payload.get("name") or payload.get("displayName")) or ""

        direct_fields = [
            payload.get("coach"),
            payload.get("headCoach"),
            payload.get("head_coach"),
        ]
        for item in direct_fields:
            if isinstance(item, str):
                _emit(team_id, team_name, item)
            elif isinstance(item, dict):
                _emit(team_id, team_name, _safe_str(item.get("name") or item.get("displayName")) or "")

        coaches = payload.get("coaches")
        if isinstance(coaches, list):
            for c in coaches:
                if isinstance(c, str):
                    _emit(team_id, team_name, c)
                elif isinstance(c, dict):
                    _emit(team_id, team_name, _safe_str(c.get("name") or c.get("displayName")) or "")

    return records


# ── ESPN Golf extractors ─────────────────────────────────

def _espn_golf_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract unique golfers from ESPN tournament game files."""
    game_files = _espn_game_files(base)
    if not game_files:
        return []
    seen: dict[str, dict[str, Any]] = {}
    for p in game_files:
        data = _load_json(p)
        if not data:
            continue
        sb = data.get("scoreboard", {})
        for comp in sb.get("competitions", []):
            for c in comp.get("competitors", []):
                pid = str(c.get("id", ""))
                if not pid:
                    continue
                ath = c.get("athlete", {})
                name = ath.get("displayName") or ath.get("fullName") or ""
                if not name:
                    continue
                if pid not in seen:
                    country = ""
                    flag = ath.get("flag", {})
                    if isinstance(flag, dict):
                        country = flag.get("alt", "")
                    seen[pid] = {
                        "id": pid,
                        "name": name,
                        "first_name": (name.split(" ", 1)[0] if " " in name else name),
                        "last_name": (name.split(" ", 1)[1] if " " in name else ""),
                        "position": "Golfer",
                        "team_id": "",
                        "team_name": country,
                        "nationality": country,
                        "season": season,
                    }
    return list(seen.values())


def _espn_golf_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract per-tournament performance for each golfer."""
    game_files = _espn_game_files(base)
    if not game_files:
        return []
    records: list[dict[str, Any]] = []
    for p in game_files:
        data = _load_json(p)
        if not data:
            continue
        game_id = str(data.get("eventId", p.parent.name if p.parent != base else p.stem))
        sb = data.get("scoreboard", {})
        game_date = sb.get("date", "")[:10]
        for comp in sb.get("competitions", []):
            for c in comp.get("competitors", []):
                pid = str(c.get("id", ""))
                if not pid:
                    continue
                ath = c.get("athlete", {})
                name = ath.get("displayName") or ""
                score_str = c.get("score")
                finish = _safe_int(c.get("order"))
                linescores = c.get("linescores", [])
                rounds_played = len([ls for ls in linescores if ls.get("period", 99) <= 4])
                # score_to_par: ESPN stores as string like "-18"
                score_to_par = _safe_int(score_str) if score_str and (
                    score_str.lstrip("-").isdigit() or score_str == "E"
                ) else None
                if score_str == "E":
                    score_to_par = 0
                # Calculate total strokes from round scores
                total_strokes = 0
                for ls in linescores:
                    period = ls.get("period")
                    if period is not None and period <= 4:
                        val = _safe_int(ls.get("value"))
                        if val:
                            total_strokes += val
                rec: dict[str, Any] = {
                    "game_id": game_id,
                    "player_id": pid,
                    "player_name": name,
                    "team_id": "",
                    "season": season,
                    "date": game_date,
                    "position": finish,
                    "score": total_strokes if total_strokes > 0 else None,
                    "score_to_par": score_to_par,
                    "rounds": rounds_played if rounds_played > 0 else None,
                }
                records.append(rec)
    return records


# ── ESPN Tennis extractors ────────────────────────────────

def _espn_tennis_load_matches(base: Path) -> list[dict[str, Any]]:
    """Batch-load all match_*.json from games/ directory efficiently."""
    matches: list[dict[str, Any]] = []
    for game_file in _espn_game_files(base):
        if not game_file.name.startswith("match_"):
            continue
        try:
            with open(game_file, "r") as fh:
                matches.append(json.load(fh))
        except Exception:
            continue
    return matches


def _espn_tennis_games(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract individual tennis matches from ESPN match_*.json files."""
    import hashlib
    all_matches = _espn_tennis_load_matches(base)
    if not all_matches:
        return []
    records: list[dict[str, Any]] = []
    seen: set[str] = set()

    for data in all_matches:
        match = data.get("matchData", {})
        match_id = str(match.get("id", data.get("eventId", "")))
        if not match_id or match_id in seen:
            continue
        seen.add(match_id)

        status_info = match.get("status", {}).get("type", {})
        status_name = status_info.get("name", "")
        if status_name == "STATUS_FINAL":
            status = "final"
        elif "IN_PROGRESS" in status_name:
            status = "in_progress"
        else:
            status = "scheduled"

        competitors = match.get("competitors", [])
        if len(competitors) != 2:
            continue

        p1, p2 = competitors[0], competitors[1]
        a1 = p1.get("athlete", {})
        a2 = p2.get("athlete", {})
        p1_name = a1.get("displayName") or a1.get("fullName") or ""
        p2_name = a2.get("displayName") or a2.get("fullName") or ""
        p1_id = str(a1.get("guid") or a1.get("id") or "")
        p2_id = str(a2.get("guid") or a2.get("id") or "")
        p1_winner = p1.get("winner", False)

        # Deterministic home/away to avoid target bias
        swap = int(hashlib.md5(match_id.encode()).hexdigest(), 16) % 2 == 0
        if swap:
            home_name, away_name = p2_name, p1_name
            home_id, away_id = p2_id, p1_id
            home_winner = not p1_winner
        else:
            home_name, away_name = p1_name, p2_name
            home_id, away_id = p1_id, p2_id
            home_winner = p1_winner

        home_score = 1 if home_winner and status == "final" else 0
        away_score = 0 if home_winner and status == "final" else (1 if status == "final" else 0)

        set_scores: dict[str, Any] = {}
        for ci, comp in enumerate(competitors):
            prefix = "away" if (ci == 0) != swap else "home"
            linescores = comp.get("linescores", [])
            for si, ls in enumerate(linescores):
                val = ls.get("value")
                if val is not None:
                    set_scores[f"{prefix}_set{si+1}"] = int(val)

        date_str = match.get("date") or match.get("startDate") or ""
        if date_str and "T" in date_str:
            date_str = date_str[:10]

        tournament_name = data.get("tournamentName", "")
        venue_info = match.get("venue", {})
        venue = venue_info.get("fullName") or venue_info.get("shortName") or tournament_name

        round_info = match.get("round", {})
        round_name = round_info.get("displayName") or round_info.get("abbreviation") or ""

        fmt = match.get("format", {})
        best_of = _safe_int(fmt.get("regulation", {}).get("periods"))

        records.append({
            "id": match_id,
            "season": season,
            "date": date_str if date_str else None,
            "status": status,
            "home_team": home_name,
            "away_team": away_name,
            "home_score": home_score,
            "away_score": away_score,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "venue": venue,
            "round": round_name,
            "best_of": best_of,
            "tournament": tournament_name,
            **set_scores,
        })
    return records


def _espn_tennis_players(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract unique tennis players from ESPN match files."""
    all_matches = _espn_tennis_load_matches(base)
    if not all_matches:
        return []
    seen: set[str] = set()
    records: list[dict[str, Any]] = []

    for data in all_matches:
        match = data.get("matchData", {})
        for comp in match.get("competitors", []):
            athlete = comp.get("athlete", {})
            pid = str(athlete.get("guid") or athlete.get("id") or "")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            flag = athlete.get("flag") or {}
            nationality = None
            if flag:
                nationality = flag.get("alt") or flag.get("href", "").split("/")[-1].replace(".png", "").upper()
            records.append({
                "id": pid,
                "name": athlete.get("displayName") or athlete.get("fullName") or "",
                "nationality": nationality,
            })
    return records


def _espn_tennis_player_stats(
    base: Path, sport: str, season: str,
) -> list[dict[str, Any]]:
    """Extract per-match player stats from ESPN tennis match files."""
    all_matches = _espn_tennis_load_matches(base)
    if not all_matches:
        return []
    records: list[dict[str, Any]] = []

    for data in all_matches:
        match = data.get("matchData", {})
        match_id = str(match.get("id", data.get("eventId", "")))
        status_name = match.get("status", {}).get("type", {}).get("name", "")
        if status_name != "STATUS_FINAL":
            continue

        date_str = match.get("date") or match.get("startDate") or ""
        if date_str and "T" in date_str:
            date_str = date_str[:10]

        competitors = match.get("competitors", [])
        if len(competitors) != 2:
            continue

        for ci, comp in enumerate(competitors):
            athlete = comp.get("athlete", {})
            pid = str(athlete.get("guid") or athlete.get("id") or "")
            pname = athlete.get("displayName") or athlete.get("fullName") or ""
            won = comp.get("winner", False)

            linescores = comp.get("linescores", [])
            opp = competitors[1 - ci]
            opp_linescores = opp.get("linescores", [])
            sets_won = 0
            sets_lost = 0
            total_games_won = 0
            total_games_lost = 0
            for si in range(max(len(linescores), len(opp_linescores))):
                my_val = linescores[si].get("value", 0) if si < len(linescores) else 0
                opp_val = opp_linescores[si].get("value", 0) if si < len(opp_linescores) else 0
                my_val = int(my_val) if my_val else 0
                opp_val = int(opp_val) if opp_val else 0
                total_games_won += my_val
                total_games_lost += opp_val
                if my_val > opp_val:
                    sets_won += 1
                elif opp_val > my_val:
                    sets_lost += 1

            records.append({
                "source": "espn",
                "game_id": match_id,
                "player_id": pid,
                "player_name": pname,
                "sport": sport,
                "season": season,
                "date": date_str if date_str else None,
                "category": "tennis",
                "won": won,
                "sets_won": sets_won,
                "sets_lost": sets_lost,
                "total_games_won": total_games_won,
                "total_games_lost": total_games_lost,
                "total_sets": len(linescores),
            })
    return records


# ── API-Sports ────────────────────────────────────────────

# ── PandaScore (esports) ──────────────────────────────────

_PANDASCORE_STATUS_MAP: dict[str, str] = {
    "finished": "final",
    "running": "in_progress",
    "not_started": "scheduled",
    "canceled": "cancelled",
    "postponed": "postponed",
}


def _pandascore_load_records(base: Path, endpoint: str) -> list[dict[str, Any]]:
    """Load PandaScore records from flat and hierarchical layouts.

    Supports legacy files (``{endpoint}.json``) and the new partitioned layout:
    ``season_types/{type}/weeks/week_xx/YYYY-MM-DD/{endpoint}/*.json``.
    """
    records: list[dict[str, Any]] = []

    # Legacy flat file
    flat = _load_json(base / f"{endpoint}.json")
    if isinstance(flat, list):
        records.extend([row for row in flat if isinstance(row, dict)])
    elif isinstance(flat, dict):
        records.append(flat)

    # New hierarchical event files
    season_types_dir = base / "season_types"
    if season_types_dir.is_dir():
        pattern = f"*/weeks/week_*/*/{endpoint}/*.json"
        for fp in sorted(season_types_dir.glob(pattern)):
            payload = _load_json(fp)
            if isinstance(payload, dict):
                records.append(payload)
            elif isinstance(payload, list):
                records.extend([row for row in payload if isinstance(row, dict)])

    return records


def _pandascore_matches(base: Path) -> list[dict[str, Any]]:
    """Load and deduplicate PandaScore match-family endpoints by match id."""
    merged: dict[str, dict[str, Any]] = {}
    order = ("matches", "matches_past", "matches_running", "matches_upcoming")
    for endpoint in order:
        for row in _pandascore_load_records(base, endpoint):
            rid = _safe_str(row.get("id"))
            if not rid:
                continue
            existing = merged.get(rid)
            if existing is None:
                merged[rid] = row
                continue
            # Keep richer payload when duplicate ids appear across endpoints.
            if len(row.keys()) > len(existing.keys()):
                merged[rid] = row

    return list(merged.values())


def _pandascore_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load PandaScore ``matches.json`` → Game schema records.

    Falls back to extracting matches embedded in ``tournaments.json``
    when ``matches.json`` is empty (common for 2024 data).
    """
    data = _pandascore_matches(base)
    if data:
        return _pandascore_games_from_matches(data, season)

    # Fallback: extract matches from tournaments.json
    tournaments = _load_json(base / "tournaments.json")
    if not tournaments or not isinstance(tournaments, list):
        return []
    return _pandascore_games_from_tournaments(tournaments, season)


def _pandascore_games_from_matches(
    data: list[dict[str, Any]], season: str,
) -> list[dict[str, Any]]:
    """Convert full PandaScore match objects (with opponents/results)."""
    records: list[dict[str, Any]] = []
    for m in data:
        match_id = m.get("id")
        if not match_id:
            continue
        opponents = m.get("opponents") or []
        home_opp = opponents[0].get("opponent", {}) if len(opponents) > 0 else {}
        away_opp = opponents[1].get("opponent", {}) if len(opponents) > 1 else {}
        results = m.get("results") or []
        home_score: int | None = None
        away_score: int | None = None
        if len(results) >= 2:
            home_score = _safe_int(results[0].get("score"))
            away_score = _safe_int(results[1].get("score"))
        begin_at = m.get("begin_at") or m.get("scheduled_at") or ""
        game_date = begin_at[:10] or None
        status_raw = m.get("status", "not_started")
        league = m.get("league") or {}
        tournament = m.get("tournament") or {}
        venue = tournament.get("country") or league.get("name")
        records.append({
            "id": str(match_id),
            "season": season,
            "date": game_date,
            "status": _PANDASCORE_STATUS_MAP.get(status_raw, "scheduled"),
            "home_team": home_opp.get("name", ""),
            "away_team": away_opp.get("name", ""),
            "home_team_id": _safe_str(home_opp.get("id")),
            "away_team_id": _safe_str(away_opp.get("id")),
            "home_score": home_score,
            "away_score": away_score,
            "venue": venue,
            "source": "pandascore",
        })
    return records


_VS_RE = re.compile(r"^(?:.*:\s*)?(.+?)\s+vs\s+(.+)$", re.IGNORECASE)


def _pandascore_games_from_tournaments(
    tournaments: list[dict[str, Any]], season: str,
) -> list[dict[str, Any]]:
    """Extract minimal match records embedded in tournament objects.

    Tournament matches lack ``opponents`` / ``results`` but carry ``name``
    (e.g. "Upper bracket QF 1: Team A vs Team B") from which team names
    can be parsed.  Matches with "TBD" in the name are skipped.
    """
    records: list[dict[str, Any]] = []
    for t in tournaments:
        t_league = t.get("league") or {}
        venue = t.get("country") or t_league.get("name")
        for m in t.get("matches") or []:
            match_id = m.get("id")
            if not match_id:
                continue
            name = m.get("name") or ""
            if "TBD" in name:
                continue
            home_team = ""
            away_team = ""
            vs_match = _VS_RE.match(name)
            if vs_match:
                home_team = vs_match.group(1).strip()
                away_team = vs_match.group(2).strip()
            begin_at = m.get("begin_at") or m.get("scheduled_at") or ""
            game_date = begin_at[:10] or None
            status_raw = m.get("status", "not_started")
            records.append({
                "id": str(match_id),
                "season": season,
                "date": game_date,
                "status": _PANDASCORE_STATUS_MAP.get(status_raw, "scheduled"),
                "home_team": home_team,
                "away_team": away_team,
                "home_team_id": None,
                "away_team_id": None,
                "home_score": None,
                "away_score": None,
                "venue": venue,
                "source": "pandascore",
            })
    return records


def _pandascore_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load PandaScore ``players.json`` → Player schema records."""
    data = _pandascore_load_records(base, "players")
    if not data:
        # Fallback for throttled seasons: derive a stable player list from
        # match participation rows produced by `_pandascore_player_stats`.
        derived_stats = _pandascore_player_stats(base, sport, season)
        if not derived_stats:
            return []
        by_player: dict[str, dict[str, Any]] = {}
        for row in derived_stats:
            pid = _safe_str(row.get("player_id"))
            if not pid:
                continue
            if pid not in by_player:
                by_player[pid] = {
                    "id": pid,
                    "name": row.get("player_name") or pid,
                    "team_id": _safe_str(row.get("team_id")),
                    "position": None,
                    "nationality": None,
                    "headshot_url": None,
                    "status": "active",
                    "source": "pandascore",
                }
        return list(by_player.values())
    records: list[dict[str, Any]] = []
    for p in data:
        pid = p.get("id")
        if not pid:
            continue
        current_team = p.get("current_team") or {}
        first = p.get("first_name") or ""
        last = p.get("last_name") or ""
        full_name = f"{first} {last}".strip()
        gamertag = p.get("name") or ""
        display = full_name if full_name else gamertag
        records.append({
            "id": str(pid),
            "name": display,
            "team_id": _safe_str(current_team.get("id")),
            "position": p.get("role"),
            "nationality": p.get("nationality"),
            "headshot_url": p.get("image_url"),
            "status": "active" if p.get("active", True) else "inactive",
            "source": "pandascore",
        })
    return records


def _pandascore_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load PandaScore ``teams.json`` → Team schema records."""
    data = _pandascore_load_records(base, "teams")
    if not data:
        return []
    records: list[dict[str, Any]] = []
    for t in data:
        tid = t.get("id")
        if not tid:
            continue
        records.append({
            "id": str(tid),
            "name": t.get("name", ""),
            "abbreviation": t.get("acronym"),
            "city": t.get("location"),
            "logo_url": t.get("image_url"),
            "source": "pandascore",
        })
    return records


def _pandascore_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Generate synthetic standings from PandaScore match results.

    Uses ``matches.json`` first; falls back to ``tournaments.json`` for
    seasons where matches are embedded in tournament objects.
    """
    # Collect all finished matches with a winner
    matches = _pandascore_matches(base)
    if not matches:
        tournaments = _load_json(base / "tournaments.json")
        if tournaments and isinstance(tournaments, list):
            for t in tournaments:
                matches.extend(t.get("matches") or [])

    # Tally wins/losses per team from opponent info or match names
    team_stats: dict[str, dict[str, Any]] = {}  # team_id → {name, wins, losses}

    for m in matches:
        if m.get("status") != "finished":
            continue
        winner_id = m.get("winner_id")
        if not winner_id:
            continue

        opponents = m.get("opponents") or []
        if opponents:
            # Full match data (matches.json) with opponent objects
            for opp_wrapper in opponents:
                opp = opp_wrapper.get("opponent", {})
                tid = opp.get("id")
                if not tid:
                    continue
                tid_str = str(tid)
                if tid_str not in team_stats:
                    team_stats[tid_str] = {
                        "name": opp.get("name", ""),
                        "wins": 0,
                        "losses": 0,
                    }
                if tid == winner_id:
                    team_stats[tid_str]["wins"] += 1
                else:
                    team_stats[tid_str]["losses"] += 1

    if not team_stats:
        return []

    records: list[dict[str, Any]] = []
    for tid_str, stats in team_stats.items():
        wins = stats["wins"]
        losses = stats["losses"]
        total = wins + losses
        records.append({
            "team_id": tid_str,
            "team_name": stats.get("name") or "",
            "wins": wins,
            "losses": losses,
            "pct": round(wins / total, 3) if total else 0.0,
            "games_played": total,
            "season": season,
            "source": "pandascore",
        })
    return records


def _pandascore_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Derive per-player game records from PandaScore matches + player/team rosters.

    PandaScore free tier doesn't provide per-match player stats (kills, deaths),
    but we can link players → teams → matches to produce participation records
    with match outcome data.  This gives ML models player-team-game context.
    """
    # Build team → players mapping from players.json
    players_data = _pandascore_load_records(base, "players")
    team_players: dict[str, list[dict[str, Any]]] = {}  # team_id → [player, ...]
    player_team: dict[str, str] = {}  # player_id → team_id

    if players_data and isinstance(players_data, list):
        for p in players_data:
            pid = p.get("id")
            ct = p.get("current_team") or {}
            tid = ct.get("id")
            if pid and tid:
                tid_str = str(tid)
                pid_str = str(pid)
                player_team[pid_str] = tid_str
                team_players.setdefault(tid_str, []).append(p)

    # Load matches
    matches_data = _pandascore_matches(base)
    if not matches_data:
        return []

    records: list[dict[str, Any]] = []
    for m in matches_data:
        if m.get("status") != "finished":
            continue
        mid = m.get("id")
        if not mid:
            continue

        winner_id = m.get("winner_id")
        opponents = m.get("opponents") or []
        if len(opponents) < 2:
            continue

        begin_at = m.get("begin_at") or m.get("scheduled_at") or ""
        game_date = begin_at[:10] or None
        results = m.get("results") or []

        # Number of maps played
        games = m.get("games") or []
        maps_played = len([g for g in games if g.get("finished")])

        for i, opp_wrapper in enumerate(opponents[:2]):
            opp = opp_wrapper.get("opponent", {})
            tid = opp.get("id")
            if not tid:
                continue
            tid_str = str(tid)
            opp_idx = 1 - i
            other_opp = opponents[opp_idx].get("opponent", {}) if opp_idx < len(opponents) else {}

            team_score = None
            opp_score = None
            if len(results) >= 2:
                team_score = _safe_int(results[i].get("score"))
                opp_score = _safe_int(results[opp_idx].get("score"))

            won = tid == winner_id

            # Get players for this team
            roster = team_players.get(tid_str, [])
            if not roster:
                # Create a single team-level record when no roster available
                roster = [{"id": f"team_{tid}", "name": opp.get("name", "")}]

            for p in roster:
                pid = str(p.get("id", ""))
                pname = p.get("name") or p.get("slug") or pid

                records.append({
                    "player_id": pid,
                    "player_name": pname,
                    "team_id": tid_str,
                    "team_name": opp.get("name", ""),
                    "game_id": str(mid),
                    "date": game_date,
                    "opponent_id": _safe_str(other_opp.get("id")),
                    "opponent_name": other_opp.get("name", ""),
                    "kills": int(won),  # 1 for win, 0 for loss (proxy)
                    "deaths": int(not won),
                    "assists": maps_played,
                    "kda": float(team_score or 0) / max(float(opp_score or 1), 1),
                    "damage": team_score,
                    "cs_per_min": None,
                    "gold_per_min": None,
                    "minutes": None,
                    "season": season,
                    "source": "pandascore",
                })
    return records


# ── OpenDota (esports – Dota 2) ─────────────────────────


def _opendota_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Generate standings from OpenDota ``teams.json`` wins/losses/rating."""
    data = _load_json(base / "reference" / "teams.json")
    if data is None:
        data = _load_json(base / "teams.json")
    if not data or not isinstance(data, list):
        return []
    records: list[dict[str, Any]] = []
    for t in data:
        tid = t.get("team_id")
        if not tid:
            continue
        tid_str = str(tid)
        wins = _safe_int(t.get("wins")) or 0
        losses = _safe_int(t.get("losses")) or 0
        total = wins + losses
        records.append({
            "team_id": tid_str,
            "team_name": t.get("name") or t.get("tag") or "",
            "wins": wins,
            "losses": losses,
            "pct": round(wins / total, 3) if total else 0.0,
            "games_played": total,
            "season": season,
            "source": "opendota",
        })
    return records


# ── MLB Stats API ─────────────────────────────────────────


def _mlbstats_teams(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_json(base / "teams.json")
    if not data:
        return []

    records: list[dict[str, Any]] = []
    for team in data.get("teams", []):
        tid = _safe_str(team.get("id"))
        if not tid:
            continue

        division = team.get("division", {}) if isinstance(team.get("division"), dict) else {}
        league = team.get("league", {}) if isinstance(team.get("league"), dict) else {}
        venue = team.get("venue", {}) if isinstance(team.get("venue"), dict) else {}

        records.append({
            "source": "mlbstats",
            "sport": sport,
            "id": tid,
            "name": _safe_str(team.get("name")) or _safe_str(team.get("teamName")) or tid,
            "abbreviation": _safe_str(team.get("abbreviation")) or None,
            "city": _safe_str(team.get("locationName")) or None,
            "division": _safe_str(division.get("name")) or None,
            "league": _safe_str(league.get("name")) or "MLB",
            "venue_name": _safe_str(venue.get("name")) or None,
            "founded_year": _safe_int(team.get("firstYearOfPlay")),
        })

    return records


def _mlbstats_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_json(base / "standings.json")
    if not data:
        return []

    records: list[dict[str, Any]] = []
    for block in data.get("records", []):
        division_name = _safe_str((block.get("division") or {}).get("name")) or None
        league_name = _safe_str((block.get("league") or {}).get("name")) or "MLB"

        for row in block.get("teamRecords", []):
            team = row.get("team", {}) if isinstance(row.get("team"), dict) else {}
            team_id = _safe_str(team.get("id"))
            if not team_id:
                continue

            home_record: str | None = None
            away_record: str | None = None
            last_ten: str | None = None
            for rec_group in row.get("records", []):
                for split in rec_group.get("splitRecords", []):
                    stype = _safe_str(split.get("type")).lower()
                    w = _safe_int(split.get("wins"))
                    l = _safe_int(split.get("losses"))
                    if w is None or l is None:
                        continue
                    value = f"{w}-{l}"
                    if stype == "home":
                        home_record = value
                    elif stype == "away":
                        away_record = value
                    elif stype in ("lastten", "last_10", "last 10"):
                        last_ten = value

            records.append({
                "source": "mlbstats",
                "sport": sport,
                "season": season,
                "team_id": team_id,
                "team_name": _safe_str(team.get("name")) or None,
                "wins": _safe_int(row.get("wins")) or 0,
                "losses": _safe_int(row.get("losses")) or 0,
                "pct": _safe_float(row.get("winningPercentage")),
                "games_played": _safe_int(row.get("gamesPlayed")),
                "rank": _safe_int(row.get("leagueRank")),
                "group": division_name,
                "conference": league_name,
                "division": division_name,
                "division_rank": _safe_int(row.get("divisionRank")),
                "overall_rank": _safe_int(row.get("sportRank")),
                "streak": _safe_str((row.get("streak") or {}).get("streakCode")) or None,
                "last_ten": last_ten,
                "home_record": home_record,
                "away_record": away_record,
            })

    return records


def _mlbstats_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    rosters_dir = base / "rosters"
    if not rosters_dir.is_dir():
        return []

    people_dir = base / "people"
    people_by_id: dict[str, dict[str, Any]] = {}
    if people_dir.is_dir():
        for pf in people_dir.glob("*.json"):
            pdata = _load_json(pf)
            if not pdata:
                continue
            people = pdata.get("people", [])
            if isinstance(people, list) and people:
                person = people[0] if isinstance(people[0], dict) else {}
                pid = _safe_str(person.get("id"))
                if pid:
                    people_by_id[pid] = person

    records: list[dict[str, Any]] = []
    for rf in sorted(rosters_dir.glob("*.json")):
        data = _load_json(rf)
        if not data:
            continue

        team_obj = data.get("team", {}) if isinstance(data.get("team"), dict) else {}
        team_name = _safe_str(team_obj.get("name")) or None
        team_id = _safe_str(team_obj.get("id")) or _safe_str(rf.stem)

        for row in data.get("roster", []):
            person = row.get("person", {}) if isinstance(row.get("person"), dict) else {}
            pid = _safe_str(person.get("id"))
            if not pid:
                continue

            profile = people_by_id.get(pid, {})
            status_raw = _safe_str((row.get("status") or {}).get("code")).upper()
            status = "injured" if status_raw in {"IL", "INJ"} else "active"
            if _safe_str(profile.get("active")).lower() == "false":
                status = "inactive"

            birth_city = _safe_str(profile.get("birthCity"))
            birth_state = _safe_str(profile.get("birthStateProvince"))
            birth_country = _safe_str(profile.get("birthCountry"))
            birth_parts = [p for p in (birth_city, birth_state, birth_country) if p]

            records.append({
                "source": "mlbstats",
                "sport": sport,
                "id": pid,
                "name": _safe_str(person.get("fullName")) or _safe_str(profile.get("fullName")) or pid,
                "team_id": team_id or None,
                "team_name": team_name,
                "position": _safe_str((row.get("position") or {}).get("abbreviation")) or None,
                "jersey_number": _safe_int(row.get("jerseyNumber")),
                "height": _safe_str(profile.get("height")) or None,
                "weight": _safe_int(profile.get("weight")),
                "birth_date": _safe_str(profile.get("birthDate")) or None,
                "birth_place": ", ".join(birth_parts) if birth_parts else None,
                "nationality": birth_country or None,
                "age": _safe_int(profile.get("currentAge")),
                "status": status,
            })

    return records


def _mlbstats_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load per-game boxscores from the MLB Stats API data."""

    # Build gamePk → date lookup from schedule.json
    schedule_data = _load_json(base / "schedule.json")
    date_lookup: dict[str, str] = {}
    if schedule_data:
        for date_entry in schedule_data.get("dates", []):
            d = date_entry.get("date", "")
            for sg in date_entry.get("games", []):
                gpk = sg.get("gamePk")
                if gpk is not None:
                    date_lookup[str(gpk)] = d

    games_dir = base / "games"
    if not games_dir.is_dir():
        return []

    records: list[dict[str, Any]] = []
    for gf in sorted(games_dir.glob("*.json")):
        data = _load_json(gf)
        if not data:
            continue

        game_pk = str(data.get("gamePk", ""))
        if not game_pk:
            continue

        boxscore = data.get("boxscore", {})
        linescore = data.get("linescore", {})
        teams_box = boxscore.get("teams", {})
        home_box = teams_box.get("home", {})
        away_box = teams_box.get("away", {})

        home_team_name = home_box.get("team", {}).get("name", "")
        away_team_name = away_box.get("team", {}).get("name", "")

        home_stats = home_box.get("teamStats", {})
        away_stats = away_box.get("teamStats", {})
        h_bat = home_stats.get("batting", {})
        a_bat = away_stats.get("batting", {})
        h_pit = home_stats.get("pitching", {})
        a_pit = away_stats.get("pitching", {})
        h_fld = home_stats.get("fielding", {})
        a_fld = away_stats.get("fielding", {})

        ls_teams = linescore.get("teams", {})
        ls_home = ls_teams.get("home", {})
        ls_away = ls_teams.get("away", {})

        # Determine status from linescore
        innings = linescore.get("innings", [])
        is_winner_set = ls_home.get("isWinner") is not None or ls_away.get("isWinner") is not None
        status = "final" if (is_winner_set or len(innings) >= 9) else "scheduled"

        # Date from schedule lookup
        game_date = date_lookup.get(game_pk, "")

        rec: dict[str, Any] = {
            "source": "mlbstats",
            "id": game_pk,
            "sport": sport,
            "season": season,
            "date": game_date or None,
            "status": status,
            "home_team": home_team_name,
            "away_team": away_team_name,
            "home_score": _safe_int(ls_home.get("runs")),
            "away_score": _safe_int(ls_away.get("runs")),
            # Batting stats
            "home_hits": _safe_int(h_bat.get("hits")),
            "away_hits": _safe_int(a_bat.get("hits")),
            "home_runs_scored": _safe_int(h_bat.get("runs")),
            "away_runs_scored": _safe_int(a_bat.get("runs")),
            "home_home_runs": _safe_int(h_bat.get("homeRuns")),
            "away_home_runs": _safe_int(a_bat.get("homeRuns")),
            "home_at_bats": _safe_int(h_bat.get("atBats")),
            "away_at_bats": _safe_int(a_bat.get("atBats")),
            "home_walks": _safe_int(h_bat.get("baseOnBalls")),
            "away_walks": _safe_int(a_bat.get("baseOnBalls")),
            "home_strikeouts": _safe_int(h_bat.get("strikeOuts")),
            "away_strikeouts": _safe_int(a_bat.get("strikeOuts")),
            "home_stolen_bases": _safe_int(h_bat.get("stolenBases")),
            "away_stolen_bases": _safe_int(a_bat.get("stolenBases")),
            "home_caught_stealing": _safe_int(h_bat.get("caughtStealing")),
            "away_caught_stealing": _safe_int(a_bat.get("caughtStealing")),
            "home_plate_appearances": _safe_int(h_bat.get("plateAppearances")),
            "away_plate_appearances": _safe_int(a_bat.get("plateAppearances")),
            "home_rbi": _safe_int(h_bat.get("rbi")),
            "away_rbi": _safe_int(a_bat.get("rbi")),
            "home_left_on_base": _safe_int(h_bat.get("leftOnBase")),
            "away_left_on_base": _safe_int(a_bat.get("leftOnBase")),
            "home_sac_flies": _safe_int(h_bat.get("sacFlies")),
            "away_sac_flies": _safe_int(a_bat.get("sacFlies")),
            "home_sac_bunts": _safe_int(h_bat.get("sacBunts")),
            "away_sac_bunts": _safe_int(a_bat.get("sacBunts")),
            "home_total_bases": _safe_int(h_bat.get("totalBases")),
            "away_total_bases": _safe_int(a_bat.get("totalBases")),
            "home_ground_into_double_play": _safe_int(h_bat.get("groundIntoDoublePlay")),
            "away_ground_into_double_play": _safe_int(a_bat.get("groundIntoDoublePlay")),
            "home_doubles": _safe_int(h_bat.get("doubles")),
            "away_doubles": _safe_int(a_bat.get("doubles")),
            "home_triples": _safe_int(h_bat.get("triples")),
            "away_triples": _safe_int(a_bat.get("triples")),
            # Pitching stats
            "home_earned_runs": _safe_int(h_pit.get("earnedRuns")),
            "away_earned_runs": _safe_int(a_pit.get("earnedRuns")),
            "home_innings_pitched": _safe_float(h_pit.get("inningsPitched")),
            "away_innings_pitched": _safe_float(a_pit.get("inningsPitched")),
            "home_pitches_thrown": _safe_int(h_pit.get("numberOfPitches")),
            "away_pitches_thrown": _safe_int(a_pit.get("numberOfPitches")),
            "home_pitching_strikeouts": _safe_int(h_pit.get("strikeOuts")),
            "away_pitching_strikeouts": _safe_int(a_pit.get("strikeOuts")),
            "home_pitching_walks": _safe_int(h_pit.get("baseOnBalls")),
            "away_pitching_walks": _safe_int(a_pit.get("baseOnBalls")),
            "home_pitching_home_runs": _safe_int(h_pit.get("homeRuns")),
            "away_pitching_home_runs": _safe_int(a_pit.get("homeRuns")),
            "home_whip": _safe_float(h_pit.get("whip")),
            "away_whip": _safe_float(a_pit.get("whip")),
            "home_era": _safe_float(h_pit.get("era")),
            "away_era": _safe_float(a_pit.get("era")),
            "home_pitching_hit_batsmen": _safe_int(h_pit.get("hitBatsmen")),
            "away_pitching_hit_batsmen": _safe_int(a_pit.get("hitBatsmen")),
            "home_wild_pitches": _safe_int(h_pit.get("wildPitches")),
            "away_wild_pitches": _safe_int(a_pit.get("wildPitches")),
            "home_batters_faced": _safe_int(h_pit.get("battersFaced")),
            "away_batters_faced": _safe_int(a_pit.get("battersFaced")),
            "home_ground_outs": _safe_int(h_pit.get("groundOuts")),
            "away_ground_outs": _safe_int(a_pit.get("groundOuts")),
            "home_fly_outs": _safe_int(h_pit.get("flyOuts")),
            "away_fly_outs": _safe_int(a_pit.get("flyOuts")),
            # Fielding stats
            "home_errors": _safe_int(h_fld.get("errors")),
            "away_errors": _safe_int(a_fld.get("errors")),
        }

        # Inning scores from linescore
        for inn in innings:
            num = inn.get("num")
            if num is None:
                continue
            if 1 <= num <= 9:
                h_inn = inn.get("home", {})
                a_inn = inn.get("away", {})
                rec[f"home_i{num}"] = _safe_int(h_inn.get("runs"))
                rec[f"away_i{num}"] = _safe_int(a_inn.get("runs"))

        records.append(rec)

    return records


def _mlbstats_player_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Extract per-player game stats from MLB Stats API boxscore payloads."""

    games_dir = base / "games"
    if not games_dir.is_dir():
        return []

    records: list[dict[str, Any]] = []
    for gf in sorted(games_dir.glob("*.json")):
        data = _load_json(gf)
        if not data:
            continue

        game_pk = str(data.get("gamePk", ""))
        if not game_pk:
            continue

        boxscore = data.get("boxscore", {})
        teams = boxscore.get("teams", {})
        for side in ("home", "away"):
            team_box = teams.get(side, {})
            team_info = team_box.get("team", {})
            team_id = _safe_str(team_info.get("id"))
            players = team_box.get("players", {})
            if not isinstance(players, dict):
                continue

            for player in players.values():
                person = player.get("person", {})
                player_id = _safe_str(person.get("id"))
                if not player_id:
                    continue

                position = player.get("position", {})
                position_abbr = _safe_str(position.get("abbreviation"))

                stats = player.get("stats", {})
                batting = stats.get("batting", {}) if isinstance(stats, dict) else {}
                pitching = stats.get("pitching", {}) if isinstance(stats, dict) else {}

                if not batting and not pitching:
                    continue

                rec: dict[str, Any] = {
                    "source": "mlbstats",
                    "game_id": game_pk,
                    "player_id": player_id,
                    "team_id": team_id,
                    "season": season,
                    "category": "baseball",
                    "position": position_abbr,
                }

                if batting:
                    rec.update({
                        "ab": _safe_int(batting.get("atBats")),
                        "hits": _safe_int(batting.get("hits")),
                        "hr": _safe_int(batting.get("homeRuns")),
                        "rbi": _safe_int(batting.get("rbi")),
                        "sb": _safe_int(batting.get("stolenBases")),
                        "runs": _safe_int(batting.get("runs")),
                        "bb": _safe_int(batting.get("baseOnBalls")),
                        "so": _safe_int(batting.get("strikeOuts")),
                        "doubles": _safe_int(batting.get("doubles")),
                        "triples": _safe_int(batting.get("triples")),
                        "pa": _safe_int(batting.get("plateAppearances")),
                        "cs": _safe_int(batting.get("caughtStealing")),
                        "hbp": _safe_int(batting.get("hitByPitch")),
                        "sac_flies": _safe_int(batting.get("sacFlies")),
                        "sac_bunts": _safe_int(batting.get("sacBunts")),
                        "lob": _safe_int(batting.get("leftOnBase")),
                        "total_bases": _safe_int(batting.get("totalBases")),
                        "gidp": _safe_int(batting.get("groundIntoDoublePlay")),
                    })

                if pitching:
                    rec.update({
                        "era": _safe_float(pitching.get("era")),
                        "strikeouts": _safe_int(pitching.get("strikeOuts")),
                        "walks": _safe_int(pitching.get("baseOnBalls")),
                        "innings": _safe_float(pitching.get("inningsPitched")),
                        "earned_runs": _safe_int(pitching.get("earnedRuns")),
                        "whip": _safe_float(pitching.get("whip")),
                        "holds": _safe_int(pitching.get("holds")),
                        "blown_saves": _safe_int(pitching.get("blownSaves")),
                        "pitches": _safe_int(pitching.get("numberOfPitches")),
                        "batters_faced": _safe_int(pitching.get("battersFaced")),
                        "wild_pitches": _safe_int(pitching.get("wildPitches")),
                    })

                    wins = _safe_int(pitching.get("wins"))
                    losses = _safe_int(pitching.get("losses"))
                    saves = _safe_int(pitching.get("saves"))
                    rec["win"] = True if wins and wins > 0 else None
                    rec["loss"] = True if losses and losses > 0 else None
                    rec["save"] = True if saves and saves > 0 else None

                records.append(rec)

    return records


def _mlbstats_team_game_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    games_dir = base / "games"
    if not games_dir.is_dir():
        return []

    records: list[dict[str, Any]] = []
    for gf in sorted(games_dir.glob("*.json")):
        data = _load_json(gf)
        if not data:
            continue

        game_id = _safe_str(data.get("gamePk"))
        if not game_id:
            continue

        boxscore = data.get("boxscore", {})
        linescore = data.get("linescore", {})
        teams = boxscore.get("teams", {}) if isinstance(boxscore, dict) else {}
        ls_teams = linescore.get("teams", {}) if isinstance(linescore, dict) else {}

        for side in ("home", "away"):
            other = "away" if side == "home" else "home"
            team_box = teams.get(side, {}) if isinstance(teams, dict) else {}
            opp_box = teams.get(other, {}) if isinstance(teams, dict) else {}

            team_id = _safe_str((team_box.get("team") or {}).get("id"))
            if not team_id:
                continue

            opponent_id = _safe_str((opp_box.get("team") or {}).get("id")) or None
            team_stats = team_box.get("teamStats", {}) if isinstance(team_box, dict) else {}
            batting = team_stats.get("batting", {}) if isinstance(team_stats, dict) else {}
            pitching = team_stats.get("pitching", {}) if isinstance(team_stats, dict) else {}
            fielding = team_stats.get("fielding", {}) if isinstance(team_stats, dict) else {}
            ls_side = ls_teams.get(side, {}) if isinstance(ls_teams, dict) else {}

            records.append({
                "source": "mlbstats",
                "sport": sport,
                "season": season,
                "game_id": game_id,
                "team_id": team_id,
                "opponent_id": opponent_id,
                "is_home": side == "home",
                "runs": _safe_int(ls_side.get("runs")) if isinstance(ls_side, dict) else _safe_int(batting.get("runs")),
                "hits": _safe_int(batting.get("hits")),
                "errors": _safe_int(fielding.get("errors")),
                "at_bats": _safe_int(batting.get("atBats")),
                "home_runs": _safe_int(batting.get("homeRuns")),
                "walks": _safe_int(batting.get("baseOnBalls")),
                "strikeouts": _safe_int(batting.get("strikeOuts")),
                "stolen_bases": _safe_int(batting.get("stolenBases")),
                "innings_pitched": _safe_float(pitching.get("inningsPitched")),
                "earned_runs": _safe_int(pitching.get("earnedRuns")),
                "pitches": _safe_int(pitching.get("numberOfPitches")),
                "whip": _safe_float(pitching.get("whip")),
                "era": _safe_float(pitching.get("era")),
            })

    return records


def _mlbstats_batter_game_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    games_dir = base / "games"
    if not games_dir.is_dir():
        return []

    records: list[dict[str, Any]] = []
    for gf in sorted(games_dir.glob("*.json")):
        data = _load_json(gf)
        if not data:
            continue

        game_id = _safe_str(data.get("gamePk"))
        if not game_id:
            continue

        teams = (data.get("boxscore") or {}).get("teams", {})
        for side in ("home", "away"):
            other = "away" if side == "home" else "home"
            team_box = teams.get(side, {}) if isinstance(teams, dict) else {}
            opp_box = teams.get(other, {}) if isinstance(teams, dict) else {}
            team_id = _safe_str((team_box.get("team") or {}).get("id")) or None
            opponent_id = _safe_str((opp_box.get("team") or {}).get("id")) or None
            players = team_box.get("players", {}) if isinstance(team_box, dict) else {}
            if not isinstance(players, dict):
                continue

            for player in players.values():
                person = player.get("person", {}) if isinstance(player, dict) else {}
                player_id = _safe_str(person.get("id"))
                if not player_id:
                    continue
                stats = player.get("stats", {}) if isinstance(player, dict) else {}
                batting = stats.get("batting", {}) if isinstance(stats, dict) else {}
                if not batting:
                    continue

                records.append({
                    "source": "mlbstats",
                    "sport": sport,
                    "season": season,
                    "game_id": game_id,
                    "player_id": player_id,
                    "team_id": team_id,
                    "opponent_id": opponent_id,
                    "is_home": side == "home",
                    "position": _safe_str((player.get("position") or {}).get("abbreviation")) or None,
                    "ab": _safe_int(batting.get("atBats")),
                    "hits": _safe_int(batting.get("hits")),
                    "hr": _safe_int(batting.get("homeRuns")),
                    "rbi": _safe_int(batting.get("rbi")),
                    "runs": _safe_int(batting.get("runs")),
                    "bb": _safe_int(batting.get("baseOnBalls")),
                    "so": _safe_int(batting.get("strikeOuts")),
                    "sb": _safe_int(batting.get("stolenBases")),
                    "doubles": _safe_int(batting.get("doubles")),
                    "triples": _safe_int(batting.get("triples")),
                    "hbp": _safe_int(batting.get("hitByPitch")),
                    "pa": _safe_int(batting.get("plateAppearances")),
                    "total_bases": _safe_int(batting.get("totalBases")),
                })

    return records


def _mlbstats_pitcher_game_stats(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    games_dir = base / "games"
    if not games_dir.is_dir():
        return []

    records: list[dict[str, Any]] = []
    for gf in sorted(games_dir.glob("*.json")):
        data = _load_json(gf)
        if not data:
            continue

        game_id = _safe_str(data.get("gamePk"))
        if not game_id:
            continue

        teams = (data.get("boxscore") or {}).get("teams", {})
        for side in ("home", "away"):
            other = "away" if side == "home" else "home"
            team_box = teams.get(side, {}) if isinstance(teams, dict) else {}
            opp_box = teams.get(other, {}) if isinstance(teams, dict) else {}
            team_id = _safe_str((team_box.get("team") or {}).get("id")) or None
            opponent_id = _safe_str((opp_box.get("team") or {}).get("id")) or None
            players = team_box.get("players", {}) if isinstance(team_box, dict) else {}
            if not isinstance(players, dict):
                continue

            for player in players.values():
                person = player.get("person", {}) if isinstance(player, dict) else {}
                player_id = _safe_str(person.get("id"))
                if not player_id:
                    continue
                stats = player.get("stats", {}) if isinstance(player, dict) else {}
                pitching = stats.get("pitching", {}) if isinstance(stats, dict) else {}
                if not pitching:
                    continue

                wins = _safe_int(pitching.get("wins"))
                losses = _safe_int(pitching.get("losses"))
                saves = _safe_int(pitching.get("saves"))

                records.append({
                    "source": "mlbstats",
                    "sport": sport,
                    "season": season,
                    "game_id": game_id,
                    "player_id": player_id,
                    "team_id": team_id,
                    "opponent_id": opponent_id,
                    "is_home": side == "home",
                    "position": _safe_str((player.get("position") or {}).get("abbreviation")) or None,
                    "innings": _safe_float(pitching.get("inningsPitched")),
                    "earned_runs": _safe_int(pitching.get("earnedRuns")),
                    "strikeouts": _safe_int(pitching.get("strikeOuts")),
                    "walks": _safe_int(pitching.get("baseOnBalls")),
                    "home_runs_allowed": _safe_int(pitching.get("homeRuns")),
                    "batters_faced": _safe_int(pitching.get("battersFaced")),
                    "pitches": _safe_int(pitching.get("numberOfPitches")),
                    "whip": _safe_float(pitching.get("whip")),
                    "era": _safe_float(pitching.get("era")),
                    "holds": _safe_int(pitching.get("holds")),
                    "blown_saves": _safe_int(pitching.get("blownSaves")),
                    "wild_pitches": _safe_int(pitching.get("wildPitches")),
                    "win": True if wins and wins > 0 else None,
                    "loss": True if losses and losses > 0 else None,
                    "save": True if saves and saves > 0 else None,
                })

    return records


def _weather_game_records(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load weather records from game-scoped weather raw files.

    Preferred layout:
      {season}/dates/{YYYY-MM-DD}/games/{game_id}.json
    """
    records: list[dict[str, Any]] = []

    for path in sorted((base / "dates").glob("*/games/*.json")):
        payload = _load_json(path)
        if not isinstance(payload, dict):
            continue

        game_id = _safe_str(payload.get("game_id") or payload.get("id") or path.stem)
        if not game_id:
            continue

        records.append({
            "source": _safe_str(payload.get("source")) or "weather",
            "sport": _safe_str(payload.get("sport")) or sport,
            "season": _safe_str(payload.get("season")) or season,
            "game_id": game_id,
            "temp_f": _safe_float(payload.get("temp_f")),
            "wind_mph": _safe_float(payload.get("wind_mph")),
            "wind_direction": _safe_str(payload.get("wind_direction")),
            "humidity_pct": _safe_float(payload.get("humidity_pct")),
            "precipitation": _safe_float(payload.get("precipitation")),
            "condition": _safe_str(payload.get("condition")),
            "dome": bool(payload.get("dome") or False),
        })

    # Legacy fallback: include any JSON under season dir that looks game-scoped.
    if records:
        return records

    for path in sorted(base.glob("**/*.json")):
        if "cities" in path.parts:
            continue
        payload = _load_json(path)
        if not isinstance(payload, dict):
            continue
        if payload.get("game_id") is None and payload.get("id") is None:
            continue

        game_id = _safe_str(payload.get("game_id") or payload.get("id") or path.stem)
        if not game_id:
            continue

        records.append({
            "source": _safe_str(payload.get("source")) or "weather",
            "sport": _safe_str(payload.get("sport")) or sport,
            "season": _safe_str(payload.get("season")) or season,
            "game_id": game_id,
            "temp_f": _safe_float(payload.get("temp_f")),
            "wind_mph": _safe_float(payload.get("wind_mph")),
            "wind_direction": _safe_str(payload.get("wind_direction")),
            "humidity_pct": _safe_float(payload.get("humidity_pct")),
            "precipitation": _safe_float(payload.get("precipitation")),
            "condition": _safe_str(payload.get("condition")),
            "dome": bool(payload.get("dome") or False),
        })

    return records


# ═════════════════════════════════════════════════════════
#  Provider Loader Registry
# ═════════════════════════════════════════════════════════

PROVIDER_LOADERS: dict[tuple[str, str], LoaderFn] = {
    # ESPN
    ("espn", "teams"):        _espn_teams,
    ("espn", "players"):      _espn_players,
    ("espn", "games"):        _espn_games,
    ("espn", "injuries"):     _espn_injuries,
    ("espn", "news"):         _espn_news,
    ("espn", "odds"):         _espn_odds,
    ("espn", "standings"):    _espn_standings,
    ("espn", "team_stats"):   _espn_team_stats,
    ("espn", "transactions"): _espn_transactions,
    ("espn", "player_stats"): _espn_player_stats,
    ("espn", "play_by_play"): _espn_play_by_play,
    ("espn", "drives"):       _espn_drives,
    ("espn", "coaches"):      _espn_coaches,
    ("odds", "odds"):          _odds_collector,
    ("odds", "player_props"):  _odds_collector_player_props,
    # OddsAPI
    ("oddsapi", "odds"):       _oddsapi_odds,
    # SGO
    ("sgo", "odds"):           _sgo_odds,
    # NBA Stats
    ("nbastats", "games"):        _nbastats_games,
    ("nbastats", "play_by_play"): _nbastats_play_by_play,
    ("nbastats", "players"):      _nbastats_players,
    ("nbastats", "team_stats"):   _nbastats_team_stats,
    ("nbastats", "player_stats"): _nbastats_player_stats,
    # NHL API
    ("nhl", "games"):        _nhl_games,
    ("nhl", "standings"):    _nhl_standings,
    ("nhl", "players"):      _nhl_players,
    ("nhl", "player_stats"): _nhl_player_stats,
    ("nhl", "draft"):        _nhl_draft,
    # StatsBomb
    ("statsbomb", "games"):        _statsbomb_games,
    ("statsbomb", "players"):      _statsbomb_players,
    ("statsbomb", "player_stats"): _statsbomb_player_stats,
    # Ergast
    ("ergast", "players"):       _ergast_players,
    ("ergast", "teams"):         _ergast_teams,
    ("ergast", "games"):         _ergast_games,
    ("ergast", "standings"):     _ergast_standings,
    ("ergast", "player_stats"):  _ergast_player_stats,
    # OpenF1
    ("openf1", "players"):      _openf1_players,
    ("openf1", "games"):        _openf1_games,
    ("openf1", "player_stats"): _openf1_player_stats,
    # Lahman
    ("lahman", "games"):        _lahman_games,
    ("lahman", "teams"):        _lahman_teams,
    ("lahman", "players"):      _lahman_players,
    ("lahman", "standings"):    _lahman_standings,
    ("lahman", "player_stats"): _lahman_player_stats,
    # MLB Stats API
    ("mlbstats", "teams"):      _mlbstats_teams,
    ("mlbstats", "standings"):  _mlbstats_standings,
    ("mlbstats", "players"):    _mlbstats_players,
    ("mlbstats", "games"):      _mlbstats_games,
    ("mlbstats", "player_stats"): _mlbstats_player_stats,
    ("mlbstats", "team_game_stats"): _mlbstats_team_game_stats,
    ("mlbstats", "batter_game_stats"): _mlbstats_batter_game_stats,
    ("mlbstats", "pitcher_game_stats"): _mlbstats_pitcher_game_stats,
    # Tennis Abstract
    ("tennisabstract", "games"):        _tennisabstract_games,
    ("tennisabstract", "players"):      _tennisabstract_players,
    ("tennisabstract", "player_stats"): _tennisabstract_player_stats,
    ("tennisabstract", "standings"):    _tennisabstract_standings,
    # UFC Stats
    ("ufcstats", "games"):        _ufcstats_games,
    ("ufcstats", "players"):      _ufcstats_players,
    ("ufcstats", "player_stats"): _ufcstats_player_stats,
    # OpenDota
    ("opendota", "teams"):        _opendota_teams,
    ("opendota", "players"):      _opendota_players,
    ("opendota", "standings"):    _opendota_standings,
    ("opendota", "games"):        _opendota_games,
    ("opendota", "player_stats"): _opendota_player_stats,
    ("opendota", "draft"):        _opendota_draft,
    ("opendota", "draft_picks"):  _opendota_draft,
    # CFBData
    ("cfbdata", "games"):      _cfbdata_games,
    ("cfbdata", "teams"):      _cfbdata_teams,
    ("cfbdata", "players"):    _cfbdata_players,
    ("cfbdata", "standings"):  _cfbdata_standings,
    ("cfbdata", "player_stats"): _cfbdata_player_stats,
    ("cfbdata", "team_stats"): _cfbdata_team_stats,
    ("cfbdata", "odds"):       _cfbdata_odds,
    ("cfbdata", "odds_history"): _cfbdata_odds,
    ("cfbdata", "play_by_play"): _cfbdata_play_by_play,
    ("cfbdata", "drives"):     _cfbdata_drives,
    ("cfbdata", "coaches"):    _cfbdata_coaches,
    ("cfbdata", "draft"):      _cfbdata_draft,
    ("cfbdata", "draft_picks"): _cfbdata_draft,
    ("cfbdata", "draft_positions"): _cfbdata_draft_positions,
    ("cfbdata", "draft_teams"): _cfbdata_draft_teams,
    ("cfbdata", "player_portal"): _cfbdata_player_portal,
    ("cfbdata", "player_returning"): _cfbdata_player_returning,
    ("cfbdata", "player_usage"): _cfbdata_player_usage,
    ("cfbdata", "rankings"):  _cfbdata_rankings,
    ("cfbdata", "records"):   _cfbdata_records,
    ("cfbdata", "recruiting"): _cfbdata_recruiting,
    ("cfbdata", "recruiting_teams"): _cfbdata_recruiting_teams,
    ("cfbdata", "recruiting_groups"): _cfbdata_recruiting_groups,
    ("cfbdata", "talent"):    _cfbdata_talent,
    ("cfbdata", "ratings_sp"): _cfbdata_ratings_sp,
    ("cfbdata", "ratings_sp_conferences"): _cfbdata_ratings_sp_conferences,
    ("cfbdata", "ratings_srs"): _cfbdata_ratings_srs,
    ("cfbdata", "ratings_elo"): _cfbdata_ratings_elo,
    ("cfbdata", "ratings_fpi"): _cfbdata_ratings_fpi,
    ("cfbdata", "ppa_teams"): _cfbdata_ppa_teams,
    ("cfbdata", "ppa_games"): _cfbdata_ppa_games,
    ("cfbdata", "ppa_players_season"): _cfbdata_ppa_players_season,
    ("cfbdata", "plays_stats"): _cfbdata_plays_stats,
    ("cfbdata", "plays_types"): _cfbdata_plays_types,
    ("cfbdata", "plays_stats_types"): _cfbdata_plays_stats_types,
    ("cfbdata", "stats_game_advanced"): _cfbdata_stats_game_advanced,
    ("cfbdata", "stats_game_havoc"): _cfbdata_stats_game_havoc,
    ("cfbdata", "games_teams"): _cfbdata_games_teams,
    ("cfbdata", "games_media"): _cfbdata_games_media,
    ("cfbdata", "conferences"): _cfbdata_conferences,
    ("cfbdata", "metrics_fg_ep"): _cfbdata_metrics_fg_ep,
    ("cfbdata", "venues"):    _cfbdata_venues,
    ("cfbdata", "stats_categories"): _cfbdata_stats_categories,
    ("cfbdata", "wp_pregame"): _cfbdata_wp_pregame,
    ("cfbdata", "teams_ats"): _cfbdata_teams_ats,
    ("cfbdata", "teams_fbs"): _cfbdata_teams_fbs,
    ("cfbdata", "games_players"): _cfbdata_games_players,
    ("cfbdata", "lines"): _cfbdata_lines,
    ("cfbdata", "plays"): _cfbdata_plays,
    ("cfbdata", "roster"): _cfbdata_roster,
    ("cfbdata", "stats_advanced"): _cfbdata_stats_advanced,
    ("cfbdata", "stats_player_season"): _cfbdata_stats_player_season,
    ("cfbdata", "stats_season"): _cfbdata_stats_season,
    ("cfbdata", "info"): _cfbdata_info,
    ("cfbdata", "calendar"): _cfbdata_calendar,
    ("cfbdata", "game_box_advanced"): _cfbdata_game_box_advanced,
    ("cfbdata", "scoreboard"): _cfbdata_scoreboard,
    ("cfbdata", "metrics_wp"): _cfbdata_metrics_wp,
    ("cfbdata", "ppa_players_games"): _cfbdata_ppa_players_games,
    ("cfbdata", "ppa_predicted"): _cfbdata_ppa_predicted,
    # Football-data.org
    ("footballdata", "games"):     _footballdata_games,
    ("footballdata", "standings"): _footballdata_standings,
    ("footballdata", "players"):   _footballdata_players,
    ("footballdata", "teams"):     _footballdata_teams,
    # Understat (soccer xG enrichment)
    ("understat", "games"):        _understat_games,
    ("understat", "player_stats"): _understat_player_stats,
    # PandaScore (esports)
    ("pandascore", "games"):         _pandascore_games,
    ("pandascore", "players"):       _pandascore_players,
    ("pandascore", "teams"):         _pandascore_teams,
    ("pandascore", "standings"):     _pandascore_standings,
    ("pandascore", "player_stats"):  _pandascore_player_stats,
    # Visual Crossing weather
    ("weather", "weather"):          _weather_game_records,
}


# ═════════════════════════════════════════════════════════
#  Normalizer
# ═════════════════════════════════════════════════════════

class Normalizer:
    """Orchestrates raw data → validated Parquet conversion for all data types."""

    def __init__(self) -> None:
        cfg = get_settings()
        self.raw_dir: Path = cfg.raw_dir
        self.out_dir: Path = cfg.normalized_dir

    # ── internal plumbing ─────────────────────────────────

    def _out_path(self, sport: str, data_type: str, season: str) -> Path:
        return self.out_dir / sport / f"{data_type}_{season}.parquet"

    def _resolve_provider_dir(
        self, sport: str, provider: str, season: str,
    ) -> Path | None:
        """Resolve ``data/raw/{provider}/{sport_dir}/{season}``."""
        sport_dir = SPORT_PROVIDER_DIR.get(sport, {}).get(provider)
        if sport_dir is None and provider == "weather":
            sport_dir = sport
        if sport_dir is None:
            return None
        return _provider_base_dir(self.raw_dir, provider, sport_dir, season)

    def _gather_raw(
        self, sport: str, data_type: str, season: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """Load and transform raw data from every relevant provider."""
        providers = providers_for(sport, data_type)
        result: dict[str, list[dict[str, Any]]] = {}
        for provider in providers:
            loader = PROVIDER_LOADERS.get((provider, data_type))
            if loader is None:
                continue
            # Providers that use *start-year* convention for cross-year
            # sports whose folder number is the *start year* of a cross-year
            # season.  The normalizer uses *end-year* convention (season "2025"
            # = 2024-25), so shift back by 1 for these providers.
            # NOTE: ESPN uses end-year convention (folder 2025 = 2024-25
            # season) — same as the normalizer — so ESPN is NOT listed here.
            load_season = season
            _start_year_providers = {
                ("nhl", "nhl"),             # NHL API: folder 2024 = 2024-25 season
                ("understat", "epl"),       # Understat: folder 2024 = 2024-25 season
                ("understat", "laliga"),
                ("understat", "bundesliga"),
                ("understat", "seriea"),
                ("understat", "ligue1"),
            }
            if data_type != "draft" and (provider, sport) in _start_year_providers:
                try:
                    shifted = str(int(season) - 1)
                    shifted_dir = self._resolve_provider_dir(sport, provider, shifted)
                    if shifted_dir is not None and shifted_dir.exists():
                        load_season = shifted
                except (ValueError, TypeError):
                    pass

            base_dir = self._resolve_provider_dir(sport, provider, load_season)
            if base_dir is None or not base_dir.exists():
                logger.debug(
                    "No data dir for %s/%s/%s → %s", provider, sport, season, base_dir,
                )
                continue
            try:
                records = loader(base_dir, sport, season)
                if records:
                    result[provider] = records
                    logger.debug(
                        "Loaded %d %s records from %s/%s/%s",
                        len(records), data_type, provider, sport, season,
                    )
            except Exception:
                logger.exception(
                    "Error loading %s/%s from %s", data_type, sport, provider,
                )
        return result

    def _normalize_generic(
        self,
        sport: str,
        data_type: str,
        season: str,
        schema_cls: type[_Base],
        id_field: str,
    ) -> int:
        """Generic normalize-merge-validate-write cycle."""
        providers = providers_for(sport, data_type)
        raw_by_provider = self._gather_raw(sport, data_type, season)
        if not raw_by_provider:
            logger.debug("No raw data for %s/%s/%s", sport, data_type, season)
            return 0

        merged = _merge_records(raw_by_provider, id_field, providers, sport=sport, data_type=data_type)
        primary_source = providers[0] if providers else "unknown"
        validated = _validate_batch(merged, schema_cls, sport, source=primary_source)
        return _write_parquet(validated, self._out_path(sport, data_type, season))

    def _normalize_untyped(
        self,
        sport: str,
        data_type: str,
        season: str,
        id_field: str,
    ) -> int:
        """Normalize and merge records without schema validation.

        Used for exploratory/auxiliary categories where providers vary by sport.
        """
        providers = providers_for(sport, data_type)
        raw_by_provider = self._gather_raw(sport, data_type, season)
        if not raw_by_provider:
            logger.debug("No raw data for %s/%s/%s", sport, data_type, season)
            return 0

        if providers:
            merged = _merge_records(raw_by_provider, id_field, providers, sport=sport, data_type=data_type)
        else:
            merged = []
            for rows in raw_by_provider.values():
                merged.extend(rows)

        if not merged:
            return 0
        return _write_parquet(merged, self._out_path(sport, data_type, season))

    # ── public per-data-type methods ──────────────────────

    def normalize_games(self, sport: str, season: str) -> int:
        """Normalize games with cross-provider dedup by home_team+date."""
        providers = providers_for(sport, "games")
        raw_by_provider = self._gather_raw(sport, "games", season)
        if not raw_by_provider:
            return 0
        merged = _merge_records(raw_by_provider, "id", providers)
        # For motorsport (F1), dedup by date alone since there's one race per
        # date and provider race names differ (e.g. "Saudi Arabian GP" vs
        # "Saudi Arabia GP").
        _motorsport = sport in ("f1", "formula1", "motogp", "indycar", "nascar")
        _is_soccer = sport in _SOCCER_SPORTS
        # Deduplicate across providers: prefer records with linescores / richer stats
        by_matchup: dict[str, dict[str, Any]] = {}
        for rec in merged:
            ht = (rec.get("home_team") or "").strip().lower()
            dt = (rec.get("date") or "")[:10]
            # For soccer, canonicalize team name for dedup key matching
            if _is_soccer:
                ht = _normalize_soccer_team_name(rec.get("home_team") or "")
            if ht and dt:
                key = dt if _motorsport else f"{ht}|{dt}"
                existing = by_matchup.get(key)
                if existing is None:
                    by_matchup[key] = rec
                else:
                    # Determine which record is "richer" to use as base
                    if _motorsport:
                        _race_fields = ("winner_name", "winner_team", "winner_time",
                                        "pole_position_driver", "fastest_lap_driver",
                                        "round_number")
                        new_ls = sum(1 for k in _race_fields if rec.get(k) is not None)
                        old_ls = sum(1 for k in _race_fields if existing.get(k) is not None)
                    elif _is_soccer:
                        # Soccer: prefer records with team stats (possession, shots, corners)
                        _soccer_rich = ("home_possession", "home_shots", "home_shots_on_target",
                                        "home_corners", "home_fouls", "home_offsides",
                                        "home_yellow_cards", "home_saves")
                        new_ls = sum(1 for k in _soccer_rich if rec.get(k) is not None)
                        old_ls = sum(1 for k in _soccer_rich if existing.get(k) is not None)
                    else:
                        _ls_keys = (("home_p1","home_p2","home_p3") if sport == "nhl"
                                    else ("home_q1","home_q2","home_q3","home_q4"))
                        new_ls = sum(1 for k in _ls_keys if rec.get(k) is not None)
                        old_ls = sum(1 for k in _ls_keys if existing.get(k) is not None)
                    if new_ls > old_ls:
                        # Merge: keep new as base, fill gaps from existing
                        for k, v in existing.items():
                            if rec.get(k) is None and v is not None:
                                rec[k] = v
                        by_matchup[key] = rec
                    else:
                        # Keep existing, merge new fields into it
                        for k, v in rec.items():
                            if existing.get(k) is None and v is not None:
                                existing[k] = v
            else:
                # No matchup key — keep as-is (use id)
                by_matchup[rec.get("id", id(rec))] = rec
        deduped = list(by_matchup.values())

        # Compute derived stats on all merged records (faceoff_pct, shot_pct, etc.)
        for rec in deduped:
            # Ensure season_type is always queryable downstream.
            if not rec.get("season_type"):
                rec["season_type"] = "regular"

            # Preserve richer game-level media when providers use non-canonical names.
            if not rec.get("broadcast"):
                for media_name_key in (
                    "broadcast_network",
                    "tv_network",
                    "channel",
                    "network",
                    "media_name",
                ):
                    media_name = rec.get(media_name_key)
                    if media_name:
                        rec["broadcast"] = media_name
                        break

            if not rec.get("broadcast_url"):
                for media_url_key in (
                    "watch_url",
                    "stream_url",
                    "video_url",
                    "highlights_url",
                    "recap_url",
                    "replay_url",
                ):
                    media_url = rec.get(media_url_key)
                    if media_url:
                        rec["broadcast_url"] = media_url
                        break

            # Keep future/scheduled scores as null (not 0/0) for consistent semantics.
            status = str(rec.get("status") or "").strip().lower()
            if status in {"scheduled", "pre", "preview", "upcoming", "postponed", "canceled", "cancelled"}:
                for score_col in (
                    "home_score",
                    "away_score",
                    "score_home",
                    "score_away",
                    "home_runs",
                    "away_runs",
                ):
                    if score_col in rec:
                        rec[score_col] = None

            _consolidate_stat_aliases(rec, sport)
            _compute_derived_stats(rec, sport)

        # Filter games outside expected season date range for cross-year sports.
        # ESPN scoreboard files can contain next-season scheduled games.
        deduped = _filter_season_date_range(deduped, sport, season)

        primary_source = providers[0] if providers else "unknown"
        validated = _validate_batch(deduped, Game, sport, source=primary_source)
        return _write_parquet(validated, self._out_path(sport, "games", season))

    def normalize_teams(self, sport: str, season: str) -> int:
        return self._normalize_generic(sport, "teams", season, Team, "id")

    def normalize_players(self, sport: str, season: str) -> int:
        providers = providers_for(sport, "players")
        raw_by_provider = self._gather_raw(sport, "players", season)
        if not raw_by_provider:
            return 0
        merged = _merge_records(raw_by_provider, "id", providers)

        # Enrich players with team_name / team_abbreviation from teams data
        teams_path = self._out_path(sport, "teams", season)
        team_by_id: dict[str, dict[str, str]] = {}
        team_by_abbr: dict[str, dict[str, str]] = {}
        if teams_path.exists():
            try:
                import pandas as _pd
                tdf = _pd.read_parquet(teams_path)
                for _, row in tdf.iterrows():
                    tid = str(row.get("id", ""))
                    info = {
                        "team_name": row.get("name") or None,
                        "team_abbreviation": row.get("abbreviation") or None,
                    }
                    if tid:
                        team_by_id[tid] = info
                    abbr = row.get("abbreviation")
                    if abbr:
                        team_by_abbr[str(abbr).upper()] = info
            except Exception:
                pass

        # Common abbreviation aliases across sports (provider → ESPN)
        _ABBR_ALIASES: dict[str, str] = {
            "NOP": "NO", "GSW": "GS", "NYK": "NY", "SAS": "SA",
            "PHO": "PHX", "BRK": "BKN", "UTH": "UTAH", "UTA": "UTAH",
            "WAS": "WSH",
        }

        for rec in merged:
            tid = str(rec.get("team_id") or "")
            abbr = (rec.get("team_abbreviation") or "").upper()
            canon_abbr = _ABBR_ALIASES.get(abbr, abbr)
            # Also try team_id as abbreviation (some providers store abbr in team_id)
            tid_as_abbr = _ABBR_ALIASES.get(tid.upper(), tid.upper()) if tid else ""
            # Try matching by team_id (numeric), then abbreviation, then aliased, then tid-as-abbr
            info = (team_by_id.get(tid)
                    or team_by_abbr.get(abbr)
                    or team_by_abbr.get(canon_abbr)
                    or team_by_abbr.get(tid.upper())
                    or team_by_abbr.get(tid_as_abbr)
                    or {})
            if not rec.get("team_name") and info.get("team_name"):
                rec["team_name"] = info["team_name"]
            if not rec.get("team_abbreviation") and info.get("team_abbreviation"):
                rec["team_abbreviation"] = info["team_abbreviation"]

        primary_source = providers[0] if providers else "unknown"
        validated = _validate_batch(merged, Player, sport, source=primary_source)
        return _write_parquet(validated, self._out_path(sport, "players", season))

    def normalize_standings(self, sport: str, season: str) -> int:
        if sport == "golf":
            return self._normalize_golf_standings(season)
        return self._normalize_generic(sport, "standings", season, Standing, "team_id")

    def _normalize_golf_standings(self, season: str) -> int:
        """Generate golf season standings from aggregated player_stats."""
        ps_path = self._out_path("golf", "player_stats", season)
        if not ps_path.exists():
            return 0
        try:
            ps = pd.read_parquet(ps_path)
        except Exception:
            return 0
        if ps.empty:
            return 0

        # Filter out blank player names
        ps = ps[ps["player_name"].notna() & (ps["player_name"].str.strip() != "")]
        pos = pd.to_numeric(ps.get("position"), errors="coerce")
        earnings = pd.to_numeric(ps.get("earnings"), errors="coerce")
        score_to_par = pd.to_numeric(ps.get("score_to_par"), errors="coerce")

        agg = ps.groupby(["player_id", "player_name"]).agg(
            events=("game_id", "nunique"),
            wins=("position", lambda x: (pd.to_numeric(x, errors="coerce") == 1).sum()),
            top5s=("position", lambda x: (pd.to_numeric(x, errors="coerce") <= 5).sum()),
            top10s=("position", lambda x: (pd.to_numeric(x, errors="coerce") <= 10).sum()),
            top25s=("position", lambda x: (pd.to_numeric(x, errors="coerce") <= 25).sum()),
            cuts_made=("position", lambda x: pd.to_numeric(x, errors="coerce").notna().sum()),
            avg_finish=("position", lambda x: pd.to_numeric(x, errors="coerce").mean()),
            best_finish=("position", lambda x: pd.to_numeric(x, errors="coerce").min()),
        ).reset_index()

        # Rank by: wins desc, then top10s desc, then avg_finish asc
        agg = agg.sort_values(
            ["wins", "top10s", "avg_finish"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
        agg["rank"] = range(1, len(agg) + 1)

        records: list[dict[str, Any]] = []
        for _, row in agg.iterrows():
            records.append({
                "source": "computed",
                "sport": "golf",
                "season": season,
                "team_id": str(row["player_id"]),
                "team_name": str(row["player_name"]),
                "conference": "PGA Tour",
                "division": "",
                "group_name": "Season Rankings",
                "wins": int(row["wins"]),
                "losses": int(row["events"] - row["cuts_made"]),  # missed cuts
                "rank": int(row["rank"]),
                "points": float(row["top10s"]),
                "games_played": int(row["events"]),
                "win_pct": round(float(row["wins"] / row["events"]) if row["events"] > 0 else 0.0, 3),
                "home_record": f'{int(row["top5s"])} top-5',
                "away_record": f'{int(row["top25s"])} top-25',
                "streak": f'Avg {row["avg_finish"]:.1f}',
            })

        validated = _validate_batch(records, Standing, "golf", source="computed")
        return _write_parquet(validated, self._out_path("golf", "standings", season))

    def normalize_odds(self, sport: str, season: str) -> int:
        # Keep one row per game/bookmaker so downstream APIs can show all books.
        count = self._normalize_generic(sport, "odds", season, Odds, "game_id+bookmaker")
        # Promote → normalized_curated and remove the intermediate flat file.
        import shutil
        from normalization.curated_parquet_builder import CuratedParquetBuilder
        try:
            CuratedParquetBuilder().build_sport(sport, [season], categories=["odds"])
            odds_file = self._out_path(sport, "odds", season)
            if odds_file.exists():
                odds_file.unlink()
                sport_dir = odds_file.parent
                if sport_dir.exists() and not any(sport_dir.iterdir()):
                    shutil.rmtree(sport_dir)
        except Exception as _exc:
            logger.warning(
                "CuratedParquetBuilder failed for %s odds — intermediate file retained: %s",
                sport,
                _exc,
            )
        return count

    def normalize_player_props(self, sport: str, season: str) -> int:
        # Preserve per-player/per-market bookmaker rows for each game.
        return self._normalize_generic(
            sport,
            "player_props",
            season,
            PlayerProp,
            "game_id+player_id+market+bookmaker",
        )

    def normalize_injuries(self, sport: str, season: str) -> int:
        return self._normalize_generic(sport, "injuries", season, Injury, "player_id")

    def normalize_news(self, sport: str, season: str) -> int:
        return self._normalize_generic(sport, "news", season, News, "id")

    def normalize_weather(self, sport: str, season: str) -> int:
        return self._normalize_generic(sport, "weather", season, Weather, "game_id")

    def normalize_player_stats(self, sport: str, season: str) -> int:
        category = SPORT_DEFINITIONS.get(sport, {}).get("category")
        schema_cls = CATEGORY_STATS_MAP.get(category)  # type: ignore[arg-type]
        if schema_cls is None:
            logger.warning(
                "No stats schema for sport=%s (category=%s)", sport, category,
            )
            return 0

        providers = providers_for(sport, "player_stats")
        raw_by_provider = self._gather_raw(sport, "player_stats", season)
        if not raw_by_provider:
            logger.debug("No raw data for %s/%s/%s", sport, "player_stats", season)
            return 0

        # Use composite key (game_id+player_id) to avoid merging distinct players.
        merged = _merge_records(raw_by_provider, "game_id+player_id", providers)
        for rec in merged:
            if isinstance(category, str):
                _coalesce_player_stat_aliases(rec, category)

        primary_source = providers[0] if providers else "unknown"
        validated = _validate_batch(merged, schema_cls, sport, source=primary_source)
        return _write_parquet(validated, self._out_path(sport, "player_stats", season))

    def normalize_predictions(self, sport: str, season: str) -> int:
        return self._normalize_generic(sport, "predictions", season, Prediction, "game_id")

    def normalize_team_stats(self, sport: str, season: str) -> int:
        """Normalize team season statistics from ESPN team_stats files."""
        providers = providers_for(sport, "team_stats")
        raw_by_provider = self._gather_raw(sport, "team_stats", season)
        if not raw_by_provider:
            return 0
        # Merge across providers by team_id
        merged = _merge_records(raw_by_provider, "team_id", providers)
        if not merged:
            return 0
        return _write_parquet(merged, self._out_path(sport, "team_stats", season))

    def normalize_coaches(self, sport: str, season: str) -> int:
        count = self._normalize_untyped(sport, "coaches", season, "coach_id")
        if count:
            return count

        # Fallback: synthesize one coach row per team so all sports expose
        # a consistent coaches dataset even when providers omit coach entities.
        team_rows = self._gather_raw(sport, "teams", season)
        if not team_rows:
            return 0

        providers = providers_for(sport, "teams")
        if providers:
            merged_teams = _merge_records(team_rows, "id", providers)
        else:
            merged_teams = []
            for rows in team_rows.values():
                merged_teams.extend(rows)

        synthetic: list[dict[str, Any]] = []
        seen: set[str] = set()
        for t in merged_teams:
            team_id = _safe_str(t.get("id") or t.get("team_id")) or ""
            team_name = _safe_str(t.get("name") or t.get("team_name") or t.get("abbreviation")) or team_id
            if not team_id and not team_name:
                continue

            coach_name = _safe_str(t.get("coach") or t.get("head_coach") or t.get("headCoach")) or "Unknown"
            key_id = team_id or team_name.lower().replace(" ", "_")
            coach_id = f"synthetic|{sport}|{season}|{key_id}"
            if coach_id in seen:
                continue
            seen.add(coach_id)
            synthetic.append({
                "coach_id": coach_id,
                "coach_name": coach_name,
                "team_id": team_id or team_name,
                "team_name": team_name,
                "season": season,
                "sport": sport,
                "source": "synthetic",
            })

        if not synthetic:
            return 0
        return _write_parquet(synthetic, self._out_path(sport, "coaches", season))

    def normalize_draft(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "draft", season, "draft_id")

    def normalize_draft_picks(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "draft_picks", season, "draft_id")

    def normalize_draft_positions(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "draft_positions", season, "draft_position_id")

    def normalize_draft_teams(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "draft_teams", season, "draft_team_id")

    def normalize_drives(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "drives", season, "game_id+drive_id")

    def normalize_odds_history(self, sport: str, season: str) -> int:
        # Keep one row per game/bookmaker snapshot; this includes historical line snapshots.
        return self._normalize_generic(sport, "odds_history", season, Odds, "game_id+bookmaker+timestamp")

    def normalize_player_portal(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "player_portal", season, "record_id")

    def normalize_player_returning(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "player_returning", season, "record_id")

    def normalize_player_usage(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "player_usage", season, "record_id")

    def normalize_rankings(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "rankings", season, "record_id")

    def normalize_records(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "records", season, "record_id")

    def normalize_recruiting(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "recruiting", season, "record_id")

    def normalize_recruiting_teams(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "recruiting_teams", season, "record_id")

    def normalize_recruiting_groups(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "recruiting_groups", season, "record_id")

    def normalize_talent(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "talent", season, "record_id")

    def normalize_ratings_sp(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ratings_sp", season, "record_id")

    def normalize_ratings_sp_conferences(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ratings_sp_conferences", season, "record_id")

    def normalize_ratings_srs(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ratings_srs", season, "record_id")

    def normalize_ratings_elo(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ratings_elo", season, "record_id")

    def normalize_ratings_fpi(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ratings_fpi", season, "record_id")

    def normalize_ppa_teams(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ppa_teams", season, "record_id")

    def normalize_ppa_games(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ppa_games", season, "record_id")

    def normalize_ppa_players_season(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ppa_players_season", season, "record_id")

    def normalize_plays_stats(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "plays_stats", season, "record_id")

    def normalize_plays_types(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "plays_types", season, "record_id")

    def normalize_plays_stats_types(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "plays_stats_types", season, "record_id")

    def normalize_stats_game_advanced(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "stats_game_advanced", season, "record_id")

    def normalize_stats_game_havoc(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "stats_game_havoc", season, "record_id")

    def normalize_games_teams(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "games_teams", season, "record_id")

    def normalize_games_media(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "games_media", season, "record_id")

    def normalize_conferences(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "conferences", season, "record_id")

    def normalize_metrics_fg_ep(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "metrics_fg_ep", season, "record_id")

    def normalize_venues(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "venues", season, "record_id")

    def normalize_stats_categories(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "stats_categories", season, "record_id")

    def normalize_wp_pregame(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "wp_pregame", season, "record_id")

    def normalize_teams_ats(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "teams_ats", season, "record_id")

    def normalize_teams_fbs(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "teams_fbs", season, "record_id")

    def normalize_games_players(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "games_players", season, "record_id")

    def normalize_lines(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "lines", season, "record_id")

    def normalize_plays(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "plays", season, "record_id")

    def normalize_roster(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "roster", season, "record_id")

    def normalize_stats_advanced(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "stats_advanced", season, "record_id")

    def normalize_stats_player_season(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "stats_player_season", season, "record_id")

    def normalize_stats_season(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "stats_season", season, "record_id")

    def normalize_info(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "info", season, "record_id")

    def normalize_calendar(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "calendar", season, "record_id")

    def normalize_game_box_advanced(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "game_box_advanced", season, "record_id")

    def normalize_scoreboard(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "scoreboard", season, "record_id")

    def normalize_metrics_wp(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "metrics_wp", season, "record_id")

    def normalize_ppa_players_games(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ppa_players_games", season, "record_id")

    def normalize_ppa_predicted(self, sport: str, season: str) -> int:
        return self._normalize_untyped(sport, "ppa_predicted", season, "record_id")

    def normalize_transactions(self, sport: str, season: str) -> int:
        """Normalize transaction records from ESPN transaction files."""
        providers = providers_for(sport, "transactions")
        raw_by_provider = self._gather_raw(sport, "transactions", season)
        if not raw_by_provider:
            return 0
        all_records: list[dict[str, Any]] = []
        for records in raw_by_provider.values():
            all_records.extend(records)
        if not all_records:
            return 0
        # Deduplicate by (date, team_id, description)
        seen: set[tuple] = set()
        unique: list[dict[str, Any]] = []
        for r in all_records:
            key = (r.get("date"), r.get("team_id"), r.get("description"))
            if key not in seen:
                seen.add(key)
                unique.append(r)
        return _write_parquet(unique, self._out_path(sport, "transactions", season))

    def normalize_team_game_stats(self, sport: str, season: str) -> int:
        return self._normalize_generic(
            sport,
            "team_game_stats",
            season,
            TeamGameStats,
            "game_id+team_id",
        )

    def normalize_batter_game_stats(self, sport: str, season: str) -> int:
        return self._normalize_generic(
            sport,
            "batter_game_stats",
            season,
            BatterGameStats,
            "game_id+player_id+team_id",
        )

    def normalize_pitcher_game_stats(self, sport: str, season: str) -> int:
        return self._normalize_generic(
            sport,
            "pitcher_game_stats",
            season,
            PitcherGameStats,
            "game_id+player_id+team_id",
        )

    # ── Sport-specific advanced normalization ─────────────

    def normalize_advanced_batting(self, sport: str, season: str) -> int:
        """MLB advanced batting: ISO, BABIP, BB%, K%, wOBA, wRC+.

        Sources: Lahman DB batting table + ESPN player stats.
        """
        if sport != "mlb":
            return 0

        records: list[dict[str, Any]] = []

        # Lahman batting data
        lahman_dir = self._resolve_provider_dir(sport, "lahman", season)
        if lahman_dir and lahman_dir.exists():
            batting_file = lahman_dir / "Batting.csv"
            if not batting_file.exists():
                batting_file = lahman_dir / "batting.csv"
            df = _load_csv(batting_file) if batting_file.exists() else pd.DataFrame()
            if not df.empty:
                # Filter to target season — Lahman has all years in one CSV
                if "yearID" in df.columns:
                    df = df[df["yearID"].astype(str) == str(season)]
                for _, row in df.iterrows():
                    ab = _safe_int(row.get("AB")) or 0
                    h = _safe_int(row.get("H")) or 0
                    bb = _safe_int(row.get("BB")) or 0
                    so = _safe_int(row.get("SO")) or 0
                    hr = _safe_int(row.get("HR")) or 0
                    hbp = _safe_int(row.get("HBP")) or 0
                    sf = _safe_int(row.get("SF")) or 0
                    doubles = _safe_int(row.get("2B")) or 0
                    triples = _safe_int(row.get("3B")) or 0
                    pa = ab + bb + hbp + sf

                    if pa < 1:
                        continue

                    singles = h - doubles - triples - hr
                    slg = (singles + 2 * doubles + 3 * triples + 4 * hr) / ab if ab > 0 else 0
                    avg = h / ab if ab > 0 else 0
                    iso = slg - avg
                    bip = ab - so - hr + sf
                    babip = (h - hr) / bip if bip > 0 else 0
                    bb_pct = bb / pa if pa > 0 else 0
                    k_pct = so / pa if pa > 0 else 0

                    # Simplified wOBA weights (2024 season approximations)
                    woba_num = (
                        0.690 * bb
                        + 0.722 * hbp
                        + 0.880 * singles
                        + 1.242 * doubles
                        + 1.569 * triples
                        + 2.015 * hr
                    )
                    woba = woba_num / pa if pa > 0 else 0

                    # wRC+ requires league context; store raw wOBA, mark wRC+ as None
                    records.append({
                        "sport": sport,
                        "source": "lahman",
                        "player_id": _safe_str(row.get("playerID")) or "",
                        "season": _safe_str(row.get("yearID")) or season,
                        "team_id": _safe_str(row.get("teamID")) or "",
                        "plate_appearances": pa,
                        "iso": round(iso, 3),
                        "babip": round(babip, 3),
                        "bb_pct": round(bb_pct, 3),
                        "k_pct": round(k_pct, 3),
                        "woba": round(woba, 3),
                        "wrc_plus": None,
                    })

        # ESPN enrichment
        espn_dir = self._resolve_provider_dir(sport, "espn", season)
        if espn_dir and espn_dir.exists():
            stats_dir = espn_dir / "player_stats"
            if stats_dir.is_dir():
                for f in stats_dir.glob("*.json"):
                    data = _load_json(f)
                    if not data or not isinstance(data, dict):
                        continue
                    pid = _safe_str(data.get("player_id")) or _safe_str(data.get("id")) or ""
                    stats = data.get("stats", data)
                    if isinstance(stats, dict):
                        existing = next((r for r in records if r["player_id"] == pid), None)
                        if existing and stats.get("wrc_plus") is not None:
                            existing["wrc_plus"] = _safe_float(stats["wrc_plus"])

        # Fallback: derive advanced metrics from normalized MLB player_stats
        # when Lahman rows are missing for the season (e.g., current year).
        if not records:
            ps_path = self._out_path(sport, "player_stats", season)
            if ps_path.exists():
                df = pd.read_parquet(ps_path)
                if not df.empty:
                    needed = [
                        "player_id", "team_id", "ab", "hits", "bb", "so", "hr",
                        "hbp", "sac_flies", "doubles", "triples",
                    ]
                    for col in needed:
                        if col not in df.columns:
                            df[col] = 0

                    grouped = (
                        df.groupby(["player_id", "team_id"], dropna=False)[needed[2:]]
                        .sum(min_count=1)
                        .reset_index()
                    )

                    for _, row in grouped.iterrows():
                        ab = _safe_int(row.get("ab")) or 0
                        h = _safe_int(row.get("hits")) or 0
                        bb = _safe_int(row.get("bb")) or 0
                        so = _safe_int(row.get("so")) or 0
                        hr = _safe_int(row.get("hr")) or 0
                        hbp = _safe_int(row.get("hbp")) or 0
                        sf = _safe_int(row.get("sac_flies")) or 0
                        doubles = _safe_int(row.get("doubles")) or 0
                        triples = _safe_int(row.get("triples")) or 0
                        pa = ab + bb + hbp + sf
                        if pa < 1:
                            continue

                        singles = h - doubles - triples - hr
                        slg = (singles + 2 * doubles + 3 * triples + 4 * hr) / ab if ab > 0 else 0
                        avg = h / ab if ab > 0 else 0
                        iso = slg - avg
                        bip = ab - so - hr + sf
                        babip = (h - hr) / bip if bip > 0 else 0
                        bb_pct = bb / pa if pa > 0 else 0
                        k_pct = so / pa if pa > 0 else 0

                        woba_num = (
                            0.690 * bb
                            + 0.722 * hbp
                            + 0.880 * singles
                            + 1.242 * doubles
                            + 1.569 * triples
                            + 2.015 * hr
                        )
                        woba = woba_num / pa if pa > 0 else 0

                        records.append({
                            "sport": sport,
                            "source": "mlbstats",
                            "player_id": _safe_str(row.get("player_id")) or "",
                            "season": season,
                            "team_id": _safe_str(row.get("team_id")) or "",
                            "plate_appearances": pa,
                            "iso": round(iso, 3),
                            "babip": round(babip, 3),
                            "bb_pct": round(bb_pct, 3),
                            "k_pct": round(k_pct, 3),
                            "woba": round(woba, 3),
                            "wrc_plus": None,
                        })

        if not records:
            return 0

        dest = self._out_path(sport, "advanced_batting", season)
        return _write_parquet(records, dest)

    def normalize_advanced_stats(self, sport: str, season: str) -> int:
        """NBA advanced stats: PER, TS%, usage%, assist ratio, rebound rate.

        Source: nbastats provider data.
        """
        if sport not in ("nba", "wnba"):
            return 0

        records: list[dict[str, Any]] = []
        nbastats_dir = self._resolve_provider_dir(sport, "nbastats", season)

        if nbastats_dir and nbastats_dir.exists():
            # Check for advanced stats files
            for pattern in ("advanced*.json", "player_stats/*.json", "players/*.json"):
                for f in nbastats_dir.glob(pattern):
                    data = _load_json(f)
                    if not data:
                        continue
                    rows = data if isinstance(data, list) else data.get("data", data.get("resultSets", []))
                    if isinstance(rows, dict):
                        rows = [rows]
                    if not isinstance(rows, list):
                        continue

                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        pid = _safe_str(
                            row.get("PLAYER_ID")
                            or row.get("player_id")
                            or row.get("id")
                        )
                        if not pid:
                            continue

                        gp = _safe_int(row.get("GP") or row.get("games_played")) or 0
                        mins = _safe_float(row.get("MIN") or row.get("minutes")) or 0

                        pts = _safe_float(row.get("PTS") or row.get("points")) or 0
                        fga = _safe_float(row.get("FGA") or row.get("field_goals_attempted")) or 0
                        fta = _safe_float(row.get("FTA") or row.get("free_throws_attempted")) or 0
                        ast = _safe_float(row.get("AST") or row.get("assists")) or 0
                        reb = _safe_float(row.get("REB") or row.get("rebounds")) or 0
                        tov = _safe_float(row.get("TOV") or row.get("turnovers")) or 0

                        ts_denom = 2 * (fga + 0.44 * fta)
                        ts_pct = pts / ts_denom if ts_denom > 0 else None
                        possessions = fga + 0.44 * fta + tov
                        usg_pct = possessions / (mins * 5 / 48) if mins > 0 else None
                        ast_ratio = ast / possessions if possessions > 0 else None

                        per = _safe_float(row.get("PER") or row.get("per"))
                        reb_rate = _safe_float(row.get("REB_PCT") or row.get("reb_pct"))

                        records.append({
                            "sport": sport,
                            "source": "nbastats",
                            "player_id": pid,
                            "player_name": _safe_str(
                                row.get("PLAYER_NAME")
                                or row.get("player_name")
                                or row.get("name")
                            ) or "",
                            "season": season,
                            "team_id": _safe_str(
                                row.get("TEAM_ID")
                                or row.get("team_id")
                            ) or "",
                            "games_played": gp,
                            "minutes": mins,
                            "per": round(per, 1) if per is not None else None,
                            "ts_pct": round(ts_pct, 3) if ts_pct is not None else None,
                            "usg_pct": round(usg_pct, 3) if usg_pct is not None else None,
                            "ast_ratio": round(ast_ratio, 3) if ast_ratio is not None else None,
                            "reb_rate": round(reb_rate, 3) if reb_rate is not None else None,
                        })

        if not records:
            return 0

        dest = self._out_path(sport, "advanced_stats", season)
        return _write_parquet(records, dest)

    def normalize_match_events(self, sport: str, season: str) -> int:
        """Soccer match events: goals, assists, cards, substitutions with timestamps.

        Source: StatsBomb event data.
        """
        soccer_sports = {"epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl"}
        if sport not in soccer_sports:
            return 0

        records: list[dict[str, Any]] = []
        statsbomb_dir = self._resolve_provider_dir(sport, "statsbomb", season)

        if statsbomb_dir and statsbomb_dir.exists():
            for pattern in ("events/*.json", "matches/*/events.json", "*.json"):
                for f in statsbomb_dir.glob(pattern):
                    data = _load_json(f)
                    if not data:
                        continue
                    if isinstance(data, list):
                        events = data
                        match_id = f.stem
                    else:
                        events = data.get("events", data.get("data", []))
                        match_id = _safe_str(data.get("match_id")) or f.stem
                    if not isinstance(events, list):
                        continue

                    for evt in events:
                        if not isinstance(evt, dict):
                            continue
                        evt_type = _safe_str(
                            evt.get("type", {}).get("name")
                            if isinstance(evt.get("type"), dict)
                            else evt.get("type")
                        )
                        if not evt_type:
                            continue

                        event_category = None
                        if evt_type.lower() in ("shot", "goal"):
                            is_goal = (
                                evt.get("shot", {}).get("outcome", {}).get("name") == "Goal"
                                if isinstance(evt.get("shot"), dict)
                                else evt_type.lower() == "goal"
                            )
                            event_category = "goal" if is_goal else "shot"
                        elif evt_type.lower() in ("pass",) and evt.get("pass", {}).get("goal_assist"):
                            event_category = "assist"
                        elif "card" in evt_type.lower() or evt_type.lower() in ("foul committed",):
                            card_type = None
                            if isinstance(evt.get("foul_committed"), dict):
                                card = evt["foul_committed"].get("card", {})
                                card_type = card.get("name") if isinstance(card, dict) else None
                            elif isinstance(evt.get("bad_behaviour"), dict):
                                card = evt["bad_behaviour"].get("card", {})
                                card_type = card.get("name") if isinstance(card, dict) else None
                            if card_type:
                                event_category = "yellow_card" if "Yellow" in card_type else "red_card"
                            else:
                                event_category = "foul"
                        elif evt_type.lower() in ("substitution",):
                            event_category = "substitution"

                        if event_category is None:
                            continue

                        player = evt.get("player", {}) if isinstance(evt.get("player"), dict) else {}
                        team = evt.get("team", {}) if isinstance(evt.get("team"), dict) else {}

                        records.append({
                            "sport": sport,
                            "source": "statsbomb",
                            "match_id": match_id,
                            "event_id": _safe_str(evt.get("id")) or "",
                            "event_type": event_category,
                            "minute": _safe_int(evt.get("minute")),
                            "second": _safe_int(evt.get("second")),
                            "player_id": _safe_str(player.get("id")) or "",
                            "player_name": _safe_str(player.get("name")) or "",
                            "team_id": _safe_str(team.get("id")) or "",
                            "team_name": _safe_str(team.get("name")) or "",
                            "season": season,
                        })

        # Also check footballdata provider for match events
        footballdata_dir = self._resolve_provider_dir(sport, "footballdata", season)
        if footballdata_dir and footballdata_dir.exists():
            games_dir = footballdata_dir / "games"
            if games_dir.is_dir():
                for f in games_dir.glob("*.json"):
                    if f.name == "all.json":
                        continue
                    data = _load_json(f)
                    if not data:
                        continue
                    mid = _safe_str(data.get("id")) or f.stem
                    goals = data.get("goals", [])
                    if isinstance(goals, list):
                        for g in goals:
                            if not isinstance(g, dict):
                                continue
                            scorer = g.get("scorer", {}) if isinstance(g.get("scorer"), dict) else {}
                            assist = g.get("assist", {}) if isinstance(g.get("assist"), dict) else {}
                            records.append({
                                "sport": sport,
                                "source": "footballdata",
                                "match_id": mid,
                                "event_id": "",
                                "event_type": "goal",
                                "minute": _safe_int(g.get("minute")),
                                "second": None,
                                "player_id": _safe_str(scorer.get("id")) or "",
                                "player_name": _safe_str(scorer.get("name")) or "",
                                "team_id": "",
                                "team_name": "",
                                "season": season,
                            })
                            if assist.get("name"):
                                records.append({
                                    "sport": sport,
                                    "source": "footballdata",
                                    "match_id": mid,
                                    "event_id": "",
                                    "event_type": "assist",
                                    "minute": _safe_int(g.get("minute")),
                                    "second": None,
                                    "player_id": _safe_str(assist.get("id")) or "",
                                    "player_name": _safe_str(assist.get("name")) or "",
                                    "team_id": "",
                                    "team_name": "",
                                    "season": season,
                                })

        if not records:
            return 0

        dest = self._out_path(sport, "match_events", season)
        return _write_parquet(records, dest)

    def normalize_play_by_play(self, sport: str, season: str) -> int:
        """Normalize provider play-by-play feeds into a unified per-play table."""
        providers = providers_for(sport, "play_by_play")
        raw_by_provider = self._gather_raw(sport, "play_by_play", season)
        if not raw_by_provider:
            return 0

        merged: list[dict[str, Any]] = []
        if providers:
            for provider in providers:
                merged.extend(raw_by_provider.get(provider, []))
        else:
            for rows in raw_by_provider.values():
                merged.extend(rows)

        if not merged:
            return 0

        deduped: dict[str, dict[str, Any]] = {}
        for rec in merged:
            game_id = _safe_str(rec.get("game_id") or rec.get("match_id")) or ""
            play_id = _safe_str(rec.get("play_id") or rec.get("event_id") or rec.get("id"))
            if not play_id:
                play_id = "|".join(
                    [
                        _safe_str(rec.get("clock")) or "",
                        _safe_str(rec.get("period")) or "",
                        _safe_str(rec.get("sequence_number")) or "",
                        (_safe_str(rec.get("description")) or "")[:80],
                    ],
                )
            key = f"{game_id}|{play_id}|{_safe_str(rec.get('source')) or ''}"
            deduped[key] = rec

        if not deduped:
            return 0

        return _write_parquet(list(deduped.values()), self._out_path(sport, "play_by_play", season))

    def normalize_ratings(self, sport: str, season: str) -> int:
        """Ratings normalization is currently disabled."""
        return 0

    def normalize_market_signals(self, sport: str, season: str) -> int:
        """Derive bookmaker line-movement signals keyed by ``game_id``.

        This is an additive enrichment table and does not modify core odds rows.
        """
        raw_by_provider = self._gather_raw(sport, "odds", season)
        if not raw_by_provider:
            return 0

        rows: list[dict[str, Any]] = []
        seq = 0
        for provider, records in raw_by_provider.items():
            for rec in records:
                game_id = str(rec.get("game_id") or "").strip()
                if not game_id:
                    continue
                bookmaker = (
                    str(rec.get("bookmaker") or rec.get("sportsbook") or provider)
                    .strip()
                    .lower()
                )
                if not bookmaker:
                    bookmaker = provider
                rows.append({
                    "game_id": game_id,
                    "bookmaker": bookmaker,
                    "provider": provider,
                    "date": rec.get("date"),
                    "home_team": rec.get("home_team"),
                    "away_team": rec.get("away_team"),
                    "timestamp": rec.get("timestamp"),
                    "h2h_home": _safe_float(rec.get("h2h_home")),
                    "h2h_away": _safe_float(rec.get("h2h_away")),
                    "spread_home": _safe_float(rec.get("spread_home")),
                    "total_line": _safe_float(rec.get("total_line")),
                    "_seq": seq,
                })
                seq += 1

        if not rows:
            return 0

        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

        def _first_valid(series: pd.Series) -> float | None:
            s = series.dropna()
            if s.empty:
                return None
            return float(s.iloc[0])

        def _last_valid(series: pd.Series) -> float | None:
            s = series.dropna()
            if s.empty:
                return None
            return float(s.iloc[-1])

        def _range_valid(series: pd.Series) -> float | None:
            s = series.dropna()
            if s.empty:
                return None
            return float(s.max() - s.min())

        out: list[dict[str, Any]] = []
        grouped = df.groupby(["game_id", "bookmaker"], dropna=False)
        for (game_id, bookmaker), g in grouped:
            g = g.sort_values(["timestamp", "_seq"], na_position="last")

            open_h2h_home = _first_valid(g["h2h_home"])
            close_h2h_home = _last_valid(g["h2h_home"])
            open_h2h_away = _first_valid(g["h2h_away"])
            close_h2h_away = _last_valid(g["h2h_away"])
            open_spread_home = _first_valid(g["spread_home"])
            close_spread_home = _last_valid(g["spread_home"])
            open_total_line = _first_valid(g["total_line"])
            close_total_line = _last_valid(g["total_line"])

            def _delta(a: float | None, b: float | None) -> float | None:
                if a is None or b is None:
                    return None
                return round(b - a, 3)

            h2h_home_move = _delta(open_h2h_home, close_h2h_home)
            h2h_away_move = _delta(open_h2h_away, close_h2h_away)
            spread_home_move = _delta(open_spread_home, close_spread_home)
            total_line_move = _delta(open_total_line, close_total_line)

            h2h_home_range = _range_valid(g["h2h_home"])
            h2h_away_range = _range_valid(g["h2h_away"])
            spread_home_range = _range_valid(g["spread_home"])
            total_line_range = _range_valid(g["total_line"])

            move_components = [
                abs(x) for x in (
                    h2h_home_move,
                    h2h_away_move,
                    spread_home_move,
                    total_line_move,
                ) if x is not None
            ]
            total_abs_move = round(sum(move_components), 3) if move_components else 0.0
            if total_abs_move >= 25:
                regime = "volatile"
            elif total_abs_move >= 5:
                regime = "moving"
            else:
                regime = "stable"

            non_null_ts = g["timestamp"].dropna()
            first_seen = non_null_ts.iloc[0].isoformat() if not non_null_ts.empty else None
            last_seen = non_null_ts.iloc[-1].isoformat() if not non_null_ts.empty else None
            rec_last = g.iloc[-1]

            out.append({
                "game_id": str(game_id),
                "sport": sport,
                "season": season,
                "bookmaker": str(bookmaker),
                "source_count": int(g["provider"].nunique()),
                "observation_count": int(len(g)),
                "first_seen_at": first_seen,
                "last_seen_at": last_seen,
                "date": rec_last.get("date"),
                "home_team": rec_last.get("home_team"),
                "away_team": rec_last.get("away_team"),
                "open_h2h_home": open_h2h_home,
                "close_h2h_home": close_h2h_home,
                "h2h_home_move": h2h_home_move,
                "h2h_home_range": round(h2h_home_range, 3) if h2h_home_range is not None else None,
                "open_h2h_away": open_h2h_away,
                "close_h2h_away": close_h2h_away,
                "h2h_away_move": h2h_away_move,
                "h2h_away_range": round(h2h_away_range, 3) if h2h_away_range is not None else None,
                "open_spread_home": open_spread_home,
                "close_spread_home": close_spread_home,
                "spread_home_move": spread_home_move,
                "spread_home_range": round(spread_home_range, 3) if spread_home_range is not None else None,
                "open_total_line": open_total_line,
                "close_total_line": close_total_line,
                "total_line_move": total_line_move,
                "total_line_range": round(total_line_range, 3) if total_line_range is not None else None,
                "aggregate_abs_move": total_abs_move,
                "market_regime": regime,
            })

        if not out:
            return 0
        validated = _validate_batch(out, MarketSignal, sport, source="derived_market")
        return _write_parquet(validated, self._out_path(sport, "market_signals", season))

    def normalize_schedule_fatigue(self, sport: str, season: str) -> int:
        """Compute team-level rest and congestion features keyed by game/team.

        This is additive enrichment that references existing normalized games.
        """
        games_path = self._out_path(sport, "games", season)
        if not games_path.exists():
            return 0

        try:
            games = pd.read_parquet(games_path)
        except Exception:
            return 0
        if games.empty:
            return 0

        if "id" not in games.columns or ("date" not in games.columns and "start_time" not in games.columns):
            return 0

        ts_source = "start_time" if "start_time" in games.columns else "date"
        games = games.copy()
        games["game_ts"] = pd.to_datetime(games[ts_source], utc=True, errors="coerce")
        if "date" in games.columns:
            games["game_date"] = games["date"].astype(str).str[:10]
        else:
            games["game_date"] = games["game_ts"].dt.strftime("%Y-%m-%d")
        games = games.dropna(subset=["game_ts"])
        if games.empty:
            return 0

        team_games: list[dict[str, Any]] = []
        for _, row in games.iterrows():
            gid = str(row.get("id") or "")
            if not gid:
                continue
            home_team = str(row.get("home_team") or "").strip()
            away_team = str(row.get("away_team") or "").strip()
            home_id = str(row.get("home_team_id") or "").strip()
            away_id = str(row.get("away_team_id") or "").strip()

            home_key = home_id or home_team.lower()
            away_key = away_id or away_team.lower()
            if home_key:
                team_games.append({
                    "game_id": gid,
                    "date": row.get("game_date"),
                    "game_ts": row.get("game_ts"),
                    "team_key": home_key,
                    "team_id": home_id or None,
                    "team_name": home_team or None,
                    "opponent_id": away_id or None,
                    "opponent_name": away_team or None,
                    "is_home": 1,
                })
            if away_key:
                team_games.append({
                    "game_id": gid,
                    "date": row.get("game_date"),
                    "game_ts": row.get("game_ts"),
                    "team_key": away_key,
                    "team_id": away_id or None,
                    "team_name": away_team or None,
                    "opponent_id": home_id or None,
                    "opponent_name": home_team or None,
                    "is_home": 0,
                })

        if not team_games:
            return 0

        tg_df = pd.DataFrame(team_games).sort_values(["team_key", "game_ts", "game_id"])
        out: list[dict[str, Any]] = []

        for _, group in tg_df.groupby("team_key", sort=False):
            prior_ts: list[pd.Timestamp] = []
            prior_sides: list[int] = []

            for _, row in group.iterrows():
                current_ts = row["game_ts"]
                prev_ts = prior_ts[-1] if prior_ts else None
                prev_side = prior_sides[-1] if prior_sides else None

                rest_days: float | None = None
                if prev_ts is not None:
                    rest_days = round((current_ts - prev_ts).total_seconds() / 86400.0, 3)

                w7_start = current_ts - pd.Timedelta(days=7)
                w14_start = current_ts - pd.Timedelta(days=14)
                games_last_7d = sum(1 for ts in prior_ts if w7_start <= ts < current_ts)
                games_last_14d = sum(1 for ts in prior_ts if w14_start <= ts < current_ts)

                home_away_switch = 1 if (prev_side is not None and int(prev_side) != int(row["is_home"])) else 0

                away_streak_before = 0
                for side in reversed(prior_sides):
                    if int(side) == 0:
                        away_streak_before += 1
                    else:
                        break

                home_streak_before = 0
                for side in reversed(prior_sides):
                    if int(side) == 1:
                        home_streak_before += 1
                    else:
                        break

                score = 0.0
                if rest_days is not None:
                    if rest_days < 1.5:
                        score += 1.0
                    elif rest_days < 3.0:
                        score += 0.4
                if games_last_7d >= 3:
                    score += 0.6
                if games_last_14d >= 6:
                    score += 0.6
                if home_away_switch:
                    score += 0.3
                if int(row["is_home"]) == 0:
                    score += 0.2

                fatigue_score = round(score, 3)
                if fatigue_score >= 1.5:
                    fatigue_level = "high"
                elif fatigue_score >= 0.7:
                    fatigue_level = "medium"
                else:
                    fatigue_level = "low"

                out.append({
                    "game_id": row["game_id"],
                    "sport": sport,
                    "season": season,
                    "date": row["date"],
                    "team_id": row["team_id"],
                    "team_name": row["team_name"],
                    "opponent_id": row["opponent_id"],
                    "opponent_name": row["opponent_name"],
                    "is_home": int(row["is_home"]),
                    "rest_days": rest_days,
                    "is_back_to_back": 1 if (rest_days is not None and rest_days < 1.5) else 0,
                    "games_last_7d": int(games_last_7d),
                    "games_last_14d": int(games_last_14d),
                    "home_away_switch": int(home_away_switch),
                    "away_streak_before": int(away_streak_before),
                    "home_streak_before": int(home_streak_before),
                    "fatigue_score": fatigue_score,
                    "fatigue_level": fatigue_level,
                })

                prior_ts.append(current_ts)
                prior_sides.append(int(row["is_home"]))

        if not out:
            return 0

        dedup: dict[str, dict[str, Any]] = {}
        for rec in out:
            team_key = str(rec.get("team_id") or rec.get("team_name") or "")
            key = f"{rec.get('game_id')}|{team_key}"
            dedup[key] = rec

        validated = _validate_batch(list(dedup.values()), ScheduleFatigue, sport, source="derived_schedule")
        return _write_parquet(validated, self._out_path(sport, "schedule_fatigue", season))

    # ── batch orchestration ───────────────────────────────

    _DATA_TYPE_METHODS = {
        "games":            "normalize_games",
        "teams":            "normalize_teams",
        "players":          "normalize_players",
        "standings":        "normalize_standings",
        "player_stats":     "normalize_player_stats",
        "odds":             "normalize_odds",
        "odds_history":     "normalize_odds_history",
        "player_props":     "normalize_player_props",
        "injuries":         "normalize_injuries",
        "news":             "normalize_news",
        "weather":          "normalize_weather",
        "team_stats":       "normalize_team_stats",
        "coaches":          "normalize_coaches",
        "draft":            "normalize_draft",
        "draft_picks":      "normalize_draft_picks",
        "draft_positions":  "normalize_draft_positions",
        "draft_teams":      "normalize_draft_teams",
        "player_portal":    "normalize_player_portal",
        "player_returning": "normalize_player_returning",
        "player_usage":     "normalize_player_usage",
        "rankings":         "normalize_rankings",
        "records":          "normalize_records",
        "recruiting":       "normalize_recruiting",
        "recruiting_teams": "normalize_recruiting_teams",
        "recruiting_groups": "normalize_recruiting_groups",
        "talent":           "normalize_talent",
        "ratings_sp":       "normalize_ratings_sp",
        "ratings_sp_conferences": "normalize_ratings_sp_conferences",
        "ratings_srs":      "normalize_ratings_srs",
        "ratings_elo":      "normalize_ratings_elo",
        "ratings_fpi":      "normalize_ratings_fpi",
        "ppa_teams":        "normalize_ppa_teams",
        "ppa_games":        "normalize_ppa_games",
        "ppa_players_season": "normalize_ppa_players_season",
        "plays_stats":      "normalize_plays_stats",
        "plays_types":      "normalize_plays_types",
        "plays_stats_types": "normalize_plays_stats_types",
        "stats_game_advanced": "normalize_stats_game_advanced",
        "stats_game_havoc": "normalize_stats_game_havoc",
        "games_teams":      "normalize_games_teams",
        "games_media":      "normalize_games_media",
        "conferences":      "normalize_conferences",
        "metrics_fg_ep":    "normalize_metrics_fg_ep",
        "venues":           "normalize_venues",
        "stats_categories": "normalize_stats_categories",
        "wp_pregame":       "normalize_wp_pregame",
        "teams_ats":        "normalize_teams_ats",
        "teams_fbs":        "normalize_teams_fbs",
        "games_players":    "normalize_games_players",
        "lines":            "normalize_lines",
        "plays":            "normalize_plays",
        "roster":           "normalize_roster",
        "stats_advanced":   "normalize_stats_advanced",
        "stats_player_season": "normalize_stats_player_season",
        "stats_season":     "normalize_stats_season",
        "info":             "normalize_info",
        "calendar":         "normalize_calendar",
        "game_box_advanced": "normalize_game_box_advanced",
        "scoreboard":       "normalize_scoreboard",
        "metrics_wp":       "normalize_metrics_wp",
        "ppa_players_games": "normalize_ppa_players_games",
        "ppa_predicted":    "normalize_ppa_predicted",
        "transactions":     "normalize_transactions",
        "team_game_stats":  "normalize_team_game_stats",
        "batter_game_stats": "normalize_batter_game_stats",
        "pitcher_game_stats": "normalize_pitcher_game_stats",
        "advanced_batting": "normalize_advanced_batting",
        "advanced_stats":   "normalize_advanced_stats",
        "match_events":     "normalize_match_events",
        "play_by_play":     "normalize_play_by_play",
        "drives":           "normalize_drives",
        "ratings":          "normalize_ratings",
        "market_signals":   "normalize_market_signals",
        "schedule_fatigue": "normalize_schedule_fatigue",
    }

    # Data types that change daily — used by pipeline for faster daily runs.
    # Must match the endpoints imported by the daily pipeline
    # (games,standings,odds,injuries,news,scoreboard).
    # Scoreboard data merges into games during normalization.
    # Types like player_stats, player_props, team_stats etc. are NOT
    # re-imported daily, so normalizing them just re-processes stale data.
    DAILY_DATA_TYPES = {
        "games", "standings", "odds", "injuries", "news",
        "market_signals", "schedule_fatigue",
    }

    def run_sport(
        self,
        sport: str,
        seasons: Sequence[str],
        data_types: Sequence[str] | None = None,
    ) -> dict[str, int]:
        """Normalize data types for one sport across the given seasons.

        Args:
            data_types: If provided, only normalize these data types.
                        Pass ``DAILY_DATA_TYPES`` for faster daily runs.
        """
        methods = self._DATA_TYPE_METHODS
        if data_types is not None:
            methods = {k: v for k, v in methods.items() if k in data_types}

        # Parallelize data type normalization — each writes to a separate
        # parquet file so there are no shared-state conflicts.
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import shutil
        from normalization.curated_parquet_builder import CuratedParquetBuilder

        def _norm_dtype(dtype: str, method_name: str) -> tuple[str, int]:
            method = getattr(self, method_name)
            count = 0
            for season in seasons:
                written = method(sport, season)
                count += written
            return dtype, count

        totals: dict[str, int] = {}
        try:
            workers = min(len(methods), 4)
            if workers > 1:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = {
                        pool.submit(_norm_dtype, dt, mn): dt
                        for dt, mn in methods.items()
                    }
                    for fut in as_completed(futs):
                        dtype = futs[fut]
                        try:
                            _, count = fut.result()
                        except Exception as exc:
                            logger.exception("%s/%s normalization failed: %s", sport, dtype, exc)
                            totals[dtype] = 0
                            continue
                        totals[dtype] = count
                        if count:
                            logger.info("%s/%s: %d total rows", sport, dtype, count)
            else:
                for dtype, method_name in methods.items():
                    try:
                        _, count = _norm_dtype(dtype, method_name)
                    except Exception as exc:
                        logger.exception("%s/%s normalization failed: %s", sport, dtype, exc)
                        count = 0
                    totals[dtype] = count
                    if count:
                        logger.info("%s/%s: %d total rows", sport, dtype, count)

            # Promote flat normalized parquets → normalized_curated hive layout.
            try:
                CuratedParquetBuilder().build_sport(sport, list(seasons))
            except Exception as _exc:
                logger.warning(
                    "CuratedParquetBuilder failed for %s: %s",
                    sport,
                    _exc,
                )

            # Auto-sync: fingerprint curated parquets and refresh DuckDB views.
            try:
                from normalization.auto_curated_sync import post_normalize_hook
                post_normalize_hook(sport, list(seasons))
            except Exception as _hook_exc:
                logger.warning(
                    "auto_curated_sync hook failed for %s (non-fatal): %s",
                    sport,
                    _hook_exc,
                )

            return totals
        finally:
            # Always remove legacy normalized intermediates for this sport.
            sport_dir = self.out_dir / sport
            if sport_dir.exists():
                try:
                    shutil.rmtree(sport_dir)
                    logger.debug("Cleaned up intermediate normalized dir: %s", sport_dir)
                except Exception as cleanup_exc:
                    logger.warning("Failed cleaning intermediate dir %s: %s", sport_dir, cleanup_exc)

            # Remove deprecated root when empty so operators only see curated storage.
            if self.out_dir.exists() and not any(self.out_dir.iterdir()):
                try:
                    self.out_dir.rmdir()
                    logger.debug("Removed empty deprecated normalized root: %s", self.out_dir)
                except Exception:
                    pass

    def _discover_raw_seasons(self, sport: str) -> list[str]:
        """Discover available raw seasons from provider folders for a sport."""
        season_set: set[str] = set()
        provider_dirs = SPORT_PROVIDER_DIR.get(sport, {})
        for provider, provider_sport_dir in provider_dirs.items():
            root = self.raw_dir / provider / provider_sport_dir
            if not root.is_dir():
                continue
            try:
                for child in root.iterdir():
                    if child.is_dir() and child.name.isdigit() and len(child.name) == 4:
                        season_set.add(child.name)
            except Exception:
                continue
        return sorted(season_set)

    def run_all(
        self,
        sports: Sequence[str] | None = None,
        seasons: Sequence[str] | None = None,
    ) -> dict[str, dict[str, int]]:
        """Normalize all data types for the requested sports and seasons.

        Parameters
        ----------
        sports:
            List of sport keys (e.g. ``["nba", "nfl"]``).
            Defaults to every sport in ``SPORT_DEFINITIONS``.
        seasons:
            Season identifiers to process. If ``None`` or includes ``"all"``,
            seasons are auto-discovered from raw provider folders per sport.
        """
        if sports is None:
            sports = list(SPORT_DEFINITIONS.keys())

        results: dict[str, dict[str, int]] = {}
        for sport in sports:
            if sport not in SPORT_DEFINITIONS:
                logger.warning("Unknown sport %r — skipping", sport)
                continue
            logger.info("═══ Normalizing %s ═══", sport.upper())
            explicit = [] if seasons is None else [str(s) for s in seasons]
            wants_all = not explicit or any(s.lower() == "all" for s in explicit)

            if wants_all:
                discovered = self._discover_raw_seasons(sport)
                manual = [s for s in explicit if s.lower() != "all"]
                seasons_to_run = sorted(set(discovered + manual))
            else:
                seasons_to_run = sorted(set(explicit))

            if not seasons_to_run:
                logger.warning("No seasons discovered for %s — skipping", sport)
                results[sport] = {}
                continue

            results[sport] = self.run_sport(sport, seasons_to_run)

        total_rows = sum(v for sport in results.values() for v in sport.values())
        logger.info(
            "Normalization complete — %d total rows across %d sports",
            total_rows, len(results),
        )
        return results
