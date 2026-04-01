# ──────────────────────────────────────────────────────────
# V5.0 Backend — Provider Priority Map
# ──────────────────────────────────────────────────────────
#
# For each sport, lists the data providers for every data
# type in order of merge priority (first = highest).
# Provider names match their directory names under data/raw/.
# When merging, non-null fields from lower-priority providers
# are preserved; conflicting fields use the higher-priority value.
# ──────────────────────────────────────────────────────────

from __future__ import annotations

from typing import TypeAlias

ProviderList: TypeAlias = list[str]
DataTypeMap: TypeAlias = dict[str, ProviderList]

# ── Master provider priority ─────────────────────────────

PROVIDER_PRIORITY: dict[str, DataTypeMap] = {
    # ── Basketball ────────────────────────────────────────
    "nba": {
        "games":        ["espn", "nbastats", "fivethirtyeight"],
        "teams":        ["espn", "nbastats"],
        "players":      ["nbastats", "espn"],
        "standings":    ["nbastats", "espn"],
        "player_stats": ["nbastats", "fivethirtyeight", "espn"],
        "odds":         ["odds", "espn", "oddsapi"],
        "player_props": ["odds", "oddsapi", "prizepicks"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      [],
        "team_stats":   ["espn"],
        "transactions": ["espn"],
    },
    "wnba": {
        "games":        ["espn"],
        "teams":        ["espn"],
        "players":      ["nbastats", "espn"],
        "standings":    ["espn"],
        "player_stats": ["nbastats", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      [],
        "team_stats":   ["espn"],
        "transactions": ["espn"],
    },
    "ncaab": {
        "games":        ["espn"],
        "teams":        ["espn"],
        "players":      ["espn"],
        "standings":    ["espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      [],
        "team_stats":   ["espn"],
        "transactions": ["espn"],
    },
    "ncaaw": {
        "games":        ["espn"],
        "teams":        ["espn"],
        "players":      ["espn"],
        "standings":    ["espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      [],
        "team_stats":   ["espn"],
        "transactions": ["espn"],
    },

    # ── Football ──────────────────────────────────────────
    "nfl": {
        "games":        ["espn", "nflfastr", "fivethirtyeight"],
        "teams":        ["espn"],
        "players":      ["nflfastr", "espn"],
        "standings":    ["espn"],
        "player_stats": ["nflfastr", "espn"],
        "odds":         ["odds", "espn", "oddsapi"],
        "player_props": ["odds", "oddsapi", "prizepicks"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
        "team_stats":   ["espn"],
        "transactions": ["espn"],
    },
    "ncaaf": {
        "games":        ["cfbdata", "espn"],
        "teams":        ["cfbdata", "espn"],
        "players":      ["cfbdata", "espn"],
        "standings":    ["cfbdata", "espn"],
        "player_stats": ["cfbdata", "espn"],
        "odds":         ["odds", "cfbdata", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
        "team_stats":   ["cfbdata", "espn"],
        "transactions": ["espn"],
    },

    # ── Baseball ──────────────────────────────────────────
    "mlb": {
        "games":        ["espn", "lahman", "mlbstats"],
        "teams":        ["espn", "lahman"],
        "players":      ["lahman", "espn"],
        "standings":    ["espn"],
        "player_stats": ["mlbstats", "lahman", "espn"],
        "odds":         ["odds", "espn", "oddsapi"],
        "player_props": ["odds", "oddsapi", "prizepicks"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
        "team_stats":   ["espn"],
        "transactions": ["espn"],
    },

    # ── Hockey ────────────────────────────────────────────
    "nhl": {
        "games":        ["nhl", "espn"],
        "teams":        ["espn"],
        "players":      ["nhl", "espn"],
        "standings":    ["nhl", "espn"],
        "player_stats": ["nhl", "espn"],
        "odds":         ["odds", "espn", "oddsapi"],
        "player_props": ["odds", "oddsapi", "prizepicks"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      [],
        "team_stats":   ["espn"],
        "transactions": ["espn"],
    },

    # ── Soccer ────────────────────────────────────────────
    "epl": {
        "games":        ["footballdata", "statsbomb", "espn"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["statsbomb", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "laliga": {
        "games":        ["statsbomb", "espn"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "bundesliga": {
        "games":        ["statsbomb", "espn"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "seriea": {
        "games":        ["statsbomb", "espn"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "ligue1": {
        "games":        ["statsbomb", "espn"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "mls": {
        "games":        ["statsbomb", "espn"],
        "teams":        ["espn"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "ucl": {
        "games":        ["statsbomb", "espn"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "nwsl": {
        "games":        ["statsbomb", "espn"],
        "teams":        ["espn"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "ligamx": {
        "games":        ["espn"],
        "teams":        ["espn"],
        "players":      ["espn"],
        "standings":    ["espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "europa": {
        "games":        ["espn"],
        "teams":        ["espn"],
        "players":      ["espn"],
        "standings":    ["espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },

    # ── Motorsport ────────────────────────────────────────
    "f1": {
        "games":        ["openf1", "ergast", "espn"],
        "teams":        ["ergast", "espn"],
        "players":      ["openf1", "ergast", "espn"],
        "standings":    ["ergast", "espn"],
        "player_stats": ["openf1", "ergast"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     [],
        "news":         [],
        "weather":      ["openweather"],
    },
    "indycar": {
        "games":        ["espn"],
        "teams":        ["espn"],
        "players":      ["espn"],
        "standings":    ["espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     [],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },

    # ── Tennis ────────────────────────────────────────────
    "atp": {
        "games":        ["tennisabstract", "espn"],
        "teams":        [],
        "players":      ["tennisabstract", "espn"],
        "standings":    ["tennisabstract", "espn"],
        "player_stats": ["tennisabstract", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     [],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "wta": {
        "games":        ["tennisabstract", "espn"],
        "teams":        [],
        "players":      ["tennisabstract", "espn"],
        "standings":    ["tennisabstract", "espn"],
        "player_stats": ["tennisabstract", "espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     [],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },

    # ── MMA ───────────────────────────────────────────────
    "ufc": {
        "games":        ["ufcstats"],
        "teams":        [],
        "players":      ["ufcstats"],
        "standings":    [],
        "player_stats": ["ufcstats"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi", "prizepicks"],
        "injuries":     [],
        "news":         [],
        "weather":      [],
    },

    # ── Esports ───────────────────────────────────────────
    "lol": {
        "games":        ["pandascore"],
        "teams":        ["pandascore"],
        "players":      ["pandascore"],
        "standings":    ["pandascore"],
        "player_stats": ["pandascore"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     [],
        "news":         [],
        "weather":      [],
    },
    "csgo": {
        "games":        ["pandascore"],
        "teams":        ["pandascore"],
        "players":      ["pandascore"],
        "standings":    ["pandascore"],
        "player_stats": ["pandascore"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     [],
        "news":         [],
        "weather":      [],
    },
    "dota2": {
        "games":        ["opendota", "pandascore"],
        "teams":        ["pandascore", "opendota"],
        "players":      ["pandascore", "opendota"],
        "standings":    ["pandascore", "opendota"],
        "player_stats": ["opendota"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     [],
        "news":         [],
        "weather":      [],
    },
    "valorant": {
        "games":        ["pandascore"],
        "teams":        ["pandascore"],
        "players":      ["pandascore"],
        "standings":    ["pandascore"],
        "player_stats": ["pandascore"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     [],
        "news":         [],
        "weather":      [],
    },

    # ── Golf ──────────────────────────────────────────────
    "golf": {
        "games":        ["espn"],
        "teams":        [],
        "players":      ["espn"],
        "standings":    ["espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
    "lpga": {
        "games":        ["espn"],
        "teams":        [],
        "players":      ["espn"],
        "standings":    ["espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["openweather"],
    },
}


# ── Helpers ───────────────────────────────────────────────

ALL_DATA_TYPES: list[str] = [
    "games",
    "teams",
    "players",
    "standings",
    "player_stats",
    "odds",
    "player_props",
    "injuries",
    "news",
    "weather",
    "team_stats",
    "transactions",
    "market_signals",
    "schedule_fatigue",
]


def providers_for(sport: str, data_type: str) -> ProviderList:
    """Return the priority-ordered provider list for *sport* / *data_type*."""
    return PROVIDER_PRIORITY.get(sport, {}).get(data_type, [])


def all_providers(sport: str) -> set[str]:
    """Return the distinct set of providers used by *sport* across all data types."""
    providers: set[str] = set()
    for dt_providers in PROVIDER_PRIORITY.get(sport, {}).values():
        providers.update(dt_providers)
    return providers
