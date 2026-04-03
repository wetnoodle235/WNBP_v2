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
FieldPriorityMap: TypeAlias = dict[str, ProviderList]
DataTypeFieldMap: TypeAlias = dict[str, FieldPriorityMap]

# ── Master provider priority ─────────────────────────────

PROVIDER_PRIORITY: dict[str, DataTypeMap] = {
    # ── Basketball ────────────────────────────────────────
    "nba": {
        "games":        ["espn", "nbastats"],
        "teams":        ["espn", "nbastats"],
        "players":      ["nbastats", "espn"],
        "standings":    ["nbastats", "espn"],
        "player_stats": ["nbastats", "espn"],
        "odds":         ["odds", "espn", "oddsapi"],
        "market_signals": ["odds", "espn", "oddsapi"],
        "player_props": ["odds", "oddsapi", "prizepicks"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      [],
        "team_stats":   ["nbastats", "espn"],
        "venues":       ["espn", "nbastats"],
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
        "games":        ["espn"],
        "teams":        ["espn"],
        "players":      ["espn"],
        "standings":    ["espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "espn", "oddsapi"],
        "player_props": ["odds", "oddsapi", "prizepicks"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
        "team_stats":   ["espn"],
        "transactions": ["espn"],
    },
    "ncaaf": {
        "games":                    ["espn", "cfbdata"],
        "teams":                    ["espn", "cfbdata"],
        "players":                  ["espn", "cfbdata"],
        "standings":                ["espn", "cfbdata"],
        "player_stats":             ["espn", "cfbdata"],
        "odds":                     ["odds", "cfbdata", "oddsapi"],
        "player_props":             ["odds", "oddsapi"],
        "injuries":                 ["espn"],
        "news":                     ["espn"],
        "weather":                  ["weather"],
        "team_stats":               ["espn", "cfbdata"],
        "stats":                    ["espn", "cfbdata"],
        "transactions":             ["espn"],
        "market_signals":           ["odds", "cfbdata", "oddsapi"],
        # CFBData-only kinds
        "drives":                   ["cfbdata"],
        "rankings":                 ["cfbdata"],
        "records":                  ["cfbdata"],
        "recruiting":               ["cfbdata"],
        "recruiting_teams":         ["cfbdata"],
        "recruiting_groups":        ["cfbdata"],
        "talent":                   ["cfbdata"],
        "ratings_sp":               ["cfbdata"],
        "ratings_sp_conferences":   ["cfbdata"],
        "ratings_srs":              ["cfbdata"],
        "ratings_elo":              ["cfbdata"],
        "ratings_fpi":              ["cfbdata"],
        "ppa_teams":                ["cfbdata"],
        "ppa_games":                ["cfbdata"],
        "ppa_players_season":       ["cfbdata"],
        "plays_stats":              ["cfbdata"],
        "plays_types":              ["cfbdata"],
        "plays_stats_types":        ["cfbdata"],
        "stats_game_advanced":      ["cfbdata"],
        "stats_game_havoc":         ["cfbdata"],
        "games_teams":              ["cfbdata"],
        "games_players":            ["cfbdata"],
        "games_media":              ["cfbdata"],
        "game_box_advanced":        ["cfbdata"],
        "conferences":              ["cfbdata"],
        "metrics_fg_ep":            ["cfbdata"],
        "metrics_wp":               ["cfbdata"],
        "venues":                   ["cfbdata"],
        "stats_categories":         ["cfbdata"],
        "stats_advanced":           ["cfbdata"],
        "stats_player_season":      ["cfbdata"],
        "stats_season":             ["cfbdata"],
        "wp_pregame":               ["cfbdata"],
        "teams_ats":                ["cfbdata"],
        "teams_fbs":                ["cfbdata"],
        "roster":                   ["cfbdata"],
        "calendar":                 ["cfbdata"],
        "info":                     ["cfbdata"],
        "scoreboard":               ["cfbdata"],
        "plays":                    ["cfbdata"],
        "lines":                    ["cfbdata"],
        "player_portal":            ["cfbdata"],
        "player_returning":         ["cfbdata"],
        "player_usage":             ["cfbdata"],
        "ppa_players_games":        ["cfbdata"],
        "ppa_predicted":            ["cfbdata"],
        "draft_picks":              ["cfbdata"],
        "coaches":                  ["espn", "cfbdata"],
        "odds_history":             ["cfbdata"],
    },

    # ── Baseball ──────────────────────────────────────────
    "mlb": {
        "games":        ["mlbstats", "espn", "lahman"],
        "teams":        ["mlbstats", "espn", "lahman"],
        "players":      ["mlbstats", "lahman", "espn"],
        "standings":    ["mlbstats", "espn", "lahman"],
        "player_stats": ["mlbstats", "lahman", "espn"],
        "team_game_stats": ["mlbstats"],
        "batter_game_stats": ["mlbstats"],
        "pitcher_game_stats": ["mlbstats"],
        "odds":         ["odds", "espn", "oddsapi"],
        "player_props": ["odds", "oddsapi", "prizepicks"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
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
        "games":        ["footballdata", "statsbomb", "espn", "understat"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["statsbomb", "espn", "understat"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "laliga": {
        "games":        ["statsbomb", "espn", "understat"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn", "understat"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "bundesliga": {
        "games":        ["statsbomb", "espn", "understat"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn", "understat"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "seriea": {
        "games":        ["statsbomb", "espn", "understat"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn", "understat"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "ligue1": {
        "games":        ["statsbomb", "espn", "understat"],
        "teams":        ["espn", "statsbomb"],
        "players":      ["statsbomb", "espn"],
        "standings":    ["espn"],
        "player_stats": ["statsbomb", "espn", "understat"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
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
        "weather":      ["weather"],
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
        "weather":      ["weather"],
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
        "weather":      ["weather"],
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
        "weather":      ["weather"],
    },
    "europa": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },

    # ── Additional top-flight leagues ─────────────────────────
    "eredivisie": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "primeiraliga": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },

    # ── Second divisions ──────────────────────────────────────
    "championship": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": ["odds", "oddsapi"],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "bundesliga2": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "serieb": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "ligue2": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     ["espn"],
        "news":         ["espn"],
        "weather":      ["weather"],
    },

    # ── International tournaments ─────────────────────────────
    "worldcup": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     [],
        "news":         ["espn"],
        "weather":      ["weather"],
    },
    "euros": {
        "games":        ["footballdata", "espn"],
        "teams":        ["footballdata", "espn"],
        "players":      ["footballdata", "espn"],
        "standings":    ["footballdata", "espn"],
        "player_stats": ["espn"],
        "odds":         ["odds", "oddsapi"],
        "player_props": [],
        "injuries":     [],
        "news":         ["espn"],
        "weather":      ["weather"],
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
        "weather":      ["weather"],
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
        "weather":      ["weather"],
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
        "weather":      ["weather"],
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
        "weather":      ["weather"],
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
        "player_stats": ["opendota", "pandascore"],
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
        "weather":      ["weather"],
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
        "weather":      ["weather"],
    },
}

# Add broad play-by-play coverage where underlying providers are already configured.
for _sport, _mapping in PROVIDER_PRIORITY.items():
    if "play_by_play" in _mapping:
        continue
    _providers: list[str] = []
    _flat = {p for _lst in _mapping.values() for p in _lst}
    if "nbastats" in _flat:
        _providers.append("nbastats")
    if "cfbdata" in _flat:
        _providers.append("cfbdata")
    if "espn" in _flat:
        _providers.append("espn")
    if _providers:
        _mapping["play_by_play"] = _providers

# Add drives coverage where providers expose drive-level data.
for _sport, _mapping in PROVIDER_PRIORITY.items():
    if "drives" in _mapping:
        continue
    _providers: list[str] = []
    _flat = {p for _lst in _mapping.values() for p in _lst}
    if "cfbdata" in _flat:
        _providers.append("cfbdata")
    if "espn" in _flat:
        _providers.append("espn")
    if _providers:
        _mapping["drives"] = _providers

# Add broad coaches coverage where known providers may contain coach metadata.
for _sport, _mapping in PROVIDER_PRIORITY.items():
    if "coaches" in _mapping:
        continue
    _providers = []
    _flat = {p for _lst in _mapping.values() for p in _lst}
    if "cfbdata" in _flat:
        _providers.append("cfbdata")
    if "espn" in _flat:
        _providers.append("espn")
    if _providers:
        _mapping["coaches"] = _providers

# Draft-like data exists in CFBData/NHL trees and Dota2 OpenDota match drafts.
for _sport, _mapping in PROVIDER_PRIORITY.items():
    if "draft" in _mapping:
        continue
    _providers = []
    _flat = {p for _lst in _mapping.values() for p in _lst}
    if "cfbdata" in _flat:
        _providers.append("cfbdata")
    if "nhl" in _flat:
        _providers.append("nhl")
    if "opendota" in _flat:
        _providers.append("opendota")
    if _providers:
        _mapping["draft"] = _providers

# Draft subtypes for structured reporting: picks / positions / teams.
for _sport, _mapping in PROVIDER_PRIORITY.items():
    _flat = {p for _lst in _mapping.values() for p in _lst}
    if "cfbdata" in _flat:
        _mapping.setdefault("draft_picks", ["cfbdata"])
        _mapping.setdefault("draft_positions", ["cfbdata"])
        _mapping.setdefault("draft_teams", ["cfbdata"])
        continue

    # Fallback for other providers: derive from generic draft data.
    if "draft" in _mapping:
        providers = list(_mapping.get("draft", []))
        _mapping.setdefault("draft_picks", providers)
        _mapping.setdefault("draft_positions", providers)
        _mapping.setdefault("draft_teams", providers)

# Player category enrichments currently sourced from CFBData endpoints.
for _sport, _mapping in PROVIDER_PRIORITY.items():
    _flat = {p for _lst in _mapping.values() for p in _lst}
    if "cfbdata" not in _flat:
        continue
    _mapping.setdefault("player_portal", ["cfbdata"])
    _mapping.setdefault("player_returning", ["cfbdata"])
    _mapping.setdefault("player_usage", ["cfbdata"])

# Odds history defaults to same providers as odds when available.
for _sport, _mapping in PROVIDER_PRIORITY.items():
    if "odds_history" in _mapping:
        continue
    odds_providers = list(_mapping.get("odds", []))
    if odds_providers:
        _mapping["odds_history"] = odds_providers


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
    "team_game_stats",
    "batter_game_stats",
    "pitcher_game_stats",
    "coaches",
    "draft",
    "player_portal",
    "player_returning",
    "player_usage",
    "draft_picks",
    "draft_positions",
    "draft_teams",
    "match_events",
    "play_by_play",
    "drives",
    "odds_history",
    "market_signals",
    "schedule_fatigue",
]


def providers_for(sport: str, data_type: str) -> ProviderList:
    """Return the priority-ordered provider list for *sport* / *data_type*."""
    return PROVIDER_PRIORITY.get(sport, {}).get(data_type, [])


# Field-level overrides for individual labels.
#
# Keys under each data type support:
# - exact field name (e.g. "home_win_probability")
# - prefix wildcard (e.g. "venue_.*")
# - catch-all wildcard "*"
#
# This map does not change existing merge behavior unless callers opt into it
# via ``providers_for_label``.
FIELD_PROVIDER_PRIORITY: dict[str, DataTypeFieldMap] = {
    "ncaaf": {
        "teams": {
            "*": ["cfbdata", "espn"],
            "logo": ["espn", "cfbdata"],
            "color": ["cfbdata", "espn"],
            "venue_.*": ["cfbdata", "espn"],
            "conference": ["cfbdata", "espn"],
        },
        "games": {
            "*": ["cfbdata", "espn"],
            "venue": ["espn", "cfbdata"],
            "broadcast": ["espn", "cfbdata"],
            "temperature": ["weather", "espn", "cfbdata"],
            "wind_speed": ["weather", "espn", "cfbdata"],
        },
        "odds": {
            "*": ["odds", "cfbdata", "oddsapi"],
            "line": ["odds", "oddsapi", "cfbdata"],
            "moneyline": ["odds", "oddsapi", "cfbdata"],
            "spread": ["odds", "oddsapi", "cfbdata"],
        },
        "team_stats": {
            "*": ["cfbdata", "espn"],
            "epa_.*": ["cfbdata", "espn"],
            "ppa_.*": ["cfbdata", "espn"],
        },
    },
}


def providers_for_label(sport: str, data_type: str, label: str) -> ProviderList:
    """Return provider priority for a specific label.

    Resolution order:
    1) exact label match
    2) prefix wildcard matches ending in ``.*``
    3) ``*`` catch-all for the data type
    4) fallback to data-type level priority from ``providers_for``
    """
    sport_map = FIELD_PROVIDER_PRIORITY.get(sport, {})
    dtype_map = sport_map.get(data_type, {})

    exact = dtype_map.get(label)
    if exact:
        return exact

    for pattern, providers in dtype_map.items():
        if not pattern.endswith(".*"):
            continue
        prefix = pattern[:-1]
        if label.startswith(prefix):
            return providers

    wildcard = dtype_map.get("*")
    if wildcard:
        return wildcard

    return providers_for(sport, data_type)


def all_providers(sport: str) -> set[str]:
    """Return the distinct set of providers used by *sport* across all data types."""
    providers: set[str] = set()
    for dt_providers in PROVIDER_PRIORITY.get(sport, {}).values():
        providers.update(dt_providers)
    return providers
