# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — Dota 2 Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 12-entity consolidated design.  Raw data from BDL Dota 2 API,
# PandaScore, and OpenDota providers are merged into 12 wide schemas.
#
# Entity overview
# ───────────────
#  1. teams              — static reference, no partitioning
#  2. players            — static reference, no partitioning
#  3. heroes             — static reference, no partitioning
#  4. items              — static reference, no partitioning
#  5. regions            — static reference, no partitioning
#  6. matches            — partition: season=
#  7. match_maps         — partition: season=
#  8. player_stats       — partition: season=
#  9. hero_stats         — partition: season=
# 10. tournaments        — partition: season=
# 11. tournament_teams   — partition: season=
# 12. tournament_rosters — partition: season=
#
# Every schema carries a mandatory ``source`` field for vendor provenance.
# ──────────────────────────────────────────────────────────────────────

from __future__ import annotations

import pyarrow as pa


# ═══════════════════════════════════════════════════════════════════════
# Helper — shorthand for pa.field with metadata description
# ═══════════════════════════════════════════════════════════════════════

def _f(name: str, dtype: pa.DataType, description: str, nullable: bool = True) -> pa.Field:
    """Create a pa.field with an embedded description in its metadata."""
    return pa.field(
        name,
        dtype,
        nullable=nullable,
        metadata={"description": description},
    )


# ═══════════════════════════════════════════════════════════════════════
# 1. teams — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

DOTA_TEAMS_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique team identifier",                   nullable=False),
    _f("name",           pa.string(),  "Team name",                                nullable=False),
    _f("slug",           pa.string(),  "URL-friendly slug"),
    _f("country_name",   pa.string(),  "Country of origin"),
    _f("region",         pa.string(),  "Competitive region"),
    _f("rank",           pa.int32(),   "Current world ranking"),
    _f("total_money",    pa.float64(), "Total prize money earned (USD)"),
    _f("tour_wins",      pa.int32(),   "Total tournament wins"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. players — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

DOTA_PLAYERS_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique player identifier",                 nullable=False),
    _f("nickname",       pa.string(),  "In-game name (IGN)",                       nullable=False),
    _f("slug",           pa.string(),  "URL-friendly slug"),
    _f("first_name",     pa.string(),  "First name"),
    _f("last_name",      pa.string(),  "Last name"),
    _f("birthday",       pa.string(),  "Date of birth (YYYY-MM-DD)"),
    _f("country_name",   pa.string(),  "Country of origin"),
    _f("country_code",   pa.string(),  "ISO country code"),
    _f("is_coach",       pa.string(),  "Whether the player is a coach"),
    _f("total_prize",    pa.float64(), "Total career prize money (USD)"),
    # Team (flattened nested)
    _f("team_id",        pa.int32(),   "Current team identifier"),
    _f("team_name",      pa.string(),  "Current team name"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. heroes — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

DOTA_HEROES_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique hero identifier",                   nullable=False),
    _f("name",           pa.string(),  "Internal hero name",                       nullable=False),
    _f("localized_name", pa.string(),  "Display-friendly hero name"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. items — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

DOTA_ITEMS_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique item identifier",                   nullable=False),
    _f("name",           pa.string(),  "Item name",                                nullable=False),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. regions — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

DOTA_REGIONS_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique region identifier",                 nullable=False),
    _f("name",           pa.string(),  "Region name",                              nullable=False),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. matches — partition: season=
# ═══════════════════════════════════════════════════════════════════════

DOTA_MATCHES_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique match identifier",                  nullable=False),
    _f("slug",           pa.string(),  "URL-friendly slug"),
    # Tournament (flattened nested)
    _f("tournament_id",  pa.int32(),   "Parent tournament identifier"),
    _f("tournament_name", pa.string(), "Parent tournament name"),
    _f("stage",          pa.string(),  "Tournament stage (e.g. playoffs, groups)"),
    # Teams
    _f("team1_id",       pa.int32(),   "Team 1 identifier"),
    _f("team1_name",     pa.string(),  "Team 1 name"),
    _f("team2_id",       pa.int32(),   "Team 2 identifier"),
    _f("team2_name",     pa.string(),  "Team 2 name"),
    # Result
    _f("winner_id",      pa.int32(),   "Winning team identifier"),
    _f("winner_name",    pa.string(),  "Winning team name"),
    _f("team1_score",    pa.int32(),   "Team 1 map score"),
    _f("team2_score",    pa.int32(),   "Team 2 map score"),
    _f("bo_type",        pa.string(),  "Best-of type (bo1, bo3, bo5)"),
    _f("status",         pa.string(),  "Match status"),
    _f("start_date",     pa.string(),  "Start date/time (ISO-8601)"),
    _f("end_date",       pa.string(),  "End date/time (ISO-8601)"),
    # Partition key
    _f("season",         pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. match_maps (games) — partition: season=
# ═══════════════════════════════════════════════════════════════════════

DOTA_MATCH_MAPS_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique match-map (game) identifier",       nullable=False),
    _f("match_id",       pa.int32(),   "Parent match identifier",                  nullable=False),
    _f("game_number",    pa.int32(),   "Game number within the match"),
    # Winner / loser (flattened nested)
    _f("winner_id",      pa.int32(),   "Winning team identifier"),
    _f("winner_name",    pa.string(),  "Winning team name"),
    _f("loser_id",       pa.int32(),   "Losing team identifier"),
    _f("loser_name",     pa.string(),  "Losing team name"),
    # Timing
    _f("duration",       pa.int32(),   "Game duration in seconds"),
    _f("status",         pa.string(),  "Game status"),
    _f("begin_at",       pa.string(),  "Game start time (ISO-8601)"),
    _f("end_at",         pa.string(),  "Game end time (ISO-8601)"),
    # Partition key
    _f("season",         pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. player_stats (aggregated) — partition: season=
# ═══════════════════════════════════════════════════════════════════════

DOTA_PLAYER_STATS_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique stat record identifier",            nullable=False),
    # Player (flattened nested)
    _f("player_id",      pa.int32(),   "Player identifier"),
    _f("player_name",    pa.string(),  "Player name"),
    # Aggregate counts
    _f("maps_played",    pa.int32(),   "Total maps played"),
    _f("total_kills",    pa.int32(),   "Total kills"),
    _f("avg_kills",      pa.float64(), "Average kills per map"),
    _f("total_deaths",   pa.int32(),   "Total deaths"),
    _f("avg_deaths",     pa.float64(), "Average deaths per map"),
    _f("total_assists",  pa.int32(),   "Total assists"),
    _f("avg_assists",    pa.float64(), "Average assists per map"),
    _f("kda",            pa.float64(), "Kill/death/assist ratio"),
    _f("kp",             pa.float64(), "Kill participation percentage"),
    _f("game_impact",    pa.float64(), "Composite game-impact score"),
    _f("avg_net_worth",  pa.float64(), "Average net worth per map"),
    _f("avg_gpm",        pa.float64(), "Average gold per minute"),
    _f("avg_xpm",        pa.float64(), "Average XP per minute"),
    _f("avg_damage",     pa.float64(), "Average hero damage per map"),
    _f("avg_last_hits",  pa.float64(), "Average last hits per map"),
    _f("avg_denies",     pa.float64(), "Average denies per map"),
    _f("avg_heal",       pa.float64(), "Average healing per map"),
    # Partition key
    _f("season",         pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. hero_stats (aggregated) — partition: season=
# ═══════════════════════════════════════════════════════════════════════

DOTA_HERO_STATS_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique stat record identifier",            nullable=False),
    # Hero (flattened nested)
    _f("hero_id",        pa.int32(),   "Hero identifier"),
    _f("hero_name",      pa.string(),  "Hero name"),
    # Pick / ban / win stats
    _f("games_count",    pa.int32(),   "Total games involving this hero"),
    _f("picks_count",    pa.int32(),   "Total times picked"),
    _f("picks_rate",     pa.float64(), "Pick rate (0-1)"),
    _f("wins_count",     pa.int32(),   "Total wins"),
    _f("loses_count",    pa.int32(),   "Total losses"),
    _f("win_rate",       pa.float64(), "Win rate (0-1)"),
    _f("ban_rate",       pa.float64(), "Ban rate (0-1)"),
    # Win rates by game phase
    _f("early_win_rate", pa.float64(), "Win rate in early-game endings"),
    _f("mid_win_rate",   pa.float64(), "Win rate in mid-game endings"),
    _f("late_win_rate",  pa.float64(), "Win rate in late-game endings"),
    # Averages
    _f("avg_kills",      pa.float64(), "Average kills per game"),
    _f("avg_deaths",     pa.float64(), "Average deaths per game"),
    _f("avg_assists",    pa.float64(), "Average assists per game"),
    _f("kda",            pa.float64(), "Kill/death/assist ratio"),
    _f("kp",             pa.float64(), "Kill participation percentage"),
    _f("game_impact",    pa.float64(), "Composite game-impact score"),
    _f("avg_net_worth",  pa.float64(), "Average net worth per game"),
    _f("avg_gpm",        pa.float64(), "Average gold per minute"),
    _f("avg_xpm",        pa.float64(), "Average XP per minute"),
    _f("avg_damage",     pa.float64(), "Average hero damage per game"),
    _f("avg_last_hits",  pa.float64(), "Average last hits per game"),
    _f("avg_denies",     pa.float64(), "Average denies per game"),
    _f("avg_heal",       pa.float64(), "Average healing per game"),
    # Partition key
    _f("season",         pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. tournaments — partition: season=
# ═══════════════════════════════════════════════════════════════════════

DOTA_TOURNAMENTS_SCHEMA = pa.schema([
    _f("id",             pa.int32(),   "Unique tournament identifier",             nullable=False),
    _f("name",           pa.string(),  "Tournament name",                          nullable=False),
    _f("slug",           pa.string(),  "URL-friendly slug"),
    _f("start_date",     pa.string(),  "Start date (YYYY-MM-DD)"),
    _f("end_date",       pa.string(),  "End date (YYYY-MM-DD)"),
    _f("prize",          pa.string(),  "Prize pool description"),
    _f("event_type",     pa.string(),  "Event type (LAN, online)"),
    _f("tier",           pa.string(),  "Tournament tier (e.g. S, A, B)"),
    _f("status",         pa.string(),  "Tournament status"),
    # Partition key
    _f("season",         pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. tournament_teams — partition: season=
# ═══════════════════════════════════════════════════════════════════════

DOTA_TOURNAMENT_TEAMS_SCHEMA = pa.schema([
    _f("tournament_id",  pa.int32(),   "Tournament identifier",                    nullable=False),
    _f("team_id",        pa.int32(),   "Team identifier",                          nullable=False),
    _f("team_name",      pa.string(),  "Team name"),
    # Partition key
    _f("season",         pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. tournament_rosters — partition: season=
# ═══════════════════════════════════════════════════════════════════════

DOTA_TOURNAMENT_ROSTERS_SCHEMA = pa.schema([
    _f("tournament_id",  pa.int32(),   "Tournament identifier",                    nullable=False),
    _f("team_id",        pa.int32(),   "Team identifier",                          nullable=False),
    _f("player_id",      pa.int32(),   "Player identifier"),
    _f("player_name",    pa.string(),  "Player name"),
    # Partition key
    _f("season",         pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Consolidated lookups
# ═══════════════════════════════════════════════════════════════════════

DOTA_SCHEMAS: dict[str, pa.Schema] = {
    "teams":              DOTA_TEAMS_SCHEMA,
    "players":            DOTA_PLAYERS_SCHEMA,
    "heroes":             DOTA_HEROES_SCHEMA,
    "items":              DOTA_ITEMS_SCHEMA,
    "regions":            DOTA_REGIONS_SCHEMA,
    "matches":            DOTA_MATCHES_SCHEMA,
    "match_maps":         DOTA_MATCH_MAPS_SCHEMA,
    "player_stats":       DOTA_PLAYER_STATS_SCHEMA,
    "hero_stats":         DOTA_HERO_STATS_SCHEMA,
    "tournaments":        DOTA_TOURNAMENTS_SCHEMA,
    "tournament_teams":   DOTA_TOURNAMENT_TEAMS_SCHEMA,
    "tournament_rosters": DOTA_TOURNAMENT_ROSTERS_SCHEMA,
}

DOTA_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning
    "teams":              [],
    "players":            [],
    "heroes":             [],
    "items":              [],
    "regions":            [],
    # Season-partitioned
    "matches":            ["season"],
    "match_maps":         ["season"],
    "player_stats":       ["season"],
    "hero_stats":         ["season"],
    "tournaments":        ["season"],
    "tournament_teams":   ["season"],
    "tournament_rosters": ["season"],
}

DOTA_ENTITY_PATHS: dict[str, str] = {
    "teams":              "teams",
    "players":            "players",
    "heroes":             "heroes",
    "items":              "items",
    "regions":            "regions",
    "matches":            "matches",
    "match_maps":         "match_maps",
    "player_stats":       "player_stats",
    "hero_stats":         "hero_stats",
    "tournaments":        "tournaments",
    "tournament_teams":   "tournament_teams",
    "tournament_rosters": "tournament_rosters",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names to the flat entity directory
# name under normalized_curated/dota/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
DOTA_TYPE_TO_ENTITY: dict[str, str | None] = {
    "teams":              "teams",
    "players":            "players",
    "heroes":             "heroes",
    "items":              "items",
    "regions":            "regions",
    "matches":            "matches",
    "match_maps":         "match_maps",
    "player_stats":       "player_stats",
    "hero_stats":         "hero_stats",
    "tournaments":        "tournaments",
    "tournament_teams":   "tournament_teams",
    "tournament_rosters": "tournament_rosters",
    # Removed entities — no raw data available (would need 4.8GB deep parsing)
    "player_match_stats": None,
    "team_match_stats":   None,
}


# ── Entity allow-list and static entities ─────────────────────────────
DOTA_ENTITY_ALLOWLIST: set[str] = {
    "teams",
    "players",
    "heroes",
    "items",
    "regions",
    "matches",
    "match_maps",
    "player_stats",
    "hero_stats",
    "tournaments",
    "tournament_teams",
    "tournament_rosters",
}

DOTA_STATIC_ENTITIES: set[str] = {"teams", "players", "heroes", "items", "regions"}
