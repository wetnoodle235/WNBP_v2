# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — CS2 Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 9-entity consolidated design.  Raw data from BDL CS2 API providers
# are merged into 9 wide schemas.
#
# Entity overview
# ───────────────
#  1. teams              — static reference, no partitioning
#  2. players            — partition: tournament_id=
#  3. tournaments        — partition: year=
#  4. tournament_teams   — partition: tournament_id=
#  5. matches            — partition: tournament_id=
#  6. match_maps         — partition: tournament_id=
#  7. rankings           — partition: year=
#  8. player_match_stats — partition: tournament_id=
#  9. team_map_pool      — partition: tournament_id=
#
# CS2 uses tournament-based partitioning rather than season/week.
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

CS_TEAMS_SCHEMA = pa.schema([
    # Core identity
    _f("team_id",      pa.int32(),  "Unique team identifier",            nullable=False),
    _f("name",         pa.string(), "Full team name",                    nullable=False),
    _f("slug",         pa.string(), "URL-friendly slug"),
    _f("short_name",   pa.string(), "Short / abbreviated team name"),
    # Provenance
    _f("source",       pa.string(), "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. players — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

CS_PLAYERS_SCHEMA = pa.schema([
    # Core identity
    _f("player_id",    pa.int32(),  "Unique player identifier",          nullable=False),
    _f("nickname",     pa.string(), "In-game alias / handle",            nullable=False),
    _f("first_name",   pa.string(), "First name"),
    _f("last_name",    pa.string(), "Last name"),
    _f("full_name",    pa.string(), "Full real name"),
    # Team association (flattened from nested)
    _f("team_id",      pa.int32(),  "Current team identifier"),
    _f("team_name",    pa.string(), "Current team name"),
    _f("team_short",   pa.string(), "Current team short name"),
    # Demographics
    _f("age",          pa.int32(),  "Player age"),
    _f("birthday",     pa.string(), "Date of birth (YYYY-MM-DD)"),
    _f("steam_id",     pa.string(), "Steam account identifier"),
    _f("is_active",    pa.bool_(),  "Whether the player is currently active"),
    # Partition key
    _f("tournament_id", pa.int32(), "Tournament context for this snapshot"),
    # Provenance
    _f("source",       pa.string(), "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. tournaments — partition: year=
# ═══════════════════════════════════════════════════════════════════════

CS_TOURNAMENTS_SCHEMA = pa.schema([
    _f("tournament_id",       pa.int32(),  "Unique tournament identifier",       nullable=False),
    _f("name",                pa.string(), "Tournament name",                    nullable=False),
    _f("slug",                pa.string(), "URL-friendly slug"),
    _f("tier",                pa.string(), "Tournament tier (S, A, B, C, etc.)"),
    _f("start_date",          pa.string(), "Start date (YYYY-MM-DD)"),
    _f("end_date",            pa.string(), "End date (YYYY-MM-DD)"),
    _f("prize_pool",          pa.float64(),"Prize pool amount"),
    _f("prize_pool_currency", pa.string(), "Prize pool currency code"),
    _f("location",            pa.string(), "Venue / city"),
    _f("country",             pa.string(), "Host country"),
    _f("is_online",           pa.bool_(),  "Whether the tournament is online"),
    _f("status",              pa.string(), "Tournament status (upcoming, ongoing, completed)"),
    # Partition key
    _f("year",                pa.int32(),  "Calendar year of the tournament"),
    # Provenance
    _f("source",              pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. tournament_teams — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

CS_TOURNAMENT_TEAMS_SCHEMA = pa.schema([
    _f("tournament_id",  pa.int32(),  "Tournament identifier",           nullable=False),
    _f("team_id",        pa.int32(),  "Team identifier",                 nullable=False),
    _f("team_name",      pa.string(), "Team name"),
    _f("team_short",     pa.string(), "Team short name"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. matches — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

CS_MATCHES_SCHEMA = pa.schema([
    _f("match_id",        pa.int32(),  "Unique match identifier",        nullable=False),
    _f("slug",            pa.string(), "URL-friendly slug"),
    # Tournament context (flattened)
    _f("tournament_id",   pa.int32(),  "Tournament identifier"),
    _f("tournament_name", pa.string(), "Tournament name"),
    # Stage context (flattened)
    _f("stage_id",        pa.int32(),  "Stage identifier"),
    _f("stage_name",      pa.string(), "Stage name"),
    _f("stage_type",      pa.string(), "Stage type (group, playoff, etc.)"),
    _f("stage_order",     pa.int32(),  "Stage ordering index"),
    _f("stage_best_of",   pa.int32(),  "Best-of for the stage"),
    _f("stage_rounds",    pa.int32(),  "Number of rounds in the stage"),
    _f("stage_start",     pa.string(), "Stage start date (YYYY-MM-DD)"),
    _f("stage_end",       pa.string(), "Stage end date (YYYY-MM-DD)"),
    _f("stage_status",    pa.string(), "Stage status"),
    # Teams
    _f("team1_id",        pa.int32(),  "Team 1 identifier"),
    _f("team1_name",      pa.string(), "Team 1 name"),
    _f("team2_id",        pa.int32(),  "Team 2 identifier"),
    _f("team2_name",      pa.string(), "Team 2 name"),
    _f("team1_score",     pa.int32(),  "Team 1 map score"),
    _f("team2_score",     pa.int32(),  "Team 2 map score"),
    _f("winner_id",       pa.int32(),  "Winning team identifier"),
    _f("winner_name",     pa.string(), "Winning team name"),
    # Match meta
    _f("best_of",         pa.int32(),  "Best-of series length"),
    _f("status",          pa.string(), "Match status (upcoming, live, completed)"),
    _f("start_time",      pa.string(), "Match start time (ISO 8601)"),
    _f("end_time",        pa.string(), "Match end time (ISO 8601)"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",         nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. match_maps — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

CS_MATCH_MAPS_SCHEMA = pa.schema([
    _f("match_map_id",     pa.int32(),  "Unique match-map identifier",   nullable=False),
    _f("match_id",         pa.int32(),  "Parent match identifier",       nullable=False),
    _f("map_name",         pa.string(), "Map name (e.g. de_dust2)"),
    _f("map_number",       pa.int32(),  "Map number in the series (1-based)"),
    _f("team1_score",      pa.int32(),  "Team 1 round score on this map"),
    _f("team2_score",      pa.int32(),  "Team 2 round score on this map"),
    _f("winner_id",        pa.int32(),  "Winning team identifier"),
    _f("winner_name",      pa.string(), "Winning team name"),
    _f("duration_seconds", pa.int32(),  "Map duration in seconds"),
    _f("overtime_rounds",  pa.int32(),  "Number of overtime rounds played"),
    # Partition key (denormalised for query convenience)
    _f("tournament_id",    pa.int32(),  "Tournament identifier for partitioning"),
    # Provenance
    _f("source",           pa.string(), "Data vendor provenance",        nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. rankings — partition: year=
# ═══════════════════════════════════════════════════════════════════════

CS_RANKINGS_SCHEMA = pa.schema([
    _f("rank",            pa.int32(),  "Ranking position",               nullable=False),
    _f("points",          pa.float64(),"Ranking points"),
    _f("ranking_type",    pa.string(), "Ranking type (e.g. world, regional)"),
    _f("ranking_date",    pa.string(), "Date of ranking snapshot (YYYY-MM-DD)"),
    # Team (flattened)
    _f("team_id",         pa.int32(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("team_short",      pa.string(), "Team short name"),
    # Partition key
    _f("year",            pa.int32(),  "Calendar year of the ranking"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",         nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. player_match_stats — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

CS_PLAYER_MATCH_STATS_SCHEMA = pa.schema([
    # Player (flattened)
    _f("player_id",          pa.int32(),  "Player identifier",           nullable=False),
    _f("player_nickname",    pa.string(), "Player in-game alias"),
    _f("match_id",           pa.int32(),  "Match identifier",            nullable=False),
    _f("team_id",            pa.int32(),  "Team identifier"),
    # Core stats
    _f("kills",              pa.int32(),  "Total kills"),
    _f("deaths",             pa.int32(),  "Total deaths"),
    _f("assists",            pa.int32(),  "Total assists"),
    _f("adr",                pa.float64(),"Average damage per round"),
    _f("kast",               pa.float64(),"KAST percentage"),
    _f("rating",             pa.float64(),"HLTV-style rating"),
    _f("headshot_percentage",pa.float64(),"Headshot kill percentage"),
    _f("first_kills",        pa.int32(),  "Opening kills"),
    _f("first_deaths",       pa.int32(),  "Opening deaths"),
    _f("clutches_won",       pa.int32(),  "Clutch rounds won"),
    # Partition key
    _f("tournament_id",      pa.int32(),  "Tournament identifier for partitioning"),
    # Provenance
    _f("source",             pa.string(), "Data vendor provenance",      nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. team_map_pool — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

CS_TEAM_MAP_POOL_SCHEMA = pa.schema([
    _f("team_id",        pa.int32(),  "Team identifier",                 nullable=False),
    _f("map_name",       pa.string(), "Map name",                        nullable=False),
    _f("matches_played", pa.int32(),  "Total matches played on this map"),
    _f("wins",           pa.int32(),  "Wins on this map"),
    _f("losses",         pa.int32(),  "Losses on this map"),
    _f("win_rate",       pa.float64(),"Win rate on this map (0–1)"),
    _f("is_permaban",    pa.bool_(),  "Whether the team always bans this map"),
    # Partition key
    _f("tournament_id",  pa.int32(),  "Tournament identifier for partitioning"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Registry — schema, partition key, and path look-ups
# ═══════════════════════════════════════════════════════════════════════

CS_SCHEMAS: dict[str, pa.Schema] = {
    "teams":              CS_TEAMS_SCHEMA,
    "players":            CS_PLAYERS_SCHEMA,
    "tournaments":        CS_TOURNAMENTS_SCHEMA,
    "tournament_teams":   CS_TOURNAMENT_TEAMS_SCHEMA,
    "matches":            CS_MATCHES_SCHEMA,
    "match_maps":         CS_MATCH_MAPS_SCHEMA,
    "rankings":           CS_RANKINGS_SCHEMA,
    "player_match_stats": CS_PLAYER_MATCH_STATS_SCHEMA,
    "team_map_pool":      CS_TEAM_MAP_POOL_SCHEMA,
}

CS_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning
    "teams":              [],
    # Tournament-based partitioning
    "players":            ["tournament_id"],
    "tournaments":        ["year"],
    "tournament_teams":   ["tournament_id"],
    "matches":            ["tournament_id"],
    "match_maps":         ["tournament_id"],
    "rankings":           ["year"],
    "player_match_stats": ["tournament_id"],
    "team_map_pool":      ["tournament_id"],
}

CS_ENTITY_PATHS: dict[str, str] = {
    "teams":              "teams",
    "players":            "players",
    "tournaments":        "tournaments",
    "tournament_teams":   "tournament_teams",
    "matches":            "matches",
    "match_maps":         "match_maps",
    "rankings":           "rankings",
    "player_match_stats": "player_match_stats",
    "team_map_pool":      "team_map_pool",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names to the flat entity directory
# name under normalized_curated/cs/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
CS_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "teams":           "teams",
    "players":         "players",
    "tournaments":     "tournaments",
    "schedule":        "matches",
    "scores":          "matches",
    "standings":       "rankings",
    "rankings":        "rankings",
    "player_stats":    "player_match_stats",
    # Non-entity normalizer artefacts — skip
    "odds":            None,
    "player_props":    None,
    "roster":          None,
    "lineups":         None,
    "plays":           None,
    "news":            None,
    "weather":         None,
    "market_signals":  None,
    "injuries":        None,
    "depth_charts":    None,
    "conferences":     None,
    "venues":          None,
}


# ── Entity allow-list and static entities ─────────────────────────────
CS_ENTITY_ALLOWLIST: set[str] = {
    "teams",
    "players",
    "tournaments",
    "tournament_teams",
    "matches",
    "match_maps",
    "rankings",
    "player_match_stats",
    "team_map_pool",
}

CS_STATIC_ENTITIES: set[str] = {"teams"}
