# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — LoL Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 14-entity consolidated design.  Raw data from BDL LoL API providers
# are merged into 14 wide schemas.
#
# Entity overview
# ───────────────
#  1. teams              — static reference, no partitioning
#  2. players            — partition: tournament_id=
#  3. champions          — static reference, no partitioning
#  4. items              — static reference, no partitioning
#  5. runes              — static reference, no partitioning
#  6. spells             — static reference, no partitioning
#  7. tournaments        — partition: year=
#  8. matches            — partition: tournament_id=
#  9. match_maps         — partition: tournament_id=
# 10. player_stats       — partition: tournament_id=
# 11. team_stats         — partition: tournament_id=
# 12. champion_stats     — partition: tournament_id=
# 13. rankings           — partition: year=
# 14. tournament_roster  — partition: tournament_id=
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

LOL_TEAMS_SCHEMA = pa.schema([
    # Core identity
    _f("team_id",        pa.int32(),  "Unique team identifier",                    nullable=False),
    _f("name",           pa.string(), "Team name (e.g. T1)",                       nullable=False),
    _f("slug",           pa.string(), "URL-friendly slug"),
    _f("country",        pa.string(), "Country of origin"),
    _f("region_id",      pa.int32(),  "Region identifier"),
    _f("region_name",    pa.string(), "Region name (e.g. LCK, LEC)"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. players — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

LOL_PLAYERS_SCHEMA = pa.schema([
    # Core identity
    _f("player_id",      pa.int32(),  "Unique player identifier",                  nullable=False),
    _f("nickname",       pa.string(), "In-game name (IGN)",                        nullable=False),
    _f("slug",           pa.string(), "URL-friendly slug"),
    _f("first_name",     pa.string(), "First name"),
    _f("last_name",      pa.string(), "Last name"),
    _f("birthday",       pa.string(), "Date of birth (YYYY-MM-DD)"),
    _f("country",        pa.string(), "Country of origin"),
    _f("country_code",   pa.string(), "ISO country code"),
    # Team (flattened nested)
    _f("team_id",        pa.int32(),  "Current team identifier"),
    _f("team_name",      pa.string(), "Current team name"),
    # Partition key
    _f("tournament_id",  pa.int32(),  "Tournament context for this record"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. champions — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

LOL_CHAMPIONS_SCHEMA = pa.schema([
    _f("champion_id",    pa.int32(),  "Unique champion identifier",                nullable=False),
    _f("name",           pa.string(), "Champion name (e.g. Ahri)",                 nullable=False),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. items — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

LOL_ITEMS_SCHEMA = pa.schema([
    _f("item_id",        pa.int32(),  "Unique item identifier",                    nullable=False),
    _f("name",           pa.string(), "Item name",                                 nullable=False),
    _f("is_trinket",     pa.bool_(),  "Whether the item is a trinket"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. runes — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

LOL_RUNES_SCHEMA = pa.schema([
    _f("rune_id",        pa.int32(),  "Unique rune identifier",                    nullable=False),
    _f("name",           pa.string(), "Rune name",                                 nullable=False),
    _f("rune_path_id",   pa.int32(),  "Rune path identifier"),
    _f("rune_path_name", pa.string(), "Rune path name (e.g. Domination)"),
    _f("rune_type",      pa.string(), "Rune type — shard, slot, or keystone"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. spells — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

LOL_SPELLS_SCHEMA = pa.schema([
    _f("spell_id",       pa.int32(),  "Unique spell identifier",                   nullable=False),
    _f("name",           pa.string(), "Spell name (e.g. Flash)",                   nullable=False),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. tournaments — partition: year=
# ═══════════════════════════════════════════════════════════════════════

LOL_TOURNAMENTS_SCHEMA = pa.schema([
    _f("tournament_id",  pa.int32(),  "Unique tournament identifier",              nullable=False),
    _f("name",           pa.string(), "Tournament name",                           nullable=False),
    _f("slug",           pa.string(), "URL-friendly slug"),
    _f("start_date",     pa.string(), "Start date (YYYY-MM-DD)"),
    _f("end_date",       pa.string(), "End date (YYYY-MM-DD)"),
    _f("prize",          pa.string(), "Prize pool description"),
    _f("tier",           pa.string(), "Tournament tier (e.g. S, A, B)"),
    _f("status",         pa.string(), "Tournament status"),
    # Partition key
    _f("year",           pa.int32(),  "Calendar year for partitioning"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. matches — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

LOL_MATCHES_SCHEMA = pa.schema([
    # Core identity
    _f("match_id",       pa.int32(),  "Unique match identifier",                   nullable=False),
    _f("slug",           pa.string(), "URL-friendly slug"),
    # Tournament (flattened nested)
    _f("tournament_id",  pa.int32(),  "Parent tournament identifier"),
    _f("tournament_name", pa.string(), "Parent tournament name"),
    # Teams
    _f("team1_id",       pa.int32(),  "Team 1 identifier"),
    _f("team1_name",     pa.string(), "Team 1 name"),
    _f("team2_id",       pa.int32(),  "Team 2 identifier"),
    _f("team2_name",     pa.string(), "Team 2 name"),
    # Result
    _f("winner_id",      pa.int32(),  "Winning team identifier"),
    _f("winner_name",    pa.string(), "Winning team name"),
    _f("team1_score",    pa.int32(),  "Team 1 map score"),
    _f("team2_score",    pa.int32(),  "Team 2 map score"),
    _f("bo_type",        pa.string(), "Best-of type (bo1, bo3, bo5)"),
    _f("status",         pa.string(), "Match status"),
    _f("start_date",     pa.string(), "Start date/time (ISO-8601)"),
    _f("end_date",       pa.string(), "End date/time (ISO-8601)"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. match_maps (games) — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

LOL_MATCH_MAPS_SCHEMA = pa.schema([
    _f("match_map_id",   pa.int32(),  "Unique match-map (game) identifier",        nullable=False),
    _f("match_id",       pa.int32(),  "Parent match identifier",                   nullable=False),
    _f("game_number",    pa.int32(),  "Game number within the match"),
    # Winner / loser (flattened nested)
    _f("winner_id",      pa.int32(),  "Winning team identifier"),
    _f("winner_name",    pa.string(), "Winning team name"),
    _f("loser_id",       pa.int32(),  "Losing team identifier"),
    _f("loser_name",     pa.string(), "Losing team name"),
    # Timing
    _f("duration",       pa.int32(),  "Game duration in seconds"),
    _f("status",         pa.string(), "Game status"),
    _f("begin_at",       pa.string(), "Game start time (ISO-8601)"),
    _f("end_at",         pa.string(), "Game end time (ISO-8601)"),
    # Partition key (derived from parent match)
    _f("tournament_id",  pa.int32(),  "Tournament context for partitioning"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. player_stats (player match-map stats) — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

LOL_PLAYER_STATS_SCHEMA = pa.schema([
    _f("stat_id",              pa.int32(),   "Unique stat record identifier",       nullable=False),
    _f("match_map_id",         pa.int32(),   "Parent match-map identifier",         nullable=False),
    # Player (flattened nested)
    _f("player_id",            pa.int32(),   "Player identifier"),
    _f("player_nickname",      pa.string(),  "Player IGN"),
    # Team (flattened nested)
    _f("team_id",              pa.int32(),   "Team identifier"),
    _f("team_name",            pa.string(),  "Team name"),
    # Champion (flattened nested)
    _f("champion_id",          pa.int32(),   "Champion played"),
    _f("champion_name",        pa.string(),  "Champion name"),
    # Core stats
    _f("role",                 pa.string(),  "Player role (top, jungle, mid, adc, support)"),
    _f("level",                pa.int32(),   "Champion level at end of game"),
    _f("kills",                pa.int32(),   "Total kills"),
    _f("deaths",               pa.int32(),   "Total deaths"),
    _f("assists",              pa.int32(),   "Total assists"),
    _f("kill_participation",   pa.float64(), "Kill participation percentage (0-1)"),
    _f("creep_score",          pa.int32(),   "Total creep score (CS)"),
    _f("gold_earned",          pa.int32(),   "Total gold earned"),
    _f("gold_per_min",         pa.float64(), "Gold earned per minute"),
    _f("total_damage",         pa.int32(),   "Total damage dealt to champions"),
    _f("wards_placed",         pa.int32(),   "Wards placed"),
    _f("wards_killed",         pa.int32(),   "Wards destroyed"),
    # Loadout (stored as JSON-encoded lists)
    _f("items",                pa.string(),  "JSON array of item IDs"),
    _f("spells",               pa.string(),  "JSON array of spell IDs"),
    _f("runes",                pa.string(),  "JSON array of rune IDs"),
    # Partition key
    _f("tournament_id",        pa.int32(),   "Tournament context for partitioning"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. team_stats (team match-map stats) — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

LOL_TEAM_STATS_SCHEMA = pa.schema([
    _f("stat_id",              pa.int32(),   "Unique stat record identifier",       nullable=False),
    _f("match_map_id",         pa.int32(),   "Parent match-map identifier",         nullable=False),
    # Team (flattened nested)
    _f("team_id",              pa.int32(),   "Team identifier"),
    _f("team_name",            pa.string(),  "Team name"),
    # Enemy team (flattened nested)
    _f("enemy_team_id",        pa.int32(),   "Enemy team identifier"),
    _f("enemy_team_name",      pa.string(),  "Enemy team name"),
    # Side / colour
    _f("side",                 pa.string(),  "Map side (blue/red)"),
    _f("color",                pa.string(),  "Side colour"),
    # Aggregate stats
    _f("kills",                pa.int32(),   "Total team kills"),
    _f("deaths",               pa.int32(),   "Total team deaths"),
    _f("assists",              pa.int32(),   "Total team assists"),
    _f("creep_score",          pa.int32(),   "Total team creep score"),
    _f("gold_earned",          pa.int32(),   "Total team gold earned"),
    _f("total_damage",         pa.int32(),   "Total team damage dealt"),
    # Objectives
    _f("baron_kills",          pa.int32(),   "Baron Nashor kills"),
    _f("dragon_kills",         pa.int32(),   "Dragon kills"),
    _f("herald_kills",         pa.int32(),   "Rift Herald kills"),
    # First objectives
    _f("first_blood",          pa.bool_(),   "Achieved first blood"),
    _f("first_tower",          pa.bool_(),   "Destroyed first tower"),
    _f("first_baron",          pa.bool_(),   "Killed first Baron"),
    _f("first_dragon",         pa.bool_(),   "Killed first Dragon"),
    # Partition key
    _f("tournament_id",        pa.int32(),   "Tournament context for partitioning"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. champion_stats — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

LOL_CHAMPION_STATS_SCHEMA = pa.schema([
    _f("stat_id",              pa.int32(),   "Unique stat record identifier",       nullable=False),
    # Champion (flattened nested)
    _f("champion_id",          pa.int32(),   "Champion identifier"),
    _f("champion_name",        pa.string(),  "Champion name"),
    # Counts
    _f("games_count",          pa.int32(),   "Total games in sample"),
    _f("picks_count",          pa.int32(),   "Times picked"),
    _f("picks_rate",           pa.float64(), "Pick rate (0-1)"),
    _f("wins_count",           pa.int32(),   "Games won"),
    _f("loses_count",          pa.int32(),   "Games lost"),
    _f("win_rate",             pa.float64(), "Win rate (0-1)"),
    _f("ban_rate",             pa.float64(), "Ban rate (0-1)"),
    # Averages
    _f("avg_kills",            pa.float64(), "Average kills per game"),
    _f("avg_deaths",           pa.float64(), "Average deaths per game"),
    _f("avg_assists",          pa.float64(), "Average assists per game"),
    _f("kda",                  pa.float64(), "KDA ratio"),
    _f("kp",                   pa.float64(), "Kill participation (0-1)"),
    _f("avg_damage",           pa.float64(), "Average damage per game"),
    _f("avg_gold_earned",      pa.float64(), "Average gold earned per game"),
    _f("avg_gold_per_min",     pa.float64(), "Average gold per minute"),
    _f("avg_creep_score",      pa.float64(), "Average creep score per game"),
    # Partition key
    _f("tournament_id",        pa.int32(),   "Tournament context for partitioning"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. rankings — partition: year=
# ═══════════════════════════════════════════════════════════════════════

LOL_RANKINGS_SCHEMA = pa.schema([
    _f("rank",                 pa.int32(),   "Rank position",                       nullable=False),
    _f("points",               pa.float64(), "Ranking points"),
    _f("ranking_type",         pa.string(),  "Ranking type / category"),
    _f("ranking_date",         pa.string(),  "Ranking snapshot date (YYYY-MM-DD)"),
    # Team (flattened nested)
    _f("team_id",              pa.int32(),   "Team identifier"),
    _f("team_name",            pa.string(),  "Team name"),
    # Partition key
    _f("year",                 pa.int32(),   "Calendar year for partitioning"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. tournament_roster — partition: tournament_id=
# ═══════════════════════════════════════════════════════════════════════

LOL_TOURNAMENT_ROSTER_SCHEMA = pa.schema([
    _f("tournament_id",        pa.int32(),   "Tournament identifier",               nullable=False),
    _f("team_id",              pa.int32(),   "Team identifier",                     nullable=False),
    # Player (flattened nested)
    _f("player_id",            pa.int32(),   "Player identifier"),
    _f("player_nickname",      pa.string(),  "Player IGN"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Consolidated lookups
# ═══════════════════════════════════════════════════════════════════════

LOL_SCHEMAS: dict[str, pa.Schema] = {
    "teams":              LOL_TEAMS_SCHEMA,
    "players":            LOL_PLAYERS_SCHEMA,
    "champions":          LOL_CHAMPIONS_SCHEMA,
    "items":              LOL_ITEMS_SCHEMA,
    "runes":              LOL_RUNES_SCHEMA,
    "spells":             LOL_SPELLS_SCHEMA,
    "tournaments":        LOL_TOURNAMENTS_SCHEMA,
    "matches":            LOL_MATCHES_SCHEMA,
    "match_maps":         LOL_MATCH_MAPS_SCHEMA,
    "player_stats":       LOL_PLAYER_STATS_SCHEMA,
    "team_stats":         LOL_TEAM_STATS_SCHEMA,
    "champion_stats":     LOL_CHAMPION_STATS_SCHEMA,
    "rankings":           LOL_RANKINGS_SCHEMA,
    "tournament_roster":  LOL_TOURNAMENT_ROSTER_SCHEMA,
}

LOL_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning
    "teams":              [],
    "champions":          [],
    "items":              [],
    "runes":              [],
    "spells":             [],
    # Tournament-partitioned
    "players":            ["tournament_id"],
    "matches":            ["tournament_id"],
    "match_maps":         ["tournament_id"],
    "player_stats":       ["tournament_id"],
    "team_stats":         ["tournament_id"],
    "champion_stats":     ["tournament_id"],
    "tournament_roster":  ["tournament_id"],
    # Year-partitioned
    "tournaments":        ["year"],
    "rankings":           ["year"],
}

LOL_ENTITY_PATHS: dict[str, str] = {
    "teams":              "teams",
    "players":            "players",
    "champions":          "champions",
    "items":              "items",
    "runes":              "runes",
    "spells":             "spells",
    "tournaments":        "tournaments",
    "matches":            "matches",
    "match_maps":         "match_maps",
    "player_stats":       "player_stats",
    "team_stats":         "team_stats",
    "champion_stats":     "champion_stats",
    "rankings":           "rankings",
    "tournament_roster":  "tournament_roster",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names to the flat entity directory
# name under normalized_curated/lol/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
LOL_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "teams":            "teams",
    "players":          "players",
    "schedule":         "matches",
    "scores":           "matches",
    "standings":        "rankings",
    "rankings":         "rankings",
    "player_stats":     "player_stats",
    "roster":           "tournament_roster",
    "tournaments":      "tournaments",
    "champions":        "champions",
    "items":            "items",
    "runes":            "runes",
    "spells":           "spells",
    # Non-entity normalizer artefacts — skip
    "odds":             None,
    "player_props":     None,
    "lineups":          None,
    "plays":            None,
    "news":             None,
    "weather":          None,
    "market_signals":   None,
    "injuries":         None,
    "depth_charts":     None,
    "conferences":      None,
    "venues":           None,
}


# ── Entity allow-list and static entities ─────────────────────────────
LOL_ENTITY_ALLOWLIST: set[str] = {
    "teams",
    "players",
    "champions",
    "items",
    "runes",
    "spells",
    "tournaments",
    "matches",
    "match_maps",
    "player_stats",
    "team_stats",
    "champion_stats",
    "rankings",
    "tournament_roster",
}

LOL_STATIC_ENTITIES: set[str] = {"champions", "items", "runes", "spells"}
