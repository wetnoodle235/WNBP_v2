# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — Tennis Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 10-entity consolidated design.  Raw data from BDL ATP API + ESPN /
# TennisAbstract providers are merged into 10 wide schemas.
#
# Entity overview
# ───────────────
#  1. players        — static reference, no partitioning
#  2. tournaments    — partition: season=
#  3. matches        — partition: season=
#  4. match_stats    — partition: season=
#  5. rankings       — partition: season=
#  6. race           — partition: season=
#  7. head_to_head   — static reference, no partitioning
#  8. career_stats   — static reference, no partitioning
#  9. odds           — partition: season=
# 10. injuries       — partition: season=
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
# 1. players — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

TENNIS_PLAYERS_SCHEMA = pa.schema([
    _f("id",             pa.string(), "Unique player identifier",                  nullable=False),
    _f("first_name",     pa.string(), "First name"),
    _f("last_name",      pa.string(), "Last name"),
    _f("full_name",      pa.string(), "Full display name"),
    _f("country",        pa.string(), "Country of origin"),
    _f("country_code",   pa.string(), "ISO country code"),
    _f("birth_place",    pa.string(), "City / region of birth"),
    _f("age",            pa.int32(),  "Player age in years"),
    _f("height_cm",      pa.int32(),  "Height in centimetres"),
    _f("weight_kg",      pa.int32(),  "Weight in kilograms"),
    _f("plays",          pa.string(), "Handedness / play style (e.g. Right-Handed)"),
    _f("turned_pro",     pa.int32(),  "Year turned professional"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. tournaments — partition: season=
# ═══════════════════════════════════════════════════════════════════════

TENNIS_TOURNAMENTS_SCHEMA = pa.schema([
    _f("id",             pa.string(), "Unique tournament identifier",              nullable=False),
    _f("name",           pa.string(), "Tournament name",                           nullable=False),
    _f("location",       pa.string(), "Host city / venue location"),
    _f("surface",        pa.string(), "Court surface (Hard, Clay, Grass, Carpet)"),
    _f("category",       pa.string(), "Tournament category (Grand Slam, Masters, ATP 500, etc.)"),
    _f("season",         pa.int32(),  "Season year"),
    _f("start_date",     pa.string(), "Start date (YYYY-MM-DD)"),
    _f("end_date",       pa.string(), "End date (YYYY-MM-DD)"),
    _f("prize_money",    pa.int32(),  "Total prize money"),
    _f("prize_currency", pa.string(), "Prize money currency code"),
    _f("draw_size",      pa.int32(),  "Draw size (number of players)"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. matches — partition: season=
# ═══════════════════════════════════════════════════════════════════════

TENNIS_MATCHES_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique match identifier",                  nullable=False),
    _f("tournament_id",   pa.string(), "Parent tournament identifier"),
    _f("tournament_name", pa.string(), "Parent tournament name"),
    _f("season",          pa.int32(),  "Season year"),
    _f("round",           pa.string(), "Round name (Final, SF, QF, R16, etc.)"),
    _f("player1_id",      pa.string(), "Player 1 identifier"),
    _f("player1_name",    pa.string(), "Player 1 display name"),
    _f("player2_id",      pa.string(), "Player 2 identifier"),
    _f("player2_name",    pa.string(), "Player 2 display name"),
    _f("winner_id",       pa.string(), "Winner identifier"),
    _f("winner_name",     pa.string(), "Winner display name"),
    _f("score",           pa.string(), "Final score string (e.g. 6-4 7-6(5) 6-3)"),
    _f("duration",        pa.string(), "Match duration (HH:MM or minutes)"),
    _f("number_of_sets",  pa.int32(),  "Number of sets played"),
    _f("match_status",    pa.string(), "Match status (completed, in_progress, scheduled, retired)"),
    _f("is_live",         pa.bool_(),  "Whether the match is currently live"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. match_stats — partition: season=
# ═══════════════════════════════════════════════════════════════════════

TENNIS_MATCH_STATS_SCHEMA = pa.schema([
    _f("id",                           pa.string(),  "Unique stat record identifier",            nullable=False),
    _f("match_id",                     pa.string(),  "Parent match identifier",                  nullable=False),
    _f("player_id",                    pa.string(),  "Player identifier"),
    _f("player_name",                  pa.string(),  "Player display name"),
    _f("set_number",                   pa.int32(),   "Set number (0 = full-match aggregate)"),
    _f("serve_rating",                 pa.int32(),   "Serve rating"),
    _f("aces",                         pa.int32(),   "Number of aces"),
    _f("double_faults",               pa.int32(),   "Number of double faults"),
    _f("first_serve_pct",             pa.float64(), "First serve percentage (0-100)"),
    _f("first_serve_points_won_pct",  pa.float64(), "First serve points won percentage (0-100)"),
    _f("second_serve_points_won_pct", pa.float64(), "Second serve points won percentage (0-100)"),
    _f("break_points_saved_pct",      pa.float64(), "Break points saved percentage (0-100)"),
    _f("return_rating",               pa.int32(),   "Return rating"),
    _f("first_return_won_pct",        pa.float64(), "First return points won percentage (0-100)"),
    _f("second_return_won_pct",       pa.float64(), "Second return points won percentage (0-100)"),
    _f("break_points_converted_pct",  pa.float64(), "Break points converted percentage (0-100)"),
    _f("total_points_won_pct",        pa.float64(), "Total points won percentage (0-100)"),
    # Provenance
    _f("source",                       pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. rankings — partition: season=
# ═══════════════════════════════════════════════════════════════════════

TENNIS_RANKINGS_SCHEMA = pa.schema([
    _f("id",             pa.string(), "Unique ranking record identifier",          nullable=False),
    _f("player_id",      pa.string(), "Player identifier"),
    _f("player_name",    pa.string(), "Player display name"),
    _f("rank",           pa.int32(),  "Rank position"),
    _f("points",         pa.int32(),  "Ranking points"),
    _f("movement",       pa.int32(),  "Rank movement since previous week (+/-)"),
    _f("ranking_date",   pa.string(), "Ranking snapshot date (YYYY-MM-DD)"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. race — partition: season=
# ═══════════════════════════════════════════════════════════════════════

TENNIS_RACE_SCHEMA = pa.schema([
    _f("id",             pa.string(), "Unique race ranking record identifier",     nullable=False),
    _f("player_id",      pa.string(), "Player identifier"),
    _f("player_name",    pa.string(), "Player display name"),
    _f("rank",           pa.int32(),  "Race rank position"),
    _f("points",         pa.int32(),  "Race ranking points"),
    _f("movement",       pa.int32(),  "Rank movement since previous week (+/-)"),
    _f("ranking_date",   pa.string(), "Ranking snapshot date (YYYY-MM-DD)"),
    _f("is_qualified",   pa.bool_(),  "Whether the player has qualified for finals"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. head_to_head — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

TENNIS_HEAD_TO_HEAD_SCHEMA = pa.schema([
    _f("id",             pa.string(), "Unique head-to-head record identifier",     nullable=False),
    _f("player1_id",     pa.string(), "Player 1 identifier"),
    _f("player1_name",   pa.string(), "Player 1 display name"),
    _f("player2_id",     pa.string(), "Player 2 identifier"),
    _f("player2_name",   pa.string(), "Player 2 display name"),
    _f("player1_wins",   pa.int32(),  "Player 1 head-to-head wins"),
    _f("player2_wins",   pa.int32(),  "Player 2 head-to-head wins"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. career_stats — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

TENNIS_CAREER_STATS_SCHEMA = pa.schema([
    _f("player_id",          pa.string(),  "Player identifier",                    nullable=False),
    _f("player_name",        pa.string(),  "Player display name"),
    _f("career_titles",      pa.int32(),   "Career titles won"),
    _f("career_prize_money", pa.float64(), "Career prize money earned"),
    _f("singles_wins",       pa.int32(),   "Career singles wins"),
    _f("singles_losses",     pa.int32(),   "Career singles losses"),
    _f("ytd_wins",           pa.int32(),   "Year-to-date wins"),
    _f("ytd_losses",         pa.int32(),   "Year-to-date losses"),
    _f("ytd_titles",         pa.int32(),   "Year-to-date titles"),
    # Provenance
    _f("source",             pa.string(),  "Data vendor provenance",               nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. odds — partition: season=
# ═══════════════════════════════════════════════════════════════════════

TENNIS_ODDS_SCHEMA = pa.schema([
    _f("id",             pa.string(), "Unique odds record identifier",             nullable=False),
    _f("match_id",       pa.string(), "Parent match identifier"),
    _f("vendor",         pa.string(), "Odds vendor / sportsbook name"),
    _f("player1_id",     pa.string(), "Player 1 identifier"),
    _f("player1_name",   pa.string(), "Player 1 display name"),
    _f("player2_id",     pa.string(), "Player 2 identifier"),
    _f("player2_name",   pa.string(), "Player 2 display name"),
    _f("player1_odds",   pa.int32(),  "Player 1 odds (American format)"),
    _f("player2_odds",   pa.int32(),  "Player 2 odds (American format)"),
    _f("updated_at",     pa.string(), "Last update timestamp (ISO-8601)"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. injuries — partition: season=
# ═══════════════════════════════════════════════════════════════════════

TENNIS_INJURIES_SCHEMA = pa.schema([
    _f("id",             pa.string(), "Unique injury record identifier",           nullable=False),
    _f("player_id",      pa.string(), "Player identifier"),
    _f("player_name",    pa.string(), "Player display name"),
    _f("status",         pa.string(), "Injury status (out, doubtful, questionable)"),
    _f("description",    pa.string(), "Injury description"),
    _f("date",           pa.string(), "Injury report date (YYYY-MM-DD)"),
    _f("type",           pa.string(), "Injury type / body part"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Consolidated lookups
# ═══════════════════════════════════════════════════════════════════════

TENNIS_SCHEMAS: dict[str, pa.Schema] = {
    "players":        TENNIS_PLAYERS_SCHEMA,
    "tournaments":    TENNIS_TOURNAMENTS_SCHEMA,
    "matches":        TENNIS_MATCHES_SCHEMA,
    "match_stats":    TENNIS_MATCH_STATS_SCHEMA,
    "rankings":       TENNIS_RANKINGS_SCHEMA,
    "race":           TENNIS_RACE_SCHEMA,
    "head_to_head":   TENNIS_HEAD_TO_HEAD_SCHEMA,
    "career_stats":   TENNIS_CAREER_STATS_SCHEMA,
    "odds":           TENNIS_ODDS_SCHEMA,
    "injuries":       TENNIS_INJURIES_SCHEMA,
}

TENNIS_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning
    "players":        [],
    "head_to_head":   [],
    "career_stats":   [],
    # Season-partitioned
    "tournaments":    ["season"],
    "matches":        ["season"],
    "match_stats":    ["season"],
    "rankings":       ["season"],
    "race":           ["season"],
    "odds":           ["season"],
    "injuries":       ["season"],
}

TENNIS_ENTITY_PATHS: dict[str, str] = {
    "players":        "players",
    "tournaments":    "tournaments",
    "matches":        "matches",
    "match_stats":    "match_stats",
    "rankings":       "rankings",
    "race":           "race",
    "head_to_head":   "head_to_head",
    "career_stats":   "career_stats",
    "odds":           "odds",
    "injuries":       "injuries",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names to the flat entity directory
# name under normalized_curated/tennis/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
TENNIS_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "players":        "players",
    "tournaments":    "tournaments",
    "matches":        "matches",
    "schedule":       "matches",
    "scores":         "matches",
    "match_stats":    "match_stats",
    "rankings":       "rankings",
    "standings":      "rankings",
    "race":           "race",
    "head_to_head":   "head_to_head",
    "career_stats":   "career_stats",
    "odds":           "odds",
    "injuries":       "injuries",
    # Non-entity normalizer artefacts — skip
    "player_props":   None,
    "lineups":        None,
    "plays":          None,
    "news":           None,
    "weather":        None,
    "market_signals": None,
    "depth_charts":   None,
    "conferences":    None,
    "venues":         None,
}


# ── Entity allow-list and static entities ─────────────────────────────
TENNIS_ENTITY_ALLOWLIST: set[str] = {
    "players",
    "tournaments",
    "matches",
    "match_stats",
    "rankings",
    "race",
    "head_to_head",
    "career_stats",
    "odds",
    "injuries",
}

TENNIS_STATIC_ENTITIES: set[str] = {"players", "head_to_head", "career_stats"}
