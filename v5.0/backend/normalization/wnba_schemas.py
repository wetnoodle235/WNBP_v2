# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — WNBA Normalized-Curated PyArrow Schemas
# ──────────────────────────────────────────────────────────────────────
#
# 14-entity consolidated design.  Raw data from various WNBA providers
# are merged into 14 wide schemas that use discriminator columns
# (``scope``, ``line_type``, ``prop_type``, ``season_type``) to
# distinguish record subtypes within a single table.
#
# Entity overview
# ───────────────
#  1. teams            — static reference, no partitioning
#  2. players          — partition: season=
#  3. games            — partition: season=
#  4. player_stats     — partition: season=
#  5. team_stats       — partition: season=
#  6. standings        — partition: season=
#  7. odds             — partition: season=
#  8. player_props     — partition: season=
#  9. plays            — partition: season=
# 10. injuries         — partition: season=
# 11. leaders          — partition: season=
# 12. venues           — static reference, no partitioning
# 13. advanced         — partition: season=
# 14. box_scores       — partition: season=
#
# WNBA uses a single-year season (e.g. 2024).
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

WNBA_TEAMS_SCHEMA = pa.schema([
    _f("team_id",        pa.string(), "Unique team identifier",             nullable=False),
    _f("name",           pa.string(), "Team name",                          nullable=False),
    _f("abbreviation",   pa.string(), "Team abbreviation (e.g. LVA)"),
    _f("city",           pa.string(), "City where team is located"),
    _f("conference",     pa.string(), "Conference assignment"),
    _f("logo_url",       pa.string(), "URL to team logo image"),
    _f("primary_color",  pa.string(), "Primary team colour hex code"),
    _f("arena",          pa.string(), "Home arena name"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. players — partition: season=
# ═══════════════════════════════════════════════════════════════════════

WNBA_PLAYERS_SCHEMA = pa.schema([
    # Core identity
    _f("player_id",      pa.string(), "Unique player identifier",           nullable=False),
    _f("name",           pa.string(), "Full display name",                  nullable=False),
    _f("first_name",     pa.string(), "Player first name"),
    _f("last_name",      pa.string(), "Player last name"),
    _f("team_id",        pa.string(), "Current team identifier"),
    _f("team_name",      pa.string(), "Current team name"),
    _f("position",       pa.string(), "Position (e.g. G, F, C)"),
    _f("jersey_number",  pa.string(), "Jersey number as string"),
    _f("height",         pa.string(), "Height (e.g. 6-0)"),
    _f("weight",         pa.int32(),  "Weight in pounds"),
    _f("birth_date",     pa.string(), "Date of birth (YYYY-MM-DD)"),
    _f("nationality",    pa.string(), "Country of origin"),
    _f("experience",     pa.int32(),  "Years of professional experience"),
    _f("college",        pa.string(), "College attended"),
    _f("draft_year",     pa.int32(),  "Year drafted"),
    _f("draft_round",    pa.int32(),  "Draft round"),
    _f("draft_pick",     pa.int32(),  "Draft pick number"),
    _f("status",         pa.string(), "Player status (active/inactive/injured)"),
    _f("season",         pa.int32(),  "Season year (e.g. 2024)"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. games — partition: season=
# ═══════════════════════════════════════════════════════════════════════

WNBA_GAMES_SCHEMA = pa.schema([
    _f("game_id",        pa.string(), "Unique game identifier",             nullable=False),
    _f("season",         pa.int32(),  "Season year (e.g. 2024)"),
    _f("date",           pa.string(), "Game date (YYYY-MM-DD)"),
    _f("start_time",     pa.string(), "Scheduled start time (ISO 8601)"),
    _f("status",         pa.string(), "Game status (scheduled/in_progress/final)"),
    # Teams
    _f("home_team_id",   pa.string(), "Home team identifier"),
    _f("home_team",      pa.string(), "Home team name"),
    _f("away_team_id",   pa.string(), "Away team identifier"),
    _f("away_team",      pa.string(), "Away team name"),
    # Score
    _f("home_score",     pa.int32(),  "Home team final score"),
    _f("away_score",     pa.int32(),  "Away team final score"),
    # Venue & metadata
    _f("venue",          pa.string(), "Venue name"),
    _f("season_type",    pa.string(), "Season type (regular/playoff/allstar)"),
    _f("overtime",       pa.bool_(),  "Whether game went to overtime"),
    # Quarter scores
    _f("home_q1",        pa.int32(),  "Home team Q1 score"),
    _f("home_q2",        pa.int32(),  "Home team Q2 score"),
    _f("home_q3",        pa.int32(),  "Home team Q3 score"),
    _f("home_q4",        pa.int32(),  "Home team Q4 score"),
    _f("away_q1",        pa.int32(),  "Away team Q1 score"),
    _f("away_q2",        pa.int32(),  "Away team Q2 score"),
    _f("away_q3",        pa.int32(),  "Away team Q3 score"),
    _f("away_q4",        pa.int32(),  "Away team Q4 score"),
    _f("attendance",     pa.int32(),  "Reported attendance"),
    _f("broadcast",      pa.string(), "Broadcast network(s)"),
    # Derived
    _f("result",         pa.string(), "Result string (e.g. W/L from home perspective)"),
    _f("score_diff",     pa.int32(),  "Score differential (home − away)"),
    _f("total_score",    pa.int32(),  "Combined total score"),
    _f("day_of_week",    pa.string(), "Day of week the game was played"),
    _f("is_weekend",     pa.bool_(),  "Whether game fell on Saturday or Sunday"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. player_stats — partition: season=
#    Discriminator: scope (game | season)
# ═══════════════════════════════════════════════════════════════════════

WNBA_PLAYER_STATS_SCHEMA = pa.schema([
    _f("player_id",                pa.string(),  "Player identifier",                nullable=False),
    _f("player_name",              pa.string(),  "Player display name"),
    _f("team_id",                  pa.string(),  "Team identifier"),
    _f("team_name",                pa.string(),  "Team name"),
    _f("season",                   pa.int32(),   "Season year"),
    _f("scope",                    pa.string(),  "Record scope: game or season"),
    _f("game_id",                  pa.string(),  "Game identifier (null for season scope)"),
    _f("date",                     pa.string(),  "Game date (YYYY-MM-DD)"),
    _f("minutes",                  pa.float64(), "Minutes played"),
    _f("points",                   pa.int32(),   "Points scored"),
    _f("rebounds",                 pa.int32(),   "Total rebounds"),
    _f("assists",                  pa.int32(),   "Assists"),
    _f("steals",                   pa.int32(),   "Steals"),
    _f("blocks",                   pa.int32(),   "Blocks"),
    _f("turnovers",                pa.int32(),   "Turnovers"),
    _f("field_goals_made",         pa.int32(),   "Field goals made"),
    _f("field_goals_attempted",    pa.int32(),   "Field goals attempted"),
    _f("field_goal_pct",           pa.float64(), "Field goal percentage"),
    _f("three_pointers_made",      pa.int32(),   "Three-pointers made"),
    _f("three_pointers_attempted", pa.int32(),   "Three-pointers attempted"),
    _f("three_point_pct",          pa.float64(), "Three-point percentage"),
    _f("free_throws_made",         pa.int32(),   "Free throws made"),
    _f("free_throws_attempted",    pa.int32(),   "Free throws attempted"),
    _f("free_throw_pct",           pa.float64(), "Free throw percentage"),
    _f("offensive_rebounds",       pa.int32(),   "Offensive rebounds"),
    _f("defensive_rebounds",       pa.int32(),   "Defensive rebounds"),
    _f("personal_fouls",           pa.int32(),   "Personal fouls"),
    _f("plus_minus",               pa.float64(), "Plus/minus rating"),
    _f("games_played",             pa.int32(),   "Games played (season scope)"),
    # Provenance
    _f("source",                   pa.string(),  "Data vendor provenance",           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. team_stats — partition: season=
#    Discriminator: scope (game | season)
# ═══════════════════════════════════════════════════════════════════════

WNBA_TEAM_STATS_SCHEMA = pa.schema([
    _f("team_id",                  pa.string(),  "Team identifier",                  nullable=False),
    _f("team_name",                pa.string(),  "Team name"),
    _f("season",                   pa.int32(),   "Season year"),
    _f("scope",                    pa.string(),  "Record scope: game or season"),
    _f("game_id",                  pa.string(),  "Game identifier (null for season scope)"),
    _f("date",                     pa.string(),  "Game date (YYYY-MM-DD)"),
    _f("points",                   pa.int32(),   "Points scored"),
    _f("rebounds",                 pa.int32(),   "Total rebounds"),
    _f("assists",                  pa.int32(),   "Assists"),
    _f("steals",                   pa.int32(),   "Steals"),
    _f("blocks",                   pa.int32(),   "Blocks"),
    _f("turnovers",                pa.int32(),   "Turnovers"),
    _f("field_goals_made",         pa.int32(),   "Field goals made"),
    _f("field_goals_attempted",    pa.int32(),   "Field goals attempted"),
    _f("field_goal_pct",           pa.float64(), "Field goal percentage"),
    _f("three_pointers_made",      pa.int32(),   "Three-pointers made"),
    _f("three_pointers_attempted", pa.int32(),   "Three-pointers attempted"),
    _f("three_point_pct",          pa.float64(), "Three-point percentage"),
    _f("free_throws_made",         pa.int32(),   "Free throws made"),
    _f("free_throws_attempted",    pa.int32(),   "Free throws attempted"),
    _f("free_throw_pct",           pa.float64(), "Free throw percentage"),
    _f("offensive_rebounds",       pa.int32(),   "Offensive rebounds"),
    _f("defensive_rebounds",       pa.int32(),   "Defensive rebounds"),
    _f("personal_fouls",           pa.int32(),   "Personal fouls"),
    _f("games_played",             pa.int32(),   "Games played (season scope)"),
    # Provenance
    _f("source",                   pa.string(),  "Data vendor provenance",           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. standings — partition: season=
# ═══════════════════════════════════════════════════════════════════════

WNBA_STANDINGS_SCHEMA = pa.schema([
    _f("team_id",          pa.string(),  "Team identifier",                  nullable=False),
    _f("team_name",        pa.string(),  "Team name"),
    _f("season",           pa.int32(),   "Season year"),
    _f("conference",       pa.string(),  "Conference assignment"),
    _f("wins",             pa.int32(),   "Total wins"),
    _f("losses",           pa.int32(),   "Total losses"),
    _f("pct",              pa.float64(), "Win percentage"),
    _f("games_played",     pa.int32(),   "Total games played"),
    _f("points_for",       pa.int32(),   "Total points scored"),
    _f("points_against",   pa.int32(),   "Total points allowed"),
    _f("rank",             pa.int32(),   "Overall league rank"),
    _f("conference_rank",  pa.int32(),   "Rank within conference"),
    _f("overall_rank",     pa.int32(),   "Overall rank (alternative)"),
    _f("streak",           pa.string(),  "Current win/loss streak (e.g. W3)"),
    _f("home_record",      pa.string(),  "Home record (e.g. 10-5)"),
    _f("away_record",      pa.string(),  "Away record (e.g. 8-7)"),
    _f("last_ten",         pa.string(),  "Record over last 10 games"),
    _f("clinch_status",    pa.string(),  "Playoff clinch status"),
    # Provenance
    _f("source",           pa.string(),  "Data vendor provenance",           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. plays — partition: season=
# ═══════════════════════════════════════════════════════════════════════

WNBA_PLAYS_SCHEMA = pa.schema([
    _f("game_id",       pa.string(), "Game identifier",                     nullable=False),
    _f("season",        pa.int32(),  "Season year"),
    _f("date",          pa.string(), "Game date (YYYY-MM-DD)"),
    _f("period",        pa.int32(),  "Game period / quarter"),
    _f("clock",         pa.string(), "Game clock at time of event"),
    _f("event_type",    pa.string(), "Event type (e.g. shot, foul, turnover)"),
    _f("description",   pa.string(), "Human-readable play description"),
    _f("team_id",       pa.string(), "Team identifier"),
    _f("team_name",     pa.string(), "Team name"),
    _f("player_id",     pa.string(), "Player identifier"),
    _f("player_name",   pa.string(), "Player display name"),
    _f("score_home",    pa.int32(),  "Running home score after play"),
    _f("score_away",    pa.int32(),  "Running away score after play"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",              nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. injuries — partition: season=
# ═══════════════════════════════════════════════════════════════════════

WNBA_INJURIES_SCHEMA = pa.schema([
    _f("player_id",     pa.string(), "Player identifier",                   nullable=False),
    _f("player_name",   pa.string(), "Player display name"),
    _f("team_id",       pa.string(), "Team identifier"),
    _f("team_name",     pa.string(), "Team name"),
    _f("season",        pa.int32(),  "Season year"),
    _f("date",          pa.string(), "Report date (YYYY-MM-DD)"),
    _f("status",        pa.string(), "Injury status (out/doubtful/questionable/probable)"),
    _f("injury_type",   pa.string(), "Type of injury (e.g. knee, ankle)"),
    _f("detail",        pa.string(), "Additional injury detail"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",              nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. leaders — partition: season=
# ═══════════════════════════════════════════════════════════════════════

WNBA_LEADERS_SCHEMA = pa.schema([
    _f("player_id",     pa.string(), "Player identifier",                   nullable=False),
    _f("player_name",   pa.string(), "Player display name"),
    _f("team_id",       pa.string(), "Team identifier"),
    _f("team_name",     pa.string(), "Team name"),
    _f("season",        pa.int32(),  "Season year"),
    _f("category",      pa.string(), "Statistical category (e.g. points, rebounds)"),
    _f("value",         pa.float64(),"Leader value for category"),
    _f("rank",          pa.int32(),  "Rank within category"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",              nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

WNBA_VENUES_SCHEMA = pa.schema([
    _f("venue_id",      pa.string(), "Unique venue identifier",             nullable=False),
    _f("name",          pa.string(), "Venue name",                          nullable=False),
    _f("city",          pa.string(), "City"),
    _f("state",         pa.string(), "State or province"),
    _f("country",       pa.string(), "Country"),
    _f("capacity",      pa.int32(),  "Seating capacity"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",              nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. advanced — partition: season=
#     Discriminator: scope (game | season)
# ═══════════════════════════════════════════════════════════════════════

WNBA_ADVANCED_SCHEMA = pa.schema([
    _f("player_id",          pa.string(),  "Player identifier",             nullable=False),
    _f("player_name",        pa.string(),  "Player display name"),
    _f("team_id",            pa.string(),  "Team identifier"),
    _f("team_name",          pa.string(),  "Team name"),
    _f("season",             pa.int32(),   "Season year"),
    _f("game_id",            pa.string(),  "Game identifier (null for season scope)"),
    _f("date",               pa.string(),  "Game date (YYYY-MM-DD)"),
    _f("scope",              pa.string(),  "Record scope: game or season"),
    _f("per",                pa.float64(), "Player Efficiency Rating"),
    _f("ts_pct",             pa.float64(), "True shooting percentage"),
    _f("efg_pct",            pa.float64(), "Effective field goal percentage"),
    _f("offensive_rating",   pa.float64(), "Offensive rating (points per 100 possessions)"),
    _f("defensive_rating",   pa.float64(), "Defensive rating (points allowed per 100 possessions)"),
    _f("net_rating",         pa.float64(), "Net rating (offensive − defensive)"),
    _f("ast_pct",            pa.float64(), "Assist percentage"),
    _f("reb_pct",            pa.float64(), "Rebound percentage"),
    _f("stl_pct",            pa.float64(), "Steal percentage"),
    _f("blk_pct",            pa.float64(), "Block percentage"),
    _f("tov_pct",            pa.float64(), "Turnover percentage"),
    _f("usg_pct",            pa.float64(), "Usage percentage"),
    _f("pace",               pa.float64(), "Pace (possessions per 40 minutes)"),
    _f("pie",                pa.float64(), "Player Impact Estimate"),
    # Provenance
    _f("source",             pa.string(),  "Data vendor provenance",        nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. box_scores — partition: season=
# ═══════════════════════════════════════════════════════════════════════

WNBA_BOX_SCORES_SCHEMA = pa.schema([
    _f("game_id",                  pa.string(),  "Game identifier",          nullable=False),
    _f("player_id",                pa.string(),  "Player identifier",        nullable=False),
    _f("player_name",              pa.string(),  "Player display name"),
    _f("team_id",                  pa.string(),  "Team identifier"),
    _f("team_name",                pa.string(),  "Team name"),
    _f("season",                   pa.int32(),   "Season year"),
    _f("date",                     pa.string(),  "Game date (YYYY-MM-DD)"),
    _f("starter",                  pa.bool_(),   "Whether the player started the game"),
    _f("minutes",                  pa.float64(), "Minutes played"),
    _f("points",                   pa.int32(),   "Points scored"),
    _f("rebounds",                 pa.int32(),   "Total rebounds"),
    _f("assists",                  pa.int32(),   "Assists"),
    _f("steals",                   pa.int32(),   "Steals"),
    _f("blocks",                   pa.int32(),   "Blocks"),
    _f("turnovers",                pa.int32(),   "Turnovers"),
    _f("field_goals_made",         pa.int32(),   "Field goals made"),
    _f("field_goals_attempted",    pa.int32(),   "Field goals attempted"),
    _f("field_goal_pct",           pa.float64(), "Field goal percentage"),
    _f("three_pointers_made",      pa.int32(),   "Three-pointers made"),
    _f("three_pointers_attempted", pa.int32(),   "Three-pointers attempted"),
    _f("three_point_pct",          pa.float64(), "Three-point percentage"),
    _f("free_throws_made",         pa.int32(),   "Free throws made"),
    _f("free_throws_attempted",    pa.int32(),   "Free throws attempted"),
    _f("free_throw_pct",           pa.float64(), "Free throw percentage"),
    _f("offensive_rebounds",       pa.int32(),   "Offensive rebounds"),
    _f("defensive_rebounds",       pa.int32(),   "Defensive rebounds"),
    _f("personal_fouls",           pa.int32(),   "Personal fouls"),
    _f("plus_minus",               pa.float64(), "Plus/minus rating"),
    # Provenance
    _f("source",                   pa.string(),  "Data vendor provenance",   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Schema registry — entity name → PyArrow schema
# ═══════════════════════════════════════════════════════════════════════

WNBA_SCHEMAS: dict[str, pa.Schema] = {
    "teams":         WNBA_TEAMS_SCHEMA,
    "players":       WNBA_PLAYERS_SCHEMA,
    "games":         WNBA_GAMES_SCHEMA,
    "player_stats":  WNBA_PLAYER_STATS_SCHEMA,
    "team_stats":    WNBA_TEAM_STATS_SCHEMA,
    "standings":     WNBA_STANDINGS_SCHEMA,
    "plays":         WNBA_PLAYS_SCHEMA,
    "injuries":      WNBA_INJURIES_SCHEMA,
    "leaders":       WNBA_LEADERS_SCHEMA,
    "venues":        WNBA_VENUES_SCHEMA,
    "advanced":      WNBA_ADVANCED_SCHEMA,
    "box_scores":    WNBA_BOX_SCORES_SCHEMA,
}


# ═══════════════════════════════════════════════════════════════════════
# Partition keys per entity
# ═══════════════════════════════════════════════════════════════════════

WNBA_ENTITY_PARTITIONS: dict[str, list[str]] = {
    "teams":         [],
    "players":       ["season"],
    "games":         ["season"],
    "player_stats":  ["season"],
    "team_stats":    ["season"],
    "standings":     ["season"],
    "plays":         ["season"],
    "injuries":      ["season"],
    "leaders":       ["season"],
    "venues":        [],
    "advanced":      ["season"],
    "box_scores":    ["season"],
}


# ═══════════════════════════════════════════════════════════════════════
# Normalizer data-type → entity routing
# ═══════════════════════════════════════════════════════════════════════
# Maps the normalizer's data-type names (the prefix of {type}_{season}.parquet)
# to the flat entity directory name under normalized_curated/wnba/.
# Types mapping to None are intentionally skipped (non-entity artefacts).

WNBA_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "games":                "games",
    "teams":                "teams",
    "players":              "players",
    "player_stats":         "player_stats",
    "team_stats":           "team_stats",
    "standings":            "standings",
    "odds":                 None,
    "odds_history":         None,
    "player_props":         None,
    "injuries":             "injuries",
    "play_by_play":         "plays",
    # Aliases / absorbed types
    "team_game_stats":      "team_stats",
    "roster":               "players",
    "info":                 "players",
    "scoreboard":           "games",
    "calendar":             "games",
    "game_box_advanced":    "box_scores",
    "stats_advanced":       "advanced",
    "stats_season":         "player_stats",
    "stats_player_season":  "player_stats",
    "stats_categories":     None,
    # Non-entity normalizer artefacts — skip
    "coaches":              None,
    "news":                 None,
    "weather":              None,
    "market_signals":       None,
    "schedule_fatigue":     None,
    "transactions":         None,
}


# ═══════════════════════════════════════════════════════════════════════
# Entity allow-list and static entities
# ═══════════════════════════════════════════════════════════════════════

WNBA_ENTITY_ALLOWLIST: set[str] = {
    "teams",
    "players",
    "games",
    "player_stats",
    "team_stats",
    "standings",
    "plays",
    "injuries",
    "leaders",
    "venues",
    "advanced",
    "box_scores",
}

WNBA_STATIC_ENTITIES: set[str] = {"teams", "venues"}
