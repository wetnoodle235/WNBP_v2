# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — Soccer Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 13-entity consolidated design.  Raw data from BDL EPL API and other
# soccer data providers are merged into 13 wide schemas that use
# discriminator columns (``stat_type``, ``event_type``, ``prop_type``)
# to distinguish record subtypes within a single table.
#
# Entity overview
# ───────────────
#  1.  teams            — static reference, no partitioning
#  2.  players          — partition: season=
#  3.  games            — partition: season=
#  4.  standings        — partition: season=
#  5.  odds             — partition: season=
#  6.  player_props     — partition: season=
#  7.  rosters          — partition: season=
#  8.  lineups          — partition: season=
#  9.  match_events     — partition: season=
# 10.  player_stats     — partition: season=
# 11.  team_stats       — partition: season=
# 12.  venues           — static reference, no partitioning
# 13.  coaches          — partition: season=
#
# Shared across all soccer leagues: epl, laliga, bundesliga, seriea,
# ligue1, mls, ucl, europa, ligamx, nwsl, eredivisie, primeiraliga,
# championship, bundesliga2, serieb, ligue2, worldcup, euros.
#
# Soccer does NOT use week-based partitioning — games are date-based.
# Season is always the start year integer (e.g. 2024 for "2024-25").
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

SOCCER_TEAMS_SCHEMA = pa.schema([
    # Core identity
    _f("team_id",        pa.int32(),  "Unique team identifier (BDL id)",          nullable=False),
    _f("name",           pa.string(), "Full team name",                           nullable=False),
    _f("short_name",     pa.string(), "Short team name"),
    _f("abbreviation",   pa.string(), "Team abbreviation (e.g. ARS, MCI)"),
    _f("location",       pa.string(), "City / location of team"),
    _f("league",         pa.string(), "League slug (e.g. epl, laliga)"),
    _f("logo_url",       pa.string(), "URL to team crest / logo image"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. players — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_PLAYERS_SCHEMA = pa.schema([
    # Core identity
    _f("player_id",      pa.int32(),  "Unique player identifier (BDL id)",        nullable=False),
    _f("first_name",     pa.string(), "First name"),
    _f("last_name",      pa.string(), "Last name"),
    _f("display_name",   pa.string(), "Full display name",                        nullable=False),
    _f("short_name",     pa.string(), "Short display name"),
    _f("date_of_birth",  pa.string(), "Date of birth (YYYY-MM-DD)"),
    _f("age",            pa.int32(),  "Age in years"),
    _f("height",         pa.string(), "Height (e.g. 183 cm)"),
    _f("weight",         pa.string(), "Weight (e.g. 76 kg)"),
    _f("citizenship",    pa.string(), "Country of citizenship"),
    _f("team_id",        pa.int32(),  "Current primary team identifier"),
    _f("team_name",      pa.string(), "Current primary team name"),
    _f("position",       pa.string(), "Primary position (GK, DF, MF, FW)"),
    # Partition key
    _f("season",         pa.int32(),  "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. games (matches) — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_GAMES_SCHEMA = pa.schema([
    # Core identity
    _f("game_id",        pa.int32(),  "Unique match identifier (BDL id)",         nullable=False),
    _f("name",           pa.string(), "Match name (e.g. Arsenal vs Chelsea)"),
    _f("short_name",     pa.string(), "Short match name"),
    # Teams
    _f("home_team_id",   pa.int32(),  "Home team identifier"),
    _f("home_team_name", pa.string(), "Home team name"),
    _f("away_team_id",   pa.int32(),  "Away team identifier"),
    _f("away_team_name", pa.string(), "Away team name"),
    # Schedule
    _f("date",           pa.string(), "Match date (YYYY-MM-DD)"),
    _f("time",           pa.string(), "Match kickoff time (UTC)"),
    # Status
    _f("status",         pa.string(), "Match status code (e.g. FT, HT, NS)"),
    _f("status_detail",  pa.string(), "Human-readable status detail"),
    # Scores
    _f("home_score",     pa.int32(),  "Home team final score"),
    _f("away_score",     pa.int32(),  "Away team final score"),
    _f("home_ht_score",  pa.int32(),  "Home team half-time score"),
    _f("away_ht_score",  pa.int32(),  "Away team half-time score"),
    # Venue
    _f("venue_name",     pa.string(), "Venue / stadium name"),
    _f("venue_city",     pa.string(), "Venue city"),
    _f("attendance",     pa.int32(),  "Match attendance"),
    # Matchday / round
    _f("matchday",       pa.int32(),  "Matchday or round number"),
    _f("league",         pa.string(), "League slug (e.g. epl, ucl)"),
    # Partition key
    _f("season",         pa.int32(),  "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. standings — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_STANDINGS_SCHEMA = pa.schema([
    # Team reference
    _f("team_id",            pa.int32(),   "Team identifier",                     nullable=False),
    _f("team_name",          pa.string(),  "Team name"),
    # Rankings
    _f("rank",               pa.int32(),   "Current league rank / position"),
    _f("rank_change",        pa.int32(),   "Rank change since last matchday"),
    _f("group_name",         pa.string(),  "Group name (for group-stage competitions)"),
    _f("note",               pa.string(),  "Qualification / relegation note"),
    # Record
    _f("games_played",       pa.int32(),   "Total games played"),
    _f("wins",               pa.int32(),   "Total wins"),
    _f("losses",             pa.int32(),   "Total losses"),
    _f("draws",              pa.int32(),   "Total draws"),
    _f("points",             pa.int32(),   "League points"),
    _f("points_per_game",    pa.float64(), "Points per game"),
    # Goals
    _f("goals_for",          pa.int32(),   "Goals scored"),
    _f("goals_against",      pa.int32(),   "Goals conceded"),
    _f("goal_differential",  pa.int32(),   "Goal difference (GF − GA)"),
    # Form
    _f("form",               pa.string(),  "Recent form string (e.g. WWDLW)"),
    _f("league",             pa.string(),  "League slug"),
    # Partition key
    _f("season",             pa.int32(),   "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",             pa.string(),  "Data vendor provenance",              nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. odds — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_ODDS_SCHEMA = pa.schema([
    # Identity
    _f("odds_id",                pa.int32(),   "Unique odds record identifier",   nullable=False),
    _f("game_id",                pa.int32(),   "Match identifier",                nullable=False),
    _f("vendor",                 pa.string(),  "Sportsbook / odds vendor name"),
    # Moneyline / 1X2
    _f("moneyline_home_odds",    pa.float64(), "Moneyline odds — home win"),
    _f("moneyline_away_odds",    pa.float64(), "Moneyline odds — away win"),
    _f("moneyline_draw_odds",    pa.float64(), "Moneyline odds — draw"),
    # Spread / Asian handicap
    _f("spread_home",            pa.float64(), "Home spread / handicap line"),
    _f("spread_home_odds",       pa.float64(), "Odds on home spread"),
    _f("spread_away_odds",       pa.float64(), "Odds on away spread"),
    # Totals
    _f("total_line",             pa.float64(), "Over/under total goals line"),
    _f("total_over_odds",        pa.float64(), "Odds on over"),
    _f("total_under_odds",       pa.float64(), "Odds on under"),
    # Timestamps
    _f("updated_at",             pa.string(),  "Odds last updated timestamp (ISO 8601)"),
    # Partition key
    _f("season",                 pa.int32(),   "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",                 pa.string(),  "Data vendor provenance",          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. player_props — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_PLAYER_PROPS_SCHEMA = pa.schema([
    # Identity
    _f("prop_id",        pa.int32(),   "Unique player prop identifier",           nullable=False),
    _f("game_id",        pa.int32(),   "Match identifier",                        nullable=False),
    _f("player_id",      pa.int32(),   "Player identifier",                       nullable=False),
    _f("vendor",         pa.string(),  "Sportsbook / odds vendor name"),
    # Prop details
    _f("prop_type",      pa.string(),  "Prop type (e.g. anytime_goalscorer, shots_on_target)"),
    _f("line_value",     pa.float64(), "Prop line value"),
    _f("over_odds",      pa.float64(), "Odds on over"),
    _f("under_odds",     pa.float64(), "Odds on under"),
    _f("odds",           pa.float64(), "Flat odds (e.g. anytime goalscorer)"),
    # Timestamps
    _f("updated_at",     pa.string(),  "Prop last updated timestamp (ISO 8601)"),
    # Partition key
    _f("season",         pa.int32(),   "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",                 nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. rosters — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_ROSTERS_SCHEMA = pa.schema([
    # References
    _f("team_id",                pa.int32(),  "Team identifier",                  nullable=False),
    _f("player_id",              pa.int32(),  "Player identifier",                nullable=False),
    _f("player_name",            pa.string(), "Player display name"),
    # Roster details
    _f("jersey_number",          pa.int32(),  "Squad jersey number"),
    _f("position",               pa.string(), "Position (e.g. Goalkeeper, Defender, Midfielder, Forward)"),
    _f("position_abbreviation",  pa.string(), "Position abbreviation (GK, DF, MF, FW)"),
    _f("is_active",              pa.bool_(),  "Whether player is active on roster"),
    # Partition key
    _f("season",                 pa.int32(),  "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",                 pa.string(), "Data vendor provenance",           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. lineups — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_LINEUPS_SCHEMA = pa.schema([
    # References
    _f("game_id",                pa.int32(),  "Match identifier",                 nullable=False),
    _f("team_id",                pa.int32(),  "Team identifier",                  nullable=False),
    _f("player_id",              pa.int32(),  "Player identifier",                nullable=False),
    _f("player_name",            pa.string(), "Player display name"),
    # Lineup details
    _f("is_starter",             pa.bool_(),  "Whether player is in starting XI"),
    _f("position",               pa.string(), "Match-day position"),
    _f("position_abbreviation",  pa.string(), "Position abbreviation (GK, DF, MF, FW)"),
    _f("formation_position",     pa.string(), "Position in formation (e.g. LCB, RW, ST)"),
    _f("jersey_number",          pa.int32(),  "Jersey number"),
    # Partition key
    _f("season",                 pa.int32(),  "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",                 pa.string(), "Data vendor provenance",           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. match_events (plays) — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_MATCH_EVENTS_SCHEMA = pa.schema([
    # Identity
    _f("event_id",             pa.int32(),  "Unique event identifier",            nullable=False),
    _f("game_id",              pa.int32(),  "Match identifier",                   nullable=False),
    _f("team_id",              pa.int32(),  "Team identifier"),
    # Event details
    _f("event_type",           pa.string(), "Event type (goal, yellow_card, red_card, substitution, etc.)"),
    _f("event_time",           pa.int32(),  "Match minute of the event"),
    _f("period",               pa.string(), "Match period (1H, 2H, ET1, ET2, PEN)"),
    _f("player_id",            pa.int32(),  "Primary player involved"),
    _f("player_name",          pa.string(), "Primary player display name"),
    _f("secondary_player_id",  pa.int32(),  "Secondary player (e.g. assist provider, sub on)"),
    _f("secondary_player_name", pa.string(), "Secondary player display name"),
    # Goal specifics
    _f("goal_type",            pa.string(), "Goal type (penalty, free_kick, header, open_play)"),
    _f("is_own_goal",          pa.bool_(),  "Whether the goal was an own goal"),
    # Partition key
    _f("season",               pa.int32(),  "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",               pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. player_stats — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_PLAYER_STATS_SCHEMA = pa.schema([
    # References
    _f("game_id",            pa.int32(),  "Match identifier",                     nullable=False),
    _f("player_id",          pa.int32(),  "Player identifier",                    nullable=False),
    _f("team_id",            pa.int32(),  "Team identifier"),
    _f("player_name",        pa.string(), "Player display name"),
    # Appearance
    _f("appearances",        pa.int32(),  "Appearances (1 if played, 0 otherwise)"),
    _f("minutes_played",     pa.int32(),  "Minutes played"),
    # Attacking
    _f("goals",              pa.int32(),  "Goals scored"),
    _f("assists",            pa.int32(),  "Assists"),
    _f("shots_total",        pa.int32(),  "Total shots"),
    _f("shots_on_target",    pa.int32(),  "Shots on target"),
    # Defensive / discipline
    _f("fouls_committed",    pa.int32(),  "Fouls committed"),
    _f("fouls_suffered",     pa.int32(),  "Fouls suffered"),
    _f("offsides",           pa.int32(),  "Offsides"),
    _f("saves",              pa.int32(),  "Saves (goalkeeper)"),
    _f("yellow_cards",       pa.int32(),  "Yellow cards"),
    _f("red_cards",          pa.int32(),  "Red cards"),
    _f("own_goals",          pa.int32(),  "Own goals"),
    # Passing
    _f("passes",             pa.int32(),  "Total passes attempted"),
    _f("pass_accuracy_pct",  pa.float64(), "Pass accuracy percentage"),
    _f("key_passes",         pa.int32(),  "Key passes (leading to a shot)"),
    # Dribbling / duels
    _f("tackles",            pa.int32(),  "Tackles won"),
    _f("interceptions",      pa.int32(),  "Interceptions"),
    _f("clearances",         pa.int32(),  "Clearances"),
    # Discriminator
    _f("stat_type",          pa.string(), "Stat scope — match, season_total, per_90"),
    # Partition key
    _f("season",             pa.int32(),  "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",             pa.string(), "Data vendor provenance",               nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. team_stats — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_TEAM_STATS_SCHEMA = pa.schema([
    # References
    _f("game_id",            pa.int32(),   "Match identifier",                    nullable=False),
    _f("team_id",            pa.int32(),   "Team identifier",                     nullable=False),
    _f("team_name",          pa.string(),  "Team name"),
    # Possession
    _f("possession_pct",     pa.float64(), "Ball possession percentage"),
    # Shots
    _f("shots",              pa.int32(),   "Total shots"),
    _f("shots_on_target",    pa.int32(),   "Shots on target"),
    _f("shots_off_target",   pa.int32(),   "Shots off target"),
    _f("blocked_shots",      pa.int32(),   "Blocked shots"),
    # Discipline
    _f("fouls",              pa.int32(),   "Fouls committed"),
    _f("yellow_cards",       pa.int32(),   "Yellow cards"),
    _f("red_cards",          pa.int32(),   "Red cards"),
    # Set pieces
    _f("corners",            pa.int32(),   "Corner kicks"),
    _f("offsides",           pa.int32(),   "Offsides"),
    # Passing
    _f("passes",             pa.int32(),   "Total passes"),
    _f("pass_accuracy_pct",  pa.float64(), "Pass accuracy percentage"),
    # Other
    _f("tackles",            pa.int32(),   "Tackles"),
    _f("interceptions",      pa.int32(),   "Interceptions"),
    _f("saves",              pa.int32(),   "Goalkeeper saves"),
    # Discriminator
    _f("stat_type",          pa.string(),  "Stat scope — match, season_total, per_game_avg"),
    # Partition key
    _f("season",             pa.int32(),   "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",             pa.string(),  "Data vendor provenance",              nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

SOCCER_VENUES_SCHEMA = pa.schema([
    # Core identity
    _f("venue_id",   pa.int32(),  "Unique venue identifier",                      nullable=False),
    _f("name",       pa.string(), "Venue / stadium name",                         nullable=False),
    _f("city",       pa.string(), "City"),
    _f("state",      pa.string(), "State / region (if applicable)"),
    _f("country",    pa.string(), "Country"),
    _f("capacity",   pa.int32(),  "Seating capacity"),
    _f("surface",    pa.string(), "Playing surface (e.g. grass, artificial)"),
    # Provenance
    _f("source",     pa.string(), "Data vendor provenance",                       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. coaches — partition: season=
# ═══════════════════════════════════════════════════════════════════════

SOCCER_COACHES_SCHEMA = pa.schema([
    # Core identity
    _f("coach_id",   pa.int32(),  "Unique coach identifier",                      nullable=False),
    _f("name",       pa.string(), "Coach full name",                              nullable=False),
    _f("team_id",    pa.int32(),  "Team identifier"),
    _f("team_name",  pa.string(), "Team name"),
    _f("nationality", pa.string(), "Coach nationality"),
    # Partition key
    _f("season",     pa.int32(),  "Season start year (e.g. 2024 for 2024-25)",    nullable=False),
    # Provenance
    _f("source",     pa.string(), "Data vendor provenance",                       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Registry — schema, partition key, and path look-ups
# ═══════════════════════════════════════════════════════════════════════

SOCCER_SCHEMAS: dict[str, pa.Schema] = {
    "teams":         SOCCER_TEAMS_SCHEMA,
    "players":       SOCCER_PLAYERS_SCHEMA,
    "games":         SOCCER_GAMES_SCHEMA,
    "standings":     SOCCER_STANDINGS_SCHEMA,
    "odds":          SOCCER_ODDS_SCHEMA,
    "player_props":  SOCCER_PLAYER_PROPS_SCHEMA,
    "rosters":       SOCCER_ROSTERS_SCHEMA,
    "lineups":       SOCCER_LINEUPS_SCHEMA,
    "match_events":  SOCCER_MATCH_EVENTS_SCHEMA,
    "player_stats":  SOCCER_PLAYER_STATS_SCHEMA,
    "team_stats":    SOCCER_TEAM_STATS_SCHEMA,
    "venues":        SOCCER_VENUES_SCHEMA,
    "coaches":       SOCCER_COACHES_SCHEMA,
}

SOCCER_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning (single flat parquets)
    "teams":         [],
    "venues":        [],
    # Season only — soccer uses dates, not weeks
    "players":       ["season"],
    "games":         ["season"],
    "standings":     ["season"],
    "odds":          ["season"],
    "player_props":  ["season"],
    "rosters":       ["season"],
    "lineups":       ["season"],
    "match_events":  ["season"],
    "player_stats":  ["season"],
    "team_stats":    ["season"],
    "coaches":       ["season"],
}

SOCCER_ENTITY_PATHS: dict[str, str] = {
    "teams":         "teams",
    "players":       "players",
    "games":         "games",
    "standings":     "standings",
    "odds":          "odds",
    "player_props":  "player_props",
    "rosters":       "rosters",
    "lineups":       "lineups",
    "match_events":  "match_events",
    "player_stats":  "player_stats",
    "team_stats":    "team_stats",
    "venues":        "venues",
    "coaches":       "coaches",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names (the prefix of {type}_{season}.parquet)
# to the flat entity directory name under normalized_curated/[league]/.
# Types mapping to None are intentionally skipped (non-entity artefacts).

SOCCER_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "teams":            "teams",
    "players":          "players",
    "player_stats":     "player_stats",
    "team_stats":       "team_stats",
    "standings":        "standings",
    "odds":             "odds",
    "odds_history":     "odds",
    "player_props":     "player_props",
    "lineups":          "lineups",
    "venues":           "venues",
    "coaches":          "coaches",
    # Aliases / absorbed types
    "roster":           "rosters",
    "schedule":         "games",
    "scores":           "games",
    "plays":            "match_events",
    "play_by_play":     "match_events",
    "game_events":      "match_events",
    # Non-entity normalizer artefacts — skip
    "injuries":         None,
    "news":             None,
    "weather":          None,
    "market_signals":   None,
    "depth_charts":     None,
}


# ── Entity allow-list and static entities ─────────────────────────────

SOCCER_ENTITY_ALLOWLIST: set[str] = {
    "teams",
    "players",
    "games",
    "standings",
    "odds",
    "player_props",
    "rosters",
    "lineups",
    "match_events",
    "player_stats",
    "team_stats",
    "venues",
    "coaches",
}

SOCCER_STATIC_ENTITIES: set[str] = {"teams", "venues"}
