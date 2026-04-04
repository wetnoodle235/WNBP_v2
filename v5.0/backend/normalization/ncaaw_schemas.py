# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — NCAAW Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 16-entity consolidated design.  Raw data from ESPN, NCAA Stats,
# Odds, and OddsAPI providers are merged into 16 wide schemas that
# use discriminator columns (``scope``, ``poll``, ``line_type``,
# ``prop_type``, ``event_type``) to distinguish record subtypes
# within a single table.
#
# Entity overview
# ───────────────
#  1. conferences      — static reference, no partitioning
#  2. teams            — static reference, no partitioning
#  3. players          — partition: season=
#  4. games            — partition: season=
#  5. player_stats     — partition: season=   (game + season scope)
#  6. team_stats       — partition: season=   (game + season scope)
#  7. standings        — partition: season=
#  8. rankings         — partition: season=
#  9. odds             — partition: season=
# 10. player_props     — partition: season=
# 11. plays            — partition: season=
# 12. injuries         — partition: season=
# 13. bracket          — partition: season=
# 14. leaders          — partition: season=
# 15. venues           — static reference, no partitioning
# 16. advanced         — partition: season=
#
# NCAAW uses date-based games (no week partitioning).
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
# 1. conferences — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

schema_conferences = pa.schema([
    _f("conference_id",   pa.string(), "Unique conference identifier",                         nullable=False),
    _f("name",            pa.string(), "Full conference name",                                 nullable=False),
    _f("abbreviation",    pa.string(), "Short abbreviation (e.g. SEC, B1G)",                   nullable=False),
    _f("classification",  pa.string(), "Conference tier — power_five/group_of_five/independent/other"),
    _f("source",          pa.string(), "Data vendor provenance",                               nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. teams — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

schema_teams = pa.schema([
    # Core identity
    _f("team_id",         pa.string(), "Unique team identifier",                     nullable=False),
    _f("name",            pa.string(), "Short team name (e.g. Gamecocks)",           nullable=False),
    _f("abbreviation",    pa.string(), "Team abbreviation (e.g. SC)",                nullable=False),
    _f("mascot",          pa.string(), "Team mascot name"),
    _f("conference",      pa.string(), "Conference name or abbreviation"),
    _f("division",        pa.string(), "Division within conference"),
    _f("city",            pa.string(), "City where team is located"),
    _f("state",           pa.string(), "State where team is located"),
    _f("logo_url",        pa.string(), "URL to team logo image"),
    _f("primary_color",   pa.string(), "Primary team colour hex code"),
    _f("arena",           pa.string(), "Home arena / venue name"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                     nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. players — partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_players = pa.schema([
    # Core identity
    _f("player_id",       pa.string(), "Unique player identifier",                   nullable=False),
    _f("name",            pa.string(), "Full display name",                          nullable=False),
    _f("first_name",      pa.string(), "Player first name"),
    _f("last_name",       pa.string(), "Player last name"),
    _f("team_id",         pa.string(), "Current team identifier"),
    _f("team_name",       pa.string(), "Current team name"),
    _f("position",        pa.string(), "Position (e.g. PG, SG, SF, PF, C)"),
    _f("jersey_number",   pa.string(), "Jersey number as string"),
    _f("height",          pa.string(), "Height (e.g. 6-1)"),
    _f("weight",          pa.int32(),  "Weight in pounds"),
    _f("class_year",      pa.string(), "Academic class year — Fr/So/Jr/Sr/Grad"),
    _f("hometown",        pa.string(), "Player hometown"),
    _f("state",           pa.string(), "Home state"),
    _f("high_school",     pa.string(), "High school attended"),
    # Partition / provenance
    _f("season",          pa.int32(),  "Season start year (e.g. 2024 for 2024-25)",  nullable=False),
    _f("source",          pa.string(), "Data vendor provenance",                     nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. games — partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_games = pa.schema([
    # Core identity
    _f("game_id",         pa.string(), "Unique game identifier",                     nullable=False),
    _f("season",          pa.int32(),  "Season start year",                          nullable=False),
    _f("date",            pa.string(), "Game date (ISO-8601)",                       nullable=False),
    _f("start_time",      pa.string(), "Scheduled start time"),
    _f("status",          pa.string(), "Game status (e.g. final, in_progress, scheduled)"),
    # Teams
    _f("home_team_id",    pa.string(), "Home team identifier"),
    _f("home_team",       pa.string(), "Home team name"),
    _f("away_team_id",    pa.string(), "Away team identifier"),
    _f("away_team",       pa.string(), "Away team name"),
    # Scores
    _f("home_score",      pa.int32(),  "Home team total score"),
    _f("away_score",      pa.int32(),  "Away team total score"),
    # Venue / context
    _f("venue",           pa.string(), "Arena / venue name"),
    _f("season_type",     pa.string(), "Season type — regular/postseason/preseason/tournament"),
    _f("conference_game", pa.bool_(),  "Whether this is a conference game"),
    _f("overtime",        pa.bool_(),  "Whether the game went to overtime"),
    # Quarter scores (NCAAW plays four 10-minute quarters)
    _f("home_q1",         pa.int32(),  "Home 1st quarter score"),
    _f("home_q2",         pa.int32(),  "Home 2nd quarter score"),
    _f("home_q3",         pa.int32(),  "Home 3rd quarter score"),
    _f("home_q4",         pa.int32(),  "Home 4th quarter score"),
    _f("away_q1",         pa.int32(),  "Away 1st quarter score"),
    _f("away_q2",         pa.int32(),  "Away 2nd quarter score"),
    _f("away_q3",         pa.int32(),  "Away 3rd quarter score"),
    _f("away_q4",         pa.int32(),  "Away 4th quarter score"),
    # Metadata
    _f("attendance",      pa.int32(),  "Reported attendance"),
    _f("broadcast",       pa.string(), "TV broadcast network"),
    # Derived
    _f("result",          pa.string(), "Game result summary"),
    _f("score_diff",      pa.int32(),  "Score differential (home − away)"),
    _f("total_score",     pa.int32(),  "Combined total score"),
    _f("day_of_week",     pa.string(), "Day of week (e.g. Saturday)"),
    _f("is_weekend",      pa.bool_(),  "Whether the game is on a weekend"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                     nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. player_stats — partition: season=
#    Discriminator: scope (game | season)
# ═══════════════════════════════════════════════════════════════════════

schema_player_stats = pa.schema([
    # Identity
    _f("player_id",               pa.string(),  "Unique player identifier",           nullable=False),
    _f("player_name",             pa.string(),  "Player display name"),
    _f("team_id",                 pa.string(),  "Team identifier"),
    _f("team_name",               pa.string(),  "Team name"),
    _f("season",                  pa.int32(),   "Season start year",                  nullable=False),
    _f("scope",                   pa.string(),  "Stat scope — game or season",        nullable=False),
    _f("game_id",                 pa.string(),  "Game identifier (null for season scope)"),
    _f("date",                    pa.string(),  "Game date (null for season scope)"),
    # Core box-score stats
    _f("minutes",                 pa.float64(), "Minutes played"),
    _f("points",                  pa.int32(),   "Points scored"),
    _f("rebounds",                pa.int32(),   "Total rebounds"),
    _f("assists",                 pa.int32(),   "Assists"),
    _f("steals",                  pa.int32(),   "Steals"),
    _f("blocks",                  pa.int32(),   "Blocks"),
    _f("turnovers",              pa.int32(),   "Turnovers"),
    # Shooting splits
    _f("field_goals_made",        pa.int32(),   "Field goals made"),
    _f("field_goals_attempted",   pa.int32(),   "Field goals attempted"),
    _f("field_goal_pct",          pa.float64(), "Field goal percentage"),
    _f("three_pointers_made",     pa.int32(),   "Three-pointers made"),
    _f("three_pointers_attempted", pa.int32(),  "Three-pointers attempted"),
    _f("three_point_pct",         pa.float64(), "Three-point percentage"),
    _f("free_throws_made",        pa.int32(),   "Free throws made"),
    _f("free_throws_attempted",   pa.int32(),   "Free throws attempted"),
    _f("free_throw_pct",          pa.float64(), "Free throw percentage"),
    # Rebound splits
    _f("offensive_rebounds",      pa.int32(),   "Offensive rebounds"),
    _f("defensive_rebounds",      pa.int32(),   "Defensive rebounds"),
    # Misc
    _f("personal_fouls",          pa.int32(),   "Personal fouls"),
    _f("plus_minus",              pa.float64(), "Plus/minus rating"),
    _f("games_played",            pa.int32(),   "Games played (season scope)"),
    # Provenance
    _f("source",                  pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. team_stats — partition: season=
#    Discriminator: scope (game | season)
# ═══════════════════════════════════════════════════════════════════════

schema_team_stats = pa.schema([
    # Identity
    _f("team_id",                 pa.string(),  "Unique team identifier",             nullable=False),
    _f("team_name",               pa.string(),  "Team name"),
    _f("season",                  pa.int32(),   "Season start year",                  nullable=False),
    _f("scope",                   pa.string(),  "Stat scope — game or season",        nullable=False),
    _f("game_id",                 pa.string(),  "Game identifier (null for season scope)"),
    _f("date",                    pa.string(),  "Game date (null for season scope)"),
    # Core stats
    _f("points",                  pa.int32(),   "Points scored"),
    _f("rebounds",                pa.int32(),   "Total rebounds"),
    _f("assists",                 pa.int32(),   "Assists"),
    _f("steals",                  pa.int32(),   "Steals"),
    _f("blocks",                  pa.int32(),   "Blocks"),
    _f("turnovers",              pa.int32(),   "Turnovers"),
    # Shooting splits
    _f("field_goals_made",        pa.int32(),   "Field goals made"),
    _f("field_goals_attempted",   pa.int32(),   "Field goals attempted"),
    _f("field_goal_pct",          pa.float64(), "Field goal percentage"),
    _f("three_pointers_made",     pa.int32(),   "Three-pointers made"),
    _f("three_pointers_attempted", pa.int32(),  "Three-pointers attempted"),
    _f("three_point_pct",         pa.float64(), "Three-point percentage"),
    _f("free_throws_made",        pa.int32(),   "Free throws made"),
    _f("free_throws_attempted",   pa.int32(),   "Free throws attempted"),
    _f("free_throw_pct",          pa.float64(), "Free throw percentage"),
    # Rebound splits
    _f("offensive_rebounds",      pa.int32(),   "Offensive rebounds"),
    _f("defensive_rebounds",      pa.int32(),   "Defensive rebounds"),
    # Misc
    _f("personal_fouls",          pa.int32(),   "Personal fouls"),
    _f("games_played",            pa.int32(),   "Games played (season scope)"),
    # Provenance
    _f("source",                  pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. standings — partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_standings = pa.schema([
    _f("team_id",          pa.string(),  "Unique team identifier",                    nullable=False),
    _f("team_name",        pa.string(),  "Team name"),
    _f("season",           pa.int32(),   "Season start year",                         nullable=False),
    _f("conference",       pa.string(),  "Conference name or abbreviation"),
    # Record
    _f("wins",             pa.int32(),   "Total wins"),
    _f("losses",           pa.int32(),   "Total losses"),
    _f("conf_wins",        pa.int32(),   "Conference wins"),
    _f("conf_losses",      pa.int32(),   "Conference losses"),
    _f("pct",              pa.float64(), "Win percentage"),
    _f("games_played",     pa.int32(),   "Games played"),
    # Scoring
    _f("points_for",       pa.int32(),   "Total points scored"),
    _f("points_against",   pa.int32(),   "Total points allowed"),
    # Rankings
    _f("rank",             pa.int32(),   "Overall rank"),
    _f("conference_rank",  pa.int32(),   "Rank within conference"),
    _f("overall_rank",     pa.int32(),   "National overall rank"),
    # Supplemental
    _f("streak",           pa.string(),  "Current streak (e.g. W5, L2)"),
    _f("home_record",      pa.string(),  "Home record (e.g. 12-3)"),
    _f("away_record",      pa.string(),  "Away record (e.g. 8-5)"),
    _f("last_ten",         pa.string(),  "Record over last 10 games"),
    _f("clinch_status",    pa.string(),  "Tournament/clinch status indicator"),
    # Provenance
    _f("source",           pa.string(),  "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. rankings — partition: season=
#    Discriminator: poll (ap | coaches | cfp | other)
# ═══════════════════════════════════════════════════════════════════════

schema_rankings = pa.schema([
    _f("team_id",            pa.string(), "Unique team identifier",                   nullable=False),
    _f("team_name",          pa.string(), "Team name"),
    _f("season",             pa.int32(),  "Season start year",                        nullable=False),
    _f("week",               pa.int32(),  "Poll week number"),
    _f("poll",               pa.string(), "Poll type — ap/coaches/cfp/other",         nullable=False),
    _f("rank",               pa.int32(),  "Rank in poll"),
    _f("first_place_votes",  pa.int32(),  "Number of first-place votes"),
    _f("points",             pa.int32(),  "Poll points received"),
    _f("previous_rank",      pa.int32(),  "Rank in previous week's poll"),
    _f("record",             pa.string(), "Team record at time of ranking"),
    # Provenance
    _f("source",             pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. odds — partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_odds = pa.schema([
    _f("game_id",          pa.string(),  "Unique game identifier",                    nullable=False),
    _f("season",           pa.int32(),   "Season start year",                         nullable=False),
    _f("date",             pa.string(),  "Game date (ISO-8601)"),
    _f("sportsbook",       pa.string(),  "Sportsbook name (e.g. DraftKings, FanDuel)"),
    # Teams
    _f("home_team",        pa.string(),  "Home team name"),
    _f("away_team",        pa.string(),  "Away team name"),
    # Spread
    _f("spread_home",      pa.float64(), "Home team spread"),
    _f("spread_away",      pa.float64(), "Away team spread"),
    _f("spread_home_odds", pa.int32(),   "Home spread American odds"),
    _f("spread_away_odds", pa.int32(),   "Away spread American odds"),
    # Moneyline
    _f("moneyline_home",   pa.int32(),   "Home moneyline"),
    _f("moneyline_away",   pa.int32(),   "Away moneyline"),
    # Totals
    _f("total_over",       pa.float64(), "Over/under line (over)"),
    _f("total_under",      pa.float64(), "Over/under line (under)"),
    _f("total_over_odds",  pa.int32(),   "Over American odds"),
    _f("total_under_odds", pa.int32(),   "Under American odds"),
    # Discriminator / metadata
    _f("line_type",        pa.string(),  "Line type — pregame/live/opening/closing"),
    _f("timestamp",        pa.string(),  "Timestamp of odds snapshot"),
    # Provenance
    _f("source",           pa.string(),  "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. player_props — partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_player_props = pa.schema([
    _f("game_id",       pa.string(), "Unique game identifier",                       nullable=False),
    _f("player_id",     pa.string(), "Unique player identifier",                     nullable=False),
    _f("player_name",   pa.string(), "Player display name"),
    _f("season",        pa.int32(),  "Season start year",                            nullable=False),
    _f("date",          pa.string(), "Game date (ISO-8601)"),
    _f("team",          pa.string(), "Team name"),
    _f("prop_type",     pa.string(), "Prop type (e.g. points, rebounds, assists)"),
    _f("line",          pa.float64(),"Prop line value"),
    _f("over_odds",     pa.int32(),  "Over American odds"),
    _f("under_odds",    pa.int32(),  "Under American odds"),
    _f("sportsbook",    pa.string(), "Sportsbook name"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",                       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. plays — partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_plays = pa.schema([
    _f("game_id",       pa.string(), "Unique game identifier",                       nullable=False),
    _f("season",        pa.int32(),  "Season start year",                            nullable=False),
    _f("date",          pa.string(), "Game date (ISO-8601)"),
    _f("period",        pa.int32(),  "Game period / quarter (1, 2, 3, 4, OT1, …)"),
    _f("clock",         pa.string(), "Game clock at time of play"),
    _f("event_type",    pa.string(), "Event type (e.g. made_shot, foul, turnover)"),
    _f("description",   pa.string(), "Human-readable play description"),
    _f("team_id",       pa.string(), "Team identifier for the play"),
    _f("team_name",     pa.string(), "Team name for the play"),
    _f("player_id",     pa.string(), "Player identifier for the play"),
    _f("player_name",   pa.string(), "Player name for the play"),
    _f("score_home",    pa.int32(),  "Home team score after the play"),
    _f("score_away",    pa.int32(),  "Away team score after the play"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",                       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. injuries — partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_injuries = pa.schema([
    _f("player_id",     pa.string(), "Unique player identifier",                     nullable=False),
    _f("player_name",   pa.string(), "Player display name"),
    _f("team_id",       pa.string(), "Team identifier"),
    _f("team_name",     pa.string(), "Team name"),
    _f("season",        pa.int32(),  "Season start year",                            nullable=False),
    _f("date",          pa.string(), "Report date (ISO-8601)"),
    _f("status",        pa.string(), "Injury status (e.g. out, day-to-day, questionable)"),
    _f("injury_type",   pa.string(), "Injury type (e.g. knee, ankle, concussion)"),
    _f("detail",        pa.string(), "Additional injury detail or notes"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",                       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. bracket — partition: season= (NCAA Tournament)
# ═══════════════════════════════════════════════════════════════════════

schema_bracket = pa.schema([
    _f("season",         pa.int32(),  "Season start year",                           nullable=False),
    _f("round",          pa.string(), "Tournament round (e.g. Round of 64, Sweet 16, Final Four)"),
    _f("region",         pa.string(), "Tournament region (e.g. Albany, Portland, Seattle, Greenville)"),
    _f("seed_home",      pa.int32(),  "Home/higher-seeded team seed"),
    _f("seed_away",      pa.int32(),  "Away/lower-seeded team seed"),
    _f("home_team_id",   pa.string(), "Home team identifier"),
    _f("home_team",      pa.string(), "Home team name"),
    _f("away_team_id",   pa.string(), "Away team identifier"),
    _f("away_team",      pa.string(), "Away team name"),
    _f("home_score",     pa.int32(),  "Home team final score"),
    _f("away_score",     pa.int32(),  "Away team final score"),
    _f("date",           pa.string(), "Game date (ISO-8601)"),
    _f("venue",          pa.string(), "Arena / venue name"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                      nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. leaders — partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_leaders = pa.schema([
    _f("player_id",     pa.string(),  "Unique player identifier",                    nullable=False),
    _f("player_name",   pa.string(),  "Player display name"),
    _f("team_id",       pa.string(),  "Team identifier"),
    _f("team_name",     pa.string(),  "Team name"),
    _f("season",        pa.int32(),   "Season start year",                           nullable=False),
    _f("category",      pa.string(),  "Statistical category (e.g. points, rebounds, assists)"),
    _f("value",         pa.float64(), "Statistical value"),
    _f("rank",          pa.int32(),   "Rank in category"),
    # Provenance
    _f("source",        pa.string(),  "Data vendor provenance",                      nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 15. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

schema_venues = pa.schema([
    _f("venue_id",  pa.string(), "Unique venue identifier",                          nullable=False),
    _f("name",      pa.string(), "Venue / arena name",                               nullable=False),
    _f("city",      pa.string(), "City"),
    _f("state",     pa.string(), "State or province"),
    _f("country",   pa.string(), "Country"),
    _f("capacity",  pa.int32(),  "Seating capacity"),
    # Provenance
    _f("source",    pa.string(), "Data vendor provenance",                           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 16. advanced — partition: season=
#     Discriminator: scope (game | season)
# ═══════════════════════════════════════════════════════════════════════

schema_advanced = pa.schema([
    # Identity
    _f("player_id",          pa.string(),  "Unique player identifier",               nullable=False),
    _f("player_name",        pa.string(),  "Player display name"),
    _f("team_id",            pa.string(),  "Team identifier"),
    _f("team_name",          pa.string(),  "Team name"),
    _f("season",             pa.int32(),   "Season start year",                      nullable=False),
    _f("game_id",            pa.string(),  "Game identifier (null for season scope)"),
    _f("date",               pa.string(),  "Game date (null for season scope)"),
    _f("scope",              pa.string(),  "Stat scope — game or season"),
    # Advanced metrics
    _f("per",                pa.float64(), "Player efficiency rating"),
    _f("ts_pct",             pa.float64(), "True shooting percentage"),
    _f("efg_pct",            pa.float64(), "Effective field goal percentage"),
    _f("offensive_rating",   pa.float64(), "Offensive rating (points per 100 possessions)"),
    _f("defensive_rating",   pa.float64(), "Defensive rating (points allowed per 100 possessions)"),
    _f("net_rating",         pa.float64(), "Net rating (off − def)"),
    _f("ast_pct",            pa.float64(), "Assist percentage"),
    _f("reb_pct",            pa.float64(), "Rebound percentage"),
    _f("stl_pct",            pa.float64(), "Steal percentage"),
    _f("blk_pct",            pa.float64(), "Block percentage"),
    _f("tov_pct",            pa.float64(), "Turnover percentage"),
    _f("usg_pct",            pa.float64(), "Usage percentage"),
    _f("pace",               pa.float64(), "Pace (possessions per 40 minutes)"),
    # Provenance
    _f("source",             pa.string(),  "Data vendor provenance",                 nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Schema registry — entity name → schema
# ═══════════════════════════════════════════════════════════════════════

NCAAW_SCHEMAS: dict[str, pa.Schema] = {
    "conferences":   schema_conferences,
    "teams":         schema_teams,
    "players":       schema_players,
    "games":         schema_games,
    "player_stats":  schema_player_stats,
    "team_stats":    schema_team_stats,
    "standings":     schema_standings,
    "rankings":      schema_rankings,
    "odds":          schema_odds,
    "player_props":  schema_player_props,
    "plays":         schema_plays,
    "injuries":      schema_injuries,
    "bracket":       schema_bracket,
    "leaders":       schema_leaders,
    "venues":        schema_venues,
    "advanced":      schema_advanced,
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names (the prefix of {type}_{season}.parquet)
# to the flat entity directory name under normalized_curated/ncaaw/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
NCAAW_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "games":            "games",
    "teams":            "teams",
    "players":          "players",
    "player_stats":     "player_stats",
    "team_stats":       "team_stats",
    "standings":        "standings",
    "rankings":         "rankings",
    "odds":             "odds",
    "player_props":     "player_props",
    "injuries":         "injuries",
    # Aliases / absorbed types
    "odds_history":     "odds",
    "play_by_play":     "plays",
    "conferences":      "conferences",
    "team_game_stats":  "team_stats",
    "roster":           "players",
    "info":             "players",
    "scoreboard":       "games",
    "calendar":         "games",
    "game_box_advanced": "advanced",
    "stats_advanced":   "advanced",
    "stats_season":     "player_stats",
    "stats_player_season": "player_stats",
    # Non-entity artefacts — skip
    "stats_categories": None,
    "coaches":          None,
    "news":             None,
    "weather":          None,
    "market_signals":   None,
    "schedule_fatigue": None,
    "transactions":     None,
    "recruiting":       None,
    "recruiting_teams": None,
    "recruiting_groups": None,
    "ratings":          None,
    "ratings_srs":      None,
    "talent":           None,
}

# ── Entity allow-list and static entities ─────────────────────────────
NCAAW_ENTITY_ALLOWLIST: set[str] = {
    "conferences",
    "teams",
    "players",
    "games",
    "player_stats",
    "team_stats",
    "standings",
    "rankings",
    "odds",
    "player_props",
    "plays",
    "injuries",
    "bracket",
    "leaders",
    "venues",
    "advanced",
}

NCAAW_STATIC_ENTITIES: set[str] = {"teams", "conferences", "venues"}
