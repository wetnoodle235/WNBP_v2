# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — NHL Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 16-entity consolidated design.  Raw data from ESPN, NHL API,
# Odds, and OddsAPI providers are merged into 16 wide schemas that
# use discriminator columns (``stat_type``, ``line_type``,
# ``season_type``, ``prop_type``) to distinguish record subtypes
# within a single table.
#
# Entity overview
# ───────────────
#  1. teams            — static reference, no partitioning
#  2. players          — partition: season=
#  3. games            — partition: season=
#  4. box_scores       — partition: season=
#  5. standings        — partition: season=
#  6. odds             — partition: season=
#  7. player_props     — partition: season=
#  8. plays            — partition: season=
#  9. injuries         — partition: season=
# 10. player_stats     — partition: season=
# 11. team_stats       — partition: season=
# 12. leaders          — partition: season=
# 13. venues           — static reference, no partitioning
# 14. conferences      — static reference, no partitioning
# 15. draft            — partition: season=
# 16. coaches          — partition: season=
#
# NHL does NOT use week-based partitioning — games are date-based.
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

NHL_TEAMS_SCHEMA = pa.schema([
    # Core identity
    _f("team_id",        pa.string(), "Unique team identifier",                   nullable=False),
    _f("name",           pa.string(), "Team name (e.g. Bruins)",                  nullable=False),
    _f("abbreviation",   pa.string(), "Team abbreviation (e.g. BOS)",             nullable=False),
    _f("city",           pa.string(), "City where team is located"),
    _f("conference",     pa.string(), "Conference — Eastern or Western"),
    _f("division",       pa.string(), "Division name (e.g. Atlantic, Central)"),
    _f("logo_url",       pa.string(), "URL to team logo image"),
    _f("primary_color",  pa.string(), "Primary team colour hex code"),
    _f("arena",          pa.string(), "Home arena name"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. players — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_PLAYERS_SCHEMA = pa.schema([
    # Core identity
    _f("player_id",      pa.string(), "Unique player identifier",                 nullable=False),
    _f("name",           pa.string(), "Full display name",                        nullable=False),
    _f("first_name",     pa.string(), "First name"),
    _f("last_name",      pa.string(), "Last name"),
    _f("team_id",        pa.string(), "Current team identifier"),
    _f("team_name",      pa.string(), "Current team name"),
    _f("position",       pa.string(), "Primary position (C, LW, RW, D, G)"),
    _f("jersey_number",  pa.string(), "Jersey number"),
    _f("height",         pa.string(), "Height (e.g. 6-1)"),
    _f("weight",         pa.int32(),  "Weight in pounds"),
    _f("birth_date",     pa.string(), "Date of birth (YYYY-MM-DD)"),
    _f("nationality",    pa.string(), "Country of origin"),
    _f("shoots",         pa.string(), "Shooting hand — L or R"),
    _f("status",         pa.string(), "Active / Injured / Inactive"),
    # Partition key
    _f("season",         pa.int32(),  "Season start year (e.g. 2024 for 2024-25)", nullable=False),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. games — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_GAMES_SCHEMA = pa.schema([
    # Core identity
    _f("game_id",              pa.string(),  "Unique game identifier",                     nullable=False),
    _f("season",               pa.int32(),   "Season start year",                          nullable=False),
    _f("date",                 pa.string(),  "Game date (YYYY-MM-DD)"),
    _f("start_time",           pa.string(),  "Scheduled start time (ISO-8601)"),
    _f("status",               pa.string(),  "Game status (scheduled, in_progress, final)"),
    # Teams
    _f("home_team_id",         pa.string(),  "Home team identifier"),
    _f("home_team",            pa.string(),  "Home team display name"),
    _f("away_team_id",         pa.string(),  "Away team identifier"),
    _f("away_team",            pa.string(),  "Away team display name"),
    # Scores
    _f("home_score",           pa.int32(),   "Home team final score"),
    _f("away_score",           pa.int32(),   "Away team final score"),
    _f("venue",                pa.string(),  "Arena / venue name"),
    _f("season_type",          pa.string(),  "Preseason, Regular, Playoffs"),
    _f("overtime",             pa.bool_(),   "Whether game went to overtime"),
    # Team-level game stats
    _f("home_shots_on_goal",   pa.int32(),   "Home team shots on goal"),
    _f("away_shots_on_goal",   pa.int32(),   "Away team shots on goal"),
    _f("home_penalty_minutes", pa.int32(),   "Home team penalty minutes"),
    _f("away_penalty_minutes", pa.int32(),   "Away team penalty minutes"),
    _f("home_power_play_goals", pa.int32(),  "Home team power play goals"),
    _f("away_power_play_goals", pa.int32(),  "Away team power play goals"),
    _f("home_hits",            pa.int32(),   "Home team hits"),
    _f("away_hits",            pa.int32(),   "Away team hits"),
    _f("home_blocked_shots",   pa.int32(),   "Home team blocked shots"),
    _f("away_blocked_shots",   pa.int32(),   "Away team blocked shots"),
    _f("home_takeaways",       pa.int32(),   "Home team takeaways"),
    _f("away_takeaways",       pa.int32(),   "Away team takeaways"),
    _f("home_giveaways",       pa.int32(),   "Home team giveaways"),
    _f("away_giveaways",       pa.int32(),   "Away team giveaways"),
    _f("home_save_pct",        pa.float64(), "Home team save percentage"),
    _f("away_save_pct",        pa.float64(), "Away team save percentage"),
    _f("home_saves",           pa.int32(),   "Home team saves"),
    _f("away_saves",           pa.int32(),   "Away team saves"),
    # Derived
    _f("result",               pa.string(),  "Result string (e.g. W, L, OTL)"),
    _f("score_diff",           pa.int32(),   "Home score minus away score"),
    _f("total_score",          pa.int32(),   "Combined score of both teams"),
    _f("day_of_week",          pa.string(),  "Day of week (e.g. Monday)"),
    _f("is_weekend",           pa.bool_(),   "Whether game is on Saturday or Sunday"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. box_scores — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_BOX_SCORES_SCHEMA = pa.schema([
    # Keys
    _f("game_id",              pa.string(), "Game identifier",                           nullable=False),
    _f("player_id",            pa.string(), "Player identifier",                         nullable=False),
    _f("player_name",          pa.string(), "Player display name"),
    _f("team_id",              pa.string(), "Team identifier"),
    _f("team_name",            pa.string(), "Team display name"),
    _f("season",               pa.int32(),  "Season start year",                         nullable=False),
    _f("date",                 pa.string(), "Game date (YYYY-MM-DD)"),
    _f("position",             pa.string(), "Player position"),
    # Discriminator
    _f("stat_type",            pa.string(), "skater or goalie"),
    # Skater stats
    _f("goals",                pa.int32(),  "Goals scored"),
    _f("assists",              pa.int32(),  "Assists"),
    _f("points",               pa.int32(),  "Total points (goals + assists)"),
    _f("plus_minus",           pa.int32(),  "Plus/minus rating"),
    _f("penalty_minutes",      pa.int32(),  "Penalty minutes"),
    _f("shots",                pa.int32(),  "Shots on goal"),
    _f("hits",                 pa.int32(),  "Hits"),
    _f("blocked_shots",        pa.int32(),  "Blocked shots"),
    _f("takeaways",            pa.int32(),  "Takeaways"),
    _f("giveaways",            pa.int32(),  "Giveaways"),
    _f("faceoff_wins",         pa.int32(),  "Faceoff wins"),
    _f("faceoff_losses",       pa.int32(),  "Faceoff losses"),
    _f("time_on_ice",          pa.string(), "Time on ice (MM:SS)"),
    _f("power_play_goals",     pa.int32(),  "Power play goals"),
    _f("power_play_assists",   pa.int32(),  "Power play assists"),
    _f("shorthanded_goals",    pa.int32(),  "Shorthanded goals"),
    # Goalie stats
    _f("saves",                pa.int32(),  "Saves (goalie)"),
    _f("goals_against",        pa.int32(),  "Goals against (goalie)"),
    _f("shots_against",        pa.int32(),  "Shots against (goalie)"),
    _f("save_pct",             pa.float64(),"Save percentage (goalie)"),
    _f("minutes",              pa.string(), "Minutes played (goalie, MM:SS)"),
    # Provenance
    _f("source",               pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. standings — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_STANDINGS_SCHEMA = pa.schema([
    # Identity
    _f("team_id",          pa.string(),  "Team identifier",                           nullable=False),
    _f("team_name",        pa.string(),  "Team display name",                         nullable=False),
    _f("season",           pa.int32(),   "Season start year",                         nullable=False),
    _f("conference",       pa.string(),  "Conference name"),
    _f("division",         pa.string(),  "Division name"),
    # Record
    _f("wins",             pa.int32(),   "Wins"),
    _f("losses",           pa.int32(),   "Losses"),
    _f("otl",              pa.int32(),   "Overtime losses"),
    _f("points",           pa.int32(),   "Standings points"),
    _f("games_played",     pa.int32(),   "Games played"),
    _f("points_for",       pa.int32(),   "Goals / points scored"),
    _f("points_against",   pa.int32(),   "Goals / points allowed"),
    _f("pct",              pa.float64(), "Points percentage"),
    # Rankings
    _f("rank",             pa.int32(),   "Overall rank"),
    _f("conference_rank",  pa.int32(),   "Conference rank"),
    _f("division_rank",    pa.int32(),   "Division rank"),
    _f("overall_rank",     pa.int32(),   "League-wide overall rank"),
    _f("streak",           pa.string(),  "Current streak (e.g. W3, L2)"),
    _f("last_ten",         pa.string(),  "Last 10 games record (e.g. 7-2-1)"),
    _f("home_record",      pa.string(),  "Home record (e.g. 20-8-3)"),
    _f("away_record",      pa.string(),  "Away record (e.g. 18-10-4)"),
    _f("clinch_status",    pa.string(),  "Playoff clinch indicator (x, y, z, e)"),
    # Provenance
    _f("source",           pa.string(),  "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. odds — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_ODDS_SCHEMA = pa.schema([
    # Keys
    _f("game_id",            pa.string(),  "Game identifier",                          nullable=False),
    _f("season",             pa.int32(),   "Season start year",                        nullable=False),
    _f("date",               pa.string(),  "Game date (YYYY-MM-DD)"),
    _f("sportsbook",         pa.string(),  "Sportsbook name"),
    _f("home_team",          pa.string(),  "Home team display name"),
    _f("away_team",          pa.string(),  "Away team display name"),
    # Spread
    _f("spread_home",        pa.float64(), "Home puck line / spread"),
    _f("spread_away",        pa.float64(), "Away puck line / spread"),
    _f("spread_home_odds",   pa.int32(),   "Home spread odds (American)"),
    _f("spread_away_odds",   pa.int32(),   "Away spread odds (American)"),
    # Moneyline
    _f("moneyline_home",     pa.int32(),   "Home moneyline odds (American)"),
    _f("moneyline_away",     pa.int32(),   "Away moneyline odds (American)"),
    # Totals
    _f("total_over",         pa.float64(), "Over line (total goals)"),
    _f("total_under",        pa.float64(), "Under line (total goals)"),
    _f("total_over_odds",    pa.int32(),   "Over odds (American)"),
    _f("total_under_odds",   pa.int32(),   "Under odds (American)"),
    # Discriminator
    _f("line_type",          pa.string(),  "open, current, closing, etc."),
    _f("timestamp",          pa.string(),  "Timestamp when line was captured (ISO-8601)"),
    # Provenance
    _f("source",             pa.string(),  "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. player_props — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_PLAYER_PROPS_SCHEMA = pa.schema([
    # Keys
    _f("game_id",       pa.string(), "Game identifier",                              nullable=False),
    _f("player_id",     pa.string(), "Player identifier",                            nullable=False),
    _f("player_name",   pa.string(), "Player display name"),
    _f("season",        pa.int32(),  "Season start year",                            nullable=False),
    _f("date",          pa.string(), "Game date (YYYY-MM-DD)"),
    _f("team",          pa.string(), "Team display name"),
    # Prop details
    _f("prop_type",     pa.string(), "Prop market type (e.g. points, saves, SOG)"),
    _f("line",          pa.float64(),"Prop line value"),
    _f("over_odds",     pa.int32(),  "Over odds (American)"),
    _f("under_odds",    pa.int32(),  "Under odds (American)"),
    _f("sportsbook",    pa.string(), "Sportsbook name"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",                       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. plays — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_PLAYS_SCHEMA = pa.schema([
    # Keys
    _f("game_id",       pa.string(),  "Game identifier",                             nullable=False),
    _f("season",        pa.int32(),   "Season start year",                           nullable=False),
    _f("date",          pa.string(),  "Game date (YYYY-MM-DD)"),
    # Play details
    _f("period",        pa.int32(),   "Period number (1, 2, 3, OT)"),
    _f("time",          pa.string(),  "Time remaining in period (MM:SS)"),
    _f("event_type",    pa.string(),  "Event type (goal, shot, hit, penalty, etc.)"),
    _f("description",   pa.string(),  "Free-text play description"),
    _f("team_id",       pa.string(),  "Team identifier for event"),
    _f("team_name",     pa.string(),  "Team display name for event"),
    _f("player_id",     pa.string(),  "Primary player identifier"),
    _f("player_name",   pa.string(),  "Primary player display name"),
    # Coordinates
    _f("x_coord",       pa.float64(), "X coordinate on rink"),
    _f("y_coord",       pa.float64(), "Y coordinate on rink"),
    # Provenance
    _f("source",        pa.string(),  "Data vendor provenance",                      nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. injuries — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_INJURIES_SCHEMA = pa.schema([
    # Identity
    _f("player_id",     pa.string(), "Player identifier",                            nullable=False),
    _f("player_name",   pa.string(), "Player display name",                          nullable=False),
    _f("team_id",       pa.string(), "Team identifier"),
    _f("team_name",     pa.string(), "Team display name"),
    _f("season",        pa.int32(),  "Season start year",                            nullable=False),
    _f("date",          pa.string(), "Injury report date (YYYY-MM-DD)"),
    # Injury details
    _f("status",        pa.string(), "Injury status (IR, Day-to-Day, Out)"),
    _f("injury_type",   pa.string(), "Injury category (e.g. Upper Body, Lower Body)"),
    _f("detail",        pa.string(), "Additional injury detail"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",                       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. player_stats — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_PLAYER_STATS_SCHEMA = pa.schema([
    # Identity
    _f("player_id",            pa.string(),  "Player identifier",                    nullable=False),
    _f("player_name",          pa.string(),  "Player display name",                  nullable=False),
    _f("team_id",              pa.string(),  "Team identifier"),
    _f("team_name",            pa.string(),  "Team display name"),
    _f("season",               pa.int32(),   "Season start year",                    nullable=False),
    # Discriminator
    _f("stat_type",            pa.string(),  "skater or goalie"),
    # Counting stats (skater + goalie share games_played)
    _f("games_played",         pa.int32(),   "Games played"),
    _f("goals",                pa.int32(),   "Goals scored"),
    _f("assists",              pa.int32(),   "Assists"),
    _f("points",               pa.int32(),   "Total points"),
    _f("plus_minus",           pa.int32(),   "Plus/minus rating"),
    _f("penalty_minutes",      pa.int32(),   "Penalty minutes"),
    _f("shots",                pa.int32(),   "Shots on goal"),
    _f("shooting_pct",         pa.float64(), "Shooting percentage"),
    _f("hits",                 pa.int32(),   "Hits"),
    _f("blocked_shots",        pa.int32(),   "Blocked shots"),
    _f("takeaways",            pa.int32(),   "Takeaways"),
    _f("giveaways",            pa.int32(),   "Giveaways"),
    _f("time_on_ice",          pa.string(),  "Average time on ice (MM:SS)"),
    _f("power_play_goals",     pa.int32(),   "Power play goals"),
    _f("power_play_assists",   pa.int32(),   "Power play assists"),
    _f("shorthanded_goals",    pa.int32(),   "Shorthanded goals"),
    _f("game_winning_goals",   pa.int32(),   "Game-winning goals"),
    # Goalie-specific
    _f("saves",                pa.int32(),   "Saves (goalie)"),
    _f("goals_against_avg",    pa.float64(), "Goals against average (goalie)"),
    _f("save_pct",             pa.float64(), "Save percentage (goalie)"),
    _f("wins",                 pa.int32(),   "Wins (goalie)"),
    _f("losses",               pa.int32(),   "Losses (goalie)"),
    _f("otl",                  pa.int32(),   "Overtime losses (goalie)"),
    _f("shutouts",             pa.int32(),   "Shutouts (goalie)"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",               nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. team_stats — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_TEAM_STATS_SCHEMA = pa.schema([
    # Identity
    _f("team_id",           pa.string(),  "Team identifier",                         nullable=False),
    _f("team_name",         pa.string(),  "Team display name",                       nullable=False),
    _f("season",            pa.int32(),   "Season start year",                       nullable=False),
    # Discriminator
    _f("stat_type",         pa.string(),  "Stat category / scope"),
    # Stats
    _f("games_played",      pa.int32(),   "Games played"),
    _f("goals_for",         pa.int32(),   "Goals scored"),
    _f("goals_against",     pa.int32(),   "Goals allowed"),
    _f("shots_for",         pa.int32(),   "Shots on goal for"),
    _f("shots_against",     pa.int32(),   "Shots on goal against"),
    _f("power_play_pct",    pa.float64(), "Power play percentage"),
    _f("penalty_kill_pct",  pa.float64(), "Penalty kill percentage"),
    _f("faceoff_pct",       pa.float64(), "Faceoff win percentage"),
    _f("hits",              pa.int32(),   "Total hits"),
    _f("blocked_shots",     pa.int32(),   "Total blocked shots"),
    _f("takeaways",         pa.int32(),   "Total takeaways"),
    _f("giveaways",         pa.int32(),   "Total giveaways"),
    # Provenance
    _f("source",            pa.string(),  "Data vendor provenance",                  nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. leaders — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_LEADERS_SCHEMA = pa.schema([
    # Identity
    _f("player_id",     pa.string(),  "Player identifier",                           nullable=False),
    _f("player_name",   pa.string(),  "Player display name",                         nullable=False),
    _f("team_id",       pa.string(),  "Team identifier"),
    _f("team_name",     pa.string(),  "Team display name"),
    _f("season",        pa.int32(),   "Season start year",                           nullable=False),
    # Leader details
    _f("category",      pa.string(),  "Statistical category (e.g. goals, assists)"),
    _f("stat_type",     pa.string(),  "Stat sub-type or scope"),
    _f("value",         pa.float64(), "Statistical value"),
    _f("rank",          pa.int32(),   "Rank within category"),
    # Provenance
    _f("source",        pa.string(),  "Data vendor provenance",                      nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

NHL_VENUES_SCHEMA = pa.schema([
    _f("venue_id",   pa.string(), "Unique venue identifier",                         nullable=False),
    _f("name",       pa.string(), "Venue / arena name",                              nullable=False),
    _f("city",       pa.string(), "City"),
    _f("state",      pa.string(), "State or province"),
    _f("country",    pa.string(), "Country"),
    _f("capacity",   pa.int32(),  "Seating capacity"),
    _f("surface",    pa.string(), "Playing surface type"),
    # Provenance
    _f("source",     pa.string(), "Data vendor provenance",                          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. conferences — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

NHL_CONFERENCES_SCHEMA = pa.schema([
    _f("conference_id",  pa.string(), "Unique conference identifier",                nullable=False),
    _f("name",           pa.string(), "Conference name (Eastern, Western)",          nullable=False),
    _f("abbreviation",   pa.string(), "Conference abbreviation"),
    _f("divisions",      pa.string(), "Comma-separated division names"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                      nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 15. draft — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_DRAFT_SCHEMA = pa.schema([
    # Identity
    _f("draft_id",      pa.string(), "Unique draft pick identifier",                 nullable=False),
    _f("season",        pa.int32(),  "Draft year / season",                          nullable=False),
    _f("round",         pa.int32(),  "Draft round"),
    _f("pick",          pa.int32(),  "Pick number within round"),
    _f("overall",       pa.int32(),  "Overall pick number"),
    # Player info
    _f("player_id",     pa.string(), "Player identifier"),
    _f("player_name",   pa.string(), "Player display name"),
    _f("team_id",       pa.string(), "Drafting team identifier"),
    _f("team_name",     pa.string(), "Drafting team display name"),
    _f("position",      pa.string(), "Player position"),
    _f("nationality",   pa.string(), "Player nationality"),
    # Provenance
    _f("source",        pa.string(), "Data vendor provenance",                       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 16. coaches — partition: season=
# ═══════════════════════════════════════════════════════════════════════

NHL_COACHES_SCHEMA = pa.schema([
    _f("coach_id",   pa.string(), "Unique coach identifier",                         nullable=False),
    _f("name",       pa.string(), "Coach display name",                              nullable=False),
    _f("team_id",    pa.string(), "Team identifier"),
    _f("team_name",  pa.string(), "Team display name"),
    _f("season",     pa.int32(),  "Season start year",                               nullable=False),
    _f("role",       pa.string(), "Coaching role (Head Coach, Assistant, etc.)"),
    # Provenance
    _f("source",     pa.string(), "Data vendor provenance",                          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Registry — schema, partition key, and path look-ups
# ═══════════════════════════════════════════════════════════════════════

NHL_SCHEMAS: dict[str, pa.Schema] = {
    "teams":         NHL_TEAMS_SCHEMA,
    "players":       NHL_PLAYERS_SCHEMA,
    "games":         NHL_GAMES_SCHEMA,
    "box_scores":    NHL_BOX_SCORES_SCHEMA,
    "standings":     NHL_STANDINGS_SCHEMA,
    "odds":          NHL_ODDS_SCHEMA,
    "player_props":  NHL_PLAYER_PROPS_SCHEMA,
    "plays":         NHL_PLAYS_SCHEMA,
    "injuries":      NHL_INJURIES_SCHEMA,
    "player_stats":  NHL_PLAYER_STATS_SCHEMA,
    "team_stats":    NHL_TEAM_STATS_SCHEMA,
    "leaders":       NHL_LEADERS_SCHEMA,
    "venues":        NHL_VENUES_SCHEMA,
    "conferences":   NHL_CONFERENCES_SCHEMA,
    "draft":         NHL_DRAFT_SCHEMA,
    "coaches":       NHL_COACHES_SCHEMA,
}

NHL_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning (single flat parquets)
    "teams":         [],
    "venues":        [],
    "conferences":   [],
    # Season only — NHL uses dates, not weeks
    "players":       ["season"],
    "games":         ["season"],
    "box_scores":    ["season"],
    "standings":     ["season"],
    "odds":          ["season"],
    "player_props":  ["season"],
    "plays":         ["season"],
    "injuries":      ["season"],
    "player_stats":  ["season"],
    "team_stats":    ["season"],
    "leaders":       ["season"],
    "draft":         ["season"],
    "coaches":       ["season"],
}

NHL_ENTITY_PATHS: dict[str, str] = {
    "teams":         "teams",
    "players":       "players",
    "games":         "games",
    "box_scores":    "box_scores",
    "standings":     "standings",
    "odds":          "odds",
    "player_props":  "player_props",
    "plays":         "plays",
    "injuries":      "injuries",
    "player_stats":  "player_stats",
    "team_stats":    "team_stats",
    "leaders":       "leaders",
    "venues":        "venues",
    "conferences":   "conferences",
    "draft":         "draft",
    "coaches":       "coaches",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names (the prefix of {type}_{season}.parquet)
# to the flat entity directory name under normalized_curated/nhl/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
NHL_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "games":            "games",
    "teams":            "teams",
    "players":          "players",
    "player_stats":     "player_stats",
    "team_stats":       "team_stats",
    "standings":        "standings",
    "odds":             "odds",
    "odds_history":     "odds",
    "player_props":     "player_props",
    "injuries":         "injuries",
    "play_by_play":     "plays",
    # Aliases / absorbed types
    "team_game_stats":  "team_stats",
    "roster":           "players",
    "info":             "players",
    "scoreboard":       "games",
    "calendar":         "games",
    "draft":            "draft",
    "coaches":          "coaches",
    # Non-entity normalizer artefacts — skip
    "news":             None,
    "weather":          None,
    "market_signals":   None,
    "schedule_fatigue": None,
    "transactions":     None,
}

# ── Entity allow-list and static entities ─────────────────────────────
NHL_ENTITY_ALLOWLIST: set[str] = {
    "teams",
    "players",
    "games",
    "box_scores",
    "standings",
    "odds",
    "player_props",
    "plays",
    "injuries",
    "player_stats",
    "team_stats",
    "leaders",
    "venues",
    "conferences",
    "draft",
    "coaches",
}

NHL_STATIC_ENTITIES: set[str] = {"teams", "venues", "conferences"}
