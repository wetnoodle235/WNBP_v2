# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — NBA Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 16-entity consolidated design.  Raw data from ESPN, NBA Stats,
# Odds, and OddsAPI providers are merged into 16 wide schemas that
# use discriminator columns (``scope``, ``stat_type``, ``metric_type``,
# ``line_type``, ``play_type``, ``prop_type``, ``contract_scope``,
# ``season_type``) to distinguish record subtypes within a single table.
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
#  9. advanced         — partition: season=
# 10. plays            — partition: season=
# 11. box_scores       — partition: season=
# 12. lineups          — partition: season=
# 13. contracts        — partition: season=
# 14. injuries         — partition: season=
# 15. leaders          — partition: season=
# 16. venues           — static reference, no partitioning
#
# NBA does NOT use week-based partitioning — games are date-based.
# Season is always the start year integer (e.g. 2024 for "2024-25").
#
# Merge map (old → new) is available in ``NBA_CONSOLIDATION_MAP``.
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

NBA_TEAMS_SCHEMA = pa.schema([
    # Core identity
    _f("id",              pa.int64(),  "Unique team identifier",                      nullable=False),
    _f("name",            pa.string(), "Short team name (e.g. Lakers)",               nullable=False),
    _f("full_name",       pa.string(), "Full team name (e.g. Los Angeles Lakers)",    nullable=False),
    _f("abbreviation",    pa.string(), "Team abbreviation (e.g. LAL)",                nullable=False),
    _f("city",            pa.string(), "City where team is located"),
    _f("conference",      pa.string(), "Conference — Eastern or Western"),
    _f("division",        pa.string(), "Division name (e.g. Pacific, Atlantic)"),
    _f("color",           pa.string(), "Primary team colour hex code"),
    _f("alternate_color", pa.string(), "Secondary team colour hex code"),
    _f("logo_url",        pa.string(), "URL to team logo image"),
    _f("is_active",       pa.bool_(),  "Whether the franchise is currently active"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                     nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. players — merges ESPN athletes + NBA Stats all_players
#    Partition: season=
#    Discriminator: status (active/inactive/injured)
# ═══════════════════════════════════════════════════════════════════════

NBA_PLAYERS_SCHEMA = pa.schema([
    # Core identity
    _f("id",              pa.int64(),  "Unique player identifier",            nullable=False),
    _f("first_name",      pa.string(), "Player first name",                   nullable=False),
    _f("last_name",       pa.string(), "Player last name",                    nullable=False),
    _f("full_name",       pa.string(), "Full display name"),
    _f("position",        pa.string(), "Position (e.g. PG, SG, SF, PF, C)"),
    _f("height",          pa.string(), "Height (e.g. 6-6)"),
    _f("weight",          pa.string(), "Weight as string (e.g. 250)"),
    _f("jersey_number",   pa.string(), "Jersey number as string"),
    _f("college",         pa.string(), "College attended"),
    _f("country",         pa.string(), "Country of origin"),
    _f("draft_year",      pa.int64(),  "Year drafted"),
    _f("draft_round",     pa.int64(),  "Draft round"),
    _f("draft_number",    pa.int64(),  "Draft pick number"),
    _f("team_id",         pa.int64(),  "Current team identifier"),
    _f("team_name",       pa.string(), "Current team name"),
    # Discriminator
    _f("status",          pa.string(), "Player status — active/inactive/injured"),
    # Profile
    _f("headshot_url",    pa.string(), "URL to player headshot image"),
    _f("age",             pa.int64(),  "Player age"),
    _f("date_of_birth",   pa.string(), "Date of birth (ISO-8601)"),
    _f("debut_year",      pa.int64(),  "Year of NBA debut"),
    _f("years_pro",       pa.int64(),  "Years of professional experience"),
    _f("from_aau",        pa.string(), "AAU / pre-college program"),
    _f("is_active",       pa.bool_(),  "Whether player is currently active in NBA"),
    # Partition / provenance
    _f("season",          pa.int64(),  "Season start year (e.g. 2024 for 2024-25)",  nullable=False),
    _f("source",          pa.string(), "Data vendor provenance",                     nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. games — merges ESPN events + NBA Stats game summaries
#    Partition: season=
#    Discriminator: season_type (regular/playoffs/preseason/ist)
# ═══════════════════════════════════════════════════════════════════════

NBA_GAMES_SCHEMA = pa.schema([
    # Core identity
    _f("id",                  pa.int64(),  "Unique game identifier",          nullable=False),
    _f("date",                pa.string(), "Game date (ISO-8601)",            nullable=False),
    _f("season",              pa.int64(),  "Season start year",               nullable=False),
    _f("season_type",         pa.string(), "Season type — regular/playoffs/preseason/ist"),
    _f("status",              pa.string(), "Game status (e.g. final, in_progress, scheduled)"),
    _f("period",              pa.int64(),  "Current period / quarter"),
    _f("time",                pa.string(), "Time remaining in current period"),
    _f("datetime",            pa.string(), "Full game datetime (ISO-8601 with timezone)"),
    # Teams
    _f("home_team_id",        pa.int64(),  "Home team identifier"),
    _f("home_team_name",      pa.string(), "Home team name"),
    _f("home_team_score",     pa.int64(),  "Home team total score"),
    _f("visitor_team_id",     pa.int64(),  "Visitor team identifier"),
    _f("visitor_team_name",   pa.string(), "Visitor team name"),
    _f("visitor_team_score",  pa.int64(),  "Visitor team total score"),
    # Period scores
    _f("home_score_q1",       pa.int64(),  "Home 1st quarter score"),
    _f("home_score_q2",       pa.int64(),  "Home 2nd quarter score"),
    _f("home_score_q3",       pa.int64(),  "Home 3rd quarter score"),
    _f("home_score_q4",       pa.int64(),  "Home 4th quarter score"),
    _f("home_score_ot",       pa.int64(),  "Home overtime total score"),
    _f("visitor_score_q1",    pa.int64(),  "Visitor 1st quarter score"),
    _f("visitor_score_q2",    pa.int64(),  "Visitor 2nd quarter score"),
    _f("visitor_score_q3",    pa.int64(),  "Visitor 3rd quarter score"),
    _f("visitor_score_q4",    pa.int64(),  "Visitor 4th quarter score"),
    _f("visitor_score_ot",    pa.int64(),  "Visitor overtime total score"),
    # Flags
    _f("postseason",          pa.bool_(),  "Whether this is a postseason game"),
    _f("overtime",            pa.bool_(),  "Whether the game went to overtime"),
    _f("ot_periods",          pa.int64(),  "Number of overtime periods played"),
    # Venue
    _f("attendance",          pa.int64(),  "Reported attendance"),
    _f("arena_name",          pa.string(), "Arena / venue name"),
    _f("arena_city",          pa.string(), "Arena city"),
    _f("arena_state",         pa.string(), "Arena state"),
    # Broadcast
    _f("tv_network",          pa.string(), "TV broadcast network"),
    _f("duration_minutes",    pa.int64(),  "Game duration in minutes"),
    # Provenance
    _f("source",              pa.string(), "Data vendor provenance",          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. player_stats — merges NBA Stats player-stats + player-game-logs
#    + season aggregates via scope + stat_type discriminators
#    Partition: season=
# ═══════════════════════════════════════════════════════════════════════

NBA_PLAYER_STATS_SCHEMA = pa.schema([
    # Identity
    _f("id",              pa.int64(),  "Unique record identifier"),
    _f("player_id",       pa.int64(),  "Player identifier",                  nullable=False),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("game_id",         pa.int64(),  "Game identifier (null for season scope)"),
    _f("game_date",       pa.string(), "Game date (ISO-8601)"),
    # Discriminators
    _f("season",          pa.int64(),  "Season start year",                  nullable=False),
    _f("season_type",     pa.string(), "Season type — regular/playoffs"),
    _f("scope",           pa.string(), "Record scope — game or season",      nullable=False),
    _f("stat_type",       pa.string(), "Stat category — base/advanced/defense/misc/scoring/usage", nullable=False),
    # Base counting stats
    _f("min",             pa.string(), "Minutes played (e.g. 36:24)"),
    _f("fgm",            pa.int64(),  "Field goals made"),
    _f("fga",            pa.int64(),  "Field goals attempted"),
    _f("fg_pct",         pa.float64(), "Field goal percentage"),
    _f("fg3m",           pa.int64(),  "Three-pointers made"),
    _f("fg3a",           pa.int64(),  "Three-pointers attempted"),
    _f("fg3_pct",        pa.float64(), "Three-point percentage"),
    _f("ftm",            pa.int64(),  "Free throws made"),
    _f("fta",            pa.int64(),  "Free throws attempted"),
    _f("ft_pct",         pa.float64(), "Free throw percentage"),
    _f("oreb",           pa.int64(),  "Offensive rebounds"),
    _f("dreb",           pa.int64(),  "Defensive rebounds"),
    _f("reb",            pa.int64(),  "Total rebounds"),
    _f("ast",            pa.int64(),  "Assists"),
    _f("stl",            pa.int64(),  "Steals"),
    _f("blk",            pa.int64(),  "Blocks"),
    _f("turnover",       pa.int64(),  "Turnovers"),
    _f("pf",             pa.int64(),  "Personal fouls"),
    _f("pts",            pa.int64(),  "Points scored"),
    _f("plus_minus",     pa.float64(), "Plus/minus"),
    # Games context
    _f("games_played",   pa.int64(),  "Games played (season scope)"),
    _f("games_started",  pa.int64(),  "Games started (season scope)"),
    # Advanced (stat_type=advanced)
    _f("offensive_rating",       pa.float64(), "Offensive rating"),
    _f("defensive_rating",       pa.float64(), "Defensive rating"),
    _f("net_rating",             pa.float64(), "Net rating"),
    _f("ast_pct",                pa.float64(), "Assist percentage"),
    _f("ast_to_turnover",        pa.float64(), "Assist-to-turnover ratio"),
    _f("ast_ratio",              pa.float64(), "Assist ratio"),
    _f("oreb_pct",               pa.float64(), "Offensive rebound percentage"),
    _f("dreb_pct",               pa.float64(), "Defensive rebound percentage"),
    _f("reb_pct",                pa.float64(), "Total rebound percentage"),
    _f("turnover_pct",           pa.float64(), "Turnover percentage"),
    _f("effective_fg_pct",       pa.float64(), "Effective field goal percentage"),
    _f("true_shooting_pct",      pa.float64(), "True shooting percentage"),
    _f("usage_pct",              pa.float64(), "Usage percentage"),
    _f("pace",                   pa.float64(), "Pace"),
    _f("pie",                    pa.float64(), "Player impact estimate"),
    # Defense (stat_type=defense)
    _f("def_rating",             pa.float64(), "Defensive rating (defense stat)"),
    _f("dreb_chance_pct",        pa.float64(), "Defensive rebound chance percentage"),
    _f("contested_shots",        pa.int64(),   "Contested shots"),
    _f("charges_drawn",          pa.int64(),   "Charges drawn"),
    _f("deflections",            pa.int64(),   "Deflections"),
    _f("loose_balls_recovered",  pa.int64(),   "Loose balls recovered"),
    _f("screen_assists",         pa.int64(),   "Screen assists"),
    _f("box_outs",               pa.int64(),   "Box outs"),
    # Scoring (stat_type=scoring)
    _f("pct_fga_2pt",            pa.float64(), "Percentage of FGA that are 2-pointers"),
    _f("pct_fga_3pt",            pa.float64(), "Percentage of FGA that are 3-pointers"),
    _f("pct_pts_2pt",            pa.float64(), "Percentage of points from 2-pointers"),
    _f("pct_pts_2pt_midrange",   pa.float64(), "Percentage of points from midrange 2s"),
    _f("pct_pts_3pt",            pa.float64(), "Percentage of points from 3-pointers"),
    _f("pct_pts_fastbreak",      pa.float64(), "Percentage of points from fastbreaks"),
    _f("pct_pts_ft",             pa.float64(), "Percentage of points from free throws"),
    _f("pct_pts_paint",          pa.float64(), "Percentage of points in the paint"),
    _f("pct_ast_2pm",            pa.float64(), "Percentage of 2PM that were assisted"),
    _f("pct_uast_2pm",           pa.float64(), "Percentage of 2PM that were unassisted"),
    _f("pct_ast_3pm",            pa.float64(), "Percentage of 3PM that were assisted"),
    _f("pct_uast_3pm",           pa.float64(), "Percentage of 3PM that were unassisted"),
    _f("pct_ast_fgm",            pa.float64(), "Percentage of FGM that were assisted"),
    _f("pct_uast_fgm",           pa.float64(), "Percentage of FGM that were unassisted"),
    # Misc (stat_type=misc)
    _f("pts_off_turnover",       pa.int64(),   "Points off turnovers"),
    _f("pts_second_chance",      pa.int64(),   "Second chance points"),
    _f("pts_fastbreak",          pa.int64(),   "Fastbreak points"),
    _f("pts_paint",              pa.int64(),   "Points in the paint"),
    _f("opp_pts_off_turnover",   pa.int64(),   "Opponent points off turnovers"),
    _f("blk_against",            pa.int64(),   "Blocked shots against"),
    _f("fouls_drawn",            pa.int64(),   "Fouls drawn"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. team_stats — merges NBA Stats team-stats + team-game-logs
#    + season aggregates via scope + stat_type discriminators
#    Partition: season=
# ═══════════════════════════════════════════════════════════════════════

NBA_TEAM_STATS_SCHEMA = pa.schema([
    # Identity
    _f("team_id",         pa.int64(),  "Team identifier",                    nullable=False),
    _f("team_name",       pa.string(), "Team name"),
    _f("game_id",         pa.int64(),  "Game identifier (null for season scope)"),
    _f("game_date",       pa.string(), "Game date (ISO-8601)"),
    # Discriminators
    _f("season",          pa.int64(),  "Season start year",                  nullable=False),
    _f("season_type",     pa.string(), "Season type — regular/playoffs"),
    _f("scope",           pa.string(), "Record scope — game or season",      nullable=False),
    _f("stat_type",       pa.string(), "Stat category — base/advanced/defense/misc/scoring/usage", nullable=False),
    # Base counting stats
    _f("min",             pa.string(), "Minutes played"),
    _f("fgm",            pa.int64(),  "Field goals made"),
    _f("fga",            pa.int64(),  "Field goals attempted"),
    _f("fg_pct",         pa.float64(), "Field goal percentage"),
    _f("fg3m",           pa.int64(),  "Three-pointers made"),
    _f("fg3a",           pa.int64(),  "Three-pointers attempted"),
    _f("fg3_pct",        pa.float64(), "Three-point percentage"),
    _f("ftm",            pa.int64(),  "Free throws made"),
    _f("fta",            pa.int64(),  "Free throws attempted"),
    _f("ft_pct",         pa.float64(), "Free throw percentage"),
    _f("oreb",           pa.int64(),  "Offensive rebounds"),
    _f("dreb",           pa.int64(),  "Defensive rebounds"),
    _f("reb",            pa.int64(),  "Total rebounds"),
    _f("ast",            pa.int64(),  "Assists"),
    _f("stl",            pa.int64(),  "Steals"),
    _f("blk",            pa.int64(),  "Blocks"),
    _f("turnover",       pa.int64(),  "Turnovers"),
    _f("pf",             pa.int64(),  "Personal fouls"),
    _f("pts",            pa.int64(),  "Points scored"),
    _f("plus_minus",     pa.float64(), "Plus/minus"),
    # Games context
    _f("games_played",   pa.int64(),  "Games played (season scope)"),
    _f("wins",           pa.int64(),  "Wins (season scope)"),
    _f("losses",         pa.int64(),  "Losses (season scope)"),
    _f("win_pct",        pa.float64(), "Win percentage (season scope)"),
    # Advanced (stat_type=advanced)
    _f("offensive_rating",       pa.float64(), "Offensive rating"),
    _f("defensive_rating",       pa.float64(), "Defensive rating"),
    _f("net_rating",             pa.float64(), "Net rating"),
    _f("ast_pct",                pa.float64(), "Team assist percentage"),
    _f("ast_to_turnover",        pa.float64(), "Team assist-to-turnover ratio"),
    _f("ast_ratio",              pa.float64(), "Team assist ratio"),
    _f("oreb_pct",               pa.float64(), "Offensive rebound percentage"),
    _f("dreb_pct",               pa.float64(), "Defensive rebound percentage"),
    _f("reb_pct",                pa.float64(), "Total rebound percentage"),
    _f("turnover_pct",           pa.float64(), "Turnover percentage"),
    _f("effective_fg_pct",       pa.float64(), "Effective field goal percentage"),
    _f("true_shooting_pct",      pa.float64(), "True shooting percentage"),
    _f("pace",                   pa.float64(), "Pace (possessions per 48 min)"),
    _f("pie",                    pa.float64(), "Player impact estimate (team-level)"),
    # Misc (stat_type=misc)
    _f("pts_off_turnover",       pa.int64(),   "Points off turnovers"),
    _f("pts_second_chance",      pa.int64(),   "Second chance points"),
    _f("pts_fastbreak",          pa.int64(),   "Fastbreak points"),
    _f("pts_paint",              pa.int64(),   "Points in the paint"),
    _f("opp_pts_off_turnover",   pa.int64(),   "Opponent points off turnovers"),
    _f("opp_pts_second_chance",  pa.int64(),   "Opponent second chance points"),
    _f("opp_pts_fastbreak",      pa.int64(),   "Opponent fastbreak points"),
    _f("opp_pts_paint",          pa.int64(),   "Opponent points in the paint"),
    _f("largest_lead",           pa.int64(),   "Largest lead in game"),
    _f("lead_changes",           pa.int64(),   "Number of lead changes"),
    _f("times_tied",             pa.int64(),   "Number of times score was tied"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. standings — merges ESPN standings + NBA Stats
#    Partition: season=
# ═══════════════════════════════════════════════════════════════════════

NBA_STANDINGS_SCHEMA = pa.schema([
    _f("team_id",            pa.int64(),   "Team identifier"),
    _f("team_name",          pa.string(),  "Team name"),
    _f("conference",         pa.string(),  "Conference — Eastern or Western"),
    _f("division",           pa.string(),  "Division name"),
    _f("season",             pa.int64(),   "Season start year",                nullable=False),
    # Win/loss
    _f("wins",               pa.int64(),   "Total wins"),
    _f("losses",             pa.int64(),   "Total losses"),
    _f("win_pct",            pa.float64(), "Win percentage"),
    _f("conference_rank",    pa.int64(),   "Conference ranking"),
    _f("division_rank",      pa.int64(),   "Division ranking"),
    _f("playoff_seed",       pa.int64(),   "Playoff seed (null if not in playoffs)"),
    # Situational records
    _f("conference_record",  pa.string(),  "Conference record (e.g. 30-22)"),
    _f("division_record",    pa.string(),  "Division record"),
    _f("home_record",        pa.string(),  "Home record"),
    _f("road_record",        pa.string(),  "Road record"),
    _f("last_10",            pa.string(),  "Last 10 games record (e.g. 7-3)"),
    _f("streak",             pa.string(),  "Current streak (e.g. W5, L2)"),
    _f("games_behind",       pa.float64(), "Games behind conference leader"),
    _f("ot_record",          pa.string(),  "Overtime games record"),
    _f("vs_over_500",        pa.string(),  "Record vs teams above .500"),
    # Clinch info
    _f("clinch_indicator",   pa.string(),  "Clinch status (e.g. x, y, z, e, p)"),
    _f("point_differential", pa.float64(), "Average point differential"),
    _f("points_for",         pa.float64(), "Points per game scored"),
    _f("points_against",     pa.float64(), "Points per game allowed"),
    # Provenance
    _f("source",             pa.string(),  "Data vendor provenance",          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. odds — merges Odds baseline + OddsAPI events
#    Partition: season=
#    Discriminator: line_type (spread/moneyline/total/prop)
# ═══════════════════════════════════════════════════════════════════════

NBA_ODDS_SCHEMA = pa.schema([
    _f("id",                  pa.int64(),  "Odds line identifier"),
    _f("game_id",             pa.int64(),  "Parent game identifier",          nullable=False),
    _f("game_date",           pa.string(), "Game date (ISO-8601)"),
    _f("season",              pa.int64(),  "Season start year",               nullable=False),
    _f("vendor",              pa.string(), "Sportsbook / odds vendor"),
    # Discriminator
    _f("line_type",           pa.string(), "Line type — spread/moneyline/total/prop", nullable=False),
    # Teams
    _f("home_team",           pa.string(), "Home team name"),
    _f("away_team",           pa.string(), "Away team name"),
    # Spread
    _f("spread_home_value",   pa.float64(), "Home spread value (e.g. -3.5)"),
    _f("spread_home_odds",    pa.int64(),   "Home spread odds (American)"),
    _f("spread_away_value",   pa.float64(), "Away spread value"),
    _f("spread_away_odds",    pa.int64(),   "Away spread odds (American)"),
    # Moneyline
    _f("moneyline_home_odds", pa.int64(),   "Home moneyline (American)"),
    _f("moneyline_away_odds", pa.int64(),   "Away moneyline (American)"),
    # Totals
    _f("total_value",         pa.float64(), "Over/under total value"),
    _f("total_over_odds",     pa.int64(),   "Over odds (American)"),
    _f("total_under_odds",    pa.int64(),   "Under odds (American)"),
    # Metadata
    _f("updated_at",          pa.string(),  "Timestamp of last odds update"),
    # Provenance
    _f("source",              pa.string(),  "Data vendor provenance",         nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. player_props — merges Odds player_props + OddsAPI props
#    Partition: season=
#    Discriminator: prop_type
# ═══════════════════════════════════════════════════════════════════════

NBA_PLAYER_PROPS_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique prop record identifier"),
    _f("game_id",         pa.int64(),  "Parent game identifier",             nullable=False),
    _f("player_id",       pa.int64(),  "Player identifier"),
    _f("player_name",     pa.string(), "Player display name"),
    _f("season",          pa.int64(),  "Season start year",                  nullable=False),
    _f("vendor",          pa.string(), "Sportsbook / odds vendor"),
    # Discriminator
    _f("prop_type",       pa.string(), "Prop type — points/rebounds/assists/threes/steals/blocks/pts_reb_ast/doubles/etc.", nullable=False),
    # Line
    _f("line_value",      pa.float64(), "Prop line value"),
    _f("market_type",     pa.string(),  "Market type (e.g. over_under, milestone, alternate)"),
    _f("over_odds",       pa.int64(),   "Over odds (American)"),
    _f("under_odds",      pa.int64(),   "Under odds (American)"),
    _f("milestone_odds",  pa.int64(),   "Milestone odds (American, e.g. 25+ pts)"),
    # Metadata
    _f("updated_at",      pa.string(),  "Timestamp of last update"),
    # Provenance
    _f("source",          pa.string(),  "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. advanced — merges NBA Stats advanced + BDL advanced stats
#    Partition: season=
#    Discriminator: metric_type (tracking/hustle/shooting/playtype/
#                                shotdashboard/clutch)
# ═══════════════════════════════════════════════════════════════════════

NBA_ADVANCED_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique record identifier"),
    _f("player_id",       pa.int64(),  "Player identifier"),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("game_id",         pa.int64(),  "Game identifier (null for season aggregate)"),
    _f("season",          pa.int64(),  "Season start year",                  nullable=False),
    _f("season_type",     pa.string(), "Season type — regular/playoffs"),
    # Discriminator
    _f("metric_type",     pa.string(), "Metric type — tracking/hustle/shooting/playtype/shotdashboard/clutch", nullable=False),
    _f("period",          pa.string(), "Period filter (e.g. full_game, q1, q4, ot)"),
    # Core advanced metrics
    _f("pie",                       pa.float64(), "Player impact estimate"),
    _f("assist_percentage",         pa.float64(), "Assist percentage"),
    _f("offensive_rating",          pa.float64(), "Offensive rating"),
    _f("defensive_rating",          pa.float64(), "Defensive rating"),
    _f("net_rating",                pa.float64(), "Net rating"),
    _f("pace",                      pa.float64(), "Pace"),
    _f("usage_percentage",          pa.float64(), "Usage percentage"),
    _f("true_shooting_percentage",  pa.float64(), "True shooting percentage"),
    _f("effective_fg_pct",          pa.float64(), "Effective field goal percentage"),
    _f("turnover_percentage",       pa.float64(), "Turnover percentage"),
    _f("rebound_percentage",        pa.float64(), "Total rebound percentage"),
    _f("oreb_percentage",           pa.float64(), "Offensive rebound percentage"),
    _f("dreb_percentage",           pa.float64(), "Defensive rebound percentage"),
    # Tracking (metric_type=tracking)
    _f("speed",                     pa.float64(), "Average speed (mph)"),
    _f("distance",                  pa.float64(), "Distance covered (miles)"),
    _f("touches",                   pa.int64(),   "Touches"),
    _f("passes",                    pa.int64(),   "Total passes made"),
    _f("secondary_assists",         pa.int64(),   "Secondary assists"),
    _f("free_throw_assists",        pa.int64(),   "Free throw assists"),
    _f("front_court_touches",       pa.int64(),   "Front court touches"),
    _f("time_of_possession",        pa.float64(), "Time of possession (seconds)"),
    _f("avg_sec_per_touch",         pa.float64(), "Average seconds per touch"),
    _f("avg_dribbles_per_touch",    pa.float64(), "Average dribbles per touch"),
    _f("pts_per_touch",             pa.float64(), "Points per touch"),
    _f("elbow_touches",             pa.int64(),   "Elbow touches"),
    _f("post_touches",              pa.int64(),   "Post touches"),
    _f("paint_touches",             pa.int64(),   "Paint touches"),
    # Hustle (metric_type=hustle)
    _f("contested_shots_2pt",       pa.int64(),   "Contested 2-point shots"),
    _f("contested_shots_3pt",       pa.int64(),   "Contested 3-point shots"),
    _f("deflections",               pa.int64(),   "Deflections"),
    _f("charges_drawn",             pa.int64(),   "Charges drawn"),
    _f("screen_assists",            pa.int64(),   "Screen assists"),
    _f("screen_assist_pts",         pa.int64(),   "Points off screen assists"),
    _f("loose_balls_recovered",     pa.int64(),   "Loose balls recovered"),
    _f("box_outs",                  pa.int64(),   "Box outs"),
    _f("box_outs_off",              pa.int64(),   "Offensive box outs"),
    _f("box_outs_def",              pa.int64(),   "Defensive box outs"),
    # Shooting (metric_type=shooting)
    _f("fg_pct_restricted_area",    pa.float64(), "FG% in restricted area"),
    _f("fg_pct_paint_non_ra",       pa.float64(), "FG% in paint (non-restricted area)"),
    _f("fg_pct_midrange",           pa.float64(), "FG% from midrange"),
    _f("fg_pct_left_corner_3",      pa.float64(), "FG% from left corner three"),
    _f("fg_pct_right_corner_3",     pa.float64(), "FG% from right corner three"),
    _f("fg_pct_above_break_3",      pa.float64(), "FG% from above the break three"),
    _f("fga_restricted_area",       pa.int64(),   "FGA in restricted area"),
    _f("fga_paint_non_ra",          pa.int64(),   "FGA in paint (non-restricted area)"),
    _f("fga_midrange",              pa.int64(),   "FGA from midrange"),
    _f("fga_left_corner_3",         pa.int64(),   "FGA from left corner three"),
    _f("fga_right_corner_3",        pa.int64(),   "FGA from right corner three"),
    _f("fga_above_break_3",         pa.int64(),   "FGA from above the break three"),
    # Playtype (metric_type=playtype)
    _f("playtype_name",             pa.string(),  "Play type name (e.g. isolation, pnr_ball_handler, transition)"),
    _f("playtype_poss",             pa.int64(),   "Possessions for this play type"),
    _f("playtype_ppp",              pa.float64(), "Points per possession for this play type"),
    _f("playtype_fg_pct",           pa.float64(), "FG% for this play type"),
    _f("playtype_ft_freq",          pa.float64(), "Free throw frequency for this play type"),
    _f("playtype_turnover_freq",    pa.float64(), "Turnover frequency for this play type"),
    _f("playtype_score_freq",       pa.float64(), "Score frequency for this play type"),
    _f("playtype_and_one_freq",     pa.float64(), "And-one frequency for this play type"),
    _f("playtype_percentile",       pa.float64(), "Percentile rank for this play type"),
    # Shot dashboard (metric_type=shotdashboard)
    _f("shot_distance_range",       pa.string(),  "Shot distance range (e.g. 0-5ft, 5-9ft)"),
    _f("shot_fga",                  pa.int64(),   "FGA for this distance range"),
    _f("shot_fgm",                  pa.int64(),   "FGM for this distance range"),
    _f("shot_fg_pct",               pa.float64(), "FG% for this distance range"),
    _f("shot_efg_pct",              pa.float64(), "eFG% for this distance range"),
    _f("shot_freq",                 pa.float64(), "Shot frequency for this distance range"),
    # Clutch (metric_type=clutch)
    _f("clutch_min",                pa.string(),  "Minutes in clutch situations"),
    _f("clutch_pts",                pa.int64(),   "Points in clutch situations"),
    _f("clutch_fg_pct",             pa.float64(), "FG% in clutch situations"),
    _f("clutch_ft_pct",             pa.float64(), "FT% in clutch situations"),
    _f("clutch_plus_minus",         pa.float64(), "Plus/minus in clutch situations"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. plays — merges NBA Stats playbyplay + BDL plays
#     Partition: season=
#     Discriminator: play_type (shot/rebound/turnover/foul/etc.)
# ═══════════════════════════════════════════════════════════════════════

NBA_PLAYS_SCHEMA = pa.schema([
    _f("game_id",         pa.int64(),  "Parent game identifier",             nullable=False),
    _f("order",           pa.int64(),  "Sequence order within the game",     nullable=False),
    _f("season",          pa.int64(),  "Season start year",                  nullable=False),
    # Game clock context
    _f("period",          pa.int64(),  "Period / quarter"),
    _f("period_display",  pa.string(), "Display label for period (e.g. Q1, OT1)"),
    _f("clock",           pa.string(), "Game clock at time of play (e.g. 07:32)"),
    # Discriminator
    _f("play_type",       pa.string(), "Play type — shot/rebound/turnover/foul/freethrow/violation/substitution/timeout/jumpball/ejection/other", nullable=False),
    _f("text",            pa.string(), "Play-by-play description text"),
    # Scoring context
    _f("home_score",      pa.int64(),  "Home score at time of play"),
    _f("away_score",      pa.int64(),  "Away score at time of play"),
    _f("scoring_play",    pa.bool_(),  "Whether this play resulted in a score"),
    _f("shooting_play",   pa.bool_(),  "Whether this play involved a shot attempt"),
    _f("score_value",     pa.int64(),  "Points scored on this play (0/1/2/3)"),
    # Team
    _f("team_id",         pa.int64(),  "Team involved in the play"),
    _f("team_name",       pa.string(), "Team name involved in the play"),
    # Shot detail
    _f("coordinate_x",   pa.float64(), "Shot chart X coordinate"),
    _f("coordinate_y",    pa.float64(), "Shot chart Y coordinate"),
    _f("shot_distance",   pa.float64(), "Shot distance in feet"),
    _f("shot_result",     pa.string(),  "Shot result — made/missed"),
    _f("shot_type",       pa.string(),  "Shot type description (e.g. Layup, Jump Shot, Dunk)"),
    _f("assisted",        pa.bool_(),   "Whether the shot was assisted"),
    _f("blocked",         pa.bool_(),   "Whether the shot was blocked"),
    # Participants
    _f("wallclock",       pa.string(),  "Wall clock time (ISO-8601)"),
    _f("participants",    pa.string(),  "JSON array of participant player IDs/names"),
    _f("player1_id",      pa.int64(),   "Primary player involved"),
    _f("player1_name",    pa.string(),  "Primary player name"),
    _f("player2_id",      pa.int64(),   "Secondary player involved (e.g. assister, blocker)"),
    _f("player2_name",    pa.string(),  "Secondary player name"),
    _f("player3_id",      pa.int64(),   "Tertiary player involved"),
    _f("player3_name",    pa.string(),  "Tertiary player name"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. box_scores — merges NBA Stats boxscores + BDL box_scores
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

NBA_BOX_SCORES_SCHEMA = pa.schema([
    # Game context
    _f("game_id",             pa.int64(),  "Game identifier",                nullable=False),
    _f("game_date",           pa.string(), "Game date (ISO-8601)"),
    _f("season",              pa.int64(),  "Season start year",              nullable=False),
    _f("season_type",         pa.string(), "Season type — regular/playoffs"),
    _f("status",              pa.string(), "Game status (e.g. final)"),
    _f("period",              pa.int64(),  "Periods played"),
    # Team scores
    _f("home_team_id",        pa.int64(),  "Home team identifier"),
    _f("home_team_name",      pa.string(), "Home team name"),
    _f("home_team_score",     pa.int64(),  "Home team total score"),
    _f("visitor_team_id",     pa.int64(),  "Visitor team identifier"),
    _f("visitor_team_name",   pa.string(), "Visitor team name"),
    _f("visitor_team_score",  pa.int64(),  "Visitor team total score"),
    _f("period_scores",       pa.string(), "Period scores as JSON string"),
    # Player box line
    _f("player_id",           pa.int64(),  "Player identifier"),
    _f("player_name",         pa.string(), "Player display name"),
    _f("team_id",             pa.int64(),  "Player's team identifier"),
    _f("min",                 pa.string(), "Minutes played (e.g. 36:24)"),
    _f("fgm",                pa.int64(),  "Field goals made"),
    _f("fga",                pa.int64(),  "Field goals attempted"),
    _f("fg_pct",             pa.float64(), "Field goal percentage"),
    _f("fg3m",               pa.int64(),  "Three-pointers made"),
    _f("fg3a",               pa.int64(),  "Three-pointers attempted"),
    _f("fg3_pct",            pa.float64(), "Three-point percentage"),
    _f("ftm",                pa.int64(),  "Free throws made"),
    _f("fta",                pa.int64(),  "Free throws attempted"),
    _f("ft_pct",             pa.float64(), "Free throw percentage"),
    _f("oreb",               pa.int64(),  "Offensive rebounds"),
    _f("dreb",               pa.int64(),  "Defensive rebounds"),
    _f("reb",                pa.int64(),  "Total rebounds"),
    _f("ast",                pa.int64(),  "Assists"),
    _f("stl",                pa.int64(),  "Steals"),
    _f("blk",                pa.int64(),  "Blocks"),
    _f("turnover",           pa.int64(),  "Turnovers"),
    _f("pf",                 pa.int64(),  "Personal fouls"),
    _f("pts",                pa.int64(),  "Points scored"),
    _f("plus_minus",         pa.float64(), "Plus/minus"),
    _f("starter",            pa.bool_(),  "Whether the player started the game"),
    _f("dnp_reason",         pa.string(), "Did-not-play reason (null if played)"),
    # Provenance
    _f("source",              pa.string(), "Data vendor provenance",         nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. lineups — merges ESPN depth_charts + BDL lineups
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

NBA_LINEUPS_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique lineup record identifier"),
    _f("game_id",         pa.int64(),  "Game identifier (null for depth charts)"),
    _f("season",          pa.int64(),  "Season start year",                  nullable=False),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("player_id",       pa.int64(),  "Player identifier"),
    _f("player_name",     pa.string(), "Player display name"),
    _f("position",        pa.string(), "Position (e.g. PG, SG, SF, PF, C)"),
    _f("starter",         pa.bool_(),  "Whether the player is a starter"),
    _f("depth_order",     pa.int64(),  "Depth chart position order (1 = starter)"),
    _f("lineup_min",      pa.string(), "Minutes played as this lineup unit (for 5-man lineups)"),
    _f("lineup_plus_minus", pa.float64(), "Plus/minus for this lineup unit"),
    _f("lineup_pts",      pa.int64(),  "Points scored by this lineup unit"),
    _f("lineup_poss",     pa.int64(),  "Possessions played by this lineup unit"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. contracts — merges ESPN athlete contracts + BDL contracts
#     Partition: season=
#     Discriminator: contract_scope (annual/aggregate)
# ═══════════════════════════════════════════════════════════════════════

NBA_CONTRACTS_SCHEMA = pa.schema([
    _f("id",                     pa.int64(),  "Unique contract record identifier"),
    _f("player_id",              pa.int64(),  "Player identifier",              nullable=False),
    _f("player_name",            pa.string(), "Player display name"),
    _f("team_id",                pa.int64(),  "Team identifier"),
    _f("team_name",              pa.string(), "Team name"),
    _f("season",                 pa.int64(),  "Season start year",              nullable=False),
    # Discriminator
    _f("contract_scope",         pa.string(), "Contract scope — annual/aggregate", nullable=False),
    # Contract details
    _f("contract_type",          pa.string(), "Contract type (e.g. standard, two-way, 10-day, rookie)"),
    _f("contract_status",        pa.string(), "Contract status (e.g. active, expired, traded)"),
    _f("start_year",             pa.int64(),  "Contract start year"),
    _f("end_year",               pa.int64(),  "Contract end year"),
    _f("contract_years",         pa.int64(),  "Total contract duration in years"),
    # Financial
    _f("base_salary",            pa.float64(), "Base salary for this season"),
    _f("cap_hit",                pa.float64(), "Salary cap hit for this season"),
    _f("dead_cap",               pa.float64(), "Dead cap value if waived"),
    _f("total_cash",             pa.float64(), "Total cash compensation"),
    _f("total_value",            pa.float64(), "Total contract value"),
    _f("average_salary",         pa.float64(), "Average annual salary"),
    _f("guaranteed_at_signing",  pa.float64(), "Amount guaranteed at signing"),
    _f("total_guaranteed",       pa.float64(), "Total guaranteed amount"),
    # Cap context
    _f("signed_using",           pa.string(),  "Exception used to sign (e.g. Bird, MLE, taxpayer, cap_space)"),
    _f("free_agent_year",        pa.int64(),   "Year of free agency"),
    _f("free_agent_status",      pa.string(),  "Free agency type (e.g. UFA, RFA)"),
    _f("trade_kicker",           pa.float64(), "Trade kicker percentage"),
    _f("player_option_year",     pa.int64(),   "Year of player option (null if none)"),
    _f("team_option_year",       pa.int64(),   "Year of team option (null if none)"),
    _f("rank",                   pa.int64(),   "Salary rank within team or league"),
    # Provenance
    _f("source",                 pa.string(),  "Data vendor provenance",       nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. injuries — merges ESPN injuries/snapshots + BDL injuries
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

NBA_INJURIES_SCHEMA = pa.schema([
    _f("player_id",       pa.int64(),  "Player identifier",                  nullable=False),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("season",          pa.int64(),  "Season start year",                  nullable=False),
    _f("status",          pa.string(), "Injury status (e.g. out, day-to-day, questionable, doubtful, probable)"),
    _f("description",     pa.string(), "Injury description (e.g. Right ankle sprain)"),
    _f("body_part",       pa.string(), "Body part affected"),
    _f("injury_type",     pa.string(), "Type of injury (e.g. sprain, strain, fracture)"),
    _f("return_date",     pa.string(), "Expected return date (ISO-8601)"),
    _f("reported_date",   pa.string(), "Date injury was reported (ISO-8601)"),
    _f("games_missed",    pa.int64(),  "Number of games missed"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 15. leaders — merges NBA Stats league-leaders + BDL leaders
#     Partition: season=
#     Discriminator: stat_type (pts/reb/ast/stl/blk/etc.)
# ═══════════════════════════════════════════════════════════════════════

NBA_LEADERS_SCHEMA = pa.schema([
    _f("player_id",       pa.int64(),  "Player identifier",                  nullable=False),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("season",          pa.int64(),  "Season start year",                  nullable=False),
    _f("season_type",     pa.string(), "Season type — regular/playoffs"),
    # Discriminator
    _f("stat_type",       pa.string(), "Stat category — pts/reb/ast/stl/blk/fg_pct/fg3_pct/ft_pct/min/eff/dd2/td3/etc.", nullable=False),
    # Values
    _f("value",           pa.float64(), "Statistical value"),
    _f("rank",            pa.int64(),   "Rank within the category"),
    _f("games_played",    pa.int64(),   "Games played"),
    _f("per_game",        pa.float64(), "Per-game average"),
    _f("total",           pa.float64(), "Season total"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 16. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

NBA_VENUES_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique venue identifier",            nullable=False),
    _f("name",            pa.string(), "Arena / venue name",                 nullable=False),
    _f("city",            pa.string(), "City"),
    _f("state",           pa.string(), "State or province"),
    _f("country",         pa.string(), "Country"),
    _f("capacity",        pa.int64(),  "Seating capacity"),
    _f("year_opened",     pa.int64(),  "Year arena was opened"),
    _f("surface",         pa.string(), "Court surface type"),
    _f("latitude",        pa.float64(), "Arena latitude"),
    _f("longitude",       pa.float64(), "Arena longitude"),
    _f("timezone",        pa.string(), "IANA timezone identifier"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Registry — schema, partition key, and path look-ups
# ═══════════════════════════════════════════════════════════════════════

NBA_SCHEMAS: dict[str, pa.Schema] = {
    "teams":         NBA_TEAMS_SCHEMA,
    "players":       NBA_PLAYERS_SCHEMA,
    "games":         NBA_GAMES_SCHEMA,
    "player_stats":  NBA_PLAYER_STATS_SCHEMA,
    "team_stats":    NBA_TEAM_STATS_SCHEMA,
    "standings":     NBA_STANDINGS_SCHEMA,
    "odds":          NBA_ODDS_SCHEMA,
    "player_props":  NBA_PLAYER_PROPS_SCHEMA,
    "advanced":      NBA_ADVANCED_SCHEMA,
    "plays":         NBA_PLAYS_SCHEMA,
    "box_scores":    NBA_BOX_SCORES_SCHEMA,
    "lineups":       NBA_LINEUPS_SCHEMA,
    "contracts":     NBA_CONTRACTS_SCHEMA,
    "injuries":      NBA_INJURIES_SCHEMA,
    "leaders":       NBA_LEADERS_SCHEMA,
    "venues":        NBA_VENUES_SCHEMA,
}

NBA_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning (single flat parquets)
    "teams":         [],
    "venues":        [],
    # Season only — NBA uses dates, not weeks
    "players":       ["season"],
    "games":         ["season"],
    "player_stats":  ["season"],
    "team_stats":    ["season"],
    "standings":     ["season"],
    "odds":          ["season"],
    "player_props":  ["season"],
    "advanced":      ["season"],
    "plays":         ["season"],
    "box_scores":    ["season"],
    "lineups":       ["season"],
    "contracts":     ["season"],
    "injuries":      ["season"],
    "leaders":       ["season"],
}

NBA_ENTITY_PATHS: dict[str, str] = {
    "teams":         "teams",
    "players":       "players",
    "games":         "games",
    "player_stats":  "player_stats",
    "team_stats":    "team_stats",
    "standings":     "standings",
    "odds":          "odds",
    "player_props":  "player_props",
    "advanced":      "advanced",
    "plays":         "plays",
    "box_scores":    "box_scores",
    "lineups":       "lineups",
    "contracts":     "contracts",
    "injuries":      "injuries",
    "leaders":       "leaders",
    "venues":        "venues",
}


# ═══════════════════════════════════════════════════════════════════════
# Migration reference — maps old raw data sources → new 16 entity names
# ═══════════════════════════════════════════════════════════════════════

NBA_CONSOLIDATION_MAP: dict[str, str] = {
    # ── ESPN raw data ─────────────────────────────────────────────────
    "espn/teams":                "espn/teams → teams",
    "espn/athletes":             "espn/athletes → players",
    "espn/events":               "espn/events → games",
    "espn/rosters":              "espn/rosters → players",
    "espn/depth_charts":         "espn/depth_charts → lineups",
    "espn/standings":            "espn/standings → standings",
    "espn/injuries":             "espn/injuries → injuries",
    "espn/snapshots/injuries":   "espn/snapshots/injuries → injuries",
    "espn/snapshots/news":       "espn/snapshots/news → players",
    "espn/snapshots/transactions": "espn/snapshots/transactions → players",
    "espn/snapshots/team_stats": "espn/snapshots/team_stats → team_stats",
    "espn/team_schedule":        "espn/team_schedule → games",
    "espn/athletes/contracts":   "espn/athletes/contracts → contracts",
    "espn/reference/teams":      "espn/reference/teams → teams",
    "espn/reference/arenas":     "espn/reference/arenas → venues",
    "espn/reference/games":      "espn/reference/games → games",

    # ── NBA Stats raw data ────────────────────────────────────────────
    "nbastats/reference/all_players":     "nbastats/all_players → players",
    "nbastats/reference/players":         "nbastats/players → players",
    "nbastats/reference/teams":           "nbastats/teams → teams",
    "nbastats/games/boxscore":            "nbastats/boxscore → box_scores",
    "nbastats/games/playbyplay":          "nbastats/playbyplay → plays",
    "nbastats/games/summary":             "nbastats/summary → games",
    "nbastats/player-stats/base":         "nbastats/player-stats/base → player_stats",
    "nbastats/player-stats/advanced":     "nbastats/player-stats/advanced → player_stats",
    "nbastats/player-stats/defense":      "nbastats/player-stats/defense → player_stats",
    "nbastats/player-stats/misc":         "nbastats/player-stats/misc → player_stats",
    "nbastats/player-stats/scoring":      "nbastats/player-stats/scoring → player_stats",
    "nbastats/player-stats/usage":        "nbastats/player-stats/usage → player_stats",
    "nbastats/team-stats/base":           "nbastats/team-stats/base → team_stats",
    "nbastats/team-stats/advanced":       "nbastats/team-stats/advanced → team_stats",
    "nbastats/team-stats/defense":        "nbastats/team-stats/defense → team_stats",
    "nbastats/team-stats/misc":           "nbastats/team-stats/misc → team_stats",
    "nbastats/team-stats/scoring":        "nbastats/team-stats/scoring → team_stats",
    "nbastats/team-stats/usage":          "nbastats/team-stats/usage → team_stats",
    "nbastats/league-leaders":            "nbastats/league-leaders → leaders",
    "nbastats/player-game-logs":          "nbastats/player-game-logs → player_stats",
    "nbastats/team-game-logs":            "nbastats/team-game-logs → team_stats",
    "nbastats/season_aggregates":         "nbastats/season_aggregates → player_stats",
    "nbastats/shot-charts":               "nbastats/shot-charts → advanced",

    # ── Odds raw data ─────────────────────────────────────────────────
    "odds/espn_baseline":        "odds/espn_baseline → odds",
    "odds/player_props":         "odds/player_props → player_props",

    # ── OddsAPI raw data ──────────────────────────────────────────────
    "oddsapi/events":            "oddsapi/events → odds",
    "oddsapi/props":             "oddsapi/props → player_props",
    "oddsapi/scores":            "oddsapi/scores → games",
}

# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names (the prefix of {type}_{season}.parquet)
# to the flat entity directory name under normalized_curated/nba/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
NBA_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "games":            "games",
    "teams":            "teams",
    "players":          "players",
    "player_stats":     "player_stats",
    "team_stats":       "team_stats",
    "standings":        "standings",
    "odds":             "odds",
    "player_props":     "player_props",
    "injuries":         "injuries",
    "play_by_play":     "plays",
    # Aliases / absorbed types
    "team_game_stats":  "team_stats",
    "odds_history":     "odds",
    "roster":           "players",
    "info":             "players",
    "scoreboard":       "games",
    "calendar":         "games",
    "game_box_advanced": "box_scores",
    "stats_advanced":   "advanced",
    "stats_season":     "player_stats",
    "stats_player_season": "player_stats",
    "stats_categories": None,
    # Non-entity normalizer artefacts — skip
    "coaches":          None,
    "news":             None,
    "transactions":     None,
    "schedule_fatigue": None,
    "market_signals":   None,
    "weather":          None,
    "draft":            None,
    "draft_picks":      None,
    "draft_positions":  None,
    "draft_teams":      None,
    "player_portal":    None,
    "player_returning": None,
    "player_usage":     None,
    "rankings":         None,
    "records":          None,
    "recruiting":       None,
    "recruiting_teams": None,
    "recruiting_groups": None,
    "talent":           None,
    "ratings_sp":       None,
    "ratings_sp_conferences": None,
    "ratings_srs":      None,
    "ratings_elo":      None,
    "ratings_fpi":      None,
    "ppa_teams":        None,
    "ppa_games":        None,
    "ppa_players_season": None,
    "ppa_players_games": None,
    "ppa_predicted":    None,
    "plays_stats":      None,
    "plays_types":      None,
    "plays_stats_types": None,
    "stats_game_advanced": None,
    "stats_game_havoc": None,
    "games_teams":      None,
    "games_media":      None,
    "games_players":    None,
    "conferences":      None,
    "metrics_fg_ep":    None,
    "metrics_wp":       None,
    "venues":           "venues",
    "wp_pregame":       None,
    "teams_ats":        None,
    "teams_fbs":        None,
    "lines":            "odds",
    "plays":            "plays",
    "advanced_stats":   "advanced",
    "advanced_batting": None,
    "batter_game_stats": None,
    "pitcher_game_stats": None,
    "match_events":     None,
    "goalie_stats":     None,
    "skater_stats":     None,
    "drives":           None,
}
