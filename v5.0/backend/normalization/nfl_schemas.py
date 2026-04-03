# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — NFL Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 18-entity consolidated design.  Raw data from ESPN, ESPN Meta,
# nflfastr, nflverse, Sleeper, Odds, OddsAPI, Action Network,
# Rotowire, Ticketmaster, Wikipedia, YouTube, Google News, Reddit,
# RSS News, and TheSportsDB providers are merged into 18 wide schemas
# that use discriminator columns (``scope``, ``stat_type``,
# ``line_type``, ``play_type``, ``prop_type``) to distinguish record
# subtypes within a single table.
#
# Entity overview
# ───────────────
#  1. teams            — static reference, no partitioning
#  2. venues           — static reference, no partitioning
#  3. conferences      — static reference, no partitioning (AFC/NFC)
#  4. players          — partition: season=
#  5. games            — partition: season=, week=
#  6. player_stats     — partition: season=, week=  (game + season scope)
#  7. team_stats       — partition: season=, week=  (game + season scope)
#  8. standings        — partition: season=
#  9. odds             — partition: season=, week=  (spread/ml/total)
# 10. player_props     — partition: season=, week=
# 11. plays            — partition: season=, week=  (plays + drives)
# 12. injuries         — partition: season=, week=  (injury reports)
# 13. advanced         — partition: season=, week=  (EPA/CPOE/air_yards)
# 14. roster           — partition: season=         (depth chart)
# 15. rankings         — partition: season=, week=  (power rankings)
# 16. transactions     — partition: season=         (trades/cuts/signings)
# 17. weather          — partition: season=, week=  (game-day weather)
# 18. coaches          — partition: season=         (HC/OC/DC/ST)
#
# NFL uses week-based partitioning (like NCAAF, unlike NBA/MLB).
# Season is the calendar year the season starts (e.g. 2024 for 2024-25).
#
# Merge map (old → new) is available in ``NFL_CONSOLIDATION_MAP``.
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

schema_teams = pa.schema([
    # Core identity
    _f("id",              pa.int32(),  "Unique team identifier",                      nullable=False),
    _f("name",            pa.string(), "Short team name (e.g. Chiefs)",               nullable=False),
    _f("full_name",       pa.string(), "Full team name (e.g. Kansas City Chiefs)",    nullable=False),
    _f("abbreviation",    pa.string(), "Team abbreviation (e.g. KC)",                 nullable=False),
    _f("city",            pa.string(), "City where team is located"),
    _f("conference",      pa.string(), "Conference — AFC or NFC"),
    _f("division",        pa.string(), "Division name (e.g. AFC West, NFC North)"),
    _f("logo_url",        pa.string(), "URL to team logo image"),
    _f("color_primary",   pa.string(), "Primary team colour hex code"),
    _f("color_secondary", pa.string(), "Secondary team colour hex code"),
    _f("is_active",       pa.bool_(),  "Whether the franchise is currently active"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                      nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

schema_venues = pa.schema([
    _f("id",               pa.int32(),   "Unique venue identifier",    nullable=False),
    _f("name",             pa.string(),  "Venue / stadium name",       nullable=False),
    _f("city",             pa.string(),  "City"),
    _f("state",            pa.string(),  "State or province"),
    _f("zip",              pa.string(),  "ZIP / postal code"),
    _f("capacity",         pa.int32(),   "Seating capacity"),
    _f("year_constructed", pa.int32(),   "Year originally constructed"),
    _f("grass",            pa.bool_(),   "True if natural grass surface"),
    _f("dome",             pa.bool_(),   "True if domed / enclosed"),
    _f("roof_type",        pa.string(),  "Roof type (e.g. open, dome, retractable)"),
    _f("timezone",         pa.string(),  "IANA timezone identifier"),
    _f("elevation",        pa.float64(), "Elevation in feet"),
    _f("source",           pa.string(),  "Data vendor provenance",     nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. conferences — static reference (no partitioning)
#    AFC / NFC with four divisions each
# ═══════════════════════════════════════════════════════════════════════

schema_conferences = pa.schema([
    _f("id",           pa.int32(),  "Unique conference identifier",       nullable=False),
    _f("name",         pa.string(), "Full conference name",               nullable=False),
    _f("abbreviation", pa.string(), "Short abbreviation (e.g. AFC, NFC)", nullable=False),
    _f("source",       pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. players — merges ESPN athletes + Sleeper + nflverse rosters
#    Partition: season=
#    Discriminator: status (active/inactive/injured/retired)
# ═══════════════════════════════════════════════════════════════════════

schema_players = pa.schema([
    # Core identity
    _f("id",              pa.int32(),  "Unique player identifier",             nullable=False),
    _f("first_name",      pa.string(), "Player first name",                    nullable=False),
    _f("last_name",       pa.string(), "Player last name",                     nullable=False),
    _f("full_name",       pa.string(), "Full display name"),
    _f("position",        pa.string(), "Position (e.g. QB, RB, WR, TE, K, DEF)"),
    _f("height",          pa.string(), "Height (e.g. 6-2)"),
    _f("weight",          pa.string(), "Weight as string (e.g. 215)"),
    _f("jersey_number",   pa.string(), "Jersey number as string"),
    _f("college",         pa.string(), "College attended"),
    _f("country",         pa.string(), "Country of origin"),
    _f("draft_year",      pa.int32(),  "Year drafted"),
    _f("draft_round",     pa.int32(),  "Draft round"),
    _f("draft_pick",      pa.int32(),  "Draft overall pick number"),
    _f("team_id",         pa.int32(),  "Current team identifier"),
    _f("team_name",       pa.string(), "Current team name"),
    # Discriminator
    _f("status",          pa.string(), "Player status — active/inactive/injured/retired"),
    # Profile
    _f("headshot_url",    pa.string(), "URL to player headshot image"),
    _f("age",             pa.int32(),  "Player age"),
    _f("date_of_birth",   pa.string(), "Date of birth (ISO-8601)"),
    _f("debut_year",      pa.int32(),  "Year of NFL debut"),
    _f("years_pro",       pa.int32(),  "Years of professional experience"),
    _f("is_active",       pa.bool_(),  "Whether player is currently active in NFL"),
    # Partition / provenance
    _f("season",          pa.int32(),  "Season year (e.g. 2024 for 2024-25)",  nullable=False),
    _f("source",          pa.string(), "Data vendor provenance",               nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. games — merges ESPN events + BDL games + nflfastr schedules
#    Partition: season=, week=
#    Discriminator: season_type (regular/postseason/preseason)
# ═══════════════════════════════════════════════════════════════════════

schema_games = pa.schema([
    # Core identity
    _f("id",                  pa.int32(),  "Unique game identifier",           nullable=False),
    _f("date",                pa.string(), "Game date (ISO-8601)",             nullable=False),
    _f("season",              pa.int32(),  "Season year",                      nullable=False),
    _f("week",                pa.int32(),  "Season week number",               nullable=False),
    _f("season_type",         pa.string(), "Season type — regular/postseason/preseason"),
    _f("status",              pa.string(), "Game status (e.g. final, in_progress, scheduled)"),
    _f("period",              pa.int32(),  "Current period / quarter"),
    _f("time",                pa.string(), "Time remaining in current period"),
    _f("datetime",            pa.string(), "Full game datetime (ISO-8601 with timezone)"),
    # Teams
    _f("home_team_id",        pa.int32(),  "Home team identifier"),
    _f("home_team_name",      pa.string(), "Home team name"),
    _f("home_team_score",     pa.int32(),  "Home team total score"),
    _f("away_team_id",        pa.int32(),  "Away team identifier"),
    _f("away_team_name",      pa.string(), "Away team name"),
    _f("away_team_score",     pa.int32(),  "Away team total score"),
    # Period scores
    _f("home_score_q1",       pa.int32(),  "Home 1st quarter score"),
    _f("home_score_q2",       pa.int32(),  "Home 2nd quarter score"),
    _f("home_score_q3",       pa.int32(),  "Home 3rd quarter score"),
    _f("home_score_q4",       pa.int32(),  "Home 4th quarter score"),
    _f("home_score_ot",       pa.int32(),  "Home overtime total score"),
    _f("away_score_q1",       pa.int32(),  "Away 1st quarter score"),
    _f("away_score_q2",       pa.int32(),  "Away 2nd quarter score"),
    _f("away_score_q3",       pa.int32(),  "Away 3rd quarter score"),
    _f("away_score_q4",       pa.int32(),  "Away 4th quarter score"),
    _f("away_score_ot",       pa.int32(),  "Away overtime total score"),
    # Flags
    _f("postseason",          pa.bool_(),  "Whether this is a postseason game"),
    _f("overtime",            pa.bool_(),  "Whether the game went to overtime"),
    # Venue
    _f("venue_id",            pa.int32(),  "Venue identifier"),
    _f("venue_name",          pa.string(), "Venue / stadium name"),
    _f("attendance",          pa.int32(),  "Reported attendance"),
    # Broadcast
    _f("tv_network",          pa.string(), "TV broadcast network"),
    _f("duration_minutes",    pa.int32(),  "Game duration in minutes"),
    # Provenance
    _f("source",              pa.string(), "Data vendor provenance",           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. player_stats — merges per-game + season stats via scope + stat_type
#    Partition: season=, week=
#    Discriminators:
#      scope     — game / season
#      stat_type — passing / rushing / receiving / defense / kicking /
#                  returning
# ═══════════════════════════════════════════════════════════════════════

schema_player_stats = pa.schema([
    # Identity
    _f("id",              pa.int32(),  "Unique record identifier"),
    _f("player_id",       pa.int32(),  "Player identifier",                   nullable=False),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int32(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("game_id",         pa.int32(),  "Game identifier (null for season scope)"),
    _f("game_date",       pa.string(), "Game date (ISO-8601)"),
    # Discriminators
    _f("season",          pa.int32(),  "Season year",                         nullable=False),
    _f("week",            pa.int32(),  "Week number (null for season scope)"),
    _f("season_type",     pa.string(), "Season type — regular/postseason"),
    _f("scope",           pa.string(), "Record scope — game or season",       nullable=False),
    _f("stat_type",       pa.string(), "Stat category — passing/rushing/receiving/defense/kicking/returning", nullable=False),
    # Games context
    _f("games_played",    pa.int32(),  "Games played (season scope)"),
    _f("games_started",   pa.int32(),  "Games started (season scope)"),
    # ── Passing (stat_type=passing) ──
    _f("passing_completions",   pa.int32(),   "Pass completions"),
    _f("passing_attempts",      pa.int32(),   "Pass attempts"),
    _f("passing_yards",         pa.int32(),   "Passing yards"),
    _f("passing_touchdowns",    pa.int32(),   "Passing touchdowns"),
    _f("passing_interceptions", pa.int32(),   "Interceptions thrown"),
    _f("passing_rating",        pa.float64(), "Passer rating"),
    _f("passing_qbr",           pa.float64(), "ESPN QBR"),
    _f("passing_sacks",         pa.int32(),   "Times sacked"),
    _f("passing_sack_yards",    pa.int32(),   "Yards lost to sacks"),
    _f("passing_long",          pa.int32(),   "Longest pass completion"),
    _f("passing_yards_per_game", pa.float64(), "Passing yards per game (season scope)"),
    # ── Rushing (stat_type=rushing) ──
    _f("rushing_attempts",       pa.int32(),   "Rush attempts"),
    _f("rushing_yards",          pa.int32(),   "Rushing yards"),
    _f("rushing_touchdowns",     pa.int32(),   "Rushing touchdowns"),
    _f("rushing_yards_per_carry", pa.float64(), "Yards per carry"),
    _f("rushing_long",           pa.int32(),   "Longest rush"),
    _f("rushing_fumbles",        pa.int32(),   "Fumbles on rushing plays"),
    _f("rushing_fumbles_lost",   pa.int32(),   "Fumbles lost on rushing plays"),
    _f("rushing_yards_per_game", pa.float64(), "Rushing yards per game (season scope)"),
    # ── Receiving (stat_type=receiving) ──
    _f("receptions",              pa.int32(),   "Total receptions"),
    _f("receiving_targets",       pa.int32(),   "Pass targets"),
    _f("receiving_yards",         pa.int32(),   "Receiving yards"),
    _f("receiving_touchdowns",    pa.int32(),   "Receiving touchdowns"),
    _f("receiving_yards_per_catch", pa.float64(), "Yards per reception"),
    _f("receiving_long",          pa.int32(),   "Longest reception"),
    _f("receiving_yards_per_game", pa.float64(), "Receiving yards per game (season scope)"),
    # ── Defense (stat_type=defense) ──
    _f("total_tackles",      pa.int32(),   "Total tackles"),
    _f("solo_tackles",       pa.int32(),   "Solo tackles"),
    _f("assist_tackles",     pa.int32(),   "Assisted tackles"),
    _f("tackles_for_loss",   pa.float64(), "Tackles for loss"),
    _f("sacks",              pa.float64(), "Sacks"),
    _f("sack_yards",         pa.float64(), "Sack yards"),
    _f("qb_hits",            pa.int32(),   "QB hits"),
    _f("interceptions",      pa.int32(),   "Defensive interceptions"),
    _f("interception_yards", pa.int32(),   "Interception return yards"),
    _f("interception_tds",   pa.int32(),   "Interception return touchdowns"),
    _f("passes_defended",    pa.int32(),   "Passes defended / broken up"),
    _f("fumbles_forced",     pa.int32(),   "Fumbles forced"),
    _f("fumbles_recovered",  pa.int32(),   "Fumbles recovered"),
    _f("fumble_recovery_tds", pa.int32(),  "Fumble recovery touchdowns"),
    _f("safeties",           pa.int32(),   "Safeties"),
    # ── Kicking (stat_type=kicking) ──
    _f("fg_made",            pa.int32(),   "Field goals made"),
    _f("fg_attempted",       pa.int32(),   "Field goals attempted"),
    _f("fg_pct",             pa.float64(), "Field goal percentage"),
    _f("fg_long",            pa.int32(),   "Longest field goal"),
    _f("xp_made",            pa.int32(),   "Extra points made"),
    _f("xp_attempted",       pa.int32(),   "Extra points attempted"),
    _f("kicking_points",     pa.int32(),   "Total kicking points"),
    # ── Returning (stat_type=returning) ──
    _f("kick_returns",       pa.int32(),   "Kick returns"),
    _f("kick_return_yards",  pa.int32(),   "Kick return yards"),
    _f("kick_return_tds",    pa.int32(),   "Kick return touchdowns"),
    _f("kick_return_long",   pa.int32(),   "Longest kick return"),
    _f("punt_returns",       pa.int32(),   "Punt returns"),
    _f("punt_return_yards",  pa.int32(),   "Punt return yards"),
    _f("punt_return_tds",    pa.int32(),   "Punt return touchdowns"),
    _f("punt_return_long",   pa.int32(),   "Longest punt return"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. team_stats — merges per-game + season stats via scope discriminator
#    Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_team_stats = pa.schema([
    # Identity
    _f("team_id",   pa.int32(),  "Team identifier"),
    _f("team_name", pa.string(), "Team name"),
    _f("game_id",   pa.int32(),  "Game identifier (null for season scope)"),
    # Discriminator
    _f("scope",  pa.string(), "Record scope — 'game' or 'season'", nullable=False),
    _f("season", pa.int32(),  "Season year",                       nullable=False),
    _f("week",   pa.int32(),  "Week number (null for season scope)"),
    # Game-level box score
    _f("first_downs",             pa.int32(),   "Total first downs"),
    _f("first_downs_passing",     pa.int32(),   "First downs via passing"),
    _f("first_downs_rushing",     pa.int32(),   "First downs via rushing"),
    _f("first_downs_penalty",     pa.int32(),   "First downs via penalty"),
    _f("third_down_conversions",  pa.int32(),   "Third-down conversions"),
    _f("third_down_attempts",     pa.int32(),   "Third-down attempts"),
    _f("fourth_down_conversions", pa.int32(),   "Fourth-down conversions"),
    _f("fourth_down_attempts",    pa.int32(),   "Fourth-down attempts"),
    _f("passing_completions",     pa.int32(),   "Passing completions"),
    _f("passing_attempts",        pa.int32(),   "Passing attempts"),
    _f("passing_yards",           pa.float64(), "Passing yards"),
    _f("passing_touchdowns",      pa.int32(),   "Passing touchdowns"),
    _f("passing_interceptions",   pa.int32(),   "Passing interceptions"),
    _f("passing_sacks",           pa.int32(),   "Times QB was sacked"),
    _f("passing_sack_yards",      pa.int32(),   "Yards lost to sacks"),
    _f("rushing_attempts",        pa.int32(),   "Rush attempts"),
    _f("rushing_yards",           pa.float64(), "Rushing yards"),
    _f("rushing_touchdowns",      pa.int32(),   "Rushing touchdowns"),
    _f("total_yards",             pa.float64(), "Total offensive yards"),
    _f("turnovers",               pa.int32(),   "Total turnovers"),
    _f("fumbles",                 pa.int32(),   "Total fumbles"),
    _f("fumbles_lost",            pa.int32(),   "Fumbles lost"),
    _f("penalties",               pa.int32(),   "Total penalties"),
    _f("penalty_yards",           pa.int32(),   "Total penalty yards"),
    _f("possession_time",         pa.string(),  "Time of possession (mm:ss)"),
    _f("redzone_attempts",        pa.int32(),   "Red zone attempts"),
    _f("redzone_conversions",     pa.int32(),   "Red zone conversions"),
    # Season aggregate
    _f("points_scored",           pa.float64(), "Season total points scored"),
    _f("points_allowed",          pa.float64(), "Season total points allowed"),
    _f("passing_yards_per_game",  pa.float64(), "Season avg passing yards per game"),
    _f("rushing_yards_per_game",  pa.float64(), "Season avg rushing yards per game"),
    _f("total_yards_per_game",    pa.float64(), "Season avg total yards per game"),
    _f("opp_passing_yards",       pa.float64(), "Season opponent passing yards"),
    _f("opp_rushing_yards",       pa.float64(), "Season opponent rushing yards"),
    _f("opp_total_yards",         pa.float64(), "Season opponent total yards"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. standings — merges standings + ATS records
#    Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_standings = pa.schema([
    _f("team_id",           pa.int32(),   "Team identifier"),
    _f("team_name",         pa.string(),  "Team name"),
    _f("conference",        pa.string(),  "Conference — AFC or NFC"),
    _f("division",          pa.string(),  "Division name"),
    _f("season",            pa.int32(),   "Season year", nullable=False),
    # Win/loss
    _f("wins",              pa.int32(),   "Total wins"),
    _f("losses",            pa.int32(),   "Total losses"),
    _f("ties",              pa.int32(),   "Total ties"),
    _f("win_pct",           pa.float64(), "Win percentage"),
    _f("conference_wins",   pa.int32(),   "Conference wins"),
    _f("conference_losses", pa.int32(),   "Conference losses"),
    _f("division_wins",     pa.int32(),   "Division wins"),
    _f("division_losses",   pa.int32(),   "Division losses"),
    _f("home_record",       pa.string(),  "Home record (e.g. 6-2)"),
    _f("away_record",       pa.string(),  "Away record (e.g. 4-4)"),
    _f("streak",            pa.string(),  "Current streak (e.g. W5, L2)"),
    _f("playoff_seed",      pa.int32(),   "Playoff seed (null if not clinched)"),
    _f("clinch_indicator",  pa.string(),  "Clinch status (e.g. x, y, z, e, p)"),
    _f("points_for",        pa.float64(), "Total points scored"),
    _f("points_against",    pa.float64(), "Total points allowed"),
    _f("point_differential", pa.float64(), "Point differential"),
    _f("strength_of_schedule", pa.float64(), "Strength of schedule"),
    # ATS (against the spread)
    _f("ats_wins",          pa.int32(),   "ATS wins"),
    _f("ats_losses",        pa.int32(),   "ATS losses"),
    _f("ats_pushes",        pa.int32(),   "ATS pushes"),
    _f("over_wins",         pa.int32(),   "Over wins"),
    _f("over_losses",       pa.int32(),   "Over losses"),
    _f("over_pushes",       pa.int32(),   "Over pushes"),
    # Provenance
    _f("source",            pa.string(),  "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. odds — merges pregame + live + history lines
#    Partition: season=, week=
#    Discriminator: line_type (spread/moneyline/total/pregame/live/history)
# ═══════════════════════════════════════════════════════════════════════

schema_odds = pa.schema([
    _f("id",        pa.int32(),  "Odds line identifier"),
    _f("game_id",   pa.int32(),  "Parent game identifier", nullable=False),
    _f("game_date", pa.string(), "Game date (ISO-8601)"),
    _f("vendor",    pa.string(), "Sportsbook / odds vendor"),
    _f("line_type", pa.string(), "Line type — pregame, live, or history", nullable=False),
    # Teams
    _f("home_team",           pa.string(),  "Home team name"),
    _f("away_team",           pa.string(),  "Away team name"),
    # Spread
    _f("spread_home_value",   pa.float64(), "Home spread value (e.g. -3.5)"),
    _f("spread_home_odds",    pa.int32(),   "Home spread odds (American)"),
    _f("spread_away_value",   pa.float64(), "Away spread value"),
    _f("spread_away_odds",    pa.int32(),   "Away spread odds (American)"),
    # Moneyline
    _f("moneyline_home_odds", pa.int32(),   "Home moneyline (American)"),
    _f("moneyline_away_odds", pa.int32(),   "Away moneyline (American)"),
    # Totals
    _f("total_value",         pa.float64(), "Over/under total value"),
    _f("total_over_odds",     pa.int32(),   "Over odds (American)"),
    _f("total_under_odds",    pa.int32(),   "Under odds (American)"),
    # Metadata
    _f("updated_at", pa.string(), "Timestamp of last odds update"),
    _f("season",     pa.int32(),  "Season year",          nullable=False),
    _f("week",       pa.int32(),  "Season week number",   nullable=False),
    _f("source",     pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. player_props — merges Odds + OddsAPI player props
#     Partition: season=, week=
#     Discriminator: prop_type
# ═══════════════════════════════════════════════════════════════════════

schema_player_props = pa.schema([
    _f("id",              pa.int32(),  "Unique prop record identifier"),
    _f("game_id",         pa.int32(),  "Parent game identifier",             nullable=False),
    _f("player_id",       pa.int32(),  "Player identifier"),
    _f("player_name",     pa.string(), "Player display name"),
    _f("season",          pa.int32(),  "Season year",                        nullable=False),
    _f("week",            pa.int32(),  "Season week number",                 nullable=False),
    _f("vendor",          pa.string(), "Sportsbook / odds vendor"),
    # Discriminator
    _f("prop_type",       pa.string(), "Prop type — passing_yards/rushing_yards/receiving_yards/touchdowns/receptions/completions/interceptions/etc.", nullable=False),
    # Line
    _f("line_value",      pa.float64(), "Prop line value"),
    _f("market_type",     pa.string(),  "Market type (e.g. over_under, alternate)"),
    _f("over_odds",       pa.int32(),   "Over odds (American)"),
    _f("under_odds",      pa.int32(),   "Under odds (American)"),
    # Metadata
    _f("updated_at",      pa.string(),  "Timestamp of last update"),
    # Provenance
    _f("source",          pa.string(),  "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. plays — merges play-by-play + drives via play_type discriminator
#     Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_plays = pa.schema([
    _f("game_id",   pa.int32(),  "Parent game identifier",          nullable=False),
    _f("play_type", pa.string(), "Record type — 'play' or 'drive'", nullable=False),
    _f("order",     pa.int32(),  "Sequence order within the game",  nullable=False),
    _f("type",      pa.string(), "Play type (e.g. Rush, Pass, Punt, Kickoff, Field Goal) or drive label"),
    _f("text",      pa.string(), "Play-by-play description text"),
    # Scoring context
    _f("home_score",    pa.int32(),  "Home score at time of play/drive"),
    _f("away_score",    pa.int32(),  "Away score at time of play/drive"),
    _f("period",        pa.int32(),  "Quarter / period"),
    _f("clock",         pa.string(), "Game clock at time of play"),
    _f("scoring_play",  pa.bool_(),  "Whether this play resulted in a score"),
    _f("score_value",   pa.int32(),  "Points scored on this play"),
    # Team
    _f("team_id",   pa.int32(),  "Team with possession"),
    _f("team_name", pa.string(), "Team name with possession"),
    # Play-specific fields (play_type='play')
    _f("down",              pa.int32(),   "Down (1-4)"),
    _f("distance",          pa.int32(),   "Yards to go"),
    _f("yard_line",         pa.int32(),   "Yard line of scrimmage"),
    _f("yards_gained",      pa.int32(),   "Yards gained on the play"),
    _f("penalty",           pa.bool_(),   "Whether a penalty occurred"),
    _f("penalty_yards",     pa.int32(),   "Penalty yardage"),
    _f("play_result",       pa.string(),  "Play result (e.g. complete, incomplete, rush, sack, penalty)"),
    # Drive-specific fields (play_type='drive')
    _f("drive_number",              pa.int32(),  "Drive sequence number"),
    _f("drive_plays",               pa.int32(),  "Number of plays in drive"),
    _f("drive_yards",               pa.int32(),  "Total yards gained in drive"),
    _f("drive_time_of_possession",  pa.string(), "Drive time of possession (mm:ss)"),
    _f("drive_result",              pa.string(), "Drive result (e.g. Touchdown, Punt, Field Goal, Turnover)"),
    _f("start_yardline",            pa.string(), "Starting yard line"),
    _f("end_yardline",              pa.string(), "Ending yard line"),
    # Partition / provenance
    _f("season", pa.int32(),  "Season year",            nullable=False),
    _f("week",   pa.int32(),  "Season week number",     nullable=False),
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. injuries — injury reports and game status
#     Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_injuries = pa.schema([
    _f("player_id",       pa.int32(),  "Player identifier",                   nullable=False),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int32(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("season",          pa.int32(),  "Season year",                         nullable=False),
    _f("week",            pa.int32(),  "Season week number",                  nullable=False),
    _f("status",          pa.string(), "Injury status (e.g. out, doubtful, questionable, probable)"),
    _f("description",     pa.string(), "Injury description (e.g. Knee - ACL)"),
    _f("body_part",       pa.string(), "Body part affected"),
    _f("practice_status", pa.string(), "Practice participation (e.g. DNP, limited, full)"),
    _f("game_status",     pa.string(), "Official game designation (e.g. out, doubtful, questionable)"),
    _f("reported_date",   pa.string(), "Date injury was reported (ISO-8601)"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",              nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. advanced — merges nflfastr/nflverse advanced stats via stat_type
#     Partition: season=, week=
#     Discriminator: stat_type (passing/rushing/receiving/defense)
# ═══════════════════════════════════════════════════════════════════════

schema_advanced = pa.schema([
    _f("player_id",   pa.int32(),  "Player identifier"),
    _f("player_name", pa.string(), "Player display name"),
    _f("team_id",     pa.int32(),  "Team identifier"),
    _f("team_name",   pa.string(), "Team name"),
    _f("game_id",     pa.int32(),  "Game identifier"),
    _f("season",      pa.int32(),  "Season year",      nullable=False),
    _f("week",        pa.int32(),  "Season week number", nullable=False),
    _f("stat_type",   pa.string(), "Stat type — passing/rushing/receiving/defense", nullable=False),
    # ── Passing advanced (stat_type=passing) ──
    _f("epa",                    pa.float64(), "Expected Points Added"),
    _f("cpoe",                   pa.float64(), "Completion Percentage Over Expected"),
    _f("air_yards",              pa.float64(), "Total air yards"),
    _f("air_yards_per_attempt",  pa.float64(), "Air yards per attempt"),
    _f("intended_air_yards",     pa.float64(), "Intended air yards"),
    _f("yac",                    pa.float64(), "Yards After Catch"),
    _f("pressure_rate",          pa.float64(), "Pressure rate faced"),
    _f("blitz_rate",             pa.float64(), "Blitz rate faced"),
    _f("time_to_throw",          pa.float64(), "Average time to throw (seconds)"),
    _f("on_target_pct",          pa.float64(), "On-target throw percentage"),
    _f("bad_throw_pct",          pa.float64(), "Bad throw percentage"),
    _f("dropback_epa",           pa.float64(), "EPA on dropbacks"),
    # ── Rushing advanced (stat_type=rushing) ──
    _f("rushing_epa",            pa.float64(), "Rushing EPA"),
    _f("yards_over_expected",    pa.float64(), "Rushing yards over expected"),
    _f("stuff_rate",             pa.float64(), "Stuff rate (% of runs stopped at or behind LOS)"),
    _f("yards_after_contact",    pa.float64(), "Yards after contact"),
    _f("breakaway_rate",         pa.float64(), "Breakaway run rate (15+ yards)"),
    _f("yards_before_contact",   pa.float64(), "Yards before contact"),
    # ── Receiving advanced (stat_type=receiving) ──
    _f("receiving_epa",          pa.float64(), "Receiving EPA"),
    _f("separation",             pa.float64(), "Average separation from defender"),
    _f("target_share",           pa.float64(), "Target share of team targets"),
    _f("air_yards_share",        pa.float64(), "Air yards share of team air yards"),
    _f("cushion",                pa.float64(), "Average cushion at snap"),
    _f("adot",                   pa.float64(), "Average depth of target"),
    _f("catch_rate",             pa.float64(), "Catch rate (receptions / targets)"),
    _f("catch_rate_over_expected", pa.float64(), "Catch rate over expected"),
    _f("drop_rate",              pa.float64(), "Drop rate"),
    # ── Defense advanced (stat_type=defense) ──
    _f("epa_allowed",            pa.float64(), "EPA allowed"),
    _f("def_pressure_rate",      pa.float64(), "Defensive pressure rate generated"),
    _f("coverage_rating",        pa.float64(), "Coverage rating"),
    _f("man_coverage_pct",       pa.float64(), "Man coverage snap percentage"),
    _f("zone_coverage_pct",      pa.float64(), "Zone coverage snap percentage"),
    _f("missed_tackle_rate",     pa.float64(), "Missed tackle rate"),
    _f("target_rate_allowed",    pa.float64(), "Target rate allowed in coverage"),
    _f("yards_per_target_allowed", pa.float64(), "Yards per target allowed in coverage"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. roster — depth chart and roster positions
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_roster = pa.schema([
    _f("player_id",            pa.int32(),  "Player identifier",              nullable=False),
    _f("player_name",          pa.string(), "Player display name"),
    _f("team_id",              pa.int32(),  "Team identifier"),
    _f("team_name",            pa.string(), "Team name"),
    _f("season",               pa.int32(),  "Season year",                    nullable=False),
    _f("position",             pa.string(), "Roster position (e.g. QB, WR, CB)"),
    _f("depth_chart_position", pa.string(), "Depth chart position label"),
    _f("depth_order",          pa.int32(),  "Depth chart order (1 = starter)"),
    _f("jersey_number",        pa.string(), "Jersey number"),
    _f("status",               pa.string(), "Roster status (e.g. active, practice_squad, IR, PUP)"),
    _f("experience",           pa.int32(),  "Years of NFL experience"),
    # Provenance
    _f("source",               pa.string(), "Data vendor provenance",         nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 15. rankings — power rankings and polls
#     Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_rankings = pa.schema([
    _f("team_id",           pa.int32(),  "Team identifier"),
    _f("team_name",         pa.string(), "Team name"),
    _f("season",            pa.int32(),  "Season year",        nullable=False),
    _f("week",              pa.int32(),  "Season week number", nullable=False),
    _f("poll",              pa.string(), "Poll name — power_ranking, AP, Coaches"),
    _f("rank",              pa.int32(),  "Rank within the poll", nullable=False),
    _f("previous_rank",     pa.int32(),  "Previous week rank"),
    _f("points",            pa.int32(),  "Total poll points"),
    _f("first_place_votes", pa.int32(),  "Number of first-place votes"),
    _f("trend",             pa.string(), "Ranking trend vs prior week"),
    _f("record",            pa.string(), "Team record at time of ranking"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 16. transactions — trades, cuts, signings, waivers
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_transactions = pa.schema([
    _f("id",                pa.int32(),  "Unique transaction identifier"),
    _f("player_id",         pa.int32(),  "Player identifier"),
    _f("player_name",       pa.string(), "Player display name"),
    _f("team_id",           pa.int32(),  "Primary team identifier"),
    _f("team_name",         pa.string(), "Primary team name"),
    _f("season",            pa.int32(),  "Season year",                       nullable=False),
    _f("transaction_type",  pa.string(), "Type — trade/cut/signing/waiver/ir/suspension/pup/retirement"),
    _f("date",              pa.string(), "Transaction date (ISO-8601)"),
    _f("description",       pa.string(), "Transaction description text"),
    _f("from_team",         pa.string(), "Originating team (for trades)"),
    _f("to_team",           pa.string(), "Destination team (for trades)"),
    # Provenance
    _f("source",            pa.string(), "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 17. weather — game-day weather conditions
#     Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_weather = pa.schema([
    _f("game_id",        pa.int32(),   "Parent game identifier",            nullable=False),
    _f("season",         pa.int32(),   "Season year",                       nullable=False),
    _f("week",           pa.int32(),   "Season week number",                nullable=False),
    _f("temperature",    pa.float64(), "Temperature at kickoff (°F)"),
    _f("wind_speed",     pa.float64(), "Wind speed (mph)"),
    _f("wind_direction", pa.string(),  "Wind direction (e.g. NNW)"),
    _f("humidity",       pa.float64(), "Relative humidity (%)"),
    _f("precipitation",  pa.float64(), "Precipitation probability or amount"),
    _f("conditions",     pa.string(),  "Weather conditions description (e.g. Clear, Rain, Snow)"),
    _f("dome",           pa.bool_(),   "True if game played in a dome"),
    _f("visibility",     pa.float64(), "Visibility in miles"),
    # Provenance
    _f("source",         pa.string(),  "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 18. coaches — head coaches and coordinators
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_coaches = pa.schema([
    _f("id",              pa.int32(),  "Unique coach identifier"),
    _f("first_name",      pa.string(), "Coach first name"),
    _f("last_name",       pa.string(), "Coach last name"),
    _f("team_id",         pa.int32(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("season",          pa.int32(),  "Season year",                        nullable=False),
    _f("role",            pa.string(), "Coaching role — HC, OC, DC, or ST"),
    _f("years_with_team", pa.int32(),  "Seasons as coach at this team"),
    _f("career_wins",     pa.int32(),  "Career wins"),
    _f("career_losses",   pa.int32(),  "Career losses"),
    _f("career_ties",     pa.int32(),  "Career ties"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Registry — schema, partition key, and path look-ups
# ═══════════════════════════════════════════════════════════════════════

NFL_SCHEMAS: dict[str, pa.Schema] = {
    "teams":         schema_teams,
    "venues":        schema_venues,
    "conferences":   schema_conferences,
    "players":       schema_players,
    "games":         schema_games,
    "player_stats":  schema_player_stats,
    "team_stats":    schema_team_stats,
    "standings":     schema_standings,
    "odds":          schema_odds,
    "player_props":  schema_player_props,
    "plays":         schema_plays,
    "injuries":      schema_injuries,
    "advanced":      schema_advanced,
    "roster":        schema_roster,
    "rankings":      schema_rankings,
    "transactions":  schema_transactions,
    "weather":       schema_weather,
    "coaches":       schema_coaches,
}

NFL_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning (single flat parquets)
    "teams":         [],
    "venues":        [],
    "conferences":   [],
    # Season only
    "players":       ["season"],
    "standings":     ["season"],
    "roster":        ["season"],
    "transactions":  ["season"],
    "coaches":       ["season"],
    # Season + week
    "games":         ["season", "week"],
    "player_stats":  ["season", "week"],
    "team_stats":    ["season", "week"],
    "odds":          ["season", "week"],
    "player_props":  ["season", "week"],
    "plays":         ["season", "week"],
    "injuries":      ["season", "week"],
    "advanced":      ["season", "week"],
    "rankings":      ["season", "week"],
    "weather":       ["season", "week"],
}

NFL_ENTITY_PATHS: dict[str, str] = {
    "teams":         "teams",
    "venues":        "venues",
    "conferences":   "conferences",
    "players":       "players",
    "games":         "games",
    "player_stats":  "player_stats",
    "team_stats":    "team_stats",
    "standings":     "standings",
    "odds":          "odds",
    "player_props":  "player_props",
    "plays":         "plays",
    "injuries":      "injuries",
    "advanced":      "advanced",
    "roster":        "roster",
    "rankings":      "rankings",
    "transactions":  "transactions",
    "weather":       "weather",
    "coaches":       "coaches",
}


# ═══════════════════════════════════════════════════════════════════════
# Migration reference — maps old raw data sources → new 18 entity names
# ═══════════════════════════════════════════════════════════════════════

NFL_CONSOLIDATION_MAP: dict[str, str] = {
    # ── ESPN raw data ─────────────────────────────────────────────────
    "espn/teams":                   "espn/teams → teams",
    "espn/athletes":                "espn/athletes → players",
    "espn/events":                  "espn/events → games",
    "espn/standings":               "espn/standings → standings",
    "espn/injuries":                "espn/injuries → injuries",
    "espn/depth_charts":            "espn/depth_charts → roster",
    "espn/rosters":                 "espn/rosters → roster",
    "espn/transactions":            "espn/transactions → transactions",
    "espn/team_schedule":           "espn/team_schedule → games",

    # ── ESPN Meta raw data ────────────────────────────────────────────
    "espnmeta/reference/teams":     "espnmeta/reference/teams → teams",
    "espnmeta/reference/venues":    "espnmeta/reference/venues → venues",
    "espnmeta/reference/games":     "espnmeta/reference/games → games",

    # ── nflfastr raw data ─────────────────────────────────────────────
    "nflfastr/play_by_play":        "nflfastr/play_by_play → plays",
    "nflfastr/player_stats":        "nflfastr/player_stats → player_stats",
    "nflfastr/roster":              "nflfastr/roster → roster",

    # ── nflverse raw data ─────────────────────────────────────────────
    "nflverse/player_stats":        "nflverse/player_stats → player_stats",
    "nflverse/advanced_passing":    "nflverse/advanced_passing → advanced",
    "nflverse/advanced_rushing":    "nflverse/advanced_rushing → advanced",
    "nflverse/advanced_receiving":  "nflverse/advanced_receiving → advanced",
    "nflverse/roster":              "nflverse/roster → roster",
    "nflverse/injuries":            "nflverse/injuries → injuries",

    # ── Sleeper raw data ──────────────────────────────────────────────
    "sleeper/players":              "sleeper/players → players",
    "sleeper/projections":          "sleeper/projections → player_stats",

    # ── Odds raw data ─────────────────────────────────────────────────
    "odds/espn_baseline":           "odds/espn_baseline → odds",
    "odds/player_props":            "odds/player_props → player_props",
    "odds/history":                 "odds/history → odds",

    # ── OddsAPI raw data ──────────────────────────────────────────────
    "oddsapi/events":               "oddsapi/events → odds",
    "oddsapi/props":                "oddsapi/props → player_props",

    # ── Action Network raw data ───────────────────────────────────────
    "actionnetwork/odds":           "actionnetwork/odds → odds",
    "actionnetwork/props":          "actionnetwork/props → player_props",

    # ── Rotowire raw data ─────────────────────────────────────────────
    "rotowire/injuries":            "rotowire/injuries → injuries",
    "rotowire/weather":             "rotowire/weather → weather",

    # ── TheSportsDB raw data ──────────────────────────────────────────
    "thesportsdb/teams":            "thesportsdb/teams → teams",
    "thesportsdb/venues":           "thesportsdb/venues → venues",
}


# ═══════════════════════════════════════════════════════════════════════
# Normalizer data-type → entity routing
# ═══════════════════════════════════════════════════════════════════════
# Maps the normalizer's data-type names (the prefix of {type}_{season}.parquet)
# to the flat entity directory name under normalized_curated/nfl/.
# Types mapping to None are intentionally skipped (non-entity artefacts).

NFL_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Core
    "teams":            "teams",
    "players":          "players",
    "games":            "games",
    "standings":        "standings",
    "venues":           "venues",
    "conferences":      "conferences",

    # Stats
    "player_stats":     "player_stats",
    "team_stats":       "team_stats",
    "team_game_stats":  "team_stats",

    # Betting
    "odds":             "odds",
    "odds_history":     "odds",
    "player_props":     "player_props",

    # Event data
    "play_by_play":     "plays",
    "drives":           "plays",

    # Supplementary
    "injuries":         "injuries",
    "transactions":     "transactions",
    "weather":          "weather",
    "coaches":          "coaches",
    "roster":           "roster",

    # Skip
    "news":             None,
    "scoreboard":       None,
    "info":             None,
    "calendar":         None,

    # nflfastr/nflverse types
    "advanced_passing":    "advanced",
    "advanced_rushing":    "advanced",
    "advanced_receiving":  "advanced",
    "stats_advanced":      "advanced",
    "rankings":            "rankings",
}
