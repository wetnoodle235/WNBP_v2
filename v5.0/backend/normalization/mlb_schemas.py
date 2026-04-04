# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — MLB Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 17-entity consolidated design.  Raw data from ESPN, MLB Stats,
# Statcast, Lahman, Retrosheet, Odds, OddsAPI, and other providers
# are merged into 17 wide schemas that use discriminator columns
# (``scope``, ``stat_type``, ``line_type``, ``play_type``,
# ``prop_type``) to distinguish record subtypes within a single table.
#
# Entity overview
# ───────────────
#  1. teams            — static reference, no partitioning
#  2. venues           — static reference, no partitioning
#  3. players          — partition: season=
#  4. games            — partition: season=
#  5. player_stats     — partition: season=
#  6. team_stats       — partition: season=
#  7. standings        — partition: season=
#  8. odds             — partition: season=
#  9. player_props     — partition: season=
# 10. plays            — partition: season=
# 11. lineups          — partition: season=
# 12. injuries         — partition: season=
# 13. advanced         — partition: season=
# 14. transactions     — partition: season=
# 15. weather          — partition: season=
# 16. leaders          — partition: season=
# 17. coaches          — partition: season=
#
# MLB uses calendar-year seasons (e.g. 2024).
#
# Merge map (old → new) is available in ``MLB_CONSOLIDATION_MAP``.
#
# Every schema carries a mandatory ``source`` field for vendor provenance.
#
# Providers (21): espn, espnmeta, mlbstats, lahman, statcast,
# retrosheet, odds, oddsapi, openmeteo, weather, actionnetwork,
# rotowire, sleeper, ticketmaster, wikipedia, youtube, googlenews,
# reddit, rssnews, googletrends, thesportsdb.
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

MLB_TEAMS_SCHEMA = pa.schema([
    # Core identity
    _f("id",              pa.int64(),  "Unique team identifier",                      nullable=False),
    _f("name",            pa.string(), "Short team name (e.g. Yankees)",              nullable=False),
    _f("full_name",       pa.string(), "Full team name (e.g. New York Yankees)",      nullable=False),
    _f("abbreviation",    pa.string(), "Team abbreviation (e.g. NYY)",                nullable=False),
    _f("city",            pa.string(), "City where team is located"),
    _f("league",          pa.string(), "League — American or National"),
    _f("division",        pa.string(), "Division name (e.g. AL East, NL West)"),
    _f("color",           pa.string(), "Primary team colour hex code"),
    _f("alternate_color", pa.string(), "Secondary team colour hex code"),
    _f("logo_url",        pa.string(), "URL to team logo image"),
    _f("venue_id",        pa.int64(),  "Home venue identifier"),
    _f("venue_name",      pa.string(), "Home ballpark name"),
    _f("first_year",      pa.int64(),  "First year of franchise"),
    _f("is_active",       pa.bool_(),  "Whether the franchise is currently active"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                     nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

MLB_VENUES_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique venue identifier",                    nullable=False),
    _f("name",            pa.string(), "Ballpark name",                              nullable=False),
    _f("city",            pa.string(), "City"),
    _f("state",           pa.string(), "State or province"),
    _f("country",         pa.string(), "Country"),
    _f("capacity",        pa.int64(),  "Seating capacity"),
    _f("year_opened",     pa.int64(),  "Year ballpark was opened"),
    _f("surface",         pa.string(), "Playing surface type (e.g. grass, turf)"),
    _f("roof_type",       pa.string(), "Roof type (e.g. open, retractable, dome)"),
    _f("left_field",      pa.int64(),  "Left field distance (feet)"),
    _f("center_field",    pa.int64(),  "Center field distance (feet)"),
    _f("right_field",     pa.int64(),  "Right field distance (feet)"),
    _f("latitude",        pa.float64(), "Ballpark latitude"),
    _f("longitude",       pa.float64(), "Ballpark longitude"),
    _f("timezone",        pa.string(), "IANA timezone identifier"),
    _f("elevation",       pa.int64(),  "Elevation in feet above sea level"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. players — merges ESPN athletes + MLB Stats rosters + Lahman
#    Partition: season=
#    Discriminator: status (active/inactive/injured)
# ═══════════════════════════════════════════════════════════════════════

MLB_PLAYERS_SCHEMA = pa.schema([
    # Core identity
    _f("id",              pa.int64(),  "Unique player identifier",            nullable=False),
    _f("first_name",      pa.string(), "Player first name",                   nullable=False),
    _f("last_name",       pa.string(), "Player last name",                    nullable=False),
    _f("full_name",       pa.string(), "Full display name"),
    _f("position",        pa.string(), "Primary position (e.g. SP, C, SS, CF, DH)"),
    _f("height",          pa.string(), "Height (e.g. 6-2)"),
    _f("weight",          pa.string(), "Weight as string (e.g. 220)"),
    _f("jersey_number",   pa.string(), "Jersey number as string"),
    _f("bats",            pa.string(), "Batting hand — L/R/S"),
    _f("throws",          pa.string(), "Throwing hand — L/R/S"),
    _f("college",         pa.string(), "College attended"),
    _f("country",         pa.string(), "Country of origin"),
    _f("birth_date",      pa.string(), "Date of birth (ISO-8601)"),
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
    _f("debut_date",      pa.string(), "MLB debut date (ISO-8601)"),
    _f("years_pro",       pa.int64(),  "Years of professional experience"),
    _f("is_active",       pa.bool_(),  "Whether player is currently active in MLB"),
    # Partition / provenance
    _f("season",          pa.int64(),  "Season year (e.g. 2024)",                    nullable=False),
    _f("source",          pa.string(), "Data vendor provenance",                     nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. games — merges ESPN events + MLB Stats game summaries
#    Partition: season=
#    Discriminator: season_type (regular/playoffs/spring_training/asg)
# ═══════════════════════════════════════════════════════════════════════

MLB_GAMES_SCHEMA = pa.schema([
    # Core identity
    _f("id",                  pa.int64(),  "Unique game identifier",          nullable=False),
    _f("date",                pa.string(), "Game date (ISO-8601)",            nullable=False),
    _f("season",              pa.int64(),  "Season year",                     nullable=False),
    _f("season_type",         pa.string(), "Season type — regular/playoffs/spring_training/asg"),
    _f("status",              pa.string(), "Game status (e.g. final, in_progress, scheduled, postponed)"),
    _f("inning",              pa.int64(),  "Current inning"),
    _f("inning_half",         pa.string(), "Inning half — top/bottom"),
    _f("datetime",            pa.string(), "Full game datetime (ISO-8601 with timezone)"),
    _f("doubleheader",        pa.bool_(),  "Whether this game is part of a doubleheader"),
    _f("game_number",         pa.int64(),  "Game number in doubleheader (1 or 2)"),
    # Teams
    _f("home_team_id",        pa.int64(),  "Home team identifier"),
    _f("home_team_name",      pa.string(), "Home team name"),
    _f("home_team_score",     pa.int64(),  "Home team total runs"),
    _f("away_team_id",        pa.int64(),  "Away team identifier"),
    _f("away_team_name",      pa.string(), "Away team name"),
    _f("away_team_score",     pa.int64(),  "Away team total runs"),
    # Linescore
    _f("home_hits",           pa.int64(),  "Home team total hits"),
    _f("home_errors",         pa.int64(),  "Home team total errors"),
    _f("away_hits",           pa.int64(),  "Away team total hits"),
    _f("away_errors",         pa.int64(),  "Away team total errors"),
    _f("home_linescore",      pa.string(), "Home inning-by-inning runs as JSON array"),
    _f("away_linescore",      pa.string(), "Away inning-by-inning runs as JSON array"),
    # Flags
    _f("postseason",          pa.bool_(),  "Whether this is a postseason game"),
    _f("extra_innings",       pa.bool_(),  "Whether the game went to extra innings"),
    _f("innings_played",      pa.int64(),  "Total innings played"),
    # Pitchers
    _f("winning_pitcher_id",  pa.int64(),  "Winning pitcher identifier"),
    _f("losing_pitcher_id",   pa.int64(),  "Losing pitcher identifier"),
    _f("save_pitcher_id",     pa.int64(),  "Save pitcher identifier"),
    # Venue
    _f("attendance",          pa.int64(),  "Reported attendance"),
    _f("venue_id",            pa.int64(),  "Venue identifier"),
    _f("venue_name",          pa.string(), "Venue / ballpark name"),
    # Broadcast
    _f("tv_network",          pa.string(), "TV broadcast network"),
    _f("duration_minutes",    pa.int64(),  "Game duration in minutes"),
    # Provenance
    _f("source",              pa.string(), "Data vendor provenance",          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. player_stats — merges batter + pitcher game logs + season aggregates
#    Partition: season=
#    Discriminators: scope (game/season), stat_type (batting/pitching/fielding)
# ═══════════════════════════════════════════════════════════════════════

MLB_PLAYER_STATS_SCHEMA = pa.schema([
    # Identity
    _f("id",              pa.int64(),  "Unique record identifier"),
    _f("player_id",       pa.int64(),  "Player identifier",                  nullable=False),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("game_id",         pa.int64(),  "Game identifier (null for season scope)"),
    _f("game_date",       pa.string(), "Game date (ISO-8601)"),
    # Discriminators
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("season_type",     pa.string(), "Season type — regular/playoffs"),
    _f("scope",           pa.string(), "Record scope — game or season",      nullable=False),
    _f("stat_type",       pa.string(), "Stat category — batting/pitching/fielding", nullable=False),
    # Games context
    _f("games_played",    pa.int64(),  "Games played (season scope)"),
    _f("games_started",   pa.int64(),  "Games started (season scope)"),
    # ── Batting stats (stat_type=batting) ──
    _f("ab",              pa.int64(),  "At bats"),
    _f("r",               pa.int64(),  "Runs scored"),
    _f("h",               pa.int64(),  "Hits"),
    _f("doubles",         pa.int64(),  "Doubles (2B)"),
    _f("triples",         pa.int64(),  "Triples (3B)"),
    _f("hr",              pa.int64(),  "Home runs"),
    _f("rbi",             pa.int64(),  "Runs batted in"),
    _f("bb",              pa.int64(),  "Walks"),
    _f("so",              pa.int64(),  "Strikeouts"),
    _f("sb",              pa.int64(),  "Stolen bases"),
    _f("cs",              pa.int64(),  "Caught stealing"),
    _f("hbp",             pa.int64(),  "Hit by pitch"),
    _f("sf",              pa.int64(),  "Sacrifice flies"),
    _f("sh",              pa.int64(),  "Sacrifice bunts"),
    _f("gidp",            pa.int64(),  "Grounded into double plays"),
    _f("pa",              pa.int64(),  "Plate appearances"),
    _f("tb",              pa.int64(),  "Total bases"),
    _f("avg",             pa.float64(), "Batting average"),
    _f("obp",             pa.float64(), "On-base percentage"),
    _f("slg",             pa.float64(), "Slugging percentage"),
    _f("ops",             pa.float64(), "On-base plus slugging"),
    _f("ibb",             pa.int64(),  "Intentional walks"),
    _f("lob",             pa.int64(),  "Left on base"),
    # ── Pitching stats (stat_type=pitching) ──
    _f("w",               pa.int64(),  "Wins"),
    _f("l",               pa.int64(),  "Losses"),
    _f("era",             pa.float64(), "Earned run average"),
    _f("g",               pa.int64(),  "Games (pitching)"),
    _f("gs",              pa.int64(),  "Games started (pitching)"),
    _f("sv",              pa.int64(),  "Saves"),
    _f("svo",             pa.int64(),  "Save opportunities"),
    _f("hld",             pa.int64(),  "Holds"),
    _f("bs",              pa.int64(),  "Blown saves"),
    _f("ip",              pa.float64(), "Innings pitched"),
    _f("p_h",             pa.int64(),  "Hits allowed (pitching)"),
    _f("p_r",             pa.int64(),  "Runs allowed (pitching)"),
    _f("er",              pa.int64(),  "Earned runs"),
    _f("p_hr",            pa.int64(),  "Home runs allowed (pitching)"),
    _f("p_bb",            pa.int64(),  "Walks allowed (pitching)"),
    _f("p_so",            pa.int64(),  "Strikeouts (pitching)"),
    _f("whip",            pa.float64(), "Walks + hits per innings pitched"),
    _f("k9",              pa.float64(), "Strikeouts per 9 innings"),
    _f("bb9",             pa.float64(), "Walks per 9 innings"),
    _f("h9",              pa.float64(), "Hits per 9 innings"),
    _f("hr9",             pa.float64(), "Home runs per 9 innings"),
    _f("k_bb",            pa.float64(), "Strikeout to walk ratio"),
    _f("cg",              pa.int64(),  "Complete games"),
    _f("sho",             pa.int64(),  "Shutouts"),
    _f("bf",              pa.int64(),  "Batters faced"),
    _f("pitches_thrown",  pa.int64(),  "Total pitches thrown"),
    _f("strikes",         pa.int64(),  "Total strikes"),
    _f("wp",              pa.int64(),  "Wild pitches"),
    _f("bk",              pa.int64(),  "Balks"),
    _f("p_hbp",           pa.int64(),  "Hit batters (pitching)"),
    # ── Fielding stats (stat_type=fielding) ──
    _f("po",              pa.int64(),  "Putouts"),
    _f("a",               pa.int64(),  "Assists (fielding)"),
    _f("e",               pa.int64(),  "Errors"),
    _f("dp",              pa.int64(),  "Double plays turned"),
    _f("fp",              pa.float64(), "Fielding percentage"),
    _f("tc",              pa.int64(),  "Total chances"),
    _f("pb",              pa.int64(),  "Passed balls (catchers)"),
    _f("sb_against",      pa.int64(),  "Stolen bases against (catchers)"),
    _f("cs_by",           pa.int64(),  "Caught stealing by (catchers)"),
    _f("fielding_pos",    pa.string(), "Fielding position for this stat line"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. team_stats — merges team game-level + season aggregate stats
#    Partition: season=
#    Discriminators: scope (game/season), stat_type (batting/pitching/fielding)
# ═══════════════════════════════════════════════════════════════════════

MLB_TEAM_STATS_SCHEMA = pa.schema([
    # Identity
    _f("team_id",         pa.int64(),  "Team identifier",                    nullable=False),
    _f("team_name",       pa.string(), "Team name"),
    _f("game_id",         pa.int64(),  "Game identifier (null for season scope)"),
    _f("game_date",       pa.string(), "Game date (ISO-8601)"),
    # Discriminators
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("season_type",     pa.string(), "Season type — regular/playoffs"),
    _f("scope",           pa.string(), "Record scope — game or season",      nullable=False),
    _f("stat_type",       pa.string(), "Stat category — batting/pitching/fielding", nullable=False),
    # Win/loss context
    _f("games_played",    pa.int64(),  "Games played (season scope)"),
    _f("wins",            pa.int64(),  "Wins"),
    _f("losses",          pa.int64(),  "Losses"),
    _f("win_pct",         pa.float64(), "Win percentage"),
    # ── Team batting ──
    _f("ab",              pa.int64(),  "At bats"),
    _f("r",               pa.int64(),  "Runs scored"),
    _f("h",               pa.int64(),  "Hits"),
    _f("doubles",         pa.int64(),  "Doubles"),
    _f("triples",         pa.int64(),  "Triples"),
    _f("hr",              pa.int64(),  "Home runs"),
    _f("rbi",             pa.int64(),  "Runs batted in"),
    _f("bb",              pa.int64(),  "Walks"),
    _f("so",              pa.int64(),  "Strikeouts"),
    _f("sb",              pa.int64(),  "Stolen bases"),
    _f("cs",              pa.int64(),  "Caught stealing"),
    _f("avg",             pa.float64(), "Batting average"),
    _f("obp",             pa.float64(), "On-base percentage"),
    _f("slg",             pa.float64(), "Slugging percentage"),
    _f("ops",             pa.float64(), "On-base plus slugging"),
    _f("lob",             pa.int64(),  "Left on base"),
    # ── Team pitching ──
    _f("era",             pa.float64(), "Team earned run average"),
    _f("ip",              pa.float64(), "Innings pitched"),
    _f("p_h",             pa.int64(),  "Hits allowed (pitching)"),
    _f("er",              pa.int64(),  "Earned runs"),
    _f("p_bb",            pa.int64(),  "Walks allowed (pitching)"),
    _f("p_so",            pa.int64(),  "Strikeouts (pitching)"),
    _f("whip",            pa.float64(), "WHIP"),
    _f("sv",              pa.int64(),  "Saves"),
    _f("cg",              pa.int64(),  "Complete games"),
    _f("sho",             pa.int64(),  "Shutouts"),
    _f("quality_starts",  pa.int64(),  "Quality starts"),
    # ── Team fielding ──
    _f("po",              pa.int64(),  "Putouts"),
    _f("a",               pa.int64(),  "Assists"),
    _f("e",               pa.int64(),  "Errors"),
    _f("dp",              pa.int64(),  "Double plays turned"),
    _f("fp",              pa.float64(), "Fielding percentage"),
    # Run context
    _f("runs_scored",     pa.int64(),  "Total runs scored"),
    _f("runs_allowed",    pa.int64(),  "Total runs allowed"),
    _f("run_differential", pa.int64(), "Run differential"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. standings — division / league / wild card standings
#    Partition: season=
# ═══════════════════════════════════════════════════════════════════════

MLB_STANDINGS_SCHEMA = pa.schema([
    _f("team_id",            pa.int64(),   "Team identifier"),
    _f("team_name",          pa.string(),  "Team name"),
    _f("league",             pa.string(),  "League — American or National"),
    _f("division",           pa.string(),  "Division name (e.g. AL East)"),
    _f("season",             pa.int64(),   "Season year",                      nullable=False),
    # Win/loss
    _f("wins",               pa.int64(),   "Total wins"),
    _f("losses",             pa.int64(),   "Total losses"),
    _f("win_pct",            pa.float64(), "Win percentage"),
    _f("division_rank",      pa.int64(),   "Division ranking"),
    _f("league_rank",        pa.int64(),   "League ranking"),
    _f("wildcard_rank",      pa.int64(),   "Wild card ranking"),
    _f("playoff_seed",       pa.int64(),   "Playoff seed (null if not in playoffs)"),
    # Situational records
    _f("division_record",    pa.string(),  "Division record (e.g. 40-36)"),
    _f("interleague_record", pa.string(),  "Interleague record"),
    _f("home_record",        pa.string(),  "Home record"),
    _f("road_record",        pa.string(),  "Road record"),
    _f("last_10",            pa.string(),  "Last 10 games record (e.g. 7-3)"),
    _f("streak",             pa.string(),  "Current streak (e.g. W5, L2)"),
    _f("games_behind",       pa.float64(), "Games behind division leader"),
    _f("wildcard_gb",        pa.float64(), "Games behind wild card leader"),
    _f("extra_innings_record", pa.string(), "Extra innings record"),
    _f("one_run_record",     pa.string(),  "One-run game record"),
    _f("vs_over_500",        pa.string(),  "Record vs teams above .500"),
    # Clinch info
    _f("clinch_indicator",   pa.string(),  "Clinch status (e.g. x, y, z, e, p)"),
    _f("runs_scored",        pa.int64(),   "Total runs scored"),
    _f("runs_allowed",       pa.int64(),   "Total runs allowed"),
    _f("run_differential",   pa.int64(),   "Run differential"),
    # Pythagorean
    _f("expected_win_pct",   pa.float64(), "Pythagorean expected win percentage"),
    # Provenance
    _f("source",             pa.string(),  "Data vendor provenance",           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. odds — merges Odds baseline + OddsAPI events
#    Partition: season=
#    Discriminator: line_type (spread/moneyline/total/prop)
# ═══════════════════════════════════════════════════════════════════════

MLB_ODDS_SCHEMA = pa.schema([
    _f("id",                  pa.int64(),  "Odds line identifier"),
    _f("game_id",             pa.int64(),  "Parent game identifier",          nullable=False),
    _f("game_date",           pa.string(), "Game date (ISO-8601)"),
    _f("season",              pa.int64(),  "Season year",                     nullable=False),
    _f("vendor",              pa.string(), "Sportsbook / odds vendor"),
    # Discriminator
    _f("line_type",           pa.string(), "Line type — spread/moneyline/total/prop", nullable=False),
    # Teams
    _f("home_team",           pa.string(), "Home team name"),
    _f("away_team",           pa.string(), "Away team name"),
    # Spread (run line)
    _f("spread_home_value",   pa.float64(), "Home spread value (e.g. -1.5)"),
    _f("spread_home_odds",    pa.int64(),   "Home spread odds (American)"),
    _f("spread_away_value",   pa.float64(), "Away spread value"),
    _f("spread_away_odds",    pa.int64(),   "Away spread odds (American)"),
    # Moneyline
    _f("moneyline_home_odds", pa.int64(),   "Home moneyline (American)"),
    _f("moneyline_away_odds", pa.int64(),   "Away moneyline (American)"),
    # Totals (over/under)
    _f("total_value",         pa.float64(), "Over/under total value"),
    _f("total_over_odds",     pa.int64(),   "Over odds (American)"),
    _f("total_under_odds",    pa.int64(),   "Under odds (American)"),
    # Metadata
    _f("updated_at",          pa.string(),  "Timestamp of last odds update"),
    # Provenance
    _f("source",              pa.string(),  "Data vendor provenance",         nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. player_props — prop bets (strikeouts, hits, HRs, etc.)
#    Partition: season=
#    Discriminator: prop_type
# ═══════════════════════════════════════════════════════════════════════

MLB_PLAYER_PROPS_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique prop record identifier"),
    _f("game_id",         pa.int64(),  "Parent game identifier",             nullable=False),
    _f("player_id",       pa.int64(),  "Player identifier"),
    _f("player_name",     pa.string(), "Player display name"),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("vendor",          pa.string(), "Sportsbook / odds vendor"),
    # Discriminator
    _f("prop_type",       pa.string(), "Prop type — strikeouts/hits/home_runs/total_bases/rbis/runs/walks/outs/earned_runs/etc.", nullable=False),
    # Line
    _f("line_value",      pa.float64(), "Prop line value"),
    _f("market_type",     pa.string(),  "Market type (e.g. over_under, milestone, alternate)"),
    _f("over_odds",       pa.int64(),   "Over odds (American)"),
    _f("under_odds",      pa.int64(),   "Under odds (American)"),
    _f("milestone_odds",  pa.int64(),   "Milestone odds (American, e.g. 2+ HR)"),
    # Metadata
    _f("updated_at",      pa.string(),  "Timestamp of last update"),
    # Provenance
    _f("source",          pa.string(),  "Data vendor provenance",            nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. plays — play-by-play + at-bat level data
#     Partition: season=
#     Discriminator: play_type (at_bat/stolen_base/pickoff/wild_pitch/
#                               balk/error/substitution/other)
# ═══════════════════════════════════════════════════════════════════════

MLB_PLAYS_SCHEMA = pa.schema([
    _f("game_id",         pa.int64(),  "Parent game identifier",             nullable=False),
    _f("order",           pa.int64(),  "Sequence order within the game",     nullable=False),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    # Game context
    _f("inning",          pa.int64(),  "Inning number"),
    _f("inning_half",     pa.string(), "Inning half — top/bottom"),
    _f("at_bat_number",   pa.int64(),  "At-bat sequence number within the game"),
    # Discriminator
    _f("play_type",       pa.string(), "Play type — at_bat/stolen_base/pickoff/wild_pitch/balk/error/substitution/other", nullable=False),
    _f("text",            pa.string(), "Play-by-play description text"),
    _f("event",           pa.string(), "Event result (e.g. Single, Strikeout, Home Run, Walk)"),
    # Scoring context
    _f("home_score",      pa.int64(),  "Home score at time of play"),
    _f("away_score",      pa.int64(),  "Away score at time of play"),
    _f("scoring_play",    pa.bool_(),  "Whether this play resulted in a run"),
    _f("runs_scored",     pa.int64(),  "Runs scored on this play"),
    _f("outs",            pa.int64(),  "Outs in the inning after this play"),
    # Participants
    _f("batter_id",       pa.int64(),  "Batter identifier"),
    _f("batter_name",     pa.string(), "Batter display name"),
    _f("pitcher_id",      pa.int64(),  "Pitcher identifier"),
    _f("pitcher_name",    pa.string(), "Pitcher display name"),
    _f("team_id",         pa.int64(),  "Batting team identifier"),
    _f("team_name",       pa.string(), "Batting team name"),
    # At-bat result detail
    _f("hit_type",        pa.string(), "Type of batted ball (e.g. line_drive, fly_ball, ground_ball, popup)"),
    _f("hit_location",    pa.string(), "Hit location description"),
    _f("total_pitches",   pa.int64(),  "Total pitches in this at-bat"),
    _f("runners_on",      pa.string(), "Base runners as JSON (e.g. [1, 3] for runners on 1st and 3rd)"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. lineups — starting lineups and batting order
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

MLB_LINEUPS_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique lineup record identifier"),
    _f("game_id",         pa.int64(),  "Game identifier"),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("player_id",       pa.int64(),  "Player identifier"),
    _f("player_name",     pa.string(), "Player display name"),
    _f("position",        pa.string(), "Fielding position (e.g. C, 1B, SS, CF, DH)"),
    _f("batting_order",   pa.int64(),  "Batting order position (1-9)"),
    _f("starter",         pa.bool_(),  "Whether the player is in the starting lineup"),
    _f("is_pitcher",      pa.bool_(),  "Whether this player is the starting pitcher"),
    _f("home_away",       pa.string(), "Whether team is home or away"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. injuries — IL stints and injury reports
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

MLB_INJURIES_SCHEMA = pa.schema([
    _f("player_id",       pa.int64(),  "Player identifier",                  nullable=False),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("status",          pa.string(), "Injury status (e.g. 10-day IL, 15-day IL, 60-day IL, day-to-day)"),
    _f("description",     pa.string(), "Injury description (e.g. Right elbow UCL tear)"),
    _f("body_part",       pa.string(), "Body part affected"),
    _f("injury_type",     pa.string(), "Type of injury (e.g. strain, tear, fracture, inflammation)"),
    _f("il_type",         pa.string(), "Injured list type (e.g. 10-day, 15-day, 60-day)"),
    _f("start_date",      pa.string(), "IL placement date (ISO-8601)"),
    _f("return_date",     pa.string(), "Expected or actual return date (ISO-8601)"),
    _f("reported_date",   pa.string(), "Date injury was reported (ISO-8601)"),
    _f("games_missed",    pa.int64(),  "Number of games missed"),
    _f("retroactive",     pa.bool_(),  "Whether IL placement is retroactive"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. advanced — Statcast batted-ball, xStats, zone data
#     Partition: season=
#     Discriminator: stat_type (batting/pitching/fielding/statcast)
# ═══════════════════════════════════════════════════════════════════════

MLB_ADVANCED_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique record identifier"),
    _f("player_id",       pa.int64(),  "Player identifier"),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("game_id",         pa.int64(),  "Game identifier (null for season aggregate)"),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("season_type",     pa.string(), "Season type — regular/playoffs"),
    # Discriminator
    _f("stat_type",       pa.string(), "Stat type — batting/pitching/fielding/statcast", nullable=False),
    # ── Statcast batting (stat_type=statcast or batting) ──
    _f("xba",             pa.float64(), "Expected batting average"),
    _f("xslg",            pa.float64(), "Expected slugging percentage"),
    _f("xwoba",           pa.float64(), "Expected weighted on-base average"),
    _f("xobp",            pa.float64(), "Expected on-base percentage"),
    _f("xiso",            pa.float64(), "Expected isolated power"),
    _f("woba",            pa.float64(), "Weighted on-base average"),
    _f("wobacon",         pa.float64(), "wOBA on contact"),
    _f("babip",           pa.float64(), "Batting average on balls in play"),
    _f("iso",             pa.float64(), "Isolated power (SLG minus AVG)"),
    _f("avg_exit_velocity", pa.float64(), "Average exit velocity (mph)"),
    _f("max_exit_velocity", pa.float64(), "Maximum exit velocity (mph)"),
    _f("avg_launch_angle",  pa.float64(), "Average launch angle (degrees)"),
    _f("barrel_pct",      pa.float64(), "Barrel percentage"),
    _f("hard_hit_pct",    pa.float64(), "Hard hit percentage (95+ mph)"),
    _f("sweet_spot_pct",  pa.float64(), "Sweet spot percentage (8-32 deg launch angle)"),
    _f("batted_balls",    pa.int64(),   "Total batted ball events"),
    _f("barrels",         pa.int64(),   "Total barrels"),
    _f("line_drive_pct",  pa.float64(), "Line drive percentage"),
    _f("ground_ball_pct", pa.float64(), "Ground ball percentage"),
    _f("fly_ball_pct",    pa.float64(), "Fly ball percentage"),
    _f("popup_pct",       pa.float64(), "Popup percentage"),
    _f("pull_pct",        pa.float64(), "Pull percentage"),
    _f("center_pct",      pa.float64(), "Center percentage"),
    _f("oppo_pct",        pa.float64(), "Opposite field percentage"),
    # ── Statcast pitching (stat_type=pitching) ──
    _f("avg_velocity",    pa.float64(), "Average fastball velocity (mph)"),
    _f("max_velocity",    pa.float64(), "Maximum pitch velocity (mph)"),
    _f("avg_spin_rate",   pa.float64(), "Average spin rate (rpm)"),
    _f("whiff_pct",       pa.float64(), "Whiff percentage (swings and misses / swings)"),
    _f("chase_pct",       pa.float64(), "Chase percentage (swings at pitches outside zone)"),
    _f("zone_pct",        pa.float64(), "Zone percentage (pitches in strike zone)"),
    _f("first_strike_pct", pa.float64(), "First pitch strike percentage"),
    _f("csw_pct",         pa.float64(), "Called strikes + whiffs percentage"),
    _f("k_pct",           pa.float64(), "Strikeout percentage"),
    _f("bb_pct",          pa.float64(), "Walk percentage"),
    _f("xera",            pa.float64(), "Expected earned run average"),
    _f("xfip",            pa.float64(), "Expected fielding-independent pitching"),
    _f("fip",             pa.float64(), "Fielding-independent pitching"),
    _f("siera",           pa.float64(), "Skill-interactive ERA"),
    _f("pitch_mix",       pa.string(),  "Pitch mix as JSON (e.g. {\"FF\": 0.55, \"SL\": 0.25})"),
    # ── Fielding advanced (stat_type=fielding) ──
    _f("outs_above_avg",  pa.float64(), "Outs above average (OAA)"),
    _f("drs",             pa.int64(),   "Defensive runs saved"),
    _f("uzr",             pa.float64(), "Ultimate zone rating"),
    _f("uzr_per_150",     pa.float64(), "UZR per 150 games"),
    _f("arm_strength",    pa.float64(), "Average arm strength (mph)"),
    _f("sprint_speed",    pa.float64(), "Sprint speed (ft/sec)"),
    _f("range_runs",      pa.float64(), "Range runs"),
    # ── Value metrics ──
    _f("war",             pa.float64(), "Wins above replacement"),
    _f("wrc_plus",        pa.float64(), "Weighted runs created plus (100 = league average)"),
    _f("ops_plus",        pa.float64(), "OPS+ (adjusted, 100 = league average)"),
    _f("era_plus",        pa.float64(), "ERA+ (adjusted, 100 = league average)"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. transactions — trades, DFA, signings, waivers
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

MLB_TRANSACTIONS_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique transaction record identifier"),
    _f("player_id",       pa.int64(),  "Player identifier"),
    _f("player_name",     pa.string(), "Player display name"),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("date",            pa.string(), "Transaction date (ISO-8601)",        nullable=False),
    _f("type",            pa.string(), "Transaction type (e.g. trade, signing, dfa, release, waiver, option, recall)"),
    _f("description",     pa.string(), "Full transaction description text"),
    _f("from_team_id",    pa.int64(),  "Originating team identifier"),
    _f("from_team_name",  pa.string(), "Originating team name"),
    _f("to_team_id",      pa.int64(),  "Destination team identifier"),
    _f("to_team_name",    pa.string(), "Destination team name"),
    _f("effective_date",  pa.string(), "Effective date of transaction (ISO-8601)"),
    _f("amount",          pa.float64(), "Financial amount (if applicable)"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 15. weather — game-day weather conditions
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

MLB_WEATHER_SCHEMA = pa.schema([
    _f("game_id",         pa.int64(),  "Parent game identifier",             nullable=False),
    _f("game_date",       pa.string(), "Game date (ISO-8601)"),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("venue_id",        pa.int64(),  "Venue identifier"),
    _f("venue_name",      pa.string(), "Venue / ballpark name"),
    # Conditions
    _f("temperature",     pa.float64(), "Temperature (°F)"),
    _f("feels_like",      pa.float64(), "Feels-like temperature (°F)"),
    _f("humidity",        pa.float64(), "Relative humidity (%)"),
    _f("dew_point",       pa.float64(), "Dew point (°F)"),
    _f("pressure",        pa.float64(), "Barometric pressure (inHg)"),
    _f("wind_speed",      pa.float64(), "Wind speed (mph)"),
    _f("wind_direction",  pa.string(),  "Wind direction (e.g. Out to CF, In from LF)"),
    _f("wind_gust",       pa.float64(), "Wind gust speed (mph)"),
    _f("precipitation",   pa.float64(), "Precipitation (inches)"),
    _f("cloud_cover",     pa.float64(), "Cloud cover (%)"),
    _f("visibility",      pa.float64(), "Visibility (miles)"),
    _f("condition",       pa.string(),  "Weather condition description (e.g. Clear, Cloudy, Rain)"),
    _f("roof_status",     pa.string(),  "Roof status — open/closed/n_a"),
    _f("first_pitch_time", pa.string(), "First pitch time (ISO-8601)"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 16. leaders — league leaders by statistical category
#     Partition: season=
#     Discriminator: stat_type
# ═══════════════════════════════════════════════════════════════════════

MLB_LEADERS_SCHEMA = pa.schema([
    _f("player_id",       pa.int64(),  "Player identifier",                  nullable=False),
    _f("player_name",     pa.string(), "Player display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("season_type",     pa.string(), "Season type — regular/playoffs"),
    # Discriminator
    _f("stat_type",       pa.string(), "Stat category — avg/hr/rbi/sb/era/so/w/sv/whip/ops/war/etc.", nullable=False),
    _f("league",          pa.string(), "League — American/National/MLB"),
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
# 17. coaches — managers and coaching staffs
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

MLB_COACHES_SCHEMA = pa.schema([
    _f("id",              pa.int64(),  "Unique coach identifier"),
    _f("first_name",      pa.string(), "Coach first name"),
    _f("last_name",       pa.string(), "Coach last name"),
    _f("full_name",       pa.string(), "Coach full display name"),
    _f("team_id",         pa.int64(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("season",          pa.int64(),  "Season year",                        nullable=False),
    _f("role",            pa.string(), "Coaching role (e.g. manager, bench, pitching, hitting, first_base, third_base, bullpen)"),
    _f("is_manager",      pa.bool_(),  "Whether this coach is the team manager"),
    _f("career_wins",     pa.int64(),  "Career managerial wins"),
    _f("career_losses",   pa.int64(),  "Career managerial losses"),
    _f("season_wins",     pa.int64(),  "Season managerial wins"),
    _f("season_losses",   pa.int64(),  "Season managerial losses"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Registry — schema, partition key, and path look-ups
# ═══════════════════════════════════════════════════════════════════════

MLB_SCHEMAS: dict[str, pa.Schema] = {
    "teams":         MLB_TEAMS_SCHEMA,
    "venues":        MLB_VENUES_SCHEMA,
    "players":       MLB_PLAYERS_SCHEMA,
    "games":         MLB_GAMES_SCHEMA,
    "player_stats":  MLB_PLAYER_STATS_SCHEMA,
    "team_stats":    MLB_TEAM_STATS_SCHEMA,
    "standings":     MLB_STANDINGS_SCHEMA,
    "odds":          MLB_ODDS_SCHEMA,
    "player_props":  MLB_PLAYER_PROPS_SCHEMA,
    "plays":         MLB_PLAYS_SCHEMA,
    "lineups":       MLB_LINEUPS_SCHEMA,
    "injuries":      MLB_INJURIES_SCHEMA,
    "advanced":      MLB_ADVANCED_SCHEMA,
    "transactions":  MLB_TRANSACTIONS_SCHEMA,
    "weather":       MLB_WEATHER_SCHEMA,
    "leaders":       MLB_LEADERS_SCHEMA,
    "coaches":       MLB_COACHES_SCHEMA,
}

MLB_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning (single flat parquets)
    "teams":         [],
    "venues":        [],
    # Season only — MLB uses calendar-year seasons
    "players":       ["season"],
    "games":         ["season"],
    "player_stats":  ["season"],
    "team_stats":    ["season"],
    "standings":     ["season"],
    "odds":          ["season"],
    "player_props":  ["season"],
    "plays":         ["season"],
    "lineups":       ["season"],
    "injuries":      ["season"],
    "advanced":      ["season"],
    "transactions":  ["season"],
    "weather":       ["season"],
    "leaders":       ["season"],
    "coaches":       ["season"],
}

MLB_ENTITY_PATHS: dict[str, str] = {
    "teams":         "teams",
    "venues":        "venues",
    "players":       "players",
    "games":         "games",
    "player_stats":  "player_stats",
    "team_stats":    "team_stats",
    "standings":     "standings",
    "odds":          "odds",
    "player_props":  "player_props",
    "plays":         "plays",
    "lineups":       "lineups",
    "injuries":      "injuries",
    "advanced":      "advanced",
    "transactions":  "transactions",
    "weather":       "weather",
    "leaders":       "leaders",
    "coaches":       "coaches",
}


# ═══════════════════════════════════════════════════════════════════════
# Migration reference — maps old raw data sources → new 18 entity names
# ═══════════════════════════════════════════════════════════════════════

MLB_CONSOLIDATION_MAP: dict[str, str] = {
    # ── ESPN raw data ─────────────────────────────────────────────────
    "espn/teams":                     "espn/teams → teams",
    "espn/athletes":                  "espn/athletes → players",
    "espn/events":                    "espn/events → games",
    "espn/rosters":                   "espn/rosters → players",
    "espn/standings":                 "espn/standings → standings",
    "espn/injuries":                  "espn/injuries → injuries",
    "espn/snapshots/injuries":        "espn/snapshots/injuries → injuries",
    "espn/snapshots/news":            "espn/snapshots/news → players",
    "espn/snapshots/transactions":    "espn/snapshots/transactions → transactions",
    "espn/snapshots/team_stats":      "espn/snapshots/team_stats → team_stats",
    "espn/team_schedule":             "espn/team_schedule → games",
    "espn/reference/teams":           "espn/reference/teams → teams",
    "espn/reference/venues":          "espn/reference/venues → venues",
    "espn/reference/games":           "espn/reference/games → games",

    # ── ESPN Meta raw data ────────────────────────────────────────────
    "espnmeta/teams":                 "espnmeta/teams → teams",
    "espnmeta/athletes":              "espnmeta/athletes → players",

    # ── MLB Stats raw data ────────────────────────────────────────────
    "mlbstats/teams":                 "mlbstats/teams → teams",
    "mlbstats/players":               "mlbstats/players → players",
    "mlbstats/rosters":               "mlbstats/rosters → players",
    "mlbstats/games":                 "mlbstats/games → games",
    "mlbstats/game_boxscore":         "mlbstats/game_boxscore → player_stats",
    "mlbstats/game_playbyplay":       "mlbstats/game_playbyplay → plays",
    "mlbstats/standings":             "mlbstats/standings → standings",
    "mlbstats/player_stats":          "mlbstats/player_stats → player_stats",
    "mlbstats/team_stats":            "mlbstats/team_stats → team_stats",
    "mlbstats/league_leaders":        "mlbstats/league_leaders → leaders",
    "mlbstats/transactions":          "mlbstats/transactions → transactions",
    "mlbstats/injuries":              "mlbstats/injuries → injuries",
    "mlbstats/venues":                "mlbstats/venues → venues",
    "mlbstats/coaches":               "mlbstats/coaches → coaches",

    # ── Lahman raw data ───────────────────────────────────────────────
    "lahman/batting":                 "lahman/batting → player_stats",
    "lahman/pitching":                "lahman/pitching → player_stats",
    "lahman/fielding":                "lahman/fielding → player_stats",
    "lahman/teams":                   "lahman/teams → teams",
    "lahman/people":                  "lahman/people → players",

    # ── Statcast raw data ─────────────────────────────────────────────
    "statcast/pitches":               "statcast/pitches → pitches",
    "statcast/batted_balls":          "statcast/batted_balls → advanced",
    "statcast/sprint_speed":          "statcast/sprint_speed → advanced",
    "statcast/player_stats":          "statcast/player_stats → advanced",

    # ── Retrosheet raw data ───────────────────────────────────────────
    "retrosheet/games":               "retrosheet/games → games",
    "retrosheet/events":              "retrosheet/events → plays",

    # ── Odds raw data ─────────────────────────────────────────────────
    "odds/espn_baseline":             "odds/espn_baseline → odds",
    "odds/player_props":              "odds/player_props → player_props",

    # ── OddsAPI raw data ──────────────────────────────────────────────
    "oddsapi/events":                 "oddsapi/events → odds",
    "oddsapi/props":                  "oddsapi/props → player_props",
    "oddsapi/scores":                 "oddsapi/scores → games",

    # ── Weather raw data ──────────────────────────────────────────────
    "openmeteo/weather":              "openmeteo/weather → weather",
    "weather/game_conditions":        "weather/game_conditions → weather",

    # ── Supplementary providers ───────────────────────────────────────
    "actionnetwork/odds":             "actionnetwork/odds → odds",
    "rotowire/lineups":               "rotowire/lineups → lineups",
    "rotowire/injuries":              "rotowire/injuries → injuries",
    "sleeper/players":                "sleeper/players → players",
    "thesportsdb/teams":              "thesportsdb/teams → teams",
    "thesportsdb/players":            "thesportsdb/players → players",
    "thesportsdb/venues":             "thesportsdb/venues → venues",
}

# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names (the prefix of {type}_{season}.parquet)
# to the flat entity directory name under normalized_curated/mlb/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
MLB_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Core entities
    "teams":              "teams",
    "players":            "players",
    "games":              "games",
    "standings":          "standings",
    "venues":             "venues",

    # Stats
    "player_stats":       "player_stats",
    "batter_game_stats":  "player_stats",
    "pitcher_game_stats": "player_stats",
    "team_stats":         "team_stats",
    "team_game_stats":    "team_stats",

    # Betting
    "odds":               "odds",
    "odds_history":       "odds",
    "player_props":       "player_props",

    # Event data
    "play_by_play":       "plays",
    "drives":             "plays",

    # Supplementary
    "injuries":           "injuries",
    "transactions":       "transactions",
    "weather":            "weather",
    "coaches":            "coaches",

    # Skip types (non-entity artefacts)
    "news":               None,
    "scoreboard":         None,
    "info":               None,
    "calendar":           None,
}
