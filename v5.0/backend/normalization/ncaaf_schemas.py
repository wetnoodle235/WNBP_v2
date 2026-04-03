# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — NCAAF Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 14-entity consolidated design.  The original 34 fine-grained entities
# have been merged into 14 wide schemas that use discriminator columns
# (``scope``, ``metric_type``, ``rating_type``, ``line_type``,
# ``play_type``) to distinguish record subtypes within a single table.
#
# Entity overview
# ───────────────
#  1. conferences      — static reference, no partitioning
#  2. teams            — partition: season=
#  3. players          — partition: season=
#  4. games            — partition: season=, week=
#  5. plays            — partition: season=, week=   (plays + drives)
#  6. player_stats     — partition: season=, week=   (game + season scope)
#  7. team_stats       — partition: season=, week=   (game + season scope)
#  8. standings        — partition: season=           (records + ATS)
#  9. rankings         — partition: season=, week=
# 10. odds             — partition: season=, week=   (pregame/live/history/props)
# 11. ratings          — partition: season=           (elo/sp/fpi/srs/talent/sp_conf)
# 12. advanced         — partition: season=, week=   (epa/ppa/havoc/win_prob)
# 13. recruiting       — partition: season=           (class/player/group)
# 14. venues           — static reference, no partitioning
#
# Merge map (old → new) is available in ``CONSOLIDATION_MAP``.
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
    _f("id",           pa.int32(),  "Unique conference identifier",       nullable=False),
    _f("name",         pa.string(), "Full conference name",               nullable=False),
    _f("abbreviation", pa.string(), "Short abbreviation (e.g. SEC, B1G)", nullable=False),
    _f("source",       pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. teams — merges old teams + coaches + roster info
#    Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_teams = pa.schema([
    # Core identity
    _f("id",              pa.int32(),  "Unique team identifier",                   nullable=False),
    _f("name",            pa.string(), "Short team name (e.g. Crimson Tide)",      nullable=False),
    _f("full_name",       pa.string(), "Full team name (e.g. Alabama Crimson Tide)", nullable=False),
    _f("abbreviation",    pa.string(), "Team abbreviation (e.g. ALA)",             nullable=False),
    _f("city",            pa.string(), "City where team is located",               nullable=False),
    _f("conference",      pa.string(), "Conference name or abbreviation",          nullable=False),
    _f("division",        pa.string(), "Division within conference"),
    _f("logo_url",        pa.string(), "URL to team logo image"),
    _f("color_primary",   pa.string(), "Primary team colour hex code"),
    _f("color_secondary", pa.string(), "Secondary team colour hex code"),
    # Coach (merged from old coaches entity)
    _f("coach_first_name", pa.string(), "Head/position coach first name"),
    _f("coach_last_name",  pa.string(), "Head/position coach last name"),
    _f("coach_position",   pa.string(), "Coaching role — HC, OC, or DC"),
    _f("coach_years",      pa.int32(),  "Seasons as coach at this team"),
    _f("coach_wins",       pa.int32(),  "Career wins at this team"),
    _f("coach_losses",     pa.int32(),  "Career losses at this team"),
    # Partition / provenance
    _f("season",           pa.int32(),  "Season year",          nullable=False),
    _f("source",           pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. players — merges old players + portal + returning_production + draft
#    Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_players = pa.schema([
    # Core identity
    _f("id",                    pa.int32(),  "Unique player identifier",        nullable=False),
    _f("first_name",            pa.string(), "Player first name",               nullable=False),
    _f("last_name",             pa.string(), "Player last name",                nullable=False),
    _f("position",              pa.string(), "Full position name"),
    _f("position_abbreviation", pa.string(), "Position abbreviation (e.g. QB, WR)"),
    _f("height",                pa.string(), "Height (e.g. 6-2)"),
    _f("weight",                pa.string(), "Weight as string"),
    _f("jersey_number",         pa.string(), "Jersey number as string"),
    _f("team_id",               pa.int32(),  "Current team identifier"),
    _f("team_name",             pa.string(), "Current team name"),
    _f("status",                pa.string(), "Player status — active/inactive/transferred/drafted"),
    # Portal / transfer fields (populated only for transfer records)
    _f("origin_team",      pa.string(),  "Transfer portal: origin team name"),
    _f("destination_team", pa.string(),  "Transfer portal: destination team name"),
    _f("transfer_date",    pa.string(),  "Transfer portal: date of transfer"),
    _f("stars",            pa.int32(),   "Transfer portal: star rating"),
    _f("rating",           pa.float64(), "Transfer portal: composite rating"),
    # Returning production
    _f("ppa_usage",        pa.float64(), "Returning production: PPA usage rate"),
    _f("total_ppa",        pa.float64(), "Returning production: total PPA"),
    _f("percent_ppa",      pa.float64(), "Returning production: percent of team PPA returning"),
    _f("percent_passing",  pa.float64(), "Returning production: percent of team passing returning"),
    _f("percent_receiving", pa.float64(), "Returning production: percent of team receiving returning"),
    _f("percent_rushing",  pa.float64(), "Returning production: percent of team rushing returning"),
    # Draft
    _f("draft_round", pa.int32(),  "NFL draft round"),
    _f("draft_pick",  pa.int32(),  "NFL draft overall pick number"),
    _f("draft_team",  pa.string(), "NFL team that drafted the player"),
    _f("draft_year",  pa.int32(),  "Year the player was drafted"),
    # Recruiting
    _f("recruit_stars",  pa.int32(),   "Recruiting star rating"),
    _f("recruit_rating", pa.float64(), "Recruiting composite rating"),
    _f("recruit_city",   pa.string(),  "Recruiting hometown city"),
    _f("recruit_state",  pa.string(),  "Recruiting hometown state"),
    # Partition / provenance
    _f("season", pa.int32(),  "Season year",            nullable=False),
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. games — merges old games + weather + media
#    Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_games = pa.schema([
    # Core identity
    _f("id",     pa.int32(),  "Unique game identifier", nullable=False),
    _f("date",   pa.string(), "Game date (ISO-8601)",   nullable=False),
    _f("season", pa.int32(),  "Season year",            nullable=False),
    _f("week",   pa.int32(),  "Season week number",     nullable=False),
    _f("status", pa.string(), "Game status (e.g. final, in_progress, scheduled)"),
    _f("period", pa.int32(),  "Current period / quarter"),
    _f("time",   pa.string(), "Time remaining in current period"),
    # Teams
    _f("home_team_id",   pa.int32(),  "Home team identifier"),
    _f("home_team_name", pa.string(), "Home team name"),
    _f("away_team_id",   pa.int32(),  "Away team identifier"),
    _f("away_team_name", pa.string(), "Away team name"),
    # Scoring
    _f("home_score",    pa.int32(), "Home total score"),
    _f("away_score",    pa.int32(), "Away total score"),
    _f("home_score_q1", pa.int32(), "Home 1st quarter score"),
    _f("home_score_q2", pa.int32(), "Home 2nd quarter score"),
    _f("home_score_q3", pa.int32(), "Home 3rd quarter score"),
    _f("home_score_q4", pa.int32(), "Home 4th quarter score"),
    _f("home_score_ot", pa.int32(), "Home overtime score"),
    _f("away_score_q1", pa.int32(), "Away 1st quarter score"),
    _f("away_score_q2", pa.int32(), "Away 2nd quarter score"),
    _f("away_score_q3", pa.int32(), "Away 3rd quarter score"),
    _f("away_score_q4", pa.int32(), "Away 4th quarter score"),
    _f("away_score_ot", pa.int32(), "Away overtime score"),
    # Venue
    _f("venue",      pa.string(), "Venue / stadium name"),
    _f("attendance", pa.int32(),  "Reported attendance"),
    # Weather (merged from old weather entity)
    _f("temperature",    pa.float64(), "Temperature at kickoff (°F)"),
    _f("wind_speed",     pa.float64(), "Wind speed (mph)"),
    _f("wind_direction", pa.string(),  "Wind direction (e.g. NNW)"),
    _f("humidity",       pa.float64(), "Relative humidity (%)"),
    _f("precipitation",  pa.float64(), "Precipitation probability or amount"),
    _f("conditions",     pa.string(),  "Weather conditions description"),
    _f("dome",           pa.bool_(),   "True if game played in a dome"),
    # Media (merged from old media entity)
    _f("tv_network",    pa.string(), "TV broadcast network"),
    _f("broadcast_url", pa.string(), "Streaming / broadcast URL"),
    _f("start_time",    pa.string(), "Scheduled start time"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. plays — merges old plays + drives via play_type discriminator
#    Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_plays = pa.schema([
    _f("game_id",   pa.int32(),  "Parent game identifier",          nullable=False),
    _f("play_type", pa.string(), "Record type — 'play' or 'drive'", nullable=False),
    _f("order",     pa.int32(),  "Sequence order within the game",  nullable=False),
    _f("type",      pa.string(), "Play type (e.g. Rush, Pass, Punt) or drive label"),
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
    # Drive-specific fields (play_type='drive')
    _f("drive_number",              pa.int32(),  "Drive sequence number"),
    _f("drive_plays",               pa.int32(),  "Number of plays in drive"),
    _f("drive_yards",               pa.int32(),  "Total yards gained in drive"),
    _f("drive_time_of_possession",  pa.string(), "Drive time of possession (mm:ss)"),
    _f("drive_result",              pa.string(), "Drive result (e.g. Touchdown, Punt, FG)"),
    _f("start_yardline",            pa.string(), "Starting yard line"),
    _f("end_yardline",              pa.string(), "Ending yard line"),
    # Partition / provenance
    _f("season", pa.int32(),  "Season year",            nullable=False),
    _f("week",   pa.int32(),  "Season week number",     nullable=False),
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. player_stats — merges per-game + season stats via scope discriminator
#    Also incorporates usage and PPA per-player metrics
#    Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_player_stats = pa.schema([
    # Identity
    _f("player_id",   pa.int32(),  "Player identifier",              nullable=False),
    _f("player_name", pa.string(), "Player display name"),
    _f("team_id",     pa.int32(),  "Team identifier"),
    _f("team_name",   pa.string(), "Team name"),
    _f("game_id",     pa.int32(),  "Game identifier (null for season scope)"),
    # Discriminator
    _f("scope",  pa.string(), "Record scope — 'game' or 'season'", nullable=False),
    _f("season", pa.int32(),  "Season year",                       nullable=False),
    _f("week",   pa.int32(),  "Week number (null for season scope)"),
    # Passing
    _f("passing_completions",   pa.int32(),   "Pass completions"),
    _f("passing_attempts",      pa.int32(),   "Pass attempts"),
    _f("passing_yards",         pa.int32(),   "Passing yards"),
    _f("passing_touchdowns",    pa.int32(),   "Passing touchdowns"),
    _f("passing_interceptions", pa.int32(),   "Interceptions thrown"),
    _f("passing_yards_per_game", pa.float64(), "Passing yards per game (season scope)"),
    _f("passing_rating",        pa.float64(), "Passer rating"),
    _f("passing_qbr",           pa.float64(), "ESPN QBR"),
    # Rushing
    _f("rushing_attempts",       pa.int32(),   "Rush attempts"),
    _f("rushing_yards",          pa.int32(),   "Rushing yards"),
    _f("rushing_touchdowns",     pa.int32(),   "Rushing touchdowns"),
    _f("rushing_yards_per_game", pa.float64(), "Rushing yards per game (season scope)"),
    _f("rushing_avg",            pa.float64(), "Yards per rush attempt"),
    _f("rushing_long",           pa.int32(),   "Longest rush"),
    # Receiving
    _f("receptions",              pa.int32(),   "Total receptions"),
    _f("receiving_yards",         pa.int32(),   "Receiving yards"),
    _f("receiving_touchdowns",    pa.int32(),   "Receiving touchdowns"),
    _f("receiving_yards_per_game", pa.float64(), "Receiving yards per game (season scope)"),
    _f("receiving_avg",           pa.float64(), "Yards per reception"),
    _f("receiving_targets",       pa.int32(),   "Pass targets"),
    _f("receiving_long",          pa.int32(),   "Longest reception"),
    # Defense
    _f("total_tackles",      pa.int32(),   "Total tackles"),
    _f("solo_tackles",       pa.int32(),   "Solo tackles"),
    _f("tackles_for_loss",   pa.float64(), "Tackles for loss"),
    _f("sacks",              pa.float64(), "Sacks"),
    _f("interceptions",      pa.int32(),   "Defensive interceptions"),
    _f("passes_defended",    pa.int32(),   "Passes defended / broken up"),
    _f("fumbles_recovered",  pa.int32(),   "Fumbles recovered"),
    _f("fumbles_forced",     pa.int32(),   "Fumbles forced"),
    # Usage / PPA
    _f("usage_overall",   pa.float64(), "Overall usage rate"),
    _f("usage_passing",   pa.float64(), "Passing usage rate"),
    _f("usage_rushing",   pa.float64(), "Rushing usage rate"),
    _f("usage_receiving", pa.float64(), "Receiving usage rate"),
    _f("ppa_overall",     pa.float64(), "Predicted Points Added — overall"),
    _f("ppa_passing",     pa.float64(), "Predicted Points Added — passing"),
    _f("ppa_rushing",     pa.float64(), "Predicted Points Added — rushing"),
    _f("ppa_receiving",   pa.float64(), "Predicted Points Added — receiving"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
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
    _f("third_down_efficiency",   pa.string(),  "Third-down conversion rate (e.g. 5-12)"),
    _f("fourth_down_efficiency",  pa.string(),  "Fourth-down conversion rate"),
    _f("passing_yards",           pa.float64(), "Passing yards"),
    _f("rushing_yards",           pa.float64(), "Rushing yards"),
    _f("total_yards",             pa.float64(), "Total offensive yards"),
    _f("turnovers",               pa.int32(),   "Total turnovers"),
    _f("penalties",               pa.int32(),   "Total penalties"),
    _f("penalty_yards",           pa.int32(),   "Total penalty yards"),
    _f("possession_time",         pa.string(),  "Time of possession (mm:ss)"),
    # Season aggregate
    _f("passing_yards_per_game",  pa.float64(), "Season avg passing yards per game"),
    _f("passing_touchdowns",      pa.float64(), "Season passing touchdowns"),
    _f("passing_interceptions",   pa.float64(), "Season passing interceptions"),
    _f("passing_qb_rating",       pa.float64(), "Season QB passer rating"),
    _f("rushing_yards_per_game",  pa.float64(), "Season avg rushing yards per game"),
    _f("rushing_touchdowns",      pa.float64(), "Season rushing touchdowns"),
    _f("receiving_yards",         pa.float64(), "Season total receiving yards"),
    _f("receiving_touchdowns",    pa.float64(), "Season receiving touchdowns"),
    _f("opp_passing_yards",       pa.float64(), "Season opponent passing yards"),
    _f("opp_rushing_yards",       pa.float64(), "Season opponent rushing yards"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. standings — merges old standings + ATS records
#    Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_standings = pa.schema([
    _f("team_id",         pa.int32(),   "Team identifier"),
    _f("team_name",       pa.string(),  "Team name"),
    _f("conference_id",   pa.int32(),   "Conference identifier"),
    _f("conference_name", pa.string(),  "Conference name"),
    _f("season",          pa.int32(),   "Season year", nullable=False),
    # Win/loss
    _f("wins",            pa.int32(),   "Total wins"),
    _f("losses",          pa.int32(),   "Total losses"),
    _f("win_percentage",  pa.float64(), "Win percentage"),
    _f("games_behind",    pa.float64(), "Games behind division leader"),
    _f("home_record",     pa.string(),  "Home record (e.g. 6-1)"),
    _f("away_record",     pa.string(),  "Away record (e.g. 4-3)"),
    _f("conference_record", pa.string(), "Conference record"),
    # ATS (against the spread)
    _f("ats_wins",    pa.int32(), "ATS wins"),
    _f("ats_losses",  pa.int32(), "ATS losses"),
    _f("ats_pushes",  pa.int32(), "ATS pushes"),
    _f("over_wins",   pa.int32(), "Over wins"),
    _f("over_losses", pa.int32(), "Over losses"),
    _f("over_pushes", pa.int32(), "Over pushes"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. rankings — partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_rankings = pa.schema([
    _f("team_id",           pa.int32(),  "Team identifier"),
    _f("team_name",         pa.string(), "Team name"),
    _f("season",            pa.int32(),  "Season year",      nullable=False),
    _f("week",              pa.int32(),  "Season week number", nullable=False),
    _f("poll",              pa.string(), "Poll name — AP Top 25, Coaches Poll, CFP"),
    _f("rank",              pa.int32(),  "Rank within the poll", nullable=False),
    _f("first_place_votes", pa.int32(),  "Number of first-place votes"),
    _f("points",            pa.int32(),  "Total poll points"),
    _f("trend",             pa.string(), "Ranking trend vs prior week"),
    _f("record",            pa.string(), "Team record at time of ranking"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. odds — merges pregame + live + history + player props
#     Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_odds = pa.schema([
    _f("id",        pa.int32(),  "Odds line identifier"),
    _f("game_id",   pa.int32(),  "Parent game identifier", nullable=False),
    _f("vendor",    pa.string(), "Sportsbook / odds vendor"),
    _f("line_type", pa.string(), "Line type — pregame, live, history, or player_prop", nullable=False),
    # Spread
    _f("spread_home_value", pa.string(), "Home spread value (e.g. -3.5)"),
    _f("spread_home_odds",  pa.int32(),  "Home spread odds (American)"),
    _f("spread_away_value", pa.string(), "Away spread value"),
    _f("spread_away_odds",  pa.int32(),  "Away spread odds (American)"),
    # Moneyline
    _f("moneyline_home_odds", pa.int32(), "Home moneyline (American)"),
    _f("moneyline_away_odds", pa.int32(), "Away moneyline (American)"),
    # Totals
    _f("total_value",      pa.string(), "Over/under total value"),
    _f("total_over_odds",  pa.int32(),  "Over odds (American)"),
    _f("total_under_odds", pa.int32(),  "Under odds (American)"),
    # Player prop fields (line_type='player_prop')
    _f("player_id",      pa.int32(),   "Player identifier (props only)"),
    _f("player_name",    pa.string(),  "Player name (props only)"),
    _f("prop_type",      pa.string(),  "Prop type (e.g. passing_yards)"),
    _f("prop_value",     pa.float64(), "Prop line value"),
    _f("prop_over_odds", pa.int32(),   "Prop over odds (American)"),
    _f("prop_under_odds", pa.int32(),  "Prop under odds (American)"),
    # Metadata
    _f("updated_at", pa.string(), "Timestamp of last odds update"),
    _f("season",     pa.int32(),  "Season year",          nullable=False),
    _f("week",       pa.int32(),  "Season week number",   nullable=False),
    _f("source",     pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. ratings — merges elo/sp/fpi/srs/talent/sp_conference via
#     rating_type discriminator
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_ratings = pa.schema([
    _f("team_id",         pa.int32(),  "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("conference_name", pa.string(), "Conference name (used for sp_conference type)"),
    _f("season",          pa.int32(),  "Season year", nullable=False),
    _f("rating_type",     pa.string(), "Rating system — elo, sp, fpi, srs, talent, or sp_conference", nullable=False),
    # Universal primary value
    _f("rating_value", pa.float64(), "Primary rating number for this system"),
    # SP+ breakdown
    _f("offense",       pa.float64(), "SP+ offensive rating"),
    _f("defense",       pa.float64(), "SP+ defensive rating"),
    _f("special_teams", pa.float64(), "SP+ special teams rating"),
    # FPI breakdown
    _f("avg_win_prob",      pa.float64(), "FPI average win probability"),
    _f("strength_of_schedule", pa.float64(), "FPI strength of schedule"),
    _f("remaining_sos",     pa.float64(), "FPI remaining strength of schedule"),
    # Talent
    _f("talent_rating", pa.float64(), "Talent composite rating"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 12. advanced — merges EPA + PPA + havoc + win_probability via
#     metric_type discriminator
#     Partition: season=, week=
# ═══════════════════════════════════════════════════════════════════════

schema_advanced = pa.schema([
    _f("game_id",   pa.int32(),  "Game identifier"),
    _f("team_id",   pa.int32(),  "Team identifier"),
    _f("team_name", pa.string(), "Team name"),
    _f("season",    pa.int32(),  "Season year",      nullable=False),
    _f("week",      pa.int32(),  "Season week number", nullable=False),
    _f("metric_type", pa.string(), "Metric type — epa, ppa, havoc, or win_probability", nullable=False),
    # Shared offense / defense split
    _f("overall", pa.float64(), "Overall value for metric"),
    _f("passing", pa.float64(), "Passing component"),
    _f("rushing", pa.float64(), "Rushing component"),
    # EPA specific
    _f("success_rate", pa.float64(), "EPA success rate"),
    # Havoc specific
    _f("front_seven", pa.float64(), "Havoc: front seven rate"),
    _f("db_havoc",    pa.float64(), "Havoc: defensive back havoc rate"),
    # Win probability specific
    _f("home_win_prob", pa.float64(), "Pre-game home win probability"),
    _f("away_win_prob", pa.float64(), "Pre-game away win probability"),
    _f("spread",        pa.float64(), "Model-implied spread"),
    _f("over_under",    pa.float64(), "Model-implied over/under"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. recruiting — merges classes + players + groups via scope
#     discriminator
#     Partition: season=
# ═══════════════════════════════════════════════════════════════════════

schema_recruiting = pa.schema([
    _f("scope",     pa.string(), "Record scope — 'class', 'player', or 'group'", nullable=False),
    _f("team_name", pa.string(), "Team name"),
    _f("season",    pa.int32(),  "Recruiting class year", nullable=False),
    # Class-level (scope='class')
    _f("class_rank",    pa.int32(),   "National recruiting class rank"),
    _f("class_points",  pa.float64(), "Total recruiting class points"),
    _f("total_commits", pa.int32(),   "Total commits in class"),
    _f("avg_rating",    pa.float64(), "Average recruit rating"),
    _f("five_star",     pa.int32(),   "Number of 5-star recruits"),
    _f("four_star",     pa.int32(),   "Number of 4-star recruits"),
    _f("three_star",    pa.int32(),   "Number of 3-star recruits"),
    # Player-level (scope='player')
    _f("player_id",   pa.int32(),   "Recruit player identifier"),
    _f("player_name", pa.string(),  "Recruit player name"),
    _f("position",    pa.string(),  "Recruit position"),
    _f("stars",       pa.int32(),   "Recruit star rating"),
    _f("rating",      pa.float64(), "Recruit composite rating"),
    _f("city",        pa.string(),  "Recruit hometown city"),
    _f("state",       pa.string(),  "Recruit hometown state"),
    _f("height",      pa.string(),  "Recruit height"),
    _f("weight",      pa.string(),  "Recruit weight"),
    # Group-level (scope='group')
    _f("position_group",    pa.string(),  "Position group name"),
    _f("group_commits",     pa.int32(),   "Commits in position group"),
    _f("group_avg_rating",  pa.float64(), "Average rating in position group"),
    _f("group_points",      pa.float64(), "Total points in position group"),
    # Provenance
    _f("source", pa.string(), "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. venues — static reference (no partitioning)
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
    _f("timezone",         pa.string(),  "IANA timezone identifier"),
    _f("elevation",        pa.float64(), "Elevation in feet"),
    _f("source",           pa.string(),  "Data vendor provenance", nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Registry — schema, partition key, and path look-ups
# ═══════════════════════════════════════════════════════════════════════

NCAAF_SCHEMAS: dict[str, pa.Schema] = {
    "conferences":  schema_conferences,
    "teams":        schema_teams,
    "players":      schema_players,
    "games":        schema_games,
    "plays":        schema_plays,
    "player_stats": schema_player_stats,
    "team_stats":   schema_team_stats,
    "standings":    schema_standings,
    "rankings":     schema_rankings,
    "odds":         schema_odds,
    "ratings":      schema_ratings,
    "advanced":     schema_advanced,
    "recruiting":   schema_recruiting,
    "venues":       schema_venues,
}

PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning
    "conferences":  [],
    "venues":       [],
    # Season only
    "teams":        ["season"],
    "players":      ["season"],
    "standings":    ["season"],
    "ratings":      ["season"],
    "recruiting":   ["season"],
    # Season + week
    "games":        ["season", "week"],
    "plays":        ["season", "week"],
    "player_stats": ["season", "week"],
    "team_stats":   ["season", "week"],
    "rankings":     ["season", "week"],
    "odds":         ["season", "week"],
    "advanced":     ["season", "week"],
}

NCAAF_ENTITY_PATHS: dict[str, str] = {
    "conferences":  "conferences",
    "teams":        "teams",
    "players":      "players",
    "games":        "games",
    "plays":        "plays",
    "player_stats": "player_stats",
    "team_stats":   "team_stats",
    "standings":    "standings",
    "rankings":     "rankings",
    "odds":         "odds",
    "ratings":      "ratings",
    "advanced":     "advanced",
    "recruiting":   "recruiting",
    "venues":       "venues",
}


# ═══════════════════════════════════════════════════════════════════════
# Migration reference — maps old 34 entity names → new 14 entity names
# ═══════════════════════════════════════════════════════════════════════

CONSOLIDATION_MAP: dict[str, str] = {
    # 1:1 carries
    "conferences":          "conferences",
    "venues":               "venues",
    "rankings":             "rankings",
    "standings":            "standings",
    # teams absorbs coaches
    "teams":                "teams",
    "coaches":              "coaches → teams",
    # players absorbs portal, returning_production, draft
    "players":              "players",
    "portal":               "portal → players",
    "returning_production": "returning_production → players",
    "draft":                "draft → players",
    "injuries":             "injuries → players",
    # games absorbs weather, media
    "games":                "games",
    "weather":              "weather → games",
    "media":                "media → games",
    # plays absorbs drives
    "plays":                "plays",
    "drives":               "drives → plays",
    # player_stats absorbs player_season_stats + usage + PPA
    "player_stats":         "player_stats",
    "player_season_stats":  "player_season_stats → player_stats",
    # team_stats absorbs team_season_stats
    "team_stats":           "team_stats",
    "team_season_stats":    "team_season_stats → team_stats",
    # odds absorbs all odds sub-entities
    "odds":                 "odds",
    # ratings consolidates all 6 rating systems
    "ratings/elo":            "ratings/elo → ratings",
    "ratings/sp":             "ratings/sp → ratings",
    "ratings/fpi":            "ratings/fpi → ratings",
    "ratings/srs":            "ratings/srs → ratings",
    "ratings/talent":         "ratings/talent → ratings",
    "ratings/sp_conference":  "ratings/sp_conference → ratings",
    # advanced consolidates all 4 advanced metric types
    "advanced/epa":             "advanced/epa → advanced",
    "advanced/ppa":             "advanced/ppa → advanced",
    "advanced/havoc":           "advanced/havoc → advanced",
    "advanced/win_probability": "advanced/win_probability → advanced",
    # recruiting consolidates classes, players, groups
    "recruiting_classes":   "recruiting_classes → recruiting",
    "recruiting_players":   "recruiting_players → recruiting",
    "recruiting_groups":    "recruiting_groups → recruiting",
}
