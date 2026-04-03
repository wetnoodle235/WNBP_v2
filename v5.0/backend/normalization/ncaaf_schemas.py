# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — NCAAF Normalized-Curated PyArrow Schemas
# ──────────────────────────────────────────────────────────────────────
#
# Flat, entity-centric schemas modelled after BallDontLie's NCAAF API
# plus WNBP-exclusive extensions (weather, recruiting, advanced, etc.).
#
# Each entity maps 1-to-1 with a top-level folder under
# ``data/normalized_curated/ncaaf/<entity>/`` and is stored as
# hive-partitioned Parquet (``season=YYYY/`` or ``season=YYYY/week=WW/``).
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
    _f("id",           pa.int64(),  "Unique conference identifier"),
    _f("name",         pa.string(), "Full conference name"),
    _f("abbreviation", pa.string(), "Short abbreviation (e.g. SEC, B1G)"),
    _f("source",       pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 2. teams — partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_teams = pa.schema([
    _f("id",           pa.int64(),  "Unique team identifier"),
    _f("conference",   pa.string(), "Conference name or abbreviation"),
    _f("city",         pa.string(), "City where team is located"),
    _f("name",         pa.string(), "Short team name (e.g. Crimson Tide)"),
    _f("full_name",    pa.string(), "Full team name (e.g. Alabama Crimson Tide)"),
    _f("abbreviation", pa.string(), "Team abbreviation (e.g. ALA)"),
    _f("source",       pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 3. players — partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_players = pa.schema([
    _f("id",                    pa.int64(),  "Unique player identifier"),
    _f("first_name",            pa.string(), "Player first name"),
    _f("last_name",             pa.string(), "Player last name"),
    _f("position",              pa.string(), "Full position name"),
    _f("position_abbreviation", pa.string(), "Position abbreviation (e.g. QB, WR)"),
    _f("height",                pa.string(), "Height (e.g. 6-2)"),
    _f("weight",                pa.int32(),  "Weight in pounds"),
    _f("jersey_number",         pa.int32(),  "Jersey number"),
    _f("team",                  pa.string(), "Team name or abbreviation"),
    _f("source",                pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 4. games — partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_games = pa.schema([
    _f("id",                 pa.int64(),        "Unique game identifier"),
    _f("date",               pa.timestamp("s"), "Game date and time (UTC)"),
    _f("season",             pa.int32(),        "Season year"),
    _f("week",               pa.int32(),        "Week number"),
    _f("status",             pa.string(),       "Game status (scheduled, in_progress, final)"),
    _f("period",             pa.int32(),        "Current period/quarter"),
    _f("time",               pa.string(),       "Clock time remaining in current period"),
    _f("home_team",          pa.string(),       "Home team name"),
    _f("visitor_team",       pa.string(),       "Visiting team name"),
    _f("home_score",         pa.int32(),        "Home team total score"),
    _f("visitor_score",      pa.int32(),        "Visitor team total score"),
    _f("home_score_q1",      pa.int32(),        "Home team Q1 score"),
    _f("home_score_q2",      pa.int32(),        "Home team Q2 score"),
    _f("home_score_q3",      pa.int32(),        "Home team Q3 score"),
    _f("home_score_q4",      pa.int32(),        "Home team Q4 score"),
    _f("home_score_ot",      pa.int32(),        "Home team overtime score"),
    _f("visitor_score_q1",   pa.int32(),        "Visitor team Q1 score"),
    _f("visitor_score_q2",   pa.int32(),        "Visitor team Q2 score"),
    _f("visitor_score_q3",   pa.int32(),        "Visitor team Q3 score"),
    _f("visitor_score_q4",   pa.int32(),        "Visitor team Q4 score"),
    _f("visitor_score_ot",   pa.int32(),        "Visitor team overtime score"),
    _f("venue_id",           pa.int64(),        "Venue identifier (FK to venues)"),
    _f("source",             pa.string(),       "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 5. plays — partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_plays = pa.schema([
    _f("game_id",       pa.int64(),  "Game identifier (FK to games)"),
    _f("order",         pa.int32(),  "Sequential play order within the game"),
    _f("type",          pa.string(), "Play type (e.g. Rush, Pass, Punt)"),
    _f("text",          pa.string(), "Human-readable play description"),
    _f("home_score",    pa.int32(),  "Home team score after this play"),
    _f("away_score",    pa.int32(),  "Away team score after this play"),
    _f("period",        pa.int32(),  "Quarter / period number"),
    _f("clock",         pa.string(), "Game clock at time of play (mm:ss)"),
    _f("scoring_play",  pa.bool_(),  "Whether this play resulted in a score"),
    _f("score_value",   pa.int32(),  "Points scored on this play (0 if none)"),
    _f("team",          pa.string(), "Team with possession"),
    _f("yard_line",     pa.int32(),  "Yard line at snap"),
    _f("down",          pa.int32(),  "Down number (1-4)"),
    _f("distance",      pa.int32(),  "Yards to go for first down"),
    _f("yards_gained",  pa.int32(),  "Net yards gained on the play"),
    _f("source",        pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 6. player_stats — per-game, partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_player_stats = pa.schema([
    _f("player",                   pa.string(), "Player name or identifier"),
    _f("player_id",                pa.int64(),  "Player unique ID (FK to players)"),
    _f("team",                     pa.string(), "Team name or abbreviation"),
    _f("game_id",                  pa.int64(),  "Game identifier (FK to games)"),
    # Passing
    _f("passing_completions",      pa.int32(),  "Pass completions"),
    _f("passing_attempts",         pa.int32(),  "Pass attempts"),
    _f("passing_yards",            pa.int32(),  "Passing yards"),
    _f("passing_td",               pa.int32(),  "Passing touchdowns"),
    _f("passing_int",              pa.int32(),  "Interceptions thrown"),
    _f("passing_qbr",              pa.float64(),"Quarterback rating (QBR)"),
    _f("passing_rating",           pa.float64(),"Passer rating"),
    # Rushing
    _f("rushing_attempts",         pa.int32(),  "Rush attempts"),
    _f("rushing_yards",            pa.int32(),  "Rushing yards"),
    _f("rushing_td",               pa.int32(),  "Rushing touchdowns"),
    _f("rushing_long",             pa.int32(),  "Longest rush"),
    # Receiving
    _f("receiving_receptions",     pa.int32(),  "Receptions"),
    _f("receiving_yards",          pa.int32(),  "Receiving yards"),
    _f("receiving_td",             pa.int32(),  "Receiving touchdowns"),
    _f("receiving_targets",        pa.int32(),  "Targets"),
    _f("receiving_long",           pa.int32(),  "Longest reception"),
    # Defense
    _f("defense_tackles",          pa.int32(),  "Total tackles"),
    _f("defense_solo",             pa.int32(),  "Solo tackles"),
    _f("defense_tfl",              pa.int32(),  "Tackles for loss"),
    _f("defense_sacks",            pa.float64(),"Sacks"),
    _f("defense_int",              pa.int32(),  "Defensive interceptions"),
    _f("defense_pd",               pa.int32(),  "Passes defended"),
    # Special teams
    _f("kick_return_yards",        pa.int32(),  "Kick return yards"),
    _f("punt_return_yards",        pa.int32(),  "Punt return yards"),
    _f("fumbles_lost",             pa.int32(),  "Fumbles lost"),
    _f("source",                   pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 7. team_stats — per-game, partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_team_stats = pa.schema([
    _f("team",                     pa.string(), "Team name or abbreviation"),
    _f("team_id",                  pa.int64(),  "Team unique ID (FK to teams)"),
    _f("game_id",                  pa.int64(),  "Game identifier (FK to games)"),
    _f("first_downs",              pa.int32(),  "Total first downs"),
    _f("third_down_conversions",   pa.int32(),  "Third-down conversions"),
    _f("third_down_attempts",      pa.int32(),  "Third-down attempts"),
    _f("fourth_down_conversions",  pa.int32(),  "Fourth-down conversions"),
    _f("fourth_down_attempts",     pa.int32(),  "Fourth-down attempts"),
    _f("passing_yards",            pa.int32(),  "Total passing yards"),
    _f("rushing_yards",            pa.int32(),  "Total rushing yards"),
    _f("total_yards",              pa.int32(),  "Total offensive yards"),
    _f("turnovers",                pa.int32(),  "Total turnovers"),
    _f("penalties",                pa.int32(),  "Number of penalties"),
    _f("penalty_yards",            pa.int32(),  "Penalty yards"),
    _f("possession_time",          pa.string(), "Time of possession (mm:ss)"),
    _f("source",                   pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 8. player_season_stats — partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_player_season_stats = pa.schema([
    _f("player",                   pa.string(), "Player name or identifier"),
    _f("player_id",                pa.int64(),  "Player unique ID (FK to players)"),
    _f("team",                     pa.string(), "Team name or abbreviation"),
    _f("season",                   pa.int32(),  "Season year"),
    _f("games_played",             pa.int32(),  "Games played in season"),
    # Passing
    _f("passing_completions",      pa.int32(),  "Season pass completions"),
    _f("passing_attempts",         pa.int32(),  "Season pass attempts"),
    _f("passing_yards",            pa.int32(),  "Season passing yards"),
    _f("passing_td",               pa.int32(),  "Season passing touchdowns"),
    _f("passing_int",              pa.int32(),  "Season interceptions thrown"),
    _f("passing_qbr",              pa.float64(),"Season average QBR"),
    _f("passing_rating",           pa.float64(),"Season passer rating"),
    # Rushing
    _f("rushing_attempts",         pa.int32(),  "Season rush attempts"),
    _f("rushing_yards",            pa.int32(),  "Season rushing yards"),
    _f("rushing_td",               pa.int32(),  "Season rushing touchdowns"),
    _f("rushing_long",             pa.int32(),  "Season longest rush"),
    # Receiving
    _f("receiving_receptions",     pa.int32(),  "Season receptions"),
    _f("receiving_yards",          pa.int32(),  "Season receiving yards"),
    _f("receiving_td",             pa.int32(),  "Season receiving touchdowns"),
    _f("receiving_targets",        pa.int32(),  "Season targets"),
    _f("receiving_long",           pa.int32(),  "Season longest reception"),
    # Defense
    _f("defense_tackles",          pa.int32(),  "Season total tackles"),
    _f("defense_solo",             pa.int32(),  "Season solo tackles"),
    _f("defense_tfl",              pa.int32(),  "Season tackles for loss"),
    _f("defense_sacks",            pa.float64(),"Season sacks"),
    _f("defense_int",              pa.int32(),  "Season defensive interceptions"),
    _f("defense_pd",               pa.int32(),  "Season passes defended"),
    _f("source",                   pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 9. team_season_stats — partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_team_season_stats = pa.schema([
    _f("team",                     pa.string(), "Team name or abbreviation"),
    _f("team_id",                  pa.int64(),  "Team unique ID (FK to teams)"),
    _f("season",                   pa.int32(),  "Season year"),
    _f("games_played",             pa.int32(),  "Total games played"),
    _f("passing_yards",            pa.int32(),  "Season total passing yards"),
    _f("passing_yards_per_game",   pa.float64(),"Passing yards per game"),
    _f("rushing_yards",            pa.int32(),  "Season total rushing yards"),
    _f("rushing_yards_per_game",   pa.float64(),"Rushing yards per game"),
    _f("receiving_yards",          pa.int32(),  "Season total receiving yards"),
    _f("total_yards",              pa.int32(),  "Season total offensive yards"),
    _f("total_yards_per_game",     pa.float64(),"Total yards per game"),
    _f("passing_td",               pa.int32(),  "Season passing touchdowns"),
    _f("rushing_td",               pa.int32(),  "Season rushing touchdowns"),
    _f("total_td",                 pa.int32(),  "Season total touchdowns"),
    _f("interceptions",            pa.int32(),  "Season interceptions thrown"),
    _f("fumbles_lost",             pa.int32(),  "Season fumbles lost"),
    _f("turnovers",                pa.int32(),  "Season total turnovers"),
    # Opponent stats
    _f("opp_passing_yards",        pa.int32(),  "Opponent season passing yards"),
    _f("opp_rushing_yards",        pa.int32(),  "Opponent season rushing yards"),
    _f("opp_total_yards",          pa.int32(),  "Opponent season total yards"),
    _f("opp_points_per_game",      pa.float64(),"Opponent points per game"),
    _f("source",                   pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 10. standings — partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_standings = pa.schema([
    _f("team",              pa.string(),  "Team name or abbreviation"),
    _f("team_id",           pa.int64(),   "Team unique ID (FK to teams)"),
    _f("conference",        pa.string(),  "Conference name"),
    _f("season",            pa.int32(),   "Season year"),
    _f("wins",              pa.int32(),   "Total wins"),
    _f("losses",            pa.int32(),   "Total losses"),
    _f("win_pct",           pa.float64(), "Win percentage"),
    _f("games_behind",      pa.float64(), "Games behind conference leader"),
    _f("home_wins",         pa.int32(),   "Home wins"),
    _f("home_losses",       pa.int32(),   "Home losses"),
    _f("away_wins",         pa.int32(),   "Away wins"),
    _f("away_losses",       pa.int32(),   "Away losses"),
    _f("conference_wins",   pa.int32(),   "Conference wins"),
    _f("conference_losses", pa.int32(),   "Conference losses"),
    _f("streak",            pa.string(),  "Current win/loss streak (e.g. W3, L2)"),
    _f("source",            pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 11. rankings — partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_rankings = pa.schema([
    _f("team",                pa.string(),  "Team name"),
    _f("team_id",             pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",              pa.int32(),   "Season year"),
    _f("week",                pa.int32(),   "Week number"),
    _f("poll",                pa.string(),  "Poll name (AP, Coaches, CFP)"),
    _f("rank",                pa.int32(),   "Team ranking"),
    _f("first_place_votes",   pa.int32(),   "First place votes received"),
    _f("points",              pa.int32(),   "Total poll points"),
    _f("trend",               pa.int32(),   "Rank change from previous week (positive = up)"),
    _f("record",              pa.string(),  "Team record (e.g. 8-1)"),
    _f("source",              pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 12. odds — partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_odds = pa.schema([
    _f("id",                   pa.int64(),        "Unique odds record identifier"),
    _f("game_id",              pa.int64(),        "Game identifier (FK to games)"),
    _f("vendor",               pa.string(),       "Odds vendor / sportsbook name"),
    _f("spread_home_value",    pa.float64(),      "Home team spread value"),
    _f("spread_home_odds",     pa.int32(),        "Home team spread odds (American)"),
    _f("spread_away_value",    pa.float64(),      "Away team spread value"),
    _f("spread_away_odds",     pa.int32(),        "Away team spread odds (American)"),
    _f("moneyline_home",       pa.int32(),        "Home team moneyline (American)"),
    _f("moneyline_away",       pa.int32(),        "Away team moneyline (American)"),
    _f("total_value",          pa.float64(),      "Over/under total value"),
    _f("total_over_odds",      pa.int32(),        "Over odds (American)"),
    _f("total_under_odds",     pa.int32(),        "Under odds (American)"),
    _f("updated_at",           pa.timestamp("s"), "Timestamp when odds were last updated"),
    _f("source",               pa.string(),       "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 13. coaches — WNBP-exclusive, partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_coaches = pa.schema([
    _f("season",     pa.int32(),  "Season year"),
    _f("team",       pa.string(), "Team name or abbreviation"),
    _f("team_id",    pa.int64(),  "Team unique ID (FK to teams)"),
    _f("first_name", pa.string(), "Coach first name"),
    _f("last_name",  pa.string(), "Coach last name"),
    _f("position",   pa.string(), "Coaching position (HC, OC, DC)"),
    _f("years",      pa.int32(),  "Years of coaching experience"),
    _f("wins",       pa.int32(),  "Career wins at this school"),
    _f("losses",     pa.int32(),  "Career losses at this school"),
    _f("hire_year",  pa.int32(),  "Year coach was hired"),
    _f("source",     pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 14. weather — WNBP-exclusive, partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_weather = pa.schema([
    _f("game_id",        pa.int64(),   "Game identifier (FK to games)"),
    _f("season",         pa.int32(),   "Season year"),
    _f("week",           pa.int32(),   "Week number"),
    _f("temperature",    pa.float64(), "Temperature in Fahrenheit"),
    _f("wind_speed",     pa.float64(), "Wind speed in mph"),
    _f("wind_direction", pa.string(),  "Wind direction (e.g. NW, SSE)"),
    _f("humidity",       pa.float64(), "Humidity percentage"),
    _f("precipitation",  pa.float64(), "Precipitation probability or inches"),
    _f("conditions",     pa.string(),  "Weather conditions description (clear, rain, snow)"),
    _f("dome",           pa.bool_(),   "Whether game is played in a dome"),
    _f("source",         pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 15. injuries — WNBP-exclusive, partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_injuries = pa.schema([
    _f("player_id",   pa.int64(),  "Player unique ID (FK to players)"),
    _f("player_name", pa.string(), "Player display name"),
    _f("team_id",     pa.int64(),  "Team unique ID (FK to teams)"),
    _f("team",        pa.string(), "Team name or abbreviation"),
    _f("season",      pa.int32(),  "Season year"),
    _f("week",        pa.int32(),  "Week number"),
    _f("status",      pa.string(), "Injury status (out, doubtful, questionable, probable)"),
    _f("injury_type", pa.string(), "Type of injury"),
    _f("body_part",   pa.string(), "Affected body part"),
    _f("return_date",  pa.date32(), "Estimated return date"),
    _f("source",      pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 16. recruiting_classes — WNBP-exclusive, partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_recruiting_classes = pa.schema([
    _f("team",          pa.string(),  "Team name or abbreviation"),
    _f("team_id",       pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",        pa.int32(),   "Recruiting class year"),
    _f("rank",          pa.int32(),   "National recruiting class rank"),
    _f("points",        pa.float64(), "Total recruiting points"),
    _f("total_commits", pa.int32(),   "Total number of commitments"),
    _f("avg_rating",    pa.float64(), "Average recruit rating"),
    _f("five_star",     pa.int32(),   "Number of 5-star recruits"),
    _f("four_star",     pa.int32(),   "Number of 4-star recruits"),
    _f("three_star",    pa.int32(),   "Number of 3-star recruits"),
    _f("source",        pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 17. recruiting_players — WNBP-exclusive, partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_recruiting_players = pa.schema([
    _f("player_id", pa.int64(),   "Unique recruit identifier"),
    _f("season",    pa.int32(),   "Recruiting class year"),
    _f("name",      pa.string(),  "Recruit full name"),
    _f("position",  pa.string(),  "Position (e.g. QB, WR, DT)"),
    _f("team",      pa.string(),  "Committed team"),
    _f("stars",     pa.int32(),   "Star rating (2-5)"),
    _f("rating",    pa.float64(), "Composite rating"),
    _f("city",      pa.string(),  "Hometown city"),
    _f("state",     pa.string(),  "Hometown state"),
    _f("height",    pa.string(),  "Height (e.g. 6-3)"),
    _f("weight",    pa.int32(),   "Weight in pounds"),
    _f("source",    pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 18. recruiting_groups — WNBP-exclusive, partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_recruiting_groups = pa.schema([
    _f("team",           pa.string(),  "Team name or abbreviation"),
    _f("team_id",        pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",         pa.int32(),   "Recruiting class year"),
    _f("position_group", pa.string(),  "Position group (e.g. QB, OL, DL, LB, DB)"),
    _f("total_commits",  pa.int32(),   "Commitments in this position group"),
    _f("avg_rating",     pa.float64(), "Average rating for position group"),
    _f("points",         pa.float64(), "Total points for position group"),
    _f("source",         pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 19. ratings — subfolder split, each partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_ratings_elo = pa.schema([
    _f("team",       pa.string(),  "Team name or abbreviation"),
    _f("team_id",    pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",     pa.int32(),   "Season year"),
    _f("elo_rating", pa.float64(), "Elo rating value"),
    _f("source",     pa.string(),  "Data vendor provenance"),
])

schema_ratings_sp = pa.schema([
    _f("team",          pa.string(),  "Team name or abbreviation"),
    _f("team_id",       pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",        pa.int32(),   "Season year"),
    _f("overall",       pa.float64(), "SP+ overall rating"),
    _f("offense",       pa.float64(), "SP+ offensive rating"),
    _f("defense",       pa.float64(), "SP+ defensive rating"),
    _f("special_teams", pa.float64(), "SP+ special teams rating"),
    _f("source",        pa.string(),  "Data vendor provenance"),
])

schema_ratings_fpi = pa.schema([
    _f("team",           pa.string(),  "Team name or abbreviation"),
    _f("team_id",        pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",         pa.int32(),   "Season year"),
    _f("fpi",            pa.float64(), "Football Power Index value"),
    _f("avg_win_prob",   pa.float64(), "Average win probability"),
    _f("sos",            pa.float64(), "Strength of schedule"),
    _f("remaining_sos",  pa.float64(), "Remaining strength of schedule"),
    _f("source",         pa.string(),  "Data vendor provenance"),
])

schema_ratings_srs = pa.schema([
    _f("team",       pa.string(),  "Team name or abbreviation"),
    _f("team_id",    pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",     pa.int32(),   "Season year"),
    _f("srs_rating", pa.float64(), "Simple Rating System value"),
    _f("source",     pa.string(),  "Data vendor provenance"),
])

schema_ratings_talent = pa.schema([
    _f("team",          pa.string(),  "Team name or abbreviation"),
    _f("team_id",       pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",        pa.int32(),   "Season year"),
    _f("talent_rating", pa.float64(), "Composite talent rating"),
    _f("source",        pa.string(),  "Data vendor provenance"),
])

schema_ratings_sp_conference = pa.schema([
    _f("conference", pa.string(),  "Conference name or abbreviation"),
    _f("season",     pa.int32(),   "Season year"),
    _f("overall",    pa.float64(), "Conference SP+ overall average"),
    _f("offense",    pa.float64(), "Conference SP+ offensive average"),
    _f("defense",    pa.float64(), "Conference SP+ defensive average"),
    _f("source",     pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 20. advanced — subfolder split, each partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_advanced_epa = pa.schema([
    _f("game_id",          pa.int64(),   "Game identifier (FK to games)"),
    _f("team",             pa.string(),  "Team name or abbreviation"),
    _f("team_id",          pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",           pa.int32(),   "Season year"),
    _f("week",             pa.int32(),   "Week number"),
    _f("epa_overall",      pa.float64(), "Overall Expected Points Added"),
    _f("epa_passing",      pa.float64(), "Passing EPA"),
    _f("epa_rushing",      pa.float64(), "Rushing EPA"),
    _f("epa_success_rate", pa.float64(), "EPA success rate"),
    _f("source",           pa.string(),  "Data vendor provenance"),
])

schema_advanced_ppa = pa.schema([
    _f("game_id",      pa.int64(),   "Game identifier (FK to games)"),
    _f("team",         pa.string(),  "Team name or abbreviation"),
    _f("team_id",      pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",       pa.int32(),   "Season year"),
    _f("week",         pa.int32(),   "Week number"),
    _f("ppa_overall",  pa.float64(), "Overall Predicted Points Added"),
    _f("ppa_passing",  pa.float64(), "Passing PPA"),
    _f("ppa_rushing",  pa.float64(), "Rushing PPA"),
    _f("source",       pa.string(),  "Data vendor provenance"),
])

schema_advanced_havoc = pa.schema([
    _f("game_id",     pa.int64(),   "Game identifier (FK to games)"),
    _f("team",        pa.string(),  "Team name or abbreviation"),
    _f("team_id",     pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",      pa.int32(),   "Season year"),
    _f("week",        pa.int32(),   "Week number"),
    _f("total_havoc", pa.float64(), "Total havoc rate"),
    _f("front_seven", pa.float64(), "Front-seven havoc rate"),
    _f("db_havoc",    pa.float64(), "Defensive-back havoc rate"),
    _f("source",      pa.string(),  "Data vendor provenance"),
])

schema_advanced_win_probability = pa.schema([
    _f("game_id",       pa.int64(),   "Game identifier (FK to games)"),
    _f("season",        pa.int32(),   "Season year"),
    _f("week",          pa.int32(),   "Week number"),
    _f("home_win_prob", pa.float64(), "Pre-game home win probability"),
    _f("away_win_prob", pa.float64(), "Pre-game away win probability"),
    _f("spread",        pa.float64(), "Predicted spread"),
    _f("over_under",    pa.float64(), "Predicted over/under total"),
    _f("source",        pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 21. drives — partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_drives = pa.schema([
    _f("game_id",             pa.int64(),  "Game identifier (FK to games)"),
    _f("season",              pa.int32(),  "Season year"),
    _f("week",                pa.int32(),  "Week number"),
    _f("team",                pa.string(), "Team with possession"),
    _f("team_id",             pa.int64(),  "Team unique ID (FK to teams)"),
    _f("drive_number",        pa.int32(),  "Sequential drive number in game"),
    _f("plays",               pa.int32(),  "Number of plays in drive"),
    _f("yards",               pa.int32(),  "Total yards gained on drive"),
    _f("time_of_possession",  pa.string(), "Drive duration (mm:ss)"),
    _f("result",              pa.string(), "Drive result (TD, FG, Punt, Turnover, etc.)"),
    _f("start_period",        pa.int32(),  "Period when drive started"),
    _f("start_yardline",      pa.int32(),  "Starting yard line"),
    _f("end_period",          pa.int32(),  "Period when drive ended"),
    _f("end_yardline",        pa.int32(),  "Ending yard line"),
    _f("source",              pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 22. draft — partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_draft = pa.schema([
    _f("season",       pa.int32(),  "Draft year"),
    _f("pick",         pa.int32(),  "Overall pick number"),
    _f("round",        pa.int32(),  "Draft round"),
    _f("team",         pa.string(), "NFL team that made the pick"),
    _f("player_name",  pa.string(), "Player full name"),
    _f("position",     pa.string(), "Position"),
    _f("college_team", pa.string(), "College team name"),
    _f("height",       pa.string(), "Height (e.g. 6-4)"),
    _f("weight",       pa.int32(),  "Weight in pounds"),
    _f("source",       pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 23. portal — transfer portal, partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_portal = pa.schema([
    _f("player_id",        pa.int64(),  "Player unique ID"),
    _f("season",           pa.int32(),  "Transfer season year"),
    _f("first_name",       pa.string(), "Player first name"),
    _f("last_name",        pa.string(), "Player last name"),
    _f("origin_team",      pa.string(), "Team transferring from"),
    _f("destination_team", pa.string(), "Team transferring to (null if uncommitted)"),
    _f("position",         pa.string(), "Player position"),
    _f("stars",            pa.int32(),  "Star rating in portal"),
    _f("transfer_date",    pa.date32(), "Date entered transfer portal"),
    _f("source",           pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 24. returning_production — partitioned by season
# ═══════════════════════════════════════════════════════════════════════

schema_returning_production = pa.schema([
    _f("team",              pa.string(),  "Team name or abbreviation"),
    _f("team_id",           pa.int64(),   "Team unique ID (FK to teams)"),
    _f("season",            pa.int32(),   "Season year"),
    _f("ppa_usage",         pa.float64(), "PPA usage metric"),
    _f("total_ppa",         pa.float64(), "Total PPA returning"),
    _f("ppa_passing",       pa.float64(), "Passing PPA returning"),
    _f("ppa_receiving",     pa.float64(), "Receiving PPA returning"),
    _f("ppa_rushing",       pa.float64(), "Rushing PPA returning"),
    _f("percent_ppa",       pa.float64(), "Percentage of total PPA returning"),
    _f("percent_passing",   pa.float64(), "Percentage of passing production returning"),
    _f("percent_receiving", pa.float64(), "Percentage of receiving production returning"),
    _f("percent_rushing",   pa.float64(), "Percentage of rushing production returning"),
    _f("source",            pa.string(),  "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 25. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

schema_venues = pa.schema([
    _f("id",               pa.int64(),  "Unique venue identifier"),
    _f("name",             pa.string(), "Venue name"),
    _f("city",             pa.string(), "City"),
    _f("state",            pa.string(), "State"),
    _f("zip",              pa.string(), "ZIP code"),
    _f("capacity",         pa.int32(),  "Seating capacity"),
    _f("year_constructed",  pa.int32(), "Year the venue was built"),
    _f("grass",            pa.bool_(),  "Whether field is natural grass"),
    _f("dome",             pa.bool_(),  "Whether venue has a dome/roof"),
    _f("timezone",         pa.string(), "IANA timezone (e.g. America/New_York)"),
    _f("elevation",        pa.float64(),"Elevation in feet above sea level"),
    _f("source",           pa.string(), "Data vendor provenance"),
])

# ═══════════════════════════════════════════════════════════════════════
# 26. media — partitioned by season, week
# ═══════════════════════════════════════════════════════════════════════

schema_media = pa.schema([
    _f("game_id",          pa.int64(),        "Game identifier (FK to games)"),
    _f("season",           pa.int32(),        "Season year"),
    _f("week",             pa.int32(),        "Week number"),
    _f("tv_network",       pa.string(),       "Broadcasting TV network"),
    _f("start_time",       pa.timestamp("s"), "Scheduled start time (UTC)"),
    _f("outlet",           pa.string(),       "Media outlet"),
    _f("is_home_blackout", pa.bool_(),        "Whether home market is blacked out"),
    _f("source",           pa.string(),       "Data vendor provenance"),
])


# ═══════════════════════════════════════════════════════════════════════
# Master registry — all schemas keyed by entity name
# ═══════════════════════════════════════════════════════════════════════

NCAAF_SCHEMAS: dict[str, pa.Schema] = {
    # BDL-mirrored entities (1-12)
    "conferences":          schema_conferences,
    "teams":                schema_teams,
    "players":              schema_players,
    "games":                schema_games,
    "plays":                schema_plays,
    "player_stats":         schema_player_stats,
    "team_stats":           schema_team_stats,
    "player_season_stats":  schema_player_season_stats,
    "team_season_stats":    schema_team_season_stats,
    "standings":            schema_standings,
    "rankings":             schema_rankings,
    "odds":                 schema_odds,
    # WNBP-exclusive entities (13-18)
    "coaches":              schema_coaches,
    "weather":              schema_weather,
    "injuries":             schema_injuries,
    "recruiting_classes":   schema_recruiting_classes,
    "recruiting_players":   schema_recruiting_players,
    "recruiting_groups":    schema_recruiting_groups,
    # Ratings sub-entities (19)
    "ratings/elo":            schema_ratings_elo,
    "ratings/sp":             schema_ratings_sp,
    "ratings/fpi":            schema_ratings_fpi,
    "ratings/srs":            schema_ratings_srs,
    "ratings/talent":         schema_ratings_talent,
    "ratings/sp_conference":  schema_ratings_sp_conference,
    # Advanced sub-entities (20)
    "advanced/epa":              schema_advanced_epa,
    "advanced/ppa":              schema_advanced_ppa,
    "advanced/havoc":            schema_advanced_havoc,
    "advanced/win_probability":  schema_advanced_win_probability,
    # Remaining entities (21-26)
    "drives":               schema_drives,
    "draft":                schema_draft,
    "portal":               schema_portal,
    "returning_production": schema_returning_production,
    "venues":               schema_venues,
    "media":                schema_media,
}

# ═══════════════════════════════════════════════════════════════════════
# Partition keys per entity
#   - Empty list  → no partitioning (static reference data)
#   - ["season"]  → season-only partitioning
#   - ["season", "week"] → season + week partitioning
# ═══════════════════════════════════════════════════════════════════════

PARTITION_KEYS: dict[str, list[str]] = {
    # No partitioning (static reference)
    "conferences":          [],
    "venues":               [],
    # Season-only
    "teams":                ["season"],
    "players":              ["season"],
    "standings":            ["season"],
    "player_season_stats":  ["season"],
    "team_season_stats":    ["season"],
    "coaches":              ["season"],
    "recruiting_classes":   ["season"],
    "recruiting_players":   ["season"],
    "recruiting_groups":    ["season"],
    "ratings/elo":          ["season"],
    "ratings/sp":           ["season"],
    "ratings/fpi":          ["season"],
    "ratings/srs":          ["season"],
    "ratings/talent":       ["season"],
    "ratings/sp_conference":["season"],
    "draft":                ["season"],
    "portal":               ["season"],
    "returning_production": ["season"],
    # Season + week
    "games":                        ["season", "week"],
    "plays":                        ["season", "week"],
    "player_stats":                 ["season", "week"],
    "team_stats":                   ["season", "week"],
    "rankings":                     ["season", "week"],
    "odds":                         ["season", "week"],
    "weather":                      ["season", "week"],
    "injuries":                     ["season", "week"],
    "advanced/epa":                 ["season", "week"],
    "advanced/ppa":                 ["season", "week"],
    "advanced/havoc":               ["season", "week"],
    "advanced/win_probability":     ["season", "week"],
    "drives":                       ["season", "week"],
    "media":                        ["season", "week"],
}

# ═══════════════════════════════════════════════════════════════════════
# Entity-to-path mapping (relative to data/normalized_curated/ncaaf/)
# ═══════════════════════════════════════════════════════════════════════

NCAAF_ENTITY_PATHS: dict[str, str] = {
    "conferences":          "conferences",
    "teams":                "teams",
    "players":              "players",
    "games":                "games",
    "plays":                "plays",
    "player_stats":         "player_stats",
    "team_stats":           "team_stats",
    "player_season_stats":  "player_season_stats",
    "team_season_stats":    "team_season_stats",
    "standings":            "standings",
    "rankings":             "rankings",
    "odds":                 "odds",
    "coaches":              "coaches",
    "weather":              "weather",
    "injuries":             "injuries",
    "recruiting_classes":   "recruiting_classes",
    "recruiting_players":   "recruiting_players",
    "recruiting_groups":    "recruiting_groups",
    "ratings/elo":          "ratings/elo",
    "ratings/sp":           "ratings/sp",
    "ratings/fpi":          "ratings/fpi",
    "ratings/srs":          "ratings/srs",
    "ratings/talent":       "ratings/talent",
    "ratings/sp_conference":"ratings/sp_conference",
    "advanced/epa":             "advanced/epa",
    "advanced/ppa":             "advanced/ppa",
    "advanced/havoc":           "advanced/havoc",
    "advanced/win_probability": "advanced/win_probability",
    "drives":               "drives",
    "draft":                "draft",
    "portal":               "portal",
    "returning_production": "returning_production",
    "venues":               "venues",
    "media":                "media",
}
