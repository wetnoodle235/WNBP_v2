# NCAAF Normalized-Curated Design Document

> **Version:** 3.0 — Consolidated 14-entity flat design  
> **Layer:** `data/normalized_curated/ncaaf/`  
> **Date:** 2025-07-15  
> **Status:** Active

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Folder Structure Diagram](#2-folder-structure-diagram)
3. [Entity Reference](#3-entity-reference)
4. [Discriminator Column Guide](#4-discriminator-column-guide)
5. [Partitioning Strategy](#5-partitioning-strategy)
6. [Vendor Priority Summary](#6-vendor-priority-summary)
7. [Migration Guide](#7-migration-guide)
8. [DuckDB Query Examples](#8-duckdb-query-examples)
9. [Comparison: Old vs New](#9-comparison-old-vs-new)

---

## 1. Executive Summary

### From 34 Deeply Nested Entities to 14 Flat Entities

The previous NCAAF normalized-curated layer organized data into **34+ entities** spread
across 6 major categories (`team/`, `player/`, `game/`, `betting/`, `analysis/`,
`recruiting/`) with up to 5+ levels of nesting (e.g.,
`team/identity/base/season=YYYY/part-*.parquet`). While logically sound, this structure
created significant friction:

- **Discovery was hard** — consumers needed a routing registry to find the right path.
- **Joins were painful** — relating a team's roster to its game stats required traversing
  multiple category trees.
- **Partitioning was inconsistent** — some entities used `season=`, others used
  `season=/week=`, and some had no partitioning at all despite being seasonal data.

### The BallDontLie-Inspired Redesign

This v3.0 design draws directly from the
[BallDontLie (BDL) NCAAF API](https://docs.balldontlie.io/) as its structural reference.
BDL organizes college football data into a small set of flat, top-level resources
(`teams`, `players`, `games`, `stats`, etc.) — each self-contained and queryable without
needing to understand a deep taxonomy.

We adopt the same philosophy: **14 flat, top-level entities** where every entity is a
single Hive-partitioned Parquet dataset at depth ≤ 3
(`entity/partition/part-*.parquet`).

### WNBP Extensions Beyond BDL

While BDL provides the structural skeleton, our pipeline (WNBP) extends it with data
sources and dimensions that BDL does not cover:

| Extension Area | Details |
|----------------|---------|
| **Weather** | Merged into `games` — game-time temperature, wind, precipitation, humidity |
| **Coaches** | Merged into `teams` — head coach name, first/last season, win/loss record |
| **Recruiting** | Standalone entity with class-level and positional group breakdowns |
| **6 Rating Systems** | Elo, SP+, FPI, SRS, talent composites, and SP+ conference ratings |
| **Advanced Metrics** | EPA, PPA, havoc rates, and win probability — all in one entity |
| **Odds Depth** | Pregame, live, historical, and player prop lines in a single entity |

### Design Principles

1. **Flat over nested** — no entity is more than one directory below the root.
2. **Discriminators over entities** — instead of `game_stats/` and `season_stats/` as
   separate folders, use a single `player_stats/` with a `scope` column.
3. **Merge related sources** — weather belongs with games, coaches belong with teams.
4. **Hive partitioning everywhere** — `season=YYYY` and optionally `week=WW` for
   DuckDB/Polars/Spark compatibility.
5. **Self-documenting paths** — `ncaaf/teams/season=2023/` needs no registry to interpret.

---

## 2. Folder Structure Diagram

```
ncaaf/
├── conferences/              ← static, no partitioning
│   └── part-*.parquet
│
├── teams/                    ← static, no partitioning
│   ├── base.parquet
│   ├── fbs.parquet
│   └── staff.parquet
│
├── players/                  ← season=YYYY
│   └── season=2023/
│       └── part-*.parquet
│
├── games/                    ← season=YYYY/week=WW
│   └── season=2023/
│       └── week=01/
│           └── part-*.parquet
│
├── plays/                    ← season=YYYY/week=WW
│   └── season=2023/
│       └── week=01/
│           └── part-*.parquet
│
├── player_stats/             ← season=YYYY/week=WW
│   └── season=2023/
│       └── week=01/
│           └── part-*.parquet
│
├── team_stats/               ← season=YYYY/week=WW
│   └── season=2023/
│       └── week=01/
│           └── part-*.parquet
│
├── standings/                ← season=YYYY
│   └── season=2023/
│       └── part-*.parquet
│
├── rankings/                 ← season=YYYY/week=WW
│   └── season=2023/
│       └── week=01/
│           └── part-*.parquet
│
├── odds/                     ← season=YYYY/week=WW
│   └── season=2023/
│       └── week=01/
│           └── part-*.parquet
│
├── ratings/                  ← season=YYYY
│   └── season=2023/
│       └── part-*.parquet
│
├── advanced/                 ← season=YYYY/week=WW
│   └── season=2023/
│       └── week=01/
│           └── part-*.parquet
│
├── recruiting/               ← season=YYYY
│   └── season=2023/
│       └── part-*.parquet
│
└── venues/                   ← static, no partitioning
    └── part-*.parquet
```

**Total: 14 top-level directories. Max path depth: 3 levels (entity → partition → file).**

---

## 3. Entity Reference

Below are the detailed field tables for all 14 entities. Columns:

- **Field** — column name in the Parquet file
- **Type** — Arrow/Parquet logical type
- **Nullable** — whether the field can be null
- **Description** — what the field represents

### 3.1 conferences (4 fields)

Small, static reference table. No partitioning.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `conference_id` | `Int64` | No | Unique conference identifier |
| `name` | `Utf8` | No | Full conference name (e.g., "Southeastern Conference") |
| `short_name` | `Utf8` | Yes | Common abbreviation (e.g., "SEC") |
| `classification` | `Utf8` | Yes | Division classification (e.g., "fbs", "fcs") |

### 3.2 teams (18 fields)

**Merges:** teams + coaches + roster metadata.  
**Partition:** `season=YYYY`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `team_id` | `Int64` | No | Unique team identifier |
| `school` | `Utf8` | No | School name (e.g., "Alabama") |
| `mascot` | `Utf8` | Yes | Team mascot (e.g., "Crimson Tide") |
| `abbreviation` | `Utf8` | Yes | Short code (e.g., "ALA") |
| `conference_id` | `Int64` | Yes | FK to `conferences.conference_id` |
| `conference_name` | `Utf8` | Yes | Denormalized conference name |
| `division` | `Utf8` | Yes | Conference division (e.g., "East", "West") |
| `classification` | `Utf8` | Yes | "fbs" or "fcs" |
| `color` | `Utf8` | Yes | Primary team color (hex) |
| `alt_color` | `Utf8` | Yes | Secondary team color (hex) |
| `logo_url` | `Utf8` | Yes | URL to team logo |
| `venue_id` | `Int64` | Yes | FK to `venues.venue_id` |
| `latitude` | `Float64` | Yes | School location latitude |
| `longitude` | `Float64` | Yes | School location longitude |
| `head_coach` | `Utf8` | Yes | Current head coach name |
| `coach_first_season` | `Int64` | Yes | Year the coach started at this school |
| `coach_last_season` | `Int64` | Yes | Year the coach left (null if current) |
| `roster_count` | `Int64` | Yes | Number of players on the roster |

### 3.3 players (32 fields)

**Merges:** players + transfer portal + returning production + draft picks + recruits.  
**Partition:** `season=YYYY`  
**Discriminator:** `status`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `player_id` | `Int64` | No | Unique player identifier |
| `first_name` | `Utf8` | Yes | Player first name |
| `last_name` | `Utf8` | Yes | Player last name |
| `full_name` | `Utf8` | No | Full display name |
| `team_id` | `Int64` | Yes | FK to `teams.team_id` |
| `team_name` | `Utf8` | Yes | Denormalized school name |
| `conference` | `Utf8` | Yes | Denormalized conference name |
| `position` | `Utf8` | Yes | Position abbreviation (e.g., "QB", "WR") |
| `jersey` | `Int64` | Yes | Jersey number |
| `height` | `Float64` | Yes | Height in inches |
| `weight` | `Float64` | Yes | Weight in pounds |
| `year` | `Utf8` | Yes | Academic year (e.g., "Senior", "Junior") |
| `status` | `Utf8` | No | **Discriminator:** "active" \| "transfer" \| "returning" \| "drafted" \| "recruit" |
| `hometown` | `Utf8` | Yes | Hometown |
| `home_state` | `Utf8` | Yes | Home state abbreviation |
| `home_country` | `Utf8` | Yes | Home country |
| `stars` | `Int64` | Yes | Recruiting star rating (1–5) |
| `rating` | `Float64` | Yes | Composite recruiting rating (0.0–1.0) |
| `recruit_rank_national` | `Int64` | Yes | National recruit ranking |
| `recruit_rank_position` | `Int64` | Yes | Position recruit ranking |
| `recruit_rank_state` | `Int64` | Yes | State recruit ranking |
| `committed_to` | `Utf8` | Yes | School committed to (recruits/transfers) |
| `transfer_origin` | `Utf8` | Yes | School transferred from |
| `transfer_destination` | `Utf8` | Yes | School transferred to |
| `transfer_eligibility` | `Utf8` | Yes | Eligibility status post-transfer |
| `returning_ppa` | `Float64` | Yes | Returning predicted points added |
| `returning_usage` | `Float64` | Yes | Returning usage rate |
| `returning_pass_ppa` | `Float64` | Yes | Returning passing PPA |
| `returning_rush_ppa` | `Float64` | Yes | Returning rushing PPA |
| `draft_round` | `Int64` | Yes | NFL draft round |
| `draft_pick` | `Int64` | Yes | NFL draft overall pick |
| `draft_nfl_team` | `Utf8` | Yes | NFL team that drafted the player |

### 3.4 games (36 fields)

**Merges:** games + weather + media/broadcast.  
**Partition:** `season=YYYY/week=WW`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `game_id` | `Int64` | No | Unique game identifier |
| `season_type` | `Utf8` | Yes | "regular", "postseason", or "both" |
| `week` | `Int64` | No | Week number within the season |
| `start_date` | `Utf8` | Yes | Game start datetime (ISO 8601) |
| `start_time_tbd` | `Boolean` | Yes | Whether start time is TBD |
| `neutral_site` | `Boolean` | Yes | Whether the game is at a neutral site |
| `conference_game` | `Boolean` | Yes | Whether this is a conference game |
| `attendance` | `Int64` | Yes | Reported attendance |
| `venue_id` | `Int64` | Yes | FK to `venues.venue_id` |
| `venue_name` | `Utf8` | Yes | Denormalized venue name |
| `home_id` | `Int64` | Yes | FK to `teams.team_id` for home team |
| `home_team` | `Utf8` | Yes | Home team school name |
| `home_conference` | `Utf8` | Yes | Home team conference |
| `home_points` | `Int64` | Yes | Home team final score |
| `home_line_scores` | `Utf8` | Yes | Home team scores by quarter (JSON array) |
| `home_pregame_elo` | `Float64` | Yes | Home team pregame Elo rating |
| `home_postgame_elo` | `Float64` | Yes | Home team postgame Elo rating |
| `away_id` | `Int64` | Yes | FK to `teams.team_id` for away team |
| `away_team` | `Utf8` | Yes | Away team school name |
| `away_conference` | `Utf8` | Yes | Away team conference |
| `away_points` | `Int64` | Yes | Away team final score |
| `away_line_scores` | `Utf8` | Yes | Away team scores by quarter (JSON array) |
| `away_pregame_elo` | `Float64` | Yes | Away team pregame Elo rating |
| `away_postgame_elo` | `Float64` | Yes | Away team postgame Elo rating |
| `excitement_index` | `Float64` | Yes | Game excitement index |
| `highlights` | `Utf8` | Yes | URL to game highlights |
| `notes` | `Utf8` | Yes | Game notes (e.g., bowl name) |
| `weather_temperature` | `Float64` | Yes | Temperature at kickoff (°F) |
| `weather_wind_speed` | `Float64` | Yes | Wind speed at kickoff (mph) |
| `weather_wind_direction` | `Utf8` | Yes | Wind direction |
| `weather_precipitation` | `Float64` | Yes | Precipitation probability (0–1) |
| `weather_humidity` | `Float64` | Yes | Relative humidity (0–1) |
| `weather_condition` | `Utf8` | Yes | Weather description (e.g., "Clear", "Rain") |
| `media_network` | `Utf8` | Yes | Broadcast network (e.g., "ESPN") |
| `media_outlet` | `Utf8` | Yes | Media outlet name |
| `media_type` | `Utf8` | Yes | Broadcast type ("tv", "web", "radio") |

### 3.5 plays (23 fields)

**Merges:** plays + drives.  
**Partition:** `season=YYYY/week=WW`  
**Discriminator:** `play_type`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `play_id` | `Int64` | No | Unique play/drive identifier |
| `game_id` | `Int64` | No | FK to `games.game_id` |
| `drive_id` | `Int64` | Yes | Drive identifier (null for drive-level rows) |
| `play_type` | `Utf8` | No | **Discriminator:** "play" \| "drive" |
| `offense` | `Utf8` | Yes | Offensive team name |
| `offense_id` | `Int64` | Yes | FK to `teams.team_id` (offense) |
| `defense` | `Utf8` | Yes | Defensive team name |
| `defense_id` | `Int64` | Yes | FK to `teams.team_id` (defense) |
| `period` | `Int64` | Yes | Game period (quarter) |
| `clock_minutes` | `Int64` | Yes | Minutes remaining on game clock |
| `clock_seconds` | `Int64` | Yes | Seconds remaining on game clock |
| `yard_line` | `Int64` | Yes | Yard line position (1–99) |
| `down` | `Int64` | Yes | Current down (1–4, null for drives) |
| `distance` | `Int64` | Yes | Yards to go for first down |
| `yards_gained` | `Int64` | Yes | Yards gained on the play |
| `play_text` | `Utf8` | Yes | Human-readable play description |
| `play_category` | `Utf8` | Yes | Category (e.g., "Pass", "Rush", "Penalty") |
| `scoring` | `Boolean` | Yes | Whether the play resulted in a score |
| `ppa` | `Float64` | Yes | Predicted points added for the play |
| `drive_start_yards` | `Int64` | Yes | Drive starting yard line |
| `drive_end_yards` | `Int64` | Yes | Drive ending yard line |
| `drive_result` | `Utf8` | Yes | Drive result (e.g., "TD", "FG", "Punt") |
| `drive_elapsed_seconds` | `Float64` | Yes | Drive duration in seconds |

### 3.6 player_stats (46 fields)

**Merges:** game-level stats + season-level stats + usage + PPA.  
**Partition:** `season=YYYY/week=WW`  
**Discriminator:** `scope`

> **Note on week partition for season-level rows:** Season-aggregate rows use `week=00`
> as a sentinel value, keeping the partition scheme uniform.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `player_id` | `Int64` | No | FK to `players.player_id` |
| `player_name` | `Utf8` | Yes | Denormalized player name |
| `team_id` | `Int64` | Yes | FK to `teams.team_id` |
| `team` | `Utf8` | Yes | Denormalized team name |
| `conference` | `Utf8` | Yes | Denormalized conference name |
| `game_id` | `Int64` | Yes | FK to `games.game_id` (null for season scope) |
| `opponent` | `Utf8` | Yes | Opponent team name (null for season scope) |
| `scope` | `Utf8` | No | **Discriminator:** "game" \| "season" |
| `position` | `Utf8` | Yes | Player position |
| `pass_completions` | `Int64` | Yes | Passing completions |
| `pass_attempts` | `Int64` | Yes | Passing attempts |
| `pass_yards` | `Float64` | Yes | Passing yards |
| `pass_td` | `Int64` | Yes | Passing touchdowns |
| `pass_int` | `Int64` | Yes | Interceptions thrown |
| `pass_ppa` | `Float64` | Yes | Passing predicted points added |
| `rush_attempts` | `Int64` | Yes | Rushing attempts |
| `rush_yards` | `Float64` | Yes | Rushing yards |
| `rush_td` | `Int64` | Yes | Rushing touchdowns |
| `rush_ppa` | `Float64` | Yes | Rushing predicted points added |
| `rec_receptions` | `Int64` | Yes | Receptions |
| `rec_yards` | `Float64` | Yes | Receiving yards |
| `rec_td` | `Int64` | Yes | Receiving touchdowns |
| `rec_ppa` | `Float64` | Yes | Receiving predicted points added |
| `fum_lost` | `Int64` | Yes | Fumbles lost |
| `fum_recovered` | `Int64` | Yes | Fumbles recovered |
| `kick_fg_made` | `Int64` | Yes | Field goals made |
| `kick_fg_att` | `Int64` | Yes | Field goal attempts |
| `kick_xp_made` | `Int64` | Yes | Extra points made |
| `kick_xp_att` | `Int64` | Yes | Extra point attempts |
| `kick_points` | `Float64` | Yes | Total kicking points |
| `punt_count` | `Int64` | Yes | Number of punts |
| `punt_yards` | `Float64` | Yes | Total punt yards |
| `punt_avg` | `Float64` | Yes | Average punt distance |
| `kick_return_yards` | `Float64` | Yes | Kick return yards |
| `kick_return_td` | `Int64` | Yes | Kick return touchdowns |
| `punt_return_yards` | `Float64` | Yes | Punt return yards |
| `punt_return_td` | `Int64` | Yes | Punt return touchdowns |
| `def_tackles` | `Float64` | Yes | Total tackles |
| `def_solo_tackles` | `Float64` | Yes | Solo tackles |
| `def_sacks` | `Float64` | Yes | Sacks |
| `def_int` | `Int64` | Yes | Defensive interceptions |
| `def_pass_deflections` | `Int64` | Yes | Passes deflected |
| `usage_overall` | `Float64` | Yes | Overall usage rate (0–1) |
| `usage_pass` | `Float64` | Yes | Pass usage rate (0–1) |
| `usage_rush` | `Float64` | Yes | Rush usage rate (0–1) |
| `total_ppa` | `Float64` | Yes | Total PPA (all phases combined) |

### 3.7 team_stats (27 fields)

**Merges:** game-level team stats + season-level team stats.  
**Partition:** `season=YYYY/week=WW`  
**Discriminator:** `scope`

> Season-aggregate rows use `week=00` as the sentinel partition value.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `team_id` | `Int64` | No | FK to `teams.team_id` |
| `team` | `Utf8` | Yes | Denormalized team name |
| `conference` | `Utf8` | Yes | Denormalized conference name |
| `game_id` | `Int64` | Yes | FK to `games.game_id` (null for season scope) |
| `opponent` | `Utf8` | Yes | Opponent name (null for season scope) |
| `scope` | `Utf8` | No | **Discriminator:** "game" \| "season" |
| `points` | `Int64` | Yes | Total points scored |
| `total_yards` | `Float64` | Yes | Total offensive yards |
| `net_pass_yards` | `Float64` | Yes | Net passing yards |
| `pass_attempts` | `Int64` | Yes | Passing attempts |
| `pass_completions` | `Int64` | Yes | Passing completions |
| `pass_td` | `Int64` | Yes | Passing touchdowns |
| `rush_yards` | `Float64` | Yes | Rushing yards |
| `rush_attempts` | `Int64` | Yes | Rushing attempts |
| `rush_td` | `Int64` | Yes | Rushing touchdowns |
| `first_downs` | `Int64` | Yes | Total first downs |
| `third_down_conv` | `Int64` | Yes | Third-down conversions |
| `third_down_att` | `Int64` | Yes | Third-down attempts |
| `fourth_down_conv` | `Int64` | Yes | Fourth-down conversions |
| `fourth_down_att` | `Int64` | Yes | Fourth-down attempts |
| `turnovers` | `Int64` | Yes | Total turnovers |
| `penalties` | `Int64` | Yes | Number of penalties |
| `penalty_yards` | `Int64` | Yes | Total penalty yards |
| `possession_seconds` | `Float64` | Yes | Time of possession in seconds |
| `interceptions` | `Int64` | Yes | Interceptions thrown |
| `fumbles_lost` | `Int64` | Yes | Fumbles lost |
| `sacks` | `Int64` | Yes | Times sacked |

### 3.8 standings (19 fields)

**Merges:** standings + records (win/loss, against-the-spread).  
**Partition:** `season=YYYY`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `team_id` | `Int64` | No | FK to `teams.team_id` |
| `team` | `Utf8` | No | School name |
| `conference` | `Utf8` | Yes | Conference name |
| `division` | `Utf8` | Yes | Conference division |
| `total_wins` | `Int64` | Yes | Total wins |
| `total_losses` | `Int64` | Yes | Total losses |
| `conference_wins` | `Int64` | Yes | Conference wins |
| `conference_losses` | `Int64` | Yes | Conference losses |
| `home_wins` | `Int64` | Yes | Home wins |
| `home_losses` | `Int64` | Yes | Home losses |
| `away_wins` | `Int64` | Yes | Away wins |
| `away_losses` | `Int64` | Yes | Away losses |
| `streak` | `Int64` | Yes | Current win/loss streak (positive = W) |
| `ats_wins` | `Int64` | Yes | Wins against the spread |
| `ats_losses` | `Int64` | Yes | Losses against the spread |
| `ats_pushes` | `Int64` | Yes | Pushes against the spread |
| `over_wins` | `Int64` | Yes | Over total wins |
| `over_losses` | `Int64` | Yes | Under total wins |
| `expected_wins` | `Float64` | Yes | Expected wins (Pythagorean or similar) |

### 3.9 rankings (11 fields)

**Partition:** `season=YYYY/week=WW`  
**Discriminator:** `poll`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `team_id` | `Int64` | Yes | FK to `teams.team_id` |
| `team` | `Utf8` | No | School name |
| `conference` | `Utf8` | Yes | Conference name |
| `poll` | `Utf8` | No | **Discriminator:** poll name (e.g., "AP Top 25", "Coaches Poll", "Playoff Committee") |
| `rank` | `Int64` | No | Rank within the poll |
| `first_place_votes` | `Int64` | Yes | Number of first-place votes received |
| `points` | `Int64` | Yes | Total poll points |
| `prev_rank` | `Int64` | Yes | Previous week's rank (null if unranked) |
| `rank_change` | `Int64` | Yes | Change in rank from previous week |
| `record_wins` | `Int64` | Yes | Team wins at this point in the season |
| `record_losses` | `Int64` | Yes | Team losses at this point in the season |

### 3.10 odds (23 fields)

**Merges:** pregame odds + live odds + line history + player props.  
**Partition:** `season=YYYY/week=WW`  
**Discriminator:** `line_type`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `game_id` | `Int64` | No | FK to `games.game_id` |
| `line_type` | `Utf8` | No | **Discriminator:** "pregame" \| "live" \| "history" \| "player_prop" |
| `provider` | `Utf8` | Yes | Sportsbook name (e.g., "DraftKings", "consensus") |
| `home_team` | `Utf8` | Yes | Home team name |
| `away_team` | `Utf8` | Yes | Away team name |
| `spread` | `Float64` | Yes | Point spread (negative = home favored) |
| `spread_open` | `Float64` | Yes | Opening spread |
| `home_spread_odds` | `Int64` | Yes | Moneyline odds for home spread |
| `away_spread_odds` | `Int64` | Yes | Moneyline odds for away spread |
| `over_under` | `Float64` | Yes | Total points over/under line |
| `over_under_open` | `Float64` | Yes | Opening over/under |
| `over_odds` | `Int64` | Yes | Moneyline odds for over |
| `under_odds` | `Int64` | Yes | Moneyline odds for under |
| `home_moneyline` | `Int64` | Yes | Home team moneyline |
| `away_moneyline` | `Int64` | Yes | Away team moneyline |
| `home_win_prob` | `Float64` | Yes | Implied home win probability (0–1) |
| `away_win_prob` | `Float64` | Yes | Implied away win probability (0–1) |
| `timestamp` | `Utf8` | Yes | ISO 8601 timestamp (for live/history lines) |
| `player_id` | `Int64` | Yes | FK to `players.player_id` (player props only) |
| `player_name` | `Utf8` | Yes | Player name (player props only) |
| `prop_type` | `Utf8` | Yes | Prop type (e.g., "passing_yards", "rushing_td") |
| `prop_line` | `Float64` | Yes | Prop line value |
| `prop_over_odds` | `Int64` | Yes | Moneyline odds for prop over |

### 3.11 ratings (14 fields)

**Merges:** Elo + SP+ + FPI + SRS + talent composites + SP+ conference ratings.  
**Partition:** `season=YYYY`  
**Discriminator:** `rating_type`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `team_id` | `Int64` | Yes | FK to `teams.team_id` (null for conference ratings) |
| `team` | `Utf8` | Yes | School name (null for conference ratings) |
| `conference` | `Utf8` | Yes | Conference name |
| `rating_type` | `Utf8` | No | **Discriminator:** "elo" \| "sp" \| "fpi" \| "srs" \| "talent" \| "sp_conference" |
| `overall` | `Float64` | Yes | Overall composite rating |
| `offense` | `Float64` | Yes | Offensive rating component |
| `defense` | `Float64` | Yes | Defensive rating component |
| `special_teams` | `Float64` | Yes | Special teams rating (SP+ only) |
| `strength_of_schedule` | `Float64` | Yes | Strength of schedule component |
| `second_order_wins` | `Float64` | Yes | Second-order win total (SP+ only) |
| `rank_overall` | `Int64` | Yes | Rank by overall rating |
| `rank_offense` | `Int64` | Yes | Rank by offensive rating |
| `rank_defense` | `Int64` | Yes | Rank by defensive rating |
| `talent_composite` | `Float64` | Yes | Talent composite score (talent type only) |

### 3.12 advanced (17 fields)

**Merges:** EPA + PPA + havoc rates + win probability.  
**Partition:** `season=YYYY/week=WW`  
**Discriminator:** `metric_type`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `game_id` | `Int64` | Yes | FK to `games.game_id` |
| `team_id` | `Int64` | Yes | FK to `teams.team_id` |
| `team` | `Utf8` | Yes | Denormalized team name |
| `opponent` | `Utf8` | Yes | Opponent team name |
| `conference` | `Utf8` | Yes | Denormalized conference name |
| `metric_type` | `Utf8` | No | **Discriminator:** "epa" \| "ppa" \| "havoc" \| "win_probability" |
| `overall` | `Float64` | Yes | Overall metric value |
| `passing` | `Float64` | Yes | Passing component |
| `rushing` | `Float64` | Yes | Rushing component |
| `first_down` | `Float64` | Yes | First-down rate or value |
| `second_down` | `Float64` | Yes | Second-down rate or value |
| `third_down` | `Float64` | Yes | Third-down rate or value |
| `explosiveness` | `Float64` | Yes | Explosiveness component |
| `success_rate` | `Float64` | Yes | Play success rate |
| `havoc_total` | `Float64` | Yes | Total havoc rate (havoc type) |
| `havoc_front_seven` | `Float64` | Yes | Front-seven havoc rate |
| `havoc_db` | `Float64` | Yes | Defensive back havoc rate |

### 3.13 recruiting (24 fields)

**Merges:** recruiting classes + positional group breakdowns.  
**Partition:** `season=YYYY`  
**Discriminator:** `scope`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `team_id` | `Int64` | Yes | FK to `teams.team_id` |
| `team` | `Utf8` | Yes | School name |
| `conference` | `Utf8` | Yes | Conference name |
| `scope` | `Utf8` | No | **Discriminator:** "class" \| "group" |
| `position_group` | `Utf8` | Yes | Position group name (null for class-level) |
| `year` | `Int64` | Yes | Recruiting class year |
| `rank_national` | `Int64` | Yes | National class ranking |
| `rank_conference` | `Int64` | Yes | Conference class ranking |
| `total_commits` | `Int64` | Yes | Total commits in the class |
| `five_stars` | `Int64` | Yes | Number of 5-star recruits |
| `four_stars` | `Int64` | Yes | Number of 4-star recruits |
| `three_stars` | `Int64` | Yes | Number of 3-star recruits |
| `total_points` | `Float64` | Yes | Total recruiting points (247 composite) |
| `avg_rating` | `Float64` | Yes | Average player rating |
| `avg_stars` | `Float64` | Yes | Average star rating |
| `top_recruit_name` | `Utf8` | Yes | Name of top-rated recruit |
| `top_recruit_position` | `Utf8` | Yes | Position of top-rated recruit |
| `top_recruit_rating` | `Float64` | Yes | Rating of top-rated recruit |
| `top_recruit_stars` | `Int64` | Yes | Star rating of top-rated recruit |
| `top_recruit_state` | `Utf8` | Yes | Home state of top-rated recruit |
| `early_enrollees` | `Int64` | Yes | Number of early enrollees |
| `juco_commits` | `Int64` | Yes | Number of JUCO commits |
| `transfer_commits` | `Int64` | Yes | Number of transfer commits |
| `avg_distance` | `Float64` | Yes | Average recruiting distance (miles) |

### 3.14 venues (12 fields)

Small, static reference table. No partitioning.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `venue_id` | `Int64` | No | Unique venue identifier |
| `name` | `Utf8` | No | Venue name (e.g., "Bryant-Denny Stadium") |
| `city` | `Utf8` | Yes | City |
| `state` | `Utf8` | Yes | State abbreviation |
| `zip` | `Utf8` | Yes | ZIP code |
| `country` | `Utf8` | Yes | Country code |
| `capacity` | `Int64` | Yes | Seating capacity |
| `year_constructed` | `Int64` | Yes | Year the venue was built |
| `surface` | `Utf8` | Yes | Playing surface (e.g., "Grass", "FieldTurf") |
| `dome` | `Boolean` | Yes | Whether the venue is a dome |
| `latitude` | `Float64` | Yes | Venue latitude |
| `longitude` | `Float64` | Yes | Venue longitude |

---

## 4. Discriminator Column Guide

Six of the 14 entities use a **discriminator column** — a special field that identifies
which logical sub-type a row belongs to. This eliminates the need for separate
entities/folders for each variant.

### How It Works

Instead of:
```
ncaaf/game_player_stats/season=2023/week=05/...
ncaaf/season_player_stats/season=2023/...
```

We have:
```
ncaaf/player_stats/season=2023/week=05/...   ← scope="game"
ncaaf/player_stats/season=2023/week=00/...   ← scope="season"
```

Filter on the discriminator to get exactly the rows you need.

### Discriminator Reference

| Entity | Column | Values | Meaning |
|--------|--------|--------|---------|
| **players** | `status` | `"active"` | Current roster player |
| | | `"transfer"` | Player in the transfer portal |
| | | `"returning"` | Returning player with production data |
| | | `"drafted"` | Player drafted to the NFL |
| | | `"recruit"` | Incoming recruit |
| **plays** | `play_type` | `"play"` | Individual play record |
| | | `"drive"` | Drive-level summary record |
| **player_stats** | `scope` | `"game"` | Per-game statistics for a player |
| | | `"season"` | Full-season aggregate statistics |
| **team_stats** | `scope` | `"game"` | Per-game team statistics |
| | | `"season"` | Full-season aggregate team statistics |
| **odds** | `line_type` | `"pregame"` | Pre-game betting lines |
| | | `"live"` | In-game live betting lines |
| | | `"history"` | Historical line movement |
| | | `"player_prop"` | Player proposition bets |
| **ratings** | `rating_type` | `"elo"` | Elo power ratings |
| | | `"sp"` | SP+ advanced ratings |
| | | `"fpi"` | ESPN Football Power Index |
| | | `"srs"` | Simple Rating System |
| | | `"talent"` | Talent composite scores |
| | | `"sp_conference"` | SP+ conference-level ratings |
| **advanced** | `metric_type` | `"epa"` | Expected Points Added |
| | | `"ppa"` | Predicted Points Added |
| | | `"havoc"` | Havoc rates (disruption metrics) |
| | | `"win_probability"` | Win probability metrics |
| **recruiting** | `scope` | `"class"` | Full recruiting class summary |
| | | `"group"` | Positional group breakdown |

### Querying by Discriminator

```sql
-- Get only game-level player stats
SELECT * FROM player_stats WHERE scope = 'game';

-- Get only SP+ ratings
SELECT * FROM ratings WHERE rating_type = 'sp';

-- Get only pregame odds
SELECT * FROM odds WHERE line_type = 'pregame';
```

---

## 5. Partitioning Strategy

All entities use Hive-style partitioning (`key=value/` directories), which is natively
understood by DuckDB, Polars, Spark, and PyArrow.

### Three Tiers

| Tier | Partitioning | Entities | Rationale |
|------|-------------|----------|-----------|
| **Static** | None | `conferences`, `teams`, `venues` | Small reference tables (< 10 MB). Full scan is trivial. Data changes rarely (team changes, venue renames, conference realignment). Stored as flat parquets with no season= partitioning. |
| **Season-only** | `season=YYYY` | `players`, `standings`, `ratings`, `recruiting` | Data is assembled once per season. Within a season, it may be updated but not subdivided by week. Season pruning eliminates ~95% of data for single-season queries. |
| **Season + Week** | `season=YYYY/week=WW` | `games`, `plays`, `player_stats`, `team_stats`, `rankings`, `odds`, `advanced` | Data changes weekly during the season. Week partitioning enables efficient incremental writes and targeted queries (e.g., "show me Week 5 stats"). |

### Partition Key Conventions

- **season** — 4-digit year (e.g., `season=2023`). Always represents the fall semester
  season.
- **week** — 2-digit zero-padded week number (e.g., `week=01`, `week=14`).
  - `week=00` is reserved for season-aggregate rows in `player_stats` and `team_stats`.
  - Postseason weeks continue from the regular season numbering.

### File Naming

Files within each partition follow the pattern `part-*.parquet` (e.g.,
`part-0000.parquet`). A partition with multiple files indicates either multi-provider
writes or row-count splits.

---

## 6. Vendor Priority Summary

Each entity has a primary data provider and, where available, a secondary/fallback source.

| Entity | Primary Provider | Secondary Provider | Notes |
|--------|----------------|--------------------|-------|
| `conferences` | CFBD API | ESPN | Rarely changes |
| `teams` | CFBD API | ESPN / BDL | Coaches from CFBD coaching endpoint |
| `players` | CFBD API | ESPN | Portal + returning + draft merged from CFBD |
| `games` | CFBD API | ESPN / BDL | Weather from CFBD weather endpoint |
| `plays` | CFBD API | — | Drive data from CFBD drives endpoint |
| `player_stats` | CFBD API | ESPN / BDL | Usage + PPA from CFBD |
| `team_stats` | CFBD API | ESPN / BDL | — |
| `standings` | CFBD API | ESPN | ATS records from CFBD records endpoint |
| `rankings` | CFBD API | AP / ESPN | Multiple polls in one entity |
| `odds` | CFBD API | DraftKings / FanDuel | Live + history from CFBD lines endpoint |
| `ratings` | CFBD API | ESPN (FPI) | 6 systems from CFBD ratings endpoints |
| `advanced` | CFBD API | — | EPA/PPA/havoc from CFBD advanced endpoints |
| `recruiting` | CFBD API | 247Sports | Class + group from CFBD recruiting |
| `venues` | CFBD API | ESPN | Rarely changes |

---

## 7. Migration Guide

This table maps every old nested path to its new flat entity. The old design used
categories like `team/`, `player/`, `game/`, `betting/`, `analysis/`, and `recruiting/`
as top-level groupings.

### Old → New Entity Mapping

| # | Old Path | New Entity | Discriminator Value |
|---|----------|------------|---------------------|
| 1 | `team/identity/base/` | `teams` | — |
| 2 | `team/identity/roster/` | `teams` (roster_count) + `players` | status="active" |
| 3 | `team/identity/coaches/` | `teams` | — (head_coach fields) |
| 4 | `team/identity/venues/` | `venues` | — |
| 5 | `team/identity/conferences/` | `conferences` | — |
| 6 | `team/stats/game_stats/` | `team_stats` | scope="game" |
| 7 | `team/stats/season_stats/` | `team_stats` | scope="season" |
| 8 | `team/stats/advanced_stats/` | `advanced` | metric_type varies |
| 9 | `team/stats/sp_ratings/` | `ratings` | rating_type="sp" |
| 10 | `team/rankings/ap_poll/` | `rankings` | poll="AP Top 25" |
| 11 | `team/rankings/coaches_poll/` | `rankings` | poll="Coaches Poll" |
| 12 | `team/rankings/cfp_rankings/` | `rankings` | poll="Playoff Committee" |
| 13 | `team/rankings/sp_plus/` | `ratings` | rating_type="sp" |
| 14 | `team/rankings/elo/` | `ratings` | rating_type="elo" |
| 15 | `team/rankings/fpi/` | `ratings` | rating_type="fpi" |
| 16 | `team/rankings/srs/` | `ratings` | rating_type="srs" |
| 17 | `team/rankings/talent/` | `ratings` | rating_type="talent" |
| 18 | `team/standings/records/` | `standings` | — |
| 19 | `team/standings/ats_records/` | `standings` | — (ats_* fields) |
| 20 | `player/identity/base/` | `players` | status="active" |
| 21 | `player/identity/portal/` | `players` | status="transfer" |
| 22 | `player/identity/returning/` | `players` | status="returning" |
| 23 | `player/identity/draft/` | `players` | status="drafted" |
| 24 | `player/identity/recruits/` | `players` | status="recruit" |
| 25 | `player/stats/game_stats/` | `player_stats` | scope="game" |
| 26 | `player/stats/season_stats/` | `player_stats` | scope="season" |
| 27 | `player/stats/usage/` | `player_stats` | — (usage_* fields) |
| 28 | `player/stats/ppa/` | `player_stats` | — (ppa fields) |
| 29 | `game/schedule/base/` | `games` | — |
| 30 | `game/schedule/media/` | `games` | — (media_* fields) |
| 31 | `game/schedule/weather/` | `games` | — (weather_* fields) |
| 32 | `game/plays/plays/` | `plays` | play_type="play" |
| 33 | `game/plays/drives/` | `plays` | play_type="drive" |
| 34 | `game/plays/play_stats/` | `player_stats` | scope="game" |
| 35 | `game/advanced/epa/` | `advanced` | metric_type="epa" |
| 36 | `game/advanced/ppa/` | `advanced` | metric_type="ppa" |
| 37 | `game/advanced/havoc/` | `advanced` | metric_type="havoc" |
| 38 | `game/advanced/win_probability/` | `advanced` | metric_type="win_probability" |
| 39 | `betting/odds/pregame/` | `odds` | line_type="pregame" |
| 40 | `betting/odds/live/` | `odds` | line_type="live" |
| 41 | `betting/odds/line_history/` | `odds` | line_type="history" |
| 42 | `betting/odds/player_props/` | `odds` | line_type="player_prop" |
| 43 | `betting/records/ats/` | `standings` | — (ats_* fields) |
| 44 | `betting/records/over_under/` | `standings` | — (over_* fields) |
| 45 | `analysis/ratings/elo/` | `ratings` | rating_type="elo" |
| 46 | `analysis/ratings/sp/` | `ratings` | rating_type="sp" |
| 47 | `analysis/ratings/fpi/` | `ratings` | rating_type="fpi" |
| 48 | `analysis/ratings/srs/` | `ratings` | rating_type="srs" |
| 49 | `analysis/ratings/talent/` | `ratings` | rating_type="talent" |
| 50 | `analysis/ratings/sp_conference/` | `ratings` | rating_type="sp_conference" |
| 51 | `analysis/advanced/epa/` | `advanced` | metric_type="epa" |
| 52 | `analysis/advanced/ppa/` | `advanced` | metric_type="ppa" |
| 53 | `analysis/advanced/havoc/` | `advanced` | metric_type="havoc" |
| 54 | `analysis/advanced/win_probability/` | `advanced` | metric_type="win_probability" |
| 55 | `analysis/rankings/ap_poll/` | `rankings` | poll="AP Top 25" |
| 56 | `analysis/rankings/coaches_poll/` | `rankings` | poll="Coaches Poll" |
| 57 | `analysis/rankings/cfp_rankings/` | `rankings` | poll="Playoff Committee" |
| 58 | `recruiting/classes/base/` | `recruiting` | scope="class" |
| 59 | `recruiting/classes/groups/` | `recruiting` | scope="group" |
| 60 | `recruiting/players/` | `players` | status="recruit" |

### Migration Notes

- Rows 9–17 and 45–57 show that the old design had duplicate rating/ranking paths under
  both `team/` and `analysis/`. The new design collapses these into single `ratings` and
  `rankings` entities.
- Old `betting/records/` is absorbed into `standings` as additional columns (`ats_*`,
  `over_*`) rather than a separate entity.
- Old `player/stats/usage/` and `player/stats/ppa/` are merged as additional columns in
  `player_stats`, not as separate discriminator values — every stat row can carry usage
  and PPA data.

---

## 8. DuckDB Query Examples

All examples assume the data lives at `data/normalized_curated/ncaaf/`.

### 8.1 Read All Teams

```sql
SELECT *
FROM read_parquet('data/normalized_curated/ncaaf/teams/*.parquet');
```

### 8.2 Get Game-Level Player Stats for Week 5

```sql
SELECT player_name, team, position,
       pass_yards, rush_yards, rec_yards, total_ppa
FROM read_parquet('data/normalized_curated/ncaaf/player_stats/season=2023/week=05/*.parquet',
                  hive_partitioning=true)
WHERE scope = 'game'
ORDER BY total_ppa DESC
LIMIT 20;
```

### 8.3 Query Season-Level Player Stats Using Scope Discriminator

```sql
SELECT player_name, team, position,
       pass_yards, pass_td, rush_yards, rush_td,
       usage_overall, total_ppa
FROM read_parquet('data/normalized_curated/ncaaf/player_stats/season=2023/week=00/*.parquet',
                  hive_partitioning=true)
WHERE scope = 'season'
  AND position = 'QB'
ORDER BY total_ppa DESC;
```

### 8.4 Filter Odds by Line Type

```sql
-- Pregame spreads for Week 10
SELECT game_id, home_team, away_team, provider,
       spread, over_under, home_moneyline, away_moneyline
FROM read_parquet('data/normalized_curated/ncaaf/odds/season=2023/week=10/*.parquet',
                  hive_partitioning=true)
WHERE line_type = 'pregame';
```

### 8.5 Get All SP+ Ratings

```sql
SELECT team, overall, offense, defense, special_teams,
       rank_overall, rank_offense, rank_defense
FROM read_parquet('data/normalized_curated/ncaaf/ratings/season=2023/*.parquet',
                  hive_partitioning=true)
WHERE rating_type = 'sp'
ORDER BY overall DESC;
```

### 8.6 Join Games with Advanced EPA Metrics

```sql
SELECT g.home_team, g.away_team, g.home_points, g.away_points,
       a.overall AS epa_overall, a.passing AS epa_passing, a.rushing AS epa_rushing
FROM read_parquet('data/normalized_curated/ncaaf/games/season=2023/week=05/*.parquet',
                  hive_partitioning=true) g
JOIN read_parquet('data/normalized_curated/ncaaf/advanced/season=2023/week=05/*.parquet',
                  hive_partitioning=true) a
  ON g.game_id = a.game_id
WHERE a.metric_type = 'epa';
```

### 8.7 Get Recruiting Classes Ranked by Points

```sql
SELECT team, conference, rank_national, total_commits,
       five_stars, four_stars, total_points, avg_rating
FROM read_parquet('data/normalized_curated/ncaaf/recruiting/season=2024/*.parquet',
                  hive_partitioning=true)
WHERE scope = 'class'
ORDER BY total_points DESC
LIMIT 25;
```

### 8.8 Find Standings with ATS Records

```sql
SELECT team, conference,
       total_wins, total_losses,
       ats_wins, ats_losses, ats_pushes,
       over_wins, over_losses,
       expected_wins
FROM read_parquet('data/normalized_curated/ncaaf/standings/season=2023/*.parquet',
                  hive_partitioning=true)
ORDER BY total_wins DESC;
```

### 8.9 Compare Elo vs SP+ Ratings

```sql
WITH elo AS (
    SELECT team_id, team, overall AS elo_rating, rank_overall AS elo_rank
    FROM read_parquet('data/normalized_curated/ncaaf/ratings/season=2023/*.parquet',
                      hive_partitioning=true)
    WHERE rating_type = 'elo'
),
sp AS (
    SELECT team_id, overall AS sp_rating, rank_overall AS sp_rank
    FROM read_parquet('data/normalized_curated/ncaaf/ratings/season=2023/*.parquet',
                      hive_partitioning=true)
    WHERE rating_type = 'sp'
)
SELECT e.team, e.elo_rating, e.elo_rank, s.sp_rating, s.sp_rank,
       ABS(e.elo_rank - s.sp_rank) AS rank_diff
FROM elo e
JOIN sp s ON e.team_id = s.team_id
ORDER BY rank_diff DESC
LIMIT 20;
```

### 8.10 Full Season Player Stat Leaders by Category

```sql
-- Passing yards leaders, season level
SELECT player_name, team, pass_yards, pass_td, pass_int, pass_ppa
FROM read_parquet('data/normalized_curated/ncaaf/player_stats/season=2023/week=00/*.parquet',
                  hive_partitioning=true)
WHERE scope = 'season' AND pass_attempts > 100
ORDER BY pass_yards DESC
LIMIT 10;
```

---

## 9. Comparison: Old vs New

| Metric | Old Design (34 entities) | New Design (14 entities) |
|--------|--------------------------|--------------------------|
| **Top-level folders** | 6 major categories (`team/`, `player/`, `game/`, `betting/`, `analysis/`, `recruiting/`) | 14 flat entities (`teams/`, `players/`, `games/`, etc.) |
| **Total entity paths** | 34+ leaf-level directories | 14 directories |
| **Max directory depth** | 5+ levels (`team/identity/base/season=YYYY/`) | 2 levels max (`games/season=YYYY/week=WW/`); static entities are flat files |
| **Path complexity** | `game/advanced/epa/season=2023/week=05/` | `advanced/season=2023/week=05/` |
| **Data discovery** | Requires routing registry or documentation | Self-documenting — `ls ncaaf/` shows all entities |
| **Duplicate paths** | Yes — ratings under both `team/` and `analysis/` | No — each concept exists in exactly one place |
| **Join ergonomics** | Must navigate across category trees | All entities at the same level, simple joins |
| **Discriminator usage** | None — separate folders for each variant | 8 discriminators eliminate ~20 extra folders |
| **Partition consistency** | Mixed — some entities had no partitioning | Uniform: static, season-only, or season+week |
| **DuckDB glob queries** | Deep nested globs: `**/team/**/season=2023/**` | Simple: `teams/*.parquet` or `games/season=2023/*.parquet` |
| **New data source effort** | Find the right category, subcategory, and depth | Create or append to the one relevant entity |
| **Cognitive load** | Must learn category taxonomy | 14 names to remember |

---

*End of document.*
