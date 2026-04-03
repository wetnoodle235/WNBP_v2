# NBA Normalized-Curated Design Document

> **Version:** 1.0 — Consolidated 16-entity flat design  
> **Layer:** `data/normalized_curated/nba/`  
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
9. [Differences from NCAAF Design](#9-differences-from-ncaaf-design)

---

## 1. Executive Summary

### From Raw Provider Silos to 16 Flat Entities

NBA raw data comes from four providers — **ESPN**, **NBA Stats**, **Odds**, and
**OddsAPI** — each with their own folder hierarchies and season formats. This design
consolidates all raw sources into **16 flat, top-level entities** using the same
BallDontLie-inspired philosophy as the NCAAF design.

### The BallDontLie-Inspired Redesign

BDL organizes NBA data into a small set of flat, top-level resources (`teams`, `players`,
`games`, `stats`, etc.) — each self-contained and queryable without needing to understand
a deep taxonomy.

We adopt the same philosophy: **16 flat, top-level entities** where every entity is a
single Hive-partitioned Parquet dataset at depth ≤ 2
(`entity/partition/part-*.parquet`).

### NBA-Specific Extensions Beyond BDL

| Extension Area | Details |
|----------------|---------|
| **Advanced Metrics** | Tracking, hustle, shooting zones, play types, shot dashboards, clutch stats |
| **Player Props** | Separate entity for player prop lines (points, rebounds, assists, etc.) |
| **Contracts** | Annual and aggregate contract data with cap hits and signing exceptions |
| **Shot Charts** | Coordinate-level shot data merged into advanced entity |
| **Lineups** | Depth charts and 5-man lineup performance data |
| **Injuries** | Comprehensive injury tracking with body part and type detail |

### Design Principles

1. **Flat over nested** — no entity is more than one directory below the root.
2. **Discriminators over entities** — instead of `base_stats/` and `advanced_stats/` as
   separate folders, use a single `player_stats/` with `scope` and `stat_type` columns.
3. **Merge related sources** — ESPN athletes and NBA Stats players merge into `players`.
4. **Hive partitioning everywhere** — `season=YYYY` for DuckDB/Polars/Spark compatibility.
5. **Self-documenting paths** — `nba/players/season=2024/` needs no registry to interpret.
6. **No weeks** — NBA uses date-based scheduling; partition by season only.
7. **Season normalization** — raw "2024-25" format normalizes to integer `2024`.

---

## 2. Folder Structure Diagram

```
nba/
├── teams/                    ← static, no partitioning
│   └── part-*.parquet
│
├── players/                  ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── games/                    ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── player_stats/             ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── team_stats/               ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── standings/                ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── odds/                     ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── player_props/             ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── advanced/                 ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── plays/                    ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── box_scores/               ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── lineups/                  ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── contracts/                ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── injuries/                 ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
├── leaders/                  ← season=YYYY
│   └── season=2024/
│       └── part-*.parquet
│
└── venues/                   ← static, no partitioning
    └── part-*.parquet
```

**Total: 16 top-level directories. Max path depth: 2 levels (entity → partition → file).**

---

## 3. Entity Reference

Below are the detailed field tables for all 16 entities. Columns:

- **Field** — column name in the Parquet file
- **Type** — Arrow/Parquet logical type
- **Nullable** — whether the field can be null
- **Description** — what the field represents

### 3.1 teams (12 fields)

Static reference table. No partitioning.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | `Int64` | No | Unique team identifier |
| `name` | `Utf8` | No | Short team name (e.g. "Lakers") |
| `full_name` | `Utf8` | No | Full team name (e.g. "Los Angeles Lakers") |
| `abbreviation` | `Utf8` | No | Team abbreviation (e.g. "LAL") |
| `city` | `Utf8` | Yes | City where team is located |
| `conference` | `Utf8` | Yes | Conference — Eastern or Western |
| `division` | `Utf8` | Yes | Division name (e.g. "Pacific") |
| `color` | `Utf8` | Yes | Primary team colour hex code |
| `alternate_color` | `Utf8` | Yes | Secondary team colour hex code |
| `logo_url` | `Utf8` | Yes | URL to team logo image |
| `is_active` | `Boolean` | Yes | Whether the franchise is currently active |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.2 players (25 fields)

**Merges:** ESPN athletes + rosters + NBA Stats all_players.  
**Partition:** `season=YYYY`  
**Discriminator:** `status` (active/inactive/injured)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | `Int64` | No | Unique player identifier |
| `first_name` | `Utf8` | No | Player first name |
| `last_name` | `Utf8` | No | Player last name |
| `full_name` | `Utf8` | Yes | Full display name |
| `position` | `Utf8` | Yes | Position (PG, SG, SF, PF, C) |
| `height` | `Utf8` | Yes | Height (e.g. "6-6") |
| `weight` | `Utf8` | Yes | Weight as string |
| `jersey_number` | `Utf8` | Yes | Jersey number |
| `college` | `Utf8` | Yes | College attended |
| `country` | `Utf8` | Yes | Country of origin |
| `draft_year` | `Int64` | Yes | Year drafted |
| `draft_round` | `Int64` | Yes | Draft round |
| `draft_number` | `Int64` | Yes | Draft pick number |
| `team_id` | `Int64` | Yes | Current team identifier |
| `team_name` | `Utf8` | Yes | Current team name |
| `status` | `Utf8` | Yes | Player status — active/inactive/injured |
| `headshot_url` | `Utf8` | Yes | URL to player headshot |
| `age` | `Int64` | Yes | Player age |
| `date_of_birth` | `Utf8` | Yes | Date of birth (ISO-8601) |
| `debut_year` | `Int64` | Yes | Year of NBA debut |
| `years_pro` | `Int64` | Yes | Years of professional experience |
| `from_aau` | `Utf8` | Yes | AAU / pre-college program |
| `is_active` | `Boolean` | Yes | Whether currently active in NBA |
| `season` | `Int64` | No | Season start year |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.3 games (33 fields)

**Merges:** ESPN events + NBA Stats game summaries + OddsAPI scores.  
**Partition:** `season=YYYY`  
**Discriminator:** `season_type` (regular/playoffs/preseason/ist)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | `Int64` | No | Unique game identifier |
| `date` | `Utf8` | No | Game date (ISO-8601) |
| `season` | `Int64` | No | Season start year |
| `season_type` | `Utf8` | Yes | Season type — regular/playoffs/preseason/ist |
| `status` | `Utf8` | Yes | Game status |
| `period` | `Int64` | Yes | Current period / quarter |
| `time` | `Utf8` | Yes | Time remaining |
| `datetime` | `Utf8` | Yes | Full datetime with timezone |
| `home_team_id` | `Int64` | Yes | Home team ID |
| `home_team_name` | `Utf8` | Yes | Home team name |
| `home_team_score` | `Int64` | Yes | Home team total score |
| `visitor_team_id` | `Int64` | Yes | Visitor team ID |
| `visitor_team_name` | `Utf8` | Yes | Visitor team name |
| `visitor_team_score` | `Int64` | Yes | Visitor team total score |
| `home_score_q1` through `home_score_ot` | `Int64` | Yes | Home period scores |
| `visitor_score_q1` through `visitor_score_ot` | `Int64` | Yes | Visitor period scores |
| `postseason` | `Boolean` | Yes | Whether postseason game |
| `overtime` | `Boolean` | Yes | Whether went to OT |
| `ot_periods` | `Int64` | Yes | Number of OT periods |
| `attendance` | `Int64` | Yes | Reported attendance |
| `arena_name` | `Utf8` | Yes | Arena name |
| `arena_city` | `Utf8` | Yes | Arena city |
| `arena_state` | `Utf8` | Yes | Arena state |
| `tv_network` | `Utf8` | Yes | TV broadcast network |
| `duration_minutes` | `Int64` | Yes | Game duration in minutes |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.4 player_stats (75+ fields)

**Merges:** NBA Stats player-stats (6 stat types) + player-game-logs + season aggregates.  
**Partition:** `season=YYYY`  
**Discriminators:** `scope` (game/season), `stat_type` (base/advanced/defense/misc/scoring/usage)

Key fields include all standard counting stats (fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, turnover, pf, pts, plus_minus) plus
advanced metrics (offensive_rating, defensive_rating, net_rating, usage_pct, pie, etc.),
defensive stats (contested_shots, charges_drawn, deflections), scoring breakdown
(pct_fga_2pt, pct_pts_3pt, etc.), and misc stats (pts_off_turnover, pts_paint, etc.).

See `nba_schemas.py` → `NBA_PLAYER_STATS_SCHEMA` for the complete field listing.

### 3.5 team_stats (55+ fields)

**Merges:** NBA Stats team-stats (6 stat types) + team-game-logs + ESPN snapshots.  
**Partition:** `season=YYYY`  
**Discriminators:** `scope` (game/season), `stat_type` (base/advanced/defense/misc/scoring/usage)

Same counting stats as player_stats plus team-level aggregates (wins, losses, win_pct,
largest_lead, lead_changes, times_tied, opponent stats).

See `nba_schemas.py` → `NBA_TEAM_STATS_SCHEMA` for the complete field listing.

### 3.6 standings (25 fields)

**Merges:** ESPN standings + NBA Stats.  
**Partition:** `season=YYYY`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `team_id` | `Int64` | Yes | Team identifier |
| `team_name` | `Utf8` | Yes | Team name |
| `conference` | `Utf8` | Yes | Conference |
| `division` | `Utf8` | Yes | Division |
| `season` | `Int64` | No | Season start year |
| `wins` | `Int64` | Yes | Total wins |
| `losses` | `Int64` | Yes | Total losses |
| `win_pct` | `Float64` | Yes | Win percentage |
| `conference_rank` | `Int64` | Yes | Conference ranking |
| `division_rank` | `Int64` | Yes | Division ranking |
| `playoff_seed` | `Int64` | Yes | Playoff seed |
| `conference_record` | `Utf8` | Yes | Conference record |
| `division_record` | `Utf8` | Yes | Division record |
| `home_record` | `Utf8` | Yes | Home record |
| `road_record` | `Utf8` | Yes | Road record |
| `last_10` | `Utf8` | Yes | Last 10 games record |
| `streak` | `Utf8` | Yes | Current streak |
| `games_behind` | `Float64` | Yes | Games behind leader |
| `ot_record` | `Utf8` | Yes | Overtime record |
| `vs_over_500` | `Utf8` | Yes | Record vs above .500 |
| `clinch_indicator` | `Utf8` | Yes | Clinch status |
| `point_differential` | `Float64` | Yes | Point differential |
| `points_for` | `Float64` | Yes | Points per game |
| `points_against` | `Float64` | Yes | Points allowed per game |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.7 odds (19 fields)

**Merges:** Odds ESPN baseline + OddsAPI events.  
**Partition:** `season=YYYY`  
**Discriminator:** `line_type` (spread/moneyline/total/prop)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | `Int64` | Yes | Odds line identifier |
| `game_id` | `Int64` | No | Parent game identifier |
| `game_date` | `Utf8` | Yes | Game date |
| `season` | `Int64` | No | Season start year |
| `vendor` | `Utf8` | Yes | Sportsbook vendor |
| `line_type` | `Utf8` | No | Line type discriminator |
| `home_team` | `Utf8` | Yes | Home team name |
| `away_team` | `Utf8` | Yes | Away team name |
| `spread_home_value` | `Float64` | Yes | Home spread value |
| `spread_home_odds` | `Int64` | Yes | Home spread odds |
| `spread_away_value` | `Float64` | Yes | Away spread value |
| `spread_away_odds` | `Int64` | Yes | Away spread odds |
| `moneyline_home_odds` | `Int64` | Yes | Home moneyline |
| `moneyline_away_odds` | `Int64` | Yes | Away moneyline |
| `total_value` | `Float64` | Yes | Over/under total |
| `total_over_odds` | `Int64` | Yes | Over odds |
| `total_under_odds` | `Int64` | Yes | Under odds |
| `updated_at` | `Utf8` | Yes | Last update timestamp |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.8 player_props (14 fields)

**Merges:** Odds player_props + OddsAPI props.  
**Partition:** `season=YYYY`  
**Discriminator:** `prop_type` (points/rebounds/assists/threes/steals/blocks/pts_reb_ast/doubles/etc.)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | `Int64` | Yes | Prop record identifier |
| `game_id` | `Int64` | No | Parent game identifier |
| `player_id` | `Int64` | Yes | Player identifier |
| `player_name` | `Utf8` | Yes | Player name |
| `season` | `Int64` | No | Season start year |
| `vendor` | `Utf8` | Yes | Sportsbook vendor |
| `prop_type` | `Utf8` | No | Prop type discriminator |
| `line_value` | `Float64` | Yes | Prop line value |
| `market_type` | `Utf8` | Yes | Market type |
| `over_odds` | `Int64` | Yes | Over odds |
| `under_odds` | `Int64` | Yes | Under odds |
| `milestone_odds` | `Int64` | Yes | Milestone odds |
| `updated_at` | `Utf8` | Yes | Last update timestamp |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.9 advanced (80+ fields)

**Merges:** NBA Stats advanced metrics + shot charts.  
**Partition:** `season=YYYY`  
**Discriminator:** `metric_type` (tracking/hustle/shooting/playtype/shotdashboard/clutch)

Comprehensive advanced analytics including player tracking data (speed, distance, touches),
hustle stats (contested shots, deflections, loose balls), shooting zones (restricted area,
midrange, corner threes), play type breakdowns (isolation, PnR, transition), shot dashboard
by distance, and clutch performance.

See `nba_schemas.py` → `NBA_ADVANCED_SCHEMA` for the complete field listing.

### 3.10 plays (30 fields)

**Merges:** NBA Stats playbyplay + BDL plays.  
**Partition:** `season=YYYY`  
**Discriminator:** `play_type` (shot/rebound/turnover/foul/freethrow/violation/substitution/timeout/jumpball/ejection/other)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `game_id` | `Int64` | No | Parent game identifier |
| `order` | `Int64` | No | Sequence order within the game |
| `season` | `Int64` | No | Season start year |
| `period` | `Int64` | Yes | Quarter / period |
| `period_display` | `Utf8` | Yes | Display label (Q1, OT1) |
| `clock` | `Utf8` | Yes | Game clock |
| `play_type` | `Utf8` | No | Play type discriminator |
| `text` | `Utf8` | Yes | Play-by-play description |
| `home_score` | `Int64` | Yes | Home score at time of play |
| `away_score` | `Int64` | Yes | Away score at time of play |
| `scoring_play` | `Boolean` | Yes | Whether this scored |
| `shooting_play` | `Boolean` | Yes | Whether this was a shot |
| `score_value` | `Int64` | Yes | Points scored (0/1/2/3) |
| `team_id` | `Int64` | Yes | Team involved |
| `team_name` | `Utf8` | Yes | Team name |
| `coordinate_x` | `Float64` | Yes | Shot chart X |
| `coordinate_y` | `Float64` | Yes | Shot chart Y |
| `shot_distance` | `Float64` | Yes | Shot distance (feet) |
| `shot_result` | `Utf8` | Yes | Made/missed |
| `shot_type` | `Utf8` | Yes | Shot type description |
| `assisted` | `Boolean` | Yes | Was the shot assisted |
| `blocked` | `Boolean` | Yes | Was the shot blocked |
| `wallclock` | `Utf8` | Yes | Wall clock time |
| `participants` | `Utf8` | Yes | JSON participant array |
| `player1_id` through `player3_id` | `Int64` | Yes | Players involved |
| `player1_name` through `player3_name` | `Utf8` | Yes | Player names |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.11 box_scores (36 fields)

**Merges:** NBA Stats boxscores + BDL box_scores.  
**Partition:** `season=YYYY`

Per-player per-game box score lines with game context (teams, scores, period info), all
standard counting stats, plus starter designation and DNP reason.

See `nba_schemas.py` → `NBA_BOX_SCORES_SCHEMA` for the complete field listing.

### 3.12 lineups (15 fields)

**Merges:** ESPN depth_charts + BDL lineups.  
**Partition:** `season=YYYY`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | `Int64` | Yes | Lineup record identifier |
| `game_id` | `Int64` | Yes | Game identifier |
| `season` | `Int64` | No | Season start year |
| `team_id` | `Int64` | Yes | Team identifier |
| `team_name` | `Utf8` | Yes | Team name |
| `player_id` | `Int64` | Yes | Player identifier |
| `player_name` | `Utf8` | Yes | Player name |
| `position` | `Utf8` | Yes | Position |
| `starter` | `Boolean` | Yes | Whether starter |
| `depth_order` | `Int64` | Yes | Depth chart position order |
| `lineup_min` | `Utf8` | Yes | Minutes as lineup unit |
| `lineup_plus_minus` | `Float64` | Yes | Lineup plus/minus |
| `lineup_pts` | `Int64` | Yes | Lineup points |
| `lineup_poss` | `Int64` | Yes | Lineup possessions |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.13 contracts (27 fields)

**Merges:** ESPN athlete contracts + BDL contracts.  
**Partition:** `season=YYYY`  
**Discriminator:** `contract_scope` (annual/aggregate)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | `Int64` | Yes | Contract record identifier |
| `player_id` | `Int64` | No | Player identifier |
| `player_name` | `Utf8` | Yes | Player name |
| `team_id` | `Int64` | Yes | Team identifier |
| `team_name` | `Utf8` | Yes | Team name |
| `season` | `Int64` | No | Season start year |
| `contract_scope` | `Utf8` | No | Scope — annual/aggregate |
| `contract_type` | `Utf8` | Yes | Type (standard, two-way, 10-day, rookie) |
| `contract_status` | `Utf8` | Yes | Status (active, expired, traded) |
| `start_year` | `Int64` | Yes | Contract start year |
| `end_year` | `Int64` | Yes | Contract end year |
| `contract_years` | `Int64` | Yes | Duration in years |
| `base_salary` | `Float64` | Yes | Base salary |
| `cap_hit` | `Float64` | Yes | Salary cap hit |
| `dead_cap` | `Float64` | Yes | Dead cap value |
| `total_cash` | `Float64` | Yes | Total cash compensation |
| `total_value` | `Float64` | Yes | Total contract value |
| `average_salary` | `Float64` | Yes | Average annual salary |
| `guaranteed_at_signing` | `Float64` | Yes | Guaranteed at signing |
| `total_guaranteed` | `Float64` | Yes | Total guaranteed |
| `signed_using` | `Utf8` | Yes | Signing exception |
| `free_agent_year` | `Int64` | Yes | Free agency year |
| `free_agent_status` | `Utf8` | Yes | FA type (UFA, RFA) |
| `trade_kicker` | `Float64` | Yes | Trade kicker % |
| `player_option_year` | `Int64` | Yes | Player option year |
| `team_option_year` | `Int64` | Yes | Team option year |
| `rank` | `Int64` | Yes | Salary rank |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.14 injuries (13 fields)

**Merges:** ESPN injuries + snapshots.  
**Partition:** `season=YYYY`

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `player_id` | `Int64` | No | Player identifier |
| `player_name` | `Utf8` | Yes | Player name |
| `team_id` | `Int64` | Yes | Team identifier |
| `team_name` | `Utf8` | Yes | Team name |
| `season` | `Int64` | No | Season start year |
| `status` | `Utf8` | Yes | Injury status (out, day-to-day, questionable, etc.) |
| `description` | `Utf8` | Yes | Injury description |
| `body_part` | `Utf8` | Yes | Body part affected |
| `injury_type` | `Utf8` | Yes | Injury type |
| `return_date` | `Utf8` | Yes | Expected return date |
| `reported_date` | `Utf8` | Yes | Date reported |
| `games_missed` | `Int64` | Yes | Games missed |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.15 leaders (13 fields)

**Merges:** NBA Stats league-leaders.  
**Partition:** `season=YYYY`  
**Discriminator:** `stat_type` (pts/reb/ast/stl/blk/fg_pct/fg3_pct/ft_pct/min/eff/dd2/td3/etc.)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `player_id` | `Int64` | No | Player identifier |
| `player_name` | `Utf8` | Yes | Player name |
| `team_id` | `Int64` | Yes | Team identifier |
| `team_name` | `Utf8` | Yes | Team name |
| `season` | `Int64` | No | Season start year |
| `season_type` | `Utf8` | Yes | Season type |
| `stat_type` | `Utf8` | No | Stat category discriminator |
| `value` | `Float64` | Yes | Statistical value |
| `rank` | `Int64` | Yes | Rank |
| `games_played` | `Int64` | Yes | Games played |
| `per_game` | `Float64` | Yes | Per-game average |
| `total` | `Float64` | Yes | Season total |
| `source` | `Utf8` | No | Data vendor provenance |

### 3.16 venues (12 fields)

Static reference table. No partitioning.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | `Int64` | No | Unique venue identifier |
| `name` | `Utf8` | No | Arena name |
| `city` | `Utf8` | Yes | City |
| `state` | `Utf8` | Yes | State or province |
| `country` | `Utf8` | Yes | Country |
| `capacity` | `Int64` | Yes | Seating capacity |
| `year_opened` | `Int64` | Yes | Year opened |
| `surface` | `Utf8` | Yes | Court surface type |
| `latitude` | `Float64` | Yes | Latitude |
| `longitude` | `Float64` | Yes | Longitude |
| `timezone` | `Utf8` | Yes | IANA timezone |
| `source` | `Utf8` | No | Data vendor provenance |

---

## 4. Discriminator Column Guide

Discriminator columns allow multiple record types to coexist in a single entity. Filter
on these columns when querying to get the subset you need.

| Entity | Discriminator | Values | Usage |
|--------|---------------|--------|-------|
| `players` | `status` | active, inactive, injured | Filter player availability |
| `games` | `season_type` | regular, playoffs, preseason, ist | Separate regular season from playoffs |
| `player_stats` | `scope` | game, season | Game-level vs season aggregates |
| `player_stats` | `stat_type` | base, advanced, defense, misc, scoring, usage | NBA Stats category |
| `team_stats` | `scope` | game, season | Game-level vs season aggregates |
| `team_stats` | `stat_type` | base, advanced, defense, misc, scoring, usage | NBA Stats category |
| `odds` | `line_type` | spread, moneyline, total, prop | Type of betting line |
| `player_props` | `prop_type` | points, rebounds, assists, threes, steals, blocks, pts_reb_ast, doubles, etc. | Prop market type |
| `advanced` | `metric_type` | tracking, hustle, shooting, playtype, shotdashboard, clutch | Advanced stat category |
| `plays` | `play_type` | shot, rebound, turnover, foul, freethrow, violation, substitution, timeout, jumpball, ejection, other | Play event type |
| `contracts` | `contract_scope` | annual, aggregate | Per-year vs total contract |
| `leaders` | `stat_type` | pts, reb, ast, stl, blk, fg_pct, fg3_pct, ft_pct, min, eff, dd2, td3, etc. | Leaderboard category |

---

## 5. Partitioning Strategy

NBA uses date-based scheduling, not weekly. All seasonal entities partition by `season=`
only (no `week=` like NCAAF).

| Tier | Entities | Partition Keys | Description |
|------|----------|----------------|-------------|
| **STATIC** | teams, venues | _(none)_ | Reference data; rarely changes |
| **SEASONAL** | players, standings, contracts, injuries, leaders | `season=` | One dataset per season |
| **GAME-LEVEL** | games, player_stats, team_stats, odds, player_props, advanced, plays, box_scores, lineups | `season=` | High-volume game data, partitioned by season |

**Season Format:** Always the start year as integer. Raw data "2024-25" normalizes to `2024`.

---

## 6. Vendor Priority Summary

| Entity | Primary Provider | Secondary | Notes |
|--------|-----------------|-----------|-------|
| teams | ESPN | NBA Stats | ESPN has richer metadata, logos |
| players | ESPN | NBA Stats | ESPN for profiles; NBA Stats for draft/debut |
| games | ESPN | NBA Stats, OddsAPI | ESPN is primary; NBA Stats for quarter scores |
| player_stats | NBA Stats | ESPN | NBA Stats is authoritative for all stat types |
| team_stats | NBA Stats | ESPN | NBA Stats is authoritative for all stat types |
| standings | ESPN | NBA Stats | ESPN for real-time standings |
| odds | Odds | OddsAPI | Dedicated odds providers most accurate |
| player_props | Odds | OddsAPI | Dedicated odds providers most accurate |
| advanced | NBA Stats | — | Sole source for tracking/hustle/shooting |
| plays | NBA Stats | — | Sole source for play-by-play |
| box_scores | NBA Stats | ESPN | NBA Stats has precise box data |
| lineups | ESPN | — | Sole source for depth charts |
| contracts | ESPN | — | Sole source for contract data |
| injuries | ESPN | — | Sole source for injury reports |
| leaders | NBA Stats | — | Sole source for league leaders |
| venues | ESPN | NBA Stats | ESPN has richer arena metadata |

---

## 7. Migration Guide

### Running the Migration

```bash
# 1. Create the 16 entity directories
python scripts/build_nba_curated_structure.py

# 2. Preview what would be migrated
python scripts/migrate_nba_curated.py --dry-run

# 3. Run the migration (with backup)
python scripts/migrate_nba_curated.py --backup

# 4. Verify
ls -la data/normalized_curated/nba/
```

### What Gets Migrated

The migration script moves parquet files from old provider-centric paths to the new
entity-centric paths. For example:

```
nba/espn/athletes/season=2024/part.parquet
→ nba/players/athletes/season=2024/part.parquet

nba/nbastats/player-stats/base/season=2024/part.parquet
→ nba/player_stats/base/season=2024/part.parquet
```

---

## 8. DuckDB Query Examples

### Get all active Lakers players for 2024-25

```sql
SELECT id, full_name, position, jersey_number
FROM read_parquet('nba/players/season=2024/part-*.parquet')
WHERE team_name = 'Los Angeles Lakers'
  AND status = 'active'
ORDER BY position, last_name;
```

### Get season averages for a player

```sql
SELECT player_name, pts, reb, ast, fg_pct, fg3_pct
FROM read_parquet('nba/player_stats/season=2024/part-*.parquet')
WHERE scope = 'season'
  AND stat_type = 'base'
  AND season_type = 'regular'
ORDER BY pts DESC
LIMIT 20;
```

### Get game odds with spreads

```sql
SELECT game_date, home_team, away_team, vendor,
       spread_home_value, moneyline_home_odds, moneyline_away_odds, total_value
FROM read_parquet('nba/odds/season=2024/part-*.parquet')
WHERE line_type = 'spread'
ORDER BY game_date;
```

### Get shooting zone stats

```sql
SELECT player_name,
       fg_pct_restricted_area,
       fg_pct_midrange,
       fg_pct_above_break_3,
       fg_pct_left_corner_3,
       fg_pct_right_corner_3
FROM read_parquet('nba/advanced/season=2024/part-*.parquet')
WHERE metric_type = 'shooting'
ORDER BY fg_pct_restricted_area DESC
LIMIT 20;
```

### Conference standings

```sql
SELECT team_name, wins, losses, win_pct,
       conference_rank, streak, last_10, games_behind
FROM read_parquet('nba/standings/season=2024/part-*.parquet')
WHERE conference = 'Western'
ORDER BY conference_rank;
```

### Player props for a game

```sql
SELECT player_name, prop_type, line_value, over_odds, under_odds, vendor
FROM read_parquet('nba/player_props/season=2024/part-*.parquet')
WHERE game_id = 12345
ORDER BY player_name, prop_type;
```

### Contract analysis

```sql
SELECT player_name, team_name, base_salary, cap_hit,
       signed_using, free_agent_year, free_agent_status
FROM read_parquet('nba/contracts/season=2024/part-*.parquet')
WHERE contract_scope = 'annual'
ORDER BY base_salary DESC
LIMIT 30;
```

### Hustle stats leaders

```sql
SELECT player_name, team_name,
       deflections, charges_drawn, loose_balls_recovered, box_outs
FROM read_parquet('nba/advanced/season=2024/part-*.parquet')
WHERE metric_type = 'hustle'
ORDER BY deflections DESC
LIMIT 20;
```

---

## 9. Differences from NCAAF Design

| Aspect | NCAAF | NBA |
|--------|-------|-----|
| **Entity Count** | 14 | 16 |
| **Partitioning** | `season=` + `week=` for game-level | `season=` only (no weeks) |
| **Season Format** | Single year integer (2024) | Start year integer (2024 for "2024-25") |
| **Conferences Entity** | Standalone `conferences` entity | Conference info embedded in `teams` and `standings` |
| **Rankings Entity** | Standalone `rankings` entity | No rankings — NBA uses standings/seeds instead |
| **Ratings Entity** | 6 rating systems (Elo, SP+, FPI, SRS, talent, SP+ conf) | No ratings entity — NBA advanced metrics cover this |
| **Recruiting Entity** | Classes, players, position groups | No recruiting — NBA draft is simpler |
| **Box Scores** | Embedded in `player_stats` / `team_stats` | Standalone `box_scores` entity |
| **Lineups** | Not present in NCAAF | Standalone `lineups` entity |
| **Contracts** | Not present in NCAAF | Standalone `contracts` entity |
| **Injuries** | Merged into `players` | Standalone `injuries` entity |
| **Leaders** | Not present in NCAAF | Standalone `leaders` entity |
| **Player Props** | Merged into `odds` | Standalone `player_props` entity |
| **Advanced Metrics** | EPA, PPA, havoc, win probability | Tracking, hustle, shooting, play type, shot dashboard, clutch |
| **Stat Type Discriminator** | Not used (uses `scope` only) | `stat_type` for 6 NBA Stats categories |
| **Providers** | ESPN, CFBData, Weather | ESPN, NBA Stats, Odds, OddsAPI |

### Key Rationale for NBA-Specific Entities

1. **`box_scores` separate from `player_stats`**: Box scores contain game-level context
   (team scores, period info) alongside per-player lines, making them a natural unit.
   `player_stats` focuses on aggregated/statistical views.

2. **`player_props` separate from `odds`**: NBA has deep prop markets with 10+ prop types
   per player per game. Keeping them separate avoids a cartesian explosion in the odds table.

3. **`contracts` standalone**: NBA salary cap is complex (Bird rights, MLE, trade kickers).
   Contract data is heavily used for front-office analytics and deserves its own entity.

4. **`injuries` standalone**: NBA load management and injury tracking is a key analytics
   use case. A separate entity enables time-series injury analysis.

5. **`leaders` standalone**: League leader boards are a first-class NBA Stats endpoint
   with their own ranking logic distinct from raw stats.

6. **No `week=` partition**: NBA plays 82+ games without a weekly structure. Games happen
   on varying dates, making date-based queries more natural than week-based.
