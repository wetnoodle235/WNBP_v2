# NCAAF Normalized-Curated Design Document

> **Version:** 2.0 — BallDontLie-inspired flat entity redesign
> **Layer:** `data/normalized_curated/ncaaf/`
> **Schema file:** `backend/normalization/ncaaf_schemas.py`
> **Builder script:** `scripts/build_ncaaf_curated_structure.py`

---

## 1. Overview

The previous NCAAF normalized_curated layout used deep 3-level nesting
(`team/identity/base`, `game/advanced/epa`, etc.). This made queries
cumbersome and violated the principle of one-entity-per-folder.

The redesigned layout mirrors **BallDontLie's NCAAF API** — each entity
occupies a single top-level folder under `ncaaf/`, stored as
hive-partitioned Parquet. This yields:

- **Flat, discoverable paths** — every entity is at most two levels deep
  (subfolder splits like `ratings/elo` are the deepest).
- **Self-describing schemas** — each entity has a single PyArrow schema
  with field-level metadata descriptions.
- **Vendor provenance** — every row carries a `source` column tracking
  which vendor produced it.
- **DuckDB-friendly** — hive partitioning lets DuckDB push down
  `season=` and `week=` predicates automatically.

---

## 2. Folder Tree

```
data/normalized_curated/ncaaf/
├── advanced/
│   ├── epa/                          # season=/week=
│   ├── havoc/                        # season=/week=
│   ├── ppa/                          # season=/week=
│   └── win_probability/              # season=/week=
├── coaches/                          # season=
├── conferences/                      # (no partitioning)
├── draft/                            # season=
├── drives/                           # season=/week=
├── games/                            # season=/week=
├── injuries/                         # season=/week=
├── media/                            # season=/week=
├── odds/                             # season=/week=
├── players/                          # season=
├── player_season_stats/              # season=
├── player_stats/                     # season=/week=
├── plays/                            # season=/week=
├── portal/                           # season=
├── rankings/                         # season=/week=
├── ratings/
│   ├── elo/                          # season=
│   ├── fpi/                          # season=
│   ├── sp/                           # season=
│   ├── sp_conference/                # season=
│   ├── srs/                          # season=
│   └── talent/                       # season=
├── recruiting_classes/               # season=
├── recruiting_groups/                # season=
├── recruiting_players/               # season=
├── returning_production/             # season=
├── standings/                        # season=
├── teams/                            # season=
├── team_season_stats/                # season=
├── team_stats/                       # season=/week=
├── venues/                           # (no partitioning)
└── weather/                          # season=/week=
```

---

## 3. Entity-to-Folder Mapping

| #  | Entity                  | Path (under `ncaaf/`)        | Origin     | Partitioning      |
|----|-------------------------|------------------------------|------------|-------------------|
| 1  | conferences             | `conferences/`               | BDL        | none              |
| 2  | teams                   | `teams/`                     | BDL        | `season=`         |
| 3  | players                 | `players/`                   | BDL        | `season=`         |
| 4  | games                   | `games/`                     | BDL        | `season=/week=`   |
| 5  | plays                   | `plays/`                     | BDL        | `season=/week=`   |
| 6  | player_stats            | `player_stats/`              | BDL        | `season=/week=`   |
| 7  | team_stats              | `team_stats/`                | BDL        | `season=/week=`   |
| 8  | player_season_stats     | `player_season_stats/`       | BDL        | `season=`         |
| 9  | team_season_stats       | `team_season_stats/`         | BDL        | `season=`         |
| 10 | standings               | `standings/`                 | BDL        | `season=`         |
| 11 | rankings                | `rankings/`                  | BDL        | `season=/week=`   |
| 12 | odds                    | `odds/`                      | BDL        | `season=/week=`   |
| 13 | coaches                 | `coaches/`                   | WNBP       | `season=`         |
| 14 | weather                 | `weather/`                   | WNBP       | `season=/week=`   |
| 15 | injuries                | `injuries/`                  | WNBP       | `season=/week=`   |
| 16 | recruiting_classes      | `recruiting_classes/`        | WNBP       | `season=`         |
| 17 | recruiting_players      | `recruiting_players/`        | WNBP       | `season=`         |
| 18 | recruiting_groups       | `recruiting_groups/`         | WNBP       | `season=`         |
| 19 | ratings/elo             | `ratings/elo/`               | WNBP       | `season=`         |
| 19 | ratings/sp              | `ratings/sp/`                | WNBP       | `season=`         |
| 19 | ratings/fpi             | `ratings/fpi/`               | WNBP       | `season=`         |
| 19 | ratings/srs             | `ratings/srs/`               | WNBP       | `season=`         |
| 19 | ratings/talent          | `ratings/talent/`            | WNBP       | `season=`         |
| 19 | ratings/sp_conference   | `ratings/sp_conference/`     | WNBP       | `season=`         |
| 20 | advanced/epa            | `advanced/epa/`              | WNBP       | `season=/week=`   |
| 20 | advanced/ppa            | `advanced/ppa/`              | WNBP       | `season=/week=`   |
| 20 | advanced/havoc          | `advanced/havoc/`            | WNBP       | `season=/week=`   |
| 20 | advanced/win_probability| `advanced/win_probability/`  | WNBP       | `season=/week=`   |
| 21 | drives                  | `drives/`                    | WNBP       | `season=/week=`   |
| 22 | draft                   | `draft/`                     | WNBP       | `season=`         |
| 23 | portal                  | `portal/`                    | WNBP       | `season=`         |
| 24 | returning_production    | `returning_production/`      | WNBP       | `season=`         |
| 25 | venues                  | `venues/`                    | WNBP       | none              |
| 26 | media                   | `media/`                     | WNBP       | `season=/week=`   |

---

## 4. Schema Field Reference

### 4.1 conferences

| Field          | Type     | Description                        |
|----------------|----------|------------------------------------|
| id             | int64    | Unique conference identifier       |
| name           | string   | Full conference name               |
| abbreviation   | string   | Short abbreviation (e.g. SEC, B1G) |
| source         | string   | Data vendor provenance             |

### 4.2 teams

| Field          | Type     | Description                                   |
|----------------|----------|-----------------------------------------------|
| id             | int64    | Unique team identifier                        |
| conference     | string   | Conference name or abbreviation               |
| city           | string   | City where team is located                    |
| name           | string   | Short team name (e.g. Crimson Tide)           |
| full_name      | string   | Full team name (e.g. Alabama Crimson Tide)    |
| abbreviation   | string   | Team abbreviation (e.g. ALA)                  |
| source         | string   | Data vendor provenance                        |

### 4.3 players

| Field                  | Type     | Description                        |
|------------------------|----------|------------------------------------|
| id                     | int64    | Unique player identifier           |
| first_name             | string   | Player first name                  |
| last_name              | string   | Player last name                   |
| position               | string   | Full position name                 |
| position_abbreviation  | string   | Position abbreviation (e.g. QB)    |
| height                 | string   | Height (e.g. 6-2)                  |
| weight                 | int32    | Weight in pounds                   |
| jersey_number          | int32    | Jersey number                      |
| team                   | string   | Team name or abbreviation          |
| source                 | string   | Data vendor provenance             |

### 4.4 games

| Field             | Type         | Description                                  |
|-------------------|--------------|----------------------------------------------|
| id                | int64        | Unique game identifier                       |
| date              | timestamp[s] | Game date and time (UTC)                     |
| season            | int32        | Season year                                  |
| week              | int32        | Week number                                  |
| status            | string       | Game status (scheduled, in_progress, final)  |
| period            | int32        | Current period/quarter                       |
| time              | string       | Clock time remaining                         |
| home_team         | string       | Home team name                               |
| visitor_team      | string       | Visiting team name                           |
| home_score        | int32        | Home team total score                        |
| visitor_score     | int32        | Visitor team total score                     |
| home_score_q1..q4 | int32       | Home team per-quarter scores                 |
| home_score_ot     | int32        | Home team overtime score                     |
| visitor_score_q1..q4 | int32    | Visitor team per-quarter scores              |
| visitor_score_ot  | int32        | Visitor team overtime score                  |
| venue_id          | int64        | Venue identifier (FK to venues)              |
| source            | string       | Data vendor provenance                       |

### 4.5 plays

| Field          | Type     | Description                                 |
|----------------|----------|---------------------------------------------|
| game_id        | int64    | Game identifier (FK to games)               |
| order          | int32    | Sequential play order within the game       |
| type           | string   | Play type (e.g. Rush, Pass, Punt)           |
| text           | string   | Human-readable play description             |
| home_score     | int32    | Home team score after this play             |
| away_score     | int32    | Away team score after this play             |
| period         | int32    | Quarter / period number                     |
| clock          | string   | Game clock at time of play (mm:ss)          |
| scoring_play   | bool     | Whether this play resulted in a score       |
| score_value    | int32    | Points scored on this play (0 if none)      |
| team           | string   | Team with possession                        |
| yard_line      | int32    | Yard line at snap                           |
| down           | int32    | Down number (1-4)                           |
| distance       | int32    | Yards to go for first down                  |
| yards_gained   | int32    | Net yards gained on the play                |
| source         | string   | Data vendor provenance                      |

### 4.6 player_stats (per-game)

| Field                   | Type     | Description                   |
|-------------------------|----------|-------------------------------|
| player                  | string   | Player name or identifier     |
| player_id               | int64    | Player unique ID              |
| team                    | string   | Team name or abbreviation     |
| game_id                 | int64    | Game identifier               |
| passing_completions     | int32    | Pass completions              |
| passing_attempts        | int32    | Pass attempts                 |
| passing_yards           | int32    | Passing yards                 |
| passing_td              | int32    | Passing touchdowns            |
| passing_int             | int32    | Interceptions thrown          |
| passing_qbr             | float64  | Quarterback rating (QBR)      |
| passing_rating          | float64  | Passer rating                 |
| rushing_attempts        | int32    | Rush attempts                 |
| rushing_yards           | int32    | Rushing yards                 |
| rushing_td              | int32    | Rushing touchdowns            |
| rushing_long            | int32    | Longest rush                  |
| receiving_receptions    | int32    | Receptions                    |
| receiving_yards         | int32    | Receiving yards               |
| receiving_td            | int32    | Receiving touchdowns          |
| receiving_targets       | int32    | Targets                       |
| receiving_long          | int32    | Longest reception             |
| defense_tackles         | int32    | Total tackles                 |
| defense_solo            | int32    | Solo tackles                  |
| defense_tfl             | int32    | Tackles for loss              |
| defense_sacks           | float64  | Sacks                         |
| defense_int             | int32    | Defensive interceptions       |
| defense_pd              | int32    | Passes defended               |
| kick_return_yards       | int32    | Kick return yards             |
| punt_return_yards       | int32    | Punt return yards             |
| fumbles_lost            | int32    | Fumbles lost                  |
| source                  | string   | Data vendor provenance        |

### 4.7 team_stats (per-game)

| Field                    | Type     | Description                  |
|--------------------------|----------|------------------------------|
| team                     | string   | Team name or abbreviation    |
| team_id                  | int64    | Team unique ID               |
| game_id                  | int64    | Game identifier              |
| first_downs              | int32    | Total first downs            |
| third_down_conversions   | int32    | Third-down conversions       |
| third_down_attempts      | int32    | Third-down attempts          |
| fourth_down_conversions  | int32    | Fourth-down conversions      |
| fourth_down_attempts     | int32    | Fourth-down attempts         |
| passing_yards            | int32    | Total passing yards          |
| rushing_yards            | int32    | Total rushing yards          |
| total_yards              | int32    | Total offensive yards        |
| turnovers                | int32    | Total turnovers              |
| penalties                | int32    | Number of penalties          |
| penalty_yards            | int32    | Penalty yards                |
| possession_time          | string   | Time of possession (mm:ss)   |
| source                   | string   | Data vendor provenance       |

### 4.8 player_season_stats

Same stat categories as `player_stats` but aggregated over the full season.
Additional fields: `season`, `games_played`.

### 4.9 team_season_stats

| Field                    | Type     | Description                        |
|--------------------------|----------|------------------------------------|
| team                     | string   | Team name or abbreviation          |
| team_id                  | int64    | Team unique ID                     |
| season                   | int32    | Season year                        |
| games_played             | int32    | Total games played                 |
| passing_yards            | int32    | Season total passing yards         |
| passing_yards_per_game   | float64  | Passing yards per game             |
| rushing_yards            | int32    | Season total rushing yards         |
| rushing_yards_per_game   | float64  | Rushing yards per game             |
| receiving_yards          | int32    | Season total receiving yards       |
| total_yards              | int32    | Season total offensive yards       |
| total_yards_per_game     | float64  | Total yards per game               |
| passing_td               | int32    | Season passing touchdowns          |
| rushing_td               | int32    | Season rushing touchdowns          |
| total_td                 | int32    | Season total touchdowns            |
| interceptions            | int32    | Season interceptions thrown        |
| fumbles_lost             | int32    | Season fumbles lost                |
| turnovers                | int32    | Season total turnovers             |
| opp_passing_yards        | int32    | Opponent season passing yards      |
| opp_rushing_yards        | int32    | Opponent season rushing yards      |
| opp_total_yards          | int32    | Opponent season total yards        |
| opp_points_per_game      | float64  | Opponent points per game           |
| source                   | string   | Data vendor provenance             |

### 4.10 standings

| Field              | Type     | Description                              |
|--------------------|----------|------------------------------------------|
| team               | string   | Team name or abbreviation                |
| team_id            | int64    | Team unique ID                           |
| conference         | string   | Conference name                          |
| season             | int32    | Season year                              |
| wins               | int32    | Total wins                               |
| losses             | int32    | Total losses                             |
| win_pct            | float64  | Win percentage                           |
| games_behind       | float64  | Games behind conference leader           |
| home_wins          | int32    | Home wins                                |
| home_losses        | int32    | Home losses                              |
| away_wins          | int32    | Away wins                                |
| away_losses        | int32    | Away losses                              |
| conference_wins    | int32    | Conference wins                          |
| conference_losses  | int32    | Conference losses                        |
| streak             | string   | Current win/loss streak (e.g. W3, L2)    |
| source             | string   | Data vendor provenance                   |

### 4.11 rankings

| Field               | Type     | Description                              |
|---------------------|----------|------------------------------------------|
| team                | string   | Team name                                |
| team_id             | int64    | Team unique ID                           |
| season              | int32    | Season year                              |
| week                | int32    | Week number                              |
| poll                | string   | Poll name (AP, Coaches, CFP)             |
| rank                | int32    | Team ranking                             |
| first_place_votes   | int32    | First place votes received               |
| points              | int32    | Total poll points                        |
| trend               | int32    | Rank change from previous week           |
| record              | string   | Team record (e.g. 8-1)                   |
| source              | string   | Data vendor provenance                   |

### 4.12 odds

| Field               | Type         | Description                          |
|---------------------|--------------|--------------------------------------|
| id                  | int64        | Unique odds record identifier        |
| game_id             | int64        | Game identifier                      |
| vendor              | string       | Odds vendor / sportsbook name        |
| spread_home_value   | float64      | Home team spread value               |
| spread_home_odds    | int32        | Home spread odds (American)          |
| spread_away_value   | float64      | Away team spread value               |
| spread_away_odds    | int32        | Away spread odds (American)          |
| moneyline_home      | int32        | Home moneyline (American)            |
| moneyline_away      | int32        | Away moneyline (American)            |
| total_value         | float64      | Over/under total value               |
| total_over_odds     | int32        | Over odds (American)                 |
| total_under_odds    | int32        | Under odds (American)                |
| updated_at          | timestamp[s] | Last update timestamp                |
| source              | string       | Data vendor provenance               |

### 4.13 coaches

| Field       | Type     | Description                            |
|-------------|----------|----------------------------------------|
| season      | int32    | Season year                            |
| team        | string   | Team name or abbreviation              |
| team_id     | int64    | Team unique ID                         |
| first_name  | string   | Coach first name                       |
| last_name   | string   | Coach last name                        |
| position    | string   | Coaching position (HC, OC, DC)         |
| years       | int32    | Years of coaching experience           |
| wins        | int32    | Career wins at this school             |
| losses      | int32    | Career losses at this school           |
| hire_year   | int32    | Year coach was hired                   |
| source      | string   | Data vendor provenance                 |

### 4.14 weather

| Field           | Type     | Description                            |
|-----------------|----------|----------------------------------------|
| game_id         | int64    | Game identifier                        |
| season          | int32    | Season year                            |
| week            | int32    | Week number                            |
| temperature     | float64  | Temperature in Fahrenheit              |
| wind_speed      | float64  | Wind speed in mph                      |
| wind_direction  | string   | Wind direction (e.g. NW, SSE)         |
| humidity        | float64  | Humidity percentage                    |
| precipitation   | float64  | Precipitation probability or inches    |
| conditions      | string   | Weather conditions description         |
| dome            | bool     | Whether game is in a dome              |
| source          | string   | Data vendor provenance                 |

### 4.15 injuries

| Field        | Type     | Description                                         |
|--------------|----------|-----------------------------------------------------|
| player_id    | int64    | Player unique ID                                    |
| player_name  | string   | Player display name                                 |
| team_id      | int64    | Team unique ID                                      |
| team         | string   | Team name or abbreviation                           |
| season       | int32    | Season year                                         |
| week         | int32    | Week number                                         |
| status       | string   | Injury status (out, doubtful, questionable, probable)|
| injury_type  | string   | Type of injury                                      |
| body_part    | string   | Affected body part                                  |
| return_date  | date32   | Estimated return date                               |
| source       | string   | Data vendor provenance                              |

### 4.16 recruiting_classes

| Field          | Type     | Description                        |
|----------------|----------|------------------------------------|
| team           | string   | Team name or abbreviation          |
| team_id        | int64    | Team unique ID                     |
| season         | int32    | Recruiting class year              |
| rank           | int32    | National recruiting class rank     |
| points         | float64  | Total recruiting points            |
| total_commits  | int32    | Total number of commitments        |
| avg_rating     | float64  | Average recruit rating             |
| five_star      | int32    | Number of 5-star recruits          |
| four_star      | int32    | Number of 4-star recruits          |
| three_star     | int32    | Number of 3-star recruits          |
| source         | string   | Data vendor provenance             |

### 4.17 recruiting_players

| Field       | Type     | Description                        |
|-------------|----------|------------------------------------|
| player_id   | int64    | Unique recruit identifier          |
| season      | int32    | Recruiting class year              |
| name        | string   | Recruit full name                  |
| position    | string   | Position                           |
| team        | string   | Committed team                     |
| stars       | int32    | Star rating (2-5)                  |
| rating      | float64  | Composite rating                   |
| city        | string   | Hometown city                      |
| state       | string   | Hometown state                     |
| height      | string   | Height (e.g. 6-3)                  |
| weight      | int32    | Weight in pounds                   |
| source      | string   | Data vendor provenance             |

### 4.18 recruiting_groups

| Field           | Type     | Description                        |
|-----------------|----------|------------------------------------|
| team            | string   | Team name or abbreviation          |
| team_id         | int64    | Team unique ID                     |
| season          | int32    | Recruiting class year              |
| position_group  | string   | Position group (e.g. QB, OL, DL)   |
| total_commits   | int32    | Commitments in this group          |
| avg_rating      | float64  | Average rating for position group  |
| points          | float64  | Total points for position group    |
| source          | string   | Data vendor provenance             |

### 4.19 ratings/*

**ratings/elo**: team, team_id, season, elo_rating, source

**ratings/sp**: team, team_id, season, overall, offense, defense, special_teams, source

**ratings/fpi**: team, team_id, season, fpi, avg_win_prob, sos, remaining_sos, source

**ratings/srs**: team, team_id, season, srs_rating, source

**ratings/talent**: team, team_id, season, talent_rating, source

**ratings/sp_conference**: conference, season, overall, offense, defense, source

### 4.20 advanced/*

**advanced/epa**: game_id, team, team_id, season, week, epa_overall, epa_passing, epa_rushing, epa_success_rate, source

**advanced/ppa**: game_id, team, team_id, season, week, ppa_overall, ppa_passing, ppa_rushing, source

**advanced/havoc**: game_id, team, team_id, season, week, total_havoc, front_seven, db_havoc, source

**advanced/win_probability**: game_id, season, week, home_win_prob, away_win_prob, spread, over_under, source

### 4.21 drives

| Field                | Type     | Description                       |
|----------------------|----------|-----------------------------------|
| game_id              | int64    | Game identifier                   |
| season               | int32    | Season year                       |
| week                 | int32    | Week number                       |
| team                 | string   | Team with possession              |
| team_id              | int64    | Team unique ID                    |
| drive_number         | int32    | Sequential drive number           |
| plays                | int32    | Number of plays in drive          |
| yards                | int32    | Total yards gained on drive       |
| time_of_possession   | string   | Drive duration (mm:ss)            |
| result               | string   | Drive result (TD, FG, Punt, etc.) |
| start_period         | int32    | Period when drive started         |
| start_yardline       | int32    | Starting yard line                |
| end_period           | int32    | Period when drive ended           |
| end_yardline         | int32    | Ending yard line                  |
| source               | string   | Data vendor provenance            |

### 4.22 draft

| Field         | Type     | Description                        |
|---------------|----------|------------------------------------|
| season        | int32    | Draft year                         |
| pick          | int32    | Overall pick number                |
| round         | int32    | Draft round                        |
| team          | string   | NFL team that made the pick        |
| player_name   | string   | Player full name                   |
| position      | string   | Position                           |
| college_team  | string   | College team name                  |
| height        | string   | Height (e.g. 6-4)                  |
| weight        | int32    | Weight in pounds                   |
| source        | string   | Data vendor provenance             |

### 4.23 portal

| Field             | Type     | Description                              |
|-------------------|----------|------------------------------------------|
| player_id         | int64    | Player unique ID                         |
| season            | int32    | Transfer season year                     |
| first_name        | string   | Player first name                        |
| last_name         | string   | Player last name                         |
| origin_team       | string   | Team transferring from                   |
| destination_team  | string   | Team transferring to (null if uncommitted)|
| position          | string   | Player position                          |
| stars             | int32    | Star rating in portal                    |
| transfer_date     | date32   | Date entered transfer portal             |
| source            | string   | Data vendor provenance                   |

### 4.24 returning_production

| Field               | Type     | Description                              |
|---------------------|----------|------------------------------------------|
| team                | string   | Team name or abbreviation                |
| team_id             | int64    | Team unique ID                           |
| season              | int32    | Season year                              |
| ppa_usage           | float64  | PPA usage metric                         |
| total_ppa           | float64  | Total PPA returning                      |
| ppa_passing         | float64  | Passing PPA returning                    |
| ppa_receiving       | float64  | Receiving PPA returning                  |
| ppa_rushing         | float64  | Rushing PPA returning                    |
| percent_ppa         | float64  | Percentage of total PPA returning        |
| percent_passing     | float64  | Percentage of passing production         |
| percent_receiving   | float64  | Percentage of receiving production       |
| percent_rushing     | float64  | Percentage of rushing production         |
| source              | string   | Data vendor provenance                   |

### 4.25 venues

| Field             | Type     | Description                           |
|-------------------|----------|---------------------------------------|
| id                | int64    | Unique venue identifier               |
| name              | string   | Venue name                            |
| city              | string   | City                                  |
| state             | string   | State                                 |
| zip               | string   | ZIP code                              |
| capacity          | int32    | Seating capacity                      |
| year_constructed   | int32   | Year venue was built                  |
| grass             | bool     | Natural grass field                   |
| dome              | bool     | Dome/roof venue                       |
| timezone          | string   | IANA timezone                         |
| elevation         | float64  | Elevation in feet                     |
| source            | string   | Data vendor provenance                |

### 4.26 media

| Field              | Type         | Description                       |
|--------------------|--------------|-----------------------------------|
| game_id            | int64        | Game identifier                   |
| season             | int32        | Season year                       |
| week               | int32        | Week number                       |
| tv_network         | string       | Broadcasting TV network           |
| start_time         | timestamp[s] | Scheduled start time (UTC)        |
| outlet             | string       | Media outlet                      |
| is_home_blackout   | bool         | Home market blackout flag         |
| source             | string       | Data vendor provenance            |

---

## 5. Partitioning Strategy

### Rules

| Strategy            | Entities                                                              |
|---------------------|-----------------------------------------------------------------------|
| **No partitioning** | conferences, venues                                                   |
| **season= only**    | teams, players, standings, player_season_stats, team_season_stats, coaches, recruiting_*, ratings/*, draft, portal, returning_production |
| **season=/week=**   | games, plays, player_stats, team_stats, rankings, odds, weather, injuries, advanced/*, drives, media |

### Rationale

- **Static reference** (conferences, venues): Small datasets that rarely change;
  a single Parquet file per entity is sufficient.
- **Season-only**: Data that accumulates per season but doesn't have a meaningful
  weekly cadence (rosters, annual stats, recruiting classes, ratings).
- **Season + week**: Game-level and weekly data. The two-level partitioning lets
  DuckDB push down both predicates, so `WHERE season = 2024 AND week = 5`
  touches only one partition.

### File layout example

```
ncaaf/games/season=2024/week=05/part-0.parquet
ncaaf/games/season=2024/week=06/part-0.parquet
ncaaf/teams/season=2024/part-0.parquet
ncaaf/conferences/part-0.parquet
```

---

## 6. Migration Guide (Old Path → New Path)

| Old path (v1 layout)                              | New path (v2 layout)                |
|----------------------------------------------------|-------------------------------------|
| `ncaaf/reference/conferences/base/`                | `ncaaf/conferences/`                |
| `ncaaf/team/identity/base/`                        | `ncaaf/teams/`                      |
| `ncaaf/team/identity/roster/`                      | `ncaaf/players/`                    |
| `ncaaf/game/schedule/base/`                        | `ncaaf/games/`                      |
| `ncaaf/game/play_by_play/plays/`                   | `ncaaf/plays/`                      |
| `ncaaf/game/box/players/`                          | `ncaaf/player_stats/`               |
| `ncaaf/season/team_stats/base/`                    | `ncaaf/team_stats/`                 |
| `ncaaf/player/season_stats/base/`                  | `ncaaf/player_season_stats/`        |
| `ncaaf/season/team_stats/advanced/`                | `ncaaf/team_season_stats/`          |
| `ncaaf/team/record/standings/`                     | `ncaaf/standings/`                  |
| `ncaaf/season/rankings/polls/`                     | `ncaaf/rankings/`                   |
| `ncaaf/market/odds/lines/`                         | `ncaaf/odds/`                       |
| `ncaaf/team/context/staff/`                        | `ncaaf/coaches/`                    |
| *(new — no v1 equivalent)*                         | `ncaaf/weather/`                    |
| `ncaaf/player/identity/injury/`                    | `ncaaf/injuries/`                   |
| `ncaaf/team/recruiting/class/`                     | `ncaaf/recruiting_classes/`         |
| `ncaaf/player/identity/recruit/`                   | `ncaaf/recruiting_players/`         |
| `ncaaf/team/recruiting/groups/`                    | `ncaaf/recruiting_groups/`          |
| `ncaaf/team/ratings/elo/`                          | `ncaaf/ratings/elo/`                |
| `ncaaf/team/ratings/sp/`                           | `ncaaf/ratings/sp/`                 |
| `ncaaf/team/ratings/fpi/`                          | `ncaaf/ratings/fpi/`                |
| `ncaaf/team/ratings/srs/`                          | `ncaaf/ratings/srs/`                |
| `ncaaf/team/ratings/talent/`                       | `ncaaf/ratings/talent/`             |
| `ncaaf/team/ratings/sp_conference/`                | `ncaaf/ratings/sp_conference/`      |
| `ncaaf/game/advanced/epa/`                         | `ncaaf/advanced/epa/`               |
| `ncaaf/game/advanced/ppa/`                         | `ncaaf/advanced/ppa/`               |
| `ncaaf/game/advanced/havoc/`                       | `ncaaf/advanced/havoc/`             |
| `ncaaf/game/advanced/win_prob/`                    | `ncaaf/advanced/win_probability/`   |
| `ncaaf/game/play_by_play/drives/`                  | `ncaaf/drives/`                     |
| `ncaaf/player/identity/draft/`                     | `ncaaf/draft/`                      |
| `ncaaf/player/portal/base/`                        | `ncaaf/portal/`                     |
| `ncaaf/player/returning/base/`                     | `ncaaf/returning_production/`       |
| `ncaaf/reference/venues/base/`                     | `ncaaf/venues/`                     |
| `ncaaf/game/schedule/media/`                       | `ncaaf/media/`                      |

---

## 7. Vendor Priority Summary

| Entity                   | Primary            | Secondary      | Tertiary    |
|--------------------------|--------------------|----------------|-------------|
| conferences              | cfbdata            | espn           | —           |
| teams                    | cfbdata            | espn           | —           |
| players                  | cfbdata            | espn           | —           |
| games                    | espn               | cfbdata        | —           |
| plays                    | cfbdata            | espn           | —           |
| player_stats             | cfbdata            | espn           | —           |
| team_stats               | cfbdata            | espn           | —           |
| player_season_stats      | cfbdata            | espn           | —           |
| team_season_stats        | cfbdata            | espn           | —           |
| standings                | espn               | cfbdata        | —           |
| rankings                 | cfbdata            | espn           | —           |
| odds                     | odds               | oddsapi        | espn        |
| coaches                  | cfbdata            | —              | —           |
| weather                  | openweather        | cfbdata        | —           |
| injuries                 | espn               | —              | —           |
| recruiting_classes       | cfbdata            | 247sports      | —           |
| recruiting_players       | cfbdata            | 247sports      | —           |
| recruiting_groups        | cfbdata            | —              | —           |
| ratings/*                | cfbdata            | espn (FPI)     | —           |
| advanced/*               | cfbdata            | —              | —           |
| drives                   | cfbdata            | espn           | —           |
| draft                    | cfbdata            | espn           | —           |
| portal                   | cfbdata            | —              | —           |
| returning_production     | cfbdata            | —              | —           |
| venues                   | cfbdata            | espn           | —           |
| media                    | cfbdata            | espn           | —           |

---

## 8. Example DuckDB Queries

### Read all games for 2024 season, week 5

```sql
SELECT *
FROM read_parquet('data/normalized_curated/ncaaf/games/season=2024/week=5/*.parquet',
                  hive_partitioning=true);
```

### Get top-25 AP rankings for latest week

```sql
SELECT team, rank, points, record
FROM read_parquet('data/normalized_curated/ncaaf/rankings/**/*.parquet',
                  hive_partitioning=true)
WHERE season = 2024 AND poll = 'AP'
ORDER BY rank
LIMIT 25;
```

### Join games with odds

```sql
WITH g AS (
    SELECT id AS game_id, date, home_team, visitor_team, home_score, visitor_score
    FROM read_parquet('data/normalized_curated/ncaaf/games/**/*.parquet',
                      hive_partitioning=true)
    WHERE season = 2024
),
o AS (
    SELECT game_id, vendor, spread_home_value, moneyline_home, total_value
    FROM read_parquet('data/normalized_curated/ncaaf/odds/**/*.parquet',
                      hive_partitioning=true)
    WHERE season = 2024
)
SELECT g.date, g.home_team, g.visitor_team,
       g.home_score, g.visitor_score,
       o.vendor, o.spread_home_value, o.total_value
FROM g
JOIN o ON g.game_id = o.game_id
ORDER BY g.date;
```

### Team EPA trends across weeks

```sql
SELECT team, week, epa_overall, epa_passing, epa_rushing
FROM read_parquet('data/normalized_curated/ncaaf/advanced/epa/**/*.parquet',
                  hive_partitioning=true)
WHERE season = 2024 AND team = 'Alabama'
ORDER BY week;
```

### Weather impact analysis

```sql
SELECT w.conditions, w.temperature, w.wind_speed,
       g.home_score + g.visitor_score AS total_points
FROM read_parquet('data/normalized_curated/ncaaf/weather/**/*.parquet',
                  hive_partitioning=true) w
JOIN read_parquet('data/normalized_curated/ncaaf/games/**/*.parquet',
                  hive_partitioning=true) g
  ON w.game_id = g.id AND w.season = g.season AND w.week = g.week
WHERE w.season = 2024 AND w.dome = false
ORDER BY total_points DESC;
```

### Recruiting class comparison

```sql
SELECT team, season, rank, total_commits, five_star, four_star, avg_rating
FROM read_parquet('data/normalized_curated/ncaaf/recruiting_classes/**/*.parquet',
                  hive_partitioning=true)
WHERE season BETWEEN 2020 AND 2024
ORDER BY season, rank
LIMIT 50;
```

### Cross-entity: player stats + injuries

```sql
SELECT ps.player, ps.team, ps.passing_yards, ps.passing_td,
       i.status AS injury_status, i.body_part
FROM read_parquet('data/normalized_curated/ncaaf/player_stats/**/*.parquet',
                  hive_partitioning=true) ps
LEFT JOIN read_parquet('data/normalized_curated/ncaaf/injuries/**/*.parquet',
                       hive_partitioning=true) i
  ON ps.player_id = i.player_id AND ps.season = i.season AND ps.week = i.week
WHERE ps.season = 2024 AND ps.week = 5;
```

---

## 9. Schema Programmatic Access

All schemas are defined in `backend/normalization/ncaaf_schemas.py`:

```python
from normalization.ncaaf_schemas import (
    NCAAF_SCHEMAS,       # dict[str, pa.Schema]  — all schemas
    PARTITION_KEYS,      # dict[str, list[str]]   — partition columns
    NCAAF_ENTITY_PATHS,  # dict[str, str]         — relative paths
)

# Get schema for a specific entity
games_schema = NCAAF_SCHEMAS["games"]

# Get partition keys
assert PARTITION_KEYS["games"] == ["season", "week"]

# Get relative path
assert NCAAF_ENTITY_PATHS["ratings/elo"] == "ratings/elo"
```

---

*Document generated for NCAAF normalized_curated redesign v2.0*
