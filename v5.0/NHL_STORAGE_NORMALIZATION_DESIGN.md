# NHL Storage + Normalization Design (v5.0)

## Scope

This document defines the NHL raw-data organization and how normalization consumes it for seasons 2020-2026.

## Raw Storage Contract

Base path:

- data/raw/nhl/nhl/

Reference namespace:

- reference/teams.json
- reference/franchises.json
- reference/seasons.json
- reference/config.json

Season namespace:

- {season}/schedule/regular.json
- {season}/schedule/playoffs.json
- {season}/standings/{date}.json
- {season}/rosters/{TEAM}.json
- {season}/club_stats/{regular|playoffs}/{TEAM}.json
- {season}/team_extras/**
- {season}/league_feeds/**
- {season}/games/{regular|playoffs}/{game_id}/boxscore.json
- {season}/games/{regular|playoffs}/{game_id}/landing.json
- {season}/games/{regular|playoffs}/{game_id}/play_by_play.json
- {season}/games/{regular|playoffs}/{game_id}/right_rail.json
- {season}/shift_charts/{game_id}.json
- {season}/players/{player_id}/*.json
- {season}/stats/**
- {season}/leaders/**
- {season}/playoffs/**
- {season}/draft/**
- {season}/meta/**
- {season}/edge/**
- {season}/replays/**
- {season}/stats_misc/**

## Normalization Inputs

Normalizer source file:

- v5.0/backend/normalization/normalizer.py

NHL extraction functions:

- _nhl_games
- _nhl_standings
- _nhl_players
- _nhl_player_stats

### Layout compatibility policy

Normalization now supports:

- New organized layout under schedule/ and games/{regular|playoffs}/{game_id}/
- Legacy fallback layout (schedule.json and games/*_boxscore.json)

This allows historical compatibility while using the reorganized importer output going forward.

## Normalized Outputs

NHL provider contributes to:

- data/normalized/nhl/games_{season}.parquet
- data/normalized/nhl/standings_{season}.parquet
- data/normalized/nhl/players_{season}.parquet
- data/normalized/nhl/player_stats_{season}.parquet

## 2020-2026 completeness checks

Minimum season-level checks for NHL:

- schedule regular/playoffs files exist
- rosters directory exists with team files
- games directories exist with nested game folders
- key normalized parquet files present for each season

These checks should be run after each full NHL 2020-2026 import cycle.
