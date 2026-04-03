# NBA Rollout Kickoff

Date: 2026-04-02

## What Was Completed

- Added NBA blended routing bootstrap rows to `config/normalized_blended_routing_registry.csv`.
- Added NBA label-level provider priority rows to `config/field_vendor_priority_registry.csv`.
- Added missing NBA `market_signals` provider mapping in `backend/normalization/provider_map.py`.
- Added NBA `nbastats` loader for `team_stats` and registered it in `backend/normalization/normalizer.py`.
- Updated NBA provider ordering for `team_stats` to `nbastats > espn`.
- Promoted NBA blended routing rows from `planned` to `implemented` in `config/normalized_blended_routing_registry.csv`.
- Registry validation now passes.
- Extended targeted aggregate build to cover both NCAAF and NBA with schema guards.

## NBA Premade Aggregate Tables Now Materialized

1. `nba_market_odds_latest`
2. `nba_game_daily_snapshot`
3. `nba_team_season_rollup`
4. `nba_team_recent_form`

## Current Observations

- NBA odds and games are present and now aggregate-backed.
- NBA team stats are now source-rich: normalized output includes both `nbastats` and `espn` rows, merged with `nbastats` precedence.
- DuckDB service now serves NBA odds from aggregate table and can load team stats from team rollup when queried through `_load_kind(..., "team_stats", ...)`.

## Quick Timing Snapshot (local)

- `nba_odds_all`: 68.17 ms
- `nba_market_odds_latest`: 4.24 ms
- `nba_game_core`: 30.68 ms
- `nba_game_daily_snapshot`: 5.68 ms
- `nba_team_season_rollup`: 0.55 ms
- `nba_team_recent_form`: 0.46 ms

## Next NBA Steps

1. Add season-type-aware team/player rollup variants if playoffs/preseason should be query-separable in API responses.
2. Add NBA player-season rollup table once `nba_player_stats` snapshot density is sufficient for stable season aggregates.
3. Re-run deep coverage and season-type audit after the next full import cycle.
