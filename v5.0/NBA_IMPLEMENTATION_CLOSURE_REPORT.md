# NBA Implementation Closure Report

Date: 2026-04-03

## Summary

NBA baseline normalization and routing coverage is now implemented and no longer marked as planned.

## Changes Completed

Loader and merge coverage:

- Added nbastats team season stats loader: _nbastats_team_stats.
- Registered loader in PROVIDER_LOADERS for (nbastats, team_stats).
- Mapped canonical team_stats fields used by curated output.
- Added two_point_fg_pct, scoring_efficiency, shooting_efficiency, and assist_turnover_ratio.

Provider priority and governance:

- Updated NBA provider priority for team_stats from espn to nbastats > espn.
- Updated NBA field-level default priority for team_stats to match.
- Promoted all NBA rows in blended routing registry from planned to implemented.

Aggregate and read-path baseline now aligned:

- nba_market_odds_latest
- nba_game_daily_snapshot
- nba_team_season_rollup
- nba_team_recent_form

## Validation Results

Registry checks:

- python3 scripts/validate_blended_registry.py
- Result: pass

Syntax checks:

- python3 -m py_compile normalization/normalizer.py normalization/provider_map.py
- Result: pass

Focused runtime check:

- Normalizer().run_sport('nba', ['2026'], ['team_stats'])
- Result: {'team_stats': 60}

Output spot-check:

- Curated file: data/normalized_curated/nba/season/team_stats/base/season=2026/part.parquet
- Rows: 60
- Source mix: espn: 30, nbastats: 30
- Canonical fields populated with non-null counts of 60 each for two_point_fg_pct, scoring_efficiency, shooting_efficiency, and assist_turnover_ratio.

## Known Remaining Gap

- Season-type coverage is still regular-season-centric in curated outputs for this local snapshot. Raw sources include playoffs, preseason, and postseason variants, but API-facing partitions remain primarily regular for NBA team and player season rollups.

## Deeper Parity Pass (NCAAF Alignment)

To move NBA closer to NCAAF-style depth, blended governance now explicitly includes additional implemented NBA kinds that were already loader-capable in the normalizer:

- play_by_play
- drives
- coaches (implemented_guarded due to source sparsity by season)

Governance updates performed:

- Added NBA rows for `play_by_play`, `drives`, and `coaches` in `config/normalized_blended_routing_registry.csv`.
- Added NBA field-vendor defaults for `play_by_play`, `drives`, and `coaches` in `config/field_vendor_priority_registry.csv`.

This keeps NBA runtime behavior and registry declarations aligned for deeper endpoint-family coverage.

Focused runtime validation for deeper kinds (updated):

- `Normalizer().run_sport('nba', ['2026'], ['play_by_play', 'drives', 'coaches'])`
- Result: `{'coaches': 30, 'play_by_play': 623555, 'drives': 0}`

Interpretation:

- `play_by_play` is now active for NBA using `nbastats` as primary source.
- Curated output confirms source mix `{'nbastats': 623555}` for season 2026 play events.
- `coaches` remains active but sparse by source-season coverage.
- `drives` remains `implemented_guarded` because basketball payloads do not expose football-style drive blocks.
