# NCAAF Provider Gap And Priority Audit

Date: 2026-04-02

## Summary

- CFBData raw endpoint folders discovered: 52
- CFBData-mapped ncaaf kinds in provider map: 48
- Missing raw endpoints not currently mapped: 14
- Current blended curated materialized category paths (season-partitioned): 49

## Missing From NCAAF Mapping (Raw Exists, Not Yet Routed)

These endpoint folders exist under `data/raw/cfbdata/ncaaf/*/` but are not currently mapped as first-class normalized kinds:

1. `calendar`
2. `game_box_advanced`
3. `games_players`
4. `info`
5. `lines`
6. `metrics_wp`
7. `plays`
8. `ppa_players_games`
9. `ppa_predicted`
10. `roster`
11. `scoreboard`
12. `stats_advanced`
13. `stats_player_season`
14. `stats_season`

## Vendor Capability Snapshot (NCAAF Raw)

- `cfbdata`: 52 endpoint folders across 7 seasons
- `espn`: 5 endpoint folders across 7 seasons
  - `depth_charts`, `events`, `reference`, `snapshots`, `teams`
- `oddsapi`: 2 endpoint folders (`events`, `scores`) in 1 season root
- `odds`: no `ncaaf` raw root discovered in this local dataset snapshot
- `weather`: no `ncaaf` raw root discovered in this local dataset snapshot

## Priority Strategy Implemented

To reduce CFBData quota pressure while preserving CFBData depth where needed, ncaaf defaults were switched to ESPN-first for baseline merged domains:

- `games`: `espn > cfbdata`
- `teams`: `espn > cfbdata`
- `players`: `espn > cfbdata`
- `standings`: `espn > cfbdata`
- `player_stats`: `espn > cfbdata`
- `team_stats`: `espn > cfbdata`
- `stats`: `espn > cfbdata`
- `coaches`: `espn > cfbdata`

CFBData remains primary for advanced/unique categories (ratings, ppa, recruiting, wp_pregame, stats_game_advanced/havoc, teams_fbs, venues, etc.).

## Recommended Completion Checklist Before Moving To Next Sport

1. Implement/loaders+routing for the 14 missing CFBData endpoint groups above.
2. Add route rows for each new normalized kind to `config/normalized_blended_routing_registry.csv`.
3. Add field-priority defaults and key label overrides in `config/field_vendor_priority_registry.csv`.
4. Rebuild ncaaf curated + refresh DuckDB.
5. Re-run coverage script and confirm no raw endpoint groups remain unrepresented.
