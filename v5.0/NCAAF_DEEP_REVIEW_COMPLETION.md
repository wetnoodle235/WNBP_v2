# NCAAF Deep Review Completion (Remaining Endpoints)

Date: 2026-04-02

## Outcome

All remaining CFBData raw endpoint groups are now represented in the ncaaf blended design either as:

1. `implemented` (loader + provider map + normalize method + routing)
2. `implemented_guarded` (loader exists and safely returns zero when raw files are absent)

No raw endpoint group is untracked.

## Coverage Summary

- Raw CFBData ncaaf endpoint groups discovered: **52**
- Missing routing rows: **0**
- Endpoint groups with non-implemented status: **0**

## Remaining Endpoint Wave Implemented

Implemented in this wave:

1. `calendar`
2. `game_box_advanced`
3. `scoreboard`
4. `metrics_wp` (guarded)
5. `ppa_players_games`
6. `ppa_predicted`

Previously completed in prior wave:

1. `games_players`
2. `lines`
3. `plays`
4. `roster`
5. `stats_advanced`
6. `stats_player_season`
7. `stats_season`
8. `info`

## Data Presence Notes (No-Clutter Review)

- `scoreboard`: raw endpoint exists but sampled payloads are empty lists in local dataset; loader is implemented and zero-safe.
- `metrics_wp`: no raw files found locally; loader is implemented_guarded and zero-safe.
- `calendar`: lightweight metadata endpoint; small row footprint and routed under `reference/calendar/windows`.

## Clutter Audit

Top-level curated folders for ncaaf remain compact and intentional:

1. `game`
2. `market`
3. `player`
4. `reference`
5. `season`
6. `team`

Top-level major folders count remains **6** (no flat-folder regression).

## New/Relevant Routed Paths Added

1. `game/box/advanced`
2. `game/schedule/scoreboard`
3. `reference/calendar/windows`
4. `reference/metrics/wp` (guarded, currently no rows)
5. `player/game_stats/ppa`
6. `season/ppa/predicted`

## DuckDB Verification (new remaining endpoints)

- `ncaaf_reference_calendar_windows`: populated
- `ncaaf_game_box_advanced`: populated
- `ncaaf_game_schedule_scoreboard`: not present (no normalized rows emitted from empty source)
- `ncaaf_reference_metrics_wp`: not present (no source files)
- `ncaaf_player_game_stats_ppa`: populated
- `ncaaf_season_ppa_predicted`: populated

## Conclusion

NCAAF endpoint implementation is now complete for all discovered CFBData raw endpoint groups, with guarded behavior for no-data endpoints and a maintained low-clutter major/minor/type storage design.
