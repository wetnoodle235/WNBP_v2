# NCAAF Aggregation Plan (Targeted, Low-Clutter)

Date: 2026-04-02

## Decision

We should add a small, targeted aggregate layer for NCAAF.

Reason:
- DuckDB is strong for raw scans and ad hoc queries.
- Current NCAAF footprint is manageable, but several endpoints repeatedly recompute the same groupings and joins.
- A minimal set of curated aggregates will reduce median and p95 API latency while keeping storage design clean.

This is not a broad denormalization effort. It is a narrow performance layer for high-read paths.

## Scope Guardrails

- Keep top-level blended majors unchanged: game, market, player, reference, season, team.
- Add at most 5-7 aggregate views/tables for NCAAF.
- Use one refresh mode: full rebuild during daily pipeline (no per-request rebuilds).
- Prefer SQL aggregates in DuckDB over pandas aggregation in request handlers.

## Priority Aggregates

1. game_daily_snapshot
- Purpose: fast scoreboard/schedule style reads by date.
- Grain: sport + game_date + game_id.
- Inputs: game core/schedule + latest market line + status fields.
- Destination: game/schedule/daily_snapshot.

2. market_latest_by_game_book
- Purpose: fast odds endpoint responses without scanning full odds history.
- Grain: sport + game_id + bookmaker + market_type.
- Inputs: odds live/history.
- Destination: market/odds/latest.

3. player_season_rollup
- Purpose: serve aggregate player stats directly for aggregate=true paths.
- Grain: sport + season + player_id.
- Inputs: player game stats + PPA game stats where available.
- Destination: player/season_stats/rollup.

4. team_season_rollup
- Purpose: fast team stats and rankings style pages.
- Grain: sport + season + team_id.
- Inputs: game stats + advanced team metrics.
- Destination: team/season_stats/rollup.

5. team_recent_form
- Purpose: rolling form windows used by prediction/live features.
- Grain: sport + as_of_date + team_id + window (3/5/10).
- Inputs: recent games + outcomes + margin + efficiency metrics.
- Destination: team/form/recent.

6. player_usage_recent
- Purpose: support player trend cards quickly.
- Grain: sport + as_of_date + player_id + window.
- Inputs: player game stats + usage categories.
- Destination: player/trends/usage_recent.

## Build Sequence

1. Baseline
- Capture query latency and row scan baselines for:
  - aggregate games/news/odds endpoints
  - player stats aggregate endpoint
  - team stats endpoint

2. Implement first three aggregates
- game_daily_snapshot
- market_latest_by_game_book
- player_season_rollup

3. Wire service preference
- In DuckDB-backed service, prefer these aggregate views first.
- Fallback to existing base views if aggregate views are absent.

4. Validate
- Parity checks versus current endpoint outputs.
- Row count and key uniqueness checks by grain.

5. Add second wave (if needed by latency)
- team_season_rollup
- team_recent_form
- player_usage_recent

## Acceptance Criteria

- No change to existing top-level folder count or taxonomy shape.
- Endpoint payload parity maintained for existing consumers.
- Median latency improves for aggregate-heavy endpoints.
- p95 latency improves for odds and aggregate stats reads.
- No duplicate/conflicting rows at aggregate grains.

## Risks And Mitigations

- Risk: aggregate drift from base tables.
  - Mitigation: rebuild aggregates in same pipeline stage and add parity checks.

- Risk: clutter from too many derived datasets.
  - Mitigation: hard cap aggregate count and enforce registry entries.

- Risk: stale latest-odds snapshots.
  - Mitigation: include update timestamp and refresh each pipeline run.

## NBA Rollout Note

After this NCAAF aggregate layer is in place, apply the exact same pattern to NBA:
- inventory raw endpoints
- confirm blended routing coverage
- apply label-level provider priorities
- implement missing loaders/guards
- rebuild normalized + refresh DuckDB
- run deep coverage and clutter audit

## Implementation Status (Phase 1)

Completed on 2026-04-02:
- Implemented first-wave aggregates in DuckDB catalog for NCAAF:
  - ncaaf_market_odds_latest
  - ncaaf_game_daily_snapshot
  - ncaaf_player_season_rollup
- Materialized as DuckDB base tables (not views) to avoid repeated recomputation.
- Wired DuckDB service preferences:
  - odds reads prefer ncaaf_market_odds_latest when available
  - aggregate player stats prefer ncaaf_player_season_rollup first
- Added safety fix for compatibility alias self-recursion in catalog refresh.

Validation snapshot:
- ncaaf_market_odds_latest: 8335 rows
- ncaaf_game_daily_snapshot: 7435 rows
- ncaaf_player_season_rollup: 5185 rows

Representative query timings (local):
- odds_all_2023: 13.33 ms
- odds_latest_2023: 3.18 ms
- games_core_today: 14.25 ms
- games_snapshot_today: 3.37 ms
- ppa_source_2025: 5.67 ms
- ppa_rollup_2025: 0.99 ms
