# Understat Storage + Normalization Design (v5.0)

## Review Outcome

- Keep Understat as a complementary soccer analytics provider focused on expected-goals (xG) and shot-level quality signals.
- Do not use Understat as a complete replacement for schedule/standings providers (for example ESPN and football-data.org).
- Understat is now collected via AJAX endpoints (not legacy HTML embedded JSON), aligned with the endpoint model used by the `collinb9/understatAPI` project.
- Since no historical Understat raw files were present in this repo, no legacy migration is required before adopting the new layout.

## Endpoint Coverage Decision

### Legacy-compatible default endpoints

- `league_standings`
- `league_matches`
- `player_xg`
- `team_xg`

These remain the default import set for backwards compatibility and predictable runtime.

### Extended endpoints (opt-in)

- `team_matches`
- `team_players`
- `team_context`
- `player_matches`
- `player_shots`
- `player_seasons`
- `match_shots`
- `match_rosters`

These map to the same endpoint families exposed by `understatAPI` (`league`, `team`, `player`, `match`) and should be used for richer xG/shot modeling workflows.

## Recommended Raw Storage Contract

Base path:

- `data/raw/understat/{sport}/{season}/`

### 1) Season reference files

- `reference/league_standings.json`
- `reference/league_players.json`
- `reference/team_xg.json`

Compatibility files kept at season root:

- `league_standings.json`
- `player_xg.json`
- `team_xg.json`
- `league_matches.json`

### 2) Match index + structured match partitions

- `matches/index.json`
- `matches/season_type/regular/week_{WW}/{YYYY-MM-DD}/{match_id}/match.json`
- `matches/season_type/regular/week_{WW}/{YYYY-MM-DD}/{match_id}/shots.json`
- `matches/season_type/regular/week_{WW}/{YYYY-MM-DD}/{match_id}/rosters.json`

### 3) Team endpoint outputs

- `teams/{team_slug}__{team_id}/matches.json`
- `teams/{team_slug}__{team_id}/players.json`
- `teams/{team_slug}__{team_id}/context.json`

### 4) Player endpoint outputs

- `players/{player_slug}__{player_id}/matches.json`
- `players/{player_slug}__{player_id}/shots.json`
- `players/{player_slug}__{player_id}/seasons.json`

## Why This Layout

- `season_type/week/date/game` partitioning mirrors the requested temporal and game-centric organization while staying practical for soccer league analysis.
- Week partitioning uses ISO week labels, which simplifies rolling-window backfills and weekly model refreshes.
- Date + match-id folders make targeted debugging and per-match re-imports straightforward.
- Team/player folders are keyed with both slug and id to keep paths readable without losing stable identity.

## Normalization Contract (Recommended)

Understat should be treated as an enrichment provider for existing soccer normalized outputs, not a standalone canonical game source.

Recommended normalized usage:

- Join `matches/index.json` and per-match `shots.json` by `match_id` to derive shot quality aggregates.
- Build game-level enrichments: xG for/against, xG differential, shot count, big chance conversion, xG per shot.
- Build player-level enrichments from `players/*/shots.json` and `players/*/seasons.json`.
- Build team trend features from `teams/*/context.json` and `teams/*/matches.json`.

No new normalized parquet contracts are required immediately; existing soccer outputs can be incrementally enriched with optional Understat-derived columns.

## Operational Notes

- Provider supports: `epl`, `laliga`, `bundesliga`, `seriea`, `ligue1`.
- No API key is required.
- Endpoint requests must include the AJAX header `X-Requested-With: XMLHttpRequest`.
- If Understat materially changes endpoint behavior in the future, disable provider execution and retain this layout contract for quick reactivation.
