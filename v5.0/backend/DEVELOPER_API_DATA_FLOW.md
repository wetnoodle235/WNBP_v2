# V5.0 Backend Developer API and Data Flow

## End-to-end data flow

The backend now treats DuckDB over curated parquet outputs as the primary read path.

1. Provider raw ingestion

- Source adapters write raw payloads to `v5.0/data/raw/{provider}/...`.

1. Normalization

- Normalizers convert provider payloads to normalized sport/kind parquet files in `v5.0/data/normalized/{sport}/{kind}_{season}.parquet`.

1. Curated normalization output

- Curated builder writes category-oriented partitioned parquet datasets to `v5.0/data/normalized_curated/{sport}/{category}/season={season}/...`.

1. DuckDB catalog

- `DuckDBCatalog` registers curated categories as views (`{sport}_{category}`), including nested categories normalized to underscore names.
- Example: `season_averages/team_stats` becomes `{sport}_season_averages_team_stats`.

1. Backend read layer

- API routes call `DataService`.
- `DataServiceDuckDB` resolves kinds to curated DuckDB views first.
- If a view is missing or a query fails, service falls back to legacy parquet read for compatibility.

## API docs and local development behavior

Docs/OpenAPI are localhost-only by default.

- `GET /docs` is available only from loopback clients.
- `GET /openapi.json` is available only from loopback clients.
- `GET /openapi-sellable.json` is available only from loopback clients.

Local auth bypass is enabled for loopback clients:

- Requests from `127.0.0.1` / `::1` do not require API keys.
- Local development traffic receives synthetic enterprise-level access for endpoint/tier checks.

Non-local clients still use standard auth and tier enforcement.

## Canonical sports endpoints (curated-aligned)

Base path: `/v1/{sport}`

Core datasets:

- `/overview`
- `/games`
- `/games/{game_id}`
- `/teams`
- `/teams/{team_id}`
- `/standings`
- `/players`
- `/players/{player_id}`
- `/odds`
- `/odds/{game_id}`
- `/predictions`
- `/injuries`
- `/news`

Curated-aligned advanced/stat datasets (snake_case canonical):

- `/player_stats`
- `/advanced_stats`
- `/match_events`
- `/ratings`
- `/market_signals`
- `/schedule_fatigue`
- `/team_stats`
- `/team_game_stats`
- `/batter_game_stats`
- `/pitcher_game_stats`
- `/transactions`
- `/schedule`
- `/simulation`
- `/live_predictions`
- `/injuries_impact`
- `/games/{game_id}/weather`

Backward-compatible legacy aliases remain for previous kebab-case routes, but canonical usage should move to snake_case.

## Kind to curated view mapping examples

`DataServiceDuckDB` maps API kinds to views such as:

- `games` -> `{sport}_games`
- `teams` -> `{sport}_teams`
- `players` -> `{sport}_players`
- `player_stats` -> `{sport}_player_stats`
- `team_stats` -> `{sport}_team_stats`
- `team_game_stats` -> `{sport}_team_game_stats`
- `advanced_stats` -> `{sport}_advanced_stats`
- `advanced_batting` -> `{sport}_advanced_batting`
- `match_events` -> `{sport}_match_events`
- `market_signals` -> `{sport}_market_signals`
- `schedule_fatigue` -> `{sport}_schedule_fatigue`
- `ratings` -> `{sport}_ratings`
- `transactions` -> `{sport}_transactions`
- `weather` -> `{sport}_weather`

## Operational notes

- Curated view discovery supports nested category folders and sanitizes names into valid view identifiers.
- `get_seasons` reads season values directly from curated views when available.
- `list_available_sports` reports data-type availability from curated views in DuckDB mode.
