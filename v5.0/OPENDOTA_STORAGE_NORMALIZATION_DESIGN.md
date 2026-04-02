# OpenDota Storage + Normalization Design (v5.0)

## Goals

- Expand OpenDota endpoint coverage using the documented API surface.
- Store high-volume match data by season type, week, and date for faster incremental processing.
- Keep low-volatility metadata in a stable `reference/` namespace.
- Preserve backward compatibility in normalization during migration.

## Raw Storage Contract

Base path:

- `data/raw/opendota/dota2/{season}/`

### 1) Reference data (season-wide)

- `reference/teams.json`
- `reference/pro_players.json`
- `reference/leagues.json`
- `reference/heroes.json`
- `reference/hero_stats.json`
- `reference/public_matches.json`
- `reference/parsed_matches.json`
- `reference/metadata.json`
- `reference/distributions.json`
- `reference/schema.json`
- `reference/health.json`
- `reference/constants/{resource}.json`
- `reference/leagues/{league_id}/matches.json`
- `reference/leagues/{league_id}/match_ids.json`
- `reference/leagues/{league_id}/teams.json`
- `reference/teams/{team_id}/matches.json`
- `reference/teams/{team_id}/players.json`
- `reference/teams/{team_id}/heroes.json`
- `reference/pro_matches_index.json`

### 2) Event data (partitioned)

- `season_types/regular/weeks/week_{WW}/dates/{YYYY-MM-DD}/pro_matches/{match_id}.json`
- `season_types/regular/weeks/week_{WW}/dates/{YYYY-MM-DD}/matches/{match_id}.json`
- `season_types/regular/weeks/week_{WW}/dates/{YYYY-MM-DD}/snapshots/live.json`

Notes:

- OpenDota does not provide a direct season-type taxonomy for all endpoints; `regular` is used as canonical partition.
- Week partition uses ISO week from `start_time`.

## Importer Endpoint Coverage

The OpenDota provider now supports:

- Existing: `pro_matches`, `matches`, `teams`, `players`, `leagues`, `heroes`, `hero_stats`
- Added: `public_matches`, `parsed_matches`, `metadata`, `distributions`, `schema`, `health`, `live`, `constants`, `league_matches`, `league_match_ids`, `league_teams`, `team_matches`, `team_players`, `team_heroes`, `top_players`, `search`, `rankings`, `benchmarks`, `records`, `scenarios_item_timings`, `scenarios_lane_roles`, `scenarios_misc`, `hero_matches`, `hero_matchups`, `hero_durations`, `hero_players`, `hero_item_popularity`, `league_details`, `team_details`, `player_details`, `player_wl`, `player_recent_matches`, `player_matches`, `player_heroes`, `player_peers`, `player_pros`, `player_totals`, `player_counts`, `player_wardmap`, `player_wordcloud`, `player_ratings`, `player_rankings`

Intentional exclusions from automatic importer execution:

- `POST /request/{match_id}` (mutating parse request)
- `GET /request/{jobId}` (requires job IDs from prior POST)
- `GET /explorer` (arbitrary SQL endpoint)
- `GET /findMatches` (requires explicit hero-team query arrays)

## Normalization Contract Updates

Normalization remains output-compatible and keeps writing:

- `data/normalized/dota2/games_{season}.parquet`
- `data/normalized/dota2/player_stats_{season}.parquet`
- `data/normalized/dota2/teams_{season}.parquet`
- `data/normalized/dota2/players_{season}.parquet`
- `data/normalized/dota2/standings_{season}.parquet`

Loader compatibility behavior:

- Prefer new structured paths under `reference/` and `season_types/.../dates/.../matches/`.
- Fallback to legacy flat paths (`teams.json`, `pro_players.json`, `matches/*.json`) when structured paths do not exist.

## Migration

`v5.0/scripts/reorganize_raw_data.py` now supports OpenDota migration:

- Moves legacy season-root files to `reference/`.
- Converts `pro_matches.json` into:
  - `reference/pro_matches_index.json`
  - date/week-partitioned `pro_matches/{match_id}.json`
- Moves legacy `matches/{match_id}.json` into date/week partitions.

Default providers for reorganization now include `opendota`.
