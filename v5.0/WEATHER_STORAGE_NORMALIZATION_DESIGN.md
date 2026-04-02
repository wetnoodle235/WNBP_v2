# Weather Storage + Normalization Design (Visual Crossing)

## Scope
This design defines how weather data is collected and stored for sports with active games, and how it is normalized for downstream model/API use.

## Collection Strategy
- Source schedule: ESPN scoreboard by sport/date.
- Source weather: Visual Crossing timeline API.
- Default window: today only.
- Analysis window: configurable via `--recent-days` (example: 7 for last week).
- Gating: only sports that actually have games in the date window generate weather calls.
- De-duplication: city+date weather calls are de-duplicated across all requested sports in a single importer run.

## Raw Layout
Canonical layout:

- `data/raw/weather/{sport}/{season}/dates/{YYYY-MM-DD}/cities/{city_slug}/weather.json`
- `data/raw/weather/{sport}/{season}/dates/{YYYY-MM-DD}/games/{game_id}.json`

Rationale:
- `season`: keeps weather aligned with existing sport season partitioning.
- `date`: enables simple day-scoped inspection and backfill control.
- `cities`: stores one fetched weather payload per city/day per sport-season partition.
- `games`: stores game-level weather records used by normalizer (`game_id` keyed).

## Raw Record Shapes
City payload (`cities/*/weather.json`):
- `source`, `provider`, `sport`, `season`, `date`
- `city`, `state`, `country`, `location_query`
- `fetched_at`
- `weather` (full Visual Crossing response)

Game payload (`games/{game_id}.json`):
- `source`, `sport`, `season`, `game_id`, `date`, `venue`
- `city`, `state`, `country`
- `temp_f`, `wind_mph`, `wind_direction`
- `humidity_pct`, `precipitation`, `condition`, `dome`
- `provider`, `fetched_at`

## Normalized Output
Normalized target:
- `data/normalized/{sport}/weather_{season}.parquet`

Schema (existing canonical `Weather` schema):
- `source`
- `game_id`
- `temp_f`
- `wind_mph`
- `wind_direction`
- `humidity_pct`
- `precipitation`
- `condition`
- `dome`

Merge key:
- `game_id`

Provider priority:
- `weather` provider is used for `weather` data type where configured.

## Reorganization Support
`reorganize_raw_data.py` includes weather migration support for legacy venue/date files into the new `dates/{date}/cities/{city}/weather.json` layout.

## Operational Notes
- Run example for last week analysis:
  - `npm run import:weather -- --sports=<csv> --seasons=<year> --recent-days=7`
- For day-of operations, omit `--recent-days` to only ingest weather for today.
