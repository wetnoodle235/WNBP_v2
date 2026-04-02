# StatsBomb Storage + Normalization Design (v5.0)

## Review Outcome

- Endpoint coverage was incomplete: `three-sixty/{match_id}.json` (freeze-frame tracking context) was not imported.
- Match ingestion had a silent truncation risk when multiple competition-season IDs map to one sport-season, because a single `matches.json` file was reused as both destination and skip guard.
- Raw organization can be improved by using match-centric partitioning so all match artifacts (`events`, `lineups`, `three_sixty`) are colocated.

## Updated Raw Storage Contract

Base path:

- `data/raw/statsbomb/{sport}/{season}/`

### 1) Reference

- `reference/competitions.json`
- `reference/competition_seasons.json`

### 2) Match Index + Source Partitions

- `matches/index.json` (deduplicated aggregate of all match rows)
- `matches/by_competition/{competition_id}/{season_id}.json` (source partition files)

### 3) Match-Centric Endpoint Files

- `matches/{match_id}/events.json`
- `matches/{match_id}/lineups.json`
- `matches/{match_id}/three_sixty.json` (when available)

## Compatibility Behavior

- Importer continues writing legacy aggregate `matches.json` for existing consumers.
- Normalizer now prefers the new layout and falls back to legacy layout:
  - `matches/index.json` -> `matches.json`
  - `matches/*/events.json` -> `events/*.json`
  - `matches/*/lineups.json` -> `lineups/*.json`

## Why This Layout Is More Efficient

- Prevents data loss from multi-competition seasons by preserving per-competition partitions and deduping into a single index.
- Improves locality: normalization of player stats reads `events` and `lineups` from the same match directory.
- Enables targeted backfills/retries at match granularity without reprocessing entire season endpoint folders.

## Endpoint Coverage Decision

Default StatsBomb endpoint set should include:

- `competitions`
- `matches`
- `events`
- `lineups`
- `three_sixty`

Notes:

- `three_sixty` is not available for every open-data match; importer should treat missing resources as non-fatal and continue.

## Normalization Contract

Normalized outputs are unchanged:

- `data/normalized/{sport}/games_{season}.parquet`
- `data/normalized/{sport}/players_{season}.parquet`
- `data/normalized/{sport}/player_stats_{season}.parquet`

Loader behavior updates:

- `games`: load from `matches/index.json` (fallback `matches.json`).
- `players`: load from match-scoped `lineups.json` (fallback legacy lineup folder).
- `player_stats`: iterate match-scoped `events.json` and join with match-scoped `lineups.json` (fallback legacy folders).

## Backfill Guidance

To backfill a season with new endpoint and layout:

```bash
cd v5.0/importers
npm run import:statsbomb -- --sports=epl,laliga,bundesliga,seriea,ligue1,mls,ucl,nwsl --seasons=2020,2021,2022,2023,2024,2025,2026
```

Dry run:

```bash
cd v5.0/importers
npm run import:statsbomb -- --sports=epl,laliga,bundesliga,seriea,ligue1,mls,ucl,nwsl --seasons=2020,2021,2022,2023,2024,2025,2026 --dry-run
```
