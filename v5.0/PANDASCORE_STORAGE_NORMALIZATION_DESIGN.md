# PandaScore Storage + Normalization Design (V5.0)

## Scope
This design documents PandaScore ingestion for esports sports used by the platform:
- `lol`
- `csgo`
- `dota2`
- `valorant`

Target seasons: `2020-2026`.

## Endpoint Coverage Review
Based on PandaScore REST references, the importer now covers these high-value fixture endpoints:
- `matches` (finished seasonal history)
- `matches_past`
- `matches_running`
- `matches_upcoming`
- `teams`
- `players`
- `leagues`
- `tournaments`
- `series`

Notes:
- We intentionally prioritized endpoints used for game schedule/results, team/player entities, and season competition structure.
- Per-game/event feeds (for example, rounds/events/frames) are available in PandaScore but are plan-sensitive and much higher volume. They are not in this phase.

## Raw Storage Layout Recommendation
For esports fixtures, a date-partitioned layout is recommended for scalability and incremental updates.

Canonical layout:
- `data/raw/pandascore/{sport}/{season}/season_types/{season_type}/weeks/week_XX/{YYYY-MM-DD}/{endpoint}/{id}.json`

Examples:
- `data/raw/pandascore/lol/2024/season_types/past/weeks/week_11/2024-03-14/matches/12345.json`
- `data/raw/pandascore/valorant/2026/season_types/upcoming/weeks/week_27/2026-07-02/matches_upcoming/99887.json`

Reference (season-wide) entities are duplicated in both legacy and reference paths:
- Legacy: `data/raw/pandascore/{sport}/{season}/{endpoint}.json`
- Reference: `data/raw/pandascore/{sport}/{season}/reference/{endpoint}/{endpoint}.json`

This keeps old readers working while enabling structured ingestion going forward.

## Backward Compatibility Strategy
The importer writes:
- Legacy flat files (`matches.json`, `teams.json`, etc.)
- New structured partition files under `season_types/...`

The normalizer reads both layouts:
- Prefers merged records across `matches`, `matches_past`, `matches_running`, `matches_upcoming`
- Falls back to legacy `matches.json`/`tournaments.json` patterns when needed

This allows existing pipelines and historical files to keep working without one-time migration downtime.

## Normalized Data Design Impact
No schema changes were required in normalized outputs.

Existing normalized targets still apply:
- `games_{season}.parquet`
- `teams_{season}.parquet`
- `players_{season}.parquet`
- `standings_{season}.parquet`
- `player_stats_{season}.parquet`

Enhancement:
- `games`/`standings`/`player_stats` loaders can now source richer and more complete match coverage from multiple match endpoint slices.

## Operational Command
Importer backfill command:

```bash
cd v5.0/importers
npm run import:pandascore -- --sports=lol,csgo,dota2,valorant --seasons=2020,2021,2022,2023,2024,2025,2026
```

Optional endpoint-limited run:

```bash
npm run import:pandascore -- --sports=lol,csgo,dota2,valorant --seasons=2020,2021,2022,2023,2024,2025,2026 --endpoints=matches,matches_past,matches_upcoming,teams,players,leagues,tournaments,series
```
