# UFC Stats Storage + Normalization Design (V5.0)

## Scope
This document defines UFC raw storage organization and normalization behavior for provider `ufcstats`.

It also records endpoint coverage review against `shanktt/ufcstats`.

## Endpoint Coverage Review
Reference reviewed: `https://github.com/shanktt/ufcstats`.

`shanktt/ufcstats` exposes one primary capability:
- fighter profile scraping (bio + career rate stats from fighter pages)

V5.0 UFC importer previously covered:
- `events`
- `fights`
- `fighter_stats`

Missing endpoint now added:
- `fighter_profiles`

## Raw Data Organization
Canonical layout for UFC (`data/raw/ufcstats/ufc/{season}`):

```text
reference/
  events.json
  fighter_profiles_index.json
  fighters/
    {fighter_id}.json

season_types/
  regular/
    weeks/
      week_XX/
        dates/
          YYYY-MM-DD/
            events/
              {event_id}/
                event.json
                fights.json
                fighter_stats/
                  {fight_id}.json
```

### Compatibility Policy
Legacy flat files are still supported and may still exist:

```text
events.json
fights/{event_id}.json
fighter_stats/{fight_id}.json
```

Importer behavior:
- Reads legacy files when present.
- Writes canonical structured files.
- Maintains backward compatibility for consumers still expecting legacy files.

Normalizer behavior:
- Reads both canonical structured files and legacy files.
- Uses canonical when present.
- Falls back to legacy automatically.

## Normalized Data Types
Provider map for sport `ufc` remains:
- `games` <- `ufcstats`
- `players` <- `ufcstats`
- `player_stats` <- `ufcstats`

### Enrichment from Fighter Profiles
When available, normalization now enriches UFC records with profile-derived fields:
- game-level: `home_reach`, `away_reach`, `home_height`, `away_height`, `home_age`, `away_age`
- player-level: `height`, `weight`, `birth_date`, `position` (stance)

## Operational Commands
Backfill 2020-2026 core UFC endpoints:

```bash
cd v5.0/importers
npm run import:ufcstats -- --sports=ufc --seasons=2020,2021,2022,2023,2024,2025,2026 --endpoints=events,fights,fighter_stats
```

Backfill fighter profiles (optional, heavier):

```bash
cd v5.0/importers
npm run import:ufcstats -- --sports=ufc --seasons=2020,2021,2022,2023,2024,2025,2026 --endpoints=fighter_profiles
```

Normalize UFC datasets after import:

```bash
cd v5.0/backend
python3 -m normalization.normalizer --sports ufc --seasons 2020 2021 2022 2023 2024 2025 2026 --data-types games players player_stats
```
