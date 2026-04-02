# OpenF1 Storage + Normalization Design (v5.0)

## Review Outcome

- Keep Ergast as the canonical historical F1 backbone for 2020-2026 race schedules, classifications, constructors, and championship tables.
- Keep OpenF1 as a complementary provider, not a replacement.
- OpenF1 is materially useful because it adds session-granular data Ergast does not provide well or at all: official session results, starting grids, race control events, team radio, overtakes, weather, telemetry-adjacent timing, and richer intra-session race state.
- OpenF1 historical coverage starts in 2023, so 2020-2022 must continue to come from Ergast alone.

## Recommended Raw Storage Contract

Base path:

- `data/raw/openf1/f1/{season}/`

### 1) Season reference

- `reference/meetings.json`
- `reference/sessions.json`

### 2) Phase / meeting / session hierarchy

- `season_phases/testing/meetings/meeting_{meeting_key}/meeting.json`
- `season_phases/testing/meetings/meeting_{meeting_key}/sessions/session_{session_key}/session.json`
- `season_phases/championship/meetings/meeting_{meeting_key}/meeting.json`
- `season_phases/championship/meetings/meeting_{meeting_key}/sessions/session_{session_key}/session.json`

Per-session endpoint files live beside `session.json`, for example:

- `.../drivers.json`
- `.../laps.json`
- `.../position.json`
- `.../weather.json`
- `.../race_control.json`
- `.../stints.json`
- `.../pit.json`
- `.../intervals.json`
- `.../team_radio.json`
- `.../overtakes.json`
- `.../session_result.json`
- `.../starting_grid.json`
- `.../championship_drivers.json`
- `.../championship_teams.json`
- `.../car_data.json`
- `.../location.json`

## Why This Layout

- F1 data is meeting-centric, not week-centric. A Grand Prix weekend is the natural unit for inspection, backfills, and debugging.
- `meeting_key` and `session_key` are stable OpenF1 identifiers, so they are better partition keys than derived ISO week folders.
- `season_phases/testing|championship` separates preseason tests from race weekends without duplicating the season reference manifests.
- Dates remain available in `meeting.json` and `session.json`, so date filtering does not require date folders in the raw tree.

## Endpoint Coverage Decision

Default collection should include the structured, analytically useful endpoints:

- `meetings`
- `sessions`
- `drivers`
- `laps`
- `position`
- `weather`
- `race_control`
- `stints`
- `pit`
- `intervals`
- `team_radio`
- `overtakes`
- `session_result`
- `starting_grid`
- `championship_drivers`
- `championship_teams`

Opt-in only due volume:

- `car_data`
- `location`

Reasoning:

- `session_result` and `starting_grid` directly improve normalized race and driver outputs.
- `team_radio`, `overtakes`, `race_control`, and `weather` are valuable event enrichments for feature engineering and qualitative review.
- `car_data` and `location` are high-volume telemetry streams and should not be default backfill targets unless specifically needed.

## Normalization Contract

Current normalized outputs remain unchanged:

- `data/normalized/f1/games_{season}.parquet`
- `data/normalized/f1/player_stats_{season}.parquet`
- `data/normalized/f1/players_{season}.parquet`
- `data/normalized/f1/teams_{season}.parquet`
- `data/normalized/f1/standings_{season}.parquet`

Loader behavior:

- Prefer the new OpenF1 structured paths under `reference/` and `season_phases/.../meetings/.../sessions/...`.
- Fallback to the legacy flat season layout (`sessions.json` and `{session_key}/...`) when structured paths do not exist.

Normalization improvements enabled by the new endpoints:

- Use `session_result.json` as the authoritative source for finish position, laps completed, and DNF/DNS/DSQ state.
- Use `starting_grid.json` to populate pole position / grid leader information.
- Continue using `laps.json`, `pit.json`, and `race_control.json` for fastest lap, pit totals, safety car counts, and red-flag counts.

## Historical Coverage Plan

- Ergast: backfill `2020,2021,2022,2023,2024,2025,2026`
- OpenF1: backfill `2023,2024,2025,2026`

This yields one combined F1 raw estate where:

- 2020-2022 rely on Ergast only.
- 2023-2026 use Ergast for canonical results/standings plus OpenF1 for session-level enrichment.

## Migration

`v5.0/scripts/reorganize_raw_data.py` should migrate legacy OpenF1 storage by:

- moving `sessions.json` to `reference/sessions.json`
- moving `meetings.json` to `reference/meetings.json` when present
- converting numeric session folders into `season_phases/{phase}/meetings/meeting_{meeting_key}/sessions/session_{session_key}/`
- writing `meeting.json` and `session.json` metadata files into the new hierarchy