# TennisAbstract Storage + Normalization Design (v5.0)

## Goals

- Document TennisAbstract endpoint coverage and file naming quirks for ATP/WTA.
- Define the raw storage contract for 2020-2026 imports.
- Define normalization behavior into canonical ATP/WTA parquet datasets.
- Capture known upstream gaps (2025-2026 match-level files not yet published).

## Source + Endpoint Contract

Source: Jeff Sackmann public datasets on GitHub (`tennis_atp`, `tennis_wta`).

Supported endpoints by tour:

- ATP:
  - `matches` -> `atp_matches_{season}.csv`
  - `futures` -> `atp_matches_futures_{season}.csv`
  - `challengers` -> `atp_matches_qual_chall_{season}.csv`
  - `rankings` -> `atp_rankings_current.csv`
- WTA:
  - `matches` -> `wta_matches_{season}.csv`
  - `qualies` -> `wta_matches_qual_itf_{season}.csv`
  - `rankings` -> `wta_rankings_current.csv`

Intentional exclusions by tour:

- ATP `qualies` is not a separate feed (covered by `qual_chall`).
- WTA `futures` and `challengers` are not available as dedicated Sackmann files.

Importer behavior requirements:

- Sport-specific endpoint filtering is applied before fetch.
- Upstream `404` for season files is treated as an expected skip, not an import failure.
- Existing files are idempotently skipped.

## Raw Storage Contract

Base path:

- `data/raw/tennisabstract/{sport}/{season}/`

Expected files by sport:

- ATP seasons with full match data:
  - `matches.csv`
  - `futures.csv`
  - `challengers.csv`
  - `rankings_current.csv`
- WTA seasons with full match data:
  - `matches.csv`
  - `qualies.csv`
  - `rankings_current.csv`
- Seasons without published match files (currently 2025-2026):
  - `rankings_current.csv` only

## 2020-2026 Coverage Review

Backfill status after importer update:

- ATP
  - 2020-2024: complete for `matches`, `futures`, `challengers`, `rankings`
  - 2025-2026: `rankings` only (Sackmann match/futures/challenger files return 404)
- WTA
  - 2020-2024: complete for `matches`, `qualies`, `rankings`
  - 2025-2026: `rankings` only (Sackmann match/qualies files return 404)

Legacy note:

- Some pre-existing WTA `challengers.csv` files may exist from older importer behavior.
- They are not part of the current contract and are ignored by normalization.

## Normalization Contract

For both ATP and WTA, TennisAbstract contributes:

- `games_{season}.parquet`
  - Source file: `matches.csv`
  - Key fields: `id`, `date`, `home_team`, `away_team`, `surface`, `round`, set-level scoring, serve/break-point stats
- `players_{season}.parquet`
  - Source file: `matches.csv`
  - Extracted from winner/loser IDs and metadata
- `player_stats_{season}.parquet`
  - Source file: `matches.csv`
  - Per-player per-match stats (`aces`, `double_faults`, `first_serve_pct`, `result`)
- `standings_{season}.parquet`
  - Source file: `rankings_current.csv`
  - Player rankings and points for the requested season label

Provider merge priority (existing):

- ATP: TennisAbstract + ESPN for games/players/player_stats/standings
- WTA: TennisAbstract + ESPN for games/players/player_stats/standings

Implications for 2025-2026:

- Match-derived datasets may be sparse or ESPN-driven where TennisAbstract match files are unavailable.
- Standings remain populated from `rankings_current.csv`.

## Validation Checklist

- Import command:
  - `cd v5.0/importers && npm run import:tennisabstract -- --sports=atp,wta --seasons=2020,2021,2022,2023,2024,2025,2026`
- Targeted normalization command:
  - `cd v5.0/backend && /home/derek/Documents/stock/.venv/bin/python - <<'PY'`
  - `from normalization.normalizer import Normalizer`
  - `n = Normalizer()`
  - `seasons = [str(y) for y in range(2020, 2027)]`
  - `print(n.run_sport('atp', seasons, data_types=('games','players','player_stats','standings')))`
  - `print(n.run_sport('wta', seasons, data_types=('games','players','player_stats','standings')))`
  - `PY`
- Confirm outputs exist in:
  - `data/normalized/atp/` (`games_2020`..`games_2026`, etc.)
  - `data/normalized/wta/` (`games_2020`..`games_2026`, etc.)

## Operational Notes

- TennisAbstract is historical and eventually consistent; latest season files can lag.
- Treat `404` on future/current seasons as normal until upstream publishes.
- Re-run importer periodically to pick up newly published season files.
