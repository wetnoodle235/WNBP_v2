# NBA Stats Storage And Normalization Design

## Goals

- Expand `nbastats` coverage beyond season aggregates.
- Organize raw data by season type, date, and game for easier downstream access.
- Preserve compatibility with legacy flat files while migrating normalization readers.

## Raw Layout

Season root:

```text
data/raw/nbastats/nba/2025-26/
  reference/
    all_players.json
    game_index.json
    players/{PLAYER_ID}/info.json
    teams/index.json
    teams/{TEAM_ID}/info.json
    teams/{TEAM_ID}/roster.json
  regular-season/
    season_aggregates/
      league-leaders.json
      player-game-logs.json
      team-game-logs.json
      player-stats/*.json
      team-stats/*.json
      tracking-stats/*.json
    dates/YYYY-MM-DD/
      games.json
      scoreboard.json
    games/{GAME_ID}/
      summary.json
      boxscore.json
      playbyplay.json
  playoffs/
    ...same shape...
```

Legacy files remain in place under `regular-season/` and `playoffs/` so existing consumers continue to work during migration.

## Importer Coverage

Season aggregates from `stats.nba.com`:

- `leagueleaders`
- `leaguedashplayerstats`
- `leaguedashteamstats`
- `playergamelogs`
- `teamgamelogs`
- `shotchartdetail`
- `leaguedashptstats` when stats.nba.com serves the requested season/measure successfully
- `scoreboardv2`
- `commonallplayers`
- `commonplayerinfo`
- `teaminfocommon`
- `commonteamroster`

Per-game detail from NBA liveData CDN:

- `boxscore_{gameId}.json`
- `playbyplay_{gameId}.json`

## Normalization Behavior

- `players`: prefer `reference/all_players.json`, fall back to season aggregate player stats.
- `games`: prefer per-game boxscore payloads, fall back to legacy/aggregate `team-game-logs.json` pairing.
- `player_stats`: prefer per-game boxscore player rows, enrich with season-level advanced stat splits, fall back to legacy player game logs.

## Backfill Command

```bash
cd v5.0/importers
npm run import:nbastats:2020-2026
```

Dry run:

```bash
cd v5.0/importers
npm run import:nbastats:2020-2026:dry
```

Historical note: the `tracking-stats` endpoint family is still implemented in the provider, but the 2020-2026 bulk script excludes it because stats.nba.com currently returns repeated HTTP 500s for many historical `leaguedashptstats` requests.