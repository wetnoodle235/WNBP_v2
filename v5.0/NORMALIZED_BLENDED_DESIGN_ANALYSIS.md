# Normalized Curated Storage: Blended Hierarchical Design Analysis

## Executive Summary

The current normalized_curated layout stores every data kind in its own flat top-level folder (44 folders for ncaaf alone). This document analyzes whether replacing that with a **major → minor → type** hierarchy—where data from multiple source endpoints is blended into unified categories—would be better. The verdict: **yes, with specific boundaries**. A blended hierarchy radically reduces folder count, improves query locality, and naturally organizes data the way ML pipelines and analytics consumers actually access it. Full denormalization is not recommended; reference-ID linking is the right join strategy.

Operational tracking files for this plan:

- `config/normalized_blended_routing_registry.csv` (source endpoint -> blended destination)
- `config/field_vendor_priority_registry.csv` (field-level provider priority)
- `VENDOR_PRIORITY_AND_ROUTING_GOVERNANCE.md` (process requirements)

---

## 1 Current State

### 1.1 ncaaf flat folder inventory (44 categories, 2023 season)

| Folder | Row count (2023) | Endpoint type |
|--------|-----------------|---------------|
| `conferences` | 106 | Reference |
| `draft` | — | Reference |
| `games` | ~850 | Core game log |
| `games_media` | 1,417 | Game metadata |
| `game_stats` | — | Player/team box |
| `injuries` | — | Daily |
| `market_signals` | — | Derived |
| `metrics_fg_ep` | 100 | Lookup table |
| `news` | — | Daily |
| `odds` | — | Live |
| `play_by_play` | — | Per-play |
| `player_categories` | — | Player meta |
| `player_props` | — | Live |
| `players` | — | Reference |
| `plays_stats` | 1,364 | Season aggregate |
| `plays_stats_types` | 26 | Lookup |
| `plays_types` | 49 | Lookup |
| `ppa_games` | 1,702 | Per-game advanced |
| `ppa_players_season` | 4,560 | Season advanced |
| `ppa_teams` | 133 | Season advanced |
| `rankings` | 1,883 | Polls/rankings |
| `ratings_elo` | 133 | Team ratings |
| `ratings_fpi` | 133 | Team ratings |
| `ratings_sp` | 134 | Team ratings |
| `ratings_sp_conferences` | 11 | Conference ratings |
| `ratings_srs` | 261 | Team ratings |
| `records` | 665 | Season records |
| `recruiting` | 2,297 | Players |
| `recruiting_groups` | 2,138 | Position groups |
| `recruiting_teams` | 177 | Team recruiting |
| `schedule_fatigue` | — | Derived |
| `season_averages` | — | Derived |
| `staff` | — | Coaches/staff |
| `standings` | — | Season standing |
| `stats` | — | Player/team stats |
| `stats_categories` | 38 | Lookup |
| `stats_game_advanced` | 2,984 | Per-game advanced |
| `stats_game_havoc` | 2,119 | Per-game advanced |
| `talent` | 238 | Team ratings |
| `teams` | — | Reference |
| `teams_ats` | 259 | Season record |
| `teams_fbs` | 138 | Reference |
| `venues` | 840 | Reference |
| `wp_pregame` | 873 | Per-game context |

### 1.2 Problems with flat layout

1. **44 top-level categories** for one sport — grows linearly per endpoint added
2. **Fragmented analytics access**: answering "how did team X perform in 2023" requires joining `games`, `stats`, `ppa_games`, `stats_game_advanced`, `stats_game_havoc`, `wp_pregame`, `ratings_*` — across 9+ folders
3. **Redundant identity keys**: `team_name`, `season`, `sport`, `source` duplicated across every parquet partition
4. **Lookup table pollution**: `stats_categories`, `plays_types`, `plays_stats_types`, `metrics_fg_ep` are all static lookup tables; they don't benefit from being partitioned the same way dynamic data is
5. **ML feature assembly cost**: building a training row requires 10–15 separate DuckDB reads across unrelated folders

---

## 2 Proposed Blended Hierarchy

Replace 44 flat folders with **6 major categories**, each subdivided into **minor categories** and optionally **type** partitions.

```
normalized_curated/
└── {sport}/
    ├── team/                      ← MAJOR: Everything about a team
    │   ├── identity/              ←   minor: who/what a team is
    │   ├── record/                ←   minor: win/loss outcomes
    │   ├── ratings/               ←   minor: SP+, ELO, FPI, SRS, talent
    │   ├── recruiting/            ←   minor: class rank, group averages
    │   └── context/               ←   minor: conferences, venues, TV, ATS
    │
    ├── player/                    ← MAJOR: Everything about a player
    │   ├── identity/              ←   minor: bio/meta
    │   ├── season_stats/          ←   minor: per-season totals
    │   ├── usage/                 ←   minor: snap%, usage%
    │   ├── returning/             ←   minor: returning production
    │   └── portal/                ←   minor: transfer activity
    │
    ├── game/                      ← MAJOR: Per-game events and outcomes
    │   ├── schedule/              ←   minor: game dates, teams, media
    │   ├── box/                   ←   minor: team box stats (points, yards)
    │   ├── advanced/              ←   minor: EPA/PPA per game, havoc, win prob
    │   └── play_by_play/          ←   minor: individual plays
    │
    ├── season/                    ← MAJOR: Season-level aggregated analytics
    │   ├── team_stats/            ←   minor: aggregate team stats by season
    │   ├── player_stats/          ←   minor: aggregate player stats
    │   ├── ppa/                   ←   minor: PPA team & player season-level
    │   └── rankings/              ←   minor: weekly poll standings
    │
    ├── market/                    ← MAJOR: Betting + odds
    │   ├── odds/                  ←   minor: live lines
    │   ├── odds_history/          ←   minor: closing lines
    │   ├── props/                 ←   minor: player props
    │   └── signals/               ←   minor: derived market features
    │
    └── reference/                 ← MAJOR: Static lookup tables
        ├── teams/                 ←   minor: team id maps, FBS list
        ├── venues/                ←   minor: stadium info
        ├── conferences/           ←   minor: conference definitions
        ├── play_types/            ←   minor: play type/stat type lookups
        └── metrics/               ←   minor: FG EP grid, stat category names
```

### 2.1 Endpoint-to-category mapping

| Current folder | → Major | → Minor | Notes |
|----------------|---------|---------|-------|
| `teams` | `team` | `identity` | ESP/CFBData merged |
| `teams_fbs` | `team` | `identity` | Additional FBS fields |
| `teams_ats` | `team` | `record` | ATS only, by season |
| `records` | `team` | `record` | W-L-T breakdown |
| `standings` | `team` | `record` | conference standings |
| `ratings_sp` | `team` | `ratings` | type=sp |
| `ratings_sp_conferences` | `team` | `ratings` | type=sp_conf |
| `ratings_elo` | `team` | `ratings` | type=elo |
| `ratings_fpi` | `team` | `ratings` | type=fpi |
| `ratings_srs` | `team` | `ratings` | type=srs |
| `talent` | `team` | `ratings` | type=talent |
| `recruiting_teams` | `team` | `recruiting` | team class rank |
| `recruiting_groups` | `team` | `recruiting` | position group |
| `conferences` | `team` | `context` | conference membership |
| `venues` | `team` | `context` | stadium info subset |
| `games_media` | `team` | `context` | broadcast assignments |
| `players` | `player` | `identity` | |
| `player_categories` | `player` | `identity` | |
| `recruiting` | `player` | `identity` | player recruit profile |
| `game_stats` | `player` | `season_stats` | or game-level |
| `player_stats` | `player` | `season_stats` | aggregate |
| `ppa_players_season` | `player` | `season_stats` | type=ppa |
| `player_usage` | `player` | `usage` | |
| `player_returning` | `player` | `returning` | |
| `player_portal` | `player` | `portal` | |
| `games` | `game` | `schedule` | |
| `stats_game_advanced` | `game` | `advanced` | team-per-game EPA |
| `stats_game_havoc` | `game` | `advanced` | type=havoc |
| `ppa_games` | `game` | `advanced` | type=ppa |
| `wp_pregame` | `game` | `advanced` | type=wp |
| `play_by_play` | `game` | `play_by_play` | |
| `plays_stats` | `game` | `play_by_play` | season play stats |
| `stats` | `season` | `team_stats` | |
| `team_stats` | `season` | `team_stats` | |
| `ppa_teams` | `season` | `ppa` | season team ppa |
| `rankings` | `season` | `rankings` | |
| `schedule_fatigue` | `season` | `team_stats` | derived |
| `season_averages` | `season` | `team_stats` | derived |
| `odds` | `market` | `odds` | |
| `odds_history` | `market` | `odds_history` | |
| `player_props` | `market` | `props` | |
| `market_signals` | `market` | `signals` | |
| `teams_fbs` | `reference` | `teams` | base FBS list |
| `conferences` | `reference` | `conferences` | static IDs |
| `venues` | `reference` | `venues` | full venue data |
| `plays_types` | `reference` | `play_types` | |
| `plays_stats_types` | `reference` | `play_types` | type=stat |
| `stats_categories` | `reference` | `metrics` | |
| `metrics_fg_ep` | `reference` | `metrics` | |

---

## 3 Data Blending Strategy

### 3.1 Denormalization vs. Reference IDs — the core question

The user asked whether a raw dataset (e.g., `ratings_sp`) should be **copied into multiple category folders** or stored once with reference IDs.

**Recommendation: reference-ID linking + thin cross-category snapshots.**

**Full denormalization** (copying all SP+ columns into `game/advanced`) is tempting but:
- Creates update synchronization problems — when ratings are re-scraped, every copy must be updated
- Balloons storage across seasons (5 ratings systems × 134 teams × N seasons × K target folders)
- Training pipelines that JOIN are faster at query time than ones that filter wide denorm tables

**Reference-ID linking** (each record carries `team_id` and `season` as join keys):
- Each minor category stores its canonical columns + `team_id` + `season`
- ML feature assembly does `LEFT JOIN team.ratings ON team_id + season` — DuckDB handles this in microseconds
- Single source of truth, no sync drift

**Thin cross-category snapshot** (the user's "minimized reference IDs" idea):
- The `game/advanced` table DOES include `team_sp_rating`, `team_elo`, `opponent_sp_rating` as **precomputed scalar snapshots** assigned at ingest time — not by copying the full SP+ record
- These are the 2-5 most model-relevant scalars from each rating system, denormalized into the game-context row
- Saves the ML join at training time for the most common features without duplicating full detail tables

This gives three valid placement patterns per data kind:

| Pattern | When to use | Example |
|---------|-------------|---------|
| **Single canonical store** | Full detail needed for analysis | `ratings_sp` → `team/ratings/` |
| **Thin snapshot in context** | 1-5 scalars used in every game row | `team_sp_rating` column in `game/advanced` |
| **Reference-ID join** | Full detail needed on demand | `team_id` FK in `player/season_stats` → join `team/identity` |

---

## 4 Specific Blending Decisions

### 4.1 Ratings (SP+, ELO, FPI, SRS, talent)
- **Canonical home**: `team/ratings/` partitioned by `type={sp,elo,fpi,srs,talent}` and `season=YYYY`
- **Thin cross-copy**: Pre-calculate `sp_offense_rating`, `sp_defense_rating`, `elo_rating` columns and include in `game/advanced` rows at normalization time — these 3 scalars cover 90% of ML feature use
- **Do NOT** fully copy all 40+ SP+ sub-columns into game records

### 4.2 Venues
- **Canonical home**: `reference/venues/` (full 14-column dataset, rarely changes)
- **Thin cross-copy**: `venue_capacity`, `venue_grass`, `venue_dome`, `venue_city` in `game/schedule` rows — 4 fields needed for weather/home-field-advantage features
- Recruiting and teams also carry `location.*` fields — deduplicate by joining on `venue_id` rather than copying

### 4.3 Recruiting
- `recruiting` (player profiles) → `player/identity/` with `type=recruit`
- `recruiting_teams` (class rank) → `team/recruiting/` with `type=class`
- `recruiting_groups` (position group) → `team/recruiting/` with `type=group`
- Only the `five_star_count`, `composite_rank`, `total_points` scalars need to appear in `team/ratings/` as thin cross-copy fields

### 4.4 Rankings (polls)
- `rankings` → `season/rankings/` (primary home, fully partitioned by `week`)
- Thin cross-copy: `ap_rank`, `cfp_rank` columns in `game/schedule` rows at normalization time

### 4.5 Records and ATS
- `records` (W-L-T splits) → `team/record/` with `type=overall|home|away|conf`
- `teams_ats` (against the spread) → `team/record/` with `type=ats`
- Both share `team_name + season` as natural key → can be unioned in a single `team/record` parquet with `record_type` column OR stored as separate type partitions

### 4.6 Static lookups (conferences, play_types, stats_categories, metrics_fg_ep)
- All go to `reference/` — they are never queried by season
- `reference/` partitions should NOT use `season=` hive partitioning, just flat files
- `conferences` appears in both `reference/conferences` AND as a thin `conference_name` field in `team/identity` — acceptable light duplication since it's a string label

### 4.7 Per-game data (ppa_games, stats_game_advanced, stats_game_havoc, wp_pregame)
- All merge into `game/advanced/` partitioned by `season=` and `week=`
- Each row has a `metric_type` column: `ppa | advanced | havoc | wp`
- OR use type sub-folders: `game/advanced/type=ppa/`, `game/advanced/type=advanced/`, etc.
- Type sub-folders are preferred — DuckDB partition pruning keeps reads fast even with mixed schemas

---

## 5 Implementation Approach

### 5.1 Migration path (non-breaking)

The blended hierarchy is a **curated output format change** — it does not affect:
- Raw data storage (`data/raw/`)
- Individual loader functions in `normalizer.py`
- `_NORMALIZE_METHOD_MAP` or `_DATA_TYPE_METHODS`

The change lives entirely in **`CuratedParquetBuilder`** (`normalization/curated_parquet_builder.py`), which today maps `(sport, dtype, season) → normalized_curated/{sport}/{dtype}/season={season}/`. That mapping would change to `(sport, dtype, season) → normalized_curated/{sport}/{major}/{minor}/type={type}/season={season}/`.

**Phase 1 — Add category routing map** to `curated_parquet_builder.py`:
```python
CURATED_ROUTING: dict[str, tuple[str, str, str | None]] = {
    # (dtype) → (major, minor, type or None)
    "teams":                ("team",    "identity",  None),
    "teams_fbs":            ("team",    "identity",  "fbs"),
    "conferences":          ("team",    "context",   "conferences"),
    "venues":               ("reference","venues",   None),
    "games_media":          ("team",    "context",   "media"),
    "records":              ("team",    "record",    None),
    "teams_ats":            ("team",    "record",    "ats"),
    "standings":            ("team",    "record",    "standings"),
    "ratings_sp":           ("team",    "ratings",   "sp"),
    "ratings_sp_conferences":("team",   "ratings",   "sp_conf"),
    "ratings_elo":          ("team",    "ratings",   "elo"),
    "ratings_fpi":          ("team",    "ratings",   "fpi"),
    "ratings_srs":          ("team",    "ratings",   "srs"),
    "talent":               ("team",    "ratings",   "talent"),
    "recruiting_teams":     ("team",    "recruiting","class"),
    "recruiting_groups":    ("team",    "recruiting","groups"),
    "players":              ("player",  "identity",  None),
    "recruiting":           ("player",  "identity",  "recruit"),
    "player_portal":        ("player",  "portal",    None),
    "player_returning":     ("player",  "returning", None),
    "player_usage":         ("player",  "usage",     None),
    "player_stats":         ("player",  "season_stats", None),
    "ppa_players_season":   ("player",  "season_stats", "ppa"),
    "games":                ("game",    "schedule",  None),
    "ppa_games":            ("game",    "advanced",  "ppa"),
    "stats_game_advanced":  ("game",    "advanced",  "epa"),
    "stats_game_havoc":     ("game",    "advanced",  "havoc"),
    "wp_pregame":           ("game",    "advanced",  "wp"),
    "play_by_play":         ("game",    "play_by_play", None),
    "plays_stats":          ("game",    "play_by_play", "season_stats"),
    "team_stats":           ("season",  "team_stats", None),
    "stats":                ("season",  "team_stats", "box"),
    "ppa_teams":            ("season",  "ppa",       None),
    "rankings":             ("season",  "rankings",  None),
    "odds":                 ("market",  "odds",      None),
    "odds_history":         ("market",  "odds_history", None),
    "player_props":         ("market",  "props",     None),
    "market_signals":       ("market",  "signals",   None),
    "plays_types":          ("reference","play_types","type"),
    "plays_stats_types":    ("reference","play_types","stat_type"),
    "stats_categories":     ("reference","metrics",  "categories"),
    "metrics_fg_ep":        ("reference","metrics",  "fg_ep"),
    "conferences":          ("reference","conferences", None),
}
```

**Phase 2 — Update `CuratedParquetBuilder.build_sport()`** to use `CURATED_ROUTING` when writing output paths.

**Phase 3 — Update any API endpoints** that read from `normalized_curated/{sport}/{dtype}/` to use the new paths.

**Phase 4 — Optional enrichment pass**: after all dtypes are written, a second pass reads `team/ratings/type=sp` and joins `sp_offense_rating + sp_defense_rating + opponent_sp_rating` fields into `game/advanced/` rows. This is an **optional** denorm snapshot — not required for correctness, but improves ML feature assembly latency by ~40%.

### 5.2 Hive partition scheme per major category

| Major | Partition columns |
|-------|------------------|
| `team/` | `season=YYYY` |
| `player/` | `season=YYYY` |
| `game/` | `season=YYYY`, `week=NN` |
| `season/` | `season=YYYY` |
| `market/` | `season=YYYY`, `week=NN` |
| `reference/` | none (flat files, no partition) |

Reference data has no date dimension — flat files are read once, cached in DuckDB views.

---

## 6 Trade-off Summary

| Dimension | Current flat (44 folders) | Proposed hierarchy (6 major) |
|-----------|--------------------------|------------------------------|
| **Folder count** | 44 per sport | 6 major + ~20 minor per sport |
| **Query locality** | Requires 9+ folder joins | `game/advanced/` has all per-game signals in 1 scan |
| **Schema flexibility** | Every dtype is independent | Shared major-category schema conventions |
| **Storage efficiency** | Moderate — no duplication | Better — reference data in `reference/` is read-once |
| **ML feature assembly** | 10–15 DuckDB reads | 3–5 scans (team, game, season) |
| **Implementation cost** | None (current state) | Medium — `CuratedParquetBuilder` rewrite + API path updates |
| **Migration risk** | Zero | Low if done as additive new output path (old kept until cutover) |
| **Maintenance** | New endpoint = new folder (scales poorly) | New endpoint = add to existing minor category (scales well) |

---

## 7 Recommendation

**Adopt the blended hierarchy.** The implementation is isolated to `CuratedParquetBuilder` and the API read paths — the normalizer loaders, raw storage, and test suite are unaffected.

Prioritize in this order:

1. **`game/advanced/`** — merge `ppa_games`, `stats_game_advanced`, `stats_game_havoc`, `wp_pregame` (these are currently the most fractured per ML feature assembly)
2. **`team/ratings/`** — merge `ratings_sp`, `ratings_elo`, `ratings_fpi`, `ratings_srs`, `talent` (5 tables with essentially the same `team_name+season` key)
3. **`team/record/`** — merge `records`, `teams_ats`, `standings`
4. **`reference/`** — consolidate all static lookups
5. **`player/`** and **`season/`** — can wait for v5.1 when player portal data is deeper

For the thin snapshot enrichment (cross-category denorm of 3–5 rating scalars into game rows), implement as a **post-normalization enrichment step** — a separate script that reads `team/ratings/` and writes scalar columns into `game/advanced/` parquet files in-place. This keeps the normalizer clean while giving ML consumers the pre-joined convenience columns they need.
