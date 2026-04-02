# DuckDB Feature Extraction Migration Notes

## Overview

All feature extractors now read data through `CuratedDataReader` (DuckDB in-memory),
replacing direct `pd.read_parquet` calls on legacy `data/normalized/` flat files.
Source: `data/normalized_curated/{sport}/{category}/season={YYYY}/[date=YYYY-MM-DD/]part.parquet`

---

## Season Coverage Improvements

| Sport | Old (legacy normalized) | New (curated DuckDB) | Delta |
|-------|------------------------|----------------------|-------|
| NBA | 2026 only | 2020–2026 | +6 seasons |
| NHL | 2026 only | 2020–2026 | +6 seasons |
| NFL | 2025 only | 2020–2025 | +5 seasons |
| MLB | 2026 only | 2020–2026 | +6 seasons |
| NCAAB | none | 2020–2026 | +7 seasons |
| NCAAF | none | 2020–2025 | +6 seasons |
| EPL | 2025 only | 2020–2025 | +5 seasons |
| MLS | 2025 only | 2020–2025 | +5 seasons |
| Bundesliga | 2025 only | 2020–2025 | +5 seasons |
| LaLiga | 2025 only | 2020–2025 | +5 seasons |
| Ligue1 | 2025 only | 2020–2025 | +5 seasons |
| ATP | 2025 only | 2020–2025 | +5 seasons |
| WTA | 2024 only | 2020–2024 | +4 seasons |
| Golf | 2025 only | 2020–2025 | +5 seasons |
| UFC | 2025 only | 2020–2025 | +5 seasons |
| CSGO | 2023 only | 2020–2023 | +3 seasons |
| F1 | 2025 only | 2020–2025 | +5 seasons |

---

## New Data Available Per Sport (vs Old Normalized)

### NBA
- `games`: per-quarter scores (home_q1–q4, away_q1–q4), h1/h2 scores, OT score
- `games`: advanced box stats: fg_pct, fg3_pct, ft_pct, rebounds, assists, turnovers, steals, blocks
- `games`: pace, possessions, offensive_rating, defensive_rating, true_shooting_pct
- `ratings`: SRS-style power ratings per team per season
- `team_season_averages`: 30-team shooting/rebounding/assist/pace season averages
- `players`, `player_season_averages`, `player_stats`: individual player data

### NHL
- `games`: period scores (home_p1/p2/p3/ot, away_p1/p2/p3/ot)
- `games`: shots_on_goal, power_play_goals, power_play_opportunities
- `games`: save_pct, shooting_pct, hits, takeaways, giveaways, faceoff_pct
- `player_stats`: per-game skater/goalie stats with goals/assists/saves/save_pct

### MLB
- `games`: per-inning scores (home_i1–i9+, away_i1–i9+)
- `games`: OPS, WHIP, K-rate, BB-rate, HR, LOB
- `batter_game_stats`: per-game individual batter stats (AB, H, HR, RBI, BB, K)
- `pitcher_game_stats`: per-game starter/reliever stats (IP, ER, K, BB, HR)
- `advanced_batting`: wOBA, wRC+, ISO, BABIP, BB%, K% by team/season
- `weather`: temperature, wind_speed, wind_dir, humidity, precipitation, dome flag
- `team_game_stats`: team-level rolling batting/pitching (runs, hits, ERA, WHIP per game)

### NFL
- `games`: per-quarter scores (home_q1–q4, away_q1–q4), h1/h2 totals, OT
- `games`: total yards, passing yards, rushing yards, turnovers, time_of_possession
- `player_props`: target receiver props, QB passing/rushing lines

### Soccer (EPL / Bundesliga / LaLiga / Ligue1 / MLS)
- `match_events`: goal timestamps, assist data, red/yellow cards with minute
- `games`: xG (expected goals), shots on target, corners, possession %
- MLS, Bundesliga, LaLiga, Ligue1 now have `market_signals`

### NCAAB / NCAAF
- `team_season_averages` and `team_stats`: full team season aggregates
- `player_props`: betting prop lines for players
- `injuries`: full injury reports for NCAAF (was missing before)
- `market_signals`: public betting % + sharp money signals

### UFC
- `market_signals`: odds/public money available
- `odds_history`: historical opening/closing line movement

---

## Missing Data (Needs Normalization Agent Work)

### High Priority — Missing Categories That Existed Before

| Sport | Missing Category | Was Present Before | Impact |
|-------|-----------------|-------------------|--------|
| ATP | `odds`, `market_signals` | Yes (partial) | Can't build line movement features |
| ATP | `injuries` | Yes (partial) | Missing injury adjustment features |
| Golf | `odds`, `market_signals` | Yes (partial) | No odds-based features |
| Golf | `injuries` | Yes (partial) | Missing WD/injury adjustment |
| LPGA | `odds`, `market_signals` | No | No line features |
| LPGA | `injuries`, `player_stats` | No | Only 3 categories available |
| WTA | `odds`, `market_signals` | Yes (partial) | Missing line features |
| UFC | `standings`, `predictions` | No | Missing quality-weighted features |
| IndyCar | Only has `games`, `news`, `schedule_fatigue` | N/A | Very sparse — needs odds/player_stats |
| UCL | `odds`, `market_signals` | No | No betting features |
| NWSL | `odds`, `market_signals` | No | No betting features |
| NWSL | `injuries` | No | Missing injury features |
| F1 | `odds`, `market_signals` | No | No betting features |

### Medium Priority — New Data Not Yet Populated

| Sport | Category Needed | Notes |
|-------|----------------|-------|
| NHL | `team_season_averages` | Available for NBA/NCAAB not NHL |
| NHL | `ratings` (power ratings) | Available for NBA not NHL |
| MLB | `ratings` (power ratings) | Would improve ML features |
| Soccer (all) | `team_season_averages` | Available for NCAAB not soccer |
| Soccer (EPL, UCL) | `match_events` | Present in Bundesliga/LaLiga/Ligue1/MLS, missing EPL/UCL |
| EPL | `injuries` | No injury data for EPL |
| Bundesliga | `injuries` | No injury data |
| Ligue1 | `injuries` | No injury data |
| MLS | `injuries` | No injury data |
| Golf | `odds_history` | No opening/closing line tracking |
| ATP | `odds_history` | No opening/closing line tracking |

### Low Priority — Enrichment Data

| Sport | Category | Notes |
|-------|---------|-------|
| All sports | `news` | Most sports have it; needed for sentiment features |
| MLB | `umpire_stats` | Useful for over/under — ump tendencies |
| NFL/NCAAF | `weather` | Only MLB has weather currently |
| UFC | `judge_stats` | Useful for decision-based markets |

---

## New BaseFeatureExtractor Methods Available

After DuckDB migration, these new methods exist on `BaseFeatureExtractor`:

```python
# Enhanced multi-season loading
self.load_games(season)              # curated → legacy fallback
self.load_player_stats(season)       # curated → legacy fallback
self.load_standings(season)          # curated → legacy fallback
self.load_team_stats(season)         # team_stats → team_season_averages → standings chain
self.load_odds(season)               # curated → legacy fallback
self.load_injuries(season)           # curated → legacy fallback
self.load_market_signals(season)     # curated → legacy fallback
self.load_schedule_fatigue(season)   # curated → legacy fallback

# NEW — curated-only categories
self.load_team_season_averages(season)  # team season aggregate stats
self.load_match_events(season)          # goal/card events with timestamps
self.load_weather(season)               # game-day weather (MLB primary)
self.load_batter_game_stats(season)     # MLB per-game batter stats
self.load_pitcher_game_stats(season)    # MLB per-game pitcher stats
self.load_advanced_batting(season)      # wOBA/wRC+/ISO/BABIP by team
self.load_ratings(season)              # SRS/power ratings
self.available_seasons(category)        # list available seasons for a category
```

---

## Code Changes Made

| File | Change |
|------|--------|
| `backend/features/data_reader.py` | NEW — DuckDB-backed CuratedDataReader singleton |
| `backend/features/base.py` | All load_* methods use DuckDB reader; new load methods added |
| `backend/features/golf.py` | Removed redundant `_load_all_games` override |
| `backend/features/soccer.py` | Removed `_load_all_games`; UCL/Europa lookup uses reader |
| `backend/features/basketball.py` | Removed `_load_all_games`; `_build_team_id_aliases` uses reader |
| `backend/features/baseball.py` | Removed `_load_all_games`; advanced_batting/batter_game_stats/pitcher_game_stats/team_game_stats all use reader |
| `backend/features/tennis.py` | `_load_all_games` uses reader; player_stats/name_id_bridge updated |
| `backend/features/motorsport.py` | Removed redundant `_load_all_games` override |
| `backend/features/esports.py` | Removed `_load_all_games`; `_load_schedule_fatigue` uses reader |

---

## DuckDB Reader Architecture

```
CuratedDataReader (thread-local singleton)
  ├── load(sport, category, season, columns) → pd.DataFrame
  │   └── reads: data/normalized_curated/{sport}/{category}/season={YYYY}/*/part.parquet
  │   └── falls back to: data/normalized/{sport}/{category}_{season}.parquet
  ├── load_all_seasons(sport, category) → pd.DataFrame (all seasons combined)
  ├── available_seasons(sport, category) → list[int]
  └── has_category(sport, category) → bool
```

DuckDB connection is **in-memory** (`:memory:`) to avoid lock conflict with the live
`data/normalized.duckdb` used by the odds poller (PID ~3694168).

---

## Next Steps

1. **Re-extract all feature parquets** using new DuckDB reader (gains 5+ extra seasons):
   ```bash
   cd /home/derek/Documents/stock/v5.0
   PYTHONPATH=backend python3 backend/ml/feature_extraction.py --sport nba --seasons 2020,2021,2022,2023,2024,2026
   # Repeat for: nhl, nfl, mlb, ncaab, ncaaf, epl, bundesliga, laliga, ligue1, mls, atp, wta, golf, ufc, csgo
   ```

2. **Retrain all models** with expanded historical data

3. **Exploit new curated columns** in feature extractors:
   - Basketball: `team_season_averages` → richer baseline stats
   - Soccer: `match_events` → goal timing, comeback rate, late goal rate features
   - MLB: `pitcher_game_stats` + `advanced_batting` + `weather` → pitcher matchup features
   - NHL: per-period scores → period-specific market features
   - NFL: per-quarter scores → Q1/Q2/Q3/Q4 market features

4. **Request normalization agent** to build missing categories (see table above)
