# Missing Data Report — Sports Prediction System v5.0

**Purpose**: This document tracks data gaps in normalized datasets that limit model accuracy.
A separate data-ingestion agent should address these gaps to improve prediction quality.

**Last Updated**: 2026-03-31

---

## Critical Issues (Blocks Feature Computation)

### 1. MLB — Pitcher Win/Loss/WHIP Columns Empty
**File(s)**: `data/normalized/mlb/player_stats_{year}.parquet`  
**Columns affected**: `win`, `loss`, `whip`  
**Status**: All 3 columns are 100% null (0/76,778 non-null in 2024)  
**Impact**: `sp_win_pct` feature always returns 0 — effectively a dead feature  
**Note**: `bb` (walks) column IS populated; `walks` (long-form) IS also populated  
**Fix needed**: Populate `win`, `loss`, and `whip` columns per pitcher per game from box score sources  
**Feature extractors affected**: `backend/features/baseball.py` (`_team_pitching_stats`)

### 2. NHL — Period/Quarter Score Splits Missing
**File(s)**: `data/normalized/nhl/games_{year}.parquet`  
**Columns affected**: `home_q1`, `home_q2`, `home_q3`, `away_q1`, `away_q2`, `away_q3`, `period`  
**Status**: 0/1,451 non-null (100% missing) for period-level scores  
**Impact**: Cannot build period-by-period momentum or comeback features  
**Note**: `home_ot` / `away_ot` ARE populated (20% fill, reflecting OT games)  
**Fix needed**: Backfill period scores from NHL API or boxscore source  

### 3. NWSL — All Advanced Stats Missing
**File(s)**: `data/normalized/nwsl/games_{year}.parquet`  
**Columns affected**: `home_ot`, `away_ot`, `home_q3`, `away_q3`, `home_q4`, `away_q4` (99%+ missing)  
**Status**: Only 84 games total; OT and period splits are 100% null  
**Impact**: NWSL model accuracy severely limited; 10 high-missingness features identified  
**Fix needed**: Backfill match statistics including period/half scores and OT indicators  

---

## High Priority (Reduces Feature Quality)

### 4. ATP/WTA — Set Score Splits, Attendance, Weather Empty
**File(s)**: `data/normalized/atp/games_{year}.parquet`, `data/normalized/wta/games_{year}.parquet`  
**Columns affected**: `attendance`, `weather`, `start_time`, `period`, `home_ot`, `away_ot`, `home_p1`, `home_p2`, `home_p3`, `away_p1`, `away_p2`, `away_p3`  
**Status**: 0% fill on all these columns  
**Impact**: Cannot build set-by-set momentum features; `avg_sets_recent` has 75%+ missingness  
**Fix needed**: Populate set-by-set scores, venue attendance, match start times from ATP/WTA API  

### 5. ATP/WTA — Odds History Limited Coverage
**File(s)**: `data/normalized/atp/odds_history.parquet` (exists), `data/normalized/wta/` (no odds_history.parquet)  
**Status**: WTA has no `odds_history.parquet` file at all  
**Impact**: Market signal features empty for WTA  
**Fix needed**: Add WTA odds history file matching ATP format  

### 6. Most Sports — Market Signals Missing
**Status**: Only NBA, NFL, MLB, NHL, EPL, LaLiga, Bundesliga, SerieA, Ligue1, MLS have `market_signals_*.parquet`  
**Missing for**: UCL, NWSL, NCAAF, NCAAB, NCAAW, ATP, WTA, UFC, F1, Golf, eSports  
**Impact**: ~13 sports lack opening/closing line movement features  
**Fix needed**: Create `market_signals_{year}.parquet` for missing sports using bookmaker API data  
**Format reference**: `data/normalized/nba/market_signals_2025.parquet` (30 columns: game_id, open/close h2h, spread, total, market_regime)

### 7. UFC — Fighter Performance Stats Sparse
**File(s)**: `data/normalized/ufc/`  
**Columns affected**: `away_momentum`, `away_sig_strikes_per_fight`, `home_early_finish_rate`, `home_avg_finish_round`  
**Status**: 80%+ missingness on efficiency/form features  
**Impact**: UFC model limited to basic win/loss history without advanced fight metrics  
**Fix needed**: Backfill per-fight metrics from UFC Stats or Tapology

### 8. DOTA2/LoL/CSGO — Player Performance Stats Sparse
**File(s)**: `data/normalized/dota2/`, `data/normalized/lol/`, `data/normalized/csgo/`  
**Status**: `home_player_avg_kda`, `away_player_avg_gpm` etc. have 80%+ missingness  
**Impact**: eSports models miss most informative player-level features  
**Fix needed**: Better player-to-game ID reconciliation; backfill KDA/GPM/damage metrics from match history APIs

---

## Medium Priority (Feature Enhancement Opportunities)

### 9. NBA — Market Signals 2026 Present But Schedule Fatigue Only Through 2025
**Files available**: `market_signals_2025.parquet`, `market_signals_2026.parquet`, `schedule_fatigue_2025.parquet`, `schedule_fatigue_2026.parquet`  
**Status**: Both 2025 and 2026 data present — well covered ✅  
**Note**: NBA is the best-covered sport for contextual features  

### 10. NFL — Market Signals Only 2025 (Limited Coverage)
**Files available**: `market_signals_2025.parquet` (497 rows), `schedule_fatigue_2025.parquet` (572 rows)  
**Status**: Data exists but covers only ~1 season  
**Fix needed**: Add 2020–2024 historical market signals to improve model training coverage  

### 11. MLB — Weather Features Sparse
**File(s)**: `data/normalized/mlb/games_{year}.parquet`  
**Columns affected**: `weather_temp`, `weather_wind`, `weather_cold`, `weather_dome`, `weather_precip`  
**Status**: 50%+ missingness (identified in weak sports report as high-missingness features)  
**Impact**: Cannot reliably model weather effects on run scoring  
**Fix needed**: Backfill venue weather data from Weather API by game date/location  

### 12. NCAAF/NCAAB/NCAAW — No Market Signals Data
**Status**: No `market_signals_*.parquet` files exist for any NCAA sport  
**Impact**: NCAA models cannot use odds movement as features  
**Fix needed**: Add market signals files for NCAA sports (especially NCAAF and NCAAB which have active betting markets)

### 13. F1 — Data Coverage Limited  
**File(s)**: `data/normalized/f1/`  
**Status**: Model saved 2026-03-30 (oldest model in system at 10:17)  
**Impact**: F1 model may be significantly outdated  
**Fix needed**: Verify F1 normalized data is current through 2026 season; refresh model  

---

## Column Naming Inconsistencies (Developer Note)

These inconsistencies cause silent feature computation failures:

| Sport | Expected Column | Actual Column | Feature Affected |
|-------|----------------|---------------|-----------------|
| MLB   | `walks`        | `bb`          | WHIP calculation (Fixed in baseball.py — uses `bb` fallback) |
| MLB   | `win` (pitcher wins) | `win` (exists but 100% null) | `sp_win_pct` |
| NHL   | `home_q1`/`away_q1` | Present but 0% fill | Period prediction features |

---

## Sports Without Normalized Player Stats (Player Prop Predictions Limited)

| Sport | Has Player Stats | Coverage | Notes |
|-------|-----------------|----------|-------|
| NBA   | ✅ Yes | 2020–2026 | Good |
| NFL   | ✅ Yes | 2020–2025 | Good |
| MLB   | ✅ Yes | 2020–2026 | Win/loss/whip empty |
| NHL   | ✅ Yes | 2020–2026 | Good |
| ATP   | ✅ Yes | 2023–2024 | Limited seasons |
| WTA   | ✅ Yes | 2023–2024 | Limited seasons |
| Soccer| ❌ Limited | Some player_stats files | Inconsistent coverage |
| UFC   | ✅ Yes | Available | High missingness |
| NCAAF | ✅ Yes | 2020–2025 | Good |
| NCAAB | Partial | Some files | Limited coverage |
| Golf  | ✅ Yes | 2020–2026 | Good |
| Esports | ✅ Yes | Variable | ID reconciliation issues |

---

## Recommended Priority Order for Data Team

1. **[CRITICAL]** MLB: Fill `win`, `loss`, `whip` columns in `player_stats_{year}.parquet` for pitchers  
2. **[HIGH]** NHL: Backfill period scores (`home_q1`, `home_q2`, `home_q3`) for all seasons  
3. **[HIGH]** NWSL: Backfill all advanced match statistics  
4. **[HIGH]** ATP/WTA: Add set-by-set scores; create WTA `odds_history.parquet`  
5. **[MEDIUM]** Add `market_signals_{year}.parquet` for: UCL, NWSL, NCAAF, NCAAB, NCAAW, ATP, WTA  
6. **[MEDIUM]** UFC: Backfill per-fight performance metrics  
7. **[MEDIUM]** MLB: Backfill weather data for all stadium games  
8. **[LOW]** eSports (DOTA2/LoL/CSGO): Fix player-to-game ID reconciliation  
9. **[LOW]** NFL/NBA/NHL: Extend market signals back to 2020 for richer training history  

---

## Data Format Reference

All normalized data should follow these conventions:

- **Parquet format** with snappy compression  
- **Game ID**: `{sport}_{home_team}_{away_team}_{YYYY-MM-DD}` (lowercase, underscores)  
- **Team IDs**: Must match `teams_{year}.parquet` `team_id` column exactly  
- **Season column**: Integer year (e.g., `2024`)  
- **Date column**: `datetime64[ns]` or ISO 8601 string  
- **Home/Away**: All per-team columns prefixed with `home_` or `away_`  
- **Market signals format**: See `data/normalized/nba/market_signals_2025.parquet` for schema  
- **Schedule fatigue format**: See `data/normalized/nba/schedule_fatigue_2025.parquet` for schema  
