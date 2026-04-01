# Missing Data Report — Sports Prediction System v5.0

**Purpose**: This document tracks data gaps in normalized datasets that limit model accuracy.
A separate data-ingestion agent should address these gaps to improve prediction quality.

**Last Updated**: 2026-04-01 (Phase 2 retrain in progress — new features: player stats differentials, momentum_diff, skater differentials, NFL player stats differentials, soccer attacking depth diff)

---

## Current Model Accuracy Summary

> **Note**: Phase 2 retrain is currently running with all bug fixes + new features. Values below reflect pre-phase-2 state.

| Sport | Best Accuracy | Best Model | Features | Status |
|-------|-------------|-----------|---------|--------|
| NFL | 63.8% | logistic | 236 | ✅ Phase 2 running (new: QB/rush/def diffs, momentum_diff) |
| NBA | 79.6% | gradient_boosting | 331 | ✅ Phase 2 running (new: pstats diffs, momentum_diff) |
| NCAAB | 78.9% | gradient_boosting | 232 | ✅ Phase 2 running (new: momentum_diff) |
| NCAAW | 90.2% | lightgbm | 224 | ✅ Phase 2 running |
| WNBA | 72.1% | random_forest | 206 | ✅ Phase 2 running |
| MLB | 100% ❌ | logistic | 253 | LEAKY — phase 2 fix running (new: momentum_diff) |
| NHL | 61.2% | logistic | 207 | ✅ Phase 2 running (new: skater diffs, momentum_diff) |
| EPL | 100% ❌ | logistic | 199 | LEAKY — phase 2 fix running (new: attacking_depth_diff, top_scorer_form_diff, momentum_diff) |
| LaLiga | 77.0% | adaboost | 193 | ✅ Phase 2 running |
| Bundesliga | 75.4% | catboost | 193 | ✅ Phase 2 running |
| SerieA | 76.3% | random_forest | 189 | ✅ Phase 2 running |
| Ligue1 | 73.9% | random_forest | 193 | ✅ Phase 2 running |
| MLS | 68.3% | adaboost | 167 | ✅ Phase 2 running |
| UCL | 96.8% ⚠️ | xgboost | 178 | Phase 2 retrain running (feature mismatch fixed) |
| NWSL | 97.8% ⚠️ | gradient_boosting | 139 | Phase 2 retrain running (feature mismatch fixed) |
| Europa | NEW | — | — | ✅ New sport — phase 2 training (2024-2025 data) |
| LigaMx | NEW | — | — | ✅ New sport — phase 2 training (2024-2025 data) |
| ATP | 61.6% | adaboost | 94 | ✅ Phase 2 running |
| WTA | 62.0% | catboost | 92 | ✅ Phase 2 running |
| F1 | 100% ❌ | random_forest | 25 | LEAKY — phase 2 fix running |
| Golf | 93.4% | lightgbm | 28 | ✅ Phase 2 running (new: world_rank features +5) |
| LPGA | NEW | — | — | ✅ New sport — phase 2 training (2024-2026 data) |
| UFC | 71.4% | random_forest | 65 | ✅ Phase 2 running (fix: sig_strikes_absorbed in empty dict) |
| CSGO | 58.0% | — | — | ✅ Phase 2 running (games_df bug fixed) |
| DOTA2 | 65.2% | xgboost | 85 | ✅ Phase 2 running (new: momentum_diff) |
| LoL | 63.9% | catboost | 29 | ✅ Phase 2 running (new: momentum_diff) |
| VALORANT | 61.6% | svc | 29 | ✅ Phase 2 running (new: momentum_diff) |
| NCAAF | 89.7% | — | — | ✅ Phase 2 running |

**Phase 2 retrain sports (27 total)**: mlb, nba, ncaab, ncaaw, wnba, nfl, ncaaf, nhl, epl, ucl, nwsl, laliga, bundesliga, ligue1, seriea, mls, europa, ligamx, atp, wta, f1, golf, lpga, ufc, csgo, lol, valorant, dota2

**New features this session**:
- All sports: `momentum_diff` (home_momentum − away_momentum)
- NBA/NCAAB/NCAAW/WNBA: `pstats_plus_minus_diff`, `pstats_top_scorer_diff`, `pstats_ast_to_diff`, `pstats_ts_pct_diff`
- NFL/NCAAF: `nfl_ps_qb_rating_diff`, `nfl_ps_pass_yds_pg_diff`, `nfl_ps_rush_yds_pg_diff`, `nfl_ps_sacks_pg_diff`, `nfl_ps_def_int_pg_diff`, `nfl_ps_rush_td_pg_diff`
- NHL: `sk_avg_plus_minus_diff`, `sk_top_scorer_pts_pg_diff`, `sk_team_pts_pg_diff`, `sk_team_shots_pg_diff`
- Soccer (all leagues): `attacking_depth_diff`, `top_scorer_form_diff`
- Golf: `world_rank`, `world_rank_inv`, `season_wins`, `season_points`, `season_games_played`
- UFC: Fixed `sig_strikes_absorbed` missing from empty defaults dict

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

### 4. F1 — Current-Race Outcome Leakage (FIXED in code, retrain pending)
**Status**: `podium`, `points_finish`, `dnf`, `fastest_lap`, `laps_completed`, `laps_completion_pct`,
`avg_speed_kph`, `pit_stops`, `avg_pit_time_s`, `safety_car_count`, `dnf_count`, `red_flag_count`,
`race_pit_stops_total` were included as features but represent CURRENT RACE outcomes (not pre-race knowledge)  
**Fix applied**: Added all these columns to `_META_COLS` in `backend/ml/train.py`  
**Retrain needed**: F1 model must be retrained (included in phase 2 script)  
**Post-fix features**: Only qualifying/grid position, historical driver pace, constructor standings, weather, tire strategy, round number remain (~18-22 valid features)  

### 5. MLB / EPL — Data Leakage (FIXED in code, retrain in progress)
**MLB**: `home_i1–i9` (inning scores) were being used as features; fixed in `_META_COLS`  
**EPL**: Race condition — EPL training started 7s before the fix was applied; model trained on leaked data  
**Fix applied**: `_META_COLS` now excludes all inning, period, and half scores (50 entries)  
**Retrain needed**: Both MLB and EPL are in phase 2 retrain script  

---

## High Priority (Reduces Feature Quality)

### 6. ATP/WTA — Set Score Splits, Attendance, Weather Empty
**File(s)**: `data/normalized/atp/games_{year}.parquet`, `data/normalized/wta/games_{year}.parquet`  
**Columns affected**: `attendance`, `weather`, `start_time`, `period`, `home_ot`, `away_ot`, `home_p1`, `home_p2`, `home_p3`, `away_p1`, `away_p2`, `away_p3`  
**Status**: 0% fill on all these columns  
**Impact**: Cannot build set-by-set momentum features; `avg_sets_recent` has 75%+ missingness  
**Fix needed**: Populate set-by-set scores, venue attendance, match start times from ATP/WTA API  

### 7. ATP/WTA — Odds History Limited Coverage
**File(s)**: `data/normalized/atp/odds_history.parquet` (exists), `data/normalized/wta/` (no odds_history.parquet)  
**Status**: WTA has no `odds_history.parquet` file at all  
**Impact**: Market signal features empty for WTA  
**Fix needed**: Add WTA odds history file matching ATP format  

### 8. Most Sports — Market Signals Missing
**Status**: Only NBA, NFL, MLB, NHL, EPL, LaLiga, Bundesliga, SerieA, Ligue1, MLS have `market_signals_*.parquet`  
**Missing for**: UCL, NWSL, NCAAF, NCAAB, NCAAW, ATP, WTA, UFC, F1, Golf, eSports  
**Impact**: ~13 sports lack opening/closing line movement features  
**Fix needed**: Create `market_signals_{year}.parquet` for missing sports using bookmaker API data  
**Format reference**: `data/normalized/nba/market_signals_2025.parquet` (30 columns: game_id, open/close h2h, spread, total, market_regime)

### 9. UFC — Fighter Performance Stats Sparse
**File(s)**: `data/normalized/ufc/`  
**Columns affected**: `away_momentum`, `away_sig_strikes_per_fight`, `home_early_finish_rate`, `home_avg_finish_round`  
**Status**: 80%+ missingness on efficiency/form features  
**Impact**: UFC model limited to basic win/loss history without advanced fight metrics  
**Fix needed**: Backfill per-fight metrics from UFC Stats or Tapology

### 10. DOTA2/LoL/CSGO/VALORANT — Team-Level Kill/Objective Stats Sparse
**File(s)**: `data/normalized/dota2/`, `data/normalized/lol/`, `data/normalized/csgo/`, `data/normalized/valorant/`  
**Status**:
- DOTA2: Has team-level data → 85 features ✅
- CSGO: 80%+ missingness on kills/objectives → only 28 features (form-only) ⚠️
- LoL: 80%+ missingness on kills/objectives → only 29 features (form-only) ⚠️
- VALORANT: 80%+ missingness → only 29 features (form-only) ⚠️
**Root cause**: PandaScore free tier doesn't provide per-match team stats for some games; game ID reconciliation failures  
**Impact**: CSGO/LoL/VALORANT models cannot use kill rates, KDA, objective control, gold economy features  
**Fix needed**: 
  1. Fix player-to-game ID reconciliation (see `scripts/normalize_dota2_game_ids.py` for reference)
  2. Backfill team-level aggregate stats (kills/deaths/assists, objectives, gold per min) from match history APIs
  3. Consider PandaScore Pro tier for per-match stats

### 11. Esports — `games_df` Bug Fixed (Code Change)
**Status**: `extract_game_features` in `backend/features/esports.py` had `games_df` undefined for economy/vision/side/ELO/form features — `all_games_df` was loaded but not aliased  
**Fix applied**: Added `games_df = all_games_df` alias after `all_games_df = self._load_all_games()`  
**Retrain needed**: CSGO, LoL, VALORANT, DOTA2 all in phase 2 retrain script  

---

## Medium Priority (Feature Enhancement Opportunities)

### 12. NBA — Market Signals 2026 Present But Schedule Fatigue Only Through 2025
**Files available**: `market_signals_2025.parquet`, `market_signals_2026.parquet`, `schedule_fatigue_2025.parquet`, `schedule_fatigue_2026.parquet`  
**Status**: Both 2025 and 2026 data present — well covered ✅  
**Note**: NBA is the best-covered sport for contextual features  

### 13. NFL — Market Signals Only 2025 (Limited Coverage)
**Files available**: `market_signals_2025.parquet` (497 rows), `schedule_fatigue_2025.parquet` (572 rows)  
**Status**: Data exists but covers only ~1 season  
**Fix needed**: Add 2020–2024 historical market signals to improve model training coverage  

### 14. MLB — Weather Features Sparse
**File(s)**: `data/normalized/mlb/games_{year}.parquet`  
**Columns affected**: `weather_temp`, `weather_wind`, `weather_cold`, `weather_dome`, `weather_precip`  
**Status**: 50%+ missingness (identified in weak sports report as high-missingness features)  
**Impact**: Cannot reliably model weather effects on run scoring  
**Fix needed**: Backfill venue weather data from Weather API by game date/location  

### 15. NCAAF/NCAAB/NCAAW — No Market Signals Data
**Status**: No `market_signals_*.parquet` files exist for any NCAA sport  
**Impact**: NCAA models cannot use odds movement as features  
**Fix needed**: Add market signals files for NCAA sports (especially NCAAF and NCAAB which have active betting markets)

### 16. F1 — Data Coverage
**File(s)**: `data/normalized/f1/`  
**Status**: F1 data present 2020–2026 but qualifying times (`q_time_ms`, `gap_to_pole_ms`) often 0.0  
**Impact**: Key predictive features (qualifying gap to pole) are not populated  
**Fix needed**: Backfill qualifying session data (Q1/Q2/Q3 times) per driver per race  

### 17. Soccer — xG (Expected Goals) Data Sparse Across Seasons
**File(s)**: `data/normalized/{epl,laliga,bundesliga,seriea,ligue1,mls,nwsl,ucl}/games_{year}.parquet`  
**Columns affected**: `home_xg`, `away_xg`, `home_xg_against`, `away_xg_against`  
**Status**: Present for EPL/LaLiga/Bundesliga (2021+) but missing for MLS, NWSL, UCL older seasons  
**Impact**: `xg_diff`, `xg_against_diff` features return 0.0 for these seasons  
**Fix needed**: Backfill xG data from StatsBomb, Understat, or Football Reference for all soccer leagues 2020+  

---

## New Features Added This Session (2026-04-01)

### Feature Extractor Improvements

**Football (NFL/NCAAF)** — Added 10 EPA/efficiency differentials:
- `epa_per_play_diff`, `pass_epa_diff`, `rush_epa_diff`, `success_rate_diff`, `explosive_play_rate_diff`
- `defensive_success_rate_diff`, `turnover_rate_diff`, `red_zone_td_rate_diff`, `third_down_conv_rate_diff`, `third_down_stop_rate_diff`
- Feature count: 183 total

**Hockey (NHL)** — Added 9 physical/goalie differentials:
- `corsi_diff`, `fenwick_diff`, `save_pct_diff`, `power_play_pct_diff`, `penalty_kill_pct_diff`
- `shots_pg_diff`, `hits_diff`, `takeaways_diff`, `faceoff_win_pct_diff`
- Feature count: 130 total

**Soccer (All leagues)** — Added 15 total differentials:
- Offense (10): `xg_diff`, `xg_against_diff`, `shots_on_target_diff`, `possession_diff`, `pass_accuracy_diff`, `corners_diff`, `saves_diff`, `shot_conversion_diff`, `pressure_diff`, `chance_quality_diff`
- Defense (5): `tackles_diff`, `clearances_diff`, `interceptions_diff`, `yellow_cards_diff`, `goals_conceded_diff`
- Feature count: 149 total

**Baseball (MLB)** — Added 12 ERA/WHIP/wOBA/wRC+ differentials:
- `era_diff`, `whip_diff`, `k9_diff`, `bb9_diff`, `obp_diff`, `slg_diff`, `ops_diff`
- `woba_diff`, `wrc_plus_diff`, `iso_diff`, `team_era_diff`, `team_batting_avg_diff`
- Added `_advanced_batting_team()` to load `advanced_batting.parquet`
- Feature count: 133 total

**Basketball (NBA/NCAAB/NCAAW/WNBA)** — Added 9 off/def/net rating differentials:
- `off_rtg_diff`, `def_rtg_diff`, `net_rtg_diff`, `ts_pct_diff`, `efg_pct_diff`
- `ast_ratio_diff`, `tov_ratio_diff`, `orb_pct_diff`, `drb_pct_diff`
- Feature count: 223 total

**Combat (UFC)** — Added 2 differentials:
- `sig_absorbed_diff`, `early_finish_rate_diff`; added `sig_strikes_absorbed` metric
- Feature count: 75 total

**Esports (CSGO/DOTA2/LoL/VALORANT)** — Added 6 team differentials + fixed games_df bug:
- `kills_pg_diff`, `kda_diff`, `objectives_pg_diff`, `first_objective_diff`, `gold_per_min_diff`, `towers_pg_diff`
- Feature count: 90 total (varies by data availability per game title)

**Tennis (ATP/WTA)** — Added 11 serve/surface/ELO differentials:
- `surface_win_pct_diff`, `first_serve_pct_diff`, `first_serve_won_pct_diff`, `second_serve_won_pct_diff`
- `break_point_conversion_diff`, `break_points_saved_pct_diff`, `aces_per_match_diff`, `double_faults_per_match_diff`
- `set_win_rate_diff`, `elo_diff`, `fatigue_diff`
- Feature count: 77 total

### ML Infrastructure Improvements

**train.py — Variance Filter Added**:
- `_apply_variance_filter(X_train, X_val)` method: drops near-zero-variance columns (std < 1e-6) computed from train data, applies same column set to val data via `reindex(fill_value=0)`
- Prevents sklearn errors when val set has zero-variance for features train set doesn't

**train.py — _META_COLS Expanded**:
- Added 14 motorsport current-race outcome columns: `podium`, `points_finish`, `dnf`, `fastest_lap`, `laps_completed`, `laps_completion_pct`, `avg_speed_kph`, `pit_stops`, `avg_pit_time_s`, `safety_car_count`, `dnf_count`, `red_flag_count`, `race_pit_stops_total`
- Total `_META_COLS` entries: 64

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
2. **[CRITICAL]** F1: Backfill qualifying session times (Q1/Q2/Q3 per driver per race) so `q_time_ms` and `gap_to_pole_ms` are populated (currently 0.0 for all races)  
3. **[HIGH]** NHL: Backfill period scores (`home_q1`, `home_q2`, `home_q3`) for all seasons  
4. **[HIGH]** NWSL: Backfill all advanced match statistics  
5. **[HIGH]** ATP/WTA: Add set-by-set scores; create WTA `odds_history.parquet`  
6. **[MEDIUM]** Add `market_signals_{year}.parquet` for: UCL, NWSL, NCAAF, NCAAB, NCAAW, ATP, WTA  
7. **[MEDIUM]** UFC: Backfill per-fight performance metrics  
8. **[MEDIUM]** MLB: Backfill weather data for all stadium games  
9. **[MEDIUM]** Soccer: Backfill xG data for MLS, NWSL, UCL older seasons (2020–2022)  
10. **[LOW]** eSports (CSGO/LoL/VALORANT): Fix player-to-game ID reconciliation; backfill kills/KDA/gold stats  
11. **[LOW]** NFL/NBA/NHL: Extend market signals back to 2020 for richer training history  

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
5. **[HIGH]** Europa/LigaMx: Expand data coverage to 2020–2023 seasons (currently only 2024–2025, ~176/342 games)  
6. **[HIGH]** LPGA: Add historical seasons back to 2020 (currently 2024–2026 only, ~68 tournaments); add `player_stats_{year}.parquet` with per-tournament scoring  
7. **[HIGH]** IndyCar: Add per-driver finishing results per race (currently only race-level data with 35 total races — insufficient for ML); format: one row per driver per race with `position`, `laps_completed`, `dnf`, `points`  
8. **[MEDIUM]** Add `market_signals_{year}.parquet` for: UCL, NWSL, NCAAF, NCAAB, NCAAW, ATP, WTA, Europa, LigaMx, LPGA  
9. **[MEDIUM]** UFC: Backfill per-fight performance metrics (currently high missingness in striking/grappling stats for older fights)  
10. **[MEDIUM]** MLB: Backfill weather data for all stadium games  
11. **[MEDIUM]** Soccer (all leagues): Add per-player `goals_pg`, `assists_pg`, `xG_pg` to `player_stats_{year}.parquet` for richer attacking depth features  
12. **[LOW]** eSports (DOTA2/LoL/CSGO): Fix player-to-game ID reconciliation; backfill kills/KDA/gold stats  
13. **[LOW]** NFL/NBA/NHL: Extend market signals back to 2020 for richer training history  
14. **[LOW]** Golf/LPGA: Add course-specific historical data (course distance, par, avg winning score) for venue-adjusted predictions  

---

## New Sports Status (Added This Session)

| Sport | Data Available | Games | Seasons | Status |
|-------|---------------|-------|---------|--------|
| Europa League | Yes | ~176 | 2024–2025 | ✅ Training in phase 2 — needs more history |
| LigaMx | Yes | ~342 | 2024–2025 | ✅ Training in phase 2 — needs more history |
| LPGA | Yes | ~68 tournaments | 2024–2026 | ✅ Training in phase 2 — needs more history |
| IndyCar | Yes (race-level only) | 35 races | 2024–2026 | ❌ Insufficient — needs per-driver format |

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
