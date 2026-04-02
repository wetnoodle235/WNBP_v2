# Missing Data Report — Sports Prediction System v5.0

**Purpose**: This document tracks data gaps in normalized datasets that limit model accuracy.
A separate data-ingestion agent should address these gaps to improve prediction quality.

**Last Updated**: 2026-04-02 14:00 — **Repair pipeline running**: Fixed hockey extractor `_skater_features()` AttributeError for early seasons (2020-2023). Fixed `consolidate_features.py` overwrite bug (was destroying historical data on every pipeline run). Repair extraction running for all truncated sports. Added quality-weighted form + opponent-adjusted efficiency to basketball extractor. New missing data discoveries: UFC weight_class/method/physical attributes empty; UFC 2020-2023 data was not being extracted (supplementary repair queued).

New markets added this session:
- **NHL**: Dynamic shots O/U (shots_over_low/mid/high) + `home_shots_advantage`
- **MLB**: Per-inning NRFI (innings 1-9: `nrfi_i1..9`), F7 winner/over, late innings (7-9) over
- **NBA/NFL/Basketball**: Close game, blowout win, one-score game (NFL)
- **NFL/NCAAF**: 2H total O/U at 3 lines, NFL scoring surge (2H > 1H)
- **All sports**: High/low scoring games (90th/10th pct total)
- **Soccer/all**: BTTS combos (btts_home_win, btts_away_win, btts_draw)
- **eSports**: 2-0 sweep markets, decider map market
- **F1/IndyCar**: Motor podium/points/DNF/fastest lap

Previously: NBA v6 79.58% (star_net_rating fix + shooting_trend). NCAAW v4 83.78%. NCAAF v4 79.18%. WTA 60.33%, ATP 62.07% (prestige_form). New markets: NFL/NCAAF total turnovers O/U + home_more_turnovers. MLB re-extraction with bat_ features. Full re-extraction pipeline for NHL(goal_streak), NBA(shooting_trend+star_net_rating), all 10 soccer leagues(h2h_home), ATP/WTA(prestige_form), WNBA/NCAAB/NCAAW(shooting_trend+star_net_rating). NFL/NCAAF re-extraction for turnovers_game target.


---

## Current Model Accuracy Summary — 2026-04-02 Retrain Results (Latest)

> All sports retrained with latest features and 2026 season data included.
> Accuracy = best winner-classification accuracy on validation set.
> 5-tier confidence brackets active: ULTRA_HIGH(>90%), VERY_HIGH(80-90%), HIGH(70-80%), MEDIUM(60-70%), LOW(50-60%)

| Sport | Winner Acc | Best Model | Training Seasons | Status | Notes |
|-------|-----------|-----------|-----------------|--------|-------|
| **NBA** | **79.58%** | catboost | 2020-2026 | ✅ v6 complete + extra markets | +shooting_trend, star_net_rating fix |
| **NFL** | **73.68%** | logistic | 2022-2025 | ✅ v4 + turnovers market queued | Re-extraction queued for turnovers_game col |
| **NHL** | **57.04%** | catboost | 2022-2026 | ✅ v3 + re-extraction queued | +goal_streak_features (6 new feats) |
| **MLB** | **58.09%** | catboost | 2022-2026 | 🔄 Re-extraction running (bat_ features) | NRFI, F5, hits markets active |
| **NCAAB** | **79.46%** | logistic | 2021-2026 | ✅ v3 + re-extraction queued | +shooting_trend |
| **NCAAW** | **83.78%** | catboost | 2023-2026 | ✅ v4 complete + extra markets done | +shooting_trend, star_net_rating fix |
| **NCAAF** | **79.18%** | catboost | 2021-2025 | 🔄 v4 extra markets in progress | QB impact features + OT model |
| **WNBA** | **62.82%** | grad_boost | 2022-2026 | ✅ v4 + re-extraction queued | +shooting_trend |
| **EPL** | **70.83%** | random_forest | 2022-2025 | ✅ v3 + re-extraction queued | +h2h_home features |
| **LaLiga** | **75.16%** | random_forest | 2022-2025 | ✅ v3 + re-extraction queued | +h2h_home features |
| **Bundesliga** | **82.96%** | adaboost | 2022-2025 | ✅ v3 + re-extraction queued | +h2h_home features |
| **Ligue1** | **75.51%** | lightgbm | 2022-2025 | ✅ v3 + re-extraction queued | +h2h_home features |
| **SerieA** | **77.19%** | logistic | 2022-2025 | ✅ v3 + re-extraction queued | +h2h_home features |
| **UCL** | **80.41%** | grad_boost | 2022-2025 | ✅ v3 + re-extraction queued | +h2h_home features |
| **MLS** | **72.35%** | random_forest | 2022-2026 | ✅ v5 + re-extraction queued | +h2h_home features |
| **LigaMx** | **70.59%** | knn | 2023-2025 | ✅ v3 + re-extraction queued | +h2h_home features |
| **Eredivisie** | **84.44%** | logistic | 2023-2025 | ✅ v1 + re-extraction queued | +h2h_home features |
| **Championship** | **71.43%** | — | 2023-2025 | ✅ v2 + re-extraction queued | +h2h_home features |
| **PrimeiraLiga** | **81.91%** | catboost | 2023-2025 | ✅ v2 + re-extraction queued | +h2h_home features |
| **ATP** | **62.07%** | catboost | 2022-2025 | ✅ v4 + re-extraction queued | +prestige_form features |
| **WTA** | **60.33%** | grad_boost | 2022-2025 | ✅ v4 + re-extraction queued | +prestige_form features |
| **NWSL** | — | — | — | ✅ Model exists (older run) | Needs retrain with new markets |
| **F1** | ~65% | — | — | ✅ Model from Mar 30 | Needs refresh |
| **Golf** | ~93% | — | — | ✅ Model exists | — |
| **UFC** | **53.85%→71%** | — | **2024-2026 only (1142 rows)** | ⚠️ NEEDS REPAIR — 2020-2023 data exists but never extracted | See UFC repair below |
| **CSGO** | ~58% | — | — | ✅ Model exists | — |
| **DOTA2** | ~65% | — | — | ✅ Model exists | — |
| **LoL** | ~64% | — | — | ✅ Model exists | — |
| **VALORANT** | ~62% | — | — | ✅ Model exists | — |
| **Bundesliga2** | ❌ NO DATA | — | — | ❌ No normalized data | See missing data section |
| **SerieB** | ❌ NO DATA | — | — | ❌ No normalized data | See missing data section |
| **Ligue2** | ❌ NO DATA | — | — | ❌ No normalized data | See missing data section |
| **WorldCup** | ❌ NO DATA | — | — | ❌ No normalized data | See missing data section |
| **IndyCar** | ❌ BLOCKED | — | 2024-2026 only (35 races) | ❌ Race-level only; need per-driver | See missing data section |
| **LPGA** | ❌ BLOCKED | — | 2024-2026 only (68 games) | ❌ Insufficient data for training | See missing data section |

### Prediction Markets Supported (as of 2026-04-01)

| Market | Sports | Notes |
|--------|--------|-------|
| Winner (H/A/D) | All sports | 3-outcome for soccer/NFL; 2-outcome for others |
| Draw / OT probability | All sports | NFL/NBA "draw" = OT likely; soccer = actual draw |
| Spread/ATS | All sports | Point spread over/under |
| Total (O/U) | All sports | Combined score over/under |
| Halftime winner | Soccer, NBA, NFL, NCAAF, WNBA | H1 score data required |
| H1 total / H2 total | Soccer (most leagues), NBA, NFL | Halftime scores required |
| Q1/Q2/Q3/Q4 O/U | NBA, NFL, NCAAF, WNBA | Quarter data required |
| Q3/Q4 combined O/U | NBA, NFL, NCAAF, WNBA | Second half over/under |
| HT/FT Double Result | Soccer, NBA, NFL, NCAAF | Lead at HT AND final winner |
| BTTS (Both Teams Score) | Soccer all leagues | Both teams to score yes/no |
| BTTS Both Halves | Soccer all leagues | Both teams score in BOTH halves |
| First half BTTS | Soccer | Both score in H1 |
| Corners O/U | Soccer (EPL/LaLiga/Bundesliga/Ligue1/SerieA) | Requires corners data |
| Cards O/U | Soccer (same as corners) | Requires card data |
| Turnovers O/U | Basketball (NBA/NCAAB/NCAAW/WNBA) | Requires turnover data |
| NFL/NCAAF Turnovers O/U | NFL, NCAAF | Total turnovers (INT+fumbles lost) O/U at 3 lines |
| NFL Home More Turnovers | NFL, NCAAF | Which team turns it over more |
| NFL 2H Total O/U | NFL, NCAAF | Second half combined total at 3 dynamic lines |
| NFL Scoring Surge | NFL, NCAAF | P(2nd half total > 1st half total) |
| Close Game | NBA/WNBA/NCAAB/NCAAW/NFL/NCAAF | P(margin ≤ sport threshold, e.g. 10 pts for NBA) |
| Blowout Win | NBA/WNBA/NCAAB/NCAAW/NFL/NCAAF | P(winner covers large blowout margin) |
| One-Score Game | NFL, NCAAF | P(final margin ≤ 8 points) |
| High/Low Scoring | NHL/NBA/WNBA/NCAAB | P(total > 90th pct) or P(total < 10th pct) |
| Both Teams High Score | NHL/NBA/WNBA/NCAAB | P(both teams score above their medians) |
| NHL Shots O/U (dynamic) | NHL | Total shots over dynamic median line (±5) |
| NHL Home Shots Advantage | NHL | P(home team outshots opponent) |
| MLB NRFI (innings 1-9) | MLB | No run first inning, per inning for all 9 innings |
| MLB F7 Winner | MLB | P(home leads after 7 innings) |
| MLB F7 Over/Under | MLB | Total runs through 7 innings vs dynamic line |
| MLB Late Innings (7-9) | MLB | Combined runs innings 7-9 over dynamic line |
| BTTS + Home Win | Soccer all leagues | BTTS AND home wins (GG & 1) |
| BTTS + Away Win | Soccer all leagues | BTTS AND away wins (GG & 2) |
| BTTS + Draw | Soccer/NFL/OT sports | BTTS AND draw/OT outcome |
| eSports 2-0 Sweep | CSGO/LoL/Valorant/Dota2 | P(home wins 2-0 clean sweep) |
| eSports Away Sweep | CSGO/LoL/Valorant/Dota2 | P(away wins 2-0) |
| eSports Decider Map | CSGO/LoL/Valorant/Dota2 | P(series goes to decisive 3rd map) |
| Motor Podium | F1 | P(driver finishes top 3) |
| Motor Points Finish | F1 | P(driver finishes top 10, scores points) |
| Motor DNF | F1 | P(driver does not finish) |
| Motor Fastest Lap | F1 | P(driver sets fastest lap) |
| 5-Tier Confidence | All | ULTRA_HIGH(>80%)/VERY_HIGH(70-80%)/HIGH(60-70%)/MEDIUM(55-60%)/LOW(<55%) |

**Total markets per game**: 40-60+ depending on sport (soccer/MLB have the most).

---

## New Improvements Added (2026-04-02 Phase 5 — Latest Session)

### CRITICAL BUG FIX: `_split_home_away` Missing from BaseExtractor

**Problem**: `BasketballExtractor._shooting_trend()` calls `self._split_home_away()` which was only defined in `HockeyExtractor`, not in `BaseExtractor`. This caused ALL NBA, WNBA, NCAAB, and NCAAW game extractions to fail with `AttributeError: 'BasketballExtractor' object has no attribute '_split_home_away'`. The result was 0 valid features extracted — models were being retrained on empty/default-only data.

**Fix**: Added `_split_home_away(self, recent, team_id)` to `backend/features/base.py` (line ~423). All sport extractors now inherit this method. Basketball fix watcher queued to re-extract all 4 basketball sports after main watcher passes.

**Impact**: NBA/WNBA/NCAAB/NCAAW models were trained on essentially random features. After re-extraction with fix, expect accuracy improvements.

### New Features Added Per Sport (Phase 5)

| Sport | Feature(s) Added | Description |
|-------|-----------------|-------------|
| Tennis | `home/away_tiebreak_win_pct`, `tiebreak_win_pct_diff` | Fraction of tiebreak sets won (pressure/clutch metric using q1-q5 scores where any score=7) |
| NHL | `team_pp_goals_pg`, `sk_team_pp_goals_pg_diff` | Avg powerplay goals per game from player_stats (pp_goals col, 31% fill rate) |
| NFL/NCAAF | `nfl_ps_kick_return_avg`, `nfl_ps_punt_return_avg`, diffs | Avg kick/punt return yards from player_stats (kr_avg, pr_avg cols) |
| Golf | `score_to_par_trend` | Linear slope of score_to_par over last 5 tournaments (positive = improving) |
| Baseball | `bat_runs_pg`, `bat_rbi_pg`, diffs | Avg runs + RBI per game from batter_game_stats rolling window |
| Basketball | `ps_off_rating`, `ps_def_rating`, `ps_off_rating_diff`, `ps_def_rating_diff` | Separate off/def rating from net_rating (off_rating and def_rating cols in NBA player_stats) |
| Soccer | `home/away_assists_pg`, `assists_pg_diff` | Avg team assists per game from player discipline features |

### Data Coverage Notes

- **NHL pp_toi**: Previously thought to be entirely null — actually **61.3% non-null** in 2024 player_stats. `pp_goals` is 31.5% non-null. Both are now used.
- **NFL player_stats position**: ALWAYS empty string `""` — use `pass_att >= 5` to identify QB rows.
- **MLB batter_game_stats**: Has `runs` and `rbi` columns (35% fill via pa/ab relationship). `lob` (left on base) NOT present in batter_game_stats.
- **Golf _fast_momentum**: Now tracks score_to_par linear trend in addition to finish position trend.

---

## New Improvements Added (2026-04-02 — Current Session)

### Tennis UUID Player ID Bridge (CRITICAL FIX)

**Problem**: ATP/WTA games for 2025/2026 use UUID-based `home_team_id` values (e.g. `"5dc21899-ba8c-2090-ba30-c0169a979977"`) but `player_stats_*.parquet` files use numeric ESPN player IDs (e.g. `"105777"`). The `_player_stats_index` keyed by numeric IDs returns `None` for UUID lookups — causing ALL serve statistics to return 0 for 71% of ATP/WTA games (2025: 5991/7678 UUID games; 2026: 2535 games).

**Fix applied in `backend/features/tennis.py`**:
- Added `_name_to_player_id: dict[str, str]` cache to `__init__()`
- Added `_build_name_id_bridge(sport_dir)` method: reads all games parquets, extracts `(player_name_lower, numeric_player_id)` pairs from rows where `home_team_id` matches `\d+` pattern (2023/2024 games), builds name→ID lookup
- Added `_resolve_player_id(player_id, player_name)` method: detects UUID pattern (contains `-`, not purely numeric), looks up player name in bridge
- Fixed `_load_all_player_stats()` to call `_build_name_id_bridge()` even when player_stats files are empty (early return path was skipping bridge build)
- Updated `extract_game_features()`: resolves `h_id_resolved = _resolve_player_id(h_id_raw, h_name)` and passes resolved numeric ID to `_player_serve_stats()`; raw UUID IDs still used for game-history lookups (form, H2H, fatigue)

**Results**: Bridge built 551 player name→ID mappings from 2023/2024 games. **3401/5991 (57%) of 2025 UUID games now resolve to numeric IDs** for serve stat lookups. Remaining ~43% are players who only appeared in 2025/2026 (no 2023/2024 history to bridge from).

**Still needed (data team)**:
- 2025 ATP/WTA `player_stats_{year}.parquet` files have NO serve columns (`aces`, `double_faults`, `first_serve_pct`, `first_serve_won_pct`, `second_serve_won_pct`, `break_points_won`, `break_points_faced`). Only basic columns present: `sets_won`, `sets_lost`, `games_won`, `games_lost`, `won`, `total_sets`. **Add full serve stat columns to 2025/2026 player_stats parquets** to enable serve-based features for current season games.
- **Add `home_team_id` numeric-to-UUID bridge** OR use player name as the canonical identifier for 2025/2026 games so UUID → numeric ID is 100% resolved (currently only 57% bridged).

### NFL QB Features — Accuracy Jump
- **NFL v5 accuracy: 75.22%** (random_forest) — up from 55.46%! 
- Added `_qb_impact_features()` using `pass_att >= 5` to identify starting QB rows (position column is ALWAYS empty string in NFL player_stats)
- 12 new QB features: `nfl_home_qb_rtg`, `nfl_home_qb_ypa`, `nfl_home_qb_td_pct`, `nfl_home_qb_int_rate`, `nfl_home_qb_pass_yds_pg`, `nfl_home_qb_rush_yards_pg` (+ away equivalents + diffs)
- **Key finding**: `player_stats.position` is always `""` for ALL 17,801 NFL player rows — use `pass_att >= 5` as QB identifier. Column is `pass_rating` (not `passer_rating`).

### Soccer Watcher Fix
- Smart soccer retrain watcher (`retrain_soccer_smart.sh`) was failing because LEAGUES list contained `primeira_liga` (with underscore) but actual feature parquet and normalized directory is `primeiraliga` (no underscore)
- Fixed: killed old watcher, launched `retrain_all_pending.sh` with correct league names
- All 10 soccer feature parquets confirmed present and populated: epl(2209), bundesliga(1773), laliga(1840), seriea(2210), ligue1(1292), mls(1759), ligamx(342), eredivisie(635), championship(1232), primeiraliga(625)

### Star Absence Detection (Basketball)
- Added `_star_absence_features()` to `basketball.py`: detects top-3 starters by avg minutes (≥15 min threshold), checks if they appear in game player_stats (absent = didn't play)
- 7 new features: `home_stars_absent`, `home_star_absence_severity`, `home_star_minutes_lost`, `home_stars_present_pct`, `home_top_star_absent` (+ away equivalents)
- NBA: 342 total features (was 335)

### European Competition Fatigue (Soccer)
- Added `_european_competition_features()` to `soccer.py`: detects if EPL/LaLiga team played UCL/Europa in prior 7 days
- 8 new features per game: `home_played_ucl_recent`, `home_played_europa_recent`, `home_days_since_euro_game`, `home_ucl_games_this_month` (+ away equivalents)
- 154/2209 EPL games (7%) have a team that played CL/Europa in prior 7 days

### Model Accuracy Update (2026-04-02)

| Sport | Winner Acc | Best Model | Notes |
|-------|-----------|-----------|-------|
| **NFL** | **75.22%** | random_forest | Up from 55.46%! QB impact features |
| **NBA** | **81.68%** | logistic | Star player features; re-extraction pending |
| **NCAAF** | **78.71%** | catboost | Multi-season; v3 retrain complete |
| NHL | (retrain in progress) | — | New 9138-row parquet; goalie features |
| NCAAB | (retrain in progress) | — | Multi-season extraction |
| NCAAW | (retrain in progress) | — | Multi-season extraction |
| ATP | 65.25% (old) | adaboost | UUID bridge fix applied; re-extraction pending |
| WTA | 63.32% (old) | naive_bayes | Same UUID fix; re-extraction pending |
| All soccer | Various | Various | Re-extraction in progress via watcher |

---



### Feature Extractor Improvements

**Baseball (MLB) — wOBA / BABIP Team ID Mismatch (FIXED)**
- **Root cause**: `_advanced_batting_team()` compared ESPN numeric team IDs (`'15'`, `'21'`) against MLB abbreviations (`'ATL'`, `'NYM'`) in `advanced_batting_*.parquet`
- **Fix**: Added `_TEAM_ID_TO_ABBREV` translation dict (30 teams) at module level in `baseball.py`; `_advanced_batting_team()` now converts ID → abbreviation before mask comparison; falls back to original ID if abbreviation lookup fails
- **Before**: `home_woba: 0.0`, `home_babip: 0.0` for ALL games
- **After**: `home_woba: 0.294`, `home_babip: 0.291` (realistic league-average values)
- **Impact**: MLB wOBA/BABIP features now provide real signal. Expected accuracy improvement in MLB winner and run total predictions. Fresh MLB v5 retrain queued.
- **Cache action**: Deleted stale `mlb_all.parquet` so retrain uses fresh extraction with fix

**Football (NFL) — New Player-Stats Efficiency Metrics (5 new features)**
- Added to `_nfl_player_features()` in `football.py`:
  - `nfl_ps_yds_per_attempt` — passing yards per attempt (aggregate QB rolling average)
  - `nfl_ps_completion_pct` — completion percentage (pass_cmp / pass_att × 100)
  - `nfl_ps_yds_per_carry` — rushing yards per carry
  - `nfl_ps_pass_td_pg` — passing touchdowns per game from QB rows
  - `nfl_ps_fumbles_lost_pg` — fumbles lost per game (offensive turnover metric)
- Each new feature: 5 home + 5 away + 5 diff = **15 new canonical features**
- NFL canonical features: **279** (was 264)
- Impact: NFL v3 winner accuracy: xgboost **76.11%** (v2 was 75.81%, +0.30pp improvement)
- All new features automatically get diff calculations via the dynamic loop

**Basketball (NBA/WNBA/NCAAB/NCAAW) — Free Throw Rate + Turnover %**
- Added to `_team_box_stats()` in `basketball.py`:
  - `ftr` (free throw rate) = `fta / fga` — ability to draw fouls (typical NBA ~20-25%)
  - `tov_pct` (turnover %) = `to / (fga + 0.44*fta + to) × 100` — possession efficiency (typical NBA ~13%)
- Added diff features: `ftr_diff`, `tov_pct_diff` (inverted: away - home, lower is better)
- NBA canonical features: **301** (was 295)
- Sample values: `home_ftr: 0.206`, `home_tov_pct: 14.4%`
- Fresh NBA/WNBA/NCAAB/NCAAW v5 retrains queued after cache invalidation

### Feature Cache Issues Found & Fixed

- **`nba_all.parquet`**: 332 cols — missing `home_ftr`, `home_tov_pct` (generated before this session's changes)
- **`mlb_all.parquet`**: 251 cols — `home_woba: 0.0` for all 3230 rows (generated before team-ID fix)
- **`ncaaw_all.parquet`**: 309 cols — missing `home_ftr`, `home_tov_pct`
- **Action**: All three deleted; fresh extraction queued via `/tmp/fresh_retrain_pipeline.sh`
- **NHL cache** (`nhl_all.parquet` 241 cols): Valid — `home_pp_pct` present; hockey.py not changed this session

---

## New Improvements Added (2026-04-03 — Latest Session)

### Quality-Weighted Form Features Added to All Sports

**Purpose**: Wins against strong opponents should be weighted more than wins against weak opponents.
A simple `win_pct` doesn't distinguish between beating a .700 team vs a .200 team.

**Implementation across extractors:**
- **Basketball** (`basketball.py`): `_quality_weighted_form()` + `_opp_adjusted_efficiency()` — optimized to O(n log n) by grouping unique opponents
- **Soccer** (`soccer.py`): `_quality_weighted_form()` + `_opp_adjusted_xg()` — xG vs opponent defensive quality
- **Hockey** (`hockey.py`): `_quality_weighted_form()` + PP vs PK cross-matchup (`pp_vs_pk_home_adv`)
- **Baseball** (`baseball.py`): `_quality_weighted_form()` — `mlb_quality_form_diff`, `mlb_quality_win_rate_diff`
- **Football** (`football.py`): `_quality_weighted_form_nfl()` — window=8 for shorter NFL season
- **eSports** (`esports.py`): `_quality_weighted_form()` — win rate weighted by opponent recent form
- **UFC/Combat** (`combat.py`): `_quality_weighted_record()` — win rate + finish rate weighted by opponent career win%
- **Tennis** (`tennis.py`): `_ranking_quality_form()` — win rate weighted by opponent ranking (rank 1 = best)

**Formula**: `quality_form = dot(wins * 2 - 1, opp_quality) / n_games`
- Positive = strong form vs quality opponents
- Negative = poor form or wins only vs weak opponents

**New features added per sport**:
- Basketball: `home_quality_form`, `home_quality_win_rate`, `home_adj_net_rtg`, `home_adj_off_rtg`, `home_adj_def_rtg` (+ away + diffs)
- Soccer: `quality_form_diff`, `adj_xg_net_diff`, `adj_xg_attack`, `adj_xg_defense` differentials
- Hockey: `nhl_quality_form_diff`, `pp_vs_pk_home_adv`, `pp_pk_matchup_diff`
- Baseball: `mlb_quality_form_diff`, `mlb_quality_win_rate_diff`
- Football: `nfl_quality_form_diff`, `nfl_quality_win_rate_diff`
- eSports: `quality_form_diff`, `quality_win_rate_diff`
- UFC: `quality_form_diff`, `quality_win_rate_diff`, `quality_finish_rate_diff`
- Tennis: `home_ranking_quality_form`, `ranking_quality_form_diff`

### WTA Features Extraction (First Time)

**Problem**: `wta_all.parquet` never existed despite WTA having 16699 rows of normalized data across 2020-2026.
**Action**: Added WTA to quality-features re-extraction script (`/tmp/repair_quality_features.sh`).
**Expected features**: ~280 features matching ATP extractor. Ranking quality form included.

### Re-Extraction Pipeline Running (2026-04-02 10:07 AM)

Two repair pipelines now running in parallel:
1. **`repair_parquets_v2.sh`** (PID 3113916) — Re-extracts NHL ✅, MLB 🔄, soccer leagues, NCAAB/NCAAW, eSports
2. **`repair_quality_features.sh`** (PID 3446148) — Re-extracts NBA 🔄, WNBA, NFL, NCAAF, ATP, WTA, CSGO, UFC

### Current Repair Status

| Sport | Status | Notes |
|-------|--------|-------|
| NHL | ✅ Done (7200+ rows) | 7 seasons extracted |
| MLB | 🔄 Running | Was 426 rows; expected ~15000+ |
| NBA | 🔄 Running | 8594 rows + quality_form features |
| WNBA | ⏳ Queued | After NBA completes |
| NFL | ⏳ Queued | After WNBA |
| NCAAF | ⏳ Queued | After NFL |
| ATP | ⏳ Queued | After NCAAF |
| WTA | ⏳ Queued | FIRST extraction ever |
| CSGO | ⏳ Queued | Add quality features |
| EPL+leagues | ⏳ Queued (in v2) | After MLB |
| NCAAB/NCAAW | ⏳ Queued (in v2) | After soccer |
| UFC | ⏳ In supplementary | Needs 2020-2023 data |

---

## New Improvements Added (2026-04-02 — Current Session)

**`consolidate_features.py` — Historical Data Destruction (FIXED)**
- **Root cause**: `consolidate_sport()` globbed only current-season seasonal parquets and wrote them to `_all.parquet`, overwriting all historical multi-season data on every daily pipeline run.
- **Fix**: Now merges new seasonal data INTO existing `_all.parquet` instead of rebuilding from scratch. Deduplicates by `game_id` after merge.
- **Impact**: Prevented catastrophic training data loss for 20+ sports. NHL was truncated to 1203 rows (2026 only); MLB to 426 rows (0+2026 only).

**`hockey.py` — `'int' object has no attribute 'fillna'` (FIXED)**
- **Root cause**: Early NHL seasons (2020-2023) lack `pp_goals` column in player_stats. `ps.get("pp_goals", 0)` returns scalar `0`; `pd.to_numeric(0).fillna(0)` fails with AttributeError.
- **Fix**: Added helper `_col()` function in `_skater_features()` returning `np.zeros(n_ps)` when column is absent. All arrays in `sk_cache` now have identical length `n_ps = len(ps)`.
- **Impact**: NHL 2020 season: 1187/1187 games extracted (0 failed). Prior: 1187/1187 failed.
- **Verified**: NHL 2020 season: 1187/1187 games. NHL 2021 season: 949/949 games. 0 failures.

### Feature Improvements

**Basketball (`basketball.py`) — Quality-Weighted Form + Opponent-Adjusted Efficiency**
- Added `_quality_weighted_form()`: weights recent win (+1)/loss (-1) by opponent's win% at that time
  - Features: `home_quality_form`, `home_quality_wins`, `home_top_opp_win_pct` (+ away + diffs)
  - Quality wins = sum of wins weighted by how good the opponents were
- Added `_opp_adjusted_efficiency()`: computes team's net margin vs opponent average net margin
  - Features: `home_adj_net_rtg`, `home_adj_off_rtg`, `home_pace_vs_opp` (+ away + diffs: `adj_net_rtg_diff`, `pace_mismatch`)
  - `adj_net_rtg` = team's recent avg margin minus opponents' typical avg margin → true quality of play
- **New features per sport**: +13 features (6 quality_form + 6 opp_adj_eff + 1 pace_mismatch)
- Applies to: NBA, WNBA, NCAAB, NCAAW

### Repair Pipeline Status (2026-04-02)

| Sport | Status | Rows Before | Expected After |
|-------|--------|-------------|----------------|
| NHL | 🔄 In progress (2022 season) | 1203 (2026 only) | ~7000+ (2020-2026) |
| MLB | ⏳ Queued | 426 (0+2026) | ~15000+ (2020-2026) |
| EPL | ⏳ Queued | 309 (2025 only) | ~3000+ (2020-2026) |
| LaLiga | ⏳ Queued | 291 (0+2025) | ~3000+ (2020-2026) |
| Ligue1 | ⏳ Queued | 243 (0+2025) | ~2500+ (2020-2026) |
| SerieA | ⏳ Queued | 300 (2025 only) | ~3000+ (2020-2026) |
| UCL | ⏳ Queued | 176 (2025 only) | ~1500+ (2020-2026) |
| MLS | ⏳ Queued | 250 (2025 only) | ~2500+ (2020-2026) |
| NCAAB | ⏳ Queued | 805 (0+2026) | ~8000+ (2020-2026) |
| NCAAW | ⏳ Queued | 819 (0+2026) | ~6000+ (2020-2026) |
| Dota2 | ⏳ Queued | 936 (0+2026) | ~3000+ (2020-2026) |
| LoL | ⏳ Queued | 1109 (0+2026) | ~4000+ (2020-2026) |
| Valorant | ⏳ Queued | 1039 (0+2026) | ~3000+ (2020-2026) |
| UFC | ⏳ Queued (supplementary) | 1142 (2024-2026) | ~3500+ (2020-2026) |
| Bundesliga | ⏳ Queued (supplementary) | 243 (2025 only) | ~2000+ (2020-2026) |
| Eredivisie | ⏳ Queued (supplementary) | 0 (no file) | ~1500+ (2020-2026) |
| Championship | ⏳ Queued (supplementary) | 0 (no file) | ~2500+ (2020-2026) |
| PrimeiraLiga | ⏳ Queued (supplementary) | 0 (no file) | ~1500+ (2020-2026) |

### New Missing Data Discoveries

**UFC — weight_class and physical attributes completely empty:**
- `weight_class` field: 0/3138 non-empty across ALL seasons (2024, 2025, 2026 player_stats)
- `method` field: 0/3138 non-empty (should be KO/TKO/Submission/Decision)
- **Physical attributes missing entirely**: height, reach, weight, age, stance (orthodox/southpaw) are not in any normalized UFC files
- **Impact**: Cannot create weight-class matchup features; can't distinguish heavyweight (KO-heavy) from flyweight (decision-heavy) bouts
- **Priority**: HIGH — see Priority List item #18 (updated)

**UFC — 2020-2023 data exists but was never extracted:**
- `data/normalized/ufc/games_{2020,2021,2022,2023}.parquet` all exist (456-520 games/year)
- `player_stats_{2020,2021,2022,2023}.parquet` all exist (912-1040 rows/year) with striking stats
- Current `ufc_all.parquet`: 1142 rows from 2024-2026 only → only 3 seasons of training data
- **Fix**: Supplementary repair script `/tmp/repair_missing_sports.sh` will extract all 7 seasons
- **Expected training rows after repair**: ~3500+ (2020-2026)

**Backtest results (2026-04-02, 7-day window):**
- Overall: **64.83%** accuracy across 489 predictions
- Elite tier (>80% confidence): 80.2% accuracy — well calibrated ✅
- High tier (70-80%): 83.6% accuracy ✅
- NHL: 58.8% (improving from 49.1% — repair in progress)
- MLB: 47.7% (below random — repair critical)
- ROI simulation: +3.44% (positive ROI)

---

## New Improvements Added (2026-04-02 — Prior Session)

### Feature Extractor Bug Fixes

**Basketball (NBA/WNBA/NCAAB/NCAAW)** — Two critical missing method implementations:
- `_standings_features()`: Was called in `extract_game_features()` but never defined → silent AttributeError. Now extracts: `stnd_win_pct`, `stnd_home_win_pct`, `stnd_away_win_pct`, `stnd_pts_margin`, `stnd_conf_rank`, `stnd_div_rank`, `stnd_overall_rank`, `stnd_streak`, `stnd_l10_win_pct` (9 features each side = 18 total)
- `_player_stats_features()`: Same — was called but undefined. Now aggregates from `player_stats_*.parquet`: `ps_net_rating`, `ps_efg_pct`, `ps_ts_pct`, `ps_usg_pct`, `ps_plus_minus`, `ps_ast_to`, `ps_reb`, `ps_pts` (8 features each side = 16 total)
- Impact: ALL NBA feature extraction was failing silently (AttributeError) before this fix

**Football (NFL/NCAAF)** — Standings features completely missing (new method added):
- `_nfl_standings_features()`: Extracts from `standings_*.parquet`: win_pct, pts_margin, home_win_pct, away_win_pct, overall_rank, streak, games_played
- Added differentials: `nfl_stnd_rank_diff`, `nfl_stnd_win_pct_diff`
- Impact: 16 new features per game; NFL models can now see division standings context

**Hockey (NHL)** — Standings missing conference/division context:
- `_nhl_standings_features()`: Added `stnd_conf_rank` (NHL `conference_rank`) and `stnd_div_rank` (`division_rank`)
- Impact: 4 new features (`home_std_stnd_conf_rank`, `home_std_stnd_div_rank`, `away_std_stnd_conf_rank`, `away_std_stnd_div_rank`)

**Soccer (EPL/all leagues)** — Shot features returning zeros:
- Root cause: EPL `games_*.parquet` has no `home_shots`/`away_shots` columns → `_shot_features()` returned all zeros
- Fix: Extended `_player_discipline_features()` to aggregate `shots_pg`, `shots_on_target_pg`, `saves_pg` from `player_stats_*.parquet`
- Fixed early-return bug: When game-level card data found, method returned immediately with zeros for shots
- Impact: EPL/soccer models now have shot data from player-level aggregation

**Baseball (MLB)** — New pitcher game stats integration:
- `_starting_pitcher_stats()`: Uses `pitcher_game_stats_*.parquet` to find starting pitcher (highest innings) for each game, then computes rolling L5 stats: `spg_era_l5`, `spg_k_rate`, `spg_bb_rate`, `spg_hr9`, `spg_ip_avg`, `spg_whip`
- Differentials: `spg_era_diff`, `spg_k_rate_diff`, `spg_bb_rate_diff`
- Impact: More accurate starter quality signal vs player_stats rolling averages

### New Data Gap Findings This Session

**Soccer leagues with missing player_stats** (affects shot feature aggregation):

| League | Has player_stats | Has odds | Has market_signals | Impact |
|--------|-----------------|----------|-------------------|--------|
| Eredivisie | ❌ Missing | ❌ Missing | ❌ Missing | Shot features return 0; no odds features |
| Championship | ❌ Missing | ❌ Missing | ❌ Missing | Shot features return 0; no odds features |
| PrimeiraLiga | ❌ Missing | ❌ Missing | ❌ Missing | Shot features return 0; no odds features |
| Euros | ❌ Missing | ❌ Missing | ❌ Missing | Shot features return 0; no odds features |
| MLS | ✅ Present | ✅ Present | ✅ Present | Full features available |
| EPL | ✅ Present | ✅ Present | ✅ Present | Fixed this session |

**Recommended additions for data team** (HIGH PRIORITY for soccer leagues):
- Add `player_stats_{year}.parquet` for: eredivisie, championship, primeiraliga, euros (2022–2025)
- Required columns: `game_id`, `player_id`, `team_id`, `sport`, `season`, `date`, `shots`, `shots_on_target`, `saves`, `goals`, `assists`, `yellow_cards`, `red_cards`
- Add `odds_{year}.parquet` for: eredivisie, championship, primeiraliga, euros (2023–2025)
- Add `market_signals_{year}.parquet` for same leagues

**NBA player_stats null columns**:
- `off_rating`, `def_rating`, `net_rating` are often null in `player_stats_*.parquet`
- `_player_stats_features()` gracefully handles nulls via `fillna(0)` but real values would improve signal
- Fix needed: Ensure rating columns are populated for 2020–2026

**NHL q1/q2/q3 period scores**: `home_q1`, `away_q1`, etc. still 0% fill for most seasons
- Period prediction features (`btts_period1_prob` etc.) use proxies, not actual period data

**CRITICAL: NFL 2023-2025 — Missing Game-Level Box Score Stats**:
- **Affected seasons**: 2023, 2024, 2025 (858 of 1698 total NFL games = 50.5% of data)
- **Missing columns**: `home_passing_yards`, `away_passing_yards`, `home_rushing_yards`, `away_rushing_yards`, `home_total_yards`, `away_total_yards`, `home_turnovers`, `away_turnovers`, `home_sacks`, `away_sacks`, `home_sacks_allowed`, `away_sacks_allowed`, `home_passing_epa`, `away_passing_epa`, `home_rushing_epa`, `away_rushing_epa`, `home_total_epa`, `away_total_epa`, `home_interceptions_thrown`, `away_interceptions_thrown`, `home_interceptions`, `away_interceptions`, `home_completion_pct`, `away_completion_pct`, `home_yards_per_play`, `away_yards_per_play`, `home_first_downs`, `away_first_downs`, `home_third_down_pct`, `away_third_down_pct`, `home_red_zone_pct`, `away_red_zone_pct`, `home_possession_seconds`, `away_possession_seconds`, `home_scoring_efficiency`, `away_scoring_efficiency`, `home_penalties`, `away_penalties`, `home_penalty_yards`, `away_penalty_yards`, `home_tackles`, `away_tackles`, `home_fumbles_lost`, `away_fumbles_lost`
- **Impact**: `_game_stats_features()` returns all zeros for 50% of training data; rolling `pass_yds_pg`, `rush_yds_pg`, `epa_*` features are computed from old data only
- **Fix needed**: Backfill `games_2023.parquet`, `games_2024.parquet`, `games_2025.parquet` with full box score stats. ESPN API endpoint: `sports/football/nfl/competitions/{id}?lang=en&region=us&enable=linescores,odds,situation` returns team stats including `passing_yards`, `rushingYards`, etc.

**Football (NFL) — `pass_int` Bug Fixed** (this session):
- `_nfl_player_features()` was calling `_mean("interceptions")` but NFL `player_stats_*.parquet` uses column `pass_int`
- Column `interceptions` doesn't exist in NFL player_stats → was always returning 0.0 for `nfl_ps_def_int_pg`
- Fixed: changed to `_mean("pass_int")`

---

## New Features Added (2026-04-01 — Phase 4)

### Home/Away Split Form Feature (ALL Team Sports)

Added `home_away_form()` method to `BaseFeatureExtractor` and applied to ALL team sport extractors:
- **Basketball** (NBA/WNBA/NCAAB/NCAAW): `home_home_ha_win_pct`, `home_home_ha_ppg`, `home_home_ha_opp_ppg`, `home_home_ha_avg_margin`, `away_away_ha_*` equivalents
- **Soccer** (EPL/LaLiga/Bundesliga/etc): Same ha_form features
- **Hockey** (NHL): Same ha_form features + restored `h2h` + `momentum` that were accidentally dropped
- **Football** (NFL/NCAAF): Same ha_form features
- **Baseball** (MLB): Same ha_form features

Key differential features added to all sports:
- `ha_win_pct_diff = home_team_home_win_pct - away_team_away_win_pct` (strong signal)
- `ha_ppg_diff = home_team_home_ppg - away_team_away_ppg`

**Why this matters**: Home teams win at distinctly different rates than their overall win rate. Tracking home performance separately provides better signal than treating all games equally.

### New Sports Added to Training (Phase 4 only)

| Sport | Seasons | Data available |
|-------|---------|---------------|
| Eredivisie | 2023-2025 | ✅ 3 seasons |
| Primeira Liga | 2023-2025 | ✅ 3 seasons |
| Championship | 2023-2025 | ✅ 3 seasons |
| Euros | 2024 | ✅ 1 season (limited) |
| IndyCar | 2024-2025 | ✅ 2 seasons (race-level) |

### New Prediction Markets (Previous Session — in Phase 4)
- NBA/WNBA/NCAAB: Three-pointer O/U (dynamic lines per sport)
- NHL: Total shots O/U (55.5 / 60.5 / 65.5)
- MLB: Total hits O/U (14.5 / 16.5 / 18.5)
- Soccer (EPL/LaLiga/etc): Second-half goals O/U (0.5 / 1.5 / 2.5)

---

## New Features Added (2026-04-01 — Phase 3 + Updates)

### Feature Extractor Improvements

**Football (NFL/NCAAF)** — Added int_thrown, tackles, fumbles_lost, scoring_efficiency, last-5 scoring trends, Strength of Schedule:
- `int_thrown_pg`, `int_thrown_allowed_pg`, `tackles_pg` to `_team_stat_averages()`
- `fumbles_lost_pg` (fumbles lost per game), `scoring_efficiency` (TD conversion rate)
- `home/away_last_n_ppg`, `home/away_last_n_opp_ppg`, `home/away_last_n_margin` (last-5 games)
- `last5_ppg_diff`, `last5_margin_diff`
- `home/away_sos_rating` (opponent win% avg over last 20 games), `sos_diff`
- New differentials: `fumbles_lost_diff`, `scoring_efficiency_diff`

**Basketball (NBA/NCAAB/WNBA)** — Added points off turnovers:
- `turnover_pts_pg` (points scored from opponent turnovers per game)
- `turnover_pts_diff` (home advantage in exploiting turnovers)

**Hockey (NHL)** — Added last-5 scoring trends + injury features:
- `home/away_last5_ppg`, `home/away_last5_opp_ppg`, `home/away_last5_margin`
- `last5_ppg_diff`, `last5_margin_diff`
- Injury features: `home/away_injury_count`, `home/away_injury_severity_score`, `injury_severity_diff`

**Soccer (All leagues)** — Added xG proxy and last-5 scoring trends:
- `xg_proxy = shots_on_target_pg × shot_conversion / 100` (real xG is 100% empty — see issue below)
- `shot_quality_idx = shot_accuracy × shot_conversion / 100`
- `xg_proxy_diff`, `shot_quality_diff`
- `home/away_last_n_ppg`, `home/away_last_n_opp_ppg`, `home/away_last_n_margin` (last-5 games)
- `last5_ppg_diff`, `last5_margin_diff`

**Baseball (MLB)** — Fixed dead features, added pitcher quality metrics + injury features:
- **FIXED**: `win`, `loss`, `whip` columns are 100% NULL — now computes ERA from `earned_runs/innings` directly
- Fixed fractional innings notation (3.2 = 3+2/3 innings, not 3.2 innings)
- New: `sp_qs_rate` (quality start rate: ERA < 4.5 per outing), `sp_avg_ip`
- New differentials: `qs_rate_diff`, `k9_diff`, `bb9_diff`
- Injury features: `home/away_injury_count`, `home/away_injury_severity_score`, `injury_severity_diff`

**Combat (UFC)** — Added 3 advanced striking metrics + market signals:
- `total_strikes_per_min` (total strikes landed ÷ total fight time)
- `sig_strike_defense` (1 - sig_strikes_absorbed/sig_strikes_attempted)
- `volume_vs_accuracy` (sig_strikes_per_min × sig_strike_accuracy)
- New differentials: `total_strikes_per_min_diff`, `sig_strike_defense_diff`, `volume_vs_accuracy_diff`
- Market signals: `market_aggregate_abs_move`, `market_h2h_home/away_move`, `market_regime_stable/moving/volatile`

### New Prediction Markets Added

**MLB — First 5 Innings (F5)**:
- `f5_home_win_prob`: P(home leads after 5 innings)
- `f5_away_win_prob`: P(away leads after 5 innings)
- `f5_tie_prob`: P(tied after 5 innings)
- `f5_over4_5_prob`, `f5_under4_5_prob`: F5 totals over/under 4.5 runs
- Data coverage: 84.4% of MLB games have all 5 inning splits

**Soccer — Correct Score Bands**:
- `score_nil_nil_prob` (0-0), `score_1_0_prob` (1-0 home), `score_0_1_prob` (0-1 away)
- `score_1_1_prob` (1-1 draw), `score_2plus_0_prob` (home 2-0+), `score_0_2plus_prob` (away 0-2+)
- `score_2_1_prob`, `score_1_2_prob`, `score_3plus_total_prob`, `score_low_total_prob`
- Data coverage: 96.9% of soccer games have home/away_score

**Soccer — First Half O/U + Win Both Halves** ✅ **RESOLVED**:
- `h1_over0_5_prob`: P(1+ goals in first half)
- `h1_over1_5_prob`: P(2+ goals in first half)
- `win_both_halves_home_prob`: P(home wins H1 AND H2)
- `win_both_halves_away_prob`: P(away wins H1 AND H2)
- ✅ **Status**: Half scores exist as `home_h1_score`/`away_h1_score` (100% fill EPL/LaLiga/Bundesliga/Ligue1/SerieA; 98.8% MLS; 81%+ 2025 in-progress). Mapped to `home_h1`/`away_h1` in feature extraction. Will train on Phase 3.
- ⚠️ UCL, Europa, LigaMx: `home_h1_score` is NULL — half market models will be skipped for these leagues

**Soccer — Corners Total Markets** ✅ **NEW**:
- `corners_over9_5_prob`, `corners_over10_5_prob`, `corners_over11_5_prob`
- Data coverage: corners data 100% filled for EPL/LaLiga/Bundesliga/Ligue1/SerieA/MLS/NWSL, 0% for UCL/Europa/LigaMx
- Will train on Phase 3

**Soccer — Cards Total Markets** ✅ **NEW**:
- `cards_over3_5_prob`, `cards_over4_5_prob`, `cards_over5_5_prob`
- Yellow + red cards (red × 2) — 100% fill for all major leagues
- Will train on Phase 3

**Hockey/Basketball — Period BTTS** (both teams score in period N):
- `btts_period1_prob`: P(both score in Q1/P1)
- `btts_period2_prob`: P(both score in Q2/P2)
- `btts_period3_prob`: P(both score in Q3/P3)
- NHL period data ~96-99% non-null; NBA quarter data ~99% non-null

### Data Gaps Discovered This Session

**Soccer xG** — `home_xg`/`away_xg` columns exist in all soccer league schemas but are 100% null. xG proxy added as workaround. **[HIGH PRIORITY for data team]**

**✅ Soccer Halftime Scores RESOLVED** — Half scores are stored as `home_h1_score`/`away_h1_score`/`home_h2_score`/`away_h2_score` (not `home_h1`). Feature extractor correctly maps these. Coverage:
- EPL, LaLiga, Bundesliga, Ligue1, SerieA: 100% for 2024, ~80% for in-progress 2025
- MLS, NWSL: ~98% for 2025
- UCL, Europa, LigaMx: **0% — half scores still missing for these 3 leagues** → H1 market models skip these

**Soccer Possession %** — `home_possession_pct`/`away_possession_pct`: Actually `home_possession`/`away_possession` IS present at 100% fill rate in EPL/LaLiga/Bundesliga/Ligue1/SerieA. UCL, Europa, LigaMx also have possession data. ✅ Feature extractor already uses this.

**MLB Win/Loss/WHIP** — All 3 columns 100% null in player_stats parquets. ERA now computed from raw `earned_runs/innings`. **[CRITICAL — fix by backfilling from boxscore APIs]**

**UFC Control Time** — `home_control_time_avg`/`away_control_time_avg` at ~66% fill (some fights only). Missing control time weakens grappling advantage features.

**Tennis Player Stats** — Only 3.8% fill on match-level stats (2023–2024 data only). Severely limits serve/return feature quality.

**Tennis Surface Type** — `surface` column is NULL in ATP/WTA games parquet files. ✅ **PARTIALLY RESOLVED**: Added venue-to-surface mapping dictionary in tennis.py that infers surface from tournament city/venue name (30+ venues mapped: Grand Slams, Masters 1000, ATP 500). However, accuracy depends on venue name matching — **data team should add explicit `surface` column to ATP/WTA games parquet files** with values: `hard|clay|grass|carpet`.

**IndyCar** — No per-driver finishing positions in race-level data format. Cannot build competitive driver models. Needs per-driver row format.

---

## Critical Issues (Blocks Feature Computation)

### 0. **[NEW — CRITICAL]** ATP/WTA 2025/2026 — Serve Stats Completely Missing from Player Stats

**File(s)**: `data/normalized/atp/player_stats_2025.parquet`, `data/normalized/wta/player_stats_2025.parquet`  
**Problem**: 2025 (and 2026) ATP/WTA `player_stats_*.parquet` files have NO serve stat columns. Only basic match result columns present: `sets_won`, `sets_lost`, `games_won`, `games_lost`, `won`, `total_sets`.  
**Missing columns**: `aces`, `double_faults`, `first_serve_pct`, `first_serve_won_pct`, `second_serve_won_pct`, `service_games`, `break_points_won`, `break_points_faced`, `break_point_save_pct`, `return_points_won_pct`, `ace_df_ratio`  
**Impact**: Serve stats (first serve %, aces/match, break conversion) return 0 for ALL 2025/2026 games (~71% of ATP dataset = 10,213/14,457 games). The UUID bridge fix (April 2026) partially helps by resolving 57% of 2025 UUID-based player IDs to numeric IDs, but there's still no serve data to look up even after resolution.  
**Current fill rate**: `home_first_serve_pct > 0` = 26.3% of total ATP games (only 2023/2024 data)  
**Fix needed**: Add complete serve stats columns to `player_stats_2025.parquet` and `player_stats_2026.parquet` for both ATP and WTA. Source: ATP/WTA official API or data provider match statistics endpoints.  

Additionally: **2025/2026 games use UUID-based `home_team_id`** but player_stats use numeric ESPN IDs. Partial fix applied (name bridge covers 57%). **Full fix**: Add `numeric_player_id` column to 2025/2026 games parquets, or standardize to numeric IDs throughout.

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
| NHL   | `home_q1`/`away_q1` | Present but 61–71% null | Period prediction features |
| NHL   | `home_faceoff_pct` / `home_faceoffs_won` / `home_faceoffs_lost` | **MISSING from all seasons** | Faceoff pct defaults to 50.0 always; no real faceoff signal |
| NHL   | `home_shorthanded_goals` / `home_shootout_goals` | **MISSING** | SH goals, shootout goals default to 0 |
| NHL   | `home_shots` | `home_shots_on_goal` (present, 0% null) | Extractor correctly uses `shots_on_goal` — OK |
| NFL   | `home_passing_yards` etc. (box score stats) | **MISSING in 2023–2025** | Game stats features return 0 for 50% of training data |
| NBA   | `home_q1`/`home_q2`/`home_q3`/`home_q4` | Present in 2020 only; MISSING 2021–2026 | Quarter-by-quarter analysis limited to 1 of 7 seasons |
| NBA   | `home_defensive_rating` / `home_net_rating` | Present in 2020 only; MISSING 2021–2026 | Advanced rating features unavailable for recent seasons |
| NBA   | `home_field_goal_pct` | `home_fg_pct` (correct, 0% null) | OK — basketball.py already uses `fg_pct` naming |

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

## Sports With NO Normalized Data (Cannot Train — Need Data Agent)

| Sport | Status | What's Needed |
|-------|--------|--------------|
| **Bundesliga 2** (German 2nd division) | ❌ No data at all | Full normalized games + player_stats parquets for 2022-2026 |
| **Serie B** (Italian 2nd division) | ❌ No data at all | Full normalized games + player_stats parquets for 2022-2026 |
| **Ligue 2** (French 2nd division) | ❌ No data at all | Full normalized games + player_stats parquets for 2022-2026 |
| **World Cup** | ❌ No data at all | Tournament games 2022 + qualifiers; limited by 4-year schedule |
| **Euros** | ❌ Empty directory | European Championship 2020(held 2021) + 2024 games + qualifiers |

---

## Recommended Priority Order for Data Team

1. **[CRITICAL — NEW]** **ATP/WTA 2025/2026 — Missing Serve Stat Columns in player_stats**: Add `aces`, `double_faults`, `first_serve_pct`, `first_serve_won_pct`, `second_serve_won_pct`, `service_games`, `break_points_won`, `break_points_faced` to `player_stats_2025.parquet` and `player_stats_2026.parquet` for both ATP and WTA. Currently ~71% of all ATP/WTA games have zero serve features. Source: ATP/WTA Stats API, FlashScore, or Sofascore match statistics.
2. **[CRITICAL — NEW]** **ATP/WTA 2025/2026 — UUID Player IDs**: Standardize `home_team_id`/`away_team_id` to numeric ESPN player IDs across ALL seasons, OR add a `numeric_player_id` mapping column to 2025/2026 games parquets. Currently UUID IDs cause failed player_stats lookups for ~43% of 2025+ games even after the name-bridge fix.
3. **[CRITICAL]** **NFL 2023–2025 — Missing Box Score Stats**: Backfill `games_2023.parquet`, `games_2024.parquet`, `games_2025.parquet` with all team-level box score stats. 858 of 1698 NFL games (50.5%) are missing: `passing_yards`, `rushing_yards`, `turnovers`, `sacks`, `EPA`, `completion_pct`, `first_downs`, `third_down_pct`, `red_zone_pct`, `possession_seconds`, `tackles`, `fumbles_lost`, `penalties`, `scoring_efficiency`, `yards_per_play`. Games 2020-2022 have these; 2023-2025 do not.
4. **[CRITICAL]** **UCL, Europa, LigaMx**: Add `home_h1_score`, `away_h1_score`, `home_h2_score`, `away_h2_score` (halftime scores) — these 3 leagues have 0% half-score coverage, blocking H1 O/U and win_both_halves markets.
5. **[CRITICAL]** MLB: Fill `win`, `loss`, `whip` columns in `player_stats_{year}.parquet` for pitchers
6. **[CRITICAL]** **Eredivisie, Championship, PrimeiraLiga, Euros**: Add `player_stats_{year}.parquet` (2022–2025) — currently returning zero shot/discipline features. Required columns: `game_id`, `player_id`, `team_id`, `sport`, `season`, `date`, `shots`, `shots_on_target`, `saves`, `goals`, `assists`, `yellow_cards`, `red_cards`
7. **[HIGH]** Soccer (all leagues): Populate `home_xg`/`away_xg` (xG expected goals) — currently 100% null in all leagues
8. **[HIGH]** **Eredivisie, Championship, PrimeiraLiga, Euros**: Add `odds_{year}.parquet` and `market_signals_{year}.parquet` — currently no betting line features for these 4 leagues
9. **[HIGH]** UCL, Europa, LigaMx: Add `home_corners`, `away_corners`, `home_yellow_cards`, `away_yellow_cards` — corners and cards models can't train for these 3 leagues
10. **[HIGH]** NHL: Backfill period scores (`home_q1`, `home_q2`, `home_q3`) for all seasons
11. **[HIGH]** NWSL: Backfill all advanced match statistics
12. **[HIGH]** ATP/WTA: Add set-by-set scores; create WTA `odds_history.parquet`
13. **[HIGH]** Europa/LigaMx: Expand data coverage to 2020–2023 seasons (currently only 2024–2025, ~176/342 games)
14. **[HIGH]** LPGA: Add historical seasons back to 2020 (currently 2024–2026 only, ~68 tournaments); add `player_stats_{year}.parquet` with per-tournament scoring
15. **[HIGH]** IndyCar: Add per-driver finishing results per race (currently only race-level data with 35 total races — insufficient for ML); format: one row per driver per race with `position`, `laps_completed`, `dnf`, `points`
16. **[HIGH]** NBA: Ensure `off_rating`, `def_rating`, `net_rating` are populated in `player_stats_*.parquet` (often null — affects `_player_stats_features()`)
17. **[MEDIUM]** Add `market_signals_{year}.parquet` for: UCL, NWSL, NCAAF, NCAAB, NCAAW, ATP, WTA, Europa, LigaMx, LPGA, Eredivisie, Championship, PrimeiraLiga, Euros
18. **[MEDIUM]** UFC: Backfill per-fight performance metrics (currently high missingness in striking/grappling stats for older fights)
19. **[MEDIUM]** MLB: Backfill weather data for all stadium games
20. **[MEDIUM]** Soccer (all leagues): Add per-player `goals_pg`, `assists_pg`, `xG_pg` to `player_stats_{year}.parquet` for richer attacking depth features
21. **[LOW]** eSports (DOTA2/LoL/CSGO): Fix player-to-game ID reconciliation; backfill kills/KDA/gold stats
22. **[LOW]** NFL/NBA/NHL: Extend market signals back to 2020 for richer training history
23. **[LOW]** Golf/LPGA: Add course-specific historical data (course distance, par, avg winning score) for venue-adjusted predictions

---

## New Sports Status (Updated 2026-04-01)

| Sport | Data Available | Games | Seasons | Status |
|-------|---------------|-------|---------|--------|
| Europa League | Yes | ~176 | 2022–2025 | ✅ v3 retrain in progress |
| LigaMx | Yes | ~342 | 2023–2025 | ✅ v3 joint saved 22:19 + extra markets |
| Eredivisie | Yes | ~300 | 2023–2025 | ❌ No model yet — needs retrain (data exists) |
| Championship | Yes | ~550 | 2023–2025 | ❌ No model yet — needs retrain (data exists) |
| LPGA | Yes (34/yr) | ~102 total | 2024–2026 | ❌ Insufficient data for ML model |
| IndyCar | Yes (race-level) | ~54 total | 2024–2026 | ❌ Needs per-driver format |
| Bundesliga 2 | ❌ None | 0 | — | ❌ No normalized data |
| Serie B | ❌ None | 0 | — | ❌ No normalized data |
| Ligue 2 | ❌ None | 0 | — | ❌ No normalized data |
| World Cup | ❌ None | 0 | — | ❌ No normalized data |

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
