# CFBData Storage Reorganization - Implementation Checklist

## Overview

This document provides a step-by-step implementation plan for reorganizing raw cfbdata from flat (`{season}/`) to hierarchical (`{season}/{week}/{date}/`) structure.

---

## Pre-Implementation Review

- [ ] Read `CFBDATA_STORAGE_REVIEW.md` (issues & benefits)
- [ ] Review `CFBDATA_STORAGE_TECHNICAL_SPEC.md` (detailed specifications)
- [ ] Review `CFBDATA_IMPLEMENTATION_CODE.md` (code examples)
- [ ] Confirm all stakeholders understand the scope
- [ ] Backup existing raw cfbdata:
  ```bash
  cp -r v5.0/data/raw/cfbdata v5.0/data/raw/cfbdata.backup
  ```

---

## Phase 1: Framework Setup (1-2 hours)

### 1.1 Update Path Helper (importers/src/core/io.ts)

- [ ] Add `rawPathWithWeekDate()` function
- [ ] Keep `rawPath()` for backwards compatibility
- [ ] Test path generation:
  ```bash
  cd v5.0/importers
  npx ts-node -e "
    import { rawPathWithWeekDate } from './src/core/io.js';
    const p = rawPathWithWeekDate('data', 'cfbdata', 'ncaaf', 2025, 1, '2025-09-06', 'games', '401547489.json');
    console.log(p);
    // Expected: data/cfbdata/ncaaf/2025/1/games/2025-09-06/401547489.json
  "
  ```

### 1.2 Create Helper Functions

- [ ] Add `getWeekAndDate()` to extract from API response
- [ ] Add `ensureDir()` to create directories
- [ ] Add imports for fs/promises

### 1.3 Create Session Memory Note

```bash
# Save plan to session memory for progress tracking
cat > /memories/session/cfbdata_migration_tracker.md << 'EOF'
# CFBData Storage Migration Tracker

## Status: IN PROGRESS

### Completed:
- [ ] Phase 1: Framework setup

### Current:
- [ ] Phase 2: Importer updates

### Pending:
- [ ] Phase 3: Normalizer updates
- [ ] Phase 4: Migrate existing data
- [ ] Phase 5: Testing & validation

## Files Modified:
- [ ] importers/src/core/io.ts
- [ ] importers/src/providers/cfbdata/index.ts (10+ functions)
- [ ] backend/normalization/normalizer.py (6+ functions)
- [ ] backend/scripts/migrate_cfbdata_structure.py (NEW)

## Timeline:
- Phase 1: DONE
- Phase 2: IN PROGRESS
- Phase 3: PENDING
- Phase 4: PENDING
- Phase 5: PENDING

## Risks:
- Path construction errors → validate with test
- Data loss during migration → compare checksums
- Normalizer bugs → test with old + new structure
EOF
```

---

## Phase 2: Update Importer (2-3 hours)

### 2.1 Update Game Import Function

**File:** `importers/src/providers/cfbdata/index.ts`

- [ ] Update `importGames()` to save per-game files
  - [ ] Extract week and date from each game
  - [ ] Create path with `rawPathWithWeekDate()`
  - [ ] Save to `{week}/games/{date}/{gameId}.json`
  - [ ] Log progress every 50 files
  - [ ] Handle missing dates gracefully

**Test:**
```bash
cd v5.0/importers
npm run import:cfbdata -- --sports=ncaaf --seasons=2025 --endpoints=games --dry-run
# Verify output mentions new paths
```

### 2.2 Update Play-by-Play Import Function

**File:** `importers/src/providers/cfbdata/index.ts`

- [ ] Update `importPlays()` to split by game
  - [ ] Load game files to build gameId → date mapping
  - [ ] Fetch plays by week
  - [ ] Group by game_id
  - [ ] Save to `{week}/plays/{date}/{gameId}_plays.json`
  - [ ] Handle weeks with no plays

**Test:**
```bash
npm run import:cfbdata -- --sports=ncaaf --seasons=2025 --endpoints=plays --dry-run
```

### 2.3 Update Player Stats Import

**File:** `importers/src/providers/cfbdata/index.ts`

- [ ] Update `importStatsPlayerSeason()` to fetch by week
  - [ ] Loop through weeks 1-16 (+ postseason)
  - [ ] Fetch `/stats/player/season` with week parameter
  - [ ] Save to `{week}/stats/players_week_{N}.json`
  - [ ] Result: ~16 files of 2-5MB instead of 1 × 30MB file

**Test:**
```bash
npm run import:cfbdata -- --sports=ncaaf --seasons=2025 --endpoints=stats_player_season --dry-run
# Check if 16 week files are created instead of 1 big file
```

### 2.4 Update Game Box Advanced

**File:** `importers/src/providers/cfbdata/index.ts`

- [ ] Update `importGameBoxAdvanced()` to use new paths
  - [ ] Load game files to get gameId list
  - [ ] For each game, save to `{week}/games/{date}/{gameId}_box_advanced.json`

**Test:**
```bash
npm run import:cfbdata -- --sports=ncaaf --seasons=2025 --endpoints=game_box_advanced --dry-run
```

### 2.5 Update Reference File Functions

**File:** `importers/src/providers/cfbdata/index.ts`

Update ~8 functions (recruiting, rankings, talent, etc.):

```
importRecruiting()        → {season}/reference/recruiting.json
importRankings()          → {season}/reference/rankings.json
importTalent()            → {season}/reference/talent.json
importTeamsFbs()          → {season}/reference/teams_fbs.json
importStatsCategories()   → {season}/reference/stats_categories.json
importPlaysTypes()        → {season}/reference/plays_types.json
importInfo()              → {season}/reference/info.json
importConferences()       → {season}/reference/conferences.json
```

- [ ] Change `rawPath()` calls to `rawPathWithWeekDate()` with `week=null`
- [ ] Keep identical logic, just new paths

**Test:**
```bash
npm run import:cfbdata -- --sports=ncaaf --seasons=2025 --endpoints=recruiting,rankings,talent --dry-run
```

### 2.6 Compile & Verify Importer

- [ ] Run TypeScript compiler with no errors:
  ```bash
  cd v5.0/importers
  npm run build
  ```
- [ ] Verify all functions updated
- [ ] Run full dry-run on a test season:
  ```bash
  npm run import:cfbdata -- --sports=ncaaf --seasons=2025 --dry-run 2>&1 | tee /tmp/import_dryrun.log
  grep -E "2025/[0-9]+/games" /tmp/import_dryrun.log | head -5
  # Should see new paths like:
  # 2025/1/games/2025-09-06/401547489.json
  ```

---

## Phase 3: Update Normalizer (2-3 hours)

### 3.1 Add Compatibility Layer

**File:** `backend/normalization/normalizer.py`

- [ ] Add `_load_json_cfbdata()` function that:
  - [ ] Looks in `{week}/{endpoint}/` for new structure
  - [ ] Falls back to `{endpoint}.json` for old structure
  - [ ] Handles both directories and individual files
  - [ ] Aggregates results from multiple files

**Test:**
```bash
cd v5.0/backend
python3 -c "
from pathlib import Path
from normalization.normalizer import _load_json_cfbdata
# Test with old structure (should still work)
data = _load_json_cfbdata(Path('data/raw/cfbdata/ncaaf/2023'), 'games')
print(f'Loaded {len(data)} games from 2023')
"
```

### 3.2 Update _cfbdata_games()

**File:** `backend/normalization/normalizer.py`

- [ ] Replace hard-coded path with `_load_json_cfbdata()`
- [ ] Test still produces same number of game records
- [ ] Verify no errors

**Test:**
```bash
python3 << 'EOF'
from pathlib import Path
from normalization.normalizer import _cfbdata_games

# Test with small season
games = _cfbdata_games(Path('data/raw/cfbdata/ncaaf/2025'), 'ncaaf', '2025')
print(f"Games: {len(games)}")
print(f"Sample: {games[0] if games else 'None'}")
EOF
```

### 3.3 Update Other _cfbdata_*() Functions

Update all functions that read raw cfbdata:

- [ ] `_cfbdata_standings()` → use `_load_json_cfbdata()`
- [ ] `_cfbdata_team_stats()` → use `_load_json_cfbdata()`
- [ ] `_cfbdata_teams()` → use `_load_json_cfbdata()`
- [ ] `_cfbdata_roster()` → use `_load_json_cfbdata()`
- [ ] `_cfbdata_player_stats()` (new) → aggregate from split week files
- [ ] Other functions as needed...

### 3.4 Test Normalizer on Old + New Data

- [ ] Run normalizer on existing 2023 data (old structure):
  ```bash
  cd v5.0/backend
  python3 -c "
  from pathlib import Path
  from normalization.normalizer import get_provider_data
  
  games = get_provider_data(Path('data/raw/cfbdata'), 'ncaaf', 2023, 'games', 'cfbdata')
  print(f'Loaded {len(games)} games from 2023 (old structure)')
  "
  ```

- [ ] Run normalizer on 2025 data (will use old structure until migration):
  ```bash
  python3 -c "
  from pathlib import Path
  from normalization.normalizer import get_provider_data
  
  games = get_provider_data(Path('data/raw/cfbdata'), 'ncaaf', 2025, 'games', 'cfbdata')
  print(f'Loaded {len(games)} games from 2025')
  "
  ```

---

## Phase 4: Migrate Existing Data (1 hour)

### 4.1 Create Migration Script

**File:** `backend/scripts/migrate_cfbdata_structure.py`

- [ ] Create script (see `CFBDATA_IMPLEMENTATION_CODE.md`)
- [ ] Test on single season first (e.g., 2025):
  ```bash
  cd v5.0/backend
  python3 scripts/migrate_cfbdata_structure.py 2025
  ```

### 4.2 Verify Migration

- [ ] Structure looks correct:
  ```bash
  ls -la data/raw/cfbdata/ncaaf/2025/1/games/*/
  # Should show: 401547489.json, 401547490.json, ...
  ```

- [ ] No data loss (count records):
  ```bash
  # Old structure: wc -l games.json
  # New structure: cat 1/games/*/*.json | jq . -c | wc -l
  ```

- [ ] Spot-check a game file:
  ```bash
  cat data/raw/cfbdata/ncaaf/2025/1/games/2025-09-06/401547489.json | jq .
  ```

- [ ] Check reference files exist:
  ```bash
  ls data/raw/cfbdata/ncaaf/2025/reference/
  # Should show: recruiting.json, rankings.json, etc.
  ```

### 4.3 Migrate All Seasons

- [ ] Migrate 2020-2025:
  ```bash
  python3 backend/scripts/migrate_cfbdata_structure.py
  ```

- [ ] Verify each season:
  ```bash
  for season in 2020 2021 2022 2023 2024 2025; do
    echo "=== $season ==="
    find data/raw/cfbdata/ncaaf/$season -type f -name "*.json" | wc -l
  done
  ```

### 4.4 Backup Old Structure (if keeping)

- [ ] Move old structure aside (or delete if confident):
  ```bash
  # Option 1: Delete old files (keep backup in .backup dir)
  rm -rf v5.0/data/raw/cfbdata/ncaaf/2025/games.json v5.0/data/raw/cfbdata/ncaaf/2025/plays/ ...
  
  # Option 2: Keep old and compare
  cp -r v5.0/data/raw/cfbdata v5.0/data/raw/cfbdata.old_structure
  ```

---

## Phase 5: Testing & Validation (1-2 hours)

### 5.1 Run Full Normalization

- [ ] Create normalized dataset from new structure:
  ```bash
  cd v5.0/backend
  python3 scripts/daily_pipeline.py --sport ncaaf --seasons 2025 --verbose
  ```

- [ ] Verify output looks correct:
  ```bash
  ls -lh data/normalized/ncaaf/2025/
  # games.csv, players.csv, etc.
  ```

- [ ] Check normalized data is non-empty:
  ```bash
  wc -l data/normalized/ncaaf/2025/games.csv
  # Should be > 100 rows
  ```

### 5.2 Compare Old vs New

- [ ] If you kept old structure, compare:
  ```bash
  # Old normalized output (from backup)
  python3 scripts/normalize.py --raw-dir data/raw/cfbdata.backup --output-dir /tmp/old_normalized
  
  # New normalized output
  python3 scripts/normalize.py --raw-dir data/raw/cfbdata --output-dir /tmp/new_normalized
  
  # Compare
  diff /tmp/old_normalized/ncaaf/2025/games.csv /tmp/new_normalized/ncaaf/2025/games.csv
  ```

### 5.3 Test Live Importer

- [ ] Test importing NEW data with new structure:
  ```bash
  cd v5.0/importers
  npm run import:cfbdata -- --sports ncaaf --seasons 2026 --dry-run
  # Should show new paths in logs
  ```

- [ ] Test importing live/recent data:
  ```bash
  npm run import:cfbdata -- --sports ncaaf --seasons 2025 --endpoints games --dry-run
  # Should skip existing, save new games to proper structure
  ```

### 5.4 Documentation

- [ ] Update README with new structure:
  ```bash
  # In v5.0/data/README.md or similar:
  cat >> README.md << 'EOF'
  
  ## CFBData Raw Structure (New Organization)
  
  As of [DATE], raw cfbdata is organized by season → week → date → game:
  
  ```
  raw/cfbdata/ncaaf/{season}/{week}/
    ├── games/{date}/{gameId}.json
    ├── plays/{date}/{gameId}_plays.json
    ├── stats/
    └── metadata/
  
  raw/cfbdata/ncaaf/{season}/reference/
    ├── recruiting.json
    └── ...
  ```
  EOF
  ```

- [ ] Update normalizer docstrings
- [ ] Update importer comments

### 5.5 Sign-Off

- [ ] All tests pass
- [ ] No data loss
- [ ] Old structure handled gracefully (backwards compatibility)
- [ ] New imports use correct paths
- [ ] Normalizer reads both old and new structures
- [ ] Documentation updated
- [ ] Delete `.backup` directory (or keep for long-term safety)

---

## Rollback Plan (If Issues Arise)

If something breaks after migration:

1. **Restore from backup:**
   ```bash
   rm -rf v5.0/data/raw/cfbdata
   cp -r v5.0/data/raw/cfbdata.backup v5.0/data/raw/cfbdata
   ```

2. **Revert code changes:**
   ```bash
   git checkout HEAD -- importers/src/providers/cfbdata/index.ts backend/normalization/normalizer.py
   ```

3. **Diagnose issue** (likely path bug or missing file)

4. **Re-implement carefully** with fixes

---

## Success Criteria

✅ All of the following must be true:

- [ ] New directory structure created correctly for all seasons
- [ ] All game files present and valid JSON
- [ ] All play-by-play files split correctly by game
- [ ] Player stats files split by week (16 files instead of 1)
- [ ] Reference files in `/reference/` directory
- [ ] Normalizer reads from both old and new structures
- [ ] Normalized output matches old output exactly
- [ ] New imports save to new structure with no errors
- [ ] No data loss (record counts match before/after)
- [ ] File sizes reasonable (no 30MB files)
- [ ] Documentation updated

---

## Time Estimates

| Phase | Task | Duration | Status |
|-------|------|----------|--------|
| 1 | Framework setup | 1-2h | ⏳ |
| 2 | Importer updates | 2-3h | ⏳ |
| 3 | Normalizer updates | 2-3h | ⏳ |
| 4 | Data migration | 1h + validation | ⏳ |
| 5 | Testing & sign-off | 1-2h | ⏳ |
| **TOTAL** | **Complete reorganization** | **~8-12 hours** | ⏳ |

---

## Quick Commands Reference

```bash
# Backup
cp -r v5.0/data/raw/cfbdata v5.0/data/raw/cfbdata.backup

# Test importer
cd v5.0/importers && npm run import:cfbdata -- --sports=ncaaf --seasons=2025 --dry-run

# Test normalizer
cd v5.0/backend && python3 -c "from normalization.normalizer import _cfbdata_games; from pathlib import Path; print(len(_cfbdata_games(Path('data/raw/cfbdata/ncaaf/2025'), 'ncaaf', '2025')))"

# Migrate
python3 v5.0/backend/scripts/migrate_cfbdata_structure.py

# Verify
find v5.0/data/raw/cfbdata/ncaaf/2025 -type f -name "*.json" | wc -l

# Compare checksums (optional)
find v5.0/data/raw/cfbdata.backup -type f -name "*.json" -exec md5sum {} \; | sort > /tmp/old.md5
find v5.0/data/raw/cfbdata -type f -name "*.json" -exec md5sum {} \; | sort > /tmp/new.md5
# Note: checksums will differ due to pretty-printing, so compare record counts instead
```

---

## Next Steps

1. Start Phase 1 (Framework Setup)
2. Review generated paths carefully
3. Proceed to Phase 2 (Importer Updates)
4. Test extensively before data migration
5. Execute migration on test season first (2025)
6. Full validation before deploying to production
