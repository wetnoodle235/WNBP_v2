# CFBData Storage Reorganization - Complete Guide

## Overview

This directory contains a complete redesign of raw cfbdata storage, moving from a flat structure to a hierarchical **season → week → date → game** organization. This improves maintainability, reduces individual file sizes (30MB → multiple 2-5MB files), and simplifies incremental updates.

---

## Documents in This Review

### 1. **CFBDATA_STORAGE_REVIEW.md** (START HERE)
- **Purpose**: High-level analysis and benefits summary
- **Contains**:
  - Current state assessment
  - Issues identified (30MB players file, no temporal org, etc.)
  - Proposed improved structure (Tier 1 & Tier 2)
  - Risk assessment and timeline
- **Read time**: 10 minutes
- **Audience**: Stakeholders, project managers, QA

### 2. **CFBDATA_STORAGE_TECHNICAL_SPEC.md** (DETAILED REFERENCE)
- **Purpose**: Complete technical specification of new structure and implementation
- **Contains**:
  - New directory layout with examples
  - File organization by endpoint
  - Week/date extraction details
  - Specific Python changes for normalizer
  - Migration script outline
  - Data size estimates
  - Validation checklist
- **Read time**: 20 minutes
- **Audience**: Developers, architects

### 3. **CFBDATA_IMPLEMENTATION_CODE.md** (CODE EXAMPLES)
- **Purpose**: Ready-to-use code snippets for implementation
- **Contains**:
  - Quick reference of path changes
  - Updated `io.ts` code
  - Updated cfbdata provider functions
  - Updated normalizer functions
  - Complete migration script
  - Test commands
- **Read time**: 30 minutes (skim for reference)
- **Audience**: Implementing developers

### 4. **CFBDATA_IMPLEMENTATION_CHECKLIST.md** (EXECUTION PLAN)
- **Purpose**: Step-by-step implementation guide with tasks and validation
- **Contains**:
  - Pre-implementation checklist
  - Phase 1-5 detailed steps
  - Commands to run at each phase
  - Testing procedures
  - Rollback plan if needed
  - Success criteria
  - Time estimates
- **Read time**: Reference document
- **Audience**: Implementing developers, QA

---

## How to Use This Review

### For Stakeholders / Decision Makers
1. Read **CFBDATA_STORAGE_REVIEW.md** (10 min)
2. Review summary table: Benefits of Proposed Structure
3. Review risks and timeline
4. **Decision**: Approve or request changes

### For Developers Implementing
1. Read **CFBDATA_STORAGE_REVIEW.md** (understand why)
2. Read **CFBDATA_STORAGE_TECHNICAL_SPEC.md** (understand what)
3. Use **CFBDATA_IMPLEMENTATION_CODE.md** as reference while coding
4. Follow **CFBDATA_IMPLEMENTATION_CHECKLIST.md** step-by-step
5. Test thoroughly at each phase

### For Code Reviewers
1. Check against **CFBDATA_STORAGE_TECHNICAL_SPEC.md** (spec compliance)
2. Verify paths match new structure (use table from Implementation Code)
3. Ensure backwards compatibility (old paths still work)
4. Test migration script on test data first

### For QA / Testing
1. Review **Phase 5** of checklist (Testing & Validation)
2. Run commands in order
3. Verify no data loss (count records before/after)
4. Check both old and new structures work with normalizer
5. Compare normalized outputs

---

## Quick Summary

### Current Problems

| Problem | Impact | Size |
|---------|--------|------|
| 30MB player stats file | Hard to load/parse, memory heavy | 30MB in single file |
| 3.6MB games file | Inefficient to access single game | All games in one file |
| Flat {season}/ structure | No temporal organization | 50+ files in root |
| Week-only play organization | Plays split by week, not by game | 8-16MB per week file |

### Proposed Solution

```
OLD: data/raw/cfbdata/ncaaf/2025/stats_player_season.json (30MB)
NEW: data/raw/cfbdata/ncaaf/2025/{week}/stats/players_{week}.json (16 files × 2-5MB)

OLD: data/raw/cfbdata/ncaaf/2025/games.json (3.6MB) + plays/week_N.json (8-16MB)
NEW: data/raw/cfbdata/ncaaf/2025/{week}/games/{date}/{gameId}.json
   + data/raw/cfbdata/ncaaf/2025/{week}/plays/{date}/{gameId}_plays.json
```

### Key Changes

| Component | Change |
|-----------|--------|
| **Importer** | 10+ functions updated to save in new structure |
| **Normalizer** | 6+ functions updated to read from new structure + backwards compatibility |
| **Path Helper** | New `rawPathWithWeekDate()` added to `io.ts` |
| **Migration** | One-time script to move existing data |

### Timeline
- **Phase 1** (Framework): 1-2 hours
- **Phase 2** (Importer): 2-3 hours
- **Phase 3** (Normalizer): 2-3 hours
- **Phase 4** (Migration): 1 hour
- **Phase 5** (Testing): 1-2 hours
- **Total**: ~8-12 hours

---

## New Directory Structure

```
data/raw/cfbdata/ncaaf/{season}/
├── {week}/                           # Weekly data (weeks 1-16 + postseason)
│   ├── metadata/
│   │   ├── week_info.json
│   │   ├── teams.json
│   │   └── conferences.json
│   ├── games/
│   │   └── {YYYY-MM-DD}/            # Date subdirectory
│   │       ├── {gameId}.json        # One game per file
│   │       ├── {gameId}_box_advanced.json
│   │       └── {gameId}_lines.json
│   ├── plays/
│   │   └── {YYYY-MM-DD}/
│   │       └── {gameId}_plays.json  # Plays for one game
│   └── stats/
│       ├── players_{week}.json      # Split from 30MB: 2-5MB per week
│       ├── teams_season_{week}.json
│       └── game_advanced_{week}.json
│
└── reference/                        # Season-wide data (week=0)
    ├── recruiting.json
    ├── rankings.json
    ├── talent.json
    ├── teams_fbs.json
    ├── conferences.json
    ├── venues.json
    ├── stats_categories.json
    ├── plays_types.json
    └── info.json
```

---

## File Size Before/After

| File | Old Size | New Size | Per-File Reduction |
|------|----------|----------|-------------------|
| stats_player_season.json | 30MB | 16 files × 2-5MB | 87.5% |
| games.json | 3.6MB | ~8-10 files × 300-600KB | 90% |
| plays/week_N.json (×16) | 8-16MB each | 70+ files × 200-300KB | 96% |
| **Total raw data** | ~150MB | ~170MB (same) | ✓ No overall increase |

The reorganization **does not add storage**, but makes individual files much more manageable.

---

## Implementation Overview

### Step 1: Framework (Phase 1)
```typescript
// Add to importers/src/core/io.ts
export function rawPathWithWeekDate(
  dataDir: string,
  provider: string,
  sport: string,
  season: number,
  week: number | null,
  date: string,
  endpoint: string,
  ...parts: string[]
): string {
  // Returns: {season}/{week}/{endpoint}/{date}/...
}
```

### Step 2: Update Importer (Phase 2)
```typescript
// In importers/src/providers/cfbdata/index.ts
async function importGames(ctx: EndpointContext) {
  for (const game of allGames) {
    const { week, date } = getWeekAndDate(game);
    const outFile = rawPathWithWeekDate(
      ctx.dataDir, NAME, ctx.sport, ctx.season,
      week, date, "games", `${gameId}.json`
    );
    writeJSON(outFile, game);
  }
}
```

### Step 3: Update Normalizer (Phase 3)
```python
# In backend/normalization/normalizer.py
def _load_json_cfbdata(base: Path, endpoint: str) -> list:
    # Try new structure first: {week}/{endpoint}/*.json
    # Fall back to old structure: {endpoint}.json
    return all_data
```

### Step 4: Migrate Data (Phase 4)
```bash
# Copy script from CFBDATA_IMPLEMENTATION_CODE.md
python3 backend/scripts/migrate_cfbdata_structure.py
```

### Step 5: Test & Validate (Phase 5)
```bash
# Run full pipeline
python3 backend/scripts/daily_pipeline.py --sport ncaaf --seasons 2025

# Verify output
wc -l data/normalized/ncaaf/2025/games.csv
```

---

## Key Features of This Design

✅ **Temporal Organization**: Data naturally follows season → week → date → game  
✅ **Smaller Files**: 30MB → 2-5MB per file (easier to parse and debug)  
✅ **Incremental Updates**: Add new weeks/games without rewriting entire files  
✅ **Better Discovery**: Easy to find games by date or week  
✅ **Scalable**: Can add by-player or by-team indices later  
✅ **Backwards Compatible**: Normalizer supports both old and new structures  
✅ **No Data Loss**: All records preserved (1:1 count before/after)  

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Path construction errors | Test path generation at each step |
| Data loss during migration | Compare record counts before/after |
| Normalizer bugs | Run on both old + new structures |
| Incomplete migration | Validate all files present |
| Breaking imports | Test with dry-run first |
| Backwards compatibility | Keep fallback to old paths |

---

## File List

All documents in `/home/derek/Documents/stock/v5.0/`:

1. **CFBDATA_STORAGE_REVIEW.md** — Executive summary and analysis
2. **CFBDATA_STORAGE_TECHNICAL_SPEC.md** — Complete technical specification
3. **CFBDATA_IMPLEMENTATION_CODE.md** — Ready-to-use code examples
4. **CFBDATA_IMPLEMENTATION_CHECKLIST.md** — Step-by-step execution guide
5. **CFBDATA_REORGANIZATION_GUIDE.md** — This file (overview)

---

## Getting Started

### For the First Time
1. Read `CFBDATA_STORAGE_REVIEW.md` (10 min)
2. Read `CFBDATA_STORAGE_TECHNICAL_SPEC.md` (20 min)
3. Skim `CFBDATA_IMPLEMENTATION_CODE.md` (5 min)
4. Decide: proceed or request changes?

### To Implement
1. Follow `CFBDATA_IMPLEMENTATION_CHECKLIST.md` step-by-step
2. Reference `CFBDATA_IMPLEMENTATION_CODE.md` for code examples
3. Check against `CFBDATA_STORAGE_TECHNICAL_SPEC.md` for spec compliance
4. Test at each phase before proceeding to next

### To Review Code
1. Cross-reference against `CFBDATA_STORAGE_TECHNICAL_SPEC.md`
2. Verify path structures match new hierarchy
3. Check backwards compatibility tests
4. Run Phase 5 validation procedures

---

## Questions?

Refer to:
- **"Why are we doing this?"** → CFBDATA_STORAGE_REVIEW.md
- **"What should the structure look like?"** → CFBDATA_STORAGE_TECHNICAL_SPEC.md
- **"How do I implement X?"** → CFBDATA_IMPLEMENTATION_CODE.md
- **"What's the next step?"** → CFBDATA_IMPLEMENTATION_CHECKLIST.md

---

## Approval Checklist

Before starting implementation, ensure:

- [ ] **Stakeholder approval**: Reviewed CFBDATA_STORAGE_REVIEW.md
- [ ] **Resource commitment**: 8-12 hours of development time allocated
- [ ] **Backup created**: `cp -r v5.0/data/raw/cfbdata v5.0/data/raw/cfbdata.backup`
- [ ] **Branch created**: `git checkout -b feat/cfbdata-reorganization`
- [ ] **Team notified**: Inform team of upcoming changes to importer/normalizer
- [ ] **Test environment ready**: Access to dev/test cfbdata for testing
- [ ] **Timeline agreed**: Phase 1-5 timeline works with sprint

---

**Status**: Ready for implementation  
**Last Updated**: 2026-04-01  
**Version**: 1.0

