# CFBData Storage Reorganization - Technical Specification

## Overview

Reorganize raw cfbdata from flat season structure to nested **season → week → date → game** hierarchy to improve manageability, reduce file sizes, and simplify incremental updates.

**Old Structure:**
```
data/raw/cfbdata/ncaaf/2025/
  ├── games.json (3.6MB)
  ├── plays/week_1.json, week_2.json, ...
  ├── stats_player_season.json (30MB)
  └── ...
```

**New Structure:**
```
data/raw/cfbdata/ncaaf/2025/{week}/
  ├── metadata/
  │   ├── week_info.json
  │   ├── teams.json
  │   └── conferences.json
  ├── games/{date}/
  │   ├── {gameId}.json
  │   ├── {gameId}_box_advanced.json
  │   └── {gameId}_lines.json
  ├── plays/{date}/
  │   └── {gameId}_plays.json
  ├── stats/
  │   └── players_{week}.json
  └── ...

data/raw/cfbdata/ncaaf/2025/reference/  (season-wide, week=0)
  ├── recruiting.json
  ├── rankings.json
  ├── talent.json
  └── ...
```

---

## File Organization by Endpoint

### Season-Wide Reference Data
**Location:** `{season}/reference/`
**Files:**
- `recruiting.json` - All recruits for season
- `recruiting_teams.json` - Recruiting by team
- `recruiting_groups.json` - Recruiting groups
- `talent.json` - Talent rankings
- `teams_fbs.json` - Static FBS team list
- `stats_categories.json` - Available stat categories
- `plays_types.json` - Play type enum
- `info.json` - API info

### Weekly Metadata
**Location:** `{season}/{week}/metadata/`
**Files:**
- `week_info.json` - DT week number, start/end dates, game count
- `teams.json` - Copy of teams endpoint (for quick access)
- `conferences.json` - Static conference data
- `venues.json` - Venue data

### Game Data
**Location:** `{season}/{week}/games/{date}/`

Each game gets its own JSON file. Extract date from `game.startDate` field (YYYY-MM-DD format).

**Files per game:**
- `{gameId}.json` - Main game record
  ```json
  {
    "id": 401547489,
    "season": 2025,
    "startDate": "2025-09-06T16:00Z",
    "homeTeam": "Alabama",
    "awayTeam": "Western Carolina",
    "homeId": 25,
    "awayId": 2500,
    "homePoints": 63,
    "awayPoints": 21
  }
  ```
  
- `{gameId}_box_advanced.json` - Advanced stats per team
- `{gameId}_weather.json` - Weather at kickoff (if available)
- `{gameId}_lines.json` - Betting lines (if available)

### Play-by-Play Data
**Location:** `{season}/{week}/plays/{date}/{gameId}_plays.json`

Current API endpoint `/plays` returns plays for all games in a week. We split by game:
- Extract `game_id` from each play record
- Group plays by `game_id`
- Save to `{gameId}_plays.json`

### Team & Player Statistics

**Team Stats (by week):**
```
{season}/{week}/stats/
  ├── teams_season_{week}.json     (aggregated team performance for week)
  ├── teams_ats_{week}.json        (ATS record for week)
  ├── teams_advanced_{week}.json   (advanced team metrics)
  └── game_advanced_{week}.json    (per-game advanced stats)
```

**Player Stats:**
```
{season}/{week}/stats/
  └── players_{week}.json          (all player performance for week)
```

Alternative (if 30MB → too large even split by week):
```
{season}/stats/by_player/
  ├── {playerId}.json              (career stats for a single player)
  └── ...
```

**Recommendation:** First try week-level split. Only split by player if file remains > 20MB.

### Rankings & Standings
```
{season}/reference/
  ├── rankings.json                (all polls, all weeks)
  └── records.json                 (conference records)
```

---

## Implementation Details

### 1. Path Generation

#### Current (io.ts):
```typescript
export function rawPath(dataDir: string, provider: string, sport: string, season: number, ...parts: string[]): string {
  return path.join(dataDir, provider, sport, String(season), ...parts);
}

// Usage:
rawPath(dataDir, "cfbdata", "ncaaf", 2025, "games.json")
// → data/raw/cfbdata/ncaaf/2025/games.json
```

#### Updated (io.ts):
Add new helper or enhance existing:
```typescript
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
  if (week === null) {
    // Season-wide reference data
    return path.join(dataDir, provider, sport, String(season), "reference", endpoint, ...parts);
  } else {
    // Weekly data
    return path.join(dataDir, provider, sport, String(season), String(week), endpoint, date, ...parts);
  }
}

// Usage examples:
rawPathWithWeekDate(dataDir, "cfbdata", "ncaaf", 2025, 1, "2025-09-06", "games", "401547489.json")
// → data/raw/cfbdata/ncaaf/2025/1/games/2025-09-06/401547489.json

rawPathWithWeekDate(dataDir, "cfbdata", "ncaaf", 2025, null, "", "recruiting.json")
// → data/raw/cfbdata/ncaaf/2025/reference/recruiting.json
```

### 2. Week & Date Extraction from API

**CFBData API Structure:**
```json
{
  "id": 401547489,
  "season": 2025,
  "week": 1,
  "seasonType": "regular",
  "startDate": "2025-09-06T16:00:00.000Z",
  "homeTeam": "Alabama",
  "awayTeam": "Western Carolina"
}
```

**Extraction:**
```typescript
const game = await cfbFetch("/games", { year: 2025, seasonType: "regular" });
// game[0].week → 1
// game[0].startDate.split('T')[0] → "2025-09-06"
```

**Handling Missing Week Data:**
- Most endpoints include `week` field in response
- For `/plays`, call with explicit `week` parameter
- For `/games` at season level, fetch all games first, extract weeks

### 3. Per-Game File Generation (from /games)

**Algorithm:**
1. Fetch `/games` for season
2. For each game:
   - Extract `id`, `startDate` (to get date YYYY-MM-DD), `week`
   - Save game record to `{week}/games/{date}/{gameId}.json`
3. Return list of downloaded games

```typescript
async function importGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const [regular, postseason] = await Promise.all([
    cfbFetch<GameRecord[]>("/games", { year: season, seasonType: "regular" }),
    cfbFetch<GameRecord[]>("/games", { year: season, seasonType: "postseason" }),
  ]);
  const allGames = [...regular, ...postseason];

  for (const game of allGames) {
    const week = game.week ?? 0; // 0 if postseason
    const date = game.startDate.split('T')[0]; // YYYY-MM-DD
    const gameId = String(game.id ?? "");
    if (!gameId) continue;

    const outFile = rawPathWithWeekDate(
      dataDir, NAME, sport, season,
      week, date,
      "games",
      `${gameId}.json`
    );
    
    if (fileExists(outFile)) continue;
    
    ensureDirSync(path.dirname(outFile)); // Create directories
    writeJSON(outFile, game);
    filesWritten++;
  }

  logger.progress(NAME, sport, "games", `Saved ${filesWritten} game files`);
  return { filesWritten, errors };
}
```

### 4. Play-by-Play Splitting

**Current:** Saves all plays for week X to `plays/week_X.json`
**New:** Splits by game within that week

```typescript
async function importPlays(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "plays", `Fetching ${season} play-by-play`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  // First, load all games to map gameId → date
  const gamesDir = path.join(dataDir, NAME, sport, String(season));
  const gameIdToDate: Map<string, string> = new Map();
  
  // Read all game files to build mapping
  for (const weekDir of fs.readdirSync(gamesDir)) {
    const weekPath = path.join(gamesDir, weekDir);
    const gamesPath = path.join(weekPath, "games");
    if (!fs.existsSync(gamesPath)) continue;
    
    for (const dateDir of fs.readdirSync(gamesPath)) {
      const datePath = path.join(gamesPath, dateDir);
      for (const file of fs.readdirSync(datePath)) {
        if (file.endsWith(".json")) {
          const gameId = file.replace(".json", "");
          gameIdToDate.set(gameId, dateDir);
        }
      }
    }
  }

  const seasonTypeInfo = [
    { seasonType: "regular", maxWeek: 16 },
    { seasonType: "postseason", maxWeek: 5 },
  ];

  for (const { seasonType, maxWeek } of seasonTypeInfo) {
    for (let week = 1; week <= maxWeek; week++) {
      try {
        const plays = await cfbFetch<Play[]>("/plays", {
          year: season,
          week,
          seasonType,
        });

        // Group plays by gameId
        const playsByGame = new Map<string, Play[]>();
        for (const play of plays) {
          const gameId = String(play.game_id ?? "");
          if (!gameId) continue;
          if (!playsByGame.has(gameId)) {
            playsByGame.set(gameId, []);
          }
          playsByGame.get(gameId)!.push(play);
        }

        // Save each game's plays separately
        for (const [gameId, gamePlays] of playsByGame) {
          const date = gameIdToDate.get(gameId) ?? "unknown";
          const outFile = rawPathWithWeekDate(
            dataDir, NAME, sport, season,
            week, date,
            "plays",
            `${gameId}_plays.json`
          );
          
          ensureDirSync(path.dirname(outFile));
          writeJSON(outFile, gamePlays);
          filesWritten++;
        }
      } catch (err) {
        errors.push(`plays/${seasonType}/week_${week}: ${err}`);
      }
    }
  }

  return { filesWritten, errors };
}
```

### 5. Stats File Handling

**Current:** `stats_player_season.json` (30MB) in root season dir

**Option A (Recommended for now):** Split by week
```typescript
// Fetch endpoint already supports week parameter
const stats = await cfbFetch("/stats/player/season", { year: season, week }); // returns [player_id, stat_name, stat_value]

// Save to: {season}/{week}/stats/players_{week}.json
// Result: 16 files × 2-5MB each instead of 1 × 30MB file
```

**Option B (if Option A files still too large):** Split by player batch
```typescript
// Group players by ID ranges: 0-999, 1000-1999, etc.
const playerBatch = Math.floor(playerId / 1000);
// Save to: {season}/stats/by_player/batch_{playerBatch}_{week}.json
```

---

## Normalizer Changes

### Current (Python):
```python
def _cfbdata_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    data = _load_json(base / "games.json")
    # Process single 3.6MB file
```

### Updated (Python):

```python
def _cfbdata_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load games from new structure: {season}/{week}/ or {season}/reference/"""
    import glob
    
    records = []
    season_path = Path(base)
    
    # Look for games in any week directory
    for games_dir in glob.glob(str(season_path / "*" / "games")):
        for date_dir in glob.glob(str(games_dir / "*")):
            for game_file in glob.glob(str(date_dir / "*.json")):
                # Skip files with suffixes like _box_advanced, _weather, etc.
                if "_" in Path(game_file).stem:
                    continue
                
                data = _load_json(Path(game_file))
                if not data:
                    continue
                
                # Process single game record (now much simpler!)
                gid = str(data.get("id", ""))
                if not gid:
                    continue
                
                rec = {
                    "id": gid,
                    "season": str(data.get("season", season)),
                    # ... rest of processing
                }
                records.append(rec)
    
    return records
```

### Wrapper for Compatibility

To support **both old and new structures**, enhance `_load_json()`:

```python
def _load_json_cfbdata(base: Path, sport: str, season: str, endpoint: str) -> list[dict[str, Any]]:
    """
    Load JSON data, trying new structure first, falling back to old.
    
    - If new structure exists at {season}/{week}/{endpoint}/, use that
    - Else if old structure exists at {season}/{endpoint}.json, use that
    """
    import glob
    
    data = []
    
    # Try new structure first
    new_paths = glob.glob(str(base / "*" / endpoint))
    if new_paths:
        for path in new_paths:
            subdata = _load_json(Path(path))
            if subdata:
                data.extend(subdata if isinstance(subdata, list) else [subdata])
    
    # Fallback to old structure if no new data found
    if not data:
        old_path = base / f"{endpoint}.json"
        if old_path.exists():
            subdata = _load_json(old_path)
            if subdata:
                data = subdata if isinstance(subdata, list) else [subdata]
    
    return data
```

---

## Data Migration Script

**One-time script to move existing data:**

```python
# backend/scripts/migrate_cfbdata_structure.py

import json
import shutil
from pathlib import Path
from datetime import datetime

RAW_DATA_DIR = Path("v5.0/data/raw")

def migrate_cfbdata_season(sport: str, season: int):
    """Migrate cfbdata from old to new structure."""
    old_base = RAW_DATA_DIR / "cfbdata" / sport / str(season)
    
    if not old_base.exists():
        print(f"No data for {sport}/{season}")
        return
    
    print(f"\n=== Migrating {sport}/{season} ===")
    
    # 1. Move season-wide references to reference/ directory
    reference_files = [
        "recruiting.json", "recruiting_teams.json", "recruiting_groups.json",
        "talent.json", "teams_fbs.json", "stats_categories.json",
        "plays_types.json", "info.json", "conferences.json", "venues.json"
    ]
    
    ref_dir = old_base / "reference"
    ref_dir.mkdir(exist_ok=True)
    
    for fname in reference_files:
        old_file = old_base / fname
        if old_file.exists():
            new_file = ref_dir / fname
            new_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_file), str(new_file))
            print(f"  Moved {fname} → reference/")
    
    # 2. Migrate games.json → {week}/games/{date}/{gameId}.json
    games_file = old_base / "games.json"
    if games_file.exists():
        with open(games_file) as f:
            games = json.load(f)
        
        for game in games:
            week = game.get("week", 0)
            date = game.get("startDate", "").split('T')[0]
            game_id = str(game.get("id", ""))
            
            if not game_id or not date:
                continue
            
            game_path = old_base / str(week) / "games" / date / f"{game_id}.json"
            game_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(game_path, 'w') as f:
                json.dump(game, f)
        
        games_file.unlink()
        print(f"  Migrated games.json → {week}/games/{{date}}/{{gameId}}.json")
    
    # 3. Migrate plays/{week_N}.json → {week}/plays/{date}/{gameId}_plays.json
    plays_dir = old_base / "plays"
    if plays_dir.exists():
        # First load games mapping
        game_id_to_date = {}
        for week_num in range(1, 20):
            for games_subdir in (old_base / str(week_num) / "games").rglob("*.json"):
                # ... build mapping
                pass
        
        for week_file in plays_dir.glob("*.json"):
            match = re.match(r"week_(\d+)\.json", week_file.name)
            if not match:
                continue
            
            week_num = int(match.group(1))
            with open(week_file) as f:
                plays = json.load(f)
            
            # Group by game_id
            plays_by_game = {}
            for play in plays:
                game_id = str(play.get("game_id", ""))
                if game_id not in plays_by_game:
                    plays_by_game[game_id] = []
                plays_by_game[game_id].append(play)
            
            # Save per game
            for game_id, game_plays in plays_by_game.items():
                date = game_id_to_date.get(game_id, "unknown")
                play_path = old_base / str(week_num) / "plays" / date / f"{game_id}_plays.json"
                play_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(play_path, 'w') as f:
                    json.dump(game_plays, f)
            
            week_file.unlink()
            print(f"  Migrated plays/week_{week_num}.json")
    
    # 4. Keep other files as-is in their week directories
    print(f"  Migration complete!")

if __name__ == "__main__":
    migrate_cfbdata_season("ncaaf", 2023)
    migrate_cfbdata_season("ncaaf", 2024)
    migrate_cfbdata_season("ncaaf", 2025)
```

---

## File Size Estimates (Post-Migration)

| File | Old → New | Benefit |
|------|-----------|---------|
| stats_player_season.json | 30MB → 16 files × 2-4MB | ✓ 100x smaller individual files |
| games.json | 3.6MB → 8-10 files × 300-600KB | ✓ Easier to parse |
| plays/{week}.json | 16 files × 8-16MB → 16 weeks × 70 games × 200-300KB | ✓ Game-level granularity |
| **Reference/** | N/A | ✓ Centralized, easy to find |

---

## Validation Checklist

- [ ] All game records preserved (count matches)
- [ ] All play records preserved  (row count matches)
- [ ] Week/date extraction matches API responses
- [ ] Path generation works for all endpoints
- [ ] Normalizer reads from both old and new structures
- [ ] Normalized outputs identical (old vs new)
- [ ] Importer save paths correct
- [ ] Directory structure created on first write
- [ ] No filename collisions
- [ ] File checksums match before/after migration
