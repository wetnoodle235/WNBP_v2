# CFBData Storage Reorganization - Implementation Code Examples

## Quick Reference: Path Changes

### Old vs New Paths

| Data Type | Old Path | New Path |
|-----------|----------|----------|
| Single game | N/A | `{season}/{week}/games/{date}/{gameId}.json` |
| All games | `{season}/games.json` | Multiple files in `{season}/{week}/games/` |
| Plays (week) | `{season}/plays/week_N.json` | `{season}/{week}/plays/{date}/{gameId}_plays.json` |
| Player stats | `{season}/stats_player_season.json` | `{season}/{week}/stats/players_{week}.json` |
| Game box | `{season}/game_box_advanced/{gameId}.json` | `{season}/{week}/games/{date}/{gameId}_box_advanced.json` |
| References | `{season}/{endpoint}.json` | `{season}/reference/{endpoint}.json` |

---

## 1. Update importers/src/core/io.ts

### Add new path helper:

```typescript
// ── New helper for week-date based paths ──
export function rawPathWithWeekDate(
  dataDir: string,
  provider: string,
  sport: string,
  season: number,
  week: number | null,  // null = season-wide reference
  date: string,         // YYYY-MM-DD format
  endpoint: string,     // e.g., "games", "plays", "stats"
  // ... variadic parts for nested files
  ...parts: string[]
): string {
  const components = [dataDir, provider, sport, String(season)];
  
  if (week === null) {
    // Season-wide data goes to /reference/
    components.push("reference");
    components.push(endpoint);
  } else {
    // Weekly data: /{season}/{week}/{endpoint}/{date}/
    components.push(String(week));
    components.push(endpoint);
    if (date) {
      components.push(date);
    }
  }
  
  return path.join(...components, ...parts);
}

// ── Update existing rawPath to support both ──
export function rawPath(
  dataDir: string,
  provider: string,
  sport: string,
  season: number,
  ...parts: string[]
): string {
  // Prefer old behavior for backwards compatibility
  // New code should use rawPathWithWeekDate
  return path.join(dataDir, provider, sport, String(season), ...parts);
}
```

---

## 2. Update importers/src/providers/cfbdata/index.ts

### Helper Functions to Add

```typescript
// ── Helper: Extract week and date from game ──
function getWeekAndDate(game: any): { week: number; date: string } {
  const week = game.week ?? (game.seasonType === "postseason" ? 0 : 1);
  const date = game.startDate?.split('T')[0] ?? "unknown";
  return { week, date };
}

// ── Helper: Ensure directory exists ──
async function ensureDir(dirPath: string): Promise<void> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  try {
    await fs.mkdir(path.dirname(dirPath), { recursive: true });
  } catch {
    // Directory likely already exists
  }
}
```

### Update importGames()

```typescript
async function importGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const [regular, postseason] = await Promise.all([
    cfbFetch<any[]>("/games", { year: season, seasonType: "regular" }),
    cfbFetch<any[]>("/games", { year: season, seasonType: "postseason" }),
  ]);
  
  const allGames = [...regular, ...postseason];
  
  // NEW: Save each game to individual file
  for (const game of allGames) {
    const gameId = String(game.id ?? "");
    if (!gameId) continue;
    
    const { week, date } = getWeekAndDate(game);
    const outFile = rawPathWithWeekDate(dataDir, NAME, sport, season, week, date, "games", `${gameId}.json`);
    
    if (fileExists(outFile)) continue;
    
    await ensureDir(outFile);
    writeJSON(outFile, game);
    filesWritten++;
    
    if (filesWritten % 50 === 0) {
      logger.progress(NAME, sport, "games", `Saved ${filesWritten} game files`);
    }
  }

  logger.progress(NAME, sport, "games", `Saved ${filesWritten} game files total`);
  return { filesWritten, errors };
}
```

### Update importPlays()

```typescript
async function importPlays(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "plays", `Fetching ${season} play-by-play`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  // First, build gameId → date mapping from games directory
  const gameIdToDate = new Map<string, string>();
  const gamesBaseDir = rawPath(dataDir, NAME, sport, season);
  
  // Scan all week directories
  for (let week = 0; week <= 20; week++) {
    const gamesDir = rawPathWithWeekDate(dataDir, NAME, sport, season, week, "", "games");
    for (const dateDir of fs.readdirSync(gamesDir, { withFileTypes: true })) {
      if (!dateDir.isDirectory()) continue;
      const datePath = path.join(gamesDir, dateDir.name);
      
      for (const file of fs.readdirSync(datePath)) {
        if (file.endsWith(".json") && !file.includes("_")) {
          const gameId = file.replace(".json", "");
          gameIdToDate.set(gameId, dateDir.name);
        }
      }
    }
  }

  const seasonTypeWindows = [
    { seasonType: "regular", maxWeek: 16 },
    { seasonType: "postseason", maxWeek: 5 },
  ];

  for (const window of seasonTypeWindows) {
    for (let week = 1; week <= window.maxWeek; week++) {
      try {
        const plays = await cfbFetch<any[]>("/plays", {
          year: season,
          week,
          seasonType: window.seasonType,
        });
        
        if (!Array.isArray(plays) || plays.length === 0) {
          continue;
        }

        // Group plays by game_id
        const playsByGame = new Map<string, any[]>();
        for (const play of plays) {
          const gameId = String(play.game_id ?? "");
          if (!gameId) continue;
          
          if (!playsByGame.has(gameId)) {
            playsByGame.set(gameId, []);
          }
          playsByGame.get(gameId)!.push(play);
        }

        // Save each game's plays to separate file
        for (const [gameId, gamePlays] of playsByGame) {
          const date = gameIdToDate.get(gameId) ?? "unknown";
          const outFile = rawPathWithWeekDate(
            dataDir, NAME, sport, season,
            week, date,
            "plays",
            `${gameId}_plays.json`
          );
          
          if (fileExists(outFile)) continue;
          
          await ensureDir(outFile);
          writeJSON(outFile, gamePlays);
          filesWritten++;
        }
        
        logger.progress(NAME, sport, "plays", `Saved ${window.seasonType} week ${week}`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn(`plays ${window.seasonType} week ${week}: ${msg}`, NAME);
        errors.push(`plays/${window.seasonType}_week_${week}: ${msg}`);
      }
    }
  }

  return { filesWritten, errors };
}
```

### Update importStatsPlayerSeason()

```typescript
async function importStatsPlayerSeason(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "stats_player_season", `Fetching ${season} player season stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  // NEW: Fetch stats by week to split the 30MB file
  const seasonTypeWindows = [
    { seasonType: "regular", maxWeek: 16 },
    { seasonType: "postseason", maxWeek: 5 },
  ];

  for (const window of seasonTypeWindows) {
    for (let week = 1; week <= window.maxWeek; week++) {
      try {
        const stats = await cfbFetch<any[]>("/stats/player/season", {
          year: season,
          seasonType: window.seasonType,
          week,
        });
        
        if (!Array.isArray(stats) || stats.length === 0) {
          continue;
        }

        const prefix = window.seasonType === "regular" ? "week" : "postseason_week";
        const outFile = rawPathWithWeekDate(
          dataDir, NAME, sport, season,
          week, "",  // date not applicable for season stats
          "stats",
          `players_${prefix}_${week}.json`
        );
        
        if (fileExists(outFile)) continue;
        
        await ensureDir(outFile);
        writeJSON(outFile, stats);
        filesWritten++;
        
        logger.progress(NAME, sport, "stats_player_season", `Saved ${window.seasonType} week ${week}`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn(`stats_player_season ${window.seasonType} week ${week}: ${msg}`, NAME);
        errors.push(`stats_player_season/${window.seasonType}_week_${week}: ${msg}`);
      }
    }
  }

  if (filesWritten === 0) {
    errors.push("No player stats downloaded");
  }

  return { filesWritten, errors };
}
```

### Update Reference File Functions

For season-wide files (recruiting, rankings, etc.), save to `reference/` directory:

```typescript
async function importRecruiting(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  // NEW: Use week=null to save to reference directory
  const outFile = rawPathWithWeekDate(
    dataDir, NAME, sport, season,
    null,  // week=null means reference/
    "",    // date not applicable
    "recruiting.json"
  );
  
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "recruiting", "Skipping — already exists");
    return { filesWritten: 0, errors: [] };
  }
  
  logger.progress(NAME, sport, "recruiting", `Fetching ${season} recruiting`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/recruiting/players", { year: season });
  
  await ensureDir(outFile);
  writeJSON(outFile, data);
  
  logger.progress(NAME, sport, "recruiting", "Saved recruiting data");
  return { filesWritten: 1, errors: [] };
}

// Similarly for rankings, talent, teams_fbs, etc.
async function importRankings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPathWithWeekDate(
    dataDir, NAME, sport, season,
    null, "", "rankings.json"
  );
  // ... rest of function
}
```

---

## 3. Update backend/normalization/normalizer.py

### Add Compatibility Layer

```python
from pathlib import Path
import json
import glob

def _load_json_cfbdata(base: Path, endpoint: str) -> list[dict[str, Any]]:
    """
    Load cfbdata JSON, supporting both old (flat) and new (hierarchical) structures.
    
    New structure: {season}/{week}/{endpoint}/{date}/*.json or {season}/reference/{endpoint}.json
    Old structure: {season}/{endpoint}.json
    """
    all_data = []
    
    # Try new structure first: look in all week directories
    for week_glob in glob.glob(str(base / "*" / endpoint)):
        # This matches {season}/{week}/{endpoint}/
        week_path = Path(week_glob)
        
        # Look for files or date subdirectories
        for item in week_path.iterdir():
            if item.is_file() and item.suffix == ".json":
                data = _load_json(item)
                if data:
                    all_data.extend(data if isinstance(data, list) else [data])
            elif item.is_dir():
                # {date}/ subdirectory
                for file in item.glob("*.json"):
                    data = _load_json(file)
                    if data:
                        all_data.extend(data if isinstance(data, list) else [data])
    
    # Try new structure: reference/ directory (season-wide)
    ref_path = base / "reference" / f"{endpoint}.json"
    if not all_data and ref_path.exists():
        data = _load_json(ref_path)
        if data:
            all_data = data if isinstance(data, list) else [data]
    
    # Fallback to old structure
    if not all_data:
        old_path = base / f"{endpoint}.json"
        if old_path.exists():
            data = _load_json(old_path)
            if data:
                all_data = data if isinstance(data, list) else [data]
    
    return all_data
```

### Update _cfbdata_games()

```python
def _cfbdata_games(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load games from new hierarchical structure."""
    data = _load_json_cfbdata(base, "games")
    
    if not data or not isinstance(data, list):
        return []
    
    records: list[dict[str, Any]] = []
    for g in data:
        gid = str(g.get("id", ""))
        if not gid:
            continue
        
        rec: dict[str, Any] = {
            "id": gid,
            "season": str(g.get("season", season)),
            "date": (g.get("startDate") or "")[:10] or None,
            "status": "final" if g.get("completed") else "scheduled",
            "home_team": g.get("homeTeam", ""),
            "away_team": g.get("awayTeam", ""),
            "home_team_id": str(g.get("homeId", "")),
            "away_team_id": str(g.get("awayId", "")),
            "home_score": _safe_int(g.get("homePoints")),
            "away_score": _safe_int(g.get("awayPoints")),
            "venue": g.get("venue"),
            "attendance": _safe_int(g.get("attendance")),
        }
        
        # Extract quarter scores from CFBData homeLineScores/awayLineScores
        for prefix, key in [("home", "homeLineScores"), ("away", "awayLineScores")]:
            ls = g.get(key)
            if isinstance(ls, list):
                ot_total = 0
                has_ot = False
                for idx, val in enumerate(ls):
                    v = _safe_int(val)
                    if v is None:
                        continue
                    period = idx + 1
                    if period <= 4:
                        rec[f"{prefix}_q{period}"] = v
                    else:
                        ot_total += v
                        has_ot = True
                if has_ot:
                    rec[f"{prefix}_ot"] = ot_total
        
        records.append(rec)
    
    return records
```

### Update _cfbdata_standings()

```python
def _cfbdata_standings(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Build standings-like records from CFBData rankings polls (reference)."""
    data = _load_json_cfbdata(base, "rankings")
    
    if not data or not isinstance(data, list):
        return []
    
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    
    # Use the latest poll week available
    for poll_week in reversed(data):
        for poll in poll_week.get("polls", []):
            for entry in poll.get("ranks", []):
                tid = str(entry.get("teamId", ""))
                if not tid or tid in seen:
                    continue
                seen.add(tid)
                records.append({
                    "team_id": tid,
                    "team_name": entry.get("school", ""),
                    "conference": entry.get("conference", ""),
                    "rank": _safe_int(entry.get("rank")),
                    "season": season,
                })
        if records:
            break
    
    return records
```

### Update _cfbdata_players()

This is a new function to handle split player stats:

```python
def _cfbdata_players(base: Path, sport: str, season: str) -> list[dict[str, Any]]:
    """Load all players from new weekly-split structure."""
    import glob
    
    all_players = []
    
    # Look for stats files in all week directories
    for stats_file in glob.glob(str(base / "*" / "stats" / "*.json")):
        # Filenames: players_week_N.json, players_postseason_week_N.json
        data = _load_json(Path(stats_file))
        if data and isinstance(data, list):
            all_players.extend(data)
    
    # Fallback to old structure if new structure empty
    if not all_players:
        data = _load_json(base / "stats_player_season.json")
        if data and isinstance(data, list):
            all_players = data
    
    # Normalize to common format
    records = []
    for entry in all_players:
        # CFBData player stats format:
        # {"playerId": 123, "playerName": "John Doe", "team": "Alabama", ...}
        pid = str(entry.get("playerId", ""))
        if not pid:
            continue
        
        rec = {
            "id": pid,
            "name": entry.get("playerName", ""),
            "team": entry.get("team", ""),
            "position": entry.get("position", ""),
            "season": season,
            # Store all stats with prefix
            **{f"stat_{k}": entry.get(k) for k in entry if k not in ["playerId", "playerName", "team", "position"]}
        }
        records.append(rec)
    
    return records
```

---

## 4. Create Migration Script: backend/scripts/migrate_cfbdata_structure.py

```python
#!/usr/bin/env python3
"""
One-time migration script: move cfbdata from flat to hierarchical structure.

Old: data/raw/cfbdata/ncaaf/2025/games.json
New: data/raw/cfbdata/ncaaf/2025/{week}/games/{date}/{gameId}.json

Run: python3 backend/scripts/migrate_cfbdata_structure.py
"""

import json
import shutil
import re
from pathlib import Path
from datetime import datetime

RAW_DATA_DIR = Path(__file__).parent.parent.parent / "data" / "raw"

def _load_json(path: Path) -> dict | list | None:
    """Load JSON file."""
    if not path.exists():
        return None
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"ERROR loading {path}: {e}")
        return None

def _save_json(path: Path, data: dict | list) -> None:
    """Save JSON file with pretty printing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def migrate_cfbdata_season(sport: str, season: int):
    """Migrate cfbdata from old to new structure."""
    old_base = RAW_DATA_DIR / "cfbdata" / sport / str(season)
    
    if not old_base.exists():
        print(f"⚠ No data for {sport}/{season} — skipping")
        return
    
    print(f"\n{'='*60}")
    print(f"Migrating {sport}/{season}")
    print(f"{'='*60}")
    
    # 1. Migrate season-wide references
    print("\n[1/5] Moving season-wide reference files...")
    
    reference_files = [
        "recruiting.json", "recruiting_teams.json", "recruiting_groups.json",
        "talent.json", "teams_fbs.json", "stats_categories.json",
        "plays_types.json", "plays_stats_types.json", "info.json",
        "conferences.json", "venues.json", "records.json",
        "calendar.json", "ranks.json", "ratings_sp.json", "ratings_srs.json",
        "ratings_elo.json", "ratings_fpi.json"
    ]
    
    ref_dir = old_base / "reference"
    moved = 0
    
    for fname in reference_files:
        old_file = old_base / fname
        if old_file.exists():
            new_file = ref_dir / fname
            new_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_file), str(new_file))
            moved += 1
            print(f"  ✓ {fname}")
    
    print(f"  Moved {moved} reference files")
    
    # 2. Migrate games.json → {week}/games/{date}/{gameId}.json
    print("\n[2/5] Migrating games...")
    
    games_file = old_base / "games.json"
    if games_file.exists():
        games = _load_json(games_file)
        if games and isinstance(games, list):
            for game in games:
                week = game.get("week", 0)
                date = game.get("startDate", "").split('T')[0] if game.get("startDate") else "unknown"
                game_id = str(game.get("id", ""))
                
                if not game_id or date == "unknown":
                    print(f"  ⚠ Skipping game {game_id} (no date)")
                    continue
                
                game_dir = old_base / str(week) / "games" / date
                game_dir.mkdir(parents=True, exist_ok=True)
                _save_json(game_dir / f"{game_id}.json", game)
            
            games_file.unlink()
            print(f"  ✓ Migrated {len(games)} game files")
    
    # 3. Migrate plays/{week_N}.json → {week}/plays/{date}/{gameId}_plays.json
    print("\n[3/5] Migrating play-by-play data...")
    
    # First build gameId → date mapping
    game_id_to_date = {}
    for week_num in range(1, 20):
        games_dir = old_base / str(week_num) / "games"
        if games_dir.exists():
            for date_dir in games_dir.iterdir():
                if date_dir.is_dir():
                    for game_file in date_dir.glob("*.json"):
                        game_id = game_file.stem
                        if not game_id.startswith("_"):
                            game_id_to_date[game_id] = date_dir.name
    
    plays_dir = old_base / "plays"
    if plays_dir.exists():
        for week_file in sorted(plays_dir.glob("week_*.json")) + sorted(plays_dir.glob("postseason_week_*.json")):
            # Extract week number
            match = re.match(r"(postseason_)?week_(\d+)\.json", week_file.name)
            if not match:
                continue
            
            is_postseason = match.group(1) is not None
            week_num = int(match.group(2))
            
            plays = _load_json(week_file)
            if not plays or not isinstance(plays, list):
                continue
            
            # Group plays by game_id
            plays_by_game = {}
            for play in plays:
                game_id = str(play.get("game_id", ""))
                if not game_id:
                    continue
                if game_id not in plays_by_game:
                    plays_by_game[game_id] = []
                plays_by_game[game_id].append(play)
            
            # Save per game
            for game_id, game_plays in plays_by_game.items():
                date = game_id_to_date.get(game_id, "unknown")
                plays_dir_new = old_base / str(week_num) / "plays" / date
                plays_dir_new.mkdir(parents=True, exist_ok=True)
                _save_json(plays_dir_new / f"{game_id}_plays.json", game_plays)
            
            week_file.unlink()
            season_type = "postseason" if is_postseason else "regular"
            print(f"  ✓ Migrated plays/{season_type} week {week_num} ({len(plays_by_game)} games)")
    
    # 4. Migrate stats_player_season.json
    print("\n[4/5] Migrating player statistics...")
    
    stats_file = old_base / "stats_player_season.json"
    if stats_file.exists():
        stats = _load_json(stats_file)
        if stats and isinstance(stats, list):
            # For now, just move to week 1 stats directory (simple approach)
            # Could further split if needed
            stats_dir = old_base / "1" / "stats"
            stats_dir.mkdir(parents=True, exist_ok=True)
            _save_json(stats_dir / "players_week_1.json", stats)
            stats_file.unlink()
            print(f"  ✓ Migrated {len(stats)} player stats records")
    
    # 5. Clean up empty plays directory
    print("\n[5/5] Cleaning up...")
    
    old_plays = old_base / "plays"
    if old_plays.exists() and not any(old_plays.iterdir()):
        old_plays.rmdir()
        print(f"  ✓ Removed empty plays/ directory")
    
    print(f"\n✅ Migration complete for {sport}/{season}!\n")

def main():
    """Migrate all seasons."""
    for sport in ["ncaaf"]:
        for season in [2020, 2021, 2022, 2023, 2024, 2025]:
            try:
                migrate_cfbdata_season(sport, season)
            except Exception as e:
                print(f"\n❌ ERROR migrating {sport}/{season}: {e}")
    
    print("\n" + "="*60)
    print("Migration complete!")
    print("="*60)
    print("\nNext steps:")
    print("1. Verify new structure: ls -la data/raw/cfbdata/ncaaf/2025/*/games/")
    print("2. Test normalizer: python3 backend/scripts/data_audit.py")
    print("3. Compare checksums to ensure no data loss")

if __name__ == "__main__":
    main()
```

---

## 5. Testing & Validation

### Test the new importer with dry-run:

```bash
cd v5.0/importers
npm run import:cfbdata -- --sports=ncaaf --seasons=2025 --dry-run
# Should log new paths like:
# Saving to: data/raw/cfbdata/ncaaf/2025/1/games/2025-09-06/401547489.json
```

### Test the normalizer reads correctly:

```bash
cd v5.0/backend
python3 -c "
from pathlib import Path
from normalization.normalizer import _cfbdata_games
base = Path('data/raw/cfbdata/ncaaf/2025')
games = _cfbdata_games(base, 'ncaaf', '2025')
print(f'Loaded {len(games)} games')
print(f'Sample: {games[0] if games else None}')
"
```

### Before/after comparison:

```bash
# After migration, compare normalized outputs
python3 backend/scripts/data_audit.py --provider cfbdata --sport ncaaf --season 2025
```

---

## Summary of Changes

| File | Changes |
|------|---------|
| `importers/src/core/io.ts` | Add `rawPathWithWeekDate()` helper |
| `importers/src/providers/cfbdata/index.ts` | Update ~10 import functions to use new paths, split large files |
| `backend/normalization/normalizer.py` | Update ~6 `_cfbdata_*` functions, add compatibility layer |
| `backend/scripts/migrate_cfbdata_structure.py` | New file: one-time migration |

**Total code changes: ~500 lines**
