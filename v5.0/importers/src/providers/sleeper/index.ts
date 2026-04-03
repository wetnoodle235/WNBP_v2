// ──────────────────────────────────────────────────────────
// Sleeper API Provider
// ──────────────────────────────────────────────────────────
// Sleeper is a fantasy sports platform with a completely free,
// no-key-required public API. Extremely high-value data:
//   • Player registry with cross-platform IDs (ESPN, Yahoo,
//     Rotowire, Sportradar, Pandascore, etc.)
//   • Real-time injury status, depth chart positions
//   • Trending players (add/drop activity)
//   • Weekly fantasy stats (pts, yards, TDs, etc.)
//
// Sports: NFL, NBA, MLB (most complete), NHL (partial)
// Docs: https://docs.sleeper.com/

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "sleeper";
const BASE = "https://api.sleeper.app/v1";
const RATE_LIMIT: RateLimitConfig = { requests: 8, perMs: 1_000 };

// Sleeper uses lowercase sport names that differ from our Sport type
const SPORT_MAP: Partial<Record<Sport, string>> = {
  nfl: "nfl",
  nba: "nba",
  mlb: "mlb",
  nhl: "nhl",
};

const SUPPORTED_SPORTS = Object.keys(SPORT_MAP) as Sport[];

// NFL weeks per season (17 regular-season games + playoffs)
const NFL_REGULAR_WEEKS = 18;
const NFL_PLAYOFF_WEEKS = [1, 2, 3, 4]; // Wildcard → Super Bowl

// NBA seasons: stats endpoint uses different season key
function nbaSeasonKey(year: number): string {
  return `${year}-${String(year + 1).slice(-2)}`;
}

interface TrendingPlayer {
  player_id: string;
  count: number;
}

async function fetchPlayerRegistry(sleeperSport: string): Promise<Record<string, unknown>[]> {
  const data = await fetchJSON<Record<string, Record<string, unknown>>>(
    `${BASE}/players/${sleeperSport}`,
    NAME, RATE_LIMIT,
  );
  if (!data) return [];
  // Only return active or recently active players (filter out very old/empty entries)
  return Object.values(data).filter((p) => {
    if (!p || typeof p !== "object") return false;
    const yrs = p["years_exp"];
    return p["active"] === true || (typeof yrs === "number" && yrs < 20);
  });
}

async function fetchTrending(sleeperSport: string, type: "add" | "drop"): Promise<TrendingPlayer[]> {
  return await fetchJSON<TrendingPlayer[]>(
    `${BASE}/players/${sleeperSport}/trending/${type}?lookback_hours=24&limit=50`,
    NAME, RATE_LIMIT,
  ) ?? [];
}

async function fetchWeeklyStats(
  sleeperSport: string,
  seasonType: "regular" | "post",
  year: number,
  week: number,
): Promise<Record<string, Record<string, number>> | null> {
  return await fetchJSON<Record<string, Record<string, number>>>(
    `${BASE}/stats/${sleeperSport}/${seasonType}/${year}?week=${week}`,
    NAME, RATE_LIMIT,
  );
}

const sleeper: Provider = {
  name: NAME,
  label: "Sleeper — player registry, injury status, depth charts, weekly fantasy stats",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["players", "trending", "stats"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    for (const sport of activeSports) {
      const sleeperSport = SPORT_MAP[sport]!;

      // ── Player Registry (current snapshot — not season-scoped) ──
      const latestSeason = Math.max(...opts.seasons);
      const registryPath = rawPath(opts.dataDir, NAME, sport, latestSeason, "players.json");

      if (!fileExists(registryPath)) {
        logger.progress(NAME, sport, "players", `Fetching ${sleeperSport} player registry`);
        if (!opts.dryRun) {
          try {
            const players = await fetchPlayerRegistry(sleeperSport);
            if (players.length > 0) {
              writeJSON(registryPath, {
                source: NAME, sport, fetched_at: new Date().toISOString(),
                count: players.length, players,
              });
              filesWritten++;
              logger.progress(NAME, sport, "players", `${players.length} players`);
            }
          } catch (err) {
            const msg = `${sport} players: ${err instanceof Error ? err.message : String(err)}`;
            logger.warn(msg, NAME);
            errors.push(msg);
          }
        }
      } else {
        logger.debug(`Player registry exists for ${sport} — skipping`, NAME);
      }

      // ── Trending (always fresh for current season) ──
      if (opts.seasons.includes(new Date().getFullYear())) {
        const trendPath = rawPath(opts.dataDir, NAME, sport, latestSeason, "trending.json");
        logger.progress(NAME, sport, "trending", "add/drop last 24h");
        if (!opts.dryRun) {
          try {
            const [add, drop] = await Promise.all([
              fetchTrending(sleeperSport, "add"),
              fetchTrending(sleeperSport, "drop"),
            ]);
            writeJSON(trendPath, {
              source: NAME, sport, fetched_at: new Date().toISOString(),
              trending_add: add, trending_drop: drop,
            });
            filesWritten++;
          } catch (err) {
            errors.push(`${sport} trending: ${err instanceof Error ? err.message : String(err)}`);
          }
        }
      }

      // ── Weekly Stats (NFL only — others are less complete) ──
      if (sport !== "nfl") continue;

      for (const year of opts.seasons) {
        // Regular season weeks
        for (let week = 1; week <= NFL_REGULAR_WEEKS; week++) {
          const weekPath = rawPath(opts.dataDir, NAME, sport, year, `stats_week_${String(week).padStart(2, "0")}.json`);
          if (fileExists(weekPath)) continue;
          if (opts.dryRun) continue;

          try {
            const stats = await fetchWeeklyStats(sleeperSport, "regular", year, week);
            if (stats && Object.keys(stats).length > 0) {
              writeJSON(weekPath, {
                source: NAME, sport, season: year, week, season_type: "regular",
                fetched_at: new Date().toISOString(),
                count: Object.keys(stats).length, stats,
              });
              filesWritten++;
            }
          } catch { /* week not yet available — skip silently */ }
        }

        // Playoff weeks
        for (const week of NFL_PLAYOFF_WEEKS) {
          const weekPath = rawPath(opts.dataDir, NAME, sport, year, `stats_playoff_week_${week}.json`);
          if (fileExists(weekPath)) continue;
          if (opts.dryRun) continue;

          try {
            const stats = await fetchWeeklyStats(sleeperSport, "post", year, week);
            if (stats && Object.keys(stats).length > 0) {
              writeJSON(weekPath, {
                source: NAME, sport, season: year, week, season_type: "post",
                fetched_at: new Date().toISOString(),
                count: Object.keys(stats).length, stats,
              });
              filesWritten++;
            }
          } catch { /* skip */ }
        }

        logger.progress(NAME, sport, "stats", `${year} weekly stats complete`);
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0]! : "multi",
      filesWritten, errors, durationMs: Date.now() - start,
    };
  },
};

export default sleeper;
