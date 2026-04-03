// ──────────────────────────────────────────────────────────
// nflverse Provider
// ──────────────────────────────────────────────────────────
// Fetches structured NFL data from the nflverse GitHub data
// releases — pre-packaged CSVs updated throughout the season.
// Covers schedules, rosters, play-by-play, and player stats.
// No API key required. Uses GitHub raw CDN.

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { parse as parseCsv } from "csv-parse/sync";
import { fetchCSV } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "nflverse";
const NFLVERSE_BASE = "https://github.com/nflverse/nflverse-data/releases/download";
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 2_000 };

const SUPPORTED_SPORTS: Sport[] = ["nfl"];
const ENDPOINTS = ["schedules", "rosters", "player_stats", "players"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

function parseCsvSafe(text: string): Record<string, string>[] {
  try {
    return parseCsv(text, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true,
    }) as Record<string, string>[];
  } catch {
    return [];
  }
}

async function importSchedules(ctx: EndpointCtx): Promise<EndpointResult> {
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "schedules.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };

  try {
    // nflverse-data "schedules" release contains a single games.csv for all seasons
    const url = `${NFLVERSE_BASE}/schedules/games.csv`;
    const csv = await fetchCSV(url, NAME, RATE_LIMIT);
    const rows = parseCsvSafe(csv);

    // Filter to requested season
    const filtered = ctx.season > 0
      ? rows.filter((r) => String(r.season ?? r.year ?? "") === String(ctx.season))
      : rows;

    writeJSON(outPath, {
      source: NAME,
      sport: ctx.sport,
      season: String(ctx.season),
      count: filtered.length,
      schedules: filtered,
      fetched_at: new Date().toISOString(),
    });
    logger.progress(NAME, ctx.sport, "schedules", `${filtered.length} games`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    return { filesWritten: 0, errors: [`schedules: ${err instanceof Error ? err.message : String(err)}`] };
  }
}

async function importRosters(ctx: EndpointCtx): Promise<EndpointResult> {
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "rosters.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };

  try {
    const url = `${NFLVERSE_BASE}/rosters/roster_${ctx.season}.csv`;
    const csv = await fetchCSV(url, NAME, RATE_LIMIT);
    const rows = parseCsvSafe(csv);
    writeJSON(outPath, {
      source: NAME,
      sport: ctx.sport,
      season: String(ctx.season),
      count: rows.length,
      rosters: rows,
      fetched_at: new Date().toISOString(),
    });
    logger.progress(NAME, ctx.sport, "rosters", `${rows.length} players`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    return { filesWritten: 0, errors: [`rosters/${ctx.season}: ${err instanceof Error ? err.message : String(err)}`] };
  }
}

async function importPlayerStats(ctx: EndpointCtx): Promise<EndpointResult> {
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "player_stats.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };

  try {
    const url = `${NFLVERSE_BASE}/player_stats/player_stats_${ctx.season}.csv`;
    const csv = await fetchCSV(url, NAME, RATE_LIMIT);
    const rows = parseCsvSafe(csv);
    writeJSON(outPath, {
      source: NAME,
      sport: ctx.sport,
      season: String(ctx.season),
      count: rows.length,
      player_stats: rows,
      fetched_at: new Date().toISOString(),
    });
    logger.progress(NAME, ctx.sport, "player_stats", `${rows.length} stat rows`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    return { filesWritten: 0, errors: [`player_stats/${ctx.season}: ${err instanceof Error ? err.message : String(err)}`] };
  }
}

async function importPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "players.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };

  try {
    const url = `${NFLVERSE_BASE}/players/players.csv`;
    const csv = await fetchCSV(url, NAME, RATE_LIMIT);
    const rows = parseCsvSafe(csv);
    writeJSON(outPath, {
      source: NAME,
      sport: ctx.sport,
      season: String(ctx.season),
      count: rows.length,
      players: rows,
      fetched_at: new Date().toISOString(),
    });
    logger.progress(NAME, ctx.sport, "players", `${rows.length} players`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    return { filesWritten: 0, errors: [`players: ${err instanceof Error ? err.message : String(err)}`] };
  }
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  schedules: importSchedules,
  rosters: importRosters,
  player_stats: importPlayerStats,
  players: importPlayers,
};

const nflverse: Provider = {
  name: NAME,
  label: "nflverse",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s): s is Sport => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];
    const activeEndpoints = (opts.endpoints.length
      ? opts.endpoints.filter((e) => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS]) as Endpoint[];

    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        for (const ep of activeEndpoints) {
          try {
            const r = await ENDPOINT_FNS[ep]({ sport, season, dataDir: opts.dataDir, dryRun: opts.dryRun });
            totalFiles += r.filesWritten;
            allErrors.push(...r.errors);
          } catch (err) {
            allErrors.push(`${ep}/${sport}/${season}: ${err instanceof Error ? err.message : String(err)}`);
          }
        }
      }
    }

    return { provider: NAME, sport: "nfl", filesWritten: totalFiles, errors: allErrors, durationMs: Date.now() - start };
  },
};

export default nflverse;
