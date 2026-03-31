// ──────────────────────────────────────────────────────────
// MLB Stats API Provider
// ──────────────────────────────────────────────────────────
// Free, no-key-required MLB data: schedules, boxscores,
// and linescore (inning-by-inning) from statsapi.mlb.com.

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { rawPath, writeJSON, readJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "mlbstats";
const BASE = "https://statsapi.mlb.com/api/v1";
const SPORTS: readonly Sport[] = ["mlb"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const ENDPOINTS = ["schedule", "boxscores"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

// ── API response types (minimal, only what we traverse) ─────

interface ScheduleGame {
  gamePk: number;
  status: { abstractGameState: string };
}

interface ScheduleDate {
  games: ScheduleGame[];
}

interface ScheduleResponse {
  dates: ScheduleDate[];
}

// ── Endpoint implementations ────────────────────────────────

async function importSchedule(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const outPath = rawPath(dataDir, NAME, "mlb", season, "schedule.json");

  if (dryRun) {
    logger.info(`[dry-run] Would fetch schedule for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const url = `${BASE}/schedule?sportId=1&season=${season}&gameType=R`;
  logger.progress(NAME, "mlb", "schedule", `Fetching ${season} regular-season schedule`);

  const data = await fetchJSON<ScheduleResponse>(url, NAME, RATE_LIMIT);
  writeJSON(outPath, data);

  const gameCount = data.dates?.reduce((n, d) => n + d.games.length, 0) ?? 0;
  logger.progress(NAME, "mlb", "schedule", `Wrote ${season} schedule (${gameCount} games)`);
  return { filesWritten: 1, errors: [] };
}

async function importBoxscores(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const schedulePath = rawPath(dataDir, NAME, "mlb", season, "schedule.json");
  const schedule = readJSON<ScheduleResponse>(schedulePath);

  if (!schedule) {
    const msg = `No schedule file for ${season} — run "schedule" endpoint first`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }

  // Collect completed-game IDs
  const gamePks: number[] = [];
  for (const date of schedule.dates ?? []) {
    for (const game of date.games) {
      if (game.status.abstractGameState === "Final") {
        gamePks.push(game.gamePk);
      }
    }
  }

  logger.progress(NAME, "mlb", "boxscores", `${season}: ${gamePks.length} completed games to check`);

  if (dryRun) {
    logger.info(`[dry-run] Would fetch ${gamePks.length} boxscores for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  let filesWritten = 0;
  const errors: string[] = [];

  for (const gamePk of gamePks) {
    const gamePath = rawPath(dataDir, NAME, "mlb", season, "games", `${gamePk}.json`);

    if (fileExists(gamePath)) continue;

    try {
      const [boxscore, linescore] = await Promise.all([
        fetchJSON(`${BASE}/game/${gamePk}/boxscore`, NAME, RATE_LIMIT),
        fetchJSON(`${BASE}/game/${gamePk}/linescore`, NAME, RATE_LIMIT),
      ]);

      writeJSON(gamePath, { gamePk, boxscore, linescore });
      filesWritten++;

      if (filesWritten % 100 === 0) {
        logger.progress(NAME, "mlb", "boxscores", `${season}: ${filesWritten} games written so far`);
      }

      await sleep(200);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn(`Failed gamePk ${gamePk}: ${msg}`, NAME);
      errors.push(`game/${gamePk}: ${msg}`);
    }
  }

  logger.progress(NAME, "mlb", "boxscores", `${season}: wrote ${filesWritten} game files`);
  return { filesWritten, errors };
}

// ── Dispatch ────────────────────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (season: number, dataDir: string, dryRun: boolean) => Promise<{ filesWritten: number; errors: string[] }>> = {
  schedule: importSchedule,
  boxscores: importBoxscores,
};

// ── Provider definition ─────────────────────────────────────

const mlbstats: Provider = {
  name: NAME,
  label: "MLB Stats API",
  sports: SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS];

    for (const season of opts.seasons) {
      for (const ep of activeEndpoints) {
        try {
          const result = await ENDPOINT_FNS[ep](season, opts.dataDir, opts.dryRun);
          totalFiles += result.filesWritten;
          allErrors.push(...result.errors);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.error(`${ep}/${season} failed: ${msg}`, NAME);
          allErrors.push(`${ep}/${season}: ${msg}`);
        }
      }
    }

    return {
      provider: NAME,
      sport: "mlb",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default mlbstats;
