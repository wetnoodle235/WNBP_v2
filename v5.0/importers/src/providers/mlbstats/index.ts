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

const ENDPOINTS = ["schedule", "boxscores", "game_feed", "teams", "standings", "rosters", "people"] as const;
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

  const url = `${BASE}/schedule?sportId=1&season=${season}&gameType=R,F,D,L,W&hydrate=linescore`;
  logger.progress(NAME, "mlb", "schedule", `Fetching ${season} schedule`);

  const data = await fetchJSON<ScheduleResponse>(url, NAME, RATE_LIMIT);
  writeJSON(outPath, data);

  const gameCount = data.dates?.reduce((n, d) => n + d.games.length, 0) ?? 0;
  logger.progress(NAME, "mlb", "schedule", `Wrote ${season} schedule (${gameCount} games)`);
  return { filesWritten: 1, errors: [] };
}

function scheduleGamePks(schedule: ScheduleResponse | null, includeAll = false): number[] {
  if (!schedule) return [];
  const gamePks: number[] = [];
  for (const date of schedule.dates ?? []) {
    for (const game of date.games ?? []) {
      if (includeAll || game.status?.abstractGameState === "Final") {
        gamePks.push(game.gamePk);
      }
    }
  }
  return gamePks;
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

  const gamePks = scheduleGamePks(schedule, false);

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

async function importGameFeed(
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

  const gamePks = scheduleGamePks(schedule, true);
  logger.progress(NAME, "mlb", "game_feed", `${season}: ${gamePks.length} games to check`);

  if (dryRun) {
    logger.info(`[dry-run] Would fetch ${gamePks.length} game feeds for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  let filesWritten = 0;
  let skippedUnavailable = 0;
  const errors: string[] = [];

  for (const gamePk of gamePks) {
    const gamePath = rawPath(dataDir, NAME, "mlb", season, "game_feed", `${gamePk}.json`);
    if (fileExists(gamePath)) continue;

    try {
      const feed = await fetchJSON(`${BASE}/game/${gamePk}/feed/live`, NAME, RATE_LIMIT, { retries: 1 });
      writeJSON(gamePath, { gamePk, feed });
      filesWritten++;

      if (filesWritten % 100 === 0) {
        logger.progress(NAME, "mlb", "game_feed", `${season}: ${filesWritten} game feeds written so far`);
      }

      await sleep(200);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);

      // Historical spring/exhibition game IDs can return 404 from feed/live.
      if (msg.includes("HTTP 404")) {
        skippedUnavailable++;
        continue;
      }

      logger.warn(`Failed game feed ${gamePk}: ${msg}`, NAME);
      errors.push(`game_feed/${gamePk}: ${msg}`);
    }
  }

  logger.progress(
    NAME,
    "mlb",
    "game_feed",
    `${season}: wrote ${filesWritten} game feed files (${skippedUnavailable} unavailable)`
  );
  return { filesWritten, errors };
}

async function importTeams(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const outPath = rawPath(dataDir, NAME, "mlb", season, "teams.json");
  if (dryRun) {
    logger.info(`[dry-run] Would fetch teams for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const url = `${BASE}/teams?sportId=1&season=${season}&hydrate=venue,division`;
  logger.progress(NAME, "mlb", "teams", `Fetching teams for ${season}`);
  const data = await fetchJSON(url, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  const count = Array.isArray((data as { teams?: unknown[] }).teams) ? ((data as { teams: unknown[] }).teams.length) : 0;
  logger.progress(NAME, "mlb", "teams", `Wrote ${season} teams (${count})`);
  return { filesWritten: 1, errors: [] };
}

async function importStandings(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const outPath = rawPath(dataDir, NAME, "mlb", season, "standings.json");
  if (dryRun) {
    logger.info(`[dry-run] Would fetch standings for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const url = `${BASE}/standings?sportId=1&season=${season}`;
  logger.progress(NAME, "mlb", "standings", `Fetching standings for ${season}`);
  const data = await fetchJSON(url, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "mlb", "standings", `Wrote ${season} standings`);
  return { filesWritten: 1, errors: [] };
}

async function importRosters(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const teamsPath = rawPath(dataDir, NAME, "mlb", season, "teams.json");
  const teamsPayload = readJSON<{ teams?: Array<{ id?: number }> }>(teamsPath);
  const teamIds = (teamsPayload?.teams ?? [])
    .map((t) => t.id)
    .filter((id): id is number => typeof id === "number");

  if (teamIds.length === 0) {
    const msg = `No teams file for ${season} — run "teams" endpoint first`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }

  if (dryRun) {
    logger.info(`[dry-run] Would fetch ${teamIds.length} rosters for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  let filesWritten = 0;
  const errors: string[] = [];
  for (const teamId of teamIds) {
    const outPath = rawPath(dataDir, NAME, "mlb", season, "rosters", `${teamId}.json`);
    if (fileExists(outPath)) continue;

    try {
      const url = `${BASE}/teams/${teamId}/roster?season=${season}&rosterType=fullSeason`;
      const data = await fetchJSON(url, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten++;

      if (filesWritten % 10 === 0) {
        logger.progress(NAME, "mlb", "rosters", `${season}: ${filesWritten} team rosters written so far`);
      }

      await sleep(200);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn(`Failed roster ${teamId}: ${msg}`, NAME);
      errors.push(`rosters/${teamId}: ${msg}`);
    }
  }

  logger.progress(NAME, "mlb", "rosters", `${season}: wrote ${filesWritten} roster files`);
  return { filesWritten, errors };
}

async function importPeople(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const rosterDir = rawPath(dataDir, NAME, "mlb", season, "rosters");
  const fs = await import("node:fs");
  const path = await import("node:path");

  if (!fs.existsSync(rosterDir)) {
    const msg = `No roster directory for ${season} — run "rosters" endpoint first`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }

  const playerIds = new Set<number>();
  const rosterFiles = fs.readdirSync(rosterDir).filter((f) => f.endsWith(".json"));
  for (const file of rosterFiles) {
    const payload = readJSON<{ roster?: Array<{ person?: { id?: number } }> }>(path.join(rosterDir, file));
    for (const row of payload?.roster ?? []) {
      const pid = row.person?.id;
      if (typeof pid === "number") playerIds.add(pid);
    }
  }

  if (dryRun) {
    logger.info(`[dry-run] Would fetch ${playerIds.size} player profiles for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  let filesWritten = 0;
  const errors: string[] = [];
  for (const playerId of playerIds) {
    const outPath = rawPath(dataDir, NAME, "mlb", season, "people", `${playerId}.json`);
    if (fileExists(outPath)) continue;

    try {
      const url = `${BASE}/people/${playerId}`;
      const data = await fetchJSON(url, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten++;

      if (filesWritten % 100 === 0) {
        logger.progress(NAME, "mlb", "people", `${season}: ${filesWritten} player files written so far`);
      }

      await sleep(200);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn(`Failed player ${playerId}: ${msg}`, NAME);
      errors.push(`people/${playerId}: ${msg}`);
    }
  }

  logger.progress(NAME, "mlb", "people", `${season}: wrote ${filesWritten} player files`);
  return { filesWritten, errors };
}

// ── Dispatch ────────────────────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (season: number, dataDir: string, dryRun: boolean) => Promise<{ filesWritten: number; errors: string[] }>> = {
  schedule: importSchedule,
  boxscores: importBoxscores,
  game_feed: importGameFeed,
  teams: importTeams,
  standings: importStandings,
  rosters: importRosters,
  people: importPeople,
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
