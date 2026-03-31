// ──────────────────────────────────────────────────────────
// NHL Official API Provider
// ──────────────────────────────────────────────────────────
// Uses api-web.nhle.com/v1 for schedule, standings, rosters,
// player profiles, game center data, and league leaders.

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "nhl";
const BASE_URL = "https://api-web.nhle.com/v1";
const SPORTS: readonly Sport[] = ["nhl"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 3, perMs: 1_000 };

const ENDPOINTS = [
  "schedule",
  "standings",
  "rosters",
  "players",
  "games",
  "leaders",
  "scores",
] as const;

type Endpoint = (typeof ENDPOINTS)[number];

interface ScheduleGame {
  id: number;
  startTimeUTC: string;
  awayTeam: { abbrev: string };
  homeTeam: { abbrev: string };
}

interface ScheduleDay {
  date: string;
  games: ScheduleGame[];
}

/** NHL season starts in October and ends in June the following year. */
function seasonDateRange(season: number): { start: string; end: string } {
  return {
    start: `${season}-10-01`,
    end: `${season + 1}-06-30`,
  };
}

/** Generate all dates between start and end (YYYY-MM-DD). */
function dateRange(start: string, end: string): string[] {
  const dates: string[] = [];
  const cur = new Date(start + "T00:00:00Z");
  const last = new Date(end + "T00:00:00Z");
  while (cur <= last) {
    dates.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return dates;
}

/** Monthly date sampling for standings (1st of each month in the season). */
function standingsDates(season: number): string[] {
  const dates: string[] = [];
  // Oct–Dec of season year
  for (let m = 10; m <= 12; m++) {
    dates.push(`${season}-${String(m).padStart(2, "0")}-01`);
  }
  // Jan–Jun of season+1 year
  for (let m = 1; m <= 6; m++) {
    dates.push(`${season + 1}-${String(m).padStart(2, "0")}-01`);
  }
  return dates;
}

// Known NHL team abbreviations
const NHL_TEAMS = [
  "ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL",
  "CBJ", "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL",
  "NSH", "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS",
  "SEA", "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WPG", "WSH",
] as const;

// ── Endpoint handlers ──────────────────────────────────────

interface EndpointCtx {
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

async function importSchedule(ctx: EndpointCtx): Promise<EndpointResult> {
  const { start, end } = seasonDateRange(ctx.season);
  const outPath = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "schedule.json");

  if (fileExists(outPath)) {
    logger.info(`Skipping schedule ${ctx.season} — exists`, NAME);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) {
    logger.info(`[dry-run] Would fetch schedule ${ctx.season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const allGames: ScheduleGame[] = [];
  // Weekly sampling across the season to catch all games
  const dates = dateRange(start, end);
  const weeklyDates = dates.filter((_, i) => i % 7 === 0);

  for (const date of weeklyDates) {
    try {
      const data = await fetchJSON<{ gameWeek?: ScheduleDay[] }>(
        `${BASE_URL}/schedule/${date}`,
        NAME,
        RATE_LIMIT,
      );
      if (data.gameWeek) {
        for (const day of data.gameWeek) {
          for (const game of day.games) {
            if (!allGames.some((g) => g.id === game.id)) {
              allGames.push(game);
            }
          }
        }
      }
    } catch (err) {
      logger.warn(`Schedule fetch failed for ${date}: ${err instanceof Error ? err.message : String(err)}`, NAME);
    }
  }

  writeJSON(outPath, allGames);
  logger.progress(NAME, "nhl", "schedule", `${allGames.length} games for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importStandings(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const dates = standingsDates(ctx.season);

  for (const date of dates) {
    const outPath = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "standings", `${date}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;

    try {
      const data = await fetchJSON(`${BASE_URL}/standings/${date}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten++;
    } catch (err) {
      const msg = `standings ${date}: ${err instanceof Error ? err.message : String(err)}`;
      logger.warn(msg, NAME);
      errors.push(msg);
    }
  }

  logger.progress(NAME, "nhl", "standings", `${filesWritten} snapshots for ${ctx.season}`);
  return { filesWritten, errors };
}

async function importRosters(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const team of NHL_TEAMS) {
    const outPath = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "rosters", `${team}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;

    try {
      const data = await fetchJSON(`${BASE_URL}/roster/${team}/current`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten++;
    } catch (err) {
      const msg = `roster ${team}: ${err instanceof Error ? err.message : String(err)}`;
      logger.warn(msg, NAME);
      errors.push(msg);
    }
  }

  logger.progress(NAME, "nhl", "rosters", `${filesWritten} teams for ${ctx.season}`);
  return { filesWritten, errors };
}

async function importPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  // Requires rosters to have been fetched to know player IDs
  // Read schedule to find game participants, then get unique player IDs from rosters
  let filesWritten = 0;
  const errors: string[] = [];

  // Try to collect player IDs from roster files
  const playerIds = new Set<number>();
  for (const team of NHL_TEAMS) {
    const rosterPath = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "rosters", `${team}.json`);
    if (!fileExists(rosterPath)) continue;
    try {
      const { readJSON } = await import("../../core/io.js");
      const roster = readJSON<Array<{ id?: number; playerId?: number }>[]>(rosterPath);
      if (Array.isArray(roster)) {
        for (const section of roster) {
          if (Array.isArray(section)) {
            for (const p of section) {
              const id = p.id ?? p.playerId;
              if (typeof id === "number") playerIds.add(id);
            }
          }
        }
      }
    } catch { /* skip */ }
  }

  if (playerIds.size === 0) {
    logger.info("No player IDs found — run rosters endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  for (const pid of playerIds) {
    const outPath = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "players", `${pid}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;

    try {
      const data = await fetchJSON(`${BASE_URL}/player/${pid}/landing`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten++;
      if (filesWritten % 50 === 0) {
        logger.progress(NAME, "nhl", "players", `${filesWritten}/${playerIds.size}`);
      }
    } catch (err) {
      const msg = `player ${pid}: ${err instanceof Error ? err.message : String(err)}`;
      errors.push(msg);
    }
  }

  logger.progress(NAME, "nhl", "players", `${filesWritten} profiles for ${ctx.season}`);
  return { filesWritten, errors };
}

async function importGames(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  // Read the schedule to get game IDs
  const schedulePath = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "schedule.json");
  if (!fileExists(schedulePath)) {
    logger.info("No schedule found — run schedule endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  const { readJSON } = await import("../../core/io.js");
  const games = readJSON<ScheduleGame[]>(schedulePath) ?? [];

  logger.info(`Fetching details for ${games.length} games in ${ctx.season}`, NAME);

  for (const game of games) {
    const gameDir = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "games");

    // Landing / summary
    const landingPath = `${gameDir}/${game.id}.json`;
    if (!fileExists(landingPath) && !ctx.dryRun) {
      try {
        const data = await fetchJSON(`${BASE_URL}/gamecenter/${game.id}/landing`, NAME, RATE_LIMIT);
        writeJSON(landingPath, data);
        filesWritten++;
      } catch (err) {
        errors.push(`game ${game.id} landing: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    // Box score
    const boxPath = `${gameDir}/${game.id}_boxscore.json`;
    if (!fileExists(boxPath) && !ctx.dryRun) {
      try {
        const data = await fetchJSON(`${BASE_URL}/gamecenter/${game.id}/boxscore`, NAME, RATE_LIMIT);
        writeJSON(boxPath, data);
        filesWritten++;
      } catch (err) {
        errors.push(`game ${game.id} boxscore: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    // Play-by-play
    const pbpPath = `${gameDir}/${game.id}_pbp.json`;
    if (!fileExists(pbpPath) && !ctx.dryRun) {
      try {
        const data = await fetchJSON(`${BASE_URL}/gamecenter/${game.id}/play-by-play`, NAME, RATE_LIMIT);
        writeJSON(pbpPath, data);
        filesWritten++;
      } catch (err) {
        errors.push(`game ${game.id} pbp: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    if (filesWritten % 100 === 0 && filesWritten > 0) {
      logger.progress(NAME, "nhl", "games", `${filesWritten} files written`);
    }
  }

  logger.progress(NAME, "nhl", "games", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

async function importLeaders(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "leaders.json");

  if (fileExists(outPath)) {
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) {
    return { filesWritten: 0, errors: [] };
  }

  try {
    const data = await fetchJSON(`${BASE_URL}/stats/leaders/current`, NAME, RATE_LIMIT);
    writeJSON(outPath, data);
    logger.progress(NAME, "nhl", "leaders", `Saved for ${ctx.season}`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    const msg = `leaders: ${err instanceof Error ? err.message : String(err)}`;
    return { filesWritten: 0, errors: [msg] };
  }
}

async function importScores(ctx: EndpointCtx): Promise<EndpointResult> {
  const { start, end } = seasonDateRange(ctx.season);
  let filesWritten = 0;
  const errors: string[] = [];

  // Sample scores weekly
  const dates = dateRange(start, end);
  const sampleDates = dates.filter((_, i) => i % 7 === 0);

  for (const date of sampleDates) {
    const outPath = rawPath(ctx.dataDir, NAME, "nhl", ctx.season, "scores", `${date}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;

    try {
      const data = await fetchJSON(`${BASE_URL}/score/${date}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten++;
    } catch (err) {
      // Non-game-day 404s are expected
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes("404")) {
        errors.push(`scores ${date}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "scores", `${filesWritten} days for ${ctx.season}`);
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  schedule:   importSchedule,
  standings:  importStandings,
  rosters:    importRosters,
  players:    importPlayers,
  games:      importGames,
  leaders:    importLeaders,
  scores:     importScores,
};

// ── Provider ───────────────────────────────────────────────

const nhl: Provider = {
  name: NAME,
  label: "NHL Official API",
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
      logger.info(`Season ${season}`, NAME);
      for (const ep of activeEndpoints) {
        try {
          const result = await ENDPOINT_FNS[ep]({ season, dataDir: opts.dataDir, dryRun: opts.dryRun });
          totalFiles += result.filesWritten;
          allErrors.push(...result.errors);
        } catch (err) {
          const msg = `${ep} ${season}: ${err instanceof Error ? err.message : String(err)}`;
          logger.error(msg, NAME);
          allErrors.push(msg);
        }
      }
    }

    return {
      provider: NAME,
      sport: "nhl",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default nhl;
