// ──────────────────────────────────────────────────────────
// V5.0 Ergast / Jolpica Provider
// ──────────────────────────────────────────────────────────
// Fetches historical F1 data from the Jolpica Ergast mirror.
// (The original Ergast API is deprecated; Jolpica provides a
// compatible replacement at api.jolpi.ca.)
// No API key required.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { writeJSON, rawPath, rawPathWithRound, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import fs from "node:fs";
import path from "node:path";

// ── Constants ───────────────────────────────────────────────

const NAME = "ergast";
const BASE_URL = "https://api.jolpi.ca/ergast/f1";

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 6_000 };

const ROUND_FETCH_OPTS = {
  retries: 10,
  retryDelayMs: 2_500,
  timeoutMs: 45_000,
};

const ROUND_REQUEST_PAUSE_MS = 2_500;
const ROUND_BETWEEN_ROUNDS_PAUSE_MS = 3_500;

const SUPPORTED_SPORTS: Sport[] = ["f1"];

const ALL_ENDPOINTS = [
  "races",
  "results",
  "qualifying",
  "sprint",
  "driver_standings",
  "constructor_standings",
  "circuits",
  "drivers",
  "constructors",
  "laps",
  "pitstops",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

/** Endpoints that are fetched per-round rather than per-season */
const PER_ROUND_ENDPOINTS: Endpoint[] = ["laps", "pitstops"];

const ROUND_SCOPED_SEASON_ENDPOINTS = new Set<Endpoint>(["races", "results", "qualifying", "sprint"]);
const REFERENCE_ENDPOINTS = new Set<Endpoint>(["circuits", "drivers", "constructors"]);
const STANDINGS_ENDPOINTS = new Set<Endpoint>(["driver_standings", "constructor_standings"]);

const ROUND_FILE_NAMES: Record<Endpoint, string> = {
  races: "race.json",
  results: "results.json",
  qualifying: "qualifying.json",
  sprint: "sprint.json",
  driver_standings: "driver_standings.json",
  constructor_standings: "constructor_standings.json",
  circuits: "circuits.json",
  drivers: "drivers.json",
  constructors: "constructors.json",
  laps: "laps.json",
  pitstops: "pitstops.json",
};

// ── Types ───────────────────────────────────────────────────

interface ErgastResponse {
  MRData: {
    xmlns: string;
    series: string;
    url: string;
    limit: string;
    offset: string;
    total: string;
    RaceTable?: {
      season: string;
      Races: ErgastRace[];
    };
    StandingsTable?: unknown;
    CircuitTable?: unknown;
    DriverTable?: unknown;
    ConstructorTable?: unknown;
    [key: string]: unknown;
  };
}

interface ErgastRace {
  season: string;
  round: string;
  raceName: string;
  [key: string]: unknown;
}

const RACE_RESULT_KEYS: Record<string, string> = {
  results: "Results",
  qualifying: "QualifyingResults",
  sprint: "SprintResults",
};

// ── Endpoint context ────────────────────────────────────────

interface EndpointContext {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

// ── URL builders ────────────────────────────────────────────

function seasonEndpointUrl(season: number, endpoint: string, limit = 1000): string {
  switch (endpoint) {
    case "races":
      return `${BASE_URL}/${season}/races.json?limit=${limit}`;
    case "results":
      return `${BASE_URL}/${season}/results.json?limit=${limit}`;
    case "qualifying":
      return `${BASE_URL}/${season}/qualifying.json?limit=${limit}`;
    case "sprint":
      return `${BASE_URL}/${season}/sprint.json?limit=${limit}`;
    case "driver_standings":
      return `${BASE_URL}/${season}/driverStandings.json`;
    case "constructor_standings":
      return `${BASE_URL}/${season}/constructorStandings.json`;
    case "circuits":
      return `${BASE_URL}/${season}/circuits.json`;
    case "drivers":
      return `${BASE_URL}/${season}/drivers.json`;
    case "constructors":
      return `${BASE_URL}/${season}/constructors.json`;
    default:
      throw new Error(`Unknown season endpoint: ${endpoint}`);
  }
}

function withOffset(url: string, offset: number): string {
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}offset=${offset}`;
}

function roundEndpointUrl(season: number, round: number, endpoint: string): string {
  switch (endpoint) {
    case "laps":
      return `${BASE_URL}/${season}/${round}/laps.json?limit=100&offset=0`;
    case "pitstops":
      return `${BASE_URL}/${season}/${round}/pitstops.json?limit=100&offset=0`;
    default:
      throw new Error(`Unknown round endpoint: ${endpoint}`);
  }
}

function roundEndpointPageUrl(
  season: number,
  round: number,
  endpoint: string,
  limit: number,
  offset: number,
): string {
  switch (endpoint) {
    case "laps":
      return `${BASE_URL}/${season}/${round}/laps.json?limit=${limit}&offset=${offset}`;
    case "pitstops":
      return `${BASE_URL}/${season}/${round}/pitstops.json?limit=${limit}&offset=${offset}`;
    default:
      throw new Error(`Unknown round endpoint: ${endpoint}`);
  }
}

function roundPageLimit(endpoint: Endpoint): number {
  if (endpoint === "laps") return 2_000;
  if (endpoint === "pitstops") return 200;
  return 100;
}

function mergeRoundPage(target: ErgastResponse, page: ErgastResponse, endpoint: Endpoint): void {
  const targetRace = target?.MRData?.RaceTable?.Races?.[0];
  const pageRace = page?.MRData?.RaceTable?.Races?.[0];
  if (!targetRace || !pageRace) return;

  if (endpoint === "laps") {
    const targetLaps = Array.isArray((targetRace as Record<string, unknown>).Laps)
      ? ((targetRace as Record<string, unknown>).Laps as unknown[])
      : [];
    const pageLaps = Array.isArray((pageRace as Record<string, unknown>).Laps)
      ? ((pageRace as Record<string, unknown>).Laps as unknown[])
      : [];
    (targetRace as Record<string, unknown>).Laps = [...targetLaps, ...pageLaps];
    return;
  }

  if (endpoint === "pitstops") {
    const targetStops = Array.isArray((targetRace as Record<string, unknown>).PitStops)
      ? ((targetRace as Record<string, unknown>).PitStops as unknown[])
      : [];
    const pageStops = Array.isArray((pageRace as Record<string, unknown>).PitStops)
      ? ((pageRace as Record<string, unknown>).PitStops as unknown[])
      : [];
    (targetRace as Record<string, unknown>).PitStops = [...targetStops, ...pageStops];
  }
}

function mergeSeasonRacePage(target: ErgastResponse, page: ErgastResponse, endpoint: Endpoint): void {
  const targetTable = target?.MRData?.RaceTable;
  const pageTable = page?.MRData?.RaceTable;
  if (!targetTable || !pageTable) return;

  const resultKey = RACE_RESULT_KEYS[endpoint];
  const racesByRound = new Map<string, Record<string, unknown>>();

  for (const race of targetTable.Races) {
    const round = String((race as Record<string, unknown>).round ?? "");
    if (round) {
      racesByRound.set(round, race as Record<string, unknown>);
    }
  }

  for (const race of pageTable.Races) {
    const raceObj = race as Record<string, unknown>;
    const round = String(raceObj.round ?? "");
    if (!round) {
      targetTable.Races.push(race);
      continue;
    }

    const existing = racesByRound.get(round);
    if (!existing) {
      targetTable.Races.push(race);
      racesByRound.set(round, raceObj);
      continue;
    }

    for (const [key, value] of Object.entries(raceObj)) {
      if (value !== undefined && value !== null && existing[key] === undefined) {
        existing[key] = value;
      }
    }

    if (resultKey) {
      const existingRows = Array.isArray(existing[resultKey]) ? (existing[resultKey] as unknown[]) : [];
      const pageRows = Array.isArray(raceObj[resultKey]) ? (raceObj[resultKey] as unknown[]) : [];
      if (pageRows.length > 0) {
        existing[resultKey] = [...existingRows, ...pageRows];
      }
    }
  }
}

function seasonEndpointPath(ctx: EndpointContext, endpoint: Endpoint): string {
  const { sport, season, dataDir } = ctx;
  if (REFERENCE_ENDPOINTS.has(endpoint)) {
    return rawPath(dataDir, NAME, sport, season, "reference", ROUND_FILE_NAMES[endpoint]);
  }
  if (STANDINGS_ENDPOINTS.has(endpoint)) {
    return rawPath(dataDir, NAME, sport, season, "standings", ROUND_FILE_NAMES[endpoint]);
  }
  return rawPath(dataDir, NAME, sport, season, ROUND_FILE_NAMES[endpoint]);
}

function roundEndpointPath(ctx: EndpointContext, endpoint: Endpoint, round: number | string): string {
  return rawPathWithRound(ctx.dataDir, NAME, ctx.sport, ctx.season, round, ROUND_FILE_NAMES[endpoint]);
}

function loadRoundScopedSeasonEndpoint(ctx: EndpointContext, endpoint: Endpoint): ErgastResponse | null {
  const roundsDir = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "rounds");
  if (!fs.existsSync(roundsDir)) return null;

  const fileName = ROUND_FILE_NAMES[endpoint];
  let merged: ErgastResponse | null = null;
  const roundDirs = fs.readdirSync(roundsDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort();

  for (const roundDir of roundDirs) {
    const filePath = path.join(roundsDir, roundDir, fileName);
    if (!fs.existsSync(filePath)) continue;
    const page = JSON.parse(fs.readFileSync(filePath, "utf-8")) as ErgastResponse;
    if (!merged) {
      merged = page;
    } else {
      mergeSeasonRacePage(merged, page, endpoint);
    }
  }

  return merged;
}

function roundNumbersForEndpoint(ctx: EndpointContext, endpoint: Endpoint): number[] {
  const roundsDir = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "rounds");
  if (!fs.existsSync(roundsDir)) return [];

  const fileName = ROUND_FILE_NAMES[endpoint];
  const roundNums = fs.readdirSync(roundsDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const match = entry.name.match(/^round_(\d+)/);
      if (!match) return null;
      const round = Number.parseInt(match[1], 10);
      const filePath = path.join(roundsDir, entry.name, fileName);
      if (!fs.existsSync(filePath)) return null;
      return Number.isFinite(round) && round > 0 ? round : null;
    })
    .filter((round): round is number => round !== null);

  return Array.from(new Set(roundNums)).sort((a, b) => a - b);
}

function shouldFetchStandingsEndpoint(ctx: EndpointContext, endpoint: Endpoint): boolean {
  const outFile = seasonEndpointPath(ctx, endpoint);
  if (!fileExists(outFile)) return true;

  const racesData = loadRoundScopedSeasonEndpoint(ctx, "races");
  const races = racesData?.MRData?.RaceTable?.Races ?? [];
  const todayIso = new Date().toISOString().slice(0, 10);
  return races.some((race) => String((race as Record<string, unknown>).date ?? "") >= todayIso);
}

function shouldFetchRaceEndpoint(ctx: EndpointContext, endpoint: Endpoint): boolean {
  if (endpoint === "races") {
    return true;
  }

  const racesData = loadRoundScopedSeasonEndpoint(ctx, "races")
    ?? (() => {
      const legacy = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "races.json");
      if (!fs.existsSync(legacy)) return null;
      return JSON.parse(fs.readFileSync(legacy, "utf-8")) as ErgastResponse;
    })();
  const races = racesData?.MRData?.RaceTable?.Races ?? [];
  if (races.length === 0) {
    return true;
  }

  const todayIso = new Date().toISOString().slice(0, 10);
  const existingRounds = new Set(roundNumbersForEndpoint(ctx, endpoint));
  const expectedRounds = races
    .filter((race) => {
      const raceObj = race as Record<string, unknown>;
      const raceDate = String(raceObj.date ?? "");
      if (endpoint === "results") {
        return !!raceDate && raceDate <= todayIso;
      }
      if (endpoint === "qualifying") {
        const qualifying = raceObj.Qualifying as Record<string, unknown> | undefined;
        const sessionDate = String(qualifying?.date ?? raceDate ?? "");
        return !!sessionDate && sessionDate <= todayIso;
      }
      if (endpoint === "sprint") {
        const sprint = raceObj.Sprint as Record<string, unknown> | undefined;
        const sessionDate = String(sprint?.date ?? "");
        return !!sessionDate && sessionDate <= todayIso;
      }
      return true;
    })
    .map((race) => Number((race as Record<string, unknown>).round ?? 0))
    .filter((round) => Number.isFinite(round) && round > 0);

  if (expectedRounds.length === 0) {
    return false;
  }

  return expectedRounds.some((round) => !existingRounds.has(round));
}

function writeRoundScopedSeasonEndpoint(
  ctx: EndpointContext,
  endpoint: Endpoint,
  data: ErgastResponse,
): number {
  const races = data?.MRData?.RaceTable?.Races ?? [];
  let filesWritten = 0;

  for (const race of races) {
    const raceObj = race as Record<string, unknown>;
    const round = Number.parseInt(String(raceObj.round ?? ""), 10);
    if (!Number.isFinite(round) || round <= 0) continue;

    const outFile = roundEndpointPath(ctx, endpoint, round);
    const raceTable = {
      ...(data.MRData.RaceTable ?? { season: String(ctx.season) }),
      round: String(round),
      Races: [race],
    };
    const payload: ErgastResponse = {
      MRData: {
        ...data.MRData,
        total: "1",
        RaceTable: raceTable,
      },
    };

    writeJSON(outFile, payload);
    filesWritten++;
  }

  return filesWritten;
}

// ── Endpoint implementations ────────────────────────────────

async function importSeasonEndpoint(ctx: EndpointContext, endpoint: Endpoint): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = seasonEndpointPath(ctx, endpoint);

  if (!ROUND_SCOPED_SEASON_ENDPOINTS.has(endpoint)) {
    if (REFERENCE_ENDPOINTS.has(endpoint) && fileExists(outFile)) {
      logger.progress(NAME, sport, endpoint, `Skipping ${season} — already exists`);
      return { filesWritten, errors };
    }
    if (STANDINGS_ENDPOINTS.has(endpoint) && !shouldFetchStandingsEndpoint(ctx, endpoint)) {
      logger.progress(NAME, sport, endpoint, `Skipping ${season} — up to date`);
      return { filesWritten, errors };
    }
  } else if (!shouldFetchRaceEndpoint(ctx, endpoint)) {
    logger.progress(NAME, sport, endpoint, `Skipping ${season} — round files up to date`);
    return { filesWritten, errors };
  }

  logger.progress(NAME, sport, endpoint, `Fetching ${season} ${endpoint}`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    // Paginate: Jolpica API caps at 100 results per page
    const LIMIT = 100;
    let offset = 0;
    let merged: ErgastResponse | null = null;

    for (;;) {
      const url = withOffset(seasonEndpointUrl(season, endpoint, LIMIT), offset);
      const page = await fetchJSON<ErgastResponse>(url, NAME, RATE_LIMIT);
      const total = parseInt(page?.MRData?.total ?? "0", 10);

      if (!merged) {
        merged = page;
      } else {
        mergeSeasonRacePage(merged, page, endpoint);
      }

      offset += LIMIT;
      if (offset >= total || total === 0) break;
      logger.progress(NAME, sport, endpoint, `${season} ${endpoint} page ${Math.ceil(offset / LIMIT)} (${offset}/${total})`);
    }

    if (merged) {
      if (ROUND_SCOPED_SEASON_ENDPOINTS.has(endpoint)) {
        filesWritten += writeRoundScopedSeasonEndpoint(ctx, endpoint, merged);
      } else {
        writeJSON(outFile, merged);
        filesWritten++;
      }
      const total = merged?.MRData?.total ?? "?";
      logger.progress(NAME, sport, endpoint, `Saved ${season} ${endpoint} (${total} results)`);
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`${endpoint} ${season}: ${msg}`, NAME);
    errors.push(`${endpoint}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importRoundEndpoint(
  ctx: EndpointContext,
  endpoint: Endpoint,
  round: number,
): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = roundEndpointPath(ctx, endpoint, round);

  if (fileExists(outFile)) {
    return { filesWritten, errors };
  }

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const LIMIT = roundPageLimit(endpoint);
    let offset = 0;
    let merged: ErgastResponse | null = null;

    for (;;) {
      const url = roundEndpointPageUrl(season, round, endpoint, LIMIT, offset);
      const page = await fetchJSON<ErgastResponse>(url, NAME, RATE_LIMIT, ROUND_FETCH_OPTS);
      const total = parseInt(page?.MRData?.total ?? "0", 10);

      if (!merged) {
        merged = page;
      } else {
        mergeRoundPage(merged, page, endpoint);
      }

      offset += LIMIT;
      if (offset >= total || total === 0) break;
      await sleep(ROUND_REQUEST_PAUSE_MS);
    }

    if (!merged) {
      return { filesWritten, errors };
    }

    writeJSON(outFile, merged);
    filesWritten++;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    // Laps/pitstops may not exist for all rounds (cancelled races, etc.)
    logger.warn(`${endpoint} ${season}/round ${round}: ${msg}`, NAME);
    errors.push(`${endpoint}/${season}/${round}: ${msg}`);
  }

  return { filesWritten, errors };
}

/** Determine which rounds should be fetched for per-round endpoints. */
async function getRoundsToFetch(ctx: EndpointContext): Promise<number[]> {
  const { sport, season, dataDir } = ctx;
  const todayIso = new Date().toISOString().slice(0, 10);

  const parseRounds = (data: ErgastResponse | null | undefined): number[] => {
    const races = data?.MRData?.RaceTable?.Races ?? [];
    const rounds: number[] = [];
    for (const race of races) {
      const roundNum = Number((race as Record<string, unknown>).round ?? 0);
      if (!Number.isFinite(roundNum) || roundNum <= 0) continue;

      const raceDate = String((race as Record<string, unknown>).date ?? "");
      const isCompletedByDate = raceDate ? raceDate <= todayIso : true;

      // If a race has result rows, it is complete regardless of date formatting.
      const hasResults = Array.isArray((race as Record<string, unknown>).Results)
        && (((race as Record<string, unknown>).Results as unknown[]).length > 0);

      if (hasResults || isCompletedByDate) {
        rounds.push(roundNum);
      }
    }

    if (rounds.length === 0) {
      return races
        .map((r) => Number((r as Record<string, unknown>).round ?? 0))
        .filter((n) => Number.isFinite(n) && n > 0);
    }

    return rounds;
  };

  // Try to read from already-saved results file
  const savedResults = loadRoundScopedSeasonEndpoint(ctx, "results");
  if (savedResults) {
    const rounds = parseRounds(savedResults);
    if (rounds.length > 0) {
      return Array.from(new Set(rounds)).sort((a, b) => a - b);
    }
  }

  const resultsFile = rawPath(dataDir, NAME, sport, season, "results.json");
  try {
    if (fs.existsSync(resultsFile)) {
      const data = JSON.parse(fs.readFileSync(resultsFile, "utf-8")) as ErgastResponse;
      const rounds = parseRounds(data);
      return Array.from(new Set(rounds)).sort((a, b) => a - b);
    }
  } catch {
    // Fall through to other sources
  }

  const savedRaces = loadRoundScopedSeasonEndpoint(ctx, "races");
  if (savedRaces) {
    const rounds = parseRounds(savedRaces);
    if (rounds.length > 0) {
      return Array.from(new Set(rounds)).sort((a, b) => a - b);
    }
  }

  const racesFile = rawPath(dataDir, NAME, sport, season, "races.json");
  try {
    if (fs.existsSync(racesFile)) {
      const data = JSON.parse(fs.readFileSync(racesFile, "utf-8")) as ErgastResponse;
      const rounds = parseRounds(data);
      return Array.from(new Set(rounds)).sort((a, b) => a - b);
    }
  } catch {
    // Fall through to fetch
  }

  // Fetch results to determine rounds.
  const url = seasonEndpointUrl(season, "results");
  try {
    const data = await fetchJSON<ErgastResponse>(url, NAME, RATE_LIMIT);
    const rounds = parseRounds(data);
    return Array.from(new Set(rounds)).sort((a, b) => a - b);
  } catch {
    return [];
  }
}

async function importPerRoundEndpoints(ctx: EndpointContext, endpoints: Endpoint[]): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  const rounds = await getRoundsToFetch(ctx);

  if (rounds.length === 0) {
    logger.info(`No rounds found for ${ctx.season} — skipping per-round data`, NAME);
    return { filesWritten, errors };
  }

  logger.progress(NAME, ctx.sport, "rounds", `${ctx.season}: ${rounds.length} rounds`);

  for (const round of rounds) {
    let touchedRound = false;
    for (const ep of endpoints) {
      try {
        const result = await importRoundEndpoint(ctx, ep, round);
        filesWritten += result.filesWritten;
        errors.push(...result.errors);
        if (result.filesWritten > 0 || result.errors.length > 0) {
          touchedRound = true;
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`${ep}/${ctx.season}/${round}: ${msg}`);
        touchedRound = true;
      }
    }
    if (touchedRound) {
      await sleep(ROUND_BETWEEN_ROUNDS_PAUSE_MS);
    }
  }

  return { filesWritten, errors };
}

// ── Provider implementation ─────────────────────────────────

const ergast: Provider = {
  name: NAME,
  label: "Ergast / Jolpica (F1)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: [...ALL_ENDPOINTS],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const sports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    const endpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) => ALL_ENDPOINTS.includes(e as Endpoint)) as Endpoint[])
      : [...ALL_ENDPOINTS];

    logger.info(
      `Starting import — ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    // Split into season-level and per-round endpoints
    const seasonEndpoints = endpoints.filter((ep) => !PER_ROUND_ENDPOINTS.includes(ep));
    const roundEndpoints = endpoints.filter((ep) => PER_ROUND_ENDPOINTS.includes(ep));

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── F1 ${season} ──`, NAME);

        const ctx: EndpointContext = {
          sport,
          season,
          dataDir: opts.dataDir,
          dryRun: opts.dryRun,
        };

        // Import season-level endpoints first (results needed for round count)
        for (const ep of seasonEndpoints) {
          try {
            const result = await importSeasonEndpoint(ctx, ep);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${season}/${ep}: ${msg}`, NAME);
            allErrors.push(`${sport}/${season}/${ep}: ${msg}`);
          }
        }

        // Import per-round endpoints
        if (roundEndpoints.length > 0) {
          try {
            const result = await importPerRoundEndpoints(ctx, roundEndpoints);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${season}/per-round: ${msg}`, NAME);
            allErrors.push(`${sport}/${season}/per-round: ${msg}`);
          }
        }
      }
    }

    const durationMs = Date.now() - start;
    logger.summary(NAME, totalFiles, allErrors.length, durationMs);

    return {
      provider: NAME,
      sport: "f1",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs,
    };
  },
};

export default ergast;
