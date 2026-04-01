// ──────────────────────────────────────────────────────────
// V5.0 Ergast / Jolpica Provider
// ──────────────────────────────────────────────────────────
// Fetches historical F1 data from the Jolpica Ergast mirror.
// (The original Ergast API is deprecated; Jolpica provides a
// compatible replacement at api.jolpi.ca.)
// No API key required.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "ergast";
const BASE_URL = "https://api.jolpi.ca/ergast/f1";

const RATE_LIMIT: RateLimitConfig = { requests: 4, perMs: 1_000 };

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

// ── Endpoint implementations ────────────────────────────────

async function importSeasonEndpoint(ctx: EndpointContext, endpoint: Endpoint): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, `${endpoint}.json`);

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, endpoint, `Skipping ${season} — already exists`);
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
      const url = seasonEndpointUrl(season, endpoint, LIMIT) + `&offset=${offset}`;
      const page = await fetchJSON<ErgastResponse>(url, NAME, RATE_LIMIT);
      const total = parseInt(page?.MRData?.total ?? "0", 10);

      if (!merged) {
        merged = page;
      } else {
        // Merge race arrays from paginated results
        const table = page?.MRData?.RaceTable;
        const mergedTable = merged?.MRData?.RaceTable;
        if (table?.Races && mergedTable?.Races) {
          mergedTable.Races.push(...table.Races);
        }
      }

      offset += LIMIT;
      if (offset >= total || total === 0) break;
      logger.progress(NAME, sport, endpoint, `${season} ${endpoint} page ${Math.ceil(offset / LIMIT)} (${offset}/${total})`);
    }

    if (merged) {
      writeJSON(outFile, merged);
      filesWritten++;
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

  const outFile = rawPath(dataDir, NAME, sport, season, endpoint, `round_${round}.json`);

  if (fileExists(outFile)) {
    return { filesWritten, errors };
  }

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const LIMIT = 100;
    let offset = 0;
    let merged: ErgastResponse | null = null;

    for (;;) {
      const url = roundEndpointPageUrl(season, round, endpoint, LIMIT, offset);
      const page = await fetchJSON<ErgastResponse>(url, NAME, RATE_LIMIT);
      const total = parseInt(page?.MRData?.total ?? "0", 10);

      if (!merged) {
        merged = page;
      } else {
        mergeRoundPage(merged, page, endpoint);
      }

      offset += LIMIT;
      if (offset >= total || total === 0) break;
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

/** Determine the number of rounds from the results endpoint */
async function getRoundCount(ctx: EndpointContext): Promise<number> {
  const { sport, season, dataDir } = ctx;

  // Try to read from already-saved results file
  const resultsFile = rawPath(dataDir, NAME, sport, season, "results.json");
  try {
    const fs = await import("node:fs");
    if (fs.existsSync(resultsFile)) {
      const data = JSON.parse(fs.readFileSync(resultsFile, "utf-8")) as ErgastResponse;
      return data.MRData.RaceTable?.Races.length ?? 0;
    }
  } catch {
    // Fall through to fetch
  }

  // Fetch results to determine round count
  const url = seasonEndpointUrl(season, "results");
  try {
    const data = await fetchJSON<ErgastResponse>(url, NAME, RATE_LIMIT);
    return data.MRData.RaceTable?.Races.length ?? 0;
  } catch {
    return 0;
  }
}

async function importPerRoundEndpoints(ctx: EndpointContext, endpoints: Endpoint[]): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  const roundCount = await getRoundCount(ctx);

  if (roundCount === 0) {
    logger.info(`No rounds found for ${ctx.season} — skipping per-round data`, NAME);
    return { filesWritten, errors };
  }

  logger.progress(NAME, ctx.sport, "rounds", `${ctx.season}: ${roundCount} rounds`);

  for (let round = 1; round <= roundCount; round++) {
    for (const ep of endpoints) {
      try {
        const result = await importRoundEndpoint(ctx, ep, round);
        filesWritten += result.filesWritten;
        errors.push(...result.errors);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`${ep}/${ctx.season}/${round}: ${msg}`);
      }
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
