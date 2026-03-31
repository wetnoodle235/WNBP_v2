// ──────────────────────────────────────────────────────────
// V5.0 OpenF1 Provider
// ──────────────────────────────────────────────────────────
// Fetches Formula 1 data from the OpenF1 public API.
// Provides session-level data: laps, positions, weather,
// pit stops, stints, race control, intervals, and drivers.
// No API key required.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "openf1";
const BASE_URL = "https://api.openf1.org/v1";

const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["f1"];

const ALL_ENDPOINTS = [
  "sessions",
  "drivers",
  "laps",
  "car_data",
  "position",
  "weather",
  "race_control",
  "stints",
  "pit",
  "intervals",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

/** Endpoints to fetch per-session (excluding sessions itself and car_data) */
const SESSION_ENDPOINTS: Endpoint[] = [
  "drivers",
  "laps",
  "position",
  "weather",
  "race_control",
  "stints",
  "pit",
  "intervals",
];

// ── Types ───────────────────────────────────────────────────

interface OpenF1Session {
  session_key: number;
  session_name: string;
  session_type: string;
  date_start: string;
  date_end: string;
  circuit_short_name: string;
  country_name: string;
  year: number;
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

// ── Helper ──────────────────────────────────────────────────

function isCurrentYear(year: number): boolean {
  return year >= new Date().getFullYear();
}

// ── Endpoint implementations ────────────────────────────────

async function importSessions(ctx: EndpointContext): Promise<{ result: EndpointResult; sessions: OpenF1Session[] }> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "sessions.json");

  // Always re-fetch sessions to get the latest list, but return cached if exists
  if (fileExists(outFile) && !isCurrentYear(season)) {
    logger.progress(NAME, sport, "sessions", `Skipping ${season} — already exists`);
    // Still need to return sessions for downstream endpoints
    try {
      const fs = await import("node:fs");
      const cached = JSON.parse(fs.readFileSync(outFile, "utf-8")) as OpenF1Session[];
      return { result: { filesWritten, errors }, sessions: cached };
    } catch {
      // Fall through to re-fetch
    }
  }

  const url = `${BASE_URL}/sessions?year=${season}`;
  logger.progress(NAME, sport, "sessions", `Fetching ${season} sessions`);

  if (dryRun) return { result: { filesWritten: 0, errors: [] }, sessions: [] };

  try {
    const sessions = await fetchJSON<OpenF1Session[]>(url, NAME, RATE_LIMIT);
    writeJSON(outFile, sessions);
    filesWritten++;
    logger.progress(NAME, sport, "sessions", `Saved ${sessions.length} sessions for ${season}`);
    return { result: { filesWritten, errors }, sessions };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`sessions ${season}: ${msg}`, NAME);
    errors.push(`sessions/${season}: ${msg}`);
    return { result: { filesWritten, errors }, sessions: [] };
  }
}

async function importSessionEndpoint(
  ctx: EndpointContext,
  session: OpenF1Session,
  endpoint: Endpoint,
): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const sessionKey = session.session_key;
  const outFile = rawPath(dataDir, NAME, sport, season, String(sessionKey), `${endpoint}.json`);

  if (fileExists(outFile)) {
    return { filesWritten, errors };
  }

  // Skip car_data for historical years — too large
  if (endpoint === "car_data" && !isCurrentYear(season)) {
    logger.progress(NAME, sport, endpoint, `Skipping car_data for historical session ${sessionKey}`);
    return { filesWritten, errors };
  }

  if (dryRun) return { filesWritten: 0, errors: [] };

  const url = `${BASE_URL}/${endpoint}?session_key=${sessionKey}`;

  try {
    const data = await fetchJSON<unknown[]>(url, NAME, RATE_LIMIT, { timeoutMs: 60_000 });
    writeJSON(outFile, data);
    filesWritten++;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`${endpoint} session ${sessionKey}: ${msg}`, NAME);
    errors.push(`${endpoint}/${season}/${sessionKey}: ${msg}`);
  }

  return { filesWritten, errors };
}

// ── Provider implementation ─────────────────────────────────

const openf1: Provider = {
  name: NAME,
  label: "OpenF1",
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

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── F1 ${season} ──`, NAME);

        // Step 1: Fetch sessions for this year
        const { result: sessionsResult, sessions } = await importSessions({
          sport,
          season,
          dataDir: opts.dataDir,
          dryRun: opts.dryRun,
        });
        totalFiles += sessionsResult.filesWritten;
        allErrors.push(...sessionsResult.errors);

        if (sessions.length === 0) {
          logger.info(`No sessions found for ${season}`, NAME);
          continue;
        }

        // Determine which per-session endpoints to fetch
        const sessionEps = endpoints.filter(
          (ep): ep is Endpoint => ep !== "sessions" && SESSION_ENDPOINTS.includes(ep),
        );

        // Also include car_data if requested
        if (endpoints.includes("car_data")) {
          sessionEps.push("car_data");
        }

        if (sessionEps.length === 0) continue;

        // Step 2: For each session, fetch all requested endpoints
        const raceAndQualiSessions = sessions.filter(
          (s) => s.session_type === "Race" || s.session_type === "Qualifying" || s.session_type === "Sprint",
        );

        const targetSessions = raceAndQualiSessions.length > 0 ? raceAndQualiSessions : sessions;
        logger.progress(NAME, sport, "sessions", `Processing ${targetSessions.length} sessions`);

        for (const session of targetSessions) {
          const label = `${session.circuit_short_name} ${session.session_name}`;
          logger.progress(NAME, sport, "session", `${label} (key=${session.session_key})`);

          for (const ep of sessionEps) {
            try {
              const ctx: EndpointContext = {
                sport,
                season,
                dataDir: opts.dataDir,
                dryRun: opts.dryRun,
              };
              const result = await importSessionEndpoint(ctx, session, ep);
              totalFiles += result.filesWritten;
              allErrors.push(...result.errors);
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              logger.error(`${sport}/${season}/${session.session_key}/${ep}: ${msg}`, NAME);
              allErrors.push(`${sport}/${season}/${session.session_key}/${ep}: ${msg}`);
            }
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

export default openf1;
