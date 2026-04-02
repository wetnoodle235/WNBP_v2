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

const RATE_LIMIT: RateLimitConfig = { requests: 3, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["f1"];

const ALL_ENDPOINTS = [
  "meetings",
  "sessions",
  "drivers",
  "laps",
  "location",
  "car_data",
  "position",
  "weather",
  "race_control",
  "stints",
  "pit",
  "intervals",
  "team_radio",
  "overtakes",
  "session_result",
  "starting_grid",
  "championship_drivers",
  "championship_teams",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

const DEFAULT_ENDPOINTS: Endpoint[] = [
  "meetings",
  "sessions",
  "drivers",
  "laps",
  "position",
  "weather",
  "race_control",
  "stints",
  "pit",
  "intervals",
  "team_radio",
  "overtakes",
  "session_result",
  "starting_grid",
  "championship_drivers",
  "championship_teams",
];

/** Endpoints to fetch per-session (excluding sessions itself and car_data) */
const SESSION_ENDPOINTS: Endpoint[] = [
  "drivers",
  "laps",
  "location",
  "position",
  "weather",
  "race_control",
  "stints",
  "pit",
  "intervals",
  "team_radio",
  "overtakes",
  "session_result",
  "starting_grid",
  "championship_drivers",
  "championship_teams",
];

const RACE_ONLY_ENDPOINTS = new Set<Endpoint>([
  "intervals",
  "pit",
  "overtakes",
  "starting_grid",
  "championship_drivers",
  "championship_teams",
]);

const HIGH_VOLUME_ENDPOINTS = new Set<Endpoint>(["car_data", "location"]);

// ── Types ───────────────────────────────────────────────────

interface OpenF1Session {
  session_key: number;
  meeting_key: number;
  session_name: string;
  session_type: string;
  date_start: string;
  date_end: string;
  circuit_short_name: string;
  country_name: string;
  year: number;
  [key: string]: unknown;
}

interface OpenF1Meeting {
  meeting_key: number;
  meeting_name?: string;
  meeting_official_name?: string;
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

interface OpenF1SeasonContext extends EndpointContext {
  meetingsByKey: Map<number, OpenF1Meeting>;
}

// ── Helper ──────────────────────────────────────────────────

function isCurrentYear(year: number): boolean {
  return year >= new Date().getFullYear();
}

function meetingPhase(meeting: OpenF1Meeting | undefined, session?: OpenF1Session): string {
  const tokens = [meeting?.meeting_name, meeting?.meeting_official_name, session?.session_name, session?.session_type]
    .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
    .join(" ")
    .toLowerCase();

  return tokens.includes("test") || tokens.includes("day 1") || tokens.includes("day 2") || tokens.includes("day 3")
    ? "testing"
    : "championship";
}

function meetingDir(dataDir: string, sport: Sport, season: number, meetingKey: number, phase: string): string {
  return rawPath(dataDir, NAME, sport, season, "season_phases", phase, "meetings", `meeting_${meetingKey}`);
}

function sessionDir(
  dataDir: string,
  sport: Sport,
  season: number,
  session: OpenF1Session,
  meetingsByKey: Map<number, OpenF1Meeting>,
): string {
  const phase = meetingPhase(meetingsByKey.get(session.meeting_key), session);
  return rawPath(
    dataDir,
    NAME,
    sport,
    season,
    "season_phases",
    phase,
    "meetings",
    `meeting_${session.meeting_key}`,
    "sessions",
    `session_${session.session_key}`,
  );
}

function shouldFetchSessionEndpoint(session: OpenF1Session, endpoint: Endpoint): boolean {
  if (!RACE_ONLY_ENDPOINTS.has(endpoint)) return true;
  return session.session_type === "Race" || session.session_type === "Sprint";
}

// ── Endpoint implementations ────────────────────────────────

async function importMeetings(ctx: EndpointContext): Promise<{ result: EndpointResult; meetings: OpenF1Meeting[] }> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "reference", "meetings.json");

  if (fileExists(outFile) && !isCurrentYear(season)) {
    logger.progress(NAME, sport, "meetings", `Skipping ${season} — already exists`);
    try {
      const fs = await import("node:fs");
      const cached = JSON.parse(fs.readFileSync(outFile, "utf-8")) as OpenF1Meeting[];
      return { result: { filesWritten, errors }, meetings: cached };
    } catch {
      // Fall through to re-fetch
    }
  }

  const url = `${BASE_URL}/meetings?year=${season}`;
  logger.progress(NAME, sport, "meetings", `Fetching ${season} meetings`);

  if (dryRun) return { result: { filesWritten: 0, errors: [] }, meetings: [] };

  try {
    const meetings = await fetchJSON<OpenF1Meeting[]>(url, NAME, RATE_LIMIT);
    writeJSON(outFile, meetings);
    filesWritten++;
    for (const meeting of meetings) {
      const phase = meetingPhase(meeting);
      writeJSON(`${meetingDir(dataDir, sport, season, meeting.meeting_key, phase)}/meeting.json`, meeting);
      filesWritten++;
    }
    logger.progress(NAME, sport, "meetings", `Saved ${meetings.length} meetings for ${season}`);
    return { result: { filesWritten, errors }, meetings };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`meetings ${season}: ${msg}`, NAME);
    errors.push(`meetings/${season}: ${msg}`);
    return { result: { filesWritten, errors }, meetings: [] };
  }
}

async function importSessions(ctx: OpenF1SeasonContext): Promise<{ result: EndpointResult; sessions: OpenF1Session[] }> {
  const { sport, season, dataDir, dryRun, meetingsByKey } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "reference", "sessions.json");

  if (fileExists(outFile) && !isCurrentYear(season)) {
    logger.progress(NAME, sport, "sessions", `Skipping ${season} — already exists`);
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
    for (const session of sessions) {
      writeJSON(`${sessionDir(dataDir, sport, season, session, meetingsByKey)}/session.json`, session);
      filesWritten++;
    }
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
  ctx: OpenF1SeasonContext,
  session: OpenF1Session,
  endpoint: Endpoint,
): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, meetingsByKey } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const sessionKey = session.session_key;
  const outFile = `${sessionDir(dataDir, sport, season, session, meetingsByKey)}/${endpoint}.json`;

  if (fileExists(outFile)) {
    return { filesWritten, errors };
  }

  if (HIGH_VOLUME_ENDPOINTS.has(endpoint) && !isCurrentYear(season)) {
    logger.progress(NAME, sport, endpoint, `Skipping ${endpoint} for historical session ${sessionKey}`);
    return { filesWritten, errors };
  }

  if (!shouldFetchSessionEndpoint(session, endpoint)) {
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
    if (msg.includes("HTTP 404") && msg.includes("No results found")) {
      writeJSON(outFile, []);
      filesWritten++;
      return { filesWritten, errors };
    }
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
      : [...DEFAULT_ENDPOINTS];

    logger.info(
      `Starting import — ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── F1 ${season} ──`, NAME);

        const baseCtx: EndpointContext = {
          sport,
          season,
          dataDir: opts.dataDir,
          dryRun: opts.dryRun,
        };

        const { result: meetingsResult, meetings } = await importMeetings(baseCtx);
        totalFiles += meetingsResult.filesWritten;
        allErrors.push(...meetingsResult.errors);

        const seasonCtx: OpenF1SeasonContext = {
          ...baseCtx,
          meetingsByKey: new Map(meetings.map((meeting) => [meeting.meeting_key, meeting])),
        };

        // Step 1: Fetch sessions for this year
        const { result: sessionsResult, sessions } = await importSessions(seasonCtx);
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
          (s) => {
            const sessionType = String(s.session_type || "");
            return sessionType === "Race"
              || sessionType === "Sprint"
              || sessionType.includes("Qualifying");
          },
        );

        const targetSessions = raceAndQualiSessions.length > 0 ? raceAndQualiSessions : sessions;
        logger.progress(NAME, sport, "sessions", `Processing ${targetSessions.length} sessions`);

        for (const session of targetSessions) {
          const label = `${session.circuit_short_name} ${session.session_name}`;
          logger.progress(NAME, sport, "session", `${label} (key=${session.session_key})`);

          for (const ep of sessionEps) {
            try {
              const result = await importSessionEndpoint(seasonCtx, session, ep);
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
