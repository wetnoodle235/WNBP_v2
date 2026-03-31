// ──────────────────────────────────────────────────────────
// V5.0 NBA Stats Provider
// ──────────────────────────────────────────────────────────
// Fetches player stats, team stats, game logs, shot charts,
// league leaders, and tracking stats from stats.nba.com.
// No API key required — uses browser-like headers.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig, FetchOptions } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── API base ────────────────────────────────────────────────
const BASE_URL = "https://stats.nba.com/stats";

// ── Rate limit: ~1 req per 1.2s ────────────────────────────
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_200 };

// ── Browser-like headers required by stats.nba.com ──────────
const NBA_HEADERS: Record<string, string> = {
  "User-Agent":
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Accept": "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  "Referer": "https://www.nba.com/",
  "Origin": "https://www.nba.com",
  "x-nba-stats-origin": "stats",
  "x-nba-stats-token": "true",
  "Connection": "keep-alive",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-site",
};

const FETCH_OPTS: FetchOptions = {
  headers: NBA_HEADERS,
  timeoutMs: 60_000,
  retries: 3,
  retryDelayMs: 3_000,
};

// ── League IDs ──────────────────────────────────────────────
const LEAGUE_IDS: Record<string, string> = {
  nba: "00",
  wnba: "10",
};

const SUPPORTED_SPORTS: Sport[] = ["nba", "wnba"];

// WNBA has limited endpoint support on stats.nba.com
const WNBA_SUPPORTED_ENDPOINTS = new Set<Endpoint>([
  "league-leaders",
  "player-stats",
  "team-stats",
  "player-game-logs",
  "team-game-logs",
]);

// ── Season types to iterate ─────────────────────────────────
const SEASON_TYPES = ["Regular Season", "Playoffs"] as const;
type SeasonType = (typeof SEASON_TYPES)[number];

// ── Endpoint definitions ────────────────────────────────────
const ALL_ENDPOINTS = [
  "league-leaders",
  "player-stats",
  "team-stats",
  "player-game-logs",
  "team-game-logs",
  "shot-charts",
  "tracking-stats",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── Measure types per endpoint ──────────────────────────────
const PLAYER_STAT_MEASURES = ["Base", "Advanced", "Defense", "Usage", "Misc", "Scoring"] as const;
const TEAM_STAT_MEASURES = ["Base", "Advanced", "Defense"] as const;
const TRACKING_MEASURE_TYPES = [
  "SpeedDistance",
  "Rebounding",
  "Passing",
  "Possessions",
  "CatchShoot",
  "PullUpShot",
  "Defense",
  "Drives",
  "ElbowTouch",
  "PostTouch",
  "PaintTouch",
] as const;

// ── Helpers ─────────────────────────────────────────────────

/** Convert a calendar year to NBA season string: 2023 → "2023-24" */
function toSeasonStr(year: number): string {
  const nextShort = String(year + 1).slice(2);
  return `${year}-${nextShort}`;
}

/** URL-encode season type: "Regular Season" → "Regular%20Season" */
function encodeSeasonType(st: SeasonType): string {
  return st.replace(/ /g, "%20");
}

/**
 * Required default parameters for the leaguedash* endpoints.
 * Without these the API often returns HTTP 500.
 */
const DASH_DEFAULTS =
  "&LastNGames=0&Month=0&OpponentTeamID=0&PORound=0" +
  "&PaceAdjust=N&Period=0&PlusMinus=N&Rank=N&TeamID=0" +
  "&College=&Conference=&Country=&DateFrom=&DateTo=" +
  "&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=" +
  "&Height=&Location=&Outcome=&PlayerExperience=&PlayerPosition=" +
  "&SeasonSegment=&ShotClockRange=&StarterBench=&TwoWay=0" +
  "&VsConference=&VsDivision=&Weight=";

/** Filesystem-safe season type slug: "Regular Season" → "regular-season" */
function seasonTypeSlug(st: SeasonType): string {
  return st.toLowerCase().replace(/ /g, "-");
}

/** Fetch from stats.nba.com with required headers */
async function nbaFetch<T = unknown>(url: string): Promise<T | null> {
  try {
    return await fetchJSON<T>(url, "nbastats", RATE_LIMIT, FETCH_OPTS);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`Fetch failed: ${msg}`, "nbastats");
    return null;
  }
}

// ── Endpoint context / result ───────────────────────────────

interface EndpointContext {
  sport: Sport;
  season: number;
  seasonStr: string;
  leagueId: string;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

function emptyResult(): EndpointResult {
  return { filesWritten: 0, errors: [] };
}

/** Build output path and check for existing file. Returns null if skipped. */
function outPath(
  ctx: EndpointContext,
  seasonType: SeasonType,
  ...segments: string[]
): string {
  const stSlug = seasonTypeSlug(seasonType);
  return rawPath(ctx.dataDir, "nbastats", ctx.sport, ctx.seasonStr, stSlug, ...segments);
}

// ── Endpoint implementations ────────────────────────────────

async function importLeagueLeaders(ctx: EndpointContext): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const st of SEASON_TYPES) {
    const url =
      `${BASE_URL}/leagueleaders?LeagueID=${ctx.leagueId}` +
      `&Season=${ctx.seasonStr}&SeasonType=${encodeSeasonType(st)}` +
      `&PerMode=PerGame&StatCategory=PTS`;

    logger.progress("nbastats", ctx.sport, "league-leaders", `${st} — ${ctx.seasonStr}`);
    if (ctx.dryRun) continue;

    const filePath = outPath(ctx, st, "league-leaders.json");
    if (fileExists(filePath)) {
      logger.progress("nbastats", ctx.sport, "league-leaders", `Skipping (exists) ${st}`);
      continue;
    }

    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/league-leaders: no data`);
      continue;
    }

    writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
    filesWritten++;
  }

  return { filesWritten, errors };
}

async function importPlayerStats(ctx: EndpointContext): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const st of SEASON_TYPES) {
    for (const measure of PLAYER_STAT_MEASURES) {
      const url =
        `${BASE_URL}/leaguedashplayerstats?LeagueID=${ctx.leagueId}` +
        `&Season=${ctx.seasonStr}&SeasonType=${encodeSeasonType(st)}` +
        `&MeasureType=${measure}&PerMode=PerGame` +
        DASH_DEFAULTS;

      const label = `player-stats/${measure}`;
      logger.progress("nbastats", ctx.sport, label, `${st} — ${ctx.seasonStr}`);
      if (ctx.dryRun) continue;

      const filePath = outPath(ctx, st, "player-stats", `${measure.toLowerCase()}.json`);
      if (fileExists(filePath)) {
        logger.progress("nbastats", ctx.sport, label, `Skipping (exists) ${st}`);
        continue;
      }

      const data = await nbaFetch(url);
      if (!data) {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/${label}: no data`);
        continue;
      }

      writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
      filesWritten++;
    }
  }

  return { filesWritten, errors };
}

async function importTeamStats(ctx: EndpointContext): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const st of SEASON_TYPES) {
    for (const measure of TEAM_STAT_MEASURES) {
      const url =
        `${BASE_URL}/leaguedashteamstats?LeagueID=${ctx.leagueId}` +
        `&Season=${ctx.seasonStr}&SeasonType=${encodeSeasonType(st)}` +
        `&MeasureType=${measure}&PerMode=PerGame` +
        DASH_DEFAULTS;

      const label = `team-stats/${measure}`;
      logger.progress("nbastats", ctx.sport, label, `${st} — ${ctx.seasonStr}`);
      if (ctx.dryRun) continue;

      const filePath = outPath(ctx, st, "team-stats", `${measure.toLowerCase()}.json`);
      if (fileExists(filePath)) {
        logger.progress("nbastats", ctx.sport, label, `Skipping (exists) ${st}`);
        continue;
      }

      const data = await nbaFetch(url);
      if (!data) {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/${label}: no data`);
        continue;
      }

      writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
      filesWritten++;
    }
  }

  return { filesWritten, errors };
}

async function importPlayerGameLogs(ctx: EndpointContext): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const st of SEASON_TYPES) {
    const url =
      `${BASE_URL}/playergamelogs?LeagueID=${ctx.leagueId}` +
      `&Season=${ctx.seasonStr}&SeasonType=${encodeSeasonType(st)}`;

    logger.progress("nbastats", ctx.sport, "player-game-logs", `${st} — ${ctx.seasonStr}`);
    if (ctx.dryRun) continue;

    const filePath = outPath(ctx, st, "player-game-logs.json");
    if (fileExists(filePath)) {
      logger.progress("nbastats", ctx.sport, "player-game-logs", `Skipping (exists) ${st}`);
      continue;
    }

    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/player-game-logs: no data`);
      continue;
    }

    writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
    filesWritten++;
  }

  return { filesWritten, errors };
}

async function importTeamGameLogs(ctx: EndpointContext): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const st of SEASON_TYPES) {
    const url =
      `${BASE_URL}/teamgamelogs?LeagueID=${ctx.leagueId}` +
      `&Season=${ctx.seasonStr}&SeasonType=${encodeSeasonType(st)}`;

    logger.progress("nbastats", ctx.sport, "team-game-logs", `${st} — ${ctx.seasonStr}`);
    if (ctx.dryRun) continue;

    const filePath = outPath(ctx, st, "team-game-logs.json");
    if (fileExists(filePath)) {
      logger.progress("nbastats", ctx.sport, "team-game-logs", `Skipping (exists) ${st}`);
      continue;
    }

    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/team-game-logs: no data`);
      continue;
    }

    writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
    filesWritten++;
  }

  return { filesWritten, errors };
}

async function importShotCharts(ctx: EndpointContext): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const st of SEASON_TYPES) {
    const url =
      `${BASE_URL}/shotchartdetail?LeagueID=${ctx.leagueId}` +
      `&Season=${ctx.seasonStr}&SeasonType=${encodeSeasonType(st)}` +
      `&PlayerID=0&TeamID=0&ContextMeasure=FGA` +
      `&GameID=&GameSegment=&LastNGames=0&Location=` +
      `&Month=0&OpponentTeamID=0&Outcome=&PORound=0` +
      `&Period=0&VsConference=&VsDivision=`;

    logger.progress("nbastats", ctx.sport, "shot-charts", `${st} — ${ctx.seasonStr}`);
    if (ctx.dryRun) continue;

    const filePath = outPath(ctx, st, "shot-charts.json");
    if (fileExists(filePath)) {
      logger.progress("nbastats", ctx.sport, "shot-charts", `Skipping (exists) ${st}`);
      continue;
    }

    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/shot-charts: no data`);
      continue;
    }

    writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
    filesWritten++;
  }

  return { filesWritten, errors };
}

async function importTrackingStats(ctx: EndpointContext): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const st of SEASON_TYPES) {
    for (const ptType of TRACKING_MEASURE_TYPES) {
      const url =
        `${BASE_URL}/leaguedashptstats?LeagueID=${ctx.leagueId}` +
        `&Season=${ctx.seasonStr}&SeasonType=${encodeSeasonType(st)}` +
        `&PtMeasureType=${ptType}&PerMode=PerGame&PlayerOrTeam=Player`;

      const label = `tracking-stats/${ptType}`;
      logger.progress("nbastats", ctx.sport, label, `${st} — ${ctx.seasonStr}`);
      if (ctx.dryRun) continue;

      const filePath = outPath(ctx, st, "tracking-stats", `${ptType.toLowerCase()}.json`);
      if (fileExists(filePath)) {
        logger.progress("nbastats", ctx.sport, label, `Skipping (exists) ${st}`);
        continue;
      }

      const data = await nbaFetch(url);
      if (!data) {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/${label}: no data`);
        continue;
      }

      writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
      filesWritten++;
    }
  }

  return { filesWritten, errors };
}

// ── Endpoint dispatch table ─────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  "league-leaders":   importLeagueLeaders,
  "player-stats":     importPlayerStats,
  "team-stats":       importTeamStats,
  "player-game-logs": importPlayerGameLogs,
  "team-game-logs":   importTeamGameLogs,
  "shot-charts":      importShotCharts,
  "tracking-stats":   importTrackingStats,
};

// ── Provider implementation ─────────────────────────────────

const nbastats: Provider = {
  name: "nbastats",
  label: "NBA Stats",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ALL_ENDPOINTS as unknown as readonly string[],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const sports = opts.sports.length
      ? opts.sports.filter((s) => LEAGUE_IDS[s])
      : SUPPORTED_SPORTS;

    const endpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) => ALL_ENDPOINTS.includes(e as Endpoint)) as Endpoint[])
      : [...ALL_ENDPOINTS];

    logger.info(
      `Starting NBA Stats import — ${sports.length} sports, ${endpoints.length} endpoints, ` +
        `${opts.seasons.length} seasons`,
      "nbastats",
    );

    for (const sport of sports) {
      const leagueId = LEAGUE_IDS[sport]!;

      // Filter endpoints for WNBA (limited support)
      const sportEndpoints =
        sport === "wnba"
          ? endpoints.filter((ep) => WNBA_SUPPORTED_ENDPOINTS.has(ep))
          : endpoints;

      if (sport === "wnba" && sportEndpoints.length < endpoints.length) {
        const skipped = endpoints.length - sportEndpoints.length;
        logger.info(`WNBA: skipping ${skipped} unsupported endpoint(s)`, "nbastats");
      }

      for (const season of opts.seasons) {
        const seasonStr = toSeasonStr(season);
        logger.info(`── ${sport.toUpperCase()} ${seasonStr} ──`, "nbastats");

        const ctx: EndpointContext = {
          sport,
          season,
          seasonStr,
          leagueId,
          dataDir: opts.dataDir,
          dryRun: opts.dryRun,
        };

        for (const ep of sportEndpoints) {
          const fn = ENDPOINT_FNS[ep];
          if (!fn) continue;

          try {
            const result = await fn(ctx);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${seasonStr}/${ep}: ${msg}`, "nbastats");
            allErrors.push(`${sport}/${seasonStr}/${ep}: ${msg}`);
          }
        }
      }
    }

    const durationMs = Date.now() - start;
    logger.summary("nbastats", totalFiles, allErrors.length, durationMs);

    return {
      provider: "nbastats",
      sport: sports.length === 1 ? sports[0] : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs,
    };
  },
};

export default nbastats;
