// ──────────────────────────────────────────────────────────
// V5.0 NBA Stats Provider
// ──────────────────────────────────────────────────────────
// Fetches player stats, team stats, game logs, shot charts,
// league leaders, and tracking stats from stats.nba.com.
// No API key required — uses browser-like headers.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig, FetchOptions } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath, fileExists, readJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── API base ────────────────────────────────────────────────
const BASE_URL = "https://stats.nba.com/stats";
const LIVE_BASE_URL = "https://cdn.nba.com/static/json/liveData";

// ── Rate limit: ~1 req per 1.2s ────────────────────────────
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_200 };
const LIVE_RATE_LIMIT: RateLimitConfig = { requests: 8, perMs: 1_000 };

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

const LIVE_FETCH_OPTS: FetchOptions = {
  timeoutMs: 45_000,
  retries: 2,
  retryDelayMs: 1_500,
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
  "players",
  "teams",
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
  "scoreboard",
  "game-details",
  "players",
  "teams",
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

async function liveFetch<T = unknown>(url: string): Promise<T | null> {
  try {
    return await fetchJSON<T>(url, "nbastats-cdn", LIVE_RATE_LIMIT, LIVE_FETCH_OPTS);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`liveData fetch failed: ${msg}`, "nbastats");
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

interface GameIndexEntry {
  gameId: string;
  date: string;
  seasonType: SeasonType;
  homeTeamId?: string;
  awayTeamId?: string;
  homeTeamName?: string;
  awayTeamName?: string;
}

type RowRecord = Record<string, unknown>;

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

function seasonAggregatePath(
  ctx: EndpointContext,
  seasonType: SeasonType,
  ...segments: string[]
): string {
  return outPath(ctx, seasonType, "season_aggregates", ...segments);
}

function referencePath(ctx: EndpointContext, ...segments: string[]): string {
  return rawPath(ctx.dataDir, "nbastats", ctx.sport, ctx.seasonStr, "reference", ...segments);
}

function datePath(
  ctx: EndpointContext,
  seasonType: SeasonType,
  date: string,
  ...segments: string[]
): string {
  return outPath(ctx, seasonType, "dates", date, ...segments);
}

function gamePath(
  ctx: EndpointContext,
  seasonType: SeasonType,
  gameId: string,
  ...segments: string[]
): string {
  return outPath(ctx, seasonType, "games", gameId, ...segments);
}

function formatScoreboardDate(date: string): string {
  const [year, month, day] = date.split("-");
  return `${month}/${day}/${year}`;
}

function promoteLegacyPath(structuredPath: string, legacyPath: string): boolean {
  if (fileExists(structuredPath)) return false;
  const payload = readJSON(legacyPath);
  if (payload === null) return false;
  writeJSON(structuredPath, payload);
  return true;
}

function writeAggregatePayload(structuredPath: string, legacyPath: string, payload: unknown): void {
  writeJSON(structuredPath, payload);
  writeJSON(legacyPath, payload);
}

function extractResultRows(data: unknown): RowRecord[] {
  if (!data || typeof data !== "object") return [];

  const payload = data as { resultSets?: unknown; resultSet?: unknown };
  const resultSets = Array.isArray(payload.resultSets)
    ? payload.resultSets
    : payload.resultSet
      ? [payload.resultSet]
      : [];

  const rows: RowRecord[] = [];
  for (const set of resultSets) {
    if (!set || typeof set !== "object") continue;
    const headers = Array.isArray((set as { headers?: unknown }).headers)
      ? (set as { headers: unknown[] }).headers.map((header) => String(header).toUpperCase())
      : [];
    const rowSet = Array.isArray((set as { rowSet?: unknown }).rowSet)
      ? (set as { rowSet: unknown[] }).rowSet
      : [];
    for (const row of rowSet) {
      if (!Array.isArray(row)) continue;
      rows.push(Object.fromEntries(headers.map((header, index) => [header, row[index]])));
    }
  }

  return rows;
}

function buildGameIndexFromRows(rows: RowRecord[], seasonType: SeasonType): GameIndexEntry[] {
  const entries = new Map<string, GameIndexEntry>();

  for (const row of rows) {
    const gameId = String(row.GAME_ID ?? "").trim();
    if (!gameId) continue;

    const date = String(row.GAME_DATE ?? "").slice(0, 10);
    const matchup = String(row.MATCHUP ?? "");
    const isHome = matchup.includes(" vs. ");
    const entry = entries.get(gameId) ?? { gameId, date, seasonType };
    const teamId = String(row.TEAM_ID ?? "").trim();
    const teamName = String(row.TEAM_NAME ?? "").trim();

    if (isHome) {
      entry.homeTeamId = teamId || entry.homeTeamId;
      entry.homeTeamName = teamName || entry.homeTeamName;
    } else {
      entry.awayTeamId = teamId || entry.awayTeamId;
      entry.awayTeamName = teamName || entry.awayTeamName;
    }

    entries.set(gameId, entry);
  }

  return [...entries.values()].sort((left, right) => left.date.localeCompare(right.date) || left.gameId.localeCompare(right.gameId));
}

function buildTeamIndex(entries: GameIndexEntry[]): Array<Record<string, string>> {
  const teams = new Map<string, string>();

  for (const entry of entries) {
    if (entry.homeTeamId && entry.homeTeamName) teams.set(entry.homeTeamId, entry.homeTeamName);
    if (entry.awayTeamId && entry.awayTeamName) teams.set(entry.awayTeamId, entry.awayTeamName);
  }

  return [...teams.entries()]
    .sort((left, right) => left[1].localeCompare(right[1]))
    .map(([teamId, teamName]) => ({ teamId, teamName }));
}

async function ensureTeamGameLogEntries(ctx: EndpointContext, seasonType: SeasonType): Promise<GameIndexEntry[]> {
  const structuredPath = seasonAggregatePath(ctx, seasonType, "team-game-logs.json");
  const legacyPath = outPath(ctx, seasonType, "team-game-logs.json");

  if (!fileExists(structuredPath)) {
    promoteLegacyPath(structuredPath, legacyPath);
  }

  let payload = readJSON(structuredPath) ?? readJSON(legacyPath);

  if (!payload && !ctx.dryRun) {
    const url =
      `${BASE_URL}/teamgamelogs?LeagueID=${ctx.leagueId}` +
      `&Season=${ctx.seasonStr}&SeasonType=${encodeSeasonType(seasonType)}`;
    payload = await nbaFetch(url);
    if (payload) {
      writeAggregatePayload(structuredPath, legacyPath, { ...(payload as object), fetchedAt: new Date().toISOString() });
    }
  }

  return buildGameIndexFromRows(extractResultRows(payload), seasonType);
}

async function loadGameIndex(ctx: EndpointContext): Promise<GameIndexEntry[]> {
  const allEntries: GameIndexEntry[] = [];

  for (const seasonType of SEASON_TYPES) {
    const entries = await ensureTeamGameLogEntries(ctx, seasonType);
    allEntries.push(...entries);

    if (!ctx.dryRun) {
      const byDate = new Map<string, GameIndexEntry[]>();
      for (const entry of entries) {
        const items = byDate.get(entry.date) ?? [];
        items.push(entry);
        byDate.set(entry.date, items);
      }

      for (const [date, games] of byDate.entries()) {
        writeJSON(datePath(ctx, seasonType, date, "games.json"), {
          sport: ctx.sport,
          season: ctx.seasonStr,
          seasonType,
          date,
          games,
          generatedAt: new Date().toISOString(),
        });
      }
    }
  }

  if (!ctx.dryRun) {
    writeJSON(referencePath(ctx, "game_index.json"), {
      sport: ctx.sport,
      season: ctx.seasonStr,
      games: allEntries,
      generatedAt: new Date().toISOString(),
    });
  }

  return allEntries;
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

    const filePath = seasonAggregatePath(ctx, st, "league-leaders.json");
    const legacyPath = outPath(ctx, st, "league-leaders.json");
    if (fileExists(filePath)) {
      logger.progress("nbastats", ctx.sport, "league-leaders", `Skipping (exists) ${st}`);
      continue;
    }
    if (promoteLegacyPath(filePath, legacyPath)) {
      filesWritten++;
      continue;
    }

    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/league-leaders: no data`);
      continue;
    }

    writeAggregatePayload(filePath, legacyPath, { ...data as object, fetchedAt: new Date().toISOString() });
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

      const filePath = seasonAggregatePath(ctx, st, "player-stats", `${measure.toLowerCase()}.json`);
      const legacyPath = outPath(ctx, st, "player-stats", `${measure.toLowerCase()}.json`);
      if (fileExists(filePath)) {
        logger.progress("nbastats", ctx.sport, label, `Skipping (exists) ${st}`);
        continue;
      }
      if (promoteLegacyPath(filePath, legacyPath)) {
        filesWritten++;
        continue;
      }

      const data = await nbaFetch(url);
      if (!data) {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/${label}: no data`);
        continue;
      }

      writeAggregatePayload(filePath, legacyPath, { ...data as object, fetchedAt: new Date().toISOString() });
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

      const filePath = seasonAggregatePath(ctx, st, "team-stats", `${measure.toLowerCase()}.json`);
      const legacyPath = outPath(ctx, st, "team-stats", `${measure.toLowerCase()}.json`);
      if (fileExists(filePath)) {
        logger.progress("nbastats", ctx.sport, label, `Skipping (exists) ${st}`);
        continue;
      }
      if (promoteLegacyPath(filePath, legacyPath)) {
        filesWritten++;
        continue;
      }

      const data = await nbaFetch(url);
      if (!data) {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/${label}: no data`);
        continue;
      }

      writeAggregatePayload(filePath, legacyPath, { ...data as object, fetchedAt: new Date().toISOString() });
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

    const filePath = seasonAggregatePath(ctx, st, "player-game-logs.json");
    const legacyPath = outPath(ctx, st, "player-game-logs.json");
    if (fileExists(filePath)) {
      logger.progress("nbastats", ctx.sport, "player-game-logs", `Skipping (exists) ${st}`);
      continue;
    }
    if (promoteLegacyPath(filePath, legacyPath)) {
      filesWritten++;
      continue;
    }

    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/player-game-logs: no data`);
      continue;
    }

    writeAggregatePayload(filePath, legacyPath, { ...data as object, fetchedAt: new Date().toISOString() });
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

    const filePath = seasonAggregatePath(ctx, st, "team-game-logs.json");
    const legacyPath = outPath(ctx, st, "team-game-logs.json");
    if (fileExists(filePath)) {
      logger.progress("nbastats", ctx.sport, "team-game-logs", `Skipping (exists) ${st}`);
      continue;
    }
    if (promoteLegacyPath(filePath, legacyPath)) {
      filesWritten++;
      continue;
    }

    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/team-game-logs: no data`);
      continue;
    }

    writeAggregatePayload(filePath, legacyPath, { ...data as object, fetchedAt: new Date().toISOString() });
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

    const filePath = seasonAggregatePath(ctx, st, "shot-charts.json");
    const legacyPath = outPath(ctx, st, "shot-charts.json");
    if (fileExists(filePath)) {
      logger.progress("nbastats", ctx.sport, "shot-charts", `Skipping (exists) ${st}`);
      continue;
    }
    if (promoteLegacyPath(filePath, legacyPath)) {
      filesWritten++;
      continue;
    }

    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/shot-charts: no data`);
      continue;
    }

    writeAggregatePayload(filePath, legacyPath, { ...data as object, fetchedAt: new Date().toISOString() });
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

      const filePath = seasonAggregatePath(ctx, st, "tracking-stats", `${ptType.toLowerCase()}.json`);
      const legacyPath = outPath(ctx, st, "tracking-stats", `${ptType.toLowerCase()}.json`);
      if (fileExists(filePath)) {
        logger.progress("nbastats", ctx.sport, label, `Skipping (exists) ${st}`);
        continue;
      }
      if (promoteLegacyPath(filePath, legacyPath)) {
        filesWritten++;
        continue;
      }

      const data = await nbaFetch(url);
      if (!data) {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/${st}/${label}: no data`);
        continue;
      }

      writeAggregatePayload(filePath, legacyPath, { ...data as object, fetchedAt: new Date().toISOString() });
      filesWritten++;
    }
  }

  return { filesWritten, errors };
}

async function importScoreboard(ctx: EndpointContext): Promise<EndpointResult> {
  const entries = await loadGameIndex(ctx);
  if (ctx.dryRun) {
    logger.info(`[dry-run] Would fetch scoreboard data for ${ctx.seasonStr}`, "nbastats");
    return emptyResult();
  }

  let filesWritten = 0;
  const errors: string[] = [];
  const seen = new Set<string>();

  for (const entry of entries) {
    const dedupeKey = `${entry.seasonType}:${entry.date}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);

    const filePath = datePath(ctx, entry.seasonType, entry.date, "scoreboard.json");
    if (fileExists(filePath)) continue;

    logger.progress("nbastats", ctx.sport, "scoreboard", `${entry.seasonType} ${entry.date}`);
    const url =
      `${BASE_URL}/scoreboardv2?DayOffset=0` +
      `&GameDate=${encodeURIComponent(formatScoreboardDate(entry.date))}` +
      `&LeagueID=${ctx.leagueId}`;
    const data = await nbaFetch(url);
    if (!data) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/${entry.seasonType}/scoreboard/${entry.date}: no data`);
      continue;
    }

    writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
    filesWritten++;
  }

  return { filesWritten, errors };
}

async function importPlayers(ctx: EndpointContext): Promise<EndpointResult> {
  const allPlayersFile = referencePath(ctx, "all_players.json");
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress("nbastats", ctx.sport, "players", ctx.seasonStr);
  if (ctx.dryRun) return emptyResult();

  let payload = readJSON(allPlayersFile);
  if (!payload) {
    const url =
      `${BASE_URL}/commonallplayers?LeagueID=${ctx.leagueId}` +
      `&Season=${ctx.seasonStr}&IsOnlyCurrentSeason=0`;
    payload = await nbaFetch(url);
    if (!payload) {
      errors.push(`${ctx.sport}/${ctx.seasonStr}/players: all players fetch failed`);
      return { filesWritten, errors };
    }
    writeJSON(allPlayersFile, { ...payload as object, fetchedAt: new Date().toISOString() });
    filesWritten++;
  }

  const playerIds = extractResultRows(payload)
    .map((row) => String(row.PERSON_ID ?? row.PLAYER_ID ?? "").trim())
    .filter(Boolean);

  const batchSize = 10;
  for (let index = 0; index < playerIds.length; index += batchSize) {
    const batch = playerIds.slice(index, index + batchSize);
    const results = await Promise.allSettled(
      batch.map(async (playerId) => {
        const filePath = referencePath(ctx, "players", playerId, "info.json");
        if (fileExists(filePath)) return false;

        const url = `${BASE_URL}/commonplayerinfo?LeagueID=${ctx.leagueId}&PlayerID=${playerId}`;
        const data = await nbaFetch(url);
        if (!data) throw new Error(`player ${playerId}: no data`);
        writeJSON(filePath, { ...data as object, fetchedAt: new Date().toISOString() });
        return true;
      }),
    );

    for (const result of results) {
      if (result.status === "fulfilled" && result.value) filesWritten++;
      if (result.status === "rejected") {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/players: ${result.reason instanceof Error ? result.reason.message : String(result.reason)}`);
      }
    }
  }

  return { filesWritten, errors };
}

async function importTeams(ctx: EndpointContext): Promise<EndpointResult> {
  const entries = await loadGameIndex(ctx);
  const teams = buildTeamIndex(entries);
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress("nbastats", ctx.sport, "teams", `${ctx.seasonStr} — ${teams.length} teams`);
  if (ctx.dryRun) return emptyResult();

  const indexFile = referencePath(ctx, "teams", "index.json");
  if (!fileExists(indexFile)) {
    writeJSON(indexFile, {
      sport: ctx.sport,
      season: ctx.seasonStr,
      teams,
      generatedAt: new Date().toISOString(),
    });
    filesWritten++;
  }

  const batchSize = 8;
  for (let index = 0; index < teams.length; index += batchSize) {
    const batch = teams.slice(index, index + batchSize);
    const results = await Promise.allSettled(
      batch.map(async ({ teamId, teamName }) => {
        let wrote = 0;
        const infoFile = referencePath(ctx, "teams", teamId, "info.json");
        const rosterFile = referencePath(ctx, "teams", teamId, "roster.json");

        if (!fileExists(infoFile)) {
          const infoUrl = `${BASE_URL}/teaminfocommon?LeagueID=${ctx.leagueId}&Season=${ctx.seasonStr}&TeamID=${teamId}`;
          const info = await nbaFetch(infoUrl);
          if (info) {
            writeJSON(infoFile, { ...info as object, fetchedAt: new Date().toISOString() });
            wrote++;
          }
        }

        if (!fileExists(rosterFile)) {
          const rosterUrl = `${BASE_URL}/commonteamroster?LeagueID=${ctx.leagueId}&Season=${ctx.seasonStr}&TeamID=${teamId}`;
          const roster = await nbaFetch(rosterUrl);
          if (roster) {
            writeJSON(rosterFile, { ...roster as object, fetchedAt: new Date().toISOString() });
            wrote++;
          }
        }

        if (!fileExists(infoFile) && !fileExists(rosterFile)) {
          throw new Error(`team ${teamId} (${teamName}): no team data`);
        }

        return wrote;
      }),
    );

    for (const result of results) {
      if (result.status === "fulfilled") filesWritten += result.value;
      if (result.status === "rejected") {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/teams: ${result.reason instanceof Error ? result.reason.message : String(result.reason)}`);
      }
    }
  }

  return { filesWritten, errors };
}

async function importGameDetails(ctx: EndpointContext): Promise<EndpointResult> {
  const entries = await loadGameIndex(ctx);
  if (ctx.dryRun) {
    logger.info(`[dry-run] Would fetch ${entries.length} game detail bundles for ${ctx.seasonStr}`, "nbastats");
    return emptyResult();
  }

  let filesWritten = 0;
  const errors: string[] = [];
  const batchSize = 8;

  logger.progress("nbastats", ctx.sport, "game-details", `${ctx.seasonStr} — ${entries.length} games`);

  for (let index = 0; index < entries.length; index += batchSize) {
    const batch = entries.slice(index, index + batchSize);
    const results = await Promise.allSettled(
      batch.map(async (entry) => {
        const summaryFile = gamePath(ctx, entry.seasonType, entry.gameId, "summary.json");
        const boxscoreFile = gamePath(ctx, entry.seasonType, entry.gameId, "boxscore.json");
        const playByPlayFile = gamePath(ctx, entry.seasonType, entry.gameId, "playbyplay.json");

        if (!fileExists(summaryFile)) {
          writeJSON(summaryFile, {
            sport: ctx.sport,
            season: ctx.seasonStr,
            ...entry,
            generatedAt: new Date().toISOString(),
          });
        }

        if (fileExists(boxscoreFile) && fileExists(playByPlayFile)) return 0;

        const [boxscore, playByPlay] = await Promise.all([
          fileExists(boxscoreFile)
            ? Promise.resolve(readJSON(boxscoreFile))
            : liveFetch(`${LIVE_BASE_URL}/boxscore/boxscore_${entry.gameId}.json`),
          fileExists(playByPlayFile)
            ? Promise.resolve(readJSON(playByPlayFile))
            : liveFetch(`${LIVE_BASE_URL}/playbyplay/playbyplay_${entry.gameId}.json`),
        ]);

        let wrote = 0;
        if (boxscore && !fileExists(boxscoreFile)) {
          writeJSON(boxscoreFile, { ...boxscore as object, fetchedAt: new Date().toISOString() });
          wrote++;
        }
        if (playByPlay && !fileExists(playByPlayFile)) {
          writeJSON(playByPlayFile, { ...playByPlay as object, fetchedAt: new Date().toISOString() });
          wrote++;
        }
        if (!boxscore && !playByPlay) {
          throw new Error(`game ${entry.gameId}: no boxscore or play-by-play data`);
        }

        return wrote;
      }),
    );

    for (const result of results) {
      if (result.status === "fulfilled") filesWritten += result.value;
      if (result.status === "rejected") {
        errors.push(`${ctx.sport}/${ctx.seasonStr}/game-details: ${result.reason instanceof Error ? result.reason.message : String(result.reason)}`);
      }
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
  "scoreboard":       importScoreboard,
  "game-details":     importGameDetails,
  "players":          importPlayers,
  "teams":            importTeams,
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
