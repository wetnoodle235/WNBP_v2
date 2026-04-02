// ──────────────────────────────────────────────────────────
// PandaScore Provider
// ──────────────────────────────────────────────────────────
// Esports data for LoL, CS:GO, Dota 2, and Valorant.
// Requires PANDASCORE_KEY env var.

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport, FetchOptions } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "pandascore";
const BASE_URL = "https://api.pandascore.co";
const API_KEY = process.env.PANDASCORE_KEY ?? "";
const PAGE_DELAY_MS = Number.parseInt(process.env.PANDASCORE_PAGE_DELAY_MS ?? "500", 10);
const ENDPOINT_DELAY_MS = Number.parseInt(process.env.PANDASCORE_ENDPOINT_DELAY_MS ?? "1500", 10);
const SEASON_DELAY_MS = Number.parseInt(process.env.PANDASCORE_SEASON_DELAY_MS ?? "2500", 10);
const MAX_PAGE_RETRIES = Number.parseInt(process.env.PANDASCORE_MAX_PAGE_RETRIES ?? "12", 10);
const PAGE_RETRY_BASE_MS = Number.parseInt(process.env.PANDASCORE_PAGE_RETRY_BASE_MS ?? "5000", 10);

const SPORTS: readonly Sport[] = ["lol", "csgo", "dota2", "valorant"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 2_000 };

/** Map our sport slug to PandaScore's game slug */
const GAME_SLUGS: Record<string, string> = {
  lol: "lol",
  csgo: "csgo",
  dota2: "dota2",
  valorant: "valorant",
};

const ENDPOINTS = [
  "matches",
  "matches_past",
  "matches_running",
  "matches_upcoming",
  "teams",
  "players",
  "leagues",
  "tournaments",
  "series",
] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface PandaScoreMatch {
  id?: number;
  begin_at?: string | null;
  scheduled_at?: string | null;
  [k: string]: unknown;
}

function authHeaders(): Record<string, string> {
  return { Authorization: `Bearer ${API_KEY}` };
}

async function apiFetch<T = unknown>(path: string, opts: FetchOptions = {}): Promise<T> {
  return fetchJSON<T>(`${BASE_URL}${path}`, NAME, RATE_LIMIT, {
    ...opts,
    headers: { ...authHeaders(), ...opts.headers },
  });
}

/** Paginate through a PandaScore endpoint until we get an empty page. */
interface PaginateResult<T> {
  items: T[];
  partial: boolean;
  error?: string;
}

async function paginateAll<T = unknown>(basePath: string, maxPages = 50): Promise<PaginateResult<T>> {
  const all: T[] = [];

  for (let page = 1; page <= maxPages; page++) {
    const sep = basePath.includes("?") ? "&" : "?";
    const url = `${basePath}${sep}page[number]=${page}&page[size]=100`;
    let items: T[];

    let pageAttempt = 0;
    while (true) {
      try {
        items = await apiFetch<T[]>(url);
        break;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const transient = /429|500|508|Resource Limit|Unreachable|Internal Server Error/i.test(msg);

        if (transient && pageAttempt < MAX_PAGE_RETRIES) {
          const delayMs = Math.min(PAGE_RETRY_BASE_MS * (pageAttempt + 1), 60_000);
          logger.warn(`Page ${page} transient error (${msg}) — retrying in ${delayMs}ms`, NAME);
          pageAttempt += 1;
          await sleep(delayMs);
          continue;
        }

        if (all.length > 0) {
          logger.warn(`Partial paginate stop at page ${page}: ${msg}`, NAME);
          return { items: all, partial: true, error: msg };
        }
        throw err;
      }
    }

    if (!Array.isArray(items) || items.length === 0) break;
    all.push(...items);

    if (items.length < 100) break;
    if (PAGE_DELAY_MS > 0) await sleep(PAGE_DELAY_MS);
  }

  return { items: all, partial: false };
}

function dateFromMatch(match: PandaScoreMatch): string | null {
  const raw = (match.begin_at ?? match.scheduled_at ?? "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return null;
  return raw;
}

function weekDir(dateIso: string): string {
  const d = new Date(`${dateIso}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return "week_00";
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((d.getTime() - yearStart.getTime()) / 86_400_000) + 1) / 7);
  return `week_${String(week).padStart(2, "0")}`;
}

function sanitizeSeasonType(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "unknown";
}

function seasonEventPath(
  ctx: EndpointCtx,
  seasonType: string,
  dateIso: string,
  endpoint: string,
  fileName: string,
): string {
  return rawPath(
    ctx.dataDir,
    NAME,
    ctx.sport,
    ctx.season,
    "season_types",
    sanitizeSeasonType(seasonType),
    "weeks",
    weekDir(dateIso),
    dateIso,
    endpoint,
    fileName,
  );
}

function referencePath(ctx: EndpointCtx, endpoint: string, fileName: string): string {
  return rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "reference", endpoint, fileName);
}

function writeStructuredMatches(
  ctx: EndpointCtx,
  endpoint: string,
  seasonType: string,
  matches: PandaScoreMatch[],
): number {
  let files = 0;
  for (const match of matches) {
    const matchId = match.id;
    if (!matchId) continue;
    const dateIso = dateFromMatch(match);
    if (!dateIso) continue;
    const outPath = seasonEventPath(ctx, seasonType, dateIso, endpoint, `${matchId}.json`);
    writeJSON(outPath, match);
    files += 1;
  }

  const indexPath = rawPath(
    ctx.dataDir,
    NAME,
    ctx.sport,
    ctx.season,
    "season_types",
    sanitizeSeasonType(seasonType),
    `${endpoint}.index.json`,
  );
  writeJSON(indexPath, {
    provider: NAME,
    sport: ctx.sport,
    season: ctx.season,
    endpoint,
    season_type: sanitizeSeasonType(seasonType),
    generated_at: new Date().toISOString(),
    count: matches.length,
  });
  files += 1;

  return files;
}

// ── Endpoint handlers ──────────────────────────────────────

interface EndpointCtx {
  sport: Sport;
  game: string;
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

async function importMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "matches.json");
  const structuredIndex = rawPath(
    ctx.dataDir,
    NAME,
    ctx.sport,
    ctx.season,
    "season_types",
    "past",
    "matches.index.json",
  );

  if (fileExists(outPath) && fileExists(structuredIndex)) {
    logger.info(`Skipping matches/${ctx.sport} ${ctx.season} — exists`, NAME);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "matches", `Fetching ${ctx.season}`);

  // Use date range filter so we don't have to paginate through ALL history
  const rangeStart = `${ctx.season}-01-01T00:00:00Z`;
  const rangeEnd = `${ctx.season}-12-31T23:59:59Z`;
  const { items: all, partial, error } = await paginateAll<PandaScoreMatch>(
    `/${ctx.game}/matches?filter[status]=finished&sort=-begin_at` +
    `&range[begin_at]=${rangeStart},${rangeEnd}`,
    200, // allow up to 20K matches per year
  );

  writeJSON(outPath, all);
  const structuredFiles = writeStructuredMatches(ctx, "matches", "past", all);
  if (partial) {
    logger.warn(`matches/${ctx.sport}/${ctx.season}: partial save (${error ?? "pagination interrupted"})`, NAME);
  }
  logger.progress(NAME, ctx.sport, "matches", `${all.length} matches for ${ctx.season}`);
  return { filesWritten: 1 + structuredFiles, errors: [] };
}

async function importMatchSlice(
  ctx: EndpointCtx,
  endpointPath: "past" | "running" | "upcoming",
  outName: string,
): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `${outName}.json`);
  const structuredIndex = rawPath(
    ctx.dataDir,
    NAME,
    ctx.sport,
    ctx.season,
    "season_types",
    endpointPath,
    `${outName}.index.json`,
  );

  if (fileExists(outPath) && fileExists(structuredIndex)) {
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, outName, `Fetching ${ctx.season}`);

  const rangeStart = `${ctx.season}-01-01T00:00:00Z`;
  const rangeEnd = `${ctx.season}-12-31T23:59:59Z`;

  const path = endpointPath === "past"
    ? `/${ctx.game}/matches/${endpointPath}?sort=-begin_at&range[begin_at]=${rangeStart},${rangeEnd}`
    : `/${ctx.game}/matches/${endpointPath}?sort=begin_at&range[begin_at]=${rangeStart},${rangeEnd}`;

  const { items: all, partial, error } = await paginateAll<PandaScoreMatch>(path, 200);
  writeJSON(outPath, all);
  const structuredFiles = writeStructuredMatches(ctx, outName, endpointPath, all);
  if (partial) {
    logger.warn(`${outName}/${ctx.sport}/${ctx.season}: partial save (${error ?? "pagination interrupted"})`, NAME);
  }

  logger.progress(NAME, ctx.sport, outName, `${all.length} matches`);
  return { filesWritten: 1 + structuredFiles, errors: [] };
}

async function importTeams(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "teams.json");
  const refPath = referencePath(ctx, "teams", "teams.json");

  if (fileExists(outPath) && fileExists(refPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "teams", `Fetching ${ctx.season}`);
  const { items: data, partial, error } = await paginateAll(`/${ctx.game}/teams?sort=name`);
  writeJSON(outPath, data);
  writeJSON(refPath, data);
  if (partial) {
    logger.warn(`teams/${ctx.sport}/${ctx.season}: partial save (${error ?? "pagination interrupted"})`, NAME);
  }
  logger.progress(NAME, ctx.sport, "teams", `${data.length} teams`);
  return { filesWritten: 2, errors: [] };
}

async function importPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "players.json");
  const refPath = referencePath(ctx, "players", "players.json");

  if (fileExists(outPath) && fileExists(refPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "players", `Fetching ${ctx.season}`);
  const { items: data, partial, error } = await paginateAll(`/${ctx.game}/players?sort=name`);
  writeJSON(outPath, data);
  writeJSON(refPath, data);
  if (partial) {
    logger.warn(`players/${ctx.sport}/${ctx.season}: partial save (${error ?? "pagination interrupted"})`, NAME);
  }
  logger.progress(NAME, ctx.sport, "players", `${data.length} players`);
  return { filesWritten: 2, errors: [] };
}

async function importLeagues(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "leagues.json");
  const refPath = referencePath(ctx, "leagues", "leagues.json");

  if (fileExists(outPath) && fileExists(refPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "leagues", `Fetching ${ctx.season}`);
  const { items: data, partial, error } = await paginateAll<unknown>(`/${ctx.game}/leagues?sort=name`);
  writeJSON(outPath, data);
  writeJSON(refPath, data);
  if (partial) {
    logger.warn(`leagues/${ctx.sport}/${ctx.season}: partial save (${error ?? "pagination interrupted"})`, NAME);
  }
  logger.progress(NAME, ctx.sport, "leagues", `${data.length} leagues`);
  return { filesWritten: 2, errors: [] };
}

async function importTournaments(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "tournaments.json");
  const refPath = referencePath(ctx, "tournaments", "tournaments.json");

  if (fileExists(outPath) && fileExists(refPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "tournaments", `Fetching ${ctx.season}`);
  const rangeStart = `${ctx.season}-01-01T00:00:00Z`;
  const rangeEnd = `${ctx.season}-12-31T23:59:59Z`;
  const { items: data, partial, error } = await paginateAll<unknown>(
    `/${ctx.game}/tournaments?sort=-begin_at&range[begin_at]=${rangeStart},${rangeEnd}`,
    100,
  );
  writeJSON(outPath, data);
  writeJSON(refPath, data);
  if (partial) {
    logger.warn(`tournaments/${ctx.sport}/${ctx.season}: partial save (${error ?? "pagination interrupted"})`, NAME);
  }
  logger.progress(NAME, ctx.sport, "tournaments", `${data.length} tournaments`);
  return { filesWritten: 2, errors: [] };
}

async function importSeries(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "series.json");
  const refPath = referencePath(ctx, "series", "series.json");

  if (fileExists(outPath) && fileExists(refPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const rangeStart = `${ctx.season}-01-01T00:00:00Z`;
  const rangeEnd = `${ctx.season}-12-31T23:59:59Z`;

  logger.progress(NAME, ctx.sport, "series", `Fetching ${ctx.season}`);
  const { items: data, partial, error } = await paginateAll<unknown>(
    `/${ctx.game}/series?sort=-begin_at&range[begin_at]=${rangeStart},${rangeEnd}`,
    100,
  );
  writeJSON(outPath, data);
  writeJSON(refPath, data);
  if (partial) {
    logger.warn(`series/${ctx.sport}/${ctx.season}: partial save (${error ?? "pagination interrupted"})`, NAME);
  }
  logger.progress(NAME, ctx.sport, "series", `${data.length} series`);
  return { filesWritten: 2, errors: [] };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  matches:     importMatches,
  matches_past: (ctx) => importMatchSlice(ctx, "past", "matches_past"),
  matches_running: (ctx) => importMatchSlice(ctx, "running", "matches_running"),
  matches_upcoming: (ctx) => importMatchSlice(ctx, "upcoming", "matches_upcoming"),
  teams:       importTeams,
  players:     importPlayers,
  leagues:     importLeagues,
  tournaments: importTournaments,
  series:      importSeries,
};

// ── Provider ───────────────────────────────────────────────

const pandascore: Provider = {
  name: NAME,
  label: "PandaScore Esports",
  sports: SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: !!API_KEY,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    if (!API_KEY) {
      logger.error("PANDASCORE_KEY not set — skipping", NAME);
      return { provider: NAME, sport: "multi", filesWritten: 0, errors: ["Missing PANDASCORE_KEY"], durationMs: 0 };
    }

    const activeSports = opts.sports.length
      ? opts.sports.filter((s): s is Sport => (SPORTS as readonly Sport[]).includes(s))
      : [...SPORTS];

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS];

    for (const sport of activeSports) {
      const game = GAME_SLUGS[sport];
      if (!game) continue;

      for (const season of opts.seasons) {
        logger.info(`${sport} / ${season}`, NAME);
        for (const ep of activeEndpoints) {
          try {
            const result = await ENDPOINT_FNS[ep]({ sport, game, season, dataDir: opts.dataDir, dryRun: opts.dryRun });
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = `${ep}/${sport}/${season}: ${err instanceof Error ? err.message : String(err)}`;
            logger.error(msg, NAME);
            allErrors.push(msg);
          }
          if (!opts.dryRun && ENDPOINT_DELAY_MS > 0) {
            await sleep(ENDPOINT_DELAY_MS);
          }
        }
        if (!opts.dryRun && SEASON_DELAY_MS > 0) {
          await sleep(SEASON_DELAY_MS);
        }
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0] : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default pandascore;
