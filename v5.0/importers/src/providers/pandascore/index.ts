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

const SPORTS: readonly Sport[] = ["lol", "csgo", "dota2", "valorant"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_000 };

/** Map our sport slug to PandaScore's game slug */
const GAME_SLUGS: Record<string, string> = {
  lol: "lol",
  csgo: "csgo",
  dota2: "dota2",
  valorant: "valorant",
};

const ENDPOINTS = ["matches", "teams", "players", "leagues", "tournaments"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

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
async function paginateAll<T = unknown>(basePath: string, maxPages = 50): Promise<T[]> {
  const all: T[] = [];

  for (let page = 1; page <= maxPages; page++) {
    const sep = basePath.includes("?") ? "&" : "?";
    const url = `${basePath}${sep}page[number]=${page}&page[size]=100`;
    const items = await apiFetch<T[]>(url);

    if (!Array.isArray(items) || items.length === 0) break;
    all.push(...items);

    if (items.length < 100) break;
  }

  return all;
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

  if (fileExists(outPath)) {
    logger.info(`Skipping matches/${ctx.sport} ${ctx.season} — exists`, NAME);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "matches", `Fetching ${ctx.season}`);

  // Use date range filter so we don't have to paginate through ALL history
  const rangeStart = `${ctx.season}-01-01T00:00:00Z`;
  const rangeEnd = `${ctx.season}-12-31T23:59:59Z`;
  const all = await paginateAll<Record<string, unknown>>(
    `/${ctx.game}/matches?filter[status]=finished&sort=-begin_at` +
    `&range[begin_at]=${rangeStart},${rangeEnd}`,
    200, // allow up to 20K matches per year
  );

  writeJSON(outPath, all);
  logger.progress(NAME, ctx.sport, "matches", `${all.length} matches for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importTeams(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "teams.json");

  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "teams", `Fetching ${ctx.season}`);
  const data = await paginateAll(`/${ctx.game}/teams?sort=name`);
  writeJSON(outPath, data);
  logger.progress(NAME, ctx.sport, "teams", `${data.length} teams`);
  return { filesWritten: 1, errors: [] };
}

async function importPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "players.json");

  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "players", `Fetching ${ctx.season}`);
  const data = await paginateAll(`/${ctx.game}/players?sort=name`);
  writeJSON(outPath, data);
  logger.progress(NAME, ctx.sport, "players", `${data.length} players`);
  return { filesWritten: 1, errors: [] };
}

async function importLeagues(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "leagues.json");

  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "leagues", `Fetching ${ctx.season}`);
  const data = await apiFetch<unknown[]>(`/${ctx.game}/leagues?sort=name`);
  writeJSON(outPath, data);
  logger.progress(NAME, ctx.sport, "leagues", `${data.length} leagues`);
  return { filesWritten: 1, errors: [] };
}

async function importTournaments(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "tournaments.json");

  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, ctx.sport, "tournaments", `Fetching ${ctx.season}`);
  const data = await apiFetch<unknown[]>(`/${ctx.game}/tournaments?sort=-begin_at&page[size]=100`);
  writeJSON(outPath, data);
  logger.progress(NAME, ctx.sport, "tournaments", `${data.length} tournaments`);
  return { filesWritten: 1, errors: [] };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  matches:     importMatches,
  teams:       importTeams,
  players:     importPlayers,
  leagues:     importLeagues,
  tournaments: importTournaments,
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
