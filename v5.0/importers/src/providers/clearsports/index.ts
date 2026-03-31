// ──────────────────────────────────────────────────────────
// ClearSports Provider
// ──────────────────────────────────────────────────────────
// Multi-sport injuries, news, and projected lineups.
// Requires CLEARSPORTS_KEY env var.

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport, FetchOptions } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "clearsports";
const BASE_URL = "https://api.clearsports.com/v1";
const API_KEY = process.env.CLEARSPORTS_KEY ?? "";

const SPORTS: readonly Sport[] = ["nba", "nfl", "mlb", "nhl", "ncaab", "ncaaf"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 3, perMs: 1_000 };

const ENDPOINTS = ["injuries", "news", "lineups"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

function authHeaders(): Record<string, string> {
  return { Authorization: `Bearer ${API_KEY}` };
}

function apiFetch<T = unknown>(path: string, opts: FetchOptions = {}): Promise<T> {
  return fetchJSON<T>(`${BASE_URL}/${path}`, NAME, RATE_LIMIT, {
    ...opts,
    headers: { ...authHeaders(), ...opts.headers },
  });
}

// ── Endpoint handlers ──────────────────────────────────────

interface EndpointCtx {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

async function importEndpoint(endpoint: Endpoint, ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `${endpoint}.json`);

  if (fileExists(outPath)) {
    logger.info(`Skipping ${endpoint}/${ctx.sport} ${ctx.season} — exists`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  if (ctx.dryRun) {
    logger.info(`[dry-run] Would fetch ${endpoint}/${ctx.sport}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  try {
    const data = await apiFetch(`${endpoint}/${ctx.sport}`);
    writeJSON(outPath, data);
    logger.progress(NAME, ctx.sport, endpoint, `Saved ${ctx.season}`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    const msg = `${endpoint}/${ctx.sport}: ${err instanceof Error ? err.message : String(err)}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }
}

// ── Provider ───────────────────────────────────────────────

const clearsports: Provider = {
  name: NAME,
  label: "ClearSports",
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
      logger.error("CLEARSPORTS_KEY not set — skipping", NAME);
      return { provider: NAME, sport: "multi", filesWritten: 0, errors: ["Missing CLEARSPORTS_KEY"], durationMs: 0 };
    }

    const activeSports = opts.sports.length
      ? opts.sports.filter((s): s is Sport => (SPORTS as readonly Sport[]).includes(s))
      : [...SPORTS];

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS];

    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        for (const ep of activeEndpoints) {
          try {
            const result = await importEndpoint(ep, { sport, season, dataDir: opts.dataDir, dryRun: opts.dryRun });
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

export default clearsports;
