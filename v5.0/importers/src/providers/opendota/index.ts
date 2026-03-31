// ──────────────────────────────────────────────────────────
// OpenDota Provider  [DISABLED]
// ──────────────────────────────────────────────────────────
// Free Dota 2 data — pro matches, match details, teams,
// players, leagues, heroes, and hero stats.
//
// DISABLED (2026-03-26): OpenDota's free API has a strict daily
// request limit that is easily exceeded. Bulk collection of
// pro_matches requires ~300 paginated requests per season, and
// match details require one request per match (thousands).
// The daily quota makes this impractical without a paid API key.
// Re-enable if an API key is configured or if only lightweight
// endpoints (heroes, hero_stats) are needed.

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "opendota";
const BASE_URL = "https://api.opendota.com/api";
const SPORTS: readonly Sport[] = ["dota2"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_200 };

const ENDPOINTS = [
  "pro_matches",
  "matches",
  "teams",
  "players",
  "leagues",
  "heroes",
  "hero_stats",
] as const;

type Endpoint = (typeof ENDPOINTS)[number];

interface ProMatch {
  match_id: number;
  start_time: number;
  [key: string]: unknown;
}

/** Convert unix timestamp to year. */
function matchYear(startTime: number): number {
  return new Date(startTime * 1000).getUTCFullYear();
}

/**
 * Approximate match_id at the END of each calendar year.
 * Pagination starts from this id and walks backwards, so we begin
 * near the target year instead of from the latest match.
 */
const SEASON_END_MATCH_ID: Record<number, number> = {
  2023: 7_400_000_000,
  2024: 8_100_000_000,
  2025: 8_700_000_000,
  2026: 9_999_999_999, // current / future
};

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

async function importProMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "pro_matches.json");

  if (fileExists(outPath)) {
    logger.info(`Skipping pro_matches ${ctx.season} — exists`, NAME);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  logger.progress(NAME, "dota2", "pro_matches", `Fetching recent pro matches`);

  const allMatches: ProMatch[] = [];
  let lessThan: number | undefined = SEASON_END_MATCH_ID[ctx.season] ?? undefined;
  let reachedTarget = false;
  const maxPages = 300;
  const errors: string[] = [];

  for (let page = 0; page < maxPages && !reachedTarget; page++) {
    const url: string = lessThan
      ? `${BASE_URL}/proMatches?less_than_match_id=${lessThan}`
      : `${BASE_URL}/proMatches`;

    let batch: ProMatch[];
    try {
      batch = await fetchJSON<ProMatch[]>(url, NAME, RATE_LIMIT);
    } catch (err) {
      // Rate limited or network error — save what we have and stop
      errors.push(`pro_matches ${ctx.season}: ${String(err)}`);
      logger.error(`pro_matches ${ctx.season}: ${String(err)}`, NAME);
      break;
    }
    if (!Array.isArray(batch) || batch.length === 0) break;

    for (const m of batch) {
      const year = matchYear(m.start_time);
      if (year === ctx.season) {
        allMatches.push(m);
      } else if (year < ctx.season) {
        reachedTarget = true;
        break;
      }
    }

    lessThan = batch[batch.length - 1].match_id;

    if (page % 5 === 0) {
      logger.progress(NAME, "dota2", "pro_matches", `Page ${page + 1}, ${allMatches.length} matches`);
    }
  }

  // Save even partial results (better than nothing)
  if (allMatches.length > 0) {
    writeJSON(outPath, allMatches);
    logger.progress(NAME, "dota2", "pro_matches", `${allMatches.length} matches for ${ctx.season}${errors.length ? " (partial — rate limited)" : ""}`);
    return { filesWritten: 1, errors };
  }

  logger.progress(NAME, "dota2", "pro_matches", `0 matches for ${ctx.season}`);
  return { filesWritten: 0, errors };
}

async function importMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  // Fetch full details for each pro match from the season
  const proPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "pro_matches.json");
  if (!fileExists(proPath)) {
    logger.info("No pro_matches found — run pro_matches endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  const { readJSON } = await import("../../core/io.js");
  const proMatches = readJSON<ProMatch[]>(proPath) ?? [];

  let filesWritten = 0;
  const errors: string[] = [];

  logger.info(`Fetching details for ${proMatches.length} matches in ${ctx.season}`, NAME);

  for (const pm of proMatches) {
    const outPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "matches", `${pm.match_id}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;

    try {
      const data = await fetchJSON(`${BASE_URL}/matches/${pm.match_id}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten++;

      if (filesWritten % 25 === 0) {
        logger.progress(NAME, "dota2", "matches", `${filesWritten}/${proMatches.length}`);
      }
    } catch (err) {
      const msg = `match ${pm.match_id}: ${err instanceof Error ? err.message : String(err)}`;
      errors.push(msg);
    }
  }

  logger.progress(NAME, "dota2", "matches", `${filesWritten} detail files for ${ctx.season}`);
  return { filesWritten, errors };
}

async function importTeams(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "teams.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/teams`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "teams", `Saved for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  // Fetch player profiles for players appearing in pro matches
  const proPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "pro_matches.json");
  if (!fileExists(proPath)) {
    logger.info("No pro_matches found — run pro_matches endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  // OpenDota proMatches doesn't directly include account_ids in the list endpoint,
  // so we get the top pro players from /proPlayers instead
  const outPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "pro_players.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/proPlayers`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "players", `Saved pro players for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importLeagues(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "leagues.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/leagues`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "leagues", `Saved for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importHeroes(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "heroes.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/heroes`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "heroes", `Saved for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importHeroStats(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "hero_stats.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/heroStats`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "hero_stats", `Saved for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  pro_matches: importProMatches,
  matches:     importMatches,
  teams:       importTeams,
  players:     importPlayers,
  leagues:     importLeagues,
  heroes:      importHeroes,
  hero_stats:  importHeroStats,
};

// ── Provider ───────────────────────────────────────────────

const opendota: Provider = {
  name: NAME,
  label: "OpenDota",
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
      sport: "dota2",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default opendota;
