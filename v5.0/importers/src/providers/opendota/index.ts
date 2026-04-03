// ──────────────────────────────────────────────────────────
// OpenDota Provider  [LIGHTWEIGHT MODE]
// ──────────────────────────────────────────────────────────
// Free Dota 2 data — pro matches, match details, teams,
// players, leagues, heroes, and hero stats.
//
// NOTE (2026-03-26): Bulk endpoints (pro_matches, matches, players, etc.)
// are excluded from DEFAULT_ENDPOINTS because OpenDota's free API quota is
// easily exhausted. Only lightweight reference endpoints (heroes, hero_stats)
// run by default — they require just 2 requests total.
// Add bulk endpoints explicitly via --endpoints if needed.

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, fileExists, readJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "opendota";
const BASE_URL = "https://api.opendota.com/api";
const SPORTS: readonly Sport[] = ["dota2"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_200 };

// All available endpoints (used for explicit --endpoints overrides)
const ENDPOINTS = [
  "pro_matches",
  "matches",
  "teams",
  "players",
  "leagues",
  "heroes",
  "hero_stats",
  "public_matches",
  "parsed_matches",
  "metadata",
  "distributions",
  "schema",
  "health",
  "live",
  "constants",
  "league_matches",
  "league_match_ids",
  "league_teams",
  "team_matches",
  "team_players",
  "team_heroes",
  "top_players",
  "search",
  "rankings",
  "benchmarks",
  "records",
  "scenarios_item_timings",
  "scenarios_lane_roles",
  "scenarios_misc",
  "hero_matches",
  "hero_matchups",
  "hero_durations",
  "hero_players",
  "hero_item_popularity",
  "league_details",
  "team_details",
  "player_details",
  "player_wl",
  "player_recent_matches",
  "player_matches",
  "player_heroes",
  "player_peers",
  "player_pros",
  "player_totals",
  "player_counts",
  "player_wardmap",
  "player_wordcloud",
  "player_ratings",
  "player_rankings",
] as const;

// Lightweight reference endpoints only — no bulk API quota burn
const DEFAULT_ENDPOINTS: readonly Endpoint[] = ["heroes", "hero_stats"];

type Endpoint = (typeof ENDPOINTS)[number];

interface ProMatch {
  match_id: number;
  start_time: number;
  radiant_team_id?: number;
  dire_team_id?: number;
  leagueid?: number;
  [key: string]: unknown;
}

const MAX_ENTITY_ENDPOINT_IDS = 200;
const MAX_HERO_IDS = 60;
const MAX_PLAYER_IDS = 200;
const CONSTANT_RESOURCES = [
  "heroes",
  "items",
  "abilities",
  "patch",
  "regions",
  "game_mode",
  "lobby_type",
] as const;

const RECORD_FIELDS = [
  "kills",
  "assists",
  "deaths",
  "hero_damage",
  "tower_damage",
  "gold_per_min",
  "xp_per_min",
  "duration",
] as const;

const SCENARIO_MISC_TYPES = [
  "radiantBigAdvantage",
  "radiantHugeAdvantage",
  "radiantWins",
  "direWins",
] as const;

/** Convert unix timestamp to year. */
function matchYear(startTime: number): number {
  return new Date(startTime * 1000).getUTCFullYear();
}

function dateFromUnix(startTime: number | undefined): string {
  if (!startTime) return "unknown-date";
  const dt = new Date(startTime * 1000);
  if (Number.isNaN(dt.getTime())) return "unknown-date";
  return dt.toISOString().slice(0, 10);
}

function isoWeek(dateIso: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateIso)) return "week_00";
  const d = new Date(`${dateIso}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return "week_00";
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((d.getTime() - yearStart.getTime()) / 86_400_000) + 1) / 7);
  return `week_${String(week).padStart(2, "0")}`;
}

function referencePath(ctx: EndpointCtx, ...segments: string[]): string {
  return rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "reference", ...segments);
}

function seasonalEventPath(ctx: EndpointCtx, dateIso: string, endpoint: string, fileName: string): string {
  return rawPath(
    ctx.dataDir,
    NAME,
    "dota2",
    ctx.season,
    "season_types",
    "regular",
    "weeks",
    isoWeek(dateIso),
    "dates",
    dateIso,
    endpoint,
    fileName,
  );
}

function loadProMatchesIndex(ctx: EndpointCtx): ProMatch[] {
  const structured = readJSON<ProMatch[]>(referencePath(ctx, "pro_matches_index.json"));
  if (Array.isArray(structured) && structured.length > 0) {
    return structured;
  }
  const legacy = readJSON<ProMatch[]>(rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "pro_matches.json"));
  if (Array.isArray(legacy)) {
    return legacy;
  }
  return [];
}

function activeLeagueIds(ctx: EndpointCtx): number[] {
  const seen = new Set<number>();
  for (const m of loadProMatchesIndex(ctx)) {
    const leagueId = Number(m.leagueid);
    if (Number.isFinite(leagueId) && leagueId > 0) {
      seen.add(leagueId);
    }
    if (seen.size >= MAX_ENTITY_ENDPOINT_IDS) break;
  }
  return [...seen];
}

function activeTeamIds(ctx: EndpointCtx): number[] {
  const seen = new Set<number>();
  for (const m of loadProMatchesIndex(ctx)) {
    const radiant = Number(m.radiant_team_id);
    const dire = Number(m.dire_team_id);
    if (Number.isFinite(radiant) && radiant > 0) seen.add(radiant);
    if (Number.isFinite(dire) && dire > 0) seen.add(dire);
    if (seen.size >= MAX_ENTITY_ENDPOINT_IDS) break;
  }
  return [...seen];
}

function activeHeroIds(ctx: EndpointCtx): number[] {
  const heroes = readJSON<Array<{ id?: number; hero_id?: number }>>(referencePath(ctx, "heroes.json"));
  const seen = new Set<number>();
  for (const hero of heroes ?? []) {
    const heroId = Number(hero?.id ?? hero?.hero_id);
    if (Number.isFinite(heroId) && heroId > 0) {
      seen.add(heroId);
    }
    if (seen.size >= MAX_HERO_IDS) break;
  }
  return [...seen];
}

function activePlayerIds(ctx: EndpointCtx): number[] {
  const players = readJSON<Array<{ account_id?: number }>>(referencePath(ctx, "pro_players.json"));
  const seen = new Set<number>();
  for (const player of players ?? []) {
    const accountId = Number(player?.account_id);
    if (Number.isFinite(accountId) && accountId > 0) {
      seen.add(accountId);
    }
    if (seen.size >= MAX_PLAYER_IDS) break;
  }
  return [...seen];
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
  const indexPath = referencePath(ctx, "pro_matches_index.json");
  const legacyPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "pro_matches.json");

  if (fileExists(indexPath) || fileExists(legacyPath)) {
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

  // Save even partial results (better than nothing).
  if (allMatches.length > 0) {
    writeJSON(indexPath, allMatches);
    let detailFiles = 0;
    for (const match of allMatches) {
      const dateIso = dateFromUnix(match.start_time);
      const detailPath = seasonalEventPath(ctx, dateIso, "pro_matches", `${match.match_id}.json`);
      if (fileExists(detailPath)) continue;
      writeJSON(detailPath, match);
      detailFiles += 1;
    }
    logger.progress(NAME, "dota2", "pro_matches", `${allMatches.length} matches for ${ctx.season}${errors.length ? " (partial — rate limited)" : ""}`);
    return { filesWritten: detailFiles + 1, errors };
  }

  logger.progress(NAME, "dota2", "pro_matches", `0 matches for ${ctx.season}`);
  return { filesWritten: 0, errors };
}

async function importMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  // Fetch full details for each pro match from the season
  const proMatches = loadProMatchesIndex(ctx);
  if (proMatches.length === 0) {
    logger.info("No pro_matches found — run pro_matches endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  let filesWritten = 0;
  const errors: string[] = [];

  logger.info(`Fetching details for ${proMatches.length} matches in ${ctx.season}`, NAME);

  for (const pm of proMatches) {
    const dateIso = dateFromUnix(pm.start_time);
    const outPath = seasonalEventPath(ctx, dateIso, "matches", `${pm.match_id}.json`);
    const legacyPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "matches", `${pm.match_id}.json`);
    if (fileExists(outPath) || fileExists(legacyPath)) continue;
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
  const outPath = referencePath(ctx, "teams.json");
  const legacyPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "teams.json");
  if (fileExists(outPath) || fileExists(legacyPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/teams`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "teams", `Saved for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  // Fetch player profiles for players appearing in pro matches
  if (loadProMatchesIndex(ctx).length === 0) {
    logger.info("No pro_matches found — run pro_matches endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  // OpenDota proMatches doesn't directly include account_ids in the list endpoint,
  // so we get the top pro players from /proPlayers instead
  const outPath = referencePath(ctx, "pro_players.json");
  const legacyPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "pro_players.json");
  if (fileExists(outPath) || fileExists(legacyPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/proPlayers`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "players", `Saved pro players for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importLeagues(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "leagues.json");
  const legacyPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "leagues.json");
  if (fileExists(outPath) || fileExists(legacyPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/leagues`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "leagues", `Saved for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importHeroes(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "heroes.json");
  const legacyPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "heroes.json");
  if (fileExists(outPath) || fileExists(legacyPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/heroes`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "heroes", `Saved for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importHeroStats(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "hero_stats.json");
  const legacyPath = rawPath(ctx.dataDir, NAME, "dota2", ctx.season, "hero_stats.json");
  if (fileExists(outPath) || fileExists(legacyPath)) return { filesWritten: 0, errors: [] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const data = await fetchJSON(`${BASE_URL}/heroStats`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "hero_stats", `Saved for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importPublicMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "public_matches.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/publicMatches`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "public_matches", `Saved sampled public matches for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importParsedMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "parsed_matches.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/parsedMatches`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  logger.progress(NAME, "dota2", "parsed_matches", `Saved parsed match ids for ${ctx.season}`);
  return { filesWritten: 1, errors: [] };
}

async function importMetadata(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "metadata.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/metadata`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  return { filesWritten: 1, errors: [] };
}

async function importDistributions(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "distributions.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/distributions`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  return { filesWritten: 1, errors: [] };
}

async function importSchema(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "schema.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/schema`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  return { filesWritten: 1, errors: [] };
}

async function importHealth(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "health.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/health`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  return { filesWritten: 1, errors: [] };
}

async function importLive(ctx: EndpointCtx): Promise<EndpointResult> {
  const dateIso = new Date().toISOString().slice(0, 10);
  const outPath = seasonalEventPath(ctx, dateIso, "snapshots", "live.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/live`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  return { filesWritten: 1, errors: [] };
}

async function importConstants(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const resource of CONSTANT_RESOURCES) {
    const outPath = referencePath(ctx, "constants", `${resource}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/constants/${resource}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`constants/${resource}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importLeagueMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const leagueId of activeLeagueIds(ctx)) {
    const outPath = referencePath(ctx, "leagues", String(leagueId), "matches.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/leagues/${leagueId}/matches`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`league_matches/${leagueId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importLeagueMatchIds(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const leagueId of activeLeagueIds(ctx)) {
    const outPath = referencePath(ctx, "leagues", String(leagueId), "match_ids.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/leagues/${leagueId}/matchIds`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`league_match_ids/${leagueId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importLeagueTeams(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const leagueId of activeLeagueIds(ctx)) {
    const outPath = referencePath(ctx, "leagues", String(leagueId), "teams.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/leagues/${leagueId}/teams`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`league_teams/${leagueId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importTeamMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const teamId of activeTeamIds(ctx)) {
    const outPath = referencePath(ctx, "teams", String(teamId), "matches.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/teams/${teamId}/matches`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`team_matches/${teamId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importTeamPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const teamId of activeTeamIds(ctx)) {
    const outPath = referencePath(ctx, "teams", String(teamId), "players.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/teams/${teamId}/players`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`team_players/${teamId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importTeamHeroes(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const teamId of activeTeamIds(ctx)) {
    const outPath = referencePath(ctx, "teams", String(teamId), "heroes.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/teams/${teamId}/heroes`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`team_heroes/${teamId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importTopPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "top_players.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/topPlayers`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  return { filesWritten: 1, errors: [] };
}

async function importSearch(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = referencePath(ctx, "search_pro.json");
  if (fileExists(outPath)) return emptyResult();
  if (ctx.dryRun) return emptyResult();
  const data = await fetchJSON(`${BASE_URL}/search?q=pro`, NAME, RATE_LIMIT);
  writeJSON(outPath, data);
  return { filesWritten: 1, errors: [] };
}

async function importRankings(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const heroId of activeHeroIds(ctx)) {
    const outPath = referencePath(ctx, "heroes", String(heroId), "rankings.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/rankings?hero_id=${heroId}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`rankings/${heroId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importBenchmarks(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const heroId of activeHeroIds(ctx)) {
    const outPath = referencePath(ctx, "heroes", String(heroId), "benchmarks.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/benchmarks?hero_id=${heroId}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`benchmarks/${heroId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importRecords(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const field of RECORD_FIELDS) {
    const outPath = referencePath(ctx, "records", `${field}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/records/${field}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`records/${field}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importScenariosItemTimings(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const heroId of activeHeroIds(ctx)) {
    const outPath = referencePath(ctx, "heroes", String(heroId), "scenarios_item_timings.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/scenarios/itemTimings?hero_id=${heroId}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`scenarios_item_timings/${heroId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importScenariosLaneRoles(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const heroId of activeHeroIds(ctx)) {
    const outPath = referencePath(ctx, "heroes", String(heroId), "scenarios_lane_roles.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/scenarios/laneRoles?hero_id=${heroId}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`scenarios_lane_roles/${heroId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importScenariosMisc(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const scenario of SCENARIO_MISC_TYPES) {
    const outPath = referencePath(ctx, "scenarios", `misc_${scenario}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/scenarios/misc?scenario=${scenario}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`scenarios_misc/${scenario}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importHeroMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  return importHeroScopedEndpoint(ctx, "matches");
}

async function importHeroMatchups(ctx: EndpointCtx): Promise<EndpointResult> {
  return importHeroScopedEndpoint(ctx, "matchups");
}

async function importHeroDurations(ctx: EndpointCtx): Promise<EndpointResult> {
  return importHeroScopedEndpoint(ctx, "durations");
}

async function importHeroPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  return importHeroScopedEndpoint(ctx, "players");
}

async function importHeroItemPopularity(ctx: EndpointCtx): Promise<EndpointResult> {
  return importHeroScopedEndpoint(ctx, "itemPopularity");
}

async function importHeroScopedEndpoint(ctx: EndpointCtx, suffix: string): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const heroId of activeHeroIds(ctx)) {
    const outPath = referencePath(ctx, "heroes", String(heroId), `${suffix}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/heroes/${heroId}/${suffix}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`heroes/${heroId}/${suffix}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importLeagueDetails(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const leagueId of activeLeagueIds(ctx)) {
    const outPath = referencePath(ctx, "leagues", String(leagueId), "details.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/leagues/${leagueId}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`league_details/${leagueId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importTeamDetails(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  for (const teamId of activeTeamIds(ctx)) {
    const outPath = referencePath(ctx, "teams", String(teamId), "details.json");
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_URL}/teams/${teamId}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`team_details/${teamId}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

async function importPlayerDetails(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "", "details");
}

async function importPlayerWl(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "wl");
}

async function importPlayerRecentMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "recentMatches");
}

async function importPlayerMatches(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "matches");
}

async function importPlayerHeroes(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "heroes");
}

async function importPlayerPeers(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "peers");
}

async function importPlayerPros(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "pros");
}

async function importPlayerTotals(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "totals");
}

async function importPlayerCounts(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "counts");
}

async function importPlayerWardmap(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "wardmap");
}

async function importPlayerWordcloud(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "wordcloud");
}

async function importPlayerRatings(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "ratings");
}

async function importPlayerRankings(ctx: EndpointCtx): Promise<EndpointResult> {
  return importPlayerScopedEndpoint(ctx, "rankings");
}

async function importPlayerScopedEndpoint(
  ctx: EndpointCtx,
  suffix: string,
  alias = "",
): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const suffixName = alias || suffix || "details";
  for (const accountId of activePlayerIds(ctx)) {
    const outPath = referencePath(ctx, "players", String(accountId), `${suffixName}.json`);
    if (fileExists(outPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const endpoint = suffix ? `/players/${accountId}/${suffix}` : `/players/${accountId}`;
      const data = await fetchJSON(`${BASE_URL}${endpoint}`, NAME, RATE_LIMIT);
      writeJSON(outPath, data);
      filesWritten += 1;
    } catch (err) {
      errors.push(`players/${accountId}/${suffixName}: ${String(err)}`);
    }
  }
  return { filesWritten, errors };
}

function emptyResult(): EndpointResult {
  return { filesWritten: 0, errors: [] };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  pro_matches: importProMatches,
  matches:     importMatches,
  teams:       importTeams,
  players:     importPlayers,
  leagues:     importLeagues,
  heroes:      importHeroes,
  hero_stats:  importHeroStats,
  public_matches: importPublicMatches,
  parsed_matches: importParsedMatches,
  metadata: importMetadata,
  distributions: importDistributions,
  schema: importSchema,
  health: importHealth,
  live: importLive,
  constants: importConstants,
  league_matches: importLeagueMatches,
  league_match_ids: importLeagueMatchIds,
  league_teams: importLeagueTeams,
  team_matches: importTeamMatches,
  team_players: importTeamPlayers,
  team_heroes: importTeamHeroes,
  top_players: importTopPlayers,
  search: importSearch,
  rankings: importRankings,
  benchmarks: importBenchmarks,
  records: importRecords,
  scenarios_item_timings: importScenariosItemTimings,
  scenarios_lane_roles: importScenariosLaneRoles,
  scenarios_misc: importScenariosMisc,
  hero_matches: importHeroMatches,
  hero_matchups: importHeroMatchups,
  hero_durations: importHeroDurations,
  hero_players: importHeroPlayers,
  hero_item_popularity: importHeroItemPopularity,
  league_details: importLeagueDetails,
  team_details: importTeamDetails,
  player_details: importPlayerDetails,
  player_wl: importPlayerWl,
  player_recent_matches: importPlayerRecentMatches,
  player_matches: importPlayerMatches,
  player_heroes: importPlayerHeroes,
  player_peers: importPlayerPeers,
  player_pros: importPlayerPros,
  player_totals: importPlayerTotals,
  player_counts: importPlayerCounts,
  player_wardmap: importPlayerWardmap,
  player_wordcloud: importPlayerWordcloud,
  player_ratings: importPlayerRatings,
  player_rankings: importPlayerRankings,
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
      : [...DEFAULT_ENDPOINTS];

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
