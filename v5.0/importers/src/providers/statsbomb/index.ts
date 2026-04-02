// ──────────────────────────────────────────────────────────
// V5.0 StatsBomb Open Data Provider
// ──────────────────────────────────────────────────────────
// Fetches free open data from StatsBomb's GitHub repository:
// competitions, matches, events, and lineups.
// No API key required — public GitHub data.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { writeJSON, rawPath, fileExists, readJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "statsbomb";
const DATA_BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data";

// GitHub raw is generous — allow 10 req/sec
const RATE_LIMIT: RateLimitConfig = { requests: 10, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = [
  "epl", "laliga", "bundesliga", "seriea", "ligue1", "ucl", "mls", "nwsl",
];

const ALL_ENDPOINTS = [
  "competitions",
  "matches",
  "events",
  "lineups",
  "three_sixty",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── Competition ID mapping ──────────────────────────────────
// StatsBomb competition IDs for the open dataset.
// Some competitions have multiple IDs across seasons;
// we map to all known IDs and filter by available data.

interface CompetitionMapping {
  competitionIds: number[];
  countryName: string;
  competitionName: string;
}

const SPORT_COMPETITIONS: Record<string, CompetitionMapping> = {
  epl:        { competitionIds: [2],       countryName: "England",       competitionName: "Premier League" },
  laliga:     { competitionIds: [11],      countryName: "Spain",         competitionName: "La Liga" },
  bundesliga: { competitionIds: [9],       countryName: "Germany",       competitionName: "1. Bundesliga" },
  seriea:     { competitionIds: [12],      countryName: "Italy",         competitionName: "Serie A" },
  ligue1:     { competitionIds: [7],       countryName: "France",        competitionName: "Ligue 1" },
  ucl:        { competitionIds: [16],      countryName: "Europe",        competitionName: "Champions League" },
  mls:        { competitionIds: [44],      countryName: "United States", competitionName: "Major League Soccer" },
  nwsl:       { competitionIds: [49],      countryName: "United States", competitionName: "NWSL" },
};

// ── StatsBomb API types ─────────────────────────────────────

interface SBCompetition {
  competition_id: number;
  season_id: number;
  competition_name: string;
  country_name: string;
  season_name: string;
  match_available: unknown;
}

interface SBMatch {
  match_id: number;
  match_date: string;
  kick_off: string | null;
  home_team: { home_team_id: number; home_team_name: string };
  away_team: { away_team_id: number; away_team_name: string };
  home_score: number | null;
  away_score: number | null;
  competition: { competition_id: number; competition_name: string };
  season: { season_id: number; season_name: string };
  [key: string]: unknown;
}

// ── Fetch helper ────────────────────────────────────────────

async function sbFetch<T = unknown>(path: string, opts?: { quietNotFound?: boolean }): Promise<T | null> {
  try {
    return await fetchJSON<T>(`${DATA_BASE}/${path}`, NAME, RATE_LIMIT);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (opts?.quietNotFound && msg.includes("404")) {
      return null;
    }
    logger.warn(`Fetch failed: ${msg}`, NAME);
    return null;
  }
}

// ── Endpoint context ────────────────────────────────────────

interface EndpointContext {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
  competitions: SBCompetition[];
  matchesCache: Map<string, SBMatch[]>;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

function seasonCacheKey(sport: Sport, season: number): string {
  return `${sport}:${season}`;
}

function uniqueMatches(matches: SBMatch[]): SBMatch[] {
  const byId = new Map<number, SBMatch>();
  for (const match of matches) {
    byId.set(match.match_id, match);
  }
  return [...byId.values()];
}

async function loadSeasonMatches(
  ctx: EndpointContext,
  compSeasons: { competitionId: number; seasonId: number; seasonName: string }[],
): Promise<SBMatch[]> {
  const key = seasonCacheKey(ctx.sport, ctx.season);
  const cached = ctx.matchesCache.get(key);
  if (cached) return cached;

  const indexFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "matches", "index.json");
  const legacyFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "matches.json");

  const existing = readJSON<SBMatch[]>(indexFile) ?? readJSON<SBMatch[]>(legacyFile);
  if (existing && Array.isArray(existing)) {
    const deduped = uniqueMatches(existing);
    ctx.matchesCache.set(key, deduped);
    return deduped;
  }

  const all: SBMatch[] = [];
  for (const { competitionId, seasonId } of compSeasons) {
    const byCompFile = rawPath(
      ctx.dataDir,
      NAME,
      ctx.sport,
      ctx.season,
      "matches",
      "by_competition",
      String(competitionId),
      `${seasonId}.json`,
    );

    let rows = readJSON<SBMatch[]>(byCompFile);
    if (!rows || !Array.isArray(rows)) {
      rows = await sbFetch<SBMatch[]>(`matches/${competitionId}/${seasonId}.json`);
      if (rows && !ctx.dryRun) {
        writeJSON(byCompFile, rows);
      }
    }

    if (rows && Array.isArray(rows)) {
      all.push(...rows);
    }
  }

  const deduped = uniqueMatches(all);
  if (!ctx.dryRun && deduped.length > 0) {
    writeJSON(indexFile, deduped);
    // Keep legacy aggregate path for compatibility.
    writeJSON(legacyFile, deduped);
  }

  ctx.matchesCache.set(key, deduped);
  return deduped;
}

// ── Core logic: resolve available competitions/seasons ──────

function resolveCompetitionSeasons(
  sport: Sport,
  requestedSeason: number,
  allCompetitions: SBCompetition[],
): { competitionId: number; seasonId: number; seasonName: string }[] {
  const mapping = SPORT_COMPETITIONS[sport];
  if (!mapping) return [];

  const results: { competitionId: number; seasonId: number; seasonName: string }[] = [];

  for (const comp of allCompetitions) {
    if (!mapping.competitionIds.includes(comp.competition_id)) continue;

    // Match season: StatsBomb uses season names like "2023/2024" or "2023"
    const seasonName = comp.season_name;
    const startYear = parseInt(seasonName.split("/")[0], 10);

    if (startYear === requestedSeason) {
      results.push({
        competitionId: comp.competition_id,
        seasonId: comp.season_id,
        seasonName,
      });
    }
  }

  return results;
}

// ── Endpoint implementations ────────────────────────────────

async function importCompetitions(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "reference", "competitions.json");
  const legacyOutFile = rawPath(dataDir, NAME, sport, season, "competitions.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "competitions", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "competitions", `Saving filtered competitions`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const mapping = SPORT_COMPETITIONS[sport];
  if (!mapping) return { filesWritten: 0, errors: [] };

  const filtered = ctx.competitions.filter((c) =>
    mapping.competitionIds.includes(c.competition_id),
  );

  writeJSON(outFile, filtered);
  writeJSON(legacyOutFile, filtered);
  logger.progress(NAME, sport, "competitions", `Saved ${filtered.length} competition entries`);
  return { filesWritten: 2, errors: [] };
}

async function importMatches(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, competitions } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const compSeasons = resolveCompetitionSeasons(sport, season, competitions);
  if (compSeasons.length === 0) {
    logger.progress(NAME, sport, "matches", `No matching competition/season for ${season}`);
    return { filesWritten, errors };
  }

  const allMatches: SBMatch[] = [];

  for (const { competitionId, seasonId, seasonName } of compSeasons) {
    const byCompFile = rawPath(
      dataDir,
      NAME,
      sport,
      season,
      "matches",
      "by_competition",
      String(competitionId),
      `${seasonId}.json`,
    );

    let matches = readJSON<SBMatch[]>(byCompFile);
    if (matches && Array.isArray(matches)) {
      logger.progress(NAME, sport, "matches", `Loaded cached matches for ${seasonName}`);
      allMatches.push(...matches);
      continue;
    }

    logger.progress(NAME, sport, "matches", `Fetching matches for ${seasonName}`);
    if (dryRun) continue;

    try {
      matches = await sbFetch<SBMatch[]>(`matches/${competitionId}/${seasonId}.json`);
      if (matches) {
        writeJSON(byCompFile, matches);
        allMatches.push(...matches);
        filesWritten++;
        logger.progress(NAME, sport, "matches", `Saved ${matches.length} matches for ${seasonName}`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`matches/${competitionId}/${seasonId}: ${msg}`);
    }
  }

  const merged = uniqueMatches(allMatches);
  if (!dryRun && merged.length > 0) {
    const indexFile = rawPath(dataDir, NAME, sport, season, "matches", "index.json");
    const legacyFile = rawPath(dataDir, NAME, sport, season, "matches.json");
    const compSeasonManifest = rawPath(dataDir, NAME, sport, season, "reference", "competition_seasons.json");
    writeJSON(indexFile, merged);
    writeJSON(legacyFile, merged);
    writeJSON(compSeasonManifest, compSeasons);
    filesWritten += 3;
  }

  ctx.matchesCache.set(seasonCacheKey(sport, season), merged);
  logger.progress(NAME, sport, "matches", `Prepared ${merged.length} unique matches`);

  return { filesWritten, errors };
}

async function importEvents(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, competitions } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const compSeasons = resolveCompetitionSeasons(sport, season, competitions);
  if (compSeasons.length === 0) {
    logger.progress(NAME, sport, "events", `No matching competition/season for ${season}`);
    return { filesWritten, errors };
  }

  const matches = await loadSeasonMatches(ctx, compSeasons);
  if (!matches || matches.length === 0) {
    logger.progress(NAME, sport, "events", `No matches found for ${season}`);
    return { filesWritten, errors };
  }

  logger.progress(NAME, sport, "events", `Fetching events for ${matches.length} matches`);
  if (dryRun) return { filesWritten, errors };

  for (const match of matches) {
    const matchId = match.match_id;
    const outFile = rawPath(dataDir, NAME, sport, season, "matches", `${matchId}`, "events.json");
    const legacyFile = rawPath(dataDir, NAME, sport, season, "events", `${matchId}.json`);
    if (fileExists(outFile) || fileExists(legacyFile)) continue;

    try {
      const events = await sbFetch(`events/${matchId}.json`);
      if (events) {
        writeJSON(outFile, events);
        filesWritten++;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`events/${matchId}: ${msg}`);
    }
  }

  logger.progress(NAME, sport, "events", `Saved ${filesWritten} event files`);

  return { filesWritten, errors };
}

async function importLineups(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, competitions } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const compSeasons = resolveCompetitionSeasons(sport, season, competitions);
  if (compSeasons.length === 0) {
    logger.progress(NAME, sport, "lineups", `No matching competition/season for ${season}`);
    return { filesWritten, errors };
  }

  const matches = await loadSeasonMatches(ctx, compSeasons);
  if (!matches || matches.length === 0) {
    logger.progress(NAME, sport, "lineups", `No matches found for ${season}`);
    return { filesWritten, errors };
  }

  logger.progress(NAME, sport, "lineups", `Fetching lineups for ${matches.length} matches`);
  if (dryRun) return { filesWritten, errors };

  for (const match of matches) {
    const matchId = match.match_id;
    const outFile = rawPath(dataDir, NAME, sport, season, "matches", `${matchId}`, "lineups.json");
    const legacyFile = rawPath(dataDir, NAME, sport, season, "lineups", `${matchId}.json`);
    if (fileExists(outFile) || fileExists(legacyFile)) continue;

    try {
      const lineups = await sbFetch(`lineups/${matchId}.json`);
      if (lineups) {
        writeJSON(outFile, lineups);
        filesWritten++;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`lineups/${matchId}: ${msg}`);
    }
  }

  logger.progress(NAME, sport, "lineups", `Saved ${filesWritten} lineup files`);

  return { filesWritten, errors };
}

async function importThreeSixty(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, competitions } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const compSeasons = resolveCompetitionSeasons(sport, season, competitions);
  if (compSeasons.length === 0) {
    logger.progress(NAME, sport, "three_sixty", `No matching competition/season for ${season}`);
    return { filesWritten, errors };
  }

  const matches = await loadSeasonMatches(ctx, compSeasons);
  if (!matches || matches.length === 0) {
    logger.progress(NAME, sport, "three_sixty", `No matches found for ${season}`);
    return { filesWritten, errors };
  }

  logger.progress(NAME, sport, "three_sixty", `Fetching freeze-frame data for ${matches.length} matches`);
  if (dryRun) return { filesWritten, errors };

  for (const match of matches) {
    const matchId = match.match_id;
    const outFile = rawPath(dataDir, NAME, sport, season, "matches", `${matchId}`, "three_sixty.json");
    const legacyFile = rawPath(dataDir, NAME, sport, season, "three_sixty", `${matchId}.json`);
    if (fileExists(outFile) || fileExists(legacyFile)) continue;

    try {
      const frames = await sbFetch(`three-sixty/${matchId}.json`, { quietNotFound: true });
      if (frames) {
        writeJSON(outFile, frames);
        filesWritten++;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`three-sixty/${matchId}: ${msg}`);
    }
  }

  logger.progress(NAME, sport, "three_sixty", `Saved ${filesWritten} three-sixty files`);

  return { filesWritten, errors };
}

// ── Endpoint dispatch map ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  competitions: importCompetitions,
  matches: importMatches,
  events: importEvents,
  lineups: importLineups,
  three_sixty: importThreeSixty,
};

// ── Provider implementation ─────────────────────────────────

const statsbomb: Provider = {
  name: NAME,
  label: "StatsBomb Open Data",
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
      `Starting import — ${sports.length} sports, ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    // Fetch master competitions list once (used by all sports/seasons)
    logger.progress(NAME, "multi", "competitions", "Fetching master competitions list");
    const allCompetitions = await sbFetch<SBCompetition[]>("competitions.json");
    if (!allCompetitions) {
      const msg = "Failed to fetch competitions.json — cannot proceed";
      logger.error(msg, NAME);
      return {
        provider: NAME,
        sport: "multi",
        filesWritten: 0,
        errors: [msg],
        durationMs: Date.now() - start,
      };
    }
    logger.progress(NAME, "multi", "competitions", `Loaded ${allCompetitions.length} competition entries`);

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── ${sport.toUpperCase()} ${season} ──`, NAME);
        const matchesCache = new Map<string, SBMatch[]>();

        for (const ep of endpoints) {
          const fn = ENDPOINT_FNS[ep];
          if (!fn) continue;

          try {
            const ctx: EndpointContext = {
              sport,
              season,
              dataDir: opts.dataDir,
              dryRun: opts.dryRun,
              competitions: allCompetitions,
              matchesCache,
            };
            const result = await fn(ctx);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${season}/${ep}: ${msg}`, NAME);
            allErrors.push(`${sport}/${season}/${ep}: ${msg}`);
          }
        }
      }
    }

    const durationMs = Date.now() - start;
    logger.summary(NAME, totalFiles, allErrors.length, durationMs);

    return {
      provider: NAME,
      sport: sports.length === 1 ? sports[0] : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs,
    };
  },
};

export default statsbomb;
