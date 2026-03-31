// ──────────────────────────────────────────────────────────
// V5.0 StatsBomb Open Data Provider
// ──────────────────────────────────────────────────────────
// Fetches free open data from StatsBomb's GitHub repository:
// competitions, matches, events, and lineups.
// No API key required — public GitHub data.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
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

async function sbFetch<T = unknown>(path: string): Promise<T | null> {
  try {
    return await fetchJSON<T>(`${DATA_BASE}/${path}`, NAME, RATE_LIMIT);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
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
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
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
  const outFile = rawPath(dataDir, NAME, sport, season, "competitions.json");
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
  logger.progress(NAME, sport, "competitions", `Saved ${filtered.length} competition entries`);
  return { filesWritten: 1, errors: [] };
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

  for (const { competitionId, seasonId, seasonName } of compSeasons) {
    const outFile = rawPath(dataDir, NAME, sport, season, "matches.json");
    if (fileExists(outFile)) {
      logger.progress(NAME, sport, "matches", `Skipping ${seasonName} — already exists`);
      continue;
    }
    logger.progress(NAME, sport, "matches", `Fetching matches for ${seasonName}`);
    if (dryRun) continue;

    try {
      const matches = await sbFetch<SBMatch[]>(`matches/${competitionId}/${seasonId}.json`);
      if (matches) {
        writeJSON(outFile, matches);
        filesWritten++;
        logger.progress(NAME, sport, "matches", `Saved ${matches.length} matches`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`matches/${competitionId}/${seasonId}: ${msg}`);
    }
  }

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

  for (const { competitionId, seasonId, seasonName } of compSeasons) {
    // First load match list to get match IDs
    const matchesFile = rawPath(dataDir, NAME, sport, season, "matches.json");
    let matches: SBMatch[] | null = null;

    if (fileExists(matchesFile)) {
      try {
        const content = await import("node:fs").then((fs) =>
          JSON.parse(fs.readFileSync(matchesFile, "utf-8")),
        );
        matches = content as SBMatch[];
      } catch {
        // Fall through to fetch
      }
    }

    if (!matches) {
      // Fetch matches inline if not already saved
      matches = await sbFetch<SBMatch[]>(`matches/${competitionId}/${seasonId}.json`);
    }

    if (!matches || matches.length === 0) {
      logger.progress(NAME, sport, "events", `No matches found for ${seasonName}`);
      continue;
    }

    logger.progress(NAME, sport, "events", `Fetching events for ${matches.length} matches (${seasonName})`);
    if (dryRun) continue;

    for (const match of matches) {
      const matchId = match.match_id;
      const outFile = rawPath(dataDir, NAME, sport, season, "events", `${matchId}.json`);
      if (fileExists(outFile)) continue;

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
  }

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

  for (const { competitionId, seasonId, seasonName } of compSeasons) {
    // Load match list
    const matchesFile = rawPath(dataDir, NAME, sport, season, "matches.json");
    let matches: SBMatch[] | null = null;

    if (fileExists(matchesFile)) {
      try {
        const content = await import("node:fs").then((fs) =>
          JSON.parse(fs.readFileSync(matchesFile, "utf-8")),
        );
        matches = content as SBMatch[];
      } catch {
        // Fall through to fetch
      }
    }

    if (!matches) {
      matches = await sbFetch<SBMatch[]>(`matches/${competitionId}/${seasonId}.json`);
    }

    if (!matches || matches.length === 0) {
      logger.progress(NAME, sport, "lineups", `No matches found for ${seasonName}`);
      continue;
    }

    logger.progress(NAME, sport, "lineups", `Fetching lineups for ${matches.length} matches (${seasonName})`);
    if (dryRun) continue;

    for (const match of matches) {
      const matchId = match.match_id;
      const outFile = rawPath(dataDir, NAME, sport, season, "lineups", `${matchId}.json`);
      if (fileExists(outFile)) continue;

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
  }

  return { filesWritten, errors };
}

// ── Endpoint dispatch map ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  competitions: importCompetitions,
  matches: importMatches,
  events: importEvents,
  lineups: importLineups,
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
