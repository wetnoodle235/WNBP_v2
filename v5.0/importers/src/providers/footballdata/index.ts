// ──────────────────────────────────────────────────────────
// V5.0 football-data.org Provider
// ──────────────────────────────────────────────────────────
// Fetches match results, standings, teams, top scorers, and
// head-to-head records from the football-data.org v4 API.
// Requires API key via FOOTBALLDATA_API_KEY env var.
// Free tier: 10 requests/min.
//
// Supported competitions (v4 API codes):
//   Tier-one leagues:  PL, BL1, BL2, PD, SA, FL1, DED, PPL, SA (via SB), MLS
//   European:          CL (ucl), EL (europa), EC (euros)
//   World:             WC (worldcup)
//   Second divisions:  ELC (championship), SB (serieb), FL2 (ligue2)

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, readJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "footballdata";
const BASE_URL = "https://api.football-data.org/v4";

// Free tier: 10 req/min → enforce 6 req/min to stay safe
const RATE_LIMIT: RateLimitConfig = { requests: 6, perMs: 60_000 };

const SUPPORTED_SPORTS: Sport[] = [
  // Tier-1 leagues (major)
  "epl", "bundesliga", "laliga", "seriea", "ligue1",
  // Additional top-flight leagues
  "eredivisie", "primeiraliga",
  // Second divisions
  "championship", "bundesliga2", "serieb", "ligue2",
  // Club European competitions
  "ucl", "europa",
  // International tournaments
  "euros", "worldcup",
  // Additional leagues
  "mls",
];

// Sport key → football-data.org competition code
const COMPETITION_CODES: Record<string, string> = {
  // Major top-flight
  epl:          "PL",
  bundesliga:   "BL1",
  laliga:       "PD",
  seriea:       "SA",
  ligue1:       "FL1",
  // Additional top-flight
  eredivisie:   "DED",
  primeiraliga: "PPL",
  // Second divisions
  championship: "ELC",
  bundesliga2:  "BL2",
  serieb:       "SB",
  ligue2:       "FL2",
  // European club
  ucl:          "CL",
  europa:       "EL",
  // International
  euros:        "EC",
  worldcup:     "WC",
  // Other
  mls:          "MLS",
};

// Sports that are tournaments (not annual rolling leagues); they only have
// data for specific calendar years (WC: 2022, EC: 2020/2024, etc.).
// The importer will silently skip 404s for seasons without a tournament.
const TOURNAMENT_SPORTS = new Set(["worldcup", "euros"]);

const ALL_ENDPOINTS = [
  "matches",
  "standings",
  "teams",
  "scorers",
  "head2head",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── Auth helpers ────────────────────────────────────────────

function getApiKey(): string | undefined {
  return process.env.FOOTBALLDATA_API_KEY
    ?? process.env.FOOTBALLDATA_TOKEN
    ?? process.env.FOOTBALL_DATA_API_KEY;
}

function authHeaders(): Record<string, string> {
  const key = getApiKey();
  return key ? { "X-Auth-Token": key } : {};
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

// ── Endpoint implementations ────────────────────────────────

async function importMatches(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const compCode = COMPETITION_CODES[sport];
  if (!compCode) return { filesWritten, errors: [`Unknown sport: ${sport}`] };

  const outFile = rawPath(dataDir, NAME, sport, season, "games", "all.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "matches", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = `${BASE_URL}/competitions/${compCode}/matches?season=${season}&limit=500`;
  logger.progress(NAME, sport, "matches", `Fetching ${season} matches`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT, {
      headers: authHeaders(),
      timeoutMs: 30_000,
    });

    if (!data?.matches) {
      logger.warn(`matches ${sport}/${season}: no matches in response`, NAME);
      errors.push(`matches/${sport}/${season}: no matches in response`);
      return { filesWritten, errors };
    }

    writeJSON(outFile, {
      league: sport,
      season,
      count: data.matches.length,
      matches: data.matches,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
    logger.progress(NAME, sport, "matches", `Saved ${data.matches.length} matches for ${season}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`matches ${sport}/${season}: ${msg}`, NAME);
    errors.push(`matches/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importStandings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const compCode = COMPETITION_CODES[sport];
  if (!compCode) return { filesWritten, errors: [`Unknown sport: ${sport}`] };

  const outFile = rawPath(dataDir, NAME, sport, season, "standings", "current.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "standings", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = `${BASE_URL}/competitions/${compCode}/standings?season=${season}`;
  logger.progress(NAME, sport, "standings", `Fetching ${season} standings`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT, {
      headers: authHeaders(),
      timeoutMs: 30_000,
    });

    if (!data?.standings) {
      logger.warn(`standings ${sport}/${season}: no standings in response`, NAME);
      errors.push(`standings/${sport}/${season}: no standings in response`);
      return { filesWritten, errors };
    }

    writeJSON(outFile, {
      league: sport,
      season,
      standings: data.standings,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
    logger.progress(NAME, sport, "standings", `Saved standings for ${season}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`standings ${sport}/${season}: ${msg}`, NAME);
    errors.push(`standings/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importTeams(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const compCode = COMPETITION_CODES[sport];
  if (!compCode) return { filesWritten, errors: [`Unknown sport: ${sport}`] };

  const outFile = rawPath(dataDir, NAME, sport, season, "teams", "all.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "teams", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = `${BASE_URL}/competitions/${compCode}/teams?season=${season}`;
  logger.progress(NAME, sport, "teams", `Fetching ${season} teams`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT, {
      headers: authHeaders(),
      timeoutMs: 30_000,
    });

    if (!data?.teams) {
      logger.warn(`teams ${sport}/${season}: no teams in response`, NAME);
      errors.push(`teams/${sport}/${season}: no teams in response`);
      return { filesWritten, errors };
    }

    writeJSON(outFile, {
      league: sport,
      season,
      count: data.teams.length,
      teams: data.teams,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
    logger.progress(NAME, sport, "teams", `Saved ${data.teams.length} teams for ${season}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`teams ${sport}/${season}: ${msg}`, NAME);
    errors.push(`teams/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importScorers(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const compCode = COMPETITION_CODES[sport];
  if (!compCode) return { filesWritten, errors: [`Unknown sport: ${sport}`] };

  const outFile = rawPath(dataDir, NAME, sport, season, "stats", "top-scorers.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "scorers", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = `${BASE_URL}/competitions/${compCode}/scorers?season=${season}&limit=100`;
  logger.progress(NAME, sport, "scorers", `Fetching ${season} scorers`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT, {
      headers: authHeaders(),
      timeoutMs: 30_000,
    });

    if (!data?.scorers) {
      logger.warn(`scorers ${sport}/${season}: no scorers in response`, NAME);
      errors.push(`scorers/${sport}/${season}: no scorers in response`);
      return { filesWritten, errors };
    }

    writeJSON(outFile, {
      league: sport,
      season,
      count: data.scorers.length,
      scorers: data.scorers,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
    logger.progress(NAME, sport, "scorers", `Saved ${data.scorers.length} scorers for ${season}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`scorers ${sport}/${season}: ${msg}`, NAME);
    errors.push(`scorers/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

// ── Head-to-head ────────────────────────────────────────────
// Reads match IDs from an already-fetched games/all.json and
// fetches /v4/matches/{id}/head2head?limit=10 for each FINISHED
// match that hasn't been cached yet.  Due to the free-tier rate
// limit this is intentionally incremental — re-run to fill gaps.

async function importHead2Head(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const matchesFile = rawPath(dataDir, NAME, sport, season, "games", "all.json");
  if (!fileExists(matchesFile)) {
    logger.progress(NAME, sport, "head2head", `Skipping ${season} — games/all.json not yet fetched`);
    return { filesWritten, errors };
  }

  const allMatches = readJSON<any>(matchesFile);
  const matches: any[] = allMatches?.matches ?? [];
  const finished = matches.filter((m: any) => m.status === "FINISHED");

  if (finished.length === 0) {
    logger.progress(NAME, sport, "head2head", `No finished matches for ${season}`);
    return { filesWritten, errors };
  }

  logger.progress(
    NAME, sport, "head2head",
    `Fetching h2h for ${finished.length} finished matches in ${season} (incremental)`,
  );

  for (const m of finished) {
    const mid = String(m.id);
    const outFile = rawPath(dataDir, NAME, sport, season, "head2head", `${mid}.json`);
    if (fileExists(outFile)) continue;

    if (dryRun) { filesWritten++; continue; }

    const url = `${BASE_URL}/matches/${mid}/head2head?limit=10`;
    try {
      const data = await fetchJSON<any>(url, NAME, RATE_LIMIT, {
        headers: authHeaders(),
        timeoutMs: 20_000,
      });
      writeJSON(outFile, {
        matchId: mid,
        sport,
        season,
        headToHead: data?.headToHead ?? null,
        aggregates: data?.aggregates ?? null,
        matches: data?.matches ?? [],
        fetchedAt: new Date().toISOString(),
      });
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`head2head/${sport}/${season}/${mid}: ${msg}`);
    }
  }

  logger.progress(NAME, sport, "head2head", `Saved ${filesWritten} h2h files for ${season}`);
  return { filesWritten, errors };
}

// ── Endpoint dispatch map ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  matches: importMatches,
  standings: importStandings,
  teams: importTeams,
  scorers: importScorers,
  head2head: importHead2Head,
};

// ── Provider implementation ─────────────────────────────────

const footballdata: Provider = {
  name: NAME,
  label: "football-data.org",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: [...ALL_ENDPOINTS],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    if (!getApiKey()) {
      logger.warn("No API key found (FOOTBALLDATA_API_KEY). Requests may be limited.", NAME);
    }

    const sports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    const endpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) => ALL_ENDPOINTS.includes(e as Endpoint)) as Endpoint[])
      : [...ALL_ENDPOINTS];

    logger.info(
      `Starting import — ${sports.length} leagues, ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      for (const season of opts.seasons) {
        // Tournament sports (WC, EC) only happen in specific years — skip
        // non-tournament years rather than making API calls that return 404.
        if (TOURNAMENT_SPORTS.has(sport)) {
          const isTournamentYear =
            (sport === "worldcup" && [2018, 2022, 2026].includes(season)) ||
            (sport === "euros" && [2016, 2020, 2021, 2024].includes(season));
          if (!isTournamentYear) {
            logger.progress(NAME, sport, "skip", `Season ${season} — no tournament held`);
            continue;
          }
        }

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

export default footballdata;
