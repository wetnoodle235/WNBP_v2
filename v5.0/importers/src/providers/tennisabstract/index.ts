// ──────────────────────────────────────────────────────────
// V5.0 Tennis Abstract Provider
// ──────────────────────────────────────────────────────────
// Fetches ATP and WTA match/ranking data from Jeff Sackmann's
// open-source tennis datasets hosted on GitHub.
// No API key required — public GitHub CDN.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchCSV } from "../../core/http.js";
import { writeText, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "tennisabstract";

const GITHUB_RAW_BASE = "https://raw.githubusercontent.com/JeffSackmann";
const ATP_REPO = `${GITHUB_RAW_BASE}/tennis_atp/master`;
const WTA_REPO = `${GITHUB_RAW_BASE}/tennis_wta/master`;

// GitHub CDN is very generous
const RATE_LIMIT: RateLimitConfig = { requests: 10, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["atp", "wta"];

const ALL_ENDPOINTS = [
  "matches",
  "rankings",
  "futures",
  "qualies",
  "challengers",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

const ENDPOINTS_BY_SPORT: Record<Sport, Endpoint[]> = {
  // ATP qualifiers/challengers are bundled in the same file
  atp: ["matches", "rankings", "futures", "challengers"],
  // WTA has qual/ITF feed; no dedicated futures/challengers files
  wta: ["matches", "rankings", "qualies"],
};

function endpointSupportedForSport(sport: Sport, endpoint: Endpoint): boolean {
  return ENDPOINTS_BY_SPORT[sport].includes(endpoint);
}

function isNotFoundError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return /\b404\b|not found/i.test(msg);
}

// ── URL builders ────────────────────────────────────────────

function repoBase(sport: Sport): string {
  return sport === "atp" ? ATP_REPO : WTA_REPO;
}

function prefix(sport: Sport): string {
  return sport === "atp" ? "atp" : "wta";
}

function matchesUrl(sport: Sport, year: number): string {
  return `${repoBase(sport)}/${prefix(sport)}_matches_${year}.csv`;
}

function futuresUrl(sport: Sport, year: number): string {
  return `${repoBase(sport)}/${prefix(sport)}_matches_futures_${year}.csv`;
}

function qualiesUrl(sport: Sport, year: number): string {
  return `${repoBase(sport)}/${prefix(sport)}_matches_qual_itf_${year}.csv`;
}

function challengersUrl(sport: Sport, year: number): string {
  // ATP uses "challengers", WTA uses "qual_itf" (already covered by qualies)
  if (sport === "wta") {
    return `${repoBase(sport)}/${prefix(sport)}_matches_qual_itf_${year}.csv`;
  }
  return `${repoBase(sport)}/${prefix(sport)}_matches_qual_chall_${year}.csv`;
}

/**
 * Rankings files are split by decade:
 *   atp_rankings_70s.csv, atp_rankings_80s.csv, ..., atp_rankings_current.csv
 * "current" covers ~2020+.
 */
function rankingsUrl(sport: Sport, year: number): string {
  if (year >= 2020) {
    return `${repoBase(sport)}/${prefix(sport)}_rankings_current.csv`;
  }
  const decade = Math.floor(year / 10) * 10;
  const suffix = `${String(decade).slice(2)}s`;
  return `${repoBase(sport)}/${prefix(sport)}_rankings_${suffix}.csv`;
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

  const outFile = rawPath(dataDir, NAME, sport, season, "matches.csv");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "matches", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = matchesUrl(sport, season);
  logger.progress(NAME, sport, "matches", `Fetching ${season} matches`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const csv = await fetchCSV(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
    writeText(outFile, csv);
    filesWritten++;
    logger.progress(NAME, sport, "matches", `Saved ${season} matches`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (isNotFoundError(err)) {
      logger.info(`No matches file for ${sport}/${season}; skipping`, NAME);
    } else {
      logger.warn(`matches ${sport}/${season}: ${msg}`, NAME);
      errors.push(`matches/${sport}/${season}: ${msg}`);
    }
  }

  return { filesWritten, errors };
}

async function importRankings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const tag = season >= 2020 ? "current" : `${Math.floor(season / 10) * 10}s`;
  const outFile = rawPath(dataDir, NAME, sport, season, `rankings_${tag}.csv`);

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "rankings", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = rankingsUrl(sport, season);
  logger.progress(NAME, sport, "rankings", `Fetching ${season} rankings (${tag})`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const csv = await fetchCSV(url, NAME, RATE_LIMIT, { timeoutMs: 60_000 });
    writeText(outFile, csv);
    filesWritten++;
    logger.progress(NAME, sport, "rankings", `Saved ${season} rankings`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`rankings ${sport}/${season}: ${msg}`, NAME);
    errors.push(`rankings/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importFutures(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (!endpointSupportedForSport(sport, "futures")) {
    return { filesWritten, errors };
  }

  const outFile = rawPath(dataDir, NAME, sport, season, "futures.csv");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "futures", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = futuresUrl(sport, season);
  logger.progress(NAME, sport, "futures", `Fetching ${season} futures`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const csv = await fetchCSV(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
    writeText(outFile, csv);
    filesWritten++;
    logger.progress(NAME, sport, "futures", `Saved ${season} futures`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    // Futures files are missing for some years as Sackmann seasons are published.
    if (isNotFoundError(err)) {
      logger.info(`No futures file for ${sport}/${season}; skipping`, NAME);
    } else {
      logger.warn(`futures ${sport}/${season}: ${msg}`, NAME);
      errors.push(`futures/${sport}/${season}: ${msg}`);
    }
  }

  return { filesWritten, errors };
}

async function importQualies(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (!endpointSupportedForSport(sport, "qualies")) {
    return { filesWritten, errors };
  }

  const outFile = rawPath(dataDir, NAME, sport, season, "qualies.csv");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "qualies", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = qualiesUrl(sport, season);
  logger.progress(NAME, sport, "qualies", `Fetching ${season} qualifying/ITF`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const csv = await fetchCSV(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
    writeText(outFile, csv);
    filesWritten++;
    logger.progress(NAME, sport, "qualies", `Saved ${season} qualies`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (isNotFoundError(err)) {
      logger.info(`No qualies file for ${sport}/${season}; skipping`, NAME);
    } else {
      logger.warn(`qualies ${sport}/${season}: ${msg}`, NAME);
      errors.push(`qualies/${sport}/${season}: ${msg}`);
    }
  }

  return { filesWritten, errors };
}

async function importChallengers(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (!endpointSupportedForSport(sport, "challengers")) {
    return { filesWritten, errors };
  }

  const outFile = rawPath(dataDir, NAME, sport, season, "challengers.csv");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "challengers", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = challengersUrl(sport, season);
  logger.progress(NAME, sport, "challengers", `Fetching ${season} challengers`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const csv = await fetchCSV(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
    writeText(outFile, csv);
    filesWritten++;
    logger.progress(NAME, sport, "challengers", `Saved ${season} challengers`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (isNotFoundError(err)) {
      logger.info(`No challengers file for ${sport}/${season}; skipping`, NAME);
    } else {
      logger.warn(`challengers ${sport}/${season}: ${msg}`, NAME);
      errors.push(`challengers/${sport}/${season}: ${msg}`);
    }
  }

  return { filesWritten, errors };
}

// ── Endpoint dispatch map ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  matches: importMatches,
  rankings: importRankings,
  futures: importFutures,
  qualies: importQualies,
  challengers: importChallengers,
};

// ── Provider implementation ─────────────────────────────────

const tennisabstract: Provider = {
  name: NAME,
  label: "Tennis Abstract (Sackmann)",
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
      `Starting import — ${sports.length} tours, ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── ${sport.toUpperCase()} ${season} ──`, NAME);

        const sportEndpoints = endpoints.filter((ep) => endpointSupportedForSport(sport, ep));

        for (const ep of sportEndpoints) {
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

export default tennisabstract;
