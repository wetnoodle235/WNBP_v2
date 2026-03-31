// ──────────────────────────────────────────────────────────
// V5.0 NFL FaSTR Provider
// ──────────────────────────────────────────────────────────
// Fetches play-by-play, roster, and player stats CSV data
// from the nflverse GitHub releases (nflverse-data).
// No API key required — public GitHub CDN.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchText } from "../../core/http.js";
import { writeText, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "nflfastr";
const RELEASES_BASE = "https://github.com/nflverse/nflverse-data/releases/download";

// GitHub CDN is generous — allow 5 req/sec
const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["nfl"];

const ALL_ENDPOINTS = [
  "play_by_play",
  "rosters",
  "player_stats",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── URL builders ────────────────────────────────────────────

function pbpUrl(year: number): string {
  return `${RELEASES_BASE}/pbp/play_by_play_${year}.csv.gz`;
}

function rosterUrl(year: number): string {
  return `${RELEASES_BASE}/rosters/roster_${year}.csv`;
}

function playerStatsUrl(year: number): string {
  return `${RELEASES_BASE}/player_stats/player_stats_${year}.csv`;
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

async function importPlayByPlay(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "play_by_play.csv.gz");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "play_by_play", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = pbpUrl(season);
  logger.progress(NAME, sport, "play_by_play", `Fetching ${season} PBP from ${url}`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    // PBP files are .csv.gz — fetch as raw binary text
    const data = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 120_000 });
    writeText(outFile, data);
    filesWritten++;
    logger.progress(NAME, sport, "play_by_play", `Saved ${season} play-by-play`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`play_by_play ${season}: ${msg}`, NAME);
    errors.push(`play_by_play/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importRosters(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "rosters.csv");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "rosters", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = rosterUrl(season);
  logger.progress(NAME, sport, "rosters", `Fetching ${season} rosters from ${url}`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 60_000 });
    writeText(outFile, data);
    filesWritten++;
    logger.progress(NAME, sport, "rosters", `Saved ${season} rosters`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`rosters ${season}: ${msg}`, NAME);
    errors.push(`rosters/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importPlayerStats(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "player_stats.csv");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "player_stats", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  const url = playerStatsUrl(season);
  logger.progress(NAME, sport, "player_stats", `Fetching ${season} player stats from ${url}`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 60_000 });
    writeText(outFile, data);
    filesWritten++;
    logger.progress(NAME, sport, "player_stats", `Saved ${season} player stats`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`player_stats ${season}: ${msg}`, NAME);
    errors.push(`player_stats/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

// ── Endpoint dispatch map ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  play_by_play: importPlayByPlay,
  rosters: importRosters,
  player_stats: importPlayerStats,
};

// ── Provider implementation ─────────────────────────────────

const nflfastr: Provider = {
  name: NAME,
  label: "NFL FaSTR (nflverse)",
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

export default nflfastr;
