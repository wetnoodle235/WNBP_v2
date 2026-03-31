// ──────────────────────────────────────────────────────────
// V5.0 College Football Data Provider
// ──────────────────────────────────────────────────────────
// Fetches games, stats, play-by-play, rankings, recruiting,
// and ratings from the College Football Data API.
// Requires CFB_DATA_KEY environment variable.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "cfbdata";
const BASE_URL = "https://api.collegefootballdata.com";
const API_KEY = process.env.CFB_DATA_KEY ?? "";

// ~5 req/sec
const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["ncaaf"];

const ALL_ENDPOINTS = [
  "games",
  "games_teams",
  "games_players",
  "plays",
  "stats_season",
  "stats_advanced",
  "rankings",
  "recruiting",
  "talent",
  "ratings_sp",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── Fetch helper ────────────────────────────────────────────

async function cfbFetch<T = unknown>(path: string, params: Record<string, string | number> = {}): Promise<T> {
  if (!API_KEY) {
    throw new Error("CFB_DATA_KEY environment variable is required");
  }

  const url = new URL(path, BASE_URL);
  for (const [k, v] of Object.entries(params)) {
    url.searchParams.set(k, String(v));
  }

  return fetchJSON<T>(url.toString(), NAME, RATE_LIMIT, {
    headers: { Authorization: `Bearer ${API_KEY}` },
  });
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

async function importGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/games", { year: season, seasonType: "regular" });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games", `Saved games`);
  return { filesWritten: 1, errors: [] };
}

async function importGamesTeams(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games_teams.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games_teams", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games_teams", `Fetching ${season} team game stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/games/teams", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games_teams", `Saved team game stats`);
  return { filesWritten: 1, errors: [] };
}

async function importGamesPlayers(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games_players.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games_players", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games_players", `Fetching ${season} player game stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/games/players", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games_players", `Saved player game stats`);
  return { filesWritten: 1, errors: [] };
}

async function importPlays(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "plays", `Fetching ${season} play-by-play (weeks 1-15)`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  for (let week = 1; week <= 15; week++) {
    const outFile = rawPath(dataDir, NAME, sport, season, "plays", `week_${week}.json`);
    if (fileExists(outFile)) {
      logger.progress(NAME, sport, "plays", `Skipping week ${week} — already exists`);
      continue;
    }

    try {
      const data = await cfbFetch("/plays", {
        year: season,
        week,
        seasonType: "regular",
      });
      writeJSON(outFile, data);
      filesWritten++;
      logger.progress(NAME, sport, "plays", `Saved week ${week}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn(`plays week ${week}: ${msg}`, NAME);
      errors.push(`plays/week_${week}: ${msg}`);
    }
  }

  return { filesWritten, errors };
}

async function importStatsSeason(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "stats_season.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "stats_season", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "stats_season", `Fetching ${season} season stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/stats/season", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "stats_season", `Saved season stats`);
  return { filesWritten: 1, errors: [] };
}

async function importStatsAdvanced(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "stats_advanced.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "stats_advanced", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "stats_advanced", `Fetching ${season} advanced stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/stats/season/advanced", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "stats_advanced", `Saved advanced stats`);
  return { filesWritten: 1, errors: [] };
}

async function importRankings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "rankings.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "rankings", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "rankings", `Fetching ${season} rankings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/rankings", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "rankings", `Saved rankings`);
  return { filesWritten: 1, errors: [] };
}

async function importRecruiting(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "recruiting.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "recruiting", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "recruiting", `Fetching ${season} recruiting`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/recruiting/players", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "recruiting", `Saved recruiting data`);
  return { filesWritten: 1, errors: [] };
}

async function importTalent(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "talent.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "talent", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "talent", `Fetching ${season} talent composite`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/talent", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "talent", `Saved talent composite`);
  return { filesWritten: 1, errors: [] };
}

async function importRatingsSp(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "ratings_sp.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "ratings_sp", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "ratings_sp", `Fetching ${season} SP+ ratings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/ratings/sp", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "ratings_sp", `Saved SP+ ratings`);
  return { filesWritten: 1, errors: [] };
}

// ── Endpoint dispatch map ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  games: importGames,
  games_teams: importGamesTeams,
  games_players: importGamesPlayers,
  plays: importPlays,
  stats_season: importStatsSeason,
  stats_advanced: importStatsAdvanced,
  rankings: importRankings,
  recruiting: importRecruiting,
  talent: importTalent,
  ratings_sp: importRatingsSp,
};

// ── Provider implementation ─────────────────────────────────

const cfbdata: Provider = {
  name: NAME,
  label: "College Football Data",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: [...ALL_ENDPOINTS],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    if (!API_KEY) {
      logger.error("CFB_DATA_KEY not set — skipping", NAME);
      return {
        provider: NAME,
        sport: "ncaaf",
        filesWritten: 0,
        errors: ["CFB_DATA_KEY environment variable is required"],
        durationMs: 0,
      };
    }

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
      `Starting import — ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
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

export default cfbdata;
