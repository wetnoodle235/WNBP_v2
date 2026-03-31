// ──────────────────────────────────────────────────────────
// V5.0 Understat Provider  [DISABLED]
// ──────────────────────────────────────────────────────────
// Scrapes xG (expected goals) data from understat.com.
// Parses JSON embedded in HTML <script> tags.
// No API key required.
//
// DISABLED (2026-03-26): Understat moved to a JS-rendered SPA.
// The embedded JSON variables (datesData, teamsData, playersData)
// are no longer present in the server-rendered HTML. Data is now
// loaded dynamically via client-side JavaScript. Fixing requires
// either headless browser scraping or reverse-engineering the
// internal API endpoints.

import { parse as parseHTML } from "node-html-parser";
import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchText } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "understat";
const BASE_URL = "https://understat.com";

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 2_000 };

const SUPPORTED_SPORTS: Sport[] = ["epl", "laliga", "bundesliga", "seriea", "ligue1"];

const ALL_ENDPOINTS = [
  "league_standings",
  "league_matches",
  "player_xg",
  "team_xg",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

/** Map our sport slugs to Understat's league path segments */
const LEAGUE_MAP: Record<string, string> = {
  epl: "EPL",
  laliga: "La_Liga",
  bundesliga: "Bundesliga",
  seriea: "Serie_A",
  ligue1: "Ligue_1",
};

// ── HTML/JSON Extraction ────────────────────────────────────

/**
 * Understat embeds data in script tags as:
 *   var datesData = JSON.parse('...')
 *   var teamsData  = JSON.parse('...')
 *   var playersData = JSON.parse('...')
 * The encoded string uses hex escapes (\xHH) for special chars.
 */
function extractEmbeddedJSON(html: string, varName: string): unknown | null {
  // Pattern: var {varName} = JSON.parse('...')
  const regex = new RegExp(`var\\s+${varName}\\s*=\\s*JSON\\.parse\\('(.+?)'\\)`, "s");
  const match = html.match(regex);
  if (!match?.[1]) return null;

  // Decode hex escapes like \x27 → ', \x22 → ", etc.
  const decoded = match[1].replace(/\\x([0-9a-fA-F]{2})/g, (_m, hex) =>
    String.fromCharCode(parseInt(hex, 16)),
  );

  try {
    return JSON.parse(decoded);
  } catch {
    return null;
  }
}

/** Extract all match IDs from the datesData structure */
function extractMatchIds(datesData: unknown): string[] {
  if (!Array.isArray(datesData)) return [];
  const ids: string[] = [];
  for (const entry of datesData) {
    if (entry && typeof entry === "object" && "id" in entry) {
      ids.push(String(entry.id));
    }
  }
  return ids;
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

// ── Fetch league page and extract all embedded data ─────────

interface LeaguePageData {
  datesData: unknown;
  teamsData: unknown;
  playersData: unknown;
}

async function fetchLeaguePage(sport: Sport, season: number): Promise<{ html: string; data: LeaguePageData }> {
  const league = LEAGUE_MAP[sport];
  if (!league) throw new Error(`Unknown league mapping for sport: ${sport}`);

  const url = `${BASE_URL}/league/${league}/${season}`;
  logger.progress(NAME, sport, "fetch", `Fetching league page: ${url}`);

  const html = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 30_000 });

  return {
    html,
    data: {
      datesData: extractEmbeddedJSON(html, "datesData"),
      teamsData: extractEmbeddedJSON(html, "teamsData"),
      playersData: extractEmbeddedJSON(html, "playersData"),
    },
  };
}

// ── Endpoint implementations ────────────────────────────────

async function importLeagueStandings(ctx: EndpointContext, pageData?: LeaguePageData): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "league_standings.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "league_standings", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = pageData ?? (await fetchLeaguePage(sport, season)).data;

    if (!data.teamsData) {
      errors.push(`league_standings/${sport}/${season}: No teamsData found in page`);
      return { filesWritten, errors };
    }

    writeJSON(outFile, data.teamsData);
    filesWritten++;
    logger.progress(NAME, sport, "league_standings", `Saved ${season} standings`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`league_standings ${sport}/${season}: ${msg}`, NAME);
    errors.push(`league_standings/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importLeagueMatches(ctx: EndpointContext, pageData?: LeaguePageData): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "league_matches.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "league_matches", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = pageData ?? (await fetchLeaguePage(sport, season)).data;

    if (!data.datesData) {
      errors.push(`league_matches/${sport}/${season}: No datesData found in page`);
      return { filesWritten, errors };
    }

    writeJSON(outFile, data.datesData);
    filesWritten++;
    logger.progress(NAME, sport, "league_matches", `Saved ${season} matches`);

    // Also fetch per-match xG data for a sample of matches
    const matchIds = extractMatchIds(data.datesData);
    logger.progress(NAME, sport, "league_matches", `Found ${matchIds.length} matches for ${season}`);

    // Fetch individual match details
    for (const matchId of matchIds) {
      const matchFile = rawPath(dataDir, NAME, sport, season, "matches", `${matchId}.json`);
      if (fileExists(matchFile)) continue;

      try {
        const matchUrl = `${BASE_URL}/match/${matchId}`;
        const matchHtml = await fetchText(matchUrl, NAME, RATE_LIMIT, { timeoutMs: 30_000 });
        const matchData = extractEmbeddedJSON(matchHtml, "match_info");
        const shotsData = extractEmbeddedJSON(matchHtml, "shotsData");
        const rostersData = extractEmbeddedJSON(matchHtml, "rostersData");

        writeJSON(matchFile, {
          matchId,
          match_info: matchData,
          shotsData,
          rostersData,
        });
        filesWritten++;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn(`match ${matchId}: ${msg}`, NAME);
        errors.push(`league_matches/${sport}/${season}/match/${matchId}: ${msg}`);
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`league_matches ${sport}/${season}: ${msg}`, NAME);
    errors.push(`league_matches/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importPlayerXg(ctx: EndpointContext, pageData?: LeaguePageData): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "player_xg.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "player_xg", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = pageData ?? (await fetchLeaguePage(sport, season)).data;

    if (!data.playersData) {
      errors.push(`player_xg/${sport}/${season}: No playersData found in page`);
      return { filesWritten, errors };
    }

    writeJSON(outFile, data.playersData);
    filesWritten++;
    logger.progress(NAME, sport, "player_xg", `Saved ${season} player xG`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`player_xg ${sport}/${season}: ${msg}`, NAME);
    errors.push(`player_xg/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

async function importTeamXg(ctx: EndpointContext, pageData?: LeaguePageData): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  const outFile = rawPath(dataDir, NAME, sport, season, "team_xg.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "team_xg", `Skipping ${season} — already exists`);
    return { filesWritten, errors };
  }

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const data = pageData ?? (await fetchLeaguePage(sport, season)).data;

    if (!data.teamsData) {
      errors.push(`team_xg/${sport}/${season}: No teamsData found in page`);
      return { filesWritten, errors };
    }

    // teamsData has per-team xG breakdowns (home/away, per-match, totals)
    writeJSON(outFile, data.teamsData);
    filesWritten++;
    logger.progress(NAME, sport, "team_xg", `Saved ${season} team xG`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`team_xg ${sport}/${season}: ${msg}`, NAME);
    errors.push(`team_xg/${sport}/${season}: ${msg}`);
  }

  return { filesWritten, errors };
}

// ── Provider implementation ─────────────────────────────────

const understat: Provider = {
  name: NAME,
  label: "Understat (xG)",
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
      `Starting import — ${sports.length} leagues, ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── ${LEAGUE_MAP[sport] ?? sport} ${season} ──`, NAME);

        // Fetch the league page once per sport/season and share across endpoints
        let pageData: LeaguePageData | undefined;
        if (!opts.dryRun) {
          try {
            const result = await fetchLeaguePage(sport, season);
            pageData = result.data;
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`Failed to fetch league page for ${sport}/${season}: ${msg}`, NAME);
            allErrors.push(`${sport}/${season}: ${msg}`);
            continue;
          }
        }

        for (const ep of endpoints) {
          try {
            const ctx: EndpointContext = {
              sport,
              season,
              dataDir: opts.dataDir,
              dryRun: opts.dryRun,
            };

            let result: EndpointResult;
            switch (ep) {
              case "league_standings":
                result = await importLeagueStandings(ctx, pageData);
                break;
              case "league_matches":
                result = await importLeagueMatches(ctx, pageData);
                break;
              case "player_xg":
                result = await importPlayerXg(ctx, pageData);
                break;
              case "team_xg":
                result = await importTeamXg(ctx, pageData);
                break;
            }

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

export default understat;
