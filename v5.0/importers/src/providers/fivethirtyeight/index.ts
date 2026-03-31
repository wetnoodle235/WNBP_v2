// ──────────────────────────────────────────────────────────
// V5.0 FiveThirtyEight Provider
// ──────────────────────────────────────────────────────────
// Fetches ELO ratings, RAPTOR player ratings, and Soccer SPI
// forecasts from the FiveThirtyEight GitHub data archive.
// No API key required — public GitHub raw files + hosted CSVs.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchCSV } from "../../core/http.js";
import { writeText, writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "fivethirtyeight";

const GH_RAW_BASE = "https://raw.githubusercontent.com/fivethirtyeight/data/master";

// GitHub CDN — generous limits
const RATE_LIMIT: RateLimitConfig = { requests: 8, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["nba", "nfl", "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl"];

const ALL_ENDPOINTS = [
  "nba-elo",
  "nba-elo-latest",
  "nba-raptor-player",
  "nba-raptor-team",
  "nfl-elo",
  "soccer-spi-matches",
  "soccer-spi-rankings",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── Sport → endpoint mapping ────────────────────────────────

const SPORT_ENDPOINTS: Record<string, Endpoint[]> = {
  nba: ["nba-elo", "nba-elo-latest", "nba-raptor-player", "nba-raptor-team"],
  nfl: ["nfl-elo"],
  // All soccer sports share the same SPI data
  epl: ["soccer-spi-matches", "soccer-spi-rankings"],
  laliga: ["soccer-spi-matches", "soccer-spi-rankings"],
  bundesliga: ["soccer-spi-matches", "soccer-spi-rankings"],
  seriea: ["soccer-spi-matches", "soccer-spi-rankings"],
  ligue1: ["soccer-spi-matches", "soccer-spi-rankings"],
  mls: ["soccer-spi-matches", "soccer-spi-rankings"],
  ucl: ["soccer-spi-matches", "soccer-spi-rankings"],
};

// ── URL definitions ─────────────────────────────────────────

const ENDPOINT_URLS: Record<Endpoint, string> = {
  "nba-elo":           `${GH_RAW_BASE}/nba-elo/nbaallelo.csv`,
  "nba-elo-latest":    `${GH_RAW_BASE}/nba-forecasts/nba_elo_latest.csv`,
  "nba-raptor-player": `${GH_RAW_BASE}/nba-raptor/modern_RAPTOR_by_player.csv`,
  "nba-raptor-team":   `${GH_RAW_BASE}/nba-raptor/modern_RAPTOR_by_team.csv`,
  "nfl-elo":           "https://projects.fivethirtyeight.com/nfl-api/nfl_elo.csv",
  "soccer-spi-matches":  "https://projects.fivethirtyeight.com/soccer-api/club/spi_matches.csv",
  "soccer-spi-rankings": "https://projects.fivethirtyeight.com/soccer-api/club/spi_global_rankings.csv",
};

// ── CSV parser (no external deps) ───────────────────────────

function splitCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

function parseCSV(csv: string): Record<string, string>[] {
  const lines = csv.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return [];

  const headers = splitCSVLine(lines[0]);
  const rows: Record<string, string>[] = [];

  for (let i = 1; i < lines.length; i++) {
    const values = splitCSVLine(lines[i]);
    if (values.length === 0) continue;
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = values[j] ?? "";
    }
    rows.push(row);
  }

  return rows;
}

// Filter rows matching any of the requested seasons
function filterBySeasons(rows: Record<string, string>[], seasons: number[]): Record<string, string>[] {
  const yearPatterns = seasons.map((s) => new RegExp(`\\b${s}\\b`));
  return rows.filter((row) =>
    Object.values(row).some((v) => yearPatterns.some((re) => re.test(v))),
  );
}

// ── Endpoint context ────────────────────────────────────────

interface EndpointContext {
  sport: Sport;
  seasons: number[];
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

// ── Generic CSV endpoint fetcher ────────────────────────────

async function fetchEndpoint(
  ep: Endpoint,
  ctx: EndpointContext,
): Promise<EndpointResult> {
  const { sport, seasons, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  // Soccer SPI endpoints are shared — only fetch once, store under first soccer sport
  const effectiveSport = ep.startsWith("soccer-") ? "epl" : sport;

  const csvFile = rawPath(dataDir, NAME, effectiveSport, "all", `${ep}.csv`);
  const jsonFile = rawPath(dataDir, NAME, effectiveSport, "all", `${ep}.json`);

  if (fileExists(csvFile) && fileExists(jsonFile)) {
    logger.progress(NAME, sport, ep, `Skipping — already exists`);
    return { filesWritten, errors };
  }

  const url = ENDPOINT_URLS[ep];
  logger.progress(NAME, sport, ep, `Fetching from ${url.slice(0, 80)}…`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  try {
    const csv = await fetchCSV(url, NAME, RATE_LIMIT, { timeoutMs: 120_000 });

    if (!csv || csv.trim().length === 0) {
      logger.warn(`${ep}: empty response`, NAME);
      errors.push(`${ep}: empty response`);
      return { filesWritten, errors };
    }

    // Save raw CSV
    writeText(csvFile, csv);
    filesWritten++;

    // Parse and save filtered JSON
    const allRows = parseCSV(csv);
    const filtered = filterBySeasons(allRows, seasons);

    writeJSON(jsonFile, {
      fetched_at: new Date().toISOString(),
      source: url,
      endpoint: ep,
      total_rows: allRows.length,
      filtered_rows: filtered.length,
      seasons,
      data: filtered,
    });
    filesWritten++;

    logger.progress(NAME, sport, ep, `Saved ${allRows.length} total rows, ${filtered.length} for requested seasons`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`${ep} ${sport}: ${msg}`, NAME);
    errors.push(`${ep}/${sport}: ${msg}`);
  }

  return { filesWritten, errors };
}

// Track which shared endpoints we've already fetched (soccer SPI)
const fetchedEndpoints = new Set<string>();

// ── Provider implementation ─────────────────────────────────

const fivethirtyeight: Provider = {
  name: NAME,
  label: "FiveThirtyEight (538)",
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

    const requestedEndpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) => ALL_ENDPOINTS.includes(e as Endpoint)) as Endpoint[])
      : [...ALL_ENDPOINTS];

    logger.info(
      `Starting import — ${sports.length} sports, ${requestedEndpoints.length} endpoints, seasons ${opts.seasons.join(",")}`,
      NAME,
    );

    // Reset shared-fetch tracking per import run
    fetchedEndpoints.clear();

    for (const sport of sports) {
      const sportEndpoints = SPORT_ENDPOINTS[sport] ?? [];
      const endpoints = sportEndpoints.filter((e) => requestedEndpoints.includes(e));

      if (endpoints.length === 0) {
        logger.info(`No endpoints for ${sport} — skipping`, NAME);
        continue;
      }

      logger.info(`── ${sport.toUpperCase()} ──`, NAME);

      for (const ep of endpoints) {
        // Skip shared endpoints already fetched (e.g., soccer SPI)
        if (fetchedEndpoints.has(ep)) {
          logger.progress(NAME, sport, ep, `Skipping — already fetched for another sport`);
          continue;
        }

        try {
          const ctx: EndpointContext = {
            sport,
            seasons: opts.seasons,
            dataDir: opts.dataDir,
            dryRun: opts.dryRun,
          };
          const result = await fetchEndpoint(ep, ctx);
          totalFiles += result.filesWritten;
          allErrors.push(...result.errors);
          fetchedEndpoints.add(ep);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.error(`${sport}/${ep}: ${msg}`, NAME);
          allErrors.push(`${sport}/${ep}: ${msg}`);
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

export default fivethirtyeight;
