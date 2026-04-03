// ──────────────────────────────────────────────────────────
// Baseball Savant (Statcast) Provider
// ──────────────────────────────────────────────────────────
// Free, no-key-required Statcast leaderboard data from
// baseballsavant.mlb.com: season-level batting and pitching
// advanced metrics (exit velocity, barrel%, hard hit%,
// xwOBA, xBA, xSLG, whiff%, etc.)
//
// Data is fetched as CSV and converted to JSON.
// Output layout: data/raw/statcast/mlb/{season}/batters.json
//                data/raw/statcast/mlb/{season}/pitchers.json

import { parse as parseCsv } from "csv-parse/sync";
import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchCSV } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "statcast";
const BASE = "https://baseballsavant.mlb.com";
const SPORTS: readonly Sport[] = ["mlb"] as const;

// Savant is generous but can rate-limit scrapers; 6 req/min is safe.
const RATE_LIMIT: RateLimitConfig = { requests: 6, perMs: 60_000 };

const ENDPOINTS = ["batting_leaderboard", "pitching_leaderboard"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

// ── Leaderboard column selections ───────────────────────────

const BATTER_COLS = [
  "pa", "ab", "hit", "single", "double", "triple", "home_run",
  "strikeout", "walk",
  "k_percent", "bb_percent",
  "batting_avg", "slg_percent", "on_base_percent", "on_base_plus_slg",
  "isolated_power",
  "babip",
  "woba", "xwoba",
  "xba", "xslg", "xiso",
  "exit_velocity_avg", "launch_angle_avg",
  "sweet_spot_percent", "barrel_batted_rate", "hard_hit_percent",
  "sprint_speed",
  "avg_best_speed",
].join(",");

const PITCHER_COLS = [
  "pa", "ab", "hit",
  "strikeout", "walk",
  "k_percent", "bb_percent",
  "batting_avg", "slg_percent", "on_base_percent",
  "woba", "xwoba",
  "xba", "xslg", "xiso",
  "exit_velocity_avg", "launch_angle_avg",
  "sweet_spot_percent", "barrel_batted_rate", "hard_hit_percent",
  "whiff_percent", "put_away",
  "avg_best_speed",
].join(",");

// ── CSV → JSON conversion ────────────────────────────────────

function csvToRecords(csvText: string): Record<string, unknown>[] {
  try {
    const rows = parseCsv(csvText, {
      columns: true,        // Use first row as header
      skip_empty_lines: true,
      trim: true,
      cast: true,           // Auto-cast numbers/booleans
    }) as Record<string, unknown>[];
    return rows;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`CSV parse error: ${msg}`, NAME);
    return [];
  }
}

// ── Endpoint implementations ────────────────────────────────

async function importBattingLeaderboard(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const outPath = rawPath(dataDir, NAME, "mlb", season, "batters.json");
  const currentYear = new Date().getFullYear();
  const isCompletedSeason = season < currentYear;

  // Skip completed seasons if file already exists (stats are final).
  // Always re-fetch the current season since stats accumulate through the year.
  if (isCompletedSeason && fileExists(outPath)) {
    logger.debug(`[skip] batters.json already exists for completed season ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  if (dryRun) {
    logger.info(`[dry-run] Would fetch batting leaderboard for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const url =
    `${BASE}/leaderboard/custom` +
    `?year=${season}` +
    `&type=batter` +
    `&filter=` +
    `&min=10` +         // Min 10 PA — filters out very small samples
    `&selections=${BATTER_COLS}` +
    `&chart=false` +
    `&csv=true`;

  logger.progress(NAME, "mlb", "batting_leaderboard", `Fetching ${season} batter leaderboard`);

  const csvText = await fetchCSV(url, NAME, RATE_LIMIT);
  if (!csvText || !csvText.trim()) {
    const msg = `Empty CSV response for batting leaderboard ${season}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }

  const records = csvToRecords(csvText);
  if (records.length === 0) {
    const msg = `No records parsed from batting leaderboard ${season}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }

  // Normalise key names to snake_case and add metadata
  const enriched = records.map((r) => ({ ...r, season, player_type: "batter", source: NAME }));
  writeJSON(outPath, enriched);

  logger.progress(NAME, "mlb", "batting_leaderboard", `Wrote ${records.length} batters for ${season}`);
  return { filesWritten: 1, errors: [] };
}

async function importPitchingLeaderboard(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const outPath = rawPath(dataDir, NAME, "mlb", season, "pitchers.json");
  const currentYear = new Date().getFullYear();
  const isCompletedSeason = season < currentYear;

  if (isCompletedSeason && fileExists(outPath)) {
    logger.debug(`[skip] pitchers.json already exists for completed season ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  if (dryRun) {
    logger.info(`[dry-run] Would fetch pitching leaderboard for ${season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const url =
    `${BASE}/leaderboard/custom` +
    `?year=${season}` +
    `&type=pitcher` +
    `&filter=` +
    `&min=5` +         // Min 5 PA faced
    `&selections=${PITCHER_COLS}` +
    `&chart=false` +
    `&csv=true`;

  logger.progress(NAME, "mlb", "pitching_leaderboard", `Fetching ${season} pitcher leaderboard`);

  const csvText = await fetchCSV(url, NAME, RATE_LIMIT);
  if (!csvText || !csvText.trim()) {
    const msg = `Empty CSV response for pitching leaderboard ${season}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }

  const records = csvToRecords(csvText);
  if (records.length === 0) {
    const msg = `No records parsed from pitching leaderboard ${season}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }

  const enriched = records.map((r) => ({ ...r, season, player_type: "pitcher", source: NAME }));
  writeJSON(outPath, enriched);

  logger.progress(NAME, "mlb", "pitching_leaderboard", `Wrote ${records.length} pitchers for ${season}`);
  return { filesWritten: 1, errors: [] };
}

// ── Provider definition ─────────────────────────────────────

const ENDPOINT_FN: Record<Endpoint, (season: number, dataDir: string, dryRun: boolean) => Promise<{ filesWritten: number; errors: string[] }>> = {
  batting_leaderboard: importBattingLeaderboard,
  pitching_leaderboard: importPitchingLeaderboard,
};

const statcast: Provider = {
  name: NAME,
  label: "Baseball Savant (Statcast)",
  sports: [...SPORTS],
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: [...ENDPOINTS],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    const endpointsToRun: Endpoint[] =
      opts.endpoints.length > 0
        ? (opts.endpoints.filter((e) => (ENDPOINTS as readonly string[]).includes(e)) as Endpoint[])
        : [...ENDPOINTS];

    for (const season of opts.seasons) {
      for (const endpoint of endpointsToRun) {
        try {
          const result = await ENDPOINT_FN[endpoint](season, opts.dataDir, opts.dryRun);
          filesWritten += result.filesWritten;
          errors.push(...result.errors);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.error(`${endpoint} ${season} failed: ${msg}`, NAME);
          errors.push(`${endpoint}/${season}: ${msg}`);
        }
      }
    }

    return {
      provider: NAME,
      sport: "mlb",
      filesWritten,
      errors,
      durationMs: Date.now() - start,
    };
  },
};

export default statcast;
