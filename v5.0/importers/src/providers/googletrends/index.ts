// ──────────────────────────────────────────────────────────
// Google Trends Provider
// ──────────────────────────────────────────────────────────
// Fetches search interest data from Google Trends via pytrends
// (Python library). Spawns a Python subprocess to avoid the
// 429 rate-limiting that blocks direct API access.
// No API key required.

import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const execFileAsync = promisify(execFile);

const NAME = "googletrends";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 5_000 };

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// Path: importers/src/providers/googletrends/ → root is ../../../../
const SCRIPT_PATH = path.resolve(__dirname, "../../../../scripts/googletrends_fetch.py");

/** Search queries that represent each sport on Google */
const SPORT_QUERIES: Partial<Record<Sport, string>> = {
  nba: "NBA",
  nfl: "NFL",
  mlb: "MLB",
  nhl: "NHL",
  ncaab: "March Madness NCAA basketball",
  ncaaf: "college football",
  wnba: "WNBA",
  epl: "Premier League",
  laliga: "La Liga",
  bundesliga: "Bundesliga",
  seriea: "Serie A",
  ligue1: "Ligue 1",
  mls: "MLS soccer",
  ucl: "Champions League",
  f1: "Formula 1",
  ufc: "UFC",
  atp: "tennis ATP",
  wta: "tennis WTA",
  golf: "PGA golf",
  lol: "League of Legends esports",
  csgo: "Counter-Strike",
  valorant: "Valorant",
};

const SUPPORTED_SPORTS = Object.keys(SPORT_QUERIES) as Sport[];

interface TrendResult {
  keyword: string;
  timeframe: string;
  geo: string;
  interest_over_time: Array<{ date: string; value: number; partial: boolean }>;
  related_queries: { top: unknown[]; rising: unknown[] };
  trending_today: string[];
  interest_over_time_error?: string;
  related_queries_error?: string;
  trending_today_error?: string;
  error?: string;
}

async function runPytrends(keyword: string): Promise<TrendResult | null> {
  try {
    const { stdout, stderr } = await execFileAsync(
      "/usr/bin/python3",
      [SCRIPT_PATH, `--keyword=${keyword}`, "--timeframe=now 7-d", "--geo=US"],
      { timeout: 60_000 },
    );
    if (stderr && !stderr.includes("FutureWarning")) {
      logger.warn(`pytrends stderr for "${keyword}": ${stderr.slice(0, 200)}`, NAME);
    }
    return JSON.parse(stdout) as TrendResult;
  } catch (err) {
    logger.warn(`pytrends subprocess failed for "${keyword}": ${String(err)}`, NAME);
    return null;
  }
}

async function importInterestOverTime(
  sport: Sport,
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const keyword = SPORT_QUERIES[sport];
  if (!keyword || dryRun) return { filesWritten: 0, errors: [] };

  const result = await runPytrends(keyword);
  if (!result || result.error) {
    const msg = `interest_over_time/${sport}: ${result?.error ?? "subprocess failed"}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }

  const points = result.interest_over_time;
  if (points.length === 0 && result.interest_over_time_error) {
    return { filesWritten: 0, errors: [`interest_over_time/${sport}: ${result.interest_over_time_error}`] };
  }

  const dateStr = new Date().toISOString().slice(0, 10);
  const outPath = rawPath(dataDir, NAME, sport, season, `interest_${dateStr}.json`);
  writeJSON(outPath, {
    source: NAME,
    sport,
    season: String(season),
    keyword,
    geo: "US",
    timeframe: "now 7-d",
    date: dateStr,
    count: points.length,
    data_points: points,
    related_queries: result.related_queries,
    fetched_at: new Date().toISOString(),
  });
  logger.progress(NAME, sport, "interest_over_time", `${points.length} data points`);
  return { filesWritten: 1, errors: [] };
}

const googletrends: Provider = {
  name: NAME,
  label: "Google Trends (via pytrends)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["interest_over_time"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    // Run sports one at a time — pytrends has server-side rate limits
    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        const r = await importInterestOverTime(sport, season, opts.dataDir, opts.dryRun);
        totalFiles += r.filesWritten;
        allErrors.push(...r.errors);
        // Small delay between calls
        if (activeSports.indexOf(sport) < activeSports.length - 1) {
          await new Promise((res) => setTimeout(res, 4_000));
        }
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0]! : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default googletrends;
