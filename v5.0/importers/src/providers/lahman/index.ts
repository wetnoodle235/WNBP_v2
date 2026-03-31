// ──────────────────────────────────────────────────────────
// Lahman (Chadwick Bureau Baseball Databank)
// ──────────────────────────────────────────────────────────
// Historical MLB data — Batting, Pitching, Fielding, etc.
// Source: https://github.com/seanlahman/baseballdatabank

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchCSV } from "../../core/http.js";
import { rawPath, writeText, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "lahman";
const BASE_URL = "https://raw.githubusercontent.com/seanlahman/baseballdatabank/master/core";
const SPORTS: readonly Sport[] = ["mlb"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 10, perMs: 1_000 };

const TABLES = [
  "Batting",
  "Pitching",
  "Fielding",
  "Teams",
  "People",
  "Appearances",
  "Salaries",
  "AllstarFull",
  "HallOfFame",
  "Managers",
  "Parks",
  "SeriesPost",
] as const;

type Table = (typeof TABLES)[number];

const ENDPOINTS = ["batting", "pitching", "fielding", "teams", "people", "appearances", "salaries", "awards"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

/** Map endpoint names to the tables they pull */
const ENDPOINT_TABLES: Record<Endpoint, readonly Table[]> = {
  batting:      ["Batting"],
  pitching:     ["Pitching"],
  fielding:     ["Fielding"],
  teams:        ["Teams"],
  people:       ["People"],
  appearances:  ["Appearances"],
  salaries:     ["Salaries"],
  awards:       ["AllstarFull", "HallOfFame", "Managers", "Parks", "SeriesPost"],
};

function tableUrl(table: Table): string {
  return `${BASE_URL}/${table}.csv`;
}

async function fetchTable(
  table: Table,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  const outPath = rawPath(dataDir, NAME, "mlb", "all", `${table}.csv`);

  if (fileExists(outPath)) {
    logger.info(`Skipping ${table} — already exists`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  if (dryRun) {
    logger.info(`[dry-run] Would fetch ${table}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const url = tableUrl(table);
  logger.progress(NAME, "mlb", table, `Fetching ${url}`);

  const csv = await fetchCSV(url, NAME, RATE_LIMIT);
  writeText(outPath, csv);
  logger.progress(NAME, "mlb", table, `Wrote ${outPath}`);
  return { filesWritten: 1, errors: [] };
}

const lahman: Provider = {
  name: NAME,
  label: "Lahman Baseball Databank",
  sports: SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS];

    // Collect unique tables from active endpoints
    const tables = new Set<Table>();
    for (const ep of activeEndpoints) {
      for (const t of ENDPOINT_TABLES[ep]) tables.add(t);
    }

    logger.info(`Fetching ${tables.size} table(s): ${[...tables].join(", ")}`, NAME);

    for (const table of tables) {
      try {
        const result = await fetchTable(table, opts.dataDir, opts.dryRun);
        totalFiles += result.filesWritten;
        allErrors.push(...result.errors);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error(`Failed to fetch ${table}: ${msg}`, NAME);
        allErrors.push(`${table}: ${msg}`);
      }
    }

    return {
      provider: NAME,
      sport: "mlb",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default lahman;
