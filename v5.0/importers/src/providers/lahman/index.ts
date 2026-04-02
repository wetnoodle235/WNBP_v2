// ──────────────────────────────────────────────────────────
// Lahman (Chadwick Bureau Baseball Databank)
// ──────────────────────────────────────────────────────────
// Historical MLB data — Batting, Pitching, Fielding, etc.
// Source: SABR Lahman comma-delimited release (Box share)

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchCSV, fetchText } from "../../core/http.js";
import { rawPath, writeText, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import fs from "node:fs";

const NAME = "lahman";
const SHARE_NAME = "y1prhc795jk8zvmelfd3jq7tl389y6cd";
const BOX_FOLDER_URL = `https://sabr.app.box.com/s/${SHARE_NAME}`;
const BOX_DOWNLOAD_URL = `https://sabr.app.box.com/index.php?rm=box_download_shared_file&shared_name=${SHARE_NAME}`;
const SPORTS: readonly Sport[] = ["mlb"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 10, perMs: 1_000 };
const FRESHNESS_TABLES = ["Teams", "Batting"] as const;
const MAX_FOLDER_PAGES = 10;

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
  return `${BOX_FOLDER_URL}`;
}

function tableCandidates(table: Table): readonly string[] {
  return [table];
}

function tableFileName(table: Table): string {
  return `${table}.csv`;
}

function extractFileIds(html: string): Map<string, string> {
  const files = new Map<string, string>();
  const rowRegex = /"typedID":"f_(\d+)".*?"extension":"([^"]*)","name":"([^"]+)"/gs;
  let match: RegExpExecArray | null;
  while ((match = rowRegex.exec(html)) !== null) {
    const [, fileId, extension, name] = match;
    if ((extension ?? "").toLowerCase() !== "csv") {
      continue;
    }
    files.set(name, fileId);
  }
  return files;
}

async function discoverTableIds(requiredTables: readonly Table[]): Promise<Map<Table, string>> {
  const remaining = new Set<string>(requiredTables.map(tableFileName));
  const resolved = new Map<Table, string>();

  for (let page = 1; page <= MAX_FOLDER_PAGES && remaining.size > 0; page++) {
    const pageUrl = page === 1 ? BOX_FOLDER_URL : `${BOX_FOLDER_URL}?page=${page}`;
    logger.progress(NAME, "mlb", "folder", `Inspecting ${pageUrl}`);
    const html = await fetchText(pageUrl, NAME, RATE_LIMIT, {
      headers: { "Accept": "text/html,*/*" },
      timeoutMs: 60_000,
    });

    const ids = extractFileIds(html);
    if (ids.size === 0) {
      break;
    }

    for (const table of requiredTables) {
      const fileName = tableFileName(table);
      const fileId = ids.get(fileName);
      if (fileId && !resolved.has(table)) {
        resolved.set(table, fileId);
        remaining.delete(fileName);
      }
    }
  }

  return resolved;
}

function maxSeasonYear(filePath: string, yearColumnIndex: number): number | null {
  if (!fileExists(filePath)) return null;

  const lines = fs.readFileSync(filePath, "utf-8").split(/\r?\n/);
  let maxYear: number | null = null;
  for (let idx = 1; idx < lines.length; idx++) {
    const line = lines[idx]?.trim();
    if (!line) continue;
    const cols = line.split(",");
    const year = Number.parseInt(cols[yearColumnIndex] ?? "", 10);
    if (!Number.isNaN(year) && (maxYear === null || year > maxYear)) {
      maxYear = year;
    }
  }
  return maxYear;
}

function validateFreshness(dataDir: string, requestedSeasons: number[]): string | null {
  const teamsPath = rawPath(dataDir, NAME, "mlb", "all", "Teams.csv");
  const battingPath = rawPath(dataDir, NAME, "mlb", "all", "Batting.csv");
  if (!fileExists(teamsPath) || !fileExists(battingPath)) {
    return null;
  }

  const teamsMaxYear = maxSeasonYear(teamsPath, 0);
  const battingMaxYear = maxSeasonYear(battingPath, 1);
  if (teamsMaxYear === null || battingMaxYear === null) {
    return null;
  }

  const currentYear = new Date().getUTCFullYear();
  const requestedLatest = requestedSeasons.length > 0 ? Math.max(...requestedSeasons) : currentYear;
  const requiredLatest = Math.min(currentYear - 1, requestedLatest);
  const observedLatest = Math.min(teamsMaxYear, battingMaxYear);

  if (observedLatest < requiredLatest) {
    return [
      `Lahman source is stale: latest season in downloaded CSVs is ${observedLatest}`,
      `but requested/import context requires at least ${requiredLatest}`,
      `(Teams=${teamsMaxYear}, Batting=${battingMaxYear}).`,
      "Update the provider source before using Lahman for modern MLB normalization.",
    ].join(" ");
  }

  return null;
}

async function fetchTable(
  table: Table,
  fileId: string,
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

  const url = `${BOX_DOWNLOAD_URL}&file_id=f_${fileId}`;
  logger.progress(NAME, "mlb", table, `Fetching ${url}`);
  const csv = await fetchCSV(url, NAME, RATE_LIMIT, {
    headers: { "Accept": "text/csv,application/csv,*/*" },
    timeoutMs: 120_000,
  });
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

    const requiredTables = [...tables];
    let tableIds = new Map<Table, string>();
    if (!opts.dryRun) {
      try {
        tableIds = await discoverTableIds(requiredTables);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error(`Failed to inspect SABR Lahman folder: ${msg}`, NAME);
        allErrors.push(`folder-discovery: ${msg}`);
      }
    }

    for (const table of tables) {
      try {
        if (!opts.dryRun) {
          const fileId = tableIds.get(table);
          if (!fileId) {
            const msg = `Could not locate ${table}.csv in SABR Lahman folder listing`;
            logger.error(msg, NAME);
            allErrors.push(`${table}: ${msg}`);
            continue;
          }
          const result = await fetchTable(table, fileId, opts.dataDir, opts.dryRun);
          totalFiles += result.filesWritten;
          allErrors.push(...result.errors);
          continue;
        }

        const result = await fetchTable(table, "", opts.dataDir, opts.dryRun);
        totalFiles += result.filesWritten;
        allErrors.push(...result.errors);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error(`Failed to fetch ${table}: ${msg}`, NAME);
        allErrors.push(`${table}: ${msg}`);
      }
    }

    if (!opts.dryRun) {
      const includesCoreTables = FRESHNESS_TABLES.every((table) => tables.has(table));
      if (includesCoreTables) {
        const freshnessError = validateFreshness(opts.dataDir, opts.seasons);
        if (freshnessError) {
          logger.error(freshnessError, NAME);
          allErrors.push(freshnessError);
        }
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
