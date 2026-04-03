// ──────────────────────────────────────────────────────────
// Retrosheet MLB Game Logs Provider
// ──────────────────────────────────────────────────────────
// Downloads official Retrosheet game logs from retrosheet.org.
// Each year is a ZIP containing a single CSV with 161 fields
// covering every MLB game: scores, hits, errors, attendance,
// weather, umpires, and play-by-play summaries.
// No API key required.

import path from "node:path";
import fs from "node:fs";
import { createWriteStream } from "node:fs";
import https from "node:https";
import { createGunzip } from "node:zlib";
import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import { parse as parseCsv } from "csv-parse/sync";

const NAME = "retrosheet";
const BASE = "https://www.retrosheet.org/gamelogs";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 2_000 };

const SUPPORTED_SPORTS: Sport[] = ["mlb"];

// Game log field names (Retrosheet order — 161 fields)
const GAMELOG_FIELDS = [
  "date","game_num","day_of_week","visitor","visitor_league","visitor_game_num",
  "home","home_league","home_game_num","visitor_score","home_score","game_outs",
  "day_night","completion","forfeit","protest","park","attendance","duration",
  "visitor_line_score","home_line_score",
  "visitor_ab","visitor_h","visitor_2b","visitor_3b","visitor_hr","visitor_rbi",
  "visitor_sh","visitor_sf","visitor_hbp","visitor_bb","visitor_ibb","visitor_k",
  "visitor_sb","visitor_cs","visitor_gdp","visitor_ci","visitor_lob",
  "visitor_pitchers","visitor_er","visitor_ter","visitor_wp","visitor_balks",
  "visitor_po","visitor_a","visitor_e","visitor_passed","visitor_dp","visitor_tp",
  "home_ab","home_h","home_2b","home_3b","home_hr","home_rbi",
  "home_sh","home_sf","home_hbp","home_bb","home_ibb","home_k",
  "home_sb","home_cs","home_gdp","home_ci","home_lob",
  "home_pitchers","home_er","home_ter","home_wp","home_balks",
  "home_po","home_a","home_e","home_passed","home_dp","home_tp",
  "umpire_hp","umpire_1b","umpire_2b","umpire_3b","umpire_lf","umpire_rf",
  "visitor_manager","home_manager",
  "winning_pitcher","losing_pitcher","save_pitcher",
  "gwrbi",
  "visitor_starter","visitor_starter_id",
  "home_starter","home_starter_id",
  "additional_info","acquisition_info",
];

/** Parse Retrosheet game log CSV (no header row) */
function parseGameLog(csv: string, season: number): Record<string, string>[] {
  try {
    const rows = parseCsv(csv, {
      columns: false,
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true,
    }) as string[][];

    return rows
      .filter((row) => String(row[0] ?? "").startsWith(String(season).slice(0, 3)))
      .map((row) => {
        const obj: Record<string, string> = {};
        GAMELOG_FIELDS.forEach((field, i) => {
          obj[field] = row[i] ?? "";
        });
        return obj;
      });
  } catch {
    return [];
  }
}

async function downloadZip(year: number, destPath: string): Promise<void> {
  const url = `${BASE}/gl${year}.zip`;

  return new Promise((resolve, reject) => {
    https.get(url, { timeout: 30_000 }, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      const out = createWriteStream(destPath);
      res.pipe(out);
      out.on("finish", () => { out.close(); resolve(); });
      out.on("error", reject);
    }).on("error", reject);
  });
}

async function importYear(
  season: number,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  if (dryRun) return { filesWritten: 0, errors: [] };

  const outPath = rawPath(dataDir, NAME, "mlb", season, `gamelogs_${season}.json`);
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };

  const tmpDir = path.join(dataDir, "_tmp_retrosheet");
  fs.mkdirSync(tmpDir, { recursive: true });
  const zipPath = path.join(tmpDir, `gl${season}.zip`);

  try {
    await downloadZip(season, zipPath);

    // Unzip in-memory using Node's built-in AdmZip-free approach
    const AdmZip = (await import("adm-zip")).default;
    const zip = new AdmZip(zipPath);
    const entry = zip.getEntries().find((e) => e.entryName.endsWith(".txt"));
    if (!entry) throw new Error(`No .txt file found in gl${season}.zip`);

    const csv = entry.getData().toString("utf-8");
    const rows = parseGameLog(csv, season);

    writeJSON(outPath, {
      source: NAME,
      sport: "mlb",
      season: String(season),
      count: rows.length,
      fields: GAMELOG_FIELDS,
      games: rows,
      fetched_at: new Date().toISOString(),
    });

    logger.progress(NAME, "mlb", "gamelogs", `${rows.length} games (${season})`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    const msg = `gamelogs/${season}: ${err instanceof Error ? err.message : String(err)}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  } finally {
    try { fs.unlinkSync(zipPath); } catch { /* ignore */ }
  }
}

const retrosheet: Provider = {
  name: NAME,
  label: "Retrosheet MLB Game Logs",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["gamelogs"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    if (!activeSports.includes("mlb")) {
      return { provider: NAME, sport: "mlb", filesWritten: 0, errors: [], durationMs: 0 };
    }

    for (const season of opts.seasons) {
      const r = await importYear(season, opts.dataDir, opts.dryRun);
      totalFiles += r.filesWritten;
      allErrors.push(...r.errors);
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

export default retrosheet;
