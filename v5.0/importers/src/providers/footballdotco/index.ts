// ──────────────────────────────────────────────────────────
// football-data.co.uk Provider
// ──────────────────────────────────────────────────────────
// Historical soccer match results + closing odds from
// multiple sportsbooks. Free CSV downloads, no key required.
// Covers EPL, Bundesliga, La Liga, Serie A, Ligue 1, and more
// back to ~2000. Odds columns include Bet365, Pinnacle,
// William Hill, Betway, Max/Avg market odds.

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchCSV } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import { parse as parseCsv } from "csv-parse/sync";

const NAME = "footballdotco";
const BASE = "https://www.football-data.co.uk/mmz4281";
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_000 };

/**
 * Season code format: last two digits of start year + last two of end year.
 * e.g. 2023-24 → "2324", 2019-20 → "1920"
 */
function seasonCode(year: number): string {
  const start = String(year).slice(2);
  const end = String(year + 1).slice(2);
  return `${start}${end}`;
}

/** Football-data.co.uk league codes per sport */
const SPORT_LEAGUES: Partial<Record<Sport, Array<{ code: string; label: string }>>> = {
  epl:        [{ code: "E0", label: "premier_league" }],
  bundesliga: [{ code: "D1", label: "bundesliga" }, { code: "D2", label: "2_bundesliga" }],
  laliga:     [{ code: "SP1", label: "la_liga" }, { code: "SP2", label: "segunda" }],
  seriea:     [{ code: "I1", label: "serie_a" }, { code: "I2", label: "serie_b" }],
  ligue1:     [{ code: "F1", label: "ligue_1" }, { code: "F2", label: "ligue_2" }],
  eredivisie: [{ code: "N1", label: "eredivisie" }],
  mls:        [],  // not available on football-data.co.uk
};

const SUPPORTED_SPORTS = (Object.keys(SPORT_LEAGUES) as Sport[]).filter(
  (s) => (SPORT_LEAGUES[s]?.length ?? 0) > 0,
);

function parseCsvSafe(text: string): Record<string, string>[] {
  try {
    return parseCsv(text, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true,
    }) as Record<string, string>[];
  } catch {
    return [];
  }
}

interface EndpointCtx {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
}
interface EndpointResult { filesWritten: number; errors: string[] }

async function importLeague(
  ctx: EndpointCtx,
  code: string,
  label: string,
): Promise<EndpointResult> {
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const sc = seasonCode(ctx.season);
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `${label}.json`);
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };

  const url = `${BASE}/${sc}/${code}.csv`;
  try {
    const csv = await fetchCSV(url, NAME, RATE_LIMIT);
    const rows = parseCsvSafe(csv);
    if (rows.length === 0) return { filesWritten: 0, errors: [] };

    writeJSON(outPath, {
      source: NAME,
      sport: ctx.sport,
      season: String(ctx.season),
      league_code: code,
      league: label,
      count: rows.length,
      matches: rows,
      fetched_at: new Date().toISOString(),
    });
    logger.progress(NAME, ctx.sport, label, `${rows.length} matches (${sc})`);
    return { filesWritten: 1, errors: [] };
  } catch (err) {
    const msg = `${label}/${ctx.season}: ${err instanceof Error ? err.message : String(err)}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }
}

const footballdotco: Provider = {
  name: NAME,
  label: "football-data.co.uk",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["results"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    for (const sport of activeSports) {
      const leagues = SPORT_LEAGUES[sport] ?? [];
      for (const season of opts.seasons) {
        for (const { code, label } of leagues) {
          const r = await importLeague(
            { sport, season, dataDir: opts.dataDir, dryRun: opts.dryRun },
            code,
            label,
          );
          totalFiles += r.filesWritten;
          allErrors.push(...r.errors);
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

export default footballdotco;
