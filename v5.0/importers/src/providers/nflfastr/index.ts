// ──────────────────────────────────────────────────────────
// NFLfastR / NFL Advanced Stats Provider
// ──────────────────────────────────────────────────────────
// Downloads Pro Football Reference advanced stats from the
// nflverse-data GitHub releases (pfr_advstats tag).
// Provides CPOE, time-to-throw, pressure rate, yards after
// contact, broken tackles, and defensive pressure stats.
// Free, no API key required.

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { fetchText } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "nflfastr";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 2_000 };
const GH_BASE = "https://github.com/nflverse/nflverse-data/releases/download";

const STAT_TYPES = ["pass", "rush", "rec", "def"] as const;
type StatType = (typeof STAT_TYPES)[number];

function parseCSV(text: string): Record<string, string>[] {
  const lines = text.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = lines[0]!.split(",").map((h) => h.replace(/"/g, "").trim());
  return lines.slice(1).map((line) => {
    const vals = line.split(",");
    return Object.fromEntries(headers.map((h, i) => [h, (vals[i] ?? "").replace(/"/g, "").trim()]));
  });
}

async function fetchAdvStats(
  granularity: "season" | "week",
  statType: StatType,
  season: number,
): Promise<Record<string, string>[]> {
  // Season files: advstats_season_{type}.csv (all seasons combined, filter by year)
  // Weekly files: advstats_week_{type}_{year}.csv (per year)
  const url = granularity === "season"
    ? `${GH_BASE}/pfr_advstats/advstats_season_${statType}.csv`
    : `${GH_BASE}/pfr_advstats/advstats_week_${statType}_${season}.csv`;
  const csv = await fetchText(url, NAME, RATE_LIMIT, {
    headers: { "User-Agent": "sports-data-importer/5.0" },
  });
  const all = parseCSV(csv);
  // Season files have all years; filter to requested season
  return granularity === "season"
    ? all.filter((row) => Number(row.season) === season)
    : all;
}

const nflfastr: Provider = {
  name: NAME,
  label: "NFLfastR Advanced Stats (PFR via nflverse-data)",
  sports: ["nfl" as Sport],
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["advstats_season", "advstats_weekly"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    if (opts.sports.length && !opts.sports.includes("nfl" as Sport)) {
      return { provider: NAME, sport: "nfl" as Sport, filesWritten: 0, errors: [], durationMs: 0 };
    }

    for (const season of opts.seasons) {
      for (const gran of ["season", "week"] as const) {
        for (const statType of STAT_TYPES) {
          try {
            const rows = await fetchAdvStats(gran, statType, season);
            if (rows.length === 0) continue;
            if (!opts.dryRun) {
              const outPath = rawPath(opts.dataDir, NAME, "nfl", season,
                `advstats_${gran}_${statType}.json`);
              writeJSON(outPath, {
                source: NAME, sport: "nfl", season: String(season),
                stat_type: statType, granularity: gran,
                count: rows.length, fetched_at: new Date().toISOString(), data: rows,
              });
              filesWritten++;
            }
            logger.progress(NAME, "nfl", `advstats_${gran}_${statType}`,
              `${rows.length} rows (${season})`);
          } catch (err) {
            const msg = `advstats_${gran}_${statType}/${season}: ${err instanceof Error ? err.message : String(err)}`;
            logger.warn(msg, NAME);
            errors.push(msg);
          }
        }
      }
    }

    return { provider: NAME, sport: "nfl" as Sport, filesWritten, errors, durationMs: Date.now() - start };
  },
};

export default nflfastr;
