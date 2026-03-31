// ──────────────────────────────────────────────────────────
// V5.0 Odds Collection Provider
// ──────────────────────────────────────────────────────────
// Unified odds collection: ESPN extraction + OddsAPI snapshots.
// ESPN odds are always free; OddsAPI requires ODDSAPI_KEY.

import type {
  Provider,
  ImportOptions,
  ImportResult,
  Sport,
  RateLimitConfig,
} from "../../core/types.js";
import { logger } from "../../core/logger.js";
import { ODDS_CONFIG, SUPPORTED_SPORTS } from "./config.js";
import { OddsScheduler } from "./scheduler.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "odds";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_000 };

const ALL_ENDPOINTS = [
  "odds",            // alias used by scheduler.py
  "opening",
  "closing",
  "snapshots",
  "espn_extract",
  "espn_player_props",
] as const;

// ── Helpers ─────────────────────────────────────────────────

function todayISO(): string {
  return new Date().toISOString().slice(0, 10);
}

function currentSeason(sport: Sport): number {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;

  const fallSports = new Set<Sport>([
    "nfl", "ncaaf", "nba", "ncaab", "nhl",
    "epl", "laliga", "bundesliga", "seriea", "ligue1",
  ]);

  if (fallSports.has(sport)) {
    return month >= 8 ? year : year - 1;
  }
  return year;
}

// ── Provider ────────────────────────────────────────────────

const oddsProvider: Provider = {
  name: NAME,
  label: "Odds Collector",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ALL_ENDPOINTS as unknown as readonly string[],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const scheduler = new OddsScheduler(opts.dataDir);
    const snapshotType = opts.snapshotType ?? "current";

    const sports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    const endpoints = opts.endpoints.length
      ? opts.endpoints.filter((e) =>
          (ALL_ENDPOINTS as readonly string[]).includes(e),
        )
      : [...ALL_ENDPOINTS];

    const wantsOddsAlias = endpoints.includes("odds");

    logger.info(
      `Starting import — ${sports.length} sports, ${endpoints.length} endpoints, snapshot=${snapshotType}`,
      NAME,
    );

    // currentYear: the CLI default when no --seasons flag is passed.
    // For ESPN extraction (file-based), always use currentSeason(sport)
    // so fall sports (NFL, EPL, etc.) resolve to their correct prior season.
    // Only use the explicit user-supplied season when it differs from the
    // auto-default (i.e., the user actually typed --seasons=YYYY).
    const currentYear = new Date().getFullYear();

    for (const sport of sports) {
      const requestedSeason = opts.seasons.length > 0
        ? opts.seasons[opts.seasons.length - 1]!
        : currentSeason(sport);
      // For ESPN file-based extractions, prefer the season-aware default.
      // If the user explicitly passed a non-current year, honour it.
      const season = requestedSeason === currentYear
        ? currentSeason(sport)
        : requestedSeason;
      const today = todayISO();

      logger.info(`── ${sport.toUpperCase()} ${season} ──`, NAME);

      if (opts.dryRun) {
        logger.info("Dry run — skipping", `${NAME}/${sport}`);
        continue;
      }

      try {
        if (snapshotType === "opening" && (wantsOddsAlias || endpoints.includes("opening"))) {
          // Opening odds collection
          const files = await scheduler.collectOpening(sport, today);
          totalFiles += files;
        } else if (snapshotType === "closing" && (wantsOddsAlias || endpoints.includes("closing"))) {
          // Closing collection — collect current snapshot as closing proxy
          const hour = new Date().getHours();
          const files = await scheduler.collectSnapshot(sport, today, hour);
          totalFiles += files;
        } else {
          // Default flow: ESPN extract as baseline, then OddsAPI snapshot if key available
          if (wantsOddsAlias || endpoints.includes("espn_extract")) {
            try {
              const files = await scheduler.extractEspnOdds(sport, season, opts.dataDir);
              totalFiles += files;
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              logger.warn(`ESPN extract: ${msg}`, `${NAME}/${sport}`);
              allErrors.push(`${sport}/espn_extract: ${msg}`);
            }
          }

          if (wantsOddsAlias || endpoints.includes("espn_player_props")) {
            try {
              const files = await scheduler.extractEspnPlayerProps(sport, season, opts.dataDir);
              totalFiles += files;
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              logger.warn(`ESPN player props: ${msg}`, `${NAME}/${sport}`);
              allErrors.push(`${sport}/espn_player_props: ${msg}`);
            }
          }

          if (ODDS_CONFIG.oddsapi_key && (wantsOddsAlias || endpoints.includes("snapshots"))) {
            try {
              const hour = new Date().getHours();
              const files = await scheduler.collectSnapshot(sport, today, hour);
              totalFiles += files;
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              logger.warn(`Snapshot: ${msg}`, `${NAME}/${sport}`);
              allErrors.push(`${sport}/snapshots: ${msg}`);
            }
          }
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error(`${sport}: ${msg}`, NAME);
        allErrors.push(`${sport}: ${msg}`);
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

export default oddsProvider;
