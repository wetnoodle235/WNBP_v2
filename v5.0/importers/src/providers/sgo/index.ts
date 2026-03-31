// ──────────────────────────────────────────────────────────
// V5.0 SGO (SportsGameOdds) Provider
// ──────────────────────────────────────────────────────────
// Fetches odds from SportsGameOdds API v2.
// Requires SGO_API_KEY environment variable.
//
// Only collects opening/closing snapshots (2 calls/sport/day).
// Do NOT use for continuous/current polling — use ESPN odds for that.

import type {
  Provider,
  ImportOptions,
  ImportResult,
  Sport,
  RateLimitConfig,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "sgo";
const BASE_URL = "https://api.sportsgameodds.com";
const API_KEY = process.env.SGO_API_KEY ?? "";

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 2_000 };

// ── Sport → SGO League ID mapping ──────────────────────────

const SPORT_TO_LEAGUE: Partial<Record<Sport, string>> = {
  nba:   "NBA",
  mlb:   "MLB",
  nfl:   "NFL",
  nhl:   "NHL",
  ncaab: "NCAAB",
  wnba:  "WNBA",
  ncaaf: "NCAAF",
};

const SUPPORTED_SPORTS = Object.keys(SPORT_TO_LEAGUE) as Sport[];

// Bookmakers to collect (must be available on SGO free/paid tier)
const ACCEPTED_BOOKMAKERS = new Set([
  "draftkings",
  "fanduel",
  "betmgm",
  "caesars",
  "fanatics",
  "betrivers",
  "betway",
  "ballybet",
  "betparx",
  "pointsbet",
  "lowvig",
  "betonline",
]);

// ── SGO API types ───────────────────────────────────────────

interface SgoBookmakerLine {
  odds?: string;
  spread?: string;
  overUnder?: string;
  lastUpdatedAt?: string;
  available?: boolean;
}

interface SgoOddEntry {
  oddID?: string;
  marketName?: string;
  byBookmaker?: Record<string, SgoBookmakerLine>;
}

interface SgoTeamNames {
  long: string;
  medium: string;
  short: string;
  location: string;
}

interface SgoEvent {
  eventID: string;
  leagueID: string;
  teams: {
    home: { teamID: string; names: SgoTeamNames };
    away: { teamID: string; names: SgoTeamNames };
  };
  status: {
    startsAt: string;
    started: boolean;
    completed: boolean;
    cancelled: boolean;
  };
  odds?: Record<string, SgoOddEntry>;
}

interface SgoApiResponse {
  success: boolean;
  data?: SgoEvent[];
  notice?: string;
  error?: string;
}

// ── Normalized record written to disk ──────────────────────
// Matches the format that normalizer.py's _sgo_odds() expects.

interface SgoOddsRecord {
  game_id: string;
  vendor: string;
  updated_at: string;
  moneyline_home_odds: number | null;
  moneyline_away_odds: number | null;
  spread_home_value: string | null;
  spread_home_odds: number | null;
  spread_away_value: string | null;
  spread_away_odds: number | null;
  total_value: string | null;
  total_over_odds: number | null;
  total_under_odds: number | null;
  _sgo_event_id: string;
  _home_team_medium: string;
  _away_team_medium: string;
  _home_team_long: string;
  _away_team_long: string;
  _event_date: string;
  _is_live: boolean;
  _market_scope: "pregame" | "live";
  _provider: "sgo";
}

// ── Helpers ─────────────────────────────────────────────────

function parseAmerican(value: string | undefined): number | null {
  if (value == null) return null;
  const n = parseInt(value, 10);
  return Number.isNaN(n) ? null : n;
}

function latestTimestamp(...timestamps: (string | undefined)[]): string {
  let latest = "";
  for (const ts of timestamps) {
    if (ts && ts > latest) latest = ts;
  }
  return latest || new Date().toISOString();
}

function todayISO(): string {
  return new Date().toISOString().slice(0, 10);
}

function currentSeason(): number {
  const now = new Date();
  const month = now.getMonth() + 1;
  return month >= 8 ? now.getFullYear() : now.getFullYear() - 1;
}

// ── Event normalizer ────────────────────────────────────────

function normalizeEvent(event: SgoEvent): SgoOddsRecord[] {
  const odds = event.odds ?? {};
  const eventDate = (event.status.startsAt ?? "").slice(0, 10);

  // Core market odd-entry keys
  const mlHome  = odds["points-home-game-ml-home"];
  const mlAway  = odds["points-away-game-ml-away"];
  const spHome  = odds["points-home-game-sp-home"];
  const spAway  = odds["points-away-game-sp-away"];
  const ouOver  = odds["points-all-game-ou-over"];
  const ouUnder = odds["points-all-game-ou-under"];

  // Collect all bookmakers present across markets
  const allBookmakers = new Set<string>();
  for (const entry of [mlHome, mlAway, spHome, spAway, ouOver, ouUnder]) {
    if (!entry?.byBookmaker) continue;
    for (const bk of Object.keys(entry.byBookmaker)) {
      allBookmakers.add(bk);
    }
  }

  const records: SgoOddsRecord[] = [];

  for (const bk of allBookmakers) {
    if (!ACCEPTED_BOOKMAKERS.has(bk)) continue;

    const mlHomeEntry  = mlHome?.byBookmaker?.[bk];
    const mlAwayEntry  = mlAway?.byBookmaker?.[bk];
    const spHomeEntry  = spHome?.byBookmaker?.[bk];
    const spAwayEntry  = spAway?.byBookmaker?.[bk];
    const ouOverEntry  = ouOver?.byBookmaker?.[bk];
    const ouUnderEntry = ouUnder?.byBookmaker?.[bk];

    // Skip bookmakers with no available lines
    const hasAnyLine =
      mlHomeEntry?.available ||
      mlAwayEntry?.available ||
      spHomeEntry?.available ||
      ouOverEntry?.available;
    if (!hasAnyLine) continue;

    const updated_at = latestTimestamp(
      mlHomeEntry?.lastUpdatedAt,
      mlAwayEntry?.lastUpdatedAt,
      spHomeEntry?.lastUpdatedAt,
      spAwayEntry?.lastUpdatedAt,
      ouOverEntry?.lastUpdatedAt,
      ouUnderEntry?.lastUpdatedAt,
    );

    records.push({
      game_id:             event.eventID,
      vendor:              bk,
      updated_at,
      moneyline_home_odds: parseAmerican(mlHomeEntry?.odds),
      moneyline_away_odds: parseAmerican(mlAwayEntry?.odds),
      spread_home_value:   spHomeEntry?.spread ?? null,
      spread_home_odds:    parseAmerican(spHomeEntry?.odds),
      spread_away_value:   spAwayEntry?.spread ?? null,
      spread_away_odds:    parseAmerican(spAwayEntry?.odds),
      total_value:         ouOverEntry?.overUnder ?? ouUnderEntry?.overUnder ?? null,
      total_over_odds:     parseAmerican(ouOverEntry?.odds),
      total_under_odds:    parseAmerican(ouUnderEntry?.odds),
      _sgo_event_id:       event.eventID,
      _home_team_medium:   event.teams.home.names.medium,
      _away_team_medium:   event.teams.away.names.medium,
      _home_team_long:     event.teams.home.names.long,
      _away_team_long:     event.teams.away.names.long,
      _event_date:         eventDate,
      _is_live:            !!event.status?.started,
      _market_scope:       event.status?.started ? "live" : "pregame",
      _provider:           "sgo",
    });
  }

  return records;
}

// ── Import odds for one sport ───────────────────────────────

async function importOdds(
  sport: Sport,
  season: number,
  dataDir: string,
  dryRun: boolean,
  snapshotType: "opening" | "closing" | "current",
): Promise<{ filesWritten: number; errors: string[] }> {
  const leagueID = SPORT_TO_LEAGUE[sport]!;
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) {
    logger.progress(NAME, sport, "odds", "Dry run — skipping");
    return { filesWritten: 0, errors: [] };
  }

  const url =
    `${BASE_URL}/v2/events` +
    `?leagueID=${encodeURIComponent(leagueID)}&oddsAvailable=true&apiKey=${API_KEY}`;

  logger.progress(NAME, sport, "odds", `Fetching ${snapshotType} odds (league=${leagueID})`);

  try {
    const resp = await fetchJSON<SgoApiResponse>(url, NAME, RATE_LIMIT);

    if (!resp?.success) {
      const msg = resp?.error ?? "success=false from SGO API";
      logger.error(`${sport}: ${msg}`, NAME);
      errors.push(`${sport}/${season}/odds: ${msg}`);
      return { filesWritten, errors };
    }

    if (resp.notice) {
      logger.warn(resp.notice, `${NAME}/${sport}`);
    }

    const events = resp.data ?? [];
    const allRecords: SgoOddsRecord[] = [];

    for (const ev of events) {
      if (ev.status?.cancelled || ev.status?.completed) continue;
      allRecords.push(...normalizeEvent(ev));
    }

    // Deduplicate: keep freshest record per (game_id, vendor)
    const seen = new Map<string, SgoOddsRecord>();
    for (const r of allRecords) {
      const key = `${r.game_id}::${r.vendor}`;
      const existing = seen.get(key);
      if (!existing || r.updated_at > existing.updated_at) {
        seen.set(key, r);
      }
    }
    const records = Array.from(seen.values());

    const date = todayISO();
    const outPath = rawPath(dataDir, NAME, sport, season, "odds", date, `${snapshotType}.json`);
    writeJSON(outPath, {
      sport,
      leagueID,
      snapshotType,
      date,
      eventCount: events.filter((e) => !e.status?.cancelled && !e.status?.completed).length,
      recordCount: records.length,
      records,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;

    logger.progress(
      NAME, sport, "odds",
      `Saved ${records.length} bookmaker lines (${events.length} events, ${snapshotType})`,
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(`${sport}/${season}: ${msg}`, NAME);
    errors.push(`${sport}/${season}/odds: ${msg}`);
  }

  return { filesWritten, errors };
}

// ── Provider export ─────────────────────────────────────────

const sgo: Provider = {
  name: NAME,
  label: "SportsGameOdds",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ["odds"] as readonly string[],
  enabled: !!API_KEY,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    if (!API_KEY) {
      logger.error("SGO_API_KEY not set — provider disabled", NAME);
      return {
        provider: NAME,
        sport: "multi",
        filesWritten: 0,
        errors: ["SGO_API_KEY environment variable is not set"],
        durationMs: Date.now() - start,
      };
    }

    const snapshotType = opts.snapshotType ?? "current";

    // Filter to SGO-supported sports
    const sports = opts.sports.length
      ? opts.sports.filter((s) => SPORT_TO_LEAGUE[s])
      : SUPPORTED_SPORTS;

    if (!sports.length) {
      logger.warn("No supported sports requested", NAME);
      return {
        provider: NAME,
        sport: "multi",
        filesWritten: 0,
        errors: [],
        durationMs: Date.now() - start,
      };
    }

    logger.info(
      `Starting import — ${sports.length} sports, snapshot=${snapshotType}`,
      NAME,
    );

    for (const sport of sports) {
      const season =
        opts.seasons.length > 0
          ? opts.seasons[opts.seasons.length - 1]!
          : currentSeason();

      logger.info(`── ${sport.toUpperCase()} ${season} ──`, NAME);

      try {
        const result = await importOdds(sport, season, opts.dataDir, opts.dryRun, snapshotType);
        totalFiles += result.filesWritten;
        allErrors.push(...result.errors);
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

export default sgo;
