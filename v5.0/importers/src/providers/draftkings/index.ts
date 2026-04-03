// ──────────────────────────────────────────────────────────
// Multi-Book Odds Provider (via ESPN Core API)
// ──────────────────────────────────────────────────────────
// Fetches game odds (spread, moneyline, total) from ESPN's
// Core API, which aggregates lines from multiple books
// including DraftKings, Caesars, BetMGM, FanDuel, etc.
// No API key required.

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "draftkings";
const ESPN_BASE = "https://sports.core.api.espn.com/v2/sports";
const SITE_API = "https://site.api.espn.com/apis/site/v2/sports";
const RATE_LIMIT: RateLimitConfig = { requests: 3, perMs: 1_000 };

const ESPN_SPORT_MAP: Partial<Record<Sport, { sport: string; league: string }>> = {
  nfl:   { sport: "football",   league: "nfl" },
  nba:   { sport: "basketball", league: "nba" },
  mlb:   { sport: "baseball",   league: "mlb" },
  nhl:   { sport: "hockey",     league: "nhl" },
  ncaaf: { sport: "football",   league: "college-football" },
  ncaab: { sport: "basketball", league: "mens-college-basketball" },
  wnba:  { sport: "basketball", league: "wnba" },
  mls:   { sport: "soccer",     league: "usa.1" },
  epl:   { sport: "soccer",     league: "eng.1" },
  ufc:   { sport: "mma",        league: "ufc" },
};

const SUPPORTED_SPORTS = Object.keys(ESPN_SPORT_MAP) as Sport[];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

interface EspnEvent { id: string; name?: string; shortName?: string; date?: string }
interface EspnOdds {
  provider?: { id: string; name: string };
  details?: string;
  overUnder?: number;
  spread?: number;
  homeTeamOdds?: { moneyLine?: number; spreadOdds?: number; favorite?: boolean };
  awayTeamOdds?: { moneyLine?: number; spreadOdds?: number; favorite?: boolean };
  overOdds?: number;
  underOdds?: number;
}

async function getEvents(sportPath: string, season: number): Promise<EspnEvent[]> {
  try {
    const url = `${SITE_API}/${sportPath}/scoreboard?season=${season}&limit=100`;
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT);
    return (data?.events ?? []).map((e: any) => ({
      id: e.id,
      name: e.name,
      shortName: e.shortName,
      date: e.date,
    }));
  } catch {
    return [];
  }
}

async function getEventOdds(espnSport: string, espnLeague: string, eventId: string): Promise<EspnOdds[]> {
  try {
    const url = `${ESPN_BASE}/${espnSport}/leagues/${espnLeague}/events/${eventId}/competitions/${eventId}/odds?limit=20`;
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT);
    return data?.items ?? [];
  } catch {
    return [];
  }
}

function normalizeOdds(event: EspnEvent, odds: EspnOdds[]): Record<string, unknown>[] {
  return odds.map((o) => ({
    event_id: event.id,
    event_name: event.name ?? event.shortName ?? "",
    event_date: event.date ?? null,
    book_id: o.provider?.id ?? null,
    book_name: o.provider?.name ?? "unknown",
    details: o.details ?? null,
    spread: o.spread ?? null,
    over_under: o.overUnder ?? null,
    home_moneyline: o.homeTeamOdds?.moneyLine ?? null,
    away_moneyline: o.awayTeamOdds?.moneyLine ?? null,
    home_spread_odds: o.homeTeamOdds?.spreadOdds ?? null,
    away_spread_odds: o.awayTeamOdds?.spreadOdds ?? null,
    over_odds: o.overOdds ?? null,
    under_odds: o.underOdds ?? null,
    home_favorite: o.homeTeamOdds?.favorite ?? null,
    source: "espn-odds",
  }));
}

async function importOdds(ctx: EndpointCtx): Promise<EndpointResult> {
  const mapping = ESPN_SPORT_MAP[ctx.sport];
  if (!mapping || ctx.dryRun) return { filesWritten: 0, errors: [] };

  const errors: string[] = [];
  const sportPath = `${mapping.sport}/${mapping.league}`;
  const events = await getEvents(sportPath, ctx.season);

  if (events.length === 0) {
    logger.info(`No events found for ${ctx.sport}/${ctx.season}`, NAME);
    return { filesWritten: 0, errors };
  }

  const allLines: Record<string, unknown>[] = [];

  // Fetch odds for up to 30 events (rate limit conscious)
  const targets = events.slice(0, 30);
  for (const event of targets) {
    const odds = await getEventOdds(mapping.sport, mapping.league, event.id);
    if (odds.length > 0) {
      allLines.push(...normalizeOdds(event, odds));
    }
  }

  const dateStr = new Date().toISOString().slice(0, 10);
  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `odds_${dateStr}.json`);
  writeJSON(outPath, {
    source: "espn-odds",
    provider: NAME,
    sport: ctx.sport,
    season: String(ctx.season),
    date: dateStr,
    event_count: events.length,
    fetched_events: targets.length,
    line_count: allLines.length,
    lines: allLines,
    fetched_at: new Date().toISOString(),
  });

  logger.progress(NAME, ctx.sport, "odds", `${allLines.length} lines from ${targets.length} events`);
  return { filesWritten: 1, errors };
}

const draftkings: Provider = {
  name: NAME,
  label: "Multi-Book Odds (ESPN Core API)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["odds"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        const r = await importOdds({ sport, season, dataDir: opts.dataDir, dryRun: opts.dryRun });
        totalFiles += r.filesWritten;
        allErrors.push(...r.errors);
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

export default draftkings;
