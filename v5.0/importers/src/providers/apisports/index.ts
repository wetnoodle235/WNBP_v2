// ──────────────────────────────────────────────────────────
// API-Sports Provider (api-sports.io)
// ──────────────────────────────────────────────────────────
// Comprehensive multi-sport API. Free tier: 100 requests/day.
// Covers football/soccer, basketball, baseball, hockey, rugby,
// volleyball, handball, and more. Provides fixtures, standings,
// player stats, injuries, odds, and predictions.
// Requires: APISPORTS_KEY environment variable.

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "apisports";
const API_KEY = process.env.APISPORTS_KEY ?? "";
const BASE = "https://v3.football.api-sports.io";
const NBA_BASE = "https://v2.nba.api-sports.io";
const MLB_BASE = "https://v2.baseball.api-sports.io";
const NHL_BASE = "https://v2.hockey.api-sports.io";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 3_000 }; // respect 100/day limit

const SPORT_LEAGUE_IDS: Partial<Record<Sport, { base: string; leagueId: number; name: string }>> = {
  epl:        { base: BASE,     leagueId: 39,  name: "Premier League" },
  laliga:     { base: BASE,     leagueId: 140, name: "La Liga" },
  bundesliga: { base: BASE,     leagueId: 78,  name: "Bundesliga" },
  seriea:     { base: BASE,     leagueId: 135, name: "Serie A" },
  ligue1:     { base: BASE,     leagueId: 61,  name: "Ligue 1" },
  ucl:        { base: BASE,     leagueId: 2,   name: "Champions League" },
  mls:        { base: BASE,     leagueId: 253, name: "MLS" },
  nba:        { base: NBA_BASE, leagueId: 12,  name: "NBA" },
  mlb:        { base: MLB_BASE, leagueId: 1,   name: "MLB" },
  nhl:        { base: NHL_BASE, leagueId: 57,  name: "NHL" },
};

const SUPPORTED_SPORTS = Object.keys(SPORT_LEAGUE_IDS) as Sport[];

function apiHeaders(): Record<string, string> {
  return { "x-apisports-key": API_KEY, "x-rapidapi-host": "v3.football.api-sports.io" };
}

async function fetchFixtures(base: string, leagueId: number, season: number): Promise<unknown[]> {
  const data = await fetchJSON<{ response: unknown[] }>(
    `${base}/fixtures?league=${leagueId}&season=${season}`,
    NAME, RATE_LIMIT, { headers: apiHeaders() },
  );
  return data?.response ?? [];
}

async function fetchStandings(base: string, leagueId: number, season: number): Promise<unknown[]> {
  const data = await fetchJSON<{ response: unknown[] }>(
    `${base}/standings?league=${leagueId}&season=${season}`,
    NAME, RATE_LIMIT, { headers: apiHeaders() },
  );
  return data?.response ?? [];
}

async function fetchInjuries(base: string, leagueId: number, season: number): Promise<unknown[]> {
  const data = await fetchJSON<{ response: unknown[] }>(
    `${base}/injuries?league=${leagueId}&season=${season}`,
    NAME, RATE_LIMIT, { headers: apiHeaders() },
  );
  return data?.response ?? [];
}

const apisports: Provider = {
  name: NAME,
  label: "API-Sports (multi-sport fixtures/standings/injuries)",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ["fixtures", "standings", "injuries"],
  enabled: !!API_KEY,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    if (!API_KEY) {
      return { provider: NAME, sport: "multi", filesWritten: 0,
        errors: ["APISPORTS_KEY not set"], durationMs: 0 };
    }

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    for (const sport of activeSports) {
      const league = SPORT_LEAGUE_IDS[sport];
      if (!league) continue;

      for (const season of opts.seasons) {
        try {
          // Fixtures
          const fixtures = await fetchFixtures(league.base, league.leagueId, season);
          if (fixtures.length > 0 && !opts.dryRun) {
            writeJSON(rawPath(opts.dataDir, NAME, sport, season, "fixtures.json"), {
              source: NAME, sport, league: league.name, season: String(season),
              count: fixtures.length, fetched_at: new Date().toISOString(), data: fixtures,
            });
            filesWritten++;
            logger.progress(NAME, sport, "fixtures", `${fixtures.length} fixtures`);
          }

          // Standings
          const standings = await fetchStandings(league.base, league.leagueId, season);
          if (standings.length > 0 && !opts.dryRun) {
            writeJSON(rawPath(opts.dataDir, NAME, sport, season, "standings.json"), {
              source: NAME, sport, league: league.name, season: String(season),
              count: standings.length, fetched_at: new Date().toISOString(), data: standings,
            });
            filesWritten++;
            logger.progress(NAME, sport, "standings", `${standings.length} entries`);
          }
        } catch (err) {
          const msg = `${sport}/${season}: ${err instanceof Error ? err.message : String(err)}`;
          logger.warn(msg, NAME);
          errors.push(msg);
        }
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0]! : "multi",
      filesWritten, errors, durationMs: Date.now() - start,
    };
  },
};

export default apisports;
