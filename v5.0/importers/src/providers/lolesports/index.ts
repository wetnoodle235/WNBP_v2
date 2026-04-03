// ──────────────────────────────────────────────────────────
// LoL Esports Provider (Riot Games Official API)
// ──────────────────────────────────────────────────────────
// Uses the Riot Games public esports API (no personal key needed —
// the key is public/shared by the esports data community).
// Provides leagues, tournaments, standings, schedule, and match
// results for all major LoL competitive regions.

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "lolesports";
// Public shared API key for the LoL esports persisted data API
const API_KEY = "0TvQnueqKa5mxJntVWt0w4LpLfEkrV1Ta8rQBb9Z";
const BASE = "https://esports-api.lolesports.com/persisted/gw";
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_500 };
const HEADERS = { "x-api-key": API_KEY };

const HL = "en-US";

// Major league IDs (LCS, LEC, LCK, LPL, Worlds, MSI, etc.)
const MAJOR_LEAGUE_IDS = [
  "98767991299243165", // LCS (NA)
  "98767991302996019", // LEC (EU)
  "98767991310872058", // LCK (KR)
  "98767991314006698", // LPL (CN)
  "98767975604431411", // Worlds
  "98767991325878492", // MSI
  "104366947889790212",// CBLOL
  "98767991332355509", // LLA
  "107898214974993351",// PCS
];

const SUPPORTED_SPORTS: Sport[] = ["lol"];

interface League {
  id: string;
  slug: string;
  name: string;
  region: string;
  image?: string;
  priority?: number;
}

interface Tournament {
  id: string;
  slug: string;
  startDate: string;
  endDate: string;
}

interface ScheduleEvent {
  startTime: string;
  state: string;
  type: string;
  blockName: string;
  league: { name: string; id: string };
  match?: {
    id: string;
    flags?: string[];
    teams?: Array<{
      name: string;
      code: string;
      image?: string;
      result?: { outcome: string | null; gameWins: number };
    }>;
    strategy?: { type: string; count: number };
  };
}

async function fetchLeagues(): Promise<League[]> {
  const data = await fetchJSON<{ data: { leagues: League[] } }>(
    `${BASE}/getLeagues?hl=${HL}`, NAME, RATE_LIMIT, { headers: HEADERS },
  );
  return data?.data?.leagues ?? [];
}

async function fetchTournaments(leagueId: string): Promise<Tournament[]> {
  const data = await fetchJSON<{ data: { leagues: Array<{ tournaments: Tournament[] }> } }>(
    `${BASE}/getTournamentsForLeague?hl=${HL}&leagueId=${leagueId}`,
    NAME, RATE_LIMIT, { headers: HEADERS },
  );
  return data?.data?.leagues?.[0]?.tournaments ?? [];
}

async function fetchSchedule(leagueId: string, pageToken?: string): Promise<{
  events: ScheduleEvent[];
  nextPageToken?: string;
}> {
  const url = pageToken
    ? `${BASE}/getSchedule?hl=${HL}&leagueId=${leagueId}&pageToken=${encodeURIComponent(pageToken)}`
    : `${BASE}/getSchedule?hl=${HL}&leagueId=${leagueId}`;
  const data = await fetchJSON<{ data: { schedule: { events: ScheduleEvent[]; pages?: { older?: string } } } }>(
    url, NAME, RATE_LIMIT, { headers: HEADERS },
  );
  return {
    events: data?.data?.schedule?.events ?? [],
    nextPageToken: data?.data?.schedule?.pages?.older,
  };
}

async function fetchStandings(tournamentId: string): Promise<unknown> {
  const data = await fetchJSON<{ data: { standings: unknown[] } }>(
    `${BASE}/getStandings?hl=${HL}&tournamentId=${tournamentId}`,
    NAME, RATE_LIMIT, { headers: HEADERS },
  );
  return data?.data?.standings ?? [];
}

const lolesports: Provider = {
  name: NAME,
  label: "LoL Esports (Riot Official API)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["leagues", "schedule", "standings"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    if (opts.sports.length && !opts.sports.includes("lol")) {
      return { provider: NAME, sport: "lol", filesWritten: 0, errors: [], durationMs: 0 };
    }

    const season = Math.max(...opts.seasons);

    // 1. Fetch and write all leagues metadata
    try {
      const leagues = await fetchLeagues();
      if (!opts.dryRun) {
        writeJSON(rawPath(opts.dataDir, NAME, "lol", season, "leagues.json"), {
          source: NAME, sport: "lol", season: String(season),
          count: leagues.length, fetched_at: new Date().toISOString(), leagues,
        });
        filesWritten++;
      }
      logger.progress(NAME, "lol", "leagues", `${leagues.length} leagues`);
    } catch (err) {
      errors.push(`leagues: ${err instanceof Error ? err.message : String(err)}`);
    }

    // 2. For each major league: fetch schedule + standings
    for (const leagueId of MAJOR_LEAGUE_IDS) {
      try {
        // Fetch schedule (paginate up to 3 pages to avoid rate limits)
        const allEvents: ScheduleEvent[] = [];
        let pageToken: string | undefined;
        let page = 0;
        do {
          const { events, nextPageToken } = await fetchSchedule(leagueId, pageToken);
          allEvents.push(...events);
          pageToken = nextPageToken;
          page++;
        } while (pageToken && page < 5);

        // Filter to requested seasons (LoL seasons roughly match calendar years)
        const seasonEvents = allEvents.filter((e) => {
          const year = new Date(e.startTime).getFullYear();
          return opts.seasons.includes(year);
        });

        if (!opts.dryRun && seasonEvents.length > 0) {
          const path = rawPath(opts.dataDir, NAME, "lol", season, `schedule_${leagueId}.json`);
          writeJSON(path, {
            source: NAME, sport: "lol", league_id: leagueId, season: String(season),
            count: seasonEvents.length, fetched_at: new Date().toISOString(),
            events: seasonEvents.map((e) => ({
              start_time: e.startTime,
              state: e.state,
              type: e.type,
              block: e.blockName,
              league: e.league.name,
              match_id: e.match?.id,
              teams: e.match?.teams?.map((t) => ({
                name: t.name,
                code: t.code,
                wins: t.result?.gameWins,
                outcome: t.result?.outcome,
              })),
              format: e.match?.strategy,
            })),
          });
          filesWritten++;
          logger.progress(NAME, "lol", "schedule",
            `${seasonEvents.length} events for league ${leagueId}`);
        }

        // Fetch tournaments + standings for this league
        const tournaments = await fetchTournaments(leagueId);
        const relevantTournaments = tournaments.filter((t) => {
          const year = new Date(t.startDate).getFullYear();
          return opts.seasons.includes(year);
        });

        for (const tournament of relevantTournaments.slice(0, 4)) {
          try {
            const standings = await fetchStandings(tournament.id);
            if (!opts.dryRun) {
              const sPath = rawPath(opts.dataDir, NAME, "lol", season,
                `standings_${tournament.id}.json`);
              writeJSON(sPath, {
                source: NAME, sport: "lol", tournament_id: tournament.id,
                tournament_slug: tournament.slug, season: String(season),
                fetched_at: new Date().toISOString(), standings,
              });
              filesWritten++;
            }
          } catch { /* skip if standings not available */ }
        }
      } catch (err) {
        const msg = `league/${leagueId}: ${err instanceof Error ? err.message : String(err)}`;
        logger.warn(msg, NAME);
        errors.push(msg);
      }
    }

    return { provider: NAME, sport: "lol", filesWritten, errors, durationMs: Date.now() - start };
  },
};

export default lolesports;
