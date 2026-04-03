// ──────────────────────────────────────────────────────────
// SofaScore Provider (unofficial public API)
// ──────────────────────────────────────────────────────────
// SofaScore exposes a public API used by their website and
// mobile apps. No API key required. Provides:
// - Live scores and match details
// - Player ratings per match
// - Team form (recent results streak)
// - Top performers / man-of-the-match data
// - Match momentum / incident timelines
//
// Rate limit: be conservative (~1 req/sec) to avoid 429s.
// User-Agent must be set to a real browser string.
//
// Coverage: NBA, NFL, MLB, NHL, EPL, La Liga, Bundesliga,
// Serie A, Ligue 1, MLS, UCL, UEFA Europa, UFC

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import { sleep } from "../../core/http.js";

const NAME = "sofascore";
const BASE = "https://api.sofascore.com/api/v1";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_200 };

// SofaScore unique IDs for each tournament
const TOURNAMENT_IDS: Partial<Record<Sport, { id: number; season_id?: number; name: string }[]>> = {
  nba:        [{ id: 132, name: "NBA" }],
  wnba:       [{ id: 182, name: "WNBA" }],
  nfl:        [{ id: 63, name: "NFL" }],
  mlb:        [{ id: 64, name: "MLB" }],
  nhl:        [{ id: 62, name: "NHL" }],
  epl:        [{ id: 17, name: "Premier League" }],
  laliga:     [{ id: 8, name: "La Liga" }],
  bundesliga: [{ id: 35, name: "Bundesliga" }],
  seriea:     [{ id: 23, name: "Serie A" }],
  ligue1:     [{ id: 34, name: "Ligue 1" }],
  mls:        [{ id: 242, name: "MLS" }],
  ucl:        [{ id: 7, name: "UEFA Champions League" }],
  europa:     [{ id: 679, name: "UEFA Europa League" }],
  ufc:        [{ id: 117, name: "UFC" }],
};

const SUPPORTED_SPORTS = Object.keys(TOURNAMENT_IDS) as Sport[];

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Accept": "application/json",
  "Referer": "https://www.sofascore.com/",
};

async function sfFetch<T = unknown>(path: string): Promise<T | null> {
  const url = `${BASE}${path}`;
  try {
    const resp = await fetch(url, { headers: HEADERS, signal: AbortSignal.timeout(15_000) });
    if (resp.status === 429) {
      logger.warn("SofaScore rate limited — backing off 10s", NAME);
      await sleep(10_000);
      return null;
    }
    if (!resp.ok) return null;
    return (await resp.json()) as T;
  } catch {
    return null;
  }
}

interface SofaEvent {
  id: number;
  slug?: string;
  startTimestamp?: number;
  homeTeam?: { id: number; name: string; nameCode?: string };
  awayTeam?: { id: number; name: string; nameCode?: string };
  homeScore?: { current?: number; period1?: number; period2?: number };
  awayScore?: { current?: number; period1?: number; period2?: number };
  status?: { type: string; description: string };
  winnerCode?: number;
  venue?: { name?: string; city?: { name?: string } };
}

interface SofaRating {
  player?: { id: number; name: string; position?: string };
  value?: string;
  countUnderTwoThirds?: number;
}

// Fetch scheduled + live events for a tournament on a given date
async function fetchEvents(
  tournamentId: number,
  dateStr: string,
  dataDir: string,
  sport: Sport,
): Promise<number> {
  const data = await sfFetch<{ events?: SofaEvent[] }>(
    `/sport/0/scheduled-events/${dateStr}`,
  );
  if (!data?.events) return 0;

  const filtered = data.events.filter(
    (e) => {
      // SofaScore returns all sports; filter by tournament
      // We rely on the fact we use per-sport date scraping sparingly
      return true; // caller already filters by tournamentId
    },
  );
  // We can't easily filter here without querying tournament info per event
  // so store the whole day's result keyed by tournament
  const tourneyEvents = await sfFetch<{ events?: SofaEvent[] }>(
    `/unique-tournament/${tournamentId}/events/last/0`,
  );
  if (!tourneyEvents?.events) return 0;

  const outPath = rawPath(dataDir, NAME, sport, "current", "events", `${dateStr}.json`);
  await writeJSON(outPath, tourneyEvents.events);
  return tourneyEvents.events.length;
}

// Fetch player ratings for a match
async function fetchMatchRatings(
  eventId: number,
  dataDir: string,
  sport: Sport,
): Promise<void> {
  const [homeRatings, awayRatings] = await Promise.all([
    sfFetch<{ player?: SofaRating[] }>(`/event/${eventId}/ratings/home`),
    sfFetch<{ player?: SofaRating[] }>(`/event/${eventId}/ratings/away`),
  ]);
  if (!homeRatings && !awayRatings) return;
  const outPath = rawPath(dataDir, NAME, sport, "current", "ratings", `${eventId}.json`);
  await writeJSON(outPath, { event_id: eventId, home: homeRatings?.player ?? [], away: awayRatings?.player ?? [] });
}

// Fetch team form (last N matches results)
async function fetchTeamForm(
  teamId: number,
  dataDir: string,
  sport: Sport,
): Promise<void> {
  const data = await sfFetch<{ events?: SofaEvent[] }>(`/team/${teamId}/events/last/0`);
  if (!data?.events?.length) return;
  const outPath = rawPath(dataDir, NAME, sport, "current", "team_form", `${teamId}.json`);
  await writeJSON(outPath, data.events.slice(0, 10));
}

// Fetch top performers for a tournament
async function fetchTopPerformers(
  tournamentId: number,
  seasonId: number | undefined,
  dataDir: string,
  sport: Sport,
): Promise<void> {
  if (!seasonId) return;
  const stats = ["goals", "assists", "rating", "saves", "cleanSheets"];
  for (const stat of stats) {
    await sleep(300);
    const data = await sfFetch<{ topPlayers?: unknown[] }>(
      `/unique-tournament/${tournamentId}/season/${seasonId}/top-players/${stat}`,
    );
    if (!data?.topPlayers?.length) continue;
    const outPath = rawPath(dataDir, NAME, sport, "current", "top_performers", `${stat}.json`);
    await writeJSON(outPath, data.topPlayers);
  }
}

// Fetch tournament season info to get current season ID
async function fetchCurrentSeasonId(tournamentId: number): Promise<number | undefined> {
  const data = await sfFetch<{ seasons?: { id: number; year: string }[] }>(
    `/unique-tournament/${tournamentId}/seasons`,
  );
  if (!data?.seasons?.length) return undefined;
  return data.seasons[0].id;
}

const sofascore: Provider = {
  name: NAME,
  label: "SofaScore (live scores, ratings, form)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["events", "ratings", "team_form", "top_performers"],
  enabled: true,

  async import(options: ImportOptions): Promise<ImportResult> {
    const { sports, dataDir = "data" } = options;
    const startMs = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    const targetSports = (sports.length ? sports : SUPPORTED_SPORTS).filter(
      (s) => TOURNAMENT_IDS[s as Sport],
    );

    for (const sport of targetSports as Sport[]) {
      const configs = TOURNAMENT_IDS[sport];
      if (!configs?.length) continue;

    for (const config of configs) {
      try {
        logger.info(`${sport} — fetching SofaScore tournament ${config.id} (${config.name})`, NAME);

        // 1. Get current season ID
        const seasonId = config.season_id ?? (await fetchCurrentSeasonId(config.id));
        await sleep(RATE_LIMIT.perMs);

        // 2. Fetch recent events
        const recentEvents = await sfFetch<{ events?: SofaEvent[] }>(
          `/unique-tournament/${config.id}/events/last/0`,
        );
        await sleep(RATE_LIMIT.perMs);

        if (recentEvents?.events?.length) {
          const outPath = rawPath(dataDir, NAME, sport, "current", "events", `${config.id}_recent.json`);
          await writeJSON(outPath, recentEvents.events);
          filesWritten++;

          // 3. Fetch ratings for last 5 events
          const last5 = recentEvents.events.slice(0, 5);
          for (const event of last5) {
            await sleep(RATE_LIMIT.perMs);
            await fetchMatchRatings(event.id, dataDir, sport);
            filesWritten++;
          }

          // 4. Fetch team form for teams in recent events
          const teamIds = new Set<number>();
          for (const e of last5) {
            if (e.homeTeam?.id) teamIds.add(e.homeTeam.id);
            if (e.awayTeam?.id) teamIds.add(e.awayTeam.id);
          }
          for (const teamId of Array.from(teamIds).slice(0, 6)) {
            await sleep(RATE_LIMIT.perMs);
            await fetchTeamForm(teamId, dataDir, sport);
            filesWritten++;
          }
        }

        // 5. Top performers
        if (seasonId) {
          await sleep(RATE_LIMIT.perMs);
          await fetchTopPerformers(config.id, seasonId, dataDir, sport);
          filesWritten += 3;
        }

        // 6. Upcoming events (next 5)
        const upcomingEvents = await sfFetch<{ events?: SofaEvent[] }>(
          `/unique-tournament/${config.id}/events/next/0`,
        );
        await sleep(RATE_LIMIT.perMs);
        if (upcomingEvents?.events?.length) {
          const outPath = rawPath(dataDir, NAME, sport, "current", "events", `${config.id}_upcoming.json`);
          await writeJSON(outPath, upcomingEvents.events);
          filesWritten++;
        }
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`${config.name}: ${msg}`);
        logger.error(`SofaScore ${sport}/${config.name} failed: ${msg}`, NAME);
      }
    } // end for config
    } // end for sport

    return {
      provider: NAME,
      sport: "multi",
      filesWritten,
      errors,
      durationMs: Date.now() - startMs,
    };
  },
};

export default sofascore;
