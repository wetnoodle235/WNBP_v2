// ──────────────────────────────────────────────────────────
// V5.0 OddsAPI Provider
// ──────────────────────────────────────────────────────────
// Fetches odds, scores, events, and player props from
// The Odds API (https://the-odds-api.com).
// Requires ODDSAPI_KEY environment variable.

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

const NAME = "oddsapi";
const BASE_URL = "https://api.the-odds-api.com";
const API_KEY = process.env.ODDSAPI_KEY ?? "";

const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_000 };

// ── Sport key mapping ───────────────────────────────────────

const SPORT_KEYS: Partial<Record<Sport, string>> = {
  nba:        "basketball_nba",
  wnba:       "basketball_wnba",
  ncaab:      "basketball_ncaab",
  nfl:        "americanfootball_nfl",
  ncaaf:      "americanfootball_ncaaf",
  mlb:        "baseball_mlb",
  nhl:        "icehockey_nhl",
  epl:        "soccer_epl",
  laliga:     "soccer_spain_la_liga",
  bundesliga: "soccer_germany_bundesliga",
  seriea:     "soccer_italy_serie_a",
  ligue1:     "soccer_france_ligue_one",
  mls:        "soccer_usa_mls",
  ufc:        "mma_mixed_martial_arts",
  atp:        "tennis_atp_french_open",
};

const SUPPORTED_SPORTS = Object.keys(SPORT_KEYS) as Sport[];

const ALL_ENDPOINTS = ["odds", "scores", "events", "player_props"] as const;
type Endpoint = (typeof ALL_ENDPOINTS)[number];

const PLAYER_PROP_MARKETS = [
  "player_points",
  "player_rebounds",
  "player_assists",
  "player_threes",
  "player_blocks",
  "player_steals",
  "player_turnovers",
  "player_points_rebounds_assists",
] as const;

// ── API response types ──────────────────────────────────────

interface OddsOutcome {
  name: string;
  price: number;
  point?: number;
  description?: string;
}

interface OddsMarket {
  key: string;
  last_update: string;
  outcomes: OddsOutcome[];
}

interface OddsBookmaker {
  key: string;
  title: string;
  last_update: string;
  markets: OddsMarket[];
}

interface OddsEvent {
  id: string;
  sport_key: string;
  sport_title: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  bookmakers: OddsBookmaker[];
}

interface ScoreItem {
  name: string;
  score: string;
}

interface ScoreEvent {
  id: string;
  sport_key: string;
  sport_title: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  completed: boolean;
  scores: ScoreItem[] | null;
  last_update: string | null;
}

interface EventItem {
  id: string;
  sport_key: string;
  sport_title: string;
  commence_time: string;
  home_team: string;
  away_team: string;
}

// ── Helpers ─────────────────────────────────────────────────

function sportKey(sport: Sport): string {
  return SPORT_KEYS[sport]!;
}

function todayISO(): string {
  return new Date().toISOString().slice(0, 10);
}

function currentSeason(sport: Sport): number {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;

  // Fall sports (NFL, NCAAF, NBA, NCAAB, NHL) use start-year for seasons
  // spanning two calendar years; soccer leagues likewise.
  const fallSports = new Set<Sport>([
    "nfl", "ncaaf", "nba", "ncaab", "nhl",
    "epl", "laliga", "bundesliga", "seriea", "ligue1",
  ]);

  if (fallSports.has(sport)) {
    return month >= 8 ? year : year - 1;
  }
  return year;
}

async function apiFetch<T = unknown>(path: string): Promise<T> {
  const separator = path.includes("?") ? "&" : "?";
  const url = `${BASE_URL}${path}${separator}apiKey=${API_KEY}`;
  return fetchJSON<T>(url, NAME, RATE_LIMIT);
}

// ── Normalization ───────────────────────────────────────────

interface NormalizedOutcome {
  name: string;
  price: number;
  point?: number;
  description?: string;
}

interface NormalizedMarket {
  key: string;
  lastUpdate: string;
  outcomes: NormalizedOutcome[];
}

interface NormalizedBookmaker {
  key: string;
  title: string;
  lastUpdate: string;
  markets: NormalizedMarket[];
}

interface NormalizedOddsEvent {
  id: string;
  sportKey: string;
  commenceTime: string;
  homeTeam: string;
  awayTeam: string;
  bookmakers: NormalizedBookmaker[];
}

function normalizeBookmaker(bm: OddsBookmaker): NormalizedBookmaker {
  return {
    key: bm.key,
    title: bm.title,
    lastUpdate: bm.last_update,
    markets: bm.markets.map((m) => ({
      key: m.key,
      lastUpdate: m.last_update,
      outcomes: m.outcomes.map((o) => {
        const out: NormalizedOutcome = { name: o.name, price: o.price };
        if (o.point !== undefined) out.point = o.point;
        if (o.description !== undefined) out.description = o.description;
        return out;
      }),
    })),
  };
}

function normalizeOddsEvent(ev: OddsEvent): NormalizedOddsEvent {
  return {
    id: ev.id,
    sportKey: ev.sport_key,
    commenceTime: ev.commence_time,
    homeTeam: ev.home_team,
    awayTeam: ev.away_team,
    bookmakers: ev.bookmakers.map(normalizeBookmaker),
  };
}

// ── Endpoint context & result ───────────────────────────────

interface EndpointContext {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
  snapshotType: "opening" | "closing" | "current";
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

// ── Endpoint: odds ──────────────────────────────────────────

async function importOdds(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, snapshotType } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) {
    logger.progress(NAME, sport, "odds", "Dry run — skipping");
    return { filesWritten: 0, errors: [] };
  }

  const key = sportKey(sport);
  const url = `/v4/sports/${key}/odds?regions=us,eu&markets=h2h,spreads,totals&oddsFormat=american`;

  logger.progress(NAME, sport, "odds", `Fetching ${snapshotType} odds`);

  try {
    const events = await apiFetch<OddsEvent[]>(url);

    if (!events || events.length === 0) {
      logger.progress(NAME, sport, "odds", "No odds data returned");
      return { filesWritten, errors };
    }

    const normalized = events.map(normalizeOddsEvent);
    const date = todayISO();

    const outPath = rawPath(dataDir, NAME, sport, season, "odds", date, `${snapshotType}.json`);
    writeJSON(outPath, {
      sport,
      sportKey: key,
      snapshotType,
      date,
      eventCount: normalized.length,
      events: normalized,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;

    logger.progress(NAME, sport, "odds", `Saved ${normalized.length} events (${snapshotType})`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(`odds: ${msg}`, `${NAME}/${sport}`);
    errors.push(`${sport}/${season}/odds: ${msg}`);
  }

  return { filesWritten, errors };
}

// ── Endpoint: scores ────────────────────────────────────────

async function importScores(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) {
    logger.progress(NAME, sport, "scores", "Dry run — skipping");
    return { filesWritten: 0, errors: [] };
  }

  const key = sportKey(sport);
  const url = `/v4/sports/${key}/scores?daysFrom=3`;

  logger.progress(NAME, sport, "scores", "Fetching recent scores");

  try {
    const scores = await apiFetch<ScoreEvent[]>(url);

    if (!scores || scores.length === 0) {
      logger.progress(NAME, sport, "scores", "No scores data returned");
      return { filesWritten, errors };
    }

    const date = todayISO();
    const outPath = rawPath(dataDir, NAME, sport, season, "scores", `${date}.json`);
    writeJSON(outPath, {
      sport,
      sportKey: key,
      date,
      daysFrom: 3,
      eventCount: scores.length,
      events: scores.map((ev) => ({
        id: ev.id,
        sportKey: ev.sport_key,
        commenceTime: ev.commence_time,
        homeTeam: ev.home_team,
        awayTeam: ev.away_team,
        completed: ev.completed,
        scores: ev.scores,
        lastUpdate: ev.last_update,
      })),
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;

    const completed = scores.filter((s) => s.completed).length;
    logger.progress(NAME, sport, "scores", `Saved ${scores.length} events (${completed} completed)`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(`scores: ${msg}`, `${NAME}/${sport}`);
    errors.push(`${sport}/${season}/scores: ${msg}`);
  }

  return { filesWritten, errors };
}

// ── Endpoint: events ────────────────────────────────────────

async function importEvents(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) {
    logger.progress(NAME, sport, "events", "Dry run — skipping");
    return { filesWritten: 0, errors: [] };
  }

  const key = sportKey(sport);
  const url = `/v4/sports/${key}/events`;

  logger.progress(NAME, sport, "events", "Fetching upcoming events");

  try {
    const events = await apiFetch<EventItem[]>(url);

    if (!events || events.length === 0) {
      logger.progress(NAME, sport, "events", "No events data returned");
      return { filesWritten, errors };
    }

    const date = todayISO();
    const outPath = rawPath(dataDir, NAME, sport, season, "events", `${date}.json`);
    writeJSON(outPath, {
      sport,
      sportKey: key,
      date,
      eventCount: events.length,
      events: events.map((ev) => ({
        id: ev.id,
        sportKey: ev.sport_key,
        commenceTime: ev.commence_time,
        homeTeam: ev.home_team,
        awayTeam: ev.away_team,
      })),
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;

    logger.progress(NAME, sport, "events", `Saved ${events.length} upcoming events`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(`events: ${msg}`, `${NAME}/${sport}`);
    errors.push(`${sport}/${season}/events: ${msg}`);
  }

  return { filesWritten, errors };
}

// ── Endpoint: player_props ──────────────────────────────────

async function importPlayerProps(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) {
    logger.progress(NAME, sport, "player_props", "Dry run — skipping");
    return { filesWritten: 0, errors: [] };
  }

  const key = sportKey(sport);

  // Step 1: Fetch the events list to get event IDs
  logger.progress(NAME, sport, "player_props", "Fetching events list for props");

  let events: EventItem[];
  try {
    events = await apiFetch<EventItem[]>(`/v4/sports/${key}/events`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(`player_props (events list): ${msg}`, `${NAME}/${sport}`);
    errors.push(`${sport}/${season}/player_props: failed to fetch events — ${msg}`);
    return { filesWritten, errors };
  }

  if (!events || events.length === 0) {
    logger.progress(NAME, sport, "player_props", "No upcoming events — skipping props");
    return { filesWritten, errors };
  }

  // Filter to upcoming events only (commence_time in the future)
  const now = new Date();
  const upcoming = events.filter((ev) => new Date(ev.commence_time) > now);

  if (upcoming.length === 0) {
    logger.progress(NAME, sport, "player_props", "No upcoming events — skipping props");
    return { filesWritten, errors };
  }

  logger.progress(NAME, sport, "player_props", `Fetching props for ${upcoming.length} upcoming games`);

  // Step 2: For each upcoming event, fetch player props
  const marketsParam = PLAYER_PROP_MARKETS.join(",");

  for (const event of upcoming) {
    const propsUrl =
      `/v4/sports/${key}/events/${event.id}/odds` +
      `?regions=us&markets=${marketsParam}&oddsFormat=american`;

    try {
      const propsData = await apiFetch<OddsEvent>(propsUrl);

      if (!propsData || !propsData.bookmakers || propsData.bookmakers.length === 0) {
        logger.progress(NAME, sport, "player_props", `No props for ${event.home_team} vs ${event.away_team}`);
        continue;
      }

      const outPath = rawPath(dataDir, NAME, sport, season, "props", `${event.id}.json`);
      writeJSON(outPath, {
        sport,
        sportKey: key,
        eventId: event.id,
        commenceTime: event.commence_time,
        homeTeam: event.home_team,
        awayTeam: event.away_team,
        bookmakers: propsData.bookmakers.map(normalizeBookmaker),
        fetchedAt: new Date().toISOString(),
      });
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn(
        `player_props ${event.id} (${event.home_team} vs ${event.away_team}): ${msg}`,
        `${NAME}/${sport}`,
      );
      errors.push(`${sport}/${season}/player_props/${event.id}: ${msg}`);
    }
  }

  logger.progress(NAME, sport, "player_props", `Done — ${filesWritten} prop files`);
  return { filesWritten, errors };
}

// ── Endpoint dispatch ───────────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  odds:         importOdds,
  scores:       importScores,
  events:       importEvents,
  player_props: importPlayerProps,
};

// ── Provider export ─────────────────────────────────────────

const oddsapi: Provider = {
  name: NAME,
  label: "The Odds API",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ALL_ENDPOINTS as unknown as readonly string[],
  enabled: !!API_KEY,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    if (!API_KEY) {
      logger.error("ODDSAPI_KEY not set — provider disabled", NAME);
      return {
        provider: NAME,
        sport: "multi",
        filesWritten: 0,
        errors: ["ODDSAPI_KEY environment variable is not set"],
        durationMs: Date.now() - start,
      };
    }

    const snapshotType = opts.snapshotType ?? "current";

    // Filter sports to those this provider supports
    const sports = opts.sports.length
      ? opts.sports.filter((s) => SPORT_KEYS[s])
      : SUPPORTED_SPORTS;

    // Filter endpoints
    const endpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) =>
          ALL_ENDPOINTS.includes(e as Endpoint),
        ) as Endpoint[])
      : [...ALL_ENDPOINTS];

    logger.info(
      `Starting import — ${sports.length} sports, ${endpoints.length} endpoints, snapshot=${snapshotType}`,
      NAME,
    );

    for (const sport of sports) {
      // Determine season: use provided seasons or auto-detect current
      const season =
        opts.seasons.length > 0
          ? opts.seasons[opts.seasons.length - 1]!
          : currentSeason(sport);

      logger.info(`── ${sport.toUpperCase()} ${season} ──`, NAME);

      for (const ep of endpoints) {
        const fn = ENDPOINT_FNS[ep];
        if (!fn) continue;

        try {
          const ctx: EndpointContext = {
            sport,
            season,
            dataDir: opts.dataDir,
            dryRun: opts.dryRun,
            snapshotType,
          };
          const result = await fn(ctx);
          totalFiles += result.filesWritten;
          allErrors.push(...result.errors);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.error(`${sport}/${season}/${ep}: ${msg}`, NAME);
          allErrors.push(`${sport}/${season}/${ep}: ${msg}`);
        }
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

export default oddsapi;
