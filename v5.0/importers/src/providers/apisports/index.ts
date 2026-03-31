// ──────────────────────────────────────────────────────────
// V5.0 API-Sports Provider
// ──────────────────────────────────────────────────────────
// Fetches data from the API-Sports family of APIs covering
// basketball, american-football, baseball, hockey, MMA, F1,
// and soccer (football). Requires API_SPORTS_KEY env var.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "apisports";
const API_KEY = process.env.API_SPORTS_KEY ?? "";

// 10 requests per minute
const RATE_LIMIT: RateLimitConfig = { requests: 10, perMs: 60_000 };

const SUPPORTED_SPORTS: Sport[] = [
  "ncaab", "ncaaw", "wnba", "nfl", "mlb", "nhl", "ufc", "f1",
  "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls",
];

// ── API domain mapping ──────────────────────────────────────

type ApiDomain = "basketball" | "american-football" | "baseball" | "hockey" | "mma" | "formula-1" | "football";

interface SportConfig {
  domain: ApiDomain;
  leagueId: number;
  sportType: SportType;
}

type SportType = "basketball" | "soccer" | "american-football" | "baseball" | "hockey" | "mma" | "f1";

const SPORT_CONFIG: Record<Sport, SportConfig> = {
  // Basketball — v1.basketball.api-sports.io
  ncaab:      { domain: "basketball", leagueId: 116, sportType: "basketball" },
  ncaaw:      { domain: "basketball", leagueId: 117, sportType: "basketball" },
  wnba:       { domain: "basketball", leagueId: 13,  sportType: "basketball" },
  // American Football — v1.american-football.api-sports.io
  nfl:        { domain: "american-football", leagueId: 1, sportType: "american-football" },
  // Baseball — v1.baseball.api-sports.io
  mlb:        { domain: "baseball", leagueId: 1, sportType: "baseball" },
  // Hockey — v1.hockey.api-sports.io
  nhl:        { domain: "hockey", leagueId: 57, sportType: "hockey" },
  // MMA — v1.mma.api-sports.io (no league id, uses UFC org)
  ufc:        { domain: "mma", leagueId: 1, sportType: "mma" },
  // F1 — v1.formula-1.api-sports.io
  f1:         { domain: "formula-1", leagueId: 1, sportType: "f1" },
  // Soccer — v3.football.api-sports.io
  epl:        { domain: "football", leagueId: 39,  sportType: "soccer" },
  laliga:     { domain: "football", leagueId: 140, sportType: "soccer" },
  bundesliga: { domain: "football", leagueId: 78,  sportType: "soccer" },
  seriea:     { domain: "football", leagueId: 135, sportType: "soccer" },
  ligue1:     { domain: "football", leagueId: 61,  sportType: "soccer" },
  mls:        { domain: "football", leagueId: 253, sportType: "soccer" },
} as Record<Sport, SportConfig>;

function baseUrl(domain: ApiDomain): string {
  if (domain === "football") return "https://v3.football.api-sports.io";
  return `https://v1.${domain}.api-sports.io`;
}

// ── Endpoint definitions per sport type ─────────────────────

const BASKETBALL_ENDPOINTS = ["games", "standings", "players_statistics", "injuries"] as const;
const SOCCER_ENDPOINTS = ["fixtures", "standings", "players_topscorers", "injuries", "transfers"] as const;
const AMFOOTBALL_ENDPOINTS = ["games", "standings", "players_statistics"] as const;
const BASEBALL_ENDPOINTS = ["games", "standings", "players_statistics"] as const;
const HOCKEY_ENDPOINTS = ["games", "standings", "players_statistics"] as const;
const MMA_ENDPOINTS = ["fights"] as const;
const F1_ENDPOINTS = ["races", "rankings_drivers", "rankings_teams"] as const;

type AnyEndpoint = string;

function endpointsForType(sportType: SportType): readonly string[] {
  switch (sportType) {
    case "basketball":       return BASKETBALL_ENDPOINTS;
    case "soccer":           return SOCCER_ENDPOINTS;
    case "american-football": return AMFOOTBALL_ENDPOINTS;
    case "baseball":         return BASEBALL_ENDPOINTS;
    case "hockey":           return HOCKEY_ENDPOINTS;
    case "mma":              return MMA_ENDPOINTS;
    case "f1":               return F1_ENDPOINTS;
  }
}

// Union of all possible endpoints for the provider interface
const ALL_ENDPOINTS = [
  ...new Set([
    ...BASKETBALL_ENDPOINTS,
    ...SOCCER_ENDPOINTS,
    ...AMFOOTBALL_ENDPOINTS,
    ...BASEBALL_ENDPOINTS,
    ...HOCKEY_ENDPOINTS,
    ...MMA_ENDPOINTS,
    ...F1_ENDPOINTS,
  ]),
];

// ── Fetch helper ────────────────────────────────────────────

interface ApiSportsResponse<T = unknown> {
  get: string;
  parameters: Record<string, string>;
  errors: Record<string, string> | unknown[];
  results: number;
  paging: { current: number; total: number };
  response: T[];
}

async function apiFetch<T = unknown>(domain: ApiDomain, path: string, params: Record<string, string | number> = {}): Promise<T[]> {
  if (!API_KEY) {
    throw new Error("API_SPORTS_KEY environment variable is required");
  }

  const url = new URL(path, baseUrl(domain));
  for (const [k, v] of Object.entries(params)) {
    url.searchParams.set(k, String(v));
  }

  const data = await fetchJSON<ApiSportsResponse<T>>(url.toString(), NAME, RATE_LIMIT, {
    headers: { "x-apisports-key": API_KEY },
  });

  // Check for API-level errors
  const errObj = data.errors;
  if (errObj && typeof errObj === "object" && !Array.isArray(errObj) && Object.keys(errObj).length > 0) {
    throw new Error(`API-Sports error: ${JSON.stringify(errObj)}`);
  }

  return data.response;
}

/** Fetch with pagination support — API-Sports uses ?page= parameter */
async function apiFetchAll<T = unknown>(domain: ApiDomain, path: string, params: Record<string, string | number> = {}): Promise<T[]> {
  if (!API_KEY) {
    throw new Error("API_SPORTS_KEY environment variable is required");
  }

  const all: T[] = [];
  let page = 1;
  let totalPages = 1;

  do {
    const url = new URL(path, baseUrl(domain));
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, String(v));
    }
    url.searchParams.set("page", String(page));

    const data = await fetchJSON<ApiSportsResponse<T>>(url.toString(), NAME, RATE_LIMIT, {
      headers: { "x-apisports-key": API_KEY },
    });

    const errObj = data.errors;
    if (errObj && typeof errObj === "object" && !Array.isArray(errObj) && Object.keys(errObj).length > 0) {
      throw new Error(`API-Sports error: ${JSON.stringify(errObj)}`);
    }

    all.push(...data.response);
    totalPages = data.paging.total;
    page++;
  } while (page <= totalPages);

  return all;
}

// ── Endpoint context ────────────────────────────────────────

interface EndpointContext {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
  config: SportConfig;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

// ── Basketball endpoints ────────────────────────────────────

async function importBasketballGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/games", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games", `Saved ${data.length} games`);
  return { filesWritten: 1, errors: [] };
}

async function importBasketballStandings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "standings.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "standings", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "standings", `Fetching ${season} standings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/standings", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "standings", `Saved standings`);
  return { filesWritten: 1, errors: [] };
}

async function importBasketballPlayerStats(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "players_statistics.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "players_statistics", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "players_statistics", `Fetching ${season} player statistics`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/players/statistics", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "players_statistics", `Saved ${data.length} player stat records`);
  return { filesWritten: 1, errors: [] };
}

async function importBasketballInjuries(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "injuries.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "injuries", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "injuries", `Fetching ${season} injuries`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/injuries", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "injuries", `Saved ${data.length} injuries`);
  return { filesWritten: 1, errors: [] };
}

// ── Soccer endpoints ────────────────────────────────────────

async function importSoccerFixtures(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "fixtures.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "fixtures", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "fixtures", `Fetching ${season} fixtures`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  // Soccer /fixtures endpoint does NOT support pagination — use apiFetch
  const data = await apiFetch(config.domain, "/fixtures", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "fixtures", `Saved ${data.length} fixtures`);
  return { filesWritten: 1, errors: [] };
}

async function importSoccerStandings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "standings.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "standings", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "standings", `Fetching ${season} standings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/standings", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "standings", `Saved standings`);
  return { filesWritten: 1, errors: [] };
}

async function importSoccerTopScorers(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "players_topscorers.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "players_topscorers", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "players_topscorers", `Fetching ${season} top scorers`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  // Soccer /players/topscorers endpoint does NOT support pagination
  const data = await apiFetch(config.domain, "/players/topscorers", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "players_topscorers", `Saved ${data.length} top scorers`);
  return { filesWritten: 1, errors: [] };
}

async function importSoccerInjuries(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "injuries.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "injuries", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "injuries", `Fetching ${season} injuries`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/injuries", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "injuries", `Saved ${data.length} injuries`);
  return { filesWritten: 1, errors: [] };
}

async function importSoccerTransfers(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "transfers.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "transfers", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "transfers", `Fetching ${season} transfers`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  // Transfers endpoint uses team parameter; fetch all teams first, then their transfers
  const teams = await apiFetch<{ team: { id: number } }>(config.domain, "/teams", { league: config.leagueId, season: season });
  const allTransfers: unknown[] = [];
  for (const entry of teams) {
    try {
      const transfers = await apiFetch(config.domain, "/transfers", { team: entry.team.id });
      allTransfers.push(...transfers);
    } catch {
      // Some teams may have no transfer data
    }
  }

  writeJSON(outFile, allTransfers);
  logger.progress(NAME, sport, "transfers", `Saved ${allTransfers.length} transfers`);
  return { filesWritten: 1, errors: [] };
}

// ── American Football endpoints ─────────────────────────────

async function importAmFootballGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/games", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games", `Saved ${data.length} games`);
  return { filesWritten: 1, errors: [] };
}

async function importAmFootballStandings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "standings.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "standings", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "standings", `Fetching ${season} standings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/standings", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "standings", `Saved standings`);
  return { filesWritten: 1, errors: [] };
}

async function importAmFootballPlayerStats(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "players_statistics.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "players_statistics", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "players_statistics", `Fetching ${season} player statistics`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/players/statistics", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "players_statistics", `Saved ${data.length} player stat records`);
  return { filesWritten: 1, errors: [] };
}

// ── Baseball endpoints ──────────────────────────────────────

async function importBaseballGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/games", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games", `Saved ${data.length} games`);
  return { filesWritten: 1, errors: [] };
}

async function importBaseballStandings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "standings.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "standings", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "standings", `Fetching ${season} standings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/standings", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "standings", `Saved standings`);
  return { filesWritten: 1, errors: [] };
}

async function importBaseballPlayerStats(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "players_statistics.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "players_statistics", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "players_statistics", `Fetching ${season} player statistics`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/players/statistics", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "players_statistics", `Saved ${data.length} player stat records`);
  return { filesWritten: 1, errors: [] };
}

// ── Hockey endpoints ────────────────────────────────────────

async function importHockeyGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/games", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games", `Saved ${data.length} games`);
  return { filesWritten: 1, errors: [] };
}

async function importHockeyStandings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "standings.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "standings", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "standings", `Fetching ${season} standings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/standings", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "standings", `Saved standings`);
  return { filesWritten: 1, errors: [] };
}

async function importHockeyPlayerStats(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "players_statistics.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "players_statistics", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "players_statistics", `Fetching ${season} player statistics`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/players/statistics", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "players_statistics", `Saved ${data.length} player stat records`);
  return { filesWritten: 1, errors: [] };
}

// ── MMA endpoints ───────────────────────────────────────────

async function importMmaFights(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "fights.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "fights", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "fights", `Fetching ${season} fights`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetchAll(config.domain, "/fights", { league: config.leagueId, season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "fights", `Saved ${data.length} fights`);
  return { filesWritten: 1, errors: [] };
}

// ── F1 endpoints ────────────────────────────────────────────

async function importF1Races(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "races.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "races", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "races", `Fetching ${season} races`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/races", { season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "races", `Saved ${data.length} races`);
  return { filesWritten: 1, errors: [] };
}

async function importF1DriverRankings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "rankings_drivers.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "rankings_drivers", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "rankings_drivers", `Fetching ${season} driver rankings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/rankings/drivers", { season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "rankings_drivers", `Saved driver rankings`);
  return { filesWritten: 1, errors: [] };
}

async function importF1TeamRankings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun, config } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "rankings_teams.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "rankings_teams", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "rankings_teams", `Fetching ${season} team rankings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await apiFetch(config.domain, "/rankings/teams", { season: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "rankings_teams", `Saved team rankings`);
  return { filesWritten: 1, errors: [] };
}

// ── Endpoint dispatch ───────────────────────────────────────

type EndpointFn = (ctx: EndpointContext) => Promise<EndpointResult>;

const ENDPOINT_DISPATCH: Record<SportType, Record<string, EndpointFn>> = {
  basketball: {
    games: importBasketballGames,
    standings: importBasketballStandings,
    players_statistics: importBasketballPlayerStats,
    injuries: importBasketballInjuries,
  },
  soccer: {
    fixtures: importSoccerFixtures,
    standings: importSoccerStandings,
    players_topscorers: importSoccerTopScorers,
    injuries: importSoccerInjuries,
    transfers: importSoccerTransfers,
  },
  "american-football": {
    games: importAmFootballGames,
    standings: importAmFootballStandings,
    players_statistics: importAmFootballPlayerStats,
  },
  baseball: {
    games: importBaseballGames,
    standings: importBaseballStandings,
    players_statistics: importBaseballPlayerStats,
  },
  hockey: {
    games: importHockeyGames,
    standings: importHockeyStandings,
    players_statistics: importHockeyPlayerStats,
  },
  mma: {
    fights: importMmaFights,
  },
  f1: {
    races: importF1Races,
    rankings_drivers: importF1DriverRankings,
    rankings_teams: importF1TeamRankings,
  },
};

// ── Provider implementation ─────────────────────────────────

const apisports: Provider = {
  name: NAME,
  label: "API-Sports",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ALL_ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    if (!API_KEY) {
      logger.error("API_SPORTS_KEY not set — skipping", NAME);
      return {
        provider: NAME,
        sport: "multi",
        filesWritten: 0,
        errors: ["API_SPORTS_KEY environment variable is required"],
        durationMs: 0,
      };
    }

    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const sports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    logger.info(
      `Starting import — ${sports.length} sports, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      const config = SPORT_CONFIG[sport];
      if (!config) continue;

      const sportEndpoints = endpointsForType(config.sportType);

      // Filter requested endpoints to those valid for this sport type
      const endpoints: string[] = opts.endpoints.length
        ? opts.endpoints.filter((e) => sportEndpoints.includes(e))
        : [...sportEndpoints];

      const dispatch = ENDPOINT_DISPATCH[config.sportType];

      for (const season of opts.seasons) {
        logger.info(`── ${sport.toUpperCase()} ${season} ──`, NAME);

        const ctx: EndpointContext = {
          sport,
          season,
          dataDir: opts.dataDir,
          dryRun: opts.dryRun,
          config,
        };

        for (const ep of endpoints) {
          const fn = dispatch[ep];
          if (!fn) continue;

          try {
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

export default apisports;
