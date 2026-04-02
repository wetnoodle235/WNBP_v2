// ──────────────────────────────────────────────────────────
// V5.0 Understat Provider
// ──────────────────────────────────────────────────────────
// Uses Understat AJAX endpoints (same family used by understatAPI):
//   getLeagueData/{league}/{season}
//   getTeamData/{team}/{season}
//   getPlayerData/{playerId}
//   getMatchData/{matchId}
// No API key required.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "understat";
const BASE_URL = "https://understat.com";

const RATE_LIMIT: RateLimitConfig = { requests: 3, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["epl", "laliga", "bundesliga", "seriea", "ligue1"];

// Legacy endpoints are kept for CLI compatibility; additional endpoints expand
// coverage to match the understatAPI surface.
const DEFAULT_ENDPOINTS = [
  "league_standings",
  "league_matches",
  "player_xg",
  "team_xg",
] as const;

const EXTENDED_ENDPOINTS = [
  "team_matches",
  "team_players",
  "team_context",
  "player_matches",
  "player_shots",
  "player_seasons",
  "match_shots",
  "match_rosters",
] as const;

const ALL_ENDPOINTS = [...DEFAULT_ENDPOINTS, ...EXTENDED_ENDPOINTS] as const;
type Endpoint = (typeof ALL_ENDPOINTS)[number];

const AJAX_HEADERS = {
  "X-Requested-With": "XMLHttpRequest",
};

const LEAGUE_MAP: Record<string, string> = {
  epl: "EPL",
  laliga: "La_Liga",
  bundesliga: "Bundesliga",
  seriea: "Serie_A",
  ligue1: "Ligue_1",
};

interface UnderstatLeagueMatch {
  id?: string | number;
  datetime?: string;
  [key: string]: unknown;
}

interface UnderstatLeaguePlayer {
  id?: string | number;
  player_name?: string;
  [key: string]: unknown;
}

interface UnderstatLeagueTeam {
  id?: string | number;
  title?: string;
  history?: unknown;
  [key: string]: unknown;
}

interface UnderstatLeagueData {
  dates?: UnderstatLeagueMatch[];
  players?: UnderstatLeaguePlayer[];
  teams?: Record<string, UnderstatLeagueTeam>;
}

interface EndpointContext {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

function toTeamSlug(teamTitle: string): string {
  return teamTitle.trim().replace(/\s+/g, "_");
}

function sanitizePathSegment(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "") || "unknown";
}

function extractMatchId(match: UnderstatLeagueMatch): string | null {
  const id = match?.id;
  if (id === undefined || id === null) return null;
  const parsed = String(id).trim();
  return parsed.length > 0 ? parsed : null;
}

function extractPlayerId(player: UnderstatLeaguePlayer): string | null {
  const id = player?.id;
  if (id === undefined || id === null) return null;
  const parsed = String(id).trim();
  return parsed.length > 0 ? parsed : null;
}

function extractDate(match: UnderstatLeagueMatch): string {
  const raw = typeof match.datetime === "string" ? match.datetime : "";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return "unknown-date";
  return parsed.toISOString().slice(0, 10);
}

function isoWeekLabel(dateIso: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateIso)) return "unknown";

  const date = new Date(`${dateIso}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return "unknown";

  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - day);

  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNum = Math.ceil((((date.getTime() - yearStart.getTime()) / 86_400_000) + 1) / 7);
  return String(weekNum).padStart(2, "0");
}

function matchPartitionPath(
  dataDir: string,
  sport: Sport,
  season: number,
  match: UnderstatLeagueMatch,
): { baseDir: string; matchId: string; date: string; week: string; seasonType: string } | null {
  const matchId = extractMatchId(match);
  if (!matchId) return null;

  // Understat league pages are regular-season league fixtures.
  const seasonType = "regular";
  const date = extractDate(match);
  const week = isoWeekLabel(date);

  const baseDir = rawPath(
    dataDir,
    NAME,
    sport,
    season,
    "matches",
    "season_type",
    seasonType,
    `week_${week}`,
    date,
    matchId,
  );

  return { baseDir, matchId, date, week, seasonType };
}

async function fetchUnderstat<T = unknown>(path: string): Promise<T> {
  return fetchJSON<T>(`${BASE_URL}/${path}`, NAME, RATE_LIMIT, {
    headers: AJAX_HEADERS,
    timeoutMs: 30_000,
    retries: 3,
  });
}

async function fetchLeagueData(sport: Sport, season: number): Promise<UnderstatLeagueData> {
  const league = LEAGUE_MAP[sport];
  if (!league) throw new Error(`Unknown league mapping for sport: ${sport}`);
  const endpoint = `getLeagueData/${league}/${season}`;
  logger.progress(NAME, sport, "fetch", `Fetching league data: ${endpoint}`);
  return fetchUnderstat<UnderstatLeagueData>(endpoint);
}

function dedupeBy<T>(rows: T[], keyFn: (row: T) => string | null): T[] {
  const map = new Map<string, T>();
  for (const row of rows) {
    const key = keyFn(row);
    if (!key) continue;
    map.set(key, row);
  }
  return [...map.values()];
}

function extractTeamEntries(data: UnderstatLeagueData): Array<{ teamId: string; title: string; slug: string }> {
  const teams = data.teams ?? {};
  const entries: Array<{ teamId: string; title: string; slug: string }> = [];

  for (const [teamId, teamObj] of Object.entries(teams)) {
    const title = typeof teamObj?.title === "string" ? teamObj.title.trim() : "";
    if (!title) continue;
    entries.push({
      teamId: String(teamId),
      title,
      slug: toTeamSlug(title),
    });
  }

  return entries;
}

async function importLeagueStandings(ctx: EndpointContext, leagueData: UnderstatLeagueData): Promise<EndpointResult> {
  const outFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "reference", "league_standings.json");
  const legacyFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "league_standings.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, ctx.sport, "league_standings", `Skipping ${ctx.season} — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const teams = leagueData.teams ?? {};
  if (!Object.keys(teams).length) {
    return { filesWritten: 0, errors: [`league_standings/${ctx.sport}/${ctx.season}: empty teams payload`] };
  }

  writeJSON(outFile, teams);
  writeJSON(legacyFile, teams);
  logger.progress(NAME, ctx.sport, "league_standings", `Saved ${Object.keys(teams).length} teams`);
  return { filesWritten: 2, errors: [] };
}

async function importLeaguePlayers(ctx: EndpointContext, leagueData: UnderstatLeagueData): Promise<EndpointResult> {
  const outFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "reference", "league_players.json");
  const legacyFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "player_xg.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, ctx.sport, "player_xg", `Skipping ${ctx.season} — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const players = dedupeBy(leagueData.players ?? [], extractPlayerId);
  if (!players.length) {
    return { filesWritten: 0, errors: [`player_xg/${ctx.sport}/${ctx.season}: empty players payload`] };
  }

  writeJSON(outFile, players);
  writeJSON(legacyFile, players);
  logger.progress(NAME, ctx.sport, "player_xg", `Saved ${players.length} players`);
  return { filesWritten: 2, errors: [] };
}

async function importLeagueMatches(ctx: EndpointContext, leagueData: UnderstatLeagueData): Promise<EndpointResult> {
  const outFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "matches", "index.json");
  const legacyFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "league_matches.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, ctx.sport, "league_matches", `Skipping ${ctx.season} — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const matches = dedupeBy(leagueData.dates ?? [], extractMatchId);
  if (!matches.length) {
    return { filesWritten: 0, errors: [`league_matches/${ctx.sport}/${ctx.season}: empty matches payload`] };
  }

  writeJSON(outFile, matches);
  writeJSON(legacyFile, matches);

  let filesWritten = 2;
  for (const match of matches) {
    const partition = matchPartitionPath(ctx.dataDir, ctx.sport, ctx.season, match);
    if (!partition) continue;
    const matchFile = `${partition.baseDir}/match.json`;
    if (fileExists(matchFile)) continue;
    writeJSON(matchFile, match);
    filesWritten++;
  }

  logger.progress(NAME, ctx.sport, "league_matches", `Saved ${matches.length} match records`);
  return { filesWritten, errors: [] };
}

async function importTeamXg(ctx: EndpointContext, leagueData: UnderstatLeagueData): Promise<EndpointResult> {
  const outFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "reference", "team_xg.json");
  const legacyFile = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "team_xg.json");

  if (fileExists(outFile)) {
    logger.progress(NAME, ctx.sport, "team_xg", `Skipping ${ctx.season} — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const teams = leagueData.teams ?? {};
  if (!Object.keys(teams).length) {
    return { filesWritten: 0, errors: [`team_xg/${ctx.sport}/${ctx.season}: empty teams payload`] };
  }

  writeJSON(outFile, teams);
  writeJSON(legacyFile, teams);
  logger.progress(NAME, ctx.sport, "team_xg", `Saved team xG for ${Object.keys(teams).length} teams`);
  return { filesWritten: 2, errors: [] };
}

async function importTeamEndpoint(
  ctx: EndpointContext,
  leagueData: UnderstatLeagueData,
  endpoint: "team_matches" | "team_players" | "team_context",
): Promise<EndpointResult> {
  const teamEntries = extractTeamEntries(leagueData);
  if (!teamEntries.length) {
    return { filesWritten: 0, errors: [`${endpoint}/${ctx.sport}/${ctx.season}: no teams found`] };
  }

  let filesWritten = 0;
  const errors: string[] = [];

  for (const team of teamEntries) {
    const teamDir = rawPath(
      ctx.dataDir,
      NAME,
      ctx.sport,
      ctx.season,
      "teams",
      `${sanitizePathSegment(team.slug)}__${team.teamId}`,
    );

    const outputName = endpoint === "team_matches"
      ? "matches.json"
      : endpoint === "team_players"
        ? "players.json"
        : "context.json";

    const outFile = `${teamDir}/${outputName}`;
    if (fileExists(outFile)) continue;
    if (ctx.dryRun) {
      filesWritten++;
      continue;
    }

    try {
      const teamData = await fetchUnderstat<Record<string, unknown>>(
        `getTeamData/${encodeURIComponent(team.slug)}/${ctx.season}`,
      );

      const payload = endpoint === "team_matches"
        ? (teamData.dates ?? [])
        : endpoint === "team_players"
          ? (teamData.players ?? [])
          : (teamData.statistics ?? {});

      writeJSON(outFile, payload);
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`${endpoint}/${ctx.sport}/${ctx.season}/${team.slug}: ${msg}`);
    }
  }

  logger.progress(NAME, ctx.sport, endpoint, `Saved ${filesWritten} files`);
  return { filesWritten, errors };
}

async function importPlayerEndpoint(
  ctx: EndpointContext,
  leagueData: UnderstatLeagueData,
  endpoint: "player_matches" | "player_shots" | "player_seasons",
): Promise<EndpointResult> {
  const players = dedupeBy(leagueData.players ?? [], extractPlayerId);
  if (!players.length) {
    return { filesWritten: 0, errors: [`${endpoint}/${ctx.sport}/${ctx.season}: no players found`] };
  }

  let filesWritten = 0;
  const errors: string[] = [];

  for (const player of players) {
    const playerId = extractPlayerId(player);
    if (!playerId) continue;

    const playerName = typeof player.player_name === "string" ? player.player_name : "unknown";
    const outFile = rawPath(
      ctx.dataDir,
      NAME,
      ctx.sport,
      ctx.season,
      "players",
      `${sanitizePathSegment(playerName)}__${playerId}`,
      endpoint === "player_matches"
        ? "matches.json"
        : endpoint === "player_shots"
          ? "shots.json"
          : "seasons.json",
    );

    if (fileExists(outFile)) continue;
    if (ctx.dryRun) {
      filesWritten++;
      continue;
    }

    try {
      const playerData = await fetchUnderstat<Record<string, unknown>>(`getPlayerData/${playerId}`);
      const payload = endpoint === "player_matches"
        ? (playerData.matches ?? [])
        : endpoint === "player_shots"
          ? (playerData.shots ?? [])
          : (playerData.groups ?? []);
      writeJSON(outFile, payload);
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`${endpoint}/${ctx.sport}/${ctx.season}/${playerId}: ${msg}`);
    }
  }

  logger.progress(NAME, ctx.sport, endpoint, `Saved ${filesWritten} files`);
  return { filesWritten, errors };
}

async function importMatchEndpoint(
  ctx: EndpointContext,
  leagueData: UnderstatLeagueData,
  endpoint: "match_shots" | "match_rosters",
): Promise<EndpointResult> {
  const matches = dedupeBy(leagueData.dates ?? [], extractMatchId);
  if (!matches.length) {
    return { filesWritten: 0, errors: [`${endpoint}/${ctx.sport}/${ctx.season}: no matches found`] };
  }

  let filesWritten = 0;
  const errors: string[] = [];

  for (const match of matches) {
    const partition = matchPartitionPath(ctx.dataDir, ctx.sport, ctx.season, match);
    if (!partition) continue;

    const outFile = endpoint === "match_shots"
      ? `${partition.baseDir}/shots.json`
      : `${partition.baseDir}/rosters.json`;

    if (fileExists(outFile)) continue;
    if (ctx.dryRun) {
      filesWritten++;
      continue;
    }

    try {
      const matchData = await fetchUnderstat<Record<string, unknown>>(`getMatchData/${partition.matchId}`);
      const payload = endpoint === "match_shots"
        ? (matchData.shots ?? {})
        : (matchData.rosters ?? {});

      writeJSON(outFile, payload);
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`${endpoint}/${ctx.sport}/${ctx.season}/${partition.matchId}: ${msg}`);
    }
  }

  logger.progress(NAME, ctx.sport, endpoint, `Saved ${filesWritten} files`);
  return { filesWritten, errors };
}

const understat: Provider = {
  name: NAME,
  label: "Understat (xG)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: [...ALL_ENDPOINTS],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const sports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    const endpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) => ALL_ENDPOINTS.includes(e as Endpoint)) as Endpoint[])
      : [...DEFAULT_ENDPOINTS];

    logger.info(
      `Starting import — ${sports.length} leagues, ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      for (const season of opts.seasons) {
        logger.info(`── ${LEAGUE_MAP[sport] ?? sport} ${season} ──`, NAME);

        let leagueData: UnderstatLeagueData;
        try {
          leagueData = await fetchLeagueData(sport, season);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          allErrors.push(`${sport}/${season}: ${msg}`);
          logger.error(`Failed league fetch ${sport}/${season}: ${msg}`, NAME);
          continue;
        }

        for (const ep of endpoints) {
          try {
            const ctx: EndpointContext = {
              sport,
              season,
              dataDir: opts.dataDir,
              dryRun: opts.dryRun,
            };

            let result: EndpointResult;
            switch (ep) {
              case "league_standings":
                result = await importLeagueStandings(ctx, leagueData);
                break;
              case "league_matches":
                result = await importLeagueMatches(ctx, leagueData);
                break;
              case "player_xg":
                result = await importLeaguePlayers(ctx, leagueData);
                break;
              case "team_xg":
                result = await importTeamXg(ctx, leagueData);
                break;
              case "team_matches":
              case "team_players":
              case "team_context":
                result = await importTeamEndpoint(ctx, leagueData, ep);
                break;
              case "player_matches":
              case "player_shots":
              case "player_seasons":
                result = await importPlayerEndpoint(ctx, leagueData, ep);
                break;
              case "match_shots":
              case "match_rosters":
                result = await importMatchEndpoint(ctx, leagueData, ep);
                break;
            }

            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            allErrors.push(`${sport}/${season}/${ep}: ${msg}`);
            logger.error(`${sport}/${season}/${ep}: ${msg}`, NAME);
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

export default understat;
