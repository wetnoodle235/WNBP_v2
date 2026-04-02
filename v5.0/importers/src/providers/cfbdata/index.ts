// ──────────────────────────────────────────────────────────
// V5.0 College Football Data Provider
// ──────────────────────────────────────────────────────────
// Fetches games, stats, play-by-play, rankings, recruiting,
// and ratings from the College Football Data API.
// Requires CFB_DATA_KEY environment variable.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath, rawPathWithWeekDate, fileExists, ensureDir } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import fs from "node:fs";
import path from "node:path";

// ── Constants ───────────────────────────────────────────────

const NAME = "cfbdata";
const BASE_URL = "https://api.collegefootballdata.com";
const API_KEY = process.env.CFB_DATA_KEY ?? "";

// ~5 req/sec
const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["ncaaf"];
const SEASON_TYPES_TO_COLLECT = ["regular", "postseason", "allstar", "spring_regular", "spring_postseason"] as const;
type SeasonType = (typeof SEASON_TYPES_TO_COLLECT)[number];
const SEASON_TYPE_WEEK_PLAN_CACHE = new Map<string, Array<{ seasonType: string; weeks: number[] }>>();

class CfbQuotaExceededError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CfbQuotaExceededError";
  }
}

function isCfbQuotaExceededError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message.toLowerCase();
  return msg.includes("monthly call quota exceeded");
}

const ALL_ENDPOINTS = [
  "games",
  "games_teams",
  "games_players",
  "games_media",
  "games_weather",
  "scoreboard",
  "game_box_advanced",
  "records",
  "calendar",
  "drives",
  "plays",
  "plays_types",
  "plays_stats",
  "plays_stats_types",
  "live_plays",
  "lines",
  "teams",
  "teams_fbs",
  "teams_ats",
  "roster",
  "coaches",
  "conferences",
  "venues",
  "stats_season",
  "stats_player_season",
  "stats_categories",
  "stats_advanced",
  "stats_game_advanced",
  "stats_game_havoc",
  "rankings",
  "recruiting",
  "recruiting_teams",
  "recruiting_groups",
  "talent",
  "ratings_sp",
  "ratings_sp_conferences",
  "ratings_srs",
  "ratings_elo",
  "ratings_fpi",
  "ppa_predicted",
  "ppa_teams",
  "ppa_games",
  "ppa_players_games",
  "ppa_players_season",
  "wp_pregame",
  "draft_teams",
  "draft_positions",
  "draft_picks",
  "wepa_team_season",
  "wepa_players_passing",
  "wepa_players_rushing",
  "wepa_players_kicking",
  "player_usage",
  "player_returning",
  "player_portal",
  "metrics_wp",
  "metrics_fg_ep",
  "info",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── Fetch helper ────────────────────────────────────────────

async function cfbFetch<T = unknown>(path: string, params: Record<string, string | number> = {}): Promise<T> {
  if (!API_KEY) {
    throw new Error("CFB_DATA_KEY environment variable is required");
  }

  const url = new URL(path, BASE_URL);
  for (const [k, v] of Object.entries(params)) {
    url.searchParams.set(k, String(v));
  }

  try {
    return await fetchJSON<T>(url.toString(), NAME, RATE_LIMIT, {
      headers: { Authorization: `Bearer ${API_KEY}` },
    });
  } catch (err) {
    if (isCfbQuotaExceededError(err)) {
      throw new CfbQuotaExceededError("CFBData monthly call quota exceeded");
    }
    throw err;
  }
}

// ── Helper functions ────────────────────────────────────────

/**
 * Extract week and date from a CFBData API game object.
 * Week defaults to 1 if not provided (non-postseason).
 * Date is extracted from startDate field (YYYY-MM-DD).
 */
function getWeekAndDate(game: any): { week: number; date: string } {
  let week = game.week ?? 1;
  if (game.seasonType === "postseason") {
    week = game.week ?? 0;
  }
  const date = game.startDate?.split('T')[0] ?? "unknown";
  return { week, date };
}

function seasonTypeKey(value: unknown): string {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) return "regular";
  if (SEASON_TYPES_TO_COLLECT.includes(normalized as SeasonType)) return normalized;
  if (normalized === "both") return "both";
  return normalized.replace(/\s+/g, "_");
}

function weekKey(week: number): string {
  return `week_${String(week).padStart(2, "0")}`;
}

function endpointPartitionPath(
  dataDir: string,
  sport: Sport,
  season: number,
  endpoint: string,
  options: {
    seasonType?: string;
    week?: number;
    date?: string;
  },
  ...segments: string[]
): string {
  const parts = [endpoint];
  if (options.seasonType) parts.push(options.seasonType);
  if (typeof options.week === "number") parts.push(weekKey(options.week));
  if (options.date) parts.push(options.date);
  return rawPath(dataDir, NAME, sport, season, ...parts, ...segments);
}

type GameMeta = { seasonType: string; week: number; date: string };

function buildGameMetaIndex(dataDir: string, sport: Sport, season: number): Map<string, GameMeta> {
  const index = new Map<string, GameMeta>();
  const seasonDir = rawPath(dataDir, NAME, sport, season);
  if (!fs.existsSync(seasonDir)) return index;

  const gamesEndpointDir = rawPath(dataDir, NAME, sport, season, "games");
  if (fs.existsSync(gamesEndpointDir)) {
    for (const seasonTypeDirEntry of fs.readdirSync(gamesEndpointDir, { withFileTypes: true })) {
      if (!seasonTypeDirEntry.isDirectory()) continue;
      const seasonType = seasonTypeDirEntry.name;
      const seasonTypeDir = path.join(gamesEndpointDir, seasonType);
      if (!fs.existsSync(seasonTypeDir)) continue;
      for (const weekDir of fs.readdirSync(seasonTypeDir)) {
        const weekPath = path.join(seasonTypeDir, weekDir);
        if (!fs.statSync(weekPath).isDirectory()) continue;
        const week = Number(weekDir.replace("week_", ""));
        for (const dateDir of fs.readdirSync(weekPath)) {
          const datePath = path.join(weekPath, dateDir);
          if (!fs.statSync(datePath).isDirectory()) continue;
          for (const file of fs.readdirSync(datePath)) {
            if (!file.endsWith(".json")) continue;
            const gameId = file.replace(/\.json$/, "");
            if (gameId.includes("_")) continue;
            index.set(gameId, { seasonType: seasonTypeKey(seasonType), week, date: dateDir });
          }
        }
      }
    }
  }

  for (let week = 1; week <= 20; week++) {
    const legacyGamesDir = rawPathWithWeekDate(dataDir, NAME, sport, season, week, "", "games");
    if (!fs.existsSync(legacyGamesDir)) continue;
    for (const dateDir of fs.readdirSync(legacyGamesDir)) {
      const datePath = path.join(legacyGamesDir, dateDir);
      if (!fs.statSync(datePath).isDirectory()) continue;
      for (const file of fs.readdirSync(datePath)) {
        if (!file.endsWith(".json")) continue;
        const gameId = file.replace(/\.json$/, "");
        if (gameId.includes("_")) continue;
        index.set(gameId, { seasonType: week >= 16 ? "postseason" : "regular", week, date: dateDir });
      }
    }
  }

  return index;
}

async function fetchSeasonTypeChunks<T>(endpoint: string, season: number): Promise<T[]> {
  const chunks: T[][] = [];
  for (const seasonType of SEASON_TYPES_TO_COLLECT) {
    try {
      chunks.push(await cfbFetch<T[]>(endpoint, { year: season, seasonType }));
    } catch (err) {
      if (err instanceof CfbQuotaExceededError) throw err;
      chunks.push([]);
    }
  }
  return chunks.flat();
}

async function fetchGamesForAllSeasonTypes(season: number): Promise<any[]> {
  const games = await fetchSeasonTypeChunks<any>("/games", season);
  const deduped = new Map<string, any>();
  for (const game of games) {
    const gameId = String(game?.id ?? "");
    if (!gameId) continue;
    if (!deduped.has(gameId)) deduped.set(gameId, game);
  }
  return Array.from(deduped.values());
}

async function discoverSeasonTypeWeeks(dataDir: string, sport: Sport, season: number): Promise<Array<{ seasonType: string; weeks: number[] }>> {
  const cacheKey = `${sport}:${season}`;
  const cached = SEASON_TYPE_WEEK_PLAN_CACHE.get(cacheKey);
  if (cached) return cached;

  const seasonTypeWeeks = new Map<string, Set<number>>();
  const gameMeta = buildGameMetaIndex(dataDir, sport, season);
  for (const meta of gameMeta.values()) {
    if (!meta.week || meta.week <= 0) continue;
    if (!seasonTypeWeeks.has(meta.seasonType)) seasonTypeWeeks.set(meta.seasonType, new Set<number>());
    seasonTypeWeeks.get(meta.seasonType)!.add(meta.week);
  }

  if (seasonTypeWeeks.size === 0) {
    const games = await fetchGamesForAllSeasonTypes(season);
    for (const game of games) {
      const seasonType = seasonTypeKey(game?.seasonType);
      const week = Number(game?.week ?? 0);
      if (!week || week <= 0) continue;
      if (!seasonTypeWeeks.has(seasonType)) seasonTypeWeeks.set(seasonType, new Set<number>());
      seasonTypeWeeks.get(seasonType)!.add(week);
    }
  }

  if (seasonTypeWeeks.size === 0) {
    seasonTypeWeeks.set("regular", new Set(Array.from({ length: 16 }, (_, i) => i + 1)));
    seasonTypeWeeks.set("postseason", new Set(Array.from({ length: 5 }, (_, i) => i + 1)));
  }

  const plan = Array.from(seasonTypeWeeks.entries())
    .map(([seasonType, weeks]) => ({ seasonType, weeks: Array.from(weeks).sort((a, b) => a - b) }))
    .sort((a, b) => a.seasonType.localeCompare(b.seasonType));

  SEASON_TYPE_WEEK_PLAN_CACHE.set(cacheKey, plan);
  return plan;
}

function deriveRowPartition(
  row: any,
  fallback: { seasonType?: string; week?: number; date?: string } = {},
): { gameId: string; seasonType: string; week: number; date?: string } | null {
  const gameId = String(row?.gameId ?? row?.id ?? "");
  if (!gameId) return null;
  const seasonType = seasonTypeKey(row?.seasonType ?? fallback.seasonType);
  const week = Number(row?.week ?? fallback.week ?? 0);
  const date = row?.startDate?.split("T")[0] ?? row?.startTime?.split("T")[0] ?? fallback.date;
  return { gameId, seasonType, week, date };
}

// ── Endpoint context ────────────────────────────────────────

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

async function importSeasonFile(
  ctx: EndpointContext,
  endpoint: string,
  outputFile: string,
  logName: string,
  extraParams: Record<string, string | number> = {},
): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, logName, outputFile);
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, logName, "Skipping — already exists");
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, logName, `Fetching ${season}`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch(endpoint, { year: season, ...extraParams });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, logName, "Saved");
  return { filesWritten: 1, errors: [] };
}

async function importStaticFile(
  ctx: EndpointContext,
  endpoint: string,
  outputFile: string,
  logName: string,
): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, logName, outputFile);
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, logName, "Skipping — already exists");
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, logName, "Fetching");
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch(endpoint);
  writeJSON(outFile, data);
  logger.progress(NAME, sport, logName, "Saved");
  return { filesWritten: 1, errors: [] };
}

// ── Endpoint implementations ────────────────────────────────

async function importGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const allGames = await fetchGamesForAllSeasonTypes(season);

  // Save each game to individual file in {week}/games/{date}/{gameId}.json
  for (const game of allGames) {
    const gameId = String(game.id ?? "");
    if (!gameId) continue;

    const { week, date } = getWeekAndDate(game);
    if (date === "unknown") {
      errors.push(`games/${gameId}: no startDate`);
      continue;
    }

    const outFile = endpointPartitionPath(
      dataDir,
      sport,
      season,
      "games",
      { seasonType: seasonTypeKey(game.seasonType), week, date },
      `${gameId}.json`,
    );

    if (fileExists(outFile)) continue;

    ensureDir(path.dirname(outFile));
    writeJSON(outFile, game);
    filesWritten++;

    if (filesWritten % 50 === 0) {
      logger.progress(NAME, sport, "games", `Saved ${filesWritten} game files`);
    }
  }

  logger.progress(NAME, sport, "games", `Saved ${filesWritten} game files total`);
  return { filesWritten, errors };
}

async function importGamesTeams(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "games_teams", `Fetching ${season} team game stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const gameMeta = buildGameMetaIndex(dataDir, sport, season);
  let filesWritten = 0;
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);

  const errors: string[] = [];
  for (const plan of seasonTypeWeeks) {
    for (const week of plan.weeks) {
      try {
        const chunk = await cfbFetch<unknown[]>("/games/teams", {
          year: season,
          seasonType: plan.seasonType,
          week,
        });
        if (Array.isArray(chunk) && chunk.length > 0) {
          for (const row of chunk) {
            const gameId = String((row as any)?.id ?? "");
            if (!gameId) continue;
            const meta = gameMeta.get(gameId);
            const date = meta?.date ?? "unknown";
            const outFile = endpointPartitionPath(
              dataDir,
              sport,
              season,
              "games_teams",
              { seasonType: plan.seasonType, week, date },
              `${gameId}.json`,
            );
            if (fileExists(outFile)) continue;
            writeJSON(outFile, row);
            filesWritten++;
          }
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`games_teams/${plan.seasonType}_week_${week}: ${msg}`);
      }
    }
  }
  logger.progress(NAME, sport, "games_teams", `Saved ${filesWritten} game team files`);
  return { filesWritten, errors };
}

async function importGamesPlayers(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "games_players", `Fetching ${season} player game stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  const gameMeta = buildGameMetaIndex(dataDir, sport, season);
  let filesWritten = 0;
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);

    const errors: string[] = [];
    for (const plan of seasonTypeWeeks) {
      for (const week of plan.weeks) {
        try {
          const chunk = await cfbFetch<unknown[]>("/games/players", {
            year: season,
            seasonType: plan.seasonType,
            week,
          });
          if (Array.isArray(chunk) && chunk.length > 0) {
            for (const row of chunk) {
              const gameId = String((row as any)?.id ?? "");
              if (!gameId) continue;
              const meta = gameMeta.get(gameId);
              const date = meta?.date ?? "unknown";
              const outFile = endpointPartitionPath(
                dataDir,
                sport,
                season,
                "games_players",
                { seasonType: plan.seasonType, week, date },
                `${gameId}.json`,
              );
              if (fileExists(outFile)) continue;
              writeJSON(outFile, row);
              filesWritten++;
            }
          }
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          // Missing week data is expected in many seasons, so record and continue.
          errors.push(`${plan.seasonType}_week_${week}: ${msg}`);
        }
      }
    }
    logger.progress(NAME, sport, "games_players", `Saved ${filesWritten} player game files`);
    return { filesWritten, errors };
}

async function importGamesMedia(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "games_media", `Fetching ${season}`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  const mediaRows = await fetchSeasonTypeChunks<any>("/games/media", season);
  let filesWritten = 0;
  for (const row of mediaRows) {
    const meta = deriveRowPartition(row);
    if (!meta || !meta.week) continue;
    const outFile = endpointPartitionPath(dataDir, sport, season, "games_media", meta, `${meta.gameId}.json`);
    if (fileExists(outFile)) continue;
    writeJSON(outFile, row);
    filesWritten++;
  }
  logger.progress(NAME, sport, "games_media", `Saved ${filesWritten} media files`);
  return { filesWritten, errors: [] };
}

async function importGamesWeather(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/games/weather", "games_weather.json", "games_weather");
}

async function importScoreboard(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "scoreboard", "scoreboard.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "scoreboard", "Skipping — already exists");
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "scoreboard", `Fetching ${season} scoreboard`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const scoreboardRows = await fetchSeasonTypeChunks<unknown>("/scoreboard", season);
  writeJSON(outFile, scoreboardRows);
  logger.progress(NAME, sport, "scoreboard", "Saved scoreboard");
  return { filesWritten: 1, errors: [] };
}

async function importGameBoxAdvanced(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "game_box_advanced", `Fetching ${season} advanced game boxes`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  let games: Array<{ id?: number | string }> = [];
  const gamesFile = rawPath(dataDir, NAME, sport, season, "games", "games.json");
  const legacyGamesFile = rawPath(dataDir, NAME, sport, season, "games.json");
  const existingGamesFile = fileExists(gamesFile) ? gamesFile : legacyGamesFile;
  if (fileExists(existingGamesFile)) {
    const existing = await import("node:fs/promises").then(async (fs) => {
      const txt = await fs.readFile(existingGamesFile, "utf-8");
      return JSON.parse(txt) as Array<{ id?: number | string }>;
    });
    if (Array.isArray(existing)) games = existing;
  }
  if (!games.length) {
    games = await fetchGamesForAllSeasonTypes(season);
  }

  const gameIds = Array.from(new Set(games.map((g) => String(g.id ?? "")).filter(Boolean)));
  for (const gameId of gameIds) {
    const outFile = rawPath(dataDir, NAME, sport, season, "game_box_advanced", `${gameId}.json`);
    if (fileExists(outFile)) continue;
    try {
      const data = await cfbFetch("/game/box/advanced", { id: gameId });
      writeJSON(outFile, data);
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`game_box_advanced/${gameId}: ${msg}`);
    }
  }
  logger.progress(NAME, sport, "game_box_advanced", `Saved ${filesWritten} game boxes`);
  return { filesWritten, errors };
}

async function importRecords(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/records", "records.json", "records");
}

async function importCalendar(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/calendar", "calendar.json", "calendar");
}

async function importDrives(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "drives", `Fetching ${season} drives`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const gameMeta = buildGameMetaIndex(dataDir, sport, season);
  const filesWrittenErrors: string[] = [];
  let filesWritten = 0;
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);

  for (const plan of seasonTypeWeeks) {
    for (const week of plan.weeks) {
      try {
        const chunk = await cfbFetch<any[]>("/drives", { year: season, seasonType: plan.seasonType, week });
        const byGame = new Map<string, any[]>();
        for (const row of chunk ?? []) {
          const gameId = String(row?.gameId ?? "");
          if (!gameId) continue;
          if (!byGame.has(gameId)) byGame.set(gameId, []);
          byGame.get(gameId)!.push(row);
        }
        for (const [gameId, rows] of byGame) {
          const meta = gameMeta.get(gameId);
          const date = meta?.date ?? "unknown";
          const outFile = endpointPartitionPath(dataDir, sport, season, "drives", { seasonType: plan.seasonType, week, date }, `${gameId}.json`);
          if (fileExists(outFile)) continue;
          writeJSON(outFile, rows);
          filesWritten++;
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        filesWrittenErrors.push(`drives/${plan.seasonType}_week_${week}: ${msg}`);
      }
    }
  }
  logger.progress(NAME, sport, "drives", `Saved ${filesWritten} drive files`);
  return { filesWritten, errors: filesWrittenErrors };
}

async function importPlays(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "plays", `Fetching ${season} play-by-play (regular + postseason)`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const gameMeta = buildGameMetaIndex(dataDir, sport, season);
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);

  for (const plan of seasonTypeWeeks) {
    for (const week of plan.weeks) {
      try {
        const plays = await cfbFetch<any[]>("/plays", {
          year: season,
          week,
          seasonType: plan.seasonType,
        });

        if (!Array.isArray(plays) || plays.length === 0) {
          continue;
        }

        // Group plays by game_id
        const playsByGame = new Map<string, any[]>();
        for (const play of plays) {
          const gameId = String(play.game_id ?? "");
          if (!gameId) continue;

          if (!playsByGame.has(gameId)) {
            playsByGame.set(gameId, []);
          }
          playsByGame.get(gameId)!.push(play);
        }

        // Save each game's plays to separate file
        for (const [gameId, gamePlays] of playsByGame) {
          const meta = gameMeta.get(gameId);
          const date = meta?.date ?? "unknown";
          const outFile = endpointPartitionPath(
            dataDir,
            sport,
            season,
            "plays",
            { seasonType: plan.seasonType, week, date },
            `${gameId}.json`,
          );

          if (fileExists(outFile)) continue;

          ensureDir(path.dirname(outFile));
          writeJSON(outFile, gamePlays);
          filesWritten++;
        }

        logger.progress(NAME, sport, "plays", `Saved ${plan.seasonType} week ${week} (${playsByGame.size} games)`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn(`plays ${plan.seasonType} week ${week}: ${msg}`, NAME);
        errors.push(`plays/${plan.seasonType}_week_${week}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, sport, "plays", `Saved ${filesWritten} play files total`);
  return { filesWritten, errors };
}

async function importPlaysTypes(ctx: EndpointContext): Promise<EndpointResult> {
  return importStaticFile(ctx, "/plays/types", "plays_types.json", "plays_types");
}

async function importPlaysStats(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/plays/stats", "plays_stats.json", "plays_stats");
}

async function importPlaysStatsTypes(ctx: EndpointContext): Promise<EndpointResult> {
  return importStaticFile(ctx, "/plays/stats/types", "plays_stats_types.json", "plays_stats_types");
}

async function importLivePlays(ctx: EndpointContext): Promise<EndpointResult> {
  return importStaticFile(ctx, "/live/plays", "live_plays.json", "live_plays");
}

async function importLines(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "lines", `Fetching ${season} betting lines`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  let filesWritten = 0;
  const errors: string[] = [];
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);

  for (const plan of seasonTypeWeeks) {
    for (const week of plan.weeks) {
      try {
        const chunk = await cfbFetch<any[]>("/lines", { year: season, seasonType: plan.seasonType, week });
        for (const row of chunk ?? []) {
          const gameId = String(row?.id ?? "");
          if (!gameId) continue;
          const date = row?.startDate?.split("T")[0] ?? "unknown";
          const outFile = endpointPartitionPath(dataDir, sport, season, "lines", { seasonType: plan.seasonType, week, date }, `${gameId}.json`);
          if (fileExists(outFile)) continue;
          writeJSON(outFile, row);
          filesWritten++;
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`lines/${plan.seasonType}_week_${week}: ${msg}`);
      }
    }
  }
  logger.progress(NAME, sport, "lines", `Saved ${filesWritten} betting line files`);
  return { filesWritten, errors };
}

async function importTeams(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "teams", "teams.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "teams", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "teams", `Fetching ${season} teams`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/teams", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "teams", `Saved teams`);
  return { filesWritten: 1, errors: [] };
}

async function importTeamsFbs(ctx: EndpointContext): Promise<EndpointResult> {
  return importStaticFile(ctx, "/teams/fbs", "teams_fbs.json", "teams_fbs");
}

async function importTeamsAts(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/teams/ats", "teams_ats.json", "teams_ats");
}

async function importRoster(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "roster", "roster.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "roster", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "roster", `Fetching ${season} rosters`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/roster", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "roster", `Saved rosters`);
  return { filesWritten: 1, errors: [] };
}

async function importCoaches(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "coaches", "coaches.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "coaches", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "coaches", `Fetching ${season} coaches`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/coaches", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "coaches", `Saved coaches`);
  return { filesWritten: 1, errors: [] };
}

async function importConferences(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "conferences", "conferences.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "conferences", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "conferences", `Fetching conferences`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/conferences");
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "conferences", `Saved conferences`);
  return { filesWritten: 1, errors: [] };
}

async function importVenues(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "venues", "venues.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "venues", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "venues", `Fetching venues`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/venues");
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "venues", `Saved venues`);
  return { filesWritten: 1, errors: [] };
}

async function importStatsSeason(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "stats_season", "stats_season.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "stats_season", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "stats_season", `Fetching ${season} season stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/stats/season", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "stats_season", `Saved season stats`);
  return { filesWritten: 1, errors: [] };
}

async function importStatsPlayerSeason(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "stats_player_season", `Fetching ${season} player season stats (by week)`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  // Fetch stats by week to split the 30MB file into manageable chunks
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);

  for (const plan of seasonTypeWeeks) {
    for (const week of plan.weeks) {
      try {
        const stats = await cfbFetch<any[]>("/stats/player/season", {
          year: season,
          seasonType: plan.seasonType,
          week,
        });

        if (!Array.isArray(stats) || stats.length === 0) {
          continue;
        }

        const outFile = endpointPartitionPath(
          dataDir,
          sport,
          season,
          "stats_player_season",
          { seasonType: plan.seasonType, week },
          "stats.json",
        );

        if (fileExists(outFile)) continue;

        ensureDir(path.dirname(outFile));
        writeJSON(outFile, stats);
        filesWritten++;

        logger.progress(NAME, sport, "stats_player_season", 
          `Saved ${plan.seasonType} week ${week} (${stats.length} rows)`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn(`stats_player_season ${plan.seasonType} week ${week}: ${msg}`, NAME);
        errors.push(`stats_player_season/${plan.seasonType}_week_${week}: ${msg}`);
      }
    }
  }

  if (filesWritten === 0) {
    errors.push("No player stats downloaded");
  }

  logger.progress(NAME, sport, "stats_player_season", `Saved ${filesWritten} stats files`);
  return { filesWritten, errors };
}

async function importStatsCategories(ctx: EndpointContext): Promise<EndpointResult> {
  return importStaticFile(ctx, "/stats/categories", "stats_categories.json", "stats_categories");
}

async function importStatsAdvanced(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "stats_advanced", "stats_advanced.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "stats_advanced", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "stats_advanced", `Fetching ${season} advanced stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/stats/season/advanced", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "stats_advanced", `Saved advanced stats`);
  return { filesWritten: 1, errors: [] };
}

async function importStatsGameAdvanced(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "stats_game_advanced", `Fetching ${season} by week`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  let filesWritten = 0;
  const errors: string[] = [];
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);
  const gameMeta = buildGameMetaIndex(dataDir, sport, season);
  for (const plan of seasonTypeWeeks) {
    for (const week of plan.weeks) {
      try {
        const chunk = await cfbFetch<any[]>("/stats/game/advanced", { year: season, seasonType: plan.seasonType, week });
        const byGame = new Map<string, any[]>();
        for (const row of chunk ?? []) {
          const gameId = String(row?.gameId ?? "");
          if (!gameId) continue;
          if (!byGame.has(gameId)) byGame.set(gameId, []);
          byGame.get(gameId)!.push(row);
        }
        for (const [gameId, rows] of byGame) {
          const meta = gameMeta.get(gameId);
          const outFile = endpointPartitionPath(dataDir, sport, season, "stats_game_advanced", { seasonType: plan.seasonType, week, date: meta?.date }, `${gameId}.json`);
          if (fileExists(outFile)) continue;
          writeJSON(outFile, rows);
          filesWritten++;
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`stats_game_advanced/${plan.seasonType}_week_${week}: ${msg}`);
      }
    }
  }
  logger.progress(NAME, sport, "stats_game_advanced", `Saved ${filesWritten} files`);
  return { filesWritten, errors };
}

async function importStatsGameHavoc(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "stats_game_havoc", `Fetching ${season} by week`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  let filesWritten = 0;
  const errors: string[] = [];
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);
  const gameMeta = buildGameMetaIndex(dataDir, sport, season);
  for (const plan of seasonTypeWeeks) {
    for (const week of plan.weeks) {
      try {
        const chunk = await cfbFetch<any[]>("/stats/game/havoc", { year: season, seasonType: plan.seasonType, week });
        const byGame = new Map<string, any[]>();
        for (const row of chunk ?? []) {
          const gameId = String(row?.gameId ?? "");
          if (!gameId) continue;
          if (!byGame.has(gameId)) byGame.set(gameId, []);
          byGame.get(gameId)!.push(row);
        }
        for (const [gameId, rows] of byGame) {
          const meta = gameMeta.get(gameId);
          const outFile = endpointPartitionPath(dataDir, sport, season, "stats_game_havoc", { seasonType: plan.seasonType, week, date: meta?.date }, `${gameId}.json`);
          if (fileExists(outFile)) continue;
          writeJSON(outFile, rows);
          filesWritten++;
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`stats_game_havoc/${plan.seasonType}_week_${week}: ${msg}`);
      }
    }
  }
  logger.progress(NAME, sport, "stats_game_havoc", `Saved ${filesWritten} files`);
  return { filesWritten, errors };
}

async function importRankings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "rankings", `Fetching ${season} rankings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/rankings", { year: season });
  let filesWritten = 0;
  for (const row of (Array.isArray(data) ? data : [])) {
    const seasonType = seasonTypeKey((row as any)?.seasonType);
    const week = Number((row as any)?.week ?? 0);
    if (!week) continue;
    const outFile = endpointPartitionPath(dataDir, sport, season, "rankings", { seasonType, week }, "rankings.json");
    if (fileExists(outFile)) continue;
    writeJSON(outFile, row);
    filesWritten++;
  }
  logger.progress(NAME, sport, "rankings", `Saved ${filesWritten} ranking files`);
  return { filesWritten, errors: [] };
}

async function importRecruiting(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "recruiting", "recruiting.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "recruiting", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "recruiting", `Fetching ${season} recruiting`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/recruiting/players", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "recruiting", `Saved recruiting data`);
  return { filesWritten: 1, errors: [] };
}

async function importRecruitingTeams(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/recruiting/teams", "recruiting_teams.json", "recruiting_teams");
}

async function importRecruitingGroups(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/recruiting/groups", "recruiting_groups.json", "recruiting_groups");
}

async function importTalent(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "talent", "talent.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "talent", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "talent", `Fetching ${season} talent composite`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/talent", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "talent", `Saved talent composite`);
  return { filesWritten: 1, errors: [] };
}

async function importRatingsSp(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "ratings_sp", "ratings_sp.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "ratings_sp", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "ratings_sp", `Fetching ${season} SP+ ratings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/ratings/sp", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "ratings_sp", `Saved SP+ ratings`);
  return { filesWritten: 1, errors: [] };
}

async function importRatingsSpConferences(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(
    ctx,
    "/ratings/sp/conferences",
    "ratings_sp_conferences.json",
    "ratings_sp_conferences",
  );
}

async function importRatingsSrs(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/ratings/srs", "ratings_srs.json", "ratings_srs");
}

async function importRatingsElo(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "ratings_elo", "ratings_elo.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "ratings_elo", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "ratings_elo", `Fetching ${season} ELO ratings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/ratings/elo", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "ratings_elo", `Saved ELO ratings`);
  return { filesWritten: 1, errors: [] };
}

async function importRatingsFpi(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "ratings_fpi", "ratings_fpi.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "ratings_fpi", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "ratings_fpi", `Fetching ${season} FPI ratings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/ratings/fpi", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "ratings_fpi", `Saved FPI ratings`);
  return { filesWritten: 1, errors: [] };
}

async function importPpaPredicted(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "ppa_predicted", "ppa_predicted.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "ppa_predicted", "Skipping — already exists");
    return { filesWritten: 0, errors: [] };
  }

  logger.progress(NAME, sport, "ppa_predicted", `Fetching ${season} (down/distance grid)`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const downs = [1, 2, 3, 4];
  const distances = [1, 2, 3, 5, 7, 10, 15, 20];
  const rows: unknown[] = [];
  const errors: string[] = [];

  for (const down of downs) {
    for (const distance of distances) {
      try {
        const chunk = await cfbFetch<unknown[]>("/ppa/predicted", {
          year: season,
          down,
          distance,
        });
        if (Array.isArray(chunk) && chunk.length) {
          const outFile = rawPath(dataDir, NAME, sport, season, "ppa_predicted", `down_${down}`, `distance_${distance}.json`);
          if (!fileExists(outFile)) {
            writeJSON(outFile, chunk);
            rows.push({ down, distance, count: chunk.length });
          }
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`ppa_predicted/down_${down}_distance_${distance}: ${msg}`);
      }
    }
  }

  writeJSON(outFile, rows);
  logger.progress(NAME, sport, "ppa_predicted", `Saved ${rows.length} grid rows`);
  return { filesWritten: 1, errors };
}

async function importPpaTeams(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/ppa/teams", "ppa_teams.json", "ppa_teams");
}

async function importPpaGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "ppa_games", `Fetching ${season}`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  const data = await cfbFetch<any[]>("/ppa/games", { year: season });
  let filesWritten = 0;
  const byGame = new Map<string, any[]>();
  for (const row of data ?? []) {
    const gameId = String(row?.gameId ?? "");
    if (!gameId) continue;
    if (!byGame.has(gameId)) byGame.set(gameId, []);
    byGame.get(gameId)!.push(row);
  }
  for (const [gameId, rows] of byGame) {
    const meta = deriveRowPartition(rows[0]);
    if (!meta || !meta.week) continue;
    const outFile = endpointPartitionPath(dataDir, sport, season, "ppa_games", meta, `${gameId}.json`);
    if (fileExists(outFile)) continue;
    writeJSON(outFile, rows);
    filesWritten++;
  }
  logger.progress(NAME, sport, "ppa_games", `Saved ${filesWritten} files`);
  return { filesWritten, errors: [] };
}

async function importPpaPlayersGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "ppa_players_games", `Fetching ${season} by week`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  const seasonTypeWeeks = await discoverSeasonTypeWeeks(dataDir, sport, season);

  let filesWritten = 0;
  const errors: string[] = [];
  for (const plan of seasonTypeWeeks) {
    for (const week of plan.weeks) {
      try {
        const chunk = await cfbFetch<unknown[]>("/ppa/players/games", {
          year: season,
          seasonType: plan.seasonType,
          week,
        });
        if (Array.isArray(chunk) && chunk.length > 0) {
          const outFile = endpointPartitionPath(dataDir, sport, season, "ppa_players_games", { seasonType: plan.seasonType, week }, "players.json");
          if (!fileExists(outFile)) {
            writeJSON(outFile, chunk);
            filesWritten++;
          }
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`ppa_players_games/${plan.seasonType}_week_${week}: ${msg}`);
      }
    }
  }
  logger.progress(NAME, sport, "ppa_players_games", `Saved ${filesWritten} weekly files`);
  return { filesWritten, errors };
}

async function importPpaPlayersSeason(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/ppa/players/season", "ppa_players_season.json", "ppa_players_season");
}

async function importWpPregame(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  logger.progress(NAME, sport, "wp_pregame", `Fetching ${season}`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  const data = await cfbFetch<any[]>("/metrics/wp/pregame", { year: season });
  let filesWritten = 0;
  for (const row of data ?? []) {
    const meta = deriveRowPartition(row);
    if (!meta || !meta.week) continue;
    const outFile = endpointPartitionPath(dataDir, sport, season, "wp_pregame", meta, `${meta.gameId}.json`);
    if (fileExists(outFile)) continue;
    writeJSON(outFile, row);
    filesWritten++;
  }
  logger.progress(NAME, sport, "wp_pregame", `Saved ${filesWritten} files`);
  return { filesWritten, errors: [] };
}

async function importDraftTeams(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/draft/teams", "draft_teams.json", "draft_teams");
}

async function importDraftPositions(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/draft/positions", "draft_positions.json", "draft_positions");
}

async function importDraftPicks(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/draft/picks", "draft_picks.json", "draft_picks");
}

async function importWepaTeamSeason(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/wepa/team/season", "wepa_team_season.json", "wepa_team_season");
}

async function importWepaPlayersPassing(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(
    ctx,
    "/wepa/players/passing",
    "wepa_players_passing.json",
    "wepa_players_passing",
  );
}

async function importWepaPlayersRushing(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(
    ctx,
    "/wepa/players/rushing",
    "wepa_players_rushing.json",
    "wepa_players_rushing",
  );
}

async function importWepaPlayersKicking(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(
    ctx,
    "/wepa/players/kicking",
    "wepa_players_kicking.json",
    "wepa_players_kicking",
  );
}

async function importPlayerUsage(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/player/usage", "player_usage.json", "player_usage");
}

async function importPlayerReturning(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/player/returning", "player_returning.json", "player_returning");
}

async function importPlayerPortal(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/player/portal", "player_portal.json", "player_portal");
}

async function importMetricsWp(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outDir = rawPath(dataDir, NAME, sport, season, "metrics_wp");
  if (fileExists(path.join(outDir, "_COMPLETE"))) {
    logger.progress(NAME, sport, "metrics_wp", "Skipping — already exists");
    return { filesWritten: 0, errors: [] };
  }

  logger.progress(NAME, sport, "metrics_wp", `Fetching ${season} by game`);
  if (dryRun) return { filesWritten: 0, errors: [] };
  const gameMeta = buildGameMetaIndex(dataDir, sport, season);

  // Use game index so endpoint-first and legacy layouts both resolve game IDs.
  const gameIds = new Set<string>(gameMeta.keys());

  let filesWritten = 0;
  const errors: string[] = [];
  for (const gameId of gameIds) {
    const meta = gameMeta.get(gameId);
    const outFile = endpointPartitionPath(
      dataDir,
      sport,
      season,
      "metrics_wp",
      { seasonType: meta?.seasonType, week: meta?.week, date: meta?.date },
      `${gameId}.json`,
    );
    if (fileExists(outFile)) continue;
    try {
      const data = await cfbFetch("/metrics/wp", { gameId });
      writeJSON(outFile, data);
      filesWritten++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`metrics_wp/${gameId}: ${msg}`);
    }
  }

  writeJSON(path.join(outDir, "_COMPLETE"), { gameCount: gameIds.size, filesWritten, errors: errors.length });
  logger.progress(NAME, sport, "metrics_wp", `Saved ${filesWritten} game wp files`);
  return { filesWritten, errors };
}

async function importMetricsFgEp(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/metrics/fg/ep", "metrics_fg_ep.json", "metrics_fg_ep");
}

async function importInfo(ctx: EndpointContext): Promise<EndpointResult> {
  return importStaticFile(ctx, "/info", "info.json", "info");
}

// ── Endpoint dispatch map ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  games: importGames,
  games_teams: importGamesTeams,
  games_players: importGamesPlayers,
  games_media: importGamesMedia,
  games_weather: importGamesWeather,
  scoreboard: importScoreboard,
  game_box_advanced: importGameBoxAdvanced,
  records: importRecords,
  calendar: importCalendar,
  drives: importDrives,
  plays: importPlays,
  plays_types: importPlaysTypes,
  plays_stats: importPlaysStats,
  plays_stats_types: importPlaysStatsTypes,
  live_plays: importLivePlays,
  lines: importLines,
  teams: importTeams,
  teams_fbs: importTeamsFbs,
  teams_ats: importTeamsAts,
  roster: importRoster,
  coaches: importCoaches,
  conferences: importConferences,
  venues: importVenues,
  stats_season: importStatsSeason,
  stats_player_season: importStatsPlayerSeason,
  stats_categories: importStatsCategories,
  stats_advanced: importStatsAdvanced,
  stats_game_advanced: importStatsGameAdvanced,
  stats_game_havoc: importStatsGameHavoc,
  rankings: importRankings,
  recruiting: importRecruiting,
  recruiting_teams: importRecruitingTeams,
  recruiting_groups: importRecruitingGroups,
  talent: importTalent,
  ratings_sp: importRatingsSp,
  ratings_sp_conferences: importRatingsSpConferences,
  ratings_srs: importRatingsSrs,
  ratings_elo: importRatingsElo,
  ratings_fpi: importRatingsFpi,
  ppa_predicted: importPpaPredicted,
  ppa_teams: importPpaTeams,
  ppa_games: importPpaGames,
  ppa_players_games: importPpaPlayersGames,
  ppa_players_season: importPpaPlayersSeason,
  wp_pregame: importWpPregame,
  draft_teams: importDraftTeams,
  draft_positions: importDraftPositions,
  draft_picks: importDraftPicks,
  wepa_team_season: importWepaTeamSeason,
  wepa_players_passing: importWepaPlayersPassing,
  wepa_players_rushing: importWepaPlayersRushing,
  wepa_players_kicking: importWepaPlayersKicking,
  player_usage: importPlayerUsage,
  player_returning: importPlayerReturning,
  player_portal: importPlayerPortal,
  metrics_wp: importMetricsWp,
  metrics_fg_ep: importMetricsFgEp,
  info: importInfo,
};

// ── Provider implementation ─────────────────────────────────

const cfbdata: Provider = {
  name: NAME,
  label: "College Football Data",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: [...ALL_ENDPOINTS],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    if (!API_KEY && !opts.dryRun) {
      logger.error("CFB_DATA_KEY not set — skipping", NAME);
      return {
        provider: NAME,
        sport: "ncaaf",
        filesWritten: 0,
        errors: ["CFB_DATA_KEY environment variable is required"],
        durationMs: 0,
      };
    }

    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const sports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    const endpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) => ALL_ENDPOINTS.includes(e as Endpoint)) as Endpoint[])
      : [...ALL_ENDPOINTS];

    logger.info(
      `Starting import — ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      NAME,
    );

    for (const sport of sports) {
      for (const season of opts.seasons) {
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
            };
            const result = await fn(ctx);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${season}/${ep}: ${msg}`, NAME);
            allErrors.push(`${sport}/${season}/${ep}: ${msg}`);
            if (err instanceof CfbQuotaExceededError) {
              logger.warn("CFBData quota exhausted; stopping remaining endpoint imports early", NAME);
              const durationMs = Date.now() - start;
              logger.summary(NAME, totalFiles, allErrors.length, durationMs);
              return {
                provider: NAME,
                sport: sports.length === 1 ? sports[0] : "multi",
                filesWritten: totalFiles,
                errors: allErrors,
                durationMs,
              };
            }
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

export default cfbdata;
