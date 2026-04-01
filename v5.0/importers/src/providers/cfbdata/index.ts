// ──────────────────────────────────────────────────────────
// V5.0 College Football Data Provider
// ──────────────────────────────────────────────────────────
// Fetches games, stats, play-by-play, rankings, recruiting,
// and ratings from the College Football Data API.
// Requires CFB_DATA_KEY environment variable.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { writeJSON, rawPath, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── Constants ───────────────────────────────────────────────

const NAME = "cfbdata";
const BASE_URL = "https://api.collegefootballdata.com";
const API_KEY = process.env.CFB_DATA_KEY ?? "";

// ~5 req/sec
const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const SUPPORTED_SPORTS: Sport[] = ["ncaaf"];

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

  return fetchJSON<T>(url.toString(), NAME, RATE_LIMIT, {
    headers: { Authorization: `Bearer ${API_KEY}` },
  });
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
  const outFile = rawPath(dataDir, NAME, sport, season, outputFile);
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
  const outFile = rawPath(dataDir, NAME, sport, season, outputFile);
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
  const outFile = rawPath(dataDir, NAME, sport, season, "games.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games", `Fetching ${season} games`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const [regular, postseason] = await Promise.all([
    cfbFetch<unknown[]>("/games", { year: season, seasonType: "regular" }),
    cfbFetch<unknown[]>("/games", { year: season, seasonType: "postseason" }),
  ]);
  const data = [...regular, ...postseason];
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games", `Saved games`);
  return { filesWritten: 1, errors: [] };
}

async function importGamesTeams(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games_teams.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games_teams", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games_teams", `Fetching ${season} team game stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const [regular, postseason] = await Promise.all([
    cfbFetch<unknown[]>("/games/teams", { year: season, seasonType: "regular" }),
    cfbFetch<unknown[]>("/games/teams", { year: season, seasonType: "postseason" }),
  ]);
  const data = [...regular, ...postseason];
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games_teams", `Saved team game stats`);
  return { filesWritten: 1, errors: [] };
}

async function importGamesPlayers(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "games_players.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "games_players", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "games_players", `Fetching ${season} player game stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const [regular, postseason] = await Promise.all([
    cfbFetch<unknown[]>("/games/players", { year: season, seasonType: "regular" }),
    cfbFetch<unknown[]>("/games/players", { year: season, seasonType: "postseason" }),
  ]);
  const data = [...regular, ...postseason];
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "games_players", `Saved player game stats`);
  return { filesWritten: 1, errors: [] };
}

async function importGamesMedia(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/games/media", "games_media.json", "games_media");
}

async function importGamesWeather(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/games/weather", "games_weather.json", "games_weather");
}

async function importScoreboard(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "scoreboard.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "scoreboard", "Skipping — already exists");
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "scoreboard", `Fetching ${season} scoreboard`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const [regular, postseason] = await Promise.all([
    cfbFetch<unknown[]>("/scoreboard", { year: season, seasonType: "regular" }),
    cfbFetch<unknown[]>("/scoreboard", { year: season, seasonType: "postseason" }),
  ]);
  writeJSON(outFile, [...regular, ...postseason]);
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
  const gamesFile = rawPath(dataDir, NAME, sport, season, "games.json");
  if (fileExists(gamesFile)) {
    const existing = await import("node:fs/promises").then(async (fs) => {
      const txt = await fs.readFile(gamesFile, "utf-8");
      return JSON.parse(txt) as Array<{ id?: number | string }>;
    });
    if (Array.isArray(existing)) games = existing;
  }
  if (!games.length) {
    const [regular, postseason] = await Promise.all([
      cfbFetch<Array<{ id?: number | string }>>("/games", { year: season, seasonType: "regular" }),
      cfbFetch<Array<{ id?: number | string }>>("/games", { year: season, seasonType: "postseason" }),
    ]);
    games = [...regular, ...postseason];
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
  const outFile = rawPath(dataDir, NAME, sport, season, "drives.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "drives", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "drives", `Fetching ${season} drives`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const [regular, postseason] = await Promise.all([
    cfbFetch<unknown[]>("/drives", { year: season, seasonType: "regular" }),
    cfbFetch<unknown[]>("/drives", { year: season, seasonType: "postseason" }),
  ]);
  const data = [...regular, ...postseason];
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "drives", `Saved drives`);
  return { filesWritten: 1, errors: [] };
}

async function importPlays(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  logger.progress(NAME, sport, "plays", `Fetching ${season} play-by-play (regular + postseason)`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const seasonTypeWindows: Array<{ seasonType: "regular" | "postseason"; maxWeek: number }> = [
    { seasonType: "regular", maxWeek: 16 },
    { seasonType: "postseason", maxWeek: 5 },
  ];

  for (const window of seasonTypeWindows) {
    for (let week = 1; week <= window.maxWeek; week++) {
      const filePrefix = window.seasonType === "regular" ? "week" : "postseason_week";
      const outFile = rawPath(dataDir, NAME, sport, season, "plays", `${filePrefix}_${week}.json`);
      if (fileExists(outFile)) {
        logger.progress(NAME, sport, "plays", `Skipping ${window.seasonType} week ${week} — already exists`);
        continue;
      }

      try {
        const data = await cfbFetch("/plays", {
          year: season,
          week,
          seasonType: window.seasonType,
        });
        writeJSON(outFile, data);
        filesWritten++;
        logger.progress(NAME, sport, "plays", `Saved ${window.seasonType} week ${week}`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn(`plays ${window.seasonType} week ${week}: ${msg}`, NAME);
        errors.push(`plays/${window.seasonType}_week_${week}: ${msg}`);
      }
    }
  }

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
  const outFile = rawPath(dataDir, NAME, sport, season, "lines.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "lines", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "lines", `Fetching ${season} betting lines`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const [regular, postseason] = await Promise.all([
    cfbFetch<unknown[]>("/lines", { year: season, seasonType: "regular" }),
    cfbFetch<unknown[]>("/lines", { year: season, seasonType: "postseason" }),
  ]);
  const data = [...regular, ...postseason];
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "lines", `Saved betting lines`);
  return { filesWritten: 1, errors: [] };
}

async function importTeams(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "teams.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "roster.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "coaches.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "conferences.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "venues.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "stats_season.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "stats_player_season.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "stats_player_season", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "stats_player_season", `Fetching ${season} player season stats`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/stats/player/season", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "stats_player_season", `Saved player season stats`);
  return { filesWritten: 1, errors: [] };
}

async function importStatsCategories(ctx: EndpointContext): Promise<EndpointResult> {
  return importStaticFile(ctx, "/stats/categories", "stats_categories.json", "stats_categories");
}

async function importStatsAdvanced(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "stats_advanced.json");
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
  return importSeasonFile(ctx, "/stats/game/advanced", "stats_game_advanced.json", "stats_game_advanced");
}

async function importStatsGameHavoc(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/stats/game/havoc", "stats_game_havoc.json", "stats_game_havoc");
}

async function importRankings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "rankings.json");
  if (fileExists(outFile)) {
    logger.progress(NAME, sport, "rankings", `Skipping — already exists`);
    return { filesWritten: 0, errors: [] };
  }
  logger.progress(NAME, sport, "rankings", `Fetching ${season} rankings`);
  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await cfbFetch("/rankings", { year: season });
  writeJSON(outFile, data);
  logger.progress(NAME, sport, "rankings", `Saved rankings`);
  return { filesWritten: 1, errors: [] };
}

async function importRecruiting(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const outFile = rawPath(dataDir, NAME, sport, season, "recruiting.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "talent.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "ratings_sp.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "ratings_elo.json");
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
  const outFile = rawPath(dataDir, NAME, sport, season, "ratings_fpi.json");
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
  return importSeasonFile(ctx, "/ppa/predicted", "ppa_predicted.json", "ppa_predicted");
}

async function importPpaTeams(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/ppa/teams", "ppa_teams.json", "ppa_teams");
}

async function importPpaGames(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/ppa/games", "ppa_games.json", "ppa_games");
}

async function importPpaPlayersGames(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/ppa/players/games", "ppa_players_games.json", "ppa_players_games");
}

async function importPpaPlayersSeason(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/ppa/players/season", "ppa_players_season.json", "ppa_players_season");
}

async function importWpPregame(ctx: EndpointContext): Promise<EndpointResult> {
  return importSeasonFile(ctx, "/metrics/wp/pregame", "wp_pregame.json", "wp_pregame");
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
  return importSeasonFile(ctx, "/metrics/wp", "metrics_wp.json", "metrics_wp");
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
