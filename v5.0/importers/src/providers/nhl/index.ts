// ──────────────────────────────────────────────────────────
// NHL Official API Provider — v2.0
// ──────────────────────────────────────────────────────────
// Two data sources:
//   api-web.nhle.com/v1  — schedule, standings, rosters, game center,
//                          leaders, playoffs, club stats, player game logs
//   api.nhle.com/stats/rest/en — aggregate player/team stats, shift charts
//
// Data layout under raw/nhl/nhl/:
//   reference/                    seasons.json, teams.json, franchises.json
//   {season}/
//     schedule/                   regular.json, playoffs.json
//     standings/                  {date}.json  (monthly snapshots)
//     rosters/                    {TEAM}.json
//     club_stats/regular|playoffs {TEAM}.json
//     games/regular|playoffs/     {game_id}/boxscore|landing|play_by_play|right_rail.json
//     shift_charts/               {game_id}.json
//     players/{player_id}/        landing.json, game_log_regular.json, game_log_playoffs.json
//     stats/skaters/              {regular|playoffs}_{report}.json
//     stats/goalies/              {regular|playoffs}_{report}.json
//     stats/teams/                {regular|playoffs}_{report}.json
//     leaders/                    skater_leaders_{regular|playoffs}.json
//                                 goalie_leaders_{regular|playoffs}.json
//     playoffs/                   bracket.json, carousel.json
//     playoffs/series/{letter}/   schedule.json

import path from "node:path";
import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, readJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "nhl";
const BASE_WEB   = "https://api-web.nhle.com/v1";
const BASE_STATS = "https://api.nhle.com/stats/rest/en";
const SPORTS: readonly Sport[] = ["nhl"] as const;

// Conservative rate limits — both APIs are public but should not be hammered
const RL_WEB:   RateLimitConfig = { requests: 3, perMs: 1_000 };
const RL_STATS: RateLimitConfig = { requests: 2, perMs: 1_000 };

const GAME_TYPE_REGULAR  = 2;
const GAME_TYPE_PLAYOFFS = 3;

// Current 32-team roster (Utah replaced Arizona starting 2024-25)
const NHL_TEAMS_CURRENT = [
  "ANA","BOS","BUF","CGY","CAR","CHI","COL","CBJ",
  "DAL","DET","EDM","FLA","LAK","MIN","MTL","NSH",
  "NJD","NYI","NYR","OTT","PHI","PIT","SEA","SJS",
  "STL","TBL","TOR","UTA","VAN","VGK","WPG","WSH",
] as const;

// Aggregate skater stat reports from stats REST API
const SKATER_REPORTS = [
  "summary",              // G, A, Pts, +/-, PIM, PPG, SHG, GWG, shots
  "bios",                 // age, position, nationality, draft info
  "realtime",             // hits, blocked shots, takeaways, giveaways, missed shots
  "powerplay",            // PP goals, assists, TOI
  "faceoffpercentages",   // faceoff wins/losses by zone
  "penalties",            // minor, major, misconduct penalties
] as const;

// Aggregate goalie stat reports from stats REST API
const GOALIE_REPORTS = [
  "summary",              // W/L/OTL, GAA, SV%, SO, GP
  "advanced",             // GSAA, high-danger SV%, medium-danger SV%
  "bios",                 // biographical info
  "startedVsRelieved",    // starter vs reliever split
] as const;

// Aggregate team stat reports from stats REST API
const TEAM_REPORTS = [
  "summary",              // GF, GA, PIM, PP%, PK%, shots/game, corsi
  "powerplay",            // PP opportunities, goals, conversion
  "penaltykill",          // PK attempts, GA, kill %
  "penalties",            // PIM, power play minutes against
] as const;

const ENDPOINTS = [
  "reference",     // teams.json, seasons.json, franchises.json  (once ever)
  "schedule",      // full season schedule split by game type
  "standings",     // monthly standing snapshots
  "rosters",       // team roster by season
  "club_stats",    // per-team stats regular + playoffs
  "team_extras",   // roster-current/prospects/club-stats-now/scoreboard-now
  "league_feeds",  // score/schedule/calendar/network snapshots
  "games",         // boxscore + landing + play-by-play + right-rail per game
  "shift_charts",  // shift chart data from stats REST API
  "players",       // player profile + game logs per season
  "skater_stats",  // aggregate skater stats (all reports, both game types)
  "goalie_stats",  // aggregate goalie stats
  "team_stats",    // aggregate team stats
  "leaders",       // top-N skater / goalie stat leaders
  "current_leaders", // current skater/goalie leaders snapshots
  "playoffs",      // bracket, series carousel, per-series schedules
  "draft",         // draft rankings, tracker, picks by season
  "meta",          // meta/location/standings-season/where-to-watch
  "edge",          // edge landings + by-the-numbers
  "edge_deep",     // edge details/comparisons/top10 families
  "replays",       // ppt replay endpoints per game event
  "stats_misc",    // stats/rest misc + reference tables
] as const;

type Endpoint = (typeof ENDPOINTS)[number];

// ── Path / season helpers ──────────────────────────────────

/** Convert start year (2023) to NHL season ID format (20232024). */
function toSeasonId(season: number): number {
  return season * 10_000 + (season + 1);
}

/** Season path helper — builds raw/nhl/nhl/{season}/...  */
function sPath(dataDir: string, season: number, ...segs: string[]): string {
  return rawPath(dataDir, NAME, "nhl", season, ...segs);
}

/** Reference path — raw/nhl/nhl/reference/... (no season component). */
function refPath(dataDir: string, ...segs: string[]): string {
  return path.join(dataDir, "raw", NAME, "nhl", "reference", ...segs);
}

/** Returns the correct team list for a given season (ARI before 2024, UTA from 2024). */
function teamsForSeason(season: number): string[] {
  const teams = [...NHL_TEAMS_CURRENT] as string[];
  if (season < 2024) {
    // Arizona Coyotes relocated; replace UTA with ARI for all pre-2024 seasons
    const i = teams.indexOf("UTA");
    if (i !== -1) teams.splice(i, 1, "ARI");
  }
  if (season < 2021) {
    // Seattle Kraken joined in 2021-22; remove SEA for earlier seasons
    const i = teams.indexOf("SEA");
    if (i !== -1) teams.splice(i, 1);
  }
  return teams;
}

/** Season window: Oct 1 → Jun 30 next year. Handles COVID-shortened seasons. */
function seasonWindow(season: number): { start: string; end: string } {
  if (season === 2019) return { start: "2019-10-01", end: "2020-10-25" }; // bubble playoffs
  if (season === 2020) return { start: "2021-01-01", end: "2021-08-15" }; // 56-game season
  return { start: `${season}-10-01`, end: `${season + 1}-06-30` };
}

/** Monthly snapshot dates for standings — 1st of each month in the season window. */
function standingsSampleDates(season: number): string[] {
  if (season === 2020) {
    return ["2021-02-01","2021-03-01","2021-04-01","2021-05-01","2021-06-01","2021-07-01"];
  }
  const dates: string[] = [];
  for (let m = 10; m <= 12; m++) dates.push(`${season}-${String(m).padStart(2,"0")}-01`);
  for (let m = 1; m <= 7; m++)  dates.push(`${season+1}-${String(m).padStart(2,"0")}-01`);
  return dates;
}

function seasonMonths(season: number): string[] {
  const { start, end } = seasonWindow(season);
  const result: string[] = [];
  const cur = new Date(`${start}T00:00:00Z`);
  cur.setUTCDate(1);
  const last = new Date(`${end}T00:00:00Z`);
  last.setUTCDate(1);
  while (cur <= last) {
    result.push(cur.toISOString().slice(0, 7));
    cur.setUTCMonth(cur.getUTCMonth() + 1);
  }
  return result;
}

function seasonWeekDates(season: number): string[] {
  const { start, end } = seasonWindow(season);
  const result: string[] = [];
  const cur = new Date(`${start}T00:00:00Z`);
  const last = new Date(`${end}T00:00:00Z`);
  while (cur.getUTCDay() !== 1) cur.setUTCDate(cur.getUTCDate() + 1);
  while (cur <= last) {
    result.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 7);
  }
  return result;
}

// ── Shared types ──────────────────────────────────────────

interface NHLGame {
  id: number;
  gameType: number;      // 1=pre, 2=regular, 3=playoffs
  startTimeUTC?: string;
  gameDate?: string;
  awayTeam: { abbrev: string; id?: number };
  homeTeam: { abbrev: string; id?: number };
  gameState?: string;
}

interface EndpointCtx {
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

// ── Endpoint: reference ───────────────────────────────────

async function importReference(ctx: EndpointCtx): Promise<EndpointResult> {
  if (ctx.dryRun) {
    logger.info("[dry-run] Would fetch reference data (teams, seasons, franchises)", NAME);
    return { filesWritten: 0, errors: [] };
  }

  let filesWritten = 0;
  const errors: string[] = [];

  const refs: Array<[string, string]> = [
    [refPath(ctx.dataDir, "teams.json"),      `${BASE_STATS}/team`],
    [refPath(ctx.dataDir, "franchises.json"), `${BASE_STATS}/franchise`],
    [refPath(ctx.dataDir, "seasons.json"),    `${BASE_WEB}/season`],
    [refPath(ctx.dataDir, "config.json"),     `${BASE_STATS}/config`],
  ];

  for (const [filePath, url] of refs) {
    if (fileExists(filePath)) continue;
    try {
      const data = await fetchJSON(url, NAME, RL_STATS);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      errors.push(`reference ${path.basename(filePath)}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  logger.progress(NAME, "nhl", "reference", `${filesWritten} files`);
  return { filesWritten, errors };
}

// ── Endpoint: schedule ────────────────────────────────────

async function importSchedule(ctx: EndpointCtx): Promise<EndpointResult> {
  const regularPath  = sPath(ctx.dataDir, ctx.season, "schedule", "regular.json");
  const playoffsPath = sPath(ctx.dataDir, ctx.season, "schedule", "playoffs.json");

  if (fileExists(regularPath) && fileExists(playoffsPath)) {
    logger.info(`Skipping schedule ${ctx.season} — both files exist`, NAME);
    return { filesWritten: 0, errors: [] };
  }
  if (ctx.dryRun) {
    logger.info(`[dry-run] Would fetch full schedule for ${ctx.season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const sid = toSeasonId(ctx.season);
  const teams = teamsForSeason(ctx.season);
  const regularGames  = new Map<number, NHLGame>();
  const playoffGames  = new Map<number, NHLGame>();

  // Fetch every team's full season schedule and deduplicate by game ID.
  // One team covers ~82 games (home+away); all 31 teams ensures 100% coverage.
  for (const team of teams) {
    try {
      const data = await fetchJSON<{ games?: NHLGame[] }>(
        `${BASE_WEB}/club-schedule-season/${team}/${sid}`,
        NAME, RL_WEB,
      );
      for (const g of data.games ?? []) {
        if (g.gameType === GAME_TYPE_REGULAR)  regularGames.set(g.id, g);
        if (g.gameType === GAME_TYPE_PLAYOFFS) playoffGames.set(g.id, g);
      }
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404")) {
        logger.warn(`schedule team ${team} ${ctx.season}: ${msg}`, NAME);
      }
    }
  }

  let filesWritten = 0;
  if (!fileExists(regularPath) && regularGames.size > 0) {
    writeJSON(regularPath, [...regularGames.values()]);
    filesWritten++;
    logger.progress(NAME, "nhl", "schedule", `${regularGames.size} regular games for ${ctx.season}`);
  }
  if (!fileExists(playoffsPath) && playoffGames.size > 0) {
    writeJSON(playoffsPath, [...playoffGames.values()]);
    filesWritten++;
    logger.progress(NAME, "nhl", "schedule", `${playoffGames.size} playoff games for ${ctx.season}`);
  }

  return { filesWritten, errors: [] };
}

// ── Endpoint: standings ───────────────────────────────────

async function importStandings(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const today = new Date().toISOString().slice(0, 10);

  for (const date of standingsSampleDates(ctx.season)) {
    if (date > today) continue; // skip future dates
    const outPath = sPath(ctx.dataDir, ctx.season, "standings", `${date}.json`);
    if (fileExists(outPath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_WEB}/standings/${date}`, NAME, RL_WEB);
      writeJSON(outPath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404")) errors.push(`standings ${date}: ${msg}`);
    }
  }

  logger.progress(NAME, "nhl", "standings", `${filesWritten} snapshots for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: rosters ────────────────────────────────────

async function importRosters(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);

  for (const team of teamsForSeason(ctx.season)) {
    const outPath = sPath(ctx.dataDir, ctx.season, "rosters", `${team}.json`);
    if (fileExists(outPath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_WEB}/roster/${team}/${sid}`, NAME, RL_WEB);
      writeJSON(outPath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404")) errors.push(`roster ${team}: ${msg}`);
    }
  }

  logger.progress(NAME, "nhl", "rosters", `${filesWritten} teams for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: club_stats ──────────────────────────────────

async function importClubStats(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);

  for (const [gameType, label] of [[GAME_TYPE_REGULAR,"regular"],[GAME_TYPE_PLAYOFFS,"playoffs"]] as const) {
    for (const team of teamsForSeason(ctx.season)) {
      const outPath = sPath(ctx.dataDir, ctx.season, "club_stats", label, `${team}.json`);
      if (fileExists(outPath) || ctx.dryRun) continue;
      try {
        const data = await fetchJSON(
          `${BASE_WEB}/club-stats/${team}/${sid}/${gameType}`,
          NAME, RL_WEB,
        );
        writeJSON(outPath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
        if (!msg.includes("404")) errors.push(`club_stats ${label} ${team}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "club_stats", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: team_extras ────────────────────────────────

async function importTeamExtras(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  for (const team of teamsForSeason(ctx.season)) {
    const out: Array<[string, string]> = [
      [sPath(ctx.dataDir, ctx.season, "team_extras", "roster_current", `${team}.json`), `${BASE_WEB}/roster/${team}/current`],
      [sPath(ctx.dataDir, ctx.season, "team_extras", "roster_seasons", `${team}.json`), `${BASE_WEB}/roster-season/${team}`],
      [sPath(ctx.dataDir, ctx.season, "team_extras", "prospects", `${team}.json`), `${BASE_WEB}/prospects/${team}`],
      [sPath(ctx.dataDir, ctx.season, "team_extras", "schedule_season_now", `${team}.json`), `${BASE_WEB}/club-schedule-season/${team}/now`],
      [sPath(ctx.dataDir, ctx.season, "team_extras", "schedule_month_now", `${team}.json`), `${BASE_WEB}/club-schedule/${team}/month/now`],
      [sPath(ctx.dataDir, ctx.season, "team_extras", "schedule_week_now", `${team}.json`), `${BASE_WEB}/club-schedule/${team}/week/now`],
      [sPath(ctx.dataDir, ctx.season, "team_extras", "club_stats_now", `${team}.json`), `${BASE_WEB}/club-stats/${team}/now`],
      [sPath(ctx.dataDir, ctx.season, "team_extras", "club_stats_seasons", `${team}.json`), `${BASE_WEB}/club-stats-season/${team}`],
      [sPath(ctx.dataDir, ctx.season, "team_extras", "scoreboard_now", `${team}.json`), `${BASE_WEB}/scoreboard/${team}/now`],
    ];

    for (const [filePath, url] of out) {
      if (fileExists(filePath) || ctx.dryRun) continue;
      try {
        const data = await fetchJSON(url, NAME, RL_WEB);
        writeJSON(filePath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
        if (!msg.includes("404") && !msg.includes("400")) {
          errors.push(`team_extras ${team}: ${msg}`);
        }
      }
    }

    for (const month of seasonMonths(ctx.season)) {
      const filePath = sPath(ctx.dataDir, ctx.season, "team_extras", "schedule_month", team, `${month}.json`);
      if (fileExists(filePath) || ctx.dryRun) continue;
      try {
        const data = await fetchJSON(`${BASE_WEB}/club-schedule/${team}/month/${month}`, NAME, RL_WEB);
        writeJSON(filePath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
        if (!msg.includes("404") && !msg.includes("400")) errors.push(`team_extras ${team} month ${month}: ${msg}`);
      }
    }

    for (const weekDate of seasonWeekDates(ctx.season)) {
      const filePath = sPath(ctx.dataDir, ctx.season, "team_extras", "schedule_week", team, `${weekDate}.json`);
      if (fileExists(filePath) || ctx.dryRun) continue;
      try {
        const data = await fetchJSON(`${BASE_WEB}/club-schedule/${team}/week/${weekDate}`, NAME, RL_WEB);
        writeJSON(filePath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
        if (!msg.includes("404") && !msg.includes("400")) errors.push(`team_extras ${team} week ${weekDate}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "team_extras", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: league_feeds ───────────────────────────────

async function importLeagueFeeds(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const today = new Date().toISOString().slice(0, 10);

  const nowEndpoints: Array<[string, string]> = [
    [sPath(ctx.dataDir, ctx.season, "league_feeds", "now", "schedule_now.json"), `${BASE_WEB}/schedule/now`],
    [sPath(ctx.dataDir, ctx.season, "league_feeds", "now", "score_now.json"), `${BASE_WEB}/score/now`],
    [sPath(ctx.dataDir, ctx.season, "league_feeds", "now", "scoreboard_now.json"), `${BASE_WEB}/scoreboard/now`],
    [sPath(ctx.dataDir, ctx.season, "league_feeds", "now", "standings_now.json"), `${BASE_WEB}/standings/now`],
    [sPath(ctx.dataDir, ctx.season, "league_feeds", "now", "schedule_calendar_now.json"), `${BASE_WEB}/schedule-calendar/now`],
    [sPath(ctx.dataDir, ctx.season, "league_feeds", "now", "network_tv_schedule_now.json"), `${BASE_WEB}/network/tv-schedule/now`],
  ];

  for (const [filePath, url] of nowEndpoints) {
    if (fileExists(filePath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(url, NAME, RL_WEB);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404") && !msg.includes("400")) {
        errors.push(`league_feeds now: ${msg}`);
      }
    }
  }

  // Capture one monthly snapshot through each season to avoid huge duplication.
  for (const date of standingsSampleDates(ctx.season)) {
    if (date > today) continue;
    const dated: Array<[string, string]> = [
      [sPath(ctx.dataDir, ctx.season, "league_feeds", "schedule", `${date}.json`), `${BASE_WEB}/schedule/${date}`],
      [sPath(ctx.dataDir, ctx.season, "league_feeds", "score", `${date}.json`), `${BASE_WEB}/score/${date}`],
      [sPath(ctx.dataDir, ctx.season, "league_feeds", "schedule_calendar", `${date}.json`), `${BASE_WEB}/schedule-calendar/${date}`],
      [sPath(ctx.dataDir, ctx.season, "league_feeds", "network_tv_schedule", `${date}.json`), `${BASE_WEB}/network/tv-schedule/${date}`],
    ];
    for (const [filePath, url] of dated) {
      if (fileExists(filePath) || ctx.dryRun) continue;
      try {
        const data = await fetchJSON(url, NAME, RL_WEB);
        writeJSON(filePath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
        if (!msg.includes("404") && !msg.includes("400")) {
          errors.push(`league_feeds ${date}: ${msg}`);
        }
      }
    }
  }

  logger.progress(NAME, "nhl", "league_feeds", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: games ───────────────────────────────────────

async function importGames(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  // Collect game IDs from both schedule files
  const allGames: NHLGame[] = [];
  for (const label of ["regular", "playoffs"] as const) {
    const p = sPath(ctx.dataDir, ctx.season, "schedule", `${label}.json`);
    allGames.push(...(readJSON<NHLGame[]>(p) ?? []));
  }

  if (allGames.length === 0) {
    logger.info("No schedule found — run schedule endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  logger.info(`Fetching game center data for ${allGames.length} games (${ctx.season})`, NAME);

  for (const game of allGames) {
    const label  = game.gameType === GAME_TYPE_PLAYOFFS ? "playoffs" : "regular";
    const gameDir = sPath(ctx.dataDir, ctx.season, "games", label, String(game.id));

    const parts: Array<[string, string]> = [
      ["boxscore",      `${BASE_WEB}/gamecenter/${game.id}/boxscore`],
      ["landing",       `${BASE_WEB}/gamecenter/${game.id}/landing`],
      ["play_by_play",  `${BASE_WEB}/gamecenter/${game.id}/play-by-play`],
      ["right_rail",    `${BASE_WEB}/gamecenter/${game.id}/right-rail`],
        ["game_story",    `${BASE_WEB}/wsc/game-story/${game.id}`],
        ["wsc_play_by_play", `${BASE_WEB}/wsc/play-by-play/${game.id}`],
        ["meta_game",     `${BASE_WEB}/meta/game/${game.id}`],
    ];

      for (const [fname, url] of parts) {
      const filePath = path.join(gameDir, `${fname}.json`);
      if (fileExists(filePath) || ctx.dryRun) continue;
        const isOptionalFeed = fname === "right_rail" || fname === "game_story" || fname === "wsc_play_by_play";
      try {
          const data = await fetchJSON(
            url,
            NAME,
            RL_WEB,
            isOptionalFeed ? { retries: 1, timeoutMs: 8_000 } : undefined,
          );
        writeJSON(filePath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
          if (isOptionalFeed && (msg.includes("404") || msg.includes("500"))) continue;
          if (!msg.includes("404")) errors.push(`game ${game.id} ${fname}: ${msg}`);
      }
    }

    if (filesWritten > 0 && filesWritten % 200 === 0) {
      logger.progress(NAME, "nhl", "games", `${filesWritten} files written`);
    }
  }

  logger.progress(NAME, "nhl", "games", `${filesWritten} game files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: shift_charts ────────────────────────────────

async function importShiftCharts(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  const games = readJSON<NHLGame[]>(sPath(ctx.dataDir, ctx.season, "schedule", "regular.json")) ?? [];
  if (games.length === 0) {
    logger.info("No regular-season schedule — run schedule first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  for (const game of games) {
    const outPath = sPath(ctx.dataDir, ctx.season, "shift_charts", `${game.id}.json`);
    if (fileExists(outPath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(
        `${BASE_STATS}/shiftcharts?cayenneExp=${encodeURIComponent(`gameId=${game.id}`)}`,
        NAME, RL_STATS,
      );
      writeJSON(outPath, data);
      filesWritten++;
      if (filesWritten % 100 === 0) {
        logger.progress(NAME, "nhl", "shift_charts", `${filesWritten}/${games.length}`);
      }
    } catch (err) {
      errors.push(`shift_charts ${game.id}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  logger.progress(NAME, "nhl", "shift_charts", `${filesWritten} games for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: players ─────────────────────────────────────

async function importPlayers(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);

  // Collect unique player IDs from all season rosters
  const playerIds = new Set<number>();
  for (const team of teamsForSeason(ctx.season)) {
    const rosterPath = sPath(ctx.dataDir, ctx.season, "rosters", `${team}.json`);
    const roster = readJSON<Record<string, Array<{ id?: number; playerId?: number }>>>(rosterPath);
    if (!roster) continue;
    for (const group of Object.values(roster)) {
      if (!Array.isArray(group)) continue;
      for (const p of group) {
        const id = p.id ?? p.playerId;
        if (typeof id === "number") playerIds.add(id);
      }
    }
  }

  if (playerIds.size === 0) {
    logger.info("No player IDs found — run rosters endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  logger.info(`Fetching data for ${playerIds.size} players in ${ctx.season}`, NAME);

  for (const pid of playerIds) {
    const playerDir = sPath(ctx.dataDir, ctx.season, "players", String(pid));

    // Player profile (landing)
    const landingPath = path.join(playerDir, "landing.json");
    if (!fileExists(landingPath) && !ctx.dryRun) {
      try {
        const data = await fetchJSON(`${BASE_WEB}/player/${pid}/landing`, NAME, RL_WEB);
        writeJSON(landingPath, data);
        filesWritten++;
      } catch (err) {
        errors.push(`player ${pid} landing: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    // Game log — regular season
    const glRegPath = path.join(playerDir, "game_log_regular.json");
    if (!fileExists(glRegPath) && !ctx.dryRun) {
      try {
        const data = await fetchJSON(
          `${BASE_WEB}/player/${pid}/game-log/${sid}/${GAME_TYPE_REGULAR}`,
          NAME, RL_WEB,
        );
        writeJSON(glRegPath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
        if (!msg.includes("404")) errors.push(`player ${pid} game_log_regular: ${msg}`);
      }
    }

    // Game log — playoffs
    const glPlayPath = path.join(playerDir, "game_log_playoffs.json");
    if (!fileExists(glPlayPath) && !ctx.dryRun) {
      try {
        const data = await fetchJSON(
          `${BASE_WEB}/player/${pid}/game-log/${sid}/${GAME_TYPE_PLAYOFFS}`,
          NAME, RL_WEB,
        );
        writeJSON(glPlayPath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
        if (!msg.includes("404")) errors.push(`player ${pid} game_log_playoffs: ${msg}`);
      }
    }

      // Game log — now
      const glNowPath = path.join(playerDir, "game_log_now.json");
      if (!fileExists(glNowPath) && !ctx.dryRun) {
        try {
          const data = await fetchJSON(`${BASE_WEB}/player/${pid}/game-log/now`, NAME, RL_WEB);
          writeJSON(glNowPath, data);
          filesWritten++;
        } catch (err) {
          const msg = String(err instanceof Error ? err.message : err);
          if (!msg.includes("404")) errors.push(`player ${pid} game_log_now: ${msg}`);
        }
      }

    if (filesWritten % 150 === 0 && filesWritten > 0) {
      logger.progress(NAME, "nhl", "players", `${filesWritten} files`);
    }
  }

  logger.progress(NAME, "nhl", "players", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: skater_stats ────────────────────────────────

async function importSkaterStats(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);

  for (const [gameType, label] of [[GAME_TYPE_REGULAR,"regular"],[GAME_TYPE_PLAYOFFS,"playoffs"]] as const) {
    for (const report of SKATER_REPORTS) {
      const outPath = sPath(ctx.dataDir, ctx.season, "stats", "skaters", `${label}_${report}.json`);
      if (fileExists(outPath) || ctx.dryRun) continue;
      const cayenne = encodeURIComponent(`seasonId=${sid} and gameTypeId=${gameType}`);
      const url = `${BASE_STATS}/skater/${report}?isAggregate=false&isGame=false`
                 + `&sort=lastName&dir=asc&start=0&limit=-1&cayenneExp=${cayenne}`;
      try {
        const data = await fetchJSON(url, NAME, RL_STATS);
        writeJSON(outPath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
          if (!msg.includes("404") && !msg.includes("400")) errors.push(`skater_stats ${label} ${report}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "skater_stats", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: goalie_stats ────────────────────────────────

async function importGoalieStats(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);

  for (const [gameType, label] of [[GAME_TYPE_REGULAR,"regular"],[GAME_TYPE_PLAYOFFS,"playoffs"]] as const) {
    for (const report of GOALIE_REPORTS) {
      const outPath = sPath(ctx.dataDir, ctx.season, "stats", "goalies", `${label}_${report}.json`);
      if (fileExists(outPath) || ctx.dryRun) continue;
      const cayenne = encodeURIComponent(`seasonId=${sid} and gameTypeId=${gameType}`);
      const url = `${BASE_STATS}/goalie/${report}?isAggregate=false&isGame=false`
                 + `&sort=lastName&dir=asc&start=0&limit=-1&cayenneExp=${cayenne}`;
      try {
        const data = await fetchJSON(url, NAME, RL_STATS);
        writeJSON(outPath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
          if (!msg.includes("404") && !msg.includes("400")) errors.push(`goalie_stats ${label} ${report}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "goalie_stats", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: team_stats ──────────────────────────────────

async function importTeamStats(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);

  for (const [gameType, label] of [[GAME_TYPE_REGULAR,"regular"],[GAME_TYPE_PLAYOFFS,"playoffs"]] as const) {
    for (const report of TEAM_REPORTS) {
      const outPath = sPath(ctx.dataDir, ctx.season, "stats", "teams", `${label}_${report}.json`);
      if (fileExists(outPath) || ctx.dryRun) continue;
      const cayenne = encodeURIComponent(`seasonId=${sid} and gameTypeId=${gameType}`);
      const url = `${BASE_STATS}/team/${report}?isAggregate=false&isGame=false`
                 + `&sort=teamId&dir=asc&start=0&limit=-1&cayenneExp=${cayenne}`;
      try {
        const data = await fetchJSON(url, NAME, RL_STATS);
        writeJSON(outPath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
          if (!msg.includes("404") && !msg.includes("400")) errors.push(`team_stats ${label} ${report}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "team_stats", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: leaders ────────────────────────────────────

async function importLeaders(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);

  for (const [gameType, label] of [[GAME_TYPE_REGULAR,"regular"],[GAME_TYPE_PLAYOFFS,"playoffs"]] as const) {
    // Skater leaders
    const skaterPath = sPath(ctx.dataDir, ctx.season, "leaders", `skater_leaders_${label}.json`);
    if (!fileExists(skaterPath) && !ctx.dryRun) {
      try {
        const data = await fetchJSON(
            `${BASE_WEB}/skater-stats-leaders/${sid}/${gameType}?limit=-1`,
          NAME, RL_WEB,
        );
        writeJSON(skaterPath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
          if (!msg.includes("404") && !msg.includes("400")) errors.push(`skater_leaders ${label}: ${msg}`);
      }
    }

    // Goalie leaders
    const goaliePath = sPath(ctx.dataDir, ctx.season, "leaders", `goalie_leaders_${label}.json`);
    if (!fileExists(goaliePath) && !ctx.dryRun) {
      try {
        const data = await fetchJSON(
            `${BASE_WEB}/goalie-stats-leaders/${sid}/${gameType}?limit=-1`,
          NAME, RL_WEB,
        );
        writeJSON(goaliePath, data);
        filesWritten++;
      } catch (err) {
        const msg = String(err instanceof Error ? err.message : err);
          if (!msg.includes("404") && !msg.includes("400")) errors.push(`goalie_leaders ${label}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "leaders", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: current_leaders ────────────────────────────

async function importCurrentLeaders(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  const out: Array<[string, string]> = [
    [sPath(ctx.dataDir, ctx.season, "leaders", "skater_leaders_current.json"), `${BASE_WEB}/skater-stats-leaders/current?limit=-1`],
    [sPath(ctx.dataDir, ctx.season, "leaders", "goalie_leaders_current.json"), `${BASE_WEB}/goalie-stats-leaders/current?limit=-1`],
  ];

  for (const [filePath, url] of out) {
    if (fileExists(filePath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(url, NAME, RL_WEB);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404") && !msg.includes("400")) {
        errors.push(`current_leaders ${ctx.season}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "current_leaders", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: playoffs ────────────────────────────────────

async function importPlayoffs(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);
  const playoffYear = ctx.season + 1; // e.g., 2024 for the 2023-24 playoffs

  // Playoff bracket (by end year)
  const bracketPath = sPath(ctx.dataDir, ctx.season, "playoffs", "bracket.json");
  if (!fileExists(bracketPath) && !ctx.dryRun) {
    try {
      const data = await fetchJSON(`${BASE_WEB}/playoff-bracket/${playoffYear}`, NAME, RL_WEB);
      writeJSON(bracketPath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404")) errors.push(`playoffs bracket: ${msg}`);
    }
  }

  // Playoff series carousel
  const carouselPath = sPath(ctx.dataDir, ctx.season, "playoffs", "carousel.json");
  if (!fileExists(carouselPath) && !ctx.dryRun) {
    try {
      const data = await fetchJSON(`${BASE_WEB}/playoff-series/carousel/${sid}/`, NAME, RL_WEB);
      writeJSON(carouselPath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404")) errors.push(`playoffs carousel: ${msg}`);
    }
  }

  // Per-series schedules (a = first-round series 1, b = first-round series 2, …)
  // Maximum 15 series in a full 4-round NHL playoff; stop on first 404.
  for (const letter of "abcdefghijklmno".split("")) {
    const seriesPath = sPath(ctx.dataDir, ctx.season, "playoffs", "series", letter, "schedule.json");
    if (fileExists(seriesPath)) continue;
    if (ctx.dryRun) continue;
    try {
      const data = await fetchJSON(
        `${BASE_WEB}/schedule/playoff-series/${sid}/${letter}/`,
        NAME, RL_WEB,
      );
      writeJSON(seriesPath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (msg.includes("404")) break; // no more series this season
      errors.push(`playoffs series ${letter}: ${msg}`);
    }
  }

  // Playoff series metadata for each letter found
  // (series carousel gives us the letter list but metadata has extra context)
  const carousel = readJSON<{ series?: Array<{ seriesLetter?: string }> }>(carouselPath);
  for (const s of carousel?.series ?? []) {
    if (!s.seriesLetter) continue;
    const metaPath = sPath(ctx.dataDir, ctx.season, "playoffs", "series", s.seriesLetter, "metadata.json");
    if (fileExists(metaPath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(
        `${BASE_WEB}/meta/playoff-series/${playoffYear}/${s.seriesLetter}`,
        NAME, RL_WEB,
      );
      writeJSON(metaPath, data);
      filesWritten++;
    } catch { /* series metadata is optional */ }
  }

  logger.progress(NAME, "nhl", "playoffs", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: draft ───────────────────────────────────────

async function importDraft(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const draftYear = ctx.season + 1;

  const out: Array<[string, string]> = [
    [sPath(ctx.dataDir, ctx.season, "draft", "rankings_now.json"), `${BASE_WEB}/draft/rankings/now`],
    [sPath(ctx.dataDir, ctx.season, "draft", "tracker_picks_now.json"), `${BASE_WEB}/draft-tracker/picks/now`],
    [sPath(ctx.dataDir, ctx.season, "draft", "picks_now.json"), `${BASE_WEB}/draft/picks/now`],
    [sPath(ctx.dataDir, ctx.season, "draft", "picks_all_rounds.json"), `${BASE_WEB}/draft/picks/${draftYear}/all`],
  ];

  for (const category of [1, 2, 3, 4]) {
    out.push([
      sPath(ctx.dataDir, ctx.season, "draft", "rankings_by_category", `${draftYear}_${category}.json`),
      `${BASE_WEB}/draft/rankings/${draftYear}/${category}`,
    ]);
  }

  for (const round of [1, 2, 3, 4, 5, 6, 7]) {
    out.push([
      sPath(ctx.dataDir, ctx.season, "draft", "picks_by_round", `${draftYear}_${round}.json`),
      `${BASE_WEB}/draft/picks/${draftYear}/${round}`,
    ]);
  }

  for (const [filePath, url] of out) {
    if (fileExists(filePath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(url, NAME, RL_WEB);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404") && !msg.includes("400")) {
        errors.push(`draft ${ctx.season}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "draft", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: meta ────────────────────────────────────────

async function importMeta(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const optionalUrls = new Set<string>([
    `${BASE_WEB}/where-to-watch`,
    `https://api-web.nhle.com/model/v1/openapi.json`,
  ]);

  const out: Array<[string, string]> = [
    [sPath(ctx.dataDir, ctx.season, "meta", "meta.json"), `${BASE_WEB}/meta`],
    [sPath(ctx.dataDir, ctx.season, "meta", "location.json"), `${BASE_WEB}/location`],
    [sPath(ctx.dataDir, ctx.season, "meta", "standings_season.json"), `${BASE_WEB}/standings-season`],
    [sPath(ctx.dataDir, ctx.season, "meta", "player_spotlight.json"), `${BASE_WEB}/player-spotlight`],
    [sPath(ctx.dataDir, ctx.season, "meta", "where_to_watch.json"), `${BASE_WEB}/where-to-watch`],
    [sPath(ctx.dataDir, ctx.season, "meta", "season_ids.json"), `${BASE_WEB}/season`],
    [sPath(ctx.dataDir, ctx.season, "meta", "partner_game_us_now.json"), `${BASE_WEB}/partner-game/US/now`],
    [sPath(ctx.dataDir, ctx.season, "meta", "partner_game_ca_now.json"), `${BASE_WEB}/partner-game/CA/now`],
    [sPath(ctx.dataDir, ctx.season, "meta", "partner_game_gb_now.json"), `${BASE_WEB}/partner-game/GB/now`],
    [sPath(ctx.dataDir, ctx.season, "meta", "partner_game_au_now.json"), `${BASE_WEB}/partner-game/AU/now`],
    [sPath(ctx.dataDir, ctx.season, "meta", "openapi_model_v1.json"), `https://api-web.nhle.com/model/v1/openapi.json`],
  ];

  for (const [filePath, url] of out) {
    if (fileExists(filePath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(url, NAME, RL_WEB, optionalUrls.has(url) ? { retries: 1, timeoutMs: 8_000 } : undefined);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (optionalUrls.has(url) && (msg.includes("404") || msg.includes("400") || msg.includes("500"))) continue;
      if (!msg.includes("404") && !msg.includes("400")) {
        errors.push(`meta ${ctx.season}: ${msg}`);
      }
    }
  }

  for (const [label, postalCode] of [["us_10001", "10001"], ["ca_m5v1e3", "M5V1E3"], ["us_90210", "90210"]] as const) {
    const filePath = sPath(ctx.dataDir, ctx.season, "meta", "postal_lookup", `${label}.json`);
    if (fileExists(filePath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(`${BASE_WEB}/postal-lookup/${postalCode}`, NAME, RL_WEB);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404") && !msg.includes("400")) errors.push(`meta postal ${postalCode}: ${msg}`);
    }
  }

  logger.progress(NAME, "nhl", "meta", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: edge ────────────────────────────────────────

async function importEdge(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);
  const optionalUrls = new Set<string>([
    `${BASE_WEB}/edge/by-the-numbers`,
  ]);

  const out: Array<[string, string]> = [
    [sPath(ctx.dataDir, ctx.season, "edge", "by_the_numbers.json"), `${BASE_WEB}/edge/by-the-numbers`],
    [sPath(ctx.dataDir, ctx.season, "edge", "team_landing_now.json"), `${BASE_WEB}/edge/team-landing/now`],
    [sPath(ctx.dataDir, ctx.season, "edge", "skater_landing_now.json"), `${BASE_WEB}/edge/skater-landing/now`],
    [sPath(ctx.dataDir, ctx.season, "edge", "goalie_landing_now.json"), `${BASE_WEB}/edge/goalie-landing/now`],
  ];

  for (const gameType of [GAME_TYPE_REGULAR, GAME_TYPE_PLAYOFFS]) {
    const label = gameType === GAME_TYPE_PLAYOFFS ? "playoffs" : "regular";
    out.push(
      [sPath(ctx.dataDir, ctx.season, "edge", `team_landing_${label}.json`), `${BASE_WEB}/edge/team-landing/${sid}/${gameType}`],
      [sPath(ctx.dataDir, ctx.season, "edge", `skater_landing_${label}.json`), `${BASE_WEB}/edge/skater-landing/${sid}/${gameType}`],
      [sPath(ctx.dataDir, ctx.season, "edge", `goalie_landing_${label}.json`), `${BASE_WEB}/edge/goalie-landing/${sid}/${gameType}`],
    );
  }

  for (const [filePath, url] of out) {
    if (fileExists(filePath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(url, NAME, RL_WEB, optionalUrls.has(url) ? { retries: 1, timeoutMs: 8_000 } : undefined);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (optionalUrls.has(url) && (msg.includes("404") || msg.includes("400") || msg.includes("500"))) continue;
      if (!msg.includes("404") && !msg.includes("400")) {
        errors.push(`edge ${ctx.season}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "edge", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: edge_deep ───────────────────────────────────

async function importEdgeDeep(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const sid = toSeasonId(ctx.season);

  const games: NHLGame[] = [
    ...(readJSON<NHLGame[]>(sPath(ctx.dataDir, ctx.season, "schedule", "regular.json")) ?? []),
    ...(readJSON<NHLGame[]>(sPath(ctx.dataDir, ctx.season, "schedule", "playoffs.json")) ?? []),
  ];
  if (games.length === 0) {
    logger.info("No schedule found — run schedule endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  const teamIds = new Set<number>();
  for (const g of games) {
    if (typeof g.homeTeam?.id === "number") teamIds.add(g.homeTeam.id);
    if (typeof g.awayTeam?.id === "number") teamIds.add(g.awayTeam.id);
  }

  const playerIds = new Set<number>();
  for (const team of teamsForSeason(ctx.season)) {
    const rosterPath = sPath(ctx.dataDir, ctx.season, "rosters", `${team}.json`);
    const roster = readJSON<Record<string, Array<{ id?: number; playerId?: number }>>>(rosterPath);
    if (!roster) continue;
    for (const group of Object.values(roster)) {
      if (!Array.isArray(group)) continue;
      for (const p of group) {
        const id = p.id ?? p.playerId;
        if (typeof id === "number") playerIds.add(id);
      }
    }
  }

  const writeIfFound = async (filePath: string, url: string): Promise<void> => {
    if (fileExists(filePath) || ctx.dryRun) return;
    try {
      const data = await fetchJSON(url, NAME, RL_WEB);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404") && !msg.includes("400") && !msg.includes("500")) {
        errors.push(`edge_deep ${ctx.season}: ${msg}`);
      }
    }
  };

  const positions = ["all", "F", "D"] as const;
  const strengths = ["all", "pp", "pk", "es"] as const;
  const zoneSorts = ["offensive", "neutral", "defensive"] as const;
  const locationCats = ["all", "high-danger", "mid-range", "long-range"] as const;
  const locationSorts = ["shots", "goals", "pct"] as const;

  for (const teamId of teamIds) {
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "team_detail", "now", `${teamId}.json`),
      `${BASE_WEB}/edge/team-detail/${teamId}/now`,
    );
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "team_comparison", "now", `${teamId}.json`),
      `${BASE_WEB}/edge/team-comparison/${teamId}/now`,
    );
    for (const detail of [
      "team-skating-distance-detail",
      "team-skating-speed-detail",
      "team-zone-time-details",
      "team-shot-speed-detail",
      "team-shot-location-detail",
    ] as const) {
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", detail, "now", `${teamId}.json`),
        `${BASE_WEB}/edge/${detail}/${teamId}/now`,
      );
    }
  }

  for (const pid of playerIds) {
    for (const detail of [
      "skater-detail",
      "skater-comparison",
      "skater-skating-distance-detail",
      "skater-skating-speed-detail",
      "skater-zone-time",
      "skater-shot-speed-detail",
      "skater-shot-location-detail",
      "goalie-detail",
      "goalie-comparison",
      "goalie-5v5-detail",
      "goalie-shot-location-detail",
      "goalie-save-percentage-detail",
    ] as const) {
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", detail.replace(/-/g, "_"), "now", `${pid}.json`),
        `${BASE_WEB}/edge/${detail}/${pid}/now`,
      );
    }
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "cat_skater_detail", "now", `${pid}.json`),
      `${BASE_WEB}/cat/edge/skater-detail/${pid}/now`,
    );
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "cat_goalie_detail", "now", `${pid}.json`),
      `${BASE_WEB}/cat/edge/goalie-detail/${pid}/now`,
    );
  }

  for (const pos of positions) {
    for (const st of strengths) {
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "team_skating_distance_top10", "now", `${pos}_${st}_total.json`),
        `${BASE_WEB}/edge/team-skating-distance-top-10/${pos}/${st}/total/now`,
      );
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "skater_distance_top10", "now", `${pos}_${st}_total.json`),
        `${BASE_WEB}/edge/skater-distance-top-10/${pos}/${st}/total/now`,
      );
      for (const z of zoneSorts) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", "skater_zone_time_top10", "now", `${pos}_${st}_${z}.json`),
          `${BASE_WEB}/edge/skater-zone-time-top-10/${pos}/${st}/${z}/now`,
        );
      }
    }
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "team_skating_speed_top10", "now", `${pos}_max.json`),
      `${BASE_WEB}/edge/team-skating-speed-top-10/${pos}/max/now`,
    );
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "team_shot_speed_top10", "now", `${pos}_max.json`),
      `${BASE_WEB}/edge/team-shot-speed-top-10/${pos}/max/now`,
    );
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "skater_speed_top10", "now", `${pos}_max.json`),
      `${BASE_WEB}/edge/skater-speed-top-10/${pos}/max/now`,
    );
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "skater_shot_speed_top10", "now", `${pos}_max.json`),
      `${BASE_WEB}/edge/skater-shot-speed-top-10/${pos}/max/now`,
    );
    for (const cat of locationCats) {
      for (const sort of locationSorts) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", "team_shot_location_top10", "now", `${pos}_${cat}_${sort}.json`),
          `${BASE_WEB}/edge/team-shot-location-top-10/${pos}/${cat}/${sort}/now`,
        );
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", "skater_shot_location_top10", "now", `${pos}_${cat}_${sort}.json`),
          `${BASE_WEB}/edge/skater-shot-location-top-10/${pos}/${cat}/${sort}/now`,
        );
      }
    }
  }
  for (const st of strengths) {
    for (const z of zoneSorts) {
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "team_zone_time_top10", "now", `${st}_${z}.json`),
        `${BASE_WEB}/edge/team-zone-time-top-10/${st}/${z}/now`,
      );
    }
  }
  await writeIfFound(
    sPath(ctx.dataDir, ctx.season, "edge", "goalie_5v5_top10", "now", `shots.json`),
    `${BASE_WEB}/edge/goalie-5v5-top-10/shots/now`,
  );
  for (const cat of locationCats) {
    for (const sort of ["shots", "saves", "goals", "pct"] as const) {
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "goalie_shot_location_top10", "now", `${cat}_${sort}.json`),
        `${BASE_WEB}/edge/goalie-shot-location-top-10/${cat}/${sort}/now`,
      );
    }
  }
  await writeIfFound(
    sPath(ctx.dataDir, ctx.season, "edge", "goalie_save_pct_top10", "now", `pct.json`),
    `${BASE_WEB}/edge/goalie-edge-save-pctg-top-10/pct/now`,
  );

  for (const gameType of [GAME_TYPE_REGULAR, GAME_TYPE_PLAYOFFS]) {
    const gt = gameType === GAME_TYPE_PLAYOFFS ? "playoffs" : "regular";

    // Team endpoints
    for (const teamId of teamIds) {
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "team_detail", gt, `${teamId}.json`),
        `${BASE_WEB}/edge/team-detail/${teamId}/${sid}/${gameType}`,
      );
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "team_comparison", gt, `${teamId}.json`),
        `${BASE_WEB}/edge/team-comparison/${teamId}/${sid}/${gameType}`,
      );
      for (const detail of [
        "team-skating-distance-detail",
        "team-skating-speed-detail",
        "team-zone-time-details",
        "team-shot-speed-detail",
        "team-shot-location-detail",
      ] as const) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", detail, gt, `${teamId}.json`),
          `${BASE_WEB}/edge/${detail}/${teamId}/${sid}/${gameType}`,
        );
      }
    }

    // Team top-10 endpoint families
    for (const pos of positions) {
      for (const st of strengths) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", "team_skating_distance_top10", gt, `${pos}_${st}_total.json`),
          `${BASE_WEB}/edge/team-skating-distance-top-10/${pos}/${st}/total/${sid}/${gameType}`,
        );
      }
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "team_skating_speed_top10", gt, `${pos}_max.json`),
        `${BASE_WEB}/edge/team-skating-speed-top-10/${pos}/max/${sid}/${gameType}`,
      );
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "team_shot_speed_top10", gt, `${pos}_max.json`),
        `${BASE_WEB}/edge/team-shot-speed-top-10/${pos}/max/${sid}/${gameType}`,
      );
      for (const cat of locationCats) {
        for (const sort of locationSorts) {
          await writeIfFound(
            sPath(ctx.dataDir, ctx.season, "edge", "team_shot_location_top10", gt, `${pos}_${cat}_${sort}.json`),
            `${BASE_WEB}/edge/team-shot-location-top-10/${pos}/${cat}/${sort}/${sid}/${gameType}`,
          );
        }
      }
    }
    for (const st of strengths) {
      for (const z of zoneSorts) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", "team_zone_time_top10", gt, `${st}_${z}.json`),
          `${BASE_WEB}/edge/team-zone-time-top-10/${st}/${z}/${sid}/${gameType}`,
        );
      }
    }

    // Player endpoints (skater/goalie detail + cat detail)
    for (const pid of playerIds) {
      for (const detail of [
        "skater-detail",
        "skater-comparison",
        "skater-skating-distance-detail",
        "skater-skating-speed-detail",
        "skater-zone-time",
        "skater-shot-speed-detail",
        "skater-shot-location-detail",
      ] as const) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", detail, gt, `${pid}.json`),
          `${BASE_WEB}/edge/${detail}/${pid}/${sid}/${gameType}`,
        );
      }
      for (const detail of [
        "goalie-detail",
        "goalie-comparison",
        "goalie-5v5-detail",
        "goalie-shot-location-detail",
        "goalie-save-percentage-detail",
      ] as const) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", detail, gt, `${pid}.json`),
          `${BASE_WEB}/edge/${detail}/${pid}/${sid}/${gameType}`,
        );
      }
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "cat_skater_detail", gt, `${pid}.json`),
        `${BASE_WEB}/cat/edge/skater-detail/${pid}/${sid}/${gameType}`,
      );
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "cat_goalie_detail", gt, `${pid}.json`),
        `${BASE_WEB}/cat/edge/goalie-detail/${pid}/${sid}/${gameType}`,
      );
    }

    // Skater/goalie top-10 endpoint families
    for (const pos of positions) {
      for (const st of strengths) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", "skater_distance_top10", gt, `${pos}_${st}_total.json`),
          `${BASE_WEB}/edge/skater-distance-top-10/${pos}/${st}/total/${sid}/${gameType}`,
        );
        for (const z of zoneSorts) {
          await writeIfFound(
            sPath(ctx.dataDir, ctx.season, "edge", "skater_zone_time_top10", gt, `${pos}_${st}_${z}.json`),
            `${BASE_WEB}/edge/skater-zone-time-top-10/${pos}/${st}/${z}/${sid}/${gameType}`,
          );
        }
      }
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "skater_speed_top10", gt, `${pos}_max.json`),
        `${BASE_WEB}/edge/skater-speed-top-10/${pos}/max/${sid}/${gameType}`,
      );
      await writeIfFound(
        sPath(ctx.dataDir, ctx.season, "edge", "skater_shot_speed_top10", gt, `${pos}_max.json`),
        `${BASE_WEB}/edge/skater-shot-speed-top-10/${pos}/max/${sid}/${gameType}`,
      );
      for (const cat of locationCats) {
        for (const sort of locationSorts) {
          await writeIfFound(
            sPath(ctx.dataDir, ctx.season, "edge", "skater_shot_location_top10", gt, `${pos}_${cat}_${sort}.json`),
            `${BASE_WEB}/edge/skater-shot-location-top-10/${pos}/${cat}/${sort}/${sid}/${gameType}`,
          );
        }
      }
    }
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "goalie_5v5_top10", gt, `shots.json`),
      `${BASE_WEB}/edge/goalie-5v5-top-10/shots/${sid}/${gameType}`,
    );
    for (const cat of locationCats) {
      for (const sort of ["shots", "saves", "goals", "pct"] as const) {
        await writeIfFound(
          sPath(ctx.dataDir, ctx.season, "edge", "goalie_shot_location_top10", gt, `${cat}_${sort}.json`),
          `${BASE_WEB}/edge/goalie-shot-location-top-10/${cat}/${sort}/${sid}/${gameType}`,
        );
      }
    }
    await writeIfFound(
      sPath(ctx.dataDir, ctx.season, "edge", "goalie_save_pct_top10", gt, `pct.json`),
      `${BASE_WEB}/edge/goalie-edge-save-pctg-top-10/pct/${sid}/${gameType}`,
    );
  }

  logger.progress(NAME, "nhl", "edge_deep", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: replays ─────────────────────────────────────

async function importReplays(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];
  const games: NHLGame[] = [
    ...(readJSON<NHLGame[]>(sPath(ctx.dataDir, ctx.season, "schedule", "regular.json")) ?? []),
    ...(readJSON<NHLGame[]>(sPath(ctx.dataDir, ctx.season, "schedule", "playoffs.json")) ?? []),
  ];

  if (games.length === 0) {
    logger.info("No schedule found — run schedule endpoint first", NAME);
    return { filesWritten: 0, errors: [] };
  }

  for (const g of games) {
    const label = g.gameType === GAME_TYPE_PLAYOFFS ? "playoffs" : "regular";
    const pbpPath = sPath(ctx.dataDir, ctx.season, "games", label, String(g.id), "play_by_play.json");
    const pbp = readJSON<Record<string, unknown>>(pbpPath);
    if (!pbp) continue;

    const source = (Array.isArray((pbp as { plays?: unknown[] }).plays)
      ? (pbp as { plays?: unknown[] }).plays
      : Array.isArray((pbp as { events?: unknown[] }).events)
        ? (pbp as { events?: unknown[] }).events
        : []) as Array<Record<string, unknown>>;

    const eventNums = new Set<number>();
    for (const ev of source) {
      const n = ev.eventId ?? ev.eventNumber ?? ev.sortOrder ?? ev.sequenceNumber;
      if (typeof n === "number") eventNums.add(n);
    }

    for (const eventNo of eventNums) {
      const base = sPath(ctx.dataDir, ctx.season, "replays", label, String(g.id), String(eventNo));
      const out: Array<[string, string]> = [
        [path.join(base, "play.json"), `${BASE_WEB}/ppt-replay/${g.id}/${eventNo}`],
        [path.join(base, "goal.json"), `${BASE_WEB}/ppt-replay/goal/${g.id}/${eventNo}`],
      ];
      for (const [filePath, url] of out) {
        if (fileExists(filePath) || ctx.dryRun) continue;
        try {
          const data = await fetchJSON(url, NAME, RL_WEB, { retries: 1, timeoutMs: 8_000 });
          writeJSON(filePath, data);
          filesWritten++;
        } catch (err) {
          const msg = String(err instanceof Error ? err.message : err);
          if (!msg.includes("404") && !msg.includes("400") && !msg.includes("500")) {
            errors.push(`replay ${g.id}/${eventNo}: ${msg}`);
          }
        }
      }
    }
  }

  logger.progress(NAME, "nhl", "replays", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint: stats_misc ─────────────────────────────────

async function importStatsMisc(ctx: EndpointCtx): Promise<EndpointResult> {
  let filesWritten = 0;
  const errors: string[] = [];

  const games: NHLGame[] = [
    ...(readJSON<NHLGame[]>(sPath(ctx.dataDir, ctx.season, "schedule", "regular.json")) ?? []),
    ...(readJSON<NHLGame[]>(sPath(ctx.dataDir, ctx.season, "schedule", "playoffs.json")) ?? []),
  ];
  const teamIds = new Set<number>();
  for (const g of games) {
    if (typeof g.homeTeam?.id === "number") teamIds.add(g.homeTeam.id);
    if (typeof g.awayTeam?.id === "number") teamIds.add(g.awayTeam.id);
  }

  const out: Array<[string, string]> = [
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "ping.json"), `https://api.nhle.com/stats/rest/ping`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "config.json"), `${BASE_STATS}/config`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "country.json"), `${BASE_STATS}/country`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "team.json"), `${BASE_STATS}/team`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "franchise.json"), `${BASE_STATS}/franchise`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "glossary.json"), `${BASE_STATS}/glossary`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "skater_info.json"), `${BASE_STATS}/skater`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "goalie_info.json"), `${BASE_STATS}/goalie`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "component_season.json"), `${BASE_STATS}/componentSeason`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "season.json"), `${BASE_STATS}/season`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "game_meta.json"), `${BASE_STATS}/game/meta`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "game.json"), `${BASE_STATS}/game`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "draft.json"), `${BASE_STATS}/draft`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "players_full.json"), `${BASE_STATS}/players?limit=-1&sort=lastName&dir=asc`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "players_sample.json"), `${BASE_STATS}/players?limit=5&sort=lastName&dir=asc`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "leaders_skaters_points.json"), `${BASE_STATS}/leaders/skaters/points`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "leaders_skaters_goals.json"), `${BASE_STATS}/leaders/skaters/goals`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "leaders_goalies_wins.json"), `${BASE_STATS}/leaders/goalies/wins`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "leaders_goalies_gaa.json"), `${BASE_STATS}/leaders/goalies/gaa`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "milestones_skaters.json"), `${BASE_STATS}/milestones/skaters`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "milestones_goalies.json"), `${BASE_STATS}/milestones/goalies`],
    [sPath(ctx.dataDir, ctx.season, "stats_misc", "content_module_overview.json"), `${BASE_STATS}/content/module/overview`],
  ];

  for (const teamId of teamIds) {
    out.push([
      sPath(ctx.dataDir, ctx.season, "stats_misc", "team_by_id", `${teamId}.json`),
      `${BASE_STATS}/team/id/${teamId}`,
    ]);
  }

  for (const [filePath, url] of out) {
    if (fileExists(filePath) || ctx.dryRun) continue;
    try {
      const data = await fetchJSON(url, NAME, RL_STATS);
      writeJSON(filePath, data);
      filesWritten++;
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      if (!msg.includes("404") && !msg.includes("400")) {
        errors.push(`stats_misc ${ctx.season}: ${msg}`);
      }
    }
  }

  logger.progress(NAME, "nhl", "stats_misc", `${filesWritten} files for ${ctx.season}`);
  return { filesWritten, errors };
}

// ── Endpoint dispatcher ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  reference:    importReference,
  schedule:     importSchedule,
  standings:    importStandings,
  rosters:      importRosters,
  club_stats:   importClubStats,
  team_extras:  importTeamExtras,
  league_feeds: importLeagueFeeds,
  games:        importGames,
  shift_charts: importShiftCharts,
  players:      importPlayers,
  skater_stats: importSkaterStats,
  goalie_stats: importGoalieStats,
  team_stats:   importTeamStats,
  leaders:      importLeaders,
  current_leaders: importCurrentLeaders,
  playoffs:     importPlayoffs,
  draft:        importDraft,
  meta:         importMeta,
  edge:         importEdge,
  edge_deep:    importEdgeDeep,
  replays:      importReplays,
  stats_misc:   importStatsMisc,
};

// ── Provider ──────────────────────────────────────────────

const nhl: Provider = {
  name: NAME,
  label: "NHL Official API",
  sports: SPORTS,
  requiresKey: false,
  rateLimit: RL_WEB,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const t0 = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS];

    for (const season of opts.seasons) {
      logger.info(`NHL ${season}–${season + 1} season`, NAME);
      for (const ep of activeEndpoints) {
        try {
          const res = await ENDPOINT_FNS[ep]({ season, dataDir: opts.dataDir, dryRun: opts.dryRun });
          totalFiles += res.filesWritten;
          allErrors.push(...res.errors);
        } catch (err) {
          const msg = `${ep} ${season}: ${err instanceof Error ? err.message : String(err)}`;
          logger.error(msg, NAME);
          allErrors.push(msg);
        }
      }
    }

    return {
      provider: NAME,
      sport: "nhl",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - t0,
    };
  },
};

export default nhl;

// ─────────────────────────────────────────────────────────────────────────────
// Legacy shim — kept so existing schedule.json files are still readable.
// The old layout stored a flat game array at {season}/schedule.json.
// The new layout stores {season}/schedule/regular.json + playoffs.json.
// When running `games` on a season that only has the old file, this shim
// converts and re-saves into the new structure automatically.
// ─────────────────────────────────────────────────────────────────────────────
function migrateOldSchedule(dataDir: string, season: number): void {
  const oldPath = rawPath(dataDir, NAME, "nhl", season, "schedule.json");
  if (!fileExists(oldPath)) return;

  const regularPath  = sPath(dataDir, season, "schedule", "regular.json");
  const playoffsPath = sPath(dataDir, season, "schedule", "playoffs.json");
  if (fileExists(regularPath) && fileExists(playoffsPath)) return;

  const games = readJSON<NHLGame[]>(oldPath) ?? [];
  const regular  = games.filter(g => g.gameType === GAME_TYPE_REGULAR || g.gameType == null);
  const playoffs = games.filter(g => g.gameType === GAME_TYPE_PLAYOFFS);

  if (!fileExists(regularPath) && regular.length > 0)  writeJSON(regularPath, regular);
  if (!fileExists(playoffsPath) && playoffs.length > 0) writeJSON(playoffsPath, playoffs);
}

// Run migration on existing seasons at module load (non-blocking, best-effort)
void (async () => {
  const dataDir = process.env["DATA_DIR"];
  if (!dataDir) return;
  for (const season of [2019, 2020, 2021, 2022, 2023, 2024, 2025]) {
    try { migrateOldSchedule(dataDir, season); } catch { /* ignore */ }
  }
})();
