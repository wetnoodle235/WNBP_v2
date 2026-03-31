// ──────────────────────────────────────────────────────────
// V5.0 ESPN Provider
// ──────────────────────────────────────────────────────────
// Fetches games, teams, standings, rosters, injuries, news,
// odds, players, scoreboards, team stats, team schedules,
// player stats, depth charts, transactions, rankings, and
// futures from ESPN's public APIs.
// No API key required.

import type { Provider, ImportOptions, ImportResult, Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { writeJSON, rawPath, fileExists, readJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

// ── ESPN API bases ──────────────────────────────────────────
const SITE_API = "https://site.api.espn.com/apis/site/v2/sports";
const CORE_API = "https://sports.core.api.espn.com/v2/sports";

// ── Rate limit: ~10 req/sec (ESPN is lenient; batching needs headroom) ──
const RATE_LIMIT: RateLimitConfig = { requests: 10, perMs: 1_000 };

// ── Sport slug mapping ──────────────────────────────────────
const SPORT_SLUGS: Partial<Record<Sport, string>> = {
  nba:        "basketball/nba",
  wnba:       "basketball/wnba",
  ncaab:      "basketball/mens-college-basketball",
  ncaaw:      "basketball/womens-college-basketball",
  nfl:        "football/nfl",
  ncaaf:      "football/college-football",
  mlb:        "baseball/mlb",
  nhl:        "hockey/nhl",
  epl:        "soccer/eng.1",
  laliga:     "soccer/esp.1",
  bundesliga: "soccer/ger.1",
  seriea:     "soccer/ita.1",
  ligue1:     "soccer/fra.1",
  mls:        "soccer/usa.1",
  ucl:        "soccer/uefa.champions",
  nwsl:       "soccer/usa.nwsl",
  golf:       "golf/pga",
  f1:         "racing/f1",
  atp:        "tennis/atp",
  wta:        "tennis/wta",
};

const SUPPORTED_SPORTS = Object.keys(SPORT_SLUGS) as Sport[];

// Soccer leagues use cross-year seasons (e.g., EPL 2023 → 2023-24 season).
const SOCCER_SPORTS = new Set<Sport>(["epl", "laliga", "bundesliga", "seriea", "ligue1", "ucl"]);

// Sports where the /summary endpoint doesn't work — use scoreboard event data directly
const SCOREBOARD_ONLY_SPORTS = new Set<Sport>(["golf", "f1", "atp", "wta"]);

const ALL_ENDPOINTS = [
  "teams",
  "standings",
  "scoreboard",
  "games",
  "rosters",
  "injuries",
  "news",
  "odds",
  "players",
  "athletes",
  "team_stats",
  "team_schedule",
  "player_stats",
  "depth_charts",
  "transactions",
  "rankings",
  "futures",
] as const;

type Endpoint = (typeof ALL_ENDPOINTS)[number];

// ── Helpers ─────────────────────────────────────────────────

function slug(sport: Sport): string {
  return SPORT_SLUGS[sport]!;
}

/** Split "basketball/nba" → { sportName: "basketball", leagueName: "nba" } */
function splitSlug(sport: Sport): { sportName: string; leagueName: string } {
  const s = slug(sport);
  const [sportName, leagueName] = s.split("/");
  return { sportName, leagueName };
}

/** Core API path requires sport/leagues/league format. */
function coreLeaguePath(sport: Sport): string {
  const { sportName, leagueName } = splitSlug(sport);
  return `${sportName}/leagues/${leagueName}`;
}

/**
 * For soccer cross-year seasons we need to pass the correct season year
 * to the ESPN API. ESPN typically indexes by the starting year.
 */
function seasonParam(sport: Sport, season: number): number {
  // ESPN uses the start year for all sports, including soccer.
  return season;
}

/**
 * Generate date range for game scoreboard iteration.
 * Returns YYYYMMDD strings for every day in the season window.
 */
function seasonDateRange(sport: Sport, season: number): string[] {
  let start: Date;
  let end: Date;

  if (SOCCER_SPORTS.has(sport)) {
    // European soccer: Aug of start year → Jun of next year
    start = new Date(season, 7, 1);   // Aug 1
    end = new Date(season + 1, 5, 30); // Jun 30
  } else {
    switch (sport) {
      case "nfl":
      case "ncaaf":
        // Football: Sep → Feb (Super Bowl)
        start = new Date(season, 8, 1);
        end = new Date(season + 1, 1, 28);
        break;
      case "nba":
      case "wnba":
      case "ncaab":
      case "ncaaw":
        // Basketball: Oct → Jun (NBA Finals) / May → Oct (WNBA)
        if (sport === "wnba") {
          start = new Date(season, 4, 1);  // May
          end = new Date(season, 9, 31);   // Oct
        } else {
          start = new Date(season, 9, 1);  // Oct
          end = new Date(season + 1, 5, 30); // Jun
        }
        break;
      case "mlb":
        // Baseball: Mar → Nov
        start = new Date(season, 2, 1);
        end = new Date(season, 10, 15);
        break;
      case "nhl":
        // Hockey: Oct → Jun
        start = new Date(season, 9, 1);
        end = new Date(season + 1, 5, 30);
        break;
      case "mls":
        // MLS: Feb → Dec
        start = new Date(season, 1, 1);
        end = new Date(season, 11, 31);
        break;
      case "golf":
        // PGA: Jan → Dec (calendar year)
        start = new Date(season, 0, 1);
        end = new Date(season, 11, 31);
        break;
      default:
        start = new Date(season, 0, 1);
        end = new Date(season, 11, 31);
    }
  }

  const dates: string[] = [];
  const cursor = new Date(start);
  while (cursor <= end) {
    const y = cursor.getFullYear();
    const m = String(cursor.getMonth() + 1).padStart(2, "0");
    const d = String(cursor.getDate()).padStart(2, "0");
    dates.push(`${y}${m}${d}`);
    cursor.setDate(cursor.getDate() + 1);
  }
  return dates;
}

/**
 * Trim a full date range to only the last N days (relative to today).
 * Returns the full range if recentDays is 0 or undefined.
 */
function filterRecentDates(dates: string[], recentDays?: number): string[] {
  if (!recentDays || recentDays <= 0) return dates;
  const now = new Date();
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - recentDays);
  const cutoffStr = `${cutoff.getFullYear()}${String(cutoff.getMonth() + 1).padStart(2, "0")}${String(cutoff.getDate()).padStart(2, "0")}`;
  // Include a 1-day buffer after today for timezone safety
  const tomorrow = new Date(now);
  tomorrow.setDate(tomorrow.getDate() + 1);
  const tomorrowStr = `${tomorrow.getFullYear()}${String(tomorrow.getMonth() + 1).padStart(2, "0")}${String(tomorrow.getDate()).padStart(2, "0")}`;
  return dates.filter(d => d >= cutoffStr && d <= tomorrowStr);
}

// ── Fetch helpers (wrapping core fetchJSON) ─────────────────

async function espnFetch<T = unknown>(url: string): Promise<T | null> {
  try {
    return await fetchJSON<T>(url, "espn", RATE_LIMIT);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn(`Fetch failed: ${msg}`, "espn");
    return null;
  }
}

// ── Endpoint implementations ────────────────────────────────

interface EndpointContext {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
  recentDays?: number;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

/** Fetch all teams for a sport/season. */
async function importTeams(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  const url = `${SITE_API}/${sportPath}/teams?limit=200`;
  logger.progress("espn", sport, "teams", `Fetching ${url}`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await espnFetch<any>(url);
  if (!data) {
    errors.push(`${sport}/${season}/teams: no data returned`);
    return { filesWritten, errors };
  }

  const teams = data.sports?.[0]?.leagues?.[0]?.teams ?? [];
  const outPath = rawPath(dataDir, "espn", sport, season, "teams.json");
  writeJSON(outPath, {
    season,
    count: teams.length,
    teams: teams.map((t: any) => t.team ?? t),
    fetchedAt: new Date().toISOString(),
  });
  filesWritten++;

  // Save individual team files (batched for speed)
  const teamsToFetch: { teamId: string; teamPath: string }[] = [];
  for (const entry of teams) {
    const team = entry.team ?? entry;
    const teamId = team.id;
    if (!teamId) continue;
    const teamPath = rawPath(dataDir, "espn", sport, season, "teams", `${teamId}.json`);
    if (fileExists(teamPath)) continue;
    teamsToFetch.push({ teamId: String(teamId), teamPath });
  }

  const TEAM_BATCH = 8;
  for (let i = 0; i < teamsToFetch.length; i += TEAM_BATCH) {
    const batch = teamsToFetch.slice(i, i + TEAM_BATCH);
    const results = await Promise.allSettled(
      batch.map(async ({ teamId, teamPath }) => {
        const detailUrl = `${SITE_API}/${sportPath}/teams/${teamId}?season=${season}`;
        const detail = await espnFetch<any>(detailUrl);
        if (detail) {
          writeJSON(teamPath, { teamId, season, ...detail, fetchedAt: new Date().toISOString() });
          return true;
        }
        return false;
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value) filesWritten++;
    }
  }

  logger.progress("espn", sport, "teams", `Saved ${filesWritten} files (${teams.length} teams)`);
  return { filesWritten, errors };
}

/** Fetch standings. */
async function importStandings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  const yr = seasonParam(sport, season);
  // Use the v2 API path (not site/v2) for full standings with entries/stats
  const url = `https://site.api.espn.com/apis/v2/sports/${sportPath}/standings?season=${yr}`;
  logger.progress("espn", sport, "standings", `Fetching ${url}`);

  if (dryRun) return { filesWritten: 0, errors: [] };

  const data = await espnFetch<any>(url);
  if (!data) {
    errors.push(`${sport}/${season}/standings: no data returned`);
    return { filesWritten, errors };
  }

  const outPath = rawPath(dataDir, "espn", sport, season, "standings.json");
  writeJSON(outPath, {
    season,
    standings: data.children ?? data.standings ?? data,
    fetchedAt: new Date().toISOString(),
  });
  filesWritten++;

  logger.progress("espn", sport, "standings", "Saved standings");
  return { filesWritten, errors };
}

/**
 * Fetch all games for a season.
 * Strategy: use the Core API events list first (fast, gets all event IDs),
 * then fall back to day-by-day scoreboard if that fails.
 * Saves an all_games manifest plus individual game summaries.
 */
async function importGames(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) {
    logger.progress("espn", sport, "games", "Dry run — skipping");
    return { filesWritten: 0, errors: [] };
  }

  // ── Step 1: Get event list via Core API (paginated) ────────
  const leaguePath = coreLeaguePath(sport);
  const yr = seasonParam(sport, season);
  let eventRefs: any[] = [];

  // Fetch all season types: 2=regular, 3=postseason
  // Page 1 first (reveals totalPages), then remaining pages concurrently
  for (const seasonType of [2, 3]) {
    const baseUrl = `${CORE_API}/${leaguePath}/seasons/${yr}/types/${seasonType}/events`;
    logger.progress("espn", sport, "games", `Fetching type ${seasonType} events: ${baseUrl}`);

    const page1 = await espnFetch<any>(`${baseUrl}?limit=100&page=1`);
    if (!page1?.items?.length) continue;

    eventRefs.push(...page1.items);
    const totalPages = page1.pageCount ?? 1;

    if (totalPages > 1) {
      // Fetch remaining pages concurrently
      const pageNums = Array.from({ length: totalPages - 1 }, (_, i) => i + 2);
      const pageResults = await Promise.allSettled(
        pageNums.map(async (p) => {
          const data = await espnFetch<any>(`${baseUrl}?limit=100&page=${p}`);
          return data?.items ?? [];
        }),
      );
      for (const r of pageResults) {
        if (r.status === "fulfilled" && r.value.length) {
          eventRefs.push(...r.value);
        }
      }
      logger.progress("espn", sport, "games", `Type ${seasonType}: fetched ${totalPages} pages concurrently (${eventRefs.length} total events)`);
    }
  }

  if (eventRefs.length === 0) {
    // Fallback: day-by-day scoreboard
    logger.progress("espn", sport, "games", "Core API empty — falling back to scoreboard iteration");
    eventRefs = await fetchGamesByScoreboard(sport, season, ctx.recentDays);
  } else {
    logger.progress("espn", sport, "games", `Core API returned ${eventRefs.length} events across all season types`);
  }

  // Save manifest
  const manifestPath = rawPath(dataDir, "espn", sport, season, "games", "all_games.json");
  writeJSON(manifestPath, {
    season,
    count: eventRefs.length,
    games: eventRefs,
    fetchedAt: new Date().toISOString(),
  });
  filesWritten++;

  logger.progress("espn", sport, "games", `${eventRefs.length} events found — fetching summaries`);

  // ── Step 2a: Merge scoreboard event IDs ─────────────────────
  // Core API may return different event IDs than scoreboard for
  // some sports (e.g., MLB). Scan recent scoreboard files and
  // add any completed game IDs not in the core API set.
  // OPTIMIZATION: Skip merge when Core API returned events for recent-days runs
  // (Core API is authoritative for recent games; merge only helps for historical backfills)
  const coreIds = new Set(eventRefs.map((r: any) => extractEventId(r)).filter(Boolean));
  if (!ctx.recentDays || eventRefs.length === 0) {
    // Full run or Core API empty — do the merge
    const sbDir = rawPath(dataDir, "espn", sport, season.toString(), "scoreboard", "");
    try {
      const { default: fs } = await import("node:fs");
      const sbPath = sbDir.replace(/\/+$/, "");
      if (fs.existsSync(sbPath)) {
        let sbFiles = fs.readdirSync(sbPath).filter((f: string) => f.endsWith(".json"));
        if (ctx.recentDays && ctx.recentDays > 0) {
          const recentDates = new Set(filterRecentDates(seasonDateRange(sport, season), ctx.recentDays));
          sbFiles = sbFiles.filter((f: string) => recentDates.has(f.replace(".json", "")));
        }
        for (const sbFile of sbFiles) {
          const sbData = readJSON<any>(`${sbPath}/${sbFile}`);
          if (!sbData?.events) continue;
          for (const ev of sbData.events) {
            const evId = String(ev.id ?? "");
            if (evId && !coreIds.has(evId)) {
              const statusName = ev.status?.type?.name ?? "";
              if (statusName === "STATUS_FINAL" || statusName === "STATUS_FULL_TIME") {
                eventRefs.push({ id: evId });
                coreIds.add(evId);
              }
            }
          }
        }
        logger.progress("espn", sport, "games", `After scoreboard merge: ${eventRefs.length} total events`);
      }
    } catch { /* scoreboard dir may not exist yet */ }
  } else {
    logger.debug(`Skipping scoreboard merge — Core API returned ${eventRefs.length} events for recent-days run`, "espn");
  }

  // ── Step 2b: Fetch individual game summaries ────────────────
  const sportPath = slug(sport);
  let saved = 0;

  // For scoreboard-only sports (golf), save scoreboard event data directly
  // because the /summary endpoint returns 502.
  if (SCOREBOARD_ONLY_SPORTS.has(sport)) {
    logger.progress("espn", sport, "games", `Scoreboard-only sport — fetching events via scoreboard`);
    const dates = filterRecentDates(seasonDateRange(sport, season), ctx.recentDays);
    const seenIds = new Set<string>();

    // Batch-fetch scoreboard dates (10 concurrent)
    const SB_GAME_BATCH = 10;
    for (let di = 0; di < dates.length; di += SB_GAME_BATCH) {
      const dateBatch = dates.slice(di, di + SB_GAME_BATCH);
      const fetchResults = await Promise.allSettled(
        dateBatch.map((date) =>
          espnFetch<any>(`${SITE_API}/${sportPath}/scoreboard?dates=${date}&limit=100`),
        ),
      );

      for (const fr of fetchResults) {
        if (fr.status !== "fulfilled" || !fr.value?.events?.length) continue;
        const data = fr.value;

        for (const ev of data.events) {
          const eventId = String(ev.id ?? "");
          if (!eventId || seenIds.has(eventId)) continue;
          seenIds.add(eventId);

          const groupings = ev.groupings ?? [];
          const isTennis = sport === "atp" || sport === "wta";

          if (isTennis && groupings.length > 0) {
            const tournPath = rawPath(dataDir, "espn", sport, season, "games", `${eventId}.json`);
            if (!fileExists(tournPath)) {
              writeJSON(tournPath, {
                eventId,
                season,
                tournament: true,
                name: ev.name ?? ev.shortName ?? "",
                summary: { header: { competitions: [] }, event: ev },
                scoreboard: ev,
                fetchedAt: new Date().toISOString(),
              });
              filesWritten++;
            }
            saved++;

            for (const group of groupings) {
              const matches = group.competitions ?? [];
              for (const match of matches) {
                const matchId = String(match.id ?? "");
                if (!matchId) continue;
                const matchPath = rawPath(dataDir, "espn", sport, season, "games", `match_${matchId}.json`);
                if (fileExists(matchPath)) { saved++; continue; }
                writeJSON(matchPath, {
                  eventId: matchId,
                  tournamentId: eventId,
                  tournamentName: ev.name ?? ev.shortName ?? "",
                  season,
                  matchData: match,
                  fetchedAt: new Date().toISOString(),
                });
                filesWritten++;
                saved++;
              }
            }
          } else {
            const gamePath = rawPath(dataDir, "espn", sport, season, "games", `${eventId}.json`);
            if (fileExists(gamePath)) { saved++; continue; }

            writeJSON(gamePath, {
              eventId,
              season,
              summary: { header: { competitions: ev.competitions ?? [] }, event: ev },
              scoreboard: ev,
              fetchedAt: new Date().toISOString(),
            });
            filesWritten++;
            saved++;
          }
        }
      }

      if (saved > 0 && (di + SB_GAME_BATCH) % 30 === 0) {
        logger.progress("espn", sport, "games", `${saved} events saved`);
      }
    }
  } else {
    // Standard: fetch individual game summaries via Site API
    // Build list of events that need fetching (skip already-final games)
    const toFetch: { eventId: string; gamePath: string }[] = [];
    const { default: fs } = await import("node:fs");
    for (const ref of eventRefs) {
      const eventId = extractEventId(ref);
      if (!eventId) continue;
      const gamePath = rawPath(dataDir, "espn", sport, season, "games", `${eventId}.json`);
      if (fileExists(gamePath)) {
        // Fast path: if file is old (>48h), assume game is final — skip JSON parse
        try {
          const mtime = fs.statSync(gamePath).mtimeMs;
          if (Date.now() - mtime > 48 * 60 * 60 * 1000) {
            saved++;
            continue;
          }
        } catch { /* stat failed, check JSON */ }
        const existing = readJSON<any>(gamePath);
        const existingStatus = existing?.summary?.header?.competitions?.[0]?.status?.type?.name ?? "unknown";
        if (existingStatus === "STATUS_FINAL" || existingStatus === "STATUS_FULL_TIME") {
          saved++;
          continue;
        }
      }
      toFetch.push({ eventId, gamePath });
    }

    logger.progress("espn", sport, "games", `${saved} already final, ${toFetch.length} to fetch`);

    // Fetch in concurrent batches (rate limit handles throttling)
    const BATCH_SIZE = 10;
    for (let i = 0; i < toFetch.length; i += BATCH_SIZE) {
      const batch = toFetch.slice(i, i + BATCH_SIZE);
      const results = await Promise.allSettled(
        batch.map(async ({ eventId, gamePath }) => {
          const summaryUrl = `${SITE_API}/${sportPath}/summary?event=${eventId}`;
          const summary = await espnFetch<any>(summaryUrl);
          if (summary) {
            writeJSON(gamePath, { eventId, season, summary, fetchedAt: new Date().toISOString() });
            return { ok: true, eventId };
          }
          return { ok: false, eventId };
        }),
      );
      for (const r of results) {
        if (r.status === "fulfilled" && r.value.ok) {
          filesWritten++;
          saved++;
        } else if (r.status === "fulfilled" && !r.value.ok) {
          errors.push(`${sport}/${season}/games/${r.value.eventId}: summary fetch failed`);
        } else if (r.status === "rejected") {
          errors.push(`${sport}/${season}/games: batch fetch error`);
        }
      }
      if (saved % 20 === 0 && saved > 0) {
        logger.progress("espn", sport, "games", `${saved}/${eventRefs.length} game summaries`);
      }
    }
  } // end else (non-scoreboard-only sports)

  logger.progress("espn", sport, "games", `Done — ${filesWritten} files written`);
  return { filesWritten, errors };
}

/** Extract event ID from a Core API $ref or a scoreboard event object. */
function extractEventId(ref: any): string | null {
  if (typeof ref === "string") {
    return ref.split("/").pop()?.split("?")[0] ?? null;
  }
  if (ref?.$ref) {
    return ref.$ref.split("/").pop()?.split("?")[0] ?? null;
  }
  if (ref?.id) return String(ref.id);
  return null;
}

/** Fallback: iterate day-by-day through the season using the scoreboard endpoint (batched). */
async function fetchGamesByScoreboard(sport: Sport, season: number, recentDays?: number): Promise<any[]> {
  const sportPath = slug(sport);
  const dates = filterRecentDates(seasonDateRange(sport, season), recentDays);
  const allEvents: any[] = [];
  const seenIds = new Set<string>();

  logger.progress("espn", sport, "games", `Scoreboard fallback: ${dates.length} days to scan (batched)`);

  const SB_BATCH = 10;
  for (let i = 0; i < dates.length; i += SB_BATCH) {
    const batch = dates.slice(i, i + SB_BATCH);
    const results = await Promise.allSettled(
      batch.map((date) =>
        espnFetch<any>(`${SITE_API}/${sportPath}/scoreboard?dates=${date}&limit=100`),
      ),
    );

    for (const r of results) {
      if (r.status === "fulfilled" && r.value?.events?.length) {
        for (const ev of r.value.events) {
          const id = String(ev.id);
          if (!seenIds.has(id)) {
            seenIds.add(id);
            allEvents.push({ id, $ref: `${CORE_API}/${coreLeaguePath(sport)}/events/${id}`, ...ev });
          }
        }
      }
    }

    const scanned = Math.min(i + SB_BATCH, dates.length);
    if (scanned % 30 === 0 || scanned === dates.length) {
      logger.progress("espn", sport, "games", `Scanned ${scanned}/${dates.length} days (${allEvents.length} events)`);
    }
  }

  return allEvents;
}

/** Fetch rosters — requires teams to be fetched first. */
async function importRosters(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  // Load team list
  const teams = await loadTeamList(dataDir, sport, season);
  if (!teams.length) {
    logger.warn(`${sport}/${season} — no teams found; run 'teams' endpoint first`, "espn");
    errors.push(`${sport}/${season}/rosters: no team list available`);
    return { filesWritten, errors };
  }

  logger.progress("espn", sport, "rosters", `Fetching rosters for ${teams.length} teams`);

  // Build list of teams needing roster fetch (skip existing)
  const toFetchRosters: { teamId: string; rosterPath: string; displayName: string }[] = [];
  for (const team of teams) {
    const teamId = team.id;
    if (!teamId) continue;
    const rosterPath = rawPath(dataDir, "espn", sport, season, "rosters", `${teamId}.json`);
    if (fileExists(rosterPath)) {
      filesWritten++;
      continue;
    }
    toFetchRosters.push({ teamId: String(teamId), rosterPath, displayName: team.displayName ?? team.name ?? teamId });
  }

  const ROSTER_BATCH = 8;
  for (let i = 0; i < toFetchRosters.length; i += ROSTER_BATCH) {
    const batch = toFetchRosters.slice(i, i + ROSTER_BATCH);
    const results = await Promise.allSettled(
      batch.map(async ({ teamId, rosterPath, displayName }) => {
        const url = `${SITE_API}/${sportPath}/teams/${teamId}/roster?season=${season}`;
        const data = await espnFetch<any>(url);
        if (data) {
          writeJSON(rosterPath, {
            teamId,
            teamName: displayName,
            season,
            athletes: data.athletes ?? data.roster ?? data,
            fetchedAt: new Date().toISOString(),
          });
          return { ok: true };
        }
        return { ok: false, teamId };
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value.ok) {
        filesWritten++;
      } else if (r.status === "fulfilled" && !r.value.ok) {
        errors.push(`${sport}/${season}/rosters/${r.value.teamId}: fetch failed`);
      }
    }
  }

  logger.progress("espn", sport, "rosters", `Done — ${filesWritten} roster files`);
  return { filesWritten, errors };
}

/** Fetch injury report for the sport. */
async function importInjuries(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  // Scoreboard-only sports (golf, F1) don't have injuries endpoints
  if (SCOREBOARD_ONLY_SPORTS.has(sport)) {
    return { filesWritten: 0, errors: [] };
  }

  // Some sports don't have a top-level injuries endpoint; fall back to per-team.
  const url = `${SITE_API}/${sportPath}/injuries`;
  logger.progress("espn", sport, "injuries", `Fetching ${url}`);

  const data = await espnFetch<any>(url);
  if (data) {
    const outPath = rawPath(dataDir, "espn", sport, season, "injuries.json");
    writeJSON(outPath, { season, injuries: data, fetchedAt: new Date().toISOString() });
    filesWritten++;
  } else {
    // Fallback: per-team injuries (batched 8 concurrent)
    const teams = await loadTeamList(dataDir, sport, season);
    const INJ_BATCH = 8;
    for (let i = 0; i < teams.length; i += INJ_BATCH) {
      const batch = teams.slice(i, i + INJ_BATCH);
      const results = await Promise.allSettled(
        batch.map(async (team) => {
          const teamId = team.id;
          if (!teamId) return false;
          const teamUrl = `${SITE_API}/${sportPath}/teams/${teamId}/injuries`;
          const teamData = await espnFetch<any>(teamUrl);
          if (teamData) {
            const teamPath = rawPath(dataDir, "espn", sport, season, "injuries", `${teamId}.json`);
            writeJSON(teamPath, {
              teamId,
              season,
              injuries: teamData,
              fetchedAt: new Date().toISOString(),
            });
            return true;
          }
          return false;
        }),
      );
      for (const r of results) {
        if (r.status === "fulfilled" && r.value) filesWritten++;
      }
    }

    if (filesWritten === 0) {
      logger.info(`${sport}/${season}/injuries: no injury data available`, "espn");
    }
  }

  logger.progress("espn", sport, "injuries", `Done — ${filesWritten} files`);
  return { filesWritten, errors };
}

/** Fetch latest news for the sport. */
async function importNews(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const url = `${SITE_API}/${sportPath}/news?limit=100`;
  logger.progress("espn", sport, "news", `Fetching ${url}`);

  const data = await espnFetch<any>(url);
  if (!data) {
    logger.info(`${sport}/${season}/news: no data returned`, "espn");
    return { filesWritten, errors };
  }

  const dateStr = new Date().toISOString().split("T")[0];
  const outPath = rawPath(dataDir, "espn", sport, season, "news", `${dateStr}.json`);
  writeJSON(outPath, {
    season,
    date: dateStr,
    count: data.articles?.length ?? 0,
    articles: data.articles ?? [],
    fetchedAt: new Date().toISOString(),
  });
  filesWritten++;

  logger.progress("espn", sport, "news", `Saved ${data.articles?.length ?? 0} articles`);
  return { filesWritten, errors };
}

/**
 * Fetch odds for games in a season.
 * Uses the Core API odds endpoint on individual events.
 * Requires games to be fetched first.
 */
async function importOdds(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  // Load event IDs from the games manifest
  const eventIds = await loadEventIds(dataDir, sport, season);
  if (!eventIds.length) {
    logger.info(`${sport}/${season} — no events in window; skipping odds`, "espn");
    return { filesWritten, errors };
  }

  logger.progress("espn", sport, "odds", `Fetching odds for ${eventIds.length} events`);

  const leaguePath = coreLeaguePath(sport);
  const allOdds: any[] = [];

  // Filter to only events that don't already have cached odds
  const uncachedEvents = eventIds.filter((eventId) => {
    const oddsPath = rawPath(dataDir, "espn", sport, season, "odds", `${eventId}.json`);
    if (fileExists(oddsPath)) {
      filesWritten++;
      return false;
    }
    return true;
  });

  // Batch-fetch odds for uncached events (6 concurrent)
  const ODDS_BATCH = 10;
  for (let i = 0; i < uncachedEvents.length; i += ODDS_BATCH) {
    const batch = uncachedEvents.slice(i, i + ODDS_BATCH);
    const batchResults = await Promise.allSettled(
      batch.map(async (eventId) => {
        const url = `${CORE_API}/${leaguePath}/events/${eventId}/competitions/${eventId}/odds?limit=20`;
        const data = await espnFetch<any>(url);
        if (!data?.items?.length) return null;

        // Resolve $ref items in parallel
        const resolved = await Promise.all(
          data.items.map(async (item: any) => {
            if (item.$ref) {
              const detail = await espnFetch<any>(item.$ref);
              if (detail) {
                detail.eventId = eventId;
                return detail;
              }
              return null;
            }
            item.eventId = eventId;
            return item;
          }),
        );
        const validResolved = resolved.filter(Boolean);

        if (validResolved.length) {
          const oddsPath = rawPath(dataDir, "espn", sport, season, "odds", `${eventId}.json`);
          writeJSON(oddsPath, {
            eventId,
            season,
            odds: validResolved,
            fetchedAt: new Date().toISOString(),
          });
          return validResolved;
        }
        return null;
      }),
    );

    for (const result of batchResults) {
      if (result.status === "fulfilled" && result.value) {
        filesWritten++;
        allOdds.push(...result.value);
      }
    }
  }

  // Save combined file
  if (allOdds.length) {
    const allPath = rawPath(dataDir, "espn", sport, season, "odds", "all_odds.json");
    writeJSON(allPath, {
      season,
      count: allOdds.length,
      odds: allOdds,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
  }

  logger.progress("espn", sport, "odds", `Done — ${filesWritten} odds files`);
  return { filesWritten, errors };
}

/**
 * Fetch player data from team rosters + athlete detail endpoints.
 * Requires teams to be fetched first.
 */
async function importPlayers(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  const leaguePath = coreLeaguePath(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const teams = await loadTeamList(dataDir, sport, season);
  if (!teams.length) {
    logger.warn(`${sport}/${season} — no teams found; run 'teams' endpoint first`, "espn");
    errors.push(`${sport}/${season}/players: no team list available`);
    return { filesWritten, errors };
  }

  logger.progress("espn", sport, "players", `Fetching player lists for ${teams.length} teams`);

  const allPlayers: any[] = [];

  // Batch per-team athlete list fetches (8 concurrent)
  const PLAYER_TEAM_BATCH = 8;
  for (let ti = 0; ti < teams.length; ti += PLAYER_TEAM_BATCH) {
    const teamBatch = teams.slice(ti, ti + PLAYER_TEAM_BATCH);
    const teamResults = await Promise.allSettled(
      teamBatch.map(async (team) => {
        const teamId = team.id;
        if (!teamId) return { teamId: null, items: [] };
        const url = `${CORE_API}/${leaguePath}/seasons/${season}/teams/${teamId}/athletes?limit=100`;
        const data = await espnFetch<any>(url);
        return { teamId: String(teamId), items: data?.items ?? [] };
      }),
    );

    // Process results: resolve $refs and save individual player files
    for (const tr of teamResults) {
      if (tr.status !== "fulfilled" || !tr.value.teamId) continue;
      const { teamId, items } = tr.value;

      for (const item of items) {
        let athlete: any = item;
        if (item.$ref && !item.id) {
          const resolved = await espnFetch<any>(item.$ref);
          if (resolved) athlete = resolved;
        }

        const playerId = athlete.id ?? extractIdFromRef(item.$ref);
        if (!playerId) continue;

        const playerPath = rawPath(dataDir, "espn", sport, season, "players", `${playerId}.json`);
        if (fileExists(playerPath)) {
          filesWritten++;
          continue;
        }

        const statsUrl = `${CORE_API}/${leaguePath}/seasons/${season}/athletes/${playerId}/statistics`;
        const stats = await espnFetch<any>(statsUrl);

        writeJSON(playerPath, {
          playerId,
          teamId,
          season,
          athlete,
          statistics: stats,
          fetchedAt: new Date().toISOString(),
        });
        filesWritten++;
        allPlayers.push({ id: playerId, teamId, name: athlete.displayName ?? athlete.fullName });
      }
    }
  }

  // Save combined player index
  if (allPlayers.length) {
    const indexPath = rawPath(dataDir, "espn", sport, season, "players", "all_players.json");
    writeJSON(indexPath, {
      season,
      count: allPlayers.length,
      players: allPlayers,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
  }

  logger.progress("espn", sport, "players", `Done — ${filesWritten} player files`);
  return { filesWritten, errors };
}

/**
 * Fetch the full athlete directory via Core API pagination.
 * Unlike `players` (which iterates per-team), this fetches the league-wide
 * athlete list and resolves each $ref for full athlete details.
 */
async function importAthletes(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const leaguePath = coreLeaguePath(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  // ── Step 1: Paginate the athletes list to collect all $refs ──
  const PAGE_SIZE = 1000;
  let page = 1;
  let totalCount = 0;
  const allRefs: Array<{ id: string; $ref: string }> = [];

  logger.progress("espn", sport, "athletes", "Fetching athlete directory…");

  // Fetch page 1 to get total count, then remaining pages concurrently
  const page1Url = `${CORE_API}/${leaguePath}/athletes?limit=${PAGE_SIZE}&page=1`;
  const page1 = await espnFetch<any>(page1Url);
  if (page1?.items?.length) {
    totalCount = page1.count ?? 0;
    logger.progress("espn", sport, "athletes", `${totalCount} athletes in directory`);

    for (const item of page1.items) {
      const ref = item.$ref ?? "";
      const id = extractIdFromRef(ref);
      if (id) allRefs.push({ id, $ref: ref });
    }

    if (page1.items.length >= PAGE_SIZE && totalCount > PAGE_SIZE) {
      const totalPages = Math.ceil(totalCount / PAGE_SIZE);
      const remainingPages = Array.from({ length: totalPages - 1 }, (_, i) => i + 2);
      const pageResults = await Promise.allSettled(
        remainingPages.map(async (p) => {
          const data = await espnFetch<any>(`${CORE_API}/${leaguePath}/athletes?limit=${PAGE_SIZE}&page=${p}`);
          return data?.items ?? [];
        }),
      );
      for (const pr of pageResults) {
        if (pr.status === "fulfilled") {
          for (const item of pr.value) {
            const ref = item.$ref ?? "";
            const id = extractIdFromRef(ref);
            if (id) allRefs.push({ id, $ref: ref });
          }
        }
      }
    }
  }

  if (!allRefs.length) {
    errors.push(`${sport}/${season}/athletes: no athlete data returned`);
    return { filesWritten, errors };
  }

  // ── Step 2: Save manifest ─────────────────────────────────
  const manifestPath = rawPath(dataDir, "espn", sport, season, "athletes", "all_athletes.json");
  writeJSON(manifestPath, {
    season,
    count: allRefs.length,
    athletes: allRefs.map((r) => ({ id: r.id })),
    fetchedAt: new Date().toISOString(),
  });
  filesWritten++;

  // ── Step 3: Resolve each athlete's full details (batched) ──
  // Build list of athletes needing fetch (skip existing)
  const toResolve: { id: string; $ref: string; athletePath: string }[] = [];
  for (const ref of allRefs) {
    const athletePath = rawPath(dataDir, "espn", sport, season, "athletes", `${ref.id}.json`);
    if (fileExists(athletePath)) {
      filesWritten++;
    } else {
      toResolve.push({ id: ref.id, $ref: ref.$ref, athletePath });
    }
  }

  logger.progress("espn", sport, "athletes", `${allRefs.length - toResolve.length} cached, ${toResolve.length} to fetch`);

  const ATH_BATCH = 10;
  let resolved = 0;
  for (let i = 0; i < toResolve.length; i += ATH_BATCH) {
    const batch = toResolve.slice(i, i + ATH_BATCH);
    const results = await Promise.allSettled(
      batch.map(async ({ id, $ref, athletePath }) => {
        const detail = await espnFetch<any>($ref.replace("http://", "https://"));
        if (detail) {
          writeJSON(athletePath, {
            athleteId: id,
            season,
            athlete: detail,
            fetchedAt: new Date().toISOString(),
          });
          return true;
        }
        return false;
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value) filesWritten++;
    }
    resolved += batch.length;
    if (resolved % 100 === 0) {
      logger.progress("espn", sport, "athletes", `${resolved}/${toResolve.length} athletes resolved`);
    }
  }

  logger.progress("espn", sport, "athletes", `Done — ${filesWritten} files (${allRefs.length} athletes)`);
  return { filesWritten, errors };
}

// ── New endpoint implementations ────────────────────────────

/** Fetch daily scoreboards for every day of the season. */
async function importScoreboard(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const dates = filterRecentDates(seasonDateRange(sport, season), ctx.recentDays);
  logger.progress("espn", sport, "scoreboard", `Fetching scoreboards for ${dates.length} days`);

  const today = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const { default: fs } = await import("node:fs");

  // Separate dates into cached (skip) and to-fetch
  const toFetch: string[] = [];
  for (const date of dates) {
    const outPath = rawPath(dataDir, "espn", sport, season, "scoreboard", `${date}.json`);
    if (!fileExists(outPath)) {
      toFetch.push(date);
      continue;
    }
    // Future dates with files → skip (can't change yet)
    if (date > today) {
      filesWritten++;
      continue;
    }
    // Past dates: use file age to avoid re-parsing old finalized games.
    // If file is older than 48 hours, assume all games are final.
    try {
      const mtime = fs.statSync(outPath).mtimeMs;
      if (Date.now() - mtime > 48 * 60 * 60 * 1000) {
        filesWritten++;
        continue;
      }
    } catch { /* stat failed, re-fetch */ }

    // Recent past file — check if all events are final
    const existing = readJSON<any>(outPath);
    const events = existing?.events ?? [];
    const allFinal = events.length === 0 || events.every((e: any) => {
      const st = e.status?.type?.name ?? "";
      return st === "STATUS_FINAL" || st === "STATUS_FULL_TIME" || st === "STATUS_POSTPONED" || st === "STATUS_CANCELED";
    });
    if (allFinal) {
      filesWritten++;
      continue;
    }
    // Has non-final events for a recent past date — re-fetch
    toFetch.push(date);
  }

  // Batch-fetch scoreboards (8 concurrent)
  const SB_BATCH = 8;
  for (let i = 0; i < toFetch.length; i += SB_BATCH) {
    const batch = toFetch.slice(i, i + SB_BATCH);
    const results = await Promise.allSettled(
      batch.map(async (date) => {
        const url = `${SITE_API}/${sportPath}/scoreboard?dates=${date}&limit=100`;
        const data = await espnFetch<any>(url);
        if (data) {
          const outPath = rawPath(dataDir, "espn", sport, season, "scoreboard", `${date}.json`);
          writeJSON(outPath, {
            season,
            date,
            events: data.events ?? [],
            count: data.events?.length ?? 0,
            fetchedAt: new Date().toISOString(),
          });
          return true;
        }
        return false;
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value) filesWritten++;
    }
    if (i > 0 && i % (SB_BATCH * 4) === 0) {
      logger.progress("espn", sport, "scoreboard", `${i}/${toFetch.length} dates fetched`);
    }
  }

  logger.progress("espn", sport, "scoreboard", `Done — ${filesWritten} scoreboard files (${toFetch.length} fetched)`);
  return { filesWritten, errors };
}

/** Fetch team-level season statistics. */
async function importTeamStats(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const teams = await loadTeamList(dataDir, sport, season);
  if (!teams.length) {
    logger.warn(`${sport}/${season} — no teams found; run 'teams' endpoint first`, "espn");
    errors.push(`${sport}/${season}/team_stats: no team list available`);
    return { filesWritten, errors };
  }

  logger.progress("espn", sport, "team_stats", `Fetching stats for ${teams.length} teams`);

  const toFetchStats: { teamId: string; outPath: string }[] = [];
  for (const team of teams) {
    const teamId = team.id;
    if (!teamId) continue;
    const outPath = rawPath(dataDir, "espn", sport, season, "team_stats", `${teamId}.json`);
    if (fileExists(outPath)) { filesWritten++; continue; }
    toFetchStats.push({ teamId: String(teamId), outPath });
  }

  const TS_BATCH = 8;
  for (let i = 0; i < toFetchStats.length; i += TS_BATCH) {
    const batch = toFetchStats.slice(i, i + TS_BATCH);
    const results = await Promise.allSettled(
      batch.map(async ({ teamId, outPath }) => {
        const url = `${SITE_API}/${sportPath}/teams/${teamId}/statistics?season=${season}`;
        const data = await espnFetch<any>(url);
        if (data) {
          writeJSON(outPath, { teamId, season, statistics: data, fetchedAt: new Date().toISOString() });
          return { ok: true };
        }
        return { ok: false, teamId };
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value.ok) filesWritten++;
      else if (r.status === "fulfilled" && !r.value.ok) errors.push(`${sport}/${season}/team_stats/${r.value.teamId}: fetch failed`);
    }
  }

  logger.progress("espn", sport, "team_stats", `Done — ${filesWritten} team stat files`);
  return { filesWritten, errors };
}

/** Fetch per-team schedule/results for the season. */
async function importTeamSchedule(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const teams = await loadTeamList(dataDir, sport, season);
  if (!teams.length) {
    logger.warn(`${sport}/${season} — no teams found; run 'teams' endpoint first`, "espn");
    errors.push(`${sport}/${season}/team_schedule: no team list available`);
    return { filesWritten, errors };
  }

  logger.progress("espn", sport, "team_schedule", `Fetching schedules for ${teams.length} teams`);

  const toFetchSched: { teamId: string; outPath: string }[] = [];
  for (const team of teams) {
    const teamId = team.id;
    if (!teamId) continue;
    const outPath = rawPath(dataDir, "espn", sport, season, "team_schedule", `${teamId}.json`);
    if (fileExists(outPath)) { filesWritten++; continue; }
    toFetchSched.push({ teamId: String(teamId), outPath });
  }

  const yr = seasonParam(sport, season);
  const SCHED_BATCH = 8;
  for (let i = 0; i < toFetchSched.length; i += SCHED_BATCH) {
    const batch = toFetchSched.slice(i, i + SCHED_BATCH);
    const results = await Promise.allSettled(
      batch.map(async ({ teamId, outPath }) => {
        const url = `${SITE_API}/${sportPath}/teams/${teamId}/schedule?season=${yr}`;
        const data = await espnFetch<any>(url);
        if (data) {
          writeJSON(outPath, { teamId, season, schedule: data, fetchedAt: new Date().toISOString() });
          return { ok: true };
        }
        return { ok: false, teamId };
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value.ok) filesWritten++;
      else if (r.status === "fulfilled" && !r.value.ok) errors.push(`${sport}/${season}/team_schedule/${r.value.teamId}: fetch failed`);
    }
  }

  logger.progress("espn", sport, "team_schedule", `Done — ${filesWritten} schedule files`);
  return { filesWritten, errors };
}

/** Fetch detailed player statistics using the Site API v2 athletes endpoint. */
async function importPlayerStats(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const { sportName, leagueName } = splitSlug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const playerIds = await loadRosterPlayerIds(dataDir, sport, season);
  if (!playerIds.length) {
    logger.warn(`${sport}/${season} — no roster players found; skipping player_stats`, "espn");
    return { filesWritten, errors: [] };
  }

  // Separate cached from to-fetch
  const toFetch: string[] = [];
  for (const pid of playerIds) {
    const outPath = rawPath(dataDir, "espn", sport, season, "player_stats", `${pid}.json`);
    if (fileExists(outPath)) {
      filesWritten++;
    } else {
      toFetch.push(pid);
    }
  }

  logger.progress("espn", sport, "player_stats", `${filesWritten} cached, ${toFetch.length} to fetch`);

  // Batch-fetch player stats (12 concurrent)
  const PS_BATCH = 12;
  let fetched = 0;
  for (let i = 0; i < toFetch.length; i += PS_BATCH) {
    const batch = toFetch.slice(i, i + PS_BATCH);
    const results = await Promise.allSettled(
      batch.map(async (playerId) => {
        const url = `https://site.api.espn.com/apis/v2/sports/${sportName}/${leagueName}/athletes/${playerId}/statistics?season=${season}`;
        const data = await espnFetch<any>(url);
        if (data) {
          const outPath = rawPath(dataDir, "espn", sport, season, "player_stats", `${playerId}.json`);
          writeJSON(outPath, {
            playerId,
            season,
            statistics: data,
            fetchedAt: new Date().toISOString(),
          });
          return true;
        }
        return false;
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value) filesWritten++;
    }
    fetched += batch.length;
    if (fetched % 60 === 0 || fetched === toFetch.length) {
      logger.progress("espn", sport, "player_stats", `${fetched}/${toFetch.length} fetched`);
    }
  }

  logger.progress("espn", sport, "player_stats", `Done — ${filesWritten} player stat files`);
  return { filesWritten, errors };
}

/** Fetch depth charts for each team (NFL, NBA, MLB, etc.). */
async function importDepthCharts(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const teams = await loadTeamList(dataDir, sport, season);
  if (!teams.length) {
    logger.warn(`${sport}/${season} — no teams found; run 'teams' endpoint first`, "espn");
    errors.push(`${sport}/${season}/depth_charts: no team list available`);
    return { filesWritten, errors };
  }

  logger.progress("espn", sport, "depth_charts", `Fetching depth charts for ${teams.length} teams`);

  const toFetchDC: { teamId: string; outPath: string }[] = [];
  for (const team of teams) {
    const teamId = team.id;
    if (!teamId) continue;
    const outPath = rawPath(dataDir, "espn", sport, season, "depth_charts", `${teamId}.json`);
    if (fileExists(outPath)) { filesWritten++; continue; }
    toFetchDC.push({ teamId: String(teamId), outPath });
  }

  const DC_BATCH = 8;
  for (let i = 0; i < toFetchDC.length; i += DC_BATCH) {
    const batch = toFetchDC.slice(i, i + DC_BATCH);
    const results = await Promise.allSettled(
      batch.map(async ({ teamId, outPath }) => {
        const url = `${SITE_API}/${sportPath}/teams/${teamId}/depthcharts`;
        const data = await espnFetch<any>(url);
        if (data) {
          writeJSON(outPath, { teamId, season, depthCharts: data, fetchedAt: new Date().toISOString() });
          return true;
        }
        return false;
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value) filesWritten++;
    }
    // Depth charts aren't available for all sports — skip silently on 404
  }

  logger.progress("espn", sport, "depth_charts", `Done — ${filesWritten} depth chart files`);
  return { filesWritten, errors };
}

/** Fetch daily transactions for the season. */
async function importTransactions(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const dates = filterRecentDates(seasonDateRange(sport, season), ctx.recentDays);
  logger.progress("espn", sport, "transactions", `Fetching transactions for ${dates.length} days`);

  // Separate cached from to-fetch
  const toFetchDates: string[] = [];
  for (const date of dates) {
    const outPath = rawPath(dataDir, "espn", sport, season, "transactions", `${date}.json`);
    if (fileExists(outPath)) { filesWritten++; } else { toFetchDates.push(date); }
  }

  const TX_BATCH = 10;
  for (let i = 0; i < toFetchDates.length; i += TX_BATCH) {
    const batch = toFetchDates.slice(i, i + TX_BATCH);
    const results = await Promise.allSettled(
      batch.map(async (date) => {
        const url = `${SITE_API}/${sportPath}/transactions?dates=${date}`;
        const data = await espnFetch<any>(url);
        if (data) {
          const items = data.items ?? data.transactions ?? [];
          if (items.length > 0 || data.count > 0) {
            const outPath = rawPath(dataDir, "espn", sport, season, "transactions", `${date}.json`);
            writeJSON(outPath, {
              season, date, transactions: items, count: items.length,
              fetchedAt: new Date().toISOString(),
            });
            return true;
          }
        }
        return false;
      }),
    );
    for (const r of results) {
      if (r.status === "fulfilled" && r.value) filesWritten++;
    }
  }

  logger.progress("espn", sport, "transactions", `Done — ${filesWritten} transaction files`);
  return { filesWritten, errors };
}

/** Fetch power rankings / FPI / BPI for the sport. */
async function importRankings(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const outPath = rawPath(dataDir, "espn", sport, season, "rankings.json");
  if (fileExists(outPath)) {
    logger.progress("espn", sport, "rankings", "Already exists — skipping");
    return { filesWritten: 1, errors: [] };
  }

  // Try the rankings endpoint first, then fall back to powerindex
  const rankingsUrl = `${SITE_API}/${sportPath}/rankings?season=${season}`;
  logger.progress("espn", sport, "rankings", `Fetching ${rankingsUrl}`);

  let data = await espnFetch<any>(rankingsUrl);
  if (!data) {
    const powerUrl = `${SITE_API}/${sportPath}/powerindex?season=${season}`;
    logger.progress("espn", sport, "rankings", `Rankings 404 — trying ${powerUrl}`);
    data = await espnFetch<any>(powerUrl);
  }

  if (data) {
    writeJSON(outPath, {
      season,
      rankings: data,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
    logger.progress("espn", sport, "rankings", "Saved rankings");
  } else {
    errors.push(`${sport}/${season}/rankings: no ranking data available`);
    logger.progress("espn", sport, "rankings", "No ranking data found");
  }

  return { filesWritten, errors };
}

/** Fetch futures/projections for the sport. */
async function importFutures(ctx: EndpointContext): Promise<EndpointResult> {
  const { sport, season, dataDir, dryRun } = ctx;
  const sportPath = slug(sport);
  let filesWritten = 0;
  const errors: string[] = [];

  if (dryRun) return { filesWritten: 0, errors: [] };

  const outPath = rawPath(dataDir, "espn", sport, season, "futures.json");
  if (fileExists(outPath)) {
    logger.progress("espn", sport, "futures", "Already exists — skipping");
    return { filesWritten: 1, errors: [] };
  }

  const url = `${SITE_API}/${sportPath}/futures?season=${season}`;
  logger.progress("espn", sport, "futures", `Fetching ${url}`);

  const data = await espnFetch<any>(url);
  if (data) {
    writeJSON(outPath, {
      season,
      futures: data,
      fetchedAt: new Date().toISOString(),
    });
    filesWritten++;
    logger.progress("espn", sport, "futures", "Saved futures");
  } else {
    errors.push(`${sport}/${season}/futures: no futures data available`);
    logger.progress("espn", sport, "futures", "No futures data found");
  }

  return { filesWritten, errors };
}

// ── Shared data-loading helpers ─────────────────────────────

// In-memory caches for I/O helpers — avoid repeated disk reads within a run.
// Keyed by "sport:season" since these don't change during an import.
const _teamListCache = new Map<string, Array<{ id: string; displayName?: string; name?: string }>>();
const _eventIdCache = new Map<string, string[]>();

/** Load the team list from previously saved teams.json (cached per sport/season). */
async function loadTeamList(
  dataDir: string,
  sport: Sport,
  season: number,
): Promise<Array<{ id: string; displayName?: string; name?: string }>> {
  const cacheKey = `${sport}:${season}`;
  const cached = _teamListCache.get(cacheKey);
  if (cached !== undefined) return cached;

  const teamsPath = rawPath(dataDir, "espn", sport, season, "teams.json");
  if (!fileExists(teamsPath)) {
    _teamListCache.set(cacheKey, []);
    return [];
  }

  try {
    const data = readJSON<any>(teamsPath);
    const teams = (data?.teams ?? []).map((t: any) => t.team ?? t);
    _teamListCache.set(cacheKey, teams);
    return teams;
  } catch {
    _teamListCache.set(cacheKey, []);
    return [];
  }
}

/** Invalidate team/event caches after Phase 1 writes new data. */
function invalidateLoadCaches(): void {
  _teamListCache.clear();
  _eventIdCache.clear();
}

/** Load event IDs from the games manifest (cached per sport/season). */
async function loadEventIds(
  dataDir: string,
  sport: Sport,
  season: number,
): Promise<string[]> {
  const cacheKey = `${sport}:${season}`;
  const cached = _eventIdCache.get(cacheKey);
  if (cached !== undefined) return cached;

  const manifestPath = rawPath(dataDir, "espn", sport, season, "games", "all_games.json");
  if (!fileExists(manifestPath)) {
    _eventIdCache.set(cacheKey, []);
    return [];
  }

  try {
    const data = readJSON<any>(manifestPath);
    const games: any[] = data?.games ?? [];
    const ids = games
      .map((g: any) => extractEventId(g))
      .filter((id): id is string => id !== null);
    _eventIdCache.set(cacheKey, ids);
    return ids;
  } catch {
    _eventIdCache.set(cacheKey, []);
    return [];
  }
}

/** Load player IDs from previously saved roster files. */
async function loadRosterPlayerIds(
  dataDir: string,
  sport: Sport,
  season: number,
): Promise<string[]> {
  const teams = await loadTeamList(dataDir, sport, season);
  if (!teams.length) return [];

  const playerIds = new Set<string>();

  try {
    for (const team of teams) {
      const teamId = team.id;
      if (!teamId) continue;

      const rosterPath = rawPath(dataDir, "espn", sport, season, "rosters", `${teamId}.json`);
      if (!fileExists(rosterPath)) continue;

      const data = readJSON<any>(rosterPath);
      const groups: any[] = data?.athletes ?? [];

      for (const group of groups) {
        // Roster files may have a nested items array per position group
        const athletes: any[] = group?.items ?? group?.athletes ?? (Array.isArray(group) ? group : []);
        for (const athlete of athletes) {
          const id = athlete?.id ?? athlete?.athlete?.id;
          if (id) playerIds.add(String(id));
        }
      }
    }
  } catch {
    return [];
  }

  return Array.from(playerIds);
}

/** Extract numeric ID from a Core API $ref URL. */
function extractIdFromRef(ref: string | undefined): string | null {
  if (!ref) return null;
  const parts = ref.split("/");
  const last = parts.pop()?.split("?")[0];
  return last && /^\d+$/.test(last) ? last : null;
}

// ── Endpoint dispatch map ───────────────────────────────────

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointContext) => Promise<EndpointResult>> = {
  teams:         importTeams,
  standings:     importStandings,
  games:         importGames,
  rosters:       importRosters,
  injuries:      importInjuries,
  news:          importNews,
  odds:          importOdds,
  players:       importPlayers,
  athletes:      importAthletes,
  scoreboard:    importScoreboard,
  team_stats:    importTeamStats,
  team_schedule: importTeamSchedule,
  player_stats:  importPlayerStats,
  depth_charts:  importDepthCharts,
  transactions:  importTransactions,
  rankings:      importRankings,
  futures:       importFutures,
};

// ── Provider implementation ─────────────────────────────────

const espn: Provider = {
  name: "espn",
  label: "ESPN",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ALL_ENDPOINTS as unknown as readonly string[],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    // Determine which sports and endpoints to run
    const sports = opts.sports.length
      ? opts.sports.filter((s) => SPORT_SLUGS[s])
      : SUPPORTED_SPORTS;

    const endpoints: Endpoint[] = opts.endpoints.length
      ? (opts.endpoints.filter((e) => ALL_ENDPOINTS.includes(e as Endpoint)) as Endpoint[])
      : [...ALL_ENDPOINTS];

    logger.info(
      `Starting ESPN import — ${sports.length} sports, ${endpoints.length} endpoints, ${opts.seasons.length} seasons`,
      "espn",
    );

    // Group endpoints by dependency phase for maximum concurrency:
    // Phase 1 (concurrent): standalone endpoints + games/scoreboard
    // Phase 2 (concurrent): endpoints that depend on games (odds)
    // Phase 3 (concurrent): endpoints that depend on teams (rosters, players, etc.)
    const PHASE1_EPS = new Set(["standings", "injuries", "news", "rankings", "futures", "transactions", "scoreboard", "games", "teams"]);
    const GAMES_DEPENDENT = new Set(["odds", "player_stats"]);
    // Everything else depends on teams or games (rosters, players, athletes, team_stats, team_schedule, depth_charts)

    const phase1Eps: Endpoint[] = [];
    const phase2Eps: Endpoint[] = [];
    const phase3Eps: Endpoint[] = [];
    for (const ep of endpoints) {
      if (PHASE1_EPS.has(ep)) {
        phase1Eps.push(ep);
      } else if (GAMES_DEPENDENT.has(ep)) {
        phase2Eps.push(ep);
      } else {
        phase3Eps.push(ep);
      }
    }

    // Process all sports concurrently within each phase.
    // Phase dependencies are per-sport (e.g., NBA odds depend on NBA games),
    // so we run all sports' Phase 1 together, then all Phase 2, then Phase 3.
    const sportSeasonPairs: { sport: string; season: number; ctx: EndpointContext }[] = [];
    for (const sport of sports) {
      for (const season of opts.seasons) {
        const seasonLabel = SOCCER_SPORTS.has(sport) ? `${season}-${String(season + 1).slice(2)}` : String(season);
        logger.info(`── ${sport.toUpperCase()} ${seasonLabel} ──`, "espn");
        sportSeasonPairs.push({
          sport,
          season,
          ctx: {
            sport,
            season,
            dataDir: opts.dataDir,
            dryRun: opts.dryRun,
            recentDays: opts.recentDays,
          },
        });
      }
    }

    const runPhase = async (eps: Endpoint[]) => {
      if (eps.length === 0) return;
      const tasks = sportSeasonPairs.flatMap(({ sport, ctx }) =>
        eps.map(async (ep) => {
          const fn = ENDPOINT_FNS[ep];
          if (!fn) return;
          try {
            const result = await fn(ctx);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error(`${sport}/${ctx.season}/${ep}: ${msg}`, "espn");
            allErrors.push(`${sport}/${ctx.season}/${ep}: ${msg}`);
          }
        }),
      );
      await Promise.allSettled(tasks);
    };

    // Phase 1: All sports' independent endpoints concurrently
    await runPhase(phase1Eps);
    // Invalidate I/O caches — Phase 1 wrote new teams/games data
    invalidateLoadCaches();
    // Phase 2: All sports' game-dependent endpoints concurrently
    await runPhase(phase2Eps);
    // Phase 3: All sports' team-dependent endpoints concurrently
    await runPhase(phase3Eps);

    const durationMs = Date.now() - start;
    logger.summary("espn", totalFiles, allErrors.length, durationMs);

    return {
      provider: "espn",
      sport: sports.length === 1 ? sports[0] : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs,
    };
  },
};

export default espn;
