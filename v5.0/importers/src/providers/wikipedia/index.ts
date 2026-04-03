// ──────────────────────────────────────────────────────────
// Wikipedia Provider
// ──────────────────────────────────────────────────────────
// Fetches team and league profile summaries from Wikipedia's
// REST API. Provides rich descriptive context for the frontend:
// team history, founding year, stadium info, notable records.
// No API key required.

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "wikipedia";
const WIKI_API = "https://en.wikipedia.org/api/rest_v1/page/summary";
const WIKI_SEARCH = "https://en.wikipedia.org/w/api.php";
const RATE_LIMIT: RateLimitConfig = { requests: 10, perMs: 1_000 };

/** Known Wikipedia page titles for major leagues and teams */
const LEAGUE_PAGES: Partial<Record<Sport, string>> = {
  nba: "National Basketball Association",
  nfl: "National Football League",
  mlb: "Major League Baseball",
  nhl: "National Hockey League",
  ncaab: "NCAA Division I men's basketball tournament",
  ncaaf: "NCAA Division I FBS football",
  wnba: "Women's National Basketball Association",
  epl: "Premier League",
  laliga: "La Liga",
  bundesliga: "Bundesliga",
  seriea: "Serie A",
  ligue1: "Ligue 1",
  mls: "Major League Soccer",
  ucl: "UEFA Champions League",
  f1: "Formula One",
  indycar: "IndyCar Series",
  ufc: "Ultimate Fighting Championship",
  atp: "ATP Tour",
  wta: "WTA Tour",
  golf: "PGA Tour",
  lpga: "LPGA",
};

/** Well-known team Wikipedia slugs by sport */
const TEAM_PAGES: Partial<Record<Sport, string[]>> = {
  nba: [
    "Boston Celtics", "Los Angeles Lakers", "Golden State Warriors",
    "Miami Heat", "New York Knicks", "Chicago Bulls", "San Antonio Spurs",
    "Dallas Mavericks", "Denver Nuggets", "Milwaukee Bucks",
    "Phoenix Suns", "Cleveland Cavaliers", "Oklahoma City Thunder",
    "Memphis Grizzlies", "New Orleans Pelicans",
  ],
  nfl: [
    "New England Patriots", "Dallas Cowboys", "San Francisco 49ers",
    "Green Bay Packers", "Kansas City Chiefs", "Pittsburgh Steelers",
    "Denver Broncos", "Seattle Seahawks", "Philadelphia Eagles",
    "Baltimore Ravens", "Buffalo Bills", "Miami Dolphins",
    "Los Angeles Rams", "Tampa Bay Buccaneers", "Cincinnati Bengals",
  ],
  mlb: [
    "New York Yankees", "Los Angeles Dodgers", "Boston Red Sox",
    "Chicago Cubs", "San Francisco Giants", "St. Louis Cardinals",
    "Houston Astros", "Atlanta Braves", "New York Mets",
    "Philadelphia Phillies", "San Diego Padres", "Texas Rangers",
  ],
  nhl: [
    "Toronto Maple Leafs", "Montreal Canadiens", "Boston Bruins",
    "New York Rangers", "Chicago Blackhawks", "Detroit Red Wings",
    "Edmonton Oilers", "Vegas Golden Knights", "Colorado Avalanche",
    "Tampa Bay Lightning", "Florida Panthers",
  ],
  epl: [
    "Arsenal F.C.", "Liverpool F.C.", "Manchester City F.C.",
    "Manchester United F.C.", "Chelsea F.C.", "Tottenham Hotspur F.C.",
    "Aston Villa F.C.", "Newcastle United F.C.", "West Ham United F.C.",
  ],
};

const ALL_SUPPORTED: Sport[] = [...new Set([
  ...(Object.keys(LEAGUE_PAGES) as Sport[]),
  ...(Object.keys(TEAM_PAGES) as Sport[]),
])];

const ENDPOINTS = ["leagues", "teams"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

async function fetchPageSummary(title: string): Promise<any | null> {
  const encoded = encodeURIComponent(title.replace(/ /g, "_"));
  try {
    return await fetchJSON<any>(`${WIKI_API}/${encoded}`, NAME, RATE_LIMIT, {
      headers: { "User-Agent": "sports-data-importer/5.0 (open-source research)" },
    });
  } catch {
    return null;
  }
}

function normalizePageSummary(data: any, title: string): Record<string, unknown> {
  return {
    title: data?.title ?? title,
    display_title: data?.displaytitle ?? title,
    description: data?.description ?? null,
    extract: data?.extract ?? null,
    extract_html: data?.extract_html ?? null,
    thumbnail_url: data?.thumbnail?.source ?? null,
    thumbnail_width: data?.thumbnail?.width ?? null,
    thumbnail_height: data?.thumbnail?.height ?? null,
    wiki_url: data?.content_urls?.desktop?.page ?? null,
    lang: data?.lang ?? "en",
    dir: data?.dir ?? "ltr",
    fetched_at: new Date().toISOString(),
  };
}

async function importLeague(ctx: EndpointCtx): Promise<EndpointResult> {
  const pageTitle = LEAGUE_PAGES[ctx.sport];
  if (!pageTitle || ctx.dryRun) return { filesWritten: 0, errors: [] };

  const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "league.json");
  if (fileExists(outPath)) return { filesWritten: 0, errors: [] };

  const data = await fetchPageSummary(pageTitle);
  if (!data) return { filesWritten: 0, errors: [`No Wikipedia data for "${pageTitle}"`] };

  writeJSON(outPath, {
    source: NAME,
    sport: ctx.sport,
    season: String(ctx.season),
    page_title: pageTitle,
    ...normalizePageSummary(data, pageTitle),
  });
  logger.progress(NAME, ctx.sport, "leagues", `Saved "${pageTitle}"`);
  return { filesWritten: 1, errors: [] };
}

async function importTeams(ctx: EndpointCtx): Promise<EndpointResult> {
  const titles = TEAM_PAGES[ctx.sport] ?? [];
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const errors: string[] = [];
  let filesWritten = 0;

  for (const title of titles) {
    const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, "_");
    const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "teams", `${slug}.json`);
    if (fileExists(outPath)) continue;

    try {
      const data = await fetchPageSummary(title);
      if (!data) { errors.push(`No data: "${title}"`); continue; }
      writeJSON(outPath, {
        source: NAME,
        sport: ctx.sport,
        season: String(ctx.season),
        page_title: title,
        ...normalizePageSummary(data, title),
      });
      filesWritten++;
    } catch (err) {
      errors.push(`teams/${title}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  logger.progress(NAME, ctx.sport, "teams", `${filesWritten}/${titles.length} team pages saved`);
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  leagues: importLeague,
  teams: importTeams,
};

const wikipedia: Provider = {
  name: NAME,
  label: "Wikipedia",
  sports: ALL_SUPPORTED,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => ALL_SUPPORTED.includes(s))
      : [...ALL_SUPPORTED];
    const activeEndpoints = (opts.endpoints.length
      ? opts.endpoints.filter((e) => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS]) as Endpoint[];

    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        for (const ep of activeEndpoints) {
          try {
            const r = await ENDPOINT_FNS[ep]({ sport, season, dataDir: opts.dataDir, dryRun: opts.dryRun });
            totalFiles += r.filesWritten;
            allErrors.push(...r.errors);
          } catch (err) {
            allErrors.push(`${ep}/${sport}/${season}: ${err instanceof Error ? err.message : String(err)}`);
          }
        }
      }
    }

    return { provider: NAME, sport: activeSports.length === 1 ? activeSports[0]! : "multi", filesWritten: totalFiles, errors: allErrors, durationMs: Date.now() - start };
  },
};

export default wikipedia;
