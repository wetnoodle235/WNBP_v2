// ──────────────────────────────────────────────────────────
// Reddit Provider
// ──────────────────────────────────────────────────────────
// Fetches recent posts from sports subreddits for news and
// community sentiment analysis. Uses Reddit's public JSON API
// — no OAuth or API key required for read-only access.

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "reddit";
const BASE_URL = "https://www.reddit.com/r";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 2_000 };

/** Primary subreddits per sport — ordered by relevance */
const SUBREDDITS: Partial<Record<Sport, string[]>> = {
  nba: ["nba", "nbadiscussion"],
  nfl: ["nfl", "fantasyfootball"],
  mlb: ["baseball", "mlb"],
  nhl: ["hockey"],
  ncaab: ["CollegeBasketball"],
  ncaaw: ["womenscollbasketball"],
  ncaaf: ["CFB"],
  wnba: ["wnba"],
  epl: ["PremierLeague"],
  laliga: ["LaLiga"],
  bundesliga: ["bundesliga"],
  seriea: ["SerieA"],
  ligue1: ["Ligue1"],
  mls: ["MLS"],
  ucl: ["championsleague"],
  nwsl: ["NWSL"],
  f1: ["formula1"],
  indycar: ["INDYCAR"],
  ufc: ["ufc", "MMA"],
  atp: ["tennis"],
  wta: ["tennis"],
  golf: ["golf"],
  lpga: ["golf"],
  lol: ["leagueoflegends", "lolesports"],
  csgo: ["GlobalOffensive"],
  valorant: ["VALORANT"],
  dota2: ["DotA2"],
};

const SUPPORTED_SPORTS = Object.keys(SUBREDDITS) as Sport[];
const ENDPOINTS = ["posts", "hot"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

async function fetchSubredditPosts(subreddit: string, sort: "new" | "hot"): Promise<any[]> {
  const url = `${BASE_URL}/${subreddit}/${sort}.json?limit=100&raw_json=1`;
  const data = await fetchJSON<any>(url, NAME, RATE_LIMIT, {
    headers: { "User-Agent": "sports-data-importer/5.0 (open-source research project)" },
  });
  const children: any[] = data?.data?.children ?? [];
  return children.map((child: any) => {
    const p = child?.data ?? {};
    return {
      id: p.id,
      title: p.title,
      author: p.author,
      url: p.url,
      permalink: `https://reddit.com${p.permalink}`,
      selftext: typeof p.selftext === "string" ? p.selftext.slice(0, 2000) : null,
      score: p.score,
      upvote_ratio: p.upvote_ratio,
      num_comments: p.num_comments,
      flair: p.link_flair_text ?? null,
      created_utc: p.created_utc,
      created_at: p.created_utc ? new Date(p.created_utc * 1000).toISOString() : null,
      subreddit: p.subreddit,
      is_self: p.is_self,
      thumbnail: p.thumbnail !== "self" && p.thumbnail !== "default" ? p.thumbnail : null,
      domain: p.domain,
    };
  });
}

async function importPosts(ctx: EndpointCtx, sort: "new" | "hot" = "new"): Promise<EndpointResult> {
  const subreddits = SUBREDDITS[ctx.sport] ?? [];
  const errors: string[] = [];
  let filesWritten = 0;
  const dateStr = new Date().toISOString().slice(0, 10);

  for (const sub of subreddits) {
    if (ctx.dryRun) continue;
    try {
      const posts = await fetchSubredditPosts(sub, sort);
      const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `${dateStr}_${sub}_${sort}.json`);
      writeJSON(outPath, {
        source: NAME,
        sport: ctx.sport,
        season: String(ctx.season),
        subreddit: sub,
        sort,
        date: dateStr,
        count: posts.length,
        posts,
        fetched_at: new Date().toISOString(),
      });
      filesWritten++;
      logger.progress(NAME, ctx.sport, "posts", `r/${sub} (${sort}): ${posts.length} posts`);
    } catch (err) {
      errors.push(`r/${sub}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  posts: (ctx) => importPosts(ctx, "new"),
  hot: (ctx) => importPosts(ctx, "hot"),
};

const redditProvider: Provider = {
  name: NAME,
  label: "Reddit",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];
    const activeEndpoints = (opts.endpoints.length
      ? opts.endpoints.filter((e) => (ENDPOINTS as readonly string[]).includes(e))
      : ["posts"]) as Endpoint[];

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

export default redditProvider;
