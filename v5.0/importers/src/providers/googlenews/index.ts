// ──────────────────────────────────────────────────────────
// Google News RSS Provider
// ──────────────────────────────────────────────────────────
// Fetches sport news articles via Google News RSS search.
// No API key required. Covers all major sports with targeted
// search queries returning the latest ~100 articles per query.

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchText } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import { parseRss } from "../../core/rss.js";

const NAME = "googlenews";
const BASE_URL = "https://news.google.com/rss/search";
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_000 };

const SPORT_QUERIES: Partial<Record<Sport, string[]>> = {
  nba: ["NBA basketball"],
  nfl: ["NFL football"],
  mlb: ["MLB baseball"],
  nhl: ["NHL hockey"],
  ncaab: ["NCAA college basketball"],
  ncaaw: ["NCAA women's basketball"],
  ncaaf: ["NCAA college football"],
  wnba: ["WNBA basketball"],
  epl: ["Premier League soccer football"],
  laliga: ["La Liga soccer"],
  bundesliga: ["Bundesliga soccer"],
  seriea: ["Serie A soccer"],
  ligue1: ["Ligue 1 soccer"],
  mls: ["MLS soccer"],
  ucl: ["Champions League soccer UEFA"],
  nwsl: ["NWSL women soccer"],
  ligamx: ["Liga MX soccer"],
  europa: ["Europa League UEFA soccer"],
  f1: ["Formula 1 F1 racing"],
  indycar: ["IndyCar racing"],
  ufc: ["UFC MMA fighting"],
  atp: ["ATP tennis"],
  wta: ["WTA tennis women"],
  golf: ["PGA golf"],
  lpga: ["LPGA women golf"],
  lol: ["League of Legends esports LCS"],
  csgo: ["CS2 CSGO Counter-Strike esports"],
  valorant: ["Valorant esports VCT"],
  dota2: ["Dota 2 esports"],
};

const SUPPORTED_SPORTS = Object.keys(SPORT_QUERIES) as Sport[];
const ENDPOINTS = ["news"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

async function importNews(ctx: EndpointCtx): Promise<EndpointResult> {
  const queries = SPORT_QUERIES[ctx.sport] ?? [];
  const errors: string[] = [];
  let filesWritten = 0;
  const dateStr = new Date().toISOString().slice(0, 10);

  for (const query of queries) {
    if (ctx.dryRun) continue;
    try {
      const url = `${BASE_URL}?q=${encodeURIComponent(query)}&hl=en-US&gl=US&ceid=US:en`;
      const xml = await fetchText(url, NAME, RATE_LIMIT, {
        headers: { "Accept": "application/rss+xml, text/xml, */*" },
      });
      const items = parseRss(xml);
      const articles = items.map((item) => ({
        title: item.title,
        link: item.link,
        description: item.description,
        published_at: item.pubDate ? new Date(item.pubDate).toISOString() : null,
        source: item.source,
        image_url: item.imageUrl,
        guid: item.guid,
      }));

      const slug = query.toLowerCase().replace(/[^a-z0-9]+/g, "_").slice(0, 60);
      const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `${dateStr}_${slug}.json`);
      writeJSON(outPath, {
        source: NAME,
        sport: ctx.sport,
        season: String(ctx.season),
        query,
        date: dateStr,
        count: articles.length,
        articles,
        fetched_at: new Date().toISOString(),
      });
      filesWritten++;
      logger.progress(NAME, ctx.sport, "news", `"${query}": ${articles.length} articles`);
    } catch (err) {
      errors.push(`news/${ctx.sport}/${query}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  news: importNews,
};

const googlenews: Provider = {
  name: NAME,
  label: "Google News RSS",
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

export default googlenews;
