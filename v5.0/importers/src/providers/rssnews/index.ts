// ──────────────────────────────────────────────────────────
// RSS News Provider (BBC Sport + Yahoo Sports)
// ──────────────────────────────────────────────────────────
// Aggregates sports news from multiple reputable RSS feeds:
// - BBC Sport (authoritative international coverage)
// - Yahoo Sports (broad US sports coverage)
// No API key required.

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

const NAME = "rssnews";
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_000 };

interface FeedSource { name: string; url: string }

const RSS_FEEDS: Partial<Record<Sport, FeedSource[]>> = {
  nfl: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/american-football/rss.xml" },
    { name: "yahoo", url: "https://sports.yahoo.com/nfl/rss.xml" },
  ],
  nba: [
    { name: "yahoo", url: "https://sports.yahoo.com/nba/rss.xml" },
  ],
  mlb: [
    { name: "yahoo", url: "https://sports.yahoo.com/mlb/rss.xml" },
  ],
  nhl: [
    { name: "yahoo", url: "https://sports.yahoo.com/nhl/rss.xml" },
  ],
  ncaab: [
    { name: "yahoo", url: "https://sports.yahoo.com/college-basketball/rss.xml" },
  ],
  ncaaf: [
    { name: "yahoo", url: "https://sports.yahoo.com/college-football/rss.xml" },
  ],
  wnba: [
    { name: "yahoo", url: "https://sports.yahoo.com/wnba/rss.xml" },
  ],
  epl: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/football/rss.xml" },
    { name: "yahoo", url: "https://sports.yahoo.com/soccer/rss.xml" },
  ],
  laliga: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/football/rss.xml" },
  ],
  bundesliga: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/football/rss.xml" },
  ],
  seriea: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/football/rss.xml" },
  ],
  ligue1: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/football/rss.xml" },
  ],
  mls: [
    { name: "yahoo", url: "https://sports.yahoo.com/soccer/rss.xml" },
  ],
  ucl: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/football/rss.xml" },
  ],
  f1: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/formula1/rss.xml" },
    { name: "yahoo", url: "https://sports.yahoo.com/motor-sports/rss.xml" },
  ],
  atp: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/tennis/rss.xml" },
    { name: "yahoo", url: "https://sports.yahoo.com/tennis/rss.xml" },
  ],
  wta: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/tennis/rss.xml" },
    { name: "yahoo", url: "https://sports.yahoo.com/tennis/rss.xml" },
  ],
  golf: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/golf/rss.xml" },
    { name: "yahoo", url: "https://sports.yahoo.com/golf/rss.xml" },
  ],
  lpga: [
    { name: "bbc", url: "https://feeds.bbci.co.uk/sport/golf/rss.xml" },
    { name: "yahoo", url: "https://sports.yahoo.com/golf/rss.xml" },
  ],
  ufc: [
    { name: "yahoo", url: "https://sports.yahoo.com/mma/rss.xml" },
  ],
};

const SUPPORTED_SPORTS = Object.keys(RSS_FEEDS) as Sport[];
const ENDPOINTS = ["news"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

async function importNews(ctx: EndpointCtx): Promise<EndpointResult> {
  const feeds = RSS_FEEDS[ctx.sport] ?? [];
  const errors: string[] = [];
  let filesWritten = 0;
  const dateStr = new Date().toISOString().slice(0, 10);

  // Deduplicate feeds by URL to avoid re-fetching shared BBC sport feeds
  const seen = new Set<string>();

  for (const feed of feeds) {
    if (seen.has(feed.url) || ctx.dryRun) continue;
    seen.add(feed.url);

    try {
      const xml = await fetchText(feed.url, NAME, RATE_LIMIT, {
        headers: { "Accept": "application/rss+xml, text/xml, */*" },
      });
      const items = parseRss(xml);
      const articles = items.map((item) => ({
        title: item.title,
        link: item.link,
        description: item.description,
        published_at: item.pubDate ? new Date(item.pubDate).toISOString() : null,
        source: feed.name,
        feed_url: feed.url,
        image_url: item.imageUrl,
        guid: item.guid,
      }));

      const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `${dateStr}_${feed.name}.json`);
      writeJSON(outPath, {
        source: NAME,
        sport: ctx.sport,
        season: String(ctx.season),
        feed_name: feed.name,
        feed_url: feed.url,
        date: dateStr,
        count: articles.length,
        articles,
        fetched_at: new Date().toISOString(),
      });
      filesWritten++;
      logger.progress(NAME, ctx.sport, "news", `${feed.name}: ${articles.length} articles`);
    } catch (err) {
      errors.push(`${feed.name}/${ctx.sport}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  news: importNews,
};

const rssnews: Provider = {
  name: NAME,
  label: "RSS News (BBC Sport + Yahoo Sports)",
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

export default rssnews;
