// ──────────────────────────────────────────────────────────
// Live Multi-Source News Poller
// ──────────────────────────────────────────────────────────
// Polls Reddit, Google News RSS, BBC/Yahoo RSS, and RotoWire
// injury feeds at a configurable interval, deduplicating
// articles and merging into the daily news file.

import path from "node:path";
import type { Sport } from "../core/types.js";
import { writeJSON, readJSON } from "../core/io.js";
import { fetchJSON, fetchText } from "../core/http.js";
import { parseRss } from "../core/rss.js";
import { logger } from "../core/logger.js";
import type { Poller } from "./scoreboard.js";

const REDDIT_RATE = { requests: 1, perMs: 2_000 };
const RSS_RATE = { requests: 2, perMs: 1_000 };
const GOOGLE_NEWS_BASE = "https://news.google.com/rss/search";
const ROTOWIRE_BASE = "https://www.rotowire.com/rss/news.php";

const ROTOWIRE_CODES: Partial<Record<Sport, string>> = {
  nfl: "NFL", nba: "NBA", mlb: "MLB", nhl: "NHL",
  wnba: "WNBA", ncaab: "CBB", ncaaf: "CFB",
};

const SPORT_SUBREDDITS: Partial<Record<Sport, string>> = {
  nba: "nba", nfl: "nfl", mlb: "baseball", nhl: "hockey",
  ncaab: "CollegeBasketball", ncaaf: "CFB", wnba: "wnba",
  epl: "PremierLeague", ufc: "ufc", f1: "formula1",
};

const SPORT_QUERIES: Partial<Record<Sport, string>> = {
  nba: "NBA basketball", nfl: "NFL football", mlb: "MLB baseball",
  nhl: "NHL hockey", ncaab: "NCAA basketball", ncaaf: "NCAA football",
  wnba: "WNBA", epl: "Premier League", ufc: "UFC MMA",
  f1: "Formula 1", golf: "PGA golf", atp: "ATP tennis",
};

interface NewsItem {
  headline: string;
  source: string;
  link?: string;
  published_at?: string | null;
  [key: string]: unknown;
}

interface DailyNewsFile {
  sport: string;
  date: string;
  articles: NewsItem[];
  lastUpdated: string;
}

function buildHeadlineKey(item: NewsItem): string {
  return item.headline.trim().toLowerCase().slice(0, 80);
}

async function fetchRedditPosts(sport: Sport): Promise<NewsItem[]> {
  const sub = SPORT_SUBREDDITS[sport];
  if (!sub) return [];
  try {
    const data = await fetchJSON<any>(
      `https://www.reddit.com/r/${sub}/new.json?limit=25&raw_json=1`,
      `live-news-reddit/${sport}`, REDDIT_RATE,
      { headers: { "User-Agent": "sports-data-importer/5.0" } },
    );
    return (data?.data?.children ?? []).map((c: any) => ({
      headline: c.data?.title ?? "",
      source: "reddit",
      link: `https://reddit.com${c.data?.permalink ?? ""}`,
      published_at: c.data?.created_utc ? new Date(c.data.created_utc * 1000).toISOString() : null,
      score: c.data?.score,
      subreddit: sub,
    })).filter((i: NewsItem) => i.headline);
  } catch {
    return [];
  }
}

async function fetchGoogleNewsItems(sport: Sport): Promise<NewsItem[]> {
  const query = SPORT_QUERIES[sport];
  if (!query) return [];
  try {
    const url = `${GOOGLE_NEWS_BASE}?q=${encodeURIComponent(query)}&hl=en-US&gl=US&ceid=US:en`;
    const xml = await fetchText(url, `live-news-google/${sport}`, RSS_RATE, {
      headers: { "Accept": "application/rss+xml, text/xml, */*" },
    });
    return parseRss(xml).map((i) => ({
      headline: i.title,
      source: "googlenews",
      link: i.link,
      published_at: i.pubDate ? new Date(i.pubDate).toISOString() : null,
      description: i.description,
    })).filter((i: NewsItem) => i.headline);
  } catch {
    return [];
  }
}

async function fetchRotoWireItems(sport: Sport): Promise<NewsItem[]> {
  const code = ROTOWIRE_CODES[sport];
  if (!code) return [];
  try {
    const xml = await fetchText(
      `${ROTOWIRE_BASE}?sport=${code}`,
      `live-news-rotowire/${sport}`, RSS_RATE,
      { headers: { "Accept": "application/rss+xml, text/xml, */*" } },
    );
    return parseRss(xml).map((i) => ({
      headline: i.title,
      source: "rotowire",
      link: i.link,
      published_at: i.pubDate ? new Date(i.pubDate).toISOString() : null,
      description: i.description,
    })).filter((i: NewsItem) => i.headline);
  } catch {
    return [];
  }
}

async function tick(sport: Sport, dataDir: string): Promise<void> {
  const today = new Date().toISOString().slice(0, 10);
  const dir = path.join(dataDir, "live", "news");
  const filePath = path.join(dir, `${sport}_${today}_multi.json`);

  const existing = readJSON<DailyNewsFile>(filePath);
  const currentArticles: NewsItem[] = existing?.articles ?? [];
  const seen = new Set(currentArticles.map(buildHeadlineKey));

  const [redditItems, googleItems, rotoItems] = await Promise.all([
    fetchRedditPosts(sport),
    fetchGoogleNewsItems(sport),
    fetchRotoWireItems(sport),
  ]);

  let added = 0;
  for (const item of [...redditItems, ...googleItems, ...rotoItems]) {
    if (!item.headline) continue;
    const key = buildHeadlineKey(item);
    if (!seen.has(key)) {
      currentArticles.push(item);
      seen.add(key);
      added++;
    }
  }

  writeJSON(filePath, {
    sport,
    date: today,
    articles: currentArticles,
    lastUpdated: new Date().toISOString(),
  });

  logger.info(
    `Multi-news poll: ${added} new (reddit:${redditItems.length} goog:${googleItems.length} roto:${rotoItems.length}) → ${currentArticles.length} total`,
    `live-news/${sport}`,
  );
}

export function pollMultiSourceNews(
  sport: Sport,
  dataDir: string,
  intervalMs: number,
): Poller {
  tick(sport, dataDir).catch((err) =>
    logger.error(`Multi-news poll failed: ${String(err)}`, `live-news/${sport}`),
  );

  const timer = setInterval(() => {
    tick(sport, dataDir).catch((err) =>
      logger.error(`Multi-news poll failed: ${String(err)}`, `live-news/${sport}`),
    );
  }, intervalMs);

  return {
    stop() {
      clearInterval(timer);
      logger.info("Multi-source news poller stopped", `live-news/${sport}`);
    },
  };
}

export const MULTI_NEWS_SPORTS: Sport[] = Object.keys(SPORT_QUERIES) as Sport[];
