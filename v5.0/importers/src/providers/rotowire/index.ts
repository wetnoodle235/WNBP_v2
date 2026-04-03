// ──────────────────────────────────────────────────────────
// RotoWire Injury/News RSS Provider
// ──────────────────────────────────────────────────────────
// Fetches player injury updates, transactions, and news from
// RotoWire's RSS feeds. No API key required. Covers NFL, NBA,
// MLB, NHL, and more. Updates in real-time during active seasons.

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchText } from "../../core/http.js";
import { rawPath, writeJSON, readJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";
import { parseRss } from "../../core/rss.js";

const NAME = "rotowire";
const BASE_RSS = "https://www.rotowire.com/rss/news.php";
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_500 };

const SPORT_CODES: Partial<Record<Sport, string>> = {
  nfl:   "NFL",
  nba:   "NBA",
  mlb:   "MLB",
  nhl:   "NHL",
  wnba:  "WNBA",
  ncaab: "CBB",
  ncaaf: "CFB",
  golf:  "PGA",
};

const SUPPORTED_SPORTS = Object.keys(SPORT_CODES) as Sport[];

interface RotoArticle {
  headline: string;
  link?: string;
  published_at?: string | null;
  description?: string;
  category?: string;
  source: string;
}

interface DailyNewsFile {
  sport: string;
  date: string;
  articles: RotoArticle[];
  lastUpdated: string;
}

function headlineKey(h: string): string {
  return h.trim().toLowerCase().slice(0, 80);
}

async function importSport(
  sport: Sport,
  code: string,
  dataDir: string,
  dryRun: boolean,
): Promise<{ filesWritten: number; errors: string[] }> {
  if (dryRun) return { filesWritten: 0, errors: [] };

  const url = `${BASE_RSS}?sport=${code}`;
  try {
    const xml = await fetchText(url, NAME, RATE_LIMIT, {
      headers: { "Accept": "application/rss+xml, text/xml, */*" },
    });
    const items = parseRss(xml);
    if (items.length === 0) return { filesWritten: 0, errors: [] };

    const today = new Date().toISOString().slice(0, 10);
    const outPath = rawPath(dataDir, NAME, sport, new Date().getFullYear(), `injuries_${today}.json`);

    const existing = readJSON<DailyNewsFile>(outPath);
    const articles: RotoArticle[] = existing?.articles ?? [];
    const seen = new Set(articles.map((a) => headlineKey(a.headline)));

    let added = 0;
    for (const item of items) {
      if (!item.title) continue;
      const key = headlineKey(item.title);
      if (!seen.has(key)) {
        articles.push({
          headline: item.title,
          link: item.link,
          published_at: item.pubDate ? new Date(item.pubDate).toISOString() : null,
          description: item.description,
          category: item.category,
          source: NAME,
        });
        seen.add(key);
        added++;
      }
    }

    writeJSON(outPath, {
      sport,
      date: today,
      articles,
      lastUpdated: new Date().toISOString(),
    });

    logger.progress(NAME, sport, "injuries", `${added} new / ${articles.length} total`);
    return { filesWritten: added > 0 || !fileExists(outPath) ? 1 : 0, errors: [] };
  } catch (err) {
    const msg = `injuries/${sport}: ${err instanceof Error ? err.message : String(err)}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }
}

const rotowire: Provider = {
  name: NAME,
  label: "RotoWire Injuries/News",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["injuries"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    for (const sport of activeSports) {
      const code = SPORT_CODES[sport];
      if (!code) continue;
      const r = await importSport(sport, code, opts.dataDir, opts.dryRun);
      totalFiles += r.filesWritten;
      allErrors.push(...r.errors);
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0]! : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default rotowire;
