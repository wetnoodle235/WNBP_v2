// ──────────────────────────────────────────────────────────
// Live News Feed Poller
// ──────────────────────────────────────────────────────────
// Polls ESPN news at a configurable interval, deduplicating
// articles by headline within each daily file.

import path from "node:path";
import type { Sport } from "../core/types.js";
import { writeJSON, readJSON } from "../core/io.js";
import { logger } from "../core/logger.js";
import { ESPN_LIVE_SPORTS, fetchNews } from "../providers/espn/live.js";

// ── Types ───────────────────────────────────────────────────

export interface Poller {
  /** Stop the polling loop. */
  stop(): void;
}

interface NewsArticle {
  headline?: string;
  [key: string]: unknown;
}

interface NewsPayload {
  articles?: NewsArticle[];
  [key: string]: unknown;
}

interface DailyNewsFile {
  sport: string;
  date: string;
  articles: NewsArticle[];
  lastUpdated: string;
}

// ── Implementation ──────────────────────────────────────────

async function tick(sport: Sport, slug: string, dataDir: string): Promise<void> {
  const data = (await fetchNews(sport, slug)) as NewsPayload;
  const incoming = data?.articles ?? [];
  if (incoming.length === 0) {
    logger.debug("News poll returned 0 articles", `live/${sport}`);
    return;
  }

  const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  const dir = path.join(dataDir, "live", "news");
  const filePath = path.join(dir, `${sport}_${today}.json`);

  // Read existing daily file (may be null on first poll of the day)
  const existing = readJSON<DailyNewsFile>(filePath);
  const currentArticles = existing?.articles ?? [];

  // Build set of known headlines for O(1) dedup
  const seen = new Set(
    currentArticles
      .map((a) => a.headline)
      .filter((h): h is string => typeof h === "string"),
  );

  let added = 0;
  for (const article of incoming) {
    if (typeof article.headline === "string" && !seen.has(article.headline)) {
      currentArticles.push(article);
      seen.add(article.headline);
      added++;
    }
  }

  const output: DailyNewsFile = {
    sport,
    date: today,
    articles: currentArticles,
    lastUpdated: new Date().toISOString(),
  };

  writeJSON(filePath, output);
  logger.info(
    `News poll: ${added} new article(s), ${currentArticles.length} total → ${filePath}`,
    `live/${sport}`,
  );
}

/**
 * Start polling the ESPN news feed for `sport` every `intervalMs` ms.
 * Articles are deduplicated by headline within each daily file.
 * Returns a handle whose `stop()` method cancels the timer.
 */
export function pollNews(
  sport: Sport,
  dataDir: string,
  intervalMs: number,
): Poller {
  const slug = ESPN_LIVE_SPORTS[sport];
  if (!slug) {
    throw new Error(`No ESPN live slug for sport "${sport}"`);
  }

  // Fire immediately, then on interval
  tick(sport, slug, dataDir).catch((err) =>
    logger.error(`News poll failed: ${String(err)}`, `live/${sport}`),
  );

  const timer = setInterval(() => {
    tick(sport, slug, dataDir).catch((err) =>
      logger.error(`News poll failed: ${String(err)}`, `live/${sport}`),
    );
  }, intervalMs);

  return {
    stop() {
      clearInterval(timer);
      logger.info("News poller stopped", `live/${sport}`);
    },
  };
}
