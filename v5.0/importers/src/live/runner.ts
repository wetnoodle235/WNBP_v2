#!/usr/bin/env node
// ──────────────────────────────────────────────────────────
// Live Data Collection Runner
// ──────────────────────────────────────────────────────────
// Standalone daemon that orchestrates scoreboard, news,
// DraftKings odds, multi-source news, and SofaScore pollers.
//
// Usage:
//   npx tsx src/live/runner.ts --sports=nba,nfl
//   npx tsx src/live/runner.ts --sports=mlb --scoreboard-interval=60 --news-interval=600
//   npx tsx src/live/runner.ts --sports=nfl --odds-interval=300 --no-odds --no-multi-news --no-sofascore

import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Sport } from "../core/types.js";
import { logger } from "../core/logger.js";
import { ESPN_LIVE_SPORTS } from "../providers/espn/live.js";
import { pollScoreboard, type Poller } from "./scoreboard.js";
import { pollNews } from "./news.js";
import { pollDraftKingsOdds, DK_LIVE_SPORTS } from "./odds_dk.js";
import { pollMultiSourceNews, MULTI_NEWS_SPORTS } from "./news_multi.js";
import { pollSofaScore, SF_SUPPORTED_SPORTS } from "./sofascore.js";

// ── __dirname polyfill ──────────────────────────────────────

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ── Defaults ────────────────────────────────────────────────

const DEFAULT_SCOREBOARD_INTERVAL_S = 30;
const DEFAULT_NEWS_INTERVAL_S = 300;
const DEFAULT_ODDS_INTERVAL_S = 300;          // DraftKings odds every 5 min
const DEFAULT_MULTI_NEWS_INTERVAL_S = 1800;   // Reddit/Google News every 30 min
const DEFAULT_SOFASCORE_INTERVAL_S = 120;     // SofaScore every 2 min
const DEFAULT_DATA_DIR = path.resolve(__dirname, "..", "..", "..", "data");

// ── CLI parser ──────────────────────────────────────────────

interface LiveArgs {
  sports: Sport[];
  scoreboardIntervalMs: number;
  newsIntervalMs: number;
  oddsIntervalMs: number;
  multiNewsIntervalMs: number;
  sofascoreIntervalMs: number;
  enableOdds: boolean;
  enableMultiNews: boolean;
  enableSofaScore: boolean;
  dataDir: string;
}

function parseCliArgs(argv: string[]): LiveArgs {
  let sports: Sport[] = [];
  let scoreboardIntervalS = DEFAULT_SCOREBOARD_INTERVAL_S;
  let newsIntervalS = DEFAULT_NEWS_INTERVAL_S;
  let oddsIntervalS = DEFAULT_ODDS_INTERVAL_S;
  let multiNewsIntervalS = DEFAULT_MULTI_NEWS_INTERVAL_S;
  let sofascoreIntervalS = DEFAULT_SOFASCORE_INTERVAL_S;
  let enableOdds = true;
  let enableMultiNews = true;
  let enableSofaScore = true;
  let dataDir = process.env["DATA_DIR"] ?? DEFAULT_DATA_DIR;

  for (const arg of argv) {
    if (arg.startsWith("--sports=")) {
      sports = arg.slice("--sports=".length).split(",").map((s) => s.trim()) as Sport[];
    } else if (arg.startsWith("--scoreboard-interval=")) {
      scoreboardIntervalS = Number(arg.slice("--scoreboard-interval=".length));
    } else if (arg.startsWith("--news-interval=")) {
      newsIntervalS = Number(arg.slice("--news-interval=".length));
    } else if (arg.startsWith("--odds-interval=")) {
      oddsIntervalS = Number(arg.slice("--odds-interval=".length));
    } else if (arg.startsWith("--multi-news-interval=")) {
      multiNewsIntervalS = Number(arg.slice("--multi-news-interval=".length));
    } else if (arg.startsWith("--sofascore-interval=")) {
      sofascoreIntervalS = Number(arg.slice("--sofascore-interval=".length));
    } else if (arg === "--no-odds") {
      enableOdds = false;
    } else if (arg === "--no-multi-news") {
      enableMultiNews = false;
    } else if (arg === "--no-sofascore") {
      enableSofaScore = false;
    } else if (arg.startsWith("--data-dir=")) {
      dataDir = arg.slice("--data-dir=".length);
    }
  }

  // Validate sports — scoreboard requires ESPN live sports, but odds/news support broader set
  const validSports = sports.filter((s) => s in ESPN_LIVE_SPORTS);
  if (validSports.length === 0) {
    const available = Object.keys(ESPN_LIVE_SPORTS).join(", ");
    logger.error(`No valid live sports specified. Available: ${available}`, "live");
    process.exit(1);
  }

  if (validSports.length < sports.length) {
    const skipped = sports.filter((s) => !(s in ESPN_LIVE_SPORTS));
    logger.warn(`Skipping unsupported live sports: ${skipped.join(", ")}`, "live");
  }

  return {
    sports: validSports,
    scoreboardIntervalMs: scoreboardIntervalS * 1_000,
    newsIntervalMs: newsIntervalS * 1_000,
    oddsIntervalMs: oddsIntervalS * 1_000,
    multiNewsIntervalMs: multiNewsIntervalS * 1_000,
    sofascoreIntervalMs: sofascoreIntervalS * 1_000,
    enableOdds,
    enableMultiNews,
    enableSofaScore,
    dataDir: path.resolve(dataDir),
  };
}

// ── Main ────────────────────────────────────────────────────

function main(): void {
  const args = parseCliArgs(process.argv.slice(2));

  logger.info(
    `Starting live daemon — sports: [${args.sports.join(", ")}], ` +
      `scoreboard: ${args.scoreboardIntervalMs / 1_000}s, ` +
      `espn-news: ${args.newsIntervalMs / 1_000}s, ` +
      `dk-odds: ${args.enableOdds ? `${args.oddsIntervalMs / 1_000}s` : "disabled"}, ` +
      `multi-news: ${args.enableMultiNews ? `${args.multiNewsIntervalMs / 1_000}s` : "disabled"}, ` +
      `sofascore: ${args.enableSofaScore ? `${args.sofascoreIntervalMs / 1_000}s` : "disabled"}, ` +
      `data: ${args.dataDir}`,
    "live",
  );

  const pollers: Poller[] = [];

  for (const sport of args.sports) {
    // ESPN scoreboard + ESPN news (always enabled)
    pollers.push(pollScoreboard(sport, args.dataDir, args.scoreboardIntervalMs));
    pollers.push(pollNews(sport, args.dataDir, args.newsIntervalMs));

    // DraftKings live odds (disable with --no-odds)
    if (args.enableOdds && sport in DK_LIVE_SPORTS) {
      try {
        pollers.push(pollDraftKingsOdds(sport, args.dataDir, args.oddsIntervalMs));
        logger.info(`DraftKings odds poller started for ${sport}`, "live");
      } catch (err) {
        logger.warn(`Could not start DK odds for ${sport}: ${String(err)}`, "live");
      }
    }

    // Reddit + Google News multi-source news (disable with --no-multi-news)
    if (args.enableMultiNews && MULTI_NEWS_SPORTS.includes(sport)) {
      pollers.push(pollMultiSourceNews(sport, args.dataDir, args.multiNewsIntervalMs));
      logger.info(`Multi-source news poller started for ${sport}`, "live");
    }

    // SofaScore live scores + player ratings (disable with --no-sofascore)
    if (args.enableSofaScore && SF_SUPPORTED_SPORTS.includes(sport)) {
      try {
        pollers.push(pollSofaScore(sport, args.dataDir, args.sofascoreIntervalMs));
        logger.info(`SofaScore poller started for ${sport}`, "live");
      } catch (err) {
        logger.warn(`Could not start SofaScore for ${sport}: ${String(err)}`, "live");
      }
    }

    logger.info(`All pollers started for ${sport}`, "live");
  }

  // Graceful shutdown
  const shutdown = (): void => {
    logger.info("Shutting down live daemon…", "live");
    for (const poller of pollers) {
      poller.stop();
    }
    logger.info("All pollers stopped. Goodbye.", "live");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main();
