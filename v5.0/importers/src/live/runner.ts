#!/usr/bin/env node
// ──────────────────────────────────────────────────────────
// Live Data Collection Runner
// ──────────────────────────────────────────────────────────
// Standalone daemon that orchestrates scoreboard + news
// pollers for one or more sports.
//
// Usage:
//   npx tsx src/live/runner.ts --sports=nba,nfl
//   npx tsx src/live/runner.ts --sports=mlb --scoreboard-interval=60 --news-interval=600

import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Sport } from "../core/types.js";
import { logger } from "../core/logger.js";
import { ESPN_LIVE_SPORTS } from "../providers/espn/live.js";
import { pollScoreboard, type Poller } from "./scoreboard.js";
import { pollNews } from "./news.js";

// ── __dirname polyfill ──────────────────────────────────────

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ── Defaults ────────────────────────────────────────────────

const DEFAULT_SCOREBOARD_INTERVAL_S = 30;
const DEFAULT_NEWS_INTERVAL_S = 300;
const DEFAULT_DATA_DIR = path.resolve(__dirname, "..", "..", "..", "data");

// ── CLI parser ──────────────────────────────────────────────

interface LiveArgs {
  sports: Sport[];
  scoreboardIntervalMs: number;
  newsIntervalMs: number;
  dataDir: string;
}

function parseCliArgs(argv: string[]): LiveArgs {
  let sports: Sport[] = [];
  let scoreboardIntervalS = DEFAULT_SCOREBOARD_INTERVAL_S;
  let newsIntervalS = DEFAULT_NEWS_INTERVAL_S;
  let dataDir = process.env["DATA_DIR"] ?? DEFAULT_DATA_DIR;

  for (const arg of argv) {
    if (arg.startsWith("--sports=")) {
      sports = arg.slice("--sports=".length).split(",").map((s) => s.trim()) as Sport[];
    } else if (arg.startsWith("--scoreboard-interval=")) {
      scoreboardIntervalS = Number(arg.slice("--scoreboard-interval=".length));
    } else if (arg.startsWith("--news-interval=")) {
      newsIntervalS = Number(arg.slice("--news-interval=".length));
    } else if (arg.startsWith("--data-dir=")) {
      dataDir = arg.slice("--data-dir=".length);
    }
  }

  // Validate sports
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
    dataDir: path.resolve(dataDir),
  };
}

// ── Main ────────────────────────────────────────────────────

function main(): void {
  const args = parseCliArgs(process.argv.slice(2));

  logger.info(
    `Starting live daemon — sports: [${args.sports.join(", ")}], ` +
      `scoreboard: ${args.scoreboardIntervalMs / 1_000}s, ` +
      `news: ${args.newsIntervalMs / 1_000}s, ` +
      `data: ${args.dataDir}`,
    "live",
  );

  const pollers: Poller[] = [];

  for (const sport of args.sports) {
    pollers.push(pollScoreboard(sport, args.dataDir, args.scoreboardIntervalMs));
    pollers.push(pollNews(sport, args.dataDir, args.newsIntervalMs));
    logger.info(`Pollers started for ${sport}`, "live");
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
