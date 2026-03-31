#!/usr/bin/env node
// ──────────────────────────────────────────────────────────
// V5.0 Importer CLI
// ──────────────────────────────────────────────────────────
// Usage:
//   tsx src/cli.ts --provider=espn --sports=nba,nfl --seasons=2023,2024,2025
//   tsx src/cli.ts --all --seasons=2023,2024,2025,2026
//   tsx src/cli.ts --list
//   tsx src/cli.ts --provider=espn --endpoints=games,standings
//   tsx src/cli.ts --provider=oddsapi --snapshot=opening
//   tsx src/cli.ts --live --sports=nba,nfl
//   tsx src/cli.ts --live --sports=nba --scoreboard-interval=60 --news-interval=600

import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import { registry } from "./core/registry.js";
import { logger } from "./core/logger.js";
import type { ImportOptions, LogLevel, Sport } from "./core/types.js";

// ── Load environment ───────────────────────────────────────
const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, "../.env") });
dotenv.config({ path: path.resolve(__dirname, "../../config/.env"), override: true });

// ── Parse CLI arguments ────────────────────────────────────
function parseArgs(argv: string[]): {
  providers: string[];
  all: boolean;
  list: boolean;
  live: boolean;
  sports: Sport[];
  seasons: number[];
  endpoints: string[];
  snapshot?: "opening" | "closing" | "current";
  dryRun: boolean;
  dataDir: string;
  recentDays: number;
  scoreboardInterval: number;
  newsInterval: number;
} {
  const args = argv.slice(2);

  const getString = (prefix: string): string | undefined => {
    const arg = args.find((a) => a.startsWith(`--${prefix}=`));
    return arg?.split("=").slice(1).join("=");
  };

  const flag = (name: string) => args.includes(`--${name}`);

  const providers = getString("provider")?.split(",") ?? getString("providers")?.split(",") ?? [];
  const sports = (getString("sport")?.split(",") ?? getString("sports")?.split(",") ?? []) as Sport[];
  const seasons = getString("season")?.split(",").map(Number)
    ?? getString("seasons")?.split(",").map(Number)
    ?? getString("years")?.split(",").map(Number)
    ?? [new Date().getFullYear()];
  const endpoints = getString("endpoints")?.split(",") ?? [];
  const snapshot = getString("snapshot") as "opening" | "closing" | "current" | undefined;
  const dataDir = getString("data-dir") ?? process.env.DATA_DIR ?? path.resolve(__dirname, "../../data");

  const scoreboardInterval = Number(getString("scoreboard-interval") ?? "30");
  const newsInterval = Number(getString("news-interval") ?? "300");
  const recentDays = Number(getString("recent-days") ?? "0");

  if (getString("log-level")) {
    logger.setLevel(getString("log-level") as LogLevel);
  }

  return {
    providers,
    all: flag("all"),
    list: flag("list"),
    live: flag("live"),
    sports,
    seasons,
    endpoints,
    snapshot,
    dryRun: flag("dry-run"),
    dataDir: path.resolve(dataDir),
    recentDays,
    scoreboardInterval,
    newsInterval,
  };
}

// ── Main ───────────────────────────────────────────────────
async function main(): Promise<void> {
  const args = parseArgs(process.argv);

  logger.info("V5.0 Importer CLI starting…");
  logger.info(`Data directory: ${args.dataDir}`);
  logger.info(`Seasons: ${args.seasons.join(", ")}`);

  // Load provider modules — only those requested for faster startup
  if (args.list || args.all || args.providers.length === 0) {
    await registry.loadAll();
  } else {
    logger.debug(`Lazy-loading ${args.providers.length} provider(s): ${args.providers.join(", ")}`);
    await registry.loadOnly(args.providers);
  }

  // --list: show all providers and exit
  if (args.list) {
    console.log("\n  Available Providers:\n");
    for (const p of registry.getAll()) {
      const status = p.enabled ? "✓" : "✗";
      const key = p.requiresKey ? "🔑" : "  ";
      console.log(`  ${status} ${key} ${p.name.padEnd(18)} ${p.sports.join(", ")}`);
    }
    console.log(`\n  ${registry.getEnabled().length}/${registry.getAll().length} enabled\n`);
    return;
  }

  // --live: start the live data daemon
  if (args.live) {
    const { pollScoreboard, pollNews } = await import("./live/index.js");
    const { ESPN_LIVE_SPORTS } = await import("./providers/espn/live.js");

    const liveSports = args.sports.length
      ? args.sports.filter((s) => s in ESPN_LIVE_SPORTS)
      : (Object.keys(ESPN_LIVE_SPORTS) as Sport[]);

    if (liveSports.length === 0) {
      logger.error("No valid live sports specified");
      process.exit(1);
    }

    logger.info(`Live daemon — sports: [${liveSports.join(", ")}]`);

    const pollers: Array<{ stop(): void }> = [];
    for (const sport of liveSports) {
      pollers.push(pollScoreboard(sport, args.dataDir, args.scoreboardInterval * 1_000));
      pollers.push(pollNews(sport, args.dataDir, args.newsInterval * 1_000));
    }

    const shutdown = (): void => {
      logger.info("Shutting down live daemon…");
      for (const p of pollers) p.stop();
      process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
    return;
  }

  // Determine which providers to run
  const providerNames = args.all ? "all" as const : args.providers;

  if (providerNames !== "all" && providerNames.length === 0) {
    logger.error("No providers specified. Use --provider=espn, --all, or --live");
    process.exit(1);
  }

  const opts: ImportOptions = {
    sports: args.sports,
    seasons: args.seasons,
    endpoints: args.endpoints,
    dataDir: args.dataDir,
    snapshotType: args.snapshot,
    dryRun: args.dryRun,
    recentDays: args.recentDays || undefined,
  };

  const results = await registry.run(providerNames, opts);

  // Summary
  const totalFiles = results.reduce((s, r) => s + r.filesWritten, 0);
  const totalErrors = results.reduce((s, r) => s + r.errors.length, 0);
  const totalMs = results.reduce((s, r) => s + r.durationMs, 0);

  console.log("\n── Import Complete ──────────────────────────────────────");
  console.log(`  Providers: ${results.length}`);
  console.log(`  Files:     ${totalFiles}`);
  console.log(`  Errors:    ${totalErrors}`);
  console.log(`  Duration:  ${(totalMs / 1000).toFixed(1)}s`);
  console.log("────────────────────────────────────────────────────────\n");

  if (totalErrors > 0) process.exit(1);
}

main().catch((err) => {
  logger.error(`Fatal: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
