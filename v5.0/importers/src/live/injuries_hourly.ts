#!/usr/bin/env node
// ──────────────────────────────────────────────────────────
// Hourly Injuries Import Runner
// ──────────────────────────────────────────────────────────
// Discovers enabled providers that support the "injuries"
// endpoint and runs injuries-only imports for each.
//
// Usage:
//   npx tsx src/live/injuries_hourly.ts
//   npx tsx src/live/injuries_hourly.ts --sports=nba,nfl
//   npx tsx src/live/injuries_hourly.ts --providers=espn --seasons=2026
//   npx tsx src/live/injuries_hourly.ts --dry-run

import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import { registry } from "../core/registry.js";
import { logger } from "../core/logger.js";
import type { ImportOptions, Sport, Provider } from "../core/types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, "../../.env") });
dotenv.config({ path: path.resolve(__dirname, "../../../config/.env"), override: true });

const DEFAULT_DATA_DIR = path.resolve(__dirname, "..", "..", "..", "data");

interface Args {
  providers: string[];
  sports: Sport[];
  seasons: number[];
  dataDir: string;
  dryRun: boolean;
}

function parseArgs(argv: string[]): Args {
  const args = argv.slice(2);

  const getString = (prefix: string): string | undefined => {
    const arg = args.find((a) => a.startsWith(`--${prefix}=`));
    return arg?.split("=").slice(1).join("=");
  };

  const providers = (getString("provider") ?? getString("providers") ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const sports = (getString("sports") ?? getString("sport") ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean) as Sport[];

  const seasons = (getString("seasons") ?? getString("season") ?? String(new Date().getFullYear()))
    .split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n));

  const dataDir = path.resolve(getString("data-dir") ?? process.env["DATA_DIR"] ?? DEFAULT_DATA_DIR);
  const dryRun = args.includes("--dry-run");

  return { providers, sports, seasons, dataDir, dryRun };
}

function supportsInjuries(provider: Provider): boolean {
  return provider.enabled && provider.endpoints.includes("injuries");
}

async function runProvider(provider: Provider, args: Args): Promise<{ files: number; errors: number }> {
  const sportFilter = args.sports.length
    ? args.sports.filter((s) => provider.sports.includes(s))
    : [...provider.sports];

  if (sportFilter.length === 0) {
    logger.info(`Skipping ${provider.name}: no matching sports`, "injuries-hourly");
    return { files: 0, errors: 0 };
  }

  const opts: ImportOptions = {
    sports: sportFilter,
    seasons: args.seasons,
    endpoints: ["injuries"],
    dataDir: args.dataDir,
    dryRun: args.dryRun,
  };

  const result = await provider.import(opts);
  logger.summary(result.provider, result.filesWritten, result.errors.length, result.durationMs);
  return { files: result.filesWritten, errors: result.errors.length };
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv);

  logger.info(
    `Hourly injuries import starting — seasons=[${args.seasons.join(",")}], dryRun=${args.dryRun}, dataDir=${args.dataDir}`,
    "injuries-hourly",
  );

  await registry.loadAll();

  let providers = registry.getAll().filter(supportsInjuries);

  if (args.providers.length) {
    const selected = new Set(args.providers);
    providers = providers.filter((p) => selected.has(p.name));
  }

  if (providers.length === 0) {
    logger.warn("No enabled providers support the injuries endpoint", "injuries-hourly");
    return;
  }

  logger.info(
    `Running injuries imports for ${providers.length} provider(s): ${providers.map((p) => p.name).join(", ")}`,
    "injuries-hourly",
  );

  let totalFiles = 0;
  let totalErrors = 0;

  for (const provider of providers) {
    try {
      const summary = await runProvider(provider, args);
      totalFiles += summary.files;
      totalErrors += summary.errors;
    } catch (err) {
      totalErrors += 1;
      logger.error(
        `Provider ${provider.name} failed: ${err instanceof Error ? err.message : String(err)}`,
        "injuries-hourly",
      );
    }
  }

  logger.info(
    `Hourly injuries import complete — providers=${providers.length}, files=${totalFiles}, errors=${totalErrors}`,
    "injuries-hourly",
  );

  if (totalErrors > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  logger.error(`Fatal: ${err instanceof Error ? err.message : String(err)}`, "injuries-hourly");
  process.exit(1);
});
