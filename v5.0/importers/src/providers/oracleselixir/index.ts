// ──────────────────────────────────────────────────────────
// Oracle's Elixir Provider  [DISABLED]
// ──────────────────────────────────────────────────────────
// LoL pro match data CSVs from oracleselixir.com (S3 CDN).
//
// DISABLED (2026-03-26): The S3 bucket hosting CSV downloads
// returns 404 for all years. The direct oracleselixir.com/data/
// URLs now serve the React SPA HTML instead of CSV files.
// Data downloads appear to require JavaScript rendering behind
// Cloudflare. Re-enable if a new public CSV endpoint is found.

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchCSV, fetchText } from "../../core/http.js";
import { rawPath, writeText, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "oracleselixir";
const SPORTS: readonly Sport[] = ["lol"] as const;

const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const ENDPOINTS = ["match_data"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

/** Candidate URLs to try in order for a given year's CSV. */
function csvUrls(year: number): string[] {
  return [
    `https://oracleselixir.com/data/${year}_match_data.csv`,
    `https://oracleselixir-downloadable-match-data.s3-us-west-2.amazonaws.com/${year}_LoL_esports_match_data_from_OraclesElixir.csv`,
  ];
}

/** Returns true when the response body looks like HTML rather than CSV. */
function looksLikeHTML(text: string): boolean {
  const head = text.trimStart().slice(0, 50).toLowerCase();
  return head.startsWith("<!doctype") || head.startsWith("<html") || head.startsWith("<!DOCTYPE");
}

// ── Endpoint handler ───────────────────────────────────────

interface EndpointCtx {
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

async function importMatchData(ctx: EndpointCtx): Promise<EndpointResult> {
  const outPath = rawPath(ctx.dataDir, NAME, "lol", ctx.season, "match_data.csv");

  if (fileExists(outPath)) {
    logger.info(`Skipping match_data ${ctx.season} — exists`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  if (ctx.dryRun) {
    logger.info(`[dry-run] Would fetch match_data ${ctx.season}`, NAME);
    return { filesWritten: 0, errors: [] };
  }

  const urls = csvUrls(ctx.season);
  logger.progress(NAME, "lol", "match_data", `Fetching ${ctx.season} CSV`);

  const attemptErrors: string[] = [];
  for (const url of urls) {
    try {
      logger.info(`Trying ${url}`, NAME);
      const csv = await fetchText(url, NAME, RATE_LIMIT, { timeoutMs: 120_000 });

      if (!csv || looksLikeHTML(csv)) {
        attemptErrors.push(`Got HTML instead of CSV from ${url}`);
        continue;
      }

      writeText(outPath, csv);
      const lines = csv.split("\n").length - 1;
      logger.progress(NAME, "lol", "match_data", `${lines} rows saved for ${ctx.season}`);
      return { filesWritten: 1, errors: [] };
    } catch (err) {
      attemptErrors.push(`${url}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  const msg = `match_data ${ctx.season}: all URLs failed — ${attemptErrors.join("; ")}`;
  logger.warn(msg, NAME);
  return { filesWritten: 0, errors: [msg] };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  match_data: importMatchData,
};

// ── Provider ───────────────────────────────────────────────

const oracleselixir: Provider = {
  name: NAME,
  label: "Oracle's Elixir (LoL)",
  sports: SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS];

    for (const season of opts.seasons) {
      for (const ep of activeEndpoints) {
        try {
          const result = await ENDPOINT_FNS[ep]({ season, dataDir: opts.dataDir, dryRun: opts.dryRun });
          totalFiles += result.filesWritten;
          allErrors.push(...result.errors);
        } catch (err) {
          const msg = `${ep} ${season}: ${err instanceof Error ? err.message : String(err)}`;
          logger.error(msg, NAME);
          allErrors.push(msg);
        }
      }
    }

    return {
      provider: NAME,
      sport: "lol",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default oracleselixir;
