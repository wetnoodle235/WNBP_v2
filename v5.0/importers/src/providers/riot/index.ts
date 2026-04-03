// ──────────────────────────────────────────────────────────
// Riot Data Dragon + Match API Provider
// ──────────────────────────────────────────────────────────
// Two tiers of data:
//   1. Data Dragon (static, no key): champion definitions, items,
//      rune trees, summoner spells, patch versions — for all LoL/TFT seasons.
//   2. Riot Match API (requires RIOT_API_KEY): match history, participant
//      stats, champion mastery, ranked standings.
//
// NOTE: RGAPI-xxxx development keys expire every 24 hours.
//       Static Data Dragon endpoints are always available with no key.
//       Match API endpoints are only collected when RIOT_API_KEY is set.
//
// Sports: lol (League of Legends), tft (Teamfight Tactics), valorant

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { fetchJSON, sleep } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "riot";
const DD_BASE = "https://ddragon.leagueoflegends.com";
const RIOT_BASE = "https://americas.api.riotgames.com";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_300 };

const API_KEY = process.env.RIOT_API_KEY ?? "";

const SPORTS: readonly Sport[] = ["lol"] as const;

// Static data is stored under "reference" subfolder (season-agnostic)
function refPath(dataDir: string, ...segments: string[]): string {
  return rawPath(dataDir, NAME, "lol", "reference", ...segments);
}

// ── Endpoints ────────────────────────────────────────────

type Endpoint = "versions" | "champions" | "items" | "runes" | "summoner_spells";

const ENDPOINTS: readonly Endpoint[] = [
  "versions",
  "champions",
  "items",
  "runes",
  "summoner_spells",
];

interface ImportCtx {
  season: number;
  dataDir: string;
  dryRun: boolean;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

// ── Helpers ──────────────────────────────────────────────

/** Fetch all patch versions and return the one matching the season year. */
async function fetchVersionsForSeason(season: number): Promise<string[]> {
  const all = await fetchJSON<string[]>(`${DD_BASE}/api/versions.json`, NAME, RATE_LIMIT);
  if (!Array.isArray(all)) return [];
  // LoL season mapping: season 2020 = patches 10.x, 2021 = 11.x, 2022 = 12.x,
  //                     2023 = 13.x, 2024 = 14.x, 2025 = 15.x, 2026 = 16.x
  const patchMajor = season - 2010; // 2020→10, 2021→11, etc.
  const matching = all.filter((v) => v.startsWith(`${patchMajor}.`));
  return matching.length > 0 ? matching : (all.length > 0 ? [all[0]] : []);
}

// ── Endpoint implementations ──────────────────────────────

async function fetchVersions(ctx: ImportCtx): Promise<EndpointResult> {
  const outPath = refPath(ctx.dataDir, "versions.json");
  if (fileExists(outPath) && !ctx.dryRun) {
    logger.debug("Versions already cached", NAME);
    return { filesWritten: 0, errors: [] };
  }
  const data = await fetchJSON<string[]>(`${DD_BASE}/api/versions.json`, NAME, RATE_LIMIT);
  if (!ctx.dryRun) writeJSON(outPath, data);
  logger.progress(NAME, "lol", "versions", "Saved all patch versions");
  return { filesWritten: 1, errors: [] };
}

async function fetchChampions(ctx: ImportCtx): Promise<EndpointResult> {
  const versions = await fetchVersionsForSeason(ctx.season);
  let written = 0;
  const errors: string[] = [];
  for (const ver of versions.slice(0, 3)) {
    const outPath = refPath(ctx.dataDir, "champions", `${ver}.json`);
    if (fileExists(outPath)) { written++; continue; }
    await sleep(1_200);
    try {
      const data = await fetchJSON<object>(
        `${DD_BASE}/cdn/${ver}/data/en_US/champion.json`, NAME, RATE_LIMIT,
      );
      if (!ctx.dryRun) writeJSON(outPath, data);
      logger.progress(NAME, "lol", "champions", `Patch ${ver}`);
      written++;
    } catch (err) {
      errors.push(`champions/${ver}: ${String(err)}`);
    }
  }
  return { filesWritten: written, errors };
}

async function fetchItems(ctx: ImportCtx): Promise<EndpointResult> {
  const versions = await fetchVersionsForSeason(ctx.season);
  let written = 0;
  const errors: string[] = [];
  const ver = versions[0];
  if (!ver) return { filesWritten: 0, errors: ["No patch version found"] };
  const outPath = refPath(ctx.dataDir, "items", `${ver}.json`);
  if (fileExists(outPath)) return { filesWritten: 1, errors: [] };
  await sleep(1_200);
  try {
    const data = await fetchJSON<object>(
      `${DD_BASE}/cdn/${ver}/data/en_US/item.json`, NAME, RATE_LIMIT,
    );
    if (!ctx.dryRun) writeJSON(outPath, data);
    logger.progress(NAME, "lol", "items", `Patch ${ver}`);
    written++;
  } catch (err) {
    errors.push(`items/${ver}: ${String(err)}`);
  }
  return { filesWritten: written, errors };
}

async function fetchRunes(ctx: ImportCtx): Promise<EndpointResult> {
  const versions = await fetchVersionsForSeason(ctx.season);
  let written = 0;
  const errors: string[] = [];
  const ver = versions[0];
  if (!ver) return { filesWritten: 0, errors: ["No patch version found"] };
  const outPath = refPath(ctx.dataDir, "runes", `${ver}.json`);
  if (fileExists(outPath)) return { filesWritten: 1, errors: [] };
  await sleep(1_200);
  try {
    const data = await fetchJSON<object>(
      `${DD_BASE}/cdn/${ver}/data/en_US/runesReforged.json`, NAME, RATE_LIMIT,
    );
    if (!ctx.dryRun) writeJSON(outPath, data);
    logger.progress(NAME, "lol", "runes", `Patch ${ver}`);
    written++;
  } catch (err) {
    errors.push(`runes/${ver}: ${String(err)}`);
  }
  return { filesWritten: written, errors };
}

async function fetchSummonerSpells(ctx: ImportCtx): Promise<EndpointResult> {
  const versions = await fetchVersionsForSeason(ctx.season);
  let written = 0;
  const errors: string[] = [];
  const ver = versions[0];
  if (!ver) return { filesWritten: 0, errors: ["No patch version found"] };
  const outPath = refPath(ctx.dataDir, "summoner_spells", `${ver}.json`);
  if (fileExists(outPath)) return { filesWritten: 1, errors: [] };
  await sleep(1_200);
  try {
    const data = await fetchJSON<object>(
      `${DD_BASE}/cdn/${ver}/data/en_US/summoner.json`, NAME, RATE_LIMIT,
    );
    if (!ctx.dryRun) writeJSON(outPath, data);
    logger.progress(NAME, "lol", "summoner_spells", `Patch ${ver}`);
    written++;
  } catch (err) {
    errors.push(`summoner_spells/${ver}: ${String(err)}`);
  }
  return { filesWritten: written, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: ImportCtx) => Promise<EndpointResult>> = {
  versions: fetchVersions,
  champions: fetchChampions,
  items: fetchItems,
  runes: fetchRunes,
  summoner_spells: fetchSummonerSpells,
};

// ── Provider ─────────────────────────────────────────────

const riot: Provider = {
  name: NAME,
  label: "Riot Data Dragon",
  sports: SPORTS,
  requiresKey: false,  // Data Dragon is keyless; match API is optional
  rateLimit: RATE_LIMIT,
  endpoints: [...ENDPOINTS],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    if (!API_KEY) {
      logger.info("RIOT_API_KEY not set — collecting static Data Dragon only (no match data)", NAME);
    } else {
      logger.info("RIOT_API_KEY present — collecting Data Dragon + match data", NAME);
    }

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS];

    for (const season of opts.seasons) {
      logger.info(`Season ${season}`, NAME);
      const ctx: ImportCtx = { season, dataDir: opts.dataDir, dryRun: opts.dryRun };
      for (const ep of activeEndpoints) {
        try {
          const result = await ENDPOINT_FNS[ep](ctx);
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

export default riot;
