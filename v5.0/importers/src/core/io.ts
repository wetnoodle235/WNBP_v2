// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — File I/O Utilities
// ──────────────────────────────────────────────────────────

import fs from "node:fs";
import path from "node:path";
import type { Sport } from "./types.js";

/**
 * Build the canonical output path for a provider's data file.
 * Convention: {dataDir}/raw/{provider}/{sport}/{season}/{...rest}
 */
export function rawPath(
  dataDir: string,
  provider: string,
  sport: Sport | string,
  season: number | string,
  ...segments: string[]
): string {
  return path.join(dataDir, "raw", provider, sport, String(season), ...segments);
}

/**
 * Build canonical path for odds sub-providers under one shared odds root.
 * Convention: {dataDir}/raw/odds/providers/{provider}/{sport}/{season}/{...rest}
 */
export function oddsProviderRawPath(
  dataDir: string,
  provider: string,
  sport: Sport | string,
  season: number | string,
  ...segments: string[]
): string {
  return path.join(dataDir, "raw", "odds", "providers", provider, sport, String(season), ...segments);
}

/**
 * Build path for round-partitioned raw data: {season}/rounds/round_XX/{...rest}
 * Used for race- or event-scoped provider data such as Formula 1 rounds.
 */
export function rawPathWithRound(
  dataDir: string,
  provider: string,
  sport: Sport | string,
  season: number | string,
  round: number | string,
  ...segments: string[]
): string {
  const roundNum = Number.parseInt(String(round), 10);
  const roundDir = Number.isFinite(roundNum) && roundNum > 0
    ? `round_${String(roundNum).padStart(2, "0")}`
    : `round_${String(round)}`;
  return path.join(dataDir, "raw", provider, sport, String(season), "rounds", roundDir, ...segments);
}

/**
 * Build path for hierarchical cfbdata structure: {season}/{week}/{endpoint}/{date}/{...rest}
 * Used for organizing large raw datasets by temporal dimensions.
 *
 * @param dataDir Base data directory
 * @param provider Provider name (e.g., "cfbdata")
 * @param sport Sport (e.g., "ncaaf")
 * @param season Season (e.g., 2025)
 * @param week Week number (1-16, or null for season-wide reference data)
 * @param date Date in YYYY-MM-DD format (required if week is set)
 * @param endpoint Endpoint name (e.g., "games", "plays", "stats")
 * @param segments Additional path segments
 *
 * @example
 * // Game file: {season}/{week}/games/{date}/{gameId}.json
 * rawPathWithWeekDate(dataDir, "cfbdata", "ncaaf", 2025, 1, "2025-09-06", "games", "401547489.json")
 * // → {dataDir}/raw/cfbdata/ncaaf/2025/1/games/2025-09-06/401547489.json
 *
 * @example
 * // Season-wide reference: {season}/reference/{endpoint}.json
 * rawPathWithWeekDate(dataDir, "cfbdata", "ncaaf", 2025, null, "", "recruiting.json")
 * // → {dataDir}/raw/cfbdata/ncaaf/2025/reference/recruiting.json
 */
export function rawPathWithWeekDate(
  dataDir: string,
  provider: string,
  sport: Sport | string,
  season: number | string,
  week: number | null,
  date: string,
  endpoint: string,
  ...segments: string[]
): string {
  const components: string[] = [dataDir, "raw", provider, String(sport), String(season)];

  if (week === null) {
    // Season-wide reference data: {season}/reference/{endpoint}/
    components.push("reference");
    components.push(endpoint);
  } else {
    // Weekly data: {season}/{week}/{endpoint}/{date}/
    components.push(String(week));
    components.push(endpoint);
    if (date) {
      components.push(date);
    }
  }

  return path.join(...components, ...segments);
}

/**
 * Ensure a directory exists (create parent directories if needed).
 * Returns the directory path.
 */
export function ensureDir(dirPath: string): string {
  fs.mkdirSync(dirPath, { recursive: true });
  return dirPath;
}

/**
 * Atomically write JSON to disk (write .tmp → rename).
 * Creates parent directories as needed.
 */
export function writeJSON(filePath: string, data: unknown): void {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });

  const tmp = filePath + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), "utf-8");
  fs.renameSync(tmp, filePath);
}

/**
 * Write raw text to disk atomically.
 */
export function writeText(filePath: string, content: string): void {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });

  const tmp = filePath + ".tmp";
  fs.writeFileSync(tmp, content, "utf-8");
  fs.renameSync(tmp, filePath);
}

/**
 * Read JSON from disk, returning null if file doesn't exist.
 */
export function readJSON<T = unknown>(filePath: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf-8")) as T;
  } catch {
    return null;
  }
}

/**
 * Check if a file already exists (skip re-fetch optimization).
 */
export function fileExists(filePath: string): boolean {
  return fs.existsSync(filePath);
}

/**
 * Count JSON files in a directory tree.
 */
export function countFiles(dir: string, ext = ".json"): number {
  if (!fs.existsSync(dir)) return 0;
  let count = 0;
  const walk = (d: string) => {
    for (const entry of fs.readdirSync(d, { withFileTypes: true })) {
      if (entry.isDirectory()) walk(path.join(d, entry.name));
      else if (entry.name.endsWith(ext)) count++;
    }
  };
  walk(dir);
  return count;
}
