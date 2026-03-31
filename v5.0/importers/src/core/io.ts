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
