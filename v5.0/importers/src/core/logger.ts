// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — Logger
// ──────────────────────────────────────────────────────────

import type { LogLevel } from "./types.js";

const LEVELS: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

let currentLevel: LogLevel = (process.env.LOG_LEVEL as LogLevel) ?? "info";

function ts(): string {
  return new Date().toISOString().slice(11, 23);
}

function shouldLog(level: LogLevel): boolean {
  return LEVELS[level] >= LEVELS[currentLevel];
}

function fmt(level: string, ctx: string, msg: string): string {
  const tag = level.toUpperCase().padEnd(5);
  return `${ts()} [${tag}] ${ctx ? `[${ctx}] ` : ""}${msg}`;
}

export const logger = {
  setLevel(level: LogLevel) {
    currentLevel = level;
  },

  debug(msg: string, ctx = "") {
    if (shouldLog("debug")) console.debug(fmt("debug", ctx, msg));
  },
  info(msg: string, ctx = "") {
    if (shouldLog("info")) console.log(fmt("info", ctx, msg));
  },
  warn(msg: string, ctx = "") {
    if (shouldLog("warn")) console.warn(fmt("warn", ctx, msg));
  },
  error(msg: string, ctx = "") {
    if (shouldLog("error")) console.error(fmt("error", ctx, msg));
  },

  /** Log import progress */
  progress(provider: string, sport: string, endpoint: string, detail: string) {
    this.info(`${endpoint} — ${detail}`, `${provider}/${sport}`);
  },

  /** Log a summary line at end of provider run */
  summary(provider: string, files: number, errors: number, durationMs: number) {
    const secs = (durationMs / 1000).toFixed(1);
    const status = errors > 0 ? `⚠ ${errors} errors` : "✓ clean";
    this.info(`Done: ${files} files, ${status}, ${secs}s`, provider);
  },
};
