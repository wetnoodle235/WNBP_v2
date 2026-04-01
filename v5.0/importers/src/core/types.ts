// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — Type Definitions
// ──────────────────────────────────────────────────────────

/** Canonical sport identifiers used across the entire platform */
export type Sport =
  | "nba" | "wnba" | "ncaab" | "ncaaw"
  | "nfl" | "ncaaf"
  | "mlb"
  | "nhl"
  | "epl" | "laliga" | "bundesliga" | "seriea" | "ligue1" | "mls" | "ucl" | "nwsl"
  | "ligamx" | "europa"
  | "f1" | "indycar"
  | "atp" | "wta"
  | "ufc"
  | "lol" | "csgo" | "dota2" | "valorant"
  | "golf" | "lpga";

export const ALL_SPORTS: readonly Sport[] = [
  "nba", "wnba", "ncaab", "ncaaw",
  "nfl", "ncaaf",
  "mlb",
  "nhl",
  "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl",
  "ligamx", "europa",
  "f1", "indycar",
  "atp", "wta",
  "ufc",
  "lol", "csgo", "dota2", "valorant",
  "golf", "lpga",
] as const;

/** What the CLI / orchestrator passes to each provider */
export interface ImportOptions {
  /** Which sports to import (empty = all that provider supports) */
  sports: Sport[];
  /** Seasons/years to cover, e.g. [2023, 2024, 2025, 2026] */
  seasons: number[];
  /** Specific endpoints to run (empty = all) */
  endpoints: string[];
  /** Base directory for writing data */
  dataDir: string;
  /** Snapshot type for live-odds providers */
  snapshotType?: "opening" | "closing" | "current";
  /** Dry run — log what would happen, don't fetch */
  dryRun: boolean;
  /** Only import data from the last N days (0 = full season) */
  recentDays?: number;
}

/** Result from a single provider import run */
export interface ImportResult {
  provider: string;
  sport: Sport | "multi";
  filesWritten: number;
  errors: string[];
  durationMs: number;
}

/** Rate-limit configuration */
export interface RateLimitConfig {
  /** Max requests per window */
  requests: number;
  /** Window size in milliseconds */
  perMs: number;
}

/**
 * The contract every provider must implement.
 * This is the core plugin interface — new sources are added
 * by creating a module that exports a Provider.
 */
export interface Provider {
  /** Unique slug, e.g. "espn", "oddsapi" */
  readonly name: string;
  /** Human-readable label */
  readonly label: string;
  /** Which sports this provider supplies data for */
  readonly sports: readonly Sport[];
  /** Whether an API key is required */
  readonly requiresKey: boolean;
  /** Rate-limit rules for this provider's API */
  readonly rateLimit: RateLimitConfig;
  /** Endpoints this provider can import */
  readonly endpoints: readonly string[];
  /** Whether this provider is currently active */
  enabled: boolean;

  /**
   * Run the import. The registry calls this with merged options.
   * Must write files to `opts.dataDir/raw/{provider.name}/{sport}/{season}/…`
   */
  import(opts: ImportOptions): Promise<ImportResult>;
}

/** Shape of provider registration entries */
export interface ProviderEntry {
  provider: Provider;
  module: string;
}

/** Logger levels */
export type LogLevel = "debug" | "info" | "warn" | "error";

/** HTTP method */
export type HttpMethod = "GET" | "POST" | "PUT" | "DELETE";

/** Fetch options for the shared HTTP client */
export interface FetchOptions {
  method?: HttpMethod;
  headers?: Record<string, string>;
  body?: unknown;
  retries?: number;
  retryDelayMs?: number;
  timeoutMs?: number;
}
