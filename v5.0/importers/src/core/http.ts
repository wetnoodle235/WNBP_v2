// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — Shared HTTP Client
// ──────────────────────────────────────────────────────────
// Provides fetch with retry, rate-limiting, and logging.

import type { FetchOptions, RateLimitConfig } from "./types.js";
import { logger } from "./logger.js";

const DEFAULT_TIMEOUT_MS = 15_000;
const DEFAULT_RETRIES = 2;
const DEFAULT_RETRY_DELAY_MS = 1_000;

/** Per-provider rate limiter — tracks request timestamps */
const buckets = new Map<string, number[]>();

async function enforceRateLimit(provider: string, config: RateLimitConfig): Promise<void> {
  let timestamps = buckets.get(provider);
  if (!timestamps) {
    timestamps = [];
    buckets.set(provider, timestamps);
  }

  const now = Date.now();
  const cutoff = now - config.perMs;

  // Purge expired timestamps from the front (oldest first)
  while (timestamps.length > 0 && timestamps[0]! < cutoff) {
    timestamps.shift();
  }

  if (timestamps.length >= config.requests) {
    const oldest = timestamps[0]!;
    const waitMs = config.perMs - (now - oldest) + 50;
    logger.debug(`Rate limit: waiting ${waitMs}ms`, provider);
    await sleep(waitMs);
    return enforceRateLimit(provider, config);
  }

  timestamps.push(Date.now());
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Fetch JSON from a URL with retry, rate-limiting, and timeout.
 * This is the single HTTP function all providers should use.
 */
export async function fetchJSON<T = unknown>(
  url: string,
  provider: string,
  rateLimit: RateLimitConfig,
  opts: FetchOptions = {},
): Promise<T> {
  const {
    method = "GET",
    headers = {},
    body,
    retries = DEFAULT_RETRIES,
    retryDelayMs = DEFAULT_RETRY_DELAY_MS,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  } = opts;

  await enforceRateLimit(provider, rateLimit);

  for (let attempt = 1; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        method,
        headers: { "Accept": "application/json", ...headers },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });

      clearTimeout(timer);

      if (res.status === 429) {
        const retryAfter = parseInt(res.headers.get("retry-after") ?? "5", 10);
        const remaining = res.headers.get("x-calllimit-remaining");
        const text = await res.text().catch(() => "");
        const lowered = text.toLowerCase();
        const quotaExhausted = remaining === "0" || lowered.includes("monthly call quota exceeded");

        if (quotaExhausted || attempt >= retries) {
          throw new Error(`HTTP 429: ${text.slice(0, 200) || "Too Many Requests"}`);
        }

        logger.warn(`429 — retry after ${retryAfter}s (attempt ${attempt}/${retries})`, provider);
        await sleep(retryAfter * 1000);
        continue;
      }

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
      }

      return (await res.json()) as T;
    } catch (err: unknown) {
      clearTimeout(timer);
      const msg = err instanceof Error ? err.message : String(err);

      if (msg.toLowerCase().includes("http 429") || msg.toLowerCase().includes("monthly call quota exceeded")) {
        throw err instanceof Error ? err : new Error(msg);
      }

      // 403 Forbidden is non-retryable (subscription/auth restriction)
      if (msg.toLowerCase().includes("http 403")) {
        throw err instanceof Error ? err : new Error(msg);
      }

      if (attempt < retries) {
        const delay = retryDelayMs * attempt;
        logger.warn(`Attempt ${attempt}/${retries} failed: ${msg} — retrying in ${delay}ms`, provider);
        await sleep(delay);
      } else {
        throw new Error(`${provider} fetch failed after ${retries} attempts: ${msg}\n  URL: ${url}`);
      }
    }
  }

  throw new Error("Unreachable");
}

/**
 * Fetch raw HTML/text from a URL (for scraping providers).
 */
export async function fetchText(
  url: string,
  provider: string,
  rateLimit: RateLimitConfig,
  opts: FetchOptions = {},
): Promise<string> {
  const {
    method = "GET",
    headers = {},
    retries = DEFAULT_RETRIES,
    retryDelayMs = DEFAULT_RETRY_DELAY_MS,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  } = opts;

  await enforceRateLimit(provider, rateLimit);

  for (let attempt = 1; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        method,
        headers: { ...headers },
        signal: controller.signal,
      });
      clearTimeout(timer);

      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (err: unknown) {
      clearTimeout(timer);
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt < retries) {
        await sleep(retryDelayMs * attempt);
      } else {
        throw new Error(`${provider} text fetch failed after ${retries} attempts: ${msg}\n  URL: ${url}`);
      }
    }
  }

  throw new Error("Unreachable");
}

/**
 * Fetch a CSV file and return raw text.
 */
export async function fetchCSV(
  url: string,
  provider: string,
  rateLimit: RateLimitConfig,
  opts: FetchOptions = {},
): Promise<string> {
  return fetchText(url, provider, rateLimit, {
    ...opts,
    headers: { "Accept": "text/csv,application/csv,*/*", ...opts.headers },
  });
}
