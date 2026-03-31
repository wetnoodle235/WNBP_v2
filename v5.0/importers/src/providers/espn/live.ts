// ──────────────────────────────────────────────────────────
// ESPN Live Endpoints
// ──────────────────────────────────────────────────────────
// Helpers for fetching real-time scoreboard and news data
// from ESPN's public site API.

import type { Sport, RateLimitConfig } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";

// ── Constants ───────────────────────────────────────────────

const SITE_API = "https://site.api.espn.com/apis/site/v2/sports";

const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_000 };

/** ESPN slugs for sports that support live scoreboard/news. */
export const ESPN_LIVE_SPORTS: Partial<Record<Sport, string>> = {
  nba: "basketball/nba",
  wnba: "basketball/wnba",
  ncaab: "basketball/mens-college-basketball",
  ncaaf: "football/college-football",
  nfl: "football/nfl",
  mlb: "baseball/mlb",
  nhl: "hockey/nhl",
  epl: "soccer/eng.1",
  laliga: "soccer/esp.1",
  bundesliga: "soccer/ger.1",
  seriea: "soccer/ita.1",
  ligue1: "soccer/fra.1",
  mls: "soccer/usa.1",
  ucl: "soccer/uefa.champions",
  ufc: "mma/ufc",
  golf: "golf/pga",
};

// ── Fetch helpers ───────────────────────────────────────────

/**
 * Fetch the live scoreboard for a sport.
 * Returns the raw ESPN scoreboard JSON payload.
 */
export async function fetchScoreboard(
  sport: Sport,
  slug: string,
): Promise<unknown> {
  const url = `${SITE_API}/${slug}/scoreboard`;
  return fetchJSON<unknown>(url, `espn-live/${sport}`, RATE_LIMIT);
}

/**
 * Fetch the latest news feed for a sport.
 * Returns the raw ESPN news JSON payload.
 */
export async function fetchNews(
  sport: Sport,
  slug: string,
): Promise<unknown> {
  const url = `${SITE_API}/${slug}/news`;
  return fetchJSON<unknown>(url, `espn-live/${sport}`, RATE_LIMIT);
}
