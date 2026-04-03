// ──────────────────────────────────────────────────────────
// SofaScore Live Snapshot Poller
// ──────────────────────────────────────────────────────────
// Periodically fetches live scores, match details, and player
// ratings from SofaScore's public API and saves snapshots.
// Runs alongside the ESPN scoreboard poller for enriched data.

import path from "node:path";
import type { Sport } from "../core/types.js";
import { writeJSON } from "../core/io.js";
import { logger } from "../core/logger.js";
import type { Poller } from "./scoreboard.js";

const SF_BASE = "https://api.sofascore.com/api/v1";
const SF_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "application/json",
  Referer: "https://www.sofascore.com/",
};

// Tournament IDs for live-supported sports
const SF_LIVE_SPORTS: Partial<Record<Sport, number[]>> = {
  nba:        [132],
  nfl:        [63],
  mlb:        [64],
  nhl:        [62],
  wnba:       [182],
  epl:        [17],
  laliga:     [8],
  bundesliga: [35],
  seriea:     [23],
  ligue1:     [34],
  mls:        [242],
  ucl:        [7],
  ufc:        [117],
};

export const SF_SUPPORTED_SPORTS = Object.keys(SF_LIVE_SPORTS) as Sport[];

async function fetchLiveEvents(tournamentId: number): Promise<unknown[]> {
  try {
    const res = await fetch(
      `${SF_BASE}/sport/football/events/live`,
      { headers: SF_HEADERS, signal: AbortSignal.timeout(12_000) }
    );
    if (!res.ok) return [];
    const json = (await res.json()) as { events?: unknown[] };
    // Filter to this tournament
    const events = json.events ?? [];
    return (events as Array<Record<string, unknown>>).filter(
      (e) => (e.tournament as Record<string, unknown>)?.id === tournamentId
    );
  } catch {
    return [];
  }
}

async function fetchTodayEvents(tournamentId: number): Promise<unknown[]> {
  try {
    const today = new Date().toISOString().slice(0, 10);
    const res = await fetch(
      `${SF_BASE}/sport/football/scheduled-events/${today}`,
      { headers: SF_HEADERS, signal: AbortSignal.timeout(12_000) }
    );
    if (!res.ok) return [];
    const json = (await res.json()) as { events?: unknown[] };
    const events = json.events ?? [];
    return (events as Array<Record<string, unknown>>).filter(
      (e) => (e.tournament as Record<string, unknown>)?.id === tournamentId
    );
  } catch {
    return [];
  }
}

// SofaScore API paths differ by sport category
const SF_SPORT_CATEGORY: Partial<Record<Sport, string>> = {
  nba: "basketball", wnba: "basketball",
  nfl: "american-football",
  mlb: "baseball",
  nhl: "ice-hockey",
  ufc: "mma",
  epl: "football", laliga: "football", bundesliga: "football",
  seriea: "football", ligue1: "football", mls: "football", ucl: "football",
};

async function tick(sport: Sport, dataDir: string): Promise<void> {
  const tournamentIds = SF_LIVE_SPORTS[sport];
  if (!tournamentIds?.length) return;

  const category = SF_SPORT_CATEGORY[sport] ?? "football";
  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  const ts = now.toISOString().replace(/[:.]/g, "-");

  const dir = path.join(dataDir, "live", "sofascore");
  const allEvents: unknown[] = [];

  for (const tid of tournamentIds) {
    // Try live events first, fallback to today's scheduled
    const liveRes = await fetch(
      `${SF_BASE}/sport/${category}/events/live`,
      { headers: SF_HEADERS, signal: AbortSignal.timeout(12_000) }
    ).then(async (r) => {
      if (!r.ok) return [];
      const j = (await r.json()) as { events?: unknown[] };
      return ((j.events ?? []) as Array<Record<string, unknown>>)
        .filter((e) => (e.tournament as Record<string, unknown>)?.id === tid);
    }).catch(() => [] as unknown[]);

    const todayRes = await fetch(
      `${SF_BASE}/sport/${category}/scheduled-events/${today}`,
      { headers: SF_HEADERS, signal: AbortSignal.timeout(12_000) }
    ).then(async (r) => {
      if (!r.ok) return [];
      const j = (await r.json()) as { events?: unknown[] };
      return ((j.events ?? []) as Array<Record<string, unknown>>)
        .filter((e) => (e.tournament as Record<string, unknown>)?.id === tid);
    }).catch(() => [] as unknown[]);

    const merged = [
      ...(liveRes as unknown[]),
      ...(todayRes as unknown[]),
    ];
    allEvents.push(...merged);
  }

  if (allEvents.length === 0) return;

  const snapshot = { sport, fetched_at: now.toISOString(), events: allEvents };

  const snapshotPath = path.join(dir, `${sport}_${ts}.json`);
  const latestPath  = path.join(dir, `${sport}_latest.json`);

  writeJSON(snapshotPath, snapshot);
  writeJSON(latestPath, snapshot);

  logger.info(
    `SofaScore snapshot saved — ${allEvents.length} events → ${latestPath}`,
    `live/${sport}`,
  );
}

/**
 * Start polling SofaScore for live/today's events for `sport` every `intervalMs`.
 * Returns a handle whose `stop()` cancels the timer.
 */
export function pollSofaScore(
  sport: Sport,
  dataDir: string,
  intervalMs: number,
): Poller {
  if (!SF_LIVE_SPORTS[sport]) {
    throw new Error(`SofaScore live not supported for sport "${sport}"`);
  }

  tick(sport, dataDir).catch((err) =>
    logger.error(`SofaScore poll failed: ${String(err)}`, `live/${sport}`),
  );

  const timer = setInterval(() => {
    tick(sport, dataDir).catch((err) =>
      logger.error(`SofaScore poll failed: ${String(err)}`, `live/${sport}`),
    );
  }, intervalMs);

  return {
    stop() {
      clearInterval(timer);
    },
  };
}
