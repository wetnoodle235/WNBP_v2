// ──────────────────────────────────────────────────────────
// Live Odds Poller (Multi-Book via ESPN Core API)
// ──────────────────────────────────────────────────────────
// Polls ESPN's Core API for real-time game odds (multiple
// sportsbooks: DraftKings, Caesars, BetMGM, FanDuel, etc.)
// Runs every 5 min during game hours to capture line movement.

import path from "node:path";
import type { Sport } from "../core/types.js";
import { writeJSON } from "../core/io.js";
import { fetchJSON } from "../core/http.js";
import { logger } from "../core/logger.js";
import type { Poller } from "./scoreboard.js";

const ESPN_BASE = "https://sports.core.api.espn.com/v2/sports";
const SITE_API = "https://site.api.espn.com/apis/site/v2/sports";
const RATE_LIMIT = { requests: 3, perMs: 1_000 };

const LIVE_SPORT_MAP: Partial<Record<Sport, { sport: string; league: string }>> = {
  nfl:   { sport: "football",   league: "nfl" },
  nba:   { sport: "basketball", league: "nba" },
  mlb:   { sport: "baseball",   league: "mlb" },
  nhl:   { sport: "hockey",     league: "nhl" },
  ncaaf: { sport: "football",   league: "college-football" },
  ncaab: { sport: "basketball", league: "mens-college-basketball" },
};

async function tick(sport: Sport, mapping: { sport: string; league: string }, dataDir: string): Promise<void> {
  try {
    const scoreboard = await fetchJSON<any>(
      `${SITE_API}/${mapping.sport}/${mapping.league}/scoreboard`,
      `live-odds/${sport}`, RATE_LIMIT,
    );
    const events: Array<{ id: string; name?: string; date?: string }> = (scoreboard?.events ?? []).slice(0, 15);
    if (events.length === 0) return;

    const allLines: Record<string, unknown>[] = [];
    for (const event of events) {
      try {
        const oddsData = await fetchJSON<any>(
          `${ESPN_BASE}/${mapping.sport}/leagues/${mapping.league}/events/${event.id}/competitions/${event.id}/odds?limit=20`,
          `live-odds/${sport}`, RATE_LIMIT,
        );
        const items: any[] = oddsData?.items ?? [];
        for (const o of items) {
          allLines.push({
            event_id: event.id,
            event_name: event.name ?? "",
            event_date: event.date ?? null,
            book_name: o?.provider?.name ?? "unknown",
            spread: o?.spread ?? null,
            over_under: o?.overUnder ?? null,
            home_moneyline: o?.homeTeamOdds?.moneyLine ?? null,
            away_moneyline: o?.awayTeamOdds?.moneyLine ?? null,
            over_odds: o?.overOdds ?? null,
            under_odds: o?.underOdds ?? null,
          });
        }
      } catch { /* skip individual event errors */ }
    }

    if (allLines.length === 0) return;

    const now = new Date();
    const ts = now.toISOString().replace(/[:.]/g, "-");
    const dir = path.join(dataDir, "live", "odds_espn");

    writeJSON(path.join(dir, `${sport}_${ts}.json`), { sport, ts: now.toISOString(), lines: allLines });
    writeJSON(path.join(dir, `${sport}_latest.json`), { sport, ts: now.toISOString(), lines: allLines });

    logger.info(`ESPN odds polled → ${allLines.length} lines (${events.length} events)`, `live-odds/${sport}`);
  } catch (err) {
    logger.warn(`ESPN odds poll failed: ${String(err)}`, `live-odds/${sport}`);
  }
}

export function pollDraftKingsOdds(
  sport: Sport,
  dataDir: string,
  intervalMs: number,
): Poller {
  const mapping = LIVE_SPORT_MAP[sport];
  if (!mapping) throw new Error(`No odds mapping for sport "${sport}"`);

  tick(sport, mapping, dataDir).catch(() => {});

  const timer = setInterval(() => {
    tick(sport, mapping, dataDir).catch(() => {});
  }, intervalMs);

  return {
    stop() {
      clearInterval(timer);
      logger.info("ESPN odds poller stopped", `live-odds/${sport}`);
    },
  };
}

export const DK_LIVE_SPORTS = LIVE_SPORT_MAP;

