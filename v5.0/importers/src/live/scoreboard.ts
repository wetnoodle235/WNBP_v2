// ──────────────────────────────────────────────────────────
// Live Scoreboard Poller
// ──────────────────────────────────────────────────────────
// Polls ESPN scoreboard at a configurable interval and
// persists timestamped snapshots plus a rolling "latest" file.

import path from "node:path";
import type { Sport } from "../core/types.js";
import { writeJSON } from "../core/io.js";
import { logger } from "../core/logger.js";
import { ESPN_LIVE_SPORTS, fetchScoreboard } from "../providers/espn/live.js";

// ── Types ───────────────────────────────────────────────────

export interface Poller {
  /** Stop the polling loop. */
  stop(): void;
}

// ── Implementation ──────────────────────────────────────────

async function tick(sport: Sport, slug: string, dataDir: string): Promise<void> {
  const data = await fetchScoreboard(sport, slug);
  const now = new Date();
  const ts = now.toISOString().replace(/[:.]/g, "-");

  const dir = path.join(dataDir, "live", "scoreboard");
  const snapshotPath = path.join(dir, `${sport}_${ts}.json`);
  const latestPath = path.join(dir, `${sport}_latest.json`);

  writeJSON(snapshotPath, data);
  writeJSON(latestPath, data);

  logger.info(`Scoreboard poll saved → ${snapshotPath}`, `live/${sport}`);
}

/**
 * Start polling the ESPN scoreboard for `sport` every `intervalMs` ms.
 * Returns a handle whose `stop()` method cancels the timer.
 */
export function pollScoreboard(
  sport: Sport,
  dataDir: string,
  intervalMs: number,
): Poller {
  const slug = ESPN_LIVE_SPORTS[sport];
  if (!slug) {
    throw new Error(`No ESPN live slug for sport "${sport}"`);
  }

  // Fire immediately, then on interval
  tick(sport, slug, dataDir).catch((err) =>
    logger.error(`Scoreboard poll failed: ${String(err)}`, `live/${sport}`),
  );

  const timer = setInterval(() => {
    tick(sport, slug, dataDir).catch((err) =>
      logger.error(`Scoreboard poll failed: ${String(err)}`, `live/${sport}`),
    );
  }, intervalMs);

  return {
    stop() {
      clearInterval(timer);
      logger.info("Scoreboard poller stopped", `live/${sport}`);
    },
  };
}
