// ──────────────────────────────────────────────────────────
// Steam Gaming Provider
// ──────────────────────────────────────────────────────────
// Steam Web API — concurrent player counts, game metadata,
// and peak player stats for major esports titles.
// No API key required. Free public endpoint.

import type {
  ImportOptions, ImportResult, Provider, RateLimitConfig, Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "steam";
const RATE_LIMIT: RateLimitConfig = { requests: 2, perMs: 1_000 };
const STATS_BASE = "https://api.steampowered.com";
const STORE_BASE = "https://store.steampowered.com/api";

// Steam App IDs for esports-relevant games
const SPORT_GAMES: Partial<Record<Sport, { appid: number; name: string }[]>> = {
  csgo:     [{ appid: 730,     name: "Counter-Strike 2" }],
  dota2:    [{ appid: 570,     name: "Dota 2" }],
  valorant: [{ appid: 0,       name: "Valorant (not on Steam)" }],
};

// Only sports with valid Steam App IDs
const STEAM_GAMES = [
  { sport: "csgo" as Sport,  appid: 730, name: "Counter-Strike 2" },
  { sport: "dota2" as Sport, appid: 570, name: "Dota 2" },
];

const SUPPORTED_SPORTS: Sport[] = STEAM_GAMES.map((g) => g.sport);

interface PlayerCountResponse {
  response: { player_count: number; result: number };
}

interface AppDetails {
  [appid: string]: {
    success: boolean;
    data?: {
      name: string;
      short_description: string;
      genres?: Array<{ id: string; description: string }>;
      developers?: string[];
      publishers?: string[];
      release_date?: { date: string };
      metacritic?: { score: number };
    };
  };
}

async function fetchCurrentPlayers(appid: number): Promise<number | null> {
  try {
    const data = await fetchJSON<PlayerCountResponse>(
      `${STATS_BASE}/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=${appid}`,
      NAME, RATE_LIMIT,
    );
    return data?.response?.result === 1 ? (data.response.player_count ?? null) : null;
  } catch {
    return null;
  }
}

async function fetchAppDetails(appid: number): Promise<AppDetails[string]["data"] | null> {
  try {
    const data = await fetchJSON<AppDetails>(
      `${STORE_BASE}/appdetails?appids=${appid}&filters=basic,genres,metacritic`,
      NAME, RATE_LIMIT,
    );
    return data?.[String(appid)]?.data ?? null;
  } catch {
    return null;
  }
}

const steam: Provider = {
  name: NAME,
  label: "Steam Gaming (concurrent players + metadata)",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ["player_counts", "app_details"],
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let filesWritten = 0;
    const errors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];

    const season = Math.max(...opts.seasons);
    const dateStr = new Date().toISOString().slice(0, 10);

    const activeGames = STEAM_GAMES.filter((g) => activeSports.includes(g.sport));

    for (const game of activeGames) {
      try {
        const [playerCount, appDetails] = await Promise.all([
          fetchCurrentPlayers(game.appid),
          fetchAppDetails(game.appid),
        ]);

        if (opts.dryRun) continue;

        const outPath = rawPath(opts.dataDir, NAME, game.sport, season, `steam_${dateStr}.json`);
        writeJSON(outPath, {
          source: NAME,
          sport: game.sport,
          game: game.name,
          app_id: game.appid,
          season: String(season),
          date: dateStr,
          current_players: playerCount,
          app_details: appDetails ? {
            description: appDetails.short_description,
            genres: appDetails.genres?.map((g) => g.description),
            developers: appDetails.developers,
            publishers: appDetails.publishers,
            release_date: appDetails.release_date?.date,
            metacritic_score: appDetails.metacritic?.score,
          } : null,
          fetched_at: new Date().toISOString(),
        });
        filesWritten++;
        logger.progress(NAME, game.sport, "player_counts",
          `${playerCount?.toLocaleString() ?? "N/A"} concurrent players in ${game.name}`);
      } catch (err) {
        const msg = `${game.sport}/${game.name}: ${err instanceof Error ? err.message : String(err)}`;
        logger.warn(msg, NAME);
        errors.push(msg);
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0]! : "multi",
      filesWritten, errors, durationMs: Date.now() - start,
    };
  },
};

export default steam;
