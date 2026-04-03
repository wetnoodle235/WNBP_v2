// ──────────────────────────────────────────────────────────
// ActionNetwork Provider
// ──────────────────────────────────────────────────────────
// Scrapes public betting trend percentages from ActionNetwork.
// Captures the "sharp vs. public" split that's missing from
// standard odds providers. Data includes:
// - % of bets on each side (public money)
// - % of handle (dollar-weighted sharp money indicator)
// No API key required — uses publicly accessible HTML pages.

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchText } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "actionnetwork";
const BASE_URL = "https://www.actionnetwork.com";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 3_000 };

const BROWSER_HEADERS = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.5",
  "Accept-Encoding": "gzip, deflate, br",
};

const SPORT_PATHS: Partial<Record<Sport, string>> = {
  nfl: "nfl",
  nba: "nba",
  mlb: "mlb",
  nhl: "nhl",
  ncaab: "ncaab",
  ncaaf: "ncaaf",
};

const SUPPORTED_SPORTS = Object.keys(SPORT_PATHS) as Sport[];
const ENDPOINTS = ["trends"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

/** Extract embedded JSON from Next.js __NEXT_DATA__ script tag */
function extractNextData(html: string): any | null {
  const match = /<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/.exec(html);
  if (!match) return null;
  try {
    return JSON.parse(match[1]!);
  } catch {
    return null;
  }
}

/** Extract bet percentage values from structured Next.js page data */
function extractBettingTrends(nextData: any): any[] {
  const games: any[] = [];
  try {
    const pageProps = nextData?.props?.pageProps ?? {};
    const rawGames = pageProps?.games ?? pageProps?.data?.games ?? [];

    for (const game of rawGames) {
      const id = game?.id ?? game?.game_id;
      const homeTeam = game?.home_team?.abbrev ?? game?.teams?.home?.name ?? null;
      const awayTeam = game?.away_team?.abbrev ?? game?.teams?.away?.name ?? null;
      const startTime = game?.start_time ?? game?.scheduled ?? null;
      const spread = game?.spread ?? null;
      const total = game?.total ?? null;

      // Bet percentages are nested under each market
      const bets = game?.consensus ?? game?.betting_percentages ?? {};

      games.push({
        game_id: id,
        home_team: homeTeam,
        away_team: awayTeam,
        start_time: startTime,
        spread_home_bet_pct: bets?.spread?.home_pct ?? bets?.spread?.home ?? null,
        spread_away_bet_pct: bets?.spread?.away_pct ?? bets?.spread?.away ?? null,
        spread_home_handle_pct: bets?.spread?.home_handle ?? null,
        spread_away_handle_pct: bets?.spread?.away_handle ?? null,
        ml_home_bet_pct: bets?.moneyline?.home_pct ?? bets?.ml?.home ?? null,
        ml_away_bet_pct: bets?.moneyline?.away_pct ?? bets?.ml?.away ?? null,
        over_bet_pct: bets?.total?.over_pct ?? bets?.total?.over ?? null,
        under_bet_pct: bets?.total?.under_pct ?? bets?.total?.under ?? null,
        over_handle_pct: bets?.total?.over_handle ?? null,
        under_handle_pct: bets?.total?.under_handle ?? null,
        current_spread: spread,
        current_total: total,
      });
    }
  } catch {
    // page structure changed — return empty
  }
  return games;
}

async function importTrends(ctx: EndpointCtx): Promise<EndpointResult> {
  const sportPath = SPORT_PATHS[ctx.sport];
  if (!sportPath || ctx.dryRun) return { filesWritten: 0, errors: [] };

  const errors: string[] = [];
  try {
    const url = `${BASE_URL}/${sportPath}/odds`;
    const html = await fetchText(url, NAME, RATE_LIMIT, { headers: BROWSER_HEADERS });

    const nextData = extractNextData(html);
    const games = nextData ? extractBettingTrends(nextData) : [];

    const dateStr = new Date().toISOString().slice(0, 10);
    const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `trends_${dateStr}.json`);
    writeJSON(outPath, {
      source: NAME,
      sport: ctx.sport,
      season: String(ctx.season),
      date: dateStr,
      count: games.length,
      games,
      fetched_at: new Date().toISOString(),
    });

    logger.progress(NAME, ctx.sport, "trends", `${games.length} games with bet percentages`);
    return { filesWritten: 1, errors };
  } catch (err) {
    const msg = `trends/${ctx.sport}: ${err instanceof Error ? err.message : String(err)}`;
    logger.warn(msg, NAME);
    return { filesWritten: 0, errors: [msg] };
  }
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  trends: importTrends,
};

const actionnetwork: Provider = {
  name: NAME,
  label: "ActionNetwork Betting Trends",
  sports: SUPPORTED_SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s) => SUPPORTED_SPORTS.includes(s))
      : [...SUPPORTED_SPORTS];
    const activeEndpoints = (opts.endpoints.length
      ? opts.endpoints.filter((e) => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS]) as Endpoint[];

    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        for (const ep of activeEndpoints) {
          try {
            const r = await ENDPOINT_FNS[ep]({ sport, season, dataDir: opts.dataDir, dryRun: opts.dryRun });
            totalFiles += r.filesWritten;
            allErrors.push(...r.errors);
          } catch (err) {
            allErrors.push(`${ep}/${sport}/${season}: ${err instanceof Error ? err.message : String(err)}`);
          }
        }
      }
    }

    return { provider: NAME, sport: activeSports.length === 1 ? activeSports[0]! : "multi", filesWritten: totalFiles, errors: allErrors, durationMs: Date.now() - start };
  },
};

export default actionnetwork;
