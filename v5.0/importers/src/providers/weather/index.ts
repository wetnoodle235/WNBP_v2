// ──────────────────────────────────────────────────────────
// Visual Crossing Weather Provider
// ──────────────────────────────────────────────────────────
// Game-driven weather ingestion:
// - Scans ESPN scoreboards for today (or --recent-days window)
// - Collects weather only for cities that actually have games
// - De-dupes city+date lookups across sports to avoid duplicate API calls

import type { ImportOptions, ImportResult, Provider, RateLimitConfig, Sport } from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "weather";
const BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline";
const SITE_API = "https://site.api.espn.com/apis/site/v2/sports";
const API_KEY = process.env.VISUAL_CROSSING_KEY ?? "";

const SPORTS: readonly Sport[] = [
  "nfl", "ncaaf", "mlb",
  "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl",
  "ligamx", "europa", "eredivisie", "primeiraliga", "championship", "bundesliga2", "serieb", "ligue2",
  "worldcup", "euros",
  "f1", "indycar",
  "atp", "wta",
  "golf", "lpga",
] as const;

const SPORT_SLUGS: Partial<Record<Sport, string>> = {
  nfl: "football/nfl",
  ncaaf: "football/college-football",
  mlb: "baseball/mlb",
  epl: "soccer/eng.1",
  laliga: "soccer/esp.1",
  bundesliga: "soccer/ger.1",
  seriea: "soccer/ita.1",
  ligue1: "soccer/fra.1",
  mls: "soccer/usa.1",
  ucl: "soccer/uefa.champions",
  nwsl: "soccer/usa.nwsl",
  ligamx: "soccer/mex.1",
  europa: "soccer/uefa.europa",
  eredivisie: "soccer/ned.1",
  primeiraliga: "soccer/por.1",
  championship: "soccer/eng.2",
  bundesliga2: "soccer/ger.2",
  serieb: "soccer/ita.2",
  ligue2: "soccer/fra.2",
  worldcup: "soccer/fifa.world",
  euros: "soccer/uefa.euro",
  f1: "racing/f1",
  indycar: "racing/irl",
  atp: "tennis/atp",
  wta: "tennis/wta",
  golf: "golf/pga",
  lpga: "golf/lpga",
};

const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

const ENDPOINTS = ["game_weather", "venue_weather"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
  recentDays?: number;
}

interface EndpointResult {
  filesWritten: number;
  errors: string[];
}

interface GameWeatherTarget {
  sport: Sport;
  season: number;
  gameId: string;
  dateIso: string;
  city: string;
  state: string | null;
  country: string | null;
  venueName: string | null;
  dome: boolean;
  citySlug: string;
  cityDateKey: string;
  locationQuery: string;
}

function slugify(input: string): string {
  return input
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80);
}

function toIsoDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{8}$/.test(text)) return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  return null;
}

function todayIsoUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

function dateKeysForRecentDays(recentDays?: number): string[] {
  const days = Math.max(1, recentDays ?? 1);
  const now = new Date();
  now.setUTCHours(0, 0, 0, 0);

  const out: string[] = [];
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(now);
    d.setUTCDate(now.getUTCDate() - i);
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

function scoreBoardDate(dateIso: string): string {
  return dateIso.replace(/-/g, "");
}

function numberOrNull(v: unknown): number | null {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function windDirectionCardinal(degrees: number | null): string | null {
  if (degrees == null) return null;
  const dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"];
  const idx = Math.round((((degrees % 360) + 360) % 360) / 22.5) % 16;
  return dirs[idx] ?? null;
}

function cityDateKey(dateIso: string, city: string, state: string | null, country: string | null): string {
  return `${dateIso}|${city.toLowerCase()}|${(state ?? "").toLowerCase()}|${(country ?? "").toLowerCase()}`;
}

function buildLocationQuery(city: string, state: string | null, country: string | null): string {
  const parts = [city, state, country].filter((p): p is string => !!p && p.trim().length > 0);
  return parts.join(",");
}

function weatherCityPath(ctx: EndpointCtx, target: GameWeatherTarget): string {
  return rawPath(
    ctx.dataDir,
    NAME,
    ctx.sport,
    ctx.season,
    "dates",
    target.dateIso,
    "cities",
    target.citySlug,
    "weather.json",
  );
}

function weatherGamePath(ctx: EndpointCtx, target: GameWeatherTarget): string {
  return rawPath(
    ctx.dataDir,
    NAME,
    ctx.sport,
    ctx.season,
    "dates",
    target.dateIso,
    "games",
    `${target.gameId}.json`,
  );
}

async function fetchScoreboardGames(sport: Sport, season: number, dateIso: string): Promise<GameWeatherTarget[]> {
  const sportPath = SPORT_SLUGS[sport];
  if (!sportPath) return [];

  const url = `${SITE_API}/${sportPath}/scoreboard?dates=${scoreBoardDate(dateIso)}&limit=300`;
  const data = await fetchJSON<any>(url, NAME, RATE_LIMIT);
  const events = Array.isArray(data?.events) ? data.events : [];
  const targets: GameWeatherTarget[] = [];

  for (const event of events) {
    const eventSeasonYear = Number(event?.season?.year);
    if (Number.isFinite(eventSeasonYear) && eventSeasonYear !== season) continue;

    const eventDate = toIsoDate(event?.date) ?? dateIso;
    const gameId = String(event?.id ?? "").trim();
    if (!gameId) continue;

    const competition = Array.isArray(event?.competitions) ? event.competitions[0] : null;
    const venue = competition?.venue ?? null;
    const addr = venue?.address ?? {};

    const city = String(addr?.city ?? "").trim();
    if (!city) continue;

    const state = String(addr?.state ?? "").trim() || null;
    const country = String(addr?.country ?? "").trim() || null;
    const locationQuery = buildLocationQuery(city, state, country);
    if (!locationQuery) continue;

    const venueName = String(venue?.fullName ?? venue?.name ?? "").trim() || null;
    const citySlug = slugify([city, state ?? country ?? ""].filter(Boolean).join("_"));
    const dome = Boolean(venue?.indoor)
      || /dome|indoor|roof closed/i.test(`${venueName ?? ""} ${venue?.roofType ?? ""}`);

    targets.push({
      sport,
      season,
      gameId,
      dateIso: eventDate,
      city,
      state,
      country,
      venueName,
      dome,
      citySlug: citySlug || "unknown_city",
      cityDateKey: cityDateKey(eventDate, city, state, country),
      locationQuery,
    });
  }

  return targets;
}

async function fetchCityWeather(locationQuery: string, dateIso: string): Promise<any> {
  const encodedLocation = encodeURIComponent(locationQuery);
  const url = `${BASE_URL}/${encodedLocation}/${dateIso}/${dateIso}?unitGroup=us&include=days,current&key=${API_KEY}&contentType=json`;
  return fetchJSON<any>(url, NAME, RATE_LIMIT);
}

function buildGameWeatherRecord(target: GameWeatherTarget, cityPayload: any): Record<string, unknown> {
  const day = Array.isArray(cityPayload?.days) && cityPayload.days.length > 0 ? cityPayload.days[0] : {};
  const current = cityPayload?.currentConditions ?? {};

  const windDirDeg = numberOrNull(day?.winddir ?? current?.winddir);

  return {
    source: NAME,
    sport: target.sport,
    season: String(target.season),
    game_id: target.gameId,
    date: target.dateIso,
    venue: target.venueName,
    city: target.city,
    state: target.state,
    country: target.country,
    temp_f: numberOrNull(day?.temp ?? current?.temp),
    wind_mph: numberOrNull(day?.windspeed ?? current?.windspeed),
    wind_direction: windDirectionCardinal(windDirDeg),
    humidity_pct: numberOrNull(day?.humidity ?? current?.humidity),
    precipitation: numberOrNull(day?.precipprob ?? day?.precip),
    condition: String(day?.conditions ?? current?.conditions ?? "").trim() || null,
    dome: target.dome,
    provider: "visualcrossing",
    fetched_at: new Date().toISOString(),
  };
}

async function importGameWeather(
  ctx: EndpointCtx,
  globalCityCache: Map<string, any>,
): Promise<EndpointResult> {
  const errors: string[] = [];
  let filesWritten = 0;

  const dateWindow = dateKeysForRecentDays(ctx.recentDays);
  const targets: GameWeatherTarget[] = [];

  for (const dateIso of dateWindow) {
    try {
      const rows = await fetchScoreboardGames(ctx.sport, ctx.season, dateIso);
      targets.push(...rows);
    } catch (err) {
      errors.push(`scoreboard/${ctx.sport}/${dateIso}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  if (targets.length === 0) {
    logger.info(`No games found for ${ctx.sport} in ${dateWindow[0]}..${dateWindow[dateWindow.length - 1]}`, NAME);
    return { filesWritten: 0, errors };
  }

  const sportCityWritten = new Set<string>();
  const gameTargets = new Map<string, GameWeatherTarget>();
  for (const target of targets) {
    gameTargets.set(`${target.dateIso}|${target.gameId}`, target);
  }

  for (const target of gameTargets.values()) {
    const gameOut = weatherGamePath(ctx, target);
    if (fileExists(gameOut)) continue;
    if (ctx.dryRun) continue;

    let cityPayload = globalCityCache.get(target.cityDateKey);
    if (!cityPayload) {
      try {
        cityPayload = await fetchCityWeather(target.locationQuery, target.dateIso);
        globalCityCache.set(target.cityDateKey, cityPayload);
      } catch (err) {
        errors.push(`weather/${target.cityDateKey}: ${err instanceof Error ? err.message : String(err)}`);
        continue;
      }
    }

    try {
      const cityOut = weatherCityPath(ctx, target);
      const cityWriteKey = `${target.dateIso}|${target.citySlug}`;
      if (!sportCityWritten.has(cityWriteKey) && !fileExists(cityOut)) {
        writeJSON(cityOut, {
          source: NAME,
          provider: "visualcrossing",
          sport: ctx.sport,
          season: String(ctx.season),
          date: target.dateIso,
          city: target.city,
          state: target.state,
          country: target.country,
          location_query: target.locationQuery,
          fetched_at: new Date().toISOString(),
          weather: cityPayload,
        });
        filesWritten++;
        sportCityWritten.add(cityWriteKey);
      }

      writeJSON(gameOut, buildGameWeatherRecord(target, cityPayload));
      filesWritten++;
    } catch (err) {
      errors.push(`write/${ctx.sport}/${target.gameId}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  logger.progress(NAME, ctx.sport, "game_weather", `${targets.length} games scanned, ${filesWritten} files written`);
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx, globalCityCache: Map<string, any>) => Promise<EndpointResult>> = {
  game_weather: importGameWeather,
  venue_weather: importGameWeather,
};

const weather: Provider = {
  name: NAME,
  label: "Visual Crossing Weather",
  sports: SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: !!API_KEY,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    if (!API_KEY) {
      logger.error("VISUAL_CROSSING_KEY not set — skipping", NAME);
      return {
        provider: NAME,
        sport: "multi",
        filesWritten: 0,
        errors: ["Missing VISUAL_CROSSING_KEY"],
        durationMs: 0,
      };
    }

    const activeSports = opts.sports.length
      ? opts.sports.filter((s): s is Sport => (SPORTS as readonly Sport[]).includes(s))
      : [...SPORTS];

    const activeEndpoints = opts.endpoints.length
      ? opts.endpoints.filter((e): e is Endpoint => (ENDPOINTS as readonly string[]).includes(e))
      : ["game_weather"];

    const globalCityCache = new Map<string, any>();

    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        logger.info(`${sport} / ${season}`, NAME);
        for (const ep of activeEndpoints) {
          try {
            const result = await ENDPOINT_FNS[ep]({
              sport,
              season,
              dataDir: opts.dataDir,
              dryRun: opts.dryRun,
              recentDays: opts.recentDays,
            }, globalCityCache);
            totalFiles += result.filesWritten;
            allErrors.push(...result.errors);
          } catch (err) {
            const msg = `${ep}/${sport}/${season}: ${err instanceof Error ? err.message : String(err)}`;
            logger.error(msg, NAME);
            allErrors.push(msg);
          }
        }
      }
    }

    return {
      provider: NAME,
      sport: activeSports.length === 1 ? activeSports[0] : "multi",
      filesWritten: totalFiles,
      errors: allErrors,
      durationMs: Date.now() - start,
    };
  },
};

export default weather;
