// ──────────────────────────────────────────────────────────
// Open-Meteo Weather Provider (free, no API key required)
// ──────────────────────────────────────────────────────────
// Complements the Visual Crossing weather provider with a fully
// free, no-key-required alternative. Same game-driven approach:
// - Scans ESPN scoreboards for upcoming/recent games
// - Geocodes venue cities via Open-Meteo geocoding API
// - Fetches daily weather forecasts for each game date

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON, fileExists } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "openmeteo";
const GEO_URL = "https://geocoding-api.open-meteo.com/v1/search";
const FORECAST_URL = "https://api.open-meteo.com/v1/forecast";
const SITE_API = "https://site.api.espn.com/apis/site/v2/sports";

const RATE_LIMIT: RateLimitConfig = { requests: 10, perMs: 1_000 };

const SPORTS: readonly Sport[] = [
  "nfl", "ncaaf", "mlb",
  "epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl",
  "ligamx", "europa", "eredivisie", "primeiraliga",
  "championship", "bundesliga2", "serieb", "ligue2",
  "worldcup", "euros",
  "f1", "indycar",
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
  golf: "golf/pga",
  lpga: "golf/lpga",
};

/** WMO Weather Interpretation Codes → human readable */
const WMO: Record<number, string> = {
  0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
  45: "Fog", 48: "Icy fog",
  51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
  61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
  71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
  77: "Snow grains",
  80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
  85: "Slight snow showers", 86: "Heavy snow showers",
  95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail",
};

const ENDPOINTS = ["game_weather"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx {
  sport: Sport;
  season: number;
  dataDir: string;
  dryRun: boolean;
  recentDays?: number;
}
interface EndpointResult { filesWritten: number; errors: string[] }

function dateWindow(recentDays?: number): string[] {
  const days = Math.max(1, recentDays ?? 1);
  const now = new Date();
  now.setUTCHours(0, 0, 0, 0);
  return Array.from({ length: days }, (_, i) => {
    const d = new Date(now);
    d.setUTCDate(now.getUTCDate() - (days - 1 - i));
    return d.toISOString().slice(0, 10);
  });
}

function toIsoDate(v: unknown): string | null {
  const s = String(v ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10);
  return null;
}

async function geocodeCity(
  city: string,
  state: string | null,
  country: string | null,
  cache: Map<string, { lat: number; lon: number } | null>,
): Promise<{ lat: number; lon: number } | null> {
  const key = `${city}|${state ?? ""}|${country ?? ""}`;
  if (cache.has(key)) return cache.get(key)!;

  try {
    const data = await fetchJSON<any>(
      `${GEO_URL}?name=${encodeURIComponent(city)}&count=5&language=en&format=json`,
      NAME, RATE_LIMIT,
    );
    const results: any[] = data?.results ?? [];
    if (!results.length) { cache.set(key, null); return null; }

    let best = results[0];
    if (state || country) {
      const match = results.find((r: any) =>
        (!state || (r.admin1 ?? "").toLowerCase().includes(state.toLowerCase())) &&
        (!country || (r.country_code ?? "").toLowerCase() === (country ?? "").toLowerCase().slice(0, 2))
      );
      if (match) best = match;
    }
    const coords = { lat: best.latitude as number, lon: best.longitude as number };
    cache.set(key, coords);
    return coords;
  } catch {
    cache.set(key, null);
    return null;
  }
}

async function importGameWeather(
  ctx: EndpointCtx,
  geocodeCache: Map<string, { lat: number; lon: number } | null>,
): Promise<EndpointResult> {
  const sportSlug = SPORT_SLUGS[ctx.sport];
  if (!sportSlug) return { filesWritten: 0, errors: [`No slug for ${ctx.sport}`] };

  const errors: string[] = [];
  let filesWritten = 0;

  for (const dateIso of dateWindow(ctx.recentDays)) {
    const dateNum = dateIso.replace(/-/g, "");
    let events: any[] = [];
    try {
      const data = await fetchJSON<any>(
        `${SITE_API}/${sportSlug}/scoreboard?dates=${dateNum}&limit=300`,
        NAME, RATE_LIMIT,
      );
      events = data?.events ?? [];
    } catch (err) {
      errors.push(`scoreboard/${ctx.sport}/${dateIso}: ${err instanceof Error ? err.message : String(err)}`);
      continue;
    }

    for (const event of events) {
      const gameId = String(event?.id ?? "").trim();
      if (!gameId) continue;

      const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, "dates", dateIso, `${gameId}.json`);
      if (fileExists(outPath) || ctx.dryRun) continue;

      const comp = Array.isArray(event?.competitions) ? event.competitions[0] : null;
      const venue = comp?.venue ?? null;
      const addr = venue?.address ?? {};
      const city = String(addr?.city ?? "").trim();
      if (!city) continue;

      const state = String(addr?.state ?? "").trim() || null;
      const country = String(addr?.country ?? "").trim() || null;
      const venueName = String(venue?.fullName ?? venue?.name ?? "").trim() || null;
      const dome = Boolean(venue?.indoor) || /dome|indoor/i.test(venueName ?? "");
      const eventDate = toIsoDate(event?.date) ?? dateIso;

      try {
        const coords = await geocodeCity(city, state, country, geocodeCache);
        if (!coords) { errors.push(`geocode_failed/${city}`); continue; }

        const weatherData = await fetchJSON<any>(
          `${FORECAST_URL}?latitude=${coords.lat}&longitude=${coords.lon}` +
          `&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,` +
          `windspeed_10m_max,winddirection_10m_dominant,weathercode,precipitation_probability_max` +
          `&temperature_unit=fahrenheit&windspeed_unit=mph&precipitation_unit=inch` +
          `&timezone=auto&start_date=${eventDate}&end_date=${eventDate}`,
          NAME, RATE_LIMIT,
        );

        const d = weatherData?.daily ?? {};
        const code = d?.weathercode?.[0] ?? null;

        writeJSON(outPath, {
          source: NAME,
          provider: "open-meteo",
          sport: ctx.sport,
          season: String(ctx.season),
          game_id: gameId,
          date: eventDate,
          venue: venueName,
          city, state, country,
          latitude: coords.lat,
          longitude: coords.lon,
          dome,
          temp_max_f: d?.temperature_2m_max?.[0] ?? null,
          temp_min_f: d?.temperature_2m_min?.[0] ?? null,
          wind_mph: d?.windspeed_10m_max?.[0] ?? null,
          wind_direction_deg: d?.winddirection_10m_dominant?.[0] ?? null,
          precipitation_in: d?.precipitation_sum?.[0] ?? null,
          precipitation_prob_pct: d?.precipitation_probability_max?.[0] ?? null,
          weather_code: code,
          condition: code != null ? (WMO[code] ?? "Unknown") : null,
          fetched_at: new Date().toISOString(),
        });
        filesWritten++;
      } catch (err) {
        errors.push(`weather/${gameId}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }

  logger.progress(NAME, ctx.sport, "game_weather", `${filesWritten} files written`);
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx, cache: Map<string, any>) => Promise<EndpointResult>> = {
  game_weather: importGameWeather,
};

const openmeteo: Provider = {
  name: NAME,
  label: "Open-Meteo Weather",
  sports: SPORTS,
  requiresKey: false,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: true,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    let totalFiles = 0;
    const allErrors: string[] = [];

    const activeSports = opts.sports.length
      ? opts.sports.filter((s): s is Sport => (SPORTS as readonly Sport[]).includes(s))
      : [...SPORTS];
    const activeEndpoints = (opts.endpoints.length
      ? opts.endpoints.filter((e) => (ENDPOINTS as readonly string[]).includes(e))
      : [...ENDPOINTS]) as Endpoint[];

    const geocodeCache = new Map<string, { lat: number; lon: number } | null>();

    for (const sport of activeSports) {
      for (const season of opts.seasons) {
        for (const ep of activeEndpoints) {
          try {
            const r = await ENDPOINT_FNS[ep]({ sport, season, dataDir: opts.dataDir, dryRun: opts.dryRun, recentDays: opts.recentDays }, geocodeCache);
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

export default openmeteo;
