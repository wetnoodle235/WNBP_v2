// ──────────────────────────────────────────────────────────
// Ticketmaster Discovery API Provider
// ──────────────────────────────────────────────────────────
// Fetches sports event listings from Ticketmaster including:
// - Primary market ticket availability and price ranges
// - Event demand signals (on-sale status, sellout indicators)
// - Venue capacity context
// Requires: TICKETMASTER_API_KEY (free tier at developer.ticketmaster.com)

import type {
  ImportOptions,
  ImportResult,
  Provider,
  RateLimitConfig,
  Sport,
} from "../../core/types.js";
import { fetchJSON } from "../../core/http.js";
import { rawPath, writeJSON } from "../../core/io.js";
import { logger } from "../../core/logger.js";

const NAME = "ticketmaster";
const BASE_URL = "https://app.ticketmaster.com/discovery/v2";
const API_KEY = process.env.TICKETMASTER_API_KEY ?? "";
const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

/** Ticketmaster segment/genre IDs for sports classifications */
const SPORT_KEYWORDS: Partial<Record<Sport, { keyword: string; segmentId: string }>> = {
  nfl: { keyword: "NFL football", segmentId: "KZFzniwnSyZfZ7v7nE" },
  nba: { keyword: "NBA basketball", segmentId: "KZFzniwnSyZfZ7v7nE" },
  mlb: { keyword: "MLB baseball", segmentId: "KZFzniwnSyZfZ7v7nE" },
  nhl: { keyword: "NHL hockey", segmentId: "KZFzniwnSyZfZ7v7nE" },
  ncaab: { keyword: "college basketball NCAA", segmentId: "KZFzniwnSyZfZ7v7nE" },
  ncaaf: { keyword: "college football NCAA", segmentId: "KZFzniwnSyZfZ7v7nE" },
  wnba: { keyword: "WNBA basketball", segmentId: "KZFzniwnSyZfZ7v7nE" },
  mls: { keyword: "MLS soccer", segmentId: "KZFzniwnSyZfZ7v7nE" },
  epl: { keyword: "Premier League soccer", segmentId: "KZFzniwnSyZfZ7v7nE" },
  ufc: { keyword: "UFC MMA", segmentId: "KZFzniwnSyZfZ7v7nE" },
  f1: { keyword: "Formula 1 Grand Prix", segmentId: "KZFzniwnSyZfZ7v7nE" },
  golf: { keyword: "PGA golf tournament", segmentId: "KZFzniwnSyZfZ7v7nE" },
  atp: { keyword: "ATP tennis", segmentId: "KZFzniwnSyZfZ7v7nE" },
  wta: { keyword: "WTA tennis", segmentId: "KZFzniwnSyZfZ7v7nE" },
};

const SUPPORTED_SPORTS = Object.keys(SPORT_KEYWORDS) as Sport[];
const ENDPOINTS = ["events"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

function normalizeEvent(event: any): Record<string, unknown> {
  const priceRanges: any[] = event.priceRanges ?? [];
  const prices = priceRanges.map((pr: any) => ({
    type: pr.type,
    currency: pr.currency,
    min: pr.min,
    max: pr.max,
  }));

  const venue = (event._embedded?.venues ?? [])[0] ?? {};
  const startDate = event.dates?.start;
  const sales = event.sales?.public ?? {};

  return {
    id: event.id,
    name: event.name,
    url: event.url,
    status: event.dates?.status?.code ?? null,
    local_date: startDate?.localDate ?? null,
    local_time: startDate?.localTime ?? null,
    utc_datetime: startDate?.dateTime ?? null,
    venue_id: venue.id ?? null,
    venue_name: venue.name ?? null,
    venue_city: venue.city?.name ?? null,
    venue_state: venue.state?.stateCode ?? null,
    venue_country: venue.country?.countryCode ?? null,
    venue_capacity: venue.generalInfo?.generalRule ? null : (venue.capacity ?? null),
    sale_start: sales.startDateTime ?? null,
    sale_end: sales.endDateTime ?? null,
    presale_active: Array.isArray(event.sales?.presales) && event.sales.presales.length > 0,
    price_ranges: prices,
    price_min: prices.length > 0 ? Math.min(...prices.map((p: any) => p.min ?? Infinity)) : null,
    price_max: prices.length > 0 ? Math.max(...prices.map((p: any) => p.max ?? 0)) : null,
    classifications: (event.classifications ?? []).map((c: any) => ({
      segment: c.segment?.name,
      genre: c.genre?.name,
      sub_genre: c.subGenre?.name,
    })),
    images: (event.images ?? []).slice(0, 3).map((img: any) => ({ url: img.url, width: img.width, height: img.height })),
  };
}

async function importEvents(ctx: EndpointCtx): Promise<EndpointResult> {
  const cfg = SPORT_KEYWORDS[ctx.sport];
  if (!cfg) return { filesWritten: 0, errors: [`No config for ${ctx.sport}`] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const errors: string[] = [];
  let filesWritten = 0;

  try {
    const params = new URLSearchParams({
      apikey: API_KEY,
      keyword: cfg.keyword,
      segmentId: cfg.segmentId,
      size: "200",
      sort: "date,asc",
      countryCode: "US",
    });
    const url = `${BASE_URL}/events.json?${params}`;
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT);

    const events: any[] = data?._embedded?.events ?? [];
    const normalized = events.map(normalizeEvent);

    const dateStr = new Date().toISOString().slice(0, 10);
    const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `events_${dateStr}.json`);
    writeJSON(outPath, {
      source: NAME,
      sport: ctx.sport,
      season: String(ctx.season),
      date: dateStr,
      count: normalized.length,
      events: normalized,
      fetched_at: new Date().toISOString(),
    });
    filesWritten++;
    logger.progress(NAME, ctx.sport, "events", `${normalized.length} events`);
  } catch (err) {
    errors.push(`events/${ctx.sport}: ${err instanceof Error ? err.message : String(err)}`);
  }
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  events: importEvents,
};

const ticketmaster: Provider = {
  name: NAME,
  label: "Ticketmaster Discovery",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: !!API_KEY,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    if (!API_KEY) {
      logger.error("TICKETMASTER_API_KEY not set — skipping", NAME);
      return { provider: NAME, sport: "multi", filesWritten: 0, errors: ["Missing TICKETMASTER_API_KEY"], durationMs: 0 };
    }

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

export default ticketmaster;
