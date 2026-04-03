// ──────────────────────────────────────────────────────────
// SeatGeek Provider
// ──────────────────────────────────────────────────────────
// Fetches secondary market event data from SeatGeek including:
// - Secondary market pricing (avg, median, low ticket prices)
// - Listing volume as a proxy for fan demand
// - Performer & venue information
// Requires: SEATGEEK_CLIENT_ID (free at seatgeek.com/developers)

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

const NAME = "seatgeek";
const BASE_URL = "https://api.seatgeek.com/2";
const CLIENT_ID = process.env.SEATGEEK_CLIENT_ID ?? "";
const RATE_LIMIT: RateLimitConfig = { requests: 5, perMs: 1_000 };

/** SeatGeek taxonomy IDs for sports */
const SPORT_TAXONOMY: Partial<Record<Sport, { taxonomyId: number; query: string }>> = {
  nfl: { taxonomyId: 27, query: "NFL" },
  nba: { taxonomyId: 21, query: "NBA" },
  mlb: { taxonomyId: 24, query: "MLB" },
  nhl: { taxonomyId: 28, query: "NHL" },
  ncaab: { taxonomyId: 22, query: "NCAA Basketball" },
  ncaaf: { taxonomyId: 29, query: "NCAA Football" },
  mls: { taxonomyId: 26, query: "MLS" },
  ufc: { taxonomyId: 30, query: "UFC" },
  golf: { taxonomyId: 47, query: "Golf" },
  atp: { taxonomyId: 32, query: "Tennis" },
  wta: { taxonomyId: 32, query: "Tennis Women" },
  f1: { taxonomyId: 31, query: "Formula 1" },
};

const SUPPORTED_SPORTS = Object.keys(SPORT_TAXONOMY) as Sport[];
const ENDPOINTS = ["events"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

function normalizeEvent(event: any): Record<string, unknown> {
  const stats = event.stats ?? {};
  return {
    id: event.id,
    title: event.title,
    short_title: event.short_title,
    type: event.type,
    url: event.url,
    datetime_utc: event.datetime_utc,
    datetime_local: event.datetime_local,
    visible_until_utc: event.visible_until_utc,
    listing_count: stats.listing_count ?? null,
    average_price: stats.average_price ?? null,
    median_price: stats.median_price ?? null,
    lowest_price: stats.lowest_price ?? null,
    highest_price: stats.highest_price ?? null,
    lowest_price_good_deals: stats.lowest_price_good_deals ?? null,
    score: event.score,
    announce_date: event.announce_date,
    venue: event.venue ? {
      id: event.venue.id,
      name: event.venue.name,
      city: event.venue.city,
      state: event.venue.state,
      country: event.venue.country,
      capacity: event.venue.capacity,
      location: event.venue.location,
    } : null,
    performers: (event.performers ?? []).slice(0, 4).map((p: any) => ({
      id: p.id,
      name: p.name,
      short_name: p.short_name,
      slug: p.slug,
      score: p.score,
      image: p.image,
      home_team: p.home_team,
    })),
    taxonomies: (event.taxonomies ?? []).map((t: any) => ({
      id: t.id,
      name: t.name,
      parent_id: t.parent_id,
    })),
  };
}

async function importEvents(ctx: EndpointCtx): Promise<EndpointResult> {
  const cfg = SPORT_TAXONOMY[ctx.sport];
  if (!cfg) return { filesWritten: 0, errors: [`No config for ${ctx.sport}`] };
  if (ctx.dryRun) return { filesWritten: 0, errors: [] };

  const errors: string[] = [];
  let filesWritten = 0;

  try {
    // Fetch upcoming events for this sport (up to 200)
    const params = new URLSearchParams({
      client_id: CLIENT_ID,
      q: cfg.query,
      taxonomies: String(cfg.taxonomyId),
      per_page: "200",
      sort: "datetime_utc.asc",
      datetime_utc: `${new Date().toISOString().slice(0, 10)}..`,
    });
    const url = `${BASE_URL}/events?${params}`;
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT);

    const events: any[] = data?.events ?? [];
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

const seatgeek: Provider = {
  name: NAME,
  label: "SeatGeek",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: !!CLIENT_ID,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    if (!CLIENT_ID) {
      logger.error("SEATGEEK_CLIENT_ID not set — skipping", NAME);
      return { provider: NAME, sport: "multi", filesWritten: 0, errors: ["Missing SEATGEEK_CLIENT_ID"], durationMs: 0 };
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

export default seatgeek;
