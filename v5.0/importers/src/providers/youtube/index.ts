// ──────────────────────────────────────────────────────────
// YouTube Data API v3 Provider
// ──────────────────────────────────────────────────────────
// Fetches sports highlight videos and official league content
// from YouTube. Great for enriching game pages with video context.
// Free tier: 10,000 units/day. Search costs 100 units per call.
// Requires: YOUTUBE_API_KEY (free at console.cloud.google.com)

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

const NAME = "youtube";
const BASE_URL = "https://www.googleapis.com/youtube/v3/search";
const API_KEY = process.env.YOUTUBE_API_KEY ?? "";
const RATE_LIMIT: RateLimitConfig = { requests: 1, perMs: 1_000 };

/** Official channel IDs for each sport — reduces noise from unofficial uploads */
const CHANNEL_IDS: Partial<Record<Sport, string>> = {
  nba: "UCWJ2lWNubArHWmf3FIHbfcQ",         // NBA Official
  nfl: "UCFWpGQQDFZGGRq9pO8wlLtQ",          // NFL
  mlb: "UCoLrcjPV5PbUrUyXq5mjc_A",          // MLB
  nhl: "UCOrg-b4KCKIhnXVgAMF9bvg",          // NHL
  epl: "UCqZQlzSHbVJrwrn5XvzrzcA",          // Premier League
  ufc: "UCvgfXK4nTYKudb0rFR6noLA",          // UFC
  f1: "UC6HfeAa0vWeSWS6IcNAjZ2A",           // Formula 1
};

const SPORT_QUERIES: Partial<Record<Sport, string>> = {
  nba: "NBA highlights today",
  nfl: "NFL highlights today",
  mlb: "MLB highlights today",
  nhl: "NHL highlights today",
  ncaab: "NCAA basketball highlights",
  ncaaf: "NCAA football highlights",
  wnba: "WNBA highlights",
  epl: "Premier League highlights",
  laliga: "La Liga highlights",
  bundesliga: "Bundesliga highlights",
  seriea: "Serie A highlights",
  ligue1: "Ligue 1 highlights",
  mls: "MLS highlights",
  ucl: "Champions League highlights",
  f1: "Formula 1 race highlights",
  ufc: "UFC fight highlights",
  atp: "ATP tennis highlights",
  wta: "WTA tennis highlights",
  golf: "PGA golf highlights",
  lpga: "LPGA golf highlights",
};

const SUPPORTED_SPORTS = Object.keys(SPORT_QUERIES) as Sport[];
const ENDPOINTS = ["highlights"] as const;
type Endpoint = (typeof ENDPOINTS)[number];

interface EndpointCtx { sport: Sport; season: number; dataDir: string; dryRun: boolean }
interface EndpointResult { filesWritten: number; errors: string[] }

function normalizeVideo(item: any): Record<string, unknown> {
  const snippet = item.snippet ?? {};
  const id = item.id ?? {};
  return {
    video_id: id.videoId ?? null,
    channel_id: snippet.channelId ?? null,
    channel_title: snippet.channelTitle ?? null,
    title: snippet.title ?? null,
    description: (snippet.description ?? "").slice(0, 500),
    published_at: snippet.publishedAt ?? null,
    thumbnail_default: snippet.thumbnails?.default?.url ?? null,
    thumbnail_medium: snippet.thumbnails?.medium?.url ?? null,
    thumbnail_high: snippet.thumbnails?.high?.url ?? null,
    live_broadcast_content: snippet.liveBroadcastContent ?? null,
    url: id.videoId ? `https://www.youtube.com/watch?v=${id.videoId}` : null,
    embed_url: id.videoId ? `https://www.youtube.com/embed/${id.videoId}` : null,
  };
}

async function importHighlights(ctx: EndpointCtx): Promise<EndpointResult> {
  const query = SPORT_QUERIES[ctx.sport];
  if (!query || ctx.dryRun) return { filesWritten: 0, errors: [] };

  const errors: string[] = [];
  let filesWritten = 0;

  try {
    const params: Record<string, string> = {
      key: API_KEY,
      q: query,
      part: "snippet",
      type: "video",
      maxResults: "25",
      order: "date",
      relevanceLanguage: "en",
      safeSearch: "none",
      videoEmbeddable: "true",
    };

    // Prefer official channel when known
    const channelId = CHANNEL_IDS[ctx.sport];
    if (channelId) params.channelId = channelId;

    const url = `${BASE_URL}?${new URLSearchParams(params)}`;
    const data = await fetchJSON<any>(url, NAME, RATE_LIMIT);
    const items: any[] = data?.items ?? [];
    const videos = items.map(normalizeVideo).filter((v) => v.video_id);

    const dateStr = new Date().toISOString().slice(0, 10);
    const outPath = rawPath(ctx.dataDir, NAME, ctx.sport, ctx.season, `highlights_${dateStr}.json`);
    writeJSON(outPath, {
      source: NAME,
      sport: ctx.sport,
      season: String(ctx.season),
      query,
      channel_id: channelId ?? null,
      date: dateStr,
      count: videos.length,
      videos,
      fetched_at: new Date().toISOString(),
    });
    filesWritten++;
    logger.progress(NAME, ctx.sport, "highlights", `${videos.length} videos`);
  } catch (err) {
    errors.push(`highlights/${ctx.sport}: ${err instanceof Error ? err.message : String(err)}`);
  }
  return { filesWritten, errors };
}

const ENDPOINT_FNS: Record<Endpoint, (ctx: EndpointCtx) => Promise<EndpointResult>> = {
  highlights: importHighlights,
};

const youtube: Provider = {
  name: NAME,
  label: "YouTube Data API",
  sports: SUPPORTED_SPORTS,
  requiresKey: true,
  rateLimit: RATE_LIMIT,
  endpoints: ENDPOINTS,
  enabled: !!API_KEY,

  async import(opts: ImportOptions): Promise<ImportResult> {
    const start = Date.now();
    if (!API_KEY) {
      logger.error("YOUTUBE_API_KEY not set — skipping", NAME);
      return { provider: NAME, sport: "multi", filesWritten: 0, errors: ["Missing YOUTUBE_API_KEY"], durationMs: 0 };
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

export default youtube;
