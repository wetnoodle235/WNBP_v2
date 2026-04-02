/**
 * API client for the v5.0 backend.
 * Server-side fetches use full URL; client-side goes through the Next.js proxy.
 */
import type {
  Game,
  Team,
  Standing,
  Odds,
  Prediction,
  News,
  Injury,
  MarketSignal,
  ScheduleFatigue,
} from "./schemas";

const isServer = typeof window === "undefined";

function resolveServerApiBase(): string {
  return (
    process.env.PLATFORM_BACKEND_URL
    ?? process.env.BACKEND_URL
    ?? process.env.API_URL
    ?? process.env.NEXT_PUBLIC_API_URL
    ?? "http://127.0.0.1:8000"
  ).replace(/\/$/, "");
}

const API_BASE = isServer
  ? resolveServerApiBase()
  : "/api/proxy";
const TIMEOUT_MS = 30_000;
const MAX_RETRIES = 2;
const RETRY_DELAY_MS = 1_000;

export interface ApiResult<T> {
  ok: boolean;
  status: number | null;
  data: T | null;
  error: string | null;
}

const warnedFetchFailures = new Set<string>();

function warnFetchDegradation(path: string, reason: string): void {
  const key = `${path}|${reason}`;
  if (warnedFetchFailures.has(key)) return;
  warnedFetchFailures.add(key);
  console.warn(`[api] degraded data for ${path}: ${reason}`);
}

async function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function fetchAPI<T>(
  path: string,
  opts?: Omit<RequestInit, "next"> & {
    params?: Record<string, string>;
    retries?: number;
    next?: { revalidate?: number | false; tags?: string[] };
  },
): Promise<ApiResult<T>> {
  let url = `${API_BASE}${path}`;
  if (opts?.params) {
    const qs = new URLSearchParams(
      Object.entries(opts.params).filter(([, v]) => v != null) as [string, string][],
    ).toString();
    if (qs) url += `?${qs}`;
  }

  const maxRetries = opts?.retries ?? MAX_RETRIES;
  let lastError: string = "network_error";

  const isPublicV1Path = path.startsWith("/v1/") && !path.startsWith("/v1/paper/");
  const isAuthOrBillingPath = path.startsWith("/auth/") || path.startsWith("/stripe/");

  const rawHeaders = opts?.headers;
  const hasAuthHeader = rawHeaders instanceof Headers
    ? rawHeaders.has("Authorization") || rawHeaders.has("X-API-Key")
    : Array.isArray(rawHeaders)
      ? rawHeaders.some(([k]) => /^(authorization|x-api-key)$/i.test(k))
      : !!rawHeaders && Object.keys(rawHeaders as Record<string, string>).some((k) => /^(authorization|x-api-key)$/i.test(k));

  const shouldCacheOnServer = isServer && isPublicV1Path && !isAuthOrBillingPath && !hasAuthHeader;
  const defaultCache: RequestCache = shouldCacheOnServer ? "force-cache" : "no-store";
  const nextConfig = opts?.next ?? (shouldCacheOnServer ? { revalidate: 45 } : undefined);

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);

    // If caller provided a signal, forward its abort to our controller
    const externalSignal = opts?.signal;
    const onExternalAbort = () => controller.abort();
    externalSignal?.addEventListener("abort", onExternalAbort);

    try {
      const res = await fetch(url, {
        ...opts,
        signal: controller.signal,
        headers: {
          "Content-Type": "application/json",
          ...opts?.headers,
        },
        cache: opts?.cache ?? defaultCache,
        ...(nextConfig ? { next: nextConfig } : {}),
      });
      clearTimeout(timeout);
      externalSignal?.removeEventListener("abort", onExternalAbort);

      if (!res.ok) {
        // Don't retry client errors (4xx) except 429
        if (res.status >= 400 && res.status < 500 && res.status !== 429) {
          return { ok: false, status: res.status, data: null, error: `http_${res.status}` };
        }
        lastError = `http_${res.status}`;
        if (attempt < maxRetries) {
          await delay(RETRY_DELAY_MS * (attempt + 1));
          continue;
        }
        return { ok: false, status: res.status, data: null, error: lastError };
      }

      const ct = res.headers.get("content-type") ?? "";
      if (!ct.includes("application/json")) {
        return { ok: false, status: res.status, data: null, error: "invalid_content_type" };
      }

      const data = (await res.json()) as T;
      return { ok: true, status: res.status, data, error: null };
    } catch (err) {
      clearTimeout(timeout);
      externalSignal?.removeEventListener("abort", onExternalAbort);

      const isAbort = err instanceof DOMException && err.name === "AbortError";
      // If external signal caused abort, don't retry — propagate immediately
      if (isAbort && externalSignal?.aborted) {
        return { ok: false, status: null, data: null, error: "aborted" };
      }
      lastError = isAbort ? "timeout" : "network_error";

      if (attempt < maxRetries) {
        await delay(RETRY_DELAY_MS * (attempt + 1));
        continue;
      }
    }
  }

  return { ok: false, status: null, data: null, error: lastError };
}

/** Map API error codes to user-friendly messages */
export function friendlyError(code: string | null): string {
  if (!code) return "Something went wrong. Please try again.";
  const map: Record<string, string> = {
    timeout: "The request timed out. Please check your connection and try again.",
    network_error: "Unable to reach our servers. Please check your internet connection.",
    invalid_content_type: "Received an unexpected response from the server.",
    http_400: "Invalid request. Please check your input and try again.",
    http_401: "Your session has expired. Please log in again.",
    http_403: "You don't have permission to access this resource.",
    http_404: "The requested resource was not found.",
    http_409: "This action conflicts with the current state. Please refresh and try again.",
    http_422: "The provided data is invalid. Please check your input.",
    http_429: "Too many requests. Please wait a moment and try again.",
    http_500: "An internal server error occurred. We're looking into it.",
    http_502: "The server is temporarily unavailable. Please try again shortly.",
    http_503: "The service is temporarily unavailable for maintenance.",
  };
  return map[code] ?? `An error occurred (${code}). Please try again.`;
}

/** Fetch and return data or null on error.
 *  Backend wraps responses in {success, data, meta} — unwrap automatically. */
async function getData<T>(path: string, params?: Record<string, string>): Promise<T | null> {
  const res = await fetchAPI<Record<string, unknown>>(path, { params });
  if (!res.ok || !res.data) {
    warnFetchDegradation(path, res.error ?? "no_data");
    return null;
  }
  // Backend v5 returns { success, data, meta } wrapper
  if ("data" in res.data && "success" in res.data) {
    if ((res.data.success as boolean) === false) {
      warnFetchDegradation(path, "wrapped_unsuccessful_response");
    }
    return res.data.data as T;
  }
  return res.data as unknown as T;
}

// ── Domain endpoints matching v5.0 backend routes ────────────────────────────

export async function getGames(
  sport: string,
  params?: Record<string, string>,
): Promise<Game[]> {
  return (await getData<Game[]>(`/v1/${sport}/games`, params)) ?? [];
}

export async function getGame(sport: string, gameId: string): Promise<Game | null> {
  return getData<Game>(`/v1/${sport}/games/${gameId}`);
}

export async function getTeams(sport: string): Promise<Team[]> {
  return (await getData<Team[]>(`/v1/${sport}/teams`, { limit: "500" })) ?? [];
}

export async function getTeam(sport: string, teamId: string): Promise<Team | null> {
  return getData<Team>(`/v1/${sport}/teams/${teamId}`);
}

export async function getStandings(
  sport: string,
  season?: number,
): Promise<Standing[]> {
  const params: Record<string, string> = {};
  if (season) params.season = String(season);
  return (await getData<Standing[]>(`/v1/${sport}/standings`, params)) ?? [];
}

export async function getStandingsWithMeta(
  sport: string,
  season?: number,
): Promise<{ data: Standing[]; seasonActive: boolean; seasonYear: string | null }> {
  const params: Record<string, string> = {};
  if (season) params.season = String(season);
  const res = await fetchAPI<Record<string, unknown>>(`/v1/${sport}/standings`, { params });
  if (!res.ok || !res.data) return { data: [], seasonActive: true, seasonYear: null };
  const wrapper = res.data as { success?: boolean; data?: Standing[]; meta?: Record<string, unknown> };
  const data = (wrapper.data as Standing[]) ?? [];
  const seasonActive = (wrapper.meta?.season_active as boolean) ?? true;
  const seasonYear = (wrapper.meta?.season_year as string) ?? (wrapper.meta?.season as string) ?? null;
  return { data, seasonActive, seasonYear };
}

export interface WeatherInfo {
  game_id: string;
  sport: string;
  venue?: string;
  dome: boolean;
  temp_f?: number | null;
  wind_mph?: number | null;
  wind_direction?: string | null;
  wind_direction_deg?: number | null;
  humidity_pct?: number | null;
  precipitation_pct?: number | null;
  condition?: string | null;
  source?: string;
}

export async function getGameWeather(sport: string, gameId: string): Promise<WeatherInfo | null> {
  return getData<WeatherInfo>(`/v1/${sport}/games/${gameId}/weather`);
}

export async function getOdds(
  sport: string,
  params?: Record<string, string>,
): Promise<Odds[]> {
  return (await getData<Odds[]>(`/v1/${sport}/odds`, params)) ?? [];
}

export async function getMarketSignals(
  sport: string,
  params?: Record<string, string>,
): Promise<MarketSignal[]> {
  return (await getData<MarketSignal[]>(`/v1/${sport}/market-signals`, params)) ?? [];
}

export async function getScheduleFatigue(
  sport: string,
  params?: Record<string, string>,
): Promise<ScheduleFatigue[]> {
  return (await getData<ScheduleFatigue[]>(`/v1/${sport}/schedule-fatigue`, params)) ?? [];
}

export async function getPredictions(
  sport: string,
  params?: Record<string, string>,
): Promise<Prediction[]> {
  return (await getData<Prediction[]>(`/v1/predictions/${sport}`, params)) ?? [];
}

export async function getInjuries(sport: string): Promise<Injury[]> {
  return (await getData<Injury[]>(`/v1/${sport}/injuries`)) ?? [];
}

export async function getNews(sport: string, limit = 20): Promise<News[]> {
  const raw =
    (await getData<Record<string, unknown>[]>(`/v1/${sport}/news`, { limit: String(limit) })) ?? [];
  // Normalize API field names to match the News schema
  return raw.map((item) => ({
    source: (item.source as string) ?? "unknown",
    id: item.id as string | undefined,
    sport: (item.sport as string) ?? sport,
    headline: (item.headline as string) ?? "",
    description: ((item.description ?? item.summary ?? null) as string | null),
    link: ((item.link ?? item.url ?? null) as string | null),
    image_url: (item.image_url ?? null) as string | null,
    published: ((item.published ?? item.published_at ?? null) as string | null),
    author: (item.author ?? null) as string | null,
  }));
}

export async function getPlayers(
  sport: string,
  params?: Record<string, string>,
): Promise<unknown[]> {
  return (await getData<unknown[]>(`/v1/${sport}/players`, params)) ?? [];
}

export async function getPlayerStats(
  sport: string,
  params?: Record<string, string>,
): Promise<unknown[]> {
  return (await getData<unknown[]>(`/v1/${sport}/player-stats`, params)) ?? [];
}

export async function getTeamStats(
  sport: string,
  params?: Record<string, string>,
): Promise<unknown[]> {
  return (await getData<unknown[]>(`/v1/${sport}/team-stats`, params)) ?? [];
}

export async function getSports(): Promise<Record<string, unknown> | null> {
  return getData<Record<string, unknown>>("/v1/sports");
}

export async function getStatus(): Promise<Record<string, unknown> | null> {
  return getData<Record<string, unknown>>("/v1/status");
}

export async function getSimulation(sport: string): Promise<Record<string, unknown> | null> {
  return getData<Record<string, unknown>>(`/v1/${sport}/simulation`);
}

// ── Auth helpers (client-side) ───────────────────────────────────────────────

const TOKEN_KEY = "wnbp_token";
const API_KEY_KEY = "wnbp_api_key";

/** Get stored auth token */
export function getStoredToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(TOKEN_KEY);
}

/** Get stored API key */
export function getStoredApiKey(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(API_KEY_KEY);
}

/** Build auth headers for API requests */
export function authHeaders(): Record<string, string> {
  const headers: Record<string, string> = {};
  const token = getStoredToken();
  const apiKey = getStoredApiKey();
  if (token) headers["Authorization"] = `Bearer ${token}`;
  if (apiKey) headers["X-API-Key"] = apiKey;
  return headers;
}

/** Authenticated fetch wrapper — attaches token + API key headers */
export async function fetchAuthAPI<T>(
  path: string,
  opts?: Omit<RequestInit, "next"> & { params?: Record<string, string> },
): Promise<ApiResult<T>> {
  return fetchAPI<T>(path, {
    ...opts,
    headers: {
      ...authHeaders(),
      ...opts?.headers,
    },
  });
}

/** Login via backend */
export async function loginUser(email: string, password: string): Promise<ApiResult<Record<string, unknown>>> {
  return fetchAPI<Record<string, unknown>>("/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
}

/** Register via backend */
export async function registerUser(
  name: string,
  email: string,
  password: string,
): Promise<ApiResult<Record<string, unknown>>> {
  return fetchAPI<Record<string, unknown>>("/auth/register", {
    method: "POST",
    body: JSON.stringify({ name, email, password }),
  });
}

/** Fetch current user profile */
export async function fetchCurrentUser(token: string): Promise<ApiResult<Record<string, unknown>>> {
  return fetchAPI<Record<string, unknown>>("/auth/me", {
    headers: { Authorization: `Bearer ${token}` },
  });
}

/** Fetch available tiers */
export async function fetchTiers(): Promise<ApiResult<Record<string, unknown>>> {
  return fetchAPI<Record<string, unknown>>("/auth/tiers");
}

/** Fetch referral stats */
export async function fetchReferrals(): Promise<ApiResult<Record<string, unknown>>> {
  return fetchAuthAPI<Record<string, unknown>>("/auth/referrals");
}

/** Regenerate API key */
export async function regenerateApiKey(): Promise<ApiResult<Record<string, unknown>>> {
  return fetchAuthAPI<Record<string, unknown>>("/auth/api-key/regenerate", { method: "POST" });
}

/** Create Stripe checkout session */
export async function createCheckout(
  tier: string,
  billingPeriod: "monthly" | "yearly",
): Promise<ApiResult<Record<string, unknown>>> {
  return fetchAuthAPI<Record<string, unknown>>("/stripe/create-checkout", {
    method: "POST",
    body: JSON.stringify({ tier, billing_period: billingPeriod }),
  });
}

/** Get Stripe customer portal URL */
export async function getStripePortal(): Promise<ApiResult<Record<string, unknown>>> {
  return fetchAuthAPI<Record<string, unknown>>("/stripe/portal");
}

/** Clear stored auth data */
export function clearAuth() {
  if (typeof window === "undefined") return;
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(API_KEY_KEY);
  // Clear auth cookie
  document.cookie = "wnbp_token=; path=/; max-age=0; SameSite=Strict";
}

/** Store auth token (localStorage + cookie for middleware) */
export function storeToken(token: string) {
  if (typeof window === "undefined") return;
  localStorage.setItem(TOKEN_KEY, token);
  document.cookie = `wnbp_token=${token}; path=/; max-age=${60 * 60 * 24 * 30}; SameSite=Strict`;
}

/** Store API key */
export function storeApiKey(key: string) {
  if (typeof window === "undefined") return;
  localStorage.setItem(API_KEY_KEY, key);
}
