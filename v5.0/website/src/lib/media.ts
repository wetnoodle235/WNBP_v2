import { resolveServerApiBase } from "./api-base";

const MEDIA_FIELDS = new Set(["logo_url", "headshot_url", "image_url"]);

function normalizeRawUrl(raw: string): string {
  return raw.replace(/\\n|\\r/g, "").trim().replace(/^['\"]|['\"]$/g, "").trim();
}

function dedupeUrls(urls: Array<string | null | undefined>): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const url of urls) {
    if (!url || seen.has(url)) continue;
    seen.add(url);
    out.push(url);
  }
  return out;
}

export function isEspnTeamLogoUrl(raw: string | null | undefined): boolean {
  if (!raw) return false;
  try {
    const value = normalizeRawUrl(raw);
    const parsed = new URL(value);
    return parsed.hostname.endsWith("espncdn.com") && parsed.pathname.includes("/i/teamlogos/");
  } catch {
    return false;
  }
}

export function isEspnHeadshotUrl(raw: string | null | undefined): boolean {
  if (!raw) return false;
  try {
    const value = normalizeRawUrl(raw);
    const parsed = new URL(value);
    return parsed.hostname.endsWith("espncdn.com") && parsed.pathname.includes("/i/headshots/");
  } catch {
    return false;
  }
}

export function resolveMediaUrl(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const value = normalizeRawUrl(raw);
  if (!value) return null;
  if (/^https?:\/\//i.test(value)) return value;
  if (value.startsWith("/api/proxy/")) return value;
  if (value.startsWith("/v1/media/")) {
    return typeof window === "undefined"
      ? `${resolveServerApiBase()}${value}`
      : `/api/proxy${value}`;
  }
  return value;
}

export function normalizeMediaPayload<T>(input: T): T {
  if (Array.isArray(input)) {
    return input.map((item) => normalizeMediaPayload(item)) as T;
  }

  if (input && typeof input === "object") {
    const record = input as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(record)) {
      if (MEDIA_FIELDS.has(key) && typeof value === "string") {
        out[key] = resolveMediaUrl(value);
      } else {
        out[key] = normalizeMediaPayload(value);
      }
    }
    return out as T;
  }

  return input;
}

export function leagueMediaProxyUrl(sport: string): string {
  const normalized = String(sport || "").toLowerCase();
  return `/api/proxy/v1/media/${normalized}/league/${normalized}/image_url.png`;
}

export function teamMediaProxyUrl(sport: string, teamId: string | number): string {
  const normalizedSport = String(sport || "").toLowerCase();
  const normalizedTeamId = encodeURIComponent(String(teamId));
  return `/api/proxy/v1/media/${normalizedSport}/team/${normalizedTeamId}/logo_url.png`;
}

export function playerMediaProxyUrl(sport: string, playerId: string | number): string {
  const normalizedSport = String(sport || "").toLowerCase();
  const normalizedPlayerId = encodeURIComponent(String(playerId));
  return `/api/proxy/v1/media/${normalizedSport}/player/${normalizedPlayerId}/headshot_url.png`;
}

export function preferredTeamLogoUrls(input: {
  sport?: string | null;
  teamId?: string | number | null;
  logoUrl?: string | null;
}): string[] {
  const resolved = resolveMediaUrl(input.logoUrl ?? null);
  const candidates: Array<string | null> = [];

  if (input.sport && input.teamId != null && input.teamId !== "") {
    candidates.push(teamMediaProxyUrl(input.sport, input.teamId));
  }

  if (resolved && !isEspnTeamLogoUrl(resolved)) {
    candidates.push(resolved);
  }

  return dedupeUrls(candidates);
}

export function preferredPlayerHeadshotUrls(input: {
  sport?: string | null;
  playerId?: string | number | null;
  headshotUrl?: string | null;
}): string[] {
  const resolved = resolveMediaUrl(input.headshotUrl ?? null);
  const candidates: Array<string | null> = [];

  if (input.sport && input.playerId != null && input.playerId !== "") {
    candidates.push(playerMediaProxyUrl(input.sport, input.playerId));
  }

  if (resolved && !isEspnHeadshotUrl(resolved)) {
    candidates.push(resolved);
  }

  return dedupeUrls(candidates);
}