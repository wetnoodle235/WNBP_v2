import { cookies } from "next/headers";

const PREMIUM_TIERS = new Set(["trial", "monthly", "yearly", "premium", "dev", "starter", "pro", "enterprise"]);
const ENTERPRISE_DOC_TIERS = new Set(["enterprise", "dev"]);
export const FREE_PREDICTION_PREVIEW_LIMIT = 3;

type PredictionPreview = {
  home_win_prob?: number | null;
  away_win_prob?: number | null;
  predicted_spread?: number | null;
  predicted_total?: number | null;
  confidence?: number | null;
  model?: string | null;
  n_models?: number | null;
};

function parseTierResponse(payload: unknown): string {
  if (!payload || typeof payload !== "object") return "free";
  const top = payload as Record<string, unknown>;
  const nested = (top.data as Record<string, unknown>) ?? top;
  const user = (nested.user as Record<string, unknown>) ?? nested;
  const tier = user.tier;
  return typeof tier === "string" && tier ? tier.trim().toLowerCase() : "free";
}

function parseTierFromToken(token: string): string {
  try {
    const parts = token.split(".");
    if (parts.length < 2) return "free";
    const payload = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8")) as Record<string, unknown>;
    const tier = payload.tier;
    return typeof tier === "string" && tier ? tier.trim().toLowerCase() : "free";
  } catch {
    return "free";
  }
}

export function hasPremiumTier(tier: string): boolean {
  return PREMIUM_TIERS.has(tier.trim().toLowerCase());
}

export function hasEnterpriseDocsAccess(tier: string): boolean {
  return ENTERPRISE_DOC_TIERS.has(tier.trim().toLowerCase());
}

function resolveServerApiBase(): string {
  return (
    process.env.PLATFORM_BACKEND_URL
    ?? process.env.NEXT_PUBLIC_API_URL
    ?? "http://127.0.0.1:8000"
  ).replace(/\/$/, "");
}

export async function getViewerTier(): Promise<string> {
  const cookieStore = await cookies();
  const token = cookieStore.get("wnbp_token")?.value;
  if (!token) return "free";

  const apiBase = resolveServerApiBase();
  try {
    const res = await fetch(`${apiBase}/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
      cache: "no-store",
    });
    if (!res.ok) return parseTierFromToken(token);
    const body = await res.json();
    return parseTierResponse(body);
  } catch {
    return parseTierFromToken(token);
  }
}

export function maskPredictionPreview<T extends PredictionPreview>(prediction: T): T {
  return {
    ...prediction,
    home_win_prob: null,
    away_win_prob: null,
    predicted_spread: null,
    predicted_total: null,
    confidence: null,
    model: null,
    n_models: null,
  };
}

export function limitPredictionPreview<T extends PredictionPreview>(predictions: T[], limit = FREE_PREDICTION_PREVIEW_LIMIT): T[] {
  return predictions.slice(0, limit).map(maskPredictionPreview);
}