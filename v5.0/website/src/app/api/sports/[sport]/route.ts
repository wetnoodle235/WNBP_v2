import { NextResponse } from "next/server";
import { isSportKey } from "@/lib/sports";
import { getViewerTier, hasPremiumTier } from "@/lib/server-access";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

// Sports available to free tier
const FREE_TIER_SPORTS = new Set(["nba"]);
// Endpoints available to free tier (via URL path components)
const FREE_TIER_ENDPOINTS = new Set(["games", "standings", "news"]);
// Result limit for free tier
const FREE_TIER_LIMIT = 10;

export async function GET(
  request: Request,
  { params }: { params: Promise<{ sport: string }> },
) {
  const { sport } = await params;

  if (!isSportKey(sport)) {
    return NextResponse.json({ error: "Unknown sport" }, { status: 404 });
  }

  const url = new URL(request.url);
  const [tier] = await Promise.all([getViewerTier()]);
  const isPremium = hasPremiumTier(tier);

  // Free tier: restrict to NBA only
  if (!isPremium && !FREE_TIER_SPORTS.has(sport)) {
    return NextResponse.json(
      {
        success: false,
        error: `Sport '${sport}' requires a paid subscription. Free tier: NBA only.`,
        viewer_tier: tier,
        upgrade_url: "/pricing",
      },
      { status: 403 },
    );
  }

  // Determine endpoint from search params or path context
  const endpointHint = url.searchParams.get("_endpoint") ?? "games";
  if (!isPremium && !FREE_TIER_ENDPOINTS.has(endpointHint)) {
    return NextResponse.json(
      {
        success: false,
        error: `Endpoint '${endpointHint}' requires a paid subscription.`,
        viewer_tier: tier,
        upgrade_url: "/pricing",
      },
      { status: 403 },
    );
  }

  const backendUrl = `${API_BASE}/v1/${sport}/games${url.search}`;

  try {
    const res = await fetch(backendUrl, {
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
    });

    if (!res.ok) {
      return NextResponse.json(
        { error: `Backend responded with ${res.status}` },
        { status: res.status },
      );
    }

    const payload = await res.json() as { success?: boolean; data?: unknown[]; meta?: Record<string, unknown> };
    const data = Array.isArray(payload.data) ? payload.data : [];

    // Apply result limit for free tier
    const visibleData = isPremium ? data : data.slice(0, FREE_TIER_LIMIT);

    return NextResponse.json(
      {
        ...payload,
        success: true,
        data: visibleData,
        meta: {
          ...(payload.meta ?? {}),
          viewer_tier: tier,
          preview_limited: !isPremium,
          total_available: data.length,
        },
      },
      {
        headers: {
          "Cache-Control": isPremium ? "private, no-store" : "public, s-maxage=60, stale-while-revalidate=30",
        },
      },
    );
  } catch {
    return NextResponse.json(
      { error: "Backend unavailable" },
      { status: 502 },
    );
  }
}
