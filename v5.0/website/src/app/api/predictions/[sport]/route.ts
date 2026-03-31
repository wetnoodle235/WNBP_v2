import { NextResponse } from "next/server";
import { isSportKey } from "@/lib/sports";
import { getViewerTier, hasPremiumTier, limitPredictionPreview, FREE_PREDICTION_PREVIEW_LIMIT } from "@/lib/server-access";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ sport: string }> },
) {
  const { sport } = await params;

  if (!isSportKey(sport)) {
    return NextResponse.json({ error: "Unknown sport" }, { status: 404 });
  }

  const url = new URL(request.url);
  const backendUrl = `${API_BASE}/v1/predictions/${sport}${url.search}`;

  try {
    const [res, tier] = await Promise.all([
      fetch(backendUrl, {
        headers: { "Content-Type": "application/json" },
        cache: "no-store",
      }),
      getViewerTier(),
    ]);

    if (!res.ok) {
      return NextResponse.json(
        { error: `Backend responded with ${res.status}` },
        { status: res.status },
      );
    }

    const payload = await res.json() as {
      success?: boolean;
      data?: Record<string, unknown>[];
      meta?: Record<string, unknown>;
    };

    const data = Array.isArray(payload.data) ? payload.data : [];
    const premium = hasPremiumTier(tier);
    const visibleData = premium ? data : limitPredictionPreview(data, FREE_PREDICTION_PREVIEW_LIMIT);

    return NextResponse.json(
      {
        ...payload,
        success: true,
        data: visibleData,
        meta: {
          ...(payload.meta ?? {}),
          viewer_tier: tier,
          preview_limited: !premium,
          preview_limit: premium ? null : FREE_PREDICTION_PREVIEW_LIMIT,
        },
      },
      {
        headers: {
          "Cache-Control": "private, no-store",
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