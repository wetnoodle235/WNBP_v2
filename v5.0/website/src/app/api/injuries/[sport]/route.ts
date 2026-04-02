import { NextResponse } from "next/server";
import { isSportKey } from "@/lib/sports";
import { resolveServerApiBase } from "@/lib/api-base";

const API_BASE = resolveServerApiBase();

export async function GET(
  request: Request,
  { params }: { params: Promise<{ sport: string }> },
) {
  const { sport } = await params;

  if (!isSportKey(sport)) {
    return NextResponse.json({ error: "Unknown sport" }, { status: 404 });
  }

  const url = new URL(request.url);
  const limit = Math.min(Number(url.searchParams.get("limit") ?? "100"), 500);

  try {
    const backendUrl = `${API_BASE}/v1/${sport}/injuries?limit=${limit}`;
    const res = await fetch(backendUrl, {
      headers: { "Content-Type": "application/json" },
      next: { revalidate: 300 },
    });

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

    const raw = Array.isArray(payload.data) ? payload.data : [];

    // Normalize fields for the InjuryImpactPanel component shape
    const injuries = raw.map((p) => ({
      player_id: String(p.player_id ?? p.id ?? ""),
      player_name: (p.player_name as string) ?? (p.name as string) ?? "Unknown",
      team_abbr: (p.team_abbreviation as string) ?? (p.team_abbr as string) ?? (p.team as string) ?? "—",
      status: (p.status as string) ?? "unknown",
      reason: (p.injury as string) ?? (p.injury_type as string) ?? undefined,
      injury_start: (p.updated as string) ?? (p.injury_start as string) ?? undefined,
      injury_end: (p.expected_return as string) ?? undefined,
    }));

    return NextResponse.json(
      {
        success: true,
        injuries,
        sport,
        meta: payload.meta ?? {},
      },
      {
        headers: { "Cache-Control": "public, s-maxage=300, stale-while-revalidate=120" },
      },
    );
  } catch {
    return NextResponse.json({ error: "Backend unavailable" }, { status: 502 });
  }
}
