import { NextResponse } from "next/server";
import { isSportKey } from "@/lib/sports";
import { resolveServerApiBase } from "@/lib/api-base";

const API_BASE = resolveServerApiBase();

// Stat column mapping from frontend stat key → backend player-stats field
const STAT_MAP: Record<string, string> = {
  // Basketball
  pts: "pts",
  reb: "reb",
  ast: "ast",
  stl: "stl",
  blk: "blk",
  fg3m: "fg3m",
  // Baseball
  hr: "hr",
  avg: "avg",
  rbi: "rbi",
  ops: "ops",
  obp: "obp",
  sb: "sb",
  era: "era",
  strikeouts: "strikeouts",
};

export async function GET(
  request: Request,
  { params }: { params: Promise<{ sport: string }> },
) {
  const { sport } = await params;

  if (!isSportKey(sport)) {
    return NextResponse.json({ error: "Unknown sport" }, { status: 404 });
  }

  const url = new URL(request.url);
  const stat = url.searchParams.get("stat") ?? "pts";
  const limit = Math.min(Number(url.searchParams.get("limit") ?? "10"), 50);
  const sortField = STAT_MAP[stat] ?? stat;

  try {
    const backendUrl = `${API_BASE}/v1/${sport}/player-stats?aggregate=true&sort=${sortField}&order=desc&limit=${limit}`;
    const res = await fetch(backendUrl, {
      headers: { "Content-Type": "application/json" },
      next: { revalidate: 120 },
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

    const leaders = raw.slice(0, limit).map((p, i) => ({
      rank: i + 1,
      player_id: p.player_id as number | null ?? null,
      player_name: (p.player_name as string) ?? (p.name as string) ?? "Unknown",
      team: (p.team as string) ?? (p.team_abbreviation as string) ?? "—",
      games_played: p.games_played as number ?? null,
      stat_value: p[sortField] != null
        ? typeof p[sortField] === "number"
          ? Number.isInteger(p[sortField] as number) ? p[sortField] : (p[sortField] as number).toFixed(1)
          : p[sortField]
        : "—",
      value: p[sortField] as number ?? null,
    }));

    return NextResponse.json(
      {
        success: true,
        leaders,
        stat,
        sport,
        season: payload.meta?.season ?? null,
      },
      {
        headers: { "Cache-Control": "public, s-maxage=120, stale-while-revalidate=60" },
      },
    );
  } catch {
    return NextResponse.json({ error: "Backend unavailable" }, { status: 502 });
  }
}
