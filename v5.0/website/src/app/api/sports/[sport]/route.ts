import { NextResponse } from "next/server";
import { isSportKey } from "@/lib/sports";

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

    const data = await res.json();
    return NextResponse.json(data, {
      headers: {
        "Cache-Control": "public, s-maxage=60, stale-while-revalidate=30",
      },
    });
  } catch {
    return NextResponse.json(
      { error: "Backend unavailable" },
      { status: 502 },
    );
  }
}
