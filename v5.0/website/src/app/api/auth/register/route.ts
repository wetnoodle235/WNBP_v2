import { NextResponse } from "next/server";
import { resolveServerApiBase } from "@/lib/api-base";

const API_BASE = resolveServerApiBase();

export async function POST(request: Request) {
  try {
    const body = await request.json() as Record<string, unknown>;
    const res = await fetch(`${API_BASE}/auth/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      cache: "no-store",
    });

    const ct = res.headers.get("content-type") ?? "";
    const payload = ct.includes("application/json")
      ? await res.json()
      : { success: false, detail: `Backend returned ${res.status}` };

    return NextResponse.json(payload, { status: res.status });
  } catch {
    return NextResponse.json({ success: false, detail: "Auth backend unavailable" }, { status: 502 });
  }
}
