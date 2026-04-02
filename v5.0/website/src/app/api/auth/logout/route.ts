import { NextResponse } from "next/server";
import { resolveServerApiBase } from "@/lib/api-base";

const API_BASE = resolveServerApiBase();

export async function POST(request: Request) {
  try {
    const auth = request.headers.get("authorization") ?? "";
    const res = await fetch(`${API_BASE}/auth/logout`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(auth ? { Authorization: auth } : {}),
      },
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
