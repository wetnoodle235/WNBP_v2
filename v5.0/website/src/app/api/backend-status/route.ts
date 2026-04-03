import { NextResponse } from "next/server";
import { resolveServerApiBase } from "@/lib/api-base";

const API_BASE = resolveServerApiBase();

export async function GET() {
  const startedAt = Date.now();
  const healthUrl = `${API_BASE}/health`;

  try {
    const res = await fetch(healthUrl, {
      method: "GET",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      signal: AbortSignal.timeout(8000),
    });

    const contentType = res.headers.get("content-type") ?? "";
    const payload = contentType.includes("application/json") ? await res.json() : null;

    return NextResponse.json(
      {
        success: res.ok,
        data: {
          backend_base: API_BASE,
          health_url: healthUrl,
          status_code: res.status,
          latency_ms: Date.now() - startedAt,
          backend_payload: payload,
        },
      },
      { status: res.ok ? 200 : 502 },
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown_error";
    return NextResponse.json(
      {
        success: false,
        data: {
          backend_base: API_BASE,
          health_url: healthUrl,
          latency_ms: Date.now() - startedAt,
        },
        error: message,
      },
      { status: 502 },
    );
  }
}
