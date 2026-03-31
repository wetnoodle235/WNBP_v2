import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

/**
 * Stub next-auth API route.
 *
 * Auth is not yet implemented, but the SessionProvider on the client
 * polls /api/auth/session, /api/auth/csrf, /api/auth/providers, and
 * posts to /api/auth/_log.  Without this route those requests 404 and
 * flood the console with errors.
 *
 * Replace this file with a real NextAuth configuration when auth is
 * ready.
 */

function jsonResponse(body: unknown, status = 200) {
  return NextResponse.json(body, { status });
}

function handler(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const segment = pathname.split("/").pop();

  switch (segment) {
    case "session":
      return jsonResponse({});
    case "csrf":
      return jsonResponse({ csrfToken: "" });
    case "providers":
      return jsonResponse({});
    case "_log":
      return jsonResponse({});
    default:
      return jsonResponse({});
  }
}

export const GET = handler;
export const POST = handler;
