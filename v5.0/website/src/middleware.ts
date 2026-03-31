import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

/** Paths that require authentication */
const PREMIUM_PATHS = ["/account", "/autobets", "/ladder"];

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const response = NextResponse.next();

  // Security headers
  response.headers.set("X-Content-Type-Options", "nosniff");
  response.headers.set("X-Frame-Options", "DENY");
  response.headers.set("Referrer-Policy", "strict-origin-when-cross-origin");
  response.headers.set("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
  response.headers.set("X-DNS-Prefetch-Control", "on");
  response.headers.set("Cross-Origin-Opener-Policy", "same-origin");

  // Rate limiting headers (informational for downstream)
  response.headers.set("X-RateLimit-Policy", "100/minute");

  // Auth check for premium routes
  if (PREMIUM_PATHS.some((p) => pathname.startsWith(p))) {
    const token =
      request.cookies.get("wnbp_token")?.value ??
      request.cookies.get("next-auth.session-token")?.value ??
      request.cookies.get("__Secure-next-auth.session-token")?.value;

    if (!token) {
      const loginUrl = new URL("/login", request.url);
      // Prevent open redirect — only allow relative paths from our own origin
      if (pathname.startsWith("/") && !pathname.startsWith("//")) {
        loginUrl.searchParams.set("callbackUrl", pathname);
      }
      return NextResponse.redirect(loginUrl);
    }
  }

  return response;
}

export const config = {
  matcher: [
    /*
     * Match all request paths except:
     * - api routes (handled separately)
     * - _next/static and _next/image (asset files)
     * - favicon.ico, public assets
     */
    "/((?!api|_next/static|_next/image|favicon.ico|public).*)",
  ],
};
