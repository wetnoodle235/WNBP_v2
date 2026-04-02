/**
 * Shared server-side API base URL resolver.
 * Priority chain:
 * PLATFORM_BACKEND_URL → BACKEND_URL → API_URL → NEXT_PUBLIC_API_URL → http://127.0.0.1:8000
 *
 * Safe to import in client components — the value is only evaluated in the
 * `typeof window === "undefined"` branch, which runs server-side (SSR/RSC).
 */
export function resolveServerApiBase(): string {
  return (
    process.env.PLATFORM_BACKEND_URL ??
    process.env.BACKEND_URL ??
    process.env.API_URL ??
    process.env.NEXT_PUBLIC_API_URL ??
    "http://127.0.0.1:8000"
  ).replace(/\/$/, "");
}
