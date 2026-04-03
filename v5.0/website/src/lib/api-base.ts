/**
 * Shared server-side API base URL resolver.
 * Priority chain:
 * PLATFORM_BACKEND_URL → BACKEND_URL → API_URL → NEXT_PUBLIC_API_URL → http://127.0.0.1:8000
 *
 * Safe to import in client components — the value is only evaluated in the
 * `typeof window === "undefined"` branch, which runs server-side (SSR/RSC).
 */
function normalizeBackendUrl(raw: string | undefined): string | null {
  if (!raw) return null;

  const normalized = raw
    .replace(/\\n|\\r/g, "")
    .trim()
    .replace(/^['\"]|['\"]$/g, "")
    .trim()
    .replace(/\/$/, "");

  if (!normalized) return null;

  try {
    const parsed = new URL(normalized);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
    return normalized;
  } catch {
    return null;
  }
}

export function resolveServerApiBase(): string {
  const candidate =
    normalizeBackendUrl(process.env.PLATFORM_BACKEND_URL) ??
    normalizeBackendUrl(process.env.BACKEND_URL) ??
    normalizeBackendUrl(process.env.API_URL) ??
    normalizeBackendUrl(process.env.NEXT_PUBLIC_API_URL);

  return candidate ?? "http://127.0.0.1:8000";
}
