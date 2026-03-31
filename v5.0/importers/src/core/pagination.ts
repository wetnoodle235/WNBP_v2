// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — Pagination Handler
// ──────────────────────────────────────────────────────────
// Shared pagination strategies: cursor, offset, and auto-detect.
// Ported from v4.0/util/api/pagination-handler.ts, adapted for v5.0.

import { logger } from "./logger.js";

export interface PaginationOptions {
  /** Items per page (default 100) */
  perPage?: number;
  /** Safety limit on total pages fetched (default Infinity) */
  maxPages?: number;
  /** Label for logging */
  label?: string;
}

/**
 * Paginate using a cursor returned by the API.
 *
 * `fetchFn` receives the current cursor (undefined on first call) and
 * must return an object with `.data` (array) and `.cursor` / `.next_cursor`.
 */
export async function fetchWithCursorPagination<T>(
  fetchFn: (cursor?: string) => Promise<{ data?: T[]; cursor?: string; next_cursor?: string } | null>,
  options: PaginationOptions = {},
): Promise<T[]> {
  const items: T[] = [];
  let cursor: string | undefined;
  let page = 0;
  const maxPages = options.maxPages ?? Infinity;

  while (page < maxPages) {
    const response = await fetchFn(cursor);
    if (!response?.data?.length) break;

    items.push(...response.data);
    cursor = response.cursor ?? response.next_cursor;
    if (!cursor) break;

    page++;
    if (options.label) {
      logger.debug(`Page ${page}: ${items.length} items so far`, options.label);
    }
  }

  return items;
}

/**
 * Paginate using offset + limit parameters.
 *
 * `fetchFn` receives `(offset, limit)` and must return an object
 * with `.data` (array).  Stops when a page returns fewer items
 * than `perPage`.
 */
export async function fetchWithOffsetPagination<T>(
  fetchFn: (offset: number, limit: number) => Promise<{ data?: T[] } | null>,
  options: PaginationOptions = {},
): Promise<T[]> {
  const items: T[] = [];
  const perPage = options.perPage ?? 100;
  const maxPages = options.maxPages ?? Infinity;
  let offset = 0;
  let page = 0;

  while (page < maxPages) {
    const response = await fetchFn(offset, perPage);
    if (!response?.data?.length) break;

    items.push(...response.data);

    if (response.data.length < perPage) break;

    offset += perPage;
    page++;
  }

  return items;
}

/**
 * Generic paginator that auto-detects cursor vs page-number style.
 *
 * `fetchFn` receives a params object (including per_page and any cursor)
 * and must return either an array or an object with `.data`, `.cursor` /
 * `.next_cursor`, or `.meta.next_cursor`.
 */
export async function fetchWithPagination<T>(
  fetchFn: (params: Record<string, unknown>) => Promise<unknown>,
  params: Record<string, unknown> = {},
  options: PaginationOptions = {},
): Promise<T[]> {
  const items: T[] = [];
  let cursor: string | undefined;
  let page = 0;
  const maxPages = options.maxPages ?? Infinity;
  const perPage = options.perPage ?? 100;

  while (page < maxPages) {
    const currentParams: Record<string, unknown> = {
      ...params,
      per_page: perPage,
      ...(cursor && { cursor }),
    };

    const response = await fetchFn(currentParams) as Record<string, unknown> | unknown[] | null;
    if (!response) break;

    const data: unknown[] = Array.isArray(response)
      ? response
      : (response as Record<string, unknown>).data as unknown[] ?? [];

    if (data.length === 0) break;
    items.push(...(data as T[]));

    // Extract next cursor from common locations
    if (!Array.isArray(response)) {
      const resp = response as Record<string, unknown>;
      cursor =
        (resp.cursor as string | undefined) ??
        (resp.next_cursor as string | undefined) ??
        ((resp.meta as Record<string, unknown> | undefined)?.next_cursor as string | undefined);
    } else {
      cursor = undefined;
    }

    if (!cursor) break;
    page++;
  }

  return items;
}
