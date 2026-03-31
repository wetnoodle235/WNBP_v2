// ──────────────────────────────────────────────────────────
// V5.0 Importer Core — Batch Processor
// ──────────────────────────────────────────────────────────
// Efficiently process items in batches with concurrency control.
// Ported from v4.0/util/api/batch-processor.ts, adapted for v5.0.

import { logger } from "./logger.js";

export interface BatchOptions<T = unknown, R = unknown> {
  batchSize?: number;
  concurrency?: number;
  onBatchComplete?: (batch: T[], results: R[], batchNum: number) => void;
  onProgress?: (current: number, total: number) => void;
  /** Label used in log messages (e.g. provider name) */
  label?: string;
}

/**
 * Process an array of items in sequential batches.
 *
 * Each batch is passed to `processFn`; results are concatenated.
 * Use this when the API requires you to send requests in groups
 * (e.g. "fetch 25 game IDs at a time").
 */
export async function fetchInBatches<T, R>(
  items: T[],
  batchSize: number,
  processFn: (batch: T[]) => Promise<R[]>,
  options: BatchOptions<T, R> = {},
): Promise<R[]> {
  const results: R[] = [];
  let batchNum = 0;

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    batchNum++;

    try {
      const batchResults = await processFn(batch);
      results.push(...batchResults);

      options.onBatchComplete?.(batch, batchResults, batchNum);
      options.onProgress?.(Math.min(i + batchSize, items.length), items.length);
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      logger.error(`Batch ${batchNum} failed: ${msg}`, options.label ?? "batch");
      throw error;
    }
  }

  return results;
}

/**
 * Process items with bounded concurrency.
 *
 * Up to `concurrency` items run simultaneously; as one finishes the
 * next item is started.  Order of results is **not** guaranteed.
 */
export async function processConcurrently<T, R>(
  items: T[],
  processFn: (item: T) => Promise<R>,
  concurrency = 4,
  label = "concurrent",
): Promise<R[]> {
  const results: R[] = [];
  const queue = [...items];
  const active: Promise<void>[] = [];

  while (queue.length > 0 || active.length > 0) {
    while (active.length < concurrency && queue.length > 0) {
      const item = queue.shift()!;
      const promise = processFn(item)
        .then((result) => {
          results.push(result);
        })
        .catch((err) => {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn(`Concurrent task failed: ${msg}`, label);
        })
        .finally(() => {
          const idx = active.indexOf(promise);
          if (idx > -1) active.splice(idx, 1);
        });

      active.push(promise);
    }

    if (active.length > 0) {
      await Promise.race(active);
    }
  }

  return results;
}
