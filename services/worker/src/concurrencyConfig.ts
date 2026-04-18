/**
 * Process-wide concurrency for the worker binary. These apply to every crawl run
 * handled by this process: BullMQ multiplexes jobs from different runs onto the same
 * worker, so per-run queues would require a different architecture.
 *
 * Defaults trade responsiveness (typical demos / modest sites) against politeness:
 * higher values drain the frontier faster but increase concurrent load on origins
 * and local CPU; lower values are gentler but feel slow on link-rich pages.
 *
 * Override with WORKER_CONCURRENCY, FETCH_CONCURRENCY, FETCH_CONCURRENCY_PER_HOST.
 */
export const DEFAULT_WORKER_CONCURRENCY = 8;
/** In-process cap on concurrent HTTP attempts across active BullMQ jobs. */
export const DEFAULT_FETCH_CONCURRENCY = 12;
/** Concurrent fetches to the same hostname from this process (Origin politeness). */
export const DEFAULT_FETCH_PER_HOST_CONCURRENCY = 6;

export function readWorkerEnvInt(name: string, fallback: number, min = 1, max = 256): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const n = Number(raw);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.floor(n)));
}
