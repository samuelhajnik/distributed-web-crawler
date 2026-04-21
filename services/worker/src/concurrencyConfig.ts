/**
 * Process-wide concurrency for the worker binary. These apply to every crawl run
 * handled by this process: BullMQ multiplexes jobs from different runs onto the same
 * worker, so per-run queues would require a different architecture.
 *
 * Defaults trade responsiveness (typical demos / modest sites) against politeness:
 * higher values drain the frontier faster but increase concurrent load on origins
 * and local CPU; lower values are gentler but feel slow on link-rich pages.
 *
 * Override with WORKER_CONCURRENCY, FETCH_CONCURRENCY, FETCH_CONCURRENCY_PER_HOST,
 * FETCH_MIN_GAP_PER_HOST_MS, FETCH_GAP_JITTER_MS,
 * FETCH_HOST_COOLDOWN_BASE_MS, FETCH_HOST_COOLDOWN_MAX_MS.
 */
export const DEFAULT_WORKER_CONCURRENCY = 8;
/** In-process cap on concurrent HTTP attempts across active BullMQ jobs. */
export const DEFAULT_FETCH_CONCURRENCY = 12;
/** Concurrent fetches to the same hostname from this process (Origin politeness). */
export const DEFAULT_FETCH_PER_HOST_CONCURRENCY = 4;
/**
 * Minimum spacing between paced outbound fetch starts per host (process-local).
 * Kept modest for demos: tight enough to shave bursts without dominating throughput—unlike stricter crawlers that prioritize politeness over coverage.
 */
export const DEFAULT_FETCH_MIN_GAP_PER_HOST_MS = 40;
/**
 * Maximum extra random delay 0..N ms sampled per paced request (see HostPacer: when minGap > 0, jitter is capped at minGap so it cannot routinely double the base gap).
 */
export const DEFAULT_FETCH_GAP_JITTER_MS = 25;
/** First backoff step after a deny/rate-limit/transient-server signal (set to 0 to disable host cooldown). */
export const DEFAULT_FETCH_HOST_COOLDOWN_BASE_MS = 500;
/** Cap per cooldown extension (exponential backoff doubles until this ceiling). */
export const DEFAULT_FETCH_HOST_COOLDOWN_MAX_MS = 5_000;

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

/** Non-negative integer env (0 allowed). Used for pacing where 0 disables a component. */
export function readWorkerEnvNonNegativeInt(name: string, fallback: number, max = 3_600_000): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    return fallback;
  }
  return Math.min(max, Math.floor(n));
}
