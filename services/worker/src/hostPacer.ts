import {
  DEFAULT_FETCH_GAP_JITTER_MS,
  DEFAULT_FETCH_MIN_GAP_PER_HOST_MS,
  readWorkerEnvNonNegativeInt
} from "./concurrencyConfig";

/**
 * In-process async mutex: one `runExclusive` at a time.
 * (Not re-entrant; sufficient for per-host pacing.)
 */
class AsyncMutex {
  private locked = false;
  private readonly queue: Array<() => void> = [];

  async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }

  private acquire(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.locked) {
        this.locked = true;
        resolve();
      } else {
        this.queue.push(resolve);
      }
    });
  }

  private release(): void {
    const next = this.queue.shift();
    if (next) {
      next();
    } else {
      this.locked = false;
    }
  }
}

export type HostPacerDeps = {
  minGapMs: number;
  jitterMaxMs: number;
  now: () => number;
  sleep: (ms: number) => Promise<void>;
  random: () => number;
};

/** When both min gap and jitter are configured, jitter draw is capped at minGapMs so arrivals stay spread without stacking a second full “gap worth” of delay every time. */
function effectiveJitterUpperBound(minGapMs: number, jitterMaxMs: number): number {
  if (jitterMaxMs <= 0) {
    return 0;
  }
  if (minGapMs <= 0) {
    return jitterMaxMs;
  }
  return Math.min(jitterMaxMs, minGapMs);
}

/**
 * Enforces a minimum spacing between *scheduled* outbound fetches to the same hostname
 * in this process, with optional random extra delay. Complements fetch concurrency
 * caps: it does not coordinate across worker replicas.
 */
export class HostPacer {
  private readonly mutexByHost = new Map<string, AsyncMutex>();
  private readonly lastPacedAt = new Map<string, number>();
  private readonly deps: HostPacerDeps;

  constructor(deps: HostPacerDeps) {
    this.deps = deps;
  }

  /**
   * Call immediately before `fetchGateway.run` (or any outbound attempt) for this URL.
   * Serializes pace math per host so concurrent jobs cannot read the same timestamp and
   * all sleep(0) together.
   */
  async waitBeforeOutboundFetch(hostname: string): Promise<void> {
    const { minGapMs, jitterMaxMs } = this.deps;
    if (minGapMs <= 0 && jitterMaxMs <= 0) {
      return;
    }
    const key = hostname.toLowerCase();
    const mutex = this.getMutex(key);
    const { now, sleep, random } = this.deps;
    await mutex.runExclusive(async () => {
      const jitterUpper = effectiveJitterUpperBound(minGapMs, jitterMaxMs);
      const jitter = jitterUpper > 0 ? Math.floor(random() * (jitterUpper + 1)) : 0;
      const last = this.lastPacedAt.get(key) ?? 0;
      const earliest = last + minGapMs + jitter;
      const delay = Math.max(0, earliest - now());
      if (delay > 0) {
        await sleep(delay);
      }
      this.lastPacedAt.set(key, now());
    });
  }

  private getMutex(key: string): AsyncMutex {
    let m = this.mutexByHost.get(key);
    if (!m) {
      m = new AsyncMutex();
      this.mutexByHost.set(key, m);
    }
    return m;
  }
}

export function loadHostPacerFromEnv(): { pacer: HostPacer; minGapMs: number; jitterMaxMs: number } {
  const minGapMs = readWorkerEnvNonNegativeInt(
    "FETCH_MIN_GAP_PER_HOST_MS",
    DEFAULT_FETCH_MIN_GAP_PER_HOST_MS
  );
  const jitterMaxMs = readWorkerEnvNonNegativeInt(
    "FETCH_GAP_JITTER_MS",
    DEFAULT_FETCH_GAP_JITTER_MS
  );
  const pacer = new HostPacer({
    minGapMs,
    jitterMaxMs,
    now: () => Date.now(),
    sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
    random: () => Math.random()
  });
  return { pacer, minGapMs, jitterMaxMs };
}
