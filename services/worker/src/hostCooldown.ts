import type { FetchClassification } from "@crawler/shared";
import {
  DEFAULT_FETCH_HOST_COOLDOWN_BASE_MS,
  DEFAULT_FETCH_HOST_COOLDOWN_MAX_MS,
  readWorkerEnvNonNegativeInt
} from "./concurrencyConfig";

/** Serialize updates to per-host cooldown state (async jobs interleave across await points). */
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

type HostCooldownEntry = {
  strikes: number;
  cooldownUntil: number;
};

/** HTTP outcomes that indicate rate limits, blocks, or struggling origin — extend host-local cooldown. */
export function shouldCooldownForHttpClassification(c: FetchClassification): boolean {
  const s = c.httpStatus;
  if (s === 403 || s === 429) {
    return true;
  }
  if (s != null && s >= 500 && s < 600) {
    return true;
  }
  return false;
}

/** Retryable transport errors without HTTP status (timeouts, resets, etc.). */
export function shouldCooldownForExecutionClassification(c: FetchClassification): boolean {
  return c.retryable && c.httpStatus == null;
}

export type HostCooldownDeps = {
  baseBackoffMs: number;
  maxBackoffMs: number;
  now: () => number;
  sleep: (ms: number) => Promise<void>;
};

/**
 * Process-local extra delay before hitting a host again after deny/rate-limit/transient-server signals.
 * Layers on top of HostPacer; does not stop crawls or share state across workers.
 */
export class HostCooldown {
  private readonly deps: HostCooldownDeps;
  private readonly mut = new AsyncMutex();
  private readonly byHost = new Map<string, HostCooldownEntry>();

  constructor(deps: HostCooldownDeps) {
    this.deps = deps;
  }

  /** Wait until this host's cooldown window has passed (no-op if disabled or cold). */
  async waitUntilCool(hostname: string): Promise<void> {
    const { baseBackoffMs, sleep } = this.deps;
    if (baseBackoffMs <= 0) {
      return;
    }
    const key = hostname.toLowerCase();
    for (;;) {
      let waitMs = 0;
      await this.mut.runExclusive(async () => {
        const e = this.byHost.get(key);
        if (!e) {
          waitMs = 0;
          return;
        }
        const now = this.deps.now();
        if (now >= e.cooldownUntil) {
          waitMs = 0;
          return;
        }
        waitMs = e.cooldownUntil - now;
      });
      if (waitMs <= 0) {
        return;
      }
      await sleep(waitMs);
    }
  }

  async recordNegative(hostname: string): Promise<void> {
    const { baseBackoffMs, maxBackoffMs, now } = this.deps;
    if (baseBackoffMs <= 0) {
      return;
    }
    const key = hostname.toLowerCase();
    await this.mut.runExclusive(async () => {
      const prev = this.byHost.get(key) ?? { strikes: 0, cooldownUntil: 0 };
      const strikes = Math.min(prev.strikes + 1, 24);
      const exponent = strikes - 1;
      const raw = Math.floor(baseBackoffMs * Math.pow(2, exponent));
      const backoff = Math.min(maxBackoffMs, raw);
      const n = now();
      this.byHost.set(key, {
        strikes,
        cooldownUntil: Math.max(prev.cooldownUntil, n + backoff)
      });
    });
  }

  /** Successful HTTP response: gradually relax strike count (does not shorten an active cooldown window). */
  async recordSuccess(hostname: string): Promise<void> {
    const key = hostname.toLowerCase();
    await this.mut.runExclusive(async () => {
      const prev = this.byHost.get(key);
      if (!prev) {
        return;
      }
      const strikes = Math.max(0, prev.strikes - 1);
      if (strikes === 0) {
        this.byHost.delete(key);
      } else {
        this.byHost.set(key, { strikes, cooldownUntil: prev.cooldownUntil });
      }
    });
  }
}

export function loadHostCooldownFromEnv(): {
  cooldown: HostCooldown;
  baseBackoffMs: number;
  maxBackoffMs: number;
} {
  const baseBackoffMs = readWorkerEnvNonNegativeInt(
    "FETCH_HOST_COOLDOWN_BASE_MS",
    DEFAULT_FETCH_HOST_COOLDOWN_BASE_MS
  );
  const maxBackoffMs = readWorkerEnvNonNegativeInt(
    "FETCH_HOST_COOLDOWN_MAX_MS",
    DEFAULT_FETCH_HOST_COOLDOWN_MAX_MS
  );
  const cooldown = new HostCooldown({
    baseBackoffMs,
    maxBackoffMs,
    now: () => Date.now(),
    sleep: (ms) => new Promise((r) => setTimeout(r, ms))
  });
  return { cooldown, baseBackoffMs, maxBackoffMs };
}
