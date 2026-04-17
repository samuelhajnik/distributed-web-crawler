/**
 * Lightweight in-process concurrency gate for outbound fetches.
 * Global + per-host caps reduce accidental overload on the target site during local runs.
 */
export class AsyncSemaphore {
  private available: number;
  private readonly pending: Array<() => void> = [];

  constructor(max: number) {
    this.available = Math.max(1, max);
  }

  async acquire(): Promise<void> {
    if (this.available > 0) {
      this.available--;
      return;
    }
    await new Promise<void>((resolve) => {
      this.pending.push(resolve);
    });
  }

  release(): void {
    const next = this.pending.shift();
    if (next) {
      next();
    } else {
      this.available++;
    }
  }
}

export function createFetchGateway(globalMax: number, perHostMax: number) {
  const globalSem = new AsyncSemaphore(globalMax);
  const hostSem = new Map<string, AsyncSemaphore>();

  function semForHost(host: string): AsyncSemaphore {
    const key = host.toLowerCase();
    let s = hostSem.get(key);
    if (!s) {
      s = new AsyncSemaphore(perHostMax);
      hostSem.set(key, s);
    }
    return s;
  }

  async function run<T>(urlStr: string, fn: () => Promise<T>): Promise<T> {
    const host = new URL(urlStr).hostname;
    const h = semForHost(host);
    await globalSem.acquire();
    await h.acquire();
    try {
      return await fn();
    } finally {
      h.release();
      globalSem.release();
    }
  }

  return { run };
}
