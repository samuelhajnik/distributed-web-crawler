export type ScopeMode = "same_host" | "same_domain";

export type CrawlRunConfig = {
  maxPages: number;
  maxDepth: number;
  scopeMode: ScopeMode;
  includeDocuments: boolean;
  followRedirects: boolean;
  demoDelayMs: number;
  requestTimeoutMs: number;
  maxRetries: number;
};

function readInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const n = Number(raw);
  return Number.isFinite(n) ? Math.floor(n) : fallback;
}

export const DEFAULT_CRAWL_RUN_CONFIG: CrawlRunConfig = {
  maxPages: readInt("CRAWL_MAX_PAGES", 5000),
  maxDepth: readInt("CRAWL_MAX_DEPTH", 25),
  scopeMode: (process.env.CRAWL_SCOPE_MODE === "same_domain" ? "same_domain" : "same_host"),
  includeDocuments: process.env.CRAWL_INCLUDE_DOCUMENTS === "1",
  followRedirects: process.env.CRAWL_FOLLOW_REDIRECTS === "1",
  demoDelayMs: readInt("CRAWL_DEMO_DELAY_MS", 0),
  // Demo/local default: fail slow origins quickly (production crawlers often use much higher values).
  requestTimeoutMs: readInt("CRAWL_REQUEST_TIMEOUT_MS", 5000),
  maxRetries: readInt("MAX_RETRIES", 2)
};

export function clampInt(value: unknown, fallback: number, min: number, max: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.floor(n)));
}

