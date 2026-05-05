import { DEFAULT_CRAWL_RUN_CONFIG, pgPool, type CrawlRunConfig } from "@crawler/shared";

const runContextCache = new Map<number, { value: RunContext; until: number }>();
const RUN_CONTEXT_TTL_MS = 60_000;

export type RunContext = {
  seedHost: string;
  seedBaseDomain: string;
  config: CrawlRunConfig;
};

function baseDomain(host: string): string {
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(host) || host === "localhost") {
    return host;
  }
  const parts = host.split(".").filter(Boolean);
  if (parts.length < 2) {
    return host;
  }
  return parts.slice(-2).join(".");
}

export function isAllowedByScope(host: string, ctx: RunContext): boolean {
  const h = host.toLowerCase();
  if (ctx.config.scopeMode === "same_domain") {
    const b = baseDomain(h);
    return b === ctx.seedBaseDomain;
  }
  return h === ctx.seedHost;
}

export function isDocumentUrl(normalized: string): boolean {
  try {
    const p = new URL(normalized).pathname.toLowerCase();
    return /\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|gz|tgz|rar|7z|tar|csv|xml|json)$/i.test(p);
  } catch {
    return false;
  }
}

export function normalizeCandidateUrl(
  baseUrl: string,
  rawHref: string,
  ctx: RunContext
): string | null {
  const href = rawHref.trim();
  if (
    !href ||
    href.startsWith("mailto:") ||
    href.startsWith("tel:") ||
    href.startsWith("javascript:")
  ) {
    return null;
  }
  let resolved: URL;
  try {
    resolved = new URL(href, baseUrl);
  } catch {
    return null;
  }
  if (!["http:", "https:"].includes(resolved.protocol)) {
    return null;
  }
  if (!isAllowedByScope(resolved.hostname, ctx)) {
    return null;
  }
  resolved.hash = "";
  if (
    (resolved.protocol === "https:" && resolved.port === "443") ||
    (resolved.protocol === "http:" && resolved.port === "80")
  ) {
    resolved.port = "";
  }
  return resolved.toString();
}

export async function getRunContext(crawlRunId: number): Promise<RunContext> {
  const now = Date.now();
  const cached = runContextCache.get(crawlRunId);
  if (cached && cached.until > now) {
    return cached.value;
  }
  const res = await pgPool.query(
    `SELECT normalized_seed_url, run_config FROM crawl_runs WHERE id = $1`,
    [crawlRunId]
  );
  if (!res.rowCount) {
    const fallback: RunContext = {
      seedHost: "",
      seedBaseDomain: "",
      config: { ...DEFAULT_CRAWL_RUN_CONFIG }
    };
    return fallback;
  }
  const row = res.rows[0] as {
    normalized_seed_url: string;
    run_config: Partial<CrawlRunConfig> | null;
  };
  const seed = new URL(row.normalized_seed_url);
  const cfg = { ...DEFAULT_CRAWL_RUN_CONFIG, ...(row.run_config ?? {}) } as Record<string, unknown>;
  delete cfg.workerConcurrency;
  delete cfg.fetchConcurrency;
  delete cfg.fetchPerHostConcurrency;
  const ctx: RunContext = {
    seedHost: seed.hostname.toLowerCase(),
    seedBaseDomain: baseDomain(seed.hostname.toLowerCase()),
    config: cfg as CrawlRunConfig
  };
  runContextCache.set(crawlRunId, { value: ctx, until: now + RUN_CONTEXT_TTL_MS });
  return ctx;
}

export function isUrlInScope(url: string, runContext: RunContext): boolean {
  try {
    return isAllowedByScope(new URL(url).hostname, runContext);
  } catch {
    return false;
  }
}
