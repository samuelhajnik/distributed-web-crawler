import { clampInt, DEFAULT_CRAWL_RUN_CONFIG, type CrawlRunConfig, type ScopeMode } from "@crawler/shared";

function toBool(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    if (value === "1" || value.toLowerCase() === "true") {
      return true;
    }
    if (value === "0" || value.toLowerCase() === "false") {
      return false;
    }
  }
  return fallback;
}

function normalizeScope(value: unknown, fallback: ScopeMode): ScopeMode {
  return value === "same_domain" ? "same_domain" : fallback;
}

/** Accepted in POST body for backwards compatibility but never stored (process-level only). */
const IGNORED_LEGACY_PER_RUN_KEYS = ["workerConcurrency", "fetchConcurrency", "fetchPerHostConcurrency"] as const;

export function stripIgnoredLegacySettings(
  input: Record<string, unknown> | undefined
): Record<string, unknown> | undefined {
  if (!input) {
    return undefined;
  }
  const out = { ...input };
  for (const k of IGNORED_LEGACY_PER_RUN_KEYS) {
    delete out[k];
  }
  return out;
}

export function buildRunConfig(overrides: Record<string, unknown> | undefined): CrawlRunConfig {
  const inCfg = overrides ?? {};
  return {
    maxPages: clampInt(inCfg.maxPages, DEFAULT_CRAWL_RUN_CONFIG.maxPages, 1, 100_000),
    maxDepth: clampInt(inCfg.maxDepth, DEFAULT_CRAWL_RUN_CONFIG.maxDepth, 0, 100),
    scopeMode: normalizeScope(inCfg.scopeMode, DEFAULT_CRAWL_RUN_CONFIG.scopeMode),
    includeDocuments: toBool(inCfg.includeDocuments, DEFAULT_CRAWL_RUN_CONFIG.includeDocuments),
    followRedirects: toBool(inCfg.followRedirects, DEFAULT_CRAWL_RUN_CONFIG.followRedirects),
    demoDelayMs: clampInt(inCfg.demoDelayMs, DEFAULT_CRAWL_RUN_CONFIG.demoDelayMs, 0, 10_000),
    requestTimeoutMs: clampInt(inCfg.requestTimeoutMs, DEFAULT_CRAWL_RUN_CONFIG.requestTimeoutMs, 500, 120_000),
    maxRetries: clampInt(inCfg.maxRetries, DEFAULT_CRAWL_RUN_CONFIG.maxRetries, 0, 20)
  };
}

export function publicRunConfig(raw: unknown): CrawlRunConfig {
  const base = typeof raw === "object" && raw !== null ? { ...(raw as Record<string, unknown>) } : {};
  for (const k of IGNORED_LEGACY_PER_RUN_KEYS) {
    delete base[k];
  }
  return buildRunConfig(base);
}
