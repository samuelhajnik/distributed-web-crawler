import http from "node:http";
import os from "node:os";
import type { Job } from "bullmq";
import { Worker } from "bullmq";
import { load } from "cheerio";
import { fetch as undiciFetch, request } from "undici";
import {
  CRAWL_QUEUE_NAME,
  CrawlJobPayload,
  classifyExecutionError,
  classifyHttpResponse,
  DEFAULT_CRAWL_RUN_CONFIG,
  type CrawlRunConfig,
  createCrawlQueue,
  pgPool,
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS,
  redisConnection
} from "@crawler/shared";
import type { FetchClassification } from "@crawler/shared";
import {
  DEFAULT_FETCH_CONCURRENCY,
  DEFAULT_FETCH_PER_HOST_CONCURRENCY,
  DEFAULT_WORKER_CONCURRENCY,
  readWorkerEnvInt
} from "./concurrencyConfig";
import { createFetchGateway } from "./fetchLimit";
import {
  loadHostCooldownFromEnv,
  shouldCooldownForExecutionClassification,
  shouldCooldownForHttpClassification
} from "./hostCooldown";
import { loadHostPacerFromEnv } from "./hostPacer";
import {
  crawlFetchDurationSeconds,
  crawlProcessingDurationSeconds,
  crawlQueueLatencySeconds,
  crawlUrlsDiscoveredTotal,
  crawlUrlsFailedTotal,
  crawlUrlsRetriedTotal,
  crawlUrlsRequeuedTotal,
  crawlUrlsVisitedTotal,
  metricsHandler,
  processedUrlsTotal
} from "./prometheus";

const workerConcurrency = readWorkerEnvInt("WORKER_CONCURRENCY", DEFAULT_WORKER_CONCURRENCY);
const fetchGlobalMax = readWorkerEnvInt("FETCH_CONCURRENCY", DEFAULT_FETCH_CONCURRENCY);
const fetchPerHostMax = readWorkerEnvInt("FETCH_CONCURRENCY_PER_HOST", DEFAULT_FETCH_PER_HOST_CONCURRENCY);
const workerId = process.env.WORKER_ID ?? `${os.hostname()}-${process.pid}`;
const metricsPort = Number(process.env.WORKER_METRICS_PORT ?? 9091);
const queue = createCrawlQueue();

/** Honest product id in a common UA shape; avoids mimicking a specific browser build. */
const DEFAULT_REQUEST_USER_AGENT =
  "Mozilla/5.0 (compatible; distributed-web-crawler/1.0)";
const REQUEST_USER_AGENT = process.env.CRAWLER_USER_AGENT?.trim() || DEFAULT_REQUEST_USER_AGENT;

/** Shared defaults for document-style GETs (undici fetch + request; redirect-following fetch reuses the same options). */
function buildRequestHeaders(): Record<string, string> {
  return {
    "user-agent": REQUEST_USER_AGENT,
    accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9"
  };
}

const runContextCache = new Map<number, { value: RunContext; until: number }>();
const RUN_CONTEXT_TTL_MS = 60_000;

type RunContext = {
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

function isDocumentUrl(normalized: string): boolean {
  try {
    const p = new URL(normalized).pathname.toLowerCase();
    return /\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|gz|tgz|rar|7z|tar|csv|xml|json)$/i.test(p);
  } catch {
    return false;
  }
}

function isAllowedByScope(host: string, ctx: RunContext): boolean {
  const h = host.toLowerCase();
  if (ctx.config.scopeMode === "same_domain") {
    const b = baseDomain(h);
    return b === ctx.seedBaseDomain;
  }
  return h === ctx.seedHost;
}

function normalizeCandidateUrl(baseUrl: string, rawHref: string, ctx: RunContext): string | null {
  const href = rawHref.trim();
  if (!href || href.startsWith("mailto:") || href.startsWith("tel:") || href.startsWith("javascript:")) {
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
  if ((resolved.protocol === "https:" && resolved.port === "443") || (resolved.protocol === "http:" && resolved.port === "80")) {
    resolved.port = "";
  }
  return resolved.toString();
}

async function getRunContext(crawlRunId: number): Promise<RunContext> {
  const now = Date.now();
  const cached = runContextCache.get(crawlRunId);
  if (cached && cached.until > now) {
    return cached.value;
  }
  const res = await pgPool.query(`SELECT normalized_seed_url, run_config FROM crawl_runs WHERE id = $1`, [crawlRunId]);
  if (!res.rowCount) {
    const fallback: RunContext = {
      seedHost: "",
      seedBaseDomain: "",
      config: { ...DEFAULT_CRAWL_RUN_CONFIG }
    };
    return fallback;
  }
  const row = res.rows[0] as { normalized_seed_url: string; run_config: Partial<CrawlRunConfig> | null };
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

const fetchGateway = createFetchGateway(fetchGlobalMax, fetchPerHostMax);
const { pacer: hostPacer, minGapMs: fetchMinGapPerHostMs, jitterMaxMs: fetchGapJitterMs } = loadHostPacerFromEnv();
const {
  cooldown: hostCooldown,
  baseBackoffMs: fetchHostCooldownBaseMs,
  maxBackoffMs: fetchHostCooldownMaxMs
} = loadHostCooldownFromEnv();

type ClaimedUrl = {
  id: number;
  crawl_run_id: number;
  normalized_url: string;
  retry_count: number;
  depth: number;
};

function logW(crawlRunId: number, urlId: number, msg: string): void {
  process.stdout.write(`[worker worker_id=${workerId} crawl_run=${crawlRunId} url_id=${urlId}] ${msg}\n`);
}

async function claimUrl(urlId: number, crawlRunIdHint: number): Promise<ClaimedUrl | null> {
  logW(crawlRunIdHint, urlId, "claim-attempt");
  const res = await pgPool.query(
    `
      UPDATE crawl_urls
      SET status = 'IN_PROGRESS',
          claimed_at = NOW(),
          claimed_by_worker = $2
      WHERE id = $1
        AND status = 'QUEUED'
      RETURNING id, crawl_run_id, normalized_url, retry_count, depth
    `,
    [urlId, workerId]
  );
  if (res.rowCount) {
    const row = res.rows[0] as ClaimedUrl;
    logW(row.crawl_run_id, row.id, "claim-success");
    return row;
  }
  logW(crawlRunIdHint, urlId, "claim-skip reason=not_queued");
  return null;
}

async function markVisited(
  crawlRunId: number,
  urlId: number,
  httpStatus: number | null,
  contentType: string | null
): Promise<void> {
  await pgPool.query(
    `
      UPDATE crawl_urls
      SET status = 'VISITED',
          last_error = NULL,
          http_status = $2,
          content_type = $3,
          visited_at = NOW(),
          claimed_at = NULL,
          claimed_by_worker = NULL
      WHERE id = $1
    `,
    [urlId, httpStatus, contentType]
  );
  crawlUrlsVisitedTotal.inc();
  logW(crawlRunId, urlId, `complete status=VISITED http_status=${httpStatus ?? "null"}`);
}

function getRetryDelayMs(retryCount: number, backoffMultiplier = 1): number {
  const computed = RETRY_BASE_DELAY_MS * (2 ** retryCount) * backoffMultiplier;
  return Math.min(RETRY_MAX_DELAY_MS, computed);
}

async function markFailed(
  crawlRunId: number,
  urlId: number,
  message: string,
  httpStatus: number | null,
  contentType: string | null
): Promise<void> {
  await pgPool.query(
    `
      UPDATE crawl_urls
      SET status = 'FAILED',
          last_error = $2,
          http_status = $3,
          content_type = $4,
          claimed_at = NULL,
          claimed_by_worker = NULL
      WHERE id = $1
    `,
    [urlId, message, httpStatus, contentType]
  );
  crawlUrlsFailedTotal.inc();
  logW(crawlRunId, urlId, `terminal-failure reason="${message}"`);
}

async function markTerminalHttpOutcome(
  crawlRunId: number,
  urlId: number,
  terminalStatus: "REDIRECT_301" | "FORBIDDEN" | "NOT_FOUND" | "HTTP_TERMINAL",
  message: string,
  httpStatus: number | null,
  contentType: string | null
): Promise<void> {
  await pgPool.query(
    `
      UPDATE crawl_urls
      SET status = $2,
          last_error = $3,
          http_status = $4,
          content_type = $5,
          claimed_at = NULL,
          claimed_by_worker = NULL
      WHERE id = $1
    `,
    [urlId, terminalStatus, message, httpStatus, contentType]
  );
  logW(crawlRunId, urlId, `terminal-http status=${terminalStatus} reason="${message}"`);
}

async function markFailedOrRetry(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  classification: FetchClassification,
  maxRetries: number
): Promise<void> {
  const shouldRetry = classification.retryable && retryCount < maxRetries;
  if (shouldRetry) {
    const backoffMultiplier = classification.backoffMultiplier ?? 1;
    const delay = getRetryDelayMs(retryCount, backoffMultiplier);
    await pgPool.query(
      `
        UPDATE crawl_urls
        SET status = 'QUEUED',
            retry_count = retry_count + 1,
            last_error = $2,
            http_status = $3,
            content_type = $4,
            claimed_at = NULL,
            claimed_by_worker = NULL
        WHERE id = $1
      `,
      [urlId, classification.reason, classification.httpStatus, classification.contentType]
    );
    await queue.add("crawl-url", { crawlRunId, urlId }, { delay, removeOnComplete: 2000, removeOnFail: 2000 });
    crawlUrlsRetriedTotal.inc();
    crawlUrlsRequeuedTotal.inc();
    logW(crawlRunId, urlId, `retry-scheduled attempt=${retryCount + 1} delay_ms=${delay} reason="${classification.reason}"`);
    return;
  }

  if (
    classification.terminalStatus === "FORBIDDEN" ||
    classification.terminalStatus === "NOT_FOUND" ||
    classification.terminalStatus === "HTTP_TERMINAL" ||
    classification.terminalStatus === "REDIRECT_301"
  ) {
    await markTerminalHttpOutcome(
      crawlRunId,
      urlId,
      classification.terminalStatus,
      classification.reason,
      classification.httpStatus,
      classification.contentType
    );
    return;
  }
  await markFailed(crawlRunId, urlId, classification.reason, classification.httpStatus, classification.contentType);
}

async function markFailedOrRetryFromError(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  err: unknown,
  maxRetries: number
): Promise<void> {
  const classification = classifyExecutionError(err);
  await markFailedOrRetry(crawlRunId, urlId, retryCount, classification, maxRetries);
}

async function markFailedOrRetryFromResponse(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  statusCode: number,
  contentType: string | null,
  maxRetries: number
): Promise<void> {
  const classification = classifyHttpResponse(statusCode, contentType);
  if (classification.reason === "success") {
    return;
  }
  await markFailedOrRetry(crawlRunId, urlId, retryCount, classification, maxRetries);
}

async function markDiscoveredUrlsEnqueued(crawlRunId: number, insertedIds: { id: number }[]): Promise<void> {
  const jobs = insertedIds.map((row) => ({
    name: "crawl-url",
    data: { crawlRunId, urlId: Number(row.id) },
    opts: { removeOnComplete: 2000, removeOnFail: 2000 }
  }));
  if (jobs.length > 0) {
    await queue.addBulk(jobs);
    crawlUrlsRequeuedTotal.inc(jobs.length);
  }
}

async function storeDiscoveredUrls(
  crawlRunId: number,
  pairs: { normalized: string; raw: string }[],
  discoveredFromUrlId: number,
  discoveredDepth: number,
  maxPages: number
): Promise<{ inserted: { id: number }[]; duplicatesSkipped: number }> {
  if (pairs.length === 0) {
    return { inserted: [], duplicatesSkipped: 0 };
  }

  const countRes = await pgPool.query(`SELECT COUNT(*)::int AS c FROM crawl_urls WHERE crawl_run_id = $1`, [crawlRunId]);
  const existing = Number(countRes.rows[0]?.c ?? 0);
  const remaining = Math.max(0, maxPages - existing);
  if (remaining === 0) {
    return { inserted: [], duplicatesSkipped: pairs.length };
  }
  const bounded = pairs.slice(0, remaining);

  const norms = bounded.map((p) => p.normalized);
  const raws = bounded.map((p) => p.raw);

  const insertRes = await pgPool.query(
    `
      INSERT INTO crawl_urls (crawl_run_id, normalized_url, raw_url, discovered_from_url_id, status, depth)
      SELECT $1, t.norm, t.raw, $3, 'QUEUED', $5
      FROM UNNEST($2::text[], $4::text[]) AS t(norm, raw)
      ON CONFLICT (crawl_run_id, normalized_url) DO NOTHING
      RETURNING id
    `,
    [crawlRunId, norms, discoveredFromUrlId, raws, discoveredDepth]
  );

  const insertedCount = insertRes.rowCount ?? 0;
  const duplicatesSkipped = pairs.length - insertedCount;
  if (insertedCount > 0) {
    crawlUrlsDiscoveredTotal.inc(insertedCount);
  }
  if (duplicatesSkipped > 0) {
    await pgPool.query(
      `
        UPDATE crawl_runs
        SET duplicates_skipped = duplicates_skipped + $2
        WHERE id = $1
      `,
      [crawlRunId, duplicatesSkipped]
    );
  }

  return { inserted: insertRes.rows, duplicatesSkipped };
}

function extractLinkPairs(
  baseUrl: string,
  html: string,
  runContext: RunContext
): { normalized: string; raw: string }[] {
  const $ = load(html);
  const out: { normalized: string; raw: string }[] = [];
  const seen = new Set<string>();
  $("a[href]").each((_idx, el) => {
    const href = $(el).attr("href");
    if (!href) {
      return;
    }
    const raw = href.trim();
    const normalized = normalizeCandidateUrl(baseUrl, raw, runContext);
    if (normalized && !seen.has(normalized)) {
      if (!runContext.config.includeDocuments && isDocumentUrl(normalized)) {
        return;
      }
      seen.add(normalized);
      out.push({ normalized, raw });
    }
  });
  return out;
}

async function processJob(job: Job<CrawlJobPayload>): Promise<void> {
  const payload = job.data;
  const queueLatencySec = Math.max(0, (Date.now() - job.timestamp) / 1000);
  crawlQueueLatencySeconds.observe(queueLatencySec);

  const claimed = await claimUrl(payload.urlId, payload.crawlRunId);
  if (!claimed) {
    return;
  }

  const fetchHost = new URL(claimed.normalized_url).hostname;

  const processingTimer = crawlProcessingDurationSeconds.startTimer();
  try {
    const runContext = await getRunContext(claimed.crawl_run_id);
    logW(claimed.crawl_run_id, claimed.id, `fetch-start url=${claimed.normalized_url}`);
    if (runContext.config.demoDelayMs > 0) {
      await new Promise((r) => setTimeout(r, runContext.config.demoDelayMs));
    }

    await hostCooldown.waitUntilCool(fetchHost);
    await hostPacer.waitBeforeOutboundFetch(fetchHost);

    const ac = new AbortController();
    const timeoutHandle = setTimeout(() => ac.abort(), runContext.config.requestTimeoutMs);
    const fetchTimer = crawlFetchDurationSeconds.startTimer();
    let statusCode = 0;
    let contentType: string | null = null;
    let readBodyText: () => Promise<string> = async () => "";
    try {
      if (runContext.config.followRedirects) {
        const response = await fetchGateway.run(claimed.normalized_url, () =>
          undiciFetch(claimed.normalized_url, {
            method: "GET",
            headers: buildRequestHeaders(),
            signal: ac.signal,
            redirect: "follow"
          })
        );
        statusCode = response.status;
        contentType = response.headers.get("content-type");
        readBodyText = () => response.text();
      } else {
        const response = await fetchGateway.run(claimed.normalized_url, () =>
          request(claimed.normalized_url, {
            method: "GET",
            headers: buildRequestHeaders(),
            signal: ac.signal
          })
        );
        statusCode = response.statusCode;
        const contentTypeHeader = response.headers["content-type"];
        contentType = typeof contentTypeHeader === "string" ? contentTypeHeader : null;
        readBodyText = () => response.body.text();
      }
    } finally {
      clearTimeout(timeoutHandle);
    }
    fetchTimer();
    logW(
      claimed.crawl_run_id,
      claimed.id,
      `fetch-result status_code=${statusCode} content_type="${contentType ?? ""}"`
    );

    const responseClass = classifyHttpResponse(statusCode, contentType);
    if (responseClass.reason !== "success") {
      if (shouldCooldownForHttpClassification(responseClass)) {
        await hostCooldown.recordNegative(fetchHost);
      }
      await markFailedOrRetryFromResponse(
        claimed.crawl_run_id,
        claimed.id,
        claimed.retry_count,
        statusCode,
        contentType,
        runContext.config.maxRetries
      );
      return;
    }

    await hostCooldown.recordSuccess(fetchHost);

    if (!String(contentType ?? "").toLowerCase().includes("text/html")) {
      await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType);
      return;
    }

    let html: string;
    try {
      html = await readBodyText();
    } catch (err) {
      const execClass = classifyExecutionError(err);
      if (shouldCooldownForExecutionClassification(execClass)) {
        await hostCooldown.recordNegative(fetchHost);
      }
      await markFailedOrRetryFromError(
        claimed.crawl_run_id,
        claimed.id,
        claimed.retry_count,
        err,
        runContext.config.maxRetries
      );
      return;
    }

    if (claimed.depth >= runContext.config.maxDepth) {
      await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType);
      logW(claimed.crawl_run_id, claimed.id, "complete mode=max_depth");
      return;
    }

    let pairs: { normalized: string; raw: string }[];
    try {
      pairs = extractLinkPairs(claimed.normalized_url, html, runContext);
    } catch (err) {
      await markFailed(
        claimed.crawl_run_id,
        claimed.id,
        `html_parse_error: ${(err as Error).message}`,
        statusCode,
        contentType
      );
      return;
    }

    const stored = await storeDiscoveredUrls(
      claimed.crawl_run_id,
      pairs,
      claimed.id,
      claimed.depth + 1,
      runContext.config.maxPages
    );
    await markDiscoveredUrlsEnqueued(claimed.crawl_run_id, stored.inserted);
    await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType);
    logW(
      claimed.crawl_run_id,
      claimed.id,
      `complete mode=html discovered=${pairs.length} inserted=${stored.inserted.length}`
    );
  } catch (err) {
    const runContext = await getRunContext(claimed.crawl_run_id);
    const execClass = classifyExecutionError(err);
    if (shouldCooldownForExecutionClassification(execClass)) {
      await hostCooldown.recordNegative(fetchHost);
    }
    await markFailedOrRetryFromError(
      claimed.crawl_run_id,
      claimed.id,
      claimed.retry_count,
      err,
      runContext.config.maxRetries
    );
  } finally {
    processingTimer();
    processedUrlsTotal.inc();
  }
}

const worker = new Worker<CrawlJobPayload>(
  CRAWL_QUEUE_NAME,
  async (job) => processJob(job),
  {
    connection: redisConnection,
    concurrency: workerConcurrency
  }
);

worker.on("failed", (_job, _err) => undefined);
worker.on("error", (_err) => undefined);

http
  .createServer(async (req, res) => {
    if (req.url === "/metrics" || req.url?.startsWith("/metrics?")) {
      try {
        const { body, contentType } = await metricsHandler();
        res.writeHead(200, { "Content-Type": contentType });
        res.end(body);
      } catch (err) {
        res.writeHead(500).end((err as Error).message);
      }
      return;
    }
    res.writeHead(404).end();
  })
  .listen(metricsPort, () => {
    process.stdout.write(
      `[component=worker worker_id=${workerId}] metrics listening on :${metricsPort} path=/metrics\n`
    );
  });

process.stdout.write(
  `[component=worker worker_id=${workerId}] started bullmq_concurrency=${workerConcurrency} fetch_concurrency=${fetchGlobalMax} fetch_per_host=${fetchPerHostMax} fetch_min_gap_per_host_ms=${fetchMinGapPerHostMs} fetch_gap_jitter_ms=${fetchGapJitterMs} fetch_host_cooldown_base_ms=${fetchHostCooldownBaseMs} fetch_host_cooldown_max_ms=${fetchHostCooldownMaxMs}\n`
);

process.on("SIGINT", async () => {
  await worker.close();
  await queue.close();
  await redisConnection.quit();
  await pgPool.end();
  process.exit(0);
});
