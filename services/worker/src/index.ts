import http from "node:http";
import os from "node:os";
import type { Job } from "bullmq";
import { Worker } from "bullmq";
import { load } from "cheerio";
import { request } from "undici";
import {
  CRAWL_QUEUE_NAME,
  CrawlJobPayload,
  classifyExecutionError,
  classifyHttpResponse,
  createCrawlQueue,
  MAX_RETRIES,
  normalizeUrl,
  pgPool,
  RETRY_429_MULTIPLIER,
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS,
  redisConnection
} from "@crawler/shared";
import type { FetchClassification } from "@crawler/shared";
import { createFetchGateway } from "./fetchLimit";
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

const workerConcurrency = Number(process.env.WORKER_CONCURRENCY ?? 5);
const workerId = process.env.WORKER_ID ?? `${os.hostname()}-${process.pid}`;
const metricsPort = Number(process.env.WORKER_METRICS_PORT ?? 9091);
const queue = createCrawlQueue();

const runHostsCache = new Map<number, { hosts: Set<string>; until: number }>();
const RUN_HOSTS_TTL_MS = 60_000;

async function getAllowedHostsForRun(crawlRunId: number): Promise<Set<string>> {
  const now = Date.now();
  const cached = runHostsCache.get(crawlRunId);
  if (cached && cached.until > now) {
    return cached.hosts;
  }
  const res = await pgPool.query(`SELECT allowed_hosts FROM crawl_runs WHERE id = $1`, [crawlRunId]);
  if (!res.rowCount) {
    return new Set();
  }
  const hosts = new Set((res.rows[0].allowed_hosts as string[]).map((h) => String(h).toLowerCase()));
  runHostsCache.set(crawlRunId, { hosts, until: now + RUN_HOSTS_TTL_MS });
  return hosts;
}

const fetchGateway = createFetchGateway(
  Number(process.env.FETCH_CONCURRENCY ?? 4),
  Number(process.env.FETCH_CONCURRENCY_PER_HOST ?? 2)
);

type ClaimedUrl = {
  id: number;
  crawl_run_id: number;
  normalized_url: string;
  retry_count: number;
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
      RETURNING id, crawl_run_id, normalized_url, retry_count
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

async function markFailedOrRetry(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  classification: FetchClassification
): Promise<void> {
  const shouldRetry = classification.retryable && retryCount < MAX_RETRIES;
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

  await markFailed(crawlRunId, urlId, classification.reason, classification.httpStatus, classification.contentType);
}

async function markFailedOrRetryFromError(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  err: unknown
): Promise<void> {
  const classification = classifyExecutionError(err);
  await markFailedOrRetry(crawlRunId, urlId, retryCount, classification);
}

async function markFailedOrRetryFromResponse(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  statusCode: number,
  contentType: string | null
): Promise<void> {
  const classification = classifyHttpResponse(statusCode, contentType, RETRY_429_MULTIPLIER);
  if (classification.reason === "success") {
    return;
  }
  await markFailedOrRetry(crawlRunId, urlId, retryCount, classification);
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
  discoveredFromUrlId: number
): Promise<{ inserted: { id: number }[]; duplicatesSkipped: number }> {
  if (pairs.length === 0) {
    return { inserted: [], duplicatesSkipped: 0 };
  }

  const norms = pairs.map((p) => p.normalized);
  const raws = pairs.map((p) => p.raw);

  const insertRes = await pgPool.query(
    `
      INSERT INTO crawl_urls (crawl_run_id, normalized_url, raw_url, discovered_from_url_id, status)
      SELECT $1, t.norm, t.raw, $3, 'QUEUED'
      FROM UNNEST($2::text[], $4::text[]) AS t(norm, raw)
      ON CONFLICT (crawl_run_id, normalized_url) DO NOTHING
      RETURNING id
    `,
    [crawlRunId, norms, discoveredFromUrlId, raws]
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
  allowedHosts: ReadonlySet<string>
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
    const normalized = normalizeUrl(baseUrl, raw, allowedHosts);
    if (normalized && !seen.has(normalized)) {
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

  const processingTimer = crawlProcessingDurationSeconds.startTimer();
  try {
    logW(claimed.crawl_run_id, claimed.id, `fetch-start url=${claimed.normalized_url}`);

    const fetchTimer = crawlFetchDurationSeconds.startTimer();
    const response = await fetchGateway.run(claimed.normalized_url, () =>
      request(claimed.normalized_url, {
        method: "GET",
        headers: {
          "user-agent": "distributed-web-crawler/1.0"
        }
      })
    );
    fetchTimer();

    const statusCode = response.statusCode;
    const contentTypeHeader = response.headers["content-type"];
    const contentType = typeof contentTypeHeader === "string" ? contentTypeHeader : null;
    logW(
      claimed.crawl_run_id,
      claimed.id,
      `fetch-result status_code=${statusCode} content_type="${contentType ?? ""}"`
    );

    const responseClass = classifyHttpResponse(statusCode, contentType, RETRY_429_MULTIPLIER);
    if (responseClass.reason !== "success") {
      await markFailedOrRetryFromResponse(
        claimed.crawl_run_id,
        claimed.id,
        claimed.retry_count,
        statusCode,
        contentType
      );
      return;
    }

    if (!String(contentType ?? "").toLowerCase().includes("text/html")) {
      await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType);
      return;
    }

    let html: string;
    try {
      html = await response.body.text();
    } catch (err) {
      await markFailedOrRetryFromError(claimed.crawl_run_id, claimed.id, claimed.retry_count, err);
      return;
    }

    const allowedHosts = await getAllowedHostsForRun(claimed.crawl_run_id);
    let pairs: { normalized: string; raw: string }[];
    try {
      pairs = extractLinkPairs(claimed.normalized_url, html, allowedHosts);
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

    const stored = await storeDiscoveredUrls(claimed.crawl_run_id, pairs, claimed.id);
    await markDiscoveredUrlsEnqueued(claimed.crawl_run_id, stored.inserted);
    await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType);
    logW(
      claimed.crawl_run_id,
      claimed.id,
      `complete mode=html discovered=${pairs.length} inserted=${stored.inserted.length}`
    );
  } catch (err) {
    await markFailedOrRetryFromError(claimed.crawl_run_id, claimed.id, claimed.retry_count, err);
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
  `[component=worker worker_id=${workerId}] started bullmq_concurrency=${workerConcurrency} fetch_concurrency=${process.env.FETCH_CONCURRENCY ?? 4} fetch_per_host=${process.env.FETCH_CONCURRENCY_PER_HOST ?? 2}\n`
);

process.on("SIGINT", async () => {
  await worker.close();
  await queue.close();
  await redisConnection.quit();
  await pgPool.end();
  process.exit(0);
});
