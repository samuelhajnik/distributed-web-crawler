import type { Job } from "bullmq";
import { fetch as undiciFetch, request } from "undici";
import {
  classifyExecutionError,
  classifyHttpResponse,
  type CrawlJobPayload,
  topUpRunSignals
} from "@crawler/shared";
import { buildRequestHeaders, logW } from "../config";
import { retryAfterFromUndiciHeaders } from "../fetch/undiciHeaders";
import { getEffectiveFinalUrl, type RedirectResolution } from "../fetch/redirects";
import { extractLinkPairs } from "../html/linkExtractor";
import {
  crawlFetchDurationSeconds,
  crawlProcessingDurationSeconds,
  crawlQueueLatencySeconds,
  processedUrlsTotal
} from "../prometheus";
import { crawlJobQueue } from "../queue";
import {
  claimNextQueuedUrl,
  hasClaimableQueuedUrls,
  markFailed,
  markRedirectOutOfScope,
  markVisited,
  type ClaimedUrl
} from "../repositories/urlClaimRepository";
import { storeDiscoveredUrls } from "../repositories/urlDiscoveryRepository";
import { getRunContext, isUrlInScope, type RunContext } from "../runContext";
import {
  shouldCooldownForExecutionClassification,
  shouldCooldownForHttpClassification
} from "../hostCooldown";
import { fetchGateway, hostCooldown, hostPacer } from "../workerDeps";
import { markFailedOrRetryFromError, markFailedOrRetryFromResponse } from "./retryPolicy";

type FetchResult = {
  statusCode: number;
  contentType: string | null;
  retryAfterHeader: string | null;
  resolution: RedirectResolution;
  readBodyText: () => Promise<string>;
};

export async function processCrawlJob(job: Job<CrawlJobPayload>): Promise<void> {
  observeQueueLatency(job);

  const claimed = await claimNextQueuedUrl(job.data.crawlRunId);
  if (!claimed) {
    return;
  }

  const processingTimer = crawlProcessingDurationSeconds.startTimer();
  try {
    await processClaimedUrl(claimed);
  } finally {
    processingTimer();
    processedUrlsTotal.inc();
  }
}

function observeQueueLatency(job: Job<CrawlJobPayload>): void {
  const queueLatencySec = Math.max(0, (Date.now() - job.timestamp) / 1000);
  crawlQueueLatencySeconds.observe(queueLatencySec);
}

async function processClaimedUrl(claimed: ClaimedUrl): Promise<void> {
  const requestedHost = new URL(claimed.normalized_url).hostname;
  let effectiveHost = requestedHost;

  try {
    const runContext = await getRunContext(claimed.crawl_run_id);
    logW(claimed.crawl_run_id, claimed.id, `fetch-start url=${claimed.normalized_url}`);
    await waitBeforeFetch(requestedHost, runContext);

    const ac = new AbortController();
    const timeoutHandle = setTimeout(() => ac.abort(), runContext.config.requestTimeoutMs);
    try {
      const fetchResult = await fetchClaimedUrl(claimed, runContext, ac.signal);
      effectiveHost = resolveEffectiveHost(fetchResult.resolution, requestedHost);
      logW(
        claimed.crawl_run_id,
        claimed.id,
        `fetch-result status_code=${fetchResult.statusCode} content_type="${fetchResult.contentType ?? ""}" requested_url=${fetchResult.resolution.requestedUrl} final_url=${fetchResult.resolution.finalUrl}`
      );
      await handleFetchedUrl(claimed, runContext, fetchResult, effectiveHost);
    } finally {
      clearTimeout(timeoutHandle);
    }
  } catch (err) {
    const runContext = await getRunContext(claimed.crawl_run_id);
    const execClass = classifyExecutionError(err);
    if (shouldCooldownForExecutionClassification(execClass)) {
      await hostCooldown.recordNegative(effectiveHost);
    }
    await markFailedOrRetryFromError(
      claimed.crawl_run_id,
      claimed.id,
      claimed.retry_count,
      err,
      runContext.config.maxRetries
    );
    await maybeTopUpRunSignals(claimed.crawl_run_id);
  }
}

async function waitBeforeFetch(requestedHost: string, runContext: RunContext): Promise<void> {
  if (runContext.config.demoDelayMs > 0) {
    await new Promise((r) => setTimeout(r, runContext.config.demoDelayMs));
  }
  await hostCooldown.waitUntilCool(requestedHost);
  await hostPacer.waitBeforeOutboundFetch(requestedHost);
}

async function fetchClaimedUrl(
  claimed: ClaimedUrl,
  runContext: RunContext,
  signal: AbortSignal
): Promise<FetchResult> {
  const fetchTimer = crawlFetchDurationSeconds.startTimer();
  let statusCode = 0;
  let contentType: string | null = null;
  let retryAfterHeader: string | null = null;
  let readBodyText: () => Promise<string> = async () => "";
  let resolution: RedirectResolution = {
    requestedUrl: claimed.normalized_url,
    finalUrl: claimed.normalized_url,
    redirected: false,
    finalInScope: true
  };

  if (runContext.config.followRedirects) {
    const response = await fetchGateway.run(claimed.normalized_url, () =>
      undiciFetch(claimed.normalized_url, {
        method: "GET",
        headers: buildRequestHeaders(),
        signal,
        redirect: "follow"
      })
    );
    statusCode = response.status;
    contentType = response.headers.get("content-type");
    retryAfterHeader = response.headers.get("retry-after");
    readBodyText = () => response.text();
    const finalUrl = getEffectiveFinalUrl(response.url, claimed.normalized_url);
    const redirected = finalUrl !== claimed.normalized_url;
    const finalInScope = !redirected || isUrlInScope(finalUrl, runContext);
    resolution = {
      requestedUrl: claimed.normalized_url,
      finalUrl,
      redirected,
      finalInScope
    };
  } else {
    const response = await fetchGateway.run(claimed.normalized_url, () =>
      request(claimed.normalized_url, {
        method: "GET",
        headers: buildRequestHeaders(),
        signal
      })
    );
    statusCode = response.statusCode;
    const contentTypeHeader = response.headers["content-type"];
    contentType = typeof contentTypeHeader === "string" ? contentTypeHeader : null;
    retryAfterHeader = retryAfterFromUndiciHeaders(response.headers);
    readBodyText = () => response.body.text();
  }

  fetchTimer();
  return { statusCode, contentType, retryAfterHeader, resolution, readBodyText };
}

async function handleFetchedUrl(
  claimed: ClaimedUrl,
  runContext: RunContext,
  fetchResult: FetchResult,
  effectiveHost: string
): Promise<void> {
  const { statusCode, contentType } = fetchResult;
  const responseClass = classifyHttpResponse(statusCode, contentType);
  if (responseClass.reason !== "success") {
    await handleNonSuccessResponse(claimed, runContext, fetchResult, effectiveHost);
    return;
  }
  await handleSuccessfulResponse(claimed, runContext, fetchResult, effectiveHost);
}

async function handleNonSuccessResponse(
  claimed: ClaimedUrl,
  runContext: RunContext,
  fetchResult: FetchResult,
  effectiveHost: string
): Promise<void> {
  const { statusCode, contentType, retryAfterHeader } = fetchResult;
  const responseClass = classifyHttpResponse(statusCode, contentType);
  if (shouldCooldownForHttpClassification(responseClass)) {
    await hostCooldown.recordNegative(effectiveHost);
  }
  await markFailedOrRetryFromResponse(
    claimed.crawl_run_id,
    claimed.id,
    claimed.retry_count,
    statusCode,
    contentType,
    runContext.config.maxRetries,
    retryAfterHeader
  );
  await maybeTopUpRunSignals(claimed.crawl_run_id);
}

async function handleSuccessfulResponse(
  claimed: ClaimedUrl,
  runContext: RunContext,
  fetchResult: FetchResult,
  effectiveHost: string
): Promise<void> {
  const { statusCode, contentType, resolution } = fetchResult;

  await hostCooldown.recordSuccess(effectiveHost);

  if (resolution.redirected && !resolution.finalInScope) {
    await markRedirectOutOfScope(
      claimed.id,
      claimed.crawl_run_id,
      statusCode,
      contentType,
      resolution
    );
    await maybeTopUpRunSignals(claimed.crawl_run_id);
    return;
  }

  if (
    !String(contentType ?? "")
      .toLowerCase()
      .includes("text/html")
  ) {
    await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType, resolution);
    await maybeTopUpRunSignals(claimed.crawl_run_id);
    return;
  }

  await handleHtmlResponse(claimed, runContext, fetchResult, effectiveHost);
}

async function handleHtmlResponse(
  claimed: ClaimedUrl,
  runContext: RunContext,
  fetchResult: FetchResult,
  effectiveHost: string
): Promise<void> {
  const { statusCode, contentType, resolution, readBodyText } = fetchResult;

  let html: string;
  try {
    html = await readBodyText();
  } catch (err) {
    await handleBodyReadError(claimed, runContext, err, effectiveHost);
    return;
  }

  if (claimed.depth >= runContext.config.maxDepth) {
    await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType, resolution);
    logW(claimed.crawl_run_id, claimed.id, "complete mode=max_depth");
    await maybeTopUpRunSignals(claimed.crawl_run_id);
    return;
  }

  let pairs: { normalized: string; raw: string }[];
  try {
    pairs = extractLinkPairs(resolution.finalUrl, html, runContext);
  } catch (err) {
    await markFailed(
      claimed.crawl_run_id,
      claimed.id,
      `html_parse_error: ${(err as Error).message}`,
      statusCode,
      contentType
    );
    await maybeTopUpRunSignals(claimed.crawl_run_id);
    return;
  }

  const stored = await storeDiscoveredUrls(
    claimed.crawl_run_id,
    pairs,
    claimed.id,
    claimed.depth + 1,
    runContext.config.maxPages
  );
  await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType, resolution);
  await maybeTopUpRunSignals(claimed.crawl_run_id);
  logW(
    claimed.crawl_run_id,
    claimed.id,
    `complete mode=html discovered=${pairs.length} inserted=${stored.inserted.length}`
  );
}

async function handleBodyReadError(
  claimed: ClaimedUrl,
  runContext: RunContext,
  err: unknown,
  effectiveHost: string
): Promise<void> {
  const execClass = classifyExecutionError(err);
  if (shouldCooldownForExecutionClassification(execClass)) {
    await hostCooldown.recordNegative(effectiveHost);
  }
  await markFailedOrRetryFromError(
    claimed.crawl_run_id,
    claimed.id,
    claimed.retry_count,
    err,
    runContext.config.maxRetries
  );
  await maybeTopUpRunSignals(claimed.crawl_run_id);
}

async function maybeTopUpRunSignals(crawlRunId: number): Promise<void> {
  if (await hasClaimableQueuedUrls(crawlRunId)) {
    await topUpRunSignals(crawlJobQueue, crawlRunId);
  }
}

function resolveEffectiveHost(resolution: RedirectResolution, fallbackHost: string): string {
  try {
    return new URL(resolution.finalUrl).hostname;
  } catch {
    return fallbackHost;
  }
}
