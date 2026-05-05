import type { Job } from "bullmq";
import { fetch as undiciFetch, request } from "undici";
import {
  classifyExecutionError,
  classifyHttpResponse,
  type CrawlJobPayload
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
import {
  claimUrl,
  markFailed,
  markRedirectOutOfScope,
  markVisited
} from "../repositories/urlClaimRepository";
import {
  markDiscoveredUrlsEnqueued,
  storeDiscoveredUrls
} from "../repositories/urlDiscoveryRepository";
import { getRunContext, isUrlInScope } from "../runContext";
import {
  shouldCooldownForExecutionClassification,
  shouldCooldownForHttpClassification
} from "../hostCooldown";
import { fetchGateway, hostCooldown, hostPacer } from "../workerDeps";
import { markFailedOrRetryFromError, markFailedOrRetryFromResponse } from "./retryPolicy";

export async function processCrawlJob(job: Job<CrawlJobPayload>): Promise<void> {
  const payload = job.data;
  const queueLatencySec = Math.max(0, (Date.now() - job.timestamp) / 1000);
  crawlQueueLatencySeconds.observe(queueLatencySec);

  const claimed = await claimUrl(payload.urlId, payload.crawlRunId);
  if (!claimed) {
    return;
  }

  const requestedHost = new URL(claimed.normalized_url).hostname;
  let effectiveHost = requestedHost;

  const processingTimer = crawlProcessingDurationSeconds.startTimer();
  try {
    const runContext = await getRunContext(claimed.crawl_run_id);
    logW(claimed.crawl_run_id, claimed.id, `fetch-start url=${claimed.normalized_url}`);
    if (runContext.config.demoDelayMs > 0) {
      await new Promise((r) => setTimeout(r, runContext.config.demoDelayMs));
    }

    await hostCooldown.waitUntilCool(requestedHost);
    await hostPacer.waitBeforeOutboundFetch(requestedHost);

    const ac = new AbortController();
    const timeoutHandle = setTimeout(() => ac.abort(), runContext.config.requestTimeoutMs);
    try {
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
            signal: ac.signal,
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
            signal: ac.signal
          })
        );
        statusCode = response.statusCode;
        const contentTypeHeader = response.headers["content-type"];
        contentType = typeof contentTypeHeader === "string" ? contentTypeHeader : null;
        retryAfterHeader = retryAfterFromUndiciHeaders(response.headers);
        readBodyText = () => response.body.text();
      }
      fetchTimer();
      try {
        effectiveHost = new URL(resolution.finalUrl).hostname;
      } catch {
        effectiveHost = requestedHost;
      }
      logW(
        claimed.crawl_run_id,
        claimed.id,
        `fetch-result status_code=${statusCode} content_type="${contentType ?? ""}" requested_url=${resolution.requestedUrl} final_url=${resolution.finalUrl}`
      );

      const responseClass = classifyHttpResponse(statusCode, contentType);
      if (responseClass.reason !== "success") {
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
        return;
      }

      await hostCooldown.recordSuccess(effectiveHost);

      if (resolution.redirected && !resolution.finalInScope) {
        await markRedirectOutOfScope(
          claimed.id,
          claimed.crawl_run_id,
          statusCode,
          contentType,
          resolution
        );
        return;
      }

      if (
        !String(contentType ?? "")
          .toLowerCase()
          .includes("text/html")
      ) {
        await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType, resolution);
        return;
      }

      let html: string;
      try {
        html = await readBodyText();
      } catch (err) {
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
        return;
      }

      if (claimed.depth >= runContext.config.maxDepth) {
        await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType, resolution);
        logW(claimed.crawl_run_id, claimed.id, "complete mode=max_depth");
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
      await markVisited(claimed.crawl_run_id, claimed.id, statusCode, contentType, resolution);
      logW(
        claimed.crawl_run_id,
        claimed.id,
        `complete mode=html discovered=${pairs.length} inserted=${stored.inserted.length}`
      );
    } finally {
      // Keep timeout active across the full request+redirect+body lifecycle.
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
  } finally {
    processingTimer();
    processedUrlsTotal.inc();
  }
}
