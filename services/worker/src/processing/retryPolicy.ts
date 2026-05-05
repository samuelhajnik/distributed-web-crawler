import type { FetchClassification } from "@crawler/shared";
import {
  classifyExecutionError,
  classifyHttpResponse,
  mergeRetryAfterWithBackoff,
  parseRetryAfterMs,
  pgPool,
  RETRY_BASE_DELAY_MS,
  RETRY_MAX_DELAY_MS
} from "@crawler/shared";
import { crawlJobQueue } from "../queue";
import { crawlUrlsRetriedTotal, crawlUrlsRequeuedTotal } from "../prometheus";
import { logW } from "../config";
import { markFailed, markTerminalHttpOutcome } from "../repositories/urlClaimRepository";

function getRetryDelayMs(retryCount: number, backoffMultiplier = 1): number {
  const computed = RETRY_BASE_DELAY_MS * 2 ** retryCount * backoffMultiplier;
  return Math.min(RETRY_MAX_DELAY_MS, computed);
}

async function markFailedOrRetry(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  classification: FetchClassification,
  maxRetries: number,
  retryAfterMs?: number | null
): Promise<void> {
  const shouldRetry = classification.retryable && retryCount < maxRetries;
  if (shouldRetry) {
    const backoffMultiplier = classification.backoffMultiplier ?? 1;
    const baseDelay = getRetryDelayMs(retryCount, backoffMultiplier);
    const delay =
      classification.httpStatus === 429
        ? mergeRetryAfterWithBackoff(baseDelay, retryAfterMs, RETRY_MAX_DELAY_MS)
        : baseDelay;
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
    await crawlJobQueue.add(
      "crawl-url",
      { crawlRunId, urlId },
      { delay, removeOnComplete: 2000, removeOnFail: 2000 }
    );
    crawlUrlsRetriedTotal.inc();
    crawlUrlsRequeuedTotal.inc();
    logW(
      crawlRunId,
      urlId,
      `retry-scheduled attempt=${retryCount + 1} delay_ms=${delay} reason="${classification.reason}"`
    );
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
  await markFailed(
    crawlRunId,
    urlId,
    classification.reason,
    classification.httpStatus,
    classification.contentType
  );
}

export async function markFailedOrRetryFromError(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  err: unknown,
  maxRetries: number
): Promise<void> {
  const classification = classifyExecutionError(err);
  await markFailedOrRetry(crawlRunId, urlId, retryCount, classification, maxRetries, null);
}

export async function markFailedOrRetryFromResponse(
  crawlRunId: number,
  urlId: number,
  retryCount: number,
  statusCode: number,
  contentType: string | null,
  maxRetries: number,
  retryAfterHeader?: string | null
): Promise<void> {
  const classification = classifyHttpResponse(statusCode, contentType);
  if (classification.reason === "success") {
    return;
  }
  const retryAfterMs =
    statusCode === 429 ? parseRetryAfterMs(retryAfterHeader ?? null, Date.now()) : null;
  await markFailedOrRetry(crawlRunId, urlId, retryCount, classification, maxRetries, retryAfterMs);
}
