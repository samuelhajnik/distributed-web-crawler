import type { FetchClassification } from "@crawler/shared";
import {
  buildRetryWakeSignalJob,
  classifyExecutionError,
  classifyHttpResponse,
  isDuplicateJobIdError,
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
    const requeueRes = await pgPool.query(
      `
        UPDATE crawl_urls u
        SET status = 'QUEUED',
            retry_count = retry_count + 1,
            last_error = $2,
            http_status = $3,
            content_type = $4,
            retry_after_at = NOW() + ($5::text || ' milliseconds')::interval,
            claimed_at = NULL,
            claimed_by_worker = NULL
        FROM crawl_runs r
        WHERE u.id = $1
          AND u.crawl_run_id = r.id
          AND u.status = 'IN_PROGRESS'
          AND r.status = 'RUNNING'
        RETURNING u.id, u.retry_after_at
      `,
      [urlId, classification.reason, classification.httpStatus, classification.contentType, delay]
    );
    if (!requeueRes.rowCount) {
      logW(crawlRunId, urlId, "retry-skip reason=not_in_progress_or_run_not_running");
      return;
    }
    const retryAfterAt = requeueRes.rows[0].retry_after_at as Date;
    const wakeJob = buildRetryWakeSignalJob(crawlRunId, delay, retryAfterAt);
    try {
      await crawlJobQueue.add(wakeJob.name, wakeJob.data, wakeJob.opts);
    } catch (err) {
      if (!isDuplicateJobIdError(err)) {
        throw err;
      }
    }
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
