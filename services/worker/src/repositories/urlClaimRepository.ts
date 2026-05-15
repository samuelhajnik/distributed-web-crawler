import { pgPool } from "@crawler/shared";
import type { RedirectResolution } from "../fetch/redirects";
import { logW, workerId } from "../config";
import { crawlUrlsFailedTotal, crawlUrlsVisitedTotal } from "../prometheus";

export type ClaimedUrl = {
  id: number;
  crawl_run_id: number;
  normalized_url: string;
  retry_count: number;
  depth: number;
};

const ACTIVE_URL_CLAIM_SQL = `
  u.id = $1
  AND u.crawl_run_id = r.id
  AND u.status = 'IN_PROGRESS'
  AND r.status = 'RUNNING'
`;

export async function hasClaimableQueuedUrls(crawlRunId: number): Promise<boolean> {
  const res = await pgPool.query(
    `
    SELECT EXISTS (
      SELECT 1
      FROM crawl_urls u
      INNER JOIN crawl_runs r ON r.id = u.crawl_run_id
      WHERE u.crawl_run_id = $1
        AND r.status = 'RUNNING'
        AND u.status = 'QUEUED'
        AND (u.retry_after_at IS NULL OR u.retry_after_at <= NOW())
    ) AS claimable
    `,
    [crawlRunId]
  );
  return Boolean(res.rows[0]?.claimable);
}

export async function claimNextQueuedUrl(crawlRunId: number): Promise<ClaimedUrl | null> {
  logW(crawlRunId, 0, "claim-next-attempt");
  const res = await pgPool.query(
    `
      WITH candidate AS (
        SELECT u.id
        FROM crawl_urls u
        INNER JOIN crawl_runs r ON r.id = u.crawl_run_id
        WHERE u.crawl_run_id = $1
          AND r.status = 'RUNNING'
          AND u.status = 'QUEUED'
          AND (u.retry_after_at IS NULL OR u.retry_after_at <= NOW())
        ORDER BY u.id
        LIMIT 1
        FOR UPDATE OF u SKIP LOCKED
      )
      UPDATE crawl_urls u
      SET status = 'IN_PROGRESS',
          claimed_at = NOW(),
          claimed_by_worker = $2,
          retry_after_at = NULL
      FROM candidate
      WHERE u.id = candidate.id
      RETURNING u.id, u.crawl_run_id, u.normalized_url, u.retry_count, u.depth
    `,
    [crawlRunId, workerId]
  );
  if (res.rowCount) {
    const row = res.rows[0] as ClaimedUrl;
    logW(row.crawl_run_id, row.id, "claim-next-success");
    return row;
  }
  logW(crawlRunId, 0, "claim-next-skip reason=no_claimable_queued_url");
  return null;
}

export async function markVisited(
  crawlRunId: number,
  urlId: number,
  httpStatus: number | null,
  contentType: string | null,
  resolution: RedirectResolution
): Promise<void> {
  const res = await pgPool.query(
    `
      UPDATE crawl_urls u
      SET status = CASE WHEN $6 THEN 'REDIRECT_FOLLOWED' ELSE 'VISITED' END,
          last_error = NULL,
          http_status = $2,
          content_type = $3,
          requested_url = $4,
          final_url = $5,
          redirected = $6,
          final_in_scope = $7,
          visited_at = NOW(),
          claimed_at = NULL,
          claimed_by_worker = NULL
      FROM crawl_runs r
      WHERE ${ACTIVE_URL_CLAIM_SQL}
    `,
    [
      urlId,
      httpStatus,
      contentType,
      resolution.requestedUrl,
      resolution.finalUrl,
      resolution.redirected,
      resolution.finalInScope
    ]
  );
  if (!res.rowCount) {
    logW(crawlRunId, urlId, "mark-visited-skip reason=not_in_progress_or_run_not_running");
    return;
  }
  crawlUrlsVisitedTotal.inc();
  logW(
    crawlRunId,
    urlId,
    `complete status=${resolution.redirected ? "REDIRECT_FOLLOWED" : "VISITED"} http_status=${httpStatus ?? "null"}`
  );
}

export async function markFailed(
  crawlRunId: number,
  urlId: number,
  message: string,
  httpStatus: number | null,
  contentType: string | null
): Promise<void> {
  const res = await pgPool.query(
    `
      UPDATE crawl_urls u
      SET status = 'FAILED',
          last_error = $2,
          http_status = $3,
          content_type = $4,
          claimed_at = NULL,
          claimed_by_worker = NULL
      FROM crawl_runs r
      WHERE ${ACTIVE_URL_CLAIM_SQL}
    `,
    [urlId, message, httpStatus, contentType]
  );
  if (!res.rowCount) {
    logW(crawlRunId, urlId, "mark-failed-skip reason=not_in_progress_or_run_not_running");
    return;
  }
  crawlUrlsFailedTotal.inc();
  logW(crawlRunId, urlId, `terminal-failure reason="${message}"`);
}

export async function markTerminalHttpOutcome(
  crawlRunId: number,
  urlId: number,
  terminalStatus: "REDIRECT_301" | "FORBIDDEN" | "NOT_FOUND" | "HTTP_TERMINAL",
  message: string,
  httpStatus: number | null,
  contentType: string | null
): Promise<void> {
  const res = await pgPool.query(
    `
      UPDATE crawl_urls u
      SET status = $2,
          last_error = $3,
          http_status = $4,
          content_type = $5,
          claimed_at = NULL,
          claimed_by_worker = NULL
      FROM crawl_runs r
      WHERE ${ACTIVE_URL_CLAIM_SQL}
    `,
    [urlId, terminalStatus, message, httpStatus, contentType]
  );
  if (!res.rowCount) {
    logW(crawlRunId, urlId, "mark-terminal-skip reason=not_in_progress_or_run_not_running");
    return;
  }
  logW(crawlRunId, urlId, `terminal-http status=${terminalStatus} reason="${message}"`);
}

export async function markRedirectOutOfScope(
  urlId: number,
  crawlRunId: number,
  statusCode: number,
  contentType: string | null,
  resolution: RedirectResolution
): Promise<void> {
  const res = await pgPool.query(
    `
      UPDATE crawl_urls u
      SET status = 'REDIRECT_OUT_OF_SCOPE',
          last_error = 'redirect_final_out_of_scope',
          http_status = $2,
          content_type = $3,
          requested_url = $4,
          final_url = $5,
          redirected = TRUE,
          final_in_scope = FALSE,
          visited_at = NOW(),
          claimed_at = NULL,
          claimed_by_worker = NULL
      FROM crawl_runs r
      WHERE ${ACTIVE_URL_CLAIM_SQL}
    `,
    [urlId, statusCode, contentType, resolution.requestedUrl, resolution.finalUrl]
  );
  if (!res.rowCount) {
    logW(crawlRunId, urlId, "mark-redirect-oos-skip reason=not_in_progress_or_run_not_running");
    return;
  }
  logW(crawlRunId, urlId, "complete status=REDIRECT_OUT_OF_SCOPE");
}
