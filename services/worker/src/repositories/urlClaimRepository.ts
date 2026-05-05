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

export async function claimUrl(urlId: number, crawlRunIdHint: number): Promise<ClaimedUrl | null> {
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

export async function markVisited(
  crawlRunId: number,
  urlId: number,
  httpStatus: number | null,
  contentType: string | null,
  resolution: RedirectResolution
): Promise<void> {
  await pgPool.query(
    `
      UPDATE crawl_urls
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
      WHERE id = $1
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

export async function markTerminalHttpOutcome(
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

export async function markRedirectOutOfScope(
  urlId: number,
  crawlRunId: number,
  statusCode: number,
  contentType: string | null,
  resolution: RedirectResolution
): Promise<void> {
  await pgPool.query(
    `
      UPDATE crawl_urls
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
      WHERE id = $1
    `,
    [urlId, statusCode, contentType, resolution.requestedUrl, resolution.finalUrl]
  );
  logW(crawlRunId, urlId, "complete status=REDIRECT_OUT_OF_SCOPE");
}
