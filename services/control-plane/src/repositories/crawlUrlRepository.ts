import { pgPool } from "@crawler/shared";

export class CrawlUrlRepository {
  async recoverStaleClaims(crawlRunId: number, staleSeconds: number): Promise<number[]> {
    const stale = await pgPool.query(
      `
      UPDATE crawl_urls u
      SET status = 'QUEUED',
          claimed_at = NULL,
          claimed_by_worker = NULL,
          retry_after_at = NULL
      FROM crawl_runs r
      WHERE u.crawl_run_id = $1
        AND u.crawl_run_id = r.id
        AND r.status = 'RUNNING'
        AND u.status = 'IN_PROGRESS'
        AND u.claimed_at IS NOT NULL
        AND u.claimed_at < NOW() - ($2::text || ' seconds')::interval
      RETURNING u.id
    `,
      [crawlRunId, staleSeconds]
    );
    return stale.rows.map((row) => Number(row.id));
  }

  async hasClaimableQueuedUrls(crawlRunId: number): Promise<boolean> {
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

  async getQueuedUrlIds(crawlRunId: number, limit: number): Promise<number[]> {
    const queuedRes = await pgPool.query(
      `
      SELECT u.id
      FROM crawl_urls u
      INNER JOIN crawl_runs r ON r.id = u.crawl_run_id
      WHERE u.crawl_run_id = $1
        AND r.status = 'RUNNING'
        AND u.status = 'QUEUED'
      ORDER BY u.id
      LIMIT $2
    `,
      [crawlRunId, limit]
    );
    return queuedRes.rows.map((row) => Number(row.id));
  }

  async getRunningGauges(): Promise<{ queued: number; in_progress: number; failed: number }> {
    const res = await pgPool.query(`
    SELECT
      COUNT(*) FILTER (WHERE u.status = 'QUEUED')::int AS queued,
      COUNT(*) FILTER (WHERE u.status = 'IN_PROGRESS')::int AS in_progress,
      COUNT(*) FILTER (WHERE u.status = 'FAILED')::int AS failed
    FROM crawl_urls u
    INNER JOIN crawl_runs r ON r.id = u.crawl_run_id
    WHERE r.status = 'RUNNING'
  `);
    return res.rows[0] as { queued: number; in_progress: number; failed: number };
  }

  async getRunSummaryTotals(crawlRunId: number): Promise<Record<string, number>> {
    const agg = await pgPool.query(
      `
      SELECT
        COUNT(*)::int AS total_discovered,
        COUNT(*) FILTER (WHERE status = 'VISITED')::int AS total_visited,
        COUNT(*) FILTER (WHERE status = 'REDIRECT_FOLLOWED')::int AS total_redirect_followed,
        COUNT(*) FILTER (WHERE status = 'REDIRECT_OUT_OF_SCOPE')::int AS total_redirect_out_of_scope,
        COUNT(*) FILTER (WHERE status = 'REDIRECT_301')::int AS total_redirect_301,
        COUNT(*) FILTER (WHERE status = 'FORBIDDEN')::int AS total_forbidden,
        COUNT(*) FILTER (WHERE status = 'NOT_FOUND')::int AS total_not_found,
        COUNT(*) FILTER (WHERE status = 'HTTP_TERMINAL')::int AS total_http_terminal,
        COUNT(*) FILTER (WHERE status = 'FAILED')::int AS total_failed,
        COUNT(*) FILTER (WHERE status = 'CANCELLED')::int AS total_cancelled,
        COUNT(*) FILTER (WHERE status = 'QUEUED')::int AS total_queued,
        COUNT(*) FILTER (WHERE status = 'IN_PROGRESS')::int AS total_in_progress
      FROM crawl_urls
      WHERE crawl_run_id = $1
      `,
      [crawlRunId]
    );
    return agg.rows[0] as Record<string, number>;
  }

  async getExportRows(crawlRunId: number, limit: number): Promise<Record<string, unknown>[]> {
    const rows = await pgPool.query(
      `
      SELECT
        id,
        normalized_url,
        status,
        http_status,
        content_type,
        retry_count,
        claimed_by_worker,
        claimed_at,
        visited_at,
        raw_url,
        discovered_from_url_id,
        depth,
        requested_url,
        final_url,
        redirected,
        final_in_scope
      FROM crawl_urls
      WHERE crawl_run_id = $1
      ORDER BY id
      LIMIT $2
      `,
      [crawlRunId, limit]
    );
    return rows.rows as Record<string, unknown>[];
  }

  async getGraphEdges(crawlRunId: number, limit: number): Promise<Record<string, unknown>[]> {
    const edges = await pgPool.query(
      `
      SELECT
        c.discovered_from_url_id AS from_url_id,
        c.id AS to_url_id,
        p.normalized_url AS from_normalized_url,
        c.normalized_url AS to_normalized_url,
        c.raw_url AS to_raw_url
      FROM crawl_urls c
      INNER JOIN crawl_urls p ON p.id = c.discovered_from_url_id
      WHERE c.crawl_run_id = $1
        AND c.discovered_from_url_id IS NOT NULL
      ORDER BY c.id
      LIMIT $2
      `,
      [crawlRunId, limit]
    );
    return edges.rows as Record<string, unknown>[];
  }

  async getNodeCount(crawlRunId: number): Promise<number> {
    const nodeCount = await pgPool.query(
      `SELECT COUNT(*)::int AS c FROM crawl_urls WHERE crawl_run_id = $1`,
      [crawlRunId]
    );
    return Number(nodeCount.rows[0].c);
  }

  async getUrlsPage(
    crawlRunId: number,
    status: string | null,
    limit: number,
    offset: number,
    sortCol: string,
    order: "ASC" | "DESC"
  ): Promise<{ total: number; rows: Record<string, unknown>[] }> {
    const totalRes = await pgPool.query(
      `
      SELECT COUNT(*)::int AS total
      FROM crawl_urls
      WHERE crawl_run_id = $1
        AND ($2::text IS NULL OR status = $2)
      `,
      [crawlRunId, status]
    );
    const total = Number(totalRes.rows[0].total);

    const urls = await pgPool.query(
      `
      SELECT
        id,
        normalized_url,
        raw_url,
        discovered_from_url_id,
        depth,
        status,
        retry_count,
        http_status,
        content_type,
        claimed_at,
        claimed_by_worker,
        visited_at,
        last_error,
        updated_at,
        requested_url,
        final_url,
        redirected,
        final_in_scope
      FROM crawl_urls
      WHERE crawl_run_id = $1
        AND ($2::text IS NULL OR status = $2)
      ORDER BY ${sortCol} ${order} NULLS LAST, id ASC
      LIMIT $3
      OFFSET $4
      `,
      [crawlRunId, status, limit, offset]
    );

    return { total, rows: urls.rows as Record<string, unknown>[] };
  }
}
