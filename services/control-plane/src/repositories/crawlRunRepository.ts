import { pgPool } from "@crawler/shared";
import type { RunCounts } from "../types";

export class CrawlRunRepository {
  async getById(crawlRunId: number): Promise<Record<string, unknown> | null> {
    const runRes = await pgPool.query("SELECT * FROM crawl_runs WHERE id = $1", [crawlRunId]);
    return runRes.rowCount === 0 ? null : (runRes.rows[0] as Record<string, unknown>);
  }

  async getRunningRunIds(): Promise<number[]> {
    const runRes = await pgPool.query("SELECT id FROM crawl_runs WHERE status = 'RUNNING'");
    return runRes.rows.map((row) => Number(row.id));
  }

  async getRunCounts(crawlRunId: number): Promise<RunCounts> {
    const countsRes = await pgPool.query(
      `
      SELECT
        COUNT(*) FILTER (WHERE status = 'QUEUED')::int AS queued_count,
        COUNT(*) FILTER (WHERE status = 'IN_PROGRESS')::int AS in_progress_count,
        COUNT(*) FILTER (WHERE status = 'VISITED')::int AS visited_count,
        COUNT(*) FILTER (WHERE status = 'REDIRECT_FOLLOWED')::int AS redirect_followed_count,
        COUNT(*) FILTER (WHERE status = 'REDIRECT_OUT_OF_SCOPE')::int AS redirect_out_of_scope_count,
        COUNT(*) FILTER (WHERE status = 'REDIRECT_301')::int AS redirect_301_count,
        COUNT(*) FILTER (WHERE status = 'FORBIDDEN')::int AS forbidden_count,
        COUNT(*) FILTER (WHERE status = 'NOT_FOUND')::int AS not_found_count,
        COUNT(*) FILTER (WHERE status = 'HTTP_TERMINAL')::int AS http_terminal_count,
        COUNT(*) FILTER (WHERE status = 'FAILED')::int AS failed_count
      FROM crawl_urls
      WHERE crawl_run_id = $1
    `,
      [crawlRunId]
    );

    return countsRes.rows[0] as RunCounts;
  }

  async listRecentWithTotals(limit: number): Promise<Record<string, unknown>[]> {
    const res = await pgPool.query(
      `
      SELECT
        r.id,
        r.status,
        r.seed_url,
        r.root_url,
        r.normalized_seed_url,
        r.started_at,
        r.completed_at,
        r.run_config,
        COUNT(u.id)::int AS discovered,
        COUNT(*) FILTER (WHERE u.status = 'VISITED')::int AS visited,
        COUNT(*) FILTER (WHERE u.status = 'QUEUED')::int AS queued,
        COUNT(*) FILTER (WHERE u.status = 'IN_PROGRESS')::int AS in_progress,
        COUNT(*) FILTER (WHERE u.status = 'FAILED')::int AS failed,
        COUNT(*) FILTER (WHERE u.status = 'CANCELLED')::int AS cancelled,
        COUNT(*) FILTER (WHERE u.status = 'REDIRECT_FOLLOWED')::int AS redirect_followed,
        COUNT(*) FILTER (WHERE u.status = 'REDIRECT_OUT_OF_SCOPE')::int AS redirect_out_of_scope,
        COUNT(*) FILTER (WHERE u.status = 'REDIRECT_301')::int AS redirect_301,
        COUNT(*) FILTER (WHERE u.status = 'FORBIDDEN')::int AS forbidden,
        COUNT(*) FILTER (WHERE u.status = 'NOT_FOUND')::int AS not_found,
        COUNT(*) FILTER (WHERE u.status = 'HTTP_TERMINAL')::int AS http_terminal
      FROM crawl_runs r
      LEFT JOIN crawl_urls u ON u.crawl_run_id = r.id
      GROUP BY r.id
      ORDER BY r.started_at DESC, r.id DESC
      LIMIT $1
      `,
      [limit]
    );
    return res.rows as Record<string, unknown>[];
  }

  async cancelRun(
    crawlRunId: number
  ): Promise<
    { notFound: true } | { status: string; changed: boolean; cancelled_url_count: number }
  > {
    const client = await pgPool.connect();
    try {
      await client.query("BEGIN");
      const runRes = await client.query(`SELECT status FROM crawl_runs WHERE id = $1 FOR UPDATE`, [
        crawlRunId
      ]);
      if (runRes.rowCount === 0) {
        await client.query("ROLLBACK");
        return { notFound: true };
      }
      const status = String(runRes.rows[0].status);
      if (status === "COMPLETED" || status === "FAILED") {
        await client.query("ROLLBACK");
        return { status, changed: false, cancelled_url_count: 0 };
      }
      if (status === "CANCELLED") {
        await client.query("ROLLBACK");
        return { status: "CANCELLED", changed: false, cancelled_url_count: 0 };
      }

      const runUpdate = await client.query(
        `
        UPDATE crawl_runs
        SET status = 'CANCELLED',
            completed_at = NOW()
        WHERE id = $1
          AND status = 'RUNNING'
        RETURNING id
      `,
        [crawlRunId]
      );
      if (runUpdate.rowCount === 0) {
        await client.query("ROLLBACK");
        const refreshed = await this.getById(crawlRunId);
        const current = String(refreshed?.status ?? status);
        return { status: current, changed: false, cancelled_url_count: 0 };
      }

      const urlUpdate = await client.query(
        `
        UPDATE crawl_urls
        SET status = 'CANCELLED',
            claimed_at = NULL,
            claimed_by_worker = NULL,
            last_error = 'crawl_cancelled'
        WHERE crawl_run_id = $1
          AND status IN ('QUEUED', 'IN_PROGRESS')
      `,
        [crawlRunId]
      );

      await client.query("COMMIT");
      return {
        status: "CANCELLED",
        changed: true,
        cancelled_url_count: urlUpdate.rowCount ?? 0
      };
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async markCompleted(
    crawlRunId: number,
    visitedCount: number,
    failedCount: number
  ): Promise<boolean> {
    const completed = await pgPool.query(
      `
        UPDATE crawl_runs
        SET status = 'COMPLETED',
            visited_count = $2,
            failed_count = $3,
            completed_at = NOW()
        WHERE id = $1 AND status = 'RUNNING'
        RETURNING id
      `,
      [crawlRunId, visitedCount, failedCount]
    );
    return Boolean(completed.rowCount);
  }
}
