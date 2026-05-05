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
