import { beforeAll, afterAll, describe, expect, it, vi } from "vitest";
import { newDb } from "pg-mem";
import type { Pool } from "pg";

const sharedMock: { pool: Pool | null } = { pool: null };

vi.mock("@crawler/shared", () => ({
  pgPool: {
    query: (...args: Parameters<Pool["query"]>) => sharedMock.pool!.query(...args),
    connect: async () => {
      const client = await sharedMock.pool!.connect();
      return {
        query: (...args: Parameters<Pool["query"]>) => client.query(...args),
        release: () => client.release()
      };
    }
  }
}));

describe("CrawlRunRepository.cancelRun (pg-mem)", () => {
  let CrawlRunRepository: typeof import("./crawlRunRepository").CrawlRunRepository;

  beforeAll(async () => {
    const db = newDb({ autoCreateForeignKeyIndices: true });
    db.public.none(`
      CREATE TABLE crawl_runs (
        id SERIAL PRIMARY KEY,
        status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
        completed_at TIMESTAMPTZ
      );
      CREATE TABLE crawl_urls (
        id SERIAL PRIMARY KEY,
        crawl_run_id INT NOT NULL REFERENCES crawl_runs(id),
        normalized_url TEXT NOT NULL,
        status TEXT NOT NULL,
        claimed_by_worker TEXT,
        claimed_at TIMESTAMPTZ,
        last_error TEXT,
        UNIQUE (crawl_run_id, normalized_url)
      );
    `);
    const pgAdapter = db.adapters.createPg();
    sharedMock.pool = new pgAdapter.Pool();
    const mod = await import("./crawlRunRepository");
    CrawlRunRepository = mod.CrawlRunRepository;
  });

  afterAll(async () => {
    await sharedMock.pool?.end();
  });

  it("cancels RUNNING run and queued/in-progress URLs in one transaction", async () => {
    const repo = new CrawlRunRepository();
    const run = await sharedMock.pool!.query(
      `INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`
    );
    const crawlRunId = run.rows[0].id as number;
    await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status) VALUES
       ($1, 'https://example.com/q', 'QUEUED'),
       ($1, 'https://example.com/p', 'IN_PROGRESS'),
       ($1, 'https://example.com/v', 'VISITED')`,
      [crawlRunId]
    );

    const result = await repo.cancelRun(crawlRunId);
    expect(result).toMatchObject({
      status: "CANCELLED",
      changed: true,
      cancelled_url_count: 2
    });

    const runRow = await sharedMock.pool!.query(`SELECT status FROM crawl_runs WHERE id = $1`, [
      crawlRunId
    ]);
    expect(runRow.rows[0].status).toBe("CANCELLED");

    const cancelled = await sharedMock.pool!.query(
      `SELECT status FROM crawl_urls WHERE crawl_run_id = $1 ORDER BY normalized_url`,
      [crawlRunId]
    );
    expect(cancelled.rows.map((r) => r.status)).toEqual(["CANCELLED", "CANCELLED", "VISITED"]);
  });

  it("is idempotent for already CANCELLED runs", async () => {
    const repo = new CrawlRunRepository();
    const run = await sharedMock.pool!.query(
      `INSERT INTO crawl_runs(status) VALUES ('CANCELLED') RETURNING id`
    );
    const crawlRunId = run.rows[0].id as number;
    const result = await repo.cancelRun(crawlRunId);
    expect(result).toMatchObject({ status: "CANCELLED", changed: false, cancelled_url_count: 0 });
  });

  it("does not mutate COMPLETED runs", async () => {
    const repo = new CrawlRunRepository();
    const run = await sharedMock.pool!.query(
      `INSERT INTO crawl_runs(status) VALUES ('COMPLETED') RETURNING id`
    );
    const crawlRunId = run.rows[0].id as number;
    const result = await repo.cancelRun(crawlRunId);
    expect(result).toMatchObject({ status: "COMPLETED", changed: false, cancelled_url_count: 0 });
  });
});
