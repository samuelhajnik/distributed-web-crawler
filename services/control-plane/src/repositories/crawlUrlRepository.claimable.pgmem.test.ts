import { beforeAll, afterAll, describe, expect, it, vi } from "vitest";
import { newDb } from "pg-mem";
import type { Pool } from "pg";

const sharedMock: { pool: Pool | null } = { pool: null };

vi.mock("@crawler/shared", () => ({
  pgPool: {
    query: (...args: Parameters<Pool["query"]>) => sharedMock.pool!.query(...args)
  }
}));

describe("CrawlUrlRepository claimable frontier (pg-mem)", () => {
  let CrawlUrlRepository: typeof import("./crawlUrlRepository").CrawlUrlRepository;

  beforeAll(async () => {
    const db = newDb({ autoCreateForeignKeyIndices: true });
    db.public.none(`
      CREATE TABLE crawl_runs (
        id SERIAL PRIMARY KEY,
        status TEXT NOT NULL
      );
      CREATE TABLE crawl_urls (
        id SERIAL PRIMARY KEY,
        crawl_run_id INT NOT NULL REFERENCES crawl_runs(id),
        normalized_url TEXT NOT NULL,
        status TEXT NOT NULL,
        retry_after_at TIMESTAMPTZ,
        UNIQUE (crawl_run_id, normalized_url)
      );
    `);
    const pgAdapter = db.adapters.createPg();
    sharedMock.pool = new pgAdapter.Pool();
    const mod = await import("./crawlUrlRepository");
    CrawlUrlRepository = mod.CrawlUrlRepository;
  });

  afterAll(async () => {
    await sharedMock.pool?.end();
  });

  it("hasClaimableQueuedUrls is false when only future-retry QUEUED rows exist", async () => {
    const repo = new CrawlUrlRepository();
    const run = await sharedMock.pool!.query(
      `INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`
    );
    const crawlRunId = run.rows[0].id as number;
    await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status, retry_after_at)
       VALUES ($1, 'https://example.com/a', 'QUEUED', '2027-01-01T00:00:00Z')`,
      [crawlRunId]
    );
    expect(await repo.hasClaimableQueuedUrls(crawlRunId)).toBe(false);
  });

  it("getRunCounts still counts future-retry QUEUED rows for completion", async () => {
    const { CrawlRunRepository } = await import("./crawlRunRepository");
    const runRepo = new CrawlRunRepository();
    const run = await sharedMock.pool!.query(
      `INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`
    );
    const crawlRunId = run.rows[0].id as number;
    await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status, retry_after_at)
       VALUES ($1, 'https://example.com/a', 'QUEUED', '2027-01-01T00:00:00Z')`,
      [crawlRunId]
    );
    const counts = await runRepo.getRunCounts(crawlRunId);
    expect(Number(counts.queued_count)).toBe(1);
  });
});
