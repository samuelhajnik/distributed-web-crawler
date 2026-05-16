import { beforeAll, afterAll, describe, expect, it, vi } from "vitest";
import { newDb } from "pg-mem";
import type { Pool } from "pg";

const sharedMock: { pool: Pool | null } = { pool: null };

vi.mock("@crawler/shared", () => ({
  pgPool: {
    query: (...args: Parameters<Pool["query"]>) => sharedMock.pool!.query(...args)
  }
}));

async function bumpGraphVersion(pool: Pool, urlId: number): Promise<number> {
  const res = await pool.query(
    `UPDATE crawl_urls
     SET graph_version = nextval('crawl_url_graph_version_seq')
     WHERE id = $1
     RETURNING graph_version`,
    [urlId]
  );
  return Number(res.rows[0].graph_version);
}

describe("CrawlUrlRepository graph delta (pg-mem)", () => {
  let CrawlUrlRepository: typeof import("./crawlUrlRepository").CrawlUrlRepository;

  beforeAll(async () => {
    const db = newDb({ autoCreateForeignKeyIndices: true });
    db.public.none(`
      CREATE TABLE crawl_runs (
        id SERIAL PRIMARY KEY,
        status TEXT NOT NULL
      );
      CREATE SEQUENCE crawl_url_graph_version_seq;
      CREATE TABLE crawl_urls (
        id SERIAL PRIMARY KEY,
        crawl_run_id INT NOT NULL REFERENCES crawl_runs(id),
        normalized_url TEXT NOT NULL,
        raw_url TEXT,
        discovered_from_url_id INT,
        depth INT NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        retry_count INT NOT NULL DEFAULT 0,
        http_status INT,
        content_type TEXT,
        claimed_at TIMESTAMPTZ,
        claimed_by_worker TEXT,
        visited_at TIMESTAMPTZ,
        last_error TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        graph_version BIGINT NOT NULL DEFAULT nextval('crawl_url_graph_version_seq'),
        requested_url TEXT,
        final_url TEXT,
        redirected BOOLEAN NOT NULL DEFAULT FALSE,
        final_in_scope BOOLEAN NOT NULL DEFAULT TRUE,
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

  it("returns rows after cursor ordered by graph_version", async () => {
    const repo = new CrawlUrlRepository();
    const run = await sharedMock.pool!.query(
      `INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`
    );
    const crawlRunId = run.rows[0].id as number;

    await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status)
       VALUES ($1, 'https://example.com/a', 'QUEUED')`,
      [crawlRunId]
    );
    const b = await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status)
       VALUES ($1, 'https://example.com/b', 'VISITED')
       RETURNING id, graph_version`,
      [crawlRunId]
    );
    const bVersion = Number(b.rows[0].graph_version);
    await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status)
       VALUES ($1, 'https://example.com/c', 'QUEUED')`,
      [crawlRunId]
    );

    const delta = await repo.getGraphDeltaRows(crawlRunId, bVersion, 100);
    expect(delta.map((r) => String(r.normalized_url))).toEqual(["https://example.com/c"]);
    const versions = delta.map((r) => Number(r.graph_version));
    expect(versions.every((v) => v > bVersion)).toBe(true);
    expect([...versions].sort((a, b) => a - b)).toEqual(versions);
  });

  it("returns status updates for lower-id rows with a newer graph_version", async () => {
    const repo = new CrawlUrlRepository();
    const run = await sharedMock.pool!.query(
      `INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`
    );
    const crawlRunId = run.rows[0].id as number;

    const seed = await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status)
       VALUES ($1, 'https://example.com/seed', 'VISITED')
       RETURNING id, graph_version`,
      [crawlRunId]
    );
    const seedId = seed.rows[0].id as number;
    const seedVersion = Number(seed.rows[0].graph_version);
    const child = await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (
         crawl_run_id, normalized_url, status, discovered_from_url_id
       ) VALUES ($1, 'https://example.com/child', 'QUEUED', $2)
       RETURNING id, graph_version`,
      [crawlRunId, seedId]
    );
    const childId = child.rows[0].id as number;
    const childInsertVersion = Number(child.rows[0].graph_version);
    expect(childInsertVersion).toBeGreaterThan(seedVersion);

    await sharedMock.pool!.query(`UPDATE crawl_urls SET status = 'VISITED' WHERE id = $1`, [
      childId
    ]);
    const childUpdateVersion = await bumpGraphVersion(sharedMock.pool!, childId);
    expect(childUpdateVersion).toBeGreaterThan(childInsertVersion);

    const delta = await repo.getGraphDeltaRows(crawlRunId, childInsertVersion, 100);
    const childRow = delta.find((r) => Number(r.id) === childId);
    expect(childRow).toBeDefined();
    expect(String(childRow?.status)).toBe("VISITED");
    expect(Number(childRow?.graph_version)).toBe(childUpdateVersion);
  });

  it("includes graph_version in getUrlsPage rows", async () => {
    const repo = new CrawlUrlRepository();
    const run = await sharedMock.pool!.query(
      `INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`
    );
    const crawlRunId = run.rows[0].id as number;
    await sharedMock.pool!.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status)
       VALUES ($1, 'https://example.com/page', 'QUEUED')`,
      [crawlRunId]
    );

    const page = await repo.getUrlsPage(crawlRunId, null, 10, 0, "id", "ASC");
    expect(page.rows).toHaveLength(1);
    expect(Number(page.rows[0].graph_version)).toBeGreaterThan(0);
  });
});
