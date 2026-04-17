import { describe, expect, it, beforeAll, afterAll } from "vitest";
import { newDb } from "pg-mem";
import { Pool } from "pg";

/**
 * In-memory Postgres: validates ON CONFLICT dedup and single-winner claim UPDATE semantics.
 */
describe("db concurrency (pg-mem)", () => {
  let pool: Pool;

  beforeAll(async () => {
    const db = newDb({ autoCreateForeignKeyIndices: true });

    db.public.none(`
      CREATE TABLE crawl_runs (
        id SERIAL PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'RUNNING'
      );
      CREATE TABLE crawl_urls (
        id SERIAL PRIMARY KEY,
        crawl_run_id INT NOT NULL REFERENCES crawl_runs(id),
        normalized_url TEXT NOT NULL,
        status TEXT NOT NULL,
        claimed_by_worker TEXT,
        UNIQUE (crawl_run_id, normalized_url)
      );
    `);

    const pgAdapter = db.adapters.createPg();
    pool = new pgAdapter.Pool();
  });

  afterAll(async () => {
    await pool.end();
  });

  it("deduplicates inserts for the same normalized URL in a run", async () => {
    const run = await pool.query(`INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`);
    const crawlRunId = run.rows[0].id as number;

    const ins = await pool.query(
      `
      INSERT INTO crawl_urls (crawl_run_id, normalized_url, status)
      VALUES ($1, $2, 'QUEUED')
      ON CONFLICT (crawl_run_id, normalized_url) DO NOTHING
      RETURNING id
      `,
      [crawlRunId, "https://example.com/a"]
    );
    await pool.query(
      `
      INSERT INTO crawl_urls (crawl_run_id, normalized_url, status)
      VALUES ($1, $2, 'QUEUED')
      ON CONFLICT (crawl_run_id, normalized_url) DO NOTHING
      `,
      [crawlRunId, "https://example.com/a"]
    );

    expect(ins.rows.length).toBe(1);

    const count = await pool.query(`SELECT COUNT(*)::int AS c FROM crawl_urls WHERE crawl_run_id = $1`, [crawlRunId]);
    expect(count.rows[0].c).toBe(1);
  });

  it("only one concurrent-style claim succeeds for the same row", async () => {
    const run = await pool.query(`INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`);
    const crawlRunId = run.rows[0].id as number;
    const url = await pool.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status) VALUES ($1, $2, 'QUEUED') RETURNING id`,
      [crawlRunId, "https://example.com/claim-test"]
    );
    const urlId = url.rows[0].id as number;

    const claimSql = `
      UPDATE crawl_urls
      SET status = 'IN_PROGRESS', claimed_by_worker = $2
      WHERE id = $1 AND status = 'QUEUED'
      RETURNING id
    `;

    const first = await pool.query(claimSql, [urlId, "worker-a"]);
    const second = await pool.query(claimSql, [urlId, "worker-b"]);

    expect(first.rowCount).toBe(1);
    expect(second.rowCount).toBe(0);

    const row = await pool.query(`SELECT status, claimed_by_worker FROM crawl_urls WHERE id = $1`, [urlId]);
    expect(row.rows[0].status).toBe("IN_PROGRESS");
    expect(row.rows[0].claimed_by_worker).toBe("worker-a");
  });

  it("parallel claim attempts still yield at most one winner", async () => {
    const run = await pool.query(`INSERT INTO crawl_runs(status) VALUES ('RUNNING') RETURNING id`);
    const crawlRunId = run.rows[0].id as number;
    const url = await pool.query(
      `INSERT INTO crawl_urls (crawl_run_id, normalized_url, status) VALUES ($1, $2, 'QUEUED') RETURNING id`,
      [crawlRunId, "https://example.com/race"]
    );
    const urlId = url.rows[0].id as number;

    const claimSql = `
      UPDATE crawl_urls
      SET status = 'IN_PROGRESS', claimed_by_worker = $2
      WHERE id = $1 AND status = 'QUEUED'
      RETURNING id
    `;

    const results = await Promise.all([
      pool.query(claimSql, [urlId, "w1"]),
      pool.query(claimSql, [urlId, "w2"]),
      pool.query(claimSql, [urlId, "w3"])
    ]);

    const winners = results.filter((r) => (r.rowCount ?? 0) > 0);
    expect(winners.length).toBe(1);
  });
});
