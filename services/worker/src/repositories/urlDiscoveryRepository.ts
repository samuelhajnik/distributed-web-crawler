import { pgPool } from "@crawler/shared";
import { crawlJobQueue } from "../queue";
import { crawlUrlsDiscoveredTotal, crawlUrlsRequeuedTotal } from "../prometheus";

export async function markDiscoveredUrlsEnqueued(
  crawlRunId: number,
  insertedIds: { id: number }[]
): Promise<void> {
  const jobs = insertedIds.map((row) => ({
    name: "crawl-url",
    data: { crawlRunId, urlId: Number(row.id) },
    opts: { removeOnComplete: 2000, removeOnFail: 2000 }
  }));
  if (jobs.length > 0) {
    await crawlJobQueue.addBulk(jobs);
    crawlUrlsRequeuedTotal.inc(jobs.length);
  }
}

export async function storeDiscoveredUrls(
  crawlRunId: number,
  pairs: { normalized: string; raw: string }[],
  discoveredFromUrlId: number,
  discoveredDepth: number,
  maxPages: number
): Promise<{ inserted: { id: number }[]; duplicatesSkipped: number }> {
  if (pairs.length === 0) {
    return { inserted: [], duplicatesSkipped: 0 };
  }

  const countRes = await pgPool.query(
    `SELECT COUNT(*)::int AS c FROM crawl_urls WHERE crawl_run_id = $1`,
    [crawlRunId]
  );
  const existing = Number(countRes.rows[0]?.c ?? 0);
  const remaining = Math.max(0, maxPages - existing);
  if (remaining === 0) {
    return { inserted: [], duplicatesSkipped: pairs.length };
  }
  const bounded = pairs.slice(0, remaining);

  const norms = bounded.map((p) => p.normalized);
  const raws = bounded.map((p) => p.raw);

  const insertRes = await pgPool.query(
    `
      INSERT INTO crawl_urls (crawl_run_id, normalized_url, raw_url, discovered_from_url_id, status, depth)
      SELECT $1, t.norm, t.raw, $3, 'QUEUED', $5
      FROM UNNEST($2::text[], $4::text[]) AS t(norm, raw)
      ON CONFLICT (crawl_run_id, normalized_url) DO NOTHING
      RETURNING id
    `,
    [crawlRunId, norms, discoveredFromUrlId, raws, discoveredDepth]
  );

  const insertedCount = insertRes.rowCount ?? 0;
  const duplicatesSkipped = pairs.length - insertedCount;
  if (insertedCount > 0) {
    crawlUrlsDiscoveredTotal.inc(insertedCount);
  }
  if (duplicatesSkipped > 0) {
    await pgPool.query(
      `
        UPDATE crawl_runs
        SET duplicates_skipped = duplicates_skipped + $2
        WHERE id = $1
      `,
      [crawlRunId, duplicatesSkipped]
    );
  }

  return { inserted: insertRes.rows, duplicatesSkipped };
}
