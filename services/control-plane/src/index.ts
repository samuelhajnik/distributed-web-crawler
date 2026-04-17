import express from "express";
import {
  buildCrawlBulkJobs,
  CLAIM_STALE_SECONDS,
  createCrawlQueue,
  RECONCILE_BATCH_SIZE,
  RECONCILE_INTERVAL_SECONDS,
  parseSeedUrl,
  pgPool,
  redisConnection
} from "@crawler/shared";
import {
  crawlCompletionChecksTotal,
  crawlQueueReconciliationCyclesTotal,
  crawlQueueReconciliationEnqueuedTotal,
  crawlReconciliationCycleDurationSeconds,
  crawlRunsCompletedTotal,
  crawlRunsStartedTotal,
  crawlStaleClaimsRecoveredTotal,
  crawlUrlsDiscoveredTotal,
  crawlUrlsFailedGauge,
  crawlUrlsInProgressGauge,
  crawlUrlsQueuedGauge,
  crawlUrlsRequeuedTotal,
  metricsHandler
} from "./prometheus";

type RunCounts = {
  queued_count: number;
  in_progress_count: number;
  visited_count: number;
  failed_count: number;
};
type CompletionStability = {
  empty_cycles: number;
};

const port = Number(process.env.CONTROL_PLANE_PORT ?? 3000);
const app = express();
app.use(express.json());

const queue = createCrawlQueue();
const completionState = new Map<number, CompletionStability>();

function logCp(crawlRunId: number | undefined, msg: string): void {
  const run = crawlRunId !== undefined ? ` crawl_run=${crawlRunId}` : "";
  process.stdout.write(`[component=control-plane]${run} ${msg}\n`);
}

function getIntervalMilliseconds(seconds: number): number {
  return Math.max(1, seconds) * 1000;
}

async function getRunCounts(crawlRunId: number): Promise<RunCounts> {
  const countsRes = await pgPool.query(
    `
      SELECT
        COUNT(*) FILTER (WHERE status = 'QUEUED')::int AS queued_count,
        COUNT(*) FILTER (WHERE status = 'IN_PROGRESS')::int AS in_progress_count,
        COUNT(*) FILTER (WHERE status = 'VISITED')::int AS visited_count,
        COUNT(*) FILTER (WHERE status = 'FAILED')::int AS failed_count
      FROM crawl_urls
      WHERE crawl_run_id = $1
    `,
    [crawlRunId]
  );

  return countsRes.rows[0] as RunCounts;
}

async function updateRunningRunGauges(): Promise<void> {
  const res = await pgPool.query(`
    SELECT
      COUNT(*) FILTER (WHERE u.status = 'QUEUED')::int AS queued,
      COUNT(*) FILTER (WHERE u.status = 'IN_PROGRESS')::int AS in_progress,
      COUNT(*) FILTER (WHERE u.status = 'FAILED')::int AS failed
    FROM crawl_urls u
    INNER JOIN crawl_runs r ON r.id = u.crawl_run_id
    WHERE r.status = 'RUNNING'
  `);
  const row = res.rows[0] as { queued: number; in_progress: number; failed: number };
  crawlUrlsQueuedGauge.set(row.queued ?? 0);
  crawlUrlsInProgressGauge.set(row.in_progress ?? 0);
  crawlUrlsFailedGauge.set(row.failed ?? 0);
}

async function finalizeRunIfStableAndComplete(crawlRunId: number, counts: RunCounts): Promise<void> {
  crawlCompletionChecksTotal.inc();
  const isEmptyFrontier = counts.queued_count === 0 && counts.in_progress_count === 0;
  const prior = completionState.get(crawlRunId)?.empty_cycles ?? 0;
  const next = isEmptyFrontier ? prior + 1 : 0;

  completionState.set(crawlRunId, { empty_cycles: next });

  if (next >= 2) {
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
      [crawlRunId, counts.visited_count, counts.failed_count]
    );
    if (completed.rowCount) {
      crawlRunsCompletedTotal.inc();
      logCp(crawlRunId, `completion-detected status=COMPLETED visited=${counts.visited_count} failed=${counts.failed_count}`);
    }
    completionState.delete(crawlRunId);
  } else if (isEmptyFrontier) {
    logCp(crawlRunId, `completion-check empty_frontier streak=${next} (need 2)`);
  }
}

async function requeueStaleClaims(crawlRunId: number): Promise<number> {
  const stale = await pgPool.query(
    `
      UPDATE crawl_urls
      SET status = 'QUEUED',
          claimed_at = NULL,
          claimed_by_worker = NULL
      WHERE crawl_run_id = $1
        AND status = 'IN_PROGRESS'
        AND claimed_at IS NOT NULL
        AND claimed_at < NOW() - ($2::text || ' seconds')::interval
      RETURNING id
    `,
    [crawlRunId, CLAIM_STALE_SECONDS]
  );

  const n = stale.rowCount ?? 0;
  if (n > 0) {
    crawlStaleClaimsRecoveredTotal.inc(n);
    logCp(crawlRunId, `stale-recovery count=${n}`);
  }

  const staleJobs = buildCrawlBulkJobs(
    crawlRunId,
    stale.rows.map((row) => Number(row.id))
  );
  if (staleJobs.length > 0) {
    await queue.addBulk(staleJobs);
    crawlUrlsRequeuedTotal.inc(staleJobs.length);
  }

  return n;
}

async function reconcileQueuedRows(crawlRunId: number): Promise<number> {
  const queuedRes = await pgPool.query(
    `
      SELECT id
      FROM crawl_urls
      WHERE crawl_run_id = $1
        AND status = 'QUEUED'
      ORDER BY id
      LIMIT $2
    `,
    [crawlRunId, RECONCILE_BATCH_SIZE]
  );

  const queuedJobs = buildCrawlBulkJobs(
    crawlRunId,
    queuedRes.rows.map((row) => Number(row.id))
  );
  if (queuedJobs.length > 0) {
    await queue.addBulk(queuedJobs);
    crawlQueueReconciliationEnqueuedTotal.inc(queuedJobs.length);
    crawlUrlsRequeuedTotal.inc(queuedJobs.length);
    logCp(crawlRunId, `reconciliation enqueued=${queuedJobs.length}`);
  }

  return queuedRes.rowCount ?? 0;
}

async function runMaintenanceForRun(crawlRunId: number): Promise<RunCounts> {
  crawlQueueReconciliationCyclesTotal.inc();
  const recovered = await requeueStaleClaims(crawlRunId);
  const reconciled = await reconcileQueuedRows(crawlRunId);
  const counts = await getRunCounts(crawlRunId);
  await finalizeRunIfStableAndComplete(crawlRunId, counts);
  await updateRunningRunGauges();
  logCp(
    crawlRunId,
    `maintenance recovered_stale=${recovered} reconciled_queued=${reconciled} queued=${counts.queued_count} in_progress=${counts.in_progress_count}`
  );
  return counts;
}

app.get("/metrics", async (_req, res) => {
  try {
    const { body, contentType } = await metricsHandler();
    res.setHeader("Content-Type", contentType);
    res.send(body);
  } catch (err) {
    res.status(500).send((err as Error).message);
  }
});

app.post("/crawl-runs", async (req, res) => {
  try {
    const body = req.body as { seedUrl?: unknown };
    if (body?.seedUrl === undefined || body.seedUrl === null) {
      res.status(400).json({ error: "seedUrl is required" });
      return;
    }
    if (typeof body.seedUrl !== "string" || !body.seedUrl.trim()) {
      res.status(400).json({ error: "seedUrl must be a non-empty string" });
      return;
    }
    const seedInput = body.seedUrl.trim();
    const parsed = parseSeedUrl(seedInput);
    if (!parsed) {
      res.status(400).json({ error: "Invalid seedUrl: expected absolute http(s) URL with a host" });
      return;
    }

    const allowedHostsArray = Array.from(parsed.allowedHosts);

    const client = await pgPool.connect();
    try {
      await client.query("BEGIN");
      const runResult = await client.query(
        `
        INSERT INTO crawl_runs(root_url, seed_url, normalized_seed_url, allowed_hosts, status)
        VALUES ($1, $2, $3, $4, 'RUNNING')
        RETURNING id, root_url, seed_url, normalized_seed_url, allowed_hosts, status, started_at
        `,
        [parsed.normalized, seedInput, parsed.normalized, allowedHostsArray]
      );
      const crawlRunId: number = runResult.rows[0].id;
      const urlResult = await client.query(
        `INSERT INTO crawl_urls(crawl_run_id, normalized_url, raw_url, status)
         VALUES ($1, $2, $3, 'QUEUED') RETURNING id`,
        [crawlRunId, parsed.normalized, seedInput]
      );
      const urlId: number = urlResult.rows[0].id;
      await client.query("COMMIT");

      crawlRunsStartedTotal.inc();
      crawlUrlsDiscoveredTotal.inc();
      logCp(crawlRunId, `crawl-started url_id=${urlId} root=${parsed.normalized}`);

      try {
        await queue.add("crawl-url", { crawlRunId, urlId }, { removeOnComplete: 2000, removeOnFail: 2000 });
        crawlUrlsRequeuedTotal.inc();
      } catch (queueErr) {
        logCp(
          crawlRunId,
          `initial-enqueue-failed url_id=${urlId} err=${(queueErr as Error).message}`
        );
      }
      res.status(201).json(runResult.rows[0]);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.get("/crawl-runs/:id/summary", async (req, res) => {
  try {
    const crawlRunId = Number(req.params.id);
    if (Number.isNaN(crawlRunId)) {
      res.status(400).json({ error: "Invalid run id" });
      return;
    }

    const runRes = await pgPool.query("SELECT * FROM crawl_runs WHERE id = $1", [crawlRunId]);
    if (runRes.rowCount === 0) {
      res.status(404).json({ error: "Run not found" });
      return;
    }

    const agg = await pgPool.query(
      `
      SELECT
        COUNT(*)::int AS total_discovered,
        COUNT(*) FILTER (WHERE status = 'VISITED')::int AS total_visited,
        COUNT(*) FILTER (WHERE status = 'FAILED')::int AS total_failed,
        COUNT(*) FILTER (WHERE status = 'QUEUED')::int AS total_queued,
        COUNT(*) FILTER (WHERE status = 'IN_PROGRESS')::int AS total_in_progress,
        COALESCE(SUM(retry_count), 0)::int AS total_retries
      FROM crawl_urls
      WHERE crawl_run_id = $1
      `,
      [crawlRunId]
    );

    const run = runRes.rows[0];
    const a = agg.rows[0];
    res.json({
      crawl_run_id: crawlRunId,
      status: run.status,
      root_url: run.root_url,
      seed_url: run.seed_url,
      normalized_seed_url: run.normalized_seed_url,
      allowed_hosts: run.allowed_hosts,
      started_at: run.started_at,
      finished_at: run.completed_at,
      duplicates_skipped: run.duplicates_skipped,
      totals: {
        discovered: a.total_discovered,
        visited: a.total_visited,
        failed: a.total_failed,
        queued: a.total_queued,
        in_progress: a.total_in_progress,
        retries: a.total_retries
      }
    });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.get("/crawl-runs/:id/export", async (req, res) => {
  try {
    const crawlRunId = Number(req.params.id);
    const format = String(req.query.format ?? "json").toLowerCase();
    const limit = Math.min(500_000, Math.max(1, Number(req.query.limit ?? 50_000)));
    if (Number.isNaN(crawlRunId)) {
      res.status(400).json({ error: "Invalid run id" });
      return;
    }

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
        discovered_from_url_id
      FROM crawl_urls
      WHERE crawl_run_id = $1
      ORDER BY id
      LIMIT $2
      `,
      [crawlRunId, limit]
    );

    if (format === "csv") {
      const header =
        "id,normalized_url,status,http_status,content_type,retry_count,claimed_by_worker,claimed_at,visited_at,raw_url,discovered_from_url_id\n";
      const lines = rows.rows.map((r) =>
        [
          r.id,
          csvEscape(r.normalized_url),
          r.status,
          r.http_status ?? "",
          csvEscape(r.content_type ?? ""),
          r.retry_count,
          csvEscape(r.claimed_by_worker ?? ""),
          r.claimed_at ?? "",
          r.visited_at ?? "",
          csvEscape(r.raw_url ?? ""),
          r.discovered_from_url_id ?? ""
        ].join(",")
      );
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader("Content-Disposition", `attachment; filename="crawl-${crawlRunId}.csv"`);
      res.send(header + lines.join("\n"));
      return;
    }

    res.json({
      crawl_run_id: crawlRunId,
      limit,
      count: rows.rowCount ?? 0,
      urls: rows.rows
    });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

function csvEscape(value: string): string {
  if (/[",\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

app.get("/crawl-runs/:id/graph", async (req, res) => {
  try {
    const crawlRunId = Number(req.params.id);
    const limit = Math.min(200_000, Math.max(1, Number(req.query.limit ?? 100_000)));
    if (Number.isNaN(crawlRunId)) {
      res.status(400).json({ error: "Invalid run id" });
      return;
    }

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

    const nodeCount = await pgPool.query(
      `SELECT COUNT(*)::int AS c FROM crawl_urls WHERE crawl_run_id = $1`,
      [crawlRunId]
    );

    res.json({
      crawl_run_id: crawlRunId,
      edge_count: edges.rowCount ?? 0,
      node_count: nodeCount.rows[0].c,
      edges: edges.rows
    });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.get("/crawl-runs/:id", async (req, res) => {
  try {
    const crawlRunId = Number(req.params.id);
    if (Number.isNaN(crawlRunId)) {
      res.status(400).json({ error: "Invalid run id" });
      return;
    }

    const runRes = await pgPool.query("SELECT * FROM crawl_runs WHERE id = $1", [crawlRunId]);
    if (runRes.rowCount === 0) {
      res.status(404).json({ error: "Run not found" });
      return;
    }

    const counts = await runMaintenanceForRun(crawlRunId);

    const refreshedRunRes = await pgPool.query("SELECT * FROM crawl_runs WHERE id = $1", [crawlRunId]);
    const run = refreshedRunRes.rows[0];
    res.json({
      crawl_run_id: crawlRunId,
      id: run.id,
      status: run.status,
      started_at: run.started_at,
      completed_at: run.completed_at,
      root_url: run.root_url,
      seed_url: run.seed_url,
      normalized_seed_url: run.normalized_seed_url,
      allowed_hosts: run.allowed_hosts,
      visited_count: counts.visited_count,
      failed_count: counts.failed_count,
      duplicates_skipped: run.duplicates_skipped,
      queue_empty: counts.queued_count === 0,
      in_progress: counts.in_progress_count,
      summary_hint: `/crawl-runs/${crawlRunId}/summary`
    });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

const SORT_COLUMNS: Record<string, string> = {
  id: "id",
  visited_at: "visited_at",
  updated_at: "updated_at",
  normalized_url: "normalized_url"
};

app.get("/crawl-runs/:id/urls", async (req, res) => {
  try {
    const crawlRunId = Number(req.params.id);
    const status = typeof req.query.status === "string" ? req.query.status.toUpperCase() : undefined;
    const limit = Math.min(200, Math.max(1, Number(req.query.limit ?? 50)));
    const offset = Math.max(0, Number(req.query.offset ?? 0));
    const sortKey = typeof req.query.sort === "string" ? req.query.sort.toLowerCase() : "id";
    const orderRaw = typeof req.query.order === "string" ? req.query.order.toLowerCase() : "asc";
    const order = orderRaw === "desc" ? "DESC" : "ASC";
    const allowedStatuses = new Set(["QUEUED", "IN_PROGRESS", "VISITED", "FAILED"]);
    if (Number.isNaN(crawlRunId)) {
      res.status(400).json({ error: "Invalid run id" });
      return;
    }
    if (status && !allowedStatuses.has(status)) {
      res.status(400).json({ error: "Invalid status filter" });
      return;
    }
    const sortCol = SORT_COLUMNS[sortKey] ?? "id";

    const totalRes = await pgPool.query(
      `
      SELECT COUNT(*)::int AS total
      FROM crawl_urls
      WHERE crawl_run_id = $1
        AND ($2::text IS NULL OR status = $2)
      `,
      [crawlRunId, status ?? null]
    );
    const total: number = totalRes.rows[0].total;

    const urls = await pgPool.query(
      `
      SELECT
        id,
        normalized_url,
        raw_url,
        discovered_from_url_id,
        status,
        retry_count,
        http_status,
        content_type,
        claimed_at,
        claimed_by_worker,
        visited_at,
        last_error,
        updated_at
      FROM crawl_urls
      WHERE crawl_run_id = $1
        AND ($2::text IS NULL OR status = $2)
      ORDER BY ${sortCol} ${order} NULLS LAST, id ASC
      LIMIT $3
      OFFSET $4
      `,
      [crawlRunId, status ?? null, limit, offset]
    );

    res.json({
      crawl_run_id: crawlRunId,
      pagination: {
        limit,
        offset,
        returned: urls.rowCount ?? 0,
        total,
        has_more: offset + (urls.rowCount ?? 0) < total
      },
      filters: { status: status ?? null, sort: sortKey, order: orderRaw },
      urls: urls.rows
    });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.get("/health", async (_req, res) => {
  try {
    await pgPool.query("SELECT 1");
    await redisConnection.ping();
    res.json({ status: "ok" });
  } catch (err) {
    res.status(500).json({ status: "error", error: (err as Error).message });
  }
});

const staleSweep = setInterval(async () => {
  const timer = crawlReconciliationCycleDurationSeconds.startTimer();
  try {
    const runRes = await pgPool.query("SELECT id FROM crawl_runs WHERE status = 'RUNNING'");
    const activeRunIds = new Set<number>();
    for (const run of runRes.rows) {
      const crawlRunId = Number(run.id);
      activeRunIds.add(crawlRunId);
      await runMaintenanceForRun(crawlRunId);
    }
    for (const trackedId of completionState.keys()) {
      if (!activeRunIds.has(trackedId)) {
        completionState.delete(trackedId);
      }
    }
    logCp(undefined, `reconciliation-cycle done runs=${runRes.rowCount ?? 0}`);
  } catch (_err) {
    // keep loop alive; this is best-effort recovery
  } finally {
    timer();
  }
}, getIntervalMilliseconds(RECONCILE_INTERVAL_SECONDS));

app.listen(port, () => {
  process.stdout.write(`[component=control-plane] listening on :${port} metrics=/metrics\n`);
});

process.on("SIGINT", async () => {
  clearInterval(staleSweep);
  await queue.close();
  await redisConnection.quit();
  await pgPool.end();
  process.exit(0);
});
