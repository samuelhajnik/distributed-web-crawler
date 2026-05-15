import {
  CLAIM_STALE_SECONDS,
  createCrawlQueue,
  parseSeedUrl,
  pgPool,
  topUpRunSignals
} from "@crawler/shared";
import {
  crawlCompletionChecksTotal,
  crawlQueueReconciliationCyclesTotal,
  crawlQueueReconciliationEnqueuedTotal,
  crawlRunsCompletedTotal,
  crawlRunsStartedTotal,
  crawlStaleClaimsRecoveredTotal,
  crawlUrlsDiscoveredTotal,
  crawlUrlsFailedGauge,
  crawlUrlsInProgressGauge,
  crawlUrlsQueuedGauge,
  crawlUrlsRequeuedTotal
} from "../prometheus";
import { csvEscape } from "../export/csvExport";
import { logCp } from "../logging";
import { buildRunConfig, publicRunConfig, stripIgnoredLegacySettings } from "../runConfig";
import { CrawlRunRepository } from "../repositories/crawlRunRepository";
import { CrawlUrlRepository } from "../repositories/crawlUrlRepository";
import type { CompletionStability, RunCounts } from "../types";

const SORT_COLUMNS: Record<string, string> = {
  id: "id",
  visited_at: "visited_at",
  updated_at: "updated_at",
  normalized_url: "normalized_url"
};

export class CrawlRunService {
  private readonly queue = createCrawlQueue();
  private readonly completionState = new Map<number, CompletionStability>();
  private readonly crawlRunRepository = new CrawlRunRepository();
  private readonly crawlUrlRepository = new CrawlUrlRepository();

  async createRun(input: unknown): Promise<{ status: number; body: unknown }> {
    const body = input as { seedUrl?: unknown; settings?: Record<string, unknown> } & Record<
      string,
      unknown
    >;
    if (body?.seedUrl === undefined || body.seedUrl === null) {
      return { status: 400, body: { error: "seedUrl is required" } };
    }
    if (typeof body.seedUrl !== "string" || !body.seedUrl.trim()) {
      return { status: 400, body: { error: "seedUrl must be a non-empty string" } };
    }
    const seedInput = body.seedUrl.trim();
    const parsed = parseSeedUrl(seedInput);
    if (!parsed) {
      return {
        status: 400,
        body: { error: "Invalid seedUrl: expected absolute http(s) URL with a host" }
      };
    }

    const allowedHostsArray = Array.from(parsed.allowedHosts);
    const rawOverrides =
      typeof body.settings === "object" && body.settings !== null
        ? (body.settings as Record<string, unknown>)
        : (body as Record<string, unknown>);
    const runConfig = buildRunConfig(stripIgnoredLegacySettings(rawOverrides));

    const client = await pgPool.connect();
    let created: Record<string, unknown>;
    let crawlRunId: number;
    let urlId: number;
    try {
      await client.query("BEGIN");
      const runResult = await client.query(
        `
        INSERT INTO crawl_runs(root_url, seed_url, normalized_seed_url, allowed_hosts, run_config, status)
        VALUES ($1, $2, $3, $4, $5::jsonb, 'RUNNING')
        RETURNING id, root_url, seed_url, normalized_seed_url, allowed_hosts, run_config, status, started_at
        `,
        [
          parsed.normalized,
          seedInput,
          parsed.normalized,
          allowedHostsArray,
          JSON.stringify(runConfig)
        ]
      );
      created = runResult.rows[0] as Record<string, unknown>;
      crawlRunId = Number(created.id);
      const urlResult = await client.query(
        `INSERT INTO crawl_urls(crawl_run_id, normalized_url, raw_url, status, depth)
         VALUES ($1, $2, $3, 'QUEUED', 0) RETURNING id`,
        [crawlRunId, parsed.normalized, seedInput]
      );
      urlId = Number(urlResult.rows[0].id);
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    crawlRunsStartedTotal.inc();
    crawlUrlsDiscoveredTotal.inc();
    logCp(crawlRunId, `crawl-started url_id=${urlId} root=${parsed.normalized}`);

    try {
      const signals = await topUpRunSignals(this.queue, crawlRunId);
      if (signals > 0) {
        crawlUrlsRequeuedTotal.inc();
      }
      logCp(crawlRunId, `run-signals-enqueued count=${signals} seed_url_id=${urlId}`);
    } catch (queueErr) {
      logCp(crawlRunId, `initial-run-signal-enqueue-failed err=${(queueErr as Error).message}`);
    }

    return { status: 201, body: { ...created, run_config: publicRunConfig(created.run_config) } };
  }

  async listCrawlRuns(limit: number): Promise<{ status: number; body: unknown }> {
    const rows = await this.crawlRunRepository.listRecentWithTotals(limit);
    return {
      status: 200,
      body: {
        runs: rows.map((row) => {
          const id = Number(row.id);
          return {
            crawl_run_id: id,
            id,
            status: row.status,
            seed_url: row.seed_url,
            root_url: row.root_url,
            normalized_seed_url: row.normalized_seed_url,
            started_at: row.started_at,
            finished_at: row.completed_at,
            completed_at: row.completed_at,
            run_config: publicRunConfig(row.run_config),
            totals: {
              discovered: Number(row.discovered ?? 0),
              visited: Number(row.visited ?? 0),
              queued: Number(row.queued ?? 0),
              in_progress: Number(row.in_progress ?? 0),
              failed: Number(row.failed ?? 0),
              cancelled: Number(row.cancelled ?? 0),
              redirect_followed: Number(row.redirect_followed ?? 0),
              redirect_out_of_scope: Number(row.redirect_out_of_scope ?? 0),
              redirect_301: Number(row.redirect_301 ?? 0),
              forbidden: Number(row.forbidden ?? 0),
              not_found: Number(row.not_found ?? 0),
              http_terminal: Number(row.http_terminal ?? 0)
            }
          };
        })
      }
    };
  }

  async getRunSummary(crawlRunId: number): Promise<{ status: number; body: unknown }> {
    const run = await this.crawlRunRepository.getById(crawlRunId);
    if (!run) {
      return { status: 404, body: { error: "Run not found" } };
    }
    const a = await this.crawlUrlRepository.getRunSummaryTotals(crawlRunId);
    return {
      status: 200,
      body: {
        crawl_run_id: crawlRunId,
        status: run.status,
        root_url: run.root_url,
        seed_url: run.seed_url,
        normalized_seed_url: run.normalized_seed_url,
        allowed_hosts: run.allowed_hosts,
        run_config: publicRunConfig(run.run_config),
        started_at: run.started_at,
        finished_at: run.completed_at,
        duplicates_skipped: run.duplicates_skipped,
        totals: {
          discovered: a.total_discovered,
          visited: a.total_visited,
          redirect_followed: a.total_redirect_followed,
          redirect_out_of_scope: a.total_redirect_out_of_scope,
          redirect_301: a.total_redirect_301,
          forbidden: a.total_forbidden,
          not_found: a.total_not_found,
          http_terminal: a.total_http_terminal,
          failed: a.total_failed,
          cancelled: a.total_cancelled,
          queued: a.total_queued,
          in_progress: a.total_in_progress
        }
      }
    };
  }

  async cancelCrawlRun(crawlRunId: number): Promise<{ status: number; body: unknown }> {
    const result = await this.crawlRunRepository.cancelRun(crawlRunId);
    if ("notFound" in result) {
      return { status: 404, body: { error: "Run not found" } };
    }
    this.completionState.delete(crawlRunId);
    return {
      status: 200,
      body: {
        crawl_run_id: crawlRunId,
        status: result.status,
        changed: result.changed,
        cancelled_url_count: result.cancelled_url_count
      }
    };
  }

  async exportRun(
    crawlRunId: number,
    format: string,
    limit: number
  ): Promise<{ status: number; body: unknown; headers?: Record<string, string> }> {
    const rows = await this.crawlUrlRepository.getExportRows(crawlRunId, limit);
    if (format === "csv") {
      const header =
        "id,normalized_url,status,http_status,content_type,retry_count,claimed_by_worker,claimed_at,visited_at,raw_url,discovered_from_url_id,depth,requested_url,final_url,redirected,final_in_scope\n";
      const lines = rows.map((r) =>
        [
          r.id,
          csvEscape(String(r.normalized_url ?? "")),
          r.status,
          r.http_status ?? "",
          csvEscape(String(r.content_type ?? "")),
          r.retry_count,
          csvEscape(String(r.claimed_by_worker ?? "")),
          r.claimed_at ?? "",
          r.visited_at ?? "",
          csvEscape(String(r.raw_url ?? "")),
          r.discovered_from_url_id ?? "",
          r.depth ?? 0,
          csvEscape(String(r.requested_url ?? "")),
          csvEscape(String(r.final_url ?? "")),
          r.redirected ?? false,
          r.final_in_scope ?? true
        ].join(",")
      );
      return {
        status: 200,
        headers: {
          "Content-Type": "text/csv; charset=utf-8",
          "Content-Disposition": `attachment; filename="crawl-${crawlRunId}.csv"`
        },
        body: header + lines.join("\n")
      };
    }

    return {
      status: 200,
      body: {
        crawl_run_id: crawlRunId,
        limit,
        count: rows.length,
        urls: rows
      }
    };
  }

  async getRunGraph(crawlRunId: number, limit: number): Promise<{ status: number; body: unknown }> {
    const edges = await this.crawlUrlRepository.getGraphEdges(crawlRunId, limit);
    const nodeCount = await this.crawlUrlRepository.getNodeCount(crawlRunId);
    return {
      status: 200,
      body: {
        crawl_run_id: crawlRunId,
        edge_count: edges.length,
        node_count: nodeCount,
        edges
      }
    };
  }

  async getRun(crawlRunId: number): Promise<{ status: number; body: unknown }> {
    const run = await this.crawlRunRepository.getById(crawlRunId);
    if (!run) {
      return { status: 404, body: { error: "Run not found" } };
    }

    const counts =
      String(run.status) === "RUNNING"
        ? await this.runMaintenanceForRun(crawlRunId)
        : await this.crawlRunRepository.getRunCounts(crawlRunId);
    const refreshedRun = await this.crawlRunRepository.getById(crawlRunId);
    if (!refreshedRun) {
      return { status: 404, body: { error: "Run not found" } };
    }

    return {
      status: 200,
      body: {
        crawl_run_id: crawlRunId,
        id: refreshedRun.id,
        status: refreshedRun.status,
        started_at: refreshedRun.started_at,
        completed_at: refreshedRun.completed_at,
        root_url: refreshedRun.root_url,
        seed_url: refreshedRun.seed_url,
        normalized_seed_url: refreshedRun.normalized_seed_url,
        allowed_hosts: refreshedRun.allowed_hosts,
        run_config: publicRunConfig(refreshedRun.run_config),
        visited_count: counts.visited_count,
        redirect_301_count: counts.redirect_301_count,
        redirect_followed_count: counts.redirect_followed_count,
        redirect_out_of_scope_count: counts.redirect_out_of_scope_count,
        forbidden_count: counts.forbidden_count,
        not_found_count: counts.not_found_count,
        http_terminal_count: counts.http_terminal_count,
        failed_count: counts.failed_count,
        duplicates_skipped: refreshedRun.duplicates_skipped,
        queue_empty: counts.queued_count === 0,
        in_progress: counts.in_progress_count,
        summary_hint: `/crawl-runs/${crawlRunId}/summary`
      }
    };
  }

  async listRunUrls(
    crawlRunId: number,
    status: string | null,
    limit: number,
    offset: number,
    sortKey: string,
    orderRaw: string
  ): Promise<{ status: number; body: unknown }> {
    const order = orderRaw === "desc" ? "DESC" : "ASC";
    const sortCol = SORT_COLUMNS[sortKey] ?? "id";
    const page = await this.crawlUrlRepository.getUrlsPage(
      crawlRunId,
      status,
      limit,
      offset,
      sortCol,
      order
    );
    return {
      status: 200,
      body: {
        crawl_run_id: crawlRunId,
        pagination: {
          limit,
          offset,
          returned: page.rows.length,
          total: page.total,
          has_more: offset + page.rows.length < page.total
        },
        filters: { status: status ?? null, sort: sortKey, order: orderRaw },
        urls: page.rows
      }
    };
  }

  async runMaintenanceForRun(crawlRunId: number): Promise<RunCounts> {
    const run = await this.crawlRunRepository.getById(crawlRunId);
    if (!run || String(run.status) !== "RUNNING") {
      return this.crawlRunRepository.getRunCounts(crawlRunId);
    }
    crawlQueueReconciliationCyclesTotal.inc();
    const recovered = await this.requeueStaleClaims(crawlRunId);
    const reconciled = await this.reconcileQueuedRows(crawlRunId);
    const counts = await this.crawlRunRepository.getRunCounts(crawlRunId);
    await this.finalizeRunIfStableAndComplete(crawlRunId, counts);
    await this.updateRunningRunGauges();
    logCp(
      crawlRunId,
      `maintenance recovered_stale=${recovered} reconciled_queued=${reconciled} queued=${counts.queued_count} in_progress=${counts.in_progress_count}`
    );
    return counts;
  }

  async runMaintenanceCycle(): Promise<number> {
    const activeRunIds = await this.crawlRunRepository.getRunningRunIds();
    const activeSet = new Set<number>();
    for (const crawlRunId of activeRunIds) {
      activeSet.add(crawlRunId);
      await this.runMaintenanceForRun(crawlRunId);
    }
    for (const trackedId of this.completionState.keys()) {
      if (!activeSet.has(trackedId)) {
        this.completionState.delete(trackedId);
      }
    }
    logCp(undefined, `reconciliation-cycle done runs=${activeRunIds.length}`);
    return activeRunIds.length;
  }

  async close(): Promise<void> {
    await this.queue.close();
  }

  private async finalizeRunIfStableAndComplete(
    crawlRunId: number,
    counts: RunCounts
  ): Promise<void> {
    crawlCompletionChecksTotal.inc();
    const isEmptyFrontier = counts.queued_count === 0 && counts.in_progress_count === 0;
    const prior = this.completionState.get(crawlRunId)?.empty_cycles ?? 0;
    const next = isEmptyFrontier ? prior + 1 : 0;

    this.completionState.set(crawlRunId, { empty_cycles: next });

    if (next >= 2) {
      const completed = await this.crawlRunRepository.markCompleted(
        crawlRunId,
        counts.visited_count,
        counts.failed_count
      );
      if (completed) {
        crawlRunsCompletedTotal.inc();
        logCp(
          crawlRunId,
          `completion-detected status=COMPLETED visited=${counts.visited_count} failed=${counts.failed_count}`
        );
      }
      this.completionState.delete(crawlRunId);
    } else if (isEmptyFrontier) {
      logCp(crawlRunId, `completion-check empty_frontier streak=${next} (need 2)`);
    }
  }

  private async requeueStaleClaims(crawlRunId: number): Promise<number> {
    const staleIds = await this.crawlUrlRepository.recoverStaleClaims(
      crawlRunId,
      CLAIM_STALE_SECONDS
    );
    const n = staleIds.length;
    if (n > 0) {
      crawlStaleClaimsRecoveredTotal.inc(n);
      logCp(crawlRunId, `stale-recovery count=${n}`);
    }

    if (n > 0) {
      const signals = await topUpRunSignals(this.queue, crawlRunId);
      if (signals > 0) {
        crawlUrlsRequeuedTotal.inc(signals);
      }
      logCp(crawlRunId, `stale-recovery run-signals-topped-up=${signals}`);
    }

    return n;
  }

  private async reconcileQueuedRows(crawlRunId: number): Promise<number> {
    const hasClaimable = await this.crawlUrlRepository.hasClaimableQueuedUrls(crawlRunId);
    if (hasClaimable) {
      const signals = await topUpRunSignals(this.queue, crawlRunId);
      if (signals > 0) {
        crawlQueueReconciliationEnqueuedTotal.inc(signals);
        crawlUrlsRequeuedTotal.inc(signals);
        logCp(crawlRunId, `reconciliation run-signals-topped-up=${signals}`);
      }
      return 1;
    }
    return 0;
  }

  private async updateRunningRunGauges(): Promise<void> {
    const row = await this.crawlUrlRepository.getRunningGauges();
    crawlUrlsQueuedGauge.set(row.queued ?? 0);
    crawlUrlsInProgressGauge.set(row.in_progress ?? 0);
    crawlUrlsFailedGauge.set(row.failed ?? 0);
  }
}
