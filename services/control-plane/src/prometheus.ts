import client from "prom-client";

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const cycleBuckets = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 120];

export const crawlReconciliationCycleDurationSeconds = new client.Histogram({
  name: "crawl_reconciliation_cycle_duration_seconds",
  help: "Wall time for one control-plane maintenance sweep (all RUNNING runs)",
  buckets: cycleBuckets,
  registers: [register]
});

export const crawlRunsStartedTotal = new client.Counter({
  name: "crawl_runs_started_total",
  help: "Crawl runs created via API",
  registers: [register]
});

export const crawlRunsCompletedTotal = new client.Counter({
  name: "crawl_runs_completed_total",
  help: "Crawl runs transitioned to COMPLETED",
  registers: [register]
});

export const crawlStaleClaimsRecoveredTotal = new client.Counter({
  name: "crawl_stale_claims_recovered_total",
  help: "Stale IN_PROGRESS rows returned to QUEUED",
  registers: [register]
});

export const crawlQueueReconciliationCyclesTotal = new client.Counter({
  name: "crawl_queue_reconciliation_cycles_total",
  help: "Maintenance loop iterations across all runs",
  registers: [register]
});

export const crawlQueueReconciliationEnqueuedTotal = new client.Counter({
  name: "crawl_queue_reconciliation_enqueued_total",
  help: "Jobs enqueued from reconciliation (QUEUED rows re-published)",
  registers: [register]
});

export const crawlUrlsRequeuedTotal = new client.Counter({
  name: "crawl_urls_requeued_total",
  help: "Jobs enqueued from control-plane (stale recovery + reconciliation + initial seed)",
  registers: [register]
});

export const crawlUrlsDiscoveredTotal = new client.Counter({
  name: "crawl_urls_discovered_total",
  help: "URLs inserted into crawl_urls from control-plane (root seed)",
  registers: [register]
});

export const crawlCompletionChecksTotal = new client.Counter({
  name: "crawl_completion_checks_total",
  help: "Completion stability checks executed",
  registers: [register]
});

export const crawlUrlsQueuedGauge = new client.Gauge({
  name: "crawl_urls_queued_gauge",
  help: "Sum of QUEUED URLs across RUNNING crawl runs",
  registers: [register]
});

export const crawlUrlsInProgressGauge = new client.Gauge({
  name: "crawl_urls_in_progress_gauge",
  help: "Sum of IN_PROGRESS URLs across RUNNING crawl runs",
  registers: [register]
});

export const crawlUrlsFailedGauge = new client.Gauge({
  name: "crawl_urls_failed_gauge",
  help: "Sum of FAILED URLs across RUNNING crawl runs",
  registers: [register]
});

export async function metricsHandler(): Promise<{ body: string; contentType: string }> {
  return {
    body: await register.metrics(),
    contentType: register.contentType
  };
}
