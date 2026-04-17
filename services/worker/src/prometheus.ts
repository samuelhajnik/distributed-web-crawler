import client from "prom-client";

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const secondsBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 15, 60];

export const crawlFetchDurationSeconds = new client.Histogram({
  name: "crawl_fetch_duration_seconds",
  help: "HTTP client request duration until response headers are available (undici request)",
  buckets: secondsBuckets,
  registers: [register]
});

export const crawlProcessingDurationSeconds = new client.Histogram({
  name: "crawl_processing_duration_seconds",
  help: "End-to-end worker handling for one claimed URL task (after successful claim)",
  buckets: secondsBuckets,
  registers: [register]
});

export const crawlQueueLatencySeconds = new client.Histogram({
  name: "crawl_queue_latency_seconds",
  help: "Time from BullMQ job creation timestamp until worker starts processing the job",
  buckets: secondsBuckets,
  registers: [register]
});

export const crawlUrlsDiscoveredTotal = new client.Counter({
  name: "crawl_urls_discovered_total",
  help: "New crawl_urls rows inserted from link extraction",
  registers: [register]
});

export const crawlUrlsVisitedTotal = new client.Counter({
  name: "crawl_urls_visited_total",
  help: "URLs marked VISITED",
  registers: [register]
});

export const crawlUrlsFailedTotal = new client.Counter({
  name: "crawl_urls_failed_total",
  help: "URLs marked FAILED",
  registers: [register]
});

export const crawlUrlsRetriedTotal = new client.Counter({
  name: "crawl_urls_retried_total",
  help: "URLs re-queued after retryable failure",
  registers: [register]
});

export const crawlUrlsRequeuedTotal = new client.Counter({
  name: "crawl_urls_requeued_total",
  help: "BullMQ jobs scheduled (includes discovery enqueue)",
  registers: [register]
});

/** Increments once per URL task that was successfully claimed (terminal outcome or retry path). */
export const processedUrlsTotal = new client.Counter({
  name: "processed_urls_total",
  help: "URL tasks completed after a successful atomic claim (one increment per claimed job handling)",
  registers: [register]
});

export async function metricsHandler(): Promise<{ body: string; contentType: string }> {
  return {
    body: await register.metrics(),
    contentType: register.contentType
  };
}
