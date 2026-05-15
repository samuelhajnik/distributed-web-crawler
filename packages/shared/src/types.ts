export interface CrawlJobPayload {
  crawlRunId: number;
  slot: number;
}

/** Legacy URL-level jobs may still exist in Redis during dev; not used for new enqueue. */
export interface LegacyCrawlJobPayload {
  crawlRunId: number;
  urlId?: number;
}
