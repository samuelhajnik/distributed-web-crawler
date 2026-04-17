import type { CrawlJobPayload } from "./types";

export type CrawlBulkJob = {
  name: string;
  data: CrawlJobPayload;
  opts: { removeOnComplete: number; removeOnFail: number };
};

const defaultOpts = { removeOnComplete: 2000, removeOnFail: 2000 };

/** Pure builder for BullMQ bulk jobs (easy to unit test, idempotent with DB claim). */
export function buildCrawlBulkJobs(crawlRunId: number, urlIds: number[]): CrawlBulkJob[] {
  return urlIds.map((urlId) => ({
    name: "crawl-url",
    data: { crawlRunId, urlId },
    opts: { ...defaultOpts }
  }));
}
