import { Worker } from "bullmq";
import { CRAWL_QUEUE_NAME, type CrawlJobPayload, redisConnection } from "@crawler/shared";
import { workerConcurrency } from "./config";
import { processCrawlJob } from "./processing/processCrawlJob";

export function createCrawlWorker(): Worker<CrawlJobPayload> {
  return new Worker<CrawlJobPayload>(CRAWL_QUEUE_NAME, async (job) => processCrawlJob(job), {
    connection: redisConnection,
    concurrency: workerConcurrency
  });
}
