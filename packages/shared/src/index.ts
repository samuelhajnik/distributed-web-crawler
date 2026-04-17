import { Queue } from "bullmq";
import dotenv from "dotenv";
import IORedis from "ioredis";
import { Pool } from "pg";
import type { CrawlJobPayload } from "./types";

dotenv.config();

export type UrlStatus = "QUEUED" | "IN_PROGRESS" | "VISITED" | "FAILED";
export type RunStatus = "RUNNING" | "COMPLETED" | "FAILED";

export type { CrawlJobPayload } from "./types";

const POSTGRES_HOST = process.env.POSTGRES_HOST ?? "localhost";
const POSTGRES_PORT = Number(process.env.POSTGRES_PORT ?? 5432);
const POSTGRES_USER = process.env.POSTGRES_USER ?? "crawler";
const POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD ?? "crawler";
const POSTGRES_DB = process.env.POSTGRES_DB ?? "crawler";
const REDIS_HOST = process.env.REDIS_HOST ?? "localhost";
const REDIS_PORT = Number(process.env.REDIS_PORT ?? 6379);

export const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? 2);
export const CLAIM_STALE_SECONDS = Number(process.env.CLAIM_STALE_SECONDS ?? 120);
export const RECONCILE_INTERVAL_SECONDS = Number(process.env.RECONCILE_INTERVAL_SECONDS ?? 10);
export const RECONCILE_BATCH_SIZE = Number(process.env.RECONCILE_BATCH_SIZE ?? 500);
export const RETRY_BASE_DELAY_MS = Number(process.env.RETRY_BASE_DELAY_MS ?? 1000);
export const RETRY_MAX_DELAY_MS = Number(process.env.RETRY_MAX_DELAY_MS ?? 30000);
export const RETRY_429_MULTIPLIER = Number(process.env.RETRY_429_MULTIPLIER ?? 4);
export const CRAWL_QUEUE_NAME = "crawl-queue";

export const pgPool = new Pool({
  host: POSTGRES_HOST,
  port: POSTGRES_PORT,
  user: POSTGRES_USER,
  password: POSTGRES_PASSWORD,
  database: POSTGRES_DB
});

export const redisConnection = new IORedis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  maxRetriesPerRequest: null
});

export function createCrawlQueue(): Queue<CrawlJobPayload> {
  return new Queue<CrawlJobPayload>(CRAWL_QUEUE_NAME, { connection: redisConnection });
}

export type { FetchClassification } from "./classification";
export { classifyExecutionError, classifyHttpResponse } from "./classification";
export { buildAllowedHostSet, normalizeAbsoluteUrl, normalizeUrl, parseSeedUrl } from "./url";
export { buildCrawlBulkJobs } from "./reconciliation";
export type { CrawlBulkJob } from "./reconciliation";
