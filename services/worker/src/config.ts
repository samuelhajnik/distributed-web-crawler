import os from "node:os";
import {
  DEFAULT_FETCH_CONCURRENCY,
  DEFAULT_FETCH_PER_HOST_CONCURRENCY,
  DEFAULT_WORKER_CONCURRENCY,
  readWorkerEnvInt
} from "./concurrencyConfig";

export const workerConcurrency = readWorkerEnvInt("WORKER_CONCURRENCY", DEFAULT_WORKER_CONCURRENCY);
export const fetchGlobalMax = readWorkerEnvInt("FETCH_CONCURRENCY", DEFAULT_FETCH_CONCURRENCY);
export const fetchPerHostMax = readWorkerEnvInt(
  "FETCH_CONCURRENCY_PER_HOST",
  DEFAULT_FETCH_PER_HOST_CONCURRENCY
);
export const workerId = process.env.WORKER_ID ?? `${os.hostname()}-${process.pid}`;
export const metricsPort = Number(process.env.WORKER_METRICS_PORT ?? 9091);

/** Honest product id in a common UA shape; avoids mimicking a specific browser build. */
const DEFAULT_REQUEST_USER_AGENT = "Mozilla/5.0 (compatible; distributed-web-crawler/1.0)";
const REQUEST_USER_AGENT = process.env.CRAWLER_USER_AGENT?.trim() || DEFAULT_REQUEST_USER_AGENT;

/** Shared defaults for document-style GETs (undici fetch + request; redirect-following fetch reuses the same options). */
export function buildRequestHeaders(): Record<string, string> {
  return {
    "user-agent": REQUEST_USER_AGENT,
    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9"
  };
}

export function logW(crawlRunId: number, urlId: number, msg: string): void {
  process.stdout.write(
    `[worker worker_id=${workerId} crawl_run=${crawlRunId} url_id=${urlId}] ${msg}\n`
  );
}
