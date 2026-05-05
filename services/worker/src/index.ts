import http from "node:http";
import { pgPool, redisConnection } from "@crawler/shared";
import {
  fetchGapJitterMs,
  fetchHostCooldownBaseMs,
  fetchHostCooldownMaxMs,
  fetchMinGapPerHostMs
} from "./workerDeps";
import { crawlJobQueue } from "./queue";
import {
  fetchGlobalMax,
  fetchPerHostMax,
  metricsPort,
  workerId,
  workerConcurrency
} from "./config";
import { metricsHandler } from "./prometheus";
import { createCrawlWorker } from "./worker";

const worker = createCrawlWorker();

worker.on("failed", (_job, _err) => undefined);
worker.on("error", (_err) => undefined);

http
  .createServer(async (req, res) => {
    if (req.url === "/metrics" || req.url?.startsWith("/metrics?")) {
      try {
        const { body, contentType } = await metricsHandler();
        res.writeHead(200, { "Content-Type": contentType });
        res.end(body);
      } catch (err) {
        res.writeHead(500).end((err as Error).message);
      }
      return;
    }
    res.writeHead(404).end();
  })
  .listen(metricsPort, () => {
    process.stdout.write(
      `[component=worker worker_id=${workerId}] metrics listening on :${metricsPort} path=/metrics\n`
    );
  });

process.stdout.write(
  `[component=worker worker_id=${workerId}] started bullmq_concurrency=${workerConcurrency} fetch_concurrency=${fetchGlobalMax} fetch_per_host=${fetchPerHostMax} fetch_min_gap_per_host_ms=${fetchMinGapPerHostMs} fetch_gap_jitter_ms=${fetchGapJitterMs} fetch_host_cooldown_base_ms=${fetchHostCooldownBaseMs} fetch_host_cooldown_max_ms=${fetchHostCooldownMaxMs}\n`
);

process.on("SIGINT", async () => {
  await worker.close();
  await crawlJobQueue.close();
  await redisConnection.quit();
  await pgPool.end();
  process.exit(0);
});
