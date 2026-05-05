import express from "express";
import path from "node:path";
import { registerCrawlRunRoutes } from "./routes/crawlRuns";
import { registerHealthRoute } from "./routes/health";
import { registerMetricsRoute } from "./routes/metrics";
import { CrawlRunService } from "./services/crawlRunService";

export function createServer(crawlRunService: CrawlRunService) {
  const app = express();
  app.use(express.json());
  const uiDir = path.resolve(__dirname, "../public/ui");
  app.use("/ui", express.static(uiDir));

  registerMetricsRoute(app);
  registerCrawlRunRoutes(app, crawlRunService);
  registerHealthRoute(app);

  return app;
}
