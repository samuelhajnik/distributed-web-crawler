import type { Express } from "express";
import { CrawlRunService } from "../services/crawlRunService";

const ALLOWED_STATUSES = new Set([
  "QUEUED",
  "IN_PROGRESS",
  "VISITED",
  "REDIRECT_FOLLOWED",
  "REDIRECT_OUT_OF_SCOPE",
  "REDIRECT_301",
  "FORBIDDEN",
  "NOT_FOUND",
  "HTTP_TERMINAL",
  "FAILED",
  "CANCELLED"
]);

function parseListLimit(raw: unknown): number {
  const n = Number(raw ?? 20);
  if (Number.isNaN(n)) {
    return 20;
  }
  return Math.min(100, Math.max(1, Math.floor(n)));
}

export function registerCrawlRunRoutes(app: Express, crawlRunService: CrawlRunService): void {
  app.get("/crawl-runs", async (req, res) => {
    try {
      const limit = parseListLimit(req.query.limit);
      const result = await crawlRunService.listCrawlRuns(limit);
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.post("/crawl-runs", async (req, res) => {
    try {
      const result = await crawlRunService.createRun(req.body);
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.post("/crawl-runs/:id/cancel", async (req, res) => {
    try {
      const crawlRunId = Number(req.params.id);
      if (Number.isNaN(crawlRunId)) {
        res.status(400).json({ error: "Invalid run id" });
        return;
      }
      const result = await crawlRunService.cancelCrawlRun(crawlRunId);
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.get("/crawl-runs/:id/summary", async (req, res) => {
    try {
      const crawlRunId = Number(req.params.id);
      if (Number.isNaN(crawlRunId)) {
        res.status(400).json({ error: "Invalid run id" });
        return;
      }
      const result = await crawlRunService.getRunSummary(crawlRunId);
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.get("/crawl-runs/:id/export", async (req, res) => {
    try {
      const crawlRunId = Number(req.params.id);
      const format = String(req.query.format ?? "json").toLowerCase();
      const limit = Math.min(500_000, Math.max(1, Number(req.query.limit ?? 50_000)));
      if (Number.isNaN(crawlRunId)) {
        res.status(400).json({ error: "Invalid run id" });
        return;
      }

      const result = await crawlRunService.exportRun(crawlRunId, format, limit);
      if (result.headers) {
        for (const [key, value] of Object.entries(result.headers)) {
          res.setHeader(key, value);
        }
      }
      if (typeof result.body === "string") {
        res.status(result.status).send(result.body);
        return;
      }
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.get("/crawl-runs/:id/graph", async (req, res) => {
    try {
      const crawlRunId = Number(req.params.id);
      const limit = Math.min(200_000, Math.max(1, Number(req.query.limit ?? 100_000)));
      if (Number.isNaN(crawlRunId)) {
        res.status(400).json({ error: "Invalid run id" });
        return;
      }
      const result = await crawlRunService.getRunGraph(crawlRunId, limit);
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.get("/crawl-runs/:id/graph-delta", async (req, res) => {
    try {
      const crawlRunId = Number(req.params.id);
      if (Number.isNaN(crawlRunId)) {
        res.status(400).json({ error: "Invalid run id" });
        return;
      }

      const limitRaw = Number(req.query.limit ?? 5000);
      const limit = Number.isNaN(limitRaw)
        ? 5000
        : Math.min(20_000, Math.max(1, Math.floor(limitRaw)));

      let afterVersion = 0;
      const afterVersionRaw = req.query.after_version;
      if (
        afterVersionRaw !== undefined &&
        afterVersionRaw !== null &&
        String(afterVersionRaw).trim() !== ""
      ) {
        const parsed = Number(afterVersionRaw);
        if (!Number.isFinite(parsed) || parsed < 0) {
          res.status(400).json({ error: "Invalid after_version" });
          return;
        }
        afterVersion = Math.floor(parsed);
      }

      const result = await crawlRunService.getRunGraphDelta(crawlRunId, afterVersion, limit);
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.get("/crawl-runs/:id", async (req, res) => {
    try {
      const crawlRunId = Number(req.params.id);
      if (Number.isNaN(crawlRunId)) {
        res.status(400).json({ error: "Invalid run id" });
        return;
      }
      const result = await crawlRunService.getRun(crawlRunId);
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.get("/crawl-runs/:id/urls", async (req, res) => {
    try {
      const crawlRunId = Number(req.params.id);
      const status =
        typeof req.query.status === "string" ? req.query.status.toUpperCase() : undefined;
      const limit = Math.min(50000, Math.max(1, Number(req.query.limit ?? 50)));
      const offset = Math.max(0, Number(req.query.offset ?? 0));
      const sortKey = typeof req.query.sort === "string" ? req.query.sort.toLowerCase() : "id";
      const orderRaw = typeof req.query.order === "string" ? req.query.order.toLowerCase() : "asc";

      if (Number.isNaN(crawlRunId)) {
        res.status(400).json({ error: "Invalid run id" });
        return;
      }
      if (status && !ALLOWED_STATUSES.has(status)) {
        res.status(400).json({ error: "Invalid status filter" });
        return;
      }

      const result = await crawlRunService.listRunUrls(
        crawlRunId,
        status ?? null,
        limit,
        offset,
        sortKey,
        orderRaw
      );
      res.status(result.status).json(result.body);
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });
}
