import express from "express";
import { describe, expect, it, vi } from "vitest";
import request from "supertest";
import { registerCrawlRunRoutes } from "./crawlRuns";
import type { CrawlRunService } from "../services/crawlRunService";

function createTestApp(cancelCrawlRun: CrawlRunService["cancelCrawlRun"]) {
  const app = express();
  const crawlRunService = { cancelCrawlRun } as unknown as CrawlRunService;
  registerCrawlRunRoutes(app, crawlRunService);
  return app;
}

describe("POST /crawl-runs/:id/cancel", () => {
  it("returns cancellation result for a running crawl", async () => {
    const cancelCrawlRun = vi.fn().mockResolvedValue({
      status: 200,
      body: {
        crawl_run_id: 9,
        status: "CANCELLED",
        changed: true,
        cancelled_url_count: 12
      }
    });
    const app = createTestApp(cancelCrawlRun);

    const res = await request(app).post("/crawl-runs/9/cancel");

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      crawl_run_id: 9,
      status: "CANCELLED",
      changed: true,
      cancelled_url_count: 12
    });
    expect(cancelCrawlRun).toHaveBeenCalledWith(9);
  });

  it("returns 404 when run does not exist", async () => {
    const cancelCrawlRun = vi.fn().mockResolvedValue({
      status: 404,
      body: { error: "Run not found" }
    });
    const app = createTestApp(cancelCrawlRun);

    const res = await request(app).post("/crawl-runs/404/cancel");

    expect(res.status).toBe(404);
    expect(res.body.error).toBe("Run not found");
  });
});
