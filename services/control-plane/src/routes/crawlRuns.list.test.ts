import express from "express";
import { describe, expect, it, vi } from "vitest";
import request from "supertest";
import { registerCrawlRunRoutes } from "./crawlRuns";
import type { CrawlRunService } from "../services/crawlRunService";

function createTestApp(listCrawlRuns: CrawlRunService["listCrawlRuns"]) {
  const app = express();
  const crawlRunService = {
    listCrawlRuns
  } as unknown as CrawlRunService;
  registerCrawlRunRoutes(app, crawlRunService);
  return app;
}

describe("GET /crawl-runs", () => {
  it("returns recent runs from the service", async () => {
    const listCrawlRuns = vi.fn().mockResolvedValue({
      status: 200,
      body: {
        runs: [
          {
            crawl_run_id: 7,
            id: 7,
            status: "RUNNING",
            seed_url: "https://example.com/",
            totals: { discovered: 3, visited: 1, queued: 2, in_progress: 0, failed: 0 }
          }
        ]
      }
    });
    const app = createTestApp(listCrawlRuns);

    const res = await request(app).get("/crawl-runs?limit=5");

    expect(res.status).toBe(200);
    expect(res.body.runs).toHaveLength(1);
    expect(res.body.runs[0].crawl_run_id).toBe(7);
    expect(listCrawlRuns).toHaveBeenCalledWith(5);
  });

  it("clamps limit to 1..100 and defaults invalid values to 20", async () => {
    const listCrawlRuns = vi.fn().mockResolvedValue({ status: 200, body: { runs: [] } });
    const app = createTestApp(listCrawlRuns);

    await request(app).get("/crawl-runs?limit=500").expect(200);
    expect(listCrawlRuns).toHaveBeenLastCalledWith(100);

    await request(app).get("/crawl-runs?limit=0").expect(200);
    expect(listCrawlRuns).toHaveBeenLastCalledWith(1);

    await request(app).get("/crawl-runs?limit=xyzzy").expect(200);
    expect(listCrawlRuns).toHaveBeenLastCalledWith(20);

    await request(app).get("/crawl-runs").expect(200);
    expect(listCrawlRuns).toHaveBeenLastCalledWith(20);
  });
});
