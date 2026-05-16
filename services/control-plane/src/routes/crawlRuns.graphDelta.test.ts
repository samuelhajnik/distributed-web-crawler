import express from "express";
import { describe, expect, it, vi } from "vitest";
import request from "supertest";
import { registerCrawlRunRoutes } from "./crawlRuns";
import type { CrawlRunService } from "../services/crawlRunService";

function createTestApp(getRunGraphDelta: CrawlRunService["getRunGraphDelta"]) {
  const app = express();
  const crawlRunService = {
    getRunGraphDelta
  } as unknown as CrawlRunService;
  registerCrawlRunRoutes(app, crawlRunService);
  return app;
}

describe("GET /crawl-runs/:id/graph-delta", () => {
  it("returns delta payload from the service", async () => {
    const getRunGraphDelta = vi.fn().mockResolvedValue({
      status: 200,
      body: {
        crawl_run_id: 3,
        urls: [{ id: 10, status: "VISITED", graph_version: "42" }],
        edges: [{ from_url_id: 1, to_url_id: 10 }],
        pagination: { limit: 5000, returned: 1, has_more: false },
        watermark: { graph_version: 42 },
        totals: { nodes: 5, edges: 4 }
      }
    });
    const app = createTestApp(getRunGraphDelta);

    const res = await request(app).get("/crawl-runs/3/graph-delta?after_version=9&limit=100");

    expect(res.status).toBe(200);
    expect(res.body.urls).toHaveLength(1);
    expect(res.body.edges).toHaveLength(1);
    expect(res.body.watermark.graph_version).toBe(42);
    expect(getRunGraphDelta).toHaveBeenCalledWith(3, 9, 100);
  });

  it("returns 400 for invalid after_version", async () => {
    const getRunGraphDelta = vi.fn();
    const app = createTestApp(getRunGraphDelta);

    const res = await request(app).get("/crawl-runs/1/graph-delta?after_version=not-a-number");

    expect(res.status).toBe(400);
    expect(getRunGraphDelta).not.toHaveBeenCalled();
  });

  it("preserves after_version in watermark when the service returns no rows", async () => {
    const getRunGraphDelta = vi.fn().mockResolvedValue({
      status: 200,
      body: {
        crawl_run_id: 2,
        urls: [],
        edges: [],
        pagination: { limit: 5000, returned: 0, has_more: false },
        watermark: { graph_version: 17 },
        totals: { nodes: 3, edges: 2 }
      }
    });
    const app = createTestApp(getRunGraphDelta);

    const res = await request(app).get("/crawl-runs/2/graph-delta?after_version=17");

    expect(res.status).toBe(200);
    expect(res.body.watermark.graph_version).toBe(17);
    expect(getRunGraphDelta).toHaveBeenCalledWith(2, 17, 5000);
  });

  it("defaults after_version to 0 when not provided", async () => {
    const getRunGraphDelta = vi.fn().mockResolvedValue({
      status: 200,
      body: {
        crawl_run_id: 1,
        urls: [],
        edges: [],
        pagination: { limit: 5000, returned: 0, has_more: false },
        watermark: { graph_version: 0 },
        totals: { nodes: 0, edges: 0 }
      }
    });
    const app = createTestApp(getRunGraphDelta);

    await request(app).get("/crawl-runs/1/graph-delta").expect(200);

    expect(getRunGraphDelta).toHaveBeenCalledWith(1, 0, 5000);
  });
});
