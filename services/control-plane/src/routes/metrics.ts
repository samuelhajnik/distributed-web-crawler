import type { Express } from "express";
import { metricsHandler } from "../prometheus";

export function registerMetricsRoute(app: Express): void {
  app.get("/metrics", async (_req, res) => {
    try {
      const { body, contentType } = await metricsHandler();
      res.setHeader("Content-Type", contentType);
      res.send(body);
    } catch (err) {
      res.status(500).send((err as Error).message);
    }
  });
}
