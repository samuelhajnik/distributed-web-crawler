import { redisConnection, pgPool } from "@crawler/shared";
import type { Express } from "express";

export function registerHealthRoute(app: Express): void {
  app.get("/health", async (_req, res) => {
    try {
      await pgPool.query("SELECT 1");
      await redisConnection.ping();
      res.json({ status: "ok" });
    } catch (err) {
      res.status(500).json({ status: "error", error: (err as Error).message });
    }
  });
}
