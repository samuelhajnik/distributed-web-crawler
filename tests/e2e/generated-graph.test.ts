import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeAll, describe, it } from "vitest";
import { assertExportMatchesOracle } from "../helpers/e2e-assert";
import {
  createCrawlRun,
  crawlerApiBase,
  exportJson,
  getSummary,
  healthCheck,
  waitForCrawlComplete
} from "../helpers/crawler-client";
import { generateHtmlGraph, writeGeneratedGraphToDisk } from "../helpers/graph-generator";
import { simulateLocalCrawl } from "../helpers/oracle";
import { startStaticSite } from "../helpers/static-site-server";

let tmpDirs: string[] = [];
afterEach(() => {
  for (const d of tmpDirs.splice(0)) {
    fs.rmSync(d, { recursive: true, force: true });
  }
});

beforeAll(async () => {
  const ok = await healthCheck();
  if (!ok) {
    throw new Error(
      `E2E requires a running crawler stack (${crawlerApiBase()}). Start with: docker compose up --build -d`
    );
  }
});

function pageCount(): number {
  const n = Number(process.env.E2E_GRAPH_PAGES ?? "11");
  return Math.min(20, Math.max(8, n));
}

function seedsToRun(): number[] {
  const raw = process.env.TEST_GRAPH_SEED;
  if (raw !== undefined && raw !== "") {
    const s = Number(raw);
    if (Number.isNaN(s)) {
      throw new Error("TEST_GRAPH_SEED must be a number");
    }
    return [s];
  }
  return [42_424, 91_817];
}

describe("E2E seeded random HTML graphs", () => {
  for (const seed of seedsToRun()) {
    it(`graph is correct for seed=${seed} (set TEST_GRAPH_SEED to rerun)`, async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "e2e-gen-"));
      tmpDirs.push(dir);
      const site = await startStaticSite(dir);
      try {
        const n = pageCount();
        const { graph, seedUrl } = generateHtmlGraph(site.baseUrl, seed, n);
        writeGeneratedGraphToDisk(dir, graph);
        const oracle = simulateLocalCrawl(seedUrl, graph);
        const crawlRunId = await createCrawlRun(seedUrl);
        await waitForCrawlComplete(crawlRunId, { timeoutMs: 120_000 });
        const summary = await getSummary(crawlRunId);
        const exp = await exportJson(crawlRunId);
        assertExportMatchesOracle(summary, exp, oracle);
      } catch (e) {
        process.stderr.write(
          `[E2E generated graph FAILED] TEST_GRAPH_SEED=${seed} E2E_GRAPH_PAGES=${pageCount()}\n`
        );
        throw e;
      } finally {
        await site.close();
      }
    });
  }
});
