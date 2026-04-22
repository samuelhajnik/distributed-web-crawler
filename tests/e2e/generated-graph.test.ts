import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeAll, describe, it } from "vitest";
import { assertExportMatchesExpected } from "../helpers/e2e-assert";
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
  const explicit = process.env.E2E_GRAPH_PAGES;
  if (explicit !== undefined && explicit !== "") {
    const n = Number(explicit);
    if (Number.isNaN(n)) {
      throw new Error("E2E_GRAPH_PAGES must be a number");
    }
    return Math.min(80, Math.max(8, n));
  }
  const tier = String(process.env.E2E_GRAPH_TIER ?? "default").toLowerCase();
  if (tier === "medium") {
    return 25;
  }
  if (tier === "stress") {
    return 50;
  }
  return 11;
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
    it(`generator-derived expectation is correct for seed=${seed} (set TEST_GRAPH_SEED to rerun)`, async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "e2e-gen-"));
      tmpDirs.push(dir);
      const site = await startStaticSite(dir);
      try {
        const n = pageCount();
        const generated = generateHtmlGraph(site.baseUrl, seed, n);
        const { graph, seedUrl, expected } = generated;
        writeGeneratedGraphToDisk(dir, graph);
        const crawlRunId = await createCrawlRun(seedUrl);
        await waitForCrawlComplete(crawlRunId, { timeoutMs: n >= 40 ? 180_000 : 120_000 });
        const summary = await getSummary(crawlRunId);
        const exp = await exportJson(crawlRunId);
        assertExportMatchesExpected(summary, exp, expected, { seedUrl });

        // Optional local cross-check: compare model-derived expectation against HTML crawl simulation.
        if (process.env.E2E_GRAPH_ORACLE_CROSSCHECK === "1") {
          const oracle = simulateLocalCrawl(seedUrl, graph);
          expectSetsEqual(expected.discoveredUrls, oracle.allUrls, "expected.discoveredUrls");
          expectSetsEqual(expected.visitedUrls, oracle.visited, "expected.visitedUrls");
          expectSetsEqual(expected.notFoundUrls, oracle.notFound, "expected.notFoundUrls");
          expectSetsEqual(expected.failedUrls, oracle.failed, "expected.failedUrls");
        }
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

function expectSetsEqual(a: Set<string>, b: Set<string>, label: string): void {
  const onlyA = [...a].filter((x) => !b.has(x)).sort();
  const onlyB = [...b].filter((x) => !a.has(x)).sort();
  if (onlyA.length || onlyB.length) {
    throw new Error(
      `${label} mismatch while cross-checking generator expectation onlyA=${JSON.stringify(onlyA)} onlyB=${JSON.stringify(onlyB)}`
    );
  }
}
