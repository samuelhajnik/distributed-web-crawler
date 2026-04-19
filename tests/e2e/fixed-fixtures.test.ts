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
import {
  writeBrokenLinkFixture,
  writeCycleFixture,
  writeDuplicateAndExternalFixture,
  writeSinglePageFixture,
  writeWwwScopeFixture
} from "../helpers/fixture-builders";
import type { LocalPageGraph } from "../helpers/oracle";
import { simulateLocalCrawl } from "../helpers/oracle";
import { startStaticSite } from "../helpers/static-site-server";

let tmpDirs: string[] = [];
afterEach(() => {
  for (const d of tmpDirs.splice(0)) {
    fs.rmSync(d, { recursive: true, force: true });
  }
});

function mkFixtureDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), "e2e-fixed-"));
  tmpDirs.push(d);
  return d;
}

beforeAll(async () => {
  const ok = await healthCheck();
  if (!ok) {
    throw new Error(
      `E2E requires a running crawler stack (${crawlerApiBase()}). Start with: docker compose up --build -d`
    );
  }
});

async function runCase(setup: (dir: string, origin: string) => { graph: LocalPageGraph; seedUrl: string }): Promise<void> {
  const dir = mkFixtureDir();
  const site = await startStaticSite(dir);
  try {
    const { graph, seedUrl } = setup(dir, site.baseUrl);
    const oracle = simulateLocalCrawl(seedUrl, graph);
    const crawlRunId = await createCrawlRun(seedUrl);
    await waitForCrawlComplete(crawlRunId, { timeoutMs: 120_000 });
    const summary = await getSummary(crawlRunId);
    const exp = await exportJson(crawlRunId);
    assertExportMatchesOracle(summary, exp, oracle);
  } finally {
    await site.close();
  }
}

describe("E2E fixed fixtures", () => {
  it("single page, no links", async () => {
    await runCase((dir, origin) => writeSinglePageFixture(dir, origin));
  });

  it("duplicates, fragment collapse, external ignored", async () => {
    await runCase((dir, origin) => writeDuplicateAndExternalFixture(dir, origin));
  });

  it("broken internal link -> one NOT_FOUND", async () => {
    await runCase((dir, origin) => writeBrokenLinkFixture(dir, origin));
  });

  it("cycle a→b→c→a terminates with three VISITED", async () => {
    await runCase((dir, origin) => writeCycleFixture(dir, origin));
  });

  it.skipIf(process.env.E2E_WWW !== "1")("www host in scope (set E2E_WWW=1 to enable)", async () => {
    await runCase((dir, origin) => writeWwwScopeFixture(dir, origin));
  });
});
