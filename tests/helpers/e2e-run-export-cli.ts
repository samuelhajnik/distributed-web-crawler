/**
 * CLI: start a tiny static fixture, run one crawl, write export JSON.
 * Usage: npx tsx tests/helpers/e2e-run-export-cli.ts --fixture dupes --out /tmp/export.json
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createCrawlRun, exportJson, waitForCrawlComplete } from "./crawler-client";
import { writeDupesForScript } from "./fixture-builders";
import { startStaticSite } from "./static-site-server";

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(name);
  if (i === -1) {
    return undefined;
  }
  return process.argv[i + 1];
}

async function main(): Promise<void> {
  const outPath = arg("--out");
  const fixture = arg("--fixture") ?? "dupes";
  if (!outPath) {
    process.stderr.write("usage: npx tsx tests/helpers/e2e-run-export-cli.ts --fixture dupes --out export.json\n");
    process.exit(2);
  }

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "e2e-cli-"));
  const site = await startStaticSite(dir);
  try {
    if (fixture === "dupes") {
      writeDupesForScript(dir);
      const seedUrl = `${site.baseUrl}/`;
      const crawlRunId = await createCrawlRun(seedUrl);
      await waitForCrawlComplete(crawlRunId, { timeoutMs: 120_000 });
      const data = await exportJson(crawlRunId);
      fs.writeFileSync(outPath, JSON.stringify(data, null, 2), "utf8");
      process.stdout.write(`wrote ${outPath} crawl_run_id=${crawlRunId}\n`);
    } else {
      throw new Error(`unknown fixture: ${fixture}`);
    }
  } finally {
    await site.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + "\n");
  process.exit(1);
});
