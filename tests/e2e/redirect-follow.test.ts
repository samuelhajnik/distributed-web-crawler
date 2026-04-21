import http from "node:http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  createCrawlRun,
  crawlerApiBase,
  exportJson,
  getSummary,
  healthCheck,
  waitForCrawlComplete
} from "../helpers/crawler-client";
import { fixturePublicHost } from "../helpers/static-site-server";

type RedirectFixtureServer = {
  baseUrl: string;
  close: () => Promise<void>;
};

function startRedirectFixtureServer(): Promise<RedirectFixtureServer> {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      const path = new URL(req.url ?? "/", "http://127.0.0.1").pathname;
      if (path === "/old") {
        res.writeHead(302, { Location: "/dir/index.html" }).end();
        return;
      }
      if (path === "/dir/index.html") {
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end('<html><body><a href="./child.html">child</a></body></html>');
        return;
      }
      if (path === "/dir/child.html") {
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end("<html><body>child</body></html>");
        return;
      }
      if (path === "/external-start") {
        res.writeHead(302, { Location: "https://example.com/" }).end();
        return;
      }
      if (path === "/a") {
        res.writeHead(302, { Location: "/b" }).end();
        return;
      }
      if (path === "/a301") {
        res.writeHead(301, { Location: "/b" }).end();
        return;
      }
      if (path === "/b") {
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end("<html><body>b</body></html>");
        return;
      }
      if (path === "/hang-redirect") {
        res.writeHead(302, { Location: "/hang-body" }).end();
        return;
      }
      if (path === "/hang-body") {
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.write("<html><body>");
        return;
      }
      res.writeHead(404).end("not found");
    });
    server.on("error", reject);
    server.listen(0, "0.0.0.0", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") {
        reject(new Error("invalid fixture address"));
        return;
      }
      resolve({
        baseUrl: `http://${fixturePublicHost()}:${addr.port}`,
        close: () =>
          new Promise((r, j) => {
            server.closeAllConnections();
            server.close((e) => (e ? j(e) : r()));
          })
      });
    });
  });
}

async function waitForRunStopped(runId: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const summary = await getSummary(runId);
    if (summary.status !== "RUNNING") {
      return;
    }
    await new Promise((r) => setTimeout(r, 750));
  }
  throw new Error(`run ${runId} did not stop within ${timeoutMs}ms`);
}

let fixture: RedirectFixtureServer;

beforeAll(async () => {
  const ok = await healthCheck();
  if (!ok) {
    throw new Error(
      `E2E requires a running crawler stack (${crawlerApiBase()}). Start with: docker compose up --build -d`
    );
  }
  fixture = await startRedirectFixtureServer();
});

afterAll(async () => {
  await fixture.close();
});

describe("E2E redirect following", () => {
  it("resolves relative links against final redirected URL", async () => {
    const runId = await createCrawlRun(`${fixture.baseUrl}/old`, { followRedirects: true });
    await waitForCrawlComplete(runId, { timeoutMs: 120_000 });
    const exp = await exportJson(runId);
    const urls = new Set(exp.urls.map((u) => u.normalized_url));
    expect(urls.has(`${fixture.baseUrl}/dir/child.html`)).toBe(true);
    expect(urls.has(`${fixture.baseUrl}/child.html`)).toBe(false);
  });

  it("marks redirect terminal when final target is out of scope and does not expand it", async () => {
    const runId = await createCrawlRun(`${fixture.baseUrl}/external-start`, {
      followRedirects: true,
      scopeMode: "same_host"
    });
    await waitForCrawlComplete(runId, { timeoutMs: 120_000 });
    const exp = await exportJson(runId);
    const start = exp.urls.find((u) => u.normalized_url === `${fixture.baseUrl}/external-start`);
    expect(start?.status).toBe("REDIRECT_OUT_OF_SCOPE");
    expect(start?.redirected).toBe(true);
    expect(start?.final_in_scope).toBe(false);
    expect(start?.final_url).toBe("https://example.com/");
    expect(exp.urls.some((u) => u.normalized_url.startsWith("https://example.com/"))).toBe(false);
  });

  it("preserves redirect visibility and final status for followed redirects", async () => {
    const runId = await createCrawlRun(`${fixture.baseUrl}/a`, { followRedirects: true });
    await waitForCrawlComplete(runId, { timeoutMs: 120_000 });
    const exp = await exportJson(runId);
    const row = exp.urls.find((u) => u.normalized_url === `${fixture.baseUrl}/a`);
    expect(row?.status).toBe("REDIRECT_FOLLOWED");
    expect(row?.final_url).toBe(`${fixture.baseUrl}/b`);
    expect(row?.http_status).toBe(200);
  });

  it("keeps legacy redirect classification when follow redirects is disabled", async () => {
    const runId = await createCrawlRun(`${fixture.baseUrl}/a301`, { followRedirects: false });
    await waitForCrawlComplete(runId, { timeoutMs: 120_000 });
    const exp = await exportJson(runId);
    const row = exp.urls.find((u) => u.normalized_url === `${fixture.baseUrl}/a301`);
    expect(row?.status).toBe("REDIRECT_301");
  });

  it("aborts hanging redirected body and does not stay in-progress indefinitely", async () => {
    const runId = await createCrawlRun(`${fixture.baseUrl}/hang-redirect`, {
      followRedirects: true,
      requestTimeoutMs: 1000,
      maxRetries: 0
    });
    await waitForRunStopped(runId, 90_000);
    const exp = await exportJson(runId);
    const row = exp.urls.find((u) => u.normalized_url === `${fixture.baseUrl}/hang-redirect`);
    expect(row?.status).toBe("FAILED");
  });
});
