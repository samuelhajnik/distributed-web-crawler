import fs from "node:fs";
import http from "node:http";
import path from "node:path";

function safeFilePath(rootDir: string, urlPath: string): string | null {
  const decoded = decodeURIComponent(urlPath.split("?")[0] ?? "");
  let rel = decoded === "/" || decoded === "" ? "index.html" : decoded.replace(/^\//, "");
  rel = path.normalize(rel);
  if (rel.startsWith("..")) {
    return null;
  }
  const abs = path.resolve(rootDir, rel);
  const root = path.resolve(rootDir);
  if (!abs.startsWith(root + path.sep) && abs !== root) {
    return null;
  }
  return abs;
}

export type StaticSiteServer = {
  /** Public origin workers must use (often `host.docker.internal` when workers run in Docker). */
  baseUrl: string;
  close: () => Promise<void>;
};

/**
 * Hostname workers use to reach this process from Docker (override with E2E_FIXTURE_HOST, e.g. `127.0.0.1` when the worker runs on the host).
 */
export function fixturePublicHost(): string {
  return process.env.E2E_FIXTURE_HOST ?? "host.docker.internal";
}

/**
 * Serves files from `rootDir` as text/html. `/` maps to `index.html`.
 * Listens on `0.0.0.0` so Dockerized workers can reach the host via `host.docker.internal`.
 */
export function startStaticSite(rootDir: string): Promise<StaticSiteServer> {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      const u = new URL(req.url ?? "/", "http://127.0.0.1");
      const filePath = safeFilePath(rootDir, u.pathname);
      if (!filePath) {
        res.writeHead(403).end("forbidden");
        return;
      }
      fs.readFile(filePath, (err, buf) => {
        if (err) {
          res.writeHead(404).end("not found");
          return;
        }
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end(buf);
      });
    });

    server.on("error", reject);
    server.listen(0, "0.0.0.0", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") {
        reject(new Error("invalid listen address"));
        return;
      }
      const baseUrl = `http://${fixturePublicHost()}:${addr.port}`;
      resolve({
        baseUrl,
        close: () =>
          new Promise((res, rej) => {
            server.close((e) => (e ? rej(e) : res()));
          })
      });
    });
  });
}
