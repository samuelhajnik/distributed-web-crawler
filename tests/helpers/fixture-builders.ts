import fs from "node:fs";
import path from "node:path";
import { normalizeAbsoluteUrl, parseSeedUrl } from "@crawler/shared";
import type { LocalPageGraph } from "./oracle";

/** Canonical normalized URL string aligned with `POST /crawl-runs` + `parseSeedUrl`. */
function u(origin: string, pathname: string): string {
  const o = origin.replace(/\/$/, "");
  const raw = pathname === "/" || pathname === "" ? `${o}/` : `${o}${pathname.startsWith("/") ? pathname : `/${pathname}`}`;
  const abs = normalizeAbsoluteUrl(raw);
  if (!abs) {
    throw new Error(`bad fixture url raw=${raw}`);
  }
  const p = parseSeedUrl(abs);
  if (!p) {
    throw new Error(`bad fixture url abs=${abs}`);
  }
  return p.normalized;
}

export function writeSinglePageFixture(dir: string, origin: string): { graph: LocalPageGraph; seedUrl: string } {
  const html = "<!doctype html><html><body><p>only</p></body></html>";
  fs.writeFileSync(path.join(dir, "index.html"), html, "utf8");
  const seedUrl = u(origin, "/");
  return {
    graph: { pages: new Map([[seedUrl, html]]) },
    seedUrl
  };
}

export function writeDuplicateAndExternalFixture(dir: string, origin: string): { graph: LocalPageGraph; seedUrl: string } {
  const seedUrl = u(origin, "/");
  const indexHtml = `<!doctype html><html><body>
    <a href="/a.html">a</a>
    <a href="/b.html">b</a>
    <a href="/a.html#section">dup</a>
    <a href="https://example.com">ext</a>
  </body></html>`;
  const aHtml = `<!doctype html><html><body><a href="/b.html">b again</a></body></html>`;
  const bHtml = "<!doctype html><html><body><p>b</p></body></html>";
  fs.writeFileSync(path.join(dir, "index.html"), indexHtml, "utf8");
  fs.writeFileSync(path.join(dir, "a.html"), aHtml, "utf8");
  fs.writeFileSync(path.join(dir, "b.html"), bHtml, "utf8");
  return {
    graph: {
      pages: new Map([
        [seedUrl, indexHtml],
        [u(origin, "/a.html"), aHtml],
        [u(origin, "/b.html"), bHtml]
      ])
    },
    seedUrl
  };
}

export function writeBrokenLinkFixture(dir: string, origin: string): { graph: LocalPageGraph; seedUrl: string } {
  const seedUrl = u(origin, "/");
  const indexHtml = `<!doctype html><html><body>
    <a href="/ok.html">ok</a>
    <a href="/missing.html">missing</a>
  </body></html>`;
  const okHtml = "<!doctype html><html><body><p>ok</p></body></html>";
  fs.writeFileSync(path.join(dir, "index.html"), indexHtml, "utf8");
  fs.writeFileSync(path.join(dir, "ok.html"), okHtml, "utf8");
  return {
    graph: {
      pages: new Map([
        [seedUrl, indexHtml],
        [u(origin, "/ok.html"), okHtml]
      ])
    },
    seedUrl
  };
}

export function writeCycleFixture(dir: string, origin: string): { graph: LocalPageGraph; seedUrl: string } {
  const seedUrl = u(origin, "/a.html");
  const a = `<!doctype html><html><body><a href="/b.html">b</a></body></html>`;
  const b = `<!doctype html><html><body><a href="/c.html">c</a></body></html>`;
  const c = `<!doctype html><html><body><a href="/a.html">a</a></body></html>`;
  fs.writeFileSync(path.join(dir, "a.html"), a, "utf8");
  fs.writeFileSync(path.join(dir, "b.html"), b, "utf8");
  fs.writeFileSync(path.join(dir, "c.html"), c, "utf8");
  return {
    graph: {
      pages: new Map([
        [u(origin, "/a.html"), a],
        [u(origin, "/b.html"), b],
        [u(origin, "/c.html"), c]
      ])
    },
    seedUrl
  };
}

/**
 * index links to www host variant; w.html exists. Static server ignores Host header.
 */
export function writeWwwScopeFixture(dir: string, origin: string): { graph: LocalPageGraph; seedUrl: string } {
  const port = new URL(origin).port;
  const wwwOrigin = `http://www.127.0.0.1:${port}`;
  const seedUrl = u(origin, "/");
  const wUrl = normalizeAbsoluteUrl(`${wwwOrigin}/w.html`)!;
  const indexHtml = `<!doctype html><html><body>
    <a href="${wwwOrigin}/w.html">www page</a>
  </body></html>`;
  const wHtml = "<!doctype html><html><body><p>w</p></body></html>";
  fs.writeFileSync(path.join(dir, "index.html"), indexHtml, "utf8");
  fs.writeFileSync(path.join(dir, "w.html"), wHtml, "utf8");
  return {
    graph: {
      pages: new Map([
        [seedUrl, indexHtml],
        [wUrl, wHtml]
      ])
    },
    seedUrl
  };
}

/** Same duplicate graph files as writeDuplicateAndExternalFixture — for worker equivalence script. */
export function writeDupesForScript(dir: string): void {
  const indexHtml = `<!doctype html><html><body>
    <a href="/a.html">a</a>
    <a href="/b.html">b</a>
  </body></html>`;
  fs.writeFileSync(path.join(dir, "index.html"), indexHtml, "utf8");
  fs.writeFileSync(path.join(dir, "a.html"), "<!doctype html><html><body><p>a</p></body></html>", "utf8");
  fs.writeFileSync(path.join(dir, "b.html"), "<!doctype html><html><body><p>b</p></body></html>", "utf8");
}
