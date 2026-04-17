import fs from "node:fs";
import path from "node:path";
import { normalizeAbsoluteUrl } from "@crawler/shared";
import type { LocalPageGraph } from "./oracle";

/** Mulberry32 — deterministic from numeric seed */
function rng(seed: number): () => number {
  let t = seed >>> 0;
  return () => {
    t += 0x6d2b79f5;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

function pick<T>(rand: () => number, arr: T[]): T {
  return arr[Math.floor(rand() * arr.length)]!;
}

export type GeneratedGraph = {
  seed: number;
  graph: LocalPageGraph;
  /** normalized seed URL (caller fills port) */
  seedPath: string;
};

/**
 * Builds N local pages under paths `/gen-0.html` … `/gen-${n-1}.html`.
 * `origin` should be like `http://127.0.0.1:PORT` (no trailing slash).
 */
export function generateHtmlGraph(origin: string, seed: number, n: number): GeneratedGraph {
  if (n < 2) {
    throw new Error("n must be >= 2");
  }
  const rand = rng(seed);
  const pages = new Map<string, string>();
  const paths = Array.from({ length: n }, (_, i) => `/gen-${i}.html`);
  const urls = paths.map((p) => normalizeAbsoluteUrl(`${origin.replace(/\/$/, "")}${p}`)!);

  const linksOut: string[][] = paths.map(() => []);

  for (let i = 0; i < n; i++) {
    const k = 1 + Math.floor(rand() * 3);
    for (let j = 0; j < k; j++) {
      const t = Math.floor(rand() * n);
      const roll = rand();
      if (roll < 0.12) {
        linksOut[i]!.push(`${origin}${paths[t]}#frag-${j}`);
      } else if (roll < 0.18) {
        linksOut[i]!.push(paths[t]!);
      } else if (roll < 0.22) {
        linksOut[i]!.push("mailto:x@y.com");
      } else if (roll < 0.26) {
        linksOut[i]!.push("tel:+1");
      } else if (roll < 0.3) {
        linksOut[i]!.push("javascript:void(0)");
      } else if (roll < 0.34) {
        linksOut[i]!.push("https://example.com/ext");
      } else if (roll < 0.38 && n >= 3) {
        const miss = (i + 17) % n;
        linksOut[i]!.push(
          normalizeAbsoluteUrl(`${origin.replace(/\/$/, "")}/gen-missing-${i}-${miss}.html`)!
        );
      } else if (roll < 0.42) {
        linksOut[i]!.push(paths[i]!);
      } else {
        linksOut[i]!.push(urls[t]!);
      }
    }
  }

  const ensureCycle = () => {
    for (let i = 0; i < n; i++) {
      const nxt = (i + 1) % n;
      if (!linksOut[i]!.includes(urls[nxt]!) && !linksOut[i]!.includes(paths[nxt]!)) {
        linksOut[i]!.push(paths[nxt]!);
      }
    }
  };
  ensureCycle();

  for (let i = 0; i < n; i++) {
    const body = [
      "<!doctype html><html><body>",
      `<title>gen-${i}</title>`,
      ...linksOut[i]!.map((h) => `<a href="${escapeAttr(h)}">link</a>`),
      "</body></html>"
    ].join("\n");
    pages.set(urls[i]!, body);
  }

  const seedUrl = urls[0]!;
  return { seed, graph: { pages }, seedUrl };
}

function escapeAttr(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;");
}

/** Writes each page in `graph` to `dir` using URL pathname as relative file path. */
export function writeGeneratedGraphToDisk(dir: string, graph: LocalPageGraph): void {
  for (const [url, html] of graph.pages.entries()) {
    const pathname = new URL(url).pathname;
    const rel = pathname.startsWith("/") ? pathname.slice(1) : pathname;
    const fp = path.join(dir, rel);
    fs.mkdirSync(path.dirname(fp), { recursive: true });
    fs.writeFileSync(fp, html, "utf8");
  }
}
