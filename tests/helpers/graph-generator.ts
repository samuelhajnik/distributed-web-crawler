import fs from "node:fs";
import path from "node:path";
import { normalizeAbsoluteUrl, normalizeUrl, parseSeedUrl } from "@crawler/shared";
import type { LocalPageGraph } from "./generated-graph-types";

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

export type GeneratedGraph = {
  seed: number;
  pageCount: number;
  graph: LocalPageGraph;
  /** normalized seed URL (caller fills origin/port) */
  seedUrl: string;
  model: GeneratedGraphModel;
  expected: GeneratedGraphExpected;
};

export type GeneratedLinkCategory =
  | "internal_absolute"
  | "internal_relative"
  | "internal_fragmented"
  | "internal_missing"
  | "internal_self"
  | "ignored_mailto"
  | "ignored_tel"
  | "ignored_javascript"
  | "external_out_of_scope";

export type GeneratedLinkSpec = {
  rawHref: string;
  category: GeneratedLinkCategory;
  targetUrl?: string;
  targetPageExists: boolean;
};

export type GeneratedPageModel = {
  index: number;
  path: string;
  url: string;
  outgoing: GeneratedLinkSpec[];
};

export type GeneratedGraphModel = {
  origin: string;
  pages: GeneratedPageModel[];
};

export type GeneratedGraphExpected = {
  discoveredUrls: Set<string>;
  visitedUrls: Set<string>;
  notFoundUrls: Set<string>;
  failedUrls: Set<string>;
  summary: { discovered: number; visited: number; notFound: number; failed: number };
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
  const htmlPages = new Map<string, string>();
  const paths = Array.from({ length: n }, (_, i) => `/gen-${i}.html`);
  const urls = paths.map((p) => normalizeAbsoluteUrl(`${origin.replace(/\/$/, "")}${p}`)!);
  const specsByPage: GeneratedLinkSpec[][] = paths.map(() => []);
  const add = (fromPage: number, spec: GeneratedLinkSpec): void => {
    specsByPage[fromPage]!.push(spec);
  };

  for (let i = 0; i < n; i++) {
    const k = 1 + Math.floor(rand() * 3);
    for (let j = 0; j < k; j++) {
      const t = Math.floor(rand() * n);
      const roll = rand();
      if (roll < 0.12) {
        add(i, {
          rawHref: `${origin}${paths[t]}#frag-${j}`,
          category: "internal_fragmented",
          targetUrl: urls[t]!,
          targetPageExists: true
        });
      } else if (roll < 0.18) {
        add(i, {
          rawHref: paths[t]!,
          category: "internal_relative",
          targetUrl: urls[t]!,
          targetPageExists: true
        });
      } else if (roll < 0.22) {
        add(i, {
          rawHref: "mailto:x@y.com",
          category: "ignored_mailto",
          targetPageExists: false
        });
      } else if (roll < 0.26) {
        add(i, {
          rawHref: "tel:+1",
          category: "ignored_tel",
          targetPageExists: false
        });
      } else if (roll < 0.3) {
        add(i, {
          rawHref: "javascript:void(0)",
          category: "ignored_javascript",
          targetPageExists: false
        });
      } else if (roll < 0.34) {
        add(i, {
          rawHref: "https://example.com/ext",
          category: "external_out_of_scope",
          targetPageExists: false
        });
      } else if (roll < 0.38 && n >= 3) {
        const miss = (i + 17) % n;
        const missingUrl = normalizeAbsoluteUrl(`${origin.replace(/\/$/, "")}/gen-missing-${i}-${miss}.html`)!;
        add(i, {
          rawHref: missingUrl,
          category: "internal_missing",
          targetUrl: missingUrl,
          targetPageExists: false
        });
      } else if (roll < 0.42) {
        add(i, {
          rawHref: paths[i]!,
          category: "internal_self",
          targetUrl: urls[i]!,
          targetPageExists: true
        });
      } else {
        add(i, {
          rawHref: urls[t]!,
          category: "internal_absolute",
          targetUrl: urls[t]!,
          targetPageExists: true
        });
      }
    }
  }

  // Mix in deterministic structural overlays for richer graph shapes.
  const hubA = 1 + Math.floor(rand() * Math.max(1, n - 1));
  const hubB = 1 + Math.floor(rand() * Math.max(1, n - 1));
  const chainStart = Math.floor(rand() * n);
  const chainLen = Math.max(3, Math.floor(n / 3));
  const repeatedTarget = Math.floor(rand() * n);

  // Long chain segment (in addition to the ring) keeps deep traversal coverage.
  for (let step = 0; step < chainLen; step++) {
    const from = (chainStart + step) % n;
    const to = (from + 1) % n;
    add(from, {
      rawHref: paths[to]!,
      category: "internal_relative",
      targetUrl: urls[to]!,
      targetPageExists: true
    });
  }

  // Hub edges: several pages point to one or two shared targets.
  for (let i = 0; i < n; i++) {
    if (i % 5 === 0) {
      add(i, {
        rawHref: paths[hubA]!,
        category: "internal_relative",
        targetUrl: urls[hubA]!,
        targetPageExists: true
      });
    }
    if (i % 9 === 4) {
      add(i, {
        rawHref: urls[hubB]!,
        category: "internal_absolute",
        targetUrl: urls[hubB]!,
        targetPageExists: true
      });
    }
  }

  // A handful of dense pages with extra fanout.
  const densePages = Math.max(1, Math.floor(n / 8));
  for (let d = 0; d < densePages; d++) {
    const from = (hubA + d * 7) % n;
    for (let extra = 0; extra < 3; extra++) {
      const to = Math.floor(rand() * n);
      add(from, {
        rawHref: paths[to]!,
        category: "internal_relative",
        targetUrl: urls[to]!,
        targetPageExists: true
      });
    }
  }

  // More repeated references to the same target.
  for (let i = 1; i < n; i += 4) {
    add(i, {
      rawHref: paths[repeatedTarget]!,
      category: "internal_relative",
      targetUrl: urls[repeatedTarget]!,
      targetPageExists: true
    });
    add(i, {
      rawHref: paths[repeatedTarget]!,
      category: "internal_relative",
      targetUrl: urls[repeatedTarget]!,
      targetPageExists: true
    });
  }

  // Ensure missing references and ignored/out-of-scope links always exist.
  for (let i = 2; i < n; i += 6) {
    const missingUrl = normalizeAbsoluteUrl(`${origin.replace(/\/$/, "")}/gen-missing-overlay-${i}.html`)!;
    add(i, {
      rawHref: missingUrl,
      category: "internal_missing",
      targetUrl: missingUrl,
      targetPageExists: false
    });
  }
  add(0, { rawHref: "mailto:x@y.com", category: "ignored_mailto", targetPageExists: false });
  add(0, { rawHref: "https://example.com/ext", category: "external_out_of_scope", targetPageExists: false });

  const ensureCycle = () => {
    for (let i = 0; i < n; i++) {
      const nxt = (i + 1) % n;
      const hasRingEdge = specsByPage[i]!.some(
        (spec) => spec.targetPageExists && spec.targetUrl === urls[nxt]
      );
      if (!hasRingEdge) {
        add(i, {
          rawHref: paths[nxt]!,
          category: "internal_relative",
          targetUrl: urls[nxt]!,
          targetPageExists: true
        });
      }
    }
  };
  ensureCycle();

  const model: GeneratedGraphModel = {
    origin: origin.replace(/\/$/, ""),
    pages: paths.map((p, i) => ({
      index: i,
      path: p,
      url: urls[i]!,
      outgoing: specsByPage[i]!
    }))
  };

  for (let i = 0; i < n; i++) {
    const body = [
      "<!doctype html><html><body>",
      `<title>gen-${i}</title>`,
      ...model.pages[i]!.outgoing.map((spec) => `<a href="${escapeAttr(spec.rawHref)}">link</a>`),
      "</body></html>"
    ].join("\n");
    htmlPages.set(urls[i]!, body);
  }

  const seedUrl = urls[0]!;
  const expected = deriveExpectedFromModel(seedUrl, model);
  return { seed, pageCount: n, graph: { pages: htmlPages }, seedUrl, model, expected };
}

function escapeAttr(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;");
}

function deriveExpectedFromModel(seedUrl: string, model: GeneratedGraphModel): GeneratedGraphExpected {
  const parsed = parseSeedUrl(seedUrl);
  if (!parsed) {
    throw new Error(`generator: invalid seed ${seedUrl}`);
  }
  const pagesByUrl = new Map(model.pages.map((p) => [p.url, p]));
  const discoveredUrls = new Set<string>([parsed.normalized]);
  const visitedUrls = new Set<string>();
  const notFoundUrls = new Set<string>();
  const failedUrls = new Set<string>();
  const queue: string[] = [parsed.normalized];

  while (queue.length > 0) {
    const current = queue.shift()!;
    const page = pagesByUrl.get(current);
    if (!page) {
      notFoundUrls.add(current);
      continue;
    }
    visitedUrls.add(current);
    for (const link of page.outgoing) {
      const normalized = normalizeUrl(current, link.rawHref, parsed.allowedHosts);
      if (!normalized) {
        continue;
      }
      if (!discoveredUrls.has(normalized)) {
        discoveredUrls.add(normalized);
        queue.push(normalized);
      }
    }
  }

  return {
    discoveredUrls,
    visitedUrls,
    notFoundUrls,
    failedUrls,
    summary: {
      discovered: discoveredUrls.size,
      visited: visitedUrls.size,
      notFound: notFoundUrls.size,
      failed: failedUrls.size
    }
  };
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
