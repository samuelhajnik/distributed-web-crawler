import { load } from "cheerio";
import { normalizeUrl, parseSeedUrl } from "@crawler/shared";
import type { LocalPageGraph } from "./generated-graph-types";

/**
 * Reference crawl over a known set of pages (normalized URL -> HTML).
 * Mirrors documented rules: same link extraction as production (a[href]),
 * {@link normalizeUrl} for scope (intentionally shared with crawler to avoid oracle drift).
 */

export type OracleResult = {
  allUrls: Set<string>;
  visited: Set<string>;
  notFound: Set<string>;
  failed: Set<string>;
  totals: { discovered: number; visited: number; notFound: number; failed: number };
};

function extractHrefs(html: string): string[] {
  const $ = load(html);
  const out: string[] = [];
  $("a[href]").each((_i, el) => {
    const h = $(el).attr("href");
    if (h) {
      out.push(h.trim());
    }
  });
  return out;
}

export function simulateLocalCrawl(seedUrl: string, graph: LocalPageGraph): OracleResult {
  const parsed = parseSeedUrl(seedUrl);
  if (!parsed) {
    throw new Error(`oracle: invalid seed ${seedUrl}`);
  }
  const seedNorm = parsed.normalized;
  const allowed = parsed.allowedHosts;

  const allUrls = new Set<string>([seedNorm]);
  const visited = new Set<string>();
  const notFound = new Set<string>();
  const failed = new Set<string>();
  const queue: string[] = [seedNorm];

  while (queue.length > 0) {
    const u = queue.shift()!;
    const html = graph.pages.get(u);
    if (html === undefined) {
      notFound.add(u);
      continue;
    }
    visited.add(u);
    for (const raw of extractHrefs(html)) {
      const n = normalizeUrl(u, raw, allowed);
      if (!n) {
        continue;
      }
      if (!allUrls.has(n)) {
        allUrls.add(n);
        queue.push(n);
      }
    }
  }

  return {
    allUrls,
    visited,
    notFound,
    failed,
    totals: {
      discovered: allUrls.size,
      visited: visited.size,
      notFound: notFound.size,
      failed: failed.size
    }
  };
}
