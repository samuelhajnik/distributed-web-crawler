import { load } from "cheerio";
import { normalizeUrl, parseSeedUrl } from "@crawler/shared";

/**
 * Reference crawl over a known set of pages (normalized URL -> HTML).
 * Mirrors documented rules: same link extraction as production (a[href]),
 * {@link normalizeUrl} for scope (intentionally shared with crawler to avoid oracle drift).
 */
export type LocalPageGraph = {
  /** Full normalized URLs that exist and return HTML 200 */
  pages: Map<string, string>;
};

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

export function assertSetsEqual(a: Set<string>, b: Set<string>, label: string): void {
  const onlyA = [...a].filter((x) => !b.has(x)).sort();
  const onlyB = [...b].filter((x) => !a.has(x)).sort();
  if (onlyA.length || onlyB.length) {
    throw new Error(`${label} mismatch onlyA=${JSON.stringify(onlyA)} onlyB=${JSON.stringify(onlyB)}`);
  }
}
