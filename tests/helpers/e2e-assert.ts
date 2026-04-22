import { expect } from "vitest";
import type { ExportResponse, SummaryResponse } from "./crawler-client";
import type { OracleResult } from "./oracle";

export type CrawlExpectation = {
  discoveredUrls: Set<string>;
  visitedUrls: Set<string>;
  notFoundUrls: Set<string>;
  failedUrls: Set<string>;
  summary: { discovered: number; visited: number; notFound: number; failed: number };
};

type AssertOptions = {
  seedUrl?: string;
};

export function assertExportMatchesExpected(
  summary: SummaryResponse,
  exp: ExportResponse,
  expected: CrawlExpectation,
  options: AssertOptions = {}
): void {
  if (summary.status !== "COMPLETED") {
    throw new Error(`expected COMPLETED got ${summary.status}`);
  }
  if (summary.totals.queued !== 0 || summary.totals.in_progress !== 0) {
    throw new Error(`frontier not empty: ${JSON.stringify(summary.totals)}`);
  }

  const st = (s: string | undefined) => String(s ?? "").trim().toUpperCase();
  const visited = new Set(
    exp.urls.filter((u) => st(u.status) === "VISITED" && u.normalized_url).map((u) => u.normalized_url)
  );
  const failed = new Set(
    exp.urls.filter((u) => st(u.status) === "FAILED" && u.normalized_url).map((u) => u.normalized_url)
  );
  const notFound = new Set(
    exp.urls.filter((u) => st(u.status) === "NOT_FOUND" && u.normalized_url).map((u) => u.normalized_url)
  );
  const all = new Set(exp.urls.map((u) => u.normalized_url));
  const seedUrl = options.seedUrl;

  assertSetsEqual(visited, expected.visitedUrls, "VISITED");
  assertSetsEqual(notFound, expected.notFoundUrls, "NOT_FOUND");
  assertSetsEqual(failed, expected.failedUrls, "FAILED");
  assertSetsEqual(all, expected.discoveredUrls, "all rows");
  assertDisjoint(visited, notFound, "VISITED", "NOT_FOUND");
  assertSubset(visited, all, "VISITED", "all rows");
  assertSubset(notFound, all, "NOT_FOUND", "all rows");
  assertSubset(failed, all, "FAILED", "all rows");
  expect(all.size).toBe(exp.urls.length); // no duplicate discovered URLs
  if (seedUrl) {
    expect(all.has(seedUrl)).toBe(true);
  }

  expect(exp.urls.length).toBe(summary.totals.discovered);
  expect(summary.totals.discovered).toBe(expected.summary.discovered);
  expect(summary.totals.visited).toBe(expected.summary.visited);
  expect(summary.totals.redirect_301).toBe(0);
  expect(summary.totals.forbidden).toBe(0);
  expect(summary.totals.not_found).toBe(expected.summary.notFound);
  expect(summary.totals.http_terminal).toBe(0);
  expect(summary.totals.failed).toBe(expected.summary.failed);
  expect(summary.totals.discovered).toBe(summary.totals.visited + summary.totals.not_found + summary.totals.failed);
}

export function assertExportMatchesOracle(
  summary: SummaryResponse,
  exp: ExportResponse,
  oracle: OracleResult,
  options: AssertOptions = {}
): void {
  const expected: CrawlExpectation = {
    discoveredUrls: oracle.allUrls,
    visitedUrls: oracle.visited,
    notFoundUrls: oracle.notFound,
    failedUrls: oracle.failed,
    summary: {
      discovered: oracle.totals.discovered,
      visited: oracle.totals.visited,
      notFound: oracle.totals.notFound,
      failed: oracle.totals.failed
    }
  };
  assertExportMatchesExpected(summary, exp, expected, options);
}

function assertSetsEqual(a: Set<string>, b: Set<string>, label: string): void {
  const onlyA = [...a].filter((x) => !b.has(x)).sort();
  const onlyB = [...b].filter((x) => !a.has(x)).sort();
  if (onlyA.length || onlyB.length) {
    throw new Error(`${label} mismatch onlyA=${JSON.stringify(onlyA)} onlyB=${JSON.stringify(onlyB)}`);
  }
}

function assertSubset(a: Set<string>, b: Set<string>, aLabel: string, bLabel: string): void {
  const onlyA = [...a].filter((x) => !b.has(x)).sort();
  if (onlyA.length) {
    throw new Error(`${aLabel} is not subset of ${bLabel}: ${JSON.stringify(onlyA)}`);
  }
}

function assertDisjoint(a: Set<string>, b: Set<string>, aLabel: string, bLabel: string): void {
  const overlap = [...a].filter((x) => b.has(x)).sort();
  if (overlap.length) {
    throw new Error(`${aLabel} and ${bLabel} overlap: ${JSON.stringify(overlap)}`);
  }
}
