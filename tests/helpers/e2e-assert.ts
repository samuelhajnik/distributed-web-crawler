import { expect } from "vitest";
import type { ExportResponse, SummaryResponse } from "./crawler-client";
import type { OracleResult } from "./oracle";
import { assertSetsEqual } from "./oracle";

export function assertExportMatchesOracle(summary: SummaryResponse, exp: ExportResponse, oracle: OracleResult): void {
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

  assertSetsEqual(visited, oracle.visited, "VISITED");
  assertSetsEqual(notFound, oracle.notFound, "NOT_FOUND");
  assertSetsEqual(failed, oracle.failed, "FAILED");
  assertSetsEqual(all, oracle.allUrls, "all rows");

  expect(exp.urls.length).toBe(summary.totals.discovered);
  expect(summary.totals.discovered).toBe(oracle.totals.discovered);
  expect(summary.totals.visited).toBe(oracle.totals.visited);
  expect(summary.totals.redirect_301).toBe(0);
  expect(summary.totals.forbidden).toBe(0);
  expect(summary.totals.not_found).toBe(oracle.totals.notFound);
  expect(summary.totals.http_terminal).toBe(0);
  expect(summary.totals.failed).toBe(oracle.totals.failed);
}
