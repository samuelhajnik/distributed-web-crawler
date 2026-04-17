import fs from "node:fs";
import { describe, expect, it } from "vitest";

type ExportFile = {
  urls?: Array<{ normalized_url?: string; status?: string }>;
};

function loadExport(path: string): ExportFile {
  return JSON.parse(fs.readFileSync(path, "utf8")) as ExportFile;
}

function sets(data: ExportFile): {
  all: Set<string>;
  visited: Set<string>;
  failed: Set<string>;
} {
  const all = new Set<string>();
  const visited = new Set<string>();
  const failed = new Set<string>();
  for (const row of data.urls ?? []) {
    const u = row.normalized_url;
    if (!u) {
      continue;
    }
    all.add(u);
    if (row.status === "VISITED") {
      visited.add(u);
    }
    if (row.status === "FAILED") {
      failed.add(u);
    }
  }
  return { all, visited, failed };
}

describe("E2E worker equivalence (compare saved exports)", () => {
  it.skipIf(!process.env.E2E_EXPORT_A || !process.env.E2E_EXPORT_B)(
    "compares two JSON exports (set E2E_EXPORT_A and E2E_EXPORT_B)",
    () => {
      const a = loadExport(process.env.E2E_EXPORT_A!);
      const b = loadExport(process.env.E2E_EXPORT_B!);
      const sa = sets(a);
      const sb = sets(b);
      const diff = (x: Set<string>, y: Set<string>) => [...x].filter((e) => !y.has(e)).sort();
      expect(diff(sa.all, sb.all)).toEqual([]);
      expect(diff(sa.visited, sb.visited)).toEqual([]);
      expect(diff(sa.failed, sb.failed)).toEqual([]);
    }
  );
});
