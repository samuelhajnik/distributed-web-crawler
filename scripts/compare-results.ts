#!/usr/bin/env npx tsx
/**
 * Compare two JSON exports from GET /crawl-runs/:id/export?format=json
 * Usage: npx tsx scripts/compare-results.ts run-a.json run-b.json
 */
import { readFileSync } from "node:fs";

type ExportFile = {
  urls?: Array<{ normalized_url?: string }>;
};

function normalizedSet(path: string): Set<string> {
  const raw = readFileSync(path, "utf8");
  const data = JSON.parse(raw) as ExportFile;
  const urls = data.urls ?? [];
  const set = new Set<string>();
  for (const row of urls) {
    if (row.normalized_url) {
      set.add(row.normalized_url);
    }
  }
  return set;
}

function diff(a: Set<string>, b: Set<string>): string[] {
  const out: string[] = [];
  for (const x of a) {
    if (!b.has(x)) {
      out.push(x);
    }
  }
  return out.sort();
}

const fileA = process.argv[2];
const fileB = process.argv[3];
if (!fileA || !fileB) {
  process.stderr.write("usage: npx tsx scripts/compare-results.ts <export-a.json> <export-b.json>\n");
  process.exit(2);
}

const setA = normalizedSet(fileA);
const setB = normalizedSet(fileB);

const onlyA = diff(setA, setB);
const onlyB = diff(setB, setA);
const identical = onlyA.length === 0 && onlyB.length === 0;

process.stdout.write(`run A: ${fileA} unique_urls=${setA.size}\n`);
process.stdout.write(`run B: ${fileB} unique_urls=${setB.size}\n`);
process.stdout.write(`missing in B (relative to A): ${onlyA.length}\n`);
if (onlyA.length) {
  process.stdout.write(onlyA.slice(0, 20).join("\n") + (onlyA.length > 20 ? "\n... (truncated)\n" : "\n"));
}
process.stdout.write(`extra in B (relative to A): ${onlyB.length}\n`);
if (onlyB.length) {
  process.stdout.write(onlyB.slice(0, 20).join("\n") + (onlyB.length > 20 ? "\n... (truncated)\n" : "\n"));
}
process.stdout.write(`identical_sets=${identical}\n`);

if (!identical) {
  process.exit(1);
}
