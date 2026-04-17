import { describe, expect, it } from "vitest";
import { buildCrawlBulkJobs } from "./reconciliation";

describe("buildCrawlBulkJobs", () => {
  it("builds one job per url id with stable payload shape", () => {
    const jobs = buildCrawlBulkJobs(7, [10, 11]);
    expect(jobs).toHaveLength(2);
    expect(jobs[0]).toMatchObject({
      name: "crawl-url",
      data: { crawlRunId: 7, urlId: 10 },
      opts: { removeOnComplete: 2000, removeOnFail: 2000 }
    });
    expect(jobs[1].data).toEqual({ crawlRunId: 7, urlId: 11 });
  });

  it("returns empty array for no ids", () => {
    expect(buildCrawlBulkJobs(1, [])).toEqual([]);
  });
});
