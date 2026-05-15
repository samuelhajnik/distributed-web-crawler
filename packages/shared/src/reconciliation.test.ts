import { describe, expect, it } from "vitest";
import type { Queue } from "bullmq";
import type { CrawlJobPayload } from "./types";
import {
  buildRetryWakeSignalJob,
  buildRunSignalJobs,
  retryWakeSignalJobId,
  runSignalJobId,
  runSignalJobRetention,
  topUpRunSignals
} from "./reconciliation";

describe("buildRunSignalJobs", () => {
  it("builds bounded slot jobs with deterministic jobId and immediate removal", () => {
    const jobs = buildRunSignalJobs(7, 3);
    expect(jobs).toHaveLength(3);
    expect(jobs[0]).toMatchObject({
      name: "crawl-run-signal",
      data: { crawlRunId: 7, slot: 0 },
      opts: expect.objectContaining({
        jobId: runSignalJobId(7, 0),
        ...runSignalJobRetention
      })
    });
    expect(jobs[0].opts.removeOnComplete).toBe(true);
    expect(jobs[0].opts.removeOnFail).toBe(true);
    expect(jobs[2].data).toEqual({ crawlRunId: 7, slot: 2 });
  });

  it("returns empty array for zero signal count", () => {
    expect(buildRunSignalJobs(1, 0)).toEqual([]);
  });
});

describe("buildRetryWakeSignalJob", () => {
  it("uses time-bucketed jobId separate from slot indices", () => {
    const eligibleAt = new Date("2026-05-15T12:00:00.000Z");
    const job = buildRetryWakeSignalJob(5, 5000, eligibleAt);
    expect(job.data).toEqual({ crawlRunId: 5, slot: -1 });
    expect(job.opts.jobId).toBe(retryWakeSignalJobId(5, Math.floor(eligibleAt.getTime() / 1000)));
    expect(job.opts.delay).toBe(5000);
    expect(job.opts.removeOnComplete).toBe(true);
    expect(job.opts.removeOnFail).toBe(true);
  });
});

describe("topUpRunSignals", () => {
  function createSlotTrackingQueue(): {
    queue: Queue<CrawlJobPayload>;
    activeJobIds: () => string[];
    removeJob: (jobId: string) => void;
  } {
    const active = new Map<string, true>();
    const queue = {
      async add(_name: string, _data: CrawlJobPayload, opts?: { jobId?: string }) {
        const jobId = opts?.jobId;
        if (!jobId) {
          throw new Error("missing jobId");
        }
        if (active.has(jobId)) {
          throw new Error(`Job ${jobId} already exists`);
        }
        active.set(jobId, true);
        return { id: jobId };
      }
    } as unknown as Queue<CrawlJobPayload>;

    return {
      queue,
      activeJobIds: () => [...active.keys()],
      removeJob: (jobId: string) => {
        active.delete(jobId);
      }
    };
  }

  it("ignores duplicate adds while a slot job is still active", async () => {
    const { queue } = createSlotTrackingQueue();
    expect(await topUpRunSignals(queue, 9, 2)).toBe(2);
    expect(await topUpRunSignals(queue, 9, 2)).toBe(0);
  });

  it("can re-enqueue the same slot jobId after the prior job is removed", async () => {
    const { queue, activeJobIds, removeJob } = createSlotTrackingQueue();
    const crawlRunId = 11;
    const signalCount = 2;

    expect(await topUpRunSignals(queue, crawlRunId, signalCount)).toBe(signalCount);
    expect(activeJobIds()).toHaveLength(signalCount);

    for (const slot of [0, 1]) {
      removeJob(runSignalJobId(crawlRunId, slot));
    }
    expect(activeJobIds()).toHaveLength(0);

    expect(await topUpRunSignals(queue, crawlRunId, signalCount)).toBe(signalCount);
    expect(activeJobIds()).toHaveLength(signalCount);
  });
});
