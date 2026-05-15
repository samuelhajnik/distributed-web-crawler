import type { JobsOptions, Queue } from "bullmq";
import type { CrawlJobPayload } from "./types";
import { readDispatchSignalsPerRun } from "./dispatchConfig";

export type CrawlBulkJob = {
  name: string;
  data: CrawlJobPayload;
  opts: JobsOptions;
};

/** Completed/failed signals must leave Redis so deterministic slot jobIds can be re-enqueued. */
export const runSignalJobRetention: Pick<JobsOptions, "removeOnComplete" | "removeOnFail"> = {
  removeOnComplete: true,
  removeOnFail: true
};

export const RUN_SIGNAL_JOB_NAME = "crawl-run-signal";

export function runSignalJobId(crawlRunId: number, slot: number): string {
  return `${RUN_SIGNAL_JOB_NAME}:${crawlRunId}:${slot}`;
}

export function retryWakeSignalJobId(crawlRunId: number, eligibleAtEpochSec: number): string {
  return `${RUN_SIGNAL_JOB_NAME}:${crawlRunId}:retry:${eligibleAtEpochSec}`;
}

/** Pure builder for bounded run-level BullMQ dispatch signals (one signal = one claim opportunity). */
export function buildRunSignalJobs(
  crawlRunId: number,
  signalCount = readDispatchSignalsPerRun()
): CrawlBulkJob[] {
  const n = Math.max(0, Math.floor(signalCount));
  const jobs: CrawlBulkJob[] = [];
  for (let slot = 0; slot < n; slot++) {
    jobs.push({
      name: RUN_SIGNAL_JOB_NAME,
      data: { crawlRunId, slot },
      opts: {
        ...runSignalJobRetention,
        jobId: runSignalJobId(crawlRunId, slot)
      }
    });
  }
  return jobs;
}

export function buildRetryWakeSignalJob(
  crawlRunId: number,
  delayMs: number,
  eligibleAt: Date
): CrawlBulkJob {
  const eligibleAtEpochSec = Math.floor(eligibleAt.getTime() / 1000);
  return {
    name: RUN_SIGNAL_JOB_NAME,
    data: { crawlRunId, slot: -1 },
    opts: {
      ...runSignalJobRetention,
      jobId: retryWakeSignalJobId(crawlRunId, eligibleAtEpochSec),
      delay: Math.max(0, Math.floor(delayMs))
    }
  };
}

export function isDuplicateJobIdError(err: unknown): boolean {
  const msg = String((err as Error)?.message ?? err ?? "");
  return msg.includes("JobId") || msg.includes("jobId") || msg.includes("already exists");
}

/** Enqueue slots 0..N-1; duplicate jobId while active is ignored (idempotent top-up). */
export async function topUpRunSignals(
  queue: Queue<CrawlJobPayload>,
  crawlRunId: number,
  signalCount = readDispatchSignalsPerRun()
): Promise<number> {
  const jobs = buildRunSignalJobs(crawlRunId, signalCount);
  let added = 0;
  for (const job of jobs) {
    try {
      await queue.add(job.name, job.data, job.opts);
      added++;
    } catch (err) {
      if (!isDuplicateJobIdError(err)) {
        throw err;
      }
    }
  }
  return added;
}
