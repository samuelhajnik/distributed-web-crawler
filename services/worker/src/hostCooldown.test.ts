import { describe, expect, it } from "vitest";
import { classifyHttpResponse } from "@crawler/shared";
import { HostCooldown, shouldCooldownForHttpClassification } from "./hostCooldown";

describe("shouldCooldownForHttpClassification", () => {
  it("still applies to retryable 429 responses", () => {
    expect(shouldCooldownForHttpClassification(classifyHttpResponse(429, null))).toBe(true);
  });
});

describe("HostCooldown", () => {
  it("no-ops wait and record when base backoff is disabled", async () => {
    const sleeps: number[] = [];
    const cd = new HostCooldown({
      baseBackoffMs: 0,
      maxBackoffMs: 1000,
      now: () => 1000,
      sleep: async (ms) => {
        sleeps.push(ms);
      }
    });
    await cd.recordNegative("example.com");
    await cd.waitUntilCool("example.com");
    expect(sleeps).toEqual([]);
  });

  it("blocks until cooldown expires", async () => {
    let t = 1000;
    const sleeps: number[] = [];
    const cd = new HostCooldown({
      baseBackoffMs: 100,
      maxBackoffMs: 10_000,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      }
    });
    await cd.recordNegative("example.com");
    await cd.waitUntilCool("example.com");
    expect(sleeps).toEqual([100]);
    expect(t).toBe(1100);
  });

  it("escalates backoff on repeated negatives", async () => {
    let t = 10_000;
    const sleeps: number[] = [];
    const cd = new HostCooldown({
      baseBackoffMs: 100,
      maxBackoffMs: 100_000,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      }
    });
    await cd.recordNegative("h.example");
    await cd.recordNegative("h.example");
    await cd.waitUntilCool("h.example");
    const lastSleep = sleeps[sleeps.length - 1];
    expect(lastSleep).toBeGreaterThanOrEqual(190);
  });

  it("decays strike count on success", async () => {
    let t = 0;
    const sleeps: number[] = [];
    const cd = new HostCooldown({
      baseBackoffMs: 100,
      maxBackoffMs: 10_000,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      }
    });
    await cd.recordNegative("ok.test");
    await cd.recordSuccess("ok.test");
    await cd.recordNegative("ok.test");
    await cd.waitUntilCool("ok.test");
    expect(sleeps).toEqual([100]);
  });
});
