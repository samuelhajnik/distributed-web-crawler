import { describe, expect, it } from "vitest";
import { HostPacer } from "./hostPacer";

describe("HostPacer", () => {
  it("skips pacing when min gap and jitter are both zero", async () => {
    const sleeps: number[] = [];
    const pacer = new HostPacer({
      minGapMs: 0,
      jitterMaxMs: 0,
      now: () => 1000,
      sleep: async (ms) => {
        sleeps.push(ms);
      },
      random: () => 0
    });
    await pacer.waitBeforeOutboundFetch("example.com");
    await pacer.waitBeforeOutboundFetch("example.com");
    expect(sleeps).toEqual([]);
  });

  it("does not delay the first paced request", async () => {
    let t = 1000;
    const sleeps: number[] = [];
    const pacer = new HostPacer({
      minGapMs: 100,
      jitterMaxMs: 0,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      },
      random: () => 0
    });
    await pacer.waitBeforeOutboundFetch("example.com");
    expect(sleeps).toEqual([]);
  });

  it("spaces same-host requests by at least minGapMs", async () => {
    let t = 1000;
    const sleeps: number[] = [];
    const pacer = new HostPacer({
      minGapMs: 100,
      jitterMaxMs: 0,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      },
      random: () => 0
    });
    await pacer.waitBeforeOutboundFetch("example.com");
    await pacer.waitBeforeOutboundFetch("example.com");
    expect(sleeps).toEqual([100]);
  });

  it("adds jitter on top of min gap when configured", async () => {
    let t = 1000;
    const sleeps: number[] = [];
    const pacer = new HostPacer({
      minGapMs: 100,
      jitterMaxMs: 50,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      },
      random: () => 0.999999
    });
    await pacer.waitBeforeOutboundFetch("example.com");
    await pacer.waitBeforeOutboundFetch("example.com");
    expect(sleeps).toEqual([150]);
  });

  it("does not serialize unrelated hosts", async () => {
    let t = 1000;
    const sleeps: number[] = [];
    const pacer = new HostPacer({
      minGapMs: 100,
      jitterMaxMs: 0,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      },
      random: () => 0
    });
    await Promise.all([
      pacer.waitBeforeOutboundFetch("a.example.com"),
      pacer.waitBeforeOutboundFetch("b.example.com")
    ]);
    expect(sleeps).toEqual([]);
  });

  it("serializes concurrent same-host waiters so they do not burst together", async () => {
    let t = 1000;
    const sleeps: number[] = [];
    const pacer = new HostPacer({
      minGapMs: 40,
      jitterMaxMs: 0,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      },
      random: () => 0
    });
    await Promise.all([
      pacer.waitBeforeOutboundFetch("example.com"),
      pacer.waitBeforeOutboundFetch("example.com")
    ]);
    expect(sleeps.sort((a, b) => a - b)).toEqual([40]);
  });

  it("caps jitter by min gap so extras do not stack past one base gap worth", async () => {
    let t = 1000;
    const sleeps: number[] = [];
    const pacer = new HostPacer({
      minGapMs: 40,
      jitterMaxMs: 120,
      now: () => t,
      sleep: async (ms) => {
        sleeps.push(ms);
        t += ms;
      },
      random: () => 0.999999
    });
    await pacer.waitBeforeOutboundFetch("example.com");
    await pacer.waitBeforeOutboundFetch("example.com");
    expect(sleeps).toEqual([80]);
  });
});
