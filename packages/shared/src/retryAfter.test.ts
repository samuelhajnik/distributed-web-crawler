import { describe, expect, it } from "vitest";
import { mergeRetryAfterWithBackoff, parseRetryAfterMs } from "./retryAfter";

describe("parseRetryAfterMs", () => {
  it("parses delay-seconds form", () => {
    expect(parseRetryAfterMs("120", 0)).toBe(120_000);
    expect(parseRetryAfterMs("0", 0)).toBe(0);
  });

  it("parses HTTP-date form as delta from now", () => {
    const now = Date.UTC(2026, 0, 1, 12, 0, 0);
    const header = new Date(now + 45_000).toUTCString();
    expect(parseRetryAfterMs(header, now)).toBe(45_000);
  });

  it("returns null for invalid or past HTTP-date", () => {
    const now = Date.UTC(2026, 0, 1, 12, 0, 0);
    expect(parseRetryAfterMs("not a date", now)).toBe(null);
    expect(parseRetryAfterMs(new Date(now - 60_000).toUTCString(), now)).toBe(null);
  });

  it("returns null for empty or absent values", () => {
    expect(parseRetryAfterMs("", 0)).toBe(null);
    expect(parseRetryAfterMs(null, 0)).toBe(null);
    expect(parseRetryAfterMs(undefined, 0)).toBe(null);
  });

  it("returns null for absurd delay-seconds", () => {
    expect(parseRetryAfterMs(`${86400 * 400}`, 0)).toBe(null);
  });
});

describe("mergeRetryAfterWithBackoff", () => {
  it("uses backoff alone when Retry-After is absent", () => {
    expect(mergeRetryAfterWithBackoff(8000, null, 30_000)).toBe(8000);
    expect(mergeRetryAfterWithBackoff(8000, undefined, 30_000)).toBe(8000);
  });

  it("uses the larger of backoff and Retry-After, capped", () => {
    expect(mergeRetryAfterWithBackoff(2000, 10_000, 30_000)).toBe(10_000);
    expect(mergeRetryAfterWithBackoff(8000, 3000, 30_000)).toBe(8000);
    expect(mergeRetryAfterWithBackoff(1000, 60_000, 30_000)).toBe(30_000);
  });
});
