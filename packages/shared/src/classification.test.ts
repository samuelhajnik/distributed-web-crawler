import { describe, expect, it } from "vitest";
import { classifyExecutionError, classifyHttpResponse } from "./classification";

describe("classifyHttpResponse", () => {
  it("treats 2xx as success", () => {
    const r = classifyHttpResponse(200, "text/html; charset=utf-8", 4);
    expect(r.reason).toBe("success");
    expect(r.retryable).toBe(false);
  });

  it("treats 5xx as retryable", () => {
    const r = classifyHttpResponse(503, null, 4);
    expect(r.retryable).toBe(true);
    expect(r.reason).toContain("503");
  });

  it("treats most 4xx as terminal", () => {
    const r = classifyHttpResponse(404, null, 4);
    expect(r.retryable).toBe(false);
  });

  it("treats 429 as retryable with multiplier", () => {
    const r = classifyHttpResponse(429, null, 4);
    expect(r.retryable).toBe(true);
    expect(r.backoffMultiplier).toBe(4);
  });
});

describe("classifyExecutionError", () => {
  it("classifies timeout-like messages as retryable", () => {
    const r = classifyExecutionError(new Error("connect ETIMEDOUT"));
    expect(r.retryable).toBe(true);
  });

  it("classifies unknown errors as non-retryable by default", () => {
    const r = classifyExecutionError(new Error("boom"));
    expect(r.retryable).toBe(false);
  });
});
