import { describe, expect, it } from "vitest";
import { classifyExecutionError, classifyHttpResponse } from "./classification";

describe("classifyHttpResponse", () => {
  it("treats 2xx as success", () => {
    const r = classifyHttpResponse(200, "text/html; charset=utf-8");
    expect(r.reason).toBe("success");
    expect(r.retryable).toBe(false);
  });

  it("treats 5xx as retryable transient server outcomes", () => {
    const r = classifyHttpResponse(503, null);
    expect(r.retryable).toBe(true);
    expect(r.reason).toBe("retryable_http_503");
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it("treats most 4xx as terminal", () => {
    const r = classifyHttpResponse(401, null);
    expect(r.retryable).toBe(false);
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it("maps 403 to terminal forbidden", () => {
    const r = classifyHttpResponse(403, null);
    expect(r.retryable).toBe(false);
    expect(r.reason).toBe("terminal_http_403");
    expect(r.terminalStatus).toBe("FORBIDDEN");
  });

  it("maps 404 to terminal not_found", () => {
    const r = classifyHttpResponse(404, null);
    expect(r.retryable).toBe(false);
    expect(r.terminalStatus).toBe("NOT_FOUND");
  });

  it("maps 410 to terminal http outcome without retry", () => {
    const r = classifyHttpResponse(410, null);
    expect(r.retryable).toBe(false);
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it("keeps 500 retryable but terminal bucket for exhausted retries", () => {
    const r = classifyHttpResponse(500, null);
    expect(r.retryable).toBe(true);
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it("maps 301 to redirect status", () => {
    const r = classifyHttpResponse(301, null);
    expect(r.retryable).toBe(false);
    expect(r.terminalStatus).toBe("REDIRECT_301");
  });

  it("maps 429 to retryable rate limit with HTTP_TERMINAL bucket for exhausted retries", () => {
    const r = classifyHttpResponse(429, null);
    expect(r.retryable).toBe(true);
    expect(r.reason).toBe("retryable_http_429");
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it.each([
    [408, "retryable_http_408"],
    [421, "retryable_http_421"],
    [425, "retryable_http_425"]
  ] as const)("treats %i as retryable transient HTTP with HTTP_TERMINAL after exhausted retries", (code, reason) => {
    const r = classifyHttpResponse(code, null);
    expect(r.retryable).toBe(true);
    expect(r.reason).toBe(reason);
    expect(r.httpStatus).toBe(code);
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
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

  it("classifies AbortController timeout abort (DOM shape) as retryable", () => {
    const err = Object.assign(new Error("This operation was aborted"), {
      name: "AbortError",
      code: 20 as const
    });
    const r = classifyExecutionError(err);
    expect(r.retryable).toBe(true);
    expect(r.reason).toBe("request_error_20: This operation was aborted");
    expect(r.terminalStatus).toBe("FAILED");
  });

  it("classifies aborted message without code as retryable", () => {
    const r = classifyExecutionError(new Error("The user aborted a request."));
    expect(r.retryable).toBe(true);
  });
});
