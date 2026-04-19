import { describe, expect, it } from "vitest";
import { classifyExecutionError, classifyHttpResponse } from "./classification";

describe("classifyHttpResponse", () => {
  it("treats 2xx as success", () => {
    const r = classifyHttpResponse(200, "text/html; charset=utf-8", 4);
    expect(r.reason).toBe("success");
    expect(r.retryable).toBe(false);
  });

  it("treats 5xx as retryable transient server outcomes", () => {
    const r = classifyHttpResponse(503, null, 4);
    expect(r.retryable).toBe(true);
    expect(r.reason).toBe("retryable_http_503");
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it("treats most 4xx as terminal", () => {
    const r = classifyHttpResponse(401, null, 4);
    expect(r.retryable).toBe(false);
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it("maps 403 to terminal forbidden", () => {
    const r = classifyHttpResponse(403, null, 4);
    expect(r.retryable).toBe(false);
    expect(r.reason).toBe("terminal_http_403");
    expect(r.terminalStatus).toBe("FORBIDDEN");
  });

  it("maps 404 to terminal not_found", () => {
    const r = classifyHttpResponse(404, null, 4);
    expect(r.retryable).toBe(false);
    expect(r.terminalStatus).toBe("NOT_FOUND");
  });

  it("maps 410 to terminal http outcome without retry", () => {
    const r = classifyHttpResponse(410, null, 4);
    expect(r.retryable).toBe(false);
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it("keeps 500 retryable but terminal bucket for exhausted retries", () => {
    const r = classifyHttpResponse(500, null, 4);
    expect(r.retryable).toBe(true);
    expect(r.terminalStatus).toBe("HTTP_TERMINAL");
  });

  it("maps 301 to redirect status", () => {
    const r = classifyHttpResponse(301, null, 4);
    expect(r.retryable).toBe(false);
    expect(r.terminalStatus).toBe("REDIRECT_301");
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
