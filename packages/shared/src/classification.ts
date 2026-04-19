export type FetchClassification = {
  retryable: boolean;
  reason: string;
  httpStatus: number | null;
  contentType: string | null;
  terminalStatus: "FAILED" | "REDIRECT_301" | "FORBIDDEN" | "NOT_FOUND" | "HTTP_TERMINAL" | null;
  backoffMultiplier?: number;
};

/**
 * Classifies HTTP status codes for a completed response (headers available).
 *
 * **408**, **421**, **425**, **429**: `retryable: true`; URL-level retries apply until **`maxRetries`**
 * is exhausted, then terminal **`HTTP_TERMINAL`** with the same HTTP status. The worker may honor
 * **`Retry-After`** for BullMQ delay when valid (**429** only); otherwise normal backoff applies.
 * Host cooldown may still apply after a **429** response.
 *
 * **5xx**: `retryable: true` until URL-level retries are exhausted, then terminal `HTTP_TERMINAL`.
 */
export function classifyHttpResponse(statusCode: number, contentType: string | null): FetchClassification {
  if (statusCode >= 200 && statusCode < 300) {
    return {
      retryable: false,
      reason: "success",
      httpStatus: statusCode,
      contentType,
      terminalStatus: null
    };
  }
  if (statusCode === 301) {
    return {
      retryable: false,
      reason: "terminal_http_301",
      httpStatus: statusCode,
      contentType,
      terminalStatus: "REDIRECT_301"
    };
  }
  if (statusCode === 403) {
    return {
      retryable: false,
      reason: "terminal_http_403",
      httpStatus: statusCode,
      contentType,
      terminalStatus: "FORBIDDEN"
    };
  }
  if (statusCode === 404) {
    return {
      retryable: false,
      reason: "terminal_http_404",
      httpStatus: statusCode,
      contentType,
      terminalStatus: "NOT_FOUND"
    };
  }
  if (statusCode === 408 || statusCode === 421 || statusCode === 425 || statusCode === 429) {
    return {
      retryable: true,
      reason: `retryable_http_${statusCode}`,
      httpStatus: statusCode,
      contentType,
      terminalStatus: "HTTP_TERMINAL"
    };
  }
  if (statusCode >= 500 && statusCode < 600) {
    return {
      retryable: true,
      reason: `retryable_http_${statusCode}`,
      httpStatus: statusCode,
      contentType,
      terminalStatus: "HTTP_TERMINAL"
    };
  }
  // Remaining 3xx/4xx (retryable subset handled above) → terminal HTTP (5xx handled above).
  if (statusCode >= 300 && statusCode < 600) {
    return {
      retryable: false,
      reason: `terminal_http_${statusCode}`,
      httpStatus: statusCode,
      contentType,
      terminalStatus: "HTTP_TERMINAL"
    };
  }
  return {
    retryable: false,
    reason: `unexpected_http_${statusCode}`,
    httpStatus: statusCode,
    contentType,
    terminalStatus: "FAILED"
  };
}

export function classifyExecutionError(err: unknown): FetchClassification {
  const message = (err as Error)?.message ?? String(err);
  const rawCode = (err as { code?: string | number })?.code;
  const codeForReason =
    rawCode !== undefined && rawCode !== null ? String(rawCode) : "";
  const codeStr = typeof rawCode === "number" ? String(rawCode) : rawCode;

  const retryableCodes = new Set([
    "ECONNRESET",
    "ECONNREFUSED",
    "EAI_AGAIN",
    "ENOTFOUND",
    "ETIMEDOUT",
    "UND_ERR_CONNECT_TIMEOUT",
    "UND_ERR_HEADERS_TIMEOUT",
    "UND_ERR_BODY_TIMEOUT",
    "UND_ERR_SOCKET"
  ]);

  const retryableByCode = typeof codeStr === "string" && retryableCodes.has(codeStr);
  const retryableByMessage = /timeout|timed out|socket|connect|dns|temporar/i.test(message);

  /** Request timeout uses AbortController — same shape as DOM/undici AbortError (e.g. code 20). */
  const errName = typeof (err as Error)?.name === "string" ? (err as Error).name : "";
  const retryableAbortFromTimeout =
    errName === "AbortError" ||
    rawCode === 20 ||
    codeStr === "20" ||
    codeStr === "ABORT_ERR" ||
    /\baborted\b/i.test(message) ||
    /\boperation was aborted\b/i.test(message);

  return {
    retryable: retryableByCode || retryableByMessage || retryableAbortFromTimeout,
    reason: `request_error${codeForReason ? `_${codeForReason}` : ""}: ${message}`,
    httpStatus: null,
    contentType: null,
    terminalStatus: "FAILED"
  };
}
