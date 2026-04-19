export type FetchClassification = {
  retryable: boolean;
  reason: string;
  httpStatus: number | null;
  contentType: string | null;
  terminalStatus: "FAILED" | "REDIRECT_301" | "FORBIDDEN" | "NOT_FOUND" | "HTTP_TERMINAL" | null;
  backoffMultiplier?: number;
};

export function classifyHttpResponse(
  statusCode: number,
  contentType: string | null,
  _retry429Multiplier: number
): FetchClassification {
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
  if (statusCode >= 500 && statusCode < 600) {
    return {
      retryable: true,
      reason: `retryable_http_${statusCode}`,
      httpStatus: statusCode,
      contentType,
      terminalStatus: "HTTP_TERMINAL"
    };
  }
  if ((statusCode >= 300 && statusCode < 600) || statusCode === 429) {
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
  const code = (err as { code?: string })?.code;
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

  const retryableByCode = code ? retryableCodes.has(code) : false;
  const retryableByMessage = /timeout|timed out|socket|connect|dns|temporar/i.test(message);
  return {
    retryable: retryableByCode || retryableByMessage,
    reason: `request_error${code ? `_${code}` : ""}: ${message}`,
    httpStatus: null,
    contentType: null,
    terminalStatus: "FAILED"
  };
}
