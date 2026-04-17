export type FetchClassification = {
  retryable: boolean;
  reason: string;
  httpStatus: number | null;
  contentType: string | null;
  backoffMultiplier?: number;
};

export function classifyHttpResponse(
  statusCode: number,
  contentType: string | null,
  retry429Multiplier: number
): FetchClassification {
  if (statusCode >= 500) {
    return { retryable: true, reason: `retryable_http_${statusCode}`, httpStatus: statusCode, contentType };
  }
  if (statusCode === 429) {
    return {
      retryable: true,
      reason: "retryable_http_429",
      httpStatus: statusCode,
      contentType,
      backoffMultiplier: Math.max(1, retry429Multiplier)
    };
  }
  if (statusCode >= 400) {
    return { retryable: false, reason: `terminal_http_${statusCode}`, httpStatus: statusCode, contentType };
  }
  if (statusCode >= 200 && statusCode < 300) {
    return { retryable: false, reason: "success", httpStatus: statusCode, contentType };
  }
  return { retryable: false, reason: `unexpected_http_${statusCode}`, httpStatus: statusCode, contentType };
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
    contentType: null
  };
}
