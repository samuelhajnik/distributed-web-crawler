/**
 * Parse HTTP `Retry-After` (RFC 7231): delay-seconds integer and/or HTTP-date.
 * Returns delay in milliseconds from `nowMs` until the suggested retry time, or null if unusable.
 */
export function parseRetryAfterMs(value: string | null | undefined, nowMs: number): number | null {
  if (value == null) {
    return null;
  }
  const trimmed = String(value).trim();
  if (trimmed === "") {
    return null;
  }

  if (/^\d+$/.test(trimmed)) {
    const sec = parseInt(trimmed, 10);
    if (!Number.isFinite(sec) || sec < 0 || sec > 86400 * 365) {
      return null;
    }
    return sec * 1000;
  }

  const whenMs = Date.parse(trimmed);
  if (Number.isNaN(whenMs)) {
    return null;
  }
  const delta = whenMs - nowMs;
  if (delta < 0 || delta > 86400000 * 365) {
    return null;
  }
  return delta;
}

/**
 * Prefer `Retry-After` when valid: delay is at least the backoff-derived base, capped at `maxDelayMs`.
 * When `retryAfterMs` is absent or invalid, returns `baseDelayMs`.
 */
export function mergeRetryAfterWithBackoff(
  baseDelayMs: number,
  retryAfterMs: number | null | undefined,
  maxDelayMs: number
): number {
  if (retryAfterMs == null || !Number.isFinite(retryAfterMs) || retryAfterMs <= 0) {
    return baseDelayMs;
  }
  return Math.min(maxDelayMs, Math.max(baseDelayMs, retryAfterMs));
}
