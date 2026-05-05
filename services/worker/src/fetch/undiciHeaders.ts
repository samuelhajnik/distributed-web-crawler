/** Undici `request()` uses Node header objects; normalize `Retry-After` for parsing. */
export function retryAfterFromUndiciHeaders(
  headers: Record<string, string | string[] | undefined>
): string | null {
  const raw = headers["retry-after"] ?? headers["Retry-After"];
  if (raw == null) {
    return null;
  }
  return Array.isArray(raw) ? (raw[0] ?? null) : raw;
}
