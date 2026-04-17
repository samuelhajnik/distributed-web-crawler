function stripDefaultPort(u: URL): void {
  if ((u.protocol === "https:" && u.port === "443") || (u.protocol === "http:" && u.port === "80")) {
    u.port = "";
  }
}

/**
 * Normalizes an absolute document URL (seed): http(s) only, clears `#fragment`,
 * strips default ports. Does not apply crawl-scope filtering.
 */
export function normalizeAbsoluteUrl(input: string): string | null {
  const trimmed = input.trim();
  if (!trimmed) {
    return null;
  }
  let u: URL;
  try {
    u = new URL(trimmed);
  } catch {
    return null;
  }
  if (!["http:", "https:"].includes(u.protocol)) {
    return null;
  }
  if (!u.hostname) {
    return null;
  }
  u.hash = "";
  stripDefaultPort(u);
  return u.toString();
}

/**
 * Strict crawl scope: the seed hostname plus a single optional `www.` counterpart
 * (apex ↔ `www.` + apex). No other subdomains.
 */
export function buildAllowedHostSet(seedHostname: string): Set<string> {
  const h = seedHostname.trim().toLowerCase();
  const out = new Set<string>();
  if (!h) {
    return out;
  }
  out.add(h);
  if (h.startsWith("www.")) {
    const apex = h.slice(4);
    if (apex) {
      out.add(apex);
    }
  } else {
    out.add(`www.${h}`);
  }
  return out;
}

export function parseSeedUrl(
  seedUrl: string
): { normalized: string; hostname: string; allowedHosts: Set<string> } | null {
  const normalized = normalizeAbsoluteUrl(seedUrl);
  if (!normalized) {
    return null;
  }
  let u: URL;
  try {
    u = new URL(normalized);
  } catch {
    return null;
  }
  const hostname = u.hostname.toLowerCase();
  if (!hostname) {
    return null;
  }
  const allowedHosts = buildAllowedHostSet(hostname);
  return { normalized, hostname, allowedHosts };
}

export function normalizeUrl(baseUrl: string, rawHref: string, allowedHosts: ReadonlySet<string>): string | null {
  const href = rawHref.trim();
  if (!href || href.startsWith("mailto:") || href.startsWith("tel:") || href.startsWith("javascript:")) {
    return null;
  }

  let resolved: URL;
  try {
    resolved = new URL(href, baseUrl);
  } catch (_err) {
    return null;
  }

  if (!["http:", "https:"].includes(resolved.protocol)) {
    return null;
  }

  const host = resolved.hostname.toLowerCase();
  if (!allowedHosts.has(host)) {
    return null;
  }

  resolved.hash = "";
  stripDefaultPort(resolved);

  return resolved.toString();
}
