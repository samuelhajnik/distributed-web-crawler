export type RedirectResolution = {
  requestedUrl: string;
  finalUrl: string;
  redirected: boolean;
  finalInScope: boolean;
};

export function getEffectiveFinalUrl(
  maybeResponseUrl: string | undefined,
  requestedUrl: string
): string {
  if (!maybeResponseUrl) {
    return requestedUrl;
  }
  try {
    const parsed = new URL(maybeResponseUrl);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return requestedUrl;
    }
    return parsed.toString();
  } catch {
    return requestedUrl;
  }
}
