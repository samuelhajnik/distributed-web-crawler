import { load } from "cheerio";
import { isDocumentUrl, normalizeCandidateUrl, type RunContext } from "../runContext";

export function extractLinkPairs(
  baseUrl: string,
  html: string,
  runContext: RunContext
): { normalized: string; raw: string }[] {
  const $ = load(html);
  const out: { normalized: string; raw: string }[] = [];
  const seen = new Set<string>();
  $("a[href]").each((_idx, el) => {
    const href = $(el).attr("href");
    if (!href) {
      return;
    }
    const raw = href.trim();
    const normalized = normalizeCandidateUrl(baseUrl, raw, runContext);
    if (normalized && !seen.has(normalized)) {
      if (!runContext.config.includeDocuments && isDocumentUrl(normalized)) {
        return;
      }
      seen.add(normalized);
      out.push({ normalized, raw });
    }
  });
  return out;
}
