const JSON_HEADERS = { "Content-Type": "application/json" };

async function readJsonOrThrow(resp, context) {
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`${context} failed (${resp.status}): ${body || resp.statusText}`);
  }
  return resp.json();
}

export async function startCrawl(seedUrl, settings) {
  const resp = await fetch("/crawl-runs", {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify({ seedUrl, settings })
  });
  return readJsonOrThrow(resp, "start crawl");
}

export async function getSummary(crawlRunId) {
  const resp = await fetch(`/crawl-runs/${crawlRunId}/summary`);
  return readJsonOrThrow(resp, "summary");
}

export async function getUrls(crawlRunId, limit = 200, offset = 0) {
  const o = Math.max(0, Number(offset) || 0);
  const resp = await fetch(
    `/crawl-runs/${crawlRunId}/urls?limit=${limit}&offset=${o}&sort=id&order=asc`
  );
  return readJsonOrThrow(resp, "urls");
}

export async function getGraph(crawlRunId, limit = 50000) {
  const resp = await fetch(`/crawl-runs/${crawlRunId}/graph?limit=${limit}`);
  return readJsonOrThrow(resp, "graph");
}
