const DEFAULT_API = "http://localhost:3000";

export function crawlerApiBase(): string {
  return (process.env.CRAWLER_API ?? DEFAULT_API).replace(/\/$/, "");
}

export type SummaryResponse = {
  status: string;
  totals: {
    discovered: number;
    visited: number;
    redirect_301: number;
    forbidden: number;
    not_found: number;
    http_terminal: number;
    failed: number;
    queued: number;
    in_progress: number;
  };
};

export type ExportRow = {
  normalized_url: string;
  status: string;
};

export type ExportResponse = {
  crawl_run_id: number;
  urls: ExportRow[];
};

export async function healthCheck(): Promise<boolean> {
  try {
    const r = await fetch(`${crawlerApiBase()}/health`);
    return r.ok;
  } catch {
    return false;
  }
}

export async function createCrawlRun(seedUrl: string): Promise<number> {
  const r = await fetch(`${crawlerApiBase()}/crawl-runs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ seedUrl })
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`createCrawlRun failed ${r.status}: ${t}`);
  }
  const body = (await r.json()) as { id: number };
  return Number(body.id);
}

/** Triggers maintenance + returns run payload including status. */
export async function getRun(crawlRunId: number): Promise<{ status: string; queue_empty: boolean; in_progress: number }> {
  const r = await fetch(`${crawlerApiBase()}/crawl-runs/${crawlRunId}`);
  if (!r.ok) {
    throw new Error(`getRun failed ${r.status}`);
  }
  return r.json() as Promise<{ status: string; queue_empty: boolean; in_progress: number }>;
}

export async function getSummary(crawlRunId: number): Promise<SummaryResponse> {
  const r = await fetch(`${crawlerApiBase()}/crawl-runs/${crawlRunId}/summary`);
  if (!r.ok) {
    throw new Error(`getSummary failed ${r.status}`);
  }
  return r.json() as Promise<SummaryResponse>;
}

export async function exportJson(crawlRunId: number): Promise<ExportResponse> {
  const r = await fetch(`${crawlerApiBase()}/crawl-runs/${crawlRunId}/export?format=json&limit=500000`);
  if (!r.ok) {
    throw new Error(`export failed ${r.status}`);
  }
  return r.json() as Promise<ExportResponse>;
}

export async function waitForCrawlComplete(
  crawlRunId: number,
  options: { timeoutMs: number; pollMs?: number }
): Promise<void> {
  const pollMs = options.pollMs ?? 500;
  const deadline = Date.now() + options.timeoutMs;
  let lastStatus = "";
  while (Date.now() < deadline) {
    const run = await getRun(crawlRunId);
    lastStatus = run.status;
    if (run.status === "COMPLETED" && run.queue_empty && run.in_progress === 0) {
      return;
    }
    await new Promise((r) => setTimeout(r, pollMs));
  }
  const summary = await getSummary(crawlRunId).catch(() => null);
  throw new Error(
    `timeout waiting for COMPLETED (last status=${lastStatus}) crawl_run=${crawlRunId} summary=${JSON.stringify(summary)}`
  );
}
