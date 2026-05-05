export function logCp(crawlRunId: number | undefined, msg: string): void {
  const run = crawlRunId !== undefined ? ` crawl_run=${crawlRunId}` : "";
  process.stdout.write(`[component=control-plane]${run} ${msg}\n`);
}
