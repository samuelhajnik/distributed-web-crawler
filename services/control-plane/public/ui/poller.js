/**
 * Tiny polling controller for phase 1 demo UI.
 * Swap this module with an SSE transport later without changing render code.
 */
export function createRunPoller(fetchTick, onData, onError, intervalMs = 1500) {
  let timer = null;
  let currentRunId = null;

  async function tick() {
    if (currentRunId == null) {
      return;
    }
    try {
      const snapshot = await fetchTick(currentRunId);
      onData(snapshot);
      const status = String(snapshot?.summary?.status ?? "").toUpperCase();
      if (status === "COMPLETED" || status === "FAILED") {
        stop();
      }
    } catch (err) {
      onError(err);
    }
  }

  function start(runId) {
    stop();
    currentRunId = runId;
    void tick();
    timer = setInterval(() => void tick(), intervalMs);
  }

  function stop() {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  }

  return { start, stop };
}
