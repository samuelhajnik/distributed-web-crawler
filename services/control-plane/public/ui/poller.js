/**
 * Tiny polling controller for phase 1 demo UI.
 * One in-flight tick at a time: next poll is scheduled only after the current tick settles,
 * avoiding overlapping requests and stale responses overwriting newer UI state.
 * Swap this module with an SSE transport later without changing render code.
 */
export function createRunPoller(fetchTick, onData, onError, initialIntervalMs = 1500) {
  let timeoutHandle = null;
  let currentRunId = null;
  let pollEveryMs = initialIntervalMs;
  let stopped = true;

  function clearSchedule() {
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
      timeoutHandle = null;
    }
  }

  async function tick() {
    if (stopped || currentRunId == null) {
      return;
    }
    const runIdAtTickStart = currentRunId;
    try {
      const snapshot = await fetchTick(runIdAtTickStart);
      if (stopped || currentRunId == null || currentRunId !== runIdAtTickStart) {
        return;
      }
      onData(snapshot);
      const status = String(snapshot?.summary?.status ?? "").toUpperCase();
      if (status === "COMPLETED" || status === "FAILED") {
        stop();
        return;
      }
    } catch (err) {
      if (!stopped) {
        onError(err);
      }
    } finally {
      if (!stopped && currentRunId != null) {
        clearSchedule();
        timeoutHandle = setTimeout(() => void tick(), pollEveryMs);
      }
    }
  }

  function start(runId) {
    stop();
    stopped = false;
    currentRunId = runId;
    void tick();
  }

  function stop() {
    stopped = true;
    clearSchedule();
  }

  /** Apply a new interval; reschedules only if a wait between ticks is already queued (not mid-tick). */
  function setPollInterval(ms) {
    pollEveryMs = ms;
    if (!stopped && currentRunId != null && timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
      timeoutHandle = setTimeout(() => void tick(), pollEveryMs);
    }
  }

  return { start, stop, setPollInterval };
}
