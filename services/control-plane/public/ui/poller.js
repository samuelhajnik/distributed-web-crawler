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
  /** True while fetchTick/onData for this tick are running. */
  let tickInFlight = false;
  /** Another tick was requested while one was running (triggerNow or overlapping tick). */
  let queuedImmediateTick = false;
  /** Resolvers for pending triggerNow() calls; flushed after the relevant tick completes. */
  let pendingTriggerCompletes = [];

  function clearSchedule() {
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
      timeoutHandle = null;
    }
  }

  function flushTriggerCompletes() {
    const completes = pendingTriggerCompletes.splice(0);
    completes.forEach((fn) => fn());
  }

  async function tick() {
    if (stopped || currentRunId == null) {
      return;
    }
    if (tickInFlight) {
      queuedImmediateTick = true;
      return;
    }
    const runIdAtTickStart = currentRunId;
    tickInFlight = true;
    let chainImmediate = false;
    try {
      const snapshot = await fetchTick(runIdAtTickStart);
      if (stopped || currentRunId == null || currentRunId !== runIdAtTickStart) {
        return;
      }
      await Promise.resolve(onData(snapshot));
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
      tickInFlight = false;
      chainImmediate = queuedImmediateTick;
      queuedImmediateTick = false;

      if (!stopped && currentRunId != null) {
        clearSchedule();
        if (chainImmediate) {
          timeoutHandle = setTimeout(() => void tick(), 0);
        } else {
          flushTriggerCompletes();
          timeoutHandle = setTimeout(() => void tick(), pollEveryMs);
        }
      } else {
        flushTriggerCompletes();
      }
    }
  }

  /**
   * Run one poll immediately (coalesced with in-flight work). Resolves when the fetch that
   * satisfies this refresh has finished (including onData). Does not reject on fetch errors
   * (onError runs instead). No-op if stopped or no active run.
   */
  function triggerNow() {
    return new Promise((resolve) => {
      if (stopped || currentRunId == null) {
        resolve();
        return;
      }
      pendingTriggerCompletes.push(resolve);
      if (tickInFlight) {
        queuedImmediateTick = true;
        return;
      }
      clearSchedule();
      void tick();
    });
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
    flushTriggerCompletes();
  }

  /** Apply a new interval; reschedules only if a wait between ticks is already queued (not mid-tick). */
  function setPollInterval(ms) {
    pollEveryMs = ms;
    if (!stopped && currentRunId != null && timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
      timeoutHandle = setTimeout(() => void tick(), pollEveryMs);
    }
  }

  return { start, stop, setPollInterval, triggerNow };
}
