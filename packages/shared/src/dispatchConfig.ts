const DEFAULT_DISPATCH_SIGNALS_PER_RUN = 32;

export function readDispatchSignalsPerRun(): number {
  const raw = process.env.DISPATCH_SIGNALS_PER_RUN;
  if (!raw) {
    return DEFAULT_DISPATCH_SIGNALS_PER_RUN;
  }
  const n = Number(raw);
  if (!Number.isFinite(n)) {
    return DEFAULT_DISPATCH_SIGNALS_PER_RUN;
  }
  return Math.min(256, Math.max(1, Math.floor(n)));
}

export { DEFAULT_DISPATCH_SIGNALS_PER_RUN };
