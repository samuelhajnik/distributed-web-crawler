# Observability

This project treats **metrics + structured logs** as first-class operator tools.

## Metrics endpoints

| Service       | URL (Docker network)                | Host dev                        |
| ------------- | ----------------------------------- | ------------------------------- |
| Control plane | `http://control-plane:3000/metrics` | `http://localhost:3000/metrics` |
| Worker        | `http://worker:9091/metrics`        | map `9091` if published         |

Prometheus (Compose): **http://localhost:9090** — verify **Targets** are healthy.

## What to watch (and why)

### Counters / gauges (existing)

- **`crawl_runs_*`**, **`crawl_urls_*` counts** — throughput and terminal outcomes.
- **`processed_urls_total`** (worker) — increments once **after a run-level BullMQ signal led to a successful Postgres claim** and processing finishes for that URL row (`VISITED`, terminal outcomes, or return to `QUEUED` for retry). Does **not** increment when no eligible URL was claimed for that signal.
- **`crawl_urls_queued_gauge`**, **`crawl_urls_in_progress_gauge`**, **`crawl_urls_failed_gauge`** (control-plane) — aggregates across **`RUNNING`** crawl runs (see exporter definitions for exact filters).
- **`crawl_stale_claims_recovered_total`** — lease recovery firing (crashed / stuck workers).
- **`crawl_queue_reconciliation_*`** — how often reconciliation attempted **`topUpRunSignals`** (bounded additions toward **`DISPATCH_SIGNALS_PER_RUN`**) because Postgres showed claimable `QUEUED` work—enqueue-gap safety valve, **not** “publish every queued URL.”
- **`crawl_completion_checks_total`** + logs — progress toward stable completion.

### Latency / performance (P2)

**Worker**

- **`crawl_fetch_duration_seconds`** — time for the gated `undici` request until the response object is returned (headers available). Rises when the origin is slow, TCP/TLS is slow, or you are saturating your own concurrency gate.
- **`crawl_processing_duration_seconds`** — end-to-end handling **after a successful claim** (includes HTML body read, parse, DB inserts for discoveries, **bounded signal top-ups**, terminal state writes). Separates “network read” from “local work”.
- **`crawl_queue_latency_seconds`** — `now - job.timestamp` at job start. Elevated when workers are saturated, Redis is slow, or concurrency is too low vs incoming work.

**Control plane**

- **`crawl_reconciliation_cycle_duration_seconds`** — wall time for one full maintenance sweep across all RUNNING runs. Spikes when Postgres is slow, many RUNNING runs require maintenance in the same sweep, or maintenance work per sweep grows with active runs.

## Detecting common failure modes

| Symptom                     | Likely signal                                                                                                                                                                                                                                                                     |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stuck frontier              | Gauges show `QUEUED>0` forever; reconciliation counters increase; queue latency high; check **`retry_after_at`** (URLs may be intentionally not claimable yet)                                                                                                                    |
| Retry storm                 | `crawl_urls_retried_total` climbs; `crawl_fetch_duration`/`processing` erratic                                                                                                                                                                                                    |
| Slow origin                 | `crawl_fetch_duration_seconds` p95/p99 up; `processing` follows if HTML is large                                                                                                                                                                                                  |
| Reconciliation churn        | `crawl_queue_reconciliation_enqueued_total` high vs visit progress—signals topping up but **not converting to claims** (workers saturated, run not `RUNNING`, URLs awaiting **`retry_after_at`**, Redis/control-plane issues); long `crawl_reconciliation_cycle_duration_seconds` |
| Near completion oscillation | completion logs + empty-frontier streak (see README)                                                                                                                                                                                                                              |

## Logs

- Control plane: `[component=control-plane] crawl_run=…`
- Worker: `[worker worker_id=… crawl_run=… url_id=…]`

Worker logs keep **`url_id`** on the hot path because processing is **per claimed URL row**, even though BullMQ carries **run-level** signals—use logs to stitch **`crawl_run_id`** + **`url_id`** back to metrics.

## A “healthy” run (conceptually)

- `crawl_queue_latency_seconds` stable and low relative to your expectations.
- `crawl_fetch_duration_seconds` consistent with the remote site.
- `crawl_reconciliation_cycle_duration_seconds` small vs reconciliation interval.
- Gauges trend toward zero frontier, then run completes (`crawl_runs_completed_total` increments once).
