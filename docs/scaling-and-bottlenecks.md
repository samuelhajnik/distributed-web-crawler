# Scaling and bottlenecks

This crawler is intentionally **small, serious, and explainable** — not an infinite-scale web harvester.

## What breaks first as you scale up

### Postgres write amplification

Every discovered URL is an `INSERT … ON CONFLICT DO NOTHING`, plus status transitions and counters. At large breadth:

- **CPU / IO** on the primary rises with insert rate.
- **Index churn** on `(crawl_run_id, normalized_url)` and status indexes.
- **Hot rows** if many workers update the same run counters (`duplicates_skipped`).

**Mitigation ideas (future):** batch inserts, partition `crawl_urls` by `crawl_run_id`, separate counter aggregation from hot row updates.

### Redis / BullMQ throughput

The queue must sustain:

- reconciliation bulk enqueues,
- discovery fan-out enqueues,
- delayed retries.

Redis becomes the ceiling when job volume spikes (many duplicates in queue are OK, but Redis still pays for each job object).

**Mitigation ideas:** reduce job payload churn, tune `removeOnComplete`, shard queues by crawl run.

### Reconciliation overhead

Reconciliation is O(frontier) work per interval (bounded by `RECONCILE_BATCH_SIZE` per run). Many RUNNING runs or huge `QUEUED` sets increase DB read + Redis writes each tick.

**Mitigation ideas:** adaptive interval, per-run backoff when frontier is huge, stronger “enqueue only if missing from queue” (adds complexity — often not worth it).

### Hot-domain skew

Each crawl run carries its own small `allowed_hosts` pair (seed host + optional `www.` counterpart). Many links still hit one origin, so workers can create **uneven fetch pressure** on that host even with polite defaults.

**Mitigation ideas:** per-host rate limits (stronger than our simple semaphore), global token bucket, crawl budgets.

### Network / remote latency

Often the dominant cost: TLS, TTFB, large HTML. Workers may be idle waiting on sockets.

**Mitigation ideas:** more workers (until origin politeness says stop), HTTP/2 where beneficial, smaller fetch scope.

### Memory / storage growth

Large sites → large `crawl_urls` tables and export files. This design **does not store page bodies**; still, metadata volume grows.

**Mitigation ideas:** retention policy, archival to object storage, crawl caps.

## Why this architecture fits “assignment scale”

- **Correctness** is centralized in Postgres (atomic claim + dedup).
- **Execution** is delegated to BullMQ (simple ops story).
- **Complexity** stays bounded: two stateful tiers (Postgres + Redis), two codebases (control-plane + worker).

## If you needed a large grid of machines

High-value evolution path:

1. **Partition the frontier** by host/shard (multiple queues, multiple DB partitions).
2. **Outbox or transactional enqueue** if you must narrow the commit/enqueue gap without reconciliation.
3. **Dedicated fetchers** vs **CPU-bound parsers** if HTML parsing dominates.
4. **Politeness service** with distributed rate limits (token bucket in Redis or centralized limiter).
5. **Object storage** for raw content / WARC if scope expands.

None of these are required for the current educational scope.
