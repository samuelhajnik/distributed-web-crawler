# Scaling and bottlenecks

This crawler is intentionally **small, serious, and explainable** — not an infinite-scale web harvester.

## What breaks first as you scale up

### Postgres write amplification

The **canonical frontier** lives here: breadth of the crawl drives insert rate and claim churn. At large breadth this tier usually dominates before Redis does.

Every discovered URL is an `INSERT … ON CONFLICT DO NOTHING`, plus status transitions and counters. At large breadth:

- **CPU / IO** on the primary rises with insert rate.
- **Index churn** on `(crawl_run_id, normalized_url)` and status indexes.
- **Hot rows** if many workers update the same run counters (`duplicates_skipped`).

**Mitigation ideas (future):** batch inserts, partition `crawl_urls` by `crawl_run_id`, separate counter aggregation from hot row updates.

### Redis / BullMQ throughput

Redis load scales with **bounded dispatch**, not with discovered URL cardinality:

- each **`RUNNING`** crawl targets up to **`DISPATCH_SIGNALS_PER_RUN`** concurrent **slot** job IDs at steady state,
- plus **delayed retry wakeup** jobs (rate bounded by retryable failures),
- plus reconciliation **`topUpRunSignals`** attempts (writes capped per tick because duplicate slot IDs short-circuit).

So Redis is **much less sensitive** to “millions of discovered URLs” than a naive `enqueue-one-job-per-URL` design.

**Mitigation ideas:** tune **`DISPATCH_SIGNALS_PER_RUN`** cautiously (trade responsiveness vs Redis/control-plane chatter), tune **`removeOnComplete`/`removeOnFail`** retention defaults if artifacts accumulate during incidents.

Sharding Redis queues **per crawl run** trades complexity for isolation but fights this repo’s deliberate choice of **shared bounded signals across runs**—treat that as a **larger architecture pivot**, not the immediate knob here.

### Reconciliation overhead

Reconciliation scans **`RUNNING`** runs and performs Postgres reads plus bounded **`topUpRunSignals`** calls toward **`DISPATCH_SIGNALS_PER_RUN`**. Cost rises primarily with **active run count** and **cheap existence checks for claimable `QUEUED` work**, not with emitting one Redis job per queued URL.

**Mitigation ideas:** adaptive reconciliation intervals under idle vs busy regimes; backoff expensive runs when the frontier is huge but signals already saturate **`DISPATCH_SIGNALS_PER_RUN`**; transactional **outbox** only if you need tighter enqueue guarantees without reconciliation breadth.

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
- **Execution wakeup** uses BullMQ for bounded scheduling across worker replicas **without mirroring the frontier** in Redis.
- **Complexity** stays bounded: two stateful tiers (Postgres + Redis), two codebases (control-plane + worker).

## If you needed a large grid of machines

High-value evolution path:

1. **Partition `crawl_urls` / shard Postgres by crawl/host keys** when inserts or contention dominate—still paired with coordinated dispatcher semantics above Postgres (avoid believing Redis queue depth equals frontier depth).
2. **Dedicated dispatcher topology or adaptive dispatch windows** when fairness/latency across many concurrent runs needs tighter guarantees than bounded slots provide (rather than naïvely multiplying queues per run inside BullMQ).
3. **Outbox or transactional enqueue** if you must narrow the commit/enqueue gap without widening reconciliation scans.
4. **Dedicated fetchers** vs **CPU-bound parsers** if HTML parsing dominates.
5. **Politeness service** with distributed rate limits (token bucket in Redis or centralized limiter).
6. **Object storage** for raw content / WARC if scope expands.

None of these are required for the current educational scope.
