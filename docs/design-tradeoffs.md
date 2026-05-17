# Design trade-offs

Short, defensible rationale for the architecture choices in this repository.

## 1) Postgres as source of truth

**Why:** Crawl correctness is fundamentally a **state machine + set membership** problem (frontier, dedup, leases). A durable RDBMS gives:

- atomic transitions (`QUEUED → IN_PROGRESS`),
- unique constraints for dedup,
- queryable inspection and exports.

**Alternative:** Redis-only frontier — faster, but durability and ad-hoc querying suffer; recovery is harder to explain.

## 2) Redis / BullMQ instead of Kafka

**Why:** Dispatch is modeled as **bounded run-level signals** plus occasional delayed wakeup jobs, not one queue message per discovered URL. Throughput targets are modest, and BullMQ makes **delayed retries** and cross-process scheduling easy to demonstrate without building a custom wakeup scheduler.

Kafka would be more appropriate for different requirements, such as much higher event volume, multi-consumer replay, or stream-processing workflows. Those requirements are outside this repository's scope, so BullMQ keeps scheduling and operator ergonomics simple.

## 3) Why not Postgres alone as the queue?

**Possible pattern:** workers continuously try to claim eligible rows from `crawl_urls` using `FOR UPDATE SKIP LOCKED`. When no row is claimable, they sleep or back off and try again.

That architecture would work. In this repository, Postgres already owns the correctness-critical state: run status, URL eligibility, retry timing, cancellation, leases, and completion. A Postgres-only worker loop would therefore be a valid simplification.

**Why this implementation still uses BullMQ:** BullMQ is not used as the source of truth for crawl work. It is used as a **bounded execution signal layer**.

A BullMQ job does not mean “process this URL.” It means “run X may have claimable work; try one Postgres claim.” The worker still asks Postgres whether the run is `RUNNING`, whether a URL is eligible, whether `retry_after_at` has passed, and whether the row can be atomically claimed.

The benefit is that workers do not need to continuously poll Postgres just to discover whether work exists. Without BullMQ, idle workers need a loop such as:

```text
claim next eligible URL
if none found:
  sleep
  try again
```

That creates a tuning trade-off: short sleeps are responsive but create noisy database polling; long sleeps reduce load but make new work and retry wakeups less responsive.

BullMQ provides useful execution primitives around that boundary:

- **bounded wakeups** — workers wait for run-level signals instead of repeatedly scanning Postgres while idle,
- **delayed retry wakeups** — retryable URLs can schedule a later run signal instead of relying only on short-interval DB polling,
- **worker concurrency control** — BullMQ controls how many signal handlers run concurrently in a worker process,
- **queue latency visibility** — signal delay is observable separately from fetch and processing time,
- **bounded dispatch** — `DISPATCH_SIGNALS_PER_RUN` prevents one large crawl from flooding Redis with one job per URL.

**Trade-offs:** This is more complex than a pure Postgres polling loop and still depends on Postgres for every real claim. BullMQ signals can also be harmlessly stale: a run may have been cancelled, another worker may have drained the frontier, or all queued rows may still be waiting for `retry_after_at`. In those cases, the Postgres claim returns nothing and the signal completes without processing a URL.

That is intentional. Correctness lives in Postgres; BullMQ only decides when a worker should try. Reconciliation remains the safety net for best-effort signal publication: if Postgres shows claimable `QUEUED` rows, the control plane tops up bounded run-level signals rather than replaying every URL row into Redis.

## 4) Why not a fully distributed ownership model without centralized durable state?

**Examples:** purely peer-to-peer URL sets, CRDT-only frontiers.

For this codebase size and goal, centralized durable state keeps correctness properties easier to explain, inspect, and test: completeness, deduplication, cancellation, retry eligibility, and crash recovery remain visible in one durable state model.

## 5) Why not aggressively canonicalize URLs?

Aggressive normalization can collapse distinct resources and **silently change completeness semantics**.

This implementation keeps normalization **conservative** and documents it explicitly. The crawler normalizes enough to make deduplication deterministic, but avoids pretending that URL identity is universally obvious across all websites.

## 6) Fit for stated requirements

This design optimizes for:

- clear **correctness properties under concurrency and failure**,
- durable and inspectable crawl state,
- bounded asynchronous worker dispatch,
- recovery from missed signals and stale claims,
- cancellation semantics that are enforced at claim/update boundaries,
- observable behavior through API, metrics, exports, and the demo UI.

It intentionally does not optimize for crawling the entire public web.
