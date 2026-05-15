# Design trade-offs

Short, defendable rationale for the architecture choices in this repository.

## 1) Postgres as source of truth

**Why:** Crawl correctness is fundamentally a **state machine + set membership** problem (frontier, dedup, leases). A durable RDBMS gives:

- atomic transitions (`QUEUED → IN_PROGRESS`),
- unique constraints for dedup,
- queryable inspection and exports.

**Alternative:** Redis-only frontier — faster, but durability and ad-hoc querying suffer; recovery is harder to explain.

## 2) Redis / BullMQ instead of Kafka

**Why:** Dispatch is modeled as **bounded run-level signals** (plus occasional delayed wakeup jobs), not one queue message per discovered URL. Throughput targets are modest, and BullMQ makes **delayed retries** and cross-process scheduling easy to demonstrate without building a custom wakeup scheduler.

Kafka would be more appropriate for different requirements, such as much higher event volume, multi-consumer replay, or stream-processing workflows. Those requirements are outside this repository's scope, so BullMQ keeps scheduling and operator ergonomics simple.

## 3) Why not Postgres alone as the queue?

**Possible pattern:** continuous `SKIP LOCKED` dequeue polling from `crawl_urls`.

**Why this implementation still uses BullMQ:**

- **bounded wakeups** reduce tight polling loops while workers sit idle,
- **delayed jobs** provide timely retry wakeups without scanning `retry_after_at` from every worker on a short interval,
- **bounded dispatch (`DISPATCH_SIGNALS_PER_RUN`)** caps Redis fan-out per run while keeping some fairness across concurrent runs on a shared pool,
- transport stays separated from durable crawl state.

**Trade-offs:**

- Cross-run fairness is **not** strict round-robin; behavior depends on shared worker capacity, BullMQ ordering, and **process-local** host pacing.
- The **Postgres claim path is central**: correctness lives in SQL transitions and guards; Redis only prompts work.

**Enqueue-after-commit** remains best-effort for signals; **reconciliation tops up bounded run signals** when Postgres shows claimable `QUEUED` rows (not “replay every row” into Redis).

## 4) Why not a fully distributed ownership model without centralized durable state?

**Examples:** purely peer-to-peer URL sets, CRDT-only frontiers.

For this codebase size and goal, centralized durable state keeps correctness properties easier to explain, inspect, and test (completeness, dedup, crash recovery).

## 5) Why not aggressively canonicalize URLs?

Aggressive normalization collapses distinct resources (tracking params, casing, etc.) and can **silently change completeness semantics**.

We keep normalization **conservative** and document it explicitly.

## 6) Fit for stated requirements

This design optimizes for:

- clear **correctness properties under concurrency and failure** (atomic claim + bounded-signal reconciliation + lease recovery),
- inspectable behavior (API + metrics + exports),
- explainable trade-offs for a demo-sized implementation.

It intentionally does not optimize for crawling the entire public web.
