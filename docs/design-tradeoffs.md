# Design trade-offs

Short, defendable rationale for the architecture choices in this repository.

## 1) Postgres as source of truth

**Why:** Crawl correctness is fundamentally a **state machine + set membership** problem (frontier, dedup, leases). A durable RDBMS gives:

- atomic transitions (`QUEUED → IN_PROGRESS`),
- unique constraints for dedup,
- queryable inspection and exports.

**Alternative:** Redis-only frontier — faster, but durability and ad-hoc querying suffer; recovery is harder to explain.

## 2) Redis / BullMQ instead of Kafka

**Why:** In this repo, work units are small (`url_id`), throughput targets are modest, and delayed retries are straightforward to demonstrate with BullMQ.

Kafka would be more appropriate for different requirements, such as much higher event volume, multi-consumer replay, or stream-processing workflows. Those requirements are outside this repository's scope, so BullMQ keeps retries and job dispatch easier to inspect.

## 3) Why not Postgres alone as the queue?

**Possible pattern:** `SKIP LOCKED` dequeue from a `crawl_urls` table.

**Why this implementation still uses BullMQ:**

- delayed job and backoff semantics are available out of the box,
- workers can consume horizontally without relying on continuous DB dequeue polling,
- transport concerns stay separated from durable crawl state.

**Trade-off:** enqueue-after-commit is best-effort, so this repo pairs it with **reconciliation** to re-publish `QUEUED` rows.

## 4) Why not a fully distributed ownership model without centralized durable state?

**Examples:** purely peer-to-peer URL sets, CRDT-only frontiers.

For this codebase size and goal, centralized durable state keeps correctness properties easier to explain, inspect, and test (completeness, dedup, crash recovery).

## 5) Why not aggressively canonicalize URLs?

Aggressive normalization collapses distinct resources (tracking params, casing, etc.) and can **silently change completeness semantics**.

We keep normalization **conservative** and document it explicitly.

## 6) Fit for stated requirements

This design optimizes for:

- clear **correctness properties under concurrency and failure** (atomic claim + reconciliation + lease recovery),
- inspectable behavior (API + metrics + exports),
- explainable trade-offs for a demo-sized implementation.

It intentionally does not optimize for crawling the entire public web.
