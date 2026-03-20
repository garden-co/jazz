# Database Learnings To Double-Check

Several MVP specs depend on low-level durability and isolation assumptions that are easy to get subtly wrong. Before we lock in APIs or document guarantees, we should sanity-check our designs against a small set of strong references.

## Priority 0: correctness claims to tighten now

### 1. Durability vocabulary and acknowledgement boundaries

We should stop any ambiguity between:

- accepted
- applied
- visible
- flushed
- durable after crash
- replicated

Internal specs to revisit:

- [durability_tier_api_unification.md](durability_tier_api_unification.md)
- [scheduled_wal_flush.md](scheduled_wal_flush.md)
- [Browser Adapters](../../status-quo/browser_adapters.md)
- [benchmarks_and_performance.md](benchmarks_and_performance.md)

Read next:

- [Durability APIs](https://transactional.blog/how-to-learn/disk-io)
- [Write-Ahead Logging (PostgreSQL)](https://www.postgresql.org/docs/current/wal-intro.html)
- [Write-Ahead Logging (SQLite)](https://sqlite.org/wal.html)

### 2. Crash consistency and recovery invariants

We should explicitly check:

- partial or torn WAL records
- checksum / corruption detection strategy
- idempotent WAL replay
- safe checkpoint replacement
- file sync vs directory sync requirements
- crash-at-any-boundary recovery behavior

Internal specs to revisit:

- [scheduled_wal_flush.md](scheduled_wal_flush.md)
- [Browser Adapters](../../status-quo/browser_adapters.md)
- [browser_e2e_test_suite.md](browser_e2e_test_suite.md)
- [noop_main_thread_storage.md](noop_main_thread_storage.md)

Read next:

- [More about Linux file APIs and durability](https://www.evanjones.ca/durability-filesystem.html)
- [Atomic Commit In SQLite](https://sqlite.org/atomiccommit.html)
- [File Locking And Concurrency In SQLite Version 3](https://www.sqlite.org/lockingv3.html)
- [ARIES](https://research.ibm.com/publications/aries-a-transaction-recovery-method-supporting-fine-granularity-locking-and-partial-rollbacks-using-write-ahead-logging)

### 3. Isolation levels and anomaly testing

We should avoid naming an isolation level unless we can reproduce the claim with concrete histories. This includes subscription and reconnect behavior, not just explicit transactions.

Internal specs to revisit:

- [../b_launch/globally_consistent_transactions.md](../b_launch/globally_consistent_transactions.md)
- [Query/Sync Integration](../../status-quo/query_sync_integration.md)
- [durability_tier_api_unification.md](durability_tier_api_unification.md)
- [supported_use_cases.md](supported_use_cases.md)

Read next:

- [Martin Kleppmann's Hermitage writeup](https://martin.kleppmann.com/2014/11/25/hermitage-testing-the-i-in-acid.html)
- [Weak Consistency: A Generalized Theory and Optimistic Implementations for Distributed Transactions](https://publications.csail.mit.edu/lcs/pubs/pdf/MIT-LCS-TR-786.pdf)
- [Elle: Inferring Isolation Anomalies from Experimental Observations](https://arxiv.org/abs/2003.10554)
- [Transaction Isolation (PostgreSQL)](https://www.postgresql.org/docs/current/transaction-iso.html)
- [Isolation In SQLite](https://www.sqlite.org/isolation.html)

### 4. Session guarantees for local-first UX

For Jazz, this may matter more than formal ANSI labels. We should decide and document which of these hold:

- read-your-writes
- monotonic reads
- monotonic writes
- consistent prefix
- whether leader/failover/reconnect can temporarily violate any of the above

Internal specs to revisit:

- [Browser Adapters](../../status-quo/browser_adapters.md)
- [efficient_resubscribe.md](efficient_resubscribe.md)
- [query_subscription_disconnect_cleanup.md](query_subscription_disconnect_cleanup.md)
- [lightweight_subscription_delta_protocol_design.md](lightweight_subscription_delta_protocol_design.md)
- [Query/Sync Integration](../../status-quo/query_sync_integration.md)

Read next:

- [Concurrency Control (PostgreSQL)](https://www.postgresql.org/docs/current/mvcc.html)
- [Isolation In SQLite](https://www.sqlite.org/isolation.html)
- [FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf)

## Priority 1: best-in-class engineering follow-through

### 5. Checkpointing, compaction, and write amplification

Questions:

- When can WAL be truncated safely?
- What causes latency spikes during checkpoint?
- How do we measure accepted writes vs flushed WAL vs compacted state?
- What happens near OPFS quota limits?

Internal specs to revisit:

- [scheduled_wal_flush.md](scheduled_wal_flush.md)
- [storage_limits_and_eviction.md](storage_limits_and_eviction.md)
- [benchmarks_and_performance.md](benchmarks_and_performance.md)

Read next:

- [Write-Ahead Logging (SQLite)](https://sqlite.org/wal.html)
- [WAL Configuration (PostgreSQL)](https://www.postgresql.org/docs/current/wal-configuration.html)

### 6. Failover, fencing, and re-attach correctness

Questions:

- Can an acknowledged write still disappear during leader change?
- Which messages must be term-fenced?
- What duplicate or out-of-order replay is possible?
- Which guarantees pause during detach / re-attach and which must not?

Internal specs to revisit:

- [Browser Adapters](../../status-quo/browser_adapters.md)
- [efficient_resubscribe.md](efficient_resubscribe.md)
- [query_subscription_disconnect_cleanup.md](query_subscription_disconnect_cleanup.md)
- [server_error_event_streaming.md](server_error_event_streaming.md)

Read next:

- [FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf)
- [Elle: Inferring Isolation Anomalies from Experimental Observations](https://arxiv.org/abs/2003.10554)

### 7. Corruption detection, crash injection, and differential testing

Best-in-class databases do not rely on reasoning alone. They build harnesses that crash constantly, replay constantly, and compare behavior against known-good systems.

Internal specs to revisit:

- [browser_e2e_test_suite.md](browser_e2e_test_suite.md)
- [weak_tests.md](weak_tests.md)
- [benchmarks_and_performance.md](benchmarks_and_performance.md)
- [catalogue_sync_e2e_test.md](catalogue_sync_e2e_test.md)

Read next:

- [How SQLite Is Tested](https://www.sqlite.org/testing.html)
- [TH3](https://www.sqlite.org/th3.html)
- [FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf)

### 8. Schema evolution with mixed versions and long-lived state

Questions:

- Can old and new clients coexist safely during a migration?
- Which metadata must be versioned alongside data and indexes?
- Which subscriptions survive schema drift, and which must fail closed?
- How do we recover from partially applied migration state?

Internal specs to revisit:

- [cross_schema_evolution_e2e_test.md](cross_schema_evolution_e2e_test.md)
- [type_change_lens_generation.md](type_change_lens_generation.md)
- [Schema Files](../../status-quo/schema_files.md)
- [self_referential_inherits.md](self_referential_inherits.md)

### 9. Backpressure, storage limits, and explicit degradation modes

Questions:

- What are our hard limits for OPFS, RAM, and subscription fan-out?
- Which behaviors degrade gracefully vs fail loudly?
- Which cleanups are best-effort and which are required for correctness?

Internal specs to revisit:

- [storage_limits_and_eviction.md](storage_limits_and_eviction.md)
- [noop_main_thread_storage.md](noop_main_thread_storage.md)
- [Browser Adapters](../../status-quo/browser_adapters.md)
- [benchmarks_and_performance.md](benchmarks_and_performance.md)

## Reference set to keep handy

- [Durability APIs](https://transactional.blog/how-to-learn/disk-io)
- [More about Linux file APIs and durability](https://www.evanjones.ca/durability-filesystem.html)
- [Martin Kleppmann's Hermitage writeup](https://martin.kleppmann.com/2014/11/25/hermitage-testing-the-i-in-acid.html)
- [Atomic Commit In SQLite](https://sqlite.org/atomiccommit.html)
- [Write-Ahead Logging (SQLite)](https://sqlite.org/wal.html)
- [Isolation In SQLite](https://www.sqlite.org/isolation.html)
- [File Locking And Concurrency In SQLite Version 3](https://www.sqlite.org/lockingv3.html)
- [How SQLite Is Tested](https://www.sqlite.org/testing.html)
- [TH3](https://www.sqlite.org/th3.html)
- [Write-Ahead Logging (PostgreSQL)](https://www.postgresql.org/docs/current/wal-intro.html)
- [WAL Configuration (PostgreSQL)](https://www.postgresql.org/docs/current/wal-configuration.html)
- [Concurrency Control (PostgreSQL)](https://www.postgresql.org/docs/current/mvcc.html)
- [Transaction Isolation (PostgreSQL)](https://www.postgresql.org/docs/current/transaction-iso.html)
- [ARIES](https://research.ibm.com/publications/aries-a-transaction-recovery-method-supporting-fine-granularity-locking-and-partial-rollbacks-using-write-ahead-logging)
- [Weak Consistency: A Generalized Theory and Optimistic Implementations for Distributed Transactions](https://publications.csail.mit.edu/lcs/pubs/pdf/MIT-LCS-TR-786.pdf)
- [Elle: Inferring Isolation Anomalies from Experimental Observations](https://arxiv.org/abs/2003.10554)
- [FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf)

## Questions to answer

- Which operations are only buffered, and which are actually durable after crash or power loss?
- Which user-visible events correspond to accepted, applied, flushed, durable, and replicated states?
- Which file replacement flows require syncing the parent directory as well as the file itself?
- Which lessons from Linux/native filesystems transfer to OPFS, and which do not?
- What isolation or session guarantees do current read/write/subscribe paths actually provide?
- Which guarantees are safe to document publicly in MVP, and which should remain explicitly best-effort?
- Which fault-injection and differential tests do we need before claiming stronger guarantees?

## Done when

- Each affected spec records any corrected assumptions.
- Public durability/isolation terminology matches tested behavior rather than intended behavior.
- We have at least one repeatable crash-injection plan and one repeatable isolation test plan.
- We have a short guarantees matrix for docs that states exactly what MVP does and does not promise.
