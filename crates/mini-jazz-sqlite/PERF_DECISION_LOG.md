# SQLite Core Performance Decision Log

## 2026-05-26 21:06 PDT

Started the performance benchmark sprint branch from
`codex/mini-jazz-sqlite-spec` after drafting the scenario list. First
implementation target: create a benchmark/reporting harness that measures
whole-topology, client-perceived latency before adding increasingly realistic
policy and cache-mode scenarios.

## 2026-05-26 21:07 PDT

The current mini runtime already supports query-scoped export/apply for
`eq_top_created_at_desc`, but not arbitrary user-column ordering like
`updated_at DESC`. First benchmark slice will use system created-time ordering
to get whole-topology metrics flowing, then add exact user-column ordered
pagination as the next de-risking step.

## 2026-05-26 21:09 PDT

First benchmark example works for a small core-only cold topology. A 1k-row
seed with 200 target-owner rows and page size 50 produced 200 synced history
rows but 1k synced transaction rows, because query-scope export currently
includes all transaction metadata. This is an important early whole-stack
finding: scoped history without scoped transaction metadata will still make
cold query bundles grow with total database history.

## 2026-05-26 21:10 PDT

Scoped query bundles now filter transaction metadata down to the history/read
records actually exported. The same 1k/200/page-50 run dropped transaction
records from 1k to 200, bundle bytes from about 298 KB to 124 KB, and
api-to-first-result from about 214 ms to 149 ms. This was a good example of a
benchmark immediately exposing a missing sync-scope feature.

## 2026-05-26 21:13 PDT

Added a first-pass user-column ordered top-N feature:
`WHERE field = value ORDER BY order_field DESC LIMIT n`. Covered it with a
whole-system durable refresh test that verifies page-boundary repair after a
new higher-ranked row arrives while the worker is closed. The implementation
still materializes and sorts rows in Rust for reads; the next performance step
is to lower this path into SQLite `ORDER BY ... LIMIT` over projection/index
tables so the benchmark matches PR #898's intent.

## 2026-05-26 21:15 PDT

Ran the first larger smoke profile: 10k total documents, 1k target-owner rows,
page size 50, core-only cold through durable edge/worker and memory tab.
Result: about 787 ms api-to-first-result; export was about 156 ms, each apply
hop about 208-213 ms, and final tab query about 0.5 ms. Current bottleneck is
bundle export/apply volume and repeated hop materialization, not local final
query latency.
