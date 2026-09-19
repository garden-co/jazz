# Policy-scoped documents

A small native example workload: 100 document owners, grouped into 25
organizations of four owners. Every document has an owner, organization,
completion flag, unique update timestamp, and title. The third owner in each
organization is its admitted member. The other owners can still read their own
documents under the direct-owner branch of the policy.

This example and its benchmark are adapted from Tobias Lins's
[#2996](https://github.com/garden-co/jazz/pull/2996). They use invented public
fixture data, not an adopter schema. See
[#2985](https://github.com/garden-co/jazz/issues/2985) and the performance log
[#2913](https://github.com/garden-co/jazz/issues/2913).

## Measurement contract (fixture revision 2)

Each sample reads the first page of 50 documents ordered by descending
`updated_at`, with a literal equality on `owner_id` or `org_id`. All reads use
the same admitted non-SYSTEM identity (owner 2), Global tier, deferred local
updates, LocalOnly propagation, and exclude deleted rows. The database is
history-complete. The original one-shot cases measure no transport or
subscription delivery.

Compare identical data, indexes and queries across an explicit unrestricted SELECT policy,
owner-only policy, and owner OR inherited organization membership. The owner
query returns 50 rows in all three arms; the org query has a matched unrestricted
and inherited-policy pair. Organization membership is fixed as scale grows.
The unrestricted control uses `PolicyExpr::True`: omitted policies now deny
non-SYSTEM reads under `INV-RLS-15`. Historical `policy_free_*` benchmark IDs
remain stable, but their intended contract is an unrestricted result, never an
empty denied read. Revision 2 adapts the fixture to current authorization semantics;
data, queries, identities, expected rows and timing boundaries are unchanged.
Retain the explicit revision boundary when comparing to revision 1: the control
now compiles an allow-all policy instead of relying on an implicit grant. Do not
compare a missing-policy empty result as an equivalent workload.

Owner 3 owns the newest documents in organization 0, so the measured organization
page requires inherited access; it cannot pass through ownership alone.
The declared indexes on owner, organization and timestamp are independent
single-column indexes, **not** an ordered compound index. A requested page size
is not evidence of bounded scan work.

CodSpeed runs the 10k and 100k table scales, three samples of one first query
each, with mimalloc and RocksDB WalNoSync. Each input reopens the seeded store
and prepares the query outside the timer. Only `all_for_identity` execution and
result construction are measured. Teardown, seed transactions, public query
preparation, and storage-counter extraction are excluded. This is runtime-cold,
not OS-page-cache-cold or end-to-end reopen latency. Divan receives result rows
as its output. No warm-query cache samples are mixed in.

The `subscribe_` cases reuse the fixture, queries, identities, allocator and
fresh-runtime boundaries, but measure `subscribe_for_identity` through its first
published page, including native runtime progress. They use **Local tier with
immediate local updates**, not Global: a standalone Global subscription requires
an authority settlement receipt this no-network fixture does not supply. The
seed has no pending writes, but these remain separately named endpoints, not
interchangeable timing samples. Subscription finalization is outside the timer.

The standalone receipt reports seed, reopen, public preparation, query and
close phases separately, plus **logical** storage read counters (not physical
disk I/O). It sweeps limits 1/10/50 and includes the owner-policy org diagnostic.
Throughput means complete page queries/s, not scanned rows/s.

The contributor's original benchmark timed reopen + query + close, omitted
exact-result assertions, and assigned organization groups as a function of
table size. These corrections deliberately establish a **new baseline**;
do not compare its old milliseconds directly with revision 1.

## Run

```sh
cargo test -p jazz-example-policy-scoped-documents-benchmark --test pages
cargo check -p jazz-example-policy-scoped-documents-benchmark --all-targets
cargo bench --profile perf -p jazz-example-policy-scoped-documents-benchmark --features jazz-benchmark-guard/mimalloc --bench walltime
cargo run --profile perf -p jazz-example-policy-scoped-documents-benchmark --features jazz-benchmark-guard/mimalloc --bin policy-read-receipt -- 10000
```

Record checkout SHA, executable hash, features, allocator and host alongside
local receipts. Do not time while builds/tests/profilers compete on the host.
The `benchmark` label enables the CodSpeed native workload matrix. Baseline
first, planner changes in a later PR. The fixture changes no storage or wire formats.

Correctness CI checks exact ordered IDs against an independent policy oracle,
including direct owner, inherited-only access, non-members, empty pages and
limits larger than the available scope.
