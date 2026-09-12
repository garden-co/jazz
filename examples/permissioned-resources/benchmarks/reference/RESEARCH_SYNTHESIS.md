# Cold-load cost synthesis

This report combines the synthetic SQL references (#2888/#2889), unchanged-engine
memory topology (#2890), slim encoded-memory reference and work budgets (#2891),
and isolated installation/read controls (#2894). It is a research result, not a
claim that a replacement engine or release-ready optimization has been built.

## Main conclusion

Shallow history is inexpensive in the reference, but Jazz's implementation pays
substantial costs both installing that history and evaluating/maintaining queries.
The evidence does not support attributing the entire gap to history, durability,
encoding, or any other single mechanism. Our strongest larger opportunity is to
reduce how much maintained relational machinery a snapshot load invokes, while
preserving the same permission and history semantics.

## What was compared

All workloads use anonymized relationships and permissions, 46,740 physical rows,
39 queries and 27,518 visible output rows. The histories contain one accepted
version per row and no parents. This deliberately prices shallow-history overhead;
it does not price arbitrary conflict resolution or long ancestry chains.

| Experiment                                              | Measured time | What it establishes                                                                       |
| ------------------------------------------------------- | ------------: | ----------------------------------------------------------------------------------------- |
| SQLite current-row queries                              |        ~31 ms | A specialized relational read floor; not sync                                             |
| SQLite history queries                                  |        ~75 ms | Shallow history need not make reads intrinsically expensive                               |
| PostgreSQL history queries                              |        ~57 ms | Same direction with another mature engine                                                 |
| SQLite durable Core→Edge→Client proxy                   |        2.84 s | Real payload persistence, indexes and query work substantially raise the read-only cost   |
| PostgreSQL durable topology proxy                       |        4.84 s | Engine choice/workload implementation matters; not a universal sync tax                   |
| Actual Jazz, RocksDB everywhere                         |       12.65 s | Full existing native topology                                                             |
| Actual Jazz, memory receivers                           |       11.51 s | Removing receiver disk persistence alone saves ~9%                                        |
| Actual Jazz, memory everywhere                          |       11.21 s | ~11% improvement; generic storage/ingestion work remains                                  |
| Slim encoded-memory topology reference                  |        269 ms | Specialized relationships and real encoded payloads, without general maintained execution |
| Slim plus explicit shallow history/fate/current indexes |        307 ms | Adds ~38 ms; still not general Jazz merge semantics                                       |

These are different controlled experiments with documented boundaries, not one
perfectly interchangeable leaderboard. SQL query-only numbers exclude loading;
SQL sync proxies include ingestion and statistics preparation. The slim proxy
omits the full wire/receipt lifecycle and arbitrary maintained graphs. Exact IDs,
application fields, payloads and the applicable permission checks are verified.
See RESULTS.md, SYNC_RESULTS.md, MEMORY.md, SLIM_MEMORY.md and HISTORY_COST.md.

## Separating installation from queries

For 46,740 captured transactions, a fresh memory-backed Jazz receiver installs in
1.71 s without application subscriptions. A first permissioned read pass then
costs 1.43 s. With 39 subscriptions initialized on the empty receiver, installation
costs 3.41 s and delivery another 0.62 s; initialization itself costs 1.00 s.
Subsequent reads against those retained queries cost ~0.71 s. The smaller capture
shows the same direction. These phase controls exclude wire admission and scope
bookkeeping and use a history-complete receiver; do not add them as a prediction
of production topology latency.

The second unretained read pass is slower (~3.29 s). A profile/code walk found
`flush_pending_binding_retractions`: a later snapshot may first propagate removal
of a previous temporary query binding through the maintained graph. This is a
concrete investigation lead, not yet an isolated explanation of the full slowdown.
Stack truncation/async inlining means ancestor-selected sample percentages must
not be presented as complete wall-clock phase shares.

The active control also exposed a correctness bug (#2892/#2893). Two incremental
evaluation continuation paths dropped pending terminal edits/order windows. Seven
subscriptions passed, an eighth caused missing group output; preserving that state
made all 39 pass on both datasets for three repetitions. The fix remains a draft
pending a focused public-API regression and broader verification.

## Work amplification

The full engine requested ~109 million allocations/reallocations and 27.7 GB of
cumulative allocation bytes, versus ~2.53 million and 363 MB in the earlier slim
reference. These are churn counters, not peak memory or bytes copied.

Only 83 storage batches were observed across receivers, with one main bulk install
each. Nevertheless, the Client-sized ingest generated ~335,000 KV sets for 27,518
rows: ~141,000 index writes, ~111,000 metadata writes and ~27,500 each for history,
current and change records. The three nodes processed ~2.09 million projection
inputs and requested ~799 MB of new projection output bytes. Client-side right
join inputs exceeded 1.13 million. Batching is already present; reducing the amount
of work inside a batch matters more than merely grouping API calls.

In the isolated installer profile, ancestor-selected samples include ~35.5% batch
application, ~19.6% persistence and ~11.1% persisted-history head recomputation.
Nested functions overlap; these percentages are diagnostic, not independent costs
to sum or a complete allocation of wall time. Even memory persistence includes
owned-key/value manipulation, layout dispatch and B-tree operations.

A deliberately incomplete counterfactual omitted the post-write head rebuild on
fresh receivers. The median sum of both install phases fell from 2.587 s to 2.434 s
in six rotated fresh processes (~6%, noisy). All shallow output checks still passed;
this does not validate merge-head semantics for general history. Removing this
entire pass plainly does not close most of the gap. See HISTORY_COST.md for ranges.

## Derived theses and estimated effort

Effort estimates below are engineering estimates for one focused engineer, not
measured delivery promises. They exclude unrelated release work.

| Direction                                                                             | Why it is plausible / likely bound                                                                                                                                                                                | Next decisive test                                                                                                                           | Estimated effort                                                                        |
| ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Carry proven fresh-install facts through history/head maintenance                     | Avoid repeated lookup and decoding of data just inserted. Existing full-topology attribution priced post-write head rebuilding at ~0.50 s, so this alone cannot produce a multi-fold speedup.                     | Counterfactual omission, then equivalence tests for existing history, partial ancestry, fate changes and duplicates.                         | 1–2 days experiment; 3–7 days safe implementation/tests if the invariant is expressible |
| Reduce generic index/metadata write amplification                                     | Hundreds of thousands of KV operations remain despite very few physical batches. Avoid construction/copying of unnecessary keys and values before reaching the backend.                                           | Per-family ablation and allocation attribution, preserving reopen/current/history and exact-match conflict checks.                           | 2–4 days diagnosis; 1–2 weeks for a bounded redesign                                    |
| Make temporary query bindings cheap to dispose                                        | A read should not routinely pay a second relational evaluation merely to remove its own private parameters. Sharing with live bindings makes naive deletion unsafe.                                               | Compare snapshot-only queries with scoped binding state and explicit cleanup timing; verify other subscribers are unaffected.                | 2–3 days prototype; 1–2 weeks implementation and lifecycle tests                        |
| Give snapshot execution a bulk path, retaining delta machinery for actual changes     | Most of this fixture is first hydration. The slim/SQL references show a large gap without changing shallow-history or permission outputs.                                                                         | Same lowered graph/operators, transient snapshot state versus fully arranged hydration; preserve complete support/negative-evidence outputs. | 3–5 days prototype; 2–4 weeks to integrate if successful                                |
| Keep immutable encoded rows once and make internal relations carry compact references | Large intermediate volume, allocation churn and repeated metadata fanout remain. This differs from rejected universal reference counting (#2848): only long-lived/shared carriers should pay ownership machinery. | Account exact retained copies and replace one high-amplification carrier; measure full wall time, not allocations alone.                     | 3–5 days prototype; 2–4 weeks depending on lifetime and storage boundaries              |
| Separate query-output materialization from authorization/support bookkeeping          | Full support closure may be needed, but repeated author/schema/version reconstruction and wide relational carriers are implementation choices.                                                                    | Attribute repeated metadata rows to specific lowered operators; prove output and supporting-version equivalence.                             | 2–4 days attribution; 1–3 weeks bounded lowering changes                                |
| Delay downstream durability / use memory-first cache persistence                      | Architecturally coherent for resyncable data, but unchanged-engine memory comparison saved only ~9–11%. It is not the leading speed thesis now.                                                                   | Revisit only after CPU/write amplification falls; separately test local unsynced edits and crash recovery.                                   | 1 week prototype; several weeks production scheduling/recovery design                   |

The first two are bounded incremental work. The middle directions attack execution
and representation, where multi-fold improvement is plausible but unproven. None
requires separate Core versus Client query semantics: a shared engine can choose
snapshot or incremental execution based on operation lifetime and available state.
Durability is an orthogonal role-dependent policy, not a reason to fork evaluators.

## Recommended order

First finish the continuation correctness regression. Then isolate temporary
binding cleanup and the Client's metadata join fanout before changing another
projection implementation. In parallel conceptually (not additional agents), design
a bulk snapshot path using the same operators and exact supporting-row contract.
Use fresh-process rotated timings, allocation/work budgets and the 27,518-row
oracle to decide whether either deserves integration. Small head/index improvements
are useful, but should not displace the larger execution experiment merely because
they are easier to implement.

No evidence currently justifies weakening permissions, dropping required witnesses,
or removing history semantics to meet the cold-load target. The references suggest
there is substantial implementation headroom first. A sub-second full topology is
not established by any of these controls; the specialized reference is a direction
and lower comparison point, not a promised Jazz result.
