# Snapshot versus live-binding execution experiment

This follows #2894 and addresses #2895. The experiment changes operation lifetime,
not the permission predicates or historical winners returned by the lowered plan.

`JAZZ_DIRECT_SNAPSHOT_EXPERIMENT=1` (testing builds only) takes the original lowered
application-row graph and replaces each BindingSource leaf with one InlineRecords
row containing the exact descriptor-bound values from the normal strict binding
resolver. It then uses Groove's existing snapshot evaluator. All other operators
and carrier fields remain intact. No temporary live binding is registered or
retracted. DAG sharing is preserved during substitution.

The initial attempted alternative—forcing inline lowering through nested permission
plans—failed safely because nested routing contracts still expected binding sources
and claim fields. It is not the measured implementation. Its patch is preserved in
the local evidence archive; no fallback that drops permissions was introduced.

## First clean comparison

Three baseline and three candidate processes, in baseline/candidate/candidate/
baseline/baseline/candidate order; optimized native, memory storage. Every process
installs both actual captured datasets, queries all 39 tables twice and verifies
exact IDs and all application fields. Installation is unchanged. Milliseconds:

| Dataset             | Mode              | First read pass | Second read pass |
| ------------------- | ----------------- | --------------: | ---------------: |
| 46,740 transactions | Ordinary bindings |           1,289 |            3,082 |
| 46,740 transactions | Concrete snapshot |             668 |              612 |
| 27,518 transactions | Ordinary bindings |             926 |            1,800 |
| 27,518 transactions | Concrete snapshot |             512 |              502 |

The larger first pass improves ~1.9× and its second pass ~5.0×. This is a read-phase
result, not yet an end-to-end sync result. A separate candidate run also checked
that an identity without fixture memberships receives zero rows from every
policy-protected table, after the permitted identity's reads.

This control includes graph substitution/compilation and public row materialization.
It does not isolate retraction cost alone: removing live binding registration also
changes hydration and graph lifetime. Operator-level tracing is the next control.

## Further experiments in progress

`JAZZ_FROZEN_SUBSCRIPTION_EXPERIMENT=1` applies the same concrete binding leaves to
live subscription graphs and selects their normal public fields. It keeps live
incremental updates, permitting a separate test of parameter routing/lifecycle
versus incremental evaluation itself. This mode is not yet validated or timed.

With `cold-settle-attribution`, `GROOVE_WORK_NODE_TRACE=1` logs computed operator
nodes, input edges, row/byte counts and descriptors. `GROOVE_TRACE_UUID` additionally
counts matches in top-level UUID/nullable-UUID fields for one synthetic row ID.
A match may identify a row or a reference to it; field names must be inspected.
It does not recursively inspect nested records or imply every carrier is captured.
These diagnostic runs must never be reported as clean timings.

The test-only substitution uses recursive traversal with a DAG memo. It is not
production-ready for arbitrarily deep graphs; a production version needs a bounded
iterative traversal, focused semantic/lifecycle tests and independent review.
No storage/wire changes or release-readiness claims are made.

## Operator trace interpretation

Separate instrumented runs count outputs of computed operators, including
retractions. These are not unique rows, allocation bytes, or clean timing runs.
For the larger dataset's second mixed-query pass, ordinary bindings produce
5,188,900 intermediate records / 2,498 MB of encoded outputs; concrete snapshots
produce 1,259,970 / 566 MB. Ordinary execution has 17,080 Tick node executions in
addition to hydration; concrete snapshots have none.

A tracked row from the largest child table appears with negative weights during
later queries on other tables. For example, the second `res_m_access_edges` query
processes 445,716 Tick output records and only 2,136 hydration records. This is
consistent with the code's queued binding-retraction lifecycle: releasing a
snapshot binding queues removal, later hydration flushes that removal through
the graph. Immediate rebinding can cancel it, so repeating a lone query does not
reproduce the cost of the full mixed-query pass.

The remaining large query still produces about 1.08 million intermediate records
in the concrete control. Its 43,000 source rows carry roughly 574 bytes each
through current-winner selection and early operators, before yielding 23,831
permitted output rows. Removing temporary bindings does not eliminate that work.

The corrected live-subscription control reuses Groove's existing bound-terminal
normalization, including root collector routing. It passes both full fixture
oracles. Repeated timing comparisons are in progress.

`JAZZ_HISTORY_LATE_SUBSCRIBE=1` is a separate harness-only control: install the
identical capture before registering the same live queries, then require their
complete initial output. It prices initial snapshot construction versus empty
subscription setup plus incremental maintenance. It does not implement safe
propagation deferral for an existing production subscription or permit dropping
already-observable intermediate updates.
