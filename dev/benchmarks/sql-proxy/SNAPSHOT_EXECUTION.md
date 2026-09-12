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

## Live and scheduling controls

Three fresh processes per mode, rotated against their own baseline. Sum includes
subscription registration, ingestion and delivery; it excludes subsequent reads.
Medians of each process's sum, milliseconds:

| Control                      | Larger dataset baseline / candidate | Smaller dataset baseline / candidate |
| ---------------------------- | ----------------------------------: | -----------------------------------: |
| Concrete live bindings       |                       4,750 / 4,248 |                        3,727 / 3,372 |
| Subscribe after installation |                       4,726 / 4,316 |                        3,745 / 3,409 |

Both effects are approximately 9–11%, not the multi-fold snapshot-read gain.
Concrete live bindings also lose ordinary prepared-read reuse: the larger
subsequent first read rises from ~695 ms to ~3,151 ms. Do not replace all prepared
subscriptions on the strength of the ingest result.

Late subscription moves most maintenance into initial hydration: larger ingest
falls from ~3,171 ms to ~1,617 ms, while registration rises from ~996 ms to
~2,684 ms. The total, not the shortened ingest phase, governs the conclusion.

A further public-API control selects Global tier with deferred local updates
(`JAZZ_HISTORY_GLOBAL_READ`). Every captured version is already globally accepted,
so output should remain equal in this fixture. This prices the additional
current/ahead overlay machinery used for Local reads; it does not justify
ignoring pending writes in production.

## Full-topology checkpoint

A single optimized all-memory checkpoint per mode yields 11,489 ms baseline,
11,511 ms concrete snapshots, and 11,660 ms concrete snapshots plus concrete
live subscriptions. All return 27,518 rows and pass the fixture assertions.
These samples establish no end-to-end improvement. The cold workload is driven
by subscriptions, so the one-shot result must not be presented as its speedup.

## Further guarded execution controls

With the `performance-experiments` feature, `GROOVE_DIRECT_INITIAL_WINNERS`
uses a compact group-to-borrowed-winner map for positive hydration input. It
retains the original arrangement for subsequent updates, including its original
ordering and byte tie-break convention. It avoids reconstructing an empty
before-image; signed and incremental paths remain unchanged. Initial read
measurements show only a small effect, with broader correctness checks pending.

`JAZZ_AUTH_JOIN_ORDER_EXPERIMENT` changes the association of the existing
permission/binding joins. Before: attach every binding to wide source rows,
then match authorized (row, route) proofs. After: join proofs to bindings on
all the same route fields, then join that compact relation to source rows by
row identity. Proof deduplication, route predicates, output identities and
multiplicity remain required. This is a guarded, unvalidated experiment, not
permission bypass or permission-result caching.

## Daytime checkpoint: full topology and fresh profile

At b060e7e234, six clean optimized all-memory Core → Edge → Client
cold-load runs gave baseline 11418/11305/11567 ms and authorization-join
experiment 11414/11280/11462 ms. Medians 11418 versus 11414 ms show no
meaningful end-to-end benefit. All runs verified 27518 output rows. The
retained-root policy-scope routing regression also passed. This remains a
research experiment, not a reviewed production change.

A fresh instrumented baseline captured 4188 cycle samples and 653960 phase
intervals without timeline drops. Exclusive wall spans across the three nodes
include approximately 1.74 s IVM update, 1.38 s storage apply/persist, 1.02 s
ingest outside nested phases, and 0.65 s witness decoding. These are
instrumented phase times, not clean benchmark comparisons. Sampled leaves
show byte hashing, comparison/copying, join index construction, field access
and allocation across several phases rather than one dominant winner walk.
Symbolization has substantial unknown frames; phase attribution is stronger
evidence than precise function percentages. A complete no-inline export was
used after the inline symbolizer proved slow and emitted addr2line warnings.

A next hypothesis, not yet implemented: version and replacement witnesses
repeatedly carry/decode static table and event-role information already known
by the terminal. Measure overlap and test a compact terminal representation
before proposing a broader ingest redesign. Existing counts suggest repeated
work but do not prove identical payloads or safely reusable decoded objects.

## Exact witness overlap measurement

`JAZZ_WITNESS_OVERLAP=1` is a testing-only diagnostic, not an optimization.
It compares complete decoded VersionRows keyed by query source and full
encoded version identity across one multisink delivery. It retains separate
version/replacement counts; equality includes the branch and record descriptor.
The map is discarded after each delivery. Diagnostic timings are unsuitable
for estimating a speedup because the comparison itself copies and indexes data.

The full all-memory cold scenario passed its 39-subscription output checks.
Across 109 nonempty multisink deliveries it decoded 104125 version witnesses
and 104128 replacement witnesses. All 104125 version witnesses had an exactly
equal replacement witness in the same delivery and source. The only unmatched
items were three replacement-only witnesses, each in a separate delivery.
Thus 104125 of 208253 decodes (approximately 50%) repeated a complete payload.
The first diagnostic, scoped to individual terminals, found zero overlap: the
two roles arrive in separate terminal batches. Any reuse must span those batches.

Code inspection agrees: the covered-source content terminals call the same
graph constructor with the same visible source and routing fields but different
event-kind literals. Both then use decode_typed_version_witness. Role lifetimes
remain independent (SourceFactOrigin); payload equality does not justify merging
add/remove state. This evidence supports sharing payload computation/decoding,
not deleting either semantic role. The previous instrumented decode phase was
about 0.65 s, so eliminating half of decoding alone suggests only about 0.3 s
of potential savings, before reuse overhead. Upstream computation and downstream
identity-copy savings remain unmeasured. No speedup is claimed.
