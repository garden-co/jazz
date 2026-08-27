# jazz — Specification · 16. Maintained subscription views

## Overview

This chapter names the target serving architecture for query-driven sync:
**every live peer subscription is maintained by groove**. A serving node should
not have a second production query engine for subscriptions. It may keep semantic
evaluators as oracles, debugging aids, or temporary migration scaffolding, but
the protocol-facing steady state is a groove subscription whose terminal stream
contains enough information to produce `ViewUpdate`s incrementally.

The old implementation name for the current prototype is not product
terminology. The intended abstraction is a **maintained subscription view**.

Invariant digest:

- `groove/SPEC/INVARIANTS.md::INV-INC-1`: Incremental delivery invariant (mechanism law). For any maintained view, the work performed to ingest, apply, and publish a change — including snapshot assembly, diffi...
- `groove/SPEC/INVARIANTS.md::INV-MV-1`: No state that feeds a maintained view may change without that maintained view observing the change, either as ordinary deltas through the runtime or as an explicit reb...
- `INV-SYNC-23`: A serving peer MUST reject a capability-gapped live subscription with SyncMessage::SubscribeRejected addressed to the requested SubscriptionKey; the rejected subscript...

## Details

### 16.1 Contract

For a peer identity, query shape, and binding, a maintained subscription view
MUST lower to a groove graph whose terminal rows describe:

- result membership: visible typed result-member additions and removals;
- matched include path rows and join witnesses required for the result set;
- version payload witnesses: content/deletion versions that may need to be
  shipped when a result becomes visible;
- replacement witnesses: current content/deletion winners needed when visible
  rows update, delete, restore, or become newly readable;
- policy witnesses: rows from read-policy filters, joins, and recursive
  reachability that can grant or revoke visibility without changing the output
  table row.

The peer state machine consumes that terminal stream, updates its per-peer
shipped/result indexes, and emits `ViewUpdate`s. It MAY deduplicate already
shipped complete transaction payloads into
`peer_payload_inventory.complete_tx_payloads`. View-complete exclusive payloads
are coverage facts for the maintained subscription view only; they do not become
complete transaction payload refs. The peer state machine MUST NOT answer a live
subscription by running an independent semantic scan.

A maintained peer publication MUST retain the complete semantic read view and
tier resolved at admission. Initial hydration, suspended retries, incremental
evaluation, authoritative reconciliation, settlement, and authorization
progress all use that same immutable context. An explicit replacement installs
one complete replacement context, and every continuation uses that retained
context; reconciliation MUST NOT substitute the default read view or reconstruct
selectors from the opaque `ReadViewKey`.

`groove/SPEC/INVARIANTS.md::INV-INC-1` is the mechanism law for this chapter:
maintained-view ingestion, application, publication, snapshot assembly, diffing,
and subscriber delivery are bounded by the size of the change and affected keys,
not by accumulated view state. `groove/SPEC/INVARIANTS.md::INV-MV-1` and the maintained-vs-one-shot
differential oracle prove observable equivalence; they do not justify a
full-state rebuild or full-state diff on the maintained path.

The high-level `Db` facade follows the same boundary for every live
subscription tier. Local subscriptions are desired and first-class: they are the
application/UI-facing maintained view over the local read frontier, including the
node's own pending committed writes. Edge and global subscriptions are maintained
views over their corresponding accepted-state frontiers, with additional
settlement/completeness requirements. Tiers select the source/frontier
expression and runtime consumption policy; they must not select a different
query engine. A facade-local
full `query_rows` refresh/diff loop is permitted only as explicitly named
migration scaffolding for alpha-compatible local live reads, not as the target
semantics.

The maintained view is a consumer preset over the shared lowered query program.
It requests result-membership facts, path/correlation facts,
payload/replacement/version witnesses, policy witnesses, and settled-frontier
facts as needed. For peer sync, those inputs are the two manifest-bound families
from ch. 8 §8.4.1: canonical authored history and authority-maintained
correlation/admission/settlement facts. The receiving node installs only that
closure and runs the identified authorized residual program locally. Only the
local terminal maps to an application subscription event. App-row projection
and internal fact emission are separate outputs of the same program; projected
terminal rows must not become a second diffing path or peer-replicated truth.

The authority's maintained view remains essential even though the receiver also
runs IVM: it decides which canonical input facts are safe for this peer, emits
opaque admission facts where policy evidence is hidden, and certifies a complete
settled closure manifest. That manifest binds the authorized residual program:
the receiver does not independently rerun authorization, infer hidden joins,
supplement it from unrelated local history, or accept a server-projected
terminal cache as a substitute for either fact family. Thus simple roots, join
results, and nested array relations all share one rule: manifest-admitted inputs
enter the local graph; terminal rows leave it.

### 16.1.1 Application subscription delta contract

The application-facing subscription stream is a stream of result deltas. A
delta contains row additions, row updates, row removals, ordered-position data
for ordered shapes, relation edge additions/removals where the query includes
relations, settled/tier metadata, and a `reset` flag. There is no separate
snapshot event type. The first delivery for a fresh subscription is a reset
delta from the empty result set; reducing that delta yields the initial view.

Consumers own the materialized result set. The contract is that applying the
delta reducer to events in stream order produces the same result as a one-shot
read at the corresponding frontier. Non-reset deltas do not carry a complete
`current`/`all` result. Reset deltas replace all previously reduced state and
then apply their additions; chunked initial hydration is coalesced below this
contract and presents as one logical reset delta whose settled state is reported
at the final chunk boundary.

Windowed ordered shapes expose window-membership transitions as ordinary
deltas. If a row leaves a finite `order_by`/`limit` window and another row enters
because of that boundary movement, the stream emits the corresponding remove
and add/update changes even when the entering row's stored cells did not change.
Per-event work is expected to be O(changed rows), not O(result set); this is the
application-surface form of `groove/SPEC/INVARIANTS.md::INV-INC-1`.

### 16.2 Policy composition

For non-system peers, the maintained graph begins from the shared
policy-composed lowered-query core from ch. 14: the user query intersected with
the table read policy under the authenticated peer identity, lowered over the
subscription's visible-current base source. Claim operands are rewritten to
server-derived parameters before lowering. `claim("user")` is the stable subject
identity. Recognized claims that do not yet have a runtime value fail closed.

Policy composition is not merely an output filter. Policy dependency tables are
part of the maintained graph. If a membership row, access row, join witness, or
recursive edge row changes visibility, the maintained view must emit the same
net result-set transition as a full rehydrate over the same committed history.
Maintained subscription views are augmentations over that core: they add
terminal membership rows, version/replacement witnesses, and peer-facing
dedup/reset semantics, rather than defining a separate query evaluator.

### 16.3 Recursive reachability

`ReachableVia` clauses lower to groove recursive graphs everywhere they appear:
user queries, read policies, write permission scopes, matched-path witnesses,
and replacement witnesses. Jazz does not branch on groove's internal recursive
execution strategy. Groove owns the choice between incremental recursion and
full recomputation when non-monotone deltas appear.

### 16.4 Production fallback boundary

Full-recompute paths are explicit test/oracle debt, not an alternate production
semantics. Once a shape has been accepted as a supported maintained
subscription, failures in maintained setup, delta application, or maintained
bundle serialization MUST surface as errors/resets on the maintained
subscription surface rather than silently repairing the stream by running a
peer-local semantic full recompute.

A forced full-recompute path is allowed only for tests, semantic oracles,
diagnostics, or an explicitly named migration harness. Such use must be:

- observable through a deterministic metric;
- covered by a regression test that states why the current maintained graph
  cannot yet express the delta safely;
- bounded to a named event kind or maintained-delta failure mode.

The target budget is zero protocol-facing semantic full recomputes for ordinary
query subscriptions. Test-only forced full-recompute paths and semantic oracle
helpers are allowed, but they must not be the normal peer serving path.

Unsupported subscription shapes are a separate capability gate. If a query
shape is outside the maintained-subscription surface, the server MUST reject the
live subscription loudly, or route it through an explicit non-subscription /
read-only API. It MUST NOT accept the live subscription and serve it by semantic
full recomputes, skip the maintained path silently, or install a best-effort
subscription with different semantics.

On a serving sync connection, capability-gapped live subscriptions fail at the
subscription boundary, not at the serving tick boundary. The server compiles the
maintained view for a `Subscribe` request before registering that usage-site
subscription as active. If the compile fails with a maintained-subscription
capability gap, the server emits `SubscribeRejected` for that exact
`SubscriptionKey`, leaves the subscription inactive, and continues serving every
other subscription on the connection. The rejection reason is the stable
protocol reason `UnsupportedShapeCapability`; detailed lowering reports stay
internal compiler vocabulary and are mapped to human-readable diagnostics at
the serving boundary (`INV-SYNC-23`).

### 16.5 Current known gaps

The current maintained-subscription surface supports ordinary live query
subscriptions whose lowered policy-composed shape can be maintained by groove,
with the strongest production coverage on the global frontier. The target
surface is tier-agnostic: local, edge, and global subscriptions use the same
lowering and maintained terminal contracts, differing only in source/frontier
selection and settlement/completeness rules. Supported maintained shapes include
unordered `limit(1)` with offset `0` lowered through `ArgMinBy` over `row_uuid`,
and ordered windows lowered through groove `TopBy`. Ordered windows preserve
the user `order_by` terms, append `row_uuid` as the stable tie field, and retain
the requested finite `offset + limit` window or unbounded ordered suffix
incrementally.

Known gaps fall into distinct buckets:

Staged convergence of read sources:

- same-table visible-current schema projection over compatible current
  partitions installs a maintained groove graph for a single root source with
  canonical natural column add, drop, copy, and rename lenses; table renames,
  projected joins, arrays, reachable traversal, and multi-hop table lineage
  remain unsupported until source-aware lowering exists;
- historical/time-travel reads with filters and joins use shared clause lowering
  over historical current rows; historical reachable is unsupported until
  source-aware reachable lowering exists;
- one-shot settled reads may materialize and post-process the shared shape
  without installing a maintained terminal stream.

These are staging gaps in base-source lowering and serving mode, not permission
to fork the query algebra. As each source becomes groove-representable, it should
reuse the same policy-composed core and differ only in base source and whether a
maintained subscription augmentation is installed.

Maintained-lowering gaps:

- aggregate lowering is not yet represented as a groove-maintained graph
  fragment for subscription deltas;
- `array_subqueries` in live subscriptions must be maintained as part of the
  lowered query program: parent membership, child relation material, relation
  edges, ordering/limit boundaries, and policy visibility must converge with the
  corresponding one-shot relation snapshot, and serving code must not compensate
  by recursively subscribing to coarse child shapes for sync coverage;
- relation delivery is covered by the active
  `groove/SPEC/INVARIANTS.md::INV-INC-1` mechanism canary in
  `crates/jazz/tests/incremental_delivery_canary.rs`. The canary is at the
  `Db` facade level because the current `jazz-tools::JazzClient` subscription
  surface rejects relation/include queries as non-simple table queries;
- application-column projection is a materialization concern layered over the
  maintained membership/version stream; projected subscription payloads must not
  become a second diff engine;
- predicate-policy lowering is incomplete where read policies still require
  direct semantic evaluation instead of a lowered maintained policy graph.

Window limitations:

- root `limit`/`offset` windows without explicit `order_by` are supported by
  injecting the ch. 6 default ascending row-id order before lowering through
  `TopBy`; this applies to both prepared bindings and policy-routed maintained
  views. Explicit order keys retain ascending row id as their stable tie-break.
- The same rule applies to every non-recursive relation-local window, including
  an `array_subquery`: without child `order_by`, each parent/correlation group
  is ordered by ascending child row id before its child-local `offset` and
  `limit`; explicit child keys retain ascending child row id as their stable
  tie-break. The child `TopBy` is partitioned by its correlation/parent key, so
  a child in one parent group cannot displace a child in another. This is the
  ch. 6 structured-child ordering contract made executable in maintained
  lowering, not source scan order.
- A bounded window over a recursive closure remains rejected loudly. A recursive
  graph produces closure tuples across seed/step iterations and depths, and its
  current public relation does not carry one source-child row id that totals
  those tuples. Ordering per iteration or per depth would be a different,
  unstable observable contract, while adding a closure-wide occurrence identity
  is larger than this maintained-window change. We therefore define no invented
  recursive default order here; recursion must gain an explicit closure identity
  and ordering contract before a recursive window can lower through `TopBy`.

Maintained error debt after a supported maintained path fails:

- some maintained-view delta cases still require conservative handling for
  replacement witnesses and unsupported exclusive sibling cases. Exclusive
  transaction deltas are not a broad full-recompute class: maintained views may
  ship view-scoped partial bundles when only some writes in an exclusive
  transaction match the maintained view;
- `current_rows_update` is not yet fully represented as the same maintained
  query-subscription abstraction for every role.

Each gap should either become a groove-maintained graph fragment, surface as a
maintained subscription error/reset, or remain documented as an explicit
non-subscription/read-only surface. Production peers must not mask these gaps
with semantic full-recompute repairs.

Implementation status for `array_subqueries`:

### Structured terminal patch contract

A maintained structured terminal does not publish a freshly encoded nested
root for every descendant change. Its incremental output is a typed structural
patch addressed by the public root `ResultKey` and an alternating path of
collection-field names and descendant `ResultKey`s. The operation vocabulary
is deliberately about terminal values, not relations:

- insert, update, and remove a root or descendant value;
- move an existing value to an explicit position in its ordered collection;
- replace a window by the equivalent ordered insert/remove/move operations;
- reset only for initial hydration, reconnect/resubscription, or an explicitly
  advertised loss of incremental continuity.

Groove owns the selected ordered keys and scalar payload for every collection
slot. It emits the smallest affected path operations without re-encoding an
unmodified ancestor. One-shot reads and initial hydration still render complete
terminal rows from that state. The transport carries this generic terminal
operation vocabulary unchanged; a client applies it to its hydrated terminal
tree and performs no joins, relation-edge interpretation, or query assembly.

This split is required by `INV-INC-1`: replacing a parent containing 20,000
children after one child insert is observably correct but still performs work
proportional to accumulated state. Root-addressing alone is insufficient unless
the changed descendant path is preserved through Groove evaluation and the
subscription carrier.

- Subscription opening: direct `array_subqueries` are accepted at the `Db` facade
  and sync registration surfaces, covered by
  `array_subquery_live_subscription_tracks_child_edges` and
  `subscriber_connection_accepts_array_subquery_register_shape_for_serving_subscription`.
- Direct child maintenance: child insert, update, delete, correlation-key moves,
  zero-child arrays, and parent removal are verified by
  `array_subquery_live_subscription_tracks_child_edges` and
  `array_subquery_subscription_reflects_child_mutations_and_parent_removal`.
- Ordering and slicing inside the subquery: an ordered child relation with a
  finite limit boundary is verified by
  `array_subquery_subscription_updates_child_order_limit_boundary`; other
  ordering/slicing combinations are not yet separately named as maintained
  coverage.
- One-shot equivalence: the maintained first delivery matches the one-shot
  relation snapshot for the covered ordered direct shape in
  `array_subquery_one_shot_and_maintained_subscription_are_equivalent`.
- RLS interaction on the nested table: policy filtering of child array contents
  per identity is verified for relation snapshots by
  `array_subquery_policy_oracle_filters_child_array_contents_per_identity`; live
  maintained policy-change deltas on child policy dependencies need explicit
  named coverage before being advertised more broadly.
- Nesting depth: nested `array_subqueries` are validated and registered for
  upstream coverage by `global_subscription_registers_array_subquery_upstream_coverage`
  and `array_subquery_attachment_registers_upstream_coverage`; maintained
  materialization and delta semantics beyond the direct child level are not yet
  separately verified by a named black-box subscription test.
- Include/requirement mode: optional array subqueries are covered by the direct
  maintained subscription tests. `AtLeastOne` and
  `MatchCorrelationCardinality` are covered for relation snapshots by
  `relation_snapshot_filters_unreadable_children_and_required_parents` and
  `array_subquery_match_correlation_cardinality_requires_every_referenced_member`;
  live maintained subscription coverage for those requirement modes is not yet
  separately named.
- Tier: local/default `Db` subscriptions are covered by the direct maintained
  subscription tests. Global/remote registration and hydration coverage is
  covered by `global_subscription_registers_array_subquery_upstream_coverage`,
  `array_subquery_attachment_registers_upstream_coverage`, and
  `array_subquery_remote_subscription_hydrates_edge_referenced_child_rows`.
  Edge-tier maintained array-subquery semantics are not yet separately named.

### 16.6 Aggressive maintained support: ordered windows and `Aggregate`

The next maintained-subscription expansion should be expressed as new groove
operators or maintained graph fragments, not as Jazz-side refresh/diff loops.
Current and next Jazz lowering targets are:

- `order_by ... limit ... offset` lowers to groove `TopBy`; missing `limit`
  means an unbounded ordered suffix after `offset`, not a Jazz-side full
  recompute.
- `group_by` and scalar aggregate projections lower to groove `Aggregate` when
  every aggregate function is in the maintained operator surface.
- "latest per object" and unordered `limit(1)` keep their narrower existing
  lowerings (`ArgMaxBy` current-row state and `ArgMinBy` over `row_uuid`) unless
  a general ordered window is required.

`TopBy` is the target for ordered result membership. The lowering must make the
order total and replay-stable: Jazz appends stable identity fields, normally
`row_uuid` or another declared primary identity, as deterministic tie fields
after the user `order_by` terms. If the user order is not unique, equal user keys
are still delivered in the same order on every node. Updates lower through the
ordinary groove `-old, +new` rule, so a changed sort key can produce both a
leave and an enter, plus boundary churn for rows displaced at the retained
window edge.

`TopBy` terminal deltas are membership deltas over the retained window, not
whole-window replacements. A row whose rank changes but remains inside the
window does not affect Jazz result membership unless the future API explicitly
projects rank metadata. This keeps `ViewUpdate.result_member_adds/removes`
aligned with the settled typed result-member model.

`Aggregate` is the target for grouped summaries. Jazz lowers each group to a
stable result-row identity derived from the group key and lowers scalar global
aggregates to a single synthetic group identity. The terminal row contains the
group fields and aggregate values; result membership appears when a group first
has output and disappears when the group no longer has output. The group fields
and aggregate values travel as a `ResultPayload` program fact keyed by the
synthetic result member. A changed summary is represented as replacement of the
aggregate result row: the maintained stream must provide enough payload and
replacement witness information for the peer state machine to emit the same net
`ViewUpdate` as a full rehydrate.

Aggregate functions are capability-gated by groove support. Maintained Jazz
subscriptions should initially accept only deterministic, retractable summaries
such as count, numeric sum, min, and max, with deterministic witness ties owned
by groove. User-defined aggregates and approximate aggregates stay outside the
maintained subscription surface until their replay semantics and payload shape
are specified.

Decision, Anselm 2026-08-07: the empty global aggregate row is **inside** the
maintained surface, and follows SQL. A scalar global aggregate over no input
rows delivers a present row reporting `0` for `count` and `NULL` for `sum`,
`avg`, `min` and `max` — the same result a one-shot read produces, as ch. 6
§6.4.2 requires. This chapter previously excluded it, which could not hold once
`groove/SPEC/3_queries_operators.md` specified the one-shot behaviour: a
one-shot read would return a row where a subscription over the same query
returned nothing.

Its identity is the one already required by ch. 6 §6.4.3 `INV-QUERY-30`: a
scalar global aggregate lowers to one fixed synthetic identity. That identity
does not depend on a group key, so the empty case needs no special derivation —
which is what makes the empty global row expressible at all. The empty row is
therefore present from attach, and the transition when the first input row
arrives is a value replacement of an existing member, not an add. Its
disappearance is likewise a value replacement back to the empty values, never a
member removal: a scalar global aggregate's member is present for the lifetime
of the subscription.

Floating-point accumulation IS inside the maintained surface, under a weaker
agreement guarantee than the exact one above. Incremental maintenance sums in
arrival order and subtracts on retraction, while a one-shot recompute sums from
scratch, so the two cannot be required to agree bit-for-bit. They MUST agree
approximately, as follows:

- `count`, `min` and `max` MUST agree **exactly**, for every value type
  including `F64`. They are counting and selection, not accumulation, so
  floating point gives them no licence to differ.
- Integer `sum` and `avg` MUST agree **exactly**. A divergence in exact integer
  arithmetic is a maintenance defect, never a rounding artifact, and this
  requirement is what makes the two distinguishable.
- `F64` `sum` and `avg` MUST agree within a tolerance proportional to
  `ε × (input rows + maintenance updates) × Σ|x|`, where `Σ|x|` is the sum of
  absolute input magnitudes for the group.

The tolerance is expressed against `Σ|x|` rather than against the result
deliberately. Under catastrophic cancellation — inputs of opposite sign summing
to near zero — the result approaches zero while the absolute error does not, so
a result-relative tolerance is unbounded and cannot be enforced.

The error term grows with maintenance updates, and an implementation is NOT
currently required to bound it. Drift may accumulate across a long-lived
subscription; a maintained `F64` `sum` or `avg` is permitted to move further
from its one-shot value the longer the subscription runs.

The constant of proportionality and whether to bound drift remain deliberately
unfixed. The differential oracle should set the constant from observed
divergence against update count. With update count unbounded, the
`ε × (input rows + maintenance updates) × Σ|x|` term becomes weak for a
sufficiently long-lived view; that is an accepted temporary trade because no
current workload runs one long enough
for the drift to matter, and that recomputation has its own cost.

The remedy, when it is wanted, is to recompute a group from its inputs after a
bounded number of updates, which converts unbounded drift into a stated bound.
What should decide it is evidence: the oracle's divergence-versus-update-count
curve, together with a real workload's observed update volume per group. If a
maintained aggregate is ever surfaced as a number a user acts on — a dashboard
total, a billing figure — this should be revisited before that ships.

Policy composition happens before these operators. A policy row changing
visibility must flow through the same `TopBy` or `Aggregate` state as a base row
change, causing ordered-window boundary churn or group-summary replacement as
needed. Jazz must not repair policy-sensitive order or aggregate results by
running a peer-local semantic scan after groove emits a broader delta.

The operational target is O(touched partitions/groups plus boundary output), not
O(result set). The allowed output is still the minimal net subscription delta:
same-tick enter/leave churn consolidates before `ViewUpdate`, deterministic ties
make replay byte-stable, and reset-result-set `ViewUpdate`s remain explicit
attach/rebuild outputs rather than the normal maintenance strategy.

### 16.7 Binding event bridge

The TypeScript/WASM/NAPI subscription surface should be a thin event bridge over
maintained subscription terminal deltas, not a second diff engine. The bridge
needs stable event records for:

- first result / settled state;
- result-row add/remove and replacement;
- matched include path and join material;
- version bundles vs `peer_payload_inventory.complete_tx_payloads`;
- errors, reset-result-set updates, and explicit full-recompute debt counters.

The Rust `WatchHandle` can remain conflated for simple callers, but the binding
ABI must expose enough structured deltas for UI stores to maintain identity,
loading state, and optimistic/settled transitions without cloning entire result
sets on every tick.

### 16.8 Open questions

None at this time.

### 16.8 Subsumed subscription-reactivity notes

The former granular-reactivity and subgraph-sharing TODOs are folded into this
chapter. Maintained views should emit enough structured terminal facts for host
bindings to choose full replacement, row-level deltas, include/path deltas, or
patch streams without rerunning a semantic query in the facade. Framework
adapters may optimize rendering granularity, but the authoritative delta source
is the maintained-view peer state.

Correlated array subqueries require shared maintenance rather than one compiled
graph per outer row. The likely direction is a binding/prepared-shape style
correlation relation that lets parent keys flow as data, then routes child
result changes back to the correct parent output.

## Open Questions

- 🔶 [#1783](https://github.com/garden-co/jazz/issues/1783) — Subscription patch and first-result API.
- 🔶 [#1765](https://github.com/garden-co/jazz/issues/1765) — Correlated subquery maintenance.
- 🔶 [#1784](https://github.com/garden-co/jazz/issues/1784) — Partition-aware deletion witnesses.
- 🔶 [#1777](https://github.com/garden-co/jazz/issues/1777) — `F64` maintained-aggregate drift constant and long-lived bound.
