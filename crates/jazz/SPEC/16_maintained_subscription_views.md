# jazz — Specification · 16. Maintained subscription views

This chapter names the target serving architecture for query-driven sync:
**every live peer subscription is maintained by groove**. A serving node should
not have a second production query engine for subscriptions. It may keep semantic
evaluators as oracles, debugging aids, or temporary migration scaffolding, but
the protocol-facing steady state is a groove subscription whose terminal stream
contains enough information to produce `ViewUpdate`s incrementally.

The old implementation name for the current prototype is not product
terminology. The intended abstraction is a **maintained subscription view**.

## 16.1 Contract

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
facts as needed, then maps those terminal rows to subscription or sync events.
App-row projection and internal fact emission are separate outputs of the same
program; projection must not become a second diffing path.

## 16.2 Policy composition

For non-system peers, the maintained graph begins from the shared
policy-composed lowered-query core from ch. 14: the user query intersected with
the table read policy under the authenticated peer identity, lowered over the
subscription's visible-current base source. Claim operands are rewritten to
server-derived parameters before lowering. `claim("sub")` is the stable subject
identity. Recognized claims that do not yet have a runtime value fail closed.

Policy composition is not merely an output filter. Policy dependency tables are
part of the maintained graph. If a membership row, access row, join witness, or
recursive edge row changes visibility, the maintained view must emit the same
net result-set transition as a full rehydrate over the same committed history.
Maintained subscription views are augmentations over that core: they add
terminal membership rows, version/replacement witnesses, and peer-facing
dedup/reset semantics, rather than defining a separate query evaluator.

## 16.3 Recursive reachability

`ReachableVia` clauses lower to groove recursive graphs everywhere they appear:
user queries, read policies, write permission scopes, matched-path witnesses,
and replacement witnesses. Jazz does not branch on groove's internal recursive
execution strategy. Groove owns the choice between incremental recursion and
full recomputation when non-monotone deltas appear.

## 16.4 Production fallback boundary

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

## 16.5 Current known gaps

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

- partitioned or schema-projected reads do not yet install a maintained groove
  graph; root current reads are source-specific one-shot reads, and projected
  joins/reachable are unsupported until source-aware lowering exists;
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
- `array_subqueries` have one-shot and local-tier relation snapshot
  materialization, but maintained subscriptions do not yet emit relation-edge
  terminal deltas across all tiers. A subscription shape with
  `array_subqueries` must be rejected at subscription open until unified
  relation/path lowering or relation-edge terminal deltas are represented in
  groove. Serving code must not compensate by recursively subscribing to
  coarse child shapes for sync coverage;
- application-column projection is a materialization concern layered over the
  maintained membership/version stream; projected subscription payloads must not
  become a second diff engine;
- predicate-policy lowering is incomplete where read policies still require
  direct semantic evaluation instead of a lowered maintained policy graph.

Window limitations:

- unordered `limit > 1` and unordered nonzero `offset` remain unsupported in
  Jazz maintained subscriptions; callers must provide explicit ordering for a
  window or ordered suffix to lower through `TopBy`.

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

## 16.6 Aggressive maintained support: ordered windows and `Aggregate`

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
by groove. Floating-point accumulation, user-defined aggregates, approximate
aggregates, and empty-global-row SQL compatibility stay outside the maintained
subscription surface until their replay semantics and payload shape are
specified.

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

## 16.7 Binding event bridge

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

## 16.8 Open questions

- 🔶 **Per-subscription capability rejection.** Capability-gapped live
  subscriptions currently fail at the serving tick boundary in some paths rather
  than producing a targeted rejection for the offending subscription. Examples
  include branch read-view subscriptions that require deletion witnesses and
  array-subquery maintained views. Public DB/subscriber paths also normalize
  `QueryCapability` gaps into the generic "maintained subscription view
  subscription does not support this query shape" protocol error, losing the
  precise `CapabilityReport`/`UnsupportedReason`/`SourceGap`. Desired shape: a
  capability-gapped subscription receives a per-subscription rejection carrying
  the specific typed gap, while the connection keeps serving other
  subscriptions. This is a serving/protocol-boundary issue, not a lowering
  workaround; lowering failures must stay loud capability errors.
