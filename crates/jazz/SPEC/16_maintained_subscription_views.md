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
- `INV-SYNC-30`: A fresh `Edge`/`Global` settled one-shot read MUST obtain settled authority coverage for its exact current usage-site subscription; an update for a detached predecessor MUST NOT satisfy it even when shape, binding, and options are equal. This freshness rule MUST NOT change local-read semantics or prevent reuse of still-live maintained subscription coverage.
- `INV-SYNC-36`: An authority synchronizes an exact, authorized input closure, never its application-output terminal. The receiver reconciles that closure with the local inputs permitted by the requested tier and runs the same maintained Groove program used for local changes. Only that receiver-local terminal may publish application rows or ordered structural edits.

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

Every reset or incremental publication of flat-tuple membership, including a
reset forwarded from an already maintained upstream subscription, MUST carry a
`ContributingMembers` fact for every declared source position. Repeated physical
source tables still have distinct positional roles. A terminal payload is not a
substitute for this canonical contributor closure: the receiver MUST be able to
reconstruct the same tuple through its ordinary one-shot path after applying the
publication, without a same-generation transient remove and re-add.

A deletion-witness transition forces authoritative membership reconciliation
only when the public result terminals are silent. If the same Groove tick
already emits a complete result membership or structured-terminal delta, that
delta is the authoritative incremental consequence and MUST be published
without reopening the maintained view. Reopening in that case would discard an
already proven removal and can repeatedly rediscover the same witness instead
of making subscriber progress.

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

#### Durable settled-program-fact keys

`INV-QUERY-36` — Every settled program fact durable key is exactly one
versioned, canonical `JPFK` codec value. Its version and variant tags are
permanent. A `ResultPayload` carries its `RecordDescriptor` as one exact,
ordinary canonical Groove record encoding and its row as a record under that
descriptor; a synthetic result member likewise carries its dynamic row and
replacement token as fixed Groove records containing the exact descriptor and
value. These nested durable values never use a Rust-private serde/postcard
encoding. Recovery MUST reject before mutating resident query state any legacy
postcard payload, malformed or truncated value, unknown version/tag, trailing
bytes, non-canonical representation, oversized field, or value beyond the
codec nesting bound. There is no compatibility decode or on-open rewrite for
the pre-freeze raw `ViewFactEntry` postcard keys. Add, remove, rewrite, and
reopen all derive the same exact key bytes. Storage-freeze issue #2249.

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

### 16.1.1 Covered input, one local maintained result

`INV-SYNC-36` fixes the authority/receiver boundary. An authority result is an
exactly scoped **covered input closure**, not a cached application result. It
contains the canonical row versions and typed program facts that the authorized
residual program may observe, together with the witnesses and completeness
evidence required to trust that closure. A receiver stages those inputs before
making their membership visible to its maintained graph. A membership or
relation fact whose referenced row version has not been admitted as part of the
same exact authority closure is incomplete and cannot advance settlement or
enter the graph.

A deletion-register witness is not authorization by itself. Its source
occurrence MUST also be admitted by the same current policy-filtered
`IncludeDeleted` preimage, matched to the exact deleted row and deletion
version identity before the witness enters the covered closure. The register
supplies the deletion transaction, branch, schema, and layer carrier; the
preimage supplies the permission to disclose that this row is deleted. A
coverage withdrawal, a later deletion after revocation, an unapproved cold
tombstone, or a different deletion winner for the same row therefore cannot be
reinterpreted as an authorized deletion. A cold subscriber may receive a
deletion witness when its current policy admits that exact preimage. This does
not create a historical entitlement: the authority evaluates the current policy
for the exact source occurrence and uses the ordinary current-row/branch winner
selection.

Every input change then enters the same receiver-local lowered Groove program:

```text
exact authority-covered inputs ----+
                                    +--> one maintained Groove graph --> app output
eligible local inputs --------------+
```

The requested tier determines which inputs participate and when the first
answer may be published; it does not select another evaluator:

- `local-first` evaluates locally known current data plus pending local
  changes, online or offline. Installing a remote closure does not retire its
  cached inputs. Remote scope withdrawal is not a stored client-side revocation;
- `remote` waits for a fresh settled closure for its exact usage-site
  subscription and evaluates only that closure, without pending local changes.
  It waits while offline;
- online `remote-if-possible` evaluates the exact authority inputs with pending
  edits/deletes applied to those inputs, plus eligible pending new inserts.
  An edit alone does not admit an existing out-of-scope row. Relationships use
  only these inputs, without expanding into cached dependency rows. Inserts
  participate in the ordinary query, including its joins, filters and windows;
- offline `remote-if-possible` evaluates local knowledge plus pending changes,
  like `local-first`. It may therefore show cached rows excluded by the last
  remote closure. Returning online replaces that fallback with fresh authority
  inputs and the bounded pending overlay. Do not reuse a detached receipt as
  fresh authority coverage;
- a local-only internal execution suppresses upstream registration but still
  uses the same lowered graph over its local source.

Source membership and stored row state are distinct. Scope withdrawal neither
deletes nor redacts previously downloaded content. An actually admitted deletion
version, however, updates locally known row state and suppresses that row in
ordinary local reads too. Pending delete/restore candidates use the ordinary
current-winner relation before the query runs: a newer pending restore supersedes
a pending delete; rejection retracts the rejected candidate and reveals the
remaining winner. No terminal-side workaround may independently reconcile this
state. Broader online local dependency expansion is deferred to #2501.

The authority never chooses positions in the receiver's application value.
Explicit `order_by`, implicit row-id order, stable tie breakers, relation-local
windows, and stored user-defined ordering data are inputs to the receiving
query program. Its local Groove collector derives the ordered root and nested
terminal edits after authority and local inputs have been reconciled. An
authority-produced root row, nested snapshot, `Insert`, `Update`, `Remove`, or
`Move` operation MUST NOT cross peer sync as result truth and MUST NOT be
applied by a facade-side reducer. Such an operation was computed without the
receiver's eligible local overlay and may name an index that is false for the
receiver's actual result.

Worked examples:

- **Strict remote root query.** The authority admits rows A and C plus the
  witnesses proving that closure complete. The receiver installs A and C as
  its remote source and runs the query's filter and order locally. It does not
  install an authority-produced `[A, C]` result cache.
- **Local-first nested insertion.** The authority closure contains children A
  and C while the receiver has an eligible pending child B. The receiving
  child collector computes `[A, B, C]`. An authority operation saying “insert C
  at index 1” would be wrong for this receiver and is therefore never sent.
- **Remote permission revocation.** A successor authority closure retracts the opaque
  admission or safe source witness that supported row A. That input retraction
  enters the same local graph and its collector removes every affected root or
  descendant occurrence in a remote-scoped result. The client does not
  re-evaluate the hidden policy, and the authority sends no presentation-level
  remove. Local-first may still show the cached row; offline fallback may show
  it again after an online remote-if-possible result excluded it.
- **Cached Local-first open.** A client can show retained same-scope A plus a
  pending insert B. A new authority closure containing only C does not evict A
  from local-first knowledge. Online remote-if-possible instead uses C plus B
  (if B matches using available query inputs); remote uses only C.
- **Reconnect.** A fresh usage-site subscription cannot reuse its detached
  predecessor's result or terminal sequence. It verifies a fresh exact closure,
  installs it, and lets the local graph publish the corresponding reset.

This rule does not make clients authorization authorities. The serving
authority still selects and proves the safe closure. The receiver runs only the
identified residual query over admitted inputs; it does not inspect hidden
policy rows, supplement missing evidence from unrelated local history, or
reinterpret an opaque admission. Relays retain and forward closures only under
their exact policy-scoped authority-result identity.

Remote one-shot reads use the same mechanism for a bounded lifetime: pin the
same live upstream stream as identical usages in the same admitted scope,
request fresh coverage, await a newer settled receipt, run the shared
local program to quiescence, materialize its terminal once, and finalize the
coverage owner on success, cancellation, or error. They do not inspect an
equal-shaped predecessor's terminal cache or execute a separate semantic scan.
Only final-pin release retires the stream. A subsequent usage opens a new wire
identity; a retired stream's late reply cannot satisfy it. The receiver validates
one ordered predecessor sequence, not duplicate sequences per local listener.

Settlement is downstream of local evaluation. A receiver may report a
generation settled only after the complete exact closure has been staged, all
referenced content witnesses are present, the corresponding local Groove tick
has quiesced, and its locally derived terminal represents that generation.
Installing carriers, publishing membership, and evaluating the graph are one
ordered semantic boundary even when storage commits and runtime scheduling use
several internal steps.

### 16.1.2 Application subscription delta contract

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

### 16.1.3 Fresh one-shot coverage

A new remote settled one-shot is a new usage site, not a request to inspect
whatever equal-shape state happens to be materialized locally. Each fresh
`Edge`/`Global` one-shot registers a current `SubscriptionKey` and completes
only after an authority-backed settled update covers that exact key. Binding-view
generation advancement alone is insufficient: a late update addressed to an
already detached equal-shape predecessor must not acknowledge its replacement
(`INV-SYNC-30`).

This usage-site freshness boundary does not create a second query algebra.
The fresh one-shot owns a transient instance of the same coverage and local
maintained-evaluation lifecycle, materializes one terminal result, and then
finalizes it. Synchronous/local one-shots remain local, and a still-live
maintained subscription may reuse its exact maintained coverage group; only a
new remote settled one-shot must prove coverage for its own current wire
subscription.

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

#### Retained receiver input pages

The same compiler-owned window stage is used by authority closure publication
and by the receiver's application collector. A closure for a bounded root or
parent window is proportional to that requested window; it is never an
authority terminal snapshot. Consequently a non-durable receiver that keeps a
page after its authority usage site detaches retains a typed **window-source
capability**, not a boolean saying that some result happened to be
materialized.

That capability contains the exact normalized source occurrence, full
validated source shape, root/parent partition, user order keys and directions,
the compiler's deterministic tie keys, window offset/limit, and the
policy-scoped receipt that supplied it. A later Local lowering may reuse it
only when its own compiler-owned descriptor is exactly the same apart from a
window wholly contained by the retained page. It then treats the retained rows
as the output of the source window and applies its requested offset relative to
that page. It MUST NOT apply the original absolute offset a second time, sort
the page in Jazz, search similar registered shapes, or use authority output
membership as a fallback.

Different source occurrences, partitions, ordering/tie contracts, schemas,
bindings, policy scopes, or non-contained windows are incompatible. They must
open fresh coverage (or use ordinary local-first inputs), even where their
table names or visible rows happen to match. Detach, revocation, reconnect, and
new scope admission retire or replace the exact capability atomically with its
covered source closure. This is the root-level application of the shared
per-parent window representation established by #1747.

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

- 🔶 [#2501](https://github.com/garden-co/jazz/issues/2501) — Whether pending changes should expand online remote-if-possible inputs into existing out-of-scope rows or cached query dependencies; see §16.1.1 for the initial strict-input rule.
- 🔶 [#1783](https://github.com/garden-co/jazz/issues/1783) — Subscription patch and first-result API.
- 🔶 [#1765](https://github.com/garden-co/jazz/issues/1765) — Correlated subquery maintenance.
- 🔶 [#1784](https://github.com/garden-co/jazz/issues/1784) — Partition-aware deletion witnesses.
- 🔶 [#1777](https://github.com/garden-co/jazz/issues/1777) — `F64` maintained-aggregate drift constant and long-lived bound.
