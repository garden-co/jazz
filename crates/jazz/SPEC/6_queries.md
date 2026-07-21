# jazz — Specification · 6. Queries

## Overview

A jazz query is a content-addressed **shape** plus a **binding**, evaluated to a
result set that includes matched include paths and join witnesses, and synced
incrementally. This chapter defines the query AST, shape/binding identity, the
matched-path result-set material, and query-driven sync at the result-set level.
Queries lower onto groove
prepared shapes (ch. 14), and provide the substrate used by authorization
(ch. 7) and sync (ch. 8).

Invariant digest:

- `INV-INC-1`: Incremental delivery invariant (mechanism law). For any maintained view, the work performed to ingest, apply, and publish a change — including snapshot assembly, diffi...
- `INV-LOWER-11`: Prepared graph lowering MUST reject != predicates against parameters until supported.
- `INV-LOWER-13`: Aggregation, ordinary read ordering, general pagination, and projection MUST be applied by the node after row materialization, not required from groove lowering, excep...
- `INV-QUERY-1`: A query graph node MUST be identified by the full NodeDescriptor consisting of operator, ordered inputs, and output; two incompatible descriptors MUST NOT share a node...
- `INV-QUERY-2`: A NodeDescriptor MUST validate operator input arity, input/output descriptor compatibility, join key arity, and field-index bounds before the runtime accepts the node.
- `INV-QUERY-3`: FilterOp MUST emit exactly the input deltas whose records satisfy its PredicateExpr, preserving record bytes and weights, for the supported predicate surface including...
- `INV-QUERY-4`: SQL predicate lowering MUST reject unsupported or ill-typed predicate expressions instead of lowering them approximately.
- `INV-QUERY-5`: MapProjectOp MUST emit one output delta per input delta, copying only configured fields into the output descriptor and preserving the input weight.
- `INV-QUERY-6`: UnwrapNullableOp MUST drop Nullable(None) input deltas, unwrap Nullable(Some()) to the inner value, and preserve the original delta weight.
- `INV-QUERY-7`: Union MUST require all non-empty inputs to have the same output descriptor and MUST preserve duplicate derivations as separate weighted deltas (UNION ALL semantics).
- `INV-QUERY-8`: An inner JoinOp MUST require equal-length left and right key vectors.
- `INV-QUERY-9`: An inner JoinOp MUST emit joined records with weight leftweight \* rightweight for matching keys, including matches produced by changes arriving on either side.
- `INV-QUERY-10`: An inner JoinOp MUST NOT double-count pairs where both matching sides changed in the same logical tick.
- `INV-QUERY-11`: Shared join arrangements MUST apply a given logical-time delta at most once per arrangement key/scope, even when multiple joins consume the arrangement.
- `INV-QUERY-12`: AntiJoin MUST output left rows only when the total right-side multiplicity for the join key is zero.
- `INV-QUERY-13`: AntiJoin MUST retract or restore visible left rows only when the right-side count crosses zero; changes that keep the right count nonzero MUST NOT emit anti-join deltas.
- `INV-QUERY-14`: Same-tick anti-join updates MUST suppress a left row that arrives with a matching right row and MUST emit a left row exactly once when it arrives in the same tick as t...
- `INV-QUERY-15`: SQL planquery MUST reject query parameters; parameterized SQL MUST go through planpreparedshape/prepared binding flow.
- `INV-QUERY-16`: SQL prepared-shape lowering MUST accept only equality predicates of the form column = $parameter or $parameter = column as binding predicates.
- `INV-QUERY-17`: SQL lowering MUST reject unsupported SELECT/set/join shapes explicitly, including SELECT DISTINCT, grouped/ordered/limited selects, non-inner joins, and non-UNION ALL...
- `INV-QUERY-19`: BindingSourceOp MUST NOT be evaluated through ordinary subscription/query graphs outside prepared shapes.
- `INV-QUERY-20`: ArgMaxByOp and ArgMinByOp MUST accept arbitrary upstream graph inputs. Base-table inputs MUST have primary-key columns exactly groupcols + ordercols; non-table inputs...
- `INV-QUERY-21`: ArgMaxByOp and ArgMinByOp MUST emit only winner changes for touched groups, suppressing non-winner changes and net-zero group deltas.
- `INV-SHAPE-16`: Prepared shapes MUST retain their output graph nodes for the lifetime of the database unless/until an explicit shape-drop API exists.

## Details

### 6.1 The query AST

The query AST is jazz's stable vocabulary for describing rows, relationships,
projections, ordering, aggregation, and windowing. It is lowered to groove rather
than to SQL, and it is not a second execution engine.

The predicate surface is `Predicate::{All, Any, Not, Eq, Ne, In, Gt, Gte, Lt,
Lte, Contains, IsNull}` over `Operand`s. Relationship traversal is expressed by
`JoinVia` for reference joins, `ReachableVia` for recursive reachability, and
`Include` for forward reference expansion. Reverse one-to-many relations and
nested relation payloads are expressed by `array_subqueries`; they are distinct
from `Include` and must not be represented as include paths. Result shaping is
expressed by `select`, `order_by`, `aggregate`, `limit`, and `offset`. Every
form listed here is part of the `Query` contract; a form not yet implemented is
marked at its definition, and there is no out-of-band gate list.
`order_by`/`aggregate`/general `limit`/`offset` are applied by the node _after_
row materialization for ordinary reads, rather than pushed into groove lowering
(ch. 14, `INV-LOWER-13`). Maintained subscription exceptions are unordered
`limit(1)` with offset `0`, which lowers through groove `ArgMinBy` over
`row_uuid`, and ordered result windows, which lower through groove `TopBy`
(ch. 14). Ordered windows may be finite (`limit` present) or an unbounded
ordered suffix (`limit` absent); the latter keeps full ordered membership and is
not a fallback to one-shot sorting. `!=` against a parameter is rejected until supported
(`INV-LOWER-11`).

An `array_subquery` names an output relation (`column_name`), an inner table,
and a correlation from a parent-scope column to an inner-table column. It may
carry child-local filters, select columns, ordering, limit, requirement, and
nested array subqueries. The MVP supports direct correlations and rejects
subquery joins until their semantics are specified. `array_subqueries` are
canonicalized into shape identity separately from includes; sibling ordering is
not semantic, but duplicate sibling `column_name`s are rejected.

### 6.2 Shapes: validated, content-addressed, schema-stamped

A shape is the validated, schema-stamped identity of a query. Validation
normalizes the AST, infers `params`, records the `schema_version` used for
validation, emits canonical bytes, and derives a `ShapeId`
(`Query::validate(&JazzSchema)` returns this as a `ValidatedQuery`).

Shape identity binds the query _and_ the schema:
`ShapeId = Uuid::new_v5(QUERY_NAMESPACE, canonical_query_bytes ‖
schema.version_id())`. The same AST validated against a different schema version
therefore has a different shape (`INV-QUERY-1`).

Canonicalization erases ordering wherever the semantics are commutative:
root/join/reachable filter order, include order and duplicates,
selected-column order, aggregate-expression order, equality operand order,
`All`/`Any` child order, and `In` value order. `order_by` remains semantic and
is preserved. Semantically identical forms therefore share a `ShapeId`, while a
real semantic change produces a different one (`INV-QUERY-2`). Validation
rejects unknown tables/columns, bad include paths, join/reference
incompatibility, operand and parameter type conflicts, and aggregate/order-by
misuse.

### 6.3 Bindings and claims

A binding supplies the values for the `Operand::Param` holes inferred during
validation. Its identity is content-addressed independently of the shape:
`BindingId = Uuid::new_v5(QUERY_NAMESPACE, canonical_binding_bytes(values))`,
with values encoded in parameter-name order. Binding rejects missing, unknown,
or type-mismatched params (`INV-QUERY-3`).

Claims are a separate input channel. `Operand::Claim` is _not_
client-supplied binding data: claim bindings are injected server-side from the
subscriber's authenticated identity and admission/session claims by policy
composition (ch. 7). `sub` is the canonical identity claim and resolves to the
authenticated `AuthorId`; additional claim names are product/admission-defined
and must come from the trusted admission/session context, never from ordinary
query bindings.

### 6.4 Result sets, include paths, and relation payloads

A result set is the authoritative membership for a canonical
`ProgramInstanceKey = (ShapeId, ResolvedReadKey, PolicySharingKey, BindingId)`.
Wire `SubscriptionKey`s are usage-site handles attached to that instance. The
ordinary current-content row projection remains `(table, row_uuid, tx_id)` for
current-row payload bundling and compatibility, but the canonical result-set
shape is a typed result member. A real-row member carries content/deletion
layer, optional deletion-register transaction, source/read-view identity,
schema projection, branch/prefix discriminator, batch identity, and optional row
digest as needed. Synthetic and path-tuple members are peers of real rows, not a
separate result-set engine (`INV-QUERY-8`).

Membership includes more than the projected output rows. Each result set carries
the matched include-reference targets and join/junction rows that contributed to
the output. Include payload material is not a separate public or internal mode:
subscription payloads contain matched include paths only, never traversed
non-matches or failed-path closure. Read-policy and policy-atomic filtering are
applied before emission (`INV-QUERY-9`, ch. 7). When a row remains in the result
but its visible content version changes, the entry tracks the new `TxId` even if
the projected cells are byte-identical (`INV-QUERY-17`).

Missing include targets affect the view/API layer, not sync membership.
`JoinMode::Inner` drops a parent whose include target is unresolvable.
`JoinMode::Holes` keeps the parent, with `require_includes` tightening holes
mode by requiring include matches. `require_includes` does not broaden the
subscription payload. Sync membership keeps holes first-class: a readable parent
is never dropped from sync solely because an included target is absent or
unreadable (`INV-QUERY-10`).

Array subqueries produce relation payload material, not nested row values inside
core rows. A relation payload is a set of row batches plus typed relation facts. A
relation fact names source and target rows and can additionally carry edge kind,
source/target version refs, recursion depth, multipath edge id, branch
alternative, terminal role, order key, and matched-vs-hole state.
For a reverse relation array, the edge source is the parent row and the target
is each visible correlated child row. For nested array subqueries, child rows
become the source for the next relation level. Child filters, select columns,
ordering, and limits affect only the child relation material; they do not change
root row membership unless the array subquery has an explicit requirement.
Unreadable child rows and their edges are omitted, while readable parents remain
visible for optional array subqueries (`INV-QUERY-21`).

Alpha-style relation traversal also has an output-changing query surface.
Supported single-hop traversal (`hopTo` shapes that project one terminal table)
is facade syntax: the core normalizes it into the same table-rooted query program
used by ordinary includes and `join_via`, then evaluates one-shot reads,
maintained subscriptions, registration, known-state, and chunked snapshot serving
through that single program family. Relation-query shapes canonicalize through
the normalized row-set vocabulary and do not get a separate sync, subscription,
or validation engine. Multi-hop traversal and `gather` remain explicit
unsupported relation operators until they can be normalized into the same program
family with matching maintained semantics.

### 6.4.1 Default result ordering

Ordering is a core-owned query semantic: it must be expressed in the lowered
plan and carried through delivered results and delta positions, never
re-derived by binding layers (ch. 13 §13.13).
Implementation note, 2026-07-21: root finite `limit`/`offset` windows without
explicit `order_by` now express this default by injecting ascending row-id order
in lowering before the slice operator.

Decision, Anselm 2026-07-18: when a relation-valued result has no explicit
`order_by`, its default order is ascending row id (`RowUuid`). This applies at
every relation-valued result boundary: root query rows, relation payloads from
`array_subqueries`, and nested include/relation subtrees. A parent row's child
relation is therefore ordered by child row id unless that child relation carries
its own explicit `order_by`. Precision note: uuidv7 row ids order by creation time at millisecond
granularity; ids minted within the same millisecond order by their random
bits — stable and deterministic, but not insertion order. Order-sensitive
tests must sort expected ids rather than assume insertion sequence.

The default is intentionally cheap: it matches
primary-index scan order for row tables, is stable under updates because row ids
are immutable, and for uuidv7-generated ids approximates creation-time order.

Explicit `order_by` overrides the row-id primary ordering for the result boundary
where it appears. Ordered row-valued results remain total and replay-stable:
after the user-declared order terms, ties are broken by ascending row id unless
the query surface later exposes an explicit, stable tie policy. Child-local
`order_by` overrides only that child relation's ordering and does not reorder
parents or sibling relation payloads.

Aggregate or grouped outputs that do not have a real row id default to ascending
group-key order. Composite group keys compare lexicographically in the query's
declared `group_by` field order: compare the first component; if equal, compare
the next component, and so on until a difference is found. Each component uses
the logical order for its declared type, matching the order-preserving storage
key encoding where that type is a valid key part (groove ch. 2). A grouped query
whose group key contains a type without a specified stable order must be rejected
until that type's ordering is specified. If an explicit `order_by` is applied to
grouped output and multiple groups tie on the user order terms, the group key is
the stable tie-breaker.

Ordering is part of the delivered result, not a presentation hint. Initial
snapshots, reset-result-set `ViewUpdate`s, maintained subscription deltas, and
settled subscriber reads must all reduce to the same ordered result as a
one-shot read at the same frontier. Incremental delivery must include enough
position/order information for insertions, removals, updates, and boundary churn
to be applied in the specified order. This composes with
`groove/SPEC/INVARIANTS.md::INV-INC-1`: because default row ordering is by
immutable id, a single-row content update that does not change membership must
not reorder neighboring rows, and a single-row insert must publish its ordered
position without scanning or diffing the accumulated relation state.

🔶 Staged implementation remainder: unbounded root relation results and
relation payload boundaries from `array_subqueries`/nested relation subtrees
still need the same plan-injected default order once their maintained graph
fragments can carry it without perturbing recursive/policy maintenance.

### 6.5 Query-driven sync

A subscription binds a shape to one binding in one read view and is addressed by
`SubscriptionKey { shape_id, binding_id, read_view }`. `RegisterShapeOptions`
carry a semantic `ReadViewSpec` describing the requested current, branch,
merged-branch, owner-qualified historic snapshot, schema-projected, and
overlay-visible view. The serving/runtime boundary derives the authoritative
resolved read identity from the semantic read view plus tier; callers do not
supply the key as independent identity. The wire vocabulary is `RegisterShape`,
`Subscribe`, `Unsubscribe`, and `ViewUpdate` (ch. 8).

The serving authority maintains the settled result set for each
program instance: the result member set plus its matched include paths,
relation edges, and join witnesses (§6.4). In Rust this server-side state is named
`maintained_subscription_views`.
The subscriber receives and stores its own **settled subscription result set**:
the rows, typed program facts, and matched include/relation material it can
answer settled reads from (§6.6).
The two sides share entry shape, but have different roles. A `ViewUpdate` with
`reset_result_set = true` resets the subscriber's settled result set.

Two correctness properties govern result-set maintenance. Incremental
result-set updates converge to the same typed result-member and program-fact
state as a reset `ViewUpdate` over the same committed history (`INV-QUERY-15`).
Reset `ViewUpdate`s retain
per-peer complete payload coverage (`INV-QUERY-7`). Payload dedup is per peer for
complete transaction payloads: an already-shipped complete payload is sent in
`peer_payload_inventory.complete_tx_payloads`, and a `VersionBundle` is emitted
at most once per update (`INV-QUERY-20`). Partial payloads, including exclusive
payloads, do not establish complete-transaction payload coverage unless the peer
has received all versions for the transaction. Exclusive `ViewUpdate` visibility
is view-atomic: a bundle may carry the exclusive versions needed for the
maintained subscription view, and result members for that view are emitted only
when that view's exclusive payload is complete (`INV-QUERY-19`, ch. 3).

Subscription lifetime is reference-counted, with no TTL: a peer's shape
registration drops when its binding count hits zero, and re-registration is
cheap and idempotent. Whether a fully-unreferenced prepared graph is also dropped
is a groove-side question; see groove `INV-SHAPE-16`, which retains it.

_Further invariants._ `INV-QUERY-16` — same-drain result churn folds by net
outcome (enter-then-leave sends no add; leave-then-reenter replaces; same-tx
retract/assert nets no update). `INV-QUERY-4` — shape registration rejects an
AST whose id doesn't match `shape_id` and parks an unknown schema version until
the catalogue arrives. `INV-QUERY-5` — a `Subscribe` attach names a registered
shape and matches the registered shape's arity; `Unsubscribe` drops that
usage-site subscription's settled subscription result set. `INV-QUERY-6` —
`RegisterShape` then `Subscribe` causes the serving side to attach the
usage-site subscription to the coverage group and answer with a reset
`ViewUpdate`.

### 6.6 Reads, settled and local

A query read is either local/unsettled or settled. A local/unsettled read returns
rows complete only relative to the node's own visible-current knowledge
(`INV-QUERY-11`). A settled read on a subscriber is answered from the
subscription's settled subscription result set; an unresolvable result-set entry is an
invariant violation, not a degraded answer (`INV-QUERY-12`).
An include-deleted one-shot read widens only the root current-row source: deleted
root rows may be returned and marked deleted, while joins, reachability access
tables, reachability edge tables, and include payloads continue to use ordinary
visible-current witnesses.

Inside an open exclusive transaction, `tx_query` records a binding-sensitive
`PredicateRead` (`INV-QUERY-13`). The later phantom check (ch. 3,
`INV-QUERY-14`) compares the shape+binding output `(RowUuid, TxId)` set at
`base_snapshot.global_base` against now.

Allowed "magic" select columns are the provenance columns `$createdAt`,
`$createdBy`, `$updatedAt`, `$updatedBy`. Alpha-compatible permission
introspection fields such as `$canRead` are not
ordinary stored columns and are not executable query columns. Permission
introspection is exposed through standalone dry-run APIs (ch. 7, ch. 13), so
current query execution must reject `$can*` predicates/projections rather than
materializing them as row fields. Dry-run policy APIs return a concrete
allow/deny result or an explicit indeterminate result when the probe lacks
required input, such as a row id for a row-id-sensitive insert policy.

### 6.7 Conformance test plan

Default result ordering is a conformance requirement for every public query
surface, but implementation work is deferred until after 2026-07-19. The test
plan below records the intended coverage without changing tests now.

- Strengthen the maintained-vs-one-shot differential oracle command
  `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
  to assert ordered equality rather than set equality for root rows and every
  relation payload. The oracle should keep using public query shapes/builders
  and compare the maintained stream's reduced result to the one-shot result at
  each checkpoint.
- Extend the TS query API coverage in
  `packages/jazz-tools/tests/ts-dsl/query-api.test.ts` so result arrays that
  currently sort ids before comparison become ordered-equality assertions once
  the two human-decision red buckets are resolved. Add explicit cases for
  default root ordering, reverse/forward relation include arrays ordered by
  child id, nested relation payloads, and explicit `orderBy` preserving its
  override with row-id tie-breaks.
- Add grouped/aggregate conformance cases for default group-key ordering:
  scalar/global aggregate output, single-column groups, and composite groups
  whose input rows are inserted in non-key order. These cases should assert the
  lexicographic group-key order and explicit `orderBy` override/tie behavior.
- Add a facade-level canary next to
  `crates/jazz/tests/incremental_delivery_canary.rs` for a large unordered
  relation/include result. It should subscribe through the public `Db` API,
  insert one child whose id belongs in the middle of the child relation, assert
  the delivered insert position/order, and keep the existing scale-independent
  allocation/byte expectation so ordered insertion remains covered by
  `INV-INC-1`.
- Keep Rust tests aligned with
  `crates/jazz-tools/TESTING_GUIDELINES.md`: prefer black-box integration tests
  through `Db`, `JazzClient`, `TestingClient`, public schema/permission builders,
  `row_input!`, and public query/subscription APIs. Do not introduce JSON-like
  schema, permission, or query definitions for this ordering coverage.

### 6.11 Subsumed query and SQL notes

The old QueryManager notes are now treated as migration context for this
chapter's stable query vocabulary. Jazz keeps one normalized query AST for
one-shot reads, live subscriptions, policy shapes, schema/lens projected reads,
and sync coverage shapes. It may choose index-first planning, materialization,
or groove lowering per shape, but these are execution strategies under one
validated shape identity.

Array subqueries remain distinct from include paths. They represent correlated
one-to-many result fields with parent-column to child-column bindings. One-shot
materialization may evaluate them directly; maintained subscriptions require
the relation/path terminal-delta machinery in ch. 16 before they are accepted as
live shapes.

SQL is an entry surface, not a second semantic model. A Jazz SQL dialect should
lower into the same query AST and reject unsupported SQL constructs loudly.
Custom DSL helpers should likewise normalize into the AST rather than building
parallel query identities.

## Open Questions

### Open questions

- 🔶 **Local one-shot reads vs. settled coverage reads.** Ordinary one-shot
  `all`/`one` reads are local-source reads: at tier `global` they evaluate over
  the globally durable rows known to the node, and may opportunistically reuse a
  settled maintained result-set cache when one exists. That cache is not a proof
  that a partial node has complete remote coverage. Any API that promises
  remote/settled coverage must request a coverage witness explicitly (for
  example by attaching/subscribing to the maintained view) and must error or
  report unsettled state when that witness is absent.
- 🔶 **Maintained array-subquery subscriptions.** One-shot reads may
  materialize `array_subqueries` as relation row batches plus edges, but live
  subscriptions reject array-subquery shapes loudly until unified
  relation/path lowering or relation-edge terminal deltas can maintain them.
  Sync coverage must not recursively register coarse child shapes as a
  production fallback.
- 🔶 **Output-changing relation queries.** The alpha-compatible `hopTo` and
  `gather` surfaces produce rows whose output table may differ from the seed
  table. Current Rust `Query::{joins, reachable}` are fixed-root filters, so
  they are not a faithful encoding for this API. Relation-query facade syntax
  should normalize immediately into the unified row-set program vocabulary
  (`TableScan`, `Filter`, `Project`, `Join`, `Union`, `Gather`, `OrderBy`,
  `Offset`, and `Limit`) rather than owning a separate validated/cache identity;
  then route TS/WASM/NAPI
  `all`/`one`/`subscribeAll` through that single path.
- 🔶 **Relay coarser covering shapes.** Upstream subscription collapse onto
  coarser covering shapes is a design direction, not a current MUST (ch. 8).
- 🔶 **Non-uuidv7 id creation-order claims.** Ascending row id is the default
  semantic order for all row ids, but only uuidv7-generated ids carry the
  creation-time approximation. Caller-supplied ids, deterministic test ids, and
  any future non-uuidv7 id source must not be documented as creation ordered
  unless that id source explicitly preserves creation-time ordering.
- 🔶 **Cross-type id and group-key comparison.** Current row tables use `RowUuid`
  identity, so default row ordering does not require comparing different id
  types inside one relation. If future relation-valued outputs can mix id types
  or grouped outputs can expose heterogeneous key domains at one key position,
  the spec needs a stable cross-type ordering rule or must reject those shapes.
- 🔶 **SQL dialect boundary.** Define the first supported SQL subset, parameter
  syntax, error reporting, and escape-hatch rules, and prove it lowers to the
  same `Query` contract as the builder DSL.
- 🔶 **COUNT aggregation.** Add terminal count queries for filtered relations,
  with reactive `COUNT(*)` as the MVP shape, without adding a separate
  aggregation result identity outside the query AST.
- 🔶 **Array-subquery dirty-list dedupe.** The former `array_subquery_tables`
  backlog noted duplicate `(node, table)` entries. Consumers tolerate duplicates,
  but deduping the tracking set would reduce mutation-time work and make the
  maintained path easier to reason about.
- 🔶 **Correlated subgraph sharing.** Per-outer-row recompilation is correct but
  too expensive for large result sets. Shared hash-index or prepared-shape based
  correlated execution should preserve parent binding semantics while avoiding
  one graph per outer row.
