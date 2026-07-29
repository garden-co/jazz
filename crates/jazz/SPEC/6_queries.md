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

- `groove/SPEC/INVARIANTS.md::INV-INC-1`: Incremental delivery invariant (mechanism law). For any maintained view, the work performed to ingest, apply, and publish a change — including snapshot assembly, diffi...
- `INV-LOWER-11`: Prepared graph lowering MUST preserve the semantics of every accepted predicate shape and explicitly reject unsupported predicate shapes.
- `INV-LOWER-13`: Aggregation, ordinary read ordering, general pagination, and projection MUST be applied by the node after row materialization, not required from groove lowering, excep...
- `groove/SPEC/INVARIANTS.md::INV-QUERY-1`: A query graph node MUST be identified by the full NodeDescriptor consisting of operator, ordered inputs, and output; two incompatible descriptors MUST NOT share a node...
- `groove/SPEC/INVARIANTS.md::INV-QUERY-2`: A NodeDescriptor MUST validate operator input arity, input/output descriptor compatibility, join key arity, and field-index bounds before the runtime accepts the node.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-3`: FilterOp MUST emit exactly the input deltas whose records satisfy its PredicateExpr, preserving record bytes and weights, for the supported predicate surface including...
- `groove/SPEC/INVARIANTS.md::INV-QUERY-4`: SQL predicate lowering MUST reject unsupported or ill-typed predicate expressions instead of lowering them approximately.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-5`: MapProjectOp MUST emit one output delta per input delta, copying only configured fields into the output descriptor and preserving the input weight.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-6`: UnwrapNullableOp MUST drop Nullable(None) input deltas, unwrap Nullable(Some()) to the inner value, and preserve the original delta weight.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-7`: Union MUST require all non-empty inputs to have the same output descriptor and MUST preserve duplicate derivations as separate weighted deltas (UNION ALL semantics).
- `groove/SPEC/INVARIANTS.md::INV-QUERY-8`: An inner JoinOp MUST require equal-length left and right key vectors.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-9`: An inner JoinOp MUST emit joined records with weight leftweight \* rightweight for matching keys, including matches produced by changes arriving on either side.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-10`: An inner JoinOp MUST NOT double-count pairs where both matching sides changed in the same logical tick.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-11`: Shared join arrangements MUST apply a given logical-time delta at most once per arrangement key/scope, even when multiple joins consume the arrangement.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-12`: AntiJoin MUST output left rows only when the total right-side multiplicity for the join key is zero.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-13`: AntiJoin MUST retract or restore visible left rows only when the right-side count crosses zero; changes that keep the right count nonzero MUST NOT emit anti-join deltas.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-14`: Same-tick anti-join updates MUST suppress a left row that arrives with a matching right row and MUST emit a left row exactly once when it arrives in the same tick as t...
- `groove/SPEC/INVARIANTS.md::INV-QUERY-15`: SQL planquery MUST reject query parameters; parameterized SQL MUST go through planpreparedshape/prepared binding flow.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-16`: SQL prepared-shape lowering MUST accept only equality predicates of the form column = $parameter or $parameter = column as binding predicates.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-17`: SQL lowering MUST reject unsupported SELECT/set/join shapes explicitly, including SELECT DISTINCT, grouped/ordered/limited selects, non-inner joins, and non-UNION ALL...
- `groove/SPEC/INVARIANTS.md::INV-QUERY-19`: BindingSourceOp MUST NOT be evaluated through ordinary subscription/query graphs outside prepared shapes.
- `groove/SPEC/INVARIANTS.md::INV-QUERY-20`: ArgMaxByOp and ArgMinByOp MUST accept arbitrary upstream graph inputs. Base-table inputs MUST have primary-key columns exactly groupcols + ordercols; non-table inputs...
- `groove/SPEC/INVARIANTS.md::INV-QUERY-21`: ArgMaxByOp and ArgMinByOp MUST emit only winner changes for touched groups, suppressing non-winner changes and net-zero group deltas.
- `groove/SPEC/INVARIANTS.md::INV-SHAPE-16`: Prepared shapes MUST retain their output graph nodes for the lifetime of the database unless/until an explicit shape-drop API exists.
- `INV-QUERY-1`: `Query::validate` MUST stamp a shape with the schema version it validated against, and `ShapeId` MUST include both canonical query bytes and `SchemaVersionId`.
- `INV-QUERY-2`: Semantically identical commutative query forms MUST produce the same `ShapeId`; semantic predicate changes MUST produce a different `ShapeId`.
- `INV-QUERY-3`: `BindingId` MUST be derived from canonical binding bytes in parameter-name order, and bindings MUST reject missing, unknown, or type-mismatched params.
- `INV-QUERY-4`: Shape registration MUST reject an AST whose content-addressed id does not match `shape_id`, and MUST park registrations naming an unknown schema version until the schema catalogue arrives.
- `INV-QUERY-5`: `Subscribe` MUST name a registered shape and match inferred parameter arity; the supplied usage-site subscription id is independent from the binding id, and `Unsubscribe` MUST drop that usage subscription's settled result set.
- `INV-QUERY-6`: `RegisterShape` followed by `Subscribe` MUST cause the serving side to attach the usage-site subscription to the matching canonical program instance `(ShapeId, ResolvedReadKey, PolicySharingKey, BindingId)` and respond with a reset-result-set `ViewUpdate`.
- `INV-QUERY-7`: A reset-result-set `ViewUpdate` MUST replace the subscription result set while retaining per-peer version dedup state.
- `INV-QUERY-8`: Query `ViewUpdate` result sets MUST be addressed by a canonical program instance and carry typed result membership with enough version/read-view context to distinguish content versions, deletion-register visibility, branch/historic membership, synthetic rows, and path tuples. Real-row members MUST expose the ordinary current-row `(table, row_uuid, content_tx_id)` projection only as a compatibility/payload-bundling projection, not as the complete identity.
- `INV-QUERY-9`: Result-set material MUST include output rows plus matched include-reference and join/junction contribution rows, MUST exclude traversed non-matches and failed include paths from subscription payloads, and MUST apply read-policy/policy-atomic filtering before emission.
- `INV-QUERY-10`: Include missing-target semantics MUST be local view/API behavior: `JoinMode::Inner` drops parents with unresolvable include targets, `JoinMode::Holes` keeps them, and `require_includes` tightens holes mode by requiring include matches without broadening payload material; sync MUST NOT drop readable parents solely because included targets are absent.
- `INV-QUERY-11`: Local/unsettled query reads MUST return rows complete only relative to node-local visible-current knowledge.
- `INV-QUERY-12`: Settled query reads on a subscriber MUST be answerable from the subscription's settled subscription result set; unresolvable result-set entries are an invariant violation rather than a degraded answer.
- `INV-QUERY-13`: `tx_query` inside an open exclusive transaction MUST record a binding-sensitive `PredicateRead { shape_id, shape, binding_id, binding_values }`.
- `INV-QUERY-14`: Exclusive predicate validation MUST reject an exclusive transaction when the shape/binding output set changed between `base_snapshot.global_base` and validation time, and MUST ignore irrelevant changes outside the shape.
- `INV-QUERY-15`: Incremental query result-set updates MUST converge to the same typed result-member and program-fact state as a full rehydrate over the same committed state.
- `INV-QUERY-16`: Same-drain result churn MUST be folded by net output-row outcome: enter-then-leave sends no stale add, leave-then-reenter replaces the old entry, and same-tx retract/assert churn sends no update.
- `INV-QUERY-17`: When a row remains in a query result but its visible content version changes, result-set entries MUST track the new `TxId` even if projected cell values are identical.
- `INV-QUERY-19`: Exclusive transaction view shipping MUST be view-atomic, not transport-atomic: a visible exclusive result for a maintained subscription view MUST include every exclusive version required by that view, but the `VersionBundle` MAY omit transaction versions outside that view.
- `INV-QUERY-20`: Query payload dedup MUST be per peer across all subscriptions for complete transaction payloads: already-covered complete payloads are referenced via `peer_payload_inventory.complete_tx_payloads`, and partial bundles, including partial mergeable or exclusive bundles, MUST NOT establish complete-transaction payload coverage.
- `INV-QUERY-21`: Array subqueries MUST be represented separately from forward `Include` paths and MUST emit relation payload edges `(source_table, source_row_uuid, relation, target_table, target_row_uuid)` plus row batches; child filters/select/order/limit affect only child relation material, optional unreadable children are omitted with their edges while readable parents remain, and explicit requirements are the only array-subquery form that can filter root membership.

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
form listed here is part of the `Query` contract. The query surface MUST either
define executable semantics for a form or reject it explicitly; it MUST NOT
silently substitute an approximate result.
`order_by`/`aggregate`/general `limit`/`offset` are applied by the node _after_
row materialization for ordinary reads, rather than pushed into groove lowering
(ch. 14, `INV-LOWER-13`). Maintained subscription exceptions are unordered
`limit(1)` with offset `0`, which lowers through groove `ArgMinBy` over
`row_uuid`, and ordered result windows, which lower through groove `TopBy`
(ch. 14). Ordered windows may be finite (`limit` present) or an unbounded
ordered suffix (`limit` absent); the latter keeps full ordered membership and is
not a fallback to one-shot sorting. Prepared graph lowering MUST preserve the
semantics of every accepted predicate shape and explicitly reject unsupported
predicate shapes (`INV-LOWER-11`).

**Implementation status (2026-07-27).** Parameterized `!=` predicates are
accepted for maintained subscriptions; the behavior is covered by
`maintained_subscription_view_ne_param_stays_maintained` in
`crates/jazz/src/peer.rs`.

An `array_subquery` names an output relation (`column_name`), an inner table,
and a correlation from a parent-scope column to an inner-table column. It may
carry child-local filters, select columns, ordering, limit, requirement, and
nested array subqueries. Array subqueries support direct correlations. They MUST
reject subquery joins unless those joins have defined query and
maintained-subscription semantics. `array_subqueries` are
canonicalized into shape identity separately from includes; sibling ordering is
not semantic, but duplicate sibling `column_name`s are rejected.

### 6.1.1 Membership and containment filters

Membership and containment semantics are core-owned query semantics. Binding
layers may provide typed builders and literal conversion, but must lower into
the core predicate vocabulary without re-implementing match rules.

Supported matrix:

| Column type | `in` | `contains` |
| --- | --- | --- |
| Text/String | Membership in a list of text values. UUID literals may be coerced to their string form for compatibility. | Substring containment with a text needle. |
| Integer / BigInt / Float / Timestamp | Membership in a list of same-type scalar values. | Rejected. |
| Boolean | Membership in a list of boolean values. | Rejected. |
| UUID/reference | Membership in a list of UUID values. String UUID literals may be coerced to UUIDs at lowering boundaries. | Rejected. |
| Enum | Membership in a list of enum-compatible values. String literals may be coerced to discriminants. | Rejected. |
| Bytea | Membership in a list of whole byte-array values. | Rejected. |
| Json | Whole-value equality membership only where the binding layer can represent the literal. | Rejected. |
| Array<T> | Membership in a list of whole-array values. | Element membership with a needle of type `T`; this includes arrays of numbers, booleans, UUID/reference values, enums, timestamps, and text. |

Invalid operator/type combinations must be rejected before execution with a
clear type error. In particular, `contains` on a scalar non-text column is never
interpreted as stringification, and `in` candidates must match the column type
except for the narrow compatibility coercions listed above. The broader
literal-vs-column coercion policy remains intentionally unspecified; new
coercions need an explicit spec decision before implementation.

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

A result set is the authoritative membership for a query in a read view.
Result-set sharing MUST be keyed by every semantic input that can affect
membership; a wire `SubscriptionKey` is a usage-site handle, not the result-set
identity. Result members MUST retain the typed membership, source, and version
information needed to deliver the result correctly; synthetic and path-tuple
members follow that same result-set contract (`INV-QUERY-8`).

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
core rows. A relation payload is a set of row batches plus typed relation facts
that identify the source and target rows across each relation level. It MUST
retain the membership, ordering, and visibility information required to apply
child changes correctly.
For a reverse relation array, the edge source is the parent row and the target
is each visible correlated child row. For nested array subqueries, child rows
become the source for the next relation level. Child filters, select columns,
ordering, and limits affect only the child relation material; they do not change
root row membership unless the array subquery has an explicit requirement.
Unreadable child rows and their edges are omitted, while readable parents remain
visible for optional array subqueries (`INV-QUERY-21`).

Alpha-style relation traversal has an output-changing query surface. A
relation-query facade MUST normalize into the same row-set program vocabulary as
ordinary queries and MUST use the same validation, identity, one-shot-read,
subscription, registration, known-state, and snapshot-serving semantics. It MUST
NOT introduce a separate sync or subscription engine.

**Implementation status (2026-07-27).** The supported single-hop `hopTo` facade
normalizes into this program family. Multi-hop traversal and `gather` are
currently rejected because matching maintained semantics have not yet been
defined.

### 6.4.1 Default result ordering

Ordering is a core-owned query semantic: it must be expressed in the lowered
plan and carried through delivered results and delta positions, never
re-derived by binding layers (ch. 13 §13.13).

Decision, Anselm 2026-07-18: when a relation-valued result has no explicit
`order_by`, its default order is ascending row id (`RowUuid`). This applies at
every relation-valued result boundary: root query rows, relation payloads from
`array_subqueries`, and nested include/relation subtrees. A parent row's child
relation is therefore ordered by child row id unless that child relation carries
its own explicit `order_by`. The default is intentionally cheap: it matches
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

### 6.5 Query-driven sync

A subscription binds a shape to one binding in one read view. `RegisterShapeOptions`
carry a semantic `ReadViewSpec` describing the requested current, branch,
merged-branch, owner-qualified historic snapshot, schema-projected, and
overlay-visible view. The serving/runtime boundary derives the authoritative
resolved read identity from the semantic read view plus tier; callers do not
supply the key as independent identity. The wire vocabulary is `RegisterShape`,
`Subscribe`, `Unsubscribe`, and `ViewUpdate` (ch. 8).

The serving authority maintains the settled result set for each program instance:
the result member set plus its matched include paths, relation edges, and join
witnesses (§6.4).
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
complete transaction payloads: a complete payload already shipped to a peer is
emitted at most once per update (`INV-QUERY-20`). Partial payloads, including exclusive
payloads, do not establish complete-transaction payload coverage unless the peer
has received all versions for the transaction. Exclusive `ViewUpdate` visibility
is view-atomic: a bundle may carry the exclusive versions needed for the
maintained subscription view, and result members for that view are emitted only
when that view's exclusive payload is complete (`INV-QUERY-19`, ch. 3).

A subscription MUST remain active until it is explicitly removed; it MUST NOT
expire solely because a TTL elapses. Registration and re-registration are
idempotent. Prepared-graph retention is an implementation choice subject to
`groove/SPEC/INVARIANTS.md::INV-SHAPE-16`.

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
surface. The test plan below records additional intended coverage.

- Strengthen the maintained-vs-one-shot differential oracle command
  `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
  to assert ordered equality rather than set equality for root rows and every
  relation payload. The oracle should keep using public query shapes/builders
  and compare the maintained stream's reduced result to the one-shot result at
  each checkpoint.
- Extend the TS query API coverage in
  `packages/jazz-tools/tests/ts-dsl/query-api.test.ts` so result arrays that
  currently sort ids before comparison become ordered-equality assertions. Add
  explicit cases for
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
  `groove/SPEC/INVARIANTS.md::INV-INC-1`.
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
and maintained subscriptions use the same relation-payload contract; the
maintained-vs-one-shot equivalence is covered by
`array_subquery_one_shot_and_maintained_subscription_are_equivalent` in
`crates/jazz/src/db/tests.rs`.

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
- 🔶 **Multi-hop output-changing relation queries.** Single-hop `hopTo` uses
  the normalized relation-query path. Define the semantics for multi-hop
  traversal and `gather`, including their result identity, ordering, and
  maintained-subscription behavior. They must normalize into the unified row-set
  program vocabulary rather than own a separate validated/cache identity before
  TS/WASM/NAPI route `all`/`one`/`subscribeAll` through them.
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
