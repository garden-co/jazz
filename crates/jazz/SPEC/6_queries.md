# jazz — Specification · 6. Queries

## Overview

A jazz query is a content-addressed **shape** plus a **binding**, evaluated to a
result set that includes matched include paths and join witnesses, and synced
incrementally. This chapter defines the query AST, shape/binding identity, the
matched-path result-set material, and query-driven sync at the result-set level.
Queries lower onto groove
prepared shapes (ch. 14), and provide the substrate used by authorization
(ch. 7) and sync (ch. 8).

### Responsibility boundary

Jazz owns query meaning and lowering. It validates schema-stamped shapes,
selects durability/read-view semantics and physical access paths, projects lens
lineages, and lowers those decisions into one Groove graph. Jazz does not
materialize a Groove source by launching another Jazz query. In particular,
policy preparation, source resolution, and schema projection MUST NOT call the
ordinary one-shot query pipeline to obtain an input relation.

Lowering itself is synchronous and pure. The implementation separates any
currently necessary asynchronous source preparation from
`lower_resolved_query_program`: preparation produces owned declarative source
descriptions, and lowering consumes only those descriptions plus validated
Jazz metadata. An `await` in lowering is a boundary violation. Async
preparation is migration debt unless it captures a snapshot explicitly named by
the read view; live-source preparation should disappear as Groove gains the
corresponding declarative source primitive.

Policy programs are explicit compilation dependencies, not work performed by
source preparation. Compilation first analyzes all `SourceRequest`s, derives
and deduplicates their policy-program requests by structural cache identity,
and prepares those programs. Source preparation may then only look up the
prepared policy graph and compose it synchronously with the protected source.
A missing dependency is an orchestration error; it MUST NOT trigger recursive
compilation, evaluation, or a retry from inside source preparation. Policy
dependency sources remain raw evidence as described below, so they do not
recursively apply their own read policy.

Groove owns evaluation of the lowered graph. Its evaluation session discovers
which concrete table, index, or arrangement inputs are not resident, suspends
the affected nodes, shares hydration work, and resumes them through Groove's
single work queue. Missing data is not represented by Jazz retries, nested
queries, or a second Jazz evaluation queue.

A Jazz operation may perform a keyed physical lookup when the operation itself
asks for one identified storage fact, such as the settled content winner for a
write-policy subject. That is not source evaluation: it neither lowers nor
executes a nested query and it cannot broaden into a relation scan. Frozen
historical or transaction-overlay values may be embedded as explicit graph
inputs when their snapshot identity is part of the requested read view. Live
current relations must remain graph sources so Groove can maintain and hydrate
them incrementally.

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
- `INV-QUERY-8`: Query `ViewUpdate` result sets MUST be addressed by a canonical program instance and carry typed result membership with enough version/read-view context to distinguish content versions, deletion-register visibility, branch-key/historical membership, synthetic rows, and path tuples. Real-row members MUST expose the ordinary current-row `(table, row_uuid, content_tx_id)` projection only as a compatibility/payload-bundling projection, not as the complete identity.
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
- `INV-QUERY-21`: Array subqueries are distinct from forward `Include` paths and are assembled by Groove's sole public output terminal into recursive root values. Public one-shot, subscription, and sync carriers MUST contain terminal roots or root-addressed structural edits, never a parallel relation-edge/row-batch representation. Child clauses affect only their slot unless an explicit requirement filters root membership.
- `INV-QUERY-22`: Structured query output MUST be constructed only by the output terminal as an ordered tree. Initial hydration and resets carry complete roots; maintained changes carry stable-keyed root/path insert, update, remove, and move edits without a higher-level assembler.
- `INV-QUERY-23`: A flat joined output occurrence MUST be identified by its ordered contributing source-row ids, not by its root row id; maintained delivery MUST address additions, removals, and replacements by that composite occurrence identity.

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

The engine-owned `RowUuid` is distinct from a declared `id` user column. Its
public query spelling is intentionally not standardized yet; see the linked
open question below rather than treating any current partial alias as API.

Any eventual alias must jointly define where it is accepted (filters, ordering,
aggregates, joins, correlations, reachability, and nested queries), whether it
can be selected, its type and nullability, collisions with declared `_id`, the
meaning of bare `id` in legacy schemas, and its relationship to typed row-id
join APIs.

Until this is resolved, implementations MUST preserve the documented declared
`id` semantics and existing explicit `RowId` operations, and MUST NOT infer a
universal `_id` contract from support in an individual query form.

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

Every binding layer MUST preserve a finite child `limit` across its builder,
normalized JSON, worker, and native-codec boundaries. An omitted child limit
means an unbounded ordered suffix. In the TypeScript DSL,
`include({ relation: true })`, nested object shorthand, and an included query
builder without `.limit(n)` all request the complete relation.

`JoinVia` is an existential reference/junction traversal: it constrains root
membership and supplies join witnesses; it is not a general relational join
whose record is publicly returned. Flat joined output is a separate target AST
form, described in §6.4.1. Conflating these two forms would make policy
traversal accidentally promise a public tuple shape.

### 6.1.1 Membership and containment filters

Membership and containment semantics are core-owned query semantics. Binding
layers may provide typed builders and literal conversion, but must lower into
the core predicate vocabulary without re-implementing match rules.

Supported matrix:

| Column type       | `in`                                                                                                      | `contains`                                                                                                                                   |
| ----------------- | --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Text/String       | Membership in a list of text values. UUID literals may be coerced to their string form for compatibility. | Substring containment with a text needle.                                                                                                    |
| Integer / BigInt  | Membership in a list of same-type scalar values after the checked literal normalization below.            | Rejected.                                                                                                                                    |
| Float / Timestamp | Membership in a list of same-type scalar values.                                                          | Rejected.                                                                                                                                    |
| Boolean           | Membership in a list of boolean values.                                                                   | Rejected.                                                                                                                                    |
| UUID/reference    | Membership in a list of UUID values. String UUID literals may be coerced to UUIDs at lowering boundaries. | Rejected.                                                                                                                                    |
| Enum              | Membership in a list of enum-compatible values. String literals may be coerced to discriminants.          | Rejected.                                                                                                                                    |
| Bytea             | Membership in a list of whole byte-array values.                                                          | Rejected.                                                                                                                                    |
| Json              | Whole-value equality membership only where the binding layer can represent the literal.                   | Rejected.                                                                                                                                    |
| Array<T>          | Membership in a list of whole-array values.                                                               | Element membership with a needle of type `T`; this includes arrays of numbers, booleans, UUID/reference values, enums, timestamps, and text. |

Invalid operator/type combinations must be rejected before execution with a
clear type error. In particular, `contains` on a scalar non-text column is never
interpreted as stringification, and `in` candidates (including parameters) must
match the column's whole-value type except for the narrow compatibility
coercions listed above. Integer literals are the one scalar compatibility rule:
for equality, ordering, and `in`, an Integer (`I32`) literal is widened when
compared with a BigInt (`I64`) column, and a BigInt literal is narrowed for an
Integer column only when its value is representable as `I32`. This normalization
happens before shape identity is derived, so equivalent width spellings share a
shape. It applies only to literals, never to mismatched column-vs-column types;
other numeric coercions remain unsupported. Thus an `Array<T>` `in` candidate
must itself be an array; a scalar `T` is rejected rather than being rewritten as
a singleton array or a `contains` predicate. Compatibility coercions may recurse
into an array-valued literal only when they preserve that array shape. Any
broader literal-vs-column coercion needs an explicit spec decision before
implementation.

`in` is whole-value membership and `contains` is single-element membership;
neither is an array subset/superset operator. A scalar `in` candidate for an
`Array<T>` remains invalid. Any future array-set predicate must be explicit,
never an implicit reinterpretation of `in`.

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
composition (ch. 7). `author` is the reserved logical identity claim and
resolves to the authenticated `AuthorSubject` JSON string `[iss,sub]`; provider
claims such as `sub` and `user_id` retain their admission-defined values. Additional claim names are product/admission-defined
and must come from the trusted admission/session context, never from ordinary
query bindings.

#### Prepared claim parameters

When a query program contains policy claims, lowering MUST first walk the
actual emitted prepared graph—including every non-raw authorization subplan—and
declare one ordered, graph-wide parameter set before it emits any graph node.
The walk includes every binding source at every nesting level, including
recursive seed/step paths and every union route within that emitted graph. Each
claim reference is declared by its canonical parameter name and type; a
repeated reference denotes the same declaration. Every binding source in the
emitted prepared graph MUST use that one shared declaration environment. A
claim in a prepared graph MUST lower as a parameter reference and MUST NOT
lower as a policy-context value literal.

A policy dependency read is raw evidence under `INV-RLS-21`, available only to
the trusted server-side policy evaluator while it decides the outer policy. It
is not a client result, subscription payload, or transport capability. Lowering
therefore does not recursively inspect that dependency table's separate policy
declaration merely to compare claim types. Likewise, independently
inline-evaluated policy branches do not share a prepared descriptor. Their
claim declarations become comparable only if a future lowering actually places
them in one descriptor.

The prepared graph descriptor MUST encode that parameter set -- names and
types, including claim-path identity where names alone do not establish it --
but MUST NOT encode the values bound for a particular identity. Parameter
values are bound independently for each execution from the authenticated policy
context. Thus two identities with the same query/policy shape legitimately
share one prepared graph while receiving results evaluated with their respective
claims. This is the query-level application of
`groove/SPEC/INVARIANTS.md::INV-QUERY-1A`: the descriptor captures every
output-affecting input's declared identity and type, while a binding supplies
its execution-time value.

A claim has one of two semantic roles wherever lowering considers an
arrangement key. In a **filter role**, such as `row.team == claim.team`, it is
an execution-time parameter and MUST NOT be baked as a constant into an
arrangement key; one shared arrangement serves every identity and filters using
the bound value. In a **partition role**, such as a maintained `top_by` window
partitioned by `claim.team`, the claim value MAY and MUST participate as a
partition dimension: the one shared arrangement contains all partitions and
each binding reads its own partition. A partition dimension describes key
structure, not a subject-specific constant, and therefore preserves graph
sharing. Lowering MUST use the established policy-comparison path for claim
comparisons; any policy-only normalization remains scoped to that comparison
path and MUST NOT change ordinary arrangement-key encodings.

### 6.4 Result sets, include paths, and structured output

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

**Decision, Anselm 2026-08-05 — one output terminal, two shapes.** Joins
produce flat, wide rows in the Groove graph. `CollectBy` is the sole output
terminal and chooses the public shape: **collect** renders an ordered recursive
tree, while **expand** renders flat tuples. A collect descriptor is a tree of
named slots: every array field names one sibling relation on its owning record
and carries its owner-group, child projection, order/tie, direction, offset,
limit, and nested slots. Slot names are unique among siblings and a nested slot
is addressed by its field-name path (for example `comments.replies`). Nothing
on the Jazz side renders,
fan-ins, or otherwise reshapes maintained output. The IVM graph and its
lowering remain flat and DBSP-native: parent rows, child rows, associations, and
joined tuples are ordinary weighted rows. A graph delta MUST NOT update an
inner collection or carry an already-rendered form.

**Implementation status (2026-08-09).** Groove `CollectBy` is the authoritative
structured terminal. One-shot reads materialize its recursive roots directly;
local application subscriptions carry typed root/path edits. The current sync
transport's terminal edits are migration scaffolding only: the intended peer
contract carries the canonical witness closure, and the receiver's local IVM
recreates these edits (ch. 8 §8.4.1). Jazz, N-API, WASM, and TypeScript transport
or apply local terminal edits but do not reconstruct structured output from
relation facts.

Nesting and flat expansion are constructed only by Groove's output-terminal
`CollectBy` (`groove/SPEC/3_queries_operators.md` §3.6.1). A collector MUST NOT
feed any graph node, including another collector; graph validation MUST reject
that shape. Nested slots are data inside that one operator rather than collector
inputs, so `INV-QUERY-27` is unchanged. In collect mode, one terminal-owned
internal collection tree reads flat associations and writes the final recursive
value. In expand mode, the same terminal reads flat wide tuples and writes
tuples directly; a tree descriptor is invalid in Expand mode, which remains
single-level and flat. The descriptor MUST
encode every input that affects either output, as required by
`groove/SPEC/INVARIANTS.md::INV-QUERY-1A`; Jazz names the query shape,
correlation, projection, ordering, bounds, source positions, and terminal mode
that supply those inputs. Jazz has no renderer or terminal policy-composition
role.

Incremental structured delivery is a typed terminal edit stream. Every edit is
addressed by a stable root key, a typed field/key path, and an edit: `Insert`
with an explicit index, `Update`, `Remove`, or `Move` with an explicit new
index. Root edits use an empty path. An initial hydration or authoritative reset
contains complete terminal roots and replaces cached terminal state before any
following FIFO edits. Ordinary changes MUST NOT fall back to relation edges,
row batches, whole-result replacement, or facade-side tree diffing.

Child and root order are semantic. The query comparator remains the source of
that order: explicit `order_by` when supplied, otherwise ascending `RowUuid`,
with the specified stable tie-break. The terminal owns the ordered state and
emits an indexed `Insert`/`Move` when a front insertion, reorder, or window
boundary crossing changes position; consumers never infer order from arrival.

Array subqueries support `order_by`, `offset`, and an optional finite `limit`
with the same semantics as those clauses at the root query. Omitting `limit`
means the complete ordered suffix; zero yields an empty child array. Logical
result size is independent of physical frame size because transport fragments
and atomically reassembles large logical messages. Child clauses change only
their terminal slot unless an explicit requirement filters the root; unreadable
children are omitted while readable parents remain for an optional relation.

The terminal's recursive value uses Groove's inline
`ValueType::Record(Box<RecordDescriptor>)` descriptor form, not a descriptor
registry. Nested record bytes MUST be canonical on decode: decode, recreate
with the declared descriptor, and byte-compare before accepting them. This is
required because `OwnedRecord::new` currently accepts arbitrary raw bytes
(`crates/groove/src/records/mod.rs:1577-1582`), and non-canonical bytes would
break weighted-delta consolidation. A record-valued value, or a type containing
one, MUST be rejected in arrangement keys and durable primary keys; it is an
opaque rendered value, never an ordering/key codec (`groove/SPEC/2_storage_model.md`
§2.2 and `groove/SPEC/3_queries_operators.md` §3.6.1).

**Decision, Anselm 2026-08-04 — children carry explicit ids.** Every child
record in a structured result MUST carry its source row id as an explicitly
projected field. Deterministic array position alone would satisfy delta
application, but an explicit id is what lets a consumer preserve object identity
across reorders without re-deriving keys — and key derivation in the consumer is
precisely the duplicated semantics this design removes.

The id MUST be an explicit projected field. It MUST NOT be recovered from
implicit source-row bytes, because that would make an internal encoding load
bearing at the public boundary.

A child id identifies the source **row**, not the output **occurrence**: one row
may appear under more than one parent, and under bag semantics the same row may
occur more than once within a single parent's array. Position therefore remains
the occurrence discriminator, and consumers MUST NOT assume a child id is unique
within one parent's array unless the query shape guarantees it.

**Decision, Anselm 2026-08-08 — transport framing does not constrain query
semantics.** Two mechanisms apply:

1. **An omitted limit is unbounded.** An array subquery MAY carry a finite child
   limit; without one it selects the complete ordered suffix after `offset`.
   Result size is a transport concern and MUST NOT require callers to invent a
   semantic row bound.
2. **Transport fragments oversized logical messages.** A result can exceed an
   individual transport frame. The transport MUST decompose and reassemble it
   atomically rather than rejecting, truncating, or partially delivering it.
   A partial parent is a wrong answer.

Alpha-style relation traversal has an output-changing query surface. A
relation-query facade MUST normalize into the same row-set program vocabulary as
ordinary queries and MUST use the same validation, identity, one-shot-read,
subscription, registration, known-state, and snapshot-serving semantics. It MUST
NOT introduce a separate sync or subscription engine.

**Implementation status (2026-07-27).** The supported single-hop `hopTo` facade
normalizes into this program family. Multi-hop traversal and `gather` are
currently rejected because matching maintained semantics have not yet been
defined.

### 6.4.1 Flat joined output, wide lowering, and output occurrences

**Decision, Anselm 2026-08-05 — expand is the other `CollectBy` mode.** A
flat join emits one row for every matching ordered tuple of the root source
followed by the declared join sources. Its public data columns are the flattened
columns of that tuple; it is not a nested relation payload and it does not
change the semantics of `JoinVia`. It is `CollectBy(mode = Expand)` over a
unary, already-wide association stream, not a Jazz renderer over several
sinks.

Flat-join lowering MUST resolve, read-policy-filter, and read-view-project each
source before it reaches the join. It then lowers the declared `FlatJoin` chain
as ordinary Groove inner joins. After each join it projects the joined
descriptor back to the stable, qualified source columns while retaining **all**
prior left-source columns and adding the new right-source columns. Thus the next
join sees the accumulated wide left row, and the final terminal input carries
every source needed for the tuple and its identity. The contribution lowerer
used for include closure intentionally does something narrower: it projects a
join to `RIGHT_JOIN_PREFIX` only
(`crates/jazz/src/node/query_engine/lowering.rs:5199-5251`). That projection is
correct for a source-membership fact, but MUST NOT be reused for flat output.

This is achievable with the existing Groove join representation. An inner join
infers a descriptor containing all left fields followed by all right fields
(`crates/groove/src/ivm/runtime/mod.rs:1354-1357` and
`7900-7917`), and its descriptor retains the ordered left and right input
descriptors (`crates/groove/src/ivm/op_types.rs:167-175`). Join arrangements are
keyed by the declared join keys, not by every carried output column
(`crates/groove/src/ivm/runtime/mod.rs:5703-5758`), so wider payload rows do not
create an arrangement-key arity obstacle. They do increase arrangement value and
terminal-payload bytes, which are ordinary descriptor/payload costs, not a
reason to introduce a second terminal. The source resolver already supplies the
policy-filtered projected source boundary before query composition
(`crates/jazz/src/node/query_eval.rs:537-1066`, `2036-2184`); wide lowering
preserves that ordering.

The one-shot public Rust facade currently returns `Vec<(ObjectId, Vec<Value>)>`
and converts the row id from the materialized root row
(`crates/jazz/src/tools/client.rs:2052-2102`). For a flat join its first element
MUST remain that root `ObjectId` for source compatibility. It is a representative
root id, not the identity of the joined result: several joined output rows may
therefore carry the same first element. One-shot callers receive complete tuples
at once and perform no incremental address-based application, so this is not an
ambiguity in one-shot semantics.

For a maintained subscription, every flat joined row MUST instead carry an
`OutputOccurrenceId`. It is the ordered vector of contributing source row ids:
`(root-row-id, join[0]-row-id, ..., join[n]-row-id)`. Source position is part of
the encoding; the query shape supplies the table/alias at each position. The
wire value is opaque to callers except for equality and reconciliation, and its
canonical positional encoding MUST be usable as a terminal-state key and for
consolidation. It MUST NOT be derived from result position, payload bytes, a
content transaction id, or an unordered set of source ids. It is consequently
stable across ticks, resets, source-content replacements, and result reordering.

**Flat payload revision digest.** A non-empty flat output also carries the
opaque `row_digest` used by durable `ResultMemberEntry`, result-payload, and
program-fact state to distinguish a source tuple whose projected payload has
changed. This is not its occurrence identity. Its permanent `JFRD` V1
preimage is `magic "JFRD" | version 1 | field_count:u32be |
descriptor_len:u32be | Groove-record-descriptor | record_len:u32be |
Groove-record`. The descriptor has the source-order payload types and the
engine-owned names `flat_join_payload_0`, `flat_join_payload_1`, …, so user
projection aliases do not affect it. `row_digest` is BLAKE3's derived-key hash
of that preimage with context `jazz.flat-join-row-digest.v1`.

For example, declared payload `(u64, string)` and values `(7, "blue")` have
the frozen preimage
`4a4652440100000002000000a8050000002a00000053000000690000009200000000000000000000000002000000120000000000000000010000000000000000010000002500000001666c61745f6a6f696e5f7061796c6f61645f300000000005000000000000000000000000120000000000000000010000000000000000010000002500000001666c61745f6a6f696e5f7061796c6f61645f31000000000a0000000000000000000000001200000000000000000000000d070000000000000002626c7565`
and digest
`d4bacd5d453e647a4da1c55842ddbf8e39a263ceb1ddb07f3f8fac090ff9480b`.
Changing `"blue"` changes the digest; renaming either projection does not.
No generic Rust serializer is part of this durable identity.

This is one type with variable arity, not two identity schemes. A collect-mode
parent has a one-element `OutputOccurrenceId`, its parent source-row id. An
expand-mode tuple has the ordered vector above. The terminal configuration
declares source position and arity, so both use the same canonical bytes as
terminal-state keys, subscriber-cache keys, and wire addresses. A source row id
remains useful object identity but is not by itself an occurrence identity: a
joined row can occur under multiple roots, and a root can occur in multiple
joined tuples.

**Multiplicity boundary.** Groove joins are weighted
(`groove/SPEC/INVARIANTS.md::INV-QUERY-9`). Ordinary flat joins use the ordered
source-id vector above. A `UNION ALL` relation arm additionally contributes its
stable normalized arm label immediately before that arm's source-row id. This
typed `(arm-label, row-id)` carrier is retained below the public projection and
is used by Root grouping, maintained membership, reset snapshots, and
`ResultKey`. Row-only keys retain the version-1 UUID-vector encoding byte for
byte; keys with derivation discriminators use the version-2 typed encoding.
Empty, duplicate-position, or out-of-range discriminators are malformed.
Nested non-recursive unions compose their stable semantic arm labels into one
typed path component; sibling insertion or reordering therefore does not rekey
unchanged arms. Sibling arm labels MUST be non-empty and unique within their
union. A producer that cannot supply unique stable semantic labels MUST fail
lowering rather than substitute traversal order or normalized node identity,
both of which can churn occurrence keys after unrelated graph edits. Recursive
bag inputs without a finite stable labeled source-row carrier MUST likewise fail
lowering rather than collapse copies.
Two semantically identical `UNION ALL` arms therefore require distinct stable
labels supplied by their normalized source. A builder without such declared
identity MUST reject that shape; it MUST NOT invent an ordinal merely to retain
duplicate derivations.

Public subscription transports MUST carry the opaque versioned `ResultKey`
sidecar aligned with added, updated, and removed rows. They MUST validate the
alignment and key encoding before applying a delta. Serializing the legacy
two-field `OutputOccurrenceId` alone is insufficient because its byte-compatible
form intentionally does not carry typed union-arm discriminators.

Maintained flat-join additions, removals, updates, reset snapshots, and the
subscriber cache MUST be keyed by `OutputOccurrenceId`; the root `ObjectId`
MUST NOT be used as that key. The local public adapter still rejects any
`Query` with `joins` (`crates/jazz/src/tools/client.rs:1956-1967`). Structured
collect-mode delivery already uses terminal-owned roots and edits, but expand
mode must establish this composite address before public joins are enabled. If
joins were enabled without that boundary change, the adapter would silently
collapse outputs sharing a root:
it searches and replaces `current_rows` by `row_uuid`
(`crates/jazz/src/tools/client.rs:2160-2192`), and its ordered delta ids are that
same `ObjectId` (`crates/jazz/src/tools/client.rs:2202-2239`). This is neither a
loud failure nor a valid delta application.

The maintained wire MUST carry `OutputOccurrenceId` as the address of each
flat-join output add, removal, and replacement, including reset snapshots. It
MAY continue to reference the ordinary typed source members/version payloads for
row bodies; it MUST NOT add a per-source provenance envelope, multi-sink
terminal input, or terminal policy-composition field merely to construct joined
output identity.

The flat-join descriptor is constructed in declared source order: root first,
then each `JoinSpec`. Every field name is qualified as
`<effective-source-name>.<column>`; the effective source name is the root alias
or root table name, and for a join its alias or table name. Aliases MUST be
unique across the root and joins. An unaliased repeated table name is therefore
rejected, as are duplicate effective names. `select`, predicates, and
`order_by` referring to a flat join MUST use qualified names unless validation
can prove an unqualified name has exactly one candidate; canonical shape bytes
and the emitted descriptor use the qualified spelling in all cases. Thus columns
with the same physical name never collide, descriptor names and field order are
stable, and a descriptor does not depend on runtime data.

The existing public `JoinSpec { table, alias, on }` advertises aliases and an
arbitrary equality from the accumulated left result to the newly joined table
(`crates/jazz/src/tools/public_api/query.rs:196-213`). The core `JoinVia` AST is
not that form: it has no alias or accumulated-left scope, and only represents a
root- or immediately-nested reference/junction traversal with a target column,
optional source column/lookup, correlations, filters, and nested joins
(`crates/jazz/src/query.rs:2045-2071`; validation at
`crates/jazz/src/query.rs:2713-2822`). **Recommendation:** extend the core with
a separately named flat-join AST and lower the public `JoinSpec` into it; do not
try to encode aliases or arbitrary accumulated joins in `JoinVia`, and do not
leave the public builder advertising a shape that the client rejects. This is a
small, explicit surface addition rather than a policy-model change.

`result_element_index` is not part of core `Query`
(`crates/jazz/src/query.rs:24-61`), and MUST be removed from the
public flat-join surface. It is fully expressible as a qualified projection:
select the desired source's qualified columns (including its explicit row-id
field where needed), or make that table the query root when its `ObjectId` is the
desired representative id. Keeping a positional tuple selector would duplicate
projection semantics and create a second, underspecified answer to which object
id identifies a flat row.

Read-policy filtering and read-view/lens projection are source operations. Each
flat-join input MUST use its resolved, policy-filtered, read-view-projected
source before it reaches the join; ordinary join retraction then propagates a
permission or source-row removal. No terminal policy composition,
facade-side fan-in, or source-specific ordering contract is required. A join
node has ordered input descriptors, each already encoding its source/read-view
and policy semantics, satisfying `groove/SPEC/INVARIANTS.md::INV-QUERY-1A` by
construction. Groove's inner `JoinOp` already emits matching joined records with
left-weight times right-weight on changes from either input
(`groove/SPEC/INVARIANTS.md::INV-QUERY-9`); this design adds only wide lowering,
the terminal mode/descriptor, and occurrence-addressed delivery.

The flat-join design is the separately named
`FlatJoin` AST, lowering of public `JoinSpec` into that AST, source-resolved
policy-filtered chained joins, and `OutputOccurrenceId`. This decision discards
the Jazz maintained-terminal renderer, facade-side fan-in/materialization,
multiple terminal sinks, and any lowering that drops already-joined source
columns before flat expansion. `result_element_index` remains discarded as a
duplicate of qualified projection semantics.

**Implementation status (2026-08-09).** Target/untested. No public flat join is
currently executable: the public client rejects `joins`, core query reads
materialize root `CurrentRow`s`, and expand-mode occurrence-addressed public
delivery is not complete. Collect-mode structured output already uses the sole
Groove terminal and typed terminal operations. The ordinary current
source resolver already applies source authorization and schema projection
before lowered query composition (`crates/jazz/src/node/query_eval.rs:537-1066`,
`2036-2184`); this target relies on that existing source boundary.

### 6.4.2 Default result ordering

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
parents or sibling relation payloads. A child relation's default and tie row id
is the child source row id, and its ordering is independently evaluated within
each parent/correlation group before that child's `offset` and `limit`; it never
uses parent order or another group’s child rows. This same comparator is required
for one-shot snapshots, maintained hydration, resets, and whole-parent
replacements.

For a flat joined result, `order_by` is the only cross-source ordering contract:
its qualified fields order output occurrences and the complete occurrence id is
the deterministic final tie-breaker. No join declaration order is an output sort
rule, and no source contributes an independent terminal ordering rule.

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
one-shot read at the same frontier. For a structured child relation, the
replacement array's positions are the complete order information; no separate
rank field or successor-position deltas are sent. This composes with
`groove/SPEC/INVARIANTS.md::INV-INC-1`: a default-id content update that does
not affect a rendered relation must not reorder neighboring children, while a
child insert/order change replaces only its touched rendered parent group rather
than scanning or diffing the accumulated view.

### 6.4.3 Aggregate result representation

An aggregate or grouped query returns its results through the same row-shaped
surface as any other query, because a caller should not need a second result
vocabulary to read a total. But an aggregate result row is not a stored row: it
has no row id, no version, no provenance, and no deletion state. This section
fixes how such a result is represented at each layer it crosses. The
representation — not the aggregation — is where this surface has repeatedly
failed, and each failure was a case that no layer had been told to handle.
Groove owns the operator-level contract, including which functions are
maintainable and the value-level null rules
(`groove/SPEC/3_queries_operators.md`); this section owns what Jazz delivers to
a caller.

An aggregate value crosses three boundaries. The groove `Aggregate` terminal
emits group fields and aggregate values into a record; Jazz carries that record
as a `ResultPayload` program fact keyed by a synthetic result member (ch. 16
§16.6); the public API renders it as the cells of a result row. The first two
layers are internal and MUST preserve the value exactly. The third is the only
layer permitted to reshape it.

**`INV-QUERY-30`** — An aggregate result member's identity MUST be derived
structurally from its group key, and a scalar global aggregate MUST lower to one
fixed synthetic identity. Neither the identity nor any delivery decision keyed on
it may be derived from, or matched against, a constructed name such as
`<table>_aggregate`. Filtering delivery by string comparison against a table name
is specifically forbidden: an aggregate member's name is a label, not a key.

**`INV-QUERY-31`** — Aggregate output types are fixed by function, not inferred
per call site. `count` is `U64` and is never null. `sum`, `min`, and `max` are
nullable over the non-nullable base type of their input, and `avg` is
`Nullable(F64)` regardless of input type. `sum` MUST NOT silently widen its
result type, and a sum exceeding its declared width MUST fail with a named
overflow error rather than wrapping, saturating, or promoting.

**`INV-QUERY-32`** — There are two distinct nullable layers, and exactly one
place where they merge. The payload layer carries SQL `NULL` as
`Nullable(None)`; the public cell layer carries a caller-visible absent value.
Both internal layers MUST keep a present-but-`NULL` aggregate distinguishable
from a payload that is absent altogether. The public boundary collapses the two
into one, and that collapse MUST happen exactly once, at that boundary, and
nowhere earlier. It is the only lossy step in the path, and the sole distinction
it is permitted to lose is between those two layers — which, at that point,
denote the same fact.

**`INV-QUERY-33`** — A group that is present with a `NULL` aggregate, a group
that is absent, and a group whose aggregate value has changed are three distinct
outcomes, and the delivered result MUST distinguish them. An empty group and a
group whose inputs are all `NULL` are both present-with-`NULL`, not absent.
Collapsing present-with-`NULL` into absence is a defect even when the rendered
cell is identical, because the two differ under a subsequent update.

**`INV-QUERY-34`** — The non-aggregate fields of a synthetic aggregate row —
row id, version, provenance, deletion state — carry no meaning. Producers MUST
NOT populate them with values that invite interpretation, and consumers MUST NOT
read them. A synthetic row's meaningful content is exactly its group fields and
its aggregate values.

**`INV-QUERY-35`** — A delivered change for an aggregate result member that the
subscriber does not currently hold MUST be delivered as an add, not an update.
Aggregate rows are replaced rather than mutated, so a retraction and its
replacement can cross on the wire; the delivery boundary normalizes this rather
than emitting an update against a member the peer has never seen. Maintained
delivery otherwise follows ch. 16 §16.6.

These are representation requirements, not delivery-strategy requirements: a
one-shot read, an initial snapshot, a maintained delta, and a settled subscriber
read of the same aggregate at the same frontier MUST all reduce to the same
represented result, per §6.4.2.

Decision, Anselm 2026-08-07: a scalar global aggregate over no input rows
delivers a present row — `0` for `count`, `NULL` for `sum`, `avg`, `min` and
`max` — following SQL, on both the one-shot and the maintained path. This is a
consequence of the reduction requirement above rather than an independent rule:
`groove/SPEC/3_queries_operators.md` already specified the one-shot behaviour, so
a maintained subscription that delivered nothing for the same query would make
the two paths disagree. Ch. 16 §16.6 carries the maintained-side detail,
including why this row is replaced rather than added or removed across the
transition to and from a non-empty input.

### 6.5 Query-driven sync

A subscription binds a shape to one binding in one read view. `RegisterShapeOptions`
carry a semantic `ReadViewSpec` describing the requested current source,
branch-key-qualified head and optional live/frozen base, owner-qualified historic
snapshot, schema projection, and transaction overlay. The serving/runtime boundary derives the authoritative
resolved read identity from the semantic read view plus tier; callers do not
supply the key as independent identity. The wire vocabulary is `RegisterShape`,
`Subscribe`, `Unsubscribe`, and `ViewUpdate` (ch. 8).

The serving authority maintains flat result members, association state, and
version witnesses for each program instance, then its output terminal renders
the structured result tree (§6.4). The subscriber receives and stores its own
**settled structured subscription result** and applies reset snapshots or typed
root/path edits directly; its environment-specific facade performs only typed
record decoding and patch application (§6.6). A `ViewUpdate` with
`reset_result_set = true` resets that settled result.

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
The author columns have logical type `String`/`Text` and support equality,
inequality, grouping, and equality-index lookup. Ordering by `$createdBy` or
`$updatedBy` is rejected: the portable `[iss,sub]` encoding has no public sort
semantics.

### 6.7 Conformance test plan

Default result ordering is a conformance requirement for every public query
surface. The test plan below records additional intended coverage.

- Strengthen the maintained-vs-one-shot differential oracle command
  `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
  to compare the canonical `ResultTree` specified in appendix D, rather than a
  root-id set. The oracle should keep using public query shapes/builders and
  compare the maintained stream's reduced result to the one-shot result at each
  checkpoint.
- Extend the TS query API coverage in
  `packages/jazz-tools/tests/ts-dsl/query-api.test.ts` so result arrays that
  currently sort ids before comparison become ordered-equality assertions. Add
  explicit cases for
  default root ordering, reverse/forward relation include arrays ordered by
  child id, nested structured arrays, and explicit `orderBy` preserving its
  override with row-id tie-breaks.
- Add grouped/aggregate conformance cases for default group-key ordering:
  scalar/global aggregate output, single-column groups, and composite groups
  whose input rows are inserted in non-key order. These cases should assert the
  lexicographic group-key order and explicit `orderBy` override/tie behavior.
- Add a facade-level canary next to
  `crates/jazz/tests/incremental_delivery_canary.rs` for a large unordered
  relation/include result. It should subscribe through the public `Db` API,
  insert one child whose id belongs in the middle of the child relation, assert
  the complete touched-parent replacement's order, and pin the bounded
  allocation/byte expectation for the collector exception to
  `groove/SPEC/INVARIANTS.md::INV-INC-1`.
- Keep Rust tests aligned with
  `crates/jazz/TESTING_GUIDELINES.md`: prefer black-box integration tests
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
and maintained subscriptions use the same structured-output contract; no current
test is cited for this target design.

SQL is an entry surface, not a second semantic model. A Jazz SQL dialect should
lower into the same query AST and reject unsupported SQL constructs loudly.
Custom DSL helpers should likewise normalize into the AST rather than building
parallel query identities.

## Open Questions

- 🔶 [#1783](https://github.com/garden-co/jazz/issues/1783) — Read settling, cancellation, offline authority-tier behavior, and relay coverage.
- 🔶 [#1776](https://github.com/garden-co/jazz/issues/1776) — Query IDs, aggregates, SQL boundary, and ordered windows.
- 🔶 [#1810](https://github.com/garden-co/jazz/issues/1810) — Typed membership filters and literal coercion boundaries.
- 🔶 [#1765](https://github.com/garden-co/jazz/issues/1765) — Array subquery maintenance and sharing.
- 🔶 [#1776](https://github.com/garden-co/jazz/issues/1776) — Public physical-row-id query spelling and array subset/superset predicates.
