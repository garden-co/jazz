# jazz — Specification · 6. Queries

A jazz query is a content-addressed **shape** plus a **binding**, evaluated to a
result set that includes matched include paths and join witnesses, and synced
incrementally. This chapter defines the query AST, shape/binding identity, the
matched-path result-set material, and query-driven sync at the result-set level.
Queries lower onto groove
prepared shapes (ch. 14), and provide the substrate used by authorization
(ch. 7) and sync (ch. 8).

## 6.1 The query AST

The query AST is jazz's stable vocabulary for describing rows, relationships,
projections, ordering, aggregation, and windowing. It is lowered to groove rather
than to SQL, and it is not a second execution engine.

The predicate surface is `Predicate::{All, Any, Not, Eq, Ne, In, Gt, Gte, Lt,
Lte, Contains, IsNull}` over `Operand`s. Relationship traversal is expressed by
`JoinVia` for reference joins, `ReachableVia` for recursive reachability, and
`Include` for reference expansion. Result shaping is expressed by `select`,
`order_by`, `aggregate`, `limit`, and `offset`. Every form listed here is part
of the `Query` contract; a form not yet implemented is marked at its definition,
and there is no out-of-band gate list. `order_by`/`aggregate`/general
`limit`/`offset` are applied by the node _after_ row materialization for
ordinary reads, rather than pushed into groove lowering (ch. 14,
`INV-LOWER-13`). Maintained subscription exceptions are unordered `limit(1)`
with offset `0`, which lowers through groove `ArgMinBy` over `row_uuid`, and
finite ordered windows, which lower through groove `TopBy` (ch. 14). `!=`
against a parameter is rejected until supported (`INV-LOWER-11`).

## 6.2 Shapes: validated, content-addressed, schema-stamped

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

## 6.3 Bindings and claims

A binding supplies the values for the `Operand::Param` holes inferred during
validation. Its identity is content-addressed independently of the shape:
`BindingId = Uuid::new_v5(QUERY_NAMESPACE, canonical_binding_bytes(values))`,
with values encoded in parameter-name order. Binding rejects missing, unknown,
or type-mismatched params (`INV-QUERY-3`).

Claims are a separate input channel. `Operand::Claim` is _not_
client-supplied binding data: recognized claims (`sub`, `team`, `isAdmin`) are
injected server-side from the subscriber's identity by policy composition
(ch. 7), and an unknown claim is a validation error. Recognition is not
evaluation support: ch. 7 defines which claims actually authorize a policy
decision. `sub` is supported; `team` and `isAdmin` fail closed.

## 6.4 Result sets and matched include paths

A result set is the authoritative row membership for a particular
`(ShapeId, BindingId)`. It is multi-table: each entry is a `ResultRowEntry =
(table, row_uuid, tx_id)` (`INV-QUERY-8`).

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

## 6.5 Query-driven sync

A subscription binds a shape to one binding and is addressed by
`SubscriptionKey { shape_id, binding_id }`. The wire vocabulary is
`RegisterShape`, `Subscribe`, `Unsubscribe`, and `ViewUpdate` (ch. 8).

The serving authority maintains the settled result set for each
`(ShapeId, BindingId)`: the `ResultRowEntry` set plus its matched include paths
and join witnesses (§6.4). In Rust this server-side state is named
`maintained_subscription_views`.
The subscriber receives and stores its own **settled subscription result set**:
the rows and matched include material it can answer settled reads from (§6.6).
The two sides share entry shape, but have different roles. A `ViewUpdate` with
`reset_result_set = true` resets the subscriber's settled result set.

Two correctness properties govern result-set maintenance. Incremental
result-set updates converge to the same row-result set as a reset `ViewUpdate`
over the same committed history (`INV-QUERY-15`). Reset `ViewUpdate`s retain
per-peer complete payload coverage (`INV-QUERY-7`). Payload dedup is per peer for
complete transaction payloads: an already-shipped complete payload is sent in
`peer_payload_inventory.complete_tx_payloads`, and a `VersionBundle` is emitted
at most once per update (`INV-QUERY-20`). Partial payloads, including exclusive
payloads, do not establish complete-transaction payload coverage unless the peer
has received all versions for the transaction. Exclusive `ViewUpdate` visibility
is view-atomic: a bundle may carry the exclusive versions needed for the
maintained subscription view, and result rows for that view are emitted only when
that view's exclusive payload is complete (`INV-QUERY-19`, ch. 3).

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

## 6.6 Reads, settled and local

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
`$createdBy`, `$updatedAt`, `$updatedBy`; permission introspection is a dry-run
API (ch. 7, ch. 13), **not** `$canRead`-style magic columns.

## Open questions

- 🔶 **Settled read without a subscription.** The design says a settled read with
  no subscription should be an error on a partial node, but `NodeState::query_rows`
  currently falls back to local/global evaluation when no settled subscription
  result set exists.
  Decide whether the rule is API-level only, partial-node-only, or an
  implementation change.
- 🔶 **Reverse relations / nested result shapes.** Current result-set entries are
  flat multi-table rows; nested result payloads and reverse relations are
  undecided — likely a ch. 13 surface question.
- 🔶 **Relay coarser covering shapes.** Upstream subscription collapse onto
  coarser covering shapes is a design direction, not a current MUST (ch. 8).
