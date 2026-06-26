# jazz — Specification · 14. Lowering to groove

jazz's first design principle (ch. 1) is that everything lowers to groove. This
chapter defines that boundary: how jazz schemas, persisted rows, current-row
maintenance, query shapes, sync result sets, and RLS policies are represented as
groove schemas, tables, `arg_max_by` graphs, and prepared shapes. It does **not**
re-own those semantics — each is defined in its own chapter; this chapter pins
only the boundary and the mapping.

For live peer subscriptions, ch. 16 sharpens this into the maintained
subscription view target: the protocol-facing serving path should be a groove
subscription over a maintained subscription terminal stream, not an independent
semantic scan.

## 14.1 The boundary

The lowering boundary keeps jazz's data model on a single storage and query
substrate. jazz lowers storage, current-row maintenance, and query/sync
evaluation onto groove, then adds distribution, history, and authorization
*above* that substrate; it defines no independent storage or query engine for
those concerns. A node opens its `groove::db::Database` from a lowered `groove`
schema and never bypasses it for queryable record storage, current-row
maintenance, or query/sync evaluation (`INV-LOWER-1`).

There is one deliberate exception: **large-value content bytes** do not lower to
groove's record/IVM machinery. Op-log *metadata* lowers normally (it rides
commit units as ordinary cells), but the content bytes live in the raw
`jazz_content` store below the table/IVM layer, reached through groove's raw
column-family handle (ch. 12). The boundary is precise: anything queryable lowers
to groove; anything only ever ranged-read lives in the content store.

## 14.2 Schema → groove

A jazz schema lowers to a complete groove schema
(`JazzSchema::lower_to_groove`, or `…_with_partitions` when partitions are in
scope). The lowered schema contains the fixed metadata tables, each
application table's layer tables, the global-current tables,
`jazz_global_changes`, and the raw KV store `jazz_content` (ch. 2,
`INV-DATA-20`).

Wire identities remain UUIDs. Lowered storage may intern those identities into
node-local `u64` aliases in `jazz_nodes`/`jazz_schema_versions`, but those
aliases must never appear on the wire (ch. 2, `INV-LOWER-3`).

*Further invariants.* `INV-LOWER-2`, `INV-LOWER-4` — content lowers to
`jazz_{table}_history` and deletion to `jazz_{table}_register`, each PK
`(row_uuid, tx_time, tx_node_id)`, never mixing user cells and `_deletion`.
`INV-LOWER-17` — `text`/`blob` lower their cell type to nullable groove `Bytes`.
`INV-LOWER-18` — `Counter` is rejected on nullable/non-integer/large-value
columns. `INV-LOWER-19` — lowered record-wrapper field indices match the groove
descriptors (debug-asserted).

## 14.3 Current rows → groove

Current-row visibility is the point where content and deletion history become
the row set seen by queries and sync. Visible current rows are computed in groove
as **content-current anti-joined with deletion-current** (ch. 4, `INV-LOWER-5`).
Non-global tiers use groove `arg_max_by` over `(tx_time, tx_node_id)` per
`row_uuid` on the history and register tables (`INV-LOWER-6`); the global tier
reads the global-current tables directly, excluding rows whose register winner
is `Deleted`, rather than scanning history (`INV-LOWER-7`). The
`jazz_global_changes` index (`by_global_seq`) backs global-base probes
(`INV-LOWER-8`, ch. 5).

## 14.4 Queries → groove

Query evaluation starts from the same visibility model as current-row reads:
lowering **begins from `visible_current_graph(table, tier)`**, so deletion
visibility is applied before user filters, joins, or reachable traversal
(`INV-LOWER-9`, ch. 6). Parameterized query shapes lower to groove prepared
shapes named `jazz-query:<shape_id>`, are cached by `(ShapeId, DurabilityTier)`,
and execute via `Database::bind_shape` with parameter types taken from the shape
(`INV-LOWER-10`, groove spec ch. 5).

There is one intended lowered-query core. That core takes an explicit **base
source** (for example visible current rows for a table/tier) and a query algebra
fragment (filters, joins, reachability, ordering/window operators that are in the
maintained surface). The base source is not hidden inside the algebra: current
rows, historical rows, partition/schema-projected rows, and future branch/lens
sources choose their source first, then reuse the same algebra lowering where
their source can be represented in groove.

Read policy composes before lowering. For non-system peers, the shape lowered by
the core is the user query intersected with the table read policy under the
server-derived peer claims; policy joins, reachability, and witness dependencies
are part of the lowered graph, not an after-the-fact output filter. Destination
policy checks MAY still evaluate directly under `INV-LOWER-20` until
prepared-shape policy lowering is complete, but the design target is the same
policy-composed core.

Identity and execution are separate concerns: aggregation and non-maintained
`order_by` are part of a shape's *semantic identity* (canonicalized into the
`ShapeId`, ch. 6), but their ordinary read execution is node-level
post-processing applied after row materialization, not pushed into groove
lowering. Maintained finite ordered windows are the exception: they lower to
groove `TopBy` so membership changes are maintained incrementally. ch. 14 owns
that execution-placement statement; ch. 6 owns the identity.

There is one maintained-subscription exception for windowing: an unordered
`limit(1)` with no explicit `order_by` and offset `0` lowers into groove as
`ArgMinBy` over the visible result rows, with an empty group and `row_uuid` as
the comparison key. This makes the chosen row deterministic without claiming an
application-visible order. Ordered maintained queries with a finite `limit`
lower into groove `TopBy`, preserving user order terms and appending `row_uuid`
as the stable tie field; `offset` is part of the retained window. Unordered
`limit > 1` and unordered nonzero `offset` remain unsupported until they either
gain explicit order semantics or a separate maintained lowering.

*Further invariants.* `INV-LOWER-13` — aggregation, ordinary read ordering,
general pagination, and projection are applied by the node *after* row
materialization (not required of groove), except maintained unordered `limit(1)`
offset `0` which lowers through `ArgMinBy` and maintained finite ordered windows
which lower through `TopBy`. For maintained subscriptions, ch. 16 tracks
aggregate/projection/predicate-policy lowering gaps separately from remaining
window capability limits. `INV-LOWER-12` — a read crossing
partitioned/schema-projected data bypasses the ordinary prepared current plan
cache; supported root current reads use projected current source rows, and
unsupported join/reachable projected shapes fail loudly until they have
source-aware lowering. Historical current reads with filters and joins lower
through the shared clause layer over a historical source; historical reachable
still requires source-aware reachable lowering. These staged source gaps must not
create a second query algebra. `INV-LOWER-11` — prepared lowering rejects `!=`
parameter predicates until supported.

## 14.5 Sync views & exclusive validation → groove

Sync view maintenance shares the same lowered query machinery as ordinary reads.
Peer state may cache a groove `Subscription` and prepared plan, recomputing a
view update from current query rows otherwise (`INV-LOWER-14`), and a whole-table
current-row view update matches the node's lowered `current_rows` result
(`INV-LOWER-15`). Result-set ids stay separate from version payloads via
per-peer dedup (ch. 8). Exclusive predicate validation compares the shape's
output `(RowUuid, TxId)` set at `base_snapshot.global_base` against now
(degenerate whole-table predicates use the global-currency-changed probe)
(`INV-LOWER-16`, ch. 3).

## Open questions

- 🔶 **Policy lowering** (`INV-LOWER-20`). RLS policies are designed to lower to
  groove prepared shapes; the implementation currently evaluates them directly
  in `node/policy.rs` (prepared-shape policy lowering arrives with the edge
  tier). Decide whether ch. 14 states the design invariant with an
  implementation exception or only the implemented behavior.
- 🔶 **Bytes primary keys.** The README lists bytes PKs as a "new" groove ask, but
  the implementation already uses `PrimaryKeyColumn::bytes` in several lowered
  tables — treat as satisfied rather than pending.
- 🔶 **Alias non-leakage coverage.** Alias→UUID remapping is done on decode, but
  no focused test proves aliases never leak on the wire for nested tx-id fields
  (`INV-LOWER-3` is `untested` until covered).
