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
_above_ that substrate; it defines no independent storage or query engine for
those concerns. A node opens its `groove::db::Database` from a lowered `groove`
schema and never bypasses it for queryable record storage, current-row
maintenance, or query/sync evaluation (`INV-LOWER-1`).

There is one deliberate exception: **large-value content bytes** do not lower to
groove's record/IVM machinery. Op-log _metadata_ lowers normally (it rides
commit units as ordinary cells), but the content bytes live in the raw
`jazz_content` store below the table/IVM layer, reached through groove's raw
column-family handle (ch. 12). The boundary is precise: anything queryable lowers
to groove; anything only ever ranged-read lives in the content store. Query and
sync row results carry large-value handles, not bodies. Value-returning APIs
materialize those handles by pulling authorized content extents and folding
op-log extents at the access boundary; encoded ops and content handles do not
escape as application cell bytes.

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

_Further invariants._ `INV-LOWER-2`, `INV-LOWER-4` — content lowers to
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
shapes named `jazz-query:<shape_id>`, are cached by
`(ShapeId, DurabilityTier, binding-param signature)`, and execute via
`Database::bind_shape` with parameter types taken from the shape
(`INV-LOWER-10`, groove spec ch. 5). The binding-param signature is part of the
cache key because the same semantic shape can be prepared with different
claim- or caller-supplied binding columns after policy augmentation.

There is one intended lowered-query core. That core takes an explicit **base
source expression graph** (for example visible current rows for a table/tier,
historic cuts, snapshot refs, explicit data branches/prefixes, overlays,
schema/lens projections, or branch merges) and a query algebra fragment
(filters, joins, reachability,
ordering/window operators that are in the maintained surface). The base source
is not hidden inside the algebra: current rows, historical rows,
partition/schema-projected rows, branch reads, transaction overlays, and
snapshot refs compose as source expressions, then reuse the same algebra
lowering where their source can be represented in groove.

The lowering request has three orthogonal parts:

- the semantic row-set body, including candidate/proposed-row sources for
  dry-run policy probes;
- the read view and policy context used to resolve sources and authorization;
- the requested app-row output profile plus internal fact outputs.

Runtime lifecycle is outside that semantic request. A one-shot read,
application live subscription, protocol sync view, or transaction-validation
read may choose different callback, reset, retry, propagation, and waiting
behavior, but the compiler-facing way to ask for evidence is only app rows plus
named terminal facts such as result membership, relation edges, read-frontier
settlement, payload witnesses, policy decisions/witnesses, predicate output
sets, and large-value extents.
Those runtime choices MUST consume the same lowered program. They must not
select a second evaluator or make coverage state part of the query shape
identity (`INV-LOWER-21`).

Read policy composes before lowering. For non-system peers, the shape lowered by
the core is the user query intersected with the table read policy under the
server-derived peer claims; policy joins, reachability, and witness dependencies
are part of the lowered graph, not an after-the-fact output filter. The prepared
program's policy sharing key records policy identity plus the claim paths read by
that lowered graph, not claim values. Claim values are runtime binding
parameters, while claim-path sets can vary by policy identity because different
identities can select different policy branches, missing-policy modes,
attribution contexts, or authorization subplans before lowering. This is why the
prepared-plan cache key includes the binding-param signature as well as the
shape and durability tier.

The current implementation split is explicit. Read policy now lowers through the
`node/query_engine` path described above. Write-time acceptance still evaluates
policy predicates directly in `node/policy.rs`: the ingest/dry-run path enters
`NodeState::write_policy_allows_version_record`, which dispatches insert,
update, and delete checks through `policy_allows*` helpers before accepting a
version. Moving read policy into the query engine therefore did not silently
change write acceptance semantics; `INV-LOWER-20` names that remaining direct
write-policy boundary.

Identity and execution are separate concerns: aggregation and non-maintained
`order_by` are part of a shape's _semantic identity_ (canonicalized into the
`ShapeId`, ch. 6), but their ordinary read execution is node-level
post-processing applied after row materialization, not pushed into groove
lowering. Maintained ordered windows are the exception: finite windows and
unbounded ordered suffixes lower to groove `TopBy` so membership changes are
maintained incrementally. ch. 14 owns that execution-placement statement; ch. 6
owns the identity.

There is one maintained-subscription exception for windowing: an unordered
`limit(1)` with no explicit `order_by` and offset `0` lowers into groove as
`ArgMinBy` over the visible result rows, with an empty group and `row_uuid` as
the comparison key. This makes the chosen row deterministic without claiming an
application-visible order. Ordered maintained queries lower into groove `TopBy`,
preserving user order terms and appending `row_uuid` as the stable tie field;
`offset` is part of the retained window. When the jazz query omits `limit`,
lowering represents the unbounded ordered suffix with `usize::MAX`, matching ch.
6's promise that maintained ordered subscriptions can omit a finite limit while
still preserving ordered membership. Unordered `limit > 1` and unordered nonzero
`offset` remain unsupported until they either gain explicit order semantics or a
separate maintained lowering.

_Further invariants._ `INV-LOWER-13` — aggregation, ordinary read ordering,
general pagination, and projection are applied by the node _after_ row
materialization (not required of groove), except maintained unordered `limit(1)`
offset `0` which lowers through `ArgMinBy` and maintained ordered windows or
ordered suffixes which lower through `TopBy`. For maintained subscriptions, ch.
16 tracks
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
The target peer-serving path consumes maintained terminal facts for result
membership, path/correlation coverage, payload/replacement/version witnesses,
policy witnesses, and read-frontier settlement, then materializes `ViewUpdate`s
from those facts plus peer inventory/runtime acknowledgements. Recomputing a
view update from current query rows is migration/oracle debt governed by ch. 16,
not an alternate production engine (`INV-LOWER-14`). Whole-table current-row
views are the normal table-rooted row-set shape, not a separate current-row
serving engine (`INV-LOWER-15`). Result-set ids stay separate from version
payloads via per-peer dedup (ch. 8). Exclusive predicate validation compares
predicate-output-set terminal facts for the shape+binding at
`base_snapshot.global_base` against now (degenerate whole-table predicates use
the global-currency-changed probe) (`INV-LOWER-16`, ch. 3).

Result membership facts are typed at the lowering boundary. Real-row membership
must preserve enough identity to distinguish content, deletion, branch,
historic/snapshot, schema-projected, and batch-scoped membership. Synthetic
aggregate/window rows emit member identity plus a `ResultPayload` fact carrying
the custom encoded record bytes. Relation/path lowering emits non-lossy path
facts rather than hiding edge kind, versions, depth, branch alternative, order,
role, or hole state in opaque revisions.

## 14.6 Access-path selection

The source resolver selects access paths by deterministic rule, never by cost
model or statistics:

1. equality on a primary-key prefix → point/prefix scan spec;
2. equality on a declared/derived boundary-arrangement key → arrangement probe;
3. global-sequence-bounded reads (historical cuts, branch bases, reconnect
   enumeration) → range scan spec over the `by_table_global_seq` arrangement;
4. otherwise → full scan, loudly counted (full-scan counters are part of the
   operational surface, ch. 17).

v1 consumers are implemented and tested: one-shot filtered reads;
position-bounded historical and branch-cut reads (this is what makes branch
`at()` and historical reachable bounded rather than gated); dry-run policy
probes; and recursion seed hydration (`INV-LOWER-22`–`INV-LOWER-24`). The
source resolver still fails loudly when a requested source cannot be represented
by a sound static path; the fallback is a counted full scan, not a different
semantic evaluator. Prepared-shape steady-state probing is the later
overlay-probe phase (groove ch. 4 §4.6).

## Open questions

- ✅ **Policy lowering** (`INV-LOWER-20`). Read policy now lowers through
  `node/query_engine` as part of the policy-composed query graph. Write-time
  acceptance still evaluates directly in `node/policy.rs` via
  `NodeState::write_policy_allows_version_record` and its `policy_allows*`
  helpers, so the spec states the implemented split rather than leaving the
  former prepared-shape policy question open.
- 🔶 **Bytes primary keys.** The README lists bytes PKs as a "new" groove ask, but
  the implementation already uses `PrimaryKeyColumn::bytes` in several lowered
  tables — treat as satisfied rather than pending.
- 🔶 **Alias non-leakage coverage.** Alias→UUID remapping is done on decode, but
  no focused test proves aliases never leak on the wire for nested tx-id fields
  (`INV-LOWER-3` is `untested` until covered).
- 🔶 **Historical implicit-include source coverage.** Historical root reads with
  filters and ordinary joins lower through `HistoryCut` sources, but shapes whose
  normalizer adds an implicit root-reference auxiliary source (for example an
  include used only to filter child rows by a parent table) do not yet add an
  aligned historical source expression for that auxiliary source. Until
  source-aware include coverage is wired into the historical read-set builder,
  these benchmark phases must report a visible
  `[needs: historical-implicit-include-source-coverage]` gate rather than being
  silently counted.
