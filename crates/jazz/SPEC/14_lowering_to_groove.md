# jazz — Specification · 14. Lowering to groove

## Overview

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

Invariant digest:

- `INV-DATA-20`: Jazz schema lowering MUST provide fixed system storage, while catalogue physical mappings MUST add the application lineage tables required at node open.
- `groove/SPEC/INVARIANTS.md::INV-INC-1`: Incremental delivery invariant (mechanism law). For any maintained view, the work performed to ingest, apply, and publish a change — including snapshot assembly, diffi...
- `INV-LOWER-1`: Jazz schemas MUST be lowered into a `groove::schema::DatabaseSchema` before opening the node's `groove::db::Database`.
- `INV-LOWER-2`: The physical content-history table for each `PhysicalTableId` MUST have composite primary key `(row_uuid, tx_time, tx_node_id)`.
- `INV-LOWER-3`: Node-local aliases in `jazz_nodes.id` and `jazz_schema_versions.id` MUST NOT be wire identities; wire tx/schema references MUST use `NodeUuid` and `SchemaVersionId`.
- `INV-LOWER-4`: Content versions MUST resolve through their schema's durable physical mapping while deletion-register versions resolve through the shared deletion-history relation by stable physical-table and canonical branch key; a single immutable version row MUST NOT contain both user cells and `_deletion`.
- `INV-LOWER-5`: Combined current rows MUST be maintained from independently selected content and deletion winners and expose an explicit visibility state.
- `INV-LOWER-6`: Local/non-global current-row maintenance MUST use bounded per-branch-local row currency selection for both content and deletion history; deletion access MUST be prefix-bounded by branch key and physical table id.
- `INV-LOWER-7`: Global current-row reads MUST use the physical lineage's combined global-current table, not scan immutable history or anti-join a register source.
- `INV-LOWER-8`: `jazz_global_changes` MUST be keyed by `(physical_table_id, row_uuid, layer, global_time)` and expose global-time and physical-table/global-time indexes.
- `INV-LOWER-9`: Query lowering MUST begin from a resolved visible-current source and therefore MUST apply deletion visibility before user filters/joins/reachable traversal.
- `INV-LOWER-10`: Parameterized query plans MUST be prepared as groove shapes with binding descriptor and stable name `jazz-query:<shape_id>`, then executed through `Database::bind_shape`; maintained subscription views with hidden routing provenance MUST prepare a clean output graph plus an internal routing graph through `Database::prepare_one_sink_with_routing`.
- `INV-LOWER-11`: Prepared graph lowering MUST preserve the semantics of every accepted predicate shape and explicitly reject unsupported predicate shapes.
- `INV-LOWER-12`: Schema projection MUST lower as a Groove source-boundary `VariantProject`. Parameter-bound joins over projected rows MUST preserve their source descriptor and payload, and plans prepared before lens publication MUST remain valid as projection cases are registered.
- `INV-LOWER-13`: Aggregation, ordinary read ordering, general pagination, and projection MUST be applied by the node after row materialization, not required from groove lowering, except maintained unordered `limit(1)` with offset `0` which MAY lower through groove `ArgMinBy` over `row_uuid`, and maintained ordered windows or ordered suffixes which MUST lower through groove `TopBy`.
- `INV-LOWER-14`: Sync query updates SHOULD consume maintained terminal facts for result membership, path/correlation coverage, payload/replacement/version witnesses, policy witnesses, and read-frontier settlement; query-row recompute paths are migration/oracle debt, not an alternate production engine.
- `INV-LOWER-15`: Whole-table current-row sync views MUST be represented as the normal table-rooted row-set shape, not a separate current-row serving engine; their result set must match the node's lowered `current_rows` result while migration code still exists.
- `INV-LOWER-16`: Exclusive predicate validation for non-degenerate shape predicates MUST compare predicate-output-set terminal facts for the shape+binding at `base_snapshot.global_base` to the corresponding current predicate-output-set facts.
- `INV-LOWER-18`: Counter merge strategy MUST NOT be accepted for nullable or non-integer columns.
- `INV-LOWER-19`: Lowered record wrapper field indexes MUST match the groove schema record descriptors used at node open.
- `INV-LOWER-20`: RLS policy declarations MUST be valid Jazz query shapes; read policy MUST lower through the query engine as part of the policy-composed read graph, while write-time acceptance MAY continue to evaluate policy predicates directly in `node/policy.rs` until write-policy prepared-shape lowering lands.
- `INV-LOWER-21`: One-shot reads, live subscriptions, sync views, and transaction-validation reads MUST consume the same lowered semantic query program; callback/reset/retry/propagation behavior MUST NOT select a second evaluator or become part of query shape identity. Runtime consumers request compiler evidence as app rows plus named terminal facts.
- `INV-LOWER-22`: Global current reads and read-policy authorization programs MUST use one normalized-program access-path derivation when sound: primary-key equality uses a primary-key scan, declared indexed-column equality uses an index probe, residual predicates remain applied, and unsupported shapes fall back to a loudly counted full scan.
- `INV-LOWER-23`: Position-bounded historical cuts and frozen branch-view base reads MUST use the
  `by_table_global_time` bounded range path when sound, returning the same rows as the
  full-scan currentness oracle while touching only the requested global-time range.
- `INV-LOWER-29`: The shared deletion-history relation MUST expose seekable `(physical_table_id, branch_key, row_uuid)` and table/branch-key-prefix access paths. No logical-table read, rebuild, or overlay operation may lower to an unbounded scan over unrelated table lineages or branch keys.
- `INV-LOWER-24`: Dry-run policy probes and recursion seed hydration MUST use the same deterministic source access-path selection as ordinary one-shot reads, with equivalence to the full-scan path and counters proving the selected path.
- `INV-LOWER-25`: A lens-projected maintained source MUST emit the same net weighted current-row and witness deltas as applying the selected natural lens path to the authoritative source.
- `INV-LOWER-26`: A structured query MUST expose one authoritative terminal output relation. Groove MUST assemble nested paths into that terminal; a child change semantically replaces or patches its owning root output, and public carriers MUST NOT require a second relation-edge delta stream.
- `INV-LOWER-27`: An enum case's authored discriminant is scoped to its row `SchemaVersionId`; lowering MUST translate it through the persistent case identity of its physical occurrence before using a local storage tag, predicate, grouping key, or ordering key.
- `INV-LOWER-28`: An additive enum case MUST be a row-level incompatibility for an older read schema, never a query or subscription error. For a current read, Global/Ahead candidates first choose their single canonical winner; the compatibility boundary then removes that unrepresentable winner before any semantic consumer (filter, ordering, grouping, aggregation, policy, relation requirement, pagination, or maintained delta) observes it. It MUST NOT fall back to an older compatible candidate. Unused enum occurrences remain undecoded and do not affect row visibility.

## Details

### 14.1 The boundary

The lowering boundary keeps jazz's data model on a single storage and query
substrate. jazz lowers storage, current-row maintenance, and query/sync
evaluation onto groove, then adds distribution, history, and authorization
_above_ that substrate; it defines no independent storage or query engine for
those concerns. A node opens its `groove::db::Database` from a lowered `groove`
schema and never bypasses it for queryable record storage, current-row
maintenance, or query/sync evaluation (`INV-LOWER-1`).

Large values preserve this ownership rule in the other direction: Groove owns
their indirect scalar format and every logical operation over it, while Jazz
supplies only the authorized opaque-locator chunk capability specified by
chapter 19. Jazz MUST NOT materialize descriptors after Groove has already
evaluated filters, policies, ordering, grouping, joins, indices, or aggregates.

### 14.2 Schema → groove

A jazz schema lowers its fixed system tables and direct record stores through
`JazzSchema::lower_to_groove`. During node open, durable
`jazz_schema_versions` mappings add one schema-versioned set of history,
register, current, ahead-current, and rejected-version tables per
`PhysicalTableId`. The full Groove schema is therefore the fixed lowering plus
the recovered physical lineages (ch. 2, `INV-DATA-20`).

Wire and catalogue semantic identities remain UUIDs. Lowered storage may intern
node, schema, physical table, physical column, and enum-variant identities into
node-local integer aliases, but those aliases must never appear on the wire,
enter content identity, or decide cross-node equality (ch. 2, `INV-LOWER-3`).

### 14.2.1 Concurrent enum cases

Jazz explicitly permits concurrent schema versions. Therefore every compact enum
discriminant in a wire row is an **authored ordinal**, qualified by the
row's `SchemaVersionId`; it is not a global enum tag. For every physical enum
occurrence, including a payload enum case and recursively inside its payload,
the semantic identity of a case is its authority-allocated identity:

```text
GlobalPhysicalEnumVariantId(UUID)
```

An inherited case retains the UUID allocated by its introducing publication.
For example, if a base schema declares `draft` and `published`, concurrent
children A and B may both declare authored ordinal `2`, respectively `archived`
and `snoozed`. They are distinct because the authority allocated distinct UUIDs. A later merge
schema maps both into its own authored ordinal space. The physical column
occurrence is part of every lookup of that identity; similarly named cases in
different columns never share a registry.

The catalogue supplies the immutable UUID manifest and lineage evidence.
Active catalogue subsets are dense prefixes: a receiver parks a later envelope
until every earlier `CatalogueSeq` has activated. Thus a node interns cases in
authoritative catalogue order, never arbitrary network receipt order, and that
order remains append-only as its prefix grows. A node may assign durable dense
local tags after resolving the global UUID, but it must persist the
mapping and translate at the wire/storage and storage/projection boundaries.
The compact row representation remains `SchemaVersionId + authored ordinal`,
because the row already carries the former; values which escape that row context
(for example standalone parameters or caches) must carry equivalent schema or
case context.

For ordering, a node uses the target schema's current representable case view.
Within that view an ancestrally earlier introduction MUST sort before a later
introduction. Concurrent siblings have no ancestral order, so a target schema's
authored view defines their presentation order. Neither that order nor the
authored ordinal is identity. It is optimized into a node-local registry tag only
after the permanent UUID has been resolved.
The durable binding also records introducing schema/position solely as ordering
provenance for the append-only physical tag vector. Changing that provenance
cannot create a distinct case: equality, collision checks, descriptor spelling,
and recursive payload paths use only the authority UUID.

Payload-enum projection preserves the selected payload record while remapping
its case tag. Same-name cases from independent schemas are distinct identities;
incompatible payload layouts reject rather than merge. The same translation
recurs at nested enum occurrences.

Projection from a physical case back into an authored schema is non-total. If a
query does not read, filter, join, order, group, authorize, or otherwise require
that occurrence, the row remains usable without decoding it. If a query
semantically requires a case absent from its target schema, the physical source
MUST deterministically omit that row before pagination, aggregation, or any
maintained-view delta processing. It MUST NOT surface an old-client query or
subscription error, substitute an old case, or invent a default. A policy
dependency is fail-closed and therefore also omits the row. An optional relation
or include may omit only its incompatible child while retaining its readable
parent; a required relation follows its explicit requirement semantics. Equality
against a known case consequently treats an absent newer case as non-matching.

_Further invariant._ `INV-LOWER-27` — local enum tags are only interned
representations of permanent global enum-variant UUIDs; simultaneous sibling ordinal
allocations cannot alias one another.

_Further invariant._ `INV-LOWER-29` — durable physical mappings use the
versioned canonical binary codec described in ch. 10. Local physical aliases are
opaque storage handles, while every table, column, scalar, payload, and nested
enum occurrence resolves a permanent authority-allocated UUID;
recovery rejects malformed, non-canonical, trailing, or unknown-reference
metadata rather than deriving identity from names, JSON map order, or arrival.

_Further invariant._ `INV-LOWER-28` — enum compatibility is evaluated at the
source boundary immediately after Global/Ahead currentness selection (or after
the equivalent historical/branch-key winner materialization), so one-shot and
maintained reads have the same row-membership semantics. A later incompatible
winner retracts rather than exposing a stale compatible predecessor, and no
downstream operator can turn it into a runtime failure.

_Further invariants._ `INV-LOWER-2`, `INV-LOWER-4` — content lowers per resolved
`PhysicalTableId` with PK `(row_uuid, tx_time, tx_node_id)`, while deletion
lowers to a universal sparse relation whose PK begins
`(physical_table_id, branch_key, row_uuid)`; immutable rows never mix user
cells and `_deletion`.
`INV-LOWER-18` — `Counter` is rejected on nullable/non-integer columns.
`INV-LOWER-19` — lowered record-wrapper field indices match the groove
descriptors (debug-asserted).

### 14.3 Current rows → groove

Current-row maintenance is the point where content and deletion history become
the row set seen by queries and sync. A combined current row holds independent
content/deletion winner references, deletion event, visibility, and projected
cells (`INV-LOWER-5`). Non-global tiers maintain it with bounded per-row winner
selection; deletion access is a prefix seek into the universal deletion history
(`INV-LOWER-6`, `INV-LOWER-29`). The global tier reads the physical lineage's
combined global-current table directly rather than scanning history or
anti-joining a register (`INV-LOWER-7`). The `jazz_global_changes` indexes keyed
by `PhysicalTableId` back global-base probes (`INV-LOWER-8`, ch. 5).

### 14.4 Queries → groove

Query evaluation starts from the same visibility model as current-row reads:
lowering begins from a resolved visible-current source, so deletion visibility
is applied before user filters, joins, or reachable traversal (`INV-LOWER-9`,
ch. 6). Parameterized query shapes normally lower to groove prepared
shapes named `jazz-query:<shape_id>`, are cached by
`(ShapeId, DurabilityTier, binding-param signature)`, and execute via
`Database::bind_shape` with parameter types taken from the shape
(`INV-LOWER-10`, groove spec ch. 5). The binding-param signature is part of the
cache key because the same semantic shape can be prepared with different
claim- or caller-supplied binding columns after policy augmentation.

There is one intended lowered-query core. That core takes an explicit **base
source expression graph** (for example visible current rows for a table/tier,
historic cuts, snapshot-qualified branch sources, explicit prefixes,
head/base overlays, schema/lens projections, or contribution merges) and a query algebra fragment
(filters, joins, reachability,
ordering/window operators that are in the maintained surface). The base source
is not hidden inside the algebra: current rows, historical rows,
schema-projected rows, branch-view reads, transaction overlays, and
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
sets.
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

Seeded reachability lowers the seed set as an ordinary relation input to the
closure node. The prepared fragment identity includes the seed table, seed
columns, descriptor, and claim paths, but not the subscribing shape id. This is
the same sharing doctrine as prepared binding sources: two resource kinds using
the same membership closure and claim paths share one maintained fragment while
their outputs still route per subscriber binding.

`inherits(parent_col)` lowering splices the parent's composed policy fragment
into the child policy with correlation rebound to the joined parent row. The
child fragment identity unions the parent's claim paths into its own sharing
identity.

The TypeScript `policy.gather({ start, step, maxDepth })` / `hopTo` surface
lowers to the seeded closure path only for exactly matching patterns: a
claim-keyed start lookup, compatible hop direction, and no extra step filters
whose semantics are not represented by seeded reachability. Other gather shapes
stay on the legacy lowering path and must fail closed if they cannot be
represented safely.

Relation facade unification is staged. The alpha-compatible public
`hopTo`/`gather` query surface arrives at the core as relation IR
(`TableScan`, `Filter`, `Join`, `Project`, `Union`, `Gather`, `Distinct`,
`OrderBy`, `Offset`, `Limit`) and must normalize into the same row-set program
vocabulary used by ordinary queries. The contained v1 slice accepts runtime
value-envelope literals such as `{ type: "Uuid", value: ... }` in relation
predicates and maps scalar acyclic hop paths whose projected output is the path
terminal onto existing `JoinVia`/nested-join shapes. That preserves the lowered
plan shape for already-supported single-hop relation queries while covering the
browser scalar `users -> teams -> orgs` relation shape.

The remaining relation IR operators require first-class row-set lowering rather
than more facade rewrites:

- `Union` should lower to a row-set `Union` with explicit source alternatives
  and a result identity that either preserves branch-source discriminators or
  proves all alternatives are the same logical real-row domain before
  deduplication.
- `Distinct` should lower after the relation input that creates duplicates,
  with stable dedupe keys carried into replacement facts so maintained views can
  retract exactly the affected membership row.
- `Gather` should lower to a recursive relation node whose seed and step are
  ordinary row-set subplans, with frontier keys, dedupe keys, max-depth, depth
  output, and path facts all represented explicitly. It must not be encoded as a
  root-table `reachable` filter when the output row set changes from the seed.
- Array-valued foreign-key hops need membership join semantics
  (`array_contains(left_key, right_row_id)` / `ContainsField`-like lowering) or
  an equivalent path edge operator. Rewriting them as scalar equality joins is
  unsound and would miss multi-valued membership changes.

Maintained subscriptions for those operators must preserve
`groove/SPEC/INVARIANTS.md::INV-INC-1`: relation
membership changes must be scale-independent in unrelated rows. In practice that
means union alternatives, distinct groups, recursive frontier rows, and
array-membership path edges all need terminal facts with enough identity to
route updates to the exact subscribed binding/result member. A staged plan is:
first normalize relation IR into row-set nodes without changing execution;
second lower union/distinct as maintained groove fragments with explicit
identity/retraction facts; third lower recursive gather using the seeded
frontier machinery and depth/dedupe facts; fourth add array-membership join
facts and extend the incremental-delivery canaries to cover scalar-hop,
array-hop, union/dedup, and recursive-gather single-row updates.

Read and write policies both lower through the `node/query_engine` path described
above. Write-time admission enters
`NodeState::write_policy_allows_version_record`, projects old and candidate data
into the policy-pinned schema, selects the matching insert/update/delete clause,
and supplies that row as an inline root source to the identity-aware
authorization subplan. Partition-relative writes use the same program over the
effective head/base view. Plain child-insert `inherits(parent_col)` selects the parent's
`update_using` clause; explicit `InheritsOperation::{Insert, Update, Delete}`
selects the matching parent write clause. There is no direct predicate
interpreter fallback (`INV-LOWER-20`).

Old-row and candidate inline sources MUST carry the same authoritative
`RowProvenance` metadata as stored current rows. `UPDATE USING` and
`DELETE USING` evaluate the retained old-row creator and updater metadata;
`UPDATE WITH CHECK` preserves the retained creator and uses the incoming
version's updater; inserts use the incoming version's full provenance. If
required metadata cannot be represented, source resolution fails closed rather
than rebinding provenance from the authenticated identity. This evidence
remains hidden source metadata and does not change application row fields or
read visibility.

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
lowering represents the ordered suffix with `TopByLimit::Unbounded`, matching
ch. 6's promise that maintained ordered subscriptions can omit a finite limit
while still preserving ordered membership. Unordered `limit > 1` and unordered
nonzero `offset` remain unsupported until they either gain explicit order
semantics or a separate maintained lowering.

_Further invariants._ `INV-LOWER-13` — aggregation, ordinary read ordering,
general pagination, and projection are applied by the node _after_ row
materialization (not required of groove), except maintained unordered `limit(1)`
offset `0` which lowers through `ArgMinBy` and maintained ordered windows or
ordered suffixes which lower through `TopBy`. For maintained subscriptions, ch.
16 tracks
aggregate/projection/predicate-policy lowering gaps separately from remaining
window capability limits. `INV-LOWER-12` — schema projection is a Groove
`VariantProject` source-boundary operation. Heterogeneous physical lineages use
the ordinary prepared path. When a parameter join unwraps a nullable source
field, its projection restores that wrapper so the prepared terminal keeps the
source descriptor and payload. Since projection cases are registered into the
live node, a plan prepared before lens publication remains valid for rows of the
new schema variant. Historical current reads with filters and joins lower through
the shared clause layer over a historical source; historical reachable still
requires source-aware reachable lowering. These staged source gaps must not
create a second query algebra. `INV-LOWER-11` — prepared lowering rejects `!=`
parameter predicates until supported.

### 14.5 Sync views & exclusive validation → groove

#### Structured app-output terminal

Nested query output is a terminal responsibility of the lowered Groove graph,
not a reconstruction responsibility of a Jazz, NAPI/WASM, or TypeScript
adapter (`INV-LOWER-26`). `CollectBy`/`CollectByTree` consumes the flat
authorized path facts and emits one fixed-descriptor output relation. Each
output occurrence is keyed by its public `ResultKey`: a single-source root uses
its `ObjectId`, while a flat join uses the ordered source tuple. Empty optional
collections are encoded in the root record, so a root with no children remains
an ordinary terminal row.

A nested child insertion, update, removal, authorization transition, or order
change therefore changes the owning terminal root. The canonical semantic
delta is a root addition, retraction, or replacement. A carrier may encode a
replacement as a structural nested patch when that is measurably useful, but
the patch is subordinate to the root occurrence and must reduce to the same
terminal relation. It must not create a separately authoritative
`relation_delta`, row/edge snapshot, or high-level assembler state.

During migration, relation facts remain permitted as internal sync coverage and
authorization evidence. They are not public query output and consumers must
not combine them with a root-row delta to reconstruct application values. A
protocol revision that carries terminal rows advertises that output mode
explicitly; receivers must not infer it from query syntax or descriptor shape.

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
must preserve enough identity to distinguish content, deletion, branch key,
historic/snapshot, schema-projected, and batch-scoped membership. Synthetic
aggregate/window rows emit member identity plus a `ResultPayload` fact carrying
the custom encoded record bytes. Relation/path lowering emits non-lossy path
facts rather than hiding edge kind, versions, depth, branch alternative, order,
role, or hole state in opaque revisions.

### 14.6 Access-path selection

The normalized-program planner selects access paths by deterministic rule,
never by cost model or statistics. Ordinary reads and read-policy
authorization programs pass through this same planner; source resolution may
reuse a policy program's recorded path only to narrow the protected source
before joining the already-compiled policy proof. It never reinterprets policy
syntax, and the proof graph remains the sole authorization decision.

1. equality on a primary-key prefix → point/prefix scan spec;
2. equality on a declared/derived boundary-arrangement key → arrangement probe;
3. global-time-bounded reads (historical cuts, frozen branch-view bases, reconnect
   enumeration) → range scan spec over the `by_table_global_time` arrangement;
4. otherwise → full scan, loudly counted (full-scan counters are part of the
   operational surface, ch. 17).

v1 consumers are implemented and tested: one-shot filtered reads and simple
global read-policy programs;
position-bounded historical and frozen-base reads, which take the
`by_table_global_time` bounded range path and must agree row-for-row with the
full-scan currentness oracle while touching only the requested global-time
range (`INV-LOWER-23`; this is what makes snapshot-qualified bases and historical reads
bounded rather than gated); dry-run policy
probes; and recursion seed hydration (`INV-LOWER-22`–`INV-LOWER-24`). The
source resolver records a loud full-scan counter when a requested source cannot
be represented by a sound static path. Alternative branches, predicates without
an eligible equality, joins, missing/nullable claims, and non-Global reads use
that fallback, not a different semantic evaluator. Prepared-shape steady-state
probing is the later
overlay-probe phase (groove ch. 4 §4.6).

### 14.7 Existence lowering for inherited-parent policy joins

Inherited-parent authorization (`inherits(parent_column)`) is existence
semantics: a child row is visible iff at least one qualifying derivation
(access edge × membership path) exists for the parent, per identity. The
normalizer marks this join `JoinMode::Semi`; the lowering implements it as a
derivation-collapse, not a groove semi-join:

1. project the parent-policy subtree down to exactly the join keys plus the
   route fields it carries (claim route fields included);
2. `arg_max_by` grouped on all of those fields — rows within a group are
   identical post-projection, so losing one of several derivations emits no
   output delta and losing the last retracts the group;
3. plain inner join against the reduced side; downstream field/route
   bookkeeping is unchanged (`last_join_right` carries the reduced field set).

Two constraints force this shape over a plain `SemiJoin` node:

- **Multisink identity routing.** One shared program serves every bound
  identity of a prepared shape; result rows are routed to subscribers by their
  claim route-field values. The claim must therefore survive to the join
  output as a runtime-bound field — never baked as a literal (prepared graphs
  are shared across identities), and never erased (a groove `SemiJoin` emits
  left rows only, which would destroy per-identity attribution).
- **Maintained deltas across permission changes.** The parent set must stay
  dynamic; freezing visible parents at open time would drop deltas for
  children under later-granted or later-revoked parents.

Without this, hydration computes each child once per derivation and
consolidates the excess away (observed: ~2.4M intermediate rows for 24k
visible children; warm reopen 4.3s → 0.4s with the collapse in place).

### 14.8 Subsumed lowering backlog

The former public-schema-subset, SQL dialect, explicit-index, and top-k notes are
folded into this chapter as lowering work. Public schema features may exist
before core execution supports them, but any executable schema must pass one
shared validator before it reaches storage or runtime open. Query, policy, and
SQL entry surfaces lower to groove through the same shape identity.

Ordered-window and top-k lowering should prefer an ordered index path when the
requested order and filters can be satisfied by storage. The fallback remains
correct materialize/sort/limit behavior, but the optimized path must preserve
policy filtering, pagination, and live subscription maintenance.

### Established policy-lowering boundary

`INV-LOWER-20` is settled: both read policy and write admission lower through
`node/query_engine`. Write admission supplies policy-pinned old and candidate
rows as inline roots, then evaluates them with the authenticated identity over
current or branch-view sources. The former direct policy interpreter is not an
alternative execution path and has been removed.

## Open Questions

- 🔶 [#1777](https://github.com/garden-co/jazz/issues/1777) — Core-owned query output and authorization-source lowering.
- 🔶 [#1776](https://github.com/garden-co/jazz/issues/1776) — Indices, ordered top-k lowering, and planner diagnostics.
- 🔶 [#1779](https://github.com/garden-co/jazz/issues/1779) — Executable schema subset and lens-projected sources.
