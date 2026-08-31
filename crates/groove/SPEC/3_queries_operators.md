# groove — Specification · 3. Queries & operators

## Overview

Queries in groove describe incremental views. They implement a subset of SQL
semantics, but the durable contract is not an execution plan in the traditional
database sense. A query becomes a graph of _operators_ that defines how weighted
row changes move through the view. This chapter specifies the _what_ of
evaluation; chapter 4 specifies the _how_ (the tick, arrangements, propagation).

Invariant digest:

- `INV-QUERY-1`: A query graph node MUST be identified by the full `NodeDescriptor` consisting of `operator`, ordered `inputs`, and `output`; two incompatible descriptors MUST NOT share a node silently.
- `INV-QUERY-1A`: A Groove node descriptor MUST fully encode every input that can affect node output, including authorization-relevant literals such as identity, claims, policy bindings, and read-view source selection. This is the precondition for sharing one live node across multiple retention scopes: retainer tags do not participate in node identity, and sharing is valid only for descriptor-identical graphs with descriptor-identical canonical input refs.
- `INV-QUERY-2`: A `NodeDescriptor` MUST validate operator input arity, input/output descriptor compatibility, join key arity, and field-index bounds before the runtime accepts the node.
- `INV-QUERY-3`: `FilterOp` MUST emit exactly the input deltas whose records satisfy its `PredicateExpr`, preserving record bytes and weights, for the supported predicate surface including `And`/`Or`, literal comparisons, field-to-field equality/inequality, and `Contains`/`ContainsField`.
- `INV-QUERY-4`: SQL predicate lowering MUST reject unsupported or ill-typed predicate expressions instead of lowering them approximately.
- `INV-QUERY-5`: `MapProjectOp` MUST emit one output delta per input delta, copying only configured fields into the output descriptor and preserving the input weight.
- `INV-QUERY-6`: `UnwrapNullableOp` MUST drop `Nullable(None)` input deltas, unwrap `Nullable(Some(_))` to the inner value, and preserve the original delta weight.
- `INV-QUERY-7`: `Union` MUST require all non-empty inputs to have the same output descriptor and MUST preserve duplicate derivations as separate weighted deltas (`UNION ALL` semantics).
- `INV-QUERY-8`: An inner `JoinOp` MUST require equal-length left and right key vectors.
- `INV-QUERY-9`: An inner JoinOp MUST emit joined records with weight leftweight \* rightweight for matching keys, including matches produced by changes arriving on either side.
- `INV-QUERY-10`: An inner `JoinOp` MUST NOT double-count pairs where both matching sides changed in the same logical tick.
- `INV-QUERY-11`: Shared join arrangements MUST apply a given logical-time delta at most once per arrangement key/scope, even when multiple joins consume the arrangement.
- `INV-QUERY-12`: `AntiJoin` MUST output left rows only when the total right-side multiplicity for the join key is zero.
- `INV-QUERY-13`: `AntiJoin` MUST retract or restore visible left rows only when the right-side count crosses zero; changes that keep the right count nonzero MUST NOT emit anti-join deltas.
- `INV-QUERY-14`: Same-tick anti-join updates MUST suppress a left row that arrives with a matching right row and MUST emit a left row exactly once when it arrives in the same tick as the last blocker retracts.
- `INV-QUERY-15`: SQL `plan_query` MUST reject query parameters; parameterized SQL MUST go through `plan_prepared_shape`/prepared binding flow.
- `INV-QUERY-16`: SQL prepared-shape lowering MUST accept only equality predicates of the form `column = $parameter` or `$parameter = column` as binding predicates.
- `INV-QUERY-17`: SQL lowering MUST reject unsupported SELECT/set/join shapes explicitly, including `SELECT DISTINCT`, grouped/ordered/limited selects, non-inner joins, and non-`UNION ALL` set operations.
- `INV-QUERY-18`: SQL inner joins MUST lower only equality column predicates, with `AND` forming multi-column join keys.
- `INV-QUERY-19`: `BindingSourceOp` MUST NOT be evaluated through ordinary subscription/query graphs outside prepared shapes.
- `INV-QUERY-20`: `ArgMaxByOp` and `ArgMinByOp` MUST accept arbitrary upstream
  graph inputs. Base-table inputs MUST have primary-key columns exactly
  `group_cols + order_cols`; every plan shape MUST compare only that declared key
  under the operator direction, then encoded full-record bytes ascending as the
  deterministic final tie-breaker. Full-record bytes MUST independently key
  multiplicity so distinct records tied on the comparison key remain
  independently retractable; arbitrary payload field order MUST NOT be appended
  to the declared comparison key.
- `INV-QUERY-21`: `ArgMaxByOp` and `ArgMinByOp` MUST emit only winner changes for touched groups, suppressing non-winner changes and net-zero group deltas.
- `INV-QUERY-22`: A query operator MUST NOT be advertised as executable unless
  the runtime can execute that operator for the advertised scope; executable
  support may be narrower than the reserved descriptor vocabulary.

- `INV-QUERY-23`: TopBy MUST order each partition's positive-multiplicity records by order_cols with declared directions, then tie_cols ascending, then encoded full-record bytes ascending; the total order MUST NOT depend on arrival or iteration order.
- `INV-QUERY-24`: `TopByOp` MUST apply bag semantics to window occupancy: a record with positive multiplicity `m` occupies `m` consecutive ordinals of the partition's ordered stream, the retained window is the ordinal range `[offset, offset + limit)` (all ordinals `>= offset` when unbounded), and records with non-positive multiplicity are absent.
- `INV-QUERY-25`: A record straddling a window boundary MUST contribute exactly its in-window copies, as one output record whose weight is the in-window copy count.
- `INV-QUERY-26`: Per touched partition TopBy MUST emit the minimal consolidated weighted diff of retained windows; unchanged in-window copy counts MUST NOT emit, including rank-only moves, unless rank metadata is declared.
- `INV-QUERY-27`: `CollectBy` MUST be an output-terminal-only operator: validation MUST reject it as an input to every graph node, including another collector.
- `INV-QUERY-28`: For each touched `CollectBy` output occurrence, the terminal MUST suppress byte-equal output. A surviving changed collect group emits exactly one old-record retraction and one new-record addition; an appearing/disappearing group emits its one addition/retraction. Expand mode emits the corresponding minimal per-occurrence additions, removals, and replacements. Its descriptor and scalar key/order inputs MUST make this deterministic.

## Details

### 3.1 Queries become graphs that weighted deltas flow through

The query graph is the canonical form of a view. It is a DAG whose nodes are
operators and whose edges carry weighted `RecordDelta`s; each input and output is
typed by a `RecordDescriptor` (ch. 2). The SQL-ish `Query` surface (§3.9) is one
way to produce such a graph, but the graph shape is the real contract
(`IvmGraph` in the reference implementation). The graph is acyclic: recursion is
represented as a single `Recursive` node containing seed and step child graphs,
not as a cycle in the DAG (ch. 6).

Node identity is content-based so overlapping subscriptions can share work
(ch. 4). A runtime node is identified by its full `NodeDescriptor`: the
`operator`, ordered `inputs`, and `output` descriptor. Identical descriptors
share one `NodeId` by hashing descriptor content; a hash collision between
incompatible descriptors fails rather than silently sharing (`INV-QUERY-1`).
Before the runtime accepts a node, it validates the `NodeDescriptor` for operator
input arity, input/output descriptor compatibility, join-key arity, and
field-index bounds (`INV-QUERY-2`).

### 3.2 Source operators

Source operators introduce rows from outside a graph or from a boundary between
query mechanisms. Each source has a distinct origin and participates in the same
weighted-delta flow as every downstream operator.

- **Table source** (`GraphBuilder::Table`) — introduces a table's committed rows
  as deltas into the graph (ch. 2, ch. 4).
- **Index** (`GraphBuilder::Index`, `IndexByOp`) — either exposes encoded index
  entries or, with a named row projection, introduces the referenced table
  rows selected by that index. The latter is still one source node, not a
  caller-materialized inline snapshot; its durable persistence is ch. 2, its
  tick participation ch. 4, and staged hydration is ch. 8.
- **Binding source** (`BindingSourceOp`) — provides the parameter-as-data
  weighted record set of a prepared shape; defined in ch. 5.
- **Frontier source** (`FrontierSourceOp`) — provides the recursion entry point;
  ch. 6.

_Further invariants._ `INV-QUERY-19` — a `BindingSourceOp` appears only inside a
prepared shape (ch. 5); a plain, non-prepared query — a parameterless subscription
or one-shot read, still the common case — never evaluates one.

_Implemented v1 amendment (unified arrangement model, ch. 4 §4.6)._ A source operator
MAY hydrate from a **static scan spec** (point / prefix / range over an
arrangement key) supplied at graph construction, instead of a full scan. The
scan spec participates in `NodeDescriptor` identity. Scan specs are static
(values known at graph build — one-shots, hydration); parameterized
steady-state probes remain binding joins (their storage-backed probe design is
the ch. 4 overlay-probe direction: binding side resident, deletions ride
deltas, binding-delta probes read the durable boundary arrangement through the
staged-write overlay so probes see post-tick state).

### 3.3 Stateless operators

Stateless operators transform or route deltas without keeping persistent
operator state. They preserve weights while changing which rows pass through, how
records are shaped, or how compatible streams are combined.

**Filter** emits exactly the input deltas whose records satisfy its
`PredicateExpr`, preserving bytes and weights (`INV-QUERY-3`). The predicate
surface is `Eq`/`Neq`/`Gt`/`GtEq`/`Lt`/`LtEq`/`IsNull`/`IsNotNull` combined with
`And`/`Or`. Graph-level filters also support field-to-field equality and
inequality (`EqField`/`NeqField`) plus array membership predicates
(`Contains`/`ContainsField`). This names the runtime-supported predicate surface;
SQL lowering remains narrower and must reject unsupported or ill-typed predicate
forms rather than approximate them (`INV-QUERY-4`).

**MapProject** emits one output delta for each input delta by copying the
configured fields into the output descriptor. **UnwrapNullable** drops
`Nullable(None)` deltas and unwraps `Nullable(Some(v))` to `v`.

**Union** combines compatible inputs with bag (`UNION ALL`) semantics: duplicate
derivations remain separate weighted deltas (`INV-QUERY-7`). Every input that
carries rows must have the **same record shape**, and that shared shape is the
union's output descriptor. Only inputs that produce identical record types can be
combined with `UNION ALL`. An input that is empty for a tick, such as a frontier
source with no bound deltas (ch. 6), contributes no rows and is exempt from the
shape match.

_Further invariants._ `INV-QUERY-5` — `MapProject` copies only configured fields
and preserves the input weight. `INV-QUERY-6` — `UnwrapNullable` preserves the
original delta weight.

### 3.4 Joins

Joins combine or suppress rows by key. groove executes the **inner equi-join**
and the **anti-join**.

An inner equi-join (`JoinOp`) emits records whose fields are ordered as _left
fields followed by right fields_. The left and right key vectors must have equal
length, and matching keys follow the product rule: the emitted record weight is
`left_weight × right_weight`. A change arriving on either side is matched against
the maintained contents of the opposite side (`INV-QUERY-9`). When both sides
change in the same logical tick, the join must not double-count the left-delta ×
right-delta cross term (`INV-QUERY-10`). This is the subtlety that makes
incremental joins correct, and chapter 4 covers how shared arrangements enforce
it.

To see the double-count concretely, take key `k` with existing left row `L1`
(weight +1) and existing right row `R1` (+1); the pre-tick join holds `L1·R1`.
In one tick we insert `L2` (left Δ +1) and `R2` (right Δ +1) under `k`. The
correct output delta is the three new pairs `L1·R2`, `L2·R1`, `L2·R2`, each +1.
Applying each side's delta against the _maintained opposite side after this
tick_ gives left Δ × right-after = `L2·R1`, `L2·R2` and right Δ × left-after =
`L1·R2`, `L2·R2` — so `L2·R2` (the left-Δ × right-Δ cross term) lands twice. The
join must subtract exactly one copy of that cross term to recover the correct +1.

An anti-join (`AntiJoin`) preserves the left descriptor. It shows a left row iff
the total right-side multiplicity for that row's key is zero (`INV-QUERY-12`),
and it emits a change only when a left row changes or the right count crosses
zero.

_Further invariants._ `INV-QUERY-8` — an inner join requires equal-length
left/right key vectors. `INV-QUERY-11` — shared join arrangements apply a given
logical-time delta at most once per arrangement key/scope (ch. 4).
`INV-QUERY-13` — anti-join changes only when the right count crosses zero.
`INV-QUERY-14` — same-tick arrivals suppress/emit a left row exactly once.

### 3.5 `ArgMaxBy` / `ArgMinBy` (maintained per-group winners)

Per-group winner selection maintains the current winning row for each group and
emits only the winner changes for groups touched by an input change (`ArgMaxByOp`
and `ArgMinByOp` in the reference implementation). These operators are
executable and graph-only: each takes any single upstream graph input, including
filtered, joined, or unioned inputs.
They may consume a recursive node's output, but neither operator may occur
inside a recursive seed or step graph (`INV-REC-13`, ch. 6).

For base-table inputs, the table primary key must equal the group columns
followed by the order columns, in that exact order (`group_cols + order_cols`).
For every plan shape, that declared key is the only key compared under the
operator direction (`INV-QUERY-20`): `ArgMaxBy` selects the greatest key and
`ArgMinBy` selects the least key. When distinct records have the same comparison
key, encoded full-record bytes ascending are the final tie-breaker for both
operators. Consequently, an exact-key tie selects the bytewise least full record
even for `ArgMaxBy`; arrival order, iteration order, and arbitrary payload-field
order do not extend or otherwise alter the declared comparison key.

Multiplicity is keyed independently by encoded full-record bytes. Distinct
records tied on the comparison key therefore remain separate candidates and can
be retracted independently. A record is eligible while its consolidated
multiplicity is positive, and a group emits one copy of its selected winner;
changes that neither remove the winner nor select a different record emit no
winner delta.

The names are module labels, not taxonomy claims: despite their `op_types` home
under "aggregate," they are winner-selection operators over graph input, not
general aggregates. jazz, an external consumer, uses `ArgMaxBy` to maintain
current-row (latest-version) state and uses `ArgMinBy` as the narrow maintained
primitive for unordered `limit(1)`: an empty group with `row_uuid` as the
comparison key yields the stable least-`row_uuid` row from the visible result
set.

_Further invariants._ `INV-QUERY-21` — `ArgMaxBy`/`ArgMinBy` suppress
non-winner and net-zero group deltas.

### 3.6 `TopBy` (maintained ordered windows)

`TopBy` is the general maintained ordered-window operator. It is the intended
replacement for ad hoc ordered `LIMIT`/`OFFSET` handling and for consumers that
need more than the single winner provided by `ArgMaxBy`/`ArgMinBy`.

A `TopBy` operator has:

- `partition_cols`: the fields that define independent groups. An empty list is
  one global partition.
- `order_cols`: the declared sort key, with per-column direction and null
  ordering.
- `tie_cols`: stable fields appended after `order_cols` to make the total order
  deterministic.
- `offset` and `limit`: the retained window bounds. `offset` is a `u64`, and
  `limit` is represented explicitly as `TopByLimit::Finite(u64)` or
  `TopByLimit::Unbounded`. A finite zero limit denotes an empty window.
- `output`: the original input record, optionally with implementation-defined
  rank metadata only when the descriptor declares it.

For each partition, `TopBy` maintains the weighted multiset of input records
plus an ordered index over `(order_cols, tie_cols, full-record bytes)`. The
ordered stream is the partition's positive-multiplicity records sorted by
`order_cols` under their declared directions, then `tie_cols` ascending, then
encoded full-record bytes ascending; this total order MUST NOT depend on
arrival or storage iteration order (`INV-QUERY-23`). A planner should prefer a
primary-key or otherwise stable identity field in `tie_cols`; relying on
full-record bytes is correct but can be expensive.

Window occupancy is bag-semantic (`INV-QUERY-24`): a record with positive
multiplicity `m` occupies `m` consecutive ordinals of the ordered stream, and
the retained window is the half-open ordinal range `[offset, offset + limit)`,
or all ordinals `>= offset` when the limit is unbounded. Records with
non-positive multiplicity are absent. The output is the weighted multiset of
in-window copies: a record whose copies straddle a window boundary contributes
exactly the copies whose ordinals fall inside the window, as a single output
record whose weight is its in-window copy count (`INV-QUERY-25`). Worked
example: records `a×2, b×1, c×3` ordered `a < b < c` with `offset 1, limit 3`
give the ordinal stream `a a b c c c` and the window `{a×1, b×1, c×1}` — the
offset consumes one of `a`'s two copies. Inserting one more copy of `b` shifts
the stream to `a a b b c c c` and the window to `{a×1, b×2}`; the emitted diff
is `-c, +b`.

Input deltas follow the ordinary weighted rule. Inserts add copies, deletes
remove copies, and updates arrive as `-old, +new` (§4.1). For every touched
partition, `TopBy` compares the pre-tick and post-tick retained windows and
emits the minimal consolidated weighted diff of output records
(`INV-QUERY-26`); output delta weights are in-window copy-count changes and may
exceed ±1. Records whose in-window copy count is unchanged MUST NOT emit —
including rows that only move rank inside the window — unless rank metadata is
part of the output descriptor. Rows outside the retained range can still cause
deltas if they cross a boundary and displace retained copies.

Hydration evaluates the same denotation from the current input snapshot. A
commit/binding tick updates only partitions touched by input deltas; maintaining
the ordered index is operator state, not a semantic rescan license. Unbounded
retained suffixes are supported for consumers such as jazz maintained ordered
subscriptions, but they can retain and diff a large portion of each partition.
Use a finite limit when the consumer only needs a bounded window.

### 3.6.1 `CollectBy` (the single output terminal)

**Target design, Anselm 2026-08-05.** `CollectBy` is the only terminal
output-shaping operator. It consumes one ordinary flat weighted record stream
and has two descriptor-selected modes:

- **Collect** renders one output record per group as a recursive tree of named
  `Array<Record>` slots. Parent/child associations remain scalar fields on flat
  input rows; nested arrays exist only in the terminal's output.
- **Expand** renders the selected flat input rows as output tuples. A wide join
  has already carried every source column into this one unary input, so expand
  neither reads multiple sinks nor reconstructs a tuple from arrangements.

No other Groove operator and no Jazz/facade component renders either shape.
`CollectBy` may retain per-group state, including a ranked window and the flat
records needed to maintain it, but graph-internal deltas remain flat in both
modes.

The `CollectBy` descriptor MUST include every output-affecting input:

- the grouping fields, terminal mode, and output occurrence-id source fields;
- in collect mode, the output parent-field sources and a tree of collection
  slots; a slot is addressed by its unique output field name within its owner
  and carries its owner-group fields, child projection, scalar order and tie
  fields, direction, offset, limit, and nested slots. Siblings are distinct
  named slots on one record; descendants are slots on a child record. A slot's
  group fields are source fields available on its owner, so it selects its own
  owner-correlated flat rows; in expand mode, the tuple projection;
- the parent/child presence rules needed to render empty arrays in collect mode.

Descriptor recursion is limited by Groove's own
`MAX_COLLECT_BY_TREE_DEPTH` validation (currently 16). It intentionally does
not share Jazz's future `MAX_STRUCTURED_RESULT_DEPTH`/
`MAX_STRUCTURED_RESULT_WIDTH`: this validates trusted executable graph shape
before runtime allocation, while those are receiver limits for untrusted wire
values. Width is bounded at the receiver boundary in PR 4; slot names are
unique at every descriptor level here.

This is an application of `INV-QUERY-1A`: descriptors, not external planner
state, define output identity and sharing. Validation MUST require a complete,
type-compatible mode projection; in collect mode, an
`Array<Record(child_descriptor)>` output slot; and in both modes a deterministic
scalar sort key followed by a complete-output-byte tie-break. It MUST validate
the occurrence-id source-field arity against the terminal mode, and reject
record-valued types, including `Array<Record>`, in group, order, tie, occurrence
identity, or other arrangement-key fields. Arrangement iteration and arrival
order are never a semantic order.

For every touched group, the terminal forms the old and new selected windows
using the same finite/unbounded window semantics as `TopBy`. In collect mode it
encodes the old and new complete parent records and byte-compares them. Equal
bytes emit nothing; a surviving changed parent emits exactly `-old_parent` and
`+new_parent`, while an appearing/disappearing parent emits its one
addition/retraction. It never emits a child-level delta. Thus a front insert or
a window-boundary change has one whole-parent replacement even when many array
positions change.

In expand mode, the terminal projects each selected wide row as one tuple,
addressed by its `OutputOccurrenceId`. It suppresses an occurrence whose old and
new tuple bytes are equal; an appearing or disappearing occurrence emits its
one addition or retraction, and a changed occurrence emits exactly one
retraction and one addition. It MUST NOT coalesce two distinct occurrence ids
because their tuple bytes match. A finite limit bounds each selected window;
an unbounded terminal's rendered work and bytes scale with its whole selected
group.

For `INV-INC-2`, let `D_g` be a touched root group's input delta, `G_g` its
retained flat state, and `T_g^-`/`T_g^+` its old/new rendered selected trees.
Define `R_g(limit)` as the larger row-and-byte footprint of the selected windows
at **every slot in those trees**, including encoded parent/child records. For
finite slots it is the sum of their selected bounded windows, not merely the
root window. Indexed state maintenance MAY cost `O(|D_g| log(1 + |G_g|))`.
Everything after that index maintenance — selecting, rendering, comparing, and
delivering — MUST be bounded by `O(|D_g| + R_g(limit))` and inspect only `D_g`
and the old/new selected tree windows, never scan, re-materialize, or diff the
rest of `G_g` or unrelated groups. Collect mode emits at most one whole-root
parent replacement, even for a descendant change. Expand uses the flat selected
occurrence diff. This is not weaker: unbounded descendants make `R_g` unbounded
by their actual selected output, but never license accumulated-state work.

`CollectBy` MUST NOT compose as a graph node. Node validation and terminal
preparation MUST reject a collector as an input to every other node, including a
second collector (`INV-QUERY-27`); planner convention is insufficient. A nested
structured result is rendered by one terminal-owned internal collector tree that
consumes flat associations and writes directly into its final output, never by
flowing an inner collection update through an ordinary graph edge. Nested slots
are descriptor data within the one terminal, not composed graph nodes, so this
does not weaken `INV-QUERY-27`. This
restriction keeps the graph's deltas flat and prevents mode-specific terminals
from drifting.

The terminal's output descriptor may contain
`ValueType::Record(Box<RecordDescriptor>)` only under the canonical-byte and
key-rejection rules of ch. 2. It is an inline descriptor form, not a registry
lookup. Groove does not assign semantics to any external query, policy, or
permission vocabulary; those systems provide descriptor-complete flat inputs to
this operator.

### 3.7 `Aggregate` (maintained grouped summaries)

`Aggregate` maintains per-group summary rows over a weighted input multiset. It
has `group_cols`, a list of aggregate functions, and an output descriptor
containing the group fields followed by aggregate result fields. An empty
`group_cols` list is one global group.

Supported maintained aggregate functions are limited to summaries whose state
can be updated by weighted deltas:

- `count(*)`: signed total input multiplicity for the group.
- `count(expr)`: signed total multiplicity where `expr` is non-null.
- `sum(expr)`: weighted sum over numeric values, including signed `I64`.
- `avg(expr)`: mean over non-null values, returned as `F64`.
- `min(expr)` / `max(expr)`: extremum over positive-multiplicity values, backed
  by an ordered value index with deterministic full-record tie accounting.
- `any_value(expr)`: the value from the deterministic least ordered witness,
  only when paired with an explicit `order_by`/tie key in the aggregate spec.

`Aggregate` state is per group. It stores aggregate accumulators and, for
retractable extrema or ordered witnesses, the value-to-record counts required to
find the next winner after a deletion. A group exists in the output only while
its input multiplicity is positive, unless the aggregate spec explicitly asks
for an SQL-style empty global aggregate row.

Non-count aggregate outputs are nullable, which is what lets empty and all-null
inputs follow SQL: an empty global aggregate row reports `NULL` for `sum`, `avg`,
`min` and `max` while `count` reports `0`, and an input group whose values are
all `NULL` reports the same. `count` is never null. Nullable INPUT columns are
accepted for every aggregate function — `NULL` values are skipped rather than
rejected — which is the case these semantics exist to serve.

Not in scope: distinct aggregates (`sum(distinct expr)` and friends) are
rejected, and floating-point replay determinism is deliberately outside the
maintained contract until specified, since summation order is not guaranteed
across replays.

Each input delta updates the affected group state. The operator computes the
group's old output row and new output row and emits the minimal consolidated
diff: `-old, +new` when a summary changes, `+new` when a group appears, `-old`
when a group disappears, and no delta for net-zero state. Same-tick churn is
consolidated by group before emission. Negative multiplicity below zero is a
runtime error: it means the upstream weighted multiset retracted a row that was
not present in that operator scope.

Determinism is part of the contract. Aggregates whose result depends on witness
choice (`min`/`max` with equal values, `any_value`) MUST use declared tie keys
and then encoded full-record bytes as the final order. Floating-point aggregate
functions are not part of the maintained contract until their replay
determinism is specified.

### 3.8 Operator advertisement and current executable scope

The runtime may carry operator descriptors before every descriptor is executable
for every graph shape. The durable contract is capability honesty: a query
operator MUST NOT be advertised as executable unless the runtime can execute that
operator for the advertised scope (`INV-QUERY-22`). Unsupported descriptors must
fail explicitly rather than be approximated.

Implementation status:

- `SemiJoin` is executable for ordinary, non-recursive graph execution. It shares
  the anti-join arrangement machinery and emits left rows whose join key has
  positive right-side multiplicity. Recursive seed/step placement remains
  unsupported.
- `Aggregate` is executable for ordinary, non-recursive graph execution over the
  implemented aggregate subset: `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` over
  supported field/nullability shapes. Distinct aggregates are rejected. Integer
  sums are bounded by the output integer type; `AVG` returns `F64`; empty/all-null
  non-count summaries are not yet SQL-compatible empty aggregate rows. Floating
  replay determinism is explicitly outside the maintained contract until
  specified.
- `Distinct` is not yet executable and remains a reserved descriptor.
- `Negate` is not yet executable and remains a reserved descriptor.
- Non-inner `JoinOpKind` variants (`Left`/`Right`/`Full`) carried by `OpType`
  remain reserved until graph semantics and runtime support are specified.

### 3.9 The SQL-lowerable subset

SQL lowering is intentionally conservative. The SQL `Query` AST is broader than
the supported graph contract, so the planner rejects unsupported shapes instead
of approximating them. Parameterized SQL is handled by prepared shapes
(`plan_prepared_shape`, ch. 5); ordinary query planning (`plan_query`) rejects
parameters, and the only binding predicate accepted is `column = $param`
(`INV-QUERY-15`, `INV-QUERY-16`).

Unsupported shapes are rejected explicitly (`INV-QUERY-17`): `SELECT DISTINCT`,
`GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, derived tables, implicit
multi-`FROM` joins, non-inner joins, non-`UNION ALL` set ops, non-column
projections, and non-field-literal predicates. "Field-literal" means a
comparison between a column and a constant: `status = 'active'`, `age >= 18`,
`deleted_at IS NULL` lower; column-to-column comparisons (`a.x = b.y` outside a
join key), arithmetic, and function calls in predicates do not. `AND`/`OR`
compositions of lowerable predicates are lowerable.

_Further invariants._ `INV-QUERY-4` — SQL predicate lowering rejects
unsupported/ill-typed predicates rather than approximating them.
`INV-QUERY-18` — SQL inner joins lower only equality predicates, with `AND`
forming multi-column join keys.

### 3.11 Subsumed operator backlog

The former ordered-index top-k project is folded into the operator/query model.
The problem statement remains valid: `ORDER BY ... LIMIT k` should not
materialize, sort, and discard the full result when an ordered index can stream
candidate rows in result order. A future streaming or pull-based path may remove
the explicit sort for eligible shapes, but it must compose with filters,
policies, joins, offsets/cursors, and incremental maintenance.

Count aggregation and projection hot-path optimizations are likewise operator
work under the same graph contract. They should add or specialize operators only
when the weighted-delta semantics stay observable-equivalent to the existing
graph.

## Open Questions

- 🔶 [#1802](https://github.com/garden-co/jazz/issues/1802) — ArgMaxBy terminology.
- 🔶 [#1776](https://github.com/garden-co/jazz/issues/1776) — Ordered windows, cursor pagination, join planning, weighted duplicates, and COUNT.
- 🔶 [#1770](https://github.com/garden-co/jazz/issues/1770) — Projection hot-path ownership and copies.
