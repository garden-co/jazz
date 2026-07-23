# groove — Specification · 3. Queries & operators

## Overview

Queries in groove describe incremental views. They implement a subset of SQL
semantics, but the durable contract is not an execution plan in the traditional
database sense. A query becomes a graph of _operators_ that defines how weighted
row changes move through the view. This chapter specifies the _what_ of
evaluation; chapter 4 specifies the _how_ (the tick, arrangements, propagation).

Invariant digest:

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
- `INV-QUERY-18`: SQL inner joins MUST lower only equality column predicates, with AND forming multi-column join keys.
- `INV-QUERY-19`: BindingSourceOp MUST NOT be evaluated through ordinary subscription/query graphs outside prepared shapes.
- `INV-QUERY-20`: ArgMaxByOp and ArgMinByOp MUST accept arbitrary upstream graph inputs. Base-table inputs MUST have primary-key columns exactly groupcols + ordercols; non-table inputs...
- `INV-QUERY-21`: ArgMaxByOp and ArgMinByOp MUST emit only winner changes for touched groups, suppressing non-winner changes and net-zero group deltas.
- `INV-QUERY-22`: OpType::SemiJoin, OpType::Distinct, OpType::Negate, and OpType::Aggregate MUST NOT be advertised as executable query operators until runtime support exists.
- `INV-QUERY-23`: TopBy MUST order each partition's positive-multiplicity records by order_cols with declared directions, then tie_cols ascending, then encoded full-record bytes ascending; the total order MUST NOT depend on arrival or iteration order.
- `INV-QUERY-24`: TopBy window occupancy is bag-semantic: a record with positive multiplicity m occupies m consecutive ordinals, the retained window is the ordinal range [offset, offset + limit), and non-positive-multiplicity records are absent.
- `INV-QUERY-25`: A record straddling a window boundary MUST contribute exactly its in-window copies, as one output record whose weight is the in-window copy count.
- `INV-QUERY-26`: Per touched partition TopBy MUST emit the minimal consolidated weighted diff of retained windows; unchanged in-window copy counts MUST NOT emit, including rank-only moves, unless rank metadata is declared.

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
- **Index** (`GraphBuilder::Index`, `IndexByOp`) — exposes an indexed view of a
  source; its durable persistence is ch. 2, its tick participation ch. 4.
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

For base-table inputs, the table primary key must equal the group columns
followed by the order columns, in that exact order (`group_cols + order_cols`).
For non-table inputs, `group_cols + order_cols` is the comparison key used to
select the winner (`INV-QUERY-20`). `ArgMaxBy` selects the greatest comparison
key; `ArgMinBy` selects the least comparison key. Ties are deterministic because
the comparison key is the declared primary-key/comparison-field sequence.

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

### 3.7 `Aggregate` (maintained grouped summaries)

`Aggregate` maintains per-group summary rows over a weighted input multiset. It
has `group_cols`, a list of aggregate functions, and an output descriptor
containing the group fields followed by aggregate result fields. An empty
`group_cols` list is one global group.

Supported maintained aggregate functions are limited to summaries whose state
can be updated by weighted deltas:

- `count(*)`: signed total input multiplicity for the group.
- `count(expr)`: signed total multiplicity where `expr` is non-null.
- `sum(expr)`: weighted sum over numeric values.
- `min(expr)` / `max(expr)`: extremum over positive-multiplicity values, backed
  by an ordered value index with deterministic full-record tie accounting.
- `any_value(expr)`: the value from the deterministic least ordered witness,
  only when paired with an explicit `order_by`/tie key in the aggregate spec.

`Aggregate` state is per group. It stores aggregate accumulators and, for
retractable extrema or ordered witnesses, the value-to-record counts required to
find the next winner after a deletion. A group exists in the output only while
its input multiplicity is positive, unless the aggregate spec explicitly asks
for an SQL-style empty global aggregate row. For maintained subscriptions,
empty-group output should be capability-gated until the output null/default
semantics are represented in the descriptor.

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

### 3.8 Reserved (non-executable) operators

Some operator descriptors are reserved names, not executable query behavior.
They are therefore **not part of the query contract** until runtime support
lands: `Aggregate`, `SemiJoin`, `Distinct`, `Negate`, and the non-inner
`JoinOpKind` variants (`Left`/`Right`/`Full`) carried by `OpType`. The
`Aggregate` semantics above are an implementation target; until runtime support
exists it must not be advertised as executable (`INV-QUERY-22`).

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

### Open questions

- 🔶 **`ArgMaxBy` terminology.** It lives under "Aggregate" in `op_types` but is
  source-backed and PK-constrained. Decide whether to rename it (e.g. a
  "latest/winner" operator) so the taxonomy doesn't imply general aggregation.
- 🔶 **Ordered top-k operator path.** Decide whether this is a new streaming
  protocol between nodes, a specialized source/limit operator, or planner sugar
  over existing ordered arrangements.
- 🔶 **Cursor pagination.** Ordered top-k needs a stable cursor model that
  resumes from the prior page without re-sorting the full result set and without
  breaking deterministic ordering for ties.
- 🔶 **JOIN plus ordered top-k.** Define when an ordered index on one side of a
  join can drive the plan without losing rows that only become eligible after
  join or policy filtering.
- 🔶 **Weighted duplicates at the jazz boundary.** `TopBy` windows are
  bag-semantic (`INV-QUERY-24`), so a maintained ordered subscription can
  observe a row with multiplicity > 1 when upstream unions or projections
  produce duplicate records. Decide whether jazz lowering must guarantee at
  most one derivation per logical result identity or whether jazz subscription
  delivery must define rendering for weighted window rows. Carrying `row_uuid`
  as a tie field makes ordering deterministic but does not enforce multiplicity
  one.
- 🔶 **COUNT aggregation.** Add a terminal count shape with weighted-delta
  maintenance and clear output descriptor semantics.
- 🔶 **Projection memcpy optimization.** `Project` should avoid unnecessary row
  copies on hot paths where descriptor layout permits borrowing or direct field
  assembly.
