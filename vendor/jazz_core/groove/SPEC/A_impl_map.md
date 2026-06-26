# groove — Specification · Appendix A. Implementation map

*Non-normative (guidance).* This appendix maps the normative chapters to the
implementation. It is a navigation aid, not a source of contract language: when
the question is what the system guarantees, the normative chapters control; when
the question is where an implementation concern lives, the source tree controls.

## A.1 Where to start

The implementation is easiest to read along the same path a query follows
through the system. Start at the database facade (`db::Database`), continue
through SQL-to-graph lowering (`ivm::planner`), inspect the graph IR
(`ivm::graph`), and then read the tick engine (`ivm::runtime`). This order
matches `src/lib.rs`, and chapters 2–4 use the same spine.

## A.2 Module layering

| module | owns | chapter |
|---|---|---|
| `records` | encoded row layout, `RecordDescriptor`, logical vs physical order | 2 |
| `schema` | declarations; `ColumnType` → `ValueType` | 2 |
| `storage` | the byte-oriented `OrderedKvStorage` interface (`memory`, `rocksdb`) | 2 |
| `queries` | SQL-ish AST (syntax only; broader than what lowers) | 3 |
| `ivm` | `planner` / `graph` / `op_types` / `runtime` | 3–6 |
| `db` | ties schema + storage + commits + subscriptions + query APIs | 1–7 |

## A.3 The runtime

The runtime is the point where the graph becomes maintained state. Its central
state (`IvmRuntime` in `ivm/runtime/mod.rs`) includes the `graph`,
`subscriptions`, `prepared_shapes`, `binding_sources`, `operator_states`,
`arrangement_states`, `eval_memo`, and `current_tick`. The important paths are:

- **the tick** — each tick first drains pending binding retractions, then
  advances the tick clock (`advance_tick`), processes durable nodes
  (`tick_durable_nodes`), updates direct subscriptions
  (`TickEvaluator::update_node`), routes prepared-shape outputs
  (`route_shape_records`), drops dead receivers, clears `eval_memo`, and records
  `RuntimeStats`. The entry point is `tick_with_params` (ch. 4).
- **hydration** — snapshots rebuild arrangements in `Replace` mode through
  `hydration_snapshot`, `hydrate_shape_graph`, and `query_snapshot` (ch. 4–5).
- **joins/arrangements** — joins share arranged state by
  `ArrangementKey { scope, input, fields, descriptor }`, with freshness recorded
  as `AsOf<…, SubTick>`. The implementation lives in `runtime/join.rs`
  (`ArrangementState`, `JoinState`, `AntiJoinState`) (ch. 4).
- **recursion** — recursive maintenance lives in `runtime/recursion.rs`, with
  `RecursiveState`, `recursive_delta`, `recompute_recursive`, and
  `hydrate_recursive_arrangements` providing the main entry points (ch. 6).
- **prepared shapes** — prepared-shape binding and output routing are maintained
  through `BindingSourceState`, `add_binding_ref`, `remove_binding_ref`,
  `shape_materialized_snapshot`, and `route_shape_records` (ch. 5).
- **persisted indices** — persisted indices are represented as
  `TableSource → IndexBy → Persist`; `runtime/persist.rs::apply_persist_delta`
  consolidates deltas and enforces uniqueness (ch. 2).

Commits enter the maintained graph as table deltas. That path lives in
`db/mod.rs`: `commit_batch`, table-delta construction, and
`consolidate_table_deltas` (ch. 4).

## A.4 Test map

The tests are organized around the same conceptual boundaries as the
implementation. Broad facade behavior lives in `src/db/tests.rs`; record
encoding lives in `src/records/tests.rs`. The `tests/` regression files isolate
specific bug classes: `anti_join_*`, `arrangement_*` for shared-arrangement
freshness, `prepared_binding_*`, `recursive_cycle_*`, and
`snapshot_subscription_*` for hydration isolation. These are where the
`INVARIANTS.md` registry's enforcing tests mostly live.

## A.5 The optimization story, in code

Before the benchmarks in appendix B, the implementation exposes the main reasons
the design is efficient. Node sharing removes duplicate graph work
(`IvmGraph::dedup_node`, `RuntimeStats::dedupe_ratio`). Shared arrangements reuse
the same keyed state (`ArrangementKey`). Prepared shapes replace many
literal-filter graphs with one maintained graph plus binding rows. Hydration and
steady-state ticks are separated by the `Replace`-vs-`Accumulate` mode split.

Freshness failures are deliberately explicit. Shared and recursive state carries
`AsOf` stamps, so stale or out-of-order reads surface as `StaleRuntimeState` or
`OutOfOrderRuntimeState` rather than silently producing wrong rows.

## A.6 Structural campaign — remaining work

The remaining structural work is about readability and ownership before team
onboarding: large concepts should be immediately findable, algorithms should
present large steps before small ones, and parallel representations or
forwarding wrappers should collapse when they do not carry distinct semantics.
Completed slices live in git history as the audit trail, not in this appendix.

Remaining groove work:

- **C1 — delete `LogicalPlan`.** The `FieldRef` infrastructure landed: planner
  emits `Resolved(index)`, external callers keep `Name`, runtime resolves only
  names. Deletion is deferred on a real blocker: prepared-shape pushdown
  (Project→Join→pushed BindingRelation) rewrites inside child graphs and needs
  each child's visible output fields, which `LogicalPlan` carries as plan
  metadata. To finish, GraphBuilder nodes must carry visible-field annotations,
  completing GraphBuilder as a self-describing IR — a real primitive, not a
  wrapper.
- **C2 — join-key re-decode.** `JoinState` stores field names already in
  `ArrangementKey`; join keys are re-decoded up to 3× per delta. Collapse and
  cache this, coordinating with propagation performance work.
- **C3 — schema into runtime.** `db/mod.rs` re-walks the schema per operation
  while the facade forwards. Move schema into the runtime and keep a thin
  facade.
- **C5 — subscription-state fold.** `SubscriptionState`,
  `PreparedShapeState`, and `BindingSourceState` triplicate output refs and
  reverse maps in the runtime; fold them.
- **C7 — confirm policy matching is unified.** The `policy.rs` home landed;
  verify no residual near-duplicate matching across the record, row, and
  current overloads, and unify any that remain.
- **Runtime split.** Add `arrangements.rs` for the central arrangement concept
  (key, state, freshness, pruning) and `gc.rs` for retainers and ephemeral
  pruning; hoist the tick narrative to the top of `runtime/mod.rs`. This lands
  after propagation performance work and coordinates with whatever it changes.
- **Planner two-phase.** Make name resolution and lowering two visible steps;
  this is subsumed by C1.

Sequencing note (2026-06-14): the cold-hydration locus is resolved — current-row
reads are now O(current+ahead) through direct global base plus ahead overlay, so
structural moves in `peer.rs` and the current-row path are unblocked.
Propagation performance is still the remaining hot-path investigation, so C2
and the runtime/arrangements split still coordinate with it. `ingest.rs` has
grown further (connection-auth, attribution, and the §4.3 large-value merge
engine), so the corresponding jazz grouping split is now more warranted.

Style rules for future structure:

1. File heads carry the concept: entry points and the large-step narrative come
   first; helpers follow; `mod` docs say what lives here and what deliberately
   does not.
2. One representation per truth; where two structures share a shape for
   different roles, the names must carry the roles.
3. No wrapper without semantics: forwarding-only types and value round-trips are
   debt by definition.
