# Query Manager - Legacy Alpha Facade

> Historical alpha note: this document describes deleted or legacy `jazz-tools` alpha internals. It is retained for migration context only; do not treat module paths or implementation details here as active architecture.

The Query Manager is the old alpha relational execution layer. In the grafted
core branch, its public schema/query vocabulary remains useful, but new
execution semantics should be implemented in core / Groove and then
surfaced through the `jazz-tools` facade.

If the storage layer answers:

> "What row batch entries and visible entries exist?"

the Query Manager answers:

> "Which rows match this query right now, and how did that answer change?"

That second part matters as much as the first. Queries in Jazz are usually long-lived subscriptions, not fire-and-forget requests.

## The Mental Model

The Query Manager works over:

- raw tables
- persisted indices
- visible row entries
- row-history fallbacks when a tier-specific winner differs from the current visible winner
- schema context from `SchemaManager`
- session/policy context for permission-aware filtering

So the engine is relational all the way through:

- index scans find candidate rows
- materialization turns those candidates into rows
- filter/sort/limit/project nodes shape the result
- output nodes emit deltas to subscribers

## Query Graph Shape

Most single-table queries compile to a graph like this:

```text
IndexScan -> Materialize -> Filter -> Sort -> Limit/Offset -> Output
```

More advanced queries add:

- `Union` for disjunctions and branch unions
- `Join` and `ArraySubquery` for relation traversal
- `PolicyFilter` for permission-aware row filtering
- `Project` and recursive relation nodes for shaping the final result

The important point is that the graph is incremental. Nodes do not recompute the whole world on every change. They consume tuple deltas and push transformed deltas downstream.

## Why It Is Index-First

Jazz does not treat table scans as the normal way to answer queries.

Instead:

- every query starts from one or more persisted index scans
- the `_id` index acts as the manifest for "all rows in this table"
- materialization only happens for rows that survived the earlier graph stages

That is what made the legacy alpha table-first engine practical on local devices: the query layer could stay reactive without eagerly decoding every stored row.

## Materialization: Where Rows Become Rows Again

`Materialize` is the point where the graph crosses from identifiers into row content.

Given candidate row ids, it:

1. resolves the row's table if necessary
2. loads the current visible entry for the relevant branch
3. falls back to a history lookup only when the query asks for a lower durability tier than the current visible winner
4. decodes or reprojects the flat row through `row_format`, projecting away the reserved `_jazz_*` columns when producing the app-facing row
5. emits added/updated/removed tuple deltas

This is why the visible region matters so much. Most current-state queries never need to reconstruct a row from raw history scans.

## One-Shot Queries and Live Subscriptions

Both APIs use the same graph engine.

### One-shot query

`db.all(...)` compiles the query as supplied, settles it, returns the first full snapshot, and tears the subscription back down. `db.one(...)` follows the same path after setting the root query limit to one, then returns the first row from that snapshot or `null`.

### Live subscription

`db.subscribeAll(...)` keeps that graph around. Later local writes, remote row batch entries, policy changes, or schema activations mark parts of the graph dirty, and the next settle pass emits just the changed rows.

This shared machinery is why one-shot reads and live reads stay behaviorally aligned.

## Maintained-View Observation Invariant

`INV-MV-1`: no state that feeds a maintained view may change without the maintained view observing
that change.

There are two valid ways to satisfy the invariant:

- mutate the state through the runtime delta path, so the maintained graph sees ordinary positive
  and negative deltas in the same tick
- explicitly rebuild the affected maintained view from its authoritative base state

Storage-level shortcuts are allowed only when no live maintained state can observe the changed
rows, or during recovery before maintained state has been rebuilt. A raw write behind the graph's
back is a correctness bug even if a fresh one-shot query would see the right answer.

The producer inventory is:

- upstream sync apply, including receiver-side bundle ingestion and fates
- local commit finalize
- fate application, including merge-back and ahead-overlay cleanup
- subscription registration, unregistration, and settled-cache replay
- repair/refetch apply
- recovery rebuild and recovery sweeps

The invariant was made explicit after five July 2026 incident classes exposed the same underlying
failure mode: fallback misclassification, bulk-load suppression, serve-dirty gating/epoch misses,
delta-path fated cleanup that removed ahead rows without retractions, and subscriber dirty
propagation dropped at a receiver-batch boundary.

The practical review question is:

> If this code changes rows, membership facts, settled sets, payload facts, fates, or coverage
> state, which maintained view observes the change, and through which delta or rebuild?

## Branches, Schemas, and Lenses

The Query Manager never assumes a single universal table image.

It works with branch-aware and schema-aware context from `SchemaManager`:

- branch names identify which visible table image to read
- live schema sets determine which branches are relevant
- lenses translate columns and row values when old data is read through a newer schema view

That means a query can still feel like a normal table query even when the runtime is simultaneously serving multiple schema generations.

## Policies and Sessions

Permission-aware reads happen inside the query pipeline rather than as an afterthought outside it.

The Query Manager can attach a session to a query and use policy graphs to answer questions like:

- should this row be visible to Alice?
- does this join path imply inherited access?
- should this subscription ever receive this row?

That is the reason sync can stay query-scoped without every transport layer
needing to understand policy evaluation itself.

The legacy alpha runtime also carried an explicit row-policy mode:

- `PermissiveLocal`: no compiled policy bundle is loaded in this runtime
- `Enforcing`: this runtime must fail closed for missing explicit policy,
  either because a compiled policy bundle is loaded or because a dynamic server
  is waiting for its current permissions head

That mode is shared across local query compilation, subscription filtering,
server-side authorization, and sync-scope derivation.

In `PermissiveLocal`, the Query Manager does not synthesize deny-all filters
just because policy clauses are absent. Local session-scoped reads and writes
remain usable for offline/local-only runtimes.

In `Enforcing`, missing explicit clauses deny by default:

- `read` requires `select.using`, otherwise rows are filtered out
- `insert` requires `insert.with_check`
- `update` requires at least one explicit update clause, and every present
  clause must pass
- `delete` prefers `delete.using`, falls back to `update.using`, and otherwise
  denies
- inherited and recursive checks fail closed when the parent policy or graph
  context cannot be resolved
- dynamic servers that have learned schema but not yet learned a permissions
  head stay closed instead of temporarily behaving like local permissive runtimes

## Seeded Reachability and Compositional Policy Atoms

Read and write policies are compiled as small boolean programs over policy atoms. The current
status-quo atoms include plain column predicates, `reachable_via`, and `inherits(parent_col)`.
Atoms compose with `AND` and `OR`; the composition is part of the policy program rather than a
post-filter outside the query graph.

`reachable_via` supports two seed forms:

- a literal claim value, the degenerate seed used by earlier policies
- a set-valued keyed lookup, written as `seededBy(seed_table, user_col = claim(path), group_col)`

The set-valued form includes same-table seeds. For example, a team table can seed reachability by
projecting its own `id` column from rows where `identity_key = claim(sub)`.

The seed relation is an ordinary closure input. A grant, revoke, or seed-column update flows
through normal IVM deltas and updates maintained subscriptions without rehydrating the whole view.
Prepared fragment identity includes the seed table, seed columns, descriptor, and claim paths, but
not the subscribing shape id. That lets resource kinds sharing the same membership closure share
one maintained fragment while still routing outputs per subscriber identity.

`inherits(parent_col)` is also an atom. A child row is readable when the parent row referenced by
`parent_col` is readable under the parent's composed read policy. Lowering splices the parent policy
fragment into the child policy with correlation rebound to the joined parent row, and the child's
fragment identity includes the parent fragment's claim paths.

Child insert authorization uses parent updateability evaluated against whereOld only. The parent
row is not changed by inserting the child, so parent whereNew/update-check clauses are not evaluated
for that child insert decision.

The TypeScript `policy.gather({ start, step, maxDepth })` / `hopTo` surface lowers to the seeded
closure path only for exactly matching patterns: a claim-keyed start lookup, compatible hop
direction, and no extra step filters whose semantics are not represented by seeded reachability.
Other gather shapes stay on the legacy lowering path and must fail closed if they cannot be
represented safely.

🔶 Open questions:

- String claim type mismatches in seeded lookups should become loud validation errors instead of
  depending on runtime empty-result behavior.

## Key Files

| File                                                            | Purpose                                                                        |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `crates/jazz-tools/src/query_manager/manager.rs`                | QueryManager orchestration and subscription lifecycle                          |
| `crates/jazz-tools/src/query_manager/graph.rs`                  | Query graph compilation and settle passes                                      |
| `crates/jazz-tools/src/query_manager/graph_nodes/`              | Node implementations such as index scan, materialize, filter, sort, and output |
| `crates/jazz-tools/src/query_manager/query.rs`                  | Query builder/data structures                                                  |
| `crates/jazz-tools/src/query_manager/relation_ir_query_plan.rs` | Relation IR planning                                                           |
| `crates/jazz-tools/src/query_manager/policy_graph.rs`           | Policy evaluation support                                                      |
| `crates/jazz-tools/src/row_format.rs`                           | Shared row decoding/reprojection                                               |
