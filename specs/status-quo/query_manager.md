# Query Manager — Status Quo

The Query Manager is where Jazz turns raw tables into live relational reads.

If the storage layer answers:

> "What row batch members and visible entries exist?"

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

That is what makes the current table-first engine practical on local devices: the query layer can stay reactive without eagerly decoding every stored row.

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

`db.all(...)` and `db.one(...)` compile a query, settle it, return the first full snapshot, and tear the subscription back down.

### Live subscription

`db.subscribeAll(...)` keeps that graph around. Later local writes, remote row batch members, policy changes, or schema activations mark parts of the graph dirty, and the next settle pass emits just the changed rows.

This shared machinery is why one-shot reads and live reads stay behaviorally aligned.

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

That is the reason sync can stay query-scoped without every transport layer needing to understand policy evaluation itself.

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
