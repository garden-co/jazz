# Subgraph Sharing (Array Subqueries) — Status Quo

This handles nested data loading — the SQL equivalent of "for each user, fetch their posts." In the TypeScript API, this is what powers `.include({ posts: true })`. Internally, it's a correlated subquery: for each row in the outer query, evaluate a sub-query with the outer row's ID as a filter.

This is architecturally separate from joins (which produce flat rows). Array subqueries produce nested arrays — each outer row gets an array of related inner rows attached to it.

## Current Approach: Recompile Per Binding

The straightforward approach: each outer row gets its own compiled query graph. This is simple and correct, though not optimal for large result sets (see [optimization TODOs](../todo/subgraph_sharing.md)).

For each unique outer row:

1. Extract correlation value from outer tuple (e.g., `user.id`)
2. Create fresh `SubgraphInstance` via `SubgraphTemplate::instantiate()`
3. Compile new `QueryGraph` with correlation value as filter (e.g., `WHERE author_id = <user.id>`)
4. Settle graph to get results
5. Store array result

> `crates/groove/src/query_manager/graph_nodes/subgraph.rs:55-153` (instantiate)
> `crates/groove/src/query_manager/graph_nodes/array_subquery.rs:139-277` (process + evaluate)

## Integration with QueryGraph

Array subquery nodes are chained into the graph pipeline after materialization:

- `compile_array_subquery()` creates nodes during graph compilation
- `array_subquery_tables` tracks which inner tables each node depends on
- On inner table change: `mark_inner_dirty()` → `reevaluate_all()` re-settles all instances
- On outer delta: `process_with_context()` evaluates subgraph for new/updated outer rows

> `crates/groove/src/query_manager/graph.rs:351-364` (compilation), `988-1052` (dirty marking), `1341-1357` (settlement)

## Features

| Feature                             | Status  | Location                               |
| ----------------------------------- | ------- | -------------------------------------- |
| Foreign key correlation             | Working | `query.rs:715-723` (correlate)         |
| Nested array subqueries             | Working | `query.rs:760-779`, `graph.rs:704-712` |
| Order by / limit within subquery    | Working | ArraySubqueryBuilder methods           |
| Select (projection) within subquery | Working | ArraySubqueryBuilder.select()          |
| Joins within subquery               | Working | ArraySubqueryBuilder.join().on()       |
| Multiple array columns              | Working | Chainable .with_array() calls          |

## API

```rust
qm.query("users")
    .with_array("posts", |sub| {
        sub.from("posts")
           .correlate("author_id", "users.id")
           .select(&["id", "title"])
           .order_by_desc("created_at")
           .limit(10)
    })
    .build();
```

Nested:

```rust
.with_array("posts", |sub| {
    sub.from("posts")
       .correlate("author_id", "users.id")
       .with_array("comments", |sub2| {
           sub2.from("comments").correlate("post_id", "posts.id")
       })
})
```

> `crates/groove/src/query_manager/query.rs:630-779` (builder API)

## Performance Observations

- **Memory**: 1000 outer rows = 1000 compiled graphs (proportional to `num_outer_rows * nodes_per_subgraph`)
- **Update cost**: `reevaluate_all()` re-settles ALL instances on any inner table change — O(outer_rows \* inner_change_frequency)
- **Common pattern**: Most subqueries correlate on foreign keys and scan the same inner table with only the filter value changing

## Test Coverage

12 comprehensive tests in `manager_tests.rs`:

- Single/multiple users with posts, user with no posts
- Delta on inner insert, delta on outer insert
- Order by, limit, select, join within subquery
- Nested arrays, multiple array columns

Plus unit tests in `array_subquery.rs` and `subgraph.rs`.

## Key Files

| File                            | Purpose                                            |
| ------------------------------- | -------------------------------------------------- |
| `graph_nodes/subgraph.rs`       | SubgraphTemplate, SubgraphInstance (187 lines)     |
| `graph_nodes/array_subquery.rs` | ArraySubqueryNode, evaluation logic (509 lines)    |
| `query.rs`                      | ArraySubquerySpec, ArraySubqueryBuilder (API)      |
| `graph.rs`                      | Compilation, dirty marking, settlement integration |
