# Problem: No plan space, no place to put optimization

## What's broken

Every query in jazz2 has exactly one execution shape. Lowering from `RelExpr` to the graph node DAG is a deterministic, mechanical translation — no choice, no alternatives, no cost. The pipeline for any `ORDER BY ... LIMIT k` query is, always:

```
IndexScan (all matching rows)
  -> Materialize (load every row from storage)
    -> Sort (in-memory, full result set)
      -> LimitOffset (window down to k rows)
```

This is fine until you want the planner to make a decision. The moment you do, there is nowhere to make it. Adding "use an ordered index when one exists" today means bolting a special case onto graph construction. Adding "pick between order-driven and filter-driven scans" means another special case. Each new optimization gets layered onto the same flat surface, with no shared abstractions for plan alternatives, costs, or properties.

## Who is affected

- **App authors using Jazz** — anyone writing a feed, leaderboard, inbox, activity log, paginated list, or any join across multiple tables. The cost penalty is invisible until tables grow, then it is uniform across query shapes: O(n log n) where O(k) was achievable.
- **End users of Jazz-powered apps** — unnecessary latency and memory pressure on the client device, especially on mobile and low-memory browsers running WASM.
- **Future contributors** — every planned optimization (predicate pushdown into joins, top-k fusion, index-driven sort elimination, multi-index intersection, join algorithm choice) collides with the same architectural absence. Without a foundation, each lands as a one-off with no compounding leverage.

## The audit (reference)

Surveying the current code:

- Parsing represents `LIMIT`, `OFFSET`, `ORDER BY` as three separate `RelExpr` nodes. They are never fused.
- Logical planning unwraps them into a `QueryEnvelope` carried alongside the plan. Still no top-k recognition.
- There is no optimizer module. No `optimizer.rs`, no `rewrite.rs`. No limit pushdown. No index-aware sort elimination. No top-k fusion.
- Execution has a `SortNode` that always materializes and sorts the full input, followed by `LimitOffsetNode` that does skip-and-discard offset.
- The `Storage` trait exposes `index_lookup`, `index_range`, `index_scan_all` — none of which support ordered iteration with direction, take, or skip. There are no cardinality methods.
- Tests cover correctness; there are no benchmarks for top-k or pagination.

The previously-scoped project at `todo/projects/ordered-index-topk-query-path/` documents the symptoms in detail and proposes a streaming pipeline to fix them. It does not propose the architectural foundation. Without that foundation, the work would land as a special case alongside today's hardcoded lowering — which leaves the next optimization in the same position the top-k optimization is in now.

## Concrete examples

These are the same shapes called out in the previous project — they remain the proximal trigger for this work.

1. **Chat app**: `SELECT * FROM messages WHERE channel_id = ? ORDER BY created_at DESC LIMIT 50`. A channel with 10 000 messages loads all 10 000 into memory to show the latest 50.

2. **Task board**: `SELECT * FROM tasks WHERE project_id = ? ORDER BY priority ASC LIMIT 20`. Even a modest project with 500 tasks pays the full sort cost for one screen of results.

3. **Infinite scroll**: User scrolls to page 3 (`OFFSET 40 LIMIT 20`). The entire result set is re-materialized and re-sorted from scratch.

4. **Joined feed**: `SELECT posts.*, users.name FROM posts JOIN users ON posts.author_id = users.id ORDER BY posts.created_at DESC LIMIT 20`. Even with both columns indexed, the full join materializes before any limit applies. Choice of join algorithm and driver table is hardcoded.

5. **Selective filter on big table**: `SELECT * FROM events WHERE tenant_id = ? ORDER BY created_at DESC LIMIT 10` where one tenant has 50 rows out of 5 000 000. The order-driven path scans tens of thousands of `created_at` entries to find ten matches; the filter-driven path sorts 50 rows. The right choice depends on cardinality, which the planner currently cannot see.

## What should happen instead

A query planner that:

- Treats execution shape as a **plan space**, not a fixed pipeline. For a given logical query there are multiple physical plans; the planner enumerates them and picks the cheapest.
- Models index access as **access paths**: each scan can produce multiple alternatives differing in which index is used, in what direction, with what predicate folded in, and what order is delivered.
- Uses **exact cardinality** from index counts (no histograms, no sampling) for the cost model. Jazz auto-indexes every column, so this is always available.
- Uses **System-R bottom-up dynamic programming** to enumerate join orders and access path combinations, memoized by `(table_set, output_order, mode)`.
- Treats incremental maintenance under deltas as a **planner constraint**: the same planner serves one-shot reads and live subscriptions; subscriptions reject plans that cannot maintain.
- Renders the chosen plan to a debuggable tree (Rust `Display`, internal only) so test snapshots can assert plan shape and humans can debug planner decisions.

The original top-k pitch then becomes an emergent consequence: rules and access paths in this architecture produce the streaming ordered-index pipeline naturally, without special cases.

## Open questions

- **Aggregation and GROUP BY**: out of scope for this project. Aggregation needs its own physical operators and cost rules; this spec must not preclude that work but does not deliver it.
- **Subqueries**: also out of scope. The IR supports them; the planner will leave them as opaque sub-plans for v1.
- **Composite indexes**: not delivered. Multi-column ORDER BY uses prefix-scan + group-sort on a single-column index, matching the original top-k pitch.
