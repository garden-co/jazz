# Pitch: Ordered Index Top-K Query Path

## Problem

Paginated ordered queries (`ORDER BY ... LIMIT k`) materialize the full result set, sort it in memory, then discard everything but the first k rows. A chat app showing the latest 50 messages in a 10 000-message channel loads all 10 000 rows. Cost is O(n log n) time + O(n) memory regardless of k — unacceptable for the feed/list/inbox patterns that dominate real apps.

## Solution

### Core idea

When the `ORDER BY` column has an index, scan that index in order and stream rows one at a time through the materialize + filter + policy pipeline, stopping after k rows pass. Only the rows actually pulled get materialized — once k rows survive filtering, the scan stops and the remaining rows are never touched. Fall back to the current Sort+LimitOffset path when no suitable index exists.

### Architecture: streaming protocol between existing nodes

Rather than merging multiple nodes into one fat node, introduce a **streaming protocol** where upstream nodes yield one row at a time and the downstream LimitOffset node signals "stop."

Replace the tail of the current pipeline:

```
BEFORE:  IndexScan(all) → Materialize(all) → PolicyFilter → Filter → Sort → LimitOffset
AFTER:   OrderedIndexScan → Materialize → PolicyFilter → Filter → LimitOffset
                                                                      │
                                                          (stop signal when k rows collected)
```

The key change: **Sort is removed** (the index provides order) and each node operates in a **pull-based streaming mode** instead of materializing its full output before passing downstream. The LimitOffset node drives the pipeline — it pulls rows one at a time through the chain and stops pulling after `offset + limit` rows pass.

```
┌──────────────────────────────────────────────────────┐
│ Streaming pull pipeline                              │
│                                                      │
│  LimitOffset (drives the loop, stops at k)           │
│       │  pull                                        │
│       ▼                                              │
│  Filter ──► pass? → forward to LimitOffset           │
│       │  pull       fail? → pull next from below     │
│       ▼                                              │
│  PolicyFilter ──► pass? → forward to Filter          │
│       │  pull         fail? → pull next from below   │
│       ▼                                              │
│  Materialize ──► load row data → forward up          │
│       │  pull                                        │
│       ▼                                              │
│  OrderedIndexScan ──► yield next (value, row_id)     │
│       │                                              │
│  index exhausted? ──► signal "no more rows"          │
└──────────────────────────────────────────────────────┘
```

Each node remains a separate graph node with its own logic — the streaming protocol is a new execution mode that existing node types opt into when the planner activates the ordered top-k path. The protocol extends to support JOINs (order-preserving nested-loop), multi-branch (k-way merge), and multi-column ORDER BY (prefix-scan + group-sort).

Key property: the pipeline keeps pulling rows from the index until k rows have passed all filters and policies. If filters/policies reject some rows, the scan naturally continues past k index entries — no explicit batching or over-fetch logic needed.

### Storage trait extension

Add `index_scan_ordered` to the `Storage` trait:

```rust
pub enum IndexScanDirection {
    Ascending,
    Descending,
}

fn index_scan_ordered(
    &self,
    table: &str,
    column: &str,
    branch: &str,
    start: Bound<&Value>,
    end: Bound<&Value>,
    direction: IndexScanDirection,
    take: usize,
    skip: usize,
    skip_past: Option<(&Value, ObjectId)>,  // cursor resume point
) -> Vec<(Value, ObjectId)>;
```

The current storage backends already iterate in sorted order internally — `BTreeMap::range()` for `MemoryStorage`, ordered scans in `opfs-btree`, and ordered native KV iteration for durable native backends such as RocksDB/SQLite. The new method just exposes this with direction + take + skip parameters.

Also add **cardinality methods** for planner strategy selection:

```rust
/// Count rows matching an exact value in an index.
fn index_count(&self, table: &str, column: &str, branch: &str, value: &Value) -> usize;

/// Count total rows in an index.
fn index_count_all(&self, table: &str, column: &str, branch: &str) -> usize;
```

For `MemoryStorage`, `index_count` is `btree.get(encode(value)).map(|set| set.len())` — O(1) hash lookup + set size. `index_count_all` sums all set sizes (or maintains a running counter on insert/remove for O(1)). Durable ordered backends can count matching keys in a prefix scan.

These give the planner **exact cardinality** — no histograms, no estimation. The data structures already exist; we're just exposing their size.

The value encoding (`encode_value`) is already lexicographically ordered, so byte-level iteration matches value ordering. No encoding changes needed.

### Query planner changes

In `graph.rs`, after lowering RelExpr to ExecutionQueryPlan, add a recognition pass:

```
if plan.order_by first column has an index
   AND plan.limit is Some
then:
   1. Choose scan source:
      - single branch → one OrderedIndexScan
      - multi-branch  → k-way merge of per-branch OrderedIndexScans

   2. Choose join strategy (if JOINs present):
      - ORDER BY column from any joined table → that table drives the ordered index nested-loop join
      - ORDER BY on expression across tables  → FALL BACK to traditional path

   3. Choose sort strategy:
      - single-column ORDER BY → index provides full order (no sort needed)
      - multi-column ORDER BY  → prefix-scan + group-sort on first column

   These compose: multi-branch + JOIN + multi-column ORDER BY can all
   be active simultaneously. Each is an independent pipeline stage.

   If any component can't be satisfied → fall back to traditional Sort+LimitOffset
```

When the ORDER BY column differs from the filter column (e.g., `WHERE channel_id = ? ORDER BY created_at`), the planner has **two strategies** to choose from:

- **Order-driven**: Scan the `created_at` index in order, filter `channel_id` inline. Good when the filter is selective (few channels, each with many messages) — early termination means we stop quickly.
- **Filter-driven**: Scan the `channel_id` index for matching rows, then sort the smaller result set. Good when the filter cardinality is low (one channel with 100 messages out of 100 000 total) — sorting 100 rows is cheaper than scanning thousands of ordered entries.

The planner picks the strategy using **exact cardinality** from the index:

```
filter_count = index_count(table, filter_col, branch, filter_value)
total_count  = index_count_all(table, filter_col, branch)
selectivity  = filter_count / total_count

if selectivity < threshold (e.g., 0.1):
   → filter-driven: scan filter index (small set), sort, apply limit
else:
   → order-driven: streaming scan on order column, filter inline
```

This is not estimation — it's counting actual index entries. The cost is O(1) for MemoryStorage (hash lookup + set size). The order-driven path still has a runtime fallback: if it reads more than N × limit rows without filling the result, it abandons and falls back to the traditional batch path.

### Multi-column ORDER BY

For `ORDER BY a ASC, b DESC LIMIT k`, use the first column's index as the driver and handle ties with a bounded secondary sort:

1. Scan column `a`'s index in ascending order
2. For each group of rows with equal `a` values, buffer the group and sort by `b DESC`
3. Emit rows from the sorted group through the filter/policy pipeline
4. Stop when k rows have passed

This is a **prefix-scan + group-sort** strategy. It works well when groups are small (few ties on the first column). When groups are large (many ties), the secondary sort approaches the cost of a full sort — the fallback threshold applies here too.

The planner activates this only when the first ORDER BY column has an index. If it doesn't, fall back to the traditional Sort+LimitOffset path.

### Cursor-based pagination

Offset-based pagination (`OFFSET 40 LIMIT 20`) requires scanning and discarding the first 40 rows on every page fetch — O(offset + limit) per page. Cursor-based pagination resumes the scan from where the previous page ended — O(k) per page regardless of depth.

A cursor is a `(value, row_id)` pair representing the last row returned on the previous page. The `index_scan_ordered` storage method accepts a `skip_past` cursor to start scanning after that position:

```rust
fn index_scan_ordered(
    &self,
    table: &str,
    column: &str,
    branch: &str,
    start: Bound<&Value>,
    end: Bound<&Value>,
    direction: IndexScanDirection,
    take: usize,
    skip: usize,
    skip_past: Option<(&Value, ObjectId)>,  // cursor resume point
) -> Vec<(Value, ObjectId)>;
```

When `skip_past` is provided, the scan starts at the cursor's position in the index and advances past it, then yields the next `take` entries. The `row_id` in the cursor disambiguates ties (multiple rows with the same order-column value).

The streaming pipeline returns the cursor for the last emitted row alongside the results. The caller passes this cursor back on the next page fetch.

**Consistency**: Cursors are "best effort" — if the cursor row is deleted between fetches, the scan resumes from the next position after where that row would have been. This can produce minor gaps if rows are deleted, or minor overlaps if rows are inserted at the boundary. This matches the behavior of cursor-based pagination in most databases.

### Incremental reactivity

The streaming pipeline must participate in the reactive delta pipeline. When a row is inserted, updated, or deleted:

1. **Insert**: If the new row's order-column value falls within the current top-k window, insert it at the correct position and evict the last row if over limit. If it falls outside the window, ignore it.
2. **Delete**: If the deleted row was in the window, remove it and potentially pull in the next row from the index to fill the gap.
3. **Update**: Treat as delete + insert.

This mirrors the current SortNode's incremental behavior but operates on a bounded window. The node maintains a sorted buffer of the current top-k rows and their order-column values to make these decisions efficiently.

**Edge case — policy/filter changes**: When a policy dependency changes (e.g., a parent row's permissions change), the node must re-evaluate which rows pass. This may require re-scanning from the index. The existing `mark_inherits_dirty` / dependency tracking mechanism triggers this.

### Interaction with policies

Policies are evaluated by the existing PolicyFilterNode, which participates in the streaming protocol. No changes to policy evaluation logic — the node just needs to support the pull-based mode alongside its existing batch mode. It uses the same PolicyContextEvaluator it uses today.

### Multi-branch handling

When a query spans multiple branches, run one ordered index scan per branch and **k-way merge** the streams:

```
Branch 1: OrderedIndexScan → stream of (value, row_id) in order
Branch 2: OrderedIndexScan → stream of (value, row_id) in order
                    ↓
            k-way merge (by order column value)
                    ↓
      Materialize → PolicyFilter → Filter → LimitOffset
```

Each branch stream is individually ordered. The merge step interleaves them, always pulling from the stream whose next value comes first. Duplicate ObjectIds across branches are deduplicated (same as the current UnionNode behavior).

The number of branches is typically small (1-3), so the merge overhead is negligible. The streaming protocol drives the merge — LimitOffset pulls from the merge, which pulls from whichever branch stream is next.

### JOINs with ordered top-k

The current JoinNode uses hash sets (unordered). To support ordered top-k across joins, use an **index nested-loop join** driven by whichever table owns the ORDER BY column:

```
A JOIN B ORDER BY A.col:  drive from A, probe B
A JOIN B ORDER BY B.col:  drive from B, probe A
```

```
OrderedIndexScan(driver.col) → Materialize driver row
       │
       ▼
  For each driver row (in order):
       probe other table's join index for matching rows
       │
       ▼
  Emit (A, B) joined tuples (preserving driver's order)
       │
       ▼
  PolicyFilter → Filter → LimitOffset (stops at k)
```

Key constraints:

- The ORDER BY column must come from **one of the joined tables** (not an expression across tables).
- Only equi-joins are supported (same as today).
- The probed table's join column must be indexed (for efficient probing).

This is an order-preserving nested-loop join: the outer loop scans the driver's index in order, the inner loop probes the other table's index for matches. Each (A, B) pair is emitted in the driver's order, which satisfies the ORDER BY.

**LEFT JOIN handling:**

- `A LEFT JOIN B ORDER BY A.col`: Drive from A (the preserved side). When B has no match, emit (A, NULL). Order preserved naturally — same as today.
- `A LEFT JOIN B ORDER BY B.col`: Drive from B. Emit matched (A, B) pairs in B's order. A rows with no B match produce (A, NULL) tuples where B.col is NULL — these sort to the start (ASC) or end (DESC) per NULL ordering and are emitted as a group at the appropriate boundary.

## Rabbit holes

1. **Selectivity threshold tuning**: The exact cardinality from `index_count` tells us how many rows match a filter, but the right threshold for switching between order-driven and filter-driven still needs tuning. Too low (e.g., 0.01) and we miss cases where filter-driven would be better; too high (e.g., 0.5) and we sort unnecessarily large sets. The runtime fallback (abandon order-driven after N × limit scans) is the safety net, but the threshold determines how often we need it.

2. **Policy re-evaluation storms**: A single permission change on a parent row could invalidate the entire top-k window, triggering a full re-scan. Need to bound the re-scan cost — possibly by keeping a small "overflow buffer" of the next few rows beyond the window so the node can refill without going back to the index.

3. **Streaming protocol complexity**: Adding a pull-based streaming mode to existing nodes (Materialize, PolicyFilter, Filter, LimitOffset) means each node now has two execution modes — batch and streaming. Both must produce identical results. The protocol needs careful design: how does a node signal "no more rows"? How does backpressure work? What happens when a node needs to buffer (e.g., PolicyFilter waiting on an async INHERITS check)?

4. **Incremental window maintenance**: The insert/delete/update logic on the bounded window is the most complex part. Inserting a row that displaces the boundary row requires knowing what the next row beyond the window is — which may require an index probe. Deleting a window row requires pulling in a replacement. The interaction between window state and the streaming protocol needs thorough testing.

5. **Cursor invalidation**: Cursor-based pagination uses `(value, row_id)` as a resume point. If the row at the cursor is deleted or its order-column value changes between page fetches, the cursor becomes stale. The scan can still resume (it just starts after a position that no longer exists), but results may have gaps or duplicates depending on what changed. Need to define the consistency semantics — are cursors "best effort" or do they guarantee gap-free pagination?

6. **IEEE 754 edge cases**: ±0.0 and NaN need careful handling in ordered scans. The value encoding already handles ±0.0 but we need to verify the streaming scan preserves these semantics.

7. **Multi-column ORDER BY group explosion**: If the first ORDER BY column has low cardinality (e.g., `ORDER BY status ASC, created_at DESC` where status has 3 values), groups can be huge. The prefix-scan degenerates to sorting a third of the table. The fallback threshold should apply per-group, not just globally.

8. **Order-preserving join complexity**: The index nested-loop join needs to probe the right table's index for each left row. If the join is unselective (many matches per left row), we emit many tuples per pull, complicating the streaming flow. The streaming protocol must handle "one pull yields multiple tuples" from the join node.

9. **JOIN + multi-branch interaction**: A join across multi-branch data means the k-way merge happens before the join, and the join must probe per-branch indexes on the right side. The right-side probe needs to union results across branches for each left row — essentially a mini multi-branch lookup per join probe. This is supported (the components compose) but the implementation complexity is real.

10. **Incremental reactivity with JOINs**: When a row changes in the joined table (B), the window may need to re-evaluate. Unlike simple queries where a single row maps to one result, a JOIN can amplify — one B row change affects multiple joined tuples. The window maintenance logic must handle this fan-out.

## No-gos

1. **Histogram-based estimation** — we use exact cardinality from index counts, not histograms or sampled statistics. This is sufficient because Jazz auto-indexes every column.

2. **Index hints or query plan control** — no user-facing way to force or prevent the optimization. The planner decides automatically.

3. **Changes to the value encoding** — the current `encode_value` already produces lexicographically ordered bytes. No encoding migration.

4. **ORDER BY on cross-table expressions** — the streaming optimization requires ORDER BY to reference a column from a single table (which becomes the driver). Expressions spanning multiple tables fall back to the traditional path.

5. **Composite indexes** — multi-column ORDER BY uses prefix-scan + group-sort on the first column's single-column index, not a composite index. No new index types.

## Testing strategy

Integration tests at the QueryManager / RuntimeCore level, using realistic domain fixtures:

1. **Basic top-k**: Create 1000 messages for alice, query `ORDER BY created_at DESC LIMIT 10`. Assert only 10 rows returned, in correct order. Verify the optimization is used (not the Sort+LimitOffset fallback).

2. **Filtered top-k**: Messages across 3 channels. Query `WHERE channel_id = 'general' ORDER BY created_at DESC LIMIT 20`. Assert correct filtering + ordering.

3. **Policy interaction**: alice can read channel A, bob cannot. Both query `ORDER BY created_at DESC LIMIT 10`. Assert alice sees channel A messages, bob doesn't — and both get correct top-k results.

4. **Incremental updates**: Subscribe to `ORDER BY score DESC LIMIT 5`. Insert a row with score higher than the current 5th row. Assert the subscription emits a delta that adds the new row and evicts the old 5th.

5. **Incremental delete**: Delete the top-scoring row from a subscribed top-5 query. Assert the 6th row gets pulled in.

6. **Filter selectivity fallback**: Create a scenario where the filter rejects most rows. Verify the query still returns correct results (even if it falls back to the non-optimized path).

7. **Planner strategy selection**: Create scenarios where order-driven vs. filter-driven strategies should differ. Verify the planner picks the better strategy (or at least doesn't pick a catastrophically bad one).

8. **Multi-column ORDER BY**: `ORDER BY dept ASC, score DESC LIMIT 10` across 1000 employees. Assert correct two-level ordering. Test with high-cardinality first column (many groups, each small) and low-cardinality first column (few groups, each large).

9. **Multi-branch top-k**: Same table across 2 branches with different data. `ORDER BY created_at DESC LIMIT 10`. Assert correct merge-sort ordering with deduplication.

10. **JOIN + ordered top-k**: `SELECT * FROM posts JOIN users ON posts.author_id = users.id ORDER BY posts.created_at DESC LIMIT 10`. Assert correct join + ordering. Test with LEFT JOIN (NULL right side rows).

11. **Fallback to traditional path**: ORDER BY on joined table's column, non-indexed ORDER BY column — should fall back to Sort+LimitOffset and still produce correct results.

12. **Edge cases**: Empty table, limit > row count, offset beyond result set, all rows filtered by policy.

13. **Pagination correctness**: Fetch page 1 (LIMIT 20 OFFSET 0), page 2 (LIMIT 20 OFFSET 20). Assert no duplicates and no gaps across pages even when rows are inserted between page fetches.
