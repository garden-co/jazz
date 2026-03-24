# Scopes

```
 ┌──────────────────────────────────────────────────────┐
 │ Correctness Baseline                                │
 │ (no dependencies — tests against existing pipeline) │
 └────────────────────────┬─────────────────────────────┘
                          │
                          ▼
 ┌──────────┐
 │ Storage  │
 └────┬─────┘
      │
      ▼
 ┌──────────────────┐
 │ Streaming        │  (needs Storage)
 │ Protocol         │
 └────┬─────────────┘
      │
      ▼
 ┌──────────────────┐
 │ Planner: Top-K   │  (needs Streaming Protocol)
 │ Recognition      │
 └────┬─────────────┘
      │
      ├──────────────┬──────────────┬──────────────┐
      ▼              ▼              ▼              ▼
 ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐
 │Multi-col  │ │Multi-     │ │ JOINs     │ │ Cursor    │
 │ORDER BY   │ │branch     │ │           │ │ Pagination│
 └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
       │  (each needs Planner +                    │
       │   Streaming Protocol)                     │
       ▼              ▼             ▼              ▼
 ┌──────────────────────────────────────────────────────┐
 │ Incremental Reactivity                              │
 │ (needs all streaming scopes above)                  │
 └──────────────────────────────────────────────────────┘

 All baseline tests must keep passing as each scope lands.
 Streaming-specific assertions are added within each scope.
```

## Correctness Baseline — tests against the existing pipeline

**No dependencies — start here**

These tests run against the current Sort+LimitOffset path and define the expected behavior. They must keep passing unchanged as each optimization scope lands — the streaming path must produce identical results.

- [ ] Basic top-k: 1000 rows, ORDER BY DESC LIMIT 10, assert correct order and count
- [ ] Filtered top-k: WHERE equality + ORDER BY + LIMIT across multiple groups
- [ ] Policy interaction: different sessions see different top-k results based on permissions
- [ ] Pagination: page 1 then page 2 via OFFSET, no duplicates or gaps
- [ ] Edge cases: empty table, limit > row count, offset beyond results, all rows filtered
- [ ] Filter selectivity fallback: most rows rejected, verify correct results
- [ ] Multi-column ORDER BY: `ORDER BY dept ASC, score DESC LIMIT 10` end-to-end
- [ ] Multi-branch: 2 branches, ordered top-k, correct merge + dedup
- [ ] JOIN + ordered top-k: `posts JOIN users ORDER BY posts.created_at DESC LIMIT 10`
- [ ] LEFT JOIN with NULLs in top-k window
- [ ] Incremental: subscribe to top-5, insert/delete/update rows, assert correct deltas

## Storage: Ordered Index Scan — expose ordered iteration from storage backends

- [ ] Add `IndexScanDirection` enum and `index_scan_ordered` method to the `Storage` trait
- [ ] Include `skip_past: Option<(&Value, ObjectId)>` cursor parameter for resume-after semantics
- [ ] Implement `index_scan_ordered` for MemoryStorage (BTreeMap::range + .rev() + take + skip + cursor resume)
- [ ] Implement `index_scan_ordered` for OpfsBTreeStorage
- [ ] Implement `index_scan_ordered` for FjallStorage
- [ ] Add `index_count(table, column, branch, value) -> usize` to the `Storage` trait (exact match cardinality)
- [ ] Add `index_count_all(table, column, branch) -> usize` to the `Storage` trait (total index cardinality)
- [ ] Implement `index_count` and `index_count_all` for all three backends (MemoryStorage: O(1) via hash lookup + set size / running counter; OpfsBTree and Fjall: prefix key count)
- [ ] Unit tests: ordered scan ascending/descending, take/skip, cursor resume, empty index, single-value index, IEEE 754 ±0.0 handling
- [ ] Unit tests: index_count and index_count_all correctness after insert/remove sequences

## Streaming Protocol — pull-based execution mode for graph nodes

**Depends on: Storage**

- [ ] Define the streaming trait/interface: `pull_next() -> Option<Tuple>` or similar
- [ ] Add streaming mode to OrderedIndexScan node (yields one `(Value, ObjectId)` at a time from `index_scan_ordered`)
- [ ] Add streaming mode to MaterializeNode (pull from upstream, load single row, forward)
- [ ] Add streaming mode to PolicyFilterNode (pull from upstream, evaluate policy, forward or skip)
- [ ] Add streaming mode to FilterNode (pull from upstream, evaluate predicate, forward or skip)
- [ ] Add streaming mode to LimitOffsetNode (pull from upstream, handle offset skip + limit stop, signal done)
- [ ] Integration test: wire up a manual streaming pipeline end-to-end, verify it produces the same results as the batch pipeline

## Planner: Top-K Recognition — detect and activate the streaming path

**Depends on: Streaming Protocol**

- [ ] Recognition pass in `graph.rs`: detect single-column ORDER BY + indexed column + LIMIT + no JOINs + single branch
- [ ] When recognized, compile the streaming pipeline (OrderedIndexScan → Materialize → PolicyFilter → Filter → LimitOffset) instead of batch pipeline with Sort
- [ ] Order-driven vs. filter-driven strategy selection using exact cardinality from `index_count` / `index_count_all`
- [ ] Runtime fallback: if order-driven streaming scan reads > N × limit rows without filling result, abandon and fall back to batch path
- [ ] Fallback for unsupported patterns: ORDER BY on joined table's column, non-indexed first ORDER BY column → traditional Sort+LimitOffset
- [ ] Tests: planner picks streaming for eligible queries, falls back for ineligible ones

## Multi-column ORDER BY — prefix-scan + group-sort on first column's index

**Depends on: Planner, Streaming Protocol**

- [ ] Extend OrderedIndexScan to yield groups of rows with equal first-column values
- [ ] Buffer each group, sort by secondary ORDER BY columns within the group
- [ ] Emit sorted group rows through the streaming pipeline
- [ ] Per-group fallback threshold: if a single group exceeds N rows, fall back to batch path for correctness
- [ ] Planner recognition: detect multi-column ORDER BY where first column is indexed, activate prefix-scan + group-sort
- [ ] Tests: `ORDER BY dept ASC, score DESC LIMIT 10` — high-cardinality first column (small groups), low-cardinality first column (large groups), single-column fallback still works

## Multi-branch — k-way merge of per-branch ordered scans

**Depends on: Planner, Streaming Protocol**

- [ ] k-way merge node: accept multiple ordered streams (one per branch), interleave by order column value
- [ ] Deduplication of same ObjectId across branches (match existing UnionNode semantics)
- [ ] Planner recognition: detect multi-branch query with ordered top-k, wire up per-branch OrderedIndexScans → merge → streaming pipeline
- [ ] Tests: same table on 2 branches with different/overlapping data, `ORDER BY created_at DESC LIMIT 10`, assert correct merge ordering and dedup

## JOINs with ordered top-k — order-preserving index nested-loop join

**Depends on: Planner, Streaming Protocol**

- [ ] Order-preserving join node: the table owning the ORDER BY column drives the scan, probing the other table's join-column index for matches
- [ ] Emit joined tuples preserving driver's order through the streaming protocol
- [ ] Support driving from either side: `A JOIN B ORDER BY A.col` (drive A, probe B) and `A JOIN B ORDER BY B.col` (drive B, probe A)
- [ ] Handle LEFT JOIN driven from preserved side: `A LEFT JOIN B ORDER BY A.col` — emit (A, NULL) when no B match
- [ ] Handle LEFT JOIN driven from optional side: `A LEFT JOIN B ORDER BY B.col` — emit matched pairs in B's order, emit (A, NULL) group at start (ASC) or end (DESC) per NULL ordering
- [ ] Handle one-to-many: one driver row can produce multiple joined tuples — streaming protocol must handle fan-out
- [ ] Planner recognition: detect JOIN + ORDER BY on any joined table's column + LIMIT, make that table the driver
- [ ] Fallback: ORDER BY on cross-table expression → traditional Sort+LimitOffset
- [ ] Tests: `posts JOIN users ORDER BY posts.created_at DESC LIMIT 10`, reverse direction `ORDER BY users.name ASC LIMIT 10`, LEFT JOIN with NULLs from both directions, one-to-many fan-out

## Cursor-based Pagination — resume scans from where the previous page ended

**Depends on: Planner, Streaming Protocol, Storage**

- [ ] Streaming pipeline returns cursor `(value, row_id)` for the last emitted row alongside results
- [ ] LimitOffsetNode accepts an optional cursor — when present, passes `skip_past` to the OrderedIndexScan instead of using `skip`
- [ ] Planner wires cursor through the pipeline when provided by the caller
- [ ] Handle cursor invalidation: if cursor row was deleted, scan resumes from the next position (best-effort semantics)
- [ ] Handle cursor with JOINs: cursor is on the driver table's order column, inner table probes resume correctly
- [ ] Handle cursor with multi-branch: cursor applies per-branch, k-way merge resumes from the right position in each branch
- [ ] Tests: fetch page 1 with LIMIT 20, use returned cursor to fetch page 2, assert no duplicates/gaps
- [ ] Tests: cursor after row deletion (gap behavior), cursor after row insertion at boundary
- [ ] Tests: cursor-based pagination through JOINed and multi-branch queries

## Incremental Reactivity — bounded window maintenance for live subscriptions

**Depends on: all streaming scopes above**

- [ ] Maintain sorted buffer of current top-k window with order-column values
- [ ] Insert: detect if new row falls within window, insert at correct position, evict last if over limit
- [ ] Delete: detect if deleted row was in window, pull in next row from index to fill gap
- [ ] Update: treat as delete + insert
- [ ] Policy/filter invalidation: re-scan from index when dependencies change (overflow buffer for efficiency)
- [ ] JOIN reactivity: change in joined table (B) that affects window tuples — handle fan-out (one B row change can affect multiple joined tuples)
- [ ] Multi-branch reactivity: change on one branch updates the merged window correctly
- [ ] Tests: subscribe to top-5, insert row that enters window, delete row that was in window, update row that changes position, policy change that hides/reveals a row
- [ ] Tests: joined query subscription — insert/delete on inner table, verify window updates correctly

Note: Cursor pagination tests and batch/streaming equivalence assertions are added within their respective scopes (Cursor Pagination, Planner). The Correctness Baseline at the top covers all behavior that must remain stable.
