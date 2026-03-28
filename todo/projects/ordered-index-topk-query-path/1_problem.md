# Problem: Paginated ordered queries materialize everything

## What's broken

Every `ORDER BY ... LIMIT` query currently follows the same pipeline:

```
IndexScan (all matching rows)
  -> Materialize (load every row from storage)
    -> Sort (in-memory, full result set)
      -> LimitOffset (window down to k rows)
```

Even `ORDER BY score DESC LIMIT 1` on a 5 000-row table loads, deserializes, and sorts all 5 000 rows before returning one. The work is **O(n log n) time + O(n) memory** regardless of how small `k` is.

This also means there's no efficient "next page" operation — fetching page 2 re-sorts the entire result set and throws away the first page again.

## Who is affected

- **App authors using Jazz** — anyone writing a feed, leaderboard, inbox, activity log, or any list UI with pagination. These are the most common query patterns in real apps.
- **End users of Jazz-powered apps** — they experience unnecessary latency and memory pressure on the client device, especially on mobile/low-memory browsers running WASM.

## Concrete examples

1. **Chat app**: `SELECT * FROM messages WHERE channel_id = ? ORDER BY created_at DESC LIMIT 50`. A channel with 10 000 messages loads all 10 000 into memory to show the latest 50.

2. **Task board**: `SELECT * FROM tasks WHERE project_id = ? ORDER BY priority ASC LIMIT 20`. Even a modest project with 500 tasks pays the full sort cost for a single screen of results.

3. **Infinite scroll**: User scrolls to page 3 (`OFFSET 40 LIMIT 20`). The entire result set is re-materialized and re-sorted from scratch, with no way to resume from where page 2 left off.

## What should happen instead

The query engine should recognize when an ordered index can satisfy the `ORDER BY` clause and scan the index in order, stopping after `k` results. Materialization should only happen for the rows that will actually be returned.

This must work **incrementally** — the reactive query pipeline (change → delta propagation) must stay intact. The optimization isn't just for cold-start; live subscriptions on paginated queries need to stay efficient too.

The optimization must compose with the full filter pipeline, including policies (permission predicates). A typical real-world query combines an equality filter, a policy check, and an ordered limit — all three must work together without falling back to full materialization.

Target: **O(k log n) time + O(k) memory** for top-k queries on indexed columns.

## Open questions

- **Multi-column ORDER BY**: In theory needed (`ORDER BY dept ASC, score DESC`), but complexity may push it out of the first version. Decide during pitch.
