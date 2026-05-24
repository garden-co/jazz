# mini-jazz-sqlite benchmarks

Run:

```sh
cargo bench -p mini-jazz-sqlite --bench sqlite_shapes
```

The first benchmark pass was run on 2026-05-24 with native `rusqlite` using
bundled SQLite. The benchmark uses in-memory SQLite databases so it measures
query shape and SQLite execution cost, not filesystem durability.

## 2026-05-24 first pass

Seed shapes:

- current projection: 100k `todos` rows on `main`
- branch snapshot: 100k base rows, 1k branches, 100 sparse overrides per branch
- page size: 50 rows

Results:

| benchmark                                           | time             | note                                      |
| --------------------------------------------------- | ---------------- | ----------------------------------------- |
| current projection, user + system filter, limit 50  | 18.927-18.981 us | `done` + `$createdAt`, ordered by system  |
| current projection plus JSON result scope, limit 50 | 31.151-31.280 us | same query with `$resultScopeJson` output |
| branch snapshot from raw history with window        | 104.18-104.72 ms | reconstructs visible row per `$rowId`     |
| branch snapshot from raw history with `NOT EXISTS`  | 115.28-117.07 ms | alternate raw-history query shape         |
| sparse branch overlay, limit 50                     | 68.680-68.913 us | shared base-current + sparse branch delta |
| seed snapshot dataset                               | 638.23-648.57 ms | includes base, history, branch deltas     |

Initial read:

- Plain current-projection reads look comfortably fast.
- JSON scope output roughly adds tens of microseconds at page size 50; worth
  comparing against a temp-table/side-result representation later.
- Reconstructing a branch snapshot directly from raw history is too slow as the
  default read path at 100k rows.
- A sparse branch overlay is much more promising for high branch counts without
  maintaining full projection tables per branch.

## 2026-05-24 raw-history index variant

Added a history index shaped like the query:

```sql
CREATE INDEX todos_history_done_created_tx_row
  ON todos__schema_v1_history(done, "$createdAt" DESC, "$txId", "$rowId");
```

Results:

| benchmark                                               | time             | note                                       |
| ------------------------------------------------------- | ---------------- | ------------------------------------------ |
| current projection, user + system filter, limit 50      | 18.688-18.734 us | stable baseline                            |
| current projection plus JSON result scope, limit 50     | 30.905-31.028 us | stable baseline                            |
| branch snapshot from raw history with window            | 102.38-103.04 ms | still reconstructs all latest row versions |
| branch snapshot `NOT EXISTS` with added history indexes | 40.622-41.265 ms | much better, still too slow for hot reads  |
| branch snapshot candidate index                         | 37.604-39.242 ms | scans query-shaped history index           |
| branch snapshot candidate index with overfetch          | 38.135-39.105 ms | limits candidates before stale check       |
| sparse branch overlay, limit 50                         | 67.534-70.909 us | unchanged order-of-magnitude winner        |
| seed snapshot dataset                                   | 732.58-741.67 ms | extra history index increases write cost   |

Initial read:

- Query-shaped history indexes improve raw snapshot reads by roughly 3x.
- They do not get close to serving-index performance for hot app reads.
- The extra historical index also increases write/seed cost.
- Raw-history query-only reads may be reasonable for cold/time-travel/admin
  paths, but hot branch subscriptions probably want at least a sparse overlay or
  shared snapshot projection.
