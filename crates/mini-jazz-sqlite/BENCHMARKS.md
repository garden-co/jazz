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
