---
"jazz-tools": patch
---

**Rust: read options.** `jazz.all_with_options(query, ReadOptions)` and `subscribe_with_options(query, ReadOptions)` cover TS's query options:

- `ReadOptions::tier(…)`;
- `wait_for_remote(Duration)` (TS's `firstLoadRemoteWaitMs`);
- `splices(column)` (TS's `splices`), so a subscription's `Updated` carries a column's changes as `splices`.
