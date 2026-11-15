---
"jazz-tools": patch
---

**Rust: read options.** `jazz.all_with_options(query, ReadOptions)` and `subscribe_with_options(query, ReadOptions)` cover TS's query options:

- `ReadOptions::tier(…)`;
- `wait_for_remote(Duration)` (TS's `firstLoadRemoteWaitMs`);
- `splices(column)` (TS's `splices`), so a subscription's `Updated` carries a column's changes as `splices`.

`ReadOptions::wait_for_remote` is for local-first reads. Combined with `tier(Remote)` it is refused, and so is a `Remote` read on an app with no server, instead of hanging. **Breaking:** `Updated` gains a `splices` field and is now `#[non_exhaustive]`, so match it with `..`.
