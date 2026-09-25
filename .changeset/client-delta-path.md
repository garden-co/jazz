---
"jazz-tools": patch
---

Make subscription deltas cheaper in the JS client: large removals in one delta, long-running windows (the per-delta cost no longer grows over time), subscriptions with `include(...)`, first-load result keys and native row decoding. `decodeNativeRow(...).valuesByColumn.get(name)` now returns the same object as the matching entry in `values`.
