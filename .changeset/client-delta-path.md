---
"jazz-tools": patch
---

Make subscription deltas cheaper in the JS client: large removals in one delta, long-running windows (the per-delta cost no longer grows over time), subscriptions with `include(...)`, first-load result keys and native row decoding. Each delta also records an undo log instead of snapshotting the whole result, and the ordered-id index stays lazy during runs of sequential changes; the deltas delivered to listeners are unchanged. `decodeNativeRow(...).valuesByColumn.get(name)` now returns the same object as the matching entry in `values`.
