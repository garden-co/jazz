---
"jazz-tools": patch
---

`migrations create` now decides whether a schema change needs a migration with the same rule a deployment uses (Rust's `schemasAreCompatible`, exported from jazz-napi). Adding enum variants or adding `branchBy` to a table now writes a migration with no steps instead of only a snapshot; before, deploying such a change over a store on the earlier version failed with "disconnected roots". A project that already recorded such a change as a snapshot only, before this release, still has no migration between those two snapshots: `migrations create` diffs against the latest snapshot only, so it won't write that one.
