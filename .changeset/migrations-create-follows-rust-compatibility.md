---
"jazz-tools": patch
---

`migrations create` now decides whether a schema change needs a migration with the same rule a deployment uses (Rust's `schemasAreCompatible`, exported from jazz-napi). Adding enum variants, changing `branchBy` or changing the default of a `branchBy` column now writes a migration with no steps instead of only a snapshot; before, deploying such a change over a store on the earlier version failed with "disconnected roots".
