---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
---

Route ordinary and mergeable-transaction upserts through their complete head-over-base branch view. Head-local rows are merged, inherited rows are copied into the head, and absent rows are inserted there consistently across native and WASM runtimes. A row hidden by a head-local tombstone is rejected instead of accepting an invisible content write; callers must use the exact-branch restore API when revival is intended. Transactional and standalone root upserts now reject tombstones consistently as well.

This adds the public `WriteTarget::BranchView` enum variant without changing mutation method signatures or the default/root behavior. Existing calls remain source-compatible unless downstream Rust code exhaustively matches `WriteTarget`; exhaustive matches must add the new variant or an intentional wildcard.
