# Scope-Based Contraction — TODO (Launch)

When a query is removed and a row falls out of scope, the client keeps what it received ("no unsend"). There's no mechanism to inform the client that certain rows are no longer being tracked. Useful for client-side GC.

Related runtime issue: `stale-client-cache-after-scope-removal`

Current scope-tracking lives in:

- `crates/jazz-tools/src/sync_manager/mod.rs`
- `crates/jazz-tools/src/sync_manager/inbox.rs`
- `crates/jazz-tools/src/sync_manager/types.rs`
