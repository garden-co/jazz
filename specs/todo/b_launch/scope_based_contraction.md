# Scope-Based Contraction — TODO (Launch)

When a query is removed and an object falls out of scope, the client keeps what it received ("no unsend"). There's no mechanism to inform the client that certain objects are no longer being tracked. Useful for client-side GC.

Related: `../a_mvp/client_state_cleanup.md`

> `crates/groove/src/sync_manager.rs:1415` (query removal)
