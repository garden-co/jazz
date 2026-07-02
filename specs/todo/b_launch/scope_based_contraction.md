# Scope-Based Contraction — TODO (Launch)

When a query is removed and a row falls out of scope, the client keeps what it
received ("no unsend"). This is spec-conformant: revocation is forward-looking
sync narrowing, not post-delivery redaction (`INV-RLS-6`). A future scope
contraction mechanism would be useful for client-side GC and cache pressure, not
for correctness of settled subscription/query results.

Legacy alpha scope-tracking lived in:

- `crates/jazz-tools/src/sync_manager/mod.rs`
- `crates/jazz-tools/src/sync_manager/inbox.rs`
- `crates/jazz-tools/src/sync_manager/types.rs`
