# On Delete Cascade — TODO

Cascading deletes for foreign key relationships, built on the existing soft/hard delete system.

## Mechanism

Uses the two-phase delete system already in place (see `../status-quo/query_manager.md`):

**Soft delete cascade:**

1. Parent row is soft-deleted (content preserved, `delete: soft` metadata, moved to `_id_deleted` index).
2. Force-traverse all FK references with `ON DELETE CASCADE` — find child rows that reference this parent.
3. Apply soft delete to each child (potentially waiting for data to load from storage/sync).
4. Soft-deleted children can be restored if the parent is restored (undelete).

**Hard delete cascade:**

1. Parent row is hard-deleted (content truncated, `delete: hard` metadata).
2. Same FK traversal — find all children.
3. Hard delete (truncate) each child.
4. Irreversible. Hard delete always wins in conflict resolution.

## Reference-Counted Cascade

When multiple parents reference the same child (e.g., content-addressed file parts shared across files, or any shared FK target), cascade must be refcount-aware:

**Soft delete:** Only soft-delete a child when ALL live references to it are soft-deleted. If file A and file B both reference part P, deleting file A alone must not soft-delete P — file B still needs it.

**Hard delete:** Only hard-delete a child when ALL references (including soft-deleted ones) are hard-deleted. A soft-deleted reference still "holds" the child against hard deletion, because soft delete is reversible.

**Undelete:** Restoring a soft-deleted parent must re-increment the live refcount of its children, potentially un-soft-deleting children that had reached refcount 0.

### Distributed Reference Counting

Refcounting is hard in distributed systems because peers have partial views:

- Peer A deletes file X (refs part P). Locally, P's refcount drops to 0 → A soft-deletes P.
- But Peer B has file Y (also refs P) that A hasn't synced yet.
- A just soft-deleted data B still needs.

**Approach: soft delete is eager/optimistic, hard delete is authoritative.**

- **Soft delete cascade** — each peer cascades based on its local refcount. If a peer's local refcount hits 0, it soft-deletes the child. When sync brings in the missing reference (file Y), the child is un-soft-deleted. This is safe because soft delete is reversible. Temporary unavailability of a shared part is the worst case.

- **Hard delete cascade** — must only happen on an authority (server) that has a global view of all references. A peer must never hard-delete a shared child based on local refcount alone. This ties into global transactions → see `../b_launch/globally_consistent_transactions.md`.

### Implementation Notes

Reference counting doesn't require an explicit counter column. The refcount is derived from scanning FK columns across all tables that reference the target table. This is an index lookup per referencing table. For content-addressed file parts, the scan checks all `uuid[]` columns that reference `file_parts`.

Whether to maintain a materialized refcount (for performance) or compute it on demand (for simplicity) is an implementation choice.

## Cascade Variants

- `ON DELETE CASCADE` — delete children when parent is deleted (refcount-aware).
- `ON DELETE SET NULL` — set FK column to null when parent is deleted (future, if needed).
- `ON DELETE RESTRICT` — prevent parent deletion if children exist (requires transaction support).

## Distributed Considerations

**Concurrent edit vs delete:** Peer A deletes parent, Peer B updates child — the delete wins because:

- Soft delete: child gets soft-deleted during cascade traversal. If the delete is reversed (undelete), B's update is preserved.
- Hard delete: child gets truncated. Hard delete is authoritative and always wins.

**Data loading:** Cascade traversal may need to wait for referenced data to be loaded (child rows might not be local yet). This means cascade is not purely synchronous — it may depend on sync completing.

**Multi-table transactions:** A cascade spanning multiple tables (e.g., delete todo → delete file → delete file_parts) is implicitly a multi-table operation. For atomicity, this should eventually use global transactions → see `../b_launch/globally_consistent_transactions.md`.

## Open Questions

- Cascade depth limits? (Prevent runaway chains: A → B → C → D → ...)
- Should cascade evaluation happen at write time or at merge/sync time?
- How to handle cascade when child rows haven't been synced yet? (Queue the cascade intent, or let sync trigger it?)
- Materialized refcount vs computed-on-demand: performance tradeoff for tables with many references.
