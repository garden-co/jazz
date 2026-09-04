# INV-API-14

- Status: now
- Coverage: ✓

## Invariant

A local write on a `Db` MUST be `DurabilityTier::Local` and queued for upstream upload; a `Db` (always a client) MUST NOT self-finalize. Self-finalization to `Accepted`/`Global` is a core `Node` behavior (ch. 9).

## Enforced by (tests)

`jazz::db::tests::db_sync_surface_uploads_client_writes_for_authority_fate`; `jazz::db::tests::core_db_self_finalizes_own_writes_to_global`

## Implementation

`jazz/src/db.rs::Db::finalize_local_commit`; `jazz/src/node/ingest.rs::NodeState::finalize_local_mergeable_commit`
