# INV-LARGE-12

- Status: now
- Coverage: ✓

## Invariant

Lifecycle metadata keys MUST select one canonical record: a staged receipt or pending upload's embedded opaque ID MUST exactly match its metadata key before recovery, acceptance, expiry, refcount, or reclaim mutation. Locator collisions are fail-closed immutable-mapping errors, never aliases or silent rewrites.

## Enforced by (tests)

`groove::db::tests::receipt_and_upload_key_identity_corruption_survives_reopen_without_mutation`; `groove::db::tests::batches::reclaim_rejects_mismatched_queue_identity_without_mutation_after_reopen`; `groove::chunks::tests::concurrent_remote_resolution_never_overwrites_an_immutable_locator`

## Implementation

`groove/src/db/{mod.rs,facade.rs,commit.rs}`; `groove/src/chunks.rs::ManagedChunkStorage::stage`
