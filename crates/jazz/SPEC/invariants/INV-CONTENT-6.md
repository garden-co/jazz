# INV-CONTENT-6

- Status: now
- Coverage: ✓

## Invariant

The private chunk backend MUST remain policy blind and replaceable behind exact immutable locator operations.

## Enforced by (tests)

`jazz::node::tests::harness::synced_descriptor_reads_through_shared_opaque_chunk_backend`; `groove::chunks::tests::managed_storage_keeps_integrity_out_of_the_byte_kv_backend`

## Implementation

`groove/src/chunks.rs::{ChunkKvStorage,ManagedChunkStorage}`; `jazz/src/node/state/lifecycle.rs::set_chunk_storage`
