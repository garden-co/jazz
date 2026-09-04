# INV-CONTENT-7

- Status: now
- Coverage: ✓

## Invariant

Chunks MUST remain outside canonical Jazz sync facts, while settlement MUST wait for exact Groove terminal quiescence over required chunks.

## Enforced by (tests)

`jazz::node::tests::harness::pushed_chunks_must_be_staged_before_the_referencing_authority_commit`; `wire_fixtures::wire_message_frame_fixtures_decode_to_expected_messages`

## Implementation

`jazz/src/protocol.rs::{ChunkRequestBatch,ChunkResponseBatch,ChunkUploadStart,ChunkUploadNodes}`; `jazz/src/db.rs::ChunkPump`
