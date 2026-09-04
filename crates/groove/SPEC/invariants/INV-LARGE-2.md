# INV-LARGE-2

- Status: now
- Coverage: ✓

## Invariant

Every IVM node MAY await storage and chunk requests through one owned evaluation session; missing chunks MUST NOT escape as a host retry protocol.

## Enforced by (tests)

`large_value_query::{query_future_stays_pending_while_required_chunks_are_paused,public_consolidation_future_keeps_chunk_suspension_inside_groove}`; `groove::ivm::runtime::evaluation_session::tests::equal_chunk_requests_share_one_retained_future`

## Implementation

`groove/src/ivm/runtime/evaluation_session.rs`; `groove/src/chunks.rs::OwnedChunkProvider`
