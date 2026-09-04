# INV-LARGE-10

- Status: now
- Coverage: ✓

## Invariant

Large-value finalization MUST independently validate the canonical descriptor, complete authenticated reachability, and final logical kind/content, then atomically publish the one receipt durably bound to the pending journal, release its upload retainers, and install an upload-id → receipt idempotency binding. A retry after durable promotion MUST return that exact descriptor-bound receipt; its metadata key and embedded receipt id MUST agree. The binding lives exactly as long as the unaccepted receipt and is removed atomically by accepted-row consumption or explicit TTL eviction; TTL protects only abandoned staging and never repairs partial promotion. Raw staging order or accounting MUST NOT authorize a malformed or unrelated descriptor; a rejected finalizer leaves no publishable receipt.

## Enforced by (tests)

`groove::db::tests::{raw_finalization_rejects_dishonest_or_unrelated_descriptors_and_survives_reopen,finalized_upload_promotion_is_atomic_and_retry_returns_its_one_receipt,completed_upload_retry_rejects_substituted_receipt_id_after_reopen}`

## Implementation

`groove/src/db/facade.rs::Database::finalize_large_value_upload`; `groove/src/large_values.rs::validate_finalized_upload`
