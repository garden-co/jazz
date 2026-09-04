# INV-API-15

- Status: now
- Coverage: ✓

## Invariant

`WriteHandle::wait(tier)` MUST return the handle `TxId` only when the requested tier is locally satisfied, MUST return `ErrorCode::WriteRejected` for rejected fates, and MUST return `ErrorCode::NotObserved` when the requested tier is not locally observed. A `Global` wait additionally MUST require `Fate::Accepted` and an authority-assigned `GlobalTime`; a bare `Global` durability claim MUST NOT complete it.

## Enforced by (tests)

`jazz::db::tests::node_runtime::global_wait_requires_authority_timestamp_after_accepted_global_durability`; `jazz::db::tests::node_runtime::pending_global_state_does_not_complete_remote_wait_or_prune_upload`

## Implementation

`jazz/src/db.rs::transaction_satisfies_wait`; `jazz/src/db.rs::WriteHandle::wait`; `jazz/src/db.rs::WriteState`
