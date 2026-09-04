# INV-TX-3

- Status: now
- Coverage: ✓

## Invariant

A commit unit whose `Transaction.n_total_writes` does not equal the delivered version count MUST be rejected by the fate authority as `RejectionReason::MalformedCommit(...)` and MUST NOT ingest version rows.

## Enforced by (tests)

`jazz::node::tests::sync::malformed_commit_unit_rejects_write_count_mismatch`

## Implementation

`jazz/src/node/ingest.rs::commit_unit_write_count_matches`; `jazz/src/node/ingest.rs::NodeState::ingest_commit_unit_once`
