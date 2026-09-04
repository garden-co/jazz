# INV-TX-4

- Status: now
- Coverage: ✓

## Invariant

Duplicate commit units with identical payloads MUST be idempotent and return the already-known fate; duplicate units with conflicting payloads MUST fail as `Error::ConflictingCommitUnit`.

## Enforced by (tests)

`jazz::node::tests::sync::commit_units_sync_upstream_and_fates_flow_back`; `jazz::node::tests::sync::duplicate_commit_units_must_match_original_payload`

## Implementation

`jazz/src/node/ingest.rs::NodeState::ingest_commit_unit_once`; `jazz/src/node/ingest.rs::NodeState::ingest_known_transaction`
