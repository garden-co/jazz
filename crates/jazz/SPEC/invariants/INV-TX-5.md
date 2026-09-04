# INV-TX-5

- Status: now
- Coverage: ✓

## Invariant

The authority MUST park a commit unit with missing parent/schema/content prerequisites and MUST decide it only after all prerequisites are present.

## Enforced by (tests)

`jazz::node::tests::sync::authority_unparks_child_after_unknown_parent_accepts`; `jazz::node::tests::sync::duplicate_unknown_parent_commit_unit_parks_once`

## Implementation

`jazz/src/node/ingest.rs::NodeState::park_commit_unit_if_missing_parents`; `jazz/src/node/ingest.rs::NodeState::park_commit_unit_if_missing_schema_versions`; `jazz/src/node/ingest.rs::NodeState::park_commit_unit_if_missing_content`; `jazz/src/node/ingest.rs::NodeState::drain_parked_commit_units`
