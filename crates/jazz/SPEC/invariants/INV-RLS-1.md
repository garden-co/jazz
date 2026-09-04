# INV-RLS-1

- Status: now
- Coverage: ✓

## Invariant

A non-system commit unit MUST be rejected with `Fate::Rejected(RejectionReason::AuthorizationDenied)` and MUST NOT ingest accepted version rows when any version in the unit fails its table write policy evaluated against the effective permission subject. `Transaction.made_by` remains provenance and differs from that subject only through a trusted serving-node path (`INV-RLS-17`, `INV-RLS-18`).

## Enforced by (tests)

jazz::node::tests::policies_rls::write_policy_rejection_cleans_up_client

## Implementation

jazz/src/node/ingest.rs::NodeState::commit_unit_satisfies_write_policies; jazz/src/node/ingest.rs::NodeState::ingest_commit_unit; jazz/src/node/policy.rs::NodeState::write_policy_allows_version_record
