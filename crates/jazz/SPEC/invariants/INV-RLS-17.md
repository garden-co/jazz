# INV-RLS-17

- Status: now
- Coverage: ✓

## Invariant

A write whose `Transaction.made_by` differs from the authenticated permission subject MUST be accepted only via a trusted serving node (a core/edge `Node` accepting a `TrustedBackend` link, ch. 9), never from a client `Db`; the write policy is evaluated against the permission subject, while `made_by` remains provenance metadata.

## Enforced by (tests)

`jazz::db::tests::core_attributed_insert_uses_core_identity_for_policy_and_user_for_made_by`; `jazz::db::tests::client_attributed_insert_to_different_user_is_rejected`

## Implementation

`jazz/src/db.rs::Db::check_attribution_allowed`; `jazz/src/node/mod.rs::MergeableCommit::effective_permission_subject`; `jazz/src/node/ingest.rs::NodeState::commit_unit_satisfies_write_policies`
