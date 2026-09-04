# INV-RLS-18

- Status: now
- Coverage: ✓

## Invariant

An uploaded commit unit MUST be authorized under the authenticated link identity: a `Session` link's `made_by` MUST equal that identity or be rejected, while a `TrustedBackend` link MAY attribute with `made_by != identity` and write policy evaluated against the link identity. This is the sync-ingest counterpart to facade attribution (`INV-RLS-17`).

## Enforced by (tests)

`jazz::db::tests::session_upload_rejects_forged_made_by_without_ingesting_rows`; `jazz::db::tests::session_upload_uses_connection_identity_for_write_policy`; `jazz::db::tests::trusted_backend_upload_uses_backend_policy_and_stores_user_made_by`

## Implementation

`jazz/src/db.rs::Node::accept_subscriber`; `jazz/src/db.rs::Node::accept_subscriber_with_trust`; `jazz/src/node/ingest.rs::CommitUnitIngestContext`; `jazz/src/node/ingest.rs::NodeState::commit_unit_satisfies_write_policies`
