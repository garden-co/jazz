# INV-API-29

- Status: now
- Coverage: ✓

## Invariant

A `Db` is a client: facade writes MUST keep `permission_subject == made_by`, and a `Db` MUST reject any attempt to attribute a write to another author. Cross-author attribution is a node-level concern on the ingest side (a trusted serving `Node`, `INV-RLS-18`, ch. 9), never a `Db` capability.

## Enforced by (tests)

`jazz::db::tests::client_attributed_insert_to_different_user_is_rejected`; `jazz::db::tests::default_insert_keeps_subject_and_made_by_equal`

## Implementation

`jazz/src/db.rs::Db::insert_attributed`; `jazz/src/db.rs::Db::write_mergeable_as_session_subject`; `jazz/src/db.rs::Db::check_attribution_allowed`; `jazz/src/node/mod.rs::MergeableCommit::permission_subject`
