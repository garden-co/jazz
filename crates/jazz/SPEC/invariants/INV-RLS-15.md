# INV-RLS-15

- Status: now
- Coverage: ✓

## Invariant

A table with no declared policy clauses MUST be public for reads and for writes by non-anonymous permission subjects. An effective permission subject under the reserved `urn:jazz:anonymous` issuer MUST be denied inserts, updates, and deletes before table-policy evaluation. Once any clause is declared for that table, its policy set is closed: a missing read, insert, delete, or whole update operation MUST deny; a declared update MAY omit either its old-row or new-row subclause. This decision MUST use the policy-owning schema across schema versions and lens projections.

## Enforced by (tests)

`jazz::node::tests::harness::policy_free_table_is_open_for_reads_and_writes`; `jazz::node::tests::harness::partial_policy_set_allows_its_declared_read_and_denies_omitted_writes_at_authority`; `jazz::tests::edge_fate_authority::core_authority_rejects_omitted_insert_after_read_policy_closes_table`; `jazz::db::tests::mutations::anonymous_authority_exclusive_write_is_rejected_before_policy_evaluation`

## Implementation

`jazz/src/schema.rs::TableSchema::has_any_policy`; `jazz/src/node/policy.rs::NodeState::write_policy_allows_version_record`; `jazz/src/node/query_eval/authorization.rs::NodeState::table_read_policy_authorization_request`; `jazz/src/node/ingest/fates.rs::NodeState::commit_unit_satisfies_write_policies`; `jazz/src/ids.rs::AuthorSubject::is_anonymous`
