# INV-RLS-20

- Status: now
- Coverage: ✓

## Invariant

Reads performed to execute a write MUST satisfy the target row's read policy. Every session-authored update, including full-row replacements, partial patches, transactions, and branch views, MUST be rejected unless the session can read the target; a hidden target must fail without disclosing its cells, existence, or policy reason. Upsert MUST require target-row read permission when a current target exists; a row-policy table MUST deny an unseen target rather than infer absence, while a table with no read policy may insert an absent target. Trusted/internal paths may inspect hidden authoritative evidence for policy evaluation but that privilege must not broaden session mutation eligibility. A row-id delete reads no user data; causal parents and deletion checks remain internal bookkeeping.

## Enforced by (tests)

`jazz::tests::authorization_scope_reentry::write_only_updates_and_upserts_are_denied_without_disclosing_the_target`; `jazz::db::tests::mutations::session_branch_updates_require_read_visibility_before_staging`; `jazz::db::tests::transactions::exclusive_session_mutations_deny_hidden_existing_targets_without_disclosure`; `jazz::db::tests::transactions::exclusive_session_absent_upsert_records_absence_and_observes_its_overlay`; `jazz::db::tests::transactions::exclusive_session_update_authorizes_snapshot_then_conflicts_on_toctou_change`; `jazz::tests::authorization_scope_reentry::maintained_authorization_restores_an_ordered_page_after_scope_reentry`

## Implementation

`jazz/src/db.rs::Db::merge_existing_cells_for_identity`; `jazz/src/db.rs::Db::upsert_target_for_identity`; `jazz/src/db.rs::Db::row_layer_parents`; `jazz/src/db/transactions.rs::Db::require_mergeable_transaction_read_visibility`; `jazz/src/db/transactions.rs::Db::exclusive_transaction_target_for_write`; `jazz/src/node/query_eval/authorization.rs::NodeState::read_policy_query_allows_open_tx_row`
