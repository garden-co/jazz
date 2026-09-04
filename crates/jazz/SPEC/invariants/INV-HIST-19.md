# INV-HIST-19

- Status: now
- Coverage: ✓

## Invariant

A retained node-local content-frontier helper MUST be keyed by `(PhysicalTableId, canonical BranchKey, RowUuid)` and encode strictly increasing, duplicate-free canonical `TxId`s as a Groove array of `(u64, uuid)` tuples; it MUST never use an opaque serialized collection or span the deletion layer.

## Enforced by (tests)

`jazz::node::tests::harness::merge_heads_key_two_nondefault_branches_independently_across_reopen`; `jazz::node::tests::harness::merge_heads_match_history_across_restart_between_concurrent_units`; `jazz::node::tests::harness::merge_heads_share_physical_identity_across_table_rename_and_restart`; `jazz::node::tests::harness::stored_merge_heads_require_a_canonical_transaction_id_array`

## Implementation

`jazz/src/node/ingest/view_updates.rs::{read_merge_heads,write_merge_heads}`
