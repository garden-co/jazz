# INV-TX-20

- Status: now
- Coverage: ✓

## Invariant

Exclusive write validation MUST be first-committer-wins: each written version's current global winner in that version's own content/deletion layer MUST equal the single recorded parent, or absence when no parent is recorded. This written-layer check is distinct from row/predicate read validation against observed visible content/deletion state (`INV-TX-16/17/18`). Thus after content `C`, a first delete `D` has no deletion parent and is accepted while the deletion register remains empty; a restore parents `D`, while content written from the deleted snapshot still parents `C`.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::{exclusive_write_write_first_committer_wins, exclusive_delete_compares_the_deletion_register_not_content, exclusive_replacement_and_restore_parent_their_own_registers}`; `jazz::node::tests::general::known_parent_must_match_exact_row_coordinate_and_layer`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::tx_write`; `jazz/src/node/global_state.rs::NodeState::visible_global_layer_tx_id_now`; `jazz/src/node/ingest/fates.rs::NodeState::validate_exclusive_commit_unit`
