# INV-BVIEW-14

- Status: now
- Coverage: ✓

## Invariant

A view-relative mutation of an inherited row MUST copy-on-write into the head branch key. An exact mutation MUST name its branch key explicitly.

## Enforced by (tests)

`jazz::tests::branch_views::db_exact_mutations_and_branch_view_reads_compose_head_over_base`

## Implementation

`db/mutations.rs::Db::update_in_branch_view`; `db/mutations.rs::Db::update_in_branch`
