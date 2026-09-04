# INV-HIST-14

- Status: now
- Coverage: ✓

## Invariant

Rejected transactions MUST NOT appear as accepted row-history entries and MUST NOT participate in currentness/domination.

## Enforced by (tests)

`jazz::node::tests::recovery::persisted_currency_tables_match_history_rows_after_reopen`

## Implementation

`jazz/src/node/ingest.rs::ingest_rejected_transaction`; `jazz/src/node/ingest.rs::ingest_known_transaction`
