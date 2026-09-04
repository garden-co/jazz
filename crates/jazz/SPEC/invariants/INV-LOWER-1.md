# INV-LOWER-1

- Status: now
- Coverage: ✓

## Invariant

Fixed system storage plus mapping-derived physical lineage tables MUST be lowered into a `groove::schema::DatabaseSchema` before full node open.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::publishing_schema_registers_new_tables_without_storage_reopen`

## Implementation

`jazz/src/schema.rs::JazzSchema::lower_to_groove`; `jazz/src/node/mod.rs::NodeState::open_full_database`
