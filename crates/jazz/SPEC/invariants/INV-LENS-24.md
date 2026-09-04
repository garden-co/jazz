# INV-LENS-24

- Status: now
- Coverage: ✓

## Invariant

A global physical UUID is permanently issued across the whole durable catalogue lineage. Admission, pending/staged replay, snapshot installation, reopen, and authority allocation MUST reject or avoid reusing any retired table, column epoch, or recursive enum-occurrence UUID; only an exact compatible source-to-target coordinate may retain its UUID.

## Enforced by (tests)

`jazz::protocol::tests::physical_identity_history_retires_table_column_and_nested_enum_across_multiple_hops`; `jazz::node::tests::harness::global_identity_retirement_rejects_multihop_reuse_live_and_after_reopen`

## Implementation

`jazz/src/{protocol.rs,node/{catalogue_ingest.rs,ingest/catalogue.rs,state/{catalogue.rs,lifecycle.rs}}}`
