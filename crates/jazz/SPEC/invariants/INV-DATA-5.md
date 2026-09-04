# INV-DATA-5

- Status: now
- Coverage: ✓

## Invariant

A `TxId` MUST identify a transaction as `(time: TxTime, node: NodeUuid)`; stored transaction rows MUST use primary key `(time, node_id)` where `node_id` is the local alias for the wire `NodeUuid`.

## Enforced by (tests)

`jazz::schema::tests::storage_lowering_declares_system_columns_by_shape`

## Implementation

`tx.rs::TxId`; `schema.rs::transactions_table`; `node/mod.rs::version_tx_id`
