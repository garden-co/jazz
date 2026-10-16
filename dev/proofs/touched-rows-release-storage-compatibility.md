# Releasing touched-row lists at settlement

Compatibility write-up for deleting `jazz_tx_touched_rows` records once their
transaction settles, required by the AGENTS.md "Durable encodings" rule.
Target release: `2.0.0-alpha.60`. SPEC 2 §2.8 ("Touched-row list") has the
normative rule.

## Old form

Every transaction kept its `jazz_tx_touched_rows` record for as long as the
node kept the transaction: 97 bytes for a one-row transaction (42-byte key,
55-byte value), on Core, on the writer and on every reader that stored the
transaction. Only rejection deleted it.

## New form

The record layout, its codec family and every other table are unchanged. What
changes is how long the record lives. The node batch whose staged
`jazz_transactions` record shows a complete mergeable transaction accepted at
a seq (`kind = Mergeable`, `fate = Accepted`, `global_time` set, view-scoped
flag clear) deletes the transaction's list, provided every listed row is on
the default branch. Exclusive transactions, view-scoped transactions and
transactions with a row on another branch keep their list as before.

After the release, nothing lists the transaction's versions. Readers that
need one of its rows already know it and read the exact history key
`(default branch, row, seq, tx_time, tx_node_id)`:

- maintained-view materialization reads the result member's row;
- relation terminals and maintained witnesses read the row in the candidate
  physical tables;
- current-row receipts read the winner's row;
- duplicate or late copies (commit units, relay units, view bundles) are
  compared row for row at the copy's coordinates; the stored record's write
  count is already part of the payload comparison, so a copy with an extra or
  missing row still conflicts.

Listing a released transaction is a typed error
(`TransactionVersionsReleased`), never an empty list. An outbox upload or a
local replay that races a settlement treats it as already settled.

## Upgrade path

None needed. A store written before this change has lists for its settled
transactions; they are read exactly as before and stay until a later batch
re-settles the transaction. No reader requires a list to be absent. A build
from before this change opening a store written after it would find no list
for settled transactions and list no versions for them; alpha.60's storage
version bump (track F) refuses such stores in older builds.

## Measured size

`crates/jazz/tests/storage_per_row_bytes.rs` (1000 todos, each inserted and
then updated in its own transaction, so 2001 transactions; raw key plus value
bytes on RocksDB), main `f1b7ac03` against this change:

| bytes per row | before | after | change |
| ------------- | -----: | ----: | -----: |
| Core          |   1259 |  1065 |   −194 |
| writer client |   1260 |  1065 |   −195 |
| reader client |    771 |   673 |    −98 |

The only table that changes is `jazz_tx_touched_rows`: 2001 records (194 B
per row) on Core and the writer and 1001 records (97 B per row) on the reader
before, none after. Per transaction, metadata drops from 215 to 118 bytes.
