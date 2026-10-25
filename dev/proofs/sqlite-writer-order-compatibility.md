# SQLite accepted writer order

An authority must reject a late write when it has already accepted a newer
write from the same original writer to the same row (`INV-HIST-23`). Checking
only the row's latest image is insufficient: another writer may have changed
it since then, and a restart must preserve the earlier decision.

The authority records the greatest accepted original transaction for each
physical table, row, branch and writer. Each check reads one exact primary key.
Work depends on the rows the unit changes; it does not scan history or unrelated
writes. Replica previews do not advance this record.

The check runs after normal permission validation, inside the write's SQLite
savepoint. A late unit rolls back every row and every frontier update. Only a
successful unit records acceptance and publishes notifications. Direct local
authority writes use the same record. Duplicate uploads retain their recorded
fate and never apply their operations again.

For example, Alice writes an older unit affecting notes A and B, then a newer
unit affecting A. If the authority receives the newer unit first, it must refuse
the older unit completely: B stays absent. Bob may then change A, or the
authority may restart; neither event permits Alice's older unit to overwrite A.

## Durable formats

`jazz.sqlite.writer-order.v1` stores one row in
`_jazz_writer_order_v1`, whose primary key is `(t,id,k,node)`:

- `t` is the existing physical table name as SQLite TEXT.
- `id` and `node` are exactly 16 UUID bytes.
- `k` is the existing exact branch-key BLOB; the root key is empty.
- `tx` is exactly eight big-endian bytes for the original writer's transaction,
  in the range `1..i64::MAX`. It is neither the authority sequence nor a
  provenance timestamp. Truncated, extended and out-of-range values fail.

`jazz.sqlite.writer-order-profile.v1` occupies slot 1 in
`_jazz_writer_order_profile_v1`. Its value is the ASCII bytes
`JAZZ-SQLITE-WRITER-ORDER`, then NUL and byte 1. Unknown versions and trailing
bytes fail admission. The fixture
`crates/jazz/fixtures/sqlite_writer_order_v1.json` pins both formats.

Both tables use explicit v1 names. Row changes, frontier records, receipt
records and allocator updates share the enclosing transaction. No sync message,
wire tag, pending-commit encoding or receipt encoding changes.

## Earlier SQLite files

A new or empty store can establish the profile only if it has no row history,
recorded receipts, local commits or existing frontier entries. A nonempty file
from an earlier SQLite build lacks enough information to claim this invariant
without reconstruction. It remains readable, but authority initialisation and
new authority writes refuse with `unsupported_storage_codecs`. This change
provides no in-place backfill or reset. Existing replicas do not gain authority
from their previews or receipt cache.

All authorities must use this implementation and qualified storage. Older
binaries ignore the new tables, so downgrade or mixed-version writes to the
same file are unsupported. This follows the coordinated-release decision for
the unpublished SQLite/E2EE stack; it is not a migration guarantee for existing
application databases.

## Qualification

Codec fixtures and existing regression suites are checked separately from the
pending public-API writer-order test port. Until that port is approved and run,
the late-upload, restart and replay scenarios are not claimed as runtime
qualification. This guard does not replace the retired randomized sync soak.
