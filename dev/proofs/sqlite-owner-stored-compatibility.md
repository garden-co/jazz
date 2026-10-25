# SQLite owner storage receipts

The SQLite relay retains a pending upload before the authority decides its fate.
Previously, that retention produced no receipt a foreground could use to wait for
Local durability. The owner now sends a separate receipt after its storage
barrier completes. Authority acceptance and rejection still use `FateV1`.

## Encoding and deployment

`jazz.sqlite.sync.owner-stored.v1` is the append-only `SyncMessage` variant
`OwnerStoredV1`, postcard tag 28, followed by the original 16-byte node identity
and unsigned transaction-number varint. The byte fixtures in
`crates/jazz/fixtures/sqlite_owner_stored_v1.json` pin the tag, field order and
varint boundaries independently of serializer defaults. Existing message tags
and the relay journal/root-pin encodings are unchanged.

All SQLite-stack peers must upgrade together. Older peers cannot decode tag 28;
this unreleased stack does not negotiate mixed-version support. The new receipt
has no authority sequence, permission decision or terminal result. It is never
converted to an accepted fate.

## Storage boundary

A synchronous SQLite owner confirms only uploads whose journal transaction has
committed and whose referenced large values are complete and validated. The
existing relay root pins retain all referenced chunks. Missing chunks must arrive
through normal sync; an incomplete or malformed value receives no storage claim.
The authority still performs permission and serializable-conflict validation.

An asynchronous host opts into deferred confirmation before accepting traffic.
The binding captures an opaque token containing the exact original upload IDs,
then awaits the real backing-store flush. The browser host uses the existing
IndexedDB flush barrier (or OPFS synchronous persistence). Only a successful
flush can confirm those captured IDs. Later arrivals need another barrier,
even if another tick runs while the first flush is pending. A missing hook or
failed flush emits no receipt and leaves the uploads available for retry.

The token is tied to one database instance. Retirement or transition to a
non-durable foreground invalidates confirmation. Browser closure also fences
flush completion before it can publish an acknowledgement. Tokens cannot be
used to acknowledge a different owner, and terminally settled uploads are
ignored if a flush finishes late.

## Receipt and wait semantics

The receipt names the original `(node, transaction)` pair. A foreground records
it only for a matching pending write. An unknown pair, another writer with the
same transaction number, or a late receipt after settlement cannot create or
settle a write. Before the receipt, a foreground's pending write reports None;
after it, Local and Pending. Global still requires the authority's actual
receipt. A known rejection fails a Local wait as well as a Global wait.

Each connection receives a Local receipt once per pending upload. A reconnect
that resends the immutable upload receives the receipt again. An owner that
reopens reconstructs the pending journal and confirms it through a new storage
barrier. Terminal fates remove per-connection acknowledgement bookkeeping.

For example, Alice sends two writes while offline. Bob's owner finishes a flush
covering the first while the second is arriving. Only the first becomes Local.
After a second flush both are Local and Pending. The authority may subsequently
accept the first and reject the second; Local never predicts that result.

## Validation and remaining scope

Core tests cover later arrivals, failed and missing flush hooks, retirement,
reconnect, wrong-origin receipts, large-value completeness, wait semantics and
fixed wire bytes. Browser tests exercise the database and transport tick APIs,
real IndexedDB flush/reopen, large uploads and later authority acceptance or
rejection. The reopen test is orderly closure, not a browser-crash simulation.

A storage receipt alone does not admit foreground mutations. The later
[private-owner port](sqlite-owner-projection-compatibility.md) adds pending-read
recovery and requires a matching owner acknowledgement before new writes. These
receipt tests deliberately queue ordinary replica writes before entering
foreground mode to isolate the durability boundary; `sqlite_owner_writes` covers
new writes after admission. Pending relay rows never become shared permission
evidence. The [browser quickstart checks](sqlite-browser-reload-qualification.md)
provide separate application coverage, with explicit limits.
