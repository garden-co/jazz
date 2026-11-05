# SQLite write fate v1 compatibility

The SQLite WASM binding now implements `Write.writeState()`. Its existing public
shape is `{ fate, global_time, durability }`. A pending local write is not global
acceptance. An accepted replica write carries the authority's sequence while the
receipt keeps its original local transaction identity. A rejected write has no
global time and no write durability; its wait rejects with the authority's error.
Reading a snapshot does not claim or consume rejection reporting.

A binding receipt belongs to the database node that produced it. Schema views
and a reopened store retain that node; another node cannot reinterpret the
receipt's numeric sequence.
Closing a WASM write handle makes its snapshot unavailable. The core sequence API
can inspect recovered writes after reopening the same node's SQLite database.

Authority conflicts remain retryable throughout the host adapters. Upstream
represents them as `WriteRejected` with its `SerializationConflict` marker;
`Write.writeState()` maps that marker to `SerializableConflict`, and mutation
callbacks report `serialization_conflict`. Local conflict callbacks keep
`transaction_conflict`. A conflict must not become `AuthorizationDenied`, which
would suppress retries and enter permission-denial fallback paths. This changes
error interpretation only; stored and transmitted bytes are unchanged.
E2EE account initialisation, device responses and space maintenance recognise
both conflict codes in their existing bounded retry paths. Each retry obtains
fresh authority evidence; permission failures and signer errors retain their
existing handling.

The public host regression
`binding_host::authority_conflicts_remain_conflicts_in_host_receipts_and_events`
covers Alice's empty read invalidated by Bob's accepted insert, then checks the
write fate, unclaimed callback, retryable wait error and surviving authority row.

## Durable encoding

The record role is `jazz.sqlite.write-fate.v1`, version 1. The existing SQLite
`_jazz_applied` table identifies a record by `(node BLOB, tx INTEGER)`. Its `result`
BLOB is postcard `Result<(), WireError>`: result tag 0 is accepted, tag 1 is
rejected; the inner `WireError` tags are 0 for conflict, 1 for rejection and 2 for
other errors, followed by the existing string encoding. Authorities already
persist these bytes. Replicas now persist the same result when receiving `Fate`,
together with `_jazz_settled` and pending-upload removal in one savepoint.

`crates/jazz/fixtures/sqlite_write_fate_v1.json` pins accepted, conflict, rejection
and other-error bytes. The fixture test checks encoding and decoding/re-encoding.
No wire tags, fields, table columns or record bytes change. This document names
the existing envelope; no version prefix is inserted into old bytes.

Earlier experimental replicas recorded rejection with a null settlement sequence
but lost the detailed error on restart. Those records now fail with `NotObserved`
when the detail is unavailable; they never become successful waits. Corrupt result
bytes fail with `Storage`. There is no migration or compatibility guarantee for
other unreleased E2EE or SQLite layouts. Use matching stack revisions.

## Browser durability

The IndexedDB VFS queues SQLite commits asynchronously. A newly committed write
therefore reports durability `None` until a successful flush barrier covers its
sequence. `wait("local")`, database flush, local pending-write waits and close use
that barrier. A captured sequence watermark prevents a completed flush from
claiming later writes as durable. Memory and synchronous OPFS retain their
existing local-persistence semantics. An observed authority acceptance remains
`Global` independently of the local browser's flush state.

Local waits still reject a known rejection. A global wait without observed
authority acceptance fails explicitly when the transaction is not queued for
upload; it cannot silently succeed for an unconnected local database. Pending
uploads continue waiting for their authority fate.

## Relay boundary

Durable relay upload forwarding is now covered by
[SQLite durable relay uploads](sqlite-relay-upload-compatibility.md). It journals
original transaction identities and waits for the authority's qualified receipt.
Non-durable foreground writes use the later
[private-owner admission and recovery port](sqlite-owner-projection-compatibility.md).
They become Local only after a matching
[owner storage receipt](sqlite-owner-stored-compatibility.md), and Global only
after the authority's acceptance. Optimistic upstream sequences are never
reported as globally accepted.

## Validation boundary

Public integration tests cover pending and accepted writes across restarts,
rejected writes and their exact recovered errors, authority/local distinctions,
receipt ownership, immediate row-state refusal and relay authority settlement. A direct SQL
corruption test is deliberate because undecodable durable bytes must fail closed.
Browser tests cover the public WASM snapshot, rejection and closed-handle paths,
plus IndexedDB flush qualification and reopening.

Write fate is distinct from E2EE's
[paired accepted-content/transaction observations](sqlite-paired-reads-compatibility.md)
and [accepted catalogue coverage and identities](sqlite-catalogue-compatibility.md).
The [browser storage bridge](sqlite-browser-storage-compatibility.md) is also
ported separately. These receipt tests do not qualify those full paths; the
[browser quickstart checks](sqlite-browser-reload-qualification.md) cover the
React chat flow and reloads. Native runtime publication and full-stack
qualification remain outstanding.
