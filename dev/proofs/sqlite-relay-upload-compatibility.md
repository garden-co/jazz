# SQLite durable relay uploads

A durable replica can now forward a downstream write to the authority. Previously
it rejected the write as unsupported. The relay journals the upload before
sending it, preserves the original writer node and transaction number, and
returns the authority's recorded acceptance or rejection to that writer. Two
writers can use the same numeric transaction number without sharing a receipt.

## Authority and scope

An ordinary relay does not apply pending downstream writes to its shared row cache.
Only the authority runs the normal candidate, permission, large-value and
serializable validation. Accepted rows arrive through ordinary sync, and the
relay reconciles the exact written row coordinates before sending a receipt.
It never reports an optimistic sequence as global acceptance.

An ordinary connection may only submit its authenticated identity. The relay
captures that connection's authenticated claims, replacing caller-supplied
claims. A host-admitted SYSTEM connection retains its explicit ability to submit
attributed or session-bound writes. Each journal identity is permanently bound
to its original connection identity, schema and request bytes: a changed payload
or another sender cannot overwrite it, even after settlement. Replays retain the
original captured scope. The owner node and registered local transaction origins
cannot be used as downstream identities.

For example, Alice and Bob can both send transaction 127 from different nodes.
Alice's accepted write and Bob's rejected write receive separate receipts. If
Alice disconnects after upload, the relay can restart and forward her original
unit; her later identical retry obtains its stored fate without another write.

## Durable and wire contracts

`jazz.sqlite.relay-upload.v1` uses `_jazz_relay_upload_v1` with an increasing
integer `ordinal`, 16-byte original `node`, positive signed-SQLite `tx`, canonical
UTF-8 `source`, 16-byte `schema_id`, BLOB `request` and `commit_msg`, and integer
`pending` (0 or 1). `(node,tx)` is unique. A partial index on `ordinal WHERE
pending=1` bounds restart work to pending uploads. The original request and
normalized forwarded message each begin with bytes `4a 52 55 01` (`JRU`, version
1), followed by exactly one existing postcard Commit (tag 4), without trailing
bytes. The envelope version also fixes that nested layout. Recovery validates
identity, encoding, sender scope and agreement between the two messages.
Terminal envelopes remain stored for exact retry binding and reconciliation.

`jazz.sqlite.relay-root.v1` uses `_jazz_lv_relay_root_v1`: 16-byte `node`, positive
integer `tx`, and 32-byte large-value root locator. Its primary key is
`(node,tx,root)` and a separate index supports root references. These pins and
the upload journal commit in one savepoint. Existing chunk-tree edges retain
available descendants across receipt expiry. The relay forwards available,
authenticated nodes; missing base content must already exist at the authority,
which validates completeness before accepting. A pending relay upload makes no
local durability promise to its sender.

`jazz.sqlite.sync.origin-write-fate.v1` appends SyncMessage tag **27**:
`node [u8;16]`, unsigned varint `tx`, unsigned varint authority `through`, then
existing postcard `Result<(),WireError>` (accepted 0; rejected 1 plus error tag
and string). Earlier tags keep their numbers. The native permissions-head gate
also emits this qualified receipt. The byte fixture
`crates/jazz/fixtures/sqlite_relay_upload_v1.json` pins the journal prefix, both
normalized/request envelopes, and accepted/conflict/rejected/other receipt bytes.

Receipt persistence, settled position, pending removal and root-pin release
share the incoming sync transaction. Completion becomes visible to downstreams
only after that transaction commits. Storage failure requeues the original
upload; transport backpressure retains the unsent suffix, and reconnect resends
pending units in journal order. Authority deduplication prevents a second commit.

## Compatibility and limits

Upgrade all SQLite peers together. Old peers cannot decode tag 27. Legacy
unqualified Fate still works for a direct pending write but is rejected when a
relay has a matching numeric transaction: guessing could settle the wrong node.
There is no migration requirement for unreleased E2EE/Groove storage or wire
layouts. This port starts at E14 because it uses the original-transaction lookup
introduced by offline initialization.

Relay uploads require the downstream, owner and upstream schema versions to
match; schema translation is explicitly unsupported. Pending uploads must be
drained before changing that schema. Unsupported wire operations, including
branch moves, fail before journalling. Work is indexed by pending unit, exact
transaction identity, touched row or referenced chunk tree; it does not scan
unrelated row tables.

Non-durable foreground writes use the later
[private-owner projection and admission port](sqlite-owner-projection-compatibility.md)
and [owner flush receipts](sqlite-owner-stored-compatibility.md). Private pending
previews are restricted to the admitted subject; they do not become shared
permission evidence or authority acceptance.

Ordinary unscoped one-shot reads retain the existing relay-cache semantics: a
serializable read can be stale until normal subscriptions hydrate the cache, and
the authority then rejects a conflicting commit. Scoped/accepted reads use their
existing forwarding path. These upload tests do not qualify the full browser or
native application. Separate [browser checks](sqlite-private-relay-websocket.md)
exercise encrypted chat, sharing and live replies through the production worker;
the NAPI port remains unpublished.

## Validation

Public core tests cover identity collisions, trusted multi-hop forwarding,
ordinary-session forgery rejection, immutable retries, offline restart with no
sender, lost rejection receipts, serializable conflicts and own dependencies,
large uploads, and reconnect after chunk/commit backpressure. Deliberate SQL fault
and corruption tests verify journal atomicity, outer receipt transaction rollback,
version/trailing-byte rejection and root retention/reclamation. Browser tests run
the public WASM byte-frame path and real IndexedDB flush/reopen. The native runtime
test checks qualified rejection while its permissions head is missing.
