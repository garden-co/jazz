# SQLite foreground initialization

A browser foreground can now seal an initialization transaction through its
private durable owner. Previously the SQLite seal primitive rejected foregrounds
because their in-memory allocator could not promise a durable journal identity.
The new asynchronous path freezes the batch, reserves an identity at the worker,
and returns only after the worker's actual storage flush. The existing synchronous
primitive remains for durable owners.

## Reservation, publication and recovery

The worker allocates in the original writer's namespace using indexed lookups of
that writer's retained reservations and uploads. The next number is at least the
foreground's requested floor and strictly exceeds previously used numbers.
Reservations do not advance the worker's publication counter: private owner
positions and foreground positions occupy separate ranges. Cancellation or a lost
tab leaves the reservation retained, so even a restarted tab using the same node
cannot reuse the old journal identity.

Each reservation binds a fresh runtime capability, original node/transaction,
authenticated subject and structural schema. The public `ReservedTxId` remains a
journal reference, not a publication capability. Only the admitted private owner
can reserve; generic relays, authorities, other subjects, incompatible schemas
and attempts to use the worker's own node are refused. There are at most 64
outstanding reservation requests per foreground and per serving session.

Publication wraps exactly one ordinary serializable Commit with the capability.
The worker checks the binding, journals the ordinary unit and its published marker
atomically, then follows normal upload, projection, chunk, permission and receipt
paths. An ordinary Commit cannot use a reserved identity without its capability.
Retries cannot replace the original author, schema or payload. Caller-supplied
claims on ordinary connections are normalized to authenticated claims; they do
not grant permission. The authority alone decides acceptance or rejection.

The host flush barrier captures particular requests. It cannot acknowledge later
arrivals, a reused session slot with a different capability, or a retired owner.
Ordinary commits are refused while a reservation identity is unresolved, because
the owner can have a higher retained floor than the tab. A cancelled request is
still resolved (and retried on reconnect) before ordinary commits resume; a late
reply advances the allocator but cannot return a usable token or publish its
retired batch. Refused reservations remain fenced until a successful retry or
a fresh foreground is opened. A successful seal publishes no
rows. Publication uses the normal Local wait, which requires retention of the
complete unit at the worker, and Global still requires a real authority receipt.

Recovery status uses exact indexed original identities. Reserved-but-unpublished
identities are not observed. A published marker with a missing unit is incomplete,
not absent. Pending units must retain serializable proof and all referenced large
values; incomplete recovery blocks outbound sync. Terminal status is retained with
the authority fate in the same receipt transaction. The partial recovery index
covers pending initialization units, without scanning unrelated history.

## Named encodings and deployment

`jazz.sqlite.owner-initialization.v1` is the new
`_jazz_owner_initialization_v1` table. Its primary key is `(node, tx)`: node and
capability nonce are 16 raw UUID bytes, tx is a positive SQLite integer, subject is
the admitted canonical subject text, and schema_id is 16 raw schema UUID bytes.
The nonce is unique. Integer state is 0 for reserved, 1 for published/pending, and
2 for terminal. `_jazz_owner_initialization_pending_v1` indexes `(node,tx)` only
where state=1. Retained reserved rows also provide the non-reuse floor.

Three appended SyncMessage variants use the existing postcard scalar encodings:

- Tag 33, `jazz.sqlite.sync.owner-initialization-reserve.v1`: request ID u64,
  nonce [u8;16], writer node [u8;16], minimum transaction u64.
- Tag 34, `jazz.sqlite.sync.owner-initialization-reserved.v1`: request ID u64,
  nonce [u8;16], Result containing either `(node [u8;16], tx u64)` or the existing
  WireError. Success follows the durable-owner flush.
- Tag 35, `jazz.sqlite.sync.owner-initialization-publish.v1`: nonce [u8;16], then
  length-prefixed bytes of exactly one ordinary Commit. No nested envelope is
  accepted. The worker forwards the normal Commit, retaining the capability locally.

`sqlite_owner_initialization_v1.json` pins all three tags, successful and refused
replies, the nested existing Commit fixture, and actual durable SQL types/values.
The fixture was assembled independently from explicit fields and unsigned-varint
rules. No existing Commit, catalogue, reservation-string or status-JSON encoding
changes. Upgrade these unreleased SQLite peers together; earlier workers reject
the new messages. No mixed-version fallback or unreleased journal migration is
introduced.

## Validation and scope

Public core tests cover flush ordering, interleaved reservation barriers, cancelled
reservation non-reuse after restart, missing-unit recovery, original authority
fates, forged publication capabilities/identities, caller-claim normalization,
terminal rejection after reopen, a fresh tab recovering a pending initialization,
closing the binding during a pending seal, and ordinary-write isolation from a
delayed or cancelled reservation across reconnect. Independent wire/storage fixtures
and existing initialization, owner, relay and permission regressions run alongside.

The [private-owner WebSocket port](sqlite-private-relay-websocket.md) now includes
reproducible browser tests against the real SQLite server and production IndexedDB
worker. They cover encrypted chat creation and reopening, sharing between two
accounts, a subscribed reply, and refusal to decrypt for an unshared account.
This does not qualify every encrypted query/subscription shape, all crash
schedules, or the remaining native runtime integration. The stack stays draft.
