# SQLite initialization publication and recovery

E14 now supports owner-bound initialization seals, exact insert-absence evidence,
single-use publication/cancellation and bounded recovery status. The shared host
and WASM bindings expose the existing SDK ABI. A seal freezes a serializable
batch and reserves its eventual public transaction ID without publishing rows
or queuing an upload. Other writes may commit while the host journals the seal.
Publication consumes the capability and runs the ordinary serializable checks,
permission checks and atomic row/outbox transaction. Failure does not reopen the
old mutable batch. Cancellation and drop leave the durable allocator floor in
place, so a journalled reservation cannot be reused after restart.

For example, Alice seals an absent founder row at reservation R, commits an
unrelated note, then publishes the founder at position P. R remains the public
identity; P orders local snapshots, row versions, waits and replay. If another
transaction inserts the founder before publication, publication conflicts and
R remains not observed. A subsequent transaction may read the pending founder
and depend on R, including after Alice restarts under a different writer node.
Bob resolves the dependency to its accepted authority position and runs the
normal current read-set and write-policy validation.

## Named encodings

- `jazz.sqlite.initialization-reservation.v1` is the UTF-8 string
  `jazz-sqlite-init-v1:<canonical lowercase hyphenated UUID>:<canonical decimal tx>`.
  The tx is positive and below SQLite's signed integer limit. Decoding rejects
  unknown versions, old HLC reservations, noncanonical UUIDs/decimal values and
  extra fields. The bytes identify journal linkage, not a publication capability.
- `jazz.sqlite.local-commit.v1` is `_jazz_local_commits_v1` with `seq INTEGER PRIMARY
KEY`, `node BLOB` (exactly 16 raw UUID bytes), `tx INTEGER`, `author TEXT` (the
  admitted owner's canonical subject), `initialization INTEGER` and `pending
INTEGER` (both boolean). Positive positions and tx values, `(node,tx)` and `tx`
  uniqueness are enforced. Each local publication records its original identity;
  ordinary publications have `tx == seq`. Reservations alone have no record.
  An initialization's pending flag is cleared in the same receipt transaction
  that records settlement and removes the outbox unit. The partial index
  `_jazz_initialization_pending_v1` covers only unacknowledged initializations.
- `jazz.sqlite.serializable-origins.v1` appends tag **2** to `WireKind` inside
  existing Commit message tag **4**. Fields are postcard `snapshot: u64`, the
  unchanged `Vec<WireRead>`, then `Vec<(local_position: u64, node: [u8;16], tx: u64)>`.
  Existing Mergeable tag 0 and Serializable tag 1 retain their exact layouts.
  Tag 2 is emitted only when an own dependency's origin differs from the old
  same-node, position-equals-tx convention. Duplicate positions/origins and
  invalid ranges are rejected. The origin map does not grant an author, claims,
  authority role or permission exception.
- The existing initialization status JSON bridge remains version 1:
  `{version:1,statuses:[...]}`, preserving input order and exact reservation
  strings. Entries are `not-observed`, `incomplete`, or `complete` with a fate
  and `none`/`local`/`global` durability. A rejected fate has a code and reason.
  Calls accept at most 64 identities. Foreign-author and absent records both
  return `not-observed`.

`crates/jazz/fixtures/sqlite_initialization_v1.json` pins reservation UTF-8 bytes,
a full Commit frame carrying the new origin map, the persisted SQL values/types
and the exact pending status JSON. Tests inspect the real SQLite row and recovery
index, decode and re-encode the wire fixture, and reject invalid reservations.
The existing `issued-seq-v1` allocator encoding is unchanged.

## Durability, recovery and isolation

Local rows, the origin map and the original upload message are committed in the
same SQLite transaction. Recovery orders pending units by publication position,
retaining original node/transaction identities across writer lease rotation.
A new runtime must have a local-commit identity record for each pending upload;
it never guesses the publication position from a reserved transaction number.
Recovery checks only the indexed pending initialization records, exact named
upload units and their operation row coordinates, including historical images.
Missing units, missing serializable proof or incomplete row manifests block
all outbound sync before an upload prefix escapes. Malformed stored bytes fail
with a storage error. A foreign owner cannot replay the unit under a new session.
The storage owner remains trusted; these records are not signatures over a
maliciously rewritten SQLite file.

Insert absence checks the exact physical row ID, including tombstones, hidden
rows and staged writes. It records an ordinary serializable read, which Bob
revalidates on upload; it does not claim fresh remote coverage. A fresh offline
owner enters the normal replica role before reserving IDs. A standalone store
with prior local-only history must already have replica admission: converting
that history into a replicated authority snapshot is explicitly rejected.

WASM seals wait for the browser VFS flush before delivering the reservation.
Closing the owner retires its tokens synchronously, including a seal waiting for
that flush. Schema views share the registry; closing a view leaves it intact.
Publication returns the ordinary write receipt, whose local wait flushes the
publication position. Status captures its result before flushing that captured
high-water mark, so it never promises durability for later unflushed writes.

## Compatibility and qualification

Upgrade all SQLite peers together. Older peers cannot decode WireKind tag 2;
there is no negotiation or downgrade support. Legacy HLC initialization journals
and existing pending outboxes without local-commit-v1 records are not migrated.
Drain those outboxes before upgrading; missing identity records fail closed.
Old settled ordinary positions retain their previous identity fallback. No
intermediate unreleased E2EE migration or compatibility layer is added.

Core tests cover interleaved and reverse-order publication, frozen handles,
conflicts, cancellation, exact IDs, restart/lease rotation, incomplete recovery,
foreign owners, failed outbox persistence, hidden/deleted/staged absence,
malformed wire mappings, forged attribution and accepted-history isolation.
Chromium tests exercise real IndexedDB persistence and the SDK's seal/status ABI,
single-use owner/view tokens and close during pending seal delivery. Existing
transaction, sync, permissions, accepted-history and catalogue tests also run.

These tests cover the SQLite core and WASM initialization primitives. The later
[foreground initialization port](sqlite-foreground-initialization-compatibility.md)
extends sealing, publication and journal recovery through a private durable
owner. [Browser storage admission](sqlite-browser-storage-compatibility.md),
[foreground write recovery](sqlite-owner-projection-compatibility.md) and
[owner flush receipts](sqlite-owner-stored-compatibility.md) are also implemented
under their documented admission and durability rules.

The [React quickstart checks](sqlite-browser-reload-qualification.md) exercise
encrypted chat creation, sharing, live replies and concurrent reloads through
the production worker. They do not qualify every encrypted query shape, crash
schedule or permission graph. NAPI/native-relay publication and the remaining
full-stack checks are still outstanding; E17 remains draft.
