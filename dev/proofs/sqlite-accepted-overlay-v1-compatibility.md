# SQLite accepted overlay checks v1

A trusted backend can now read accepted rows after staging a write to their root
or permission dependency tables, provided that the authority verifies that the
staged changes leave the query result unchanged. Previously all such writes
caused a refusal, including harmless changes elsewhere in the same table.

The check extends [accepted scoped evidence](sqlite-accepted-scoped-evidence-v1-compatibility.md).
It publishes no write and does not replace the transaction's Global receipt.

## Decision and rollback

Alice stages an unchanged grant and reads her document. The authority first
runs her original query at its pinned sequence. It then simulates all staged
writes under Alice's normal write permissions and immutable request claims.
A second query includes that staged state. The rows must match in order,
version and projected values. The authority rolls back the savepoint on either
success or failure before sending an answer.

A staged update to another document can therefore succeed when Alice's query
filters it out. A grant that would expose an uncached document refuses the read.
An ordered window is compared as requested: a newly visible document outside
the window may leave it unchanged, while one that displaces a result refuses it.
The authority never limits this comparison to IDs in the backend's cache.

The write target must not have advanced beyond the pinned cut. Such a change
returns `TransactionConflict` before simulation, preserving the authority's
newer state. This check is conservative for the client's own earlier pending
writes too: their later settlement does not qualify a newer write parent for
this receipt. Normal Global commit still performs its complete read-set, write
parent and permission validation.

Historical policy sources use the existing derived UUID indexes, with the
simulated publication sequence admitted as an additional current image. A
staged image masks the older image at that coordinate. Roots use the same
snapshot-plus-own-write semantics as ordinary transaction queries. Write
permission checks use the normal executor, including candidate-created checks;
there is no unchecked application of the staged writes at the authority.

Rollback restores rows, history, chunks and scratch-table changes. Pending
change notifications, deleted-image queues, temporary claims and the speculative
write clock are restored too. No commit fate, upload, issued sequence or accepted
content receipt is created by the simulation.

## Trust, scope and cancellation

Only a connection authenticated as SYSTEM can submit the request. It must name
a non-SYSTEM session identity; the authority applies that identity's policies.
An ordinary session cannot delegate this simulation, even for its own identity.
Relays forward the request and its exact authority response. Their partial
caches cannot substitute an answer. Schema translation across relay versions
remains unsupported.

Request IDs belong to one upstream connection. A separate response kind prevents
ordinary query coverage from satisfying the check. The backend checks the pinned
cut and verifies that no write was appended to the staged prefix while it waited.
Cancelled, failed and unfinished reads continue to block commit. A reconnect,
schema change or late answer cannot move the check to another transaction. The
existing 15-second accepted-read deadline includes this extra round trip.

A related correction makes ordinary and paired scoped reads use authenticated
catalogue policies when those policies arrive after the query was prepared.
A policy-free bootstrap schema can no longer turn an authorised row-local result
into an empty local recheck. Relays use the same governing policies.

## Bounded work

A request carries at most 1,024 writes and 4 MiB of encoded staged operations.
It accepts only root targets on unbranched tables. Other shapes receive a typed
unsupported error. The two query evaluations share the existing bounded SQL
work budget; normal write policy checks retain their existing limits.

Only values explicitly staged in the batch are materialised for the request.
For a chunked value, the batch's private overlay makes its derived nodes
available, and the sender checks its declared size before reconstructing it.
The authority receives logical values and uses normal write encoding during the
simulation. Raw indirect value descriptors are refused. Unrelated cached values
are never loaded to build the request. The simulation and its local preparation
are rolled back; normal sync still transports the eventual committed chunks.

## Wire encoding and deployment

Two postcard variants append to `SyncMessage`:

- `jazz.sqlite.sync.accepted-overlay-check.v1`, tag 38 (`0x26`),
  `AcceptedOverlayCheckV1`: `id: u64`, `query: Query`,
  `binding: BTreeMap<String, Value>`, `through: u64`, `include_deleted: bool`,
  `author: String`, `claims: BTreeMap<String, Value>`, `ops: Vec<WireOp>`.
- `jazz.sqlite.sync.accepted-overlay-checked.v1`, tag 39 (`0x27`),
  `AcceptedOverlayCheckedV1`: `id: u64`, `through: u64`.

Fields appear in that order. Query, value and write-operation carriers retain
their existing wire layouts. The response is a statement about the matching
request's staged projection, not query completeness or transaction acceptance.
It becomes visible only after its containing sync batch commits. Refusals use
the existing typed `Refused` response.

`crates/jazz/fixtures/sqlite_accepted_overlay_v1.json` pins both messages' exact
bytes, including a multi-byte cut, a window, session claims and an update.
No durable encoding or earlier message tag changes.

Upgrade the authority, backends and every relay together. There is no capability
negotiation; older endpoints cannot decode tags 38 and 39. A failed check must
not fall back to local approval, an ordinary answer or an unscoped request.
Intermediate unreleased E2EE versions have no mixed-version guarantee.

Public API regressions cover direct and relayed successful checks, uncached
phantoms, windows, chunked staged values, stale write targets, rollback,
connection trust, write permissions, immutable claims, response substitution,
cancellation and concurrent staging. These checks do not qualify the full
browser, mobile or unpublished native runtime port.
