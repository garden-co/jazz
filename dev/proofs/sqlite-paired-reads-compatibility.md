# SQLite paired accepted reads, version 1

`allSettlementMetadata` now returns stored root rows and their accepted content
transactions through the shared host and WASM bindings. It uses SQLite's normal
query, permissions, sync, chunk transport and serializable validation paths.
Callers still await the enclosing transaction's Global receipt before acting
on a decision. A successful read alone does not mean its transaction committed.

## Read contract

A Remote read obtains a typed authority answer. The first queued read pins the
transaction's cut; subsequent paired reads request that cut. The replica checks
that its ordered root ids equal the authority result and that every returned
row state has authority-issued content evidence. The cut excludes commits that
were pending locally at begin and later settled beyond that authority position.
Ordinary transaction reads keep their own-write overlay; paired accepted reads
do not extend the authority result with it. Pagination hydrates the normal
bounded prefix, then applies the original offset locally. The read set records
the original query, including its window, for authority validation. Large values
hydrate through ordinary chunk transport, including at a relay. Requests have a
15-second bound and fail when authority coverage or required chunks cannot be
observed. Reconnect, rollback and schema changes cannot transfer a pending read
to another transaction or upstream connection.

LocalOnly is an offline observation, not a completeness claim. It uses the
accepted sequence and local evidence-admission ordinal captured at transaction
begin. Both root rows and policy dependencies use that cut. Pending local
inserts, updates, deletes and grants are excluded. An older authority version
received after begin is also excluded: its sequence alone is insufficient.
Remote reads with `e2ee_snapshot_at_begin` additionally compare the rows and
content settlements with that original accepted snapshot. Unknown evidence
produces a refusal, never a fabricated transaction id.

Paired reads register before their futures are polled and queue with ordinary
transaction reads. Cancelled, failed or unfinished paired reads prevent commit.
Rollback wakes queued and active readers; a reused opaque transaction id does
not inherit an old read. A staged write affecting the result refuses the paired
read. Read-only commits still send their read sets for validation, so an empty
result followed by a phantom insert conflicts just like a changed existing row.

One immutable identity/claims scope governs the transaction's paired reads,
subsequent ordinary reads and commit. An ordinary authenticated connection can
request only its authenticated identity and identical claims. Only an explicitly
trusted SYSTEM connection can delegate another scope. The host's existing
backend boundary and all ordinary policy checks remain in force.

Supported queries return stored, unbranched root rows, with ordinary predicates,
projections, ordering, windows and optional deleted rows. History, branched
roots, explicit query traversals, relationship output, flat joins and aggregates
return a typed unsupported error. Policies still use the normal policy compiler;
missing supporting evidence cannot qualify an incomplete result as complete.

The [accepted scoped extension](sqlite-accepted-scoped-evidence-v1-compatibility.md)
adds bounded permission evidence for trusted backends, including historical
indexed dependencies and recursive helpers. The
[accepted overlay check](sqlite-accepted-overlay-v1-compatibility.md) allows
staged writes when the authority verifies that their projection leaves the
original accepted result unchanged.

## Worked cases

Bob's content write B is accepted at authority position 7. A later deletion D
changes row state at position 8. Alice's paired read including deleted rows
returns D's retained content with B's writer transaction and position 7. A
content-free restore keeps B; newly written content gets a new witness. An
identical payload written in another transaction is still a different witness.

Alice pins position 7 while her own insert, update or delete is pending. That
write settles at position 8 before a second paired read. The second read still
returns the position-7 contents and their original authority evidence. Its read
set remains subject to normal commit validation: a decision changed by the
position-8 write conflicts and must be retried. A later own commit cannot cause
a false incomplete-result error or silently replace content at a matching id.

Alice begins while she has accepted evidence only for A. She later receives B,
even if B was accepted upstream before begin. Her LocalOnly observation retains
A. A Remote read can return B, but with the begin-snapshot requirement it refuses
unless B was already admitted at begin. Restarting the replica preserves this
distinction. Duplicate deliveries do not change the admission frontier.

A relay with an empty or partial cache forwards a typed request to its authority,
waits for that answer and any needed chunks, and forwards the exact authorized
result and stored evidence. It cannot substitute its own local coverage or guess
origins from partial history. Relay reads do not enable relay write forwarding.

## Encodings

All new messages append to the existing postcard `SyncMessage` enum:

- `jazz.sqlite.sync.accepted-row.v1`, tag 21, `AcceptedRowsV1`: a vector of
  `(table string, row id [u8;16], branch-key bytes, row-state u64, content)`.
  Content is `(authority content sequence u64, original writer node [u8;16],
original writer transaction u64)`. Valid evidence requires
  `0 < content sequence <= row-state sequence < LOCAL_BASE` and
  `0 < writer transaction <= i64::MAX`. Ordinary authorized image shipping
  supplies evidence; only authorities issue it and replicas forward received
  records. It is transport evidence from the trusted upstream, not a signature.
- `jazz.sqlite.sync.accepted-fetch.v1`, tag 22, `AcceptedFetchV1`: request id,
  query, bindings, optional authority cut, include-deleted flag, optional
  `(canonical author string, claims map)`. An ordinary Answer cannot satisfy it.
- `jazz.sqlite.sync.accepted-answer.v1`, tag 23, `AcceptedAnswerV1`: request id,
  authority through-sequence, ordered root ids. Optimistic through-sequences are
  invalid. This is coverage for the requested query, not transaction acceptance.
- `jazz.sqlite.accepted-row.v1`: `_jazz_accepted_rows_v1` keys records by stored
  table name, row-id bytes, branch-key bytes and row-state sequence. The `content`
  BLOB is exactly the content tuple above in postcard form; trailing bytes are
  invalid. `observed INTEGER PRIMARY KEY AUTOINCREMENT` is local admission order,
  independent of authority commit order. Repeating an identical proof is a
  no-op; conflicting or invalid inbound proof fails with Protocol, malformed
  stored bytes with Storage. Proofs and row images share the sync savepoint.

The table is created idempotently on open. Existing caches receive no guessed
backfill: they acquire evidence through normal authority image refresh. Wire
names map to stored names using the same schema translation as row images.
No existing row, commit, Origins, content-reference or earlier message encoding
changes. `sqlite_accepted_v1.json` pins all new tags and field order, integer
boundaries, an authenticated scope and the durable value. Tests also inspect
actual persisted bytes after public reads and inject corruption to check refusal.

The existing `jazz.binding.settled-rows.v1` ABI is retained: u32 little-endian
metadata byte length, UTF-8 JSON metadata, then the ordinary binding row codec.
Metadata-only mode returns just JSON. Each item has `rowId`, `transactionId` and
`position` strings. The empty paired frame is `020000005b5d00`; host and Chromium
tests exercise both modes and match row bytes to the normal binding output.

## Deployment limits

Upgrade all SQLite peers together. Tags 21–23 are not negotiated, and older
binaries cannot decode them. A newer replica with no accepted proof cannot infer
it from legacy Origins or a local receipt. Downgrade and mixed-version deployment
are unsupported; no compatibility with retired Groove or earlier unreleased
E2EE schemas is claimed. Catalogue identity/coverage is covered separately in
`sqlite-catalogue-compatibility.md` and E17’s
`sqlite-catalogue-policies-compatibility.md`. Browser SDK storage admission remains
a separate boundary, so these scoped checks do not qualify the full browser
E2EE runtime.
