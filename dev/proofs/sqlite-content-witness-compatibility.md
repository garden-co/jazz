# SQLite content witnesses, version 1

Ordinary SQLite fetches and subscription deltas now carry the original
transaction identity for the content they ship. Deletes and restores without
new content retain the preceding content transaction. A state change's sequence
must not stand in for that content transaction.

This is the transport and persistence prerequisite for paired E2EE reads.
Origins identify a writer; they do not themselves establish authority acceptance,
a shared accepted-at-begin snapshot, query completeness or a Global transaction
receipt. The separate [paired-read contract](sqlite-paired-reads-compatibility.md)
adds those read-time checks and exposes `allSettlementMetadata`. Global commit
acceptance remains a separate receipt wait.

## Encodings

- `jazz.sqlite.sync.content-origins.v1` names the existing `Origins` message,
  tag 18. Its fields and postcard bytes are unchanged: a sequence of
  `(accepted sequence, writer node, writer transaction)` tuples. Ordinary reads
  now send it before their row images. `sqlite_content_origins_v1.json` pins its
  bytes, including a writer transaction above the optimistic-sequence boundary.
- `jazz.sqlite.sync.content-witness.v1` is the new append-only tag 20,
  `ContentWitnessesV1`. Each reference contains, in order, the logical table
  name, 16-byte row id, branch-key bytes, row-state sequence and content
  sequence. A reference is valid only when
  `0 < content sequence < row-state sequence < LOCAL_BASE`.
  `sqlite_content_witness_v1.json` pins the whole message. Existing tags,
  including `ScopedFetchV1` at tag 19, retain their layouts.
- `jazz.sqlite.content-witness.v1` is the durable representation. The
  `_jazz_content_witness_v1` table keys each reference by stored table name,
  row-id bytes, branch-key bytes and integer row-state sequence. Its `content`
  BLOB is exactly one postcard u64: the content sequence, with no trailing
  bytes. The same fixture pins boundary values; a persistence test checks
  actual stored bytes after a normal public `Db::open_at` fetch and restart.
  Malformed values fail with `Storage`; invalid or conflicting incoming links
  fail with `Protocol`. A recorded link is immutable.

Logical table names on the wire translate through the same session schema as
row images. Durable keys use the receiver's stored table name. The new table is
created idempotently on open; existing row/image/commit layouts do not change.
There is no speculative backfill from a replica's existing history.

## Worked cases and limits

Bob uploads content in transaction B. The authority later deletes the row in D.
A cold ordinary fetch including deleted rows ships D's tombstone, B's content
image, B's original writer identity and the explicit D-to-B reference. Alice's
replica keeps D current and B historical. A restore without new content keeps
B; a restore with new content gets its own content transaction.

A relay may have missed B while retaining older content A, even with identical
payload bytes. It forwards D-to-B only if it received that exact reference and
retains B's image. Without that link it ships the permitted current row but
neither guesses D-to-A nor invents its own transaction identity. Pending row
sequences never become accepted origins or references. Reopening a partial
replica preserves these distinctions.

A complete local store finds at most one preceding content image per selected
non-content state, using the history primary key `(id, branch, seq)` and a
sequence bound. A relay uses the exact persisted content sequence. Ordinary
content images need no extra reference or history lookup. Payloads deduplicate
row versions; they do not include every version of a row or load unrelated
rows. Same-id rows in separate branches retain separate references. A copied
branch state without a same-branch retained content version remains without a
reference.

The normal query, session policy and supporting-row checks select images before
witness lookup. Witnesses reveal only preceding content of an already readable
row, under the existing history permission contract. Missing or unreadable rows
produce no extra content or origins. Clients cannot submit the server-to-client
reference message as an upload. References do not grant permission, freshness,
coverage, or a serializable transaction receipt.

## Deployment compatibility

Upgrade every SQLite peer together. Tag 20 is not negotiated; an older binary
cannot decode a response carrying it. A newer replica can receive rows from an
older peer, but missing references remain unknown, including after restart.
Downgrading the runtime after enabling this message is unsupported. This change
does not supply compatibility with the retired Groove runtime or earlier
unreleased E2EE schemas.

The focused tests cover ordinary fetches and subscription updates, uploaded
writer identities, deletions/restores, branch isolation, snapshot cuts, relay
forwarding, history gaps, pending writes, permission denial, offline restarts,
corrupt durable values, conflicting/invalid messages, message direction and
exact wire/storage value bytes. The full E2EE SDK/browser runtime remains
unqualified by these transport tests.
