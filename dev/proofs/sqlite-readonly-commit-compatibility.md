# SQLite read-only commit compatibility

Serializable transactions now submit their read set even when they have no
mutation operations. Previously an empty batch returned the next sequence
without allocating it or queuing an upload. Its global wait therefore had no
authority receipt, and a subsequent transaction could reuse the same identity.

The normal commit path now allocates every completed batch, including empty
mergeable batches and callback transactions. Replicas persist and upload the
existing Commit message, and the authority validates every serializable read
before recording its fate. An unchanged read can receive global acceptance;
an updated row or an inserted query phantom rejects the transaction. Offline
commits remain pending until they reach the authority. Empty transactions do
not gain any exception to session, attribution or relay restrictions.

A read-only session batch has no mutation from which to infer its author.
Its bound identity and claims now populate the existing author/session fields.
For example, a trusted backend's Alice read is checked as Alice, even if the
backend also caches Bob's rows. An ordinary Alice connection cannot submit a
Mallory session merely because the batch has no writes. The authority restores
its previous claims after validation, as it does for mutation batches.

## Durable allocator

Encoding `jazz.sqlite.issued-sequence.v1`, version 1, stores one exact postcard
unsigned 64-bit integer at `_jazz_meta.k = "issued-seq-v1"`. Its byte fixtures
are in `crates/jazz/fixtures/sqlite_issued_sequence_v1.json`. A malformed value,
trailing bytes or a value outside the SQLite signed sequence range fails open
with a storage error; the allocator never silently resets on corrupt metadata.

The allocator floor is written inside each commit's existing SQLite savepoint,
together with rows or receipt metadata. Failed writes roll it back. Reopening
uses the maximum of the floor, stored row versions, pending local transaction
ids, this node's retained fate/settlement ids and recorded authority positions.
The latter evidence also protects existing SQLite files without this key.
Another replica's local ids are never treated as this node's allocator floor.

No table columns change. Old binaries ignore the new key and may reuse ids for
transactions without rows, so downgrading a store after using this behavior is
not supported. No migration guarantee for intermediate E2EE releases is added.

## Wire

The existing Commit message is named `jazz.sqlite.sync.read-only-commit.v1` for
this fixture, version 1. It retains SyncMessage tag 4, the existing serializable
snapshot/read/own fields, author/session fields and an empty operations list.
`crates/jazz/fixtures/sqlite_readonly_commit_v1.json` pins an exact example and
round-trips every field. No wire tag or payload layout changes. Pending commits
and accepted/rejected fates retain their existing encodings.

Every validating authority must run the durable allocator fix: an older
authority can accept an empty upload but lose its sequence floor on restart.
Normal global waits still require the authority's receipt; local persistence
never substitutes for acceptance.

## Validation and limits

Public API tests exercise unchanged reads, stale rows, phantom inserts, distinct
receipts, empty callback transactions, replica and authority restarts, scoped
session claims and forged delegation. Narrow SQL fixture/corruption tests are
necessary to inspect allocator bytes that have no row representation. A browser
WASM test checks the public read/commit/wait APIs and authority qualification.

This removes a prerequisite for accepted-history transactions. It does not yet
implement the paired accepted-content/settlement read API, accepted catalogue
identities, the SDK worker storage bridge or relay uploads. Non-durable
foreground commits continue to reject explicitly, including empty commits.
