# SQLite accepted catalogue, version 1

The SQLite core, shared host and WASM bindings now implement `tableIdentity`,
`columnIdentity` and `coverCatalogue`. The native server installs the exact UUID
manifests already authored in its durable catalogue; opening a replica, admitting
an unpublished local schema or registering a typed view never mints accepted IDs.
A fresh standalone history-complete authority explicitly initialises its genesis
once. Reopening it retains the persisted manifest.

## Identity and coverage contract

An owner resolves names against the accepted current write descriptor. A fixed
view resolves against its registered descriptor. Renaming a table or column
preserves its published UUID; unknown names and unpublished views return no ID.
Missing accepted metadata is `NotObserved`, and malformed durable bytes are
`Storage`. Lookup is synchronous and local, and remains available offline.
Table lookup reads one indexed table record; a missing record checks the indexed
schema descriptor to distinguish an unknown name from damaged storage.

Fresh coverage is a separate request. The authority checks that the requested
opened schema has an accepted manifest and an admitted query view, and that its
current catalogue and installed policies agree. It evaluates an ordinary
`Query::from(table).limit(0)` under the request's immutable author and claims.
No row bodies are sent, and coverage does not grant permission to read rows.
It proves that the named source and policies were evaluated at the accepted
catalogue head. The enclosing E2EE operation must still obtain all its other
required row evidence and transaction acceptance.

An ordinary session may request only its authenticated author and identical
claims. An explicitly trusted SYSTEM link may delegate a scope. A cold relay
forwards this request to its authority, including the opened schema, and returns
the exact covered head and accepted descriptors. Cached metadata alone cannot
satisfy fresh coverage, even on a live downstream connection. Requests time out
after 15 seconds, fail on upstream loss/reconnect, and clean up when cancelled.
A response cannot complete after its metadata rolled back or changed. The native
server holds catalogue, scoped and accepted-content reads until the initial
permissions head is installed, just as it holds ordinary reads.

Metadata carries at most the current descriptor and the requested fixed
view's descriptor. It does not scan or ship the whole catalogue history per
read. Complete lineage validation runs at authority publication/installation:
content IDs and ordering, one genesis, exact manifest coverage, complete
predecessor mappings, inherited UUIDs and non-reuse of retired IDs. Installing a
snapshot cannot forget already accepted descriptors. Local admission remains
separate from accepted catalogue publication.

For example, Alice opens `notes.text`. Bob publishes a rename to `tasks.body`.
After normal metadata sync, Alice's owner resolves the new names to the same
UUIDs while her fixed old view still resolves `notes.text`. Her opened query
schema does not silently change. Offline lookup keeps those IDs; a fresh
coverage request requires Bob's authority again. A locally registered `drafts`
view has no accepted UUID until its descriptor is published upstream.

## Encodings

The new postcard `SyncMessage` variants append tags; earlier tags are unchanged:

- Tag 24, `jazz.sqlite.sync.catalogue-metadata.v1`: head, then a vector of schema
  records. This is accepted cached metadata from the trusted upstream, not a
  signature or fresh coverage assertion.
- Tag 25, `jazz.sqlite.sync.catalogue-fetch.v1`: request u64, table string, opened
  schema UUID, optional `(canonical author string, claims map)`.
- Tag 26, `jazz.sqlite.sync.catalogue-covered.v1`: request u64, exact head,
  opened schema UUID. Ordinary Answers cannot satisfy a catalogue request.

Three idempotently created SQLite tables hold postcard blobs, with no guessed
backfill of existing caches:

- `_jazz_catalogue_head_v1(slot=1,value)`, `jazz.sqlite.catalogue-head.v1`:
  namespace `[u8;32]`, publication through u64, active-schema revision u64,
  current schema UUID. These positions are catalogue positions, not row clocks.
- `_jazz_catalogue_schema_v1(version BLOB PRIMARY KEY,value)`,
  `jazz.sqlite.catalogue-schema.v1`: `SchemaVersion` (UUID plus its public-schema
  JSON string), then `PhysicalIdentityManifest`. Policies are removed from
  cached descriptors. The manifest is a sorted table-name map containing table
  UUID and sorted column-name map, each with column UUID and sorted enum-name
  map of occurrence UUID vectors. Defaults/index metadata may vary without
  changing lineage structure; published identities remain immutable.
- `_jazz_catalogue_table_v1(version BLOB,name TEXT,value, PRIMARY KEY(version,name))`,
  `jazz.sqlite.catalogue-table.v1`: the corresponding table identity value above,
  allowing indexed lookup without loading unrelated table manifests. Schema keys
  use raw 16-byte UUIDs; UUIDs inside postcard blobs use the existing UUID serde
  bytes encoding, including its length prefix.

`jazz.sqlite.catalogue-namespace.v1` is a BLAKE3 derive-key digest of postcard
`(genesis schema UUID, genesis manifest)`. It identifies the catalogue namespace;
it never derives table or column UUIDs from names or schema hashes. Persisted
caches reject root rebinding, backwards publication/revision positions and
conflicting immutable manifests. Metadata and head installation share one
savepoint. Stored decoders reject trailing bytes and malformed records.

`sqlite_catalogue_v1.json` pins all new wire tags and durable blobs, an
explicit authenticated scope, varint boundaries and the namespace digest.
Tests also inspect the actual installed SQLite values, corrupt stored records,
and deliver a successful answer followed by corrupt metadata in one sync tick.
The existing binding ABI is unchanged: IDs are 16 raw UUID bytes, an unknown
name is an empty byte array, and successful coverage is an empty byte array.
Real Chromium tests exercise both immediate lookup and pending wire coverage.

## Deployment limits

Upgrade all SQLite peers together. Tags 24–26 are not negotiated; older binaries
cannot decode them. Mixed-version deployment and downgrade are unsupported.
Existing authorities import their own persisted manifests; existing replicas
obtain them by normal metadata sync. No retired Groove or unreleased E2EE
backwards-compatibility guarantee is introduced.

Catalogue identity policy intrinsics are covered by E17’s
`sqlite-catalogue-policies-compatibility.md`. Authenticated catalogue cache seals,
offline journals, browser-worker storage admission and NAPI remain separate
prerequisites for the full E2EE SDK runtime. These scoped tests do not qualify
the end-to-end encrypted application path.
