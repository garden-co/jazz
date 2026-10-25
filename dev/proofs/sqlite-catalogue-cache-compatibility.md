# SQLite authenticated catalogue capture, version 1

E14 implements `hasAuthenticatedCatalogue`, `takeAuthenticatedCatalogueState`,
`validateCatalogueCaptureReplacement` and cached-catalogue WASM opens. Previously
these calls rejected on SQLite. A host can now persist a capture outside an
account's row store and restore accepted physical UUIDs into a fresh offline
runtime. [Initialization seals](sqlite-initialization-compatibility.md) and
[foreground journal recovery](sqlite-foreground-initialization-compatibility.md)
are separate contracts.

## Trust and admission

Captures come from the accepted metadata installed by the ordinary upstream
protocol, or from an explicitly initialized authority. All typed handles share
one capture owner. `hasAuthenticatedCatalogue` never drains it. Taking state
moves the current capture once, even if the calling handle's requested schema
is not published. Readiness requires an accepted descriptor for that handle's
opened schema, not merely a current catalogue head. Pending state is restored
if a sync tick rolls its metadata back.

The existing SDK retains moved bytes across cache-publication failures and
exposes readiness only after its durable publication barrier. Its host cache
binds bytes to the exact registry authority, application and environment.
The core envelope is not a signature and cannot authenticate arbitrary input.
The Rust restore API is `unsafe` with this host-trust precondition; WASM's open
argument is the existing trusted host-cache boundary, after identity validation.
An invalid self-signed proof is rejected even with a valid catalogue capture.

Restoring a cache installs metadata and a durable admission marker together.
On a cache-bootstrapped runtime, reads, subscriptions and writes reject schemas
without accepted descriptors, including zero-row reads and queries prepared
before restoration. An accepted typed view can become ready independently of
an unpublished owner view. This check uses an indexed descriptor-presence query,
not a manifest decode or application-row scan on every operation. Ordinary
uncached local opens retain their existing admission behaviour.

A one-shot binding read at the remote tier, or a local-first read that may wait
for the authority, can refresh missing admission before preparing its query. This uses the normal
catalogue coverage request with the requested schema and read identity, then
rechecks admission through the ordinary query path. Local-only, immediate,
frozen and transaction reads do not fetch admission. A local-first timeout
includes the metadata wait; unavailable or unpublished schemas stay unusable.
The guard remains an indexed descriptor lookup and no encoding changes.

For example, Alice captures Bob's `notes.text` descriptor and opens offline
under a proposed `drafts.text` schema. She can look up the cached `notes` UUID,
but cannot use it to admit `drafts`, even after a SQLite restart. A typed `notes`
view is ready. Once Bob publishes a rename to `tasks.body`, her next capture
retains the old descriptor and the exact shared table/column UUIDs. Restoring
an older host cache into her newer durable store keeps the newer head and
re-exports the union for the host to publish.

Restoration does not install permissions, become an authority, mint IDs or
provide fresh catalogue coverage. A fresh coverage request still goes through
the normal authority/query/policy path. Cached memory opens cannot also request
creation of a standalone authority. Initialization seals, absence evidence and publish/cancel/status primitives
are covered by `sqlite-initialization-compatibility.md`. Browser-worker storage
admission and full journal orchestration through that owner remain unqualified.

## Encoding and compatibility

`jazz.sqlite.catalogue-capture.v1` consists of the exact ASCII bytes
`JAZZ-SQLITE-CATALOGUE`, NUL, byte `01`, followed by postcard encoding of:

1. `CatalogueHeadV1`, in the existing `jazz.sqlite.catalogue-head.v1` layout.
2. A vector of `CatalogueSchemaV1` records, sorted by schema UUID, in the existing
   `jazz.sqlite.catalogue-schema.v1` layout. This contains all locally accepted
   descriptors, not the authority's unreceived history. No rows, policy claims
   or coverage receipts are encoded.

The complete bytes are pinned in `sqlite_catalogue_capture_v1.json`, composed
from the independently pinned catalogue head and schema fixture. Decoding rejects
unknown headers/versions, truncation, trailing or noncanonical bytes, nil head
coordinates, unsorted or duplicate records, incomplete manifests and a head
whose current descriptor is missing. Replacement rejects a changed namespace,
backwards publication/revision positions, conflicting active revisions, dropped
accepted descriptors and rebound UUIDs. Source process/node rotation is allowed
when the persisted catalogue namespace and manifests remain the same. Rotating
the namespace within an existing host-cache scope is deliberately rejected.

Shared host caches merge separately authenticated captures under their existing
publication lock or transaction. `mergeCatalogueCaptures` retains the union of
accepted descriptors and the newer comparable head. An older publisher cannot
roll back the cache or erase descriptors learned by another owner. Overlapping
descriptors must keep their physical identities and lineage structure; namespace
changes, conflicting active revisions and incomparable head positions still fail.
The strict replacement validator retains its existing rejection rules.

For example, Alice may have received schema A and current schema C while Bob
received schema B and C. Either publication order produces the same A/B/C cache.
A later stale A-only capture keeps C as the current head. This merge does not
admit a live owner's unopened views, install policies or create fresh coverage;
normal core admission and authority receipts still govern those operations.
The result uses the same canonical v1 encoding and unchanged host envelopes.
The native relay, Node, browser and ephemeral cache all resolve against the
current stored bytes inside their atomic publication operation.

`jazz.sqlite.catalogue-cache-mode.v1` is the SQLite table
`_jazz_catalogue_cache_mode_v1(slot INTEGER PRIMARY KEY CHECK(slot=1))`.
The presence of its sole row `(1)` requires accepted-schema admission after
restart, even without a host-cache argument. Absence retains ordinary uncached
open behaviour. A failed cache import cannot leave this marker behind. The
fixture test inspects the actual stored row after a public SQLite open/restore.
No existing catalogue blob, wire tag, UUID or schema-ID encoding changes.

Export work is proportional to the explicitly requested capture: it visits the
accepted catalogue descriptors to serialize them, never application rows or
unreceived authority lineage history. Repeated observations without metadata
changes do not serialize another capture. Fresh cache-only opens do not echo
the installed bytes. Reopened durable roots export their accepted state again,
because the independent host cache may be behind that root.

This is a new unreleased SQLite format. Retired Groove captures are rejected;
there is no backwards compatibility or downgrade guarantee. An older SQLite
binary would ignore the admission marker and cannot implement this contract,
so hosts must upgrade the runtime and cache consumer together. The earlier
upgrade-all-peers requirement for catalogue sync tags still applies.

Tests exercise the public core lifecycle, rollback, malformed and incompatible
replacement, stale-cache restart, exact bytes, WASM memory/self-signed opens and
real Chromium IndexedDB flush/close/reopen. The
[browser-worker storage bridge](sqlite-browser-storage-compatibility.md) and
[React quickstart qualification](sqlite-browser-reload-qualification.md) now have
separate coverage. Catalogue capture tests alone do not qualify the full encrypted
SDK application, migrations or the unpublished native runtime port.
