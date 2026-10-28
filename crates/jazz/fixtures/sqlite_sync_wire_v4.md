# SQLite semantic sync encoding v4

`jazz.sqlite.sync.v4` carries catalogue metadata and fresh authority coverage
alongside main's ordinary SQLite sync messages. Every semantic frame starts with
five bytes: `4a 53 51 4c 04` (`JSQL` and version 4). The remainder is exactly one
canonical postcard `SyncMessage`; overlong varints and trailing bytes are refused.

The WebSocket Hello, structured Error, compression and frame-batch encodings are
unchanged. Those carrier frames do not gain this semantic prefix. Native hosts
use the same public semantic decoder; byte transports additionally apply their
per-frame and reassembly limits. Large Rows publications retain their existing
RowsPart framing and become visible only when their terminal Rows frame arrives.

## Message layout

The ordinary message tags 0 through 20 and their field bodies are unchanged from
`sqlite_sync_wire_v1.json`. This includes main's Fetch read-view field and its
ordinary relay receipts. The new messages append these tags:

| Tag | Message               | Body fields, in order                                      |
| --- | --------------------- | ---------------------------------------------------------- |
| 21  | `CatalogueMetadataV1` | catalogue head, vector of structural schema manifests      |
| 22  | `CatalogueFetchV1`    | request ID, table name, opened schema UUID, optional scope |
| 23  | `CatalogueCoveredV1`  | request ID, catalogue head, opened schema UUID             |
| 24  | `PolicyDescriptorV1`  | policy descriptor                                          |

The head, schema manifest and policy descriptor use the named v1 encodings in
[the catalogue storage contract](sqlite_catalogue_storage_v1.md). A scope is an
optional pair of canonical author string and ordered claims map, using the
existing protocol Value encoding. An ordinary connection may only request its
own authenticated author and claims. A SYSTEM backend connection may explicitly
delegate a scope. Missing connection claims inherit the host's claims; an
explicitly empty map does not.

Metadata delivery does not grant access to rows. Fresh coverage uses the normal
compiler and permission path with LIMIT 0 and returns no row bodies. A relay
forwards the request to its authority; its cached metadata cannot fulfil a fresh
request while offline. Lookup of already accepted physical IDs is a separate,
local operation.

A coverage receipt must match the exact accepted head and opened descriptor.
Rollback, reconnect, cancellation or a changed head prevents an outstanding
request from completing against stale or uncommitted metadata. Closed relay
sessions release their pending upstream requests. The runtime-neutral scheduler
hook requests an immediate tick and a deadline wake; hosts supplying that hook
must drive the database and poll pending work when those wakes fire.

Policy descriptors are cached and delivered for an exact head. Ordinary host
policies still follow their existing activation APIs. Private-owner adoption and
its fail-closed policy governor belong to the private-owner layer.

## Fixtures and compatibility

`sqlite_sync_wire_v4.json` contains 24 semantic message variants (RowsPart is
covered by framing tests), rejected fates, carrier frames, a scoped catalogue
request and malformed/historical rejection cases. Ordinary bodies are copied
unchanged from the historical pinned corpus and given the explicit v4 prefix.
The catalogue bytes in `sqlite_catalogue_wire_v4.json` are assembled from literal
field values and independently pinned storage records, without calling the
production serializer. The test port routes both Rust and the NAPI/WASM artifact
corpus through the versioned public decoder.

The historical unversioned corpus remains byte-for-byte unchanged. Unversioned
SQLite semantic frames and E2EE semantic versions 2 and 3 are refused. Version 3
cannot be reused: its enum layout differs from this extraction's main-based
layout. Future unknown versions are also refused before interpreting the body.

**Endpoints need matching semantic versions.** This change does not provide a
rolling-upgrade bridge for old SQLite clients. A current carrier Hello alone does
not establish semantic compatibility; a mismatched semantic frame closes the
link. Deployment must coordinate server and client artifacts before using this
layer. This branch is local preparation, not a deployment.

No existing SQLite row/history/chunk/pending-write encoding changes. In particular,
persisted internal SyncMessage records remain unprefixed, and their existing
variant tags and bodies are unchanged. Adding these network variants does not
change storage format 1. The previously extracted catalogue storage tables retain
their named v1 encodings.
