# Runtime And Public Boundary

## 21. Semantic System Fields

Semantic system fields may be exposed with `$` names:

```text
id
$createdAt
$updatedAt
$createdBy
$updatedBy
```

`id` is the ordinary public row id field. `$createdAt`, `$updatedAt`,
`$createdBy`, and `$updatedBy` are the selected baseline semantic system fields.
Queries must be able to filter and sort over both user columns and semantic
system fields, including `$createdAt` and `$updatedAt`. Transaction/version
metadata such as public transaction ids may be exposed later through explicit
metadata APIs rather than as ordinary row fields by default.

Physical application row tables use `j_` engine columns. Pure system tables do
not need the `j_` prefix because all their columns are engine-owned.

User columns whose names collide with the reserved physical prefix are escaped
by the layout codec.

Open issues:

- which fields are queryable, synced, or policy-protected by default

## 22. Product Runtime And Topology

The semantic runtime roles are:

- local replica
- trusted peer / edge
- global authority

Runtime topology changes where storage lives and where queries settle. It must
not change query, write, policy, branch, or sync meaning.

The Rust runtime is the semantic boundary for one-shot query execution and live
subscriptions. Platform bindings should expose generic query/subscription entry
points over the runtime's semantic descriptor shape instead of duplicating query
semantics in each host language.

The default browser/app binding surface should stay at the semantic API level:
writes, `query`, `one`, `subscribe`, and storage/runtime administration. Lower
level peer-transport hooks belong behind an explicit sync topology API rather
than appearing as ordinary application methods.

Query descriptor values crossing JS/WASM boundaries must be parsed as bounded
values before execution. Numeric window fields such as limit and offset must be
finite, non-negative integers representable by the runtime and SQLite execution
types. Invalid or oversized values fail at the boundary instead of wrapping,
truncating, or changing the requested page.

Browser durable mode may use:

- main-thread in-memory runtime
- durable worker runtime
- SharedWorker or tab broker

Browser WASM is a binding and storage topology, not a separate product model.
It may run against memory-only SQLite or browser-durable storage such as OPFS,
and it may live on the main thread or inside a worker. Those choices affect
latency, persistence, and crash behavior, but not row semantics, query results,
subscription diffs, transaction outcomes, or sync facts.

The main thread may run queries directly against an in-memory core. In durable
browser topology, each tab may have its own in-memory SQLite node connected to a
shared durable worker/tab-broker node as a trusted upstream peer. The
worker/broker owns durable storage and upstream sync.

The baseline mirroring strategy is active-scope mirroring: main-thread tabs
mirror only the query scopes needed for immediate synchronous UI. The durable
worker/broker owns the broader durable cache, retained history, reconnect
state, and upstream sync. Larger or less latency-sensitive queries may execute
directly against the worker asynchronously and deliver results/events to the UI.
Async worker-backed subscriptions are an implementation recommendation, not a
separate semantic subscription model.

Memory-only runtimes are first-class for tests, demos, and the full distributed
system harness. The important property is controllable topology and
in-memory-ness, not browser APIs.

Edges may permanently reject mergeable transactions when schema validation,
policy evaluation, quotas, or other receive-time checks fail. Edge policy
evaluation may be slightly stale with respect to permission-influencing rows;
that staleness is an accepted product tradeoff for mergeable transactions.

The global authority owns global epochs, exclusive transaction
acceptance/rejection, global durability, and catalogue publication.

Hosted apps have app id, sync URL, global authority placement, optional edge
placement, catalogue heads/revisions, hosted auth configuration, quotas, upload
limits, and observability namespace.

Storage is isolated by app, environment/namespace, and storage driver. A runtime
must not accidentally share row history, transaction state, auth secrets, or
catalogue state across apps or namespaces merely because they use the same
physical browser/native storage backend.

Transport should stay thin. It carries typed sync and catalogue messages; it
does not implement a second query engine.

The sync protocol version is bumped when the message contract changes. The
client transaction upload protocol is version `2` and requires the server to
advertise `ProtocolCapabilities { tx_upload: true, ... }`. Clients that need
upload reject a server hello without that capability instead of silently
falling back to download-only behavior.

Reconnect should use replay-window recovery first and full scope/frontier
snapshot fallback when the replay window is insufficient. Active subscriptions
are desired state and should be replayed on reconnect. Query descriptors are
not durable disk state: after a tab or worker restart, downstream live
subscriptions replay to the worker/broker, then trickle upstream from there.
Local transaction upload state is different: ordinary committed local
transactions are durable retry state, so reconnect scans the upload registry and
replays active transaction uploads even when there are no active
subscriptions.

Open issues:

- how edges discover policy-influencing rows
- edge policy-readiness/freshness model
- replay-window and reconnect encoding
- SharedWorker/tab-broker ownership handoff
- SQLite WASM startup and binary-size constraints
- OPFS/locality behavior
- React Native/native packaging constraints

## 23. Files, Images, And Binary Data

Files are product-visible as row-modeled assets. Applications should be able to
declare conventional file metadata and chunk/part tables, use normal row
permissions and relation inheritance for access, and load authorized file bytes
through product APIs such as `loadFileAsBlob`. The byte storage mechanics may
move out of row history, but the product shape remains relational and
policy-controlled.

The core requirements are:

- rows may reference external blobs
- blob metadata is ordinary policy-controlled relational data
- blob durability may gate transaction publication at a tier
- blob fetch must be authorized through the same session/policy model
- immutable blob chunks may be shared by digest across branches

File bytes may live in SQLite blobs, OPFS/blob storage, object storage,
filesystem storage, or another byte store.

File content is immutable in v0. Replacing a file creates a new content version.

For now, query-scoped sync may include file bytes when scoped rows reference
files and the receiving session is authorized. Future protocols may use
authorized fetch handles or separate blob transfer.

Deletes or permission changes on owning rows may cascade to file access
according to declared relation semantics. File serving must re-check session and
policy rather than treating stored bytes as public once uploaded.

Open issues:

- exact conventional schema for file and part tables
- product API shape for loading authorized file bytes
- upload limits and validation
- partial/resumable upload protocol
- mutable file/chunk strategy
- whether chunks are ordinary rows or specialized byte-store entries

## 24. Errors And Explanations

Errors are structured, discriminable, and usable from write promises and global
runtime callbacks.

Application-facing surfaces:

- write promise rejection
- transaction outcome rejection
- global rejection/error callback
- subscription error callback
- query failure
- sync connection error

Promise rejection and global callback should receive the same error object shape
for the same transaction outcome.

Errors carry stable machine codes plus human-readable messages. Human messages
may evolve; machine codes are the compatibility surface.

Likely machine-code families:

- `policy_denied`
- `constraint_failed`
- `conflict_rejected`
- `schema_missing`
- `schema_incompatible`
- `catalogue_missing`
- `permission_missing`
- `transport_failed`
- `quota_exceeded`
- `storage_failed`
- `invalid_transaction`
- `exclusive_requires_global`
- `auth_failed`

Transaction rejection details are durable side data keyed by transaction id.
They are not a wide field on the hot transaction row.

Write promises, explicit transaction waits, transaction-info APIs, and global
rejection/error subscriptions are all views over durable transaction outcome and
rejection records. A write promise may reject immediately when the local runtime
or waited tier observes rejection. A global callback/subscription surfaces
rejected transactions discovered later, including unawaited writes and
sync-delivered outcome changes. If safe rejection detail is later enriched for
the same public transaction id, rejection subscriptions should be able to emit
that update without changing the transaction id.

Policy denial and validation explanations should be as detailed as safe without
leaking privileged information. Ordinary clients must not distinguish hidden
rows from nonexistent rows through error detail. Trusted-peer and authority logs
should preserve richer details.

For ordinary untrusted clients, policy and validation rejection detail should be
minimal: a stable code such as `permission_denied` and the attempted write that
failed, identified by table, row id, and operation within the transaction.
Details such as hidden dependency row ids, recursive policy paths, and whether a
particular hidden row exists are privileged diagnostics and belong only on
trusted-peer or authority-side surfaces.

Developer diagnostics may be richer and less stable than application errors.
Useful diagnostics include SQL lowering traces, policy lowering traces, missing
index advice, recursive policy unsupported-shape reports, schema/lens graph
errors, generated physical layout explanations, and subscription invalidation
explanations.

Open issues:

- exact stable code taxonomy
- public error object shape
- timeout defaults for unsettled queries/subscriptions
- redaction rules

## 25. Wire/Public Boundary

APIs and wire protocols use public ids.

Hot storage may use physical integer surrogates for:

- nodes
- transactions
- rows
- branches
- tables, schemas, and columns

On export:

```text
physical ids -> public ids -> bundle
```

On incoming sync:

```text
bundle public ids -> physical ids -> embedded database writes
```

Physical ids must not leak into public equality, ordering, persistence, or sync
semantics.

The identity codec should be centralized. SQL-generating subsystems must not
invent ad hoc conversions.
