# Groove-native large values decision log

This log records implementation forks that were not settled by the normative
Jazz and Groove specifications. It is append-only for the lifetime of the
feature branch; superseded entries are retained and marked as such.

## 2026-08-23 — Descriptor identity and transport correlation

The canonical identity of an uploaded value is its `LargeValueRef`. Jazz wire
messages do not carry a staging-claim or upload-attempt ID. The current peer
transport is ordered, does not duplicate messages within a connection, and
serializes outstanding large-value negotiations on each connection; reconnect
starts a new negotiation. Therefore a second correlation nonce would carry no
additional correctness information today. If a future transport permits
concurrent or duplicated same-descriptor exchanges on one connection, it must
add a hop-local message correlation field, not a canonical upload identity.

## 2026-08-23 — Restartable root-first frontier

Groove derives the next missing frontier from locally persisted, authenticated
nodes instead of persisting a separate frontier. This makes a start or retry
after restart self-describing and prevents frontier metadata from disagreeing
with chunk storage. Frontier results are bounded before they cross into Jazz.

## 2026-08-23 — Concurrent uploads of identical content

Staging claims are timestamped and fungible per exact descriptor. Concurrent
peers can receive the same authenticated frontier. If one peer installs it
first, Groove accepts another peer's stale response only when every no-longer-
missing node is byte-for-byte identical to the locally verified node. It then
issues an independent claim without recounting or rewriting those nodes.
Unproven or conflicting nodes remain rejected.

Malformed descriptor/node content is a descriptor-scoped protocol rejection,
not a peer-fatal error. Backend/storage failures remain connection errors so a
caller does not mistake unavailable infrastructure for invalid content.

## 2026-08-23 — Public partial-value API shape

Partial reads and edits are addressed by ordinary Jazz `(table, row, column)`
identity. Public APIs do not accept or return `LargeValueRef`, locators, staging
claims, or chunk handles. Jazz resolves the authorized current row and delegates
content work to Groove. Default reads still return ordinary owned primitives;
NAPI and WASM continue to copy completed results into host-owned memory.

## 2026-08-23 — One lowering seam for transaction kinds

Exclusive transactions lower oversized scalar cells through the same Groove
preparation helper used by mergeable publication before encoding their version
records. Publication then discovers and atomically accepts the matching live
claim from the descriptor, exactly as authority ingress does. Transaction kind
does not create a second large-value admission mechanism or put claim identity
on `CommitUnit`.

Exclusive overlays retain physical descriptors internally so an update to a
different column preserves exact locators. Their public read API hydrates those
cells to logical primitives, and commit accepts a descriptor-bearing cell only
when it exactly matches the transaction's projected base snapshot. Thus the
optimization neither exposes descriptors nor creates a handcrafted-descriptor
publication path.

## 2026-08-23 — Nullable scalar representation

Nullability remains a schema-level wrapper around the ordinary scalar. A
present nullable string or bytes value may therefore contain the same indirect
physical arm as its non-nullable counterpart. High-level range/edit APIs unwrap
that physical arm privately and preserve the nullable wrapper on publication;
`null` has no byte/text/JSON content and receives the ordinary type error.

## 2026-08-23 — Physical inheritance across schema views

An unchanged indirect cell is resolved in the schema version authoring the new
commit, not only the database's current write schema. Jazz projects the stored
winner through the registered lens while retaining the descriptor arm, then
requires exact descriptor equality. This preserves locators across historical
schema-view writes without allowing a caller to introduce a descriptor.

## 2026-08-23 — Binding codec version boundary

Adding Groove's explicit `Value::Large` arm intentionally changes the unreleased
postcard `Value` discriminants after `Bytes`. TypeScript's query-literal and
branch-selector encoders move those tags in lockstep. Physical string, bytes,
JSON, and public enum record fields now carry the inline/indirect scalar tag;
binding decoders consume exactly one inline tag and reject an indirect arm at a
logical result boundary. Tests and golden snapshots were updated rather than
adding a compatibility decoder for the unreleased format.

## 2026-08-23 — Invariant coverage boundary

The implementation checkpoint promotes only invariants exercised by concrete
tests. Locator-discovery authorization and all-retainer collection remain
explicit Jazz targets, and persistent reusable derived-state identity remains
an explicit Groove target. They are not silently treated as completed by the
high-level API slice: the current bearer-locator read model, refcount-aware row
mutation, streaming operators, and owned-result bindings are independently
covered, while the broader future capabilities retain `target | untested`
registry status until their own designs and adversarial suites land.

## 2026-08-23 — Reclamation and active evaluation coordination

The first collector uses a coarse database-wide ephemeral evaluation retainer,
not per-root persisted lease counts. A reclamation pass starts only when the
Groove chunk provider has no suspended requests or verified-byte leases, and
new requests wait behind an active pass. This closes the critical interval in
which an evaluator has authenticated a branch but has not requested all of its
descendants. The coarse guard may defer unrelated orphan work, but reclamation
is maintenance rather than foreground correctness work and the design avoids
persisting transient executor state. Per-root lease coordination remains a
future throughput optimization that must preserve the same safety boundary.

## 2026-08-23 — Default upload admission and staging lifetime

An unconfigured Jazz host admits at most 256 MiB of pushed chunk bytes per
one-second window and expires unaccepted completed roots after ten minutes.
The byte allowance matches the existing maximum logical wire-message size, so
one maximum-sized operation is not rejected merely because defaults are active;
larger sustained ingress must naturally span windows. The TTL is long enough
for slow push-then-row synchronization but finite, so forgetting to install a
host-specific policy no longer disables abandoned-upload collection. Hosts may
tighten either value without changing wire or Groove storage semantics.

## 2026-08-23 — First streaming-create API boundary

The first high-level streaming-create API is native Rust
`Db::insert_streaming_value`, backed directly by Groove's bounded streaming tree
builder and incremental staging channel. It accepts the other ordinary row
cells plus one streamed string, bytes, or JSON column and publishes only after
the stream and validation finish.

The TypeScript API uses the ordinary insert payload shape:
`await db.insertStreaming(table, { streamedColumn: source, ...otherData })`.
The schema DSL derives a separate streaming-init union from exact SQL column
metadata, replacing exactly one Text, JSON, or Bytea field with the source while
retaining every other ordinary insert constraint. This avoids treating UUIDs as
streamable merely because both UUID and Text map to TypeScript `string`. The
runtime schema infers the physical kind rather than trusting a caller tag.
Node/NAPI accepts a `ReadableStream` or `AsyncIterable` and now drives the same
resumable Groove push preparation as Browser/WASM. The adapter subdivides large
host chunks into bounded windows; Jazz meters each finalized encoded-node batch
before Groove persists it, and rejection evicts and closes the pending upload.
Producer failure aborts before any root is accepted. We still do not expose
pre-collected arrays or synchronous JS callbacks.

## 2026-08-23 — Streaming mutation parity

The object-shaped API extends to `updateStreaming` and `upsertStreaming` with a
separately derived streaming-update type: exactly one eligible column is a
source and all other fields are optional. The NAPI finish boundary carries one
mutation enum plus the ordinary trusted identity, `updatedAt`, head and base;
Jazz performs the normal existence, parent, inheritance and authorization work
after EOF. Insert derives its selector from authored `branchBy` cells and does
not accept a separate branch option. Update/upsert retain branch-view options
because a row UUID does not identify one branch-local version. Streaming a
branch column is rejected.

## 2026-08-23 — Browser/WASM streaming parity

WASM uses a Groove-owned resumable push preparation rather than buffering the
source or pretending that a single-threaded browser can provide a blocking
reader. Each awaited host push emits a bounded batch of canonical nodes and
Jazz persists it as a pending Groove upload before applying backpressure.
Incremental JSON syntax validation retains structural state but no token
contents, so a single huge JSON string does not become a whole-value buffer.
Finish registers the validated root and invokes the same insert/update/upsert
publication path as NAPI. The staging limiter clock is `web_time` so this path
is portable to WASM.
