# jazz — Specification · 19. Large-value capabilities

## Overview

Groove owns the logical and physical semantics and all storage lifecycle of
large values (Groove spec chapter 9). Jazz owns high-level access/write APIs,
sync, row/version authorization, locator disclosure, and staging acceptance or
eviction policy. Jazz never implements chunk storage, parses nodes, maintains a
stateful root-authorization registry, or coordinates durable collection.

Chunks are not Jazz rows and do not form a second Jazz query or synchronization
model. A Jazz row/version contains an ordinary Groove large-value descriptor.
Jazz treats its root locator and hashes as opaque physical fields. Authorized
disclosure of a locator is the read capability; there is no later operation- or
view-specific grant check. Jazz never parses content trees, applies edit
tails, computes UTF-16 metrics, interprets JSON, or decides which chunks an IVM
operator needs.

Each runtime column also freezes an internal large-value semantic kind at
schema lowering: bytes, text, or JSON. JSON remains the existing logical
string-shaped Groove value, but the physical-row descriptor carries its
schema-derived JSON context. Inline primitives and chunked descriptors do not
duplicate that context: their ordinary Groove enum schema is parameterized by
the column kind. Every independently addressed immutable tree node carries a
Groove-owned kind witness, checked against the schema-derived expected kind and
bound into the locator-independent logical hash, so a JSON root cannot be
replayed as text (or vice versa). The witness is not client-chosen or visible to
logical queries, policies, indices, or application results.

Invariant digest:

- `INV-CONTENT-1`: Jazz does not duplicate Groove's large-value semantics.
- `INV-CONTENT-2`: readable row versions disclose their root locators;
  authenticated traversal reveals descendants recursively.
- `INV-CONTENT-3`: chunk knowledge and cache presence do not widen row access.
- `INV-CONTENT-4`: staging grants no publication authority.
- `INV-CONTENT-5`: authorized owner-row publication is the only way to publish
  a new root in Jazz state.
- `INV-CONTENT-6`: Groove's chunk storage is replaceable and policy blind.
- `INV-CONTENT-7`: sync/view settlement accounts for required disclosed roots
  without turning chunks into canonical Jazz facts.
- `INV-CONTENT-8`: collection cannot remove chunks reachable from retained
  readable history or live staged/publication leases.

## 19.1 Cross-layer boundary

The ownership split is:

| Groove owns                               | Jazz owns                         |
| ----------------------------------------- | --------------------------------- |
| inline/indirect encoding and locators     | high-level access/write APIs      |
| chunk storage and backend composition     | row/version authorization         |
| staging persistence and mechanics         | locator disclosure after auth     |
| immutable format, integrity and traversal | owner-row publication semantics   |
| edit tails and consolidation              | canonical row/version sync        |
| byte/text/JSON interpretation             | view lifecycle and settlement     |
| cache, leases and evaluator suspension    | policy-sensitive public errors    |
| durable refcounts and reclamation         | ingress rate/expiry/accept policy |

Jazz's existing row/view authorization controls locator discovery: an authorized
descriptor reveals its root and authenticated Groove nodes reveal descendants.
There is no second stateful authorization registry. Groove talks directly to
local chunk storage. Jazz orchestration MUST NOT catch a `MissingChunk`, inspect
a request frontier, or retry a Groove evaluation closure; it may fulfill the
still-pending Groove request through the auxiliary sync channel in §19.6.

`INV-CONTENT-1`: tree shape, chunking, edits, logical comparison, materialization
and integrity validation have one implementation in Groove. Jazz MUST treat the
descriptor and all nodes as opaque values except for the root locator/hash fields
required to bind authorization and lifecycle.

## 19.2 Opaque traversal capabilities

Every retrievable node has two independent identities:

```text
NodeRef {
  object_hash,     // commits to exact encoded bytes and child locators
  locator,         // exact random 256-bit retrieval capability
}
```

The owner descriptor separately carries Groove's deterministic `logical_hash`.
Logical hashes identify equal values; object hashes authenticate the precise
locator-bearing retrieval graph. Neither hash grants retrieval authority.

The authorized Jazz row/version reveals the root `NodeRef`. Fetching a branch
reveals only the locators of its children. Fetching those children recursively
reveals exactly that tree. Groove's authorized storage path accepts exact
locators and does not expose
listing, prefix scan, hash lookup, cross-tenant existence tests, or locator
metadata.

Hashes are not authorization: predictable values can have guessable hashes.
The private blob backend may deduplicate exact objects by object/content hash, but its
public/proxied namespace is opaque locator to internal object. Multiple locators
may map to one physical blob without revealing that equality.

Locators are exactly 256 random bits drawn by Groove from the operating-system
CSPRNG as defense in depth, excluded from Groove logical
identity, and omitted or irreversibly redacted from logs, traces, analytics,
error messages and metrics labels. Unknown, expired and unauthorized locator
lookups expose indistinguishable public failure details.

`INV-CONTENT-2`: a subject authorized to observe a concrete row version may
receive that version's root locator and may recursively request locators learned
from successfully fetched, Groove-verified nodes beneath it.

`INV-CONTENT-3`: knowing a logical/object hash, possessing a locally cached blob, or
having read a different row/root MUST NOT authorize a locator. Every Groove
operation requests only locators discovered from its admitted descriptors and
authenticated parents. Possession after authorized discovery is sufficient.

## 19.3 Read and policy authorization

An authority evaluates ordinary Jazz read policy for a candidate row/version.
After success, its view may disclose the row's descriptor. A receiver installs
that descriptor into Groove; Groove then walks its own storage directly.

Read policies that themselves inspect a large value execute at an authority
whose internal capability may resolve the candidate root for policy evaluation.
The candidate row/root is not released to the reader until policy succeeds. A
partial client MUST NOT fetch an undisclosed root merely to decide whether it
may read that root; authority admission remains authoritative.

Chunk storage is private to Groove and receives no Jazz schema, subject, policy,
branch, or query context. Jazz grants roots to Groove rather than issuing raw
backend credentials or mediating individual chunk reads.

Locator non-discovery does not revoke plaintext already received by an authorized
client. The initial guarantee is non-discovery: removing authorization prevents
future root disclosure and proxy traversal through live Jazz sessions. Stronger
expiry or encryption/key revocation may be layered on without changing Groove's
logical format.

## 19.4 Upload, staging, and publication

Groove prepares a logical write, allocates locators, and stores immutable chunks
in its own unreachable staging generation. Jazz owns ingress rate limiting,
expiry, acceptance,
and eviction policy through Groove's staging API. Staging does not require or
imply permission to mutate a Jazz row.

Publication is:

```text
Groove prepares descriptor + chunks without visible mutation
  -> Groove stages immutable locator/hash/blob mappings
  -> Jazz evaluates ordinary row Insert/Update authorization
  -> Groove validates that the staged root is complete
  -> Jazz publishes the ordinary owner-row version
  -> Groove's physical-record batch installs the durable root reference
```

The row mutation remains the atomic Jazz value/conflict/history operation. Blob
staging before it is intentionally not atomic: unreachable immutable chunks are
harmless and expire. The row MUST NOT publish unless its exact root is available
and Groove can validate the bounded tree/descriptor. Finalization itself is
that admission boundary: regardless of prior staging call order, it validates
the complete authenticated reachable tree, canonically replays the edit tail
against that immutable base (including source-derived text coordinates and
whole-value-only JSON replacement), validates the final logical scalar, and binds
the pending upload to the exact canonical descriptor before issuing a receipt.
A pending upload's chunk journal or accounting cannot be reused to finalize a
different descriptor. A failed/rejected mutation publishes neither the row
version nor root reachability.

Groove persists timestamped retainer claims keyed by the completed descriptor.
After ordinary Jazz write authorization, the same Groove physical-record batch
that inserts the owner version consumes any live matching claim and installs a
durable root reference. Claims for the same immutable descriptor are fungible; a
descriptor-keyed concurrent upload may reuse already-local exact nodes after
its own pending record is bound to that descriptor. Upload-attempt identity is
neither canonical row state nor publication authority, and it cannot authorize
a different descriptor.
A rejected transaction consumes nothing, and its unclaimed retainer expires by
ordinary TTL maintenance. Acceptance is never a separate transaction before or
after row publication.

Sync is intentionally asymmetric. Upload is root-first push-before-row: the
writer starts with the complete large-value descriptor, then sends only the
bounded node batches the receiver reports missing. Groove authenticates the
root and discovers branch children from authenticated nodes. Its bounded
missing frontier is re-derived from persisted authenticated nodes after a
restart. The upload becomes `Staged` only when every
reachable node is locally present and the descriptor's kind, metrics, logical
hash, depth and fan-out validate. Only then does the writer send the ordinary
referencing `CommitUnit`. No sender `finish` assertion is trusted.
Concurrent peers may hold the same now-stale frontier. A receiver treats an
extra node as idempotent only when its exact locator, hash and encoded bytes
already match local storage; a conflicting or unauthenticated node yields the
descriptor-scoped `Rejected` result without poisoning the peer connection.
Download remains locator-driven pull with `ChunkRequestBatch` and
`ChunkResponseBatch`; download bytes are not upload ingress and do not consume
the upload rate limit. Edges terminate or forward push uploads before forwarding
the referencing row, just as missing pull requests may be relayed independently.

Groove persists each retainer claim's creation time and incoming byte/node
accounting. Jazz charges every upload against a simple incoming-byte
rate limit, including an idempotent upload whose immutable mappings already
exist. This bounds ingress work rather than retained physical storage. Jazz
queries opaque receipts to evict expired roots and never enumerates locators or
chunks. Expiry is performed only by explicit host maintenance. Ordinary chunk
push, finalization, and row acceptance check that the journal or receipt is
still present, but do not reject it based on wall-clock age. If maintenance has
removed it, the operation fails safely and the client must upload again; a
stale handle cannot recreate an evicted journal.

`RateLimited` is retryable backpressure, not rejection: the receiver retains
the descriptor-scoped pending claim and every previously accepted node, and the
sender retries the exact unaccepted node batch after a bounded admission delay.
It MUST NOT discard the upload or reject the referencing transaction merely
because one batch was rate limited. The staging maximum age is mandatory and
finite for both incomplete uploads and completed receipts; configuration may
tighten it but cannot disable expiry. Thus an abandoned resumable upload is
eventually reclaimed by the ordinary host maintenance pass.

The current wire result does not carry a receiver-provided retry-after. Until
it does, every sender waits the named, bounded one-second admission delay before
retrying the retained batch. That delay is a real host deadline, never a
`Deferred` microtask; unrelated peer work continues while it waits.

Jazz exposes the same policy setter and idempotent maintenance operation through
Rust `Db`, server-shell, NAPI, and WASM boundaries. Native servers/NAPI runtimes
invoke maintenance from their host timer; browser runtimes use a JavaScript
timer or worker alarm. A timer merely requests `evictExpiredStagedLargeValues`:
the host never receives staging ids, locators, chunks, or deletion authority.
Maintenance is the only TTL enforcement point and bounds retention of abandoned
uploads. Its eviction is serialized with upload continuation and receipt
consumption, so an operation observes either a present journal/receipt or its
absence rather than racing eviction into recreation.

The initial unconfigured policy admits 256 MiB of pushed bytes per one-second
window and expires unaccepted completed roots after ten minutes. The byte bound
matches the maximum logical wire-message size; deployments may configure tighter
product-specific limits through the same API.

Node/leaf uploads are immutable. Reusing a locator with different bytes is a
hard integrity failure. The backend may deduplicate equal bytes internally.

`INV-CONTENT-4`: successful staging MUST NOT create readable application state,
authorize a Jazz mutation, or make a root reachable from Jazz history.

`INV-CONTENT-5`: only a normally authorized Jazz Insert/Update that atomically
publishes an owner-row version may introduce a root into Jazz canonical state.
All logical write paths, including Rust `Db`, bindings, transactions, merge,
sync ingress and repair, MUST pass through the same Groove lowering/admission
boundary; no caller may publish an unlowered oversized value or handcrafted
descriptor.

Present nullable scalar cells use the same inline/indirect physical choice
inside their nullable wrapper. Partial reads and edits preserve that wrapper;
exclusive and mergeable transactions share the same lowering seam before
version publication.

## 19.5 Groove storage contract

Groove owns a policy-blind asynchronous chunk KV dependency. Jazz neither
implements nor wraps it. At minimum the byte plane supports exact immutable
operations:

```text
put_if_absent(locator, expected_hash, bytes)
get(locator)
delete(locator, expected_hash)
```

Groove owns locator allocation, integrity verification, staging, refcounts and
deletion scheduling. Jazz owns ingress/expiry policy, the decision that grants
descriptor roots, and public error shaping. The storage implementation receives
no Jazz policy context and may privately deduplicate by content hash.

Crash-consistent metadata—child edges, durable counts, staging generations and
the reclamation queue—must commit with Groove physical-record mutations. An
implementation may keep this metadata in Groove's ordered transactional store
while composing a simpler async byte KV for large blobs; exposing two unrelated
transaction owners to Jazz is forbidden.

Deployments may implement the backend with memory, filesystem, RocksDB, OPFS,
S3, R2, another blob service, or a composition of local cache and remote store.
Backend choice and completion timing do not alter schema, query, authorization,
history, conflict, or synchronization semantics.

`INV-CONTENT-6`: no chunk storage implementation may evaluate Jazz policy or
understand Jazz schemas/queries. Replacing it MUST preserve exact immutable KV,
durable metadata and availability semantics.

## 19.6 Sync, views, and completeness

Canonical Jazz sync carries the authorized row/version descriptor, not every
chunk as a row or fact. The descriptor discloses the root locator only inside
the authority-approved view. Groove evaluation on the receiver then requests
required descendants through its installed capability.

A data subscription may publish independent results whose Groove closures are
complete while another result is blocked on chunks. A particular result is not
published until Groove proves all chunks required by its operators. Jazz does
not reinterpret that pending state as row absence, predicate falsehood, or
subscription settlement.

Settlement has two distinct conditions:

1. Jazz has received and verified its complete canonical row/view closure and
   authority frontier under the normal sync protocol.
2. Groove has reached quiescence for the terminal's currently demanded chunks.

Chunk cache eviction after publication does not retract settlement or logical
data. A later operation may suspend to reload evicted bytes. Permanently missing
or corrupt required chunks end the affected low-level terminal/query with a
typed failure; Jazz's existing high-level recovery may reinstall only where its
continuity rules permit.

`INV-CONTENT-7`: chunks MUST NOT become a second canonical sync fact format, but
Jazz MUST NOT report a terminal/result settled while its exact Groove evaluation
is blocked on required chunks.

### 19.6.1 Auxiliary chunk-demand channel

When Groove misses local cache/storage, its original evaluation future remains
pending and invokes a Jazz-installed `MissingChunkResolver`. Jazz multiplexes
opaque batches of `(request_id, locator, expected_object_hash)` over the peer
connection and completes that resolver future from response batches. Jazz does
not catch/retry an evaluation error or inspect node bytes. Groove authenticates
responses, installs valid bytes/edge metadata locally, coalesces consumers, and
wakes dependent evaluations.

A receiving peer first asks its Groove instance for an exact local chunk. On
local miss Jazz forwards the request strictly upstream along the authority
topology. Request ids are hop-local; mappings and responses follow the reverse
path. Every hop coalesces identical requests, applies request/byte backpressure,
propagates cancellation when its last consumer disappears, and decrements a hop
limit. Intermediate transport failures are retryable; only the terminal
authority returns definitive normalized unavailability. Shared-store deployments
normally satisfy the first local lookup and use the same protocol unchanged.

Jazz performs that local check through a cloneable Groove-owned exact-read
service, not by retaining or invoking the configured byte-KV backend. The
service exposes neither staging nor deletion. This lets the auxiliary pump
remain independent of the Jazz node lock without moving storage ownership back
across the boundary.

Relayed bytes create no durable reference at an intermediary that owns no row;
they may pass through or enter Groove's bounded cache. Forwarding never proceeds
sideways or downstream and initially never recursively searches arbitrary peers.
These messages are auxiliary transport, not canonical Jazz facts or frontiers.

Auxiliary transport progress is independent of Jazz semantic ticks. Each peer
link exposes an executor-neutral I/O pump with three operations: route an
incoming decoded auxiliary message, drain bounded outbound auxiliary batches,
and await outbound readiness. The pump never acquires the Jazz node-state lock.
Canonical messages bypass it and remain queued for `Node::tick`. This separation
is required because a semantic tick may itself be suspended in Groove awaiting
the chunk response that the pump must deliver.

Bindings retain the pump beside their socket. Browser/WASM websocket callbacks
route inbound frames and use a microtask awakened by outbound readiness to drain
frames. NAPI polls database work without blocking the JavaScript thread and
routes auxiliary frames independently. A native server keeps Groove and its
non-`Send` storage capabilities on the dedicated shell thread; that thread runs
a local async executor whose wire-demultiplexer task continues while a semantic
tick is suspended. The Tokio socket remains a byte carrier and wakes that local
task when it stages inbound data. No environment busy-polls, moves Groove's
backend across threads, or re-enters a locked `PeerConnection`. Disconnect
completes outstanding hop-local requests safely and discards late responses.

In-process semantic transports use the same pump: upstream and subscriber
`PeerConnection::tick` calls drain its bounded queues as part of their normal
non-blocking service pass. A shared resolver exposes only whether local chunk
demand is pending and a monotonic completion generation. Jazz uses those cheap
signals to poll retained Groove query work and refresh affected subscriptions;
quiescent semantic ticks do not scan unrelated subscriptions. Binding-owned
socket drivers may drain the same queue concurrently because taking a batch is
atomic, and transport backpressure restores the batch before retry.

## 19.7 History, retention, and collection

Jazz has no large-value-specific durable root registry. A Jazz version is an
ordinary Groove physical record, and Groove's persisted record mutation owns
the descriptor reference delta described in Groove chapter 9. Jazz updates and
logical deletes append versions and therefore do not release old roots. The
existing edge-cache eviction and rejected-version cleanup paths physically
delete or move versions; their ordinary Groove batches account for descriptors
like every other physical-record mutation.

Jazz supplies authorization-scoped retrieval capabilities but does not trace
trees, rebuild reference counts from Jazz history, or coordinate collection.
Groove and the content backend own immutable child-edge counts, staging
protection, active leases, resumable zero-count cascades, and authenticated
audit/rebuild traversal. NAPI/WASM host-owned result copies add no backend lease.

`INV-CONTENT-8`: Jazz MUST NOT bypass Groove's refcount-aware physical-record
mutation path when inserting, deleting, moving, evicting, or recovering a row
version containing descriptors.

## 19.8 Binding and public API consequences

Schemas continue to declare ordinary string, bytes and JSON columns. Small and
large values use the same idiomatic full-value API. The TypeScript public
surface is the typed query and mutation DSL; it MUST NOT expose imperative
large-value read, append, or splice helpers on `Db` or `JazzClient`.

A `select` object may request one partial scalar per selected field:

```ts
app.things.where({ id: thingId }).select({
  byteField: { from: 1_000_000, to: 2_000_000 },
  textField: { from: 4, to: 124 },
  textFieldUtf8: { fromUtf8: 4, toUtf8: 67 },
  jsonField: { at: "/someKey/11/otherKey" },
});
```

Ranges are half-open `[from, to)`. Byte ranges apply only to bytes; `{from,to}`
on text is in UTF-16 code units; `{fromUtf8,toUtf8}` opts into UTF-8 bytes.
`{at}` applies only to JSON and is an RFC 6901 JSON Pointer. Results remain an
ordinary `Uint8Array`, `string`, or decoded JSON subtree. Selecting a field by
name (or omitting `select`) materializes the complete primitive as before.
The query IR retains the descriptor now, and the binding validates and applies
its public coordinate/pointer contract. Until exact per-terminal demand
propagation is completed, the binding may obtain the selected carrier column
and slice that value at the binding boundary; it must not resolve unrelated
columns or treat that fallback as a chunk-demand model. Issue #2090 tracks
replacing that temporary carrier fallback with exact terminal demand into
Groove. It does not gate the executability or correctness of this public API.

`Db.applyDiffs` accepts partial mutations, while `Db.update` retains ordinary
whole-column replacement semantics:

```ts
db.applyDiffs(app.things, thingId, {
  byteField: { within: bytePage, splices: [{ at: 4, delete: 3, insert: bytes }] },
  textField: { within: textPage, splices: [{ at: 3, delete: 1, insert: "x" }] },
  textFieldUtf8: { within: utf8Page, splices: [{ atUtf8: 3, deleteUtf8: 1, insert: "x" }] },
  jsonField: { edits: [{ op: "set", at: "/someKey/11", value: next }] },
});
```

`within` is the same range descriptor used by the read and makes splice
coordinates relative to its beginning. Splices are applied sequentially, so
each later coordinate addresses the result of previous splices. A deletion
MUST stay within the selected page; insertion at its end is allowed. Text
defaults to UTF-16; UTF-8 splice fields opt in explicitly. Bounds, integer
overflow, UTF-8 boundaries, and UTF-16 surrogate splits fail rather than round.
JSON edits use RFC 6901 pointer escaping, fail for a missing path or incompatible
schema/kind/nullability, and lower to Groove's ordinary binary edit-tail model;
they do not create a JSON-specific storage model. All fields in one `applyDiffs`
commit atomically. There is deliberately no page staleness/CAS promise in this
API revision.

The Rust/Jazz binding layer may retain internal primitives at its translation
boundary, but authorization occurs before resolving a private physical cell and
unmodified columns inherit their exact descriptor without hydration.

Native Rust additionally exposes `Db::insert_streaming_value`. The caller
supplies the ordinary non-streamed row cells, the target column, and a
`std::io::Read`; Jazz derives the scalar kind exclusively from that column's
schema. A bounded producer bridge feeds the same resumable push
constructor and persisted pending-upload lifecycle used by NAPI and WASM; there
is no second reader-specific staging path. Jazz charges each finalized batch
before Groove persists it. Jazz does not publish the row until EOF, complete
text/JSON validation, staging-policy admission, and ordinary row-write
admission have succeeded. Terminal reader, validation, admission, and
publication failures immediately evict the pending claim; cancellation may
leave only that expiring claim. The API validates the target column and
physical kind before consuming the reader and preserves a present nullable
wrapper where required.

TypeScript exposes the mutation family with ordinary payload shapes as
`Db.insertStreaming(table, { streamedColumn: source, ...otherData })`. From the
exact DSL column metadata, each typed table derives a separate streaming-init
union with one Text, JSON, or Bytea column replaced by a required stream source;
all other columns retain their ordinary insert types and required/defaulted
status. UUID and other columns whose TypeScript values merely resemble a
streamable scalar are therefore rejected statically. The runtime schema remains
the final authority and determines the physical kind without a caller-supplied
tag. Sources are `ReadableStream<Uint8Array | string>` or
`AsyncIterable<Uint8Array | string>`; Bytea accepts only byte chunks. The
operation returns a promise for a write handle containing the generated row id,
not a materialized copy of the streamed value.

`Db.updateStreaming(table, rowId, patch)` and
`Db.upsertStreaming(table, rowId, values)` use a derived streaming-update union:
one stream is required and every other field is optional, matching the ordinary
update/upsert surface. All three mutations carry ordinary trusted identity and
custom `updatedAt` context. Insert derives its exact branch selector from the
non-streamed `branchBy` cells; streaming a branch column is rejected. Update and
upsert accept the ordinary head/base branch-view options because one `RowUuid`
may exist in several branch coordinates. Jazz resolves existence, inheritance,
parents and authorization only after the producer finishes, so a slow upload
cannot commit against stale pre-upload row state. Publication accepts the staged
root in the same ordinary row-version commit.

NAPI and Browser/WASM both use Groove's resumable push constructor. The adapter
subdivides arbitrarily large host chunks into bounded windows; each awaited
`push` advances content-defined chunking, incrementally validates UTF-8/JSON
syntax, and asks Jazz to charge the finalized encoded-node batch against the
ingress window before Groove persists it under the pending upload. Rejection
evicts and closes that upload before the over-limit batch is written. Explicit
abort and terminal validation failure also evict the persisted pending claim
immediately, releasing its chunk retainers without waiting for TTL. `finish`
registers the validated root without charging its already-metered nodes twice
and performs the same ordinary Jazz mutation lifecycle. Neither binding
collects or spools the whole logical value or holds a Jazz transaction open
while JavaScript produces data.

NAPI and WASM preserve the existing encoded-row boundary and copy completed
results into host-owned buffers. No chunk lease crosses either binding. Complete
string, bytes and JSON values decoded by TypeScript therefore live inside the
host-owned row copy rather than a Rust-backed external buffer.

The public API does not expose locators, integrity hashes, tree nodes, cache
leases, retry tokens, partially materialized handles, or Groove request state.

## Open questions

- Whether strong post-authorization expiry requires encrypted chunks and
  root-scoped key delivery; the initial bearer-locator boundary prevents new
  discovery but cannot revoke plaintext already received.
- Exact retention frontier and collection cadence for each deployment tier.
- Any future Jazz history truncation/thinning design must express removal as
  ordinary refcount-aware Groove physical-record deletions, preserve versions
  still required by snapshots/branches, and define crash-consistent batching
  with its truncation frontier. Direct storage deletion is forbidden.
- Whether direct signed blob downloads are worth adding after the proxied path
  is measured.
- Whether a future multipart/handle binding protocol should expose chunk-backed
  `Blob`/bytes results. It is outside the current design and would require host
  finalizers, external-memory accounting and lease-aware backpressure.
