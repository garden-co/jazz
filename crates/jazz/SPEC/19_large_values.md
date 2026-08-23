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

| Groove owns                               | Jazz owns                          |
| ----------------------------------------- | ---------------------------------- |
| inline/indirect encoding and locators     | high-level access/write APIs       |
| chunk storage and backend composition     | row/version authorization          |
| staging persistence and mechanics         | locator disclosure after auth      |
| immutable format, integrity and traversal | owner-row publication semantics    |
| edit tails and consolidation              | canonical row/version sync         |
| byte/text/JSON interpretation             | view lifecycle and settlement      |
| cache, leases and evaluator suspension    | policy-sensitive public errors     |
| durable refcounts and reclamation         | staging quotas/accept/evict policy |

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
  locator,         // random opaque Groove storage key
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

Locators are random with sufficient entropy as defense in depth, excluded from Groove logical
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
in its own unreachable staging generation. Jazz owns quota, expiry, acceptance,
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
and Groove can validate the bounded tree/descriptor. A failed/rejected mutation
publishes neither the row version nor root reachability.

Groove returns an opaque `StagedLargeValueId`, descriptor and accounting receipt.
After ordinary Jazz write authorization, Jazz adds that id to the same Groove
physical-record batch that inserts the owner version. Groove atomically verifies
the descriptor match, consumes staging ownership and installs durable root
references. Jazz rejection calls Groove's idempotent staging eviction API.
Acceptance is never a separate transaction before or after row publication.

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

## 19.5 Groove storage contract

Groove owns a policy-blind asynchronous chunk KV dependency. Jazz neither
implements nor wraps it. At minimum the byte plane supports exact immutable
operations:

```text
put_if_absent(locator, expected_hash, bytes)
get(locator)
delete(locator, expected_hash)
```

Groove owns locator allocation, integrity verification, quotas, staging,
refcounts and deletion scheduling. Jazz owns the policy decision that grants
descriptor roots and public error shaping. The storage implementation receives
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
frames. NAPI uses the same readiness future through an async-safe notification
to the Node event loop; a Rust-owned native socket may instead drive it from a
Tokio task. No environment must create a Rust thread, busy-poll, or re-enter a
locked `PeerConnection`. Disconnect completes outstanding hop-local requests
safely and discards late responses.

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
large values use the same idiomatic full-value API. Query options may request
byte slices, UTF-16 text slices or JSON pointers. Mutation options may express
append or splice operations; complete primitives replace complete values.

Rust exposes explicit byte and UTF-16 text coordinates. TypeScript exposes
UTF-16 text coordinates and byte coordinates for bytes. Invalid UTF-8 boundaries
and UTF-16 positions splitting surrogate pairs fail rather than round.

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
