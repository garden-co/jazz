# jazz — Specification · 19. Large-value capabilities

## Overview

Groove owns the logical and physical semantics of large values (Groove spec
chapter 9). Jazz owns distribution, authorization, row/version publication,
durability, and the host capability through which Groove retrieves or stages
immutable chunks.

Chunks are not Jazz rows and do not form a second Jazz query or synchronization
model. A Jazz row/version contains an ordinary Groove large-value descriptor.
Jazz treats its root locator as an opaque bearer capability and its hashes as
opaque identity/integrity fields. Jazz never parses content trees, applies edit
tails, computes UTF-16 metrics, interprets JSON, or decides which chunks an IVM
operator needs.

Invariant digest:

- `INV-CONTENT-1`: Jazz does not duplicate Groove's large-value semantics.
- `INV-CONTENT-2`: readable row versions authorize their disclosed root
  locators; traversal reveals descendants recursively.
- `INV-CONTENT-3`: chunk knowledge and cache presence do not widen row access.
- `INV-CONTENT-4`: staging grants no publication authority.
- `INV-CONTENT-5`: authorized owner-row publication is the only way to publish
  a new root in Jazz state.
- `INV-CONTENT-6`: the chunk backend is replaceable and policy blind.
- `INV-CONTENT-7`: sync/view settlement accounts for required authorized roots
  without turning chunks into canonical Jazz facts.
- `INV-CONTENT-8`: collection cannot remove chunks reachable from retained
  readable history or live staged/publication leases.

## 19.1 Cross-layer boundary

The ownership split is:

| Groove owns                            | Jazz owns                               |
| -------------------------------------- | --------------------------------------- |
| inline/indirect storage encoding       | row/version authorization               |
| FastCDC and recursive content grouping | root-locator disclosure                 |
| immutable node format and integrity    | chunk proxy/backend selection           |
| edit tails and consolidation           | staging quotas and expiry               |
| byte/text/JSON interpretation          | authorized owner-row publication        |
| per-operator lazy demands              | sync/view lifecycle and settlement      |
| chunk validation                       | durable root reachability accounting    |
| evaluator suspension and resumption    | blob credentials and operational policy |
| cache and evaluation leases            | long-term unreachable-object collection |

Jazz supplies an implementation of Groove's chunk capability when it opens the
Groove database/operation. Groove awaits that capability inside its evaluation
session. Jazz query, policy, mutation, and subscription orchestration MUST NOT
catch a `MissingChunk` error, inspect a request frontier, or retry a Groove
evaluation closure.

`INV-CONTENT-1`: tree shape, chunking, edits, logical comparison, materialization
and integrity validation have one implementation in Groove. Jazz MUST treat the
descriptor and all nodes as opaque values except for the root locator/hash fields
required to bind authorization and lifecycle.

## 19.2 Opaque traversal capabilities

Every retrievable node has two independent identities:

```text
NodeRef {
  object_hash,     // commits to exact encoded bytes and child locators
  locator,         // random bearer capability, Jazz routed
}
```

The owner descriptor separately carries Groove's deterministic `logical_hash`.
Logical hashes identify equal values; object hashes authenticate the precise
locator-bearing retrieval graph. Neither hash grants retrieval authority.

The authorized Jazz row/version reveals the root `NodeRef`. Fetching a branch
reveals only the locators of its children. Fetching those children recursively
reveals exactly that tree. The proxy accepts exact locators and does not expose
listing, prefix scan, hash lookup, cross-tenant existence tests, or locator
metadata.

Hashes are not authorization: predictable values can have guessable hashes.
The private blob backend may deduplicate exact objects by object/content hash, but its
public/proxied namespace is opaque locator to internal object. Multiple locators
may map to one physical blob without revealing that equality.

Locators are random with sufficient entropy, excluded from Groove logical
identity, and omitted or irreversibly redacted from logs, traces, analytics,
error messages and metrics labels. Unknown, expired and unauthorized locator
lookups expose indistinguishable public failure details.

`INV-CONTENT-2`: a subject authorized to observe a concrete row version may
receive that version's root locator and may recursively request locators learned
from successfully fetched, Groove-verified nodes beneath it.

`INV-CONTENT-3`: knowing a logical/object hash, possessing a locally cached blob, or
having read a different row/root MUST NOT authorize a locator. Every Groove
operation consumes chunks only through the capability installed for its exact
Jazz read/authorization context.

## 19.3 Read and policy authorization

An authority evaluates ordinary Jazz read policy for a candidate row/version.
After success, its view may disclose the row's root locator. A receiver installs
that locator into the capability for the authorized view/session and can walk
the tree through the Jazz proxy.

Read policies that themselves inspect a large value execute at an authority
whose internal capability may resolve the candidate root for policy evaluation.
The candidate row/root is not released to the reader until policy succeeds. A
partial client MUST NOT fetch an undisclosed root merely to decide whether it
may read that root; authority admission remains authoritative.

The first implementation proxies chunk requests through Jazz infrastructure.
The blob store is private and receives no Jazz schema, subject, policy, branch,
or query context. Direct blob-store downloads with signed short-lived grants are
possible later but are not required by this format.

Bearer locators do not revoke plaintext already received by an authorized
client. The initial guarantee is non-discovery: removing authorization prevents
future root disclosure and proxy traversal through live Jazz sessions. Stronger
expiry or encryption/key revocation may be layered on without changing Groove's
logical format.

## 19.4 Upload, staging, and publication

Groove prepares a logical write into a physical descriptor plus immutable
chunks. Jazz proxies those chunks into an unreachable staging namespace under
fresh locators. Staging is quota limited and expiry bound but does not require
or imply permission to mutate an application row.

Publication is:

```text
Groove prepares descriptor + chunks without visible mutation
  -> Jazz stages immutable locator/hash/blob mappings
  -> Jazz evaluates ordinary row Insert/Update authorization
  -> Groove/Jazz validate that the referenced staged root is complete
  -> Jazz publishes the ordinary owner-row version
  -> Jazz marks the root publication reachable
```

The row mutation remains the atomic Jazz value/conflict/history operation. Blob
staging before it is intentionally not atomic: unreachable immutable chunks are
harmless and expire. The row MUST NOT publish unless its exact root is available
and Groove can validate the bounded tree/descriptor. A failed/rejected mutation
publishes neither the row version nor root reachability.

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

## 19.5 Backend contract

The minimal private backend supports:

```text
stage(locator, expected_hash, bytes)
get(locator)
retain(publication_or_lease, locator/root)
release(publication_or_lease)
collect_unreachable(before_frontier)
```

The Jazz proxy owns authentication, quotas, rate limits, routing and public
error shaping. Groove owns verification of returned bytes. The backend owns
durable immutable storage and private content-hash deduplication.

Deployments may implement the backend with memory, filesystem, RocksDB, OPFS,
S3, R2, another blob service, or a composition of local cache and remote store.
Backend choice and completion timing do not alter schema, query, authorization,
history, conflict, or synchronization semantics.

`INV-CONTENT-6`: no backend may need to evaluate Jazz policy or understand Jazz
schemas/queries. Replacing it MUST preserve the exact locator/immutability and
availability contract.

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

## 19.7 History, retention, and collection

Every retained Jazz row version containing a large-value descriptor is a root
of reachability. Current rows, retained history, branch/snapshot references,
pending transactions, staged mutations, active sync/view sessions, in-flight
chunk requests, and externally retained chunk-backed results may add leases or
retention constraints.

Collection is tracing or equivalent conservative accounting from published
root locators and live staging/operation leases. It may process trees and
history asynchronously with bounded memory. False retention is acceptable;
premature collection is not. Internal content-hash deduplication requires the
backend to retain a physical blob while any locator references it.

Historical thinning, if later introduced, releases roots only when the owning
Jazz versions cease to be retained under the history specification. Chunk
collection cannot weaken snapshot or branch-view readability.

`INV-CONTENT-8`: collection MUST NOT remove a locator/blob reachable from any
retained owner-row version or live staging, evaluation, sync, publication, or
result lease. Recovery MUST reconstruct conservative reachability before
collection resumes.

## 19.8 Binding and public API consequences

Schemas continue to declare ordinary string, bytes and JSON columns. Small and
large values use the same idiomatic full-value API. Query options may request
byte slices, UTF-16 text slices or JSON pointers. Mutation options may express
append or splice operations; complete primitives replace complete values.

Rust exposes explicit byte and UTF-16 text coordinates. TypeScript exposes
UTF-16 text coordinates and byte coordinates for bytes. Invalid UTF-8 boundaries
and UTF-16 positions splitting surrogate pairs fail rather than round.

Complete string and parsed JSON results are ordinary owned primitives. Complete
bytes may copy into an owned buffer or use a chunk-backed host `Blob`/external
buffer representation. If a binding transfers zero-copy chunk leases, it must
account them as external memory and release them through host finalization.

The public API does not expose locators, integrity hashes, tree nodes, cache
leases, retry tokens, partially materialized handles, or Groove request state.

## Open questions

- Whether strong post-authorization expiry requires encrypted chunks and
  root-scoped key delivery; the initial bearer-locator boundary prevents new
  discovery but cannot revoke plaintext already received.
- Exact retention frontier and collection cadence for each deployment tier.
- Whether direct signed blob downloads are worth adding after the proxied path
  is measured.
- Whether chunk-backed `Blob`/bytes results belong in the first public binding
  API or remain a transparent optimization.
