# Groove Specification · 9. Large values

## Overview

Groove represents large `bytes`, `string`, and `json` values as ordinary logical
scalars whose physical value may be an immutable content-defined tree plus a
bounded edit tail. Large values are not rows, relations, arrangements, or a
second query engine. Every relational operator observes the same logical value
it would observe if the complete primitive were inline.

The physical bytes may be absent from the local process. Any IVM node may make
progress, complete, yield, or suspend on one or more evaluation requests. Chunk
requests use the interruptible evaluation lifecycle from chapter 8 alongside
ordered-storage requests. The evaluation session, not a host retry loop, owns
blocked work, request sharing, cancellation, resumption, and failure scope.

Invariant digest:

- `INV-LARGE-1`: physical representation is unobservable to logical consumers.
- `INV-LARGE-2`: any node may suspend on a complete discovered request frontier.
- `INV-LARGE-3`: suspended work publishes no partial state or terminal output.
- `INV-LARGE-4`: tree construction and edit interpretation are deterministic,
  bounded, and integrity checked.
- `INV-LARGE-5`: operators request only the evidence needed by their semantics.
- `INV-LARGE-6`: streaming evaluation has bounded resident input memory.
- `INV-LARGE-7`: chunk residency, evaluation leases, and result ownership are
  separate lifetimes.
- `INV-LARGE-8`: persistent derived state is keyed by exact logical input and
  operator identity.

## 9.1 Logical and physical values

The public Groove type remains `bytes`, `string`, or `json`. Inline and indirect
storage are physical arms of those types, conceptually:

```text
StoredScalar = Inline(logical bytes) | Large(LargeValueRef)

LargeValueRef {
  kind,
  format_version,
  logical_hash,
  root: NodeRef { object_hash, locator },
  byte_length,
  utf16_length?,
  edit_tail,
}
```

`logical_hash` is deterministic content identity. `object_hash` authenticates
the exact encoded node, including the child locators that it reveals, and
`locator` is the opaque retrieval capability interpreted by the host-supplied
chunk provider. Groove treats object hashes and locators as non-semantic:
changing only the retrieval graph cannot change logical equality, ordering,
grouping, an IVM node id, an index key, or query output.

Small logical values remain inline. Above a versioned threshold, Groove emits a
large descriptor and immutable chunks. Once indirect, a value may remain
indirect below the threshold; demotion is representation-only compaction.

The physical tag is part of Groove's storage encoding, not a magic prefix in a
user string or JSON value. Every admitted cell has exactly one unambiguous arm.

`INV-LARGE-1`: filters, policies lowered into Groove, joins, grouping, ordering,
aggregation, indices, projections, subscriptions, and application results MUST
observe the logical value. They MUST NOT compare or expose descriptors, hashes,
locators, tree nodes, chunk boundaries, or edit-tail encoding.

## 9.2 Tree and chunk format

Leaves contain logical source bytes. Branches contain ordered child references
and exact aggregate metrics:

```text
NodeRef { object_hash, locator }
Leaf    { format, bytes }
Branch  { format, children: [{ node_ref, byte_length, utf16_length? }] }
```

Leaves are selected by a versioned FastCDC-like content-defined chunker with
hard minimum, target, and maximum sizes. Branches use content-defined grouping
over complete child descriptors. Recursive grouping produces a deterministic
prolly tree: identical kind, format and logical base bytes produce the same
logical hashes and shape independent of edit history. A branch's object hash
commits to its exact child `NodeRef`s, including locators; the separate logical
hash excludes retrieval identities. Unchanged nodes may retain their locators
across versions, while an independently created equal value may have a different
retrieval graph and the same logical identity.

Text leaf boundaries are valid UTF-8 code-point boundaries. Text branches also
carry exact aggregate UTF-16 code-unit lengths. JSON uses literal validated
UTF-8 source bytes; it is not stored as a persistent object graph.

Every decoded node is checked against the expected object hash learned from its
parent (or the owner descriptor for the root). Branch fanout, depth, child
metrics, total metrics and encoded sizes
are bounded and checked. Unknown format versions, cycles, dishonest metrics,
invalid UTF-8, invalid JSON, arithmetic overflow, trailing bytes, and malformed
child references fail the affected evaluation closure.

`INV-LARGE-4`: identical logical base bytes under one format MUST produce the
same logical hashes and tree shape. A reader MUST validate every fetched node's
object hash before it can satisfy an operator or reveal authenticated descendant
locators.

## 9.3 Bounded edit tail

Recent mutations are byte replacements applied in order:

```text
Replace { offset, delete_length, insert_bytes, utf16_effects? }
```

Each offset addresses the value produced by all preceding edits in the tail.
Append is a replacement at the current length with zero deletion. Insert,
delete, overwrite, text splice, file mutation, and JSON replacement lower to
the same primitive.

Admission bounds patch count, total encoded tail bytes, inserted bytes, and all
range arithmetic. The final result must satisfy the logical kind; intermediate
states within one atomic tail need not independently be valid text or JSON.

When adding an edit would exceed a bound, Groove streams the current logical
value through the edit, rechunks until content boundaries resynchronize, stages
new immutable chunks, and emits a new root with an empty tail. It does not need
the complete value resident at once.

Reads map a requested logical range backwards through the bounded tail. Inserted
portions are served from tail bytes; remaining portions become base-tree ranges.
Only tree paths and leaves intersecting those ranges are requested.

## 9.4 Evaluation requests

Chapter 8's storage-specific registry becomes one evaluation-request registry:

```rust,ignore
enum EvaluationRequestKey {
    Storage(StorageRequestKey),
    Chunk(ChunkRequestKey),
}

enum EvaluationRequestOutput {
    Storage(StorageRequestOutput),
    Chunk(VerifiedChunk),
}
```

The exact Rust representation may differ, but it has one lifecycle. Equal keys
within a compatible access context share one in-flight future and result. A
chunk request key contains the expected object hash and opaque locator; the
access context is fixed by the Groove database/operation capability and MUST
participate in request-sharing identity wherever authorizations can differ.

Any IVM node may return one of these conceptual outcomes:

```text
Complete(prepared output)
Await(complete currently discovered request set)
Yielded(private continuation state)
Failed(scoped error)
```

Storage and chunk dependencies may be discovered in rounds. A branch node must
be loaded before its child locators are known. The evaluator runs all independent
runnable work and unions every currently discoverable request before awaiting.

A node attempt may retain explicit, bounded node-specific state in the owned
evaluation session. It MUST NOT retain a recursively nested future as its
continuation. It MUST NOT mutate published arrangements, memo state, durable
indices, or terminal output before completion. Cancellation or stale-input
invalidation discards its private state without rollback.

`INV-LARGE-2`: every node kind MAY await storage and chunk requests through the
same evaluation session. Missing chunks MUST NOT escape Groove as a retryable
query error or host-driven re-evaluation protocol.

`INV-LARGE-3`: a blocked, yielded, cancelled, stale, or failed node MUST expose
none of its partial accumulator, arrangement changes, index changes, or terminal
output. Independent closures may continue and publish according to chapter 8.

## 9.5 Lazy scalar operations

Large values pass through records as logical references. There is no mandatory
`ResolveLargeValues` graph node. Each consuming operator uses Groove-owned,
fallible scalar operations through its evaluation context. Inline values take
the same path and complete immediately.

The internal operation vocabulary includes at least:

```text
logical byte length
logical UTF-16 length
byte range
UTF-16 text range
full bytes/text/JSON
logical equality
lexicographic comparison
logical hash
JSON pointer / structural parse demand
sequential logical-byte cursor
```

Operators request only evidence needed to decide their own output:

- equality first checks kind and authenticated metrics, then compares successive
  logical ranges and stops at the first mismatch;
- lexical comparison stops at the first differing logical unit;
- `count(*)` does not touch unused large columns;
- projection requests only its selected full value, range, or JSON pointer;
- ordering requests enough prefixes to prove the ordering actually needed;
- grouping and equality joins may use a logical hash as a candidate key but
  MUST verify collisions through logical equality;
- length uses authenticated descriptor metrics without fetching leaves;
- JSON pointer parsing requests only enough source to locate and validate the
  selected subtree when the parser can prove that safely;
- full JSON parsing and application materialization may require every logical
  byte, but still need not make one additional contiguous byte copy internally.

An operator may discover additional requests after earlier chunks arrive. This
is progress, not a retry of a visible operation.

`INV-LARGE-5`: an operator MUST NOT require full materialization merely because
its input is indirect. Its demand is the minimum conservative evidence needed
to produce exactly the same result as the inline oracle.

## 9.6 Streaming nodes

Every node can retain bounded private evaluation state, so no special streaming
node subtype exists. A streaming computation repeatedly:

```text
consume a resident logical window
  -> update private cursor and accumulator
  -> release the input lease
  -> await the next required chunks or yield to the work budget
  -> complete one prepared output
```

The cursor is over the final logical value, not raw tree leaves; it accounts for
edit-tail insertions, deletions and replacements. Cursor state contains the
exact `LargeValueRef`, logical offset and bounded tree traversal stack. A source
version replacement invalidates the cursor before any result is published.

Examples include tokenization, term-frequency extraction, checksums, streaming
validation, and vector embedding. TF/IDF should normally lower into a streaming
per-document tokenizer that prepares `(document, term, count)` deltas followed
by ordinary arrangements, joins and aggregates for corpus-wide IDF. Embedding
state is keyed by model/tokenizer version and configuration.

If an accumulator itself is unbounded, it uses evaluation-private spill storage
rather than retaining unbounded memory. Spill data is unpublished, namespaced
by evaluation identity, and deleted on cancellation/failure. Completion creates
one prepared delta or derived value; the ordinary non-suspending publication
boundary applies it atomically.

`INV-LARGE-6`: sequential processing of a large value MUST be possible with
resident input memory bounded by the configured chunk/window size plus explicit
operator accumulator and spill budgets, independent of the value's total size.

## 9.7 Chunk residency and result ownership

Four quantities are separately accounted:

1. durable bytes in the host's chunk backend;
2. cache-owned verified bytes;
3. leases held by active evaluations;
4. buffers retained by returned or externally owned results.

One shared, byte-weighted cache keeps verified chunks under a configured budget.
An evaluation receives a lease such as `Arc<ChunkBytes>`. Streaming work drops
each lease after consuming its window. Eviction removes the cache's ownership
immediately even if an evaluator or result still owns a lease; bytes are freed
when the last owner releases them. In-flight request coalescing is separate from
cache retention.

Durable IVM records and arrangements retain `LargeValueRef`, not loaded chunks.
Derived state retains its declared output, never incidental input leases.

A terminal requesting an idiomatic full primitive owns a materialized string,
byte value, or parsed JSON value and releases input chunks after conversion. A
chunk-backed bytes/blob result may instead own immutable chunk leases; that
ownership transfers across a binding and is released by the host object's
normal destructor/finalizer. Groove does not retain old emitted results merely
because application code might retain them.

Cache eviction cannot reclaim application-owned output. Implementations expose
separate metrics and backpressure for cache bytes, evaluation leases, external
result buffers, operator state, and spill storage.

`INV-LARGE-7`: removing a chunk from the shared cache MUST remain safe while an
evaluation or returned result owns it. Conversely, durable IVM state MUST NOT
pin input chunks unless its declared output is explicitly chunk backed.

## 9.8 Writes, consolidation, and derived state

Write preparation accepts ordinary logical values or typed edit operations.
Groove produces:

```text
PreparedLargeValue {
  physical_cell,
  staged_chunks,
}
```

Preparation may suspend while reading an old tree, may stream through it, and
has no visible database mutation. The host stages immutable chunks. Publication
of the owning row and the resulting descriptor follows the host database's
ordinary authorization and atomic row-publication boundary.

Derived indices and expensive operator outputs identify their inputs with:

```text
logical value identity (root hash + canonical tail)
operator identity and version
operator configuration
model/tokenizer/schema version where applicable
```

Changing only retrieval locators does not invalidate derived state. Changing
any logical input does. A partially computed derivation is never installed as
current state.

`INV-LARGE-8`: reusable derived state MUST commit to every semantic input and
operator/configuration version that can affect its output and MUST NOT commit to
non-semantic chunk location.

## 9.9 JSON semantics

JSON source remains literal UTF-8 JSON bytes in the same tree and edit format as
bytes and text. Complete replacement is deterministically lowered to byte edits;
persisted operations do not form a second JSON-tree mutation protocol.

Queries may use a streaming parser to satisfy pointer and predicate demands.
The parser retains bounded syntactic state and requests further logical windows
as needed. It may finish early only when it has proved the exact semantic answer
and any validation required by the operation. Full materialization returns the
ordinary parsed JSON value.

A three-way JSON merge parses the common base and both attributed candidates,
computes a semantic side-attributed diff, applies the configured merge strategy,
and emits ordinary canonical byte edits or a consolidated tree. Ambiguous array
identity and moves remain conflicts unless application data provides stable
identity. Formatting policy is strategy-defined; untouched source bytes remain
exact.

## 9.10 Failure and completeness

Chunk failure is scoped like chapter 8's node-evaluation failure:

- unknown locator, authorization denial, permanent absence, corruption, invalid
  format, and retryable backend failure are distinct internal causes;
- a failed node terminates its dependent low-level terminals and invalidates
  affected private/maintained state;
- unrelated graph closures continue;
- a one-shot query does not complete while required derivations are unresolved;
- a subscription publishes only outputs proven complete for that terminal;
- replacement of a referenced value invalidates blocked work for the old value;
- installing a chunk never mutates logical database time or create an IVM row
  delta by itself; it only makes blocked evaluation runnable.

## Open questions

- Exact initial thresholds, chunk profile, edit-tail bounds and cache defaults
  require benchmark receipts across text, files, JSON and append workloads.
- Which full-value persistent index forms should be supported initially rather
  than rejected in favor of explicit hash/path/metric indices.
- Whether application-facing chunk-backed byte/blob results belong in the first
  API or remain an internal/binding optimization.
- Exact JSON formatting policy after semantic merges.
- Long-term collection policy for unreachable immutable chunks is host-owned and
  specified by Jazz chapter 19 for the Jazz integration.
