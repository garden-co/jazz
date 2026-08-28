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
storage are physical arms of those types, encoded as one engine-owned ordinary
Groove enum that is not visible in a schema, query, policy, index, or result:

```text
StoredScalar<kind> = enum {
  Primitive { value: primitive for kind },
  Chunked {
    format_version,
    logical_hash,
    root: { object_hash, locator },
    byte_length,
    utf16_length?,
    edit_tail: [ { offset, delete_length, insert_bytes,
                   utf16_offset, delete_utf16_length, insert_utf16_length } ],
  },
}

LargeValueRef {
  format_version,
  kind,
  logical_hash,
  root: NodeRef { object_hash, locator },
  byte_length,
  utf16_length?,
  edit_tail,
}
```

The enum case tags are stable: `Primitive = 2`, `Chunked = 3`. Tags 0 and 1
are permanently reserved so persisted values from the superseded private
`[tag] + payload` codec fail closed instead of colliding with a canonical
length-prefixed record. Its payloads use only Groove's canonical primitive,
record, array, nullable, and enum codecs;
there is no private tag byte or postcard envelope for a scalar descriptor.
Every engine-owned record field has a permanent, one-based numeric record-slot
identity independent of source declaration order: `NodeRef = { 1: object_hash, 2: locator }`, edits are
`{ 1: offset, 2: delete_length, 3: insert_bytes, 4: utf16_offset,
5: delete_utf16_length, 6: insert_utf16_length }`, and `LargeValueRef` is
`{ 1: format_version, 2: logical_hash, 3: root, 4: byte_length,
5: utf16_length, 6: edit_tail }`. Changing an ID is a format change, not a
refactor. Implementations normalize source declarations by ID, then materialize
each ID as its actual record ordinal. A skipped or retired ID remains a
reserved `Nullable<bytes>` slot encoded exactly as `null`; readers reject a
nonempty reserved slot and writers never compact it away. Thus source reorder
is harmless, while renumbering `locator:2` to `locator:3`, an edit field, or a
reference field changes canonical physical bytes and rejects the old layout.
`bytes` uses the bytes primitive and `string` uses the string primitive. JSON
retains Groove's existing canonical JSON-as-string logical representation, so
its primitive backing is string as well. The ordinary enum schema is
parameterized by the immutable kind supplied by schema lowering, so neither
physical arm duplicates that context. Inline payloads are interpreted as the
primitive selected by that schema; the same raw UTF-8 content can therefore be
a valid string or JSON value when stored under the corresponding column kind.
Every independently addressed immutable tree node carries and authenticates its
own format and kind before traversal. Internal raw string/bytes backing primitives
only terminate this self-hosting enum encoding and are impossible at the public
schema or logical-operator boundary.

`logical_hash` is deterministic content identity. `object_hash` authenticates
the exact encoded node, including the child locators that it reveals, and
`locator` is an opaque random 256-bit capability interpreted only by
Groove's chunk subsystem. Storage adapters derive any backend-specific or
prefixed key internally; such keys never cross the retrieval protocol. Groove
treats object hashes and locators as non-semantic:
changing only the retrieval graph cannot change logical equality, ordering,
grouping, an IVM node id, an index key, or query output.

Groove allocates every new locator internally from 32 unmodified bytes supplied
by the operating system CSPRNG. Production construction, streaming,
consolidation, append, and edit APIs do not accept locator allocators from
callers; tests may inject deterministic allocators only through crate-private
test helpers.

Small logical values remain inline. Above a versioned threshold, Groove emits a
large descriptor and immutable chunks. Once indirect, a value may remain
indirect below the threshold; demotion is representation-only compaction.

The physical enum is part of Groove's storage encoding, not a magic prefix in a
user string or JSON value. Every admitted cell has exactly one unambiguous arm.

`INV-LARGE-1`: filters, policies lowered into Groove, joins, grouping, ordering,
aggregation, indices, projections, subscriptions, and application results MUST
observe the logical value. They MUST NOT compare or expose descriptors, hashes,
locators, tree nodes, chunk boundaries, or edit-tail encoding.

### Groove-owned async storage

Groove owns both the storage dependency and every operation over it. The byte
plane may be implemented by a small policy-blind async KV interface with exact
`get`, immutable `put_if_absent`, and hash-guarded `delete`; Groove allocates
random locators and performs integrity checks. Implementations may be memory,
filesystem, OPFS, RocksDB, or a remote blob adapter.

The reference implementation names this backend seam `ChunkKvStorage` and
wraps it in Groove's `ManagedChunkStorage`. The wrapper, rather than each
backend, authenticates bytes and enforces immutable locator semantics. The
default persistent adapter stores byte mappings in Groove's reserved ordered
storage family, so RocksDB, OPFS/IDB and memory inherit their normal reopen and
durability lifecycle without Jazz retaining a backend handle.

Child edges, durable refcounts, staging generations and reclamation work require
atomic metadata updates with physical row mutations. The default composition
stores that metadata in Groove's transactional ordered storage and uses the
async KV only for immutable bytes. A backend claiming a unified implementation
may provide the same guarantees internally. Jazz/other callers never coordinate
the two planes.

Authorization is orthogonal to storage. Jazz's ordinary row/view authorization
controls descriptor and locator discovery; Groove maintains no mutable root
grant registry. Once disclosed, an opaque locator plus its authenticated hash
is sufficient for exact retrieval. Groove discovers descendants only by
authenticating a parent node. The KV backend receives no policy identity or
authorization context.

## 9.2 Tree and chunk format

Leaves contain logical source bytes. Branches contain ordered child references
and exact aggregate metrics:

```text
NodeRef { object_hash, locator }
Leaf    { format, kind, bytes }
Branch  { format, kind, children: [{ node_ref, byte_length, utf16_length? }] }
```

Leaves are selected by a versioned FastCDC-like content-defined chunker with
hard minimum, target, and maximum sizes. Branches use content-defined grouping
over a private kind/format-neutral content fingerprint derived from complete
child descriptors. Keeping this grouping fingerprint separate from logical
identity means a representation-version or semantic-kind distinction does not
arbitrarily reshuffle otherwise identical content. Recursive grouping produces
a deterministic prolly tree: identical kind, format and logical base bytes
produce the same logical hashes and shape independent of edit history. A branch's object hash
commits to its exact child `NodeRef`s, including locators; the separate logical
hash excludes retrieval identities. Unchanged nodes may retain their locators
across versions, while an independently created equal value may have a different
retrieval graph and the same logical identity.

The current immutable-node format is version 1. Every leaf and branch embeds
both that format and its semantic kind. Decoding MUST reject either field when
it differs from the expected descriptor context. The locator-independent
logical hash commits to the format and kind as well as the leaf bytes or branch
child descriptors. Groove derives that logical identity from the grouping
fingerprint with a reversible full-width kind/format domain mask, allowing
localized consolidation to recover canonical grouping without persisting a
second hash in each child descriptor. Consequently, identical UTF-8 or JSON-compatible bytes do
not share logical identities across bytes, text, and JSON values.

### V1 codec dispatch and permanent layout

`LargeValueRef.format_version` selects the immutable-node codec before any
descriptor-guided traversal, upload-frontier walk, finalization, edit-tail
replay, materialization, or upload export interprets a node. The selected codec
MUST reject a node whose embedded format differs. A decoder MUST NOT try the
current codec after an unknown, malformed, or mismatched format fails.

V1 is the only supported case in the format dispatch table. Its stored-scalar
arm remains the schema-known `Primitive = 2 | Chunked = 3` enum: schema lowers
the bytes/string/JSON kind, so neither arm adds a client-controlled kind tag.
V1's permanent numeric identities are:

```text
ChunkNode = enum { Leaf = 0, Branch = 1 }
Leaf        = { 1: format:u8, 2: kind:u8, 3: bytes:bytes }
Branch      = { 1: format:u8, 2: kind:u8, 3: children:[BranchChild] }
BranchChild = {
  1: object_hash:bytes32, 2: locator:bytes32, 3: byte_length:u64,
  4: utf16_length:u64?, 5: logical_hash:bytes32
}
```

These records use ordinary normative Groove enum/record/scalar encoding. V1
has no serde/postcard envelope or private node tag. Exact v1 fixtures cover a
leaf node and object hash, an indirect descriptor with a bounded edit tail, and
the schema-known stored-scalar wrapper. Each fixture decodes to the stated
semantic value and byte-identically re-encodes; trailing, alternate, unknown,
or descriptor/node-version-mismatched bytes fail before child discovery,
locator disclosure, upload accounting, or metadata mutation. A future format
MUST add one explicit dispatch case and its own reviewed fixtures; it cannot
reinterpret v1 bytes or use a fallback decoder.

The following v1 fixtures are authoritative hex, where `object` is
`object_hash(encoded_node)` and `logical` is the locator-independent node
logical hash. They are repeated verbatim by
`large_values::tests::v1_codec_golden_bytes_decode_semantically_and_reject_alternates`.

| semantic value     | canonical node bytes         | object                                                             | logical / metrics                                                                         |
| ------------------ | ---------------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| bytes `v1-fixture` | `00010076312d66697874757265` | `84e5cf641223d7cd2110f4d1b891d150af05976427192759fd8aba94df9b638e` | `357c7ae2a6895ca8c8f21120af72cc37cdc43d2929f506cb17a543c03e90da65`; bytes `10`            |
| string `v1-🙂`     | `00010176312df09f9982`       | `8ca733330fa66126ffe0096382780f11b272047aea7be36fc5c23b6f16d8680a` | `9e87a0d9054fae5cbe72f01ea5ee76cf97c0073ae4955797470365ab08952661`; bytes `7`, UTF-16 `5` |
| JSON `{"n":-0}`    | `0001027b226e223a2d307d`     | `a9865ebcd28c3e1868f670948dd798c1ff5287c025efe5d26b344c261e628dcc` | `4be873c1509bff9e7e4a861e5e405c29088d67a49cd685ec82be6f4fb4532a28`; bytes/UTF-16 `8`      |

For the bytes leaf above, locator `44` repeated 32 times, logical hash
`357c…da65`, and the no-op bounded tail `Replace { offset:9, delete:1,
insert:"e", utf16:* = 0 }`, the descriptor fixture is:

```text
00010a000000000000000000000000000000003a0000007e000000357c7ae2a6895ca8c8f21120af72cc37cdc43d2929f506cb17a543c03e90da652400000084e5cf641223d7cd2110f4d1b891d150af05976427192759fd8aba94df9b638e4444444444444444444444444444444444444444444444444444444444444444010000000900000000000000010000000000000000000000000000000000000000000000000000000000000065
```

The same value wrapped in the bytes schema's ordinary `Chunked = 3` scalar arm
is exactly the preceding bytes with its leading `00` enum tag replaced by `03`.

`INV-LARGE-11`: descriptor-led format dispatch and canonical v1 fixture
validation MUST fail closed before a malformed physical representation can
change lifecycle state or disclose an authenticated child capability.

V1 dispatch is joint: the outer descriptor's `format_version` is selected
before its root record, every nested edit record, or the `Chunked = 3`
stored-scalar payload is interpreted. The schema-known scalar kind still
supplies bytes/string/JSON context; it is not a version selector and never
supplies a client-controlled kind tag. A future version therefore needs one
reviewed descriptor/edit/scalar decoder as well as its node decoder. It MUST
NOT reuse V1's edit-coordinate validation simply because the outer enum tag is
unchanged.

The raw dispatch preflight reads only the canonical outer enum tag and the
first fixed `format_version:u8` descriptor payload byte. It does not bind the
descriptor record, calculate variable-field spans, decode a root `NodeRef`, or
decode an edit before selecting the codec. Therefore an unknown/future version
with a truncated or deliberately non-V1 nested layout fails as
`UnsupportedFormat`, rather than as a V1 malformed record. Once V1 is selected,
the full current descriptor/edit/scalar payload is decoded and exactly
re-encoded; V1 malformed or trailing bytes still fail closed.

Staged-root receipts and pending-upload journals store the canonical complete
`LargeValueRef` encoding as an opaque `bytes` metadata field (nullable bytes
for an unbound pending descriptor), not as a nested generic descriptor value.
On every recovery/read path Groove runs that byte field through the same raw
preflight and codec decoder before it interprets root or edit fields. The
`future_descriptor_metadata_fails_before_v1_binding_without_mutation` reopen
receipt installs a future outer tag/version plus invalid nested bytes in both
journals, requires `UnsupportedFormat`, and proves the durable records remain
byte-identical after rejection. No metadata reader may create a V1 decoding
bypass by embedding a descriptor inside another record.

V1's content-defined profile is permanent format data:

```text
leaf minimum / target / maximum = 16,384 / 65,536 / 262,144 bytes
leaf short / long masks         = 0x0001ffff / 0x00007fff
branch minimum / target / max   = 4 / 16 / 64 children
branch short / long masks       = 0x0000001f / 0x00000007
gear(byte) = SplitMix64(byte + 0x9e3779b97f4a7c15), with multipliers
             0xbf58476d1ce4e5b9 and 0x94d049bb133111eb
```

The leaf and branch rolling accumulators shift left by one and add that gear
value per input byte or recovered grouping-hash byte. Before target they cut
on the short mask; at/after target they cut on the long mask; the stated hard
maximum always cuts. These values include the gear construction, not merely
the visible size constants. They cannot be tuned, seeded from a runtime RNG,
or changed for one backend without a new immutable format version.
`v1_content_defined_profile_manifest_is_permanent` freezes gear vectors,
short- and long-mask leaf ranges over a deterministic multi-megabyte input,
the no-match hard maximum, UTF-8 boundary repair, and every object/logical hash
of a multi-branch grouping manifest. This is a planted sensitivity receipt:
changing either mask or any gear parameter changes at least one frozen range
or branch object/logical hash.

V1 JSON is literal validated UTF-8 source, never a normalized serialization.
Its canonical receipt uses the exact source
`{"literal":"\\uD83D\\uDE42","dup":1,"dup":2,"n":-0,"scientific":1e+00,"text":"\\u00e9"}`;
the leaf bytes, object hash, and logical hash are respectively
`0002027b226c69746572616c223a225c75443833445c7544453432222c22647570223a312c22647570223a322c226e223a2d302c22736369656e7469666963223a31652b30302c2274657874223a225c7530306539227d`,
`c41c347e4880ecb7545a859a9fee9e011551ac5795ced9b5c7b58c77bf69f8b0`, and
`b538953f883dc4ca231dc62746a6b3e8413b772e2baf267f16be791bd58fcb3f`.
The source preserves duplicate spelling and number spelling physically; a
parsed object read has ordinary last-duplicate-key behavior, retains negative
zero, and decodes the Unicode escapes. The exact descriptor and scalar
fixtures for a whole-document replacement containing literal `🙂` and `é` are
authoritatively exercised by
`v1_json_literal_duplicate_numeric_unicode_and_tail_fixtures_are_canonical`.
They prove exact re-encoding, trailing-byte rejection, malformed UTF-8/JSON
rejection, whole-value-only JSON edits, UTF-8-to-UTF-16 metric agreement, and
tail consolidation back to an empty-tail immutable root.

Nodes use Groove's ordinary canonical enum/record codec rather than a private
serialization envelope. A leaf is `{ format, kind, bytes }`; a branch is
`{ format, kind, children }`, where each child is the ordinary record
`{ object_hash, locator, byte_length, utf16_length?, logical_hash }`. The exact
canonical bytes are object-hashed. A byte appended to a leaf's raw-bytes field
is therefore authenticated content, not ignorable trailing data.
Branch array counts are bounded before allocating or decoding child records.
The same untyped authenticated structural validator (object hash, canonical
encoding, format, kind-shaped metrics, leaf bounds, fanout, and overflow) is
used by traversal, upload admission, and metadata-only storage observers.

Every branch child MUST report a positive `byte_length`; only a root `Leaf`
may represent an empty logical value. Canonical construction and consolidation
omit empty replacement segments while other bytes remain and emit exactly one
empty root leaf when the final value is empty. This keeps every branch edge a
positive contribution to logical-size accounting.

This rule is deliberately fail-closed for historical compatibility.
Authenticated persisted or wire branch nodes containing a zero-byte child are
malformed and may no longer be readable. Groove does not migrate them because
discovering or reconstructing their descendants would traverse the adversarial
graph that this rule excludes; physical metadata can still reclaim their raw
chunks independently.

Text leaf boundaries are valid UTF-8 code-point boundaries. Text branches also
carry exact aggregate UTF-16 code-unit lengths. JSON uses literal validated
UTF-8 source bytes; it is not stored as a persistent object graph.

Every decoded node is checked against the expected object hash learned from its
parent (or the owner descriptor for the root). Branch fanout, depth, positive
child byte metrics, total metrics, and encoded sizes are bounded and checked.
Unknown format versions, cycles, dishonest metrics, invalid UTF-8, invalid
JSON, arithmetic overflow, and malformed child references fail the affected
evaluation closure.

Postcard decoding alone is insufficient because it accepts a valid value with
trailing bytes. Every path that interprets node structure MUST require an exact
byte-for-byte canonical re-encoding, including metadata-only traversal before
the caller has an expected logical kind. Evaluation additionally verifies the
expected object hash, format, kind, logical hash, and metrics.

The authenticated structure is a DAG, not necessarily a tree physically: one
exact `NodeRef` may occur repeatedly in a branch or beneath several parents.
Physical reachability, upload-frontier, admission and collection walks visit
that immutable node once while validating that every incoming edge agrees on
its logical hash and metrics. They still retain the exact active path needed to
reject a cycle. Logical evaluation is different: each edge occurrence denotes
bytes at a distinct logical position and therefore remains observable. Every
synchronous materialization or range attempt charges both visited occurrences
and expanded child edges against a deterministic work budget; exceeding it is a
typed failure rather than an unbounded allocation or CPU loop.

The encoded-node size ceiling is checked before hashing or deserialization, so
an authenticated transport envelope cannot turn one malicious node into an
unbounded hashing or allocation operation.

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
delete, overwrite, text splice, and file mutation lower to the same primitive.
JSON currently admits only a complete replacement of the current logical value;
the inserted source must itself be valid JSON.

Admission bounds patch count, total encoded tail bytes, inserted bytes, and all
range arithmetic. For text, the byte boundaries and the exact UTF-16 offset and
length effects are recomputed against the source value produced by preceding
edits. An untrusted staged descriptor is replayed from its immutable base before
publication so forged text coordinates, partial JSON edits, and noncanonical
tails fail closed.

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

The exact Rust representation may differ, but it has one lifecycle. Equal exact
keys within one Groove database share one in-flight future and result. A chunk
request key contains the expected object hash and opaque locator. Jazz performs
authorization before descriptor disclosure, so Groove does not add a second
mutable access-context identity to request sharing.

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

The Jazz query DSL maps its page descriptors directly to this vocabulary:
byte `[from,to)` ranges, UTF-16 `[from,to)` text ranges, UTF-8 byte ranges for
text, and RFC 6901 JSON-pointer demand. A range descriptor is a demand, not a
materialized value or a storage API. Its coordinate system is validated before
evaluation; text UTF-8 endpoints MUST be code-point boundaries and UTF-16
endpoints MUST NOT split a surrogate pair. The result is exactly the requested
logical primitive/subtree and exposes neither descriptor nor chunk details.

For partial updates, Jazz lowers a page-relative sequential splice list into
the ordinary bounded edit tail. Each splice is interpreted against the result
of previous splices, and a deletion is valid only when it remains within the
original selected page after translating that sequential coordinate. JSON
pointer edits are likewise lowered to the same logical bytes/edit tail after
RFC 6901 resolution; there is no JSON-specific mutable representation.

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

JSON validity is enforced when a JSON value is prepared for publication. Reads
assume that write-admission invariant rather than revalidating every unread
suffix. A pointer read fails if the demanded source is malformed, but it may
finish without fetching unrelated later bytes once the selected value is
complete. This is the same trust boundary as every other schema/type invariant:
storage and sync do not admit host-fabricated physical descriptors as ordinary
writes.

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
The execution work budget is independent of storage suspension: after consuming
the configured logical-byte allowance, a node MUST yield even if every required
chunk is resident. A failed or cancelled window does not advance its published
cursor, and a partial accumulator is never exposed as an operator result.

The reference operator is a streaming checksum. It has a fixed-size accumulator,
is easy to compare with a one-shot oracle, and exercises cursor resumption,
chunk suspension, work-budget yielding, cancellation, source invalidation and
atomic result publication without introducing text semantics or unbounded
operator state. As a graph transformation it preserves the input row and
replaces one String/Bytes field with a named 32-byte BLAKE3 field. Completion
creates one prepared derived row; the ordinary non-suspending publication
boundary applies it atomically. Updates retract the checksum of the exact old
logical value and insert the checksum of the exact replacement value.

This checksum exists primarily as a conformance and scaling probe for streaming
nodes. Its builder surface is available to black-box tests and benchmarks, but
it is not an application-facing checksum API or a product feature commitment.

`INV-LARGE-6`: sequential processing of a large value MUST be possible with
resident input memory bounded by the configured chunk/window size plus its
declared bounded accumulator, independent of the value's total size.

## 9.7 Chunk residency and result ownership

Four quantities are separately accounted:

1. durable bytes in Groove's chunk storage;
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
byte value, or parsed JSON value and releases input chunks after conversion.
For this design, NAPI and WASM preserve their existing encoded-row boundary and
copy the completed encoded result into host-owned memory. No chunk lease crosses
either binding, no Rust finalizer participates in host-buffer lifetime, and
Groove does not retain old emitted results merely because application code
might retain its copy.

Cache eviction cannot reclaim application-owned output. Implementations expose
separate metrics and backpressure for cache bytes, evaluation leases, external
result buffers, and operator state.

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
has no visible database mutation. Groove allocates locators and stages immutable
chunks through its async storage. Publication of the owning row and descriptor
follows the caller's ordinary authorization and Groove's atomic physical-record
boundary.

Durable ownership follows Groove's physical record lifecycle, not a host-side
root registry. Inserting a physical record increments every large-value root
named in that record; deleting it decrements those roots in the same persisted
batch. Replacement applies the descriptor multiset delta. Cache or arrangement
eviction is not physical-record deletion and never changes durable counts.

When an immutable node mapping is first installed, Groove records its child
edges exactly once. A node's transition from inactive to active contributes one
persisted inbound reference to each child; additional root/parent references to
an already-active immutable node do not duplicate its child edges. The inverse
zero transition recursively removes those contributions and queues the node for
bounded, restartable reclamation. Idempotently restaging identical content is
edge-neutral and installs zero new physical bytes/nodes. Its staging receipt
still reports the full incoming upload size so Jazz can rate-limit ingress work
independently of physical deduplication. Locator mappings independently retain
any deduplicated physical blob.

Prepared chunks not yet present in a physical record use a separate persisted,
expiring staging generation. Active reads use temporary leases. Neither is a
durable record reference.

The first collector coordinates through a coarse database-wide ephemeral
evaluation retainer. It starts a pass only when there are no suspended chunk
requests or verified-byte leases, and new requests wait behind the pass. This
protects descendants that an evaluator learned from an authenticated branch but
has not requested yet; per-root coordination may later reduce conservative
deferral without weakening that boundary.

Remote uploads begin with the descriptor and traverse root-first. Groove
authenticates each received node before using its child edges, derives the
missing frontier from persisted nodes, and asks for only absent nodes; Jazz never buffers a whole
tree. Completion is derived from an empty validated frontier rather than a
sender finalization assertion. Partial uploads and completed timestamped
retainer claims carry persisted creation/accounting metadata so host-driven
expiry can reclaim abandoned uploads without consulting Jazz row history.
Those timestamps are GC and resource-management metadata, not synchronous
admission deadlines: while a journal or receipt remains present, chunk pushes,
finalization, and publication may proceed regardless of wall-clock age. Once
maintenance evicts it, stale handles fail by absence and cannot recreate it.
Frontier collection itself stops at the negotiated batch bound; it does not
allocate an unbounded list and truncate it after returning. A stale frontier
response racing another uploader is accepted only when its redundant nodes
exactly match the already-installed immutable mappings, after which Groove may
issue a distinct timestamped claim without recounting those nodes.

The initial metadata layout reserves Groove's `__groove_large_values` logical
storage family. Completed staging persists descriptor-keyed retainer claims plus
accounting receipts. A `DatabaseBatch` consumes any live claim only when the
same batch contains its exact descriptor; the row delta adds durable ownership
while consuming the claim removes staging ownership. Root zero-crossings update
recursive node counts and append exact `(locator, hash)` work to a persisted
reclamation queue. The reclaimer removes metadata only after hash-guarded byte
deletion succeeds, so crashes retry without walking row history.

The metadata key namespace is also the record discriminant. `root/<NodeRef>`
contains the canonical Groove record `{ 1: durable:u64, 2: staged:u64,
3: node_active:bool }`; `node/<NodeRef>` contains `{ 1: references:u64,
2: upload_references:u64, 3: children:[NodeRef] }`; `staged/<StagingId>`
contains `{ 1: id:bytes16, 2: value_ref:LargeValueRef,
3: encoded_bytes:u64, 4: node_count:u64, 5: created_at_ms:u64 }`; and
`upload/<StagingId>` contains `{ 1: id:bytes16, 2: descriptor:LargeValueRef?,
3: receipt_id:bytes16?, 4: encoded_bytes:u64, 5: node_count:u64,
6: created_at_ms:u64, 7: chunks:[NodeRef] }`. Numeric field IDs are permanent,
one-based physical record slots and source declaration order is not semantic.
If a future layout retires or skips an ID, it MUST retain every gap as a
reserved `Nullable<bytes>` record slot encoded exactly as `null`; readers MUST
reject a nonempty reserved slot and writers MUST NOT compact it away. Thus
renumbering a field (for example `children:3` to `children:4`) changes the
record layout and cannot be silently normalized by declaration-name sorting.
Each value is exactly its normal
Groove record encoding: there is no metadata magic prefix, private type tag,
or serde/postcard envelope. Key prefixes are fixed engine-owned namespaces and
must select their sole expected record shape; malformed, truncated, trailing,
non-canonical, or mismatched records fail before any lifecycle/GC mutation.
`reclaim/<NodeRef>` repeats that exact canonical `NodeRef` record as its value
so reclamation MUST compare key and value before any lifecycle/GC mutation and
cannot retarget a queue entry; `install/<NodeRef>` is an empty presence marker
whose key alone carries the identity.

`INV-LARGE-9`: physical-record mutation and descriptor-reference deltas MUST be
crash-consistent and idempotent. A node/blob MUST NOT be reclaimed while its
durable inbound count, staging protection, or active lease is nonzero.

Derived indices and expensive operator outputs identify their inputs with:

```text
logical value identity (root hash + canonical tail)
operator identity and version
operator configuration
schema version where applicable
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
- Whether a future NAPI/WASM protocol should carry chunk-backed byte/blob
  handles or multipart buffers for zero-copy results. This is an optimization
  outside the current design; it would require explicit host finalizers,
  external-memory accounting and lease-aware backpressure.
- Exact JSON formatting policy after semantic merges.
- Exact Jazz ingress limits and host maintenance cadence for staging expiry.
  Groove persists receipt timestamps and performs requested eviction, but does
  not choose policy or run a backend-specific expiry worker. Jazz exposes one
  idempotent maintenance call across its Rust, server, NAPI, and WASM surfaces;
  each environment supplies its ordinary native or JavaScript timer cadence.
- Any future history truncation/thinning implementation MUST delete physical
  versions through Groove's refcount-aware record mutation path. Direct storage
  deletion would leak or prematurely reclaim large-value trees.
