# Mutable-file adapter plan: embedded content manifest

Status: implementation plan for the file PR stacked on the embedded-content
foundation. It deliberately adds no `snapshot_heads` table and no legacy
large-value compatibility path.

## Boundary with the foundation PR

The foundation owns the generic atomic content-column contract:

```ts
type ContentManifest<Root, Tail> = {
  root: Root;
  editTail: Tail;
};
```

It also owns decoding a manifest before a content-aware merge strategy,
interior query, or index consumes it. A manifest is one conflict value: Jazz
must never choose `root` from one candidate and `editTail` from another.

This PR only supplies `content.file()`: its root/table codec, edit-tail codec,
materializer, operations, and tests. It must not redefine generic manifest
atomicity, generic materialization hooks, generic interior-index plumbing, or
the other three content kinds.

An application row is the file's mutable identity:

```ts
documents.attachment = { root: F7, editTail: [...] };
```

Copying that manifest is an exact snapshot. A separate independently mutable
file is an ordinary application row with a `content.file()` column, rather
than a mandatory substrate head.

## File shape

`FileRootId` names an immutable, domain-scoped, content-addressed root of a
persistent byte B-tree. A root records the byte length, fixed `bytes`
coordinate unit, root node ID, encoding/schema version, and its authorization
domain. It does **not** have a back-reference to the owning application row.

Immutable child rows are content-addressed from canonical binary encodings:

- `FileNode`: height, ordered child IDs, byte lengths, and node version;
- `FileLeaf`: either bounded inline bytes, a slice of an immutable external
  descriptor, or a bounded patch-chain leaf;
- `BlobDescriptor`: immutable object generation, object digest, offset/range,
  byte length, encryption/key version, authorization domain, and descriptor
  version.

The hash preimage includes the row kind, version, authorization/encryption
domain, and every semantic field. It is never ordinary JSON. Identical content
can deduplicate only within that domain. A known ID never authorizes a read.
Inserting an already-existing derived ID is accepted only when its canonical
contents match exactly; a different payload under that ID is corruption.

## The tail is per affected extent, not global

Unlike an append-only stream, a file has no single logical mutable tail. Its
bounded `editTail` is an ordered, canonicalized set of byte edits, each tied to
an immutable base leaf/slice and anchored to the manifest's root:

```ts
type FileEdit =
  | {
      kind: "overwrite";
      baseLeaf: FileLeafId;
      offset: u32;
      deleteBytes: u32;
      insert: InlineBytes | BlobDescriptorId;
    }
  | { kind: "insert"; anchor: FileBoundary; insert: InlineBytes | BlobDescriptorId }
  | { kind: "delete"; start: FileBoundary; deleteBytes: u32 };

type FileTail = {
  baseRoot: FileRootId;
  edits: readonly FileEdit[];
};
```

The exact wire names can follow the foundation conventions, but these
properties are non-negotiable:

1. The tail is anchored to `root`; it cannot be replayed against another root.
2. Edits have a deterministic order and non-overlap/coordinate validation.
3. The cap is global for the encoded tail _and_ local for every changed leaf's
   patch chain. A file may therefore have several active patch chains.
4. A tail is materialized as part of the logical byte sequence before merge,
   index extraction, or interior query observes the value.
5. Consolidation applies all accepted tail edits into a new immutable B-tree,
   then publishes `{ root: F8, editTail: [] }` atomically as one content value.

Small overwrites need only a manifest update while their patches fit. A
consolidation copies the changed leaves and the B-tree paths to them; untouched
nodes, descriptor slices, and leaves are reused. It must never rewrite an
entire file merely because a small middle range changed.

## Operations and read paths

`readRange(manifest, range)` must validate the manifest, seek the root tree,
overlay only tail edits intersecting `range`, and fetch only necessary leaf
bytes. Descriptor-backed segments must use a bounded object-store range fetch;
they may not materialize the whole file to serve a small range.

`overwrite`, `insert`, and `delete` work through persistent
`split(root, offset)` / `concat(left, right)` primitives. Boundary fragments
are represented as descriptor slices plus inline or descriptor-backed patches;
the implementation preserves byte offset arithmetic and rejects overflow,
out-of-bounds ranges, malformed slice boundaries, and mismatched lengths.

File-aware merge materializes each candidate's `root + editTail` under its
declared base and merges only the supported byte-edit intent. Concurrent
overlapping writes need an explicit policy (initially conflict, unless the
foundation has supplied a deterministic operation merge rule); it must not
silently create a byte sequence that neither writer authored. A merge result
is re-encoded as one validated file manifest.

For `content.file()` interior queries/indices, the foundation invokes the file
materializer with the smallest requested byte interval. The initial adapter
should expose only range/metadata predicates that can be answered this way.
It must not claim that arbitrary full-content indexing is cheap or automatic;
an index definition that needs all bytes explicitly requests full
materialization and is subject to an adapter size bound.

## Upload, descriptor, and authority boundary

An external-object upload is not publication. The sequence is:

1. authorize the writer and quota before issuing an upload receipt;
2. perform a private conditional upload, binding its digest, generation,
   domain, encryption/key version, and permitted range to that receipt;
3. verify the returned descriptor against the receipt;
4. create immutable descriptor/tree rows and publish the new application-row
   manifest in one Jazz transaction; then await authority;
5. retain an unreferenced uploaded object only as a private, grace-period
   orphan if authority rejects the transaction; authoritative reachability
   cleanup may collect it later.

No signed URL, reusable bearer token, or mutable object handle is persisted in
an immutable descriptor. A read signer rechecks the caller's access to the
referencing manifest/root and only then issues a short-lived,
audience/method/range-limited capability. The reachability check follows the
application row's authorization domain; guessing a root or descriptor ID must
not bypass it.

## Acceptance test plan

The implementation PR should use public Jazz APIs for black-box integration
tests and add narrow codec/tree tests only where a corruption rejection cannot
be induced through that surface. Before Rust tests are written, follow
`crates/jazz/TESTING_GUIDELINES.md`.

| concern                     | required evidence                                                                                                                                                                                                          |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| embedded snapshot identity  | a row version reads exactly its `{root, editTail}`; copying it remains unchanged after later owner-row file edits                                                                                                          |
| multi-extent tails          | independent small overwrites in two leaves remain separately represented/materialized; the test plants an erroneous single-global-tail implementation and observes the wrong bytes                                         |
| persistent edits            | middle overwrite, insert, delete, split/concat and boundary-crossing edits preserve byte results and reuse untouched subtree IDs                                                                                           |
| range reads                 | a 64 KiB range from a descriptor-backed large file requests only intersecting ranges and overlays only intersecting patches; plant full-file hydration and make the request fail                                           |
| consolidation               | cap crossing produces a new root and an empty tail, preserves old-root reads, and does not change untouched descriptors/nodes                                                                                              |
| content-addressed integrity | same canonical row inserts idempotently; different bytes/metadata under a derived ID, altered length vectors, wrong domain/version, malformed child height, and slice overflow are rejected                                |
| merge                       | disjoint anchored edits have the specified deterministic outcome; same-range writes conflict (or exercise the explicitly selected operation rule); root/tail cross-candidate mixing is rejected                            |
| interior materialization    | file merge strategy and range predicate/index see bytes after overlaying tail; an intentionally root-only implementation fails the assertion                                                                               |
| history and permission      | historical owner-row manifests remain readable under valid historical access, while guessed root/leaf/descriptor IDs and cross-domain references are denied                                                                |
| upload receipt              | altered digest, generation, key/domain, range, expired receipt, replayed receipt, unauthorised upload, and direct descriptor publication all fail; valid upload becomes readable only after accepted authority publication |
| corruption and cleanup      | corrupt blob range/digest response fails closed; rejected publication leaves no readable descriptor; cleanup only collects an unreachable expired private orphan, never a descriptor reachable through retained history    |

The PR must record what it can and cannot prove locally: an in-memory fake can
prove receipt binding and reachability decisions, but a real object-store
integration receipt is required before claiming durable upload or HTTP Range
behavior.

## Stack boundary and gates

This adapter is PR 4 above the foundation and the text, stream, and JSON
adapter PRs. It takes the final stack base supplied by the coordinator; it does
not rebase itself to `main` or duplicate the foundation in order to compile
early. Focused tests plus the shared manifest materialization/merge/index
canaries run while iterating; normal canonical gates and the sensitive-data
guard run before the coordinator publishes the stack.

Tooling friction observed during planning: the prior experiments use several
different terms (`extent`, `leaf`, `part`, `frontier`) and a standalone JSON
model, so their guarantees need to be re-expressed as typed adapter invariants
rather than copied as implementation shapes. A shared manifest fixture and
public object-store receipt harness would prevent every adapter from rebuilding
those seams independently.
